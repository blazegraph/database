/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Aug 16, 2010
 */

package com.bigdata.bop.controller;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.SampleIndex;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.relation.rule.Rule;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IChunkedIterator;

/**
 * A join graph with annotations for estimated cardinality and other details in
 * support of runtime query optimization. A join graph is a collection of
 * relations and joins which connect those relations. This boils down to a
 * collection of {@link IPredicate}s (selects on relations) and shared variables
 * (which identify joins).
 * <p>
 * 
 * @see http://arxiv.org/PS_cache/arxiv/pdf/0810/0810.4809v1.pdf, XQuery Join
 *      Graph Isolation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * TODO Some edges can be eliminated by transitivity. For example, given
 * 
 *       <pre>
 * query:
 * 
 * :- (A loves B), (B loves A), (B marriedTo C).
 * 
 * vertices: 
 * 
 * v1=(A loves B);
 * v2=(B loves A);
 * v3=(B marriedTo C);
 * 
 * edges:
 * 
 * e1=(v1,v2) // JOIN( SCAN(A loves B), SCAN(B loves A)) 
 * e2=(v1,v3) // JOIN( SCAN(A loves B), SCAN(B marriedTo C))
 * e3=(v2,v3) // JOIN( SCAN(B loves A), SCAN(B marriedTo C))
 *   
 * It is necessary to execute e1 and either e2 or e3, but not both e2 and e3.
 * </pre>
 * 
 * TODO In order to combine pipelining with runtime query optimization we need
 *       to sample based on the first chunk(s) delivered by the pipeline. If
 *       necessary, we can buffer multiple chunks for semi-selective queries.
 *       However, for unselective queries we would accept as many buffers worth
 *       of bindings as were allowed for a given join and then sample the
 *       binding sets from left hand side (the buffers) and run those samples
 *       against the right hand side (the local shard).
 */
public class JoinGraph extends PipelineOp {

	private static final transient Logger log = Logger.getLogger(JoinGraph.class);
	
    private static final long serialVersionUID = 1L;

    /**
     * Known annotations.
     */
    public interface Annotations extends PipelineOp.Annotations {
		/**
    	 * The vertices of the join graph expressed an an {@link IPredicate}[]. 
    	 */
		String VERTICES = JoinGraph.class.getName() + ".vertices";
		
		/**
		 * The initial limit for cutoff sampling (default {@value #DEFAULT_LIMIT}).
		 */
		String LIMIT = JoinGraph.class.getName() + ".limit";
		
		int DEFAULT_LIMIT = 100; 
    }

	/**
     * @see Annotations#VERTICES
     */
    public IPredicate[] getVertices() {
    
    	return (IPredicate[]) getRequiredProperty(Annotations.VERTICES);
    	
    }

    /**
     * @see Annotations#LIMIT
     */
    public int getLimit() {
    	
    	return getProperty(Annotations.LIMIT, Annotations.DEFAULT_LIMIT);
    	
    }
    
    public JoinGraph(final NV ...anns) {
        
		this(BOpBase.NOARGS, NV.asMap(anns));
    	
    }

	/**
	 * 
	 * TODO We can derive the vertices from the join operators or the join
	 *       operators from the vertices. However, if a specific kind of join
	 *       operator is required then the question is whether we have better
	 *       information to make that choice when the join graph is evaluated or
	 *       before it is constructed.
	 * 
	 * TODO How we will handle optional joins? Presumably they are outside of
	 *       the code join graph as part of the tail attached to that join
	 *       graph.
	 * 
	 * TODO How can join constraints be moved around? Just attach them where
	 *       ever a variable becomes bound? And when do we filter out variables
	 *       which are not required downstream? Once we decide on a join path
	 *       and execute it fully (rather than sampling that join path).
	 */
    public JoinGraph(final BOp[] args, final Map<String,Object> anns) {

    	super(args,anns);

        switch (getEvaluationContext()) {
        case CONTROLLER:
            break;
        default:
            throw new UnsupportedOperationException(
                    Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new JoinGraphTask(context));

    }

    /**
     * Used to assign row identifiers.
     */
	static private final IVariable<Integer> ROWID = Var.var("__rowid");

    /**
     * A sample of a {@link Vertex} (an access path).
     */
    public static class VertexSample {

		/**
		 * Fast range count. This will be the same for each sample taken
		 * (assuming a read historical view or even a time scale of query which
		 * is significantly faster than update).
		 */
		public final long rangeCount;

    	/**
		 * The limit used to produce the {@link #sample}.
		 */
    	public final int limit;

		/**
		 * When <code>true</code>, the result is not a sample but the
		 * materialized access path.
		 * 
		 * @todo When <code>true</code>, we could run the join against the
		 *       sample rather than the disk. This would require wrapping the
		 *       sample as an access path. Since all exact samples will be
		 *       pretty small, this is not likely to have any great performance
		 *       benefit.
		 */
    	public final boolean exact;
    	
		/**
		 * Sample.
		 */
		final Object[] sample;

		/**
		 * 
		 * @param rangeCount
		 * @param limit
		 * @param exact
		 * @param sample
		 */
		public VertexSample(final long rangeCount, final int limit, final boolean exact, final Object[] sample) {

			if (rangeCount < 0L)
				throw new IllegalArgumentException();

			if (limit <= 0)
				throw new IllegalArgumentException();
			
			if (sample == null)
				throw new IllegalArgumentException();

			this.rangeCount = rangeCount;
			
			this.limit = limit;
			
			this.exact = exact;
			
			this.sample = sample;
			
		}

		public String toString() {
			return "VertexSample{rangeCount=" + rangeCount + ",limit=" + limit
					+ ",exact=" + exact + ", sampleSize=" + sample.length + "}";
		}
		
    }

	/**
	 * A vertex of the join graph is an annotated relation (this corresponds to
	 * an {@link IPredicate} with additional annotations to support the adaptive
	 * query optimization algorithm).
	 * <p>
	 * The unique identifier for a {@link Vertex} (within a given join graph) is
	 * the {@link BOp.Annotations#BOP_ID} decorating its {@link IPredicate}.
	 * {@link #hashCode()} is defined in terms of this unique identifier so we
	 * can readily detect when a {@link Set} already contains a given
	 * {@link Vertex}.
	 */
	public static class Vertex implements Serializable {

		/**
         * 
         */
		private static final long serialVersionUID = 1L;

		public final IPredicate<?> pred;

		/**
		 * The most recently taken sample of the {@link Vertex}.
		 */
		VertexSample sample = null;
		
		Vertex(final IPredicate<?> pred) {

			if (pred == null)
				throw new IllegalArgumentException();
			
			this.pred = pred;
			
		}
		
		public String toString() {

			return "Vertex{pred=" + pred + ",sample=" + sample + "}";
		
		}

		/**
		 * Equals is based on a reference test.
		 */
		public boolean equals(Object o) {
			return this == o;
		}

		/**
		 * The hash code is just the {@link BOp.Annotations#BOP_ID} of the
		 * associated {@link IPredicate}.
		 */
		public int hashCode() {
			return pred.getId();
		}

		/**
		 * Take a sample of the vertex. If the sample is already exact, then
		 * this is a NOP.
		 * 
		 * @param context
		 * @param limit
		 *            The sample cutoff.
		 */
		public void sample(final BOpContextBase context, final int limit) {

			if (context == null)
				throw new IllegalArgumentException();

			if (limit <= 0)
				throw new IllegalArgumentException();
			
			final VertexSample oldSample = this.sample;

			if(oldSample != null && oldSample.exact) {

				/*
				 * The old sample is already the full materialization of the
				 * vertex.
				 */
				
				return;
				
			}
			
			final IRelation r = context.getRelation(pred);

			final IAccessPath ap = context.getAccessPath(r, pred);

			final long rangeCount = oldSample == null ? ap
					.rangeCount(false/* exact */) : oldSample.rangeCount;

			if (rangeCount <= limit) {

				/*
				 * Materialize the access path.
				 * 
				 * @todo This could be more efficient if we raised it onto the
				 * AP or if we overrode CHUNK_CAPACITY and the fully buffered
				 * iterator threshold such that everything was materialized as a
				 * single chunk.
				 */
				
				final List<Object> tmp = new ArrayList<Object>((int) rangeCount);

				final IChunkedIterator<Object> itr = ap.iterator();

				try {

					while (itr.hasNext()) {

						tmp.add(itr.next());

					}

				} finally {

					itr.close();
				}

				sample = new VertexSample(rangeCount, limit, true/* exact */,
						tmp.toArray(new Object[0]));
				
				return;

			}

			/*
			 * Materialize a random sample from the access path. 
			 */
			
			final SampleIndex sampleOp = new SampleIndex(new BOp[] {}, //
					NV.asMap(//
							new NV(SampleIndex.Annotations.PREDICATE, pred),//
							new NV(SampleIndex.Annotations.LIMIT, limit)));

			sample = new VertexSample(rangeCount, limit, false/*exact*/, sampleOp
					.eval(context));

		}
		
	}

	/**
	 * A sample of an {@link Edge} (a join).
	 */
	public static class EdgeSample {

		/**
		 * The fast range count (aka cardinality) for the source vertex of the
		 * edge (whichever vertex has the lower cardinality).
		 */
		public final long rangeCount;

		/**
		 * The limit used to sample the edge (this is the limit on the #of
		 * solutions generated by the cutoff join used when this sample was
		 * taken).
		 */
		public final int limit;

		/**
		 * The #of binding sets out of the source sample vertex sample which
		 * were consumed.
		 */
		public final int inputCount;

		/**
		 * The #of binding sets generated before the join was cutoff.
		 * <p>
		 * Note: If the outputCount is zero then this is a good indicator that
		 * there is an error in the query such that the join will not select
		 * anything. This is not 100%, merely indicative.
		 */
		public final int outputCount;
		
		/**
		 * The ratio of the #of input samples consumed to the #of output samples
		 * generated (the join hit ratio or scale factor).
		 */
		public final double f;
		
		/**
		 * The estimated cardinality of the join.
		 */
		public final long estimatedCardinality;

		/**
		 * Flag is set when the estimate is likely to be a lower bound for the
		 * cardinality of the edge.
		 * <p>
		 * If the {@link #inputCount} is ONE (1) and the {@link #outputCount} is
		 * the {@link #limit} then the {@link #estimatedCardinality} is a lower
		 * bound as more than {@link #outputCount} solutions could have been
		 * produced by the join against a single input solution.
		 */
		public final boolean estimateIsLowerBound;

		/**
		 * Flag indicates that the {@link #estimatedCardinality} underflowed.
		 * <p>
		 * Note: When the source vertex sample was not exact, then it is
		 * possible for the cardinality estimate to underflow. When, in
		 * addition, {@link #outputCount} is LT {@link #limit}, then feeding the
		 * sample of source tuples in is not sufficient to generated the desired
		 * #of output tuples. In this case, {@link #f join hit ratio} will be
		 * low. It may even be that zero output tuples were generated, in which
		 * case the join hit ratio will appear to be zero. However, the join hit
		 * ratio actually underflowed and an apparent join hit ratio of zero
		 * does not imply that the join will be empty unless the source vertex
		 * sample is actually the fully materialized access path - see
		 * {@link VertexSample#exact} and {@link #exact}.
		 */
		public final boolean estimateIsUpperBound;

		/**
		 * <code>true</code> if the sample is the exact solution for the join
		 * path.
		 * <p>
		 * Note: If the entire source vertex is being feed into the sample,
		 * {@link VertexSample#exact} flags this condition, and outputCount is
		 * also LT the limit, then the edge sample is the actual result of the
		 * join. That is, feeding all source tuples into the join gives fewer
		 * than the desired number of output tuples.
		 * 
		 * @todo This field marks this condition and should be used to avoid
		 *       needless recomputation of a join whose exact solution is
		 *       already known.
		 */
		public final boolean exact; 
		
		/**
		 * The sample of the solutions for the join path.
		 */
		private final IBindingSet[] sample;

		/**
		 * Create an object which encapsulates a sample of an edge.
		 * 
		 * @param limit
		 *            The limit used to sample the edge (this is the limit on
		 *            the #of solutions generated by the cutoff join used when
		 *            this sample was taken).
		 * @param sourceVertexSample
		 *            The sample for source vertex of the edge (whichever vertex
		 *            has the lower cardinality).
		 * @param inputCount
		 *            The #of binding sets out of the source sample vertex
		 *            sample which were consumed.
		 * @param outputCount
		 *            The #of binding sets generated before the join was cutoff.
		 */
		EdgeSample(final VertexSample sourceVertexSample, final int limit,
				final int inputCount, final int outputCount,
				final IBindingSet[] sample) {

			if(sample == null)
				throw new IllegalArgumentException();
			
			this.rangeCount = sourceVertexSample.rangeCount;
			
			this.limit = limit;
			
			this.inputCount = inputCount;
			
			this.outputCount = outputCount;
			
			f = outputCount == 0 ? 0 : (outputCount / (double) inputCount);

			estimatedCardinality = (long) (rangeCount * f);
			
			estimateIsLowerBound = inputCount == 1 && outputCount == limit;
			
			estimateIsUpperBound = !sourceVertexSample.exact
					&& outputCount < limit;
			
			this.exact = sourceVertexSample.exact && outputCount < limit;
			
			this.sample = sample;
		}
		
		public String toString() {
			return getClass().getName() + "{inputRangeCount=" + rangeCount
					+ ", limit=" + limit + ", inputCount=" + inputCount
					+ ", outputCount=" + outputCount + ", f=" + f
					+ ", estimatedCardinality=" + estimatedCardinality
					+ ", estimateIsLowerBound=" + estimateIsLowerBound
					+ ", estimateIsUpperBound=" + estimateIsUpperBound
					+ ", sampleIsExactSolution=" + exact
					+ "}";
		}
		
	};

	/**
	 * An edge of the join graph is an annotated join operator. The edges of the
	 * join graph are undirected. Edges exist when the vertices share at least
	 * one variable.
	 * <p>
	 * {@link #hashCode()} is defined in terms of the unordered hash codes of
	 * the individual vertices.
	 */
	public static class Edge implements Serializable {

		/**
         * 
         */
		private static final long serialVersionUID = 1L;

		/**
		 * The vertices connected by that edge.
		 */
		public final Vertex v1, v2;

		/**
		 * The set of shared variables.
		 */
		public final Set<IVariable<?>> shared;

		/**
		 * The last sample for this edge and <code>null</code> if the edge has
		 * not been sampled.
		 */
		public EdgeSample sample = null;
		
		public Edge(final Vertex v1, final Vertex v2, final Set<IVariable<?>> shared) {
			if (v1 == null)
				throw new IllegalArgumentException();
			if (v2 == null)
				throw new IllegalArgumentException();
			if (shared==null)
				throw new IllegalArgumentException();
			if (shared.isEmpty())
				throw new IllegalArgumentException();
			this.v1 = v1;
			this.v2 = v2;
			this.shared = shared;
		}

		/**
		 * Note: The vertices of the edge are labeled using the
		 * {@link BOp.Annotations#BOP_ID} associated with the {@link IPredicate}
		 * for each vertex.
		 */
		public String toString() {
			
			return "Edge{ (V" + v1.pred.getId() + ",V" + v2.pred.getId() + ")"
					+ ", shared=" + shared.toString() + ", sample=" + sample
					+ "}";

		}

		/**
		 * Equality is determined by reference testing.
		 */
		public boolean equals(final Object o) {
			
			return this == o;
			
		}

		/**
		 * The hash code of an edge is the hash code of the vertex with the
		 * smaller hash code X 31 plus the hash code of the vertex with the
		 * larger hash code. This definition compensates for the arbitrary order
		 * in which the vertices may be expressed and also recognizes that the
		 * vertex hash codes are based on the bop ids, which are often small
		 * integers.
		 */
		public int hashCode() {

			if (hash == 0) {

				final int h1 = v1.hashCode();
				final int h2 = v2.hashCode();

				final int h;
				if (h1 < h2) {
				
					h = h1 * 31 + h2;
					
				} else {
					
					h = h2 * 31 + h1;
					
				}

				hash = h;

			}
	        return hash;

	    }
	    private int hash;

	    /**
		 * Return the vertex with the smaller estimated cardinality.
		 * 
		 * @throws IllegalStateException
		 *             if either vertex has not been sampled.
		 */
		public Vertex getMinimumCardinalityVertex() {

			if (v1.sample == null) // vertex not sampled.
				throw new IllegalStateException();
			
			if (v2.sample == null) // vertex not sampled.
				throw new IllegalStateException();
			
			return (v1.sample.rangeCount < v2.sample.rangeCount) ? v1 : v2;
			
		}
		
	    /**
		 * Return the vertex with the larger estimated cardinality (the vertex
		 * not returned by {@link #getMinimumCardinalityVertex()}).
		 * 
		 * @throws IllegalStateException
		 *             if either vertex has not been sampled.
		 */
		public Vertex getMaximumCardinalityVertex() {

			// The vertex with the minimum cardinality.
			final Vertex o = getMinimumCardinalityVertex();
			
			// Return the other vertex.
			return (v1 == o) ? v2 : v1;
			
		}
		
		/**
		 * Estimate the cardinality of the edge.
		 * 
		 * @param context
		 * 
		 * @return The estimated cardinality of the edge.
		 * 
		 * @throws Exception 
		 */
		public long estimateCardinality(final QueryEngine queryEngine,
				final int limit) throws Exception {

			if (limit <= 0)
				throw new IllegalArgumentException();

			/*
			 * Figure out which vertex has the smaller cardinality. The sample
			 * of that vertex is used since it is more representative than the
			 * sample of the other vertex.
			 */
			// vertex v, vprime
			final Vertex v, vp;
			if (v1.sample == null) // vertex not sampled.
				throw new IllegalStateException();
			if (v2.sample == null) // vertex not sampled.
				throw new IllegalStateException();
			if (v1.sample.rangeCount < v2.sample.rangeCount) {
				v = v1;
				vp = v2;
			} else {
				v = v2;
				vp = v1;
			}

			/*
			 * TODO This is difficult to setup because we do not have a concept
			 * (or class) corresponding to a fly weight relation and we do not
			 * have a general purpose relation, just arrays or sequences of
			 * IBindingSets. Also, all relations are persistent. Temporary
			 * relations are on a temporary store and are locatable by their
			 * namespace rather than being Objects.
			 * 
			 * The algorithm presupposes fly weight / temporary relations this
			 * both to wrap the sample and to store the computed intermediate
			 * results.
			 * 
			 * Note: The PipelineJoin does not have a means to halt after a
			 * limit is satisfied. In order to achieve this, we have to wrap it
			 * with a SliceOp.
			 * 
			 * Together, this means that we are dealing with IBindingSet[]s for
			 * both the input and the output of the cutoff evaluation of the
			 * edge rather than rows of the materialized relation.
			 * 
			 * TODO On subsequent iterations we would probably re-sample [v]
			 * and we would run against the materialized intermediate result for
			 * [v'].
			 */

			/*
			 * Convert the source sample into an IBindingSet[].
			 * 
			 * @todo We might as well do this when we sample the vertex.
			 */
			final IBindingSet[] sourceSample = new IBindingSet[v.sample.sample.length];
			{
				for (int i = 0; i < sourceSample.length; i++) {
					final IBindingSet bset = new HashBindingSet();
					BOpContext.copyValues((IElement) v.sample.sample[i], v.pred, bset);
					sourceSample[i] = bset;
				}
			}

			// Sample the edge and save the sample on the edge as a side-effect.
			this.sample = estimateCardinality(queryEngine, limit, v, vp, sourceSample);
			
			return sample.estimatedCardinality;

		}

		/**
		 * Estimate the cardinality of the edge.
		 * 
		 * @param queryEngine
		 * @param limit
		 * @param vSource
		 *            The source vertex.
		 * @param vTarget
		 *            The target vertex
		 * @param sourceSample
		 *            The sample for the source vertex. When this is a one-step
		 *            estimation of the cardinality of the edge, then this
		 *            sample is taken from the {@link VertexSample}. When the
		 *            edge (vSource,vTarget) extends some {@link Path}, then
		 *            this is taken from the {@link EdgeSample} for that
		 *            {@link Path}.
		 * 
		 * @return The result of sampling that edge.
		 * 
		 * @throws Exception
		 */
		public EdgeSample estimateCardinality(final QueryEngine queryEngine,
				final int limit, final Vertex vSource, final Vertex vTarget,
				IBindingSet[] sourceSample) throws Exception {

			if (limit <= 0)
				throw new IllegalArgumentException();
		
			// Inject a rowId column.
			sourceSample = BOpUtility.injectRowIdColumn(ROWID, 1/* start */,
					sourceSample);

			/*
			 * TODO Any constraints on the edge (other than those implied by
			 * shared variables) need to be annotated on the join. Constraints
			 * (other than range constraints which are directly coded by the
			 * predicate) will not reduce the effort to compute the join, but
			 * they can reduce the cardinality of the join and that is what we
			 * are trying to estimate here.
			 */
			final PipelineJoin joinOp = new PipelineJoin(new BOp[] {}, //
					new NV(BOp.Annotations.BOP_ID, 1),//
					new NV(PipelineJoin.Annotations.PREDICATE,vTarget.pred.setBOpId(3))
					);

			final SliceOp sliceOp = new SliceOp(new BOp[] { joinOp },//
					NV.asMap(//
							new NV(BOp.Annotations.BOP_ID, 2), //
							new NV(SliceOp.Annotations.LIMIT, (long)limit), //
							new NV(
									BOp.Annotations.EVALUATION_CONTEXT,
									BOpEvaluationContext.CONTROLLER)));

			// run the cutoff sampling of the edge.
			final UUID queryId = UUID.randomUUID();
			final RunningQuery runningQuery = queryEngine.eval(queryId,
					sliceOp, new LocalChunkMessage<IBindingSet>(queryEngine,
							queryId, joinOp.getId()/* startId */,
							-1 /* partitionId */,
							new ThickAsynchronousIterator<IBindingSet[]>(
									new IBindingSet[][] { sourceSample })));

			// #of source samples consumed.
			int inputCount = 0;
			// #of output samples generated.
			int outputCount = 0;
			final List<IBindingSet> result = new LinkedList<IBindingSet>();
			try {
				try {
					IBindingSet bset = null;
					// Figure out the #of source samples consumed.
					final Iterator<IBindingSet> itr = new Dechunkerator<IBindingSet>(
							runningQuery.iterator());
					while (itr.hasNext()) {
						bset = itr.next();
						result.add(bset);
						outputCount++;
					}
					// #of input rows consumed.
					inputCount = bset == null ? 0 : ((Integer) bset.get(ROWID)
							.get());
				} finally {
					// verify no problems. FIXME Restore test of the query.
//					runningQuery.get();
				}
			} finally {
				runningQuery.cancel(true/* mayInterruptIfRunning */);
			}

			/*
			 * Note: This needs to be based on the source vertex having the
			 * minimum cardinality for the Path which is being extended which
			 * connects via some edge defined in the join graph. If a different
			 * vertex is chosen as the source then the estimated cardinality
			 * will be falsely high by whatever ratio the chosen vertex
			 * cardinality exceeds the one having the minimum cardinality which
			 * is connected via an edge to the target vertex).
			 */
			final VertexSample moreSelectiveVertexSample = vSource.sample.rangeCount < vTarget.sample.rangeCount ? vSource.sample
					: vTarget.sample;
			
			final EdgeSample edgeSample = new EdgeSample(
					moreSelectiveVertexSample/* vSource.sample */, limit,
					inputCount, outputCount, result
							.toArray(new IBindingSet[result.size()]));

			if (log.isInfoEnabled())
				log.info("edge=" + this + ", sample=" + edgeSample);

			return edgeSample;

		}
		
	}

//	/**
//	 * A path sample includes the materialized binding sets from the as-executed
//	 * join path.
//	 * 
//	 * @todo The sample {@link IBindingSet}[] could be saved with the
//	 *       {@link EdgeSample}. However, when we are sampling a join path we
//	 *       want to associate the net sample with the path, not each edge in
//	 *       that path, because we need to be able to generate join paths in
//	 *       which the path is extended from any vertex already part of the path
//	 *       to any vertex which has not yet incorporated in the path and has
//	 *       not yet been executed. To do this we need to intermediate results
//	 *       for the path, which includes all variables bound by each join for
//	 *       each edge in the path, not just on an edge by edge basis.
//	 */
//	public static class PathSample extends EdgeSample {
//
//		/**
//		 * <code>true</code> if the sample is the exact solution for the join path.
//		 */
//		private final boolean exact; 
//		
//		/**
//		 * The sample of the solutions for the join path.
//		 */
//		private final IBindingSet[] sample;
//
//		PathSample(final long inputRangeCount, final int limit,
//				final int inputCount, final int outputCount,
//				final boolean exact, final IBindingSet[] sample) {
//
//			super(inputRangeCount, limit, inputCount, outputCount);
//
//			if(sample == null)
//				throw new IllegalArgumentException();
//			
//			this.exact = exact;
//			
//			this.sample = sample;
//			
//		}
//
//		public String toString() {
//
//			return super.toString() + ":{exact=" + exact + ", sampleSize="
//					+ sample.length + "}";
//
//		}
//		
//	}
	
	/**
	 * A sequence of {@link Edge}s (aka join steps).
	 */
	public static class Path {
		
		public final List<Edge> edges;

		/*
		 * These fields carry state used by chainSample. It would be better to
		 * have that state on a data structure which is purely local to
		 * chainSample, but perhaps Path is that data structure.
		 */
		
		public EdgeSample sample = null;

//		/**
//		 * Input to the next round of sampling.
//		 */
//		private VertexSample inputSample;

		/**
		 * The vertex at which the path from which this path was derived
		 * stopped. This is initialized to the source vertex when entering the
		 * chainSample() method.
		 */
		private Vertex stopVertex;
		
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("Path{");
			boolean first = true;
			for (Edge e : edges) {
				if (!first)
					sb.append(",");
				sb.append("(" + e.v1.pred.getId() + "," + e.v2.pred.getId() + ")");
				first = false;
			}
			sb.append(",sample=" + sample + "}");
			return sb.toString();
		}

		/**
		 * Create an empty path.
		 */
		public Path() {
			this.edges = new LinkedList<Edge>();
		}

		/**
		 * Create a path from a single edge.
		 * 
		 * @param e
		 *            The edge.
		 */
		public Path(final Edge e) {
			if (e == null)
				throw new IllegalArgumentException();
			this.edges = new LinkedList<Edge>();
			this.edges.add(e);
			this.sample = e.sample;
		}

		/**
		 * Return <code>true</code> iff the {@link Path} contains at least one
		 * {@link Edge} for that {@link Vertex}.
		 * 
		 * @param v
		 *            The vertex
		 * 
		 * @return true if the vertex is already part of the path.
		 */
		public boolean contains(final Vertex v) {

			if (v == null)
				throw new IllegalArgumentException();

			for (Edge e : edges) {

				if (e.v1 == v || e.v2 == v)
					return true;

			}

			return false;
		}
		
		/**
		 * Add an edge to a path, computing the estimated cardinality of the new
		 * path, and returning the new path.
		 * 
		 * @param queryEngine
		 * @param limit
		 * @param e
		 *            The edge.
		 * 
		 * @return The new path.
		 * 
		 * @throws Exception
		 */
		public Path addEdge(final QueryEngine queryEngine, final int limit,
				final Edge e) throws Exception {

			if (e == null)
				throw new IllegalArgumentException();

			// Figure out which vertices are already part of this path.
			final boolean v1Found = contains(e.v1);
			final boolean v2Found = contains(e.v2);

			if (!v1Found && !v2Found)
				throw new IllegalArgumentException(
						"Edge does not extend path: edge=" + e + ", path="
								+ this);

			if (v1Found && v2Found)
				throw new IllegalArgumentException(
						"Edge already present in path: edge=" + e + ", path="
								+ this);

			// The vertex which is already part of this path.
			final Vertex sourceVertex = v1Found ? e.v1 : e.v2;

			// The new vertex, which is not part of this path.
			final Vertex targetVertex = v1Found ? e.v2 : e.v1;

			// Extend the path.
			final Path tmp = new Path();

			tmp.edges.addAll(edges);

			tmp.edges.add(e);

			/*
			 * Chain sample the edge.
			 * 
			 * Note: ROX uses the intermediate result I(p) for the existing path
			 * as the input when sampling the edge. The corresponding concept
			 * for us is the sample for this Path, which will have all variable
			 * bindings produced so far. In order to estimate the cardinality of
			 * the new join path we have to do a one step cutoff evaluation of
			 * the new Edge, given the sample available on the current Path.
			 * 
			 * TODO It is possible for the path sample to be empty. Unless the
			 * sample also happens to be exact, this is an indication that the
			 * estimated cardinality has underflowed. How are we going to deal
			 * with this situation?!? What would appear to matter is the amount
			 * of work being performed by the join in achieving that low
			 * cardinality. If we have to do a lot of work to get a small
			 * cardinality then we should prefer join paths which achieve the
			 * same reduction in cardinality with less 'intermediate
			 * cardinality' - that is, by examining fewer possible solutions.
			 */

//			final IBindingSet[] sample = BOpUtility.injectRowIdColumn(ROWID,
//					0/* start */, this.sample.sample);
			
			final EdgeSample edgeSample = e.estimateCardinality(queryEngine,
					limit, sourceVertex, targetVertex, this.sample.sample);

			tmp.sample = edgeSample;
			
//			tmp.stopVertex = e.getMaximumCardinalityVertex();
			
			return tmp;
			
		}
		
//		/**
//		 * Equality is defined by comparison of the unordered set of edges.
//		 */
//		public boolean equals(final Object o) {
//			if (this == o)
//				return true;
//			if (!(o instanceof Path))
//				return false;
//			final Path t = (Path) o;
//			if (edges.length != t.edges.length)
//				return false;
//			for (Edge e : edges) {
//				boolean found = false;
//				for (Edge x : t.edges) {
//					if (x.equals(e)) {
//						found = true;
//						break;
//					}
//				}
//				if (!found)
//					return false;
//			}
//			return true;
//		}
//		
//		/**
//		 * The hash code of path is defined as the bit-wise XOR of the hash
//		 * codes of the edges in that path.
//		 */
//	    public int hashCode() {
//
//	        if (hash == 0) {
//
//	            int result = 0;
//
//	        	for(Edge e : edges) {
//
//	                result ^= e.hashCode();
//
//	            }
//
//	            hash = result;
//
//	        }
//	        return hash;
//
//	    }
//	    private int hash;

	}

	/**
	 * Comma delimited table showing the estimated join hit ratio, the estimated
	 * cardinality, and the set of vertices for each of the specified join
	 * paths.
	 * 
	 * @param a
	 *            An array of join paths.
	 *            
	 * @return A table with that data.
	 */
	static public String showTable(final Path[] a) {
		final StringBuilder sb = new StringBuilder();
		final Formatter f = new Formatter(sb);
		for (Path x : a) {
			if (x.sample == null) {
				f.format("%7s, %10s", "N/A", "N/A");
			} else {
				f.format("% 7.2f, % 10d", x.sample.f,
						x.sample.estimatedCardinality);
			}
			sb.append(",");
			for (Edge e : x.edges)
				sb.append(" (" + e.v1.pred.getId() + " " + e.v2.pred.getId()
						+ ")");
			sb.append("\n");
		}
		return sb.toString();
	}
	
	/**
	 * A join graph (data structure and methods only).
	 * 
	 * Note: ROX was stated in terms of materialization of intermediate results.
	 * Bigdata was originally designed to support pipelined join evaluation in
	 * which the zero investment property is true (there exists an index for the
	 * join). While support is being developed for operator-at-once joins (e.g.,
	 * hash joins), that support is aimed at more efficient evaluation of high
	 * cardinality joins using multi-block IO. Therefore, unlike ROX, the
	 * runtime query optimizer does not materialize the intermediate results
	 * when chain sampling. Instead, it feeds a sample into a cutoff pipeline
	 * evaluation for the join path. Since some join paths can eliminate a lot
	 * of intermediate solutions and hence take a long time to satisfy the
	 * cutoff, we also specify a timeout for the cutoff evaluation of a join
	 * path. Given the zero investment property (an index exists for the join),
	 * if the cutoff is not satisfied within the timeout, then the join has a
	 * low correlation. If no solutions are generated within the timeout, then
	 * the estimate of the correlation "underflows".
	 * 
	 * Note: timeouts are a bit tricky when you are not running on a real-time
	 * platform. In particular, heavy swapping or heavy GC workloads could both
	 * cause a timeout to expire because no work was done on sampling the join
	 * path rather than because there was a lot of work to be done. Therefore,
	 * the timeout should be used to protect against join paths which take a
	 * long time to materialize <i>cutoff</i> solutions rather than to fine tune
	 * the running time of the query optimizer.
	 * 
	 * TODO Runtime query optimization is probably useless (or else should rely
	 * on materialization of intermediate results) when the cardinality of the
	 * vertices and edges for the query is small. This would let us balance the
	 * design characteristics of MonetDB and bigdata. For this purpose, we need
	 * to flag when a {@link VertexSample} is complete (e.g., the cutoff is GTE
	 * the actual range count). This also needs to be done for each join path so
	 * we can decide when the sample for the path is in fact the exact solution
	 * rather than an estimate of the cardinality of the solution together with
	 * a sample of the solution.
	 */
	public static class JGraph {

		/**
		 * Vertices of the join graph.
		 */
		private final Vertex[] V;

		/**
		 * Edges of the join graph.
		 */
		private final Edge[] E;

		// The set of vertices which have been consumed by the query.
		private final Set<Vertex> executedVertices = new LinkedHashSet<Vertex>();
		
		public List<Vertex> getVertices() {
			return Collections.unmodifiableList(Arrays.asList(V));
		}

		public List<Edge> getEdges() {
			return Collections.unmodifiableList(Arrays.asList(E));
		}

		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("JoinGraph");
			sb.append("{V=[");
			for(Vertex v : V) {
				sb.append("\nV["+v.pred.getId()+"]="+v);
			}
			sb.append("],E=[");
			for(Edge e : E) {
				sb.append("\n"+e);
			}
			sb.append("\n],ExecutedVertices=[");
			for(Vertex v : executedVertices) {
				sb.append("\nV["+v.pred.getId()+"]="+v);
			}
			sb.append("\n]}");
			return sb.toString();
			
//			return super.toString() + "{V=" + Arrays.toString(V) + ",E="
//			+ Arrays.toString(E) + ", executedVertices="+executedVertices+"}"; 
		}
		
		public JGraph(final IPredicate[] v) {

            if (v == null)
                throw new IllegalArgumentException();

			if (v.length < 2)
				throw new IllegalArgumentException();

			V = new Vertex[v.length];

			for (int i = 0; i < v.length; i++) {

				V[i] = new Vertex(v[i]);

			}

			/*
			 * Identify the edges by looking for shared variables among the
			 * predicates.
			 */
			{

				final List<Edge> tmp = new LinkedList<Edge>();

				for (int i = 0; i < v.length; i++) {

					final IPredicate<?> p1 = v[i];

					for (int j = i + 1; j < v.length; j++) {

						final IPredicate<?> p2 = v[j];

						final Set<IVariable<?>> shared = Rule.getSharedVars(p1,
								p2);

						if (shared != null && !shared.isEmpty()) {

							tmp.add(new Edge(V[i], V[j], shared));

						}

					}

				}

				E = tmp.toArray(new Edge[0]);

			}

		}

		/**
		 * Return the {@link Vertex} whose {@link IPredicate} is associated with
		 * the given {@link BOp.Annotations#BOP_ID}.
		 * 
		 * @param bopId
		 *            The bop identifier.
		 * @return The {@link Vertex} -or- <code>null</code> if there is no such
		 *         vertex in the join graph.
		 */
		public Vertex getVertex(int bopId) {
			for(Vertex v : V) {
				if(v.pred.getId()==bopId)
					return v;
			}
			return null;
		}

		/**
		 * Return the {@link Edge} associated with the given vertices. The
		 * vertices may appear in any order.
		 * 
		 * @param v1
		 *            One vertex.
		 * @param v2
		 *            Another vertex.
		 * 
		 * @return The edge -or- <code>null</code> if there is no such edge in
		 *         the join graph.
		 */
		public Edge getEdge(Vertex v1, Vertex v2) {
			for(Edge e : E) {
				if (e.v1 == v1 && e.v2 == v2)
					return e;
				if (e.v1 == v2 && e.v2 == v1)
					return e;
			}
			return null;
		}
		
		/**
		 * Obtain a sample and estimated cardinality (fast range count) for each vertex.
		 * 
		 * @param context
		 * @param limit
		 *            The sample size.
		 */
		public void sampleVertices(final BOpContextBase context, final int limit) {

			for (Vertex v : V) {

				v.sample(context, limit);
				
			}
			
		}

		/**
		 * Estimate the cardinality of each edge.
		 * 
		 * @param context
		 * 
		 * @throws Exception
		 */
		public void estimateEdgeWeights(final QueryEngine queryEngine,
				final int limit) throws Exception {

			for (Edge e : E) {

				if (e.v1.sample == null || e.v2.sample == null) {

					/*
					 * We can only estimate the cardinality of edges connecting
					 * vertices for which samples were obtained.
					 */
					continue;

				}

				e.estimateCardinality(
						queryEngine, limit);

			}

		}

		/**
		 * Return the {@link Edge} having the minimum estimated cardinality out
		 * of those edges whose cardinality has been estimated.
		 * 
		 * @param A
		 *            set of vertices to be excluded from consideration
		 *            (optional).
		 * 
		 * @return The minimum cardinality edge -or- <code>null</code> if there
		 *         are no {@link Edge}s having an estimated cardinality.
		 */
		public Edge getMinimumCardinalityEdge(final Set<Vertex> visited) {
		
			long minCard = Long.MIN_VALUE;
			Edge minEdge = null;

			for (Edge e : E) {

				if (e.sample == null) {
			
					// Edge has not been sampled.
					continue;

				}

				if (visited != null
						&& (visited.contains(e.v1) || visited.contains(e.v2))) {
					
					// A vertex of that edge has already been consumed.
					continue;
					
				}
				
				final long estimatedCardinality = e.sample.estimatedCardinality;

				if (minEdge == null || estimatedCardinality < minCard) {

					minEdge = e;

					minCard = estimatedCardinality;

				}

			}

			return minEdge;
			
		}

//		/**
//		 * Return the {@link Edge} having the minimum estimated cardinality out
//		 * of those edges whose cardinality has been estimated.
//		 * 
//		 * @return The minimum cardinality edge -or- <code>null</code> if there
//		 *         are no {@link Edge}s having an estimated cardinality.
//		 */
//		public Edge getMinimumCardinalityEdge() {
//		
//			return getMinimumCardinalityEdge(null);
//			
//		}
		
		/**
		 * Return the #of edges in which the given vertex appears where the
		 * other vertex of the edge does not appear in the set of visited
		 * vertices.
		 * 
		 * @param v
		 *            The vertex.
		 * @param visited
		 *            A set of vertices to be excluded from consideration.
		 *            
		 * @return The #of such edges.
		 */
		public int getEdgeCount(final Vertex v, final Set<Vertex> visited) {

			return getEdges(v, visited).size();

		}

		/**
		 * Return the edges in which the given vertex appears where the other
		 * vertex of the edge does not appear in the set of visited vertices.
		 * 
		 * @param v
		 *            The vertex.
		 * @param visited
		 *            A set of vertices to be excluded from consideration
		 *            (optional).
		 * 
		 * @return Those edges.
		 */
		public List<Edge> getEdges(final Vertex v, final Set<Vertex> visited) {
			
			if (v == null)
				throw new IllegalArgumentException();

			if (visited != null && visited.contains(v))
				return Collections.emptyList();
			
			final List<Edge> tmp = new LinkedList<Edge>();
			
			for (Edge e : E) {
			
				if (v.equals(e.v1) || v.equals(e.v2)) {

					if (visited != null) {

						if (visited.contains(e.v1))
							continue;

						if (visited.contains(e.v2))
							continue;

					}

					tmp.add(e);
					
				}
				
			}
			
			return tmp;
			
		}

		/**
		 * 
		 * @param queryEngine
		 * @param limit
		 *            The limit for sampling a vertex and the initial limit for
		 *            cutoff join evaluation. A reasonable value is
		 *            <code>100</code>.
		 * @param timeout
		 *            The timeout for cutoff join path evaluation
		 *            (milliseconds). A reasonable value is <code>100</code>ms.
		 * @throws Exception
		 * 
		 *             FIXME This must either return the query plan or copy the
		 *             results as they are materialized to the sink for the join
		 *             graph operator.
		 * 
		 * 
		 * @todo We do not need the [timeout] as long as we evaluate each cutoff
		 *       join separately. The limited number of input solutions to the
		 *       join automatically limits the amount of work the join can do.
		 *       However, if we do cutoff evaluation of a series of edges then
		 *       it is possible to do a lot of work in order to find [limit]
		 *       solutions. In this case, a [timeout] protects us against join
		 *       paths which have poor correlations and large cardinality for
		 *       their vertices (a lot of solutions are considered to produce
		 *       very few results).
		 */
		public void runtimeOptimizer(final QueryEngine queryEngine,
				final int limit, final long timeout) throws Exception {

			final BOpContextBase context = new BOpContextBase(queryEngine);

			if (log.isInfoEnabled())
				log.info("limit=" + limit);

			/*
			 * Sample the vertices.
			 * 
			 * TODO Sampling for scale-out not yet finished.
			 * 
			 * FIXME Re-sampling will always produce the same sample depending
			 * on the sample operator impl (it should be random, but it is not).
			 */
			sampleVertices(context, limit);

			if(log.isDebugEnabled())
				log.debug("joinGraph=" + toString());

			/*
			 * Estimate the cardinality and weights for each edge, obtaining the
			 * Edge with the minimum estimated cardinality. This will be the
			 * starting point for the join graph evaluation.
			 * 
			 * @todo It would be very interesting to see the variety and/or
			 * distribution of the values bound when the edge is sampled. This
			 * can be easily done using a hash map with a counter. That could
			 * tell us a lot about the cardinality of the next join path
			 * (sampling the join path also tells us a lot, but it does not
			 * explain it as much as seeing the histogram of the bound values).
			 * I believe that there are some interesting online algorithms for
			 * computing the N most frequent observations and the like which
			 * could be used here.
			 * 
			 * TODO ROX is choosing the starting edge based on the minimum
			 * estimated cardinality. However, it is possible for there to be
			 * more than one edge with an estimated cardinality which is
			 * substantially to the minimum estimated cardinality. It would be
			 * best to start from multiple vertices so we can explore join paths
			 * which begin with those alternative starting vertices as well.
			 * (LUBM Q2 is an example of such a query).
			 */
			estimateEdgeWeights(queryEngine, limit);

			while(moreEdgesToVisit(executedVertices)) {
				
				// Decide on the next join path to execute.
				final Path p = chainSample(queryEngine, limit, timeout);
				
				for(Edge e : p.edges) {

					/*
					 * FIXME Finish the algorithm.
					 * 
					 * Execute the edge. We have two choices here. If join path
					 * is currently materialized and the expected cardinality of
					 * the edge is small to moderate (LTE limit * 10) then we
					 * can simply materialize the result of evaluating the edge.
					 * 
					 * In this case, we replace the sample for the vertex with
					 * the actual result of evaluating the edge. [This concept
					 * pre-supposes that a vertex sample is the set of matching
					 * elements and that we do not store the binding sets which
					 * satisfy the join path. I think that this is perhaps the
					 * primary point of difference for MonetDB/ROX and bigdata.
					 * bigdata is working with IBindingSet[]s and should
					 * associate the set of intermediate solutions which
					 * represent the materialized intermediate result with the
					 * join path, not the vertex or the edge.]
					 * 
					 * Otherwise, either the join path is already only a sample
					 * or the expected cardinality of this edge is too large so
					 * we do the cutoff evaluation of the edge in order to
					 * propagate a sample.
					 * 
					 * 1. exec(e,T1(v1),T2(v2))
					 */

					executedVertices.add(e.v1);
					executedVertices.add(e.v2);
					
				}

				/*
				 * Re-sample edges branching from any point in the path which we
				 * just executed. The purpose of this is to improve the
				 * detection of correlations using a materialized sample of the
				 * intermediate results (which will be correlated) rather than
				 * independent samples of the vertices (which are not
				 * correlated).
				 * 
				 * Also, note that ROX only samples vertices which satisfy the
				 * zero investment property and therefore there could be
				 * vertices which have not yet been sampled if some vertices are
				 * not associated with an index.
				 * 
				 * @todo This could just be another call to sampleVertices() and
				 * estimateEdgeWeights() if those methods accepted the set of
				 * already executed vertices so they could make the proper
				 * exclusions (or if we had a method which returned the
				 * un-executed vertices and/or edges).
				 */
//				e.v1.sample(context, limit);
//				e.v2.sample(context, limit);
				
			}

		}

		/**
		 * Return <code>true</code> iff there exists at least one {@link Edge}
		 * branching from a vertex NOT found in the set of vertices which have
		 * visited.
		 * 
		 * @param visited
		 *            A set of vertices.
		 * 
		 * @return <code>true</code> if there are more edges to explore.
		 */
		private boolean moreEdgesToVisit(final Set<Vertex> visited) {
			
			// Consider all edges.
			for(Edge e : E) {
				
				if (visited.contains(e.v1) && visited.contains(e.v2)) {
					/*
					 * Since both vertices for this edge have been executed the
					 * edge is now redundant. Either it was explicitly executed
					 * or another join path was used which implies the edge by
					 * transitivity in the join graph.
					 */
					continue;
				}

				/*
				 * We found a counter example (an edge which has not been
				 * explored).
				 */
				if (log.isTraceEnabled())
					log.trace("Edge has not been explored: " + e);

				return true;

			}

			// No more edges to explore.
			return false;
			
		}

		/**
		 * E
		 * 
		 * @param limit
		 * @return
		 * 
		 *         TODO How to indicate the set of edges which remain to be
		 *         explored?
		 * 
		 * @throws Exception
		 */
		public Path chainSample(final QueryEngine queryEngine, final int limit,
				final long timeout) throws Exception {

			final Vertex source;
			{
				/*
				 * Find the edge having the minimum estimated cardinality.
				 */
				final Edge e = getMinimumCardinalityEdge(executedVertices);

				if (e == null)
					throw new RuntimeException("No weighted edges.");

				/*
				 * Decide which vertex of that edge will be the starting point
				 * for chain sampling (if any).
				 */
				if (getEdgeCount(e.v1, executedVertices) > 1
						|| getEdgeCount(e.v2, executedVertices) > 1) {
					/*
					 * There is at least one vertex of that edge which branches.
					 * Chain sampling will begin with the vertex of that edge
					 * which has the lower estimated cardinality.
					 * 
					 * TODO It could be that the minimum cardinality vertex does
					 * not branch. What happens for that code path? Do we just
					 * execute that edge and then reenter chain sampling? If so,
					 * it would be cleared to test for this condition explicitly
					 * up front.
					 */
					source = e.getMinimumCardinalityVertex();
				} else {
					/*
					 * There is no vertex which branches for that edge. This is
					 * a stopping condition for chain sampling. The path
					 * consisting of just that edge is returned and should be
					 * executed by the caller.
					 */
					return new Path(e);
				}

			}
			
			/*
			 * Setup some data structures for one or more breadth first
			 * expansions of the set of path(s) which are being sampled. This
			 * iteration will continue until we reach a stopping condition.
			 */

			// The set of paths being considered.
			final List<Path> paths = new LinkedList<Path>();

			{
				// The current path.
				final Path p = new Path();

				p.stopVertex = source;
//				p.inputSample = source.sample;
				paths.add(p);
			}

			// initialize the cutoff to the limit used to sample the vertices.
			int cutoff = limit;
			long cutoffMillis = timeout;

			final Set<Vertex> unsampled = new LinkedHashSet<Vertex>(
					executedVertices);

			/*
			 * One breadth first expansion of the join paths.
			 * 
			 * Note: This expands each join path one vertex in each iteration.
			 * However, different join paths can expand from different vertices.
			 * 
			 * For ROX, each join path is expanded from the last vertex which
			 * was added to that join path so the initial edge for each join
			 * path strongly determines the edges in the join graph along which
			 * that join path can grow.
			 * 
			 * For bigdata, we can grow the path from any vertex already in the
			 * path to any vertex which (a) is not yet in the path; and (b) has
			 * not yet been evaluated.
			 * 
			 * This suggests that this loop must consider each of the paths to
			 * decide whether that path can be extended.
			 */
			while (moreEdgesToVisit(unsampled)) {

				// increment the cutoff.
				cutoff += limit;
				cutoffMillis += timeout;

				// Consider each path.
				for(Path p : paths) {

					/*
					 * The vertex at which we stopped expanding that path the
					 * last time.
					 * 
					 * TODO ROX might have to traverse vertex to vertex along
					 * edges, but we can execute any edge whose preconditions
					 * have been satisfied.
					 */
					final Vertex v = p.stopVertex;
					
					// TODO depends on the notion of the paths remaining.
					if (getEdgeCount(v, null/*executed+sampled(p)*/) > 0) {
						/*
						 * This path branches at this vertex, so remove the old
						 * path 1st.
						 */
						paths.remove(p);
					}
					
					// For each edge which is a neighbor of the vertex [v].
					final List<Edge> neighbors = null;
					for(Edge e : neighbors) {
						// 1. append the edge to the path
						final Path p1 = p.addEdge(queryEngine, cutoff, e);
						// 3. add the path to paths.
						paths.add(p1);
					}
					
				}

				final Path p = getSelectedJoinPath(paths.toArray(new Path[paths.size()]));
				
				if(p != null) {
					
					return p;
					
				}

			} // while(moreEdgesToSample)

			final Path p = getBestAlternativeJoinPath(paths.toArray(new Path[paths.size()]));
			
			if(p != null) {
				
				return p;
				
			}
			
			// TODO ROX as given can return null here, which looks like a bug.
			return null;
			
		} // chainSample()

		/**
		 * Return the path which is selected by the termination criteria
		 * (looking for a path which dominates the alternatives).
		 * 
		 * @param a
		 *            An array of {@link Path}s to be tested.
		 * 
		 * @return The selected path -or- <code>null</code> if none of the paths
		 *         is selected.
		 * 
		 * @todo Should we only explore beneath the diagonal?
		 * 
		 * @todo What is the basis for comparing the expected cardinality of
		 *       join paths? Where one path is not simply the one step extension
		 *       of the other.
		 *       <p>
		 *       This rule might only be able to compare the costs for paths in
		 *       which one path directly extends another.
		 *       <p>
		 *       It is not clear that this code is comparing all paths which
		 *       need to be compared.
		 */
		public Path getSelectedJoinPath(final Path[] a) {
			final StringBuilder sb = new StringBuilder();
			final Formatter f = new Formatter(sb);
			for (int i = 0; i < a.length; i++) {
				Path p = null;
				final Path Pi = a[i];
				if (Pi.sample == null)
					throw new RuntimeException("Not sampled: " + Pi);
				for (int j = 0; j < a.length; j++) {
					if (i == j)
						continue;
					final Path Pj = a[j];
					if (Pj.sample == null)
						throw new RuntimeException("Not sampled: " + Pj);
					final long costPi = Pi.sample.estimatedCardinality;
					final double sfPi = Pi.sample.f;
					final long costPj = Pj.sample.estimatedCardinality;
					final long expectedCombinedCost = costPi
							+ (long) (sfPi * costPj);
					final boolean lt = expectedCombinedCost < costPj;
					{
						f
								.format(
										"Comparing: P[% 2d] with P[% 2d] : % 10d + (% 7.2f * % 10d) %2s %10d",
										i, j, costPi, sfPi, costPj, (lt ? "<"
												: ">="), costPj);
						System.err.println(sb);
						sb.setLength(0);
					}
					if (lt) {
						p = Pi;
					} else {
						p = null;
						break;
					}
				} // Pj
				if (p != null)
					return p;
			} // Pi
			/*
			 * None of the paths is a winner according to the selection
			 * criteria.
			 */
			return null;
		}

		/**
		 * Termination condition if no more edges to sample. This
		 * breaks the deadlock by preferring the path whose .... 
		 */
		public Path getBestAlternativeJoinPath(final Path[] a) {
			for (int i = 0; i < a.length; i++) {
				Path p = null;
				final Path Pi = a[i];
				if (Pi.sample == null)
					throw new RuntimeException("Not sampled: " + Pi);
				for (int j = 0; j < a.length; j++) {
					if (i == j)
						continue;
					final Path Pj = a[j];
					if (Pj.sample == null)
						throw new RuntimeException("Not sampled: " + Pj);
					final long costPi = Pi.sample.estimatedCardinality;
					final double sfPi = Pi.sample.f;
					final double sfPj = Pj.sample.f;
					final long costPj = Pj.sample.estimatedCardinality;
					if (costPi + sfPi * costPj < costPj + sfPj * costPi) {
						p = Pi;
					} else {
						p = null;
						break;
					}
				} // Pj
				if (p != null)
					return p;
			} // Pi
			/*
			 * None of the paths is a winner according to the selection
			 * criteria.
			 * 
			 * @todo falling out the bottom here looks like a bug.
			 */
			return null;
		}

	} // class JGraph

    /**
     * Evaluation of a {@link JoinGraph}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class JoinGraphTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final JGraph g;
        
        private int limit;
        
        JoinGraphTask(final BOpContext<IBindingSet> context) {

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            limit = getLimit();
            
            if (limit <= 0)
                throw new IllegalArgumentException();

        	final IPredicate[] v = getVertices();
        	
            g = new JGraph(v);

        }

        public Void call() throws Exception {

        	// sample the vertices.
			g.sampleVertices(context, limit);

			if (log.isInfoEnabled())
				log.info("sampledVertices=" + Arrays.toString(g.V));

			// estimate the cardinality of the join graph edges.
			g.estimateEdgeWeights(context.getRunningQuery().getQueryEngine(),
					limit);

			if (log.isInfoEnabled())
				log.info("estimatedEdgeWeights=" + Arrays.toString(g.E));

			// TODO Auto-generated method stub
			throw new UnsupportedOperationException();
        }

    }

}
