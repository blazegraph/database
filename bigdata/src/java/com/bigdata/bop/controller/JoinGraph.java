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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
import com.bigdata.bop.Constant;
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
 * @todo Some edges can be eliminated by transitivity. For example, given
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
 * @todo In order to combine pipelining with runtime query optimization we need
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
		 * The initial sample size (default {@value #DEFAULT_SAMPLE_SIZE}).
		 */
		String SAMPLE_SIZE = JoinGraph.class.getName() + ".sampleSize";
		
		int DEFAULT_SAMPLE_SIZE = 100; 
    }

	/**
     * @see Annotations#VERTICES
     */
    public IPredicate[] getVertices() {
    
    	return (IPredicate[]) getRequiredProperty(Annotations.VERTICES);
    	
    }

    /**
     * @see Annotations#SAMPLE_SIZE
     */
    public int getSampleSize() {
    	
    	return getProperty(Annotations.SAMPLE_SIZE, Annotations.DEFAULT_SAMPLE_SIZE);
    	
    }
    
    public JoinGraph(final NV ...anns) {
        
		this(BOpBase.NOARGS, NV.asMap(anns));
    	
    }

	/**
	 * 
	 * @todo We can derive the vertices from the join operators or the join
	 *       operators from the vertices. However, if a specific kind of join
	 *       operator is required then the question is whether we have better
	 *       information to make that choice when the join graph is evaluated or
	 *       before it is constructed.
	 * 
	 * @todo How we will handle optional joins? Presumably they are outside of
	 *       the code join graph as part of the tail attached to that join
	 *       graph.
	 * 
	 * @todo How can join constraints be moved around? Just attach them where
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
	 * A vertex of the join graph is an annotated relation (this corresponds to
	 * an {@link IPredicate} with additional annotations to support the adaptive
	 * query optimization algorithm).
	 */
	public static class Vertex implements Serializable {

		/**
         * 
         */
		private static final long serialVersionUID = 1L;

		final IPredicate<?> pred;

		/**
		 * The limit used to produce the {@link #sample}.
		 */
		int limit;
		
		/**
		 * Fast range count and <code>null</code> until initialized.
		 */
		Long rangeCount;

		/**
		 * Sample (when not-null).
		 */
		Object[] sample;

		Vertex(final IPredicate<?> pred) {

			if (pred == null)
				throw new IllegalArgumentException();
			
			this.pred = pred;
			
		}
		
		public String toString() {

			return "\nVertex{pred=" + pred + ",rangeCount=" + rangeCount
					+ ",sampleSize=" + (sample == null ? "N/A" : sample.length)
					+ "}";
		
		}

		public void sample(final BOpContextBase context,final int limit) {

			final IRelation r = context.getRelation(pred);

			final IAccessPath ap = context.getAccessPath(r, pred);

			if (rangeCount == null) {

				rangeCount = ap.rangeCount(false/* exact */);

			}

			if (sample == null) { // @todo new sample each time?

				final SampleIndex sampleOp = new SampleIndex(new BOp[] {}, //
						NV.asMap(//
						new NV(SampleIndex.Annotations.PREDICATE, pred),//
						new NV(SampleIndex.Annotations.LIMIT, limit)));
				
				sample = sampleOp.eval(context);
				
				this.limit = limit;
				
			}
			
		}
		
	}

	/**
	 * An edge of the join graph is an annotated join operator. The edges of the
	 * join graph are undirected. Edges exist when the vertices share at least
	 * one variable.
	 */
	public static class Edge implements Serializable {

		/**
         * 
         */
		private static final long serialVersionUID = 1L;

		/**
		 * The vertices connected by that edge.
		 */
		final Vertex v1, v2;

		/**
		 * The set of shared variables.
		 */
		final Set<IVariable<?>> shared;

		class EdgeSample {

			/**
			 * The fast range count (aka cardinality) for the source vertex of
			 * the edge (whichever vertex has the lower cardinality).
			 */
			final long inputRangeCount;
			/**
			 * The limit used to sample the edge (this is the limit on the #of
			 * solutions generated by the cutoff join used when this sample was
			 * taken).
			 */
			final int limit;
			/**
			 * The #of binding sets out of the source sample vertex sample which
			 * were consumed.
			 */
			final int inputCount;
			/**
			 * The #of binding sets generated before the join was cutoff.
			 */
			final int outputCount;
			/**
			 * The ratio of the #of input samples consumed to the #of output
			 * samples generated.
			 */
			final double f;
			/**
			 * The estimated cardinality of the join.
			 */
			final long estimatedCardinality;

			/**
			 * @param limit
			 *            The limit used to sample the edge (this is the limit
			 *            on the #of solutions generated by the cutoff join used
			 *            when this sample was taken).
			 * @param inputRangeCount
			 *            The fast range count (aka cardinality) for the source
			 *            vertex of the edge (whichever vertex has the lower
			 *            cardinality).
			 * @param inputCount
			 *            The #of binding sets out of the source sample vertex
			 *            sample which were consumed.
			 * @param outputCount
			 *            The #of binding sets generated before the join was
			 *            cutoff.
			 * 
			 * @todo If the outputCount is zero then this is a good indicator
			 *       that there is an error in the query such that the join will
			 *       not select anything. This is not 100%, merely indicative.
			 */
			EdgeSample(final long inputRangeCount, final int limit, final int inputCount,
					final int outputCount) {

				this.inputRangeCount = inputRangeCount;
				
				this.limit = limit;
				
				this.inputCount = inputCount;
				
				this.outputCount = outputCount;
				
				f = outputCount == 0 ? 0 : (outputCount / (double) inputCount);

				estimatedCardinality = (long) (inputRangeCount * f);
				
			}
			
			public String toString() {
				return "EdgeSample" + "{inputRangeCount=" + inputRangeCount
						+ ", limit=" + limit + ", inputCount=" + inputCount
						+ ", outputCount=" + outputCount + ", f=" + f
						+ ", estimatedCardinality=" + estimatedCardinality
						+ "}";
			}
			
		};

		/**
		 * The last sample for this edge and <code>null</code> if the edge has
		 * not been sampled.
		 */
		EdgeSample sample = null;
		
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
		
		public String toString() {
			
			return "\nEdge{v1=" + v1.pred.getId() + ",v2=" + v2.pred.getId()
					+ ",shared=" + shared.toString() + ", sample=" + sample + "}";

		}

		/**
		 * Estimate the cardinality of the edge.
		 * 
		 * @param context
		 * @throws Exception 
		 */
		public void estimateCardinality(final QueryEngine queryEngine,
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
			if (v1.rangeCount < v2.rangeCount) {
				v = v1;
				vp = v2;
			} else {
				v = v2;
				vp = v1;
			}

			/*
			 * @todo This is difficult to setup because we do not have a concept
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
			 * @todo On subsequent iterations we would probably re-sample [v]
			 * and we would run against the materialized intermediate result for
			 * [v'].
			 */

			/*
			 * Convert the source sample into an IBindingSet[], injecting a
			 * rowid column.
			 */
			final IVariable<Integer> ROWID = Var.var("__rowid");
			final IBindingSet[] sample = new IBindingSet[v.sample.length];
			{
				for (int i = 0; i < sample.length; i++) {
					final IBindingSet bset = new HashBindingSet();
					BOpContext.copyValues((IElement) v.sample[i], v.pred, bset);
					bset.set(ROWID, new Constant<Integer>(Integer.valueOf(i)));
					sample[i] = bset;
				}
			}

			/*
			 * @todo Any constraints on the edge (other than those implied by
			 * shared variables) need to be annotated on the join. Constraints
			 * (other than range constraints which are directly coded by the
			 * predicate) will not reduce the effort to compute the join, but
			 * they can reduce the cardinality of the join and that is what we
			 * are trying to estimate here.
			 */
			final PipelineJoin joinOp = new PipelineJoin(new BOp[] {}, //
					new NV(BOp.Annotations.BOP_ID, 1),//
					new NV(PipelineJoin.Annotations.PREDICATE,vp.pred.setBOpId(3))
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
									new IBindingSet[][] { sample })));

			// #of source samples consumed.
			int inputCount = 0;
			// #of output samples generated.
			int outputCount = 0;
			try {
				try {
					IBindingSet bset = null;
					// Figure out the #of source samples consumed.
					final Iterator<IBindingSet> itr = new Dechunkerator<IBindingSet>(
							runningQuery.iterator());
					while (itr.hasNext()) {
						bset = itr.next();
						outputCount++;
					}
					// #of input rows consumed. Note: +1 since origin ZERO.
					inputCount = bset == null ? 0 : ((Integer) bset.get(ROWID)
							.get()) + 1;
				} finally {
					// verify no problems. FIXME Restore test of the query.
//					runningQuery.get();
				}
			} finally {
				runningQuery.cancel(true/* mayInterruptIfRunning */);
			}

			this.sample = new EdgeSample(v.rangeCount, limit, inputCount,
					outputCount);
			
			if (log.isInfoEnabled())
				log.info("edge=" + this + sample);

		}
		
	}

	/**
	 * A join graph (data structure and methods only).
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

		public List<Vertex> getVertices() {
			return Collections.unmodifiableList(Arrays.asList(V));
		}

		public List<Edge> getEdges() {
			return Collections.unmodifiableList(Arrays.asList(E));
		}

		public String toString() {
			return super.toString() + "{V=" + Arrays.toString(V) + ",E="
					+ Arrays.toString(E) + "}"; 
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
		public void estimateEdgeWeights(final QueryEngine queryEngine, final int limit) throws Exception {
			
			for(Edge e : E) {
			
				if (e.v1.sample == null || e.v2.sample == null) {
				
					/*
					 * We can only estimate the cardinality of edges connecting
					 * vertices for which samples were obtained.
					 */
					continue;
					
				}
				
				e.estimateCardinality(queryEngine, limit);
				
			}
			
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
        
        JoinGraphTask(final BOpContext<IBindingSet> context) {

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

        	final IPredicate[] v = getVertices();
        	
            final int sampleSize = getSampleSize();
            
            if (sampleSize <= 0)
                throw new IllegalArgumentException();

            g = new JGraph(v);

        }

        public Void call() throws Exception {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException();
        }

    }

}
