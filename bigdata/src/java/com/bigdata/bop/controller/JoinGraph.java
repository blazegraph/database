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
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import com.bigdata.bop.BOpIdFactory;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.SampleIndex;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.join.PipelineJoin.PipelineJoinStats;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.relation.rule.Rule;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.IChunkedIterator;

/**
 * A join graph with annotations for estimated cardinality and other details in
 * support of runtime query optimization. A join graph is a collection of
 * relations and joins which connect those relations. This boils down to a
 * collection of {@link IPredicate}s (selects on relations) and shared variables
 * (which identify joins). Operators other than standard joins (including
 * optional joins, sort, order by, etc.) must be handled downstream from the
 * join graph in a "tail plan".
 * 
 * @see http://arxiv.org/PS_cache/arxiv/pdf/0810/0810.4809v1.pdf, XQuery Join
 *      Graph Isolation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Examine the overhead of the runtime optimizer. Look at ways to prune
 *       its costs. For example, by pruning the search, by recognizing when the
 *       query is simple enough to execute directly, by recognizing when we have
 *       already materialized the answer to the query, etc.
 * 
 * @todo Cumulative estimated cardinality is an estimate of the work to be done.
 *       However, the actual cost of a join depends on whether we will use
 *       nested index subquery or a hash join and the cost of that operation on
 *       the database. There could be counter examples where the cost of the
 *       hash join with a range scan using the unbound variable is LT the nested
 *       index subquery. For those cases, we will do the same amount of IO on
 *       the hash join but there will still be a lower cardinality to the join
 *       path since we are feeding in fewer solutions to be joined.
 * 
 * @todo Look at the integration with the SAIL. We decorate the joins with some
 *       annotations. Those will have to be correctly propagated to the "edges"
 *       in order for edge sampling and incremental evaluation (or final
 *       evaluation) to work. The {@link DataSetJoin} essentially inlines one of
 *       its access paths. That should really be changed into an inline access
 *       path and a normal join operator so we can defer some of the details
 *       concerning the join operator annotations until we decide on the join
 *       path to be executed. An inline AP really implies an inline relation,
 *       which in turn implies that the query is a searchable context for
 *       query-local resources.
 *       <p>
 *       For s/o, when the AP is remote, the join evaluation context must be ANY
 *       and otherwise (for s/o) it must be SHARDED.
 *       <p>
 *       Since the join graph is fed the vertices (APs), it does not have access
 *       to the annotated joins so we need to generated appropriately annotated
 *       joins when sampling an edge and when evaluation a subquery.
 * 
 * @todo Examine behavior when we do not have perfect covering indices. This
 *       will mean that some vertices can not be sampled using an index and that
 *       estimation of their cardinality will have to await the estimation of
 *       the cardinality of the edge(s) leading to that vertex. Still, the
 *       approach should be able to handle queries without perfect / covering
 *       automatically. Then experiment with carrying fewer statement indices
 *       for quads.
 */
public class JoinGraph extends PipelineOp {

	private static final transient Logger log = Logger
			.getLogger(JoinGraph.class);

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
		 * The initial limit for cutoff sampling (default
		 * {@value #DEFAULT_LIMIT}).
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

	public JoinGraph(final NV... anns) {

		this(BOpBase.NOARGS, NV.asMap(anns));

	}

	public JoinGraph(final BOp[] args, final Map<String, Object> anns) {

		super(args, anns);

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
		 * TODO When <code>true</code>, we could run the join against the sample
		 * rather than the disk. This would require wrapping the sample as an
		 * access path. Since all exact samples will be pretty small, this is
		 * not likely to have any great performance benefit.
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
		public VertexSample(final long rangeCount, final int limit,
				final boolean exact, final Object[] sample) {

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
		 * Take a sample of the vertex, updating {@link #sample} as a
		 * side-effect. If the sample is already exact, then this is a NOP. If
		 * the vertex was already sampled to that limit, then this is a NOP (you
		 * have to raise the limit to re-sample the vertex).
		 * 
		 * @param limit
		 *            The sample cutoff.
		 */
		public void sample(final QueryEngine queryEngine, final int limit) {

			if (queryEngine == null)
				throw new IllegalArgumentException();

			if (limit <= 0)
				throw new IllegalArgumentException();

			final VertexSample oldSample = this.sample;

			if (oldSample != null && oldSample.exact) {

				/*
				 * The old sample is already the full materialization of the
				 * vertex.
				 */

				return;

			}

			if (oldSample != null && oldSample.limit >= limit) {

				/*
				 * The vertex was already sampled to this limit. 
				 */
				
				return;
				
			}
			
			final BOpContextBase context = new BOpContextBase(queryEngine);
			
			final IRelation r = context.getRelation(pred);

			final IAccessPath ap = context.getAccessPath(r, pred);

			final long rangeCount = oldSample == null ? ap
					.rangeCount(false/* exact */) : oldSample.rangeCount;

			if (rangeCount <= limit) {

				/*
				 * Materialize the access path.
				 * 
				 * TODO This could be more efficient if we raised it onto the AP
				 * or if we overrode CHUNK_CAPACITY and the fully buffered
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

			} else {

				/*
				 * Materialize a random sample from the access path.
				 */

				final SampleIndex sampleOp = new SampleIndex(
						new BOp[] {}, //
						NV.asMap(//
							new NV(SampleIndex.Annotations.PREDICATE, pred),//
							new NV(SampleIndex.Annotations.LIMIT, limit)));

				sample = new VertexSample(rangeCount, limit, false/* exact */,
						sampleOp.eval(context));

			}

			if (log.isTraceEnabled())
				log.trace("Sampled: " + sample);

			return;
			
		}

	}

	/**
	 * Type safe enumeration describes the edge condition (if any) for a
	 * cardinality estimate.
	 */
	public static enum EstimateEnum {
		/**
		 * An estimate, but not any of the edge conditions.
		 */
		Normal(" "),
		/**
		 * The cardinality estimate is exact.
		 */
		Exact("E"),
		/**
		 * The cardinality estimation is a lower bound (the actual cardinality
		 * may be higher than the estimated value).
		 */
		LowerBound("L"),
		/**
		 * Flag is set when the cardinality estimate underflowed (false zero
		 * (0)).
		 */
		Underflow("U");
		
		private EstimateEnum(final String code) {
			
			this.code = code;
			
		}

		private final String code;

		public String getCode() {

			return code;
			
		}
		
	} // EstimateEnum
	
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
		 * <code>true</code> iff the source sample is exact (because the source
		 * is either a fully materialized vertex or an edge whose solutions have
		 * been fully materialized).
		 */
		public final boolean sourceSampleExact;
		
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
		 * Indicates whether the estimate is exact, an upper bound, or a lower
		 * bound.
		 * 
		 * TODO This field should be used to avoid needless re-computation of a
		 * join whose exact solution is already known.
		 */
		public final EstimateEnum estimateEnum;
		
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
		EdgeSample(
				// final VertexSample sourceVertexSample,
				final long sourceSampleRangeCount,//
				final boolean sourceSampleExact, //
				final int sourceSampleLimit,//
				final int limit,//
				final int inputCount, //
				final int outputCount,//
				final IBindingSet[] sample) {

			if (sample == null)
				throw new IllegalArgumentException();

			// this.rangeCount = sourceVertexSample.rangeCount;
			this.rangeCount = sourceSampleRangeCount;
			
			this.sourceSampleExact = sourceSampleExact;

			this.limit = limit;

			this.inputCount = inputCount;

			this.outputCount = outputCount;

			f = outputCount == 0 ? 0 : (outputCount / (double) inputCount);

			estimatedCardinality = (long) (rangeCount * f);

			if (sourceSampleExact && outputCount < limit) {
				/*
				 * Note: If the entire source vertex is being fed into the
				 * cutoff join and the cutoff join outputCount is LT the limit,
				 * then the sample is the actual result of the join. That is,
				 * feeding all source solutions into the join gives fewer than
				 * the desired number of output solutions.
				 */
				estimateEnum = EstimateEnum.Exact;
			} else if (inputCount == 1 && outputCount == limit) {
				/*
				 * If the inputCount is ONE (1) and the outputCount is the
				 * limit, then the estimated cardinality is a lower bound as
				 * more than outputCount solutions might be produced by the join
				 * when presented with a single input solution.
				 */
				estimateEnum = EstimateEnum.LowerBound;
			} else if (!sourceSampleExact
					&& inputCount == Math.min(sourceSampleLimit, rangeCount)
					&& outputCount == 0) {
				/*
				 * When the source sample was not exact, the inputCount is EQ to
				 * the lesser of the source range count and the source sample
				 * limit, and the outputCount is ZERO (0), then feeding in all
				 * source solutions in is not sufficient to generate any output
				 * solutions. In this case, the estimated join hit ratio appears
				 * to be zero. However, the estimation of the join hit ratio
				 * actually underflowed and the real join hit ratio might be a
				 * small non-negative value. A real zero can only be identified
				 * by executing the full join.
				 * 
				 * Note: An apparent join hit ratio of zero does NOT imply that
				 * the join will be empty (unless the source vertex sample is
				 * actually the fully materialized access path - this case is
				 * covered above).
				 */
				estimateEnum = EstimateEnum.Underflow;
			} else {
				estimateEnum = EstimateEnum.Normal;
			}

			this.sample = sample;
		}

		public String toString() {
			return getClass().getName() //
					+ "{ rangeCount=" + rangeCount//
					+ ", sourceSampleExact=" + sourceSampleExact//
					+ ", limit=" + limit //
					+ ", inputCount=" + inputCount//
					+ ", outputCount=" + outputCount //
					+ ", f=" + f//
					+ ", estimatedCardinality=" + estimatedCardinality//
					+ ", estimateEnum=" + estimateEnum//
//					+ ", estimateIsLowerBound=" + estimateIsLowerBound//
//					+ ", estimateIsUpperBound=" + estimateIsUpperBound//
//					+ ", sampleIsExactSolution=" + estimateIsExact //
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
		 * <p>
		 * Note: This sample is only the one-step cutoff evaluation of the edge
		 * given a sample of its vertex having the lesser cardinality. It is NOT
		 * the cutoff sample of a join path having this edge except for the
		 * degenerate case where the edge is the first edge in the join path.
		 */
		public EdgeSample sample = null;

		public Edge(final Vertex v1, final Vertex v2,
				final Set<IVariable<?>> shared) {
			if (v1 == null)
				throw new IllegalArgumentException();
			if (v2 == null)
				throw new IllegalArgumentException();
			if (shared == null)
				throw new IllegalArgumentException();
			if (shared.isEmpty())
				throw new IllegalArgumentException();
			this.v1 = v1;
			this.v2 = v2;
			this.shared = shared;
		}

		/**
		 * The edge label is formed from the {@link BOp.Annotations#BOP_ID} of
		 * its ordered vertices (v1,v2).
		 */
		public String getLabel() {

			return "(" + v1.pred.getId() + "," + v2.pred.getId() + ")";
			
		}
		
		/**
		 * Note: The vertices of the edge are labeled using the
		 * {@link BOp.Annotations#BOP_ID} associated with the {@link IPredicate}
		 * for each vertex.
		 */
		public String toString() {

			return "Edge{ "+getLabel()+", estCard="
					+ (sample == null ? "N/A" : sample.estimatedCardinality)
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
		 * Estimate the cardinality of the edge, updating {@link #sample} as a
		 * side-effect. This is a NOP if the edge has already been sampled at
		 * that <i>limit</i>. This is a NOP if the edge sample is exact.
		 * 
		 * @param context
		 * 
		 * @return The new {@link EdgeSample} (this is also updated on
		 *         {@link #sample} as a side-effect).
		 * 
		 * @throws Exception
		 */
		public EdgeSample estimateCardinality(final QueryEngine queryEngine,
				final int limit) throws Exception {

			if (limit <= 0)
				throw new IllegalArgumentException();

//			/*
//			 * Note: There is never a need to "re-sample" the edge. Unlike ROX,
//			 * we always can sample a vertex. This means that we can sample the
//			 * edges exactly once, during the initialization of the join graph.
//			 */
//			if (sample != null)
//				throw new RuntimeException();

			if (sample != null) {

				if (sample.limit >= limit) {

					// Already sampled at that limit.
					return sample;

				}

				if (sample.estimateEnum == EstimateEnum.Exact) {

					// Sample is exact (fully materialized result).
					return sample;

				}
				
			}
			
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
			 * Convert the source sample into an IBindingSet[].
			 * 
			 * TODO We might as well do this when we sample the vertex.
			 */
			final IBindingSet[] sourceSample = new IBindingSet[v.sample.sample.length];
			{
				for (int i = 0; i < sourceSample.length; i++) {
					final IBindingSet bset = new HashBindingSet();
					BOpContext.copyValues((IElement) v.sample.sample[i],
							v.pred, bset);
					sourceSample[i] = bset;
				}
			}

			// Sample the edge and save the sample on the edge as a side-effect.
			this.sample = estimateCardinality(queryEngine, limit, v, vp,
					v.sample.rangeCount, v.sample.exact, v.sample.limit,
					sourceSample);

			return sample;

		}

		/**
		 * Estimate the cardinality of the edge given a sample of either a
		 * vertex or a join path leading up to that edge.
		 * <p>
		 * Note: The caller is responsible for protecting against needless
		 * re-sampling.
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
				final long sourceSampleRangeCount,
				final boolean sourceSampleExact,
				final int sourceSampleLimit,
				final IBindingSet[] sourceSample)
				throws Exception {

			if (limit <= 0)
				throw new IllegalArgumentException();

			/*
			 * Note: This sets up a cutoff pipeline join operator which makes an
			 * accurate estimate of the #of input solutions consumed and the #of
			 * output solutions generated. From that, we can directly compute
			 * the join hit ratio. This approach is preferred to injecting a
			 * "RowId" column as the estimates are taken based on internal
			 * counters in the join operator and the join operator knows how to
			 * cutoff evaluation as soon as the limit is satisfied, thus
			 * avoiding unnecessary effort.
			 * 
			 * TODO Any constraints on the edge (other than those implied by
			 * shared variables) need to be annotated on the join. Constraints
			 * (other than range constraints which are directly coded by the
			 * predicate) will not reduce the effort to compute the join, but
			 * they can reduce the cardinality of the join and that is what we
			 * are trying to estimate here.
			 * 
			 * TODO How can join constraints be moved around? Just attach them
			 * where ever a variable becomes bound? And when do we filter out
			 * variables which are not required downstream? Once we decide on a
			 * join path and execute it fully (rather than sampling that join
			 * path).
			 */
			final int joinId = 1;
			final PipelineJoin joinOp = new PipelineJoin(new BOp[] {}, //
				new NV(BOp.Annotations.BOP_ID, joinId),//
				new NV(PipelineJoin.Annotations.PREDICATE, vTarget.pred
						.setBOpId(3)),
				// disallow parallel evaluation.
				new NV(PipelineJoin.Annotations.MAX_PARALLEL,0),
				// disable access path coalescing 
				new NV(PipelineJoin.Annotations.COALESCE_DUPLICATE_ACCESS_PATHS,false),
				// cutoff join.
				new NV(PipelineJoin.Annotations.LIMIT,(long)limit),
				/*
				 * Note: In order to have an accurate estimate of the join
				 * hit ratio we need to make sure that the join operator
				 * runs using a single PipelineJoinStats instance which will
				 * be visible to us when the query is cutoff. In turn, this
				 * implies that the join must be evaluated on the query
				 * controller.
				 * 
				 * @todo This implies that sampling of scale-out joins must
				 * be done using remote access paths.
				 */
				new NV(PipelineJoin.Annotations.SHARED_STATE,true),
				new NV(PipelineJoin.Annotations.EVALUATION_CONTEXT,BOpEvaluationContext.CONTROLLER)
			);

			final PipelineOp queryOp = joinOp;
			
			// run the cutoff sampling of the edge.
			final UUID queryId = UUID.randomUUID();
			final RunningQuery runningQuery = queryEngine.eval(queryId,
					queryOp, new LocalChunkMessage<IBindingSet>(queryEngine,
							queryId, joinOp.getId()/* startId */,
							-1 /* partitionId */,
							new ThickAsynchronousIterator<IBindingSet[]>(
									new IBindingSet[][] { sourceSample })));

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
					}
				} finally {
					// verify no problems.
					runningQuery.get();
				}
			} finally {
				runningQuery.cancel(true/* mayInterruptIfRunning */);
			}

			// The join hit ratio can be computed directly from these stats.
			final PipelineJoinStats joinStats = (PipelineJoinStats) runningQuery
					.getStats().get(joinId);

			if (log.isTraceEnabled())
				log.trace(joinStats.toString());
			
			/*
			 * TODO Improve comments here. See if it is possible to isolate a
			 * common base class which would simplify the setup of the cutoff
			 * join and the computation of the sample stats.
			 */

			final EdgeSample edgeSample = new EdgeSample(
					sourceSampleRangeCount, //
					sourceSampleExact, // @todo redundant with sourceSampleLimit
					sourceSampleLimit, //
					limit, //
					(int) joinStats.inputSolutions.get(),//
					(int) joinStats.outputSolutions.get(), //
					result.toArray(new IBindingSet[result.size()]));

			if (log.isDebugEnabled())
				log.debug(getLabel() + " : newSample=" + edgeSample);

			return edgeSample;

		}

	}

	/**
	 * A sequence of {@link Edge}s (aka join steps).
	 */
	public static class Path {

		/**
		 * An immutable ordered list of the edges in the (aka the sequence of
		 * joins represented by this path).
		 */
		public final List<Edge> edges;

		/**
		 * The sample obtained by the step-wise cutoff evaluation of the ordered
		 * edges of the path.
		 * <p>
		 * Note: This sample is generated one edge at a time rather than by
		 * attempting the cutoff evaluation of the entire join path (the latter
		 * approach does allow us to limit the amount of work to be done to
		 * satisfy the cutoff).
		 */
		public EdgeSample sample;

		/**
		 * The cumulative estimated cardinality of the path. This is zero for an
		 * empty path. For a path consisting of a single edge, this is the
		 * estimated cardinality of that edge. When creating a new path adding
		 * an edge to an existing path, the cumulative cardinality of the new
		 * path is the cumulative cardinality of the existing path plus the
		 * estimated cardinality of the cutoff join of the new edge given the
		 * input sample of the existing path.
		 */
		final public long cumulativeEstimatedCardinality;

		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("Path{");
			boolean first = true;
			for (Edge e : edges) {
				if (!first)
					sb.append(",");
				sb.append(e.getLabel());
				first = false;
			}
			sb.append(",cumEstCard=" + cumulativeEstimatedCardinality
					+ ",sample=" + sample + "}");
			return sb.toString();
		}

		/**
		 * Create an empty path.
		 */
		public Path() {
			this.edges = Collections.emptyList();
			this.cumulativeEstimatedCardinality = 0;
			this.sample = null;
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

			if (e.sample == null)
				throw new IllegalArgumentException("Not sampled: " + e);

			this.edges = Collections.singletonList(e);

			this.sample = e.sample;

			this.cumulativeEstimatedCardinality = e.sample.estimatedCardinality;

		}

		/**
		 * Constructor used by {@link #addEdge(QueryEngine, int, Edge)}
		 * 
		 * @param edges
		 *            The edges in the new path.
		 * @param cumulativeEstimatedCardinality
		 *            The cumulative estimated cardinality of the new path.
		 * @param sample
		 *            The sample from the last
		 */
		private Path(final List<Edge> edges,
				final long cumulativeEstimatedCardinality,
				final EdgeSample sample) {

			if (edges == null)
				throw new IllegalArgumentException();

			if (cumulativeEstimatedCardinality < 0)
				throw new IllegalArgumentException();

			if (sample == null)
				throw new IllegalArgumentException();

			this.edges = Collections.unmodifiableList(edges);

			this.cumulativeEstimatedCardinality = cumulativeEstimatedCardinality;

			this.sample = sample;

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
		 * Return <code>true</code> if this path is an unordered variant of the
		 * given path (same vertices in any order).
		 * 
		 * @param p
		 *            Another path.
		 * 
		 * @return <code>true</code> if this path is an unordered variant of the
		 *         given path.
		 */
		public boolean isUnorderedVariant(final Path p) {

			if (p == null)
				throw new IllegalArgumentException();

			if (edges.size() != p.edges.size()) {
				/*
				 * Fast rejection. This assumes that each edge after the first
				 * adds one distinct vertex to the path. That assumption is
				 * enforced by #addEdge().
				 */
				return false;
			}

			final Vertex[] v1 = getVertices();
			final Vertex[] v2 = p.getVertices();

			if (v1.length != v2.length) {

				// Reject (this case is also covered by the test above).
				return false;
				
			}

			/*
			 * Scan the vertices of the caller's path. If any of those vertices
			 * are NOT found in this path the paths are not unordered variations
			 * of one aother.
			 */
			for (int i = 0; i < v2.length; i++) {

				final Vertex tmp = v2[i];

				boolean found = false;
				for (int j = 0; j < v1.length; j++) {

					if (v1[j] == tmp) {
						found = true;
						break;
					}

				}

				if (!found) {
					return false;
				}

			}

			return true;

		}

		/**
		 * Return the vertices in this path (in path order). For the first edge,
		 * the minimum cardinality vertex is always reported first (this is
		 * critical for producing the correct join plan). For the remaining
		 * edges in the path, the unvisited is reported.
		 * 
		 * @return The vertices (in path order).
		 * 
		 *         TODO This could be rewritten without the toArray() using a
		 *         method which visits the vertices of a path in any order.
		 *         
		 *         @todo unit test for the first vertex to be reported.
		 */
		public Vertex[] getVertices() {
			
			final Set<Vertex> tmp = new LinkedHashSet<Vertex>();

			for (Edge e : edges) {

				if (tmp.isEmpty()) {
					/*
					 * The first edge is handled specially in order to report
					 * the minimum cardinality vertex first.
					 */
					tmp.add(e.getMinimumCardinalityVertex());
					tmp.add(e.getMaximumCardinalityVertex());

				} else {

					tmp.add(e.v1);

					tmp.add(e.v2);

				}
				
			}
			
			final Vertex[] a = tmp.toArray(new Vertex[tmp.size()]);
			
			return a;
			
		}

		/**
		 * Return the {@link IPredicate}s associated with the vertices of the
		 * join path in path order.
		 * 
		 * @see #getVertices()
		 */
		public IPredicate[] getPredicates() {

			// The vertices in the selected evaluation order.
			final Vertex[] vertices = getVertices();

			// The predicates in the same order as the vertices.
			final IPredicate[] preds = new IPredicate[vertices.length];

			for (int i = 0; i < vertices.length; i++) {

				preds[i] = vertices[i].pred;

			}

			return preds;

		}
		
		/**
		 * Return the {@link BOp} identifiers of the predicates associated with
		 * each vertex in path order.
		 */
		static public int[] getVertexIds(final List<Edge> edges) {

			final Set<Vertex> tmp = new LinkedHashSet<Vertex>();
			
			for (Edge e : edges) {
			
				tmp.add(e.v1);
				
				tmp.add(e.v2);
				
			}
			
			final Vertex[] a = tmp.toArray(new Vertex[tmp.size()]);

			final int[] b = new int[a.length];
			
			for (int i = 0; i < a.length; i++) {
			
				b[i] = a[i].pred.getId();
				
			}
			
			return b;
			
		}

		/**
		 * Return <code>true</code> if this path begins with the given path.
		 * 
		 * @param p
		 *            The given path.
		 * 
		 * @return <code>true</code> if this path begins with the given path.
		 */
		public boolean beginsWith(final Path p) {

			if (p == null)
				throw new IllegalArgumentException();

			if (p.edges.size() > edges.size()) {
				// Proven false since the caller's path is longer.
				return false;
			}

			for (int i = 0; i < p.edges.size(); i++) {
				final Edge eSelf = edges.get(i);
				final Edge eOther = p.edges.get(i);
				if (eSelf != eOther) {
					return false;
				}
			}

			return true;
		}

		/**
		 * Add an edge to a path, computing the estimated cardinality of the new
		 * path, and returning the new path. The cutoff join is performed using
		 * the {@link #sample} of <i>this</i> join path and the actual access
		 * path for the target vertex.
		 * 
		 * @param queryEngine
		 * @param limit
		 * @param e
		 *            The edge.
		 * 
		 * @return The new path. The materialized sample for the new path is the
		 *         sample obtained by the cutoff join for the edge added to the
		 *         path.
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
			 * FIXME It is possible for the path sample to be empty. Unless the
			 * sample also happens to be exact, this is an indication that the
			 * estimated cardinality has underflowed. We track the estimated
			 * cumulative cardinality, so this does not make the join path an
			 * immediate winner, but it does mean that we can not probe further
			 * on that join path as we lack any intermediate solutions to feed
			 * into the downstream joins. [If we re-sampled the edges in the
			 * join path in each round then this would help to establish a
			 * better estimate in successive rounds.]
			 */

			final EdgeSample edgeSample = e.estimateCardinality(queryEngine,
					limit, sourceVertex, targetVertex,
					this.sample.estimatedCardinality,
					this.sample.estimateEnum == EstimateEnum.Exact,
					this.sample.limit,//
					this.sample.sample//
					);

			{

				final List<Edge> edges = new ArrayList<Edge>(
						this.edges.size() + 1);

				edges.addAll(this.edges);

				edges.add(e);

				final long cumulativeEstimatedCardinality = this.cumulativeEstimatedCardinality
						+ edgeSample.estimatedCardinality;

				// Extend the path.
				final Path tmp = new Path(edges,
						cumulativeEstimatedCardinality, edgeSample);

				return tmp;

			}

		}

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

		return showTable(a, null/* pruned */);
		
	}

	/**
	 * Comma delimited table showing the estimated join hit ratio, the estimated
	 * cardinality, and the set of vertices for each of the specified join
	 * paths.
	 * 
	 * @param a
	 *            A set of paths (typically those before pruning).
	 * @param pruned
	 *            The set of paths after pruning (those which were retained)
	 *            (optional). When given, the paths which were pruned are marked
	 *            in the table.
	 * 
	 * @return A table with that data.
	 */
	static public String showTable(final Path[] a,final Path[] pruned) {
		final StringBuilder sb = new StringBuilder();
		final Formatter f = new Formatter(sb);
		f.format("%5s %10s%1s * %7s (%3s/%3s) = %10s%1s : %10s %10s",
				"path",//
				"rangeCount",//
				"",// sourceSampleExact
				"f",//
				"out",//
				"in",//
				"estCard",//
				"",// estimateIs(Exact|LowerBound|UpperBound)
				"sumEstCard",//
				"joinPath\n"
				);
		for (int i = 0; i < a.length; i++) {
			final Path x = a[i];
			// true iff the path survived pruning.
			Boolean prune = null;
			if (pruned != null) {
				prune = Boolean.TRUE;
				for (Path y : pruned) {
					if (y == x) {
						prune = Boolean.FALSE;
						break;
					}
				}
			}
			if (x.sample == null) {
				f.format("p[%2d] %10d%1s * %7s (%3s/%3s) = %10s%1s : %10s", i, "N/A", "", "N/A", "N/A", "N/A", "N/A", "", "N/A");
			} else {
				f.format("p[%2d] %10d%1s * % 7.2f (%3d/%3d) = % 10d%1s : % 10d", i,
						x.sample.rangeCount,//
						x.sample.sourceSampleExact?"E":"",//
						x.sample.f,//
						x.sample.outputCount,//
						x.sample.inputCount,//
						x.sample.estimatedCardinality,//
						x.sample.estimateEnum.getCode(),//
						x.cumulativeEstimatedCardinality//
						);
			}
			sb.append("  [");
			final Vertex[] vertices = x.getVertices();
			for (Vertex v : vertices) {
				f.format("%2d ", v.pred.getId());
			}
			sb.append("]");
			if (pruned != null) {
				if (prune)
					sb.append(" pruned");
			}
			// for (Edge e : x.edges)
			// sb.append(" (" + e.v1.pred.getId() + " " + e.v2.pred.getId()
			// + ")");
			sb.append("\n");
		}
		return sb.toString();
	}

	/**
	 * A runtime optimizer for a join graph. The {@link JoinGraph} bears some
	 * similarity to ROX (Runtime Optimizer for XQuery), but has several
	 * significant differences:
	 * <ol>
	 * <li>
	 * 1. ROX starts from the minimum cardinality edge of the minimum
	 * cardinality vertex. The {@link JoinGraph} starts with one or more low
	 * cardinality vertices.</li>
	 * <li>
	 * 2. ROX always extends the last vertex added to a given join path. The
	 * {@link JoinGraph} extends all vertices having unexplored edges in each
	 * breadth first expansion.</li>
	 * <li>
	 * 3. ROX is designed to interleave operator-at-once evaluation of join path
	 * segments which dominate other join path segments. The {@link JoinGraph}
	 * is designed to prune all join paths which are known to be dominated by
	 * other join paths for the same set of vertices in each round and iterates
	 * until a join path is identified which uses all vertices and has the
	 * minimum expected cumulative estimated cardinality. Join paths which
	 * survive pruning are re-sampled as necessary in order to obtain better
	 * information about edges in join paths which have a low estimated
	 * cardinality in order to address a problem with underflow of the
	 * cardinality estimates.</li>
	 * </ol>
	 * 
	 * TODO For join graphs with a large number of vertices we may need to
	 * constrain the #of vertices which are explored in parallel. This could be
	 * done by only branching the N lowest cardinality vertices from the already
	 * connected edges. Since fewer vertices are being explored in parallel,
	 * paths are more likely to converge onto the same set of vertices at which
	 * point we can prune the dominated paths.
	 * 
	 * TODO Compare the cumulative expected cardinality of a join path with the
	 * expected cost of a join path. The latter allows us to also explore
	 * alternative join strategies, such as the parallel subquery versus scan
	 * and filter decision for named graph and default graph SPARQL queries.
	 * 
	 * TODO Coalescing duplicate access paths can dramatically reduce the work
	 * performed by a pipelined nested index subquery. (A hash join eliminates
	 * all duplicate access paths using a scan and filter approach.) If we will
	 * run a pipeline nested index subquery join, then should the runtime query
	 * optimizer prefer paths with duplicate access paths?
	 * 
	 * TODO How can we handle things like lexicon joins. A lexicon join is is
	 * only evaluated when the dynamic type of a variable binding indicates that
	 * the RDF Value must be materialized by a join against the ID2T index.
	 * Binding sets having inlined values can simply be routed around the join
	 * against the ID2T index. Routing around saves network IO in scale-out
	 * where otherwise we would route binding sets having identifiers which do
	 * not need to be materialized to the ID2T shards.
	 * 
	 * @see <a
	 *      href="http://www-db.informatik.uni-tuebingen.de/files/research/pathfinder/publications/rox-demo.pdf">
	 *      ROX </a>
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
			final StringBuilder sb = new StringBuilder();
			sb.append("JoinGraph");
			sb.append("{V=[");
			for (Vertex v : V) {
				sb.append("\nV[" + v.pred.getId() + "]=" + v);
			}
			sb.append("],E=[");
			for (Edge e : E) {
				sb.append("\n" + e);
			}
			sb.append("\n]}");
			return sb.toString();
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
		 * 
		 * @param queryEngine
		 * @param limit
		 *            The limit for sampling a vertex and the initial limit for
		 *            cutoff join evaluation.
		 * 
		 * @throws Exception
		 */
		public Path runtimeOptimizer(final QueryEngine queryEngine,
				final int limit) throws Exception {

			// Setup the join graph.
			Path[] paths = round0(queryEngine, limit, 2/* nedges */);

			/*
			 * The input paths for the first round have two vertices (one edge
			 * is two vertices). Each round adds one more vertex, so we have
			 * three vertices by the end of round 1. We are done once we have
			 * generated paths which include all vertices.
			 * 
			 * This occurs at round := nvertices - 1
			 */

			final int nvertices = V.length;

			int round = 1;

			while (round < nvertices - 1) {

				paths = expand(queryEngine, limit, round++, paths);

			}

			// Should be one winner.
			assert paths.length == 1;

			return paths[0];

		}

		/**
		 * Choose the starting vertices.
		 * 
		 * @param nedges
		 *            The maximum #of edges to choose.
		 */
		public Path[] choseStartingPaths(final int nedges) {

			final List<Path> tmp = new LinkedList<Path>();

			// All edges in the graph.
			final Edge[] edges = getEdges().toArray(new Edge[0]);

			// Sort them by ascending expected cardinality.
			Arrays.sort(edges, 0, edges.length,
					EstimatedEdgeCardinalityComparator.INSTANCE);

			// Choose the top-N edges (those with the least cardinality).
			for (int i = 0; i < edges.length && i < nedges; i++) {

				tmp.add(new Path(edges[i]));

			}

			final Path[] a = tmp.toArray(new Path[tmp.size()]);

			return a;

		}

		/**
		 * Choose up to <i>nedges</i> edges to be the starting point.
		 * 
		 * @param queryEngine
		 *            The query engine.
		 * @param limit
		 *            The cutoff used when sampling the vertices and when
		 *            sampling the edges.
		 * @param nedges
		 *            The maximum #of edges to choose. Those having the smallest
		 *            expected cardinality will be chosen.
		 * 
		 * @throws Exception
		 */
		public Path[] round0(final QueryEngine queryEngine, final int limit,
				final int nedges) throws Exception {

			/*
			 * Sample the vertices.
			 */
			sampleVertices(queryEngine, limit);

			if (log.isInfoEnabled()) {
				final StringBuilder sb = new StringBuilder();
				sb.append("Vertices:\n");
				for (Vertex v : V) {
					sb.append(v.toString());
					sb.append("\n");
				}
				log.info(sb.toString());
			}

			/*
			 * Estimate the cardinality for each edge.
			 * 
			 * TODO It would be very interesting to see the variety and/or
			 * distribution of the values bound when the edge is sampled. This
			 * can be easily done using a hash map with a counter. That could
			 * tell us a lot about the cardinality of the next join path
			 * (sampling the join path also tells us a lot, but it does not
			 * explain it as much as seeing the histogram of the bound values).
			 * I believe that there are some interesting online algorithms for
			 * computing the N most frequent observations and the like which
			 * could be used here.
			 */
			estimateEdgeWeights(queryEngine, limit);

			if (log.isDebugEnabled()) {
				final StringBuilder sb = new StringBuilder();
				sb.append("Edges:\n");
				for (Edge e : E) {
					sb.append(e.toString());
					sb.append("\n");
				}
				log.debug(sb.toString());
			}

			/*
			 * Choose the initial set of paths.
			 */
			final Path[] paths_t0 = choseStartingPaths(nedges);

			if (log.isInfoEnabled())
				log.info("\n*** Paths @ t0\n" + JoinGraph.showTable(paths_t0));

			return paths_t0;

		}

		/**
		 * Do one breadth first expansion.
		 * 
		 * @param queryEngine
		 *            The query engine.
		 * @param limitIn
		 *            The limit (this is automatically multiplied by the round
		 *            to increase the sample size in each round).
		 * @param round
		 *            The round number in [1:n].
		 * @param a
		 *            The set of paths from the previous round. For the first
		 *            round, this is formed from the initial set of edges to
		 *            consider.
		 * 
		 * @return The set of paths which survived pruning in this round.
		 * 
		 * @throws Exception
		 */
		public Path[] expand(final QueryEngine queryEngine, int limitIn,
				final int round, final Path[] a) throws Exception {

			if (queryEngine == null)
				throw new IllegalArgumentException();
			if (limitIn <= 0)
				throw new IllegalArgumentException();
			if (round <= 0)
				throw new IllegalArgumentException();
			if (a == null)
				throw new IllegalArgumentException();
			if (a.length == 0)
				throw new IllegalArgumentException();
			
			// increment the limit by itself in each round.
			final int limit = (round + 1) * limitIn;

			if (log.isDebugEnabled())
				log.debug("round=" + round + ", limit=" + limit
						+ ", #paths(in)=" + a.length);

//			final List<Path> tmp = new LinkedList<Path>();
//
//			// First, copy all existing paths.
//			for (Path x : a) {
//				tmp.add(x);
//			}

			/*
			 * Re-sample all vertices which are part of any of the existing
			 * paths.
			 * 
			 * Note: A request to re-sample a vertex is a NOP unless the limit
			 * has been increased since the last time the vertex was sampled. It
			 * is also a NOP if the vertex has been fully materialized.
			 * 
			 * TODO We only really need to deepen those paths where we have a
			 * low estimated join hit ratio. Paths with a higher join hit ratio
			 * already have a decent estimate of the cardinality and a decent
			 * sample size and can be explored without resampling.
			 */
			if (log.isDebugEnabled())
				log.debug("Re-sampling in-use vertices: limit=" + limit);

			for (Path x : a) {

				for(Edge e : x.edges) {
					
					e.v1.sample(queryEngine, limit);
					e.v2.sample(queryEngine, limit);
					
				}
				
			}

			/*
			 * Re-sample the cutoff join for each edge in each of the existing
			 * paths using the newly re-sampled vertices.
			 * 
			 * Note: The only way to increase the accuracy of our estimates for
			 * edges as we extend the join paths is to re-sample each edge in
			 * the join path in path order.
			 * 
			 * Note: An edge must be sampled for each distinct join path prefix
			 * in which it appears within each round. However, it is common for
			 * surviving paths to share a join path prefix, so do not re-sample
			 * a given path prefix more than once per round. Also, do not
			 * re-sample paths which are from rounds before the immediately
			 * previous round as those paths will not be extended in this round.
			 */
			if (log.isDebugEnabled())
				log.debug("Re-sampling in-use path segments: limit=" + limit);

			final Map<int[], EdgeSample> edgePaths = new LinkedHashMap<int[], EdgeSample>();
			
			for (Path x : a) {

				// The edges which we have visited in this path.
				final List<Edge> edges = new LinkedList<Edge>();
				
				// The vertices which we have visited in this path.
				final Set<Vertex> vertices = new LinkedHashSet<Vertex>();
				
				EdgeSample priorEdgeSample = null;
				
				for(Edge e : x.edges) {
					
					// Add edge to the visited set for this join path.
					edges.add(e);

					// Generate unique key for this join path segment.
					final int[] ids = Path.getVertexIds(edges);

					if (priorEdgeSample == null) {

						/*
						 * This is the first edge in the path.
						 * 
						 * Test our local table of join path segment estimates
						 * to see if we have already re-sampled that edge. If
						 * not, then re-sample it now.
						 */

						// Test sample cache.
						EdgeSample edgeSample = edgePaths.get(ids);

						if (edgeSample == null) {
							if (e.sample != null && e.sample.limit >= limit) {

								// The existing sample for that edge is fine.
								edgeSample = e.sample;
								
							} else {

								/*
								 * Re-sample the edge, updating the sample on
								 * the edge as a side-effect. The cutoff sample
								 * is based on the vertex sample for the minimum
								 * cardinality vertex.
								 */

								edgeSample = e.estimateCardinality(queryEngine,
										limit);

							}

							// Cache the sample.
							if (edgePaths.put(ids, edgeSample) != null)
								throw new AssertionError();

						}

						// Add both vertices to the visited set.
						vertices.add(e.v1);
						vertices.add(e.v2);

						// Save sample. It will be used to re-sample the next edge.
						priorEdgeSample = edgeSample;

						continue;
						
					}

					final boolean v1Found = vertices.contains(e.v1);

					// The source vertex for the new edge.
					final Vertex sVertex = v1Found ? e.v1 : e.v2;

					// The target vertex for the new edge.
					final Vertex tVertex = v1Found ? e.v2 : e.v1;

					// Look for sample for this path in our cache.
					EdgeSample edgeSample = edgePaths.get(ids);

					if (edgeSample == null) {
						
						/*
						 * This is some N-step edge in the path, where N is
						 * greater than ONE (1). The source vertex is the vertex
						 * which already appears in the prior edges of this join
						 * path. The target vertex is the next vertex which is
						 * visited by the join path. The sample pass in is the
						 * prior edge sample - that is, the sample from the path
						 * segment less the target vertex. This is the sample
						 * that we just updated when we visited the prior edge
						 * of the path.
						 */

						edgeSample = e
								.estimateCardinality(
										queryEngine,
										limit,
										sVertex,
										tVertex,//
										priorEdgeSample.estimatedCardinality,//
										priorEdgeSample.estimateEnum == EstimateEnum.Exact,
										priorEdgeSample.limit,//
										priorEdgeSample.sample//
								);

						if (log.isDebugEnabled())
							log.debug("Resampled: " + Arrays.toString(ids)
									+ " : " + edgeSample);
						
						if (edgePaths.put(ids, edgeSample) != null)
							throw new AssertionError();

					}

					// Save sample. It will be used to re-sample the next edge.
					priorEdgeSample = edgeSample;

					// Add target vertex to the visited set.
					vertices.add(tVertex);

				} // next Edge [e] in Path [x]

				// Save the result on the path.
				x.sample = priorEdgeSample;
				
			} // next Path [x].
			
			/*
			 * Expand each path one step from each vertex which branches to an
			 * unused vertex.
			 */

			if (log.isDebugEnabled())
				log.debug("Expanding paths: limit=" + limit + ", #paths(in)="
						+ a.length);

			final List<Path> tmp = new LinkedList<Path>();
			
			for (Path x : a) {

//				final int nedges = x.edges.size();
//
//				if (nedges < round) {
//
//					// Path is from a previous round.
//					continue;
//					
//				}

				// The set of vertices used to expand this path in this round.
				final Set<Vertex> used = new LinkedHashSet<Vertex>();

				// Check all edges in the graph.
				for (Edge edgeInGraph : E) {

					// Figure out which vertices are already part of this path.
					final boolean v1Found = x.contains(edgeInGraph.v1);
					final boolean v2Found = x.contains(edgeInGraph.v2);

					if (!v1Found && !v2Found) {
						// Edge is not connected to this path.
						continue;
					}

					if (v1Found && v2Found) {
						// Edge is already present in this path.
						continue;
					}

					// the target vertex for the new edge.
					final Vertex tVertex = v1Found ? edgeInGraph.v2
							: edgeInGraph.v1;

//					// the source vertex for the new edge.
//					final Vertex sVertex = v1Found ? edgeInGraph.v1
//							: edgeInGraph.v2;

					if (used.contains(tVertex)) {
						// Vertex already used to extend this path.
						continue;
					}

					// add the new vertex to the set of used vertices.
					used.add(tVertex);

					// (Re-)sample vertex before we sample a new edge
					tVertex.sample(queryEngine, limit);
					
					// Extend the path to the new vertex.
					final Path p = x.addEdge(queryEngine, limit, edgeInGraph);

					// Add to the set of paths for this round.
					tmp.add(p);

				}

			}

			final Path[] paths_tp1 = tmp.toArray(new Path[tmp.size()]);

			final Path[] paths_tp1_pruned = pruneJoinPaths(paths_tp1);

			if (log.isDebugEnabled())
				log.debug("\n*** round=" + round + ", limit=" + limit
						+ " : generated paths\n"
						+ JoinGraph.showTable(paths_tp1, paths_tp1_pruned));

			if (log.isInfoEnabled())
				log.info("\n*** round=" + round + ", limit=" + limit
						+ ": paths{in=" + a.length + ",considered="
						+ paths_tp1.length + ",out=" + paths_tp1_pruned.length
						+ "}\n" + JoinGraph.showTable(paths_tp1_pruned));

			return paths_tp1_pruned;

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
			for (Vertex v : V) {
				if (v.pred.getId() == bopId)
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
			for (Edge e : E) {
				if (e.v1 == v1 && e.v2 == v2)
					return e;
				if (e.v1 == v2 && e.v2 == v1)
					return e;
			}
			return null;
		}

		/**
		 * Obtain a sample and estimated cardinality (fast range count) for each
		 * vertex.
		 * 
		 * @param queryEngine
		 * @param limit
		 *            The sample size.
		 */
		public void sampleVertices(final QueryEngine queryEngine, final int limit) {

			for (Vertex v : V) {

				v.sample(queryEngine, limit);

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

				e.estimateCardinality(queryEngine, limit);

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

		// /**
		// * Return the {@link Edge} having the minimum estimated cardinality
		// out
		// * of those edges whose cardinality has been estimated.
		// *
		// * @return The minimum cardinality edge -or- <code>null</code> if
		// there
		// * are no {@link Edge}s having an estimated cardinality.
		// */
		// public Edge getMinimumCardinalityEdge() {
		//		
		// return getMinimumCardinalityEdge(null);
		//			
		// }

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
		 * Prune paths which are dominated by other paths. Paths are extended in
		 * each round. Paths from previous rounds are always pruned. Of the new
		 * paths in each round, the following rule is applied to prune the
		 * search to just those paths which are known to dominate the other
		 * paths covering the same set of vertices:
		 * <p>
		 * If there is a path, [p] != [p1], where [p] is an unordered variant of
		 * [p1] (that is the vertices of p are the same as the vertices of p1),
		 * and the cumulative cost of [p] is LTE the cumulative cost of [p1],
		 * then [p] dominates (or is equivalent to) [p1] and p1 should be
		 * pruned.
		 * 
		 * @param a
		 *            A set of paths.
		 * 
		 * @return The set of paths with all dominated paths removed.
		 */
		public Path[] pruneJoinPaths(final Path[] a) {
			/*
			 * Find the length of the longest path(s). All shorter paths are
			 * dropped in each round.
			 */
			int maxPathLen = 0;
			for(Path p : a) {
				if(p.edges.size()>maxPathLen) {
					maxPathLen = p.edges.size();
				}
			}
			final StringBuilder sb = new StringBuilder();
			final Formatter f = new Formatter(sb);
			final Set<Path> pruned = new LinkedHashSet<Path>();
			for (int i = 0; i < a.length; i++) {
				final Path Pi = a[i];
				if (Pi.sample == null)
					throw new RuntimeException("Not sampled: " + Pi);
				if (Pi.edges.size() < maxPathLen) {
					/*
					 * Only the most recently generated set of paths survive to
					 * the next round.
					 */
					pruned.add(Pi);
					continue;
				}
				if (pruned.contains(Pi))
					continue;
				for (int j = 0; j < a.length; j++) {
					if (i == j)
						continue;
					final Path Pj = a[j];
					if (Pj.sample == null)
						throw new RuntimeException("Not sampled: " + Pj);
					if (pruned.contains(Pj))
						continue;
					final boolean isPiSuperSet = Pi.isUnorderedVariant(Pj);
					if (!isPiSuperSet) {
						// Can not directly compare these join paths.
						continue;
					}
					final long costPi = Pi.cumulativeEstimatedCardinality;
					final long costPj = Pj.cumulativeEstimatedCardinality;
					final boolean lte = costPi <= costPj;
					List<Integer> prunedByThisPath = null;
					if (lte) {
						prunedByThisPath = new LinkedList<Integer>();
						if (pruned.add(Pj))
							prunedByThisPath.add(j);
						for (int k = 0; k < a.length; k++) {
							final Path x = a[k];
							if (x.beginsWith(Pj)) {
								if (pruned.add(x))
									prunedByThisPath.add(k);
							}
						}
					}
					if (log.isDebugEnabled()) {
						f
								.format(
										"Comparing: P[%2d] with P[%2d] : %10d %2s %10d %s",
										i, j, costPi, (lte ? "<=" : ">"),
										costPj, lte ? " *** pruned "
												+ prunedByThisPath : "");
						log.debug(sb);
						sb.setLength(0);
					}
				} // Pj
			} // Pi
			final Set<Path> keep = new LinkedHashSet<Path>();
			for (Path p : a) {
				if (pruned.contains(p))
					continue;
				keep.add(p);
			}
			final Path[] b = keep.toArray(new Path[keep.size()]);
			return b;
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

			// Create the join graph.
			final JGraph g = new JGraph(getVertices());

			// Find the best join path.
			final Path p = g.runtimeOptimizer(context.getRunningQuery()
					.getQueryEngine(), limit);

			// Factory avoids reuse of bopIds assigned to the predicates.
			final BOpIdFactory idFactory = new BOpIdFactory();

			// Generate the query from the join path.
			final PipelineOp queryOp = JoinGraph.getQuery(idFactory, p
					.getPredicates());

			// Run the query, blocking until it is done.
			JoinGraph.runSubquery(context, queryOp);

			return null;

		}

	} // class JoinGraphTask

	/**
	 * Places vertices into order by the {@link BOp#getId()} associated with
	 * their {@link IPredicate}.
	 */
	private static class BOpIdComparator implements Comparator<Vertex> {

		private static final transient Comparator<Vertex> INSTANCE = new BOpIdComparator();

		@Override
		public int compare(final Vertex o1, final Vertex o2) {
			final int id1 = o1.pred.getId();
			final int id2 = o2.pred.getId();
			if (id1 < id2)
				return -1;
			if (id2 > id1)
				return 1;
			return 0;
		}

	}

	/**
	 * Places edges into order by ascending estimated cardinality. Edges which
	 * are not weighted are ordered to the end.
	 * 
	 * TODO unit tests, including with unweighted edges.
	 */
	private static class EstimatedEdgeCardinalityComparator implements
			Comparator<Edge> {

		public static final transient Comparator<Edge> INSTANCE = new EstimatedEdgeCardinalityComparator();

		@Override
		public int compare(final Edge o1, final Edge o2) {
			if (o1.sample == null && o2.sample == null) {
				// Neither edge is weighted.
				return 0;
			}
			if (o1.sample == null) {
				// o1 is not weighted, but o2 is. sort o1 to the end.
				return 1;
			}
			if (o2.sample == null) {
				// o2 is not weighted. sort o2 to the end.
				return -1;
			}
			final long id1 = o1.sample.estimatedCardinality;
			final long id2 = o2.sample.estimatedCardinality;
			if (id1 < id2)
				return -1;
			if (id1 > id2)
				return 1;
			return 0;
		}

	}

	/*
	 * Static methods:
	 * 
	 * @todo Keep with JGraph or move to utility class. However, the precise
	 * manner in which the query plan is generated is still up in the air since
	 * we are not yet handling anything except standard joins in the runtime
	 * optimizer.
	 */
	
	/**
	 * Generate a query plan from an ordered collection of predicates.
	 * 
	 * @param p
	 *            The join path.
	 *            
	 * @return The query plan.
	 */
	static public PipelineOp getQuery(final BOpIdFactory idFactory,
			final IPredicate[] preds) {

		final PipelineJoin[] joins = new PipelineJoin[preds.length];

//        final PipelineOp startOp = new StartOp(new BOp[] {},
//                NV.asMap(new NV[] {//
//                        new NV(Predicate.Annotations.BOP_ID, idFactory
//                                .nextId()),//
//                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
//                                BOpEvaluationContext.CONTROLLER),//
//                }));
//
//		PipelineOp lastOp = startOp;
		PipelineOp lastOp = null;

//		final Set<IVariable> vars = new LinkedHashSet<IVariable>();
//		for(IPredicate p : preds) {
//			for(BOp arg : p.args()) {
//				if(arg instanceof IVariable) {
//					vars.add((IVariable)arg);
//				}
//			}
//		}
		
        for (int i = 0; i < preds.length; i++) {

			// The next vertex in the selected join order.
			final IPredicate p = preds[i];

			final List<NV> anns = new LinkedList<NV>();

			anns.add(new NV(PipelineJoin.Annotations.PREDICATE, p));

			anns.add(new NV(PipelineJoin.Annotations.BOP_ID, idFactory
					.nextId()));

//			anns.add(new NV(PipelineJoin.Annotations.EVALUATION_CONTEXT, BOpEvaluationContext.ANY));
//
//			anns.add(new NV(PipelineJoin.Annotations.SELECT, vars.toArray(new IVariable[vars.size()])));

			final PipelineJoin joinOp = new PipelineJoin(
					lastOp == null ? new BOp[0] : new BOp[] { lastOp },
					anns.toArray(new NV[anns.size()]));

			joins[i] = joinOp;

			lastOp = joinOp;

		}

//		final PipelineOp queryOp = lastOp;

		/*
		 * FIXME Why does wrapping with this slice appear to be
		 * necessary? (It is causing runtime errors when not wrapped).
		 * Is this a bopId collision which is not being detected?
		 */
		final PipelineOp queryOp = new SliceOp(new BOp[] { lastOp }, NV
				.asMap(new NV[] {
						new NV(JoinGraph.Annotations.BOP_ID, idFactory.nextId()), //
						new NV(JoinGraph.Annotations.EVALUATION_CONTEXT,
								BOpEvaluationContext.CONTROLLER) }) //
		);

		return queryOp;

	}

	/**
	 * Execute the selected join path.
	 * <p>
	 * Note: When executing the query, it is actually being executed as a
	 * subquery. Therefore we have to take appropriate care to ensure that the
	 * results are copied out of the subquery and into the parent query. See
	 * {@link AbstractSubqueryOp} for how this is done.
	 * 
	 * @todo When we execute the query, we should clear the references to the
	 *       samples (unless they are exact, in which case they can be used as
	 *       is) in order to release memory associated with those samples if the
	 *       query is long running. Samples must be held until we have
	 *       identified the final join path since each vertex will be used by
	 *       each maximum length join path and we use the samples from the
	 *       vertices to re-sample the surviving join paths in each round.
	 * 
	 * @todo If there is a slice on the outer query, then the query result may
	 *       well be materialized by now.
	 * 
	 * @todo If there are source binding sets then they need to be applied above
	 *       (when we are sampling) and below (when we evaluate the selected
	 *       join path).
	 * 
	 *       FIXME runQuery() is not working correctly. The query is being
	 *       halted by a {@link BufferClosedException} which appears before it
	 *       has materialized the necessary results.
	 */
	static public void runSubquery(final BOpContext<IBindingSet> parentContext,
			final PipelineOp queryOp) {

		IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = null;

		try {
			
			if (log.isInfoEnabled())
				log.info("Running: " + BOpUtility.toString(queryOp));

			final PipelineOp startOp = (PipelineOp) BOpUtility
					.getPipelineStart(queryOp);

			if (log.isInfoEnabled())
				log.info("StartOp: " + BOpUtility.toString(startOp));

			// Run the query.
			final UUID queryId = UUID.randomUUID();

			final QueryEngine queryEngine = parentContext.getRunningQuery()
					.getQueryEngine();

			final RunningQuery runningQuery = queryEngine
					.eval(
							queryId,
							queryOp,
							new LocalChunkMessage<IBindingSet>(
									queryEngine,
									queryId,
									startOp.getId()/* startId */,
									-1 /* partitionId */,
									/*
									 * @todo pass in the source binding sets
									 * here and also when sampling the
									 * vertices.
									 */
									new ThickAsynchronousIterator<IBindingSet[]>(
											new IBindingSet[][] { new IBindingSet[] { new HashBindingSet() } })));

			// Iterator visiting the subquery solutions.
			subquerySolutionItr = runningQuery.iterator();

			// Copy solutions from the subquery to the query.
			final long nout = BOpUtility
					.copy(subquerySolutionItr, parentContext.getSink(),
							null/* sink2 */, null/* constraints */, null/* stats */);

			System.out.println("nout=" + nout);
			
			// verify no problems.
			runningQuery.get();

			System.out.println("Future Ok");

		} catch (Throwable t) {

			log.error(t,t);
			
			/*
			 * If a subquery fails, then propagate the error to the parent
			 * and rethrow the first cause error out of the subquery.
			 */
			throw new RuntimeException(parentContext.getRunningQuery()
					.halt(t));

		} finally {

			if (subquerySolutionItr != null)
				subquerySolutionItr.close();

		}

	}

}
