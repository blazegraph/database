/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Feb 22, 2011
 */
package com.bigdata.bop.joinGraph.rto;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.join.PipelineJoin.PipelineJoinStats;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.Dechunkerator;

/**
 * An edge of the join graph is an annotated join operator. The edges of the
 * join graph are undirected. Edges exist when the vertices share at least one
 * variable.
 * <p>
 * {@link #hashCode()} is defined in terms of the unordered hash codes of the
 * individual vertices.
 */
public class Edge implements Serializable {

    private static final transient Logger log = Logger.getLogger(Edge.class);

    private static final long serialVersionUID = 1L;

    /**
     * The vertices connected by that edge.
     */
    public final Vertex v1, v2;

    // /**
    // * The set of shared variables.
    // */
    // public final Set<IVariable<?>> shared;

    /**
     * The last sample for this edge and <code>null</code> if the edge has not
     * been sampled.
     * <p>
     * Note: This sample is only the one-step cutoff evaluation of the edge
     * given a sample of its vertex having the lesser cardinality. It is NOT the
     * cutoff sample of a join path having this edge except for the degenerate
     * case where the edge is the first edge in the join path.
     */
    transient EdgeSample sample = null;

    /**
     * 
     * @param path
     *            The path which the edge is extending.
     * @param v1
     *            A vertex in that path which serves as the source of this edge.
     * @param v2
     * 
     *            FIXME EDGES : The concept of the "source" of an edge is
     *            actually quite misleading. This was originally an (arbitrary)
     *            vertex which shared a variable with the target vertex.
     *            However, in order to handle joins which are only indirectly
     *            constrained by a constraint we need to allow for a source
     *            vertex which does not share any variable (directly) with the
     *            target vertex. In addition, we also need the source path or
     *            the set of constraints to be attached to the edge. Finally, we
     *            can no longer share edges since they have to have some aspect
     *            of history attached. All in all, the "edge" is really just the
     *            last aspect of a path so what we have are ordered arrays of
     *            predicates and the constraints which run when each predicate
     *            is evaluated as part of a join.
     */
    // * @param shared
    public Edge(//
            // final IPredicate<?>[] path,
            final Vertex v1, final Vertex v2
    // , final Set<IVariable<?>> shared
    ) {
        if (v1 == null)
            throw new IllegalArgumentException();
        if (v2 == null)
            throw new IllegalArgumentException();
        // if (shared == null)
        // throw new IllegalArgumentException();
        // Note: We need to allow edges which do not share variables
        // if (shared.isEmpty())
        // throw new IllegalArgumentException();
        this.v1 = v1;
        this.v2 = v2;
        // this.shared = shared;
    }

    /**
     * The edge label is formed from the {@link BOp.Annotations#BOP_ID} of its
     * ordered vertices (v1,v2).
     */
    public String getLabel() {

        return "(" + v1.pred.getId() + "," + v2.pred.getId() + ")";

    }

    /**
     * Note: The vertices of the edge are labeled using the
     * {@link BOp.Annotations#BOP_ID} associated with the {@link IPredicate} for
     * each vertex.
     */
    public String toString() {

        return "Edge{ " + getLabel() + ", estCard="
                + (sample == null ? "N/A" : sample.estimatedCardinality)
                // + ", shared=" + shared.toString() +
                + ", sample=" + sample//
                + "}";

    }

    /**
     * Equality is determined by reference testing.
     */
    public boolean equals(final Object o) {

        return this == o;

    }

    /**
     * The hash code of an edge is the hash code of the vertex with the smaller
     * hash code X 31 plus the hash code of the vertex with the larger hash
     * code. This definition compensates for the arbitrary order in which the
     * vertices may be expressed and also recognizes that the vertex hash codes
     * are based on the bop ids, which are often small integers.
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

        return (v1.sample.estimatedCardinality < v2.sample.estimatedCardinality) ? v1 : v2;

    }

    /**
     * Return the vertex with the larger estimated cardinality (the vertex not
     * returned by {@link #getMinimumCardinalityVertex()}).
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
     * side-effect. This is a NOP if the edge has already been sampled at that
     * <i>limit</i>. This is a NOP if the edge sample is exact.
     * 
     * @param queryEngine
     *            The query engine.
     * @param limit
     *            The sample size.
     * 
     * @return The new {@link EdgeSample} (this is also updated on
     *         {@link #sample} as a side-effect).
     * 
     * @throws Exception
     * 
     *             FIXME This is actually using the source vertex as the source
     *             sample which is WRONG.
     */
    public EdgeSample estimateCardinality(final QueryEngine queryEngine,
            final int limit) throws Exception {

        if (limit <= 0)
            throw new IllegalArgumentException();

        // /*
        // * Note: There is never a need to "re-sample" the edge. Unlike ROX,
        // * we always can sample a vertex. This means that we can sample the
        // * edges exactly once, during the initialization of the join graph.
        // */
        // if (sample != null)
        // throw new RuntimeException();

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
         * Figure out which vertex has the smaller cardinality. The sample of
         * that vertex is used since it is more representative than the sample
         * of the other vertex.
         * 
         * Note: If there are constraints which can run for this edge, then they
         * will be attached when the edge is sampled.
         */
        // vertex v, vprime
        final Vertex v, vp;
        if (v1.sample == null) // vertex not sampled.
            throw new IllegalStateException();
        if (v2.sample == null) // vertex not sampled.
            throw new IllegalStateException();
        if (v1.sample.estimatedCardinality < v2.sample.estimatedCardinality) {
            v = v1;
            vp = v2;
        } else {
            v = v2;
            vp = v1;
        }

//        /*
//         * Convert the source sample into an IBindingSet[].
//         * 
//         * Note: This is now done when we sample the vertex.
//         */
//        final IBindingSet[] sourceSample = new IBindingSet[v.sample.sample.length];
//        {
//            for (int i = 0; i < sourceSample.length; i++) {
//                final IBindingSet bset = new HashBindingSet();
//                BOpContext.copyValues((IElement) v.sample.sample[i], v.pred,
//                        bset);
//                sourceSample[i] = bset;
//            }
//        }

        // Sample the edge and save the sample on the edge as a side-effect.
        this.sample = estimateCardinality(queryEngine, limit, v, vp,
                v.sample // the source sample.
//                v.sample.estimatedCardinality,//
//                v.sample.estimateEnum == EstimateEnum.Exact, //
//                v.sample.limit,//
//                v.sample.sample//
                );

        return sample;

    }

    /**
     * Estimate the cardinality of the edge given a sample of either a vertex or
     * a join path leading up to that edge.
     * <p>
     * Note: The caller is responsible for protecting against needless
     * re-sampling.
     * 
     * @param queryEngine
     *            The query engine.
     * @param limit
     *            The limit for the cutoff join.
     * @param vSource
     *            The source vertex.
     * @param vTarget
     *            The target vertex
     * @param sourceSample
     *            The input sample for the cutoff join. When this is a one-step
     *            estimation of the cardinality of the edge, then this sample is
     *            taken from the {@link VertexSample}. When the edge (vSource,
     *            vTarget) extends some {@link Path}, then this is taken from
     *            the {@link EdgeSample} for that {@link Path}.
     * 
     * @return The result of sampling that edge.
     * 
     * @throws Exception
     */
    // * @param path The path which is being extended.
    public EdgeSample estimateCardinality(//
            final QueryEngine queryEngine,//
            final int limit,//
            // final IPredicate<?>[] path,//
            final Vertex vSource,// 
            final Vertex vTarget,//
            final SampleBase sourceSample//
//            final long sourceEstimatedCardinality,//
//            final boolean sourceSampleExact,//
//            final int sourceSampleLimit,//
//            final IBindingSet[] sourceSample//
    ) throws Exception {

        if (limit <= 0)
            throw new IllegalArgumentException();

        /*
         * Note: This sets up a cutoff pipeline join operator which makes an
         * accurate estimate of the #of input solutions consumed and the #of
         * output solutions generated. From that, we can directly compute the
         * join hit ratio. This approach is preferred to injecting a "RowId"
         * column as the estimates are taken based on internal counters in the
         * join operator and the join operator knows how to cutoff evaluation as
         * soon as the limit is satisfied, thus avoiding unnecessary effort.
         */
        /*
         * The set of constraint(s) (if any) which will be applied when we
         * perform the cutoff evaluation of this edge (aka join).
         * 
         * FIXME CONSTRAINTS - we need the join path to decide which constraints
         * will be attached when we sample this edge (or at least the set of
         * variables which are already known to be bound).
         */
        final IConstraint[] constraints = null;
        final int joinId = 1;
        final Map<String, Object> anns = NV.asMap(
            //
            new NV(BOp.Annotations.BOP_ID, joinId),//
            // @todo Why not use a factory which avoids bopIds
            // already in use?
            new NV(PipelineJoin.Annotations.PREDICATE, vTarget.pred.setBOpId(3)),
            // Note: does not matter since not executed by the query
            // controller.
            // // disallow parallel evaluation of tasks
            // new NV(PipelineOp.Annotations.MAX_PARALLEL,1),
            // disallow parallel evaluation of chunks.
            new NV(PipelineJoin.Annotations.MAX_PARALLEL_CHUNKS, 0),
            // disable access path coalescing
            new NV( PipelineJoin.Annotations.COALESCE_DUPLICATE_ACCESS_PATHS, false), //
            // pass in constraints on this join.
            new NV(PipelineJoin.Annotations.CONSTRAINTS, constraints),//
            // cutoff join.
            new NV(PipelineJoin.Annotations.LIMIT, (long) limit),
            /*
             * Note: In order to have an accurate estimate of the
             * join hit ratio we need to make sure that the join
             * operator runs using a single PipelineJoinStats
             * instance which will be visible to us when the query
             * is cutoff. In turn, this implies that the join must
             * be evaluated on the query controller.
             * 
             * @todo This implies that sampling of scale-out joins
             * must be done using remote access paths.
             */
            new NV(PipelineJoin.Annotations.SHARED_STATE, true),//
            new NV(PipelineJoin.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.CONTROLLER)//
            );

        @SuppressWarnings("unchecked")
        final PipelineJoin<?> joinOp = new PipelineJoin(new BOp[] {}, anns);

        final PipelineOp queryOp = joinOp;

        // run the cutoff sampling of the edge.
        final UUID queryId = UUID.randomUUID();
        final IRunningQuery runningQuery = queryEngine.eval(queryId, queryOp,
                new LocalChunkMessage<IBindingSet>(queryEngine, queryId, joinOp
                        .getId()/* startId */, -1 /* partitionId */,
                        new ThickAsynchronousIterator<IBindingSet[]>(
                                new IBindingSet[][] { sourceSample.sample })));

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

        // #of solutions in.
        final int inputCount = (int) joinStats.inputSolutions.get();

        // #of solutions out.
        long outputCount = joinStats.outputSolutions.get();

        // cumulative range count of the sampled access paths.
        final long sumRangeCount = joinStats.accessPathRangeCount.get();

        final EstimateEnum estimateEnum;
        if (sourceSample.estimateEnum == EstimateEnum.Exact
                && outputCount < limit) {
            /*
             * Note: If the entire source vertex is being fed into the cutoff
             * join and the cutoff join outputCount is LT the limit, then the
             * sample is the actual result of the join. That is, feeding all
             * source solutions into the join gives fewer than the desired
             * number of output solutions.
             */
            estimateEnum = EstimateEnum.Exact;
        } else if (inputCount == 1 && outputCount == limit) {
            /*
             * If the inputCount is ONE (1) and the outputCount is the limit,
             * then the estimated cardinality is a lower bound as more than
             * outputCount solutions might be produced by the join when
             * presented with a single input solution.
             * 
             * However, this condition suggests that the sum of the sampled
             * range counts is a much better estimate of the cardinality of this
             * join.
             * 
             * For example, consider a join feeding a rangeCount of 16 into a
             * rangeCount of 175000. With a limit of 100, we estimated the
             * cardinality at 1600L (lower bound). In fact, the cardinality is
             * 16*175000. This falsely low estimate can cause solutions which
             * are really better to be dropped.
             */
            // replace outputCount with the sum of the sampled range counts.
            outputCount = sumRangeCount;
            estimateEnum = EstimateEnum.LowerBound;
        } else if (!(sourceSample.estimateEnum != EstimateEnum.Exact)
                && inputCount == Math.min(sourceSample.limit,
                        sourceSample.estimatedCardinality) && outputCount == 0) {
            /*
             * When the source sample was not exact, the inputCount is EQ to the
             * lesser of the source range count and the source sample limit, and
             * the outputCount is ZERO (0), then feeding in all source solutions
             * in is not sufficient to generate any output solutions. In this
             * case, the estimated join hit ratio appears to be zero. However,
             * the estimation of the join hit ratio actually underflowed and the
             * real join hit ratio might be a small non-negative value. A real
             * zero can only be identified by executing the full join.
             * 
             * Note: An apparent join hit ratio of zero does NOT imply that the
             * join will be empty (unless the source vertex sample is actually
             * the fully materialized access path - this case is covered above).
             */
            estimateEnum = EstimateEnum.Underflow;
        } else {
            estimateEnum = EstimateEnum.Normal;
        }

        final double f = outputCount == 0 ? 0
                : (outputCount / (double) inputCount);

        final long estimatedCardinality = (long) (sourceSample.estimatedCardinality * f);

        final EdgeSample edgeSample = new EdgeSample(//
//                sourceSample.estimatedCardinality, //
//                sourceSample.estimateEnum, // 
//                sourceSample.limit, //
                sourceSample,//
                inputCount,//
                outputCount, //
                f, //
                // args to SampleBase
                estimatedCardinality, //
                limit, //
                estimateEnum,//
                result.toArray(new IBindingSet[result.size()]));

        if (log.isDebugEnabled())
            log.debug(getLabel() + " : newSample=" + edgeSample);

        return edgeSample;

    }

}
