package com.bigdata.rdf.graph.analytics;

import org.apache.log4j.Logger;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.GASUtil;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.impl.GASRunner;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

/**
 * SSSP (Single Source, Shortest Path). This analytic computes the shortest path
 * to each vertex in the graph starting from the given vertex. Only connected
 * vertices are visited by this implementation (the frontier never leaves the
 * connected component in which the starting vertex is located).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO There is no reason to do a gather on the first round. Add
 *         isGather()? (parallel to isChanged() for scatter?)
 * 
 *         TODO Add reducer pattern for finding the maximum degree vertex.
 * 
 *         TODO Add parameter for directed versus undirected SSSP. When
 *         undirected, the gather and scatter are for AllEdges. Otherwise,
 *         gather on in-edges and scatter on out-edges. Also, we need to use a
 *         getOtherVertex(e) method to figure out the other edge when using
 *         undirected scatter/gather. Add unit test for undirected.
 */
@SuppressWarnings("rawtypes")
public class SSSP implements IGASProgram<SSSP.VS, SSSP.ES, Integer/* dist */> {

    private static final Logger log = Logger.getLogger(SSSP.class);

    /**
     * The length of an edge.
     * 
     * FIXME RDR: This should be modified to use link weights with RDR. We need
     * a pattern to get the link attributes materialized with the {@link ISPO}
     * for the link. That could be done using a read-ahead filter on the
     * striterator if the link weights are always clustered with the ground
     * triple.
     * 
     * When we make this change, the distance should be of the same type as the
     * link weight or generalized as <code>double</code>.
     */
    private final static int EDGE_LENGTH = 1;
    
    static class VS {

        /**
         * The minimum observed distance (in hops) from the source to this
         * vertex and initially {@link Integer#MAX_VALUE}. When this value is
         * modified, the {@link #changed} flag is set as a side-effect.
         */
        private int dist = Integer.MAX_VALUE;

        private boolean changed = false;

//        /**
//         * Set the distance for the vertex to ZERO. This is done for the
//         * starting vertex.
//         */
//        public void zero() {
//            synchronized (this) {
//                dist = 0;
//                changed = true;
//            }
//        }

        /**
         * Return <code>true</code> if the {@link #dist()} was updated by the
         * last APPLY.
         */
        public boolean isChanged() {
            synchronized (this) {
                return changed;
            }
        }

        /**
         * The current estimate of the minimum distance from the starting vertex
         * to this vertex and {@link Integer#MAX_VALUE} until this vertex is
         * visited.
         */
        public int dist() {
            synchronized (this) {
                return dist;
            }
        }

        @Override
        public String toString() {

            return "{dist=" + dist() + ", changed=" + isChanged() + "}";

        }

    }// class VS

    /**
     * Edge state is not used.
     */
    static class ES {

    }

    private static final Factory<IV, SSSP.VS> vertexStateFactory = new Factory<IV, SSSP.VS>() {

        @Override
        public SSSP.VS initialValue(final IV value) {

            return new VS();

        }

    };

    @Override
    public Factory<IV, SSSP.VS> getVertexStateFactory() {

        return vertexStateFactory;

    }

    @Override
    public Factory<ISPO, SSSP.ES> getEdgeStateFactory() {

        return null;

    }

    @Override
    public EdgesEnum getGatherEdges() {

        return EdgesEnum.InEdges;

    }

    @Override
    public EdgesEnum getScatterEdges() {

        return EdgesEnum.OutEdges;

    }

    /**
     * Set the {@link VS#dist()} to ZERO (0).
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void init(final IGASContext<SSSP.VS, SSSP.ES, Integer> ctx,
            final IV u) {

        final VS us = ctx.getState(u);

        synchronized (us) {

            // Set distance to zero for starting vertex.
            us.dist = 0;
            
            // Must be true to trigger scatter in the 1st round!
            us.changed = true;
            
        }
        
    }
    
    /**
     * <code>src.dist + edge_length (1)</code>
     * <p>
     * {@inheritDoc}
     */
    @Override
    public Integer gather(final IGASContext<SSSP.VS, SSSP.ES, Integer> ctx,
            final IV u, final ISPO e) {

//        assert e.o().equals(u);

        final VS src = ctx.getState(e.s());
        
        final int d = src.dist();

        if (d == Integer.MAX_VALUE) {

            // Note: Avoids overflow (wrapping around to a negative value).
            return d;

        }

        return d + EDGE_LENGTH;

    }

    /**
     * MIN
     */
    @Override
    public Integer sum(final Integer left, final Integer right) {

        return Math.min(left, right);

    }

    /**
     * Update the {@link VS#dist()} and {@link VS#isChanged()} based on the new
     * <i>sum</i>.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SSSP.VS apply(IGASContext<SSSP.VS, SSSP.ES, Integer> ctx,
            final IV u, final Integer sum) {

        if (sum != null) {

//            log.error("u=" + u + ", us=" + us + ", sum=" + sum);

            // Get the state for that vertex.
            final SSSP.VS us = ctx.getState(u);

            final int minDist = sum;

            synchronized(us) {
                us.changed = false;
                if (us.dist > minDist) {
                    us.dist = minDist;
                    us.changed = true;
                    if (log.isDebugEnabled())
                        log.debug("u=" + u + ", us=" + us + ", minDist=" + minDist);
                    return us;
                }
            }
        }

        // No change.
        return null;

    }

    @Override
    public boolean isChanged(final IGASContext<SSSP.VS, SSSP.ES, Integer> ctx,
            final IV u) {

        return ctx.getState(u).isChanged();

    }

    /**
     * The remote vertex is scheduled if this vertex is changed.
     * <p>
     * Note: We are scattering to out-edges. Therefore, this vertex is
     * {@link ISPO#s()}. The remote vertex is {@link ISPO#o()}.
     * <p>
     * {@inheritDoc}
     * 
     * FIXME Test both variations on a variety of data sets and see which is
     * better:
     * 
     * <p>
     * Zhisong wrote: In the original GASengine, the scatter operator only need
     * to access the status of the source: src.changes.
     * 
     * To check the status of destination, it needs to load destination data:
     * dst.dist and edge data: e. And then check if new dist is different from
     * the old value.
     * 
     * Bryan wrote: I will have to think about this more. It sounds like it
     * depends on the fan-out of the scatter at time t versus the fan-in of the
     * gather at time t+1. The optimization might only benefit if a reasonable
     * fraction of the destination vertices wind up NOT being retriggered. I
     * will try on these variations in the Java code as well. *
     * </p>
     */
    @Override
    public void scatter(final IGASContext<SSSP.VS, SSSP.ES, Integer> ctx,
            final IV u, final ISPO e) {

        final IV other = GASUtil.getOtherVertex(u, e);
        
        final VS selfState = ctx.getState(u);
        
        final VS otherState = ctx.getState(other);

        // last observed distance for the remote vertex.
        final int otherDist = otherState.dist();
        
        // new distance for the remote vertex.
        final int newDist = selfState.dist() + EDGE_LENGTH;
        
        if (newDist < otherDist) {

            synchronized (otherState) {
                otherState.dist = newDist;
                otherState.changed = true;
            }
            
            if (log.isDebugEnabled())
                log.debug("u=" + u + " @ " + selfState.dist()
                        + ", scheduling: " + other + " with newDist=" + newDist);

            // Then add the remote vertex to the next frontier.
            ctx.schedule(e.o());

        }

    }

    /**
     * Performance test harness.
     */
    public static void main(final String[] args) throws Exception {

        new GASRunner<SSSP.VS, SSSP.ES, Integer>(args) {

            @Override
            protected IGASProgram<SSSP.VS, SSSP.ES, Integer> newGASProgram() {

                return new SSSP();

            }

        }.call();

    }

}
