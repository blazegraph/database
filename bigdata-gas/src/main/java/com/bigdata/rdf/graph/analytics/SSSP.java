/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.analytics;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.rdf.graph.BinderBase;
import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.FrontierEnum;
import com.bigdata.rdf.graph.IBinder;
import com.bigdata.rdf.graph.IBindingExtractor;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IPredecessor;
import com.bigdata.rdf.graph.impl.BaseGASProgram;

/**
 * SSSP (Single Source, Shortest Path). This analytic computes the shortest path
 * to each connected vertex in the graph starting from the given vertex. Only
 * connected vertices are visited by this implementation (the frontier never
 * leaves the connected component in which the starting vertex is located).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         FIXME Add a reducer to report the actual minimum length paths. This
 *         is similar to a BFS tree, but the path lengths are not integer values
 *         so we need a different data structure to collect them (we need to
 *         store the predecesor when we run SSSP to do this).
 */
public class SSSP extends BaseGASProgram<SSSP.VS, SSSP.ES, Integer/* dist */> 
		implements IPredecessor<SSSP.VS, SSSP.ES, Integer/* dist */> {

    private static final Logger log = Logger.getLogger(SSSP.class);

    /**
     * The length of an edge.
     * 
     * FIXME RDR: This should be modified to use link weights with RDR. We need
     * a pattern to get the link attributes materialized with the {@link Statement}
     * for the link. That could be done using a read-ahead filter on the
     * striterator if the link weights are always clustered with the ground
     * triple. See {@link #decodeStatement(Value)}.
     * <P>
     * When we make this change, the distance should be of the same type as the
     * link weight or generalized as <code>double</code>.
     * <p>
     * Maybe add a factory method or alternative constructor for the version of
     * SSSP that uses link weights? All we need to do is filter out anything
     * that is not a link weight. In addition, it will often be true that there
     * is a single link attribute type that is of interest, so the caller should
     * also be able to specify that.
     */
    private final static double EDGE_LENGTH = 1.0f;
    
    public static class VS {

        /**
         * The minimum observed distance (in hops) from the source to this
         * vertex and initially {@link Integer#MAX_VALUE}. When this value is
         * modified, the {@link #changed} flag is set as a side-effect.
         * 
         * FIXME This really needs to be a floating point value, probably
         * double. We also need tests with non-integer weights and non- positive
         * weights.
         */
        private Double dist = Double.MAX_VALUE;

//        /**
//         * Note: This flag is cleared by apply() and then conditionally set
//         * iff the {@link #dist()} is replaced by the new value from the
//         * gather.  Thus, if the gather does not reduce the value, then the
//         * propagation of the algorithm is halted. However, this causes the
//         * algorithm to NOT scatter for round zero, which causes it to halt.
//         * I plan to fix the algorithm by doing the "push" style update in
//         * the scatter phase. That will completely remove the gather phase
//         * of the algorithm.
//         */
//        private boolean changed = false;

        /**
         * The predecessor is the source vertex to visit a given target vertex
         * with the minimum observed distance.
         */
        private final AtomicReference<Value> predecessor = new AtomicReference<Value>();

        /**
         * Return the vertex preceding this vertex on the shortest path.
         */
        public Value predecessor() {

            return predecessor.get();
            
        }

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

//        /**
//         * Return <code>true</code> if the {@link #dist()} was updated by the
//         * last APPLY.
//         */
//        public boolean isChanged() {
//            synchronized (this) {
//                return changed;
//            }
//        }

        /**
         * The current estimate of the minimum distance from the starting vertex
         * to this vertex and {@link Integer#MAX_VALUE} until this vertex is
         * visited.
         */
        public double dist() {
            synchronized (this) {
                return dist;
            }
        }

        @Override
        public String toString() {

            return "{dist=" + dist() + ", predecessor=" + predecessor.get()
//                    + ", changed=" + isChanged() 
                    + "}";

        }

        /**
         * Mark this as a starting vertex (distance:=ZERO, changed:=true).
         */
        synchronized private void setStartingVertex() {

            // Set distance to zero for starting vertex.
            dist = 0.0;
            this.predecessor.set(null);

//            // Must be true to trigger scatter in the 1st round!
//            changed = true;

        }

//        /**
//         * Update the vertex state to the minimum of the combined sum and its
//         * current state.
//         * 
//         * @param u
//         *            The vertex that is the owner of this {@link VS vertex
//         *            state} (used only for debug info).
//         * @param sum
//         *            The combined sum from the gather phase.
//         * 
//         * @return <code>this</code> iff the vertex state was modified.
//         * 
//         *         FIXME PREDECESSOR: We can not track the predecessor because
//         *         the SSSP algorithm currently uses a GATHER phase and a
//         *         SCATTER phase rather than doing all the work in a push-style
//         *         SCATTER phase.
//         */
//        synchronized private VS apply(final Value u, final Integer sum) {
//
//            final int minDist = sum;
//
//            changed = false;
//            if (dist > minDist) {
//                dist = minDist;
//                changed = true;
//                if (log.isDebugEnabled())
//                    log.debug("u=" + u + ", us=" + this + ", minDist="
//                            + minDist);
//                return this;
//            }
//            
//            return null;
//
//        }

        /**
         * Update the vertex state to the new (reduced) distance.
         * 
         * @param predecessor
         *            The vertex that propagated the update to this vertex.
         * @param newDist
         *            The new distance.
         *            
         * @return <code>true</code> iff this vertex state was changed.
         */
        synchronized private boolean scatter(final Value predecessor,
                final double newDist) {
            /*
             * Validate that the distance has decreased while holding the lock.
             */
            if (newDist < dist) {
                dist = newDist;
                this.predecessor.set(predecessor);
//                changed = true;
                return true;
            }
            return false;
        }

    }// class VS

    /**
     * Edge state is not used.
     */
    public static class ES {

    }

    private static final Factory<Value, SSSP.VS> vertexStateFactory = new Factory<Value, SSSP.VS>() {

        @Override
        public SSSP.VS initialValue(final Value value) {

            return new VS();

        }

    };

    @Override
    public Factory<Value, SSSP.VS> getVertexStateFactory() {

        return vertexStateFactory;

    }

    @Override
    public FrontierEnum getInitialFrontierEnum() {

        return FrontierEnum.SingleVertex;
        
    }
    
//    @Override
//    public Factory<ISPO, SSSP.ES> getEdgeStateFactory() {
//
//        return null;
//
//    }

    @Override
    public EdgesEnum getGatherEdges() {

//        return EdgesEnum.InEdges;
        return EdgesEnum.NoEdges;

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
    public void initVertex(final IGASContext<SSSP.VS, SSSP.ES, Integer> ctx,
            final IGASState<SSSP.VS, SSSP.ES, Integer> state, final Value u) {

        final VS us = state.getState(u);

        us.setStartingVertex();
        
    }
    
    /**
     * <code>src.dist + edge_length (1)</code>
     * <p>
     * {@inheritDoc}
     */
    @Override
    public Integer gather(final IGASState<SSSP.VS, SSSP.ES, Integer> state,
            final Value u, final Statement e) {
        throw new UnsupportedOperationException();

////        assert e.getObject().equals(u);
//
////        final VS src = state.getState(e.getSubject());
//        final VS src = state.getState(u);
//        
//        final int d = src.dist();
//
//        if (d == Integer.MAX_VALUE) {
//
//            // Note: Avoids overflow (wrapping around to a negative value).
//            return d;
//
//        }
//
//        return d + EDGE_LENGTH;

    }

    /**
     * UNUSED.
     */
    @Override
    public Integer sum(final IGASState<SSSP.VS, SSSP.ES, Integer> state,
            final Integer left, final Integer right) {
        throw new UnsupportedOperationException();
//        return Math.min(left, right);

    }

    /** NOP. */
//    * Update the {@link VS#dist()} and {@link VS#isChanged()} based on the new
//    * <i>sum</i>.
//    * <p>
//    * {@inheritDoc}
    @Override
    public SSSP.VS apply(final IGASState<SSSP.VS, SSSP.ES, Integer> state,
            final Value u, final Integer sum) {

//        if (sum != null) {
//
////            log.error("u=" + u + ", us=" + us + ", sum=" + sum);
//
//            // Get the state for that vertex.
//            final SSSP.VS us = state.getState(u);
//
//            return us.apply(u, sum);
//            
//        }

        // No change.
        return null;

    }

    /*
     * Note: Enabling this check causes SSSP to fail. The problem is that the
     * changed flag is cleared when we enter apply(). Therefore, it gets cleared
     * on the first round before we do the scatter and testing us.isChanged()
     * here causes the scatter step to be skipped. This causes the propagation
     * of the updates to stop.  This issue is also documented on the [isChanged]
     * flag of the vertex state.
     */
//    @Override
//    public boolean isChanged(final IGASState<SSSP.VS, SSSP.ES, Integer> state,
//            final Value u) {
//
//        return state.getState(u).isChanged();
//
//    }

    /**
     * The remote vertex is scheduled the weighted edge from this vertex to the
     * remote vertex plus the weight on this vertex is less than the weight on
     * the remote vertex.
     */
    @Override
    public void scatter(final IGASState<SSSP.VS, SSSP.ES, Integer> state,
            final IGASScheduler sch, final Value u, final Statement e) {

        final Value other = state.getOtherVertex(u, e);
        
        final VS selfState = state.getState(u);
        
        final VS otherState = state.getState(other);

        final Literal l = state.getLinkAttr(u, e);
        
        final double edgeLength;
        if (l != null) {
        	
        	if (log.isDebugEnabled())
        		log.debug(l);
        	
        	edgeLength = l.doubleValue();
        	
        } else {
        	
        	edgeLength = EDGE_LENGTH;
        	
        }
        	
        // new distance for the remote vertex.
        final double newDist = selfState.dist() + edgeLength; //EDGE_LENGTH;
        
        // last observed distance for the remote vertex.
        final double otherDist = otherState.dist();

        // Note: test first without lock.
        if (newDist < otherDist) {

            // Tested again inside VS while holding lock.
            if (otherState.scatter(u/* predecessor */, newDist)) {

                if (log.isDebugEnabled())
                    log.debug("u=" + u + " @ " + selfState.dist()
                            + ", scheduling: " + other + " with newDist="
                            + newDist);

                // Then add the remote vertex to the next frontier.
                sch.schedule(other);

            }

        }

    }

    @Override
    public boolean nextRound(final IGASContext<VS, ES, Integer> ctx) {

        return true;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * <dl>
     * <dt>1</dt>
     * <dd>The shortest distance from the initial frontier to the vertex.</dd>
     * </dl>
     */
    @Override
    public List<IBinder<SSSP.VS, SSSP.ES, Integer>> getBinderList() {

        final List<IBinder<SSSP.VS, SSSP.ES, Integer>> tmp = super
                .getBinderList();

        tmp.add(new BinderBase<SSSP.VS, SSSP.ES, Integer>() {

            @Override
            public int getIndex() {
                return Bindings.DISTANCE;
            }

            @Override
            public Value bind(final ValueFactory vf,
                    final IGASState<SSSP.VS, SSSP.ES, Integer> state,
                    final Value u) {

                return vf.createLiteral(state.getState(u).dist());

            }

        });

        tmp.add(new BinderBase<SSSP.VS, SSSP.ES, Integer>() {
            
            @Override
            public int getIndex() {
                return Bindings.PREDECESSOR;
            }
            
            @Override
            public Value bind(final ValueFactory vf,
                    final IGASState<SSSP.VS, SSSP.ES, Integer> state, final Value u) {

                return state.getState(u).predecessor.get();

            }

        });

        return tmp;

    }

    /**
     * Additional {@link IBindingExtractor.IBinder}s exposed by {@link SSSP}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Bindings extends BaseGASProgram.Bindings {
        
        /**
         * The shortest distance to the vertex.
         */
        int DISTANCE = 1;
        
        /**
         * The predecessor vertex on a shortest path.
         * 
         */
        int PREDECESSOR = 2;
        
    }

	@Override
	public void prunePaths(IGASContext<VS, ES, Integer> ctx,
			Value[] targetVertices) {
		
        if (ctx == null)
            throw new IllegalArgumentException();

        if (targetVertices == null)
            throw new IllegalArgumentException();
        
        final IGASState<SSSP.VS, SSSP.ES, Integer> gasState = ctx.getGASState();

        final Set<Value> retainSet = new HashSet<Value>();

        for (Value v : targetVertices) {

            if (!gasState.isVisited(v)) {

                // This target was not reachable.
                continue;

            }

            /*
             * Walk the precessors back to a starting vertex.
             */
            Value current = v;

            while (current != null) {

                retainSet.add(current);

                final SSSP.VS currentState = gasState.getState(current);

                final Value predecessor = currentState.predecessor();

                current = predecessor;

            }
            
        } // next target vertex.
        
        gasState.retainAll(retainSet);
		
	}

}
