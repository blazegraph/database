package com.bigdata.rdf.graph.analytics;

import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.impl.BaseGASProgram;

import cutthecrap.utils.striterators.IStriterator;

/**
 * Breadth First Search (BFS) is an iterative graph traversal primitive. The
 * frontier is expanded iteratively until no new vertices are discovered. Each
 * visited vertex is marked with the round (origin ZERO) in which it was
 * visited. This is its distance from the initial frontier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BFS extends BaseGASProgram<BFS.VS, BFS.ES, Void> {

    static class VS {

        /**
         * <code>-1</code> until visited. When visited, set to the current round
         * in order to assign each vertex its traversal depth.
         * <p>
         * Note: It is possible that the same vertex may be visited multiple
         * times in a given expansion (from one or more source vertices that all
         * target the same destination vertex). However, in this case the same
         * value will be assigned by each visitor. Thus, synchronization is only
         * required for visibility of the update within the round. As long as
         * one thread reports that it modified the depth, the vertex will be
         * scheduled.
         */
        private final AtomicInteger depth = new AtomicInteger(-1);

        /**
         * The depth at which this vertex was first visited (origin ZERO) and
         * <code>-1</code> if the vertex has not been visited.
         */
        public int depth() {
//            synchronized (this) {
                return depth.get();
//            }
        }

        /**
         * Note: This marks the vertex at the current traversal depth.
         * 
         * @return <code>true</code> if the vertex was visited for the first
         *         time in this round and the calling thread is the thread that
         *         first visited the vertex (this helps to avoid multiple
         *         scheduling of a vertex).
         */
        public boolean visit(final int depth) {
            if (this.depth.compareAndSet(-1/* expect */, depth/* newValue */)) {
                // Scheduled by this thread.
                return true;
            }
            return false;
//            synchronized (this) {
//                if (this.depth == -1) {
//                    this.depth = depth;
//                    return true;
//                }
//                return false;
//            }
        }

        @Override
        public String toString() {
            return "{depth=" + depth() + "}";
        }

    }// class VS

    /**
     * Edge state is not used.
     */
    static class ES {

    }

    private static final Factory<Value, BFS.VS> vertexStateFactory = new Factory<Value, BFS.VS>() {

        @Override
        public BFS.VS initialValue(final Value value) {

            return new VS();

        }

    };

    @Override
    public Factory<Value, BFS.VS> getVertexStateFactory() {

        return vertexStateFactory;

    }

    @Override
    public Factory<Statement, BFS.ES> getEdgeStateFactory() {

        return null;

    }

    @Override
    public EdgesEnum getGatherEdges() {

        return EdgesEnum.NoEdges;

    }

    @Override
    public EdgesEnum getScatterEdges() {

        return EdgesEnum.OutEdges;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to only visit the edges of the graph.
     */
    @Override
    public IStriterator constrainFilter(
            final IGASContext<BFS.VS, BFS.ES, Void> ctx, final IStriterator itr) {

        return itr.addFilter(getEdgeOnlyFilter(ctx));

    }

    /**
     * Not used.
     */
    @Override
    public void init(final IGASState<BFS.VS, BFS.ES, Void> state, final Value u) {

        state.getState(u).visit(0);
        
    }
    
    /**
     * Not used.
     */
    @Override
    public Void gather(IGASState<BFS.VS, BFS.ES, Void> state, Value u, Statement e) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not used.
     */
    @Override
    public Void sum(Void left, Void right) {
        throw new UnsupportedOperationException();
    }

    /**
     * NOP
     */
    @Override
    public BFS.VS apply(final IGASState<BFS.VS, BFS.ES, Void> state, final Value u, 
            final Void sum) {

        return null;
        
    }

    /**
     * Returns <code>true</code>.
     */
    @Override
    public boolean isChanged(IGASState<VS, ES, Void> state, Value u) {

        return true;
        
    }

    /**
     * The remote vertex is scheduled for activation unless it has already been
     * visited.
     * <p>
     * Note: We are scattering to out-edges. Therefore, this vertex is
     * {@link Statement#getSubject()}. The remote vertex is
     * {@link Statement#getObject()}.
     */
    @Override
    public void scatter(final IGASState<BFS.VS, BFS.ES, Void> state,
            final IGASScheduler sch, final Value u, final Statement e) {

        // remote vertex state.
        final VS otherState = state.getState(e.getObject());

        // visit.
        if (otherState.visit(state.round() + 1)) {

            /*
             * This is the first visit for the remote vertex. Add it to the
             * schedule for the next iteration.
             */

            sch.schedule(e.getObject());

        }

    }

    @Override
    public boolean nextRound(IGASContext<BFS.VS, BFS.ES, Void> ctx) {

        return true;
        
    }

}
