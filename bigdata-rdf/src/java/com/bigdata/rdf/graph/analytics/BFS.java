package com.bigdata.rdf.graph.analytics;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.impl.GASRunner;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

/**
 * Breadth First Search (BFS) is an iterative graph traversal primitive. The
 * frontier is expanded iteratively until no new vertices are discovered. Each
 * visited vertex is marked with the round (origin ZERO) in which it was
 * visited. This is its distance from the initial frontier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("rawtypes")
public class BFS implements IGASProgram<BFS.VS, BFS.ES, Void> {

    static class VS {

        /**
         * <code>-1</code> until visited. When visited, set to the current round
         * in order to assign each vertex its traversal depth.
         */
        private int depth = -1;

        /**
         * The depth at which this vertex was first visited (origin ZERO) and
         * <code>-1</code> if the vertex has not been visited.
         */
        public int depth() {
            synchronized (this) {
                return depth;
            }
        }

        /**
         * Note: This marks the vertex at the current traversal depth.
         * <p>
         * Note: It is possible that the same vertex may be visited multiple
         * times in a given expansion (from one or more source vertices that all
         * target the same destination vertex).
         * 
         * @return <code>true</code> if the vertex was visited for the first
         *         time in this round and the calling thread is the thread that
         *         first visited the vertex (this helps to avoid multiple
         *         scheduling of a vertex).
         */
        public boolean visit(final int depth) {
            synchronized (this) {
                if (this.depth == -1) {
                    this.depth = depth;
                    return true;
                }
                return false;
            }
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

    private static final Factory<IV, BFS.VS> vertexStateFactory = new Factory<IV, BFS.VS>() {

        @Override
        public BFS.VS initialValue(final IV value) {

            return new VS();

        }

    };

    @Override
    public Factory<IV, BFS.VS> getVertexStateFactory() {

        return vertexStateFactory;

    }

    @Override
    public Factory<ISPO, BFS.ES> getEdgeStateFactory() {

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
     * Not used.
     */
    @Override
    public void init(IGASContext<BFS.VS, BFS.ES, Void> ctx, IV u) {
        ctx.getState(u).visit(0);
        
    }
    
    /**
     * Not used.
     */
    @Override
    public Void gather(IGASContext<BFS.VS, BFS.ES, Void> ctx, IV u, ISPO e) {
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
    public BFS.VS apply(final IGASContext<BFS.VS, BFS.ES, Void> ctx, final IV u, 
            final Void sum) {

        return null;
        
    }

    /**
     * Returns <code>true</code>.
     */
    @Override
    public boolean isChanged(IGASContext<VS, ES, Void> ctx, IV u) {

        return true;
        
    }

    /**
     * The remote vertex is scheduled for activation unless it has already been
     * visited.
     * <p>
     * Note: We are scattering to out-edges. Therefore, this vertex is
     * {@link ISPO#s()}. The remote vertex is {@link ISPO#o()}.
     */
    @Override
    public void scatter(final IGASContext<BFS.VS, BFS.ES, Void> ctx,
            final IV u, final ISPO e) {

        // remote vertex state.
        final VS otherState = ctx.getState(e.o());

        // visit.
        if (otherState.visit(ctx.round() + 1)) {

            /*
             * This is the first visit for the remote vertex. Add it to the
             * schedule for the next iteration.
             */

            ctx.schedule(e.o());

        }

    }

    /**
     * Performance testing harness.
     */
    public static void main(final String[] args) throws Exception {

        new GASRunner<BFS.VS, BFS.ES, Void>(args) {

            @Override
            protected IGASProgram<BFS.VS, BFS.ES, Void> newGASProgram() {

                return new BFS();

            }

        }.call();
        
    }

}
