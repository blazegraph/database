package com.bigdata.rdf.graph.impl;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.GASUtil;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGASStats;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.IStaticFrontier;

public class GASContext<VS, ES, ST> implements IGASContext<VS, ES, ST> {

    private static final Logger log = Logger.getLogger(GASContext.class);

    private final GASEngine gasEngine;

    /**
     * Used to access the graph (a KB instance).
     */
    private final IGraphAccessor graphAccessor;

    /**
     * This {@link IGASState}.
     */
    private final IGASState<VS, ES, ST> gasState;

    /**
     * The graph analytic to be executed.
     */
    private final IGASProgram<VS, ES, ST> program;

    /**
     * 
     * @param namespace
     *            The namespace of the graph (KB instance).
     * @param timestamp
     *            The timestamp of the graph view (this should be a read-only
     *            view for non-blocking index reads).
     * @param gasProgram
     *            The program to execute against that graph.
     */
    public GASContext(final GASEngine gasEngine,
            final IGraphAccessor graphAccessor,
            final IGASState<VS, ES, ST> gasState,
            final IGASProgram<VS, ES, ST> gasProgram) {

        if (gasEngine == null)
            throw new IllegalArgumentException();

        if (graphAccessor == null)
            throw new IllegalArgumentException();
        
        if (gasState == null)
            throw new IllegalArgumentException();

        if (gasProgram == null)
            throw new IllegalArgumentException();

        this.gasEngine = gasEngine;

        this.graphAccessor = graphAccessor;

        this.program = gasProgram;

        this.gasState = gasState;

    }

    @Override
    public IGASState<VS, ES, ST> getGASState() {
        return gasState;
    }

    @Override
    public IGASProgram<VS, ES, ST> getGASProgram() {
        return program;
    }

    @Override
    public IGASStats call() throws Exception {

        final GASStats total = new GASStats();

        while (!gasState.frontier().isEmpty()) {

            final GASStats roundStats = new GASStats();

            doRound(roundStats);

            total.add(roundStats);

        }

        if (log.isInfoEnabled())
            log.info("Done: " + total);

        gasState.traceState();

        // Done
        return total;

    }

    /**
     * {@inheritDoc}
     * 
     * TODO This is an Asynchronous implementation. Further, it does not attempt
     * to race ahead to accelerate convergence (unlike an asynchronous neural
     * network).
     * 
     * TODO There should be an option for property value access during the APPLY
     * (either no property values are required, or some (or all) are required
     * and must optionally be materialized. Likewise, there could be an option
     * to force the materialization of the URIs for the (s,p,o).
     * <p>
     * Property value access is on the SPO index. If we are doing a reverse
     * gather (out-edges) then it will be right there and the Apply should be
     * pushed into the Gather. If we are doing a forward gather (in-edges), then
     * we are reading on OSP and will need to do a separate read on SPO.
     */
    @Override
    public boolean doRound(final IGASStats stats) throws InterruptedException,
            ExecutionException, Exception {

        // The fontier for this round.
        final IStaticFrontier f = gasState.frontier();

        // Conditionally log the computation state.
        gasState.traceState();

        /*
         * TODO This logic allows us to push down the APPLY into the GATHER or
         * SCATTER depending on some characteristics of the algorithm. Is this
         * worth while?
         * 
         * TODO The ability to pushd down the APPLY for AllEdges for the GATHER
         * depends on our using the union of the in-edges and out-edges
         * iterators to visit those edges. That union means that we do not have
         * to preserve the accumulant across the in-edges and out-edges aspects
         * of the GATHER. If this UNION over the iterators causes problems with
         * other optimizations, then it could be discarded. Note that this is
         * not an issue for the SCATTER since we can scatter over the in-edges
         * and out-edges for any given vertex independently (so long as the
         * APPLY is done before the SCATTER - this would not work if we pushed
         * down the APPLY into the SCATTER).
         */
        final EdgesEnum gatherEdges = program.getGatherEdges();
        final EdgesEnum scatterEdges = program.getScatterEdges();
        final boolean pushDownApplyInGather;
        final boolean pushDownApplyInScatter;
        final boolean runApplyStage;

        if (scatterEdges == EdgesEnum.NoEdges) {
            // Do APPLY() in GATHER.
            pushDownApplyInGather = true;
            pushDownApplyInScatter = false;
            runApplyStage = false;
        } else if (gatherEdges == EdgesEnum.NoEdges) {
            // APPLY() in SCATTER.
            pushDownApplyInGather = false;
            pushDownApplyInScatter = true;
            runApplyStage = false;
        } else {
            /*
             * Do not push down the APPLY.
             * 
             * TODO We could still push down the apply into the GATHER if we are
             * doing both stages.
             */
            pushDownApplyInGather = false;
            pushDownApplyInScatter = false;
            runApplyStage = true;
        }

        /*
         * GATHER
         */

        final long beginGather = System.nanoTime();

        final long gatherEdgeCount;
        if (gatherEdges == EdgesEnum.NoEdges) {

            gatherEdgeCount = 0L;

        } else {

            gatherEdgeCount = gatherEdges(graphAccessor, f, gatherEdges,
                    pushDownApplyInGather);

        }

        final long elapsedGather = System.nanoTime() - beginGather;

        /*
         * APPLY
         */

        final long elapsedApply;

        if (runApplyStage) {

            final long beginApply = System.nanoTime();

            apply(f);

            elapsedApply = System.nanoTime() - beginApply;

        } else {

            elapsedApply = 0L;

        }

        /*
         * SCATTER
         */

        final long beginScatter = System.nanoTime();

        final long scatterEdgeCount;

        if (scatterEdges == EdgesEnum.NoEdges) {

            scatterEdgeCount = 0L;

        } else {

            scatterEdgeCount = scatterEdges(graphAccessor, f,
                    gasState.getScheduler(), scatterEdges,
                    pushDownApplyInScatter);

        }

        final long elapsedScatter = System.nanoTime() - beginScatter;

        /*
         * Reporting.
         */

        final long totalElapsed = elapsedGather + elapsedApply + elapsedScatter;

        final long totalEdges = scatterEdgeCount + gatherEdgeCount;

        stats.add(f.size(), totalEdges, totalElapsed);

        if (log.isInfoEnabled()) {

            log.info("\ntotal"//
                    + ": fontierSize="
                    + f.size() //
                    + ", ms="
                    + TimeUnit.NANOSECONDS.toMillis(totalElapsed)//
                    + ", edges="
                    + totalEdges//
                    + ", teps="
                    + GASUtil.getTEPS(totalEdges, totalElapsed)//
                    + "\ngather"//
                    + ": ms="
                    + TimeUnit.NANOSECONDS.toMillis(elapsedGather)//
                    + ", nedges="
                    + gatherEdgeCount//
                    + ", fanIn="
                    + GASUtil.fanOut(f.size(), gatherEdgeCount)//
                    + ", teps="
                    + GASUtil.getTEPS(gatherEdgeCount, elapsedGather) //
                    + (runApplyStage ? ", apply="
                            + TimeUnit.NANOSECONDS.toMillis(elapsedApply) : "")//
                    + "\nscatter"//
                    + ": ms="
                    + TimeUnit.NANOSECONDS.toMillis(elapsedScatter)//
                    + ", nedges="
                    + scatterEdgeCount //
                    + ", fanOut="
                    + GASUtil.fanOut(f.size(), scatterEdgeCount) //
                    + ", teps="
                    + GASUtil.getTEPS(scatterEdgeCount, elapsedScatter)//
            );

        }

        // End the round, advance the counter, and compact new frontier.
        gasState.endRound();
        
        /*
         * Handshake with the GASProgram. If it votes to continue -OR- the new
         * frontier is not empty, then we will do another round.
         */

        final boolean nextRound = program.nextRound(this) || !gasState.frontier().isEmpty();

        if(nextRound) {

            /*
             * Optionally advance the view of the graph before the next round.
             */
            graphAccessor.advanceView();
            
        }
        
        return nextRound;
        
    } // doRound()

    /**
     * Do APPLY.
     * 
     * TODO The apply() should be parallelized. For some algorithms, there is a
     * moderate amount of work per vertex in apply(). Use {@link #nthreads} to
     * set the parallelism.
     * <p>
     * Note: This is very similar to the {@link IGASState#reduce(IReducer)}
     * operation. This operates over the frontier. reduce() operates over the
     * activated vertices. Both need fine grained parallelism. Both can have
     * either light or moderately heavy operations (a dot product would be an
     * example of a heavier operation).
     */
    private void apply(final IStaticFrontier f) {

        for (Value u : f) {

            program.apply(gasState, u, null/* sum */);

        }

    }

    /**
     * @param inEdges
     *            when <code>true</code> the GATHER is over the in-edges.
     *            Otherwise it is over the out-edges.
     * @param pushDownApply
     *            When <code>true</code>, the APPLY() will be done during the
     *            GATHER.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private long scatterEdges(final IGraphAccessor graphAccessor,
            final IStaticFrontier f, final IGASScheduler sch,
            final EdgesEnum scatterEdges, final boolean pushDownApply)
            throws InterruptedException, ExecutionException, Exception {

        if (scatterEdges == null)
            throw new IllegalArgumentException();

        class ScatterVertexTaskFactory implements VertexTaskFactory<Long> {

            public Callable<Long> newVertexTask(final Value u) {

                return new ScatterTask(u) {
                    @Override
                    protected boolean pushDownApply() {
                        return pushDownApply;
                    }

                    @Override
                    protected EdgesEnum getEdgesEnum() {
                        return scatterEdges;
                    }

                    @Override
                    protected IGASScheduler scheduler() {
                        return sch;
                    }

                    @Override
                    protected IGraphAccessor graphAccessor() {
                        return graphAccessor;
                    }
                };
            };
        }

        return gasEngine.newFrontierStrategy(new ScatterVertexTaskFactory(), f)
                .call();

    }

    /**
     * @param gatherEdges
     *            The edges to be gathered.
     * @param pushDownApply
     *            When <code>true</code>, the APPLY() will be done during the
     *            GATHER.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private long gatherEdges(final IGraphAccessor graphAccessor,
            final IStaticFrontier f, //final IScheduler sch,
            final EdgesEnum gatherEdges, final boolean pushDownApply)
            throws InterruptedException, ExecutionException, Exception {

        if (gatherEdges == null)
            throw new IllegalArgumentException();

        class GatherVertexTaskFactory implements VertexTaskFactory<Long> {

            public Callable<Long> newVertexTask(final Value u) {

                return new GatherTask(u) {
                    @Override
                    protected boolean pushDownApply() {
                        return pushDownApply;
                    }

                    @Override
                    protected EdgesEnum getEdgesEnum() {
                        return gatherEdges;
                    }

                    /**
                     * Note: The API does not permit vertices to be scheduled
                     * for execution during the GATHER phase.
                     */
                    @Override
                    protected IGASScheduler scheduler() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    protected IGraphAccessor graphAccessor() {
                        return graphAccessor;
                    }
                };
            };
        }

        return gasEngine.newFrontierStrategy(new GatherVertexTaskFactory(), f)
                .call();

    }

    /**
     * Base class for SCATTER or GATHER of edges for a vertex.
     * <p>
     * Note: An abstract task pattern is used to factor out parameters that are
     * constants within the scope of the scatter for each vertex in the
     * frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    abstract private class VertexEdgesTask implements Callable<Long> {

        protected final Value u;

        public VertexEdgesTask(final Value u) {

            this.u = u;

        }

        abstract protected IGraphAccessor graphAccessor();
        
        abstract protected boolean pushDownApply();

        abstract protected EdgesEnum getEdgesEnum();

        abstract protected IGASScheduler scheduler();

    }

    /**
     * Scatter for the edges of a single vertex.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    abstract private class ScatterTask extends VertexEdgesTask {

        public ScatterTask(final Value u) {

            super(u);

        }

        /**
         * Execute the scatter for the vertex.
         * 
         * @return The #of visited edges.
         */
        public Long call() throws Exception {

            final boolean TRACE = log.isTraceEnabled();

            if (pushDownApply()) {

                /*
                 * Run the APPLY as part of the SCATTER.
                 * 
                 * TODO This can be done on a thread pool or fork/join pool
                 * since we know that there are no duplicates in the frontier.
                 */

                program.apply(gasState, u, null/* sum */);

            }

            if (!program.isChanged(gasState, u)) {

                // Unchanged. Do not scatter.
                return 0L;

            }

            /*
             * Visit the (in|out)-edges of that vertex.
             */
            long nedges = 0L;

            final IGASScheduler sch = scheduler();

            final Iterator<Statement> eitr = graphAccessor.getEdges(
                    GASContext.this, u, getEdgesEnum());

            try {

                while (eitr.hasNext()) {

                    // edge
                    final Statement e = eitr.next();

                    nedges++;

                    if (TRACE) // TODO Batch resolve if @ TRACE
                        log.trace("e=" + gasState.toString(e));

                    program.scatter(gasState, sch, u, e);

                }

            } finally {

//                eitr.close();

            }

            return nedges;

        }

    } // ScatterTask

    /**
     * Gather for the edges of a single vertex.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    abstract private class GatherTask extends VertexEdgesTask {

        public GatherTask(final Value u) {

            super(u);

        }

        @Override
        public Long call() throws Exception {

            long nedges = 0;

            final Iterator<Statement> eitr = graphAccessor.getEdges(
                    GASContext.this, u, getEdgesEnum());

            try {

                /*
                 * Note: since (left,right) may be null, we need to known if
                 * left is defined.
                 */
                boolean first = true;

                ST left = null;

                while (eitr.hasNext()) {

                    final Statement e = eitr.next();

                    if (log.isTraceEnabled()) // TODO Batch resolve if @ TRACE
                        log.trace("u=" + u + ", e=" + gasState.toString(e) + ", sum="
                                + left);

                    final ST right = program.gather(gasState, u, e);

                    if (first) {

                        left = right;

                        first = false;

                    } else {

                        left = program.sum(left, right);

                    }

                }

                if (pushDownApply()) {

                    /*
                     * Run the APPLY as part of the GATHER.
                     * 
                     * TODO This can be done on a thread pool or fork/join pool
                     * since we know that there are no duplicates in the
                     * frontier.
                     */

                    program.apply(gasState, u, left/* sum */);

                }

            } finally {

//                eitr.close();

            }

            return nedges;

        }

    } // GatherTask

} // GASContext
