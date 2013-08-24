package com.bigdata.rdf.graph.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.util.concurrent.LatchedExecutor;

/**
 * {@link IGASEngine} for dynamic activation of vertices. This implementation
 * maintains a frontier and lazily initializes the vertex state when the vertex
 * is visited for the first time. This is appropriate for algorithms, such as
 * BFS, that use a dynamic frontier.
 * 
 * TODO Algorithms that need to visit all vertices in each round (CC, BC, PR)
 * can be more optimially executed by a different implementation strategy. The
 * vertex state should be arranged in a dense map (maybe an array) and presized.
 * For example, this could be done on the first pass when we identify a vertex
 * index for each distinct V in visitation order.
 * 
 * TODO Vectored expansion with conditional materialization of attribute values
 * could be achieved using CONSTRUCT. This would force URI materialization as
 * well. If we drop down one level, then we can push in the frontier and avoid
 * the materialization. Or we can just write an operator that accepts a frontier
 * and returns the new frontier and which maintains an internal map containing
 * both the visited vertices, the vertex state, and the edge state.
 * 
 * TODO Some computations could be maintained and accelerated. A great example
 * is Shortest Path (as per RDF3X). Reachability queries for a hierarchy can
 * also be maintained and accelerated (again, RDF3X using a ferrari index).
 * 
 * TODO Option to materialize Literals (or to declare the set of literals of
 * interest) [Note: We can also require that people inline all URIs and Literals
 * if they need to have them materialized, but a materialization filter for
 * Gather and Scatter would be nice if it can be selective for just those
 * attributes or vertex identifiers that matter).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("rawtypes")
public class GASEngine implements IGASEngine {

//    private static final Logger log = Logger.getLogger(GASEngine.class);

    /**
     * The {@link IIndexManager} is used to access the graph.
     */
    private final IIndexManager indexManager;

    /**
     * The {@link ExecutorService} used to parallelize tasks.
     */
    private final ExecutorService executorService;
    /**
     * The parallelism for the SCATTER and GATHER phases.
     */
    private final int nthreads;

    /**
     * The parallelism for the SCATTER and GATHER phases.
     */
    public int getNThreads() {
        
        return nthreads;

    }
    
    /**
     * 
     * @param indexManager
     *            The index manager.
     * @param nthreads
     *            The number of threads to use for the SCATTER and GATHER
     *            phases.
     * 
     *            TODO Scale-out: The {@link IIndexmanager} MAY be an
     *            {@link IBigdataFederation}. The {@link GASEngine} would
     *            automatically use remote indices. However, for proper
     *            scale-out we want to partition the work and the VS/ES so that
     *            would imply a different {@link IGASEngine} design.
     */
    public GASEngine(final IIndexManager indexManager, final int nthreads) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (nthreads <= 0)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;
        
        this.nthreads = nthreads;
        
        this.executorService = indexManager.getExecutorService();
        
    }

    /**
     * {@inheritDoc}
     * 
     * FIXME Dynamic graphs: Allowing {@link ITx#READ_COMMITTED} to be specified
     * for the timestamp this class provides some support for dynamic graphs,
     * but for some use cases we would want to synchronize things such the
     * iteration is performed (or re-converged) with each commit point or to
     * replay a series of commit points (either through the commit record index
     * or through the history index).
     * <p>
     * Note: READ_COMMITTED is NOT a good idea. It will use the wrong kind of
     * index object (ReadCommittedView, which has a nasty synchronization hot
     * spot).
     */
    @Override
    public <VS, ES, ST> IGASContext<VS, ES, ST> newGASContext(
            final String namespace, final long timestamp,
            final IGASProgram<VS, ES, ST> program) {

        return new GASContext<VS, ES, ST>(this/* GASEngine */, namespace,
                timestamp, program);

    }

    /*
     * TODO If we use our own thread pool, then we need to shut it down here. We
     * also need to terminate each IGASContext.
     */
    
    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void shutdownNow() {
        // TODO Auto-generated method stub
        
    }

    /**
     * Return a view of the specified graph (aka KB) as of the specified
     * timestamp.
     * 
     * @param namespace
     *            The namespace of the graph.
     * @param timestamp
     *            The timestamp of the view.
     * @return The graph.
     * 
     * @throws RuntimeException
     *             if the graph could not be resolved.
     */
    protected AbstractTripleStore getKB(final String namespace, long timestamp) {

        if (timestamp == ITx.READ_COMMITTED) {

            /**
             * Note: This code is request the view as of the the last commit
             * time. If we use ITx.READ_COMMITTED here then it will cause the
             * Journal to provide us with a ReadCommittedIndex and that has a
             * synchronization hot spot!
             */

            timestamp = indexManager.getLastCommitTime();

        }

        final AbstractTripleStore kb = (AbstractTripleStore) indexManager
                .getResourceLocator().locate(namespace, timestamp);

        if (kb == null) {

            throw new RuntimeException("Not found: namespace=" + namespace
                    + ", timestamp=" + TimestampUtility.toString(timestamp));

        }

        return kb;

    }

    /**
     * Factory for the parallelism strategy that is used to map a task across
     * the frontier.
     * 
     * @param taskFactory
     *            The task to be mapped across the frontier.
     * 
     * @return The strategy that will map that task across the frontier.
     */
    Callable<Long> newFrontierStrategy(
            final VertexTaskFactory<Long> taskFactory, final IStaticFrontier f) {

        if (nthreads == 1)
            return new RunInCallersThreadFrontierStrategy(taskFactory, f);

        return new LatchedExecutorFrontierStrategy(taskFactory,
                executorService, nthreads, f);

    }

    /**
     * Abstract base class for a strategy that will map a task across the
     * frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private abstract class AbstractFrontierStrategy implements Callable<Long> {

        final protected VertexTaskFactory<Long> taskFactory;

        AbstractFrontierStrategy(final VertexTaskFactory<Long> taskFactory) {

            this.taskFactory = taskFactory;

        }

    }

    /**
     * Stategy uses the callers thread to map the task across the frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class RunInCallersThreadFrontierStrategy extends
            AbstractFrontierStrategy {

        private final IStaticFrontier f;

        RunInCallersThreadFrontierStrategy(
                final VertexTaskFactory<Long> taskFactory,
                final IStaticFrontier f) {

            super(taskFactory);

            this.f = f;

        }

        public Long call() throws Exception {

            long nedges = 0L;

            // For all vertices in the frontier.
            for (IV u : f) {

                nedges += taskFactory.newVertexTask(u).call();

            }

            return nedges;

        }

    }


    /**
     * Stategy uses the callers thread to map the task across the frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class LatchedExecutorFrontierStrategy extends
            AbstractFrontierStrategy {

        private final ExecutorService executorService;
        private final int nparallel;
        private final IStaticFrontier f;

        LatchedExecutorFrontierStrategy(
                final VertexTaskFactory<Long> taskFactory,
                final ExecutorService executorService, final int nparallel,
                final IStaticFrontier f) {

            super(taskFactory);

            this.executorService = executorService;

            this.nparallel = nparallel;

            this.f = f;

        }

        @Override
        public Long call() throws Exception {

            final List<FutureTask<Long>> tasks = new ArrayList<FutureTask<Long>>(
                    f.size());

            long nedges = 0L;

            final LatchedExecutor e = new LatchedExecutor(executorService,
                    nparallel);

            try {

                // For all vertices in the frontier.
                for (IV u : f) {

                    // Future will compute scatter for vertex.
                    final FutureTask<Long> ft = new FutureTask<Long>(
                            taskFactory.newVertexTask(u));

                    // Add to set of created futures.
                    tasks.add(ft);

                    // Enqueue future for execution.
                    e.execute(ft);

                }

                // Await/check futures.
                for (FutureTask<Long> ft : tasks) {

                    nedges += ft.get();

                }

            } finally {

                // Ensure any error cancels all futures.
                for (FutureTask<Long> ft : tasks) {

                    if (ft != null) {

                        // Cancel Future iff created (ArrayList has nulls).
                        ft.cancel(true/* mayInterruptIfRunning */);

                    }

                }

            }

            return nedges;

        }

    }

} // GASEngine
