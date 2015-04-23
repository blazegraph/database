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
package com.bigdata.rdf.graph.impl;

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.frontier.StaticFrontier2;
import com.bigdata.rdf.graph.impl.scheduler.CHMScheduler;

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
abstract public class GASEngine implements IGASEngine {

//    private static final Logger log = Logger.getLogger(GASEngine.class);

    /**
     * The {@link ExecutorService} used to parallelize tasks (iff
     * {@link #nthreads} GT ONE).
     */
    private final ExecutorService executorService;
    
    /**
     * The parallelism for the SCATTER and GATHER phases.
     */
    private final int nthreads;

    /**
     * The factory for the {@link IGASScheduler}.
     */
    private final AtomicReference<Class<IGASSchedulerImpl>> schedulerClassRef;

    @Override
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
     */
    @SuppressWarnings("unchecked")
    public GASEngine(final int nthreads) {

        if (nthreads <= 0)
            throw new IllegalArgumentException();

        this.nthreads = nthreads;

        this.executorService = nthreads == 0 ? null : Executors
                .newFixedThreadPool(nthreads, new DaemonThreadFactory(
                        GASEngine.class.getSimpleName()));

        this.schedulerClassRef = new AtomicReference<Class<IGASSchedulerImpl>>();
        
        this.schedulerClassRef.set((Class) CHMScheduler.class);

    }

    @Override
    public void shutdown() {

        if (executorService != null) {

            executorService.shutdown();
            
        }
        
    }

    @Override
    public void shutdownNow() {

        if (executorService != null) {
        
            executorService.shutdownNow();
            
        }
        
    }

    /**
     * Factory for the parallelism strategy that is used to map a task across
     * the frontier. The returned {@link Callable} should be executed in the
     * caller's thread. The {@link Callable} will schedule tasks that consume
     * the frontier. A variety of frontier strategies are implemented. Those
     * that execute in parallel do so using the thread pool associated with the
     * {@link IGASEngine}.
     * 
     * @param taskFactory
     *            The task to be mapped across the frontier.
     * 
     * @return The strategy that will map that task across the frontier.
     */
    protected Callable<Long> newFrontierStrategy(
            final VertexTaskFactory<Long> taskFactory, final IStaticFrontier f) {

        if (nthreads == 1)
            return new RunInCallersThreadFrontierStrategy(taskFactory, f);

        return new ParallelFrontierStrategy(taskFactory, f);

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

        @Override
        public Long call() throws Exception {

            long nedges = 0L;

            // For all vertices in the frontier.
            for (Value u : f) {

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
    private class ParallelFrontierStrategy extends AbstractFrontierStrategy {

        private final IStaticFrontier f;
        
        /**
         * Used to eliminate duplicates from the frontier at the time that we
         * schedule the tasks. This shifts the duplicate elimination burden from
         * the period between the rounds into the execution of the round. Since
         * we schedule the tasks that consume the frontier from a single thread,
         * we do not need to use a thread-safe collection. We also do not care
         * about order for this collection. It is only there to filter out
         * duplicate work.
         */
        private final HashSet<Value> scheduled;

        ParallelFrontierStrategy(final VertexTaskFactory<Long> taskFactory,
                final IStaticFrontier f) {

            super(taskFactory);

            this.f = f;

            /*
             * Pre-allocate based on the known size of the frontier. The
             * frontier may have duplicates (depending on its implementation) so
             * this size is conservative.
             * 
             * If the frontier is known to be compact, then this map is not
             * initialized and is not used.
             */
            this.scheduled = f.isCompact() ? null : new HashSet<Value>(f.size());

        }

        @Override
        public Long call() throws Exception {

            /*
             * Note: This places the tasks onto the queue for the executor
             * service in the caller's thread. Tasks begin executing as soon as
             * they are submitted. This allows the threads that will consume the
             * frontier to get started before all of the tasks have been
             * created.
             * 
             * TODO This does not check the futures until all tasks have been
             * created. It would be nicer if we had a queue model going with one
             * queue to submit the tasks and another to drain them. This would
             * require either non-blocking operations in a single thread or two
             * threads.
             */
            final List<FutureTask<Long>> tasks = new LinkedList<FutureTask<Long>>();

            long nedges = 0L;
            
            try {

                // For all vertices in the frontier.
                for (Value u : f) {

                    if (scheduled != null) {

// null values are NOT expected. 
//                        
//                        if (u == null) {
//                            
//                            // null references are ignored for non-compact
//                            // frontiers.
//                            continue;
//                        }

                        if (!scheduled.add(u)) {

                            // already scheduled.
                            continue;

                        }
                    }

                    // Future will compute scatter for vertex.
                    final FutureTask<Long> ft = new FutureTask<Long>(
                            taskFactory.newVertexTask(u));

                    // Add to set of created futures.
                    tasks.add(ft);

                    // Enqueue future for execution.
                    executorService.execute(ft);

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

    /**
     * If there is an {@link ExecutorService} for the {@link GASEngine}, then
     * return it (nthreads GT 1).
     * 
     * @throws UnsupportedOperationException
     *             if nthreads==1.
     */
    public ExecutorService getGASThreadPool() {

        if (executorService == null)
            throw new UnsupportedOperationException();
        
        return executorService;
        
    }

    public void setSchedulerClass(final Class<IGASSchedulerImpl> newValue) {

        if(newValue == null)
            throw new IllegalArgumentException();
        
        schedulerClassRef.set(newValue);
        
    }
    
    public Class<IGASSchedulerImpl> getSchedulerClass() {

        return schedulerClassRef.get();
        
    }
    
    /**
     * Return an {@link IStaticFrontier} - this is the object that models the
     * frontier that is consumed during a given round of evaluation.
     * 
     * TODO Config overrides
     */
    public IStaticFrontier newStaticFrontier() {
    
        return new StaticFrontier2();
        
    }
    
    public IGASSchedulerImpl newScheduler() {

        final Class<IGASSchedulerImpl> cls = schedulerClassRef.get();

        try {

            final Constructor<IGASSchedulerImpl> ctor = cls
                    .getConstructor(new Class[] { GASEngine.class });

            final IGASSchedulerImpl sch = ctor
                    .newInstance(new Object[] { this });

            return sch;

        } catch (Exception e) {

            throw new RuntimeException(e);

        }

    }

    @Override
    public <VS, ES, ST> IGASContext<VS, ES, ST> newGASContext(
            final IGraphAccessor graphAccessor,
            final IGASProgram<VS, ES, ST> gasProgram) {

        final IGASState<VS, ES, ST> gasState = newGASState(graphAccessor,
                gasProgram);

        return new GASContext<VS, ES, ST>(this/* GASEngine */, graphAccessor,
                gasState, gasProgram);

    }

    public <VS, ES, ST> IGASState<VS, ES, ST> newGASState(
            final IGraphAccessor graphAccessor,
            final IGASProgram<VS, ES, ST> gasProgram) {

        final IStaticFrontier frontier = newStaticFrontier();

        final IGASSchedulerImpl gasScheduler = newScheduler();

        return new GASState<VS, ES, ST>(this, graphAccessor, frontier,
                gasScheduler, gasProgram);

    }

} // GASEngine
