/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 9, 2008
 */
package com.bigdata.rdf.load;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask;

/**
 * This is a utility class designed for concurrent load of recursively processed
 * files and directories.
 * <p>
 * Note: Distributed concurrent load may be realized using a pre-defined hash
 * function, e.g., of the file name, modulo the #of hosts on which the loader
 * will run in order to have each file processed by one out of N hosts in a
 * cluster. There are two basic ways to go about this:
 * <ol>
 * 
 * <li> First, each host has a directory containing the data to be loaded by a
 * client running on that host. In this case, the clients are started up and run
 * independently.</li>
 * 
 * <li> The data to be loaded are on Network Attached Storage (NAS) or in a
 * {@link BigdataFileSystem}. {@link #nclients} is set to the #of clients that
 * will be run, each assigned a distinct {@link #clientNum}, and N clients are
 * started, typically each on its own host. Those clients will then read from
 * the central source and each client will accept the file for processing iff
 * <code>hash(file) % nclients == clientNum</code>. This distributes the
 * processing load among the clients using the hash of the file name.</li>
 * 
 * </ol>
 * 
 * <p>
 * 
 * Note: if individual file load tasks fail, they will be automatically retried
 * up to {@link #maxtries} times. If only some tasks succeed then the caller
 * decide what to do about the data load. Your basic options are to rollback to
 * a known state before the data load, drop the target
 * {@link AbstractTripleStore}, or accept that you have a partial data load and
 * that you can live with that outcome (realistic for large data sets). The
 * worst problem that you might expect is that not all statements will be found
 * on all access paths.
 * 
 * <p>
 * Note: Closure is NOT maintained during the load operation. However, you can
 * perform a database at once closure afterwards if you are bulk loading some
 * dataset into an empty database.
 * 
 * @todo As an alternative to indexing the locally loaded data, experiment with
 *       converting {@link StatementBuffer}s to {@link IBuffer}s (using the
 *       distributed terms indices), and then write out the long[3] data into a
 *       raw file. Once the local data have been converted to long[]s we can
 *       sort them into total SPO order (by chunks if necessary) and build the
 *       scale-out SPO index. The same process could then be done for each of
 *       the other access paths (OSP, POS).
 * 
 * @todo refactor further and reconcile with map/reduce processsing, the
 *       {@link BigdataFileSystem}, etc.
 * 
 * @todo support a {@link BigdataFileSystem} as a source.
 * 
 * @todo support as a map/reduce job assigning files from a
 *       {@link BigdataFileSystem} to clients so the source is a queue of files
 *       assigned to that map task.
 * 
 * @todo reporting for tasks that fail after retry - perhaps on a file? Leave to
 *       the caller? (available from the {@link #failedQueue}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConcurrentDataLoader<T extends Runnable, F> {

    protected static final Logger log = Logger
            .getLogger(ConcurrentDataLoader.class);
    
    /**
     * True iff the {@link #log} level is WARN or less.
     */
    final protected boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();
    
    /**
     * Thread pool providing concurrent load services.
     */
    protected final ThreadPoolExecutor loadService;

    /**
     * Lock used to make decisions about the queue state and exit conditions atomic.
     */
    protected final ReentrantLock lock = new ReentrantLock();
    
    /**
     * Counters for aggregated {@link WorkflowTask} progress.
     */
    protected final WorkflowTaskCounters counters = new WorkflowTaskCounters();

    /**
     * Tasks on the {@link #errorQueue} will be retried.
     */
    protected final LinkedBlockingQueue<WorkflowTask<T, F>> errorQueue = new LinkedBlockingQueue<WorkflowTask<T, F>>(
    /* unbounded capacity */);

    /**
     * Tasks on the {@link #failedQueue} will not be retried.
     */
    protected final LinkedBlockingQueue<WorkflowTask<T, F>> failedQueue = new LinkedBlockingQueue<WorkflowTask<T, F>>(
    /* unbounded capacity */);

    /**
     * The maximum #of times an attempt will be made to load any given file.
     */
    protected final int maxtries;
    
    /**
     * The #of threads given to the ctor (the pool can be using a different
     * number of threads since that is partly dynamic).
     */
    protected final int nthreads;
    
    /**
     * The #of scanned files accepted tasked to this client for processing. When
     * there is more than one client, the total of {@link #taskedCount} across
     * all clients should sum to the value of {@link #nscanned} for any of those
     * clients ({@link #nscanned} should be the same for all clients).
     */
    protected final AtomicInteger taskedCount = new AtomicInteger(0);
    
    public int getTaskedCount() {
        
        return taskedCount.get();
        
    }
    
    protected final AbstractFederation fed;
    
    protected final ScheduledFuture loadServiceStatisticsFuture;
    
    /**
     * The amount of delay in milliseconds that will be imposed on the caller's
     * {@link Thread} when a {@link RejectedExecutionException} is thrown when
     * attempting to submit a task for execution. A delay is imposed in order to
     * give the tasks already in the queue time to progress before the caller
     * can try to (re-)submit a task for execution.
     */
    protected final long rejectedExecutionDelay;

    /**
     * Ctor for a concurrent data loader with a default queue capacity.
     * 
     * @param fed
     *            The federation.
     * @param nthreads
     *            The #of threads that will process load tasks in parallel.
     */
    public ConcurrentDataLoader(final IBigdataFederation fed, final int nthreads) {
        
        this(fed, nthreads, Math.max(100, nthreads * 2),
                250/* rejectedExecutionDelay(ms) */, 3/* maxtries */);
        
    }

    /**
     * Ctor for a concurrent data loader. When used in combination with the
     * asynchronous write API, an unbounded thread pool should be used to feed
     * the asynchronous write {@link BlockingBuffer}s.
     * 
     * @param fed
     *            The federation.
     * @param nthreads
     *            The #of threads that will process load tasks in parallel -or-
     *            {@link Integer#MAX_VALUE} to use an unbounded thread pool fed
     *            by a {@link SynchronousQueue}.
     * @param queueCapacity
     *            The capacity of the queue of jobs awaiting execution (ignored
     *            when using an unbounded thread pool).
     * @param rejectedExecutionDelay
     *            The delay in milliseconds between resubmits of a task when the
     *            queue of tasks awaiting execution is at capacity.
     * @param maxtries
     *            The maximum #of times an attempt will be made to load any
     *            given file.
     */
    public ConcurrentDataLoader(final IBigdataFederation fed,
            final int nthreads, final int queueCapacity,
            final long rejectedExecutionDelay, final int maxtries) {

        if (fed == null)
            throw new IllegalArgumentException();
        
        if (nthreads <= 0)
            throw new IllegalArgumentException();

        if (queueCapacity < 0)
            throw new IllegalArgumentException();

        if (rejectedExecutionDelay < 1)
            throw new IllegalArgumentException();

        if (maxtries < 1)
            throw new IllegalArgumentException();

        this.fed = (AbstractFederation)fed;
        
        this.nthreads = nthreads;
        
        this.rejectedExecutionDelay = rejectedExecutionDelay;
        
        this.maxtries = maxtries;
        
        /*
         * Setup the load service. We will run the tasks that read the data and
         * load it into the database on this service.
         */

        if (nthreads == Integer.MAX_VALUE) {
            /*
             * Unbounded thread pool using a synchronous queue.
             */
            loadService = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(new DaemonThreadFactory(getClass()
                            .getName()
                            + ".loadService"));
        } else {
            /*
             * Fixed capacity thread pool using caller's choice of queue.
             */
            final BlockingQueue<Runnable> queue;
            switch (queueCapacity) {
            case 0:
                queue = new SynchronousQueue<Runnable>();
                break;
            case Integer.MAX_VALUE:
                /*
                 * Note: NOT a good idea as the provider can run without blocking
                 * and fill the queue faster than the data can be loaded.
                 */
                queue = new LinkedBlockingQueue<Runnable>();
                break;
            default:
                /*
                 * Note: CDL handles RejectedExecutionExceptions and will retry
                 * the submission of the task. This allows us to use a bounded
                 * queue in combination with a bounded thread pool.
                 */
                queue = new LinkedBlockingQueue<Runnable>(queueCapacity);
                break;
            }
            loadService = new ThreadPoolExecutor(nthreads, nthreads,
                    Integer.MAX_VALUE, TimeUnit.NANOSECONDS, queue,
                    new DaemonThreadFactory(getClass().getName()
                            + ".loadService"));
        }
        
        /*
         * Setup reporting to the load balancer.
         */
        {
            final CounterSet tmp = getCounters();

            final ThreadPoolExecutorStatisticsTask loadServiceStatisticsTask = new ThreadPoolExecutorStatisticsTask(
                    "Load Service", loadService, counters);

            // setup sampling for the [loadService]
            loadServiceStatisticsFuture = this.fed.addScheduledTask(
                    loadServiceStatisticsTask, 0/* initialDelay */,
                    1000/* delay */, TimeUnit.MILLISECONDS);

            // add sampled counters to those reported by the client.
            tmp.makePath("Load Service").attach(
                    loadServiceStatisticsTask.getCounters());
        }
        
    }

    /**
     * Make sure the {@link #loadService} was shutdown and cancel the statistics
     * task if it's still running.
     */
    protected void finalize() throws Throwable {

        shutdownNow();

        loadServiceStatisticsFuture.cancel(true/* mayInterrupt */);
        
        super.finalize();

    }

    public void shutdown() {
        
        // make sure this thread pool is shutdown.
        loadService.shutdown();

        // report out the final CounterSet state.
        fed.reportCounters();

    }

    public void shutdownNow() {

        loadService.shutdownNow();
        
    }
    
    /**
     * Return the {@link CounterSet} to be reported to the
     * {@link ILoadBalancerService}. The caller is responsible for attaching
     * the counters to those reported by {@link JiniFederation#getCounterSet()}
     * <p>
     * Note: the CDL relies on some side-effects on the returned
     * {@link CounterSet} to establish the
     * {@link ThreadPoolExecutorStatisticsTask} and have it report its counters
     * via the client to the LBS.
     * 
     * @return The {@link CounterSet} for the {@link ConcurrentDataLoader}.
     */
    synchronized 
    public CounterSet getCounters() {

        if (counterSet == null) {

            counterSet = new CounterSet();
            
            counterSet.addCounter("#threads",
                    new OneShotInstrument<Integer>(nthreads));

            {

                CounterSet tmp = counterSet.makePath("Load Service");
                
                tmp.addCounter("#tasked", new Instrument<Long>() {

                    @Override
                    protected void sample() {

                        setValue((long) taskedCount.get());

                    }
                });
                tmp.addCounter("taskRejectCount", new Instrument<Long>() {

                    @Override
                    protected void sample() {

                        setValue((long) counters.taskRejectCount.get());

                    }
                });
                tmp.addCounter("taskRetryCount", new Instrument<Long>() {

                    @Override
                    protected void sample() {

                        setValue((long) counters.taskRetryCount.get());

                    }
                });
                tmp.addCounter("taskCancelCount", new Instrument<Long>() {

                    @Override
                    protected void sample() {

                        setValue((long) counters.taskCancelCount.get());

                    }
                });
                tmp.addCounter("taskFatalCount", new Instrument<Long>() {

                    @Override
                    protected void sample() {

                        setValue((long) counters.taskFatalCount.get());

                    }
                });
                
            }

        }
        
        return counterSet;
        
    }
    private CounterSet counterSet;
    
    /**
     * If there is a task on the {@link #errorQueue} then re-submit it.
     * 
     * @return <code>true</code> if an error task was re-submitted.
     * 
     * @throws InterruptedException
     */
    protected boolean consumeErrorTask() throws InterruptedException {

        // Note: remove head of queue iff present (does not block).
        final WorkflowTask<T, F> errorTask = errorQueue.poll();

        if (errorTask != null) {

            if (log.isInfoEnabled())
                log.info("Re-submitting task=" + errorTask.target);

            // re-submit a task that produced an error.
            try {

                new WorkflowTask<T, F>(errorTask).submit();

                counters.taskRetryCount.incrementAndGet();

                return true;

            } catch (RejectedExecutionException ex) {

                /*
                 * Ignore. Another attempt will be made to re-submit the error
                 * task at another time. Eventually the queue will be short
                 * enough that the task can be submitted for execution.
                 */

                // pause a bit so that the running tasks can progress.
                Thread.sleep(rejectedExecutionDelay/* ms */);

                return false;

            }

        }

        return false;
 
    }
    
    /**
     * Wait for the {@link #loadService} to be "complete" AND for the
     * {@link #futuresQueue} to be empty.
     * <P>
     * Note: We have to hack what it means for the {@link #loadService} to be
     * complete. We can not rely on
     * {@link ExecutorService#awaitTermination(long, TimeUnit)} to wait until
     * all tasks are done because the {@link FuturesTask} will re-submit tasks
     * that fail up to {@link #maxtries} times.
     * <P>
     * Note: The {@link #lock} is used to make the test atomic with respect to
     * the {@link #lock} and the activity of the {@link WorkflowTask}
     * (especially with respect to the take()/put() behavior used to re-submit
     * failed tasks).
     * 
     * @throws InterruptedException
     * 
     * @todo There is no way to make the test atomic with respect to the
     *       <code>loadService.getActiveCount()</code> and
     *       <code>loadService.getQueue().isEmpty()</code> so we wait until
     *       the test succeeds a few times in a row.
     * 
     * @todo add a signal in place of this timeout / polling stuff?
     */
    public boolean awaitCompletion(final long timeout, final TimeUnit unit)
            throws InterruptedException {

        if (log.isInfoEnabled())
            log.info(counters.toString());
        
        final long beginWait = System.currentTimeMillis();
        
        long lastNoticeMillis = beginWait;
        
        while (true) {

            lock.lockInterruptibly();
            try {

                final int loadActiveCount = loadService.getActiveCount();
                final int loadQueueSize = loadService.getQueue().size();
                final int errorQueueSize = errorQueue.size();
                final int failedQueueSize = failedQueue.size();

                final long now = System.currentTimeMillis();

                if (log.isDebugEnabled()) {

                    log.debug("Awaiting completion" //
                            + ": loadActiveCount=" + loadActiveCount //
                            + ", loadQueueSize=" + loadQueueSize //
                            + ", errorQueueSize=" + errorQueueSize//
                            + ", failedQueueSize=" + failedQueueSize//
                            + ", elapsedWait=" + (now - beginWait)//
                            + ", " + counters);

                }

                // consume any error tasks (re-submit them).
                if (consumeErrorTask())
                    continue;

                if (loadActiveCount == 0 && loadQueueSize == 0
                        && errorQueueSize == 0) {

                    if (log.isInfoEnabled())
                        log.info("complete");

                    return true;

                }

                {

                    final long elapsed = System.currentTimeMillis() - beginWait;

                    if (TimeUnit.NANOSECONDS.convert(elapsed, unit) > timeout) {

                        if (WARN)
                            log.warn("timeout");

                        return false;

                    }

                }

                if (log.isInfoEnabled()) {

                    final long elapsed = now - lastNoticeMillis;

                    if (elapsed > 5000) {

                        lastNoticeMillis = now;

                        log.info("Awaiting completion" //
                                + ": loadActiveCount=" + loadActiveCount //
                                + ", loadQueueSize=" + loadQueueSize //
                                + ", errorQueueSize=" + errorQueueSize//
                                + ", failedQueueSize=" + failedQueueSize//
                                + ", elapsedWait=" + (now - beginWait)//
                                + ", " + counters);

                    }

                }

                Thread.sleep(100/* ms */);

            } finally {

                lock.unlock();

            }

        } // while(true)

    } // awaitTermination

    /**
     * Submits a task to the {@link #loadService}. If the queue is full then
     * this will block until the task can be submitted.
     * <p>
     * Note: the URI form of the file filename for each individual file to be
     * loaded is taken as the baseURI for that file. This is standard practice.
     * 
     * @param resource
     *            The resource to be loader (a File or a URL, but NOT a
     *            directory).
     * @param taskFactory
     * 
     * @throws IOException
     *             if the resource identifies a directory rather than a plain
     *             file or a URL.
     * @throws InterruptedException
     *             if the caller is interrupted while waiting to submit the
     *             task.
     * @throws Exception
     *             if the task could not be created.
     * 
     * @todo watch signal for queue not full?
     */
    public Future submitTask(final String resource,
            final ITaskFactory<T> taskFactory) throws InterruptedException,
            Exception {
        
        if(log.isDebugEnabled()) log.debug("Processing: resource=" + resource);
        
        {
            
            final File tmp = new File(resource);
         
            if (tmp.isDirectory()) {
            
                throw new IOException(resource + " is a directory.");
            
            }
            
        }
        
        final T target = taskFactory.newTask(resource);
        
        // Note: reset every time we log a rejected exception message.
        long begin = System.currentTimeMillis();
        
        /*
         * If there is an error task then consume (re-submit) it now. This keeps
         * the errorQueue from building up while we are scanning the source
         * files.
         */
        try {
            consumeErrorTask();
        } catch(InterruptedException t) {
            throw t;
        } catch (Throwable t) {
            log.warn(this, t);
        }

        while (true) {
            
            try {

                // wrap as a WorkflowTask.
                final WorkflowTask<T, F> workflowTask = newWorkflowTask(
                        target, loadService, lock, errorQueue, failedQueue,
                        counters, maxtries);

                // submit the task, return its Future.
                final Future f = workflowTask.submit();
                
                // one more task submitted for execution.
                taskedCount.incrementAndGet();

                return f;
                
            } catch (RejectedExecutionException ex) {

                /*
                 * Note: This makes the submit of the original input for
                 * processing robust.
                 * 
                 * Note: The retry is already robust since the error queue is
                 * consumed by polling (a) when still reading the inputs; and
                 * (b) when awaiting completion.
                 * 
                 * @todo move the rejected ex handler inside of the workflow
                 * task but allow override of the retry policy?
                 * 
                 * Note: the rejected execution exists because we are capping
                 * the maximum size of the queue feeding the [loadService]. You
                 * need to keep this in mind when interpreting the average queue
                 * size for the [loadService].
                 */
                
                // the task could not be submitted.
                
                final long now = System.currentTimeMillis();
                
                final long elapsed = now - begin;
                
                if(elapsed > 5000) {
                
                    /*
                     * Only issue log statements every 5 seconds.
                     */
                
                    // reset 
                    begin = now;
                    
                    if(log.isInfoEnabled())
                    log.info("loadService queue full"//
                        + ": queueSize="+ loadService.getQueue().size()//
                        + ", poolSize=" + loadService.getPoolSize()//
                        + ", active="+ loadService.getActiveCount()//
                        + ", completed="+ loadService.getCompletedTaskCount()//
                        + ", "+counters
                        );
                
                }
                
                // resubmit task for execution after a delay
                Thread.sleep(rejectedExecutionDelay/* ms */);
                
            }

        }

    }
    
    /**
     * Hook for subclassing the {@link WorkflowTask}
     * 
     * @param <T>
     * @param <F>
     * @param target
     * @param service
     * @param lock
     * @param errorQueue
     * @param failedQueue
     * @param counters
     * @param maxtries
     * 
     * @return
     */
    protected WorkflowTask<T, F> newWorkflowTask(//
            T target, //
            ExecutorService service, //
            ReentrantLock lock,//
            Queue<WorkflowTask<T, F>> errorQueue,//
            Queue<WorkflowTask<T, F>> failedQueue,//
            WorkflowTaskCounters counters, //
            int maxtries//
    ) {

        return new WorkflowTask<T, F>(target, service, lock, errorQueue,
                failedQueue, counters, maxtries);

    }

}
