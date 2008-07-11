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
package com.bigdata.rdf.store;

import java.beans.Statement;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.journal.QueueStatisticsTask;
import com.bigdata.journal.TaskCounters;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPOBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.repo.BigdataRepository;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.ClientException;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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
 * {@link BigdataRepository}. {@link #nclients} is set to the #of clients that
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
 * @todo consider naming each instance and having the instance place its
 *       counters under the client using that name as part of its namespace.
 *       This will allow clients to avoid replacing the counters from prior runs
 *       if they so choose.
 * 
 * @todo experiment with varying #clients and buffer capacity.
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
 *       {@link BigdataRepository}, etc.
 * 
 * @todo support a {@link BigdataRepository} as a source.
 * 
 * @todo support as a map/reduce job assigning files from a
 *       {@link BigdataRepository} to clients so the source is a queue of files
 *       assigned to that map task.
 * 
 * @todo reporting for tasks that fail after retry - perhaps on a file? Leave to
 *       the caller? (available from the {@link #failedQueue}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConcurrentDataLoader {

    protected static final Logger log = Logger
            .getLogger(ConcurrentDataLoader.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Thread pool provinding concurrent load services.
     */
    final ThreadPoolExecutor loadService;

    /**
     * Counters for aggregated {@link WorkflowTask} progress.
     */
    final WorkflowTaskCounters counters = new WorkflowTaskCounters();
    
    /**
     * Tasks on the {@link #successQueue} have completed successfully, but may
     * have been retried before they succeeded.
     */
    final LinkedBlockingQueue<WorkflowTask<ReaderTask, Object>> successQueue = new LinkedBlockingQueue<WorkflowTask<ReaderTask, Object>>(
    /* unbounded capacity */);

    /**
     * Tasks on the {@link #errorQueue} will be retried.
     */
    final LinkedBlockingQueue<WorkflowTask<ReaderTask, Object>> errorQueue = new LinkedBlockingQueue<WorkflowTask<ReaderTask, Object>>(
    /* unbounded capacity */);

    /**
     * Tasks on the {@link #failedQueue} will not be retried.
     */
    final LinkedBlockingQueue<WorkflowTask<ReaderTask, Object>> failedQueue = new LinkedBlockingQueue<WorkflowTask<ReaderTask, Object>>(
    /* unbounded capacity */);

    /**
     * The maximum #of times an attempt will be made to load any given file.
     * 
     * @todo ctor param.
     */
    final int maxtries = 3;
    
    /**
     * The #of threads given to the ctor (the pool can be using a different
     * number of threads since that is partly dynamic).
     */
    final int nthreads;
    
    /**
     * The #of clients when running as a distributed client (more than one
     * {@link ConcurrentDataLoader} instance, typically on more than one host).
     */
    final int nclients;
    
    /**
     * The identified assigned to this client in the half-open interval [0:{@link #nclients}).
     */
    final int clientNum;

    /**
     * The #of files scanned.
     * 
     * @deprecated this is only a little ahead of the {@link #loadService} and
     *             the more I isolate these things the less it belongs here. It
     *             should be with the logic to scan the file system (vs some
     *             other source).
     */
    final AtomicInteger nscanned = new AtomicInteger(0);
    
//    /**
//     * The elapsed time during which one or more tasks was running on this
//     * service.
//     */
//    public long getElapsedServiceTime() {
//        
//        final long elapsedThisRun;
//        
//        final long startTime = this.startTime;
//
//        if (startTime == 0L) {
//        
//            elapsedThisRun = 0L;
//            
//        } else {
//            
//            // still running.
//            elapsedThisRun = System.currentTimeMillis() - startTime;
//
//        }
//
//        return elapsedPriorRuns + elapsedThisRun;
//        
//    }
//    private long startTime;
//    private long elapsedPriorRuns;
    
    final AbstractFederation fed;
    
    final ScheduledFuture loadServiceStatisticsFuture;
    
    /**
     * The amount of delay in milliseconds that will be imposed on the caller's
     * {@link Thread} when a {@link RejectedExecutionException} is thrown when
     * attempting to submit a task for execution. A delay is imposed in order to
     * give the tasks already in the queue time to progress before the caller
     * can try to (re-)submit a task for execution.
     */
    protected final long REJECTED_EXECUTION_DELAY = 250L;

    /**
     * Create and run a concurrent data load operation.
     * 
     * @param client
     *            ...
     * @param nthreads
     *            The #of concurrent loaders.
     * @param bufferCapacity
     *            The capacity of the {@link StatementBuffer} (the #of
     *            statements that are buffered into a batch operation).
     * @param baseURL
     *            The baseURL and <code>""</code> if none is required.
     * @param fallback
     *            An attempt will be made to determine the interchange syntax
     *            using {@link RDFFormat}. If no determination can be made then
     *            the loader will presume that the files are in the format
     *            specified by this parameter (if any). Files whose format can
     *            not be determined will be logged as errors.
     * @param nclients
     *            The #of client processes that will share the data load
     *            process. Each client process MUST be started independently in
     *            its own JVM. All clients MUST have access to the files to be
     *            loaded.
     * @param clientNum
     *            The client host identifier in [0:nclients-1]. The clients will
     *            load files where
     *            <code>filename.hashCode() % nclients == clientNum</code>.
     *            If there are N clients loading files using the same pathname
     *            to the data then this will divide the files more or less
     *            equally among the clients. (If the data to be loaded are
     *            pre-partitioned then you do not need to specify either
     *            <i>nclients</i> or <i>clientNum</i>.)
     * 
     * @todo the baseURL is ignored and probably should either be dropped or
     *       made part of the visitation pattern for something with richer
     *       metadata than a file system. right now it always uses the
     *       individual file to be loaded as the baseURL for that file.
     */
    public ConcurrentDataLoader(IBigdataClient client, int nthreads,
            int nclients, int clientNum) {

        this.fed = (AbstractFederation) client.getFederation();
        
        this.nthreads = nthreads;
        
        this.nclients = nclients;
        
        this.clientNum = clientNum;
        
        /*
         * Setup the load service. We will run the tasks that read the data and
         * load it into the database on this service.
         * 
         * Note: we limit the #of tasks waiting in the queue so that we don't
         * let the file scan get too far ahead of the executing tasks. This
         * reduces the latency for startup and the memory overhead significantly
         * when reading a large collection of files. There is a minimum queue
         * size so that we can be efficient for the file system reads.
         */

        // loadService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
        // nthreads, DaemonThreadFactory.defaultThreadFactory());
        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(
                Math.max(100, nthreads * 2));

        loadService = new ThreadPoolExecutor(nthreads, nthreads,
                Integer.MAX_VALUE, TimeUnit.NANOSECONDS, queue,
                DaemonThreadFactory.defaultThreadFactory());
        
        /*
         * Setup reporting to the load balancer.
         */
        final CounterSet tmp = getCounters(fed);

        final QueueStatisticsTask loadServiceStatisticsTask = new QueueStatisticsTask(
                "Load Service", loadService, counters);

        // setup sampling for the [loadService]
        loadServiceStatisticsFuture = fed.addScheduledStatisticsTask(
                loadServiceStatisticsTask, 0/* initialDelay */,
                1000/* delay */, TimeUnit.MILLISECONDS);

        // add sampled counters to those reported by the client.
        loadServiceStatisticsTask.addCounters(tmp
                .makePath("Load Service"));

    }

    /**
     * Cancel the statistics task if it's still running.
     */
    protected void finalize() throws Throwable {

        super.finalize();

        loadServiceStatisticsFuture.cancel(true/* mayInterrupt */);

    }
    
    public void shutdown() {
        
        // make sure this thread pool is shutdown.
        loadService.shutdown();

        // report out the final counter set state.
        fed.reportCounters();

    }

    public void shutdownNow() {

        loadService.shutdownNow();
        
    }
    
    /**
     * Setup the {@link CounterSet} to be reported to the
     * {@link ILoadBalancerService}.
     * <p>
     * Note: This add the counters to be reported to the client's counter set.
     * The added counters will be reported when the client reports its own
     * counters.
     * 
     * @param fed
     * 
     * @return The {@link CounterSet} for the {@link ConcurrentDataLoader}.
     */
    synchronized public CounterSet getCounters(IBigdataFederation fed) {

        if (ourCounterSet == null) {

            final String path = fed.getClientCounterPathPrefix()
                    + "Concurrent Data Loader";

            // make path to the counter set for the data loader.
            ourCounterSet = fed.getCounterSet().makePath(path);

            ourCounterSet.addCounter("#clients", new OneShotInstrument<Integer>(
                            nclients));

            ourCounterSet.addCounter("clientNum", new OneShotInstrument<Integer>(
                    clientNum));

            {

                CounterSet tmp = ourCounterSet.makePath("Load Service");
                
                tmp.addCounter("#scanned", new Instrument<Long>() {

                    @Override
                    protected void sample() {

                        setValue((long) nscanned.get());

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
        
        return ourCounterSet;
        
    }
    private CounterSet ourCounterSet;
    
    /**
     * If there is a task on the {@link #errorQueue} then re-submit it.
     * 
     * @return <code>true</code> if an error task was re-submitted.
     * 
     * @throws InterruptedException
     */
    public boolean consumeErrorTask() throws InterruptedException {

        // Note: remove head of queue iff present (does not block).
        final WorkflowTask<ReaderTask, Object> errorTask = errorQueue.poll();

        if (errorTask != null) {

            if(INFO)
            log.info("Re-submitting task="+errorTask.target);
            
            // re-submit a task that produced an error.
            try {

                new WorkflowTask<ReaderTask, Object>(errorTask).submit();

                counters.taskRetryCount.incrementAndGet();

                return true;
                
            } catch(RejectedExecutionException ex) {

                /*
                 * Ignore. The task will be re-tried again. Eventually the queue
                 * will be short enough that the task can be submitted for
                 * execution.
                 */
                
                // pause a bit so that the running tasks can progress.
                Thread.sleep(REJECTED_EXECUTION_DELAY/*ms*/);
                
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
     * Note: The {@link #futuresLock} is used to make the test atomic with
     * respect to the {@link #futuresLock} and the activity of the
     * {@link FuturesTask} (especially with respect to the take()/put() behavior
     * used to re-submit failed tasks).
     * <P>
     * Note: There is no way to make the test atomic with respect to the
     * <code>loadService.getActiveCount()</code> and
     * <code>loadService.getQueue().isEmpty()</code> so we wait until the test
     * succeeds a few times in a row.
     * 
     * @throws InterruptedException
     * 
     * FIXME This is not bullet proof. awaitCompletion() can return immediately
     * when the loadService is just starting up if it does its tests after the
     * load service does a take() on its queue and before the task has been
     * assigned to a worker thread. The same problem can arise near the end of a
     * run, in which case the last task might not execute.
     */
    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {

        log.info(counters.toString());
        
        final long beginWait = System.currentTimeMillis();
        
        long lastNoticeMillis = beginWait;
        
        while (true) {
            
            final int loadActiveCount = loadService.getActiveCount();
            final int loadQueueSize = loadService.getQueue().size();
            final int errorQueueSize = errorQueue.size();
            final int failedQueueSize = failedQueue.size();

            final long now = System.currentTimeMillis();

            if (DEBUG) {

                log.debug("Awaiting completion" //
                        + ": loadActiveCount=" + loadActiveCount //
                        + ", loadQueueSize=" + loadQueueSize //
                        + ", errorQueueSize=" + errorQueueSize//
                        + ", failedQueueSize=" + failedQueueSize//
                        + ", elapsedWait=" + (now - beginWait)//
                        + ", " + counters);
                
            }

            // consume any error tasks (re-submit them).
            if(consumeErrorTask()) continue;

            if (loadActiveCount == 0 && loadQueueSize == 0 && errorQueueSize == 0) {

                log.info("complete");

                return true;
                
            }

            {
                
                final long elapsed = System.currentTimeMillis() - beginWait;
                
                if(TimeUnit.NANOSECONDS.convert(elapsed, unit)>timeout) {
                
                    log.warn("timeout");

                    return false;
                    
                }
                
            }
            
            if (INFO) {

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

        }
        
    }
    
    /**
     * Scans file(s) recursively starting with the named file, creates a task
     * using the {@link ITaskFactory} for each file that passes the filter, and
     * submits the task.
     * 
     * @param file
     *            Either a plain file or directory containing files to be
     *            processed.
     * @param filter
     *            An optional filter.
     * @param taskFactory
     * 
     * @throws InterruptedException
     *             if the thread is interrupted while queuing tasks.
     */
    public void process(File file, FilenameFilter filter,
            ITaskFactory taskFactory) throws InterruptedException {

        if (file == null)
            throw new IllegalArgumentException();

        if (taskFactory == null)
            throw new IllegalArgumentException();

        process2(file, filter, taskFactory);

        /* Let some tasks at least get submitted.
         * 
         * FIXME This is a hack for awaitCompletion()
         */
        Thread.sleep(1000);

    }
    
    private void process2(File file, FilenameFilter filter,
            ITaskFactory taskFactory) throws InterruptedException {

        if (file.isDirectory()) {

            log.info("Scanning directory: " + file);

            final File[] files = filter == null ? file.listFiles() : file
                    .listFiles(filter);

            for (final File f : files) {

                process2(f, filter, taskFactory);

            }

        } else {

            /*
             * Processing a standard file.
             */

            log.info("Scanning file: " + file);

            nscanned.incrementAndGet();

            if (nclients > 1) {

                /*
                 * More than one client will run so we need to allocate the
                 * files fairly to each client.
                 * 
                 * FIXME This is done by the #of files scanned modulo the #of
                 * clients. When that expression evaluates to the [clientNum]
                 * then the file will be allocated to this client. (The problem
                 * with this approach is that it is sensitive to the order in
                 * which the files are visited in the file systems. The
                 * hash(filename) approach is much more robust as long as the
                 * same pathname is used, perhaps an absolute pathname. I need
                 * to review this in practice since I have seen what appeared to
                 * be a strong bias in favor of one client when scanning the
                 * U1000 dataset on server1 and server2.)
                 */ 
                 /* 
//                 * This trick allocates files to clients based on the hash of the
//                 * pathname module the #of clients. If that expression does not
//                 * evaluate to the assigned clientNum then the file will NOT be
//                 * loaded by this host.
                 */
                    
                if ((nscanned.get() /* file.getPath().hashCode() */% nclients == clientNum)) {

                    log.info("Client" + clientNum + " tasked: " + file);

                    submitTask(file.toString(), taskFactory );

                }   
            
            } else {
                
                /*
                 * Only one client so it loads all of the files.
                 */

                submitTask(file.toString(), taskFactory);

            }
                
        }

    }

    /**
     * Submits a task to the {@link #loadService}.
     * 
     * @param resource
     * 
     * @throws InterruptedException
     */
    public void submitTask(String resource,ITaskFactory taskFactory) throws InterruptedException {
        
        log.info("Processing: resource=" + resource);
        
        final Runnable target;
        try {

            target = taskFactory.newTask(resource);
        
        } catch(Exception ex) {
            
            log.warn("Could not start task: resource=" + resource + " : " + ex);
            
            return;
            
        }

        // Note: reset every time we log a rejected exception message.
        long begin = System.currentTimeMillis();
        
        while (true) {

//            final long submitCount = counters.taskSubmitCount.get();
//            
//            if (submitCount > 0 && submitCount < 20) {
//
//                // stagger the first few tasks.
//                Thread.sleep(50/* ms */);
//                
//            }
            
            try {

                // wrap as a WorkflowTask.
                final WorkflowTask workflowTask = new WorkflowTask(target,
                        loadService, successQueue, errorQueue, failedQueue,
                        counters, maxtries);

                // submit the task.
                workflowTask.submit();
                
                /*
                 * If there is an error task then consume (re-submit) it now.
                 * This keeps the errorQueue from building up while we are
                 * scanning the source files.
                 */
                consumeErrorTask();

                return;

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
                    
                    if(INFO)
                    log.info("loadService queue full"//
                        + ": queueSize="+ loadService.getQueue().size()//
                        + ", poolSize=" + loadService.getPoolSize()//
                        + ", active="+ loadService.getActiveCount()//
                        + ", completed="+ loadService.getCompletedTaskCount()//
                        + ", "+counters
                        );
                
                }
                
                // But retry the task every 1/4 second.
                
                Thread.sleep(REJECTED_EXECUTION_DELAY/*ms*/);
                
            }

        }

    }
    
    /**
     * A class designed to pass a task from queue to queue treating the queues
     * as workflow states. Tasks begin on the
     * {@link ConcurrentDataLoader#loadService} (which has its own queue of
     * submitted but not yet running tasks) and are moved onto either the
     * {@link ConcurrentDataLoader#successQueue} or the
     * {@link ConcurrentDataLoader#errorQueue} as appropriate. Tasks which can
     * be retried are re-submitted to the
     * {@link ConcurrentDataLoader#loadService} while tasks which can no longer
     * be retried are placed on the {@link ConcurrentDataLoader#failedQueue}.
     * 
     * @param T
     *            The type of the target (T) task.
     * @param F
     *            The return type of the task's {@link Future} (F).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class WorkflowTask<T extends Runnable,F> implements Runnable {
        
        protected static final Logger log = Logger
                .getLogger(WorkflowTask.class);

        /**
         * True iff the {@link #log} level is WARN or less.
         */
        final protected static boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
                .toInt();

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
                .toInt();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
                .toInt();

        /**
         * The {@link Future} for this {@link ReaderTask}.
         * <p>
         * Note: this field is set each time the {@link ReaderTask} is submitted
         * to the {@link ConcurrentDataLoader#loadService}. It is never
         * cleared, so it always reflects the last {@link Future}.
         */
        private Future<F> future;

        /**
         * The time when the task was first created and on retry set to the time
         * when the task was queued for retry (this may be used to identify
         * tasks that are not terminating).
         */
        final long beginTime;

        /**
         * The maximum #of times this task will be retried on error before
         * failing.
         */
        final int maxtries;
        
        /**
         * The #of tries so far (0 on the first try).
         */
        final int ntries;
        
        final T target;
        
        final ExecutorService service;
        
        final BlockingQueue<WorkflowTask<T, F>> successQueue;
        
        final BlockingQueue<WorkflowTask<T, F>> errorQueue;
        
        final BlockingQueue<WorkflowTask<T, F>> failedQueue;
               
        final WorkflowTaskCounters counters;

        private long nanoTime_submitTask;
        private long nanoTime_beginWork;
        private long nanoTime_finishedWork;
        
        /**
         * Ctor for retry of a failed task.
         * 
         * @param t
         *            The failed task.
         */
        public WorkflowTask(WorkflowTask<T, F> t) {
            
            this(t.target, t.service, t.successQueue, t.errorQueue, t.failedQueue, t.counters,
                    t.maxtries, t.ntries);
            
        }

        /**
         * Ctor for a new task.
         * 
         * @param target
         * @param service
         * @param successQueue
         * @param errorQueue
         * @param counters
         * @param maxtries
         */
        public WorkflowTask(T target,
                ExecutorService service,
                BlockingQueue<WorkflowTask<T, F>> successQueue,
                BlockingQueue<WorkflowTask<T, F>> errorQueue,
                BlockingQueue<WorkflowTask<T, F>> failedQueue,
                WorkflowTaskCounters counters, int maxtries) {

            this(target, service, successQueue, errorQueue, failedQueue, counters, maxtries, 0/* ntries */);
            
        }

        /**
         * Core impl.
         * 
         * @param target
         * @param service
         * @param successQueue
         * @param errorQueue
         * @param failedQueue
         * @param counters
         * @param maxtries
         * @param ntries
         */
        protected WorkflowTask(T target,
                ExecutorService service,
                BlockingQueue<WorkflowTask<T, F>> successQueue,
                BlockingQueue<WorkflowTask<T, F>> errorQueue,
                BlockingQueue<WorkflowTask<T, F>> failedQueue,
                WorkflowTaskCounters counters, int maxtries, int ntries) {

            if (target == null)
                throw new IllegalArgumentException();
        
            if (service == null)
                throw new IllegalArgumentException();
            
            if (successQueue == null)
                throw new IllegalArgumentException();
            
            if (errorQueue == null)
                throw new IllegalArgumentException();
            
            if (failedQueue == null)
                throw new IllegalArgumentException();
            
            if (counters == null)
                throw new IllegalArgumentException();
            
            if( maxtries < 0)
                throw new IllegalArgumentException();
            
            if (ntries >= maxtries)
                throw new IllegalArgumentException();
            
            this.beginTime = System.currentTimeMillis();

            this.target = target;
            
            this.service = service;
            
            this.successQueue = successQueue;

            this.errorQueue = errorQueue;

            this.failedQueue = failedQueue;

            this.counters = counters;
        
            this.maxtries = maxtries;
            
            this.ntries = ntries + 1;
            
        }
        
        /**
         * Submit the task for execution on the {@link #service}
         * 
         * @return The {@link Future}, which is also available from
         *         {@link #getFuture()}.
         */
        public Future<F> submit() {

            if (future != null) {

                // task was already submitted.
                throw new IllegalStateException();
            
            }
            
            if (log.isInfoEnabled())
                log.info("Submitting task=" + target + " : " + counters);

            // attempt to submit the task.
            try {
                // note submit time.
                nanoTime_submitTask = System.nanoTime();
                // increment the counter.
                counters.taskSubmitCount.incrementAndGet();
                // submit task.
                future = (Future<F>) service.submit(this);
            } catch(RejectedExecutionException ex) {
                // task was rejected.
                // clear submit time.
                nanoTime_submitTask = 0L;
                // and back out the #of submitted tasks.
                counters.taskSubmitCount.decrementAndGet();
                counters.taskRejectCount.incrementAndGet();
                throw ex;
            }

            if (log.isInfoEnabled())
                log.info("Submitted task="+target+" : "+counters);

            return future;

        }
        
        /**
         * Return the {@link Future} of the target.
         * 
         * @throws IllegalStateException
         *             if the {@link Future} has not been set.
         */
        public Future<F> getFuture() {

            if (future == null)
                throw new IllegalStateException();
            
            return future;
            
        }

        public void run() {

            try {
                
                runTarget();

            } catch (InterruptedException e) {
                
                // quit on interrupt.
                return;
                
            }
            
        }
        
        protected void runTarget() throws InterruptedException {
            
            log.info("Running task="+target+" : "+counters);
            
            nanoTime_beginWork = System.nanoTime();
            counters.queueWaitingTime.addAndGet(nanoTime_beginWork - nanoTime_submitTask);

            try {

                target.run();

                success();
                
            } catch (Throwable ex) {

                error(ex);
                
                throw new RuntimeException(ex);
                                
            } finally {

                nanoTime_finishedWork = System.nanoTime();
                
                // increment by the amount of time that the task was executing.
                counters.serviceNanoTime.addAndGet(nanoTime_finishedWork - nanoTime_beginWork);

                // increment by the total time from submit to completion.
                counters.queuingNanoTime.addAndGet(nanoTime_finishedWork - nanoTime_submitTask);

            }
            
        }
        
        protected void success() throws InterruptedException {
            
            counters.taskCompleteCount.incrementAndGet();

            counters.taskSuccessCount.incrementAndGet();

            if (log.isInfoEnabled())
                log.info("Success task="+target+" : "+counters);

            // may block.
            successQueue.put(this);
            
        }
        
        protected void error(Throwable t) throws InterruptedException {

            counters.taskCompleteCount.incrementAndGet();

            counters.taskFailCount.incrementAndGet();

            if(t instanceof CancellationException) {

                /*
                 * An operation submitted by the task was cancelled due to a
                 * timeout configured for the IBigdataClient's thread pool.
                 */
                
                counters.taskCancelCount.incrementAndGet();
                
            }
            
            if(InnerCause.isInnerCause(t, ClientException.class)) {

                /*
                 * Note: The way printStackTrace() behaves it appears to use
                 * getStackTrace() to format the stack trace for viewing. This
                 * seems to be custom tailored for the [cause] property for a
                 * Throwable. However, ClientException can report more than one
                 * cause from parallel executions of subtasks split out of an
                 * original request. In order for those cause_s_ to be made
                 * visible in the stack trace we have to unwrap the thrown
                 * exception until we get the [ClientException]. ClientException
                 * knows how to print a stack trace that shows ALL of its
                 * causes.
                 */
                
                t = InnerCause.getInnerCause(t, ClientException.class);
                
            }
            
            if (ntries < maxtries) {

                if (WARN)
                    log.warn("error (will retry): task=" + target + ", cause=" + t, t);

                // may block
                errorQueue.put(this);

            } else {

                // note: always with stack trace on final error.
                log.error("failed (will not retry): task=" + target+", cause="+t, t);

                counters.taskFatalCount.incrementAndGet();

                // may block.
                failedQueue.put(this);

            }
            
        }
                
    }
    
    /**
     * Counters updated by a {@link WorkflowTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo extract a common interface or impl for the
     *       {@link QueueStatisticsTask} so that we can report the response
     *       times here.
     */
    public static class WorkflowTaskCounters extends TaskCounters {
        
        /**
         * The #of tasks for which a {@link RejectedExecutionException} was
         * thrown when the task was
         * {@link ExecutorService#submit(java.util.concurrent.Callable) submitted}.
         * <p>
         * Note: A task is not considered to be "submitted" until it is accepted
         * for execution by some service. Therefore a task which is rejected
         * does NOT cause {@link TaskCounters#taskSubmitCount} to be incremented
         * and the retry of a rejected task is not counted by
         * {@link #taskRetryCount}.
         */
        final public AtomicInteger taskRejectCount = new AtomicInteger(0);

        /**
         * The #of tasks that have been re-submitted following some error which
         * caused the task to fail.
         */
        final public AtomicInteger taskRetryCount = new AtomicInteger(0);
        
        /**
         * #of tasks that failed due to a {@link CancellationException} - this is
         * the exception thrown when the
         * {@link IBigdataClient.Options#CLIENT_TASK_TIMEOUT} is exceeded for some
         * operation causing the operation to be cancelled and the task to fail.
         */
        final public AtomicLong taskCancelCount = new AtomicLong();

        /**
         * The #of tasks which have failed and will not be retried as their max
         * retry count has been reached.
         */
        final public AtomicInteger taskFatalCount = new AtomicInteger(0);
        
        public String toString() {
         
            return super.toString() + ", #reject=" + taskRejectCount
                    + ", #retry=" + taskRetryCount + ", #cancel="
                    + taskCancelCount + ", #fatal=" + taskFatalCount;
            
        }
        
    }
    
    /*
     * RDF SPECIFIC STUFF
     */
    
    /**
     * A factory for {@link Runnable} tasks.
     */
    public static interface ITaskFactory {
        
        public Runnable newTask(String file) throws Exception;
        
    }
    
    /**
     * A factory for {@link StatementBuffer}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IStatementBufferFactory {

        /**
         * Return the {@link StatementBuffer} to be used for a task.
         */
        public StatementBuffer getStatementBuffer();
        
    }
    
    /**
     * Tasks either loads a RDF resource or verifies that the told triples found
     * in that resource are present in the database. The difference between data
     * load and data verify is just the behavior of the {@link StatementBuffer}
     * returned by {@link ConcurrentDataLoader#getStatementBuffer()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ReaderTask implements Runnable {

        protected static final Logger log = Logger.getLogger(ReaderTask.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
                .toInt();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
                .toInt();

        /**
         * The resource to be loaded.
         */
        final String resource;
        
        /**
         * The base URL for that resource.
         */
        final String baseURL;
        
        /**
         * The RDF interchange syntax that the file uses.
         */
        final RDFFormat rdfFormat;

        /**
         * Validate the RDF interchange syntax when <code>true</code>.
         */
        final boolean verifyData;
        
        final IStatementBufferFactory bufferFactory;
        
        final AtomicLong toldTriples;
        
        /**
         * The time when the task was first created.
         */
        final long createTime;
        
        public String toString() {
            
            return "LoadTask"//
            +"{ resource="+resource
            +", elapsed="+(System.currentTimeMillis()-createTime)//
            +"}"//
            ;
            
        }
        
        /**
         * 
         * Note: Updates to <i>toldTriples</i> MUST NOT occur unless the task
         * succeeds, otherwise tasks which error and then retry will cause
         * double-counting.
         * 
         * @param resource
         * @param baseURL
         * @param rdfFormat
         * @param verifyData
         * @param bufferFactory
         * @param toldTriples
         */
        public ReaderTask(String resource, String baseURL, RDFFormat rdfFormat,
                boolean verifyData, IStatementBufferFactory bufferFactory, AtomicLong toldTriples) {

            if (resource == null)
                throw new IllegalArgumentException();

            if (baseURL == null)
                throw new IllegalArgumentException();

            if (rdfFormat == null)
                throw new IllegalArgumentException();
            
            if (bufferFactory == null)
                throw new IllegalArgumentException();

            if (toldTriples == null)
                throw new IllegalArgumentException();
            
            this.resource = resource;
            
            this.baseURL = baseURL;

            this.rdfFormat = rdfFormat;

            this.verifyData = verifyData;
            
            this.bufferFactory = bufferFactory;
            
            this.toldTriples = toldTriples;
            
            this.createTime = System.currentTimeMillis();
            
        }

        public void run() {

            final LoadStats loadStats;
            try {

                loadStats = readData();

            } catch (Exception e) {

                /*
                 * Note: no stack trace and only a warning - we will either
                 * retry or declare the input as filed.
                 */
                log.warn("resource=" + resource + ", error=" + e);

                throw new RuntimeException("resource=" + resource + " : " + e, e);

            }
            
            // Note: IFF the task succeeds!
            toldTriples.addAndGet(loadStats.toldTriples);

        }

        /**
         * Reads an RDF resource and either loads it into the database or
         * verifies that the triples in the resource are found in the database.
         */
        protected LoadStats readData() throws Exception {

            final long begin = System.currentTimeMillis();

            // get buffer - determines data load vs database validate.
            final StatementBuffer buffer = bufferFactory.getStatementBuffer();
            
            // make sure that the buffer is empty.
            buffer.clear();
            
            log.info("loading: " + resource);

            final PresortRioLoader loader = new PresortRioLoader(buffer);

            // open reader on the file.
            final InputStream rdfStream = new FileInputStream(resource);

            // Obtain a buffered reader on the input stream.
            final Reader reader = new BufferedReader(new InputStreamReader(
                    rdfStream));

            try {

                final LoadStats stats = new LoadStats();

                // run the parser.
                // @todo reuse the same underlying parser instance?
                loader.loadRdf(reader, baseURL, rdfFormat, verifyData);

                long nstmts = loader.getStatementsAdded();

                stats.toldTriples = nstmts;

                stats.loadTime = System.currentTimeMillis() - begin;
                
                stats.totalTime = System.currentTimeMillis() - begin;

                /*
                 * This reports the load rate for the file, but this will only
                 * be representative of the real throughput if autoFlush is
                 * enabled (that is, if the statements for each file are flushed
                 * through to the database when that file is processed rather
                 * than being accumulated in a thread-local buffer).
                 */
                if (INFO)
                    log.info(stats.toString());

                return stats;

            } catch (Exception ex) {

                /*
                 * Note: discard anything in the buffer. This prevents the
                 * buffer from retaining data after a failed load operation.
                 */
                buffer.clear();
                
                // rethrow the exception.
                throw ex;

            } finally {

                reader.close();

                rdfStream.close();

            }

        }

    };

    /**
     * Statements inserted into the buffer are verified against the database. No
     * new {@link Value}s or {@link Statement}s will be written on the
     * database by this class. The #of {@link URI}, {@link Literal}, and told
     * triples not found in the database are reported by various counters.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * FIXME The counters are being updated on each incremental write rather
     * than tracked on a per-task basis and then updated iff the task as a whole
     * succeeds. This causes double-counting of both found and not found totals
     * when a task errors and then retries. The counters need to be attached to
     * the task and the task logic extended to capture them rather than to the
     * statement buffer (a bit of a mess).
     */
    public static class VerifyStatementBuffer extends StatementBuffer {

        final protected static Logger log = Logger.getLogger(VerifyStatementBuffer.class);
        
        /**
         * True iff the {@link #log} level is WARN or less.
         */
        final protected  static boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
                .toInt();

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final protected  static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
                .toInt();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
                .toInt();

        final AtomicLong nterms, ntermsNotFound, ntriples, ntriplesNotFound;
        
        /**
         * @param database
         * @param capacity
         */
        public VerifyStatementBuffer(AbstractTripleStore database,
                int capacity, AtomicLong nterms, AtomicLong ntermsNotFound,
                AtomicLong ntriples, AtomicLong ntriplesNotFound) {
            
            super(database, capacity);

            this.nterms = nterms;
            
            this.ntermsNotFound = ntermsNotFound;
            
            this.ntriples = ntriples;
            
            this.ntriplesNotFound = ntriplesNotFound;
            
        }
        
        /**
         * Overriden to batch verify the terms and statements in the buffer.
         * 
         * FIXME Verify that {@link StatementBuffer#flush()} is doing the right
         * thing for this case (esp, how it handles bnodes when appearing as
         * {s,p,o} or when appearing as the statement identifier).
         */
        protected void incrementalWrite() {

            if (INFO) {
                log.info("numValues=" + numValues + ", numStmts=" + numStmts);
            }

            // Verify terms (batch operation).
            if (numValues > 0) {

                database.getLexiconRelation()
                        .addTerms(values, numValues, true/* readOnly */);

            }

            for( int i=0; i<numValues; i++ ) {
                
                final _Value v = values[i];

                nterms.incrementAndGet();

                if (v.termId == IRawTripleStore.NULL) {
                    
                    if(WARN) log.warn("Unknown term: "+v);

                    ntermsNotFound.incrementAndGet();

                }
                
            }
            
            // Verify statements (batch operation).
            if (numStmts > 0) {

                final SPO[] a = new SPO[numStmts];
                final _Statement[] b = new _Statement[numStmts];
                
                // #of SPOs generated for testing.
                int n = 0;
                
                for(int i=0; i<numStmts; i++) {
                  
                    final _Statement s = stmts[i];

                    ntriples.incrementAndGet();

                    if (s.s.termId == IRawTripleStore.NULL
                            || s.p.termId == IRawTripleStore.NULL
                            || s.o.termId == IRawTripleStore.NULL) {
                        
                        if(WARN) log.warn("Unknown statement (one or more unknown terms) "+s);
                        
                        ntriplesNotFound.incrementAndGet();
                        
                        continue;
                        
                    }
                    
                    a[n] = new SPO(s.s.termId,s.p.termId,s.o.termId);
                    
                    b[n] = s;
                    
                    n++;
                    
                }
                
                final IChunkedOrderedIterator<SPO> itr = database
                        .bulkCompleteStatements(a, n);

                try {

                    while (itr.hasNext()) {

                        itr.next();

                    }

                } finally {

                    itr.close();

                }
                
                for (int i = 0; i < n; i++) {

                    final SPO spo = a[i];

                    if (!spo.hasStatementType()) {

                        ntriplesNotFound.incrementAndGet();

                        if(WARN)
                        log.warn("Statement not in database: " + b[i]+" ("+spo+")");

                        continue;

                    }

                    if (spo.getType() != StatementEnum.Explicit) {
                        
                        ntriplesNotFound.incrementAndGet();

                        if(WARN)
                        log.warn("Statement not explicit database: "+b[i]+" is marked as "+spo.getType());
                        
                        continue;
                        
                    }
                    
                }
                
            }
            
            // Reset the state of the buffer (but not the bnodes nor deferred stmts).
            _clear();

        }
        
    }

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class LoadStatementBufferFactory implements IStatementBufferFactory {

        private final AbstractTripleStore db;

        private final int bufferCapacity;

        public LoadStatementBufferFactory(AbstractTripleStore db,
                int bufferCapacity) {

            this.db = db;
           
            this.bufferCapacity = bufferCapacity;
            
        }
        
        /**
         * Return the {@link ThreadLocal} {@link StatementBuffer} to be used for a
         * task.
         */
        public StatementBuffer getStatementBuffer() {

            /*
             * Note: this is a thread-local so the same buffer object is always
             * reused by the same thread.
             */

            return threadLocal.get();
            
        }
        
        private ThreadLocal<StatementBuffer> threadLocal = new ThreadLocal<StatementBuffer>() {

            protected synchronized StatementBuffer initialValue() {
                
                return new StatementBuffer(db, bufferCapacity);
                
            }

        };
        
    }

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class VerifyStatementBufferFactory implements IStatementBufferFactory {

        private final AbstractTripleStore db;

        private final int bufferCapacity;

        public final AtomicLong nterms = new AtomicLong(),
                ntermsNotFound = new AtomicLong(), ntriples = new AtomicLong(),
                ntriplesNotFound = new AtomicLong();

        public VerifyStatementBufferFactory(AbstractTripleStore db,
                int bufferCapacity) {

            this.db = db;
           
            this.bufferCapacity = bufferCapacity;
            
        }
        
        /**
         * Return the {@link ThreadLocal} {@link StatementBuffer} to be used for a
         * task.
         */
        public StatementBuffer getStatementBuffer() {

            /*
             * Note: this is a thread-local so the same buffer object is always
             * reused by the same thread.
             */

            return threadLocal.get();
            
        }
        
        private ThreadLocal<StatementBuffer> threadLocal = new ThreadLocal<StatementBuffer>() {

            protected synchronized StatementBuffer initialValue() {

                return new VerifyStatementBuffer(db, bufferCapacity, nterms,
                        ntermsNotFound, ntriples, ntriplesNotFound);
                
            }

        };
        
    }

    /**
     * Factory for tasks for loading RDF resources into a database or validating
     * RDF resources against a database.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo report the #of resources processed in each case.
     */
    public static class AbstractRDFTaskFactory implements ITaskFactory {

        protected static final Logger log = Logger
                .getLogger(RDFLoadTaskFactory.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
                .toInt();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
                .toInt();

        /**
         * The database on which the data will be written.
         */
        final AbstractTripleStore db;

        /**
         * The timestamp set when {@link #notifyStart()} is invoked.
         */
        private long beginTime;

        /**
         * The timestamp set when {@link #notifyEnd()} is invoked.
         */
        private long endTime;
        
        /**
         * Notify that the factory will begin running tasks. This sets the
         * {@link #beginTime} used by {@link #elapsed()} to report the run time
         * of the tasks.
         */
        public void notifyStart() {
                        
            endTime = 0L;
            
            beginTime = System.currentTimeMillis();
            
        }

        /**
         * Notify that the factory is done running tasks (for now).  This
         * places a cap on the time reported by {@link #elapsed()}.
         * 
         * @todo Once we are done loading data the client should be told to
         *       flush its counters to the load balancer so that we have the
         *       final state snapshot once it is ready.
         */
        public void notifyEnd() {
            
            endTime = System.currentTimeMillis();

            assert beginTime <= endTime;
            
        }
        
        /**
         * The elapsed time, counting only the time between
         * {@link #notifyStart()} and {@link #notifyEnd()}.
         */
        public long elapsed() {
            
            if(endTime==0L) {
                
                // Still running.
                return System.currentTimeMillis() - beginTime;
                
            } else {
                
                // Done.
                
                final long elapsed = endTime - beginTime;
                
                assert elapsed >= 0L;
                
                return elapsed;
                
            }
            
        }

//      /**
//      * The baseURL and "" if none is needed.
//      */
//     final String baseURL;
     
        /**
         * An attempt will be made to determine the interchange syntax using
         * {@link RDFFormat}. If no determination can be made then the loader
         * will presume that the files are in the format specified by this
         * parameter (if any). Files whose format can not be determined will be
         * logged as errors.
         */
        final RDFFormat fallback;

        /**
         * Validation of RDF by the RIO parser is disabled.
         */
        final boolean verifyData;

        final IStatementBufferFactory bufferFactory;

        /**
         * #of told triples loaded into the database by successfully completed {@link ReaderTask}s.
         */
        final AtomicLong toldTriples = new AtomicLong(0);

        /**
         * Guess at the {@link RDFFormat}.
         * 
         * @param filename
         *            Some filename.
         * 
         * @return The {@link RDFFormat} -or- <code>null</code> iff
         *         {@link #fallback} is <code>null</code> and the no format
         *         was recognized for the <i>filename</i>
         */
        public RDFFormat getRDFFormat(String filename) {

            final RDFFormat rdfFormat = //
            fallback == null //
            ? RDFFormat.forFileName(filename) //
                    : RDFFormat.forFileName(filename, fallback)//
            ;

            return rdfFormat;

        }

        protected AbstractRDFTaskFactory(AbstractTripleStore db,
                boolean verifyData, RDFFormat fallback,
                IStatementBufferFactory bufferFactory) {

            this.db = db;
            
            this.verifyData = verifyData;

            this.fallback = fallback;
            
            this.bufferFactory = bufferFactory;
            
        }

        public Runnable newTask(String resource) throws Exception {
            
            log.info("resource="+resource);
            
            final RDFFormat rdfFormat = getRDFFormat( resource );
            
            if (rdfFormat == null) {

                throw new RuntimeException(
                        "Could not determine interchange syntax - skipping : file="
                                + resource);

            }

            /*
             * FIXME resolve what the baseURL SHOULD be and generalize for
             * alternative source (bigdata repo, file system, map/reduce master,
             * etc.)
             * 
             * Note: conversion is to a URL.
             */
            final String baseURL;

            try {

                baseURL = new File(resource).toURL().toString();

            } catch (MalformedURLException e) {

                throw new RuntimeException("resource=" + resource);

            }
            
            return new ReaderTask(resource, baseURL, rdfFormat,
                    verifyData, bufferFactory, toldTriples );
            
        }
        
    }

    /**
     * Factory for tasks for verifying a database against RDF resources.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class RDFVerifyTaskFactory extends AbstractRDFTaskFactory {

        public RDFVerifyTaskFactory(AbstractTripleStore db, int bufferCapacity,
                boolean verifyData, RDFFormat fallback) {

            super(db, verifyData, fallback, new VerifyStatementBufferFactory(
                    db, bufferCapacity));

        }

        public long getTermCount() {
            
            return ((VerifyStatementBufferFactory)bufferFactory).nterms.get();
            
        }
        
        public long getTermNotFoundCount() {
            
            return ((VerifyStatementBufferFactory)bufferFactory).ntermsNotFound.get();
            
        }
        
        public long getTripleCount() {
            
            return ((VerifyStatementBufferFactory)bufferFactory).ntriples.get();
            
        }
        
        public long getTripleNotFoundCount() {
            
            return ((VerifyStatementBufferFactory)bufferFactory).ntriplesNotFound.get();
            
        }
        
        /**
         * Report on #terms and #stmts not found as well as #triples processed
         * and found.
         */
        public String reportTotals() {

            // total run time.
            final long elapsed = elapsed();

            final long tripleCount = getTripleCount();

            final double tps = (long) (((double) tripleCount) / ((double) elapsed) * 1000d);

            return "Processed: #terms=" + getTermCount() + " ("
                    + getTermNotFoundCount() + " not found), #stmts="
                    + tripleCount + " (" + getTripleNotFoundCount()
                    + " not found)" + ", rate=" + tps + " in " + elapsed
                    + " ms.";
                
        }
        
    }
    
    /**
     * Factory for tasks for loading RDF resources into a database.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class RDFLoadTaskFactory extends AbstractRDFTaskFactory {

        public RDFLoadTaskFactory(AbstractTripleStore db, int bufferCapacity,
                boolean verifyData, RDFFormat fallback) {

            super(db, verifyData, fallback, new LoadStatementBufferFactory(db,
                    bufferCapacity));

        }

        /**
         * Sets up some additional counters for reporting by the client to the
         * {@link ILoadBalancerService}.
         * 
         * @todo in the base class also?
         * 
         * @param tmp
         */
        public void setupCounters(CounterSet tmp) {
            
            tmp.addCounter("toldTriples", new Instrument<Long>() {

                @Override
                protected void sample() {

                    setValue(toldTriples.get());

                }
            });

            tmp.addCounter("elapsed", new Instrument<Long>() {

                @Override
                protected void sample() {

                    final long elapsed = elapsed();

                    setValue(elapsed);

                }
            });

            /*
             * Note: This is the told triples per second rate for _this_ client
             * only. When you are loading using multiple instances of the concurrent
             * data loader, then the total told triples per second rate is the
             * aggregation across all of those instances.
             */
            tmp.addCounter("toldTriplesPerSec", new Instrument<Long>() {

                @Override
                protected void sample() {

                    final long elapsed = elapsed();

                    final double tps = (long) (((double) toldTriples.get())
                            / ((double) elapsed) * 1000d);

                    setValue((long) tps);

                }
            });

        }
        
        /**
         * Report totals.
         * <p>
         * Note: these totals reflect the actual state of the database, not just
         * the #of triples written by this client. Therefore if there are
         * concurent writes then the apparent TPS here will be higher than was
         * reported by the counters for just this client -- all writes on the
         * database will have been attributed to just this client.
         */
        public String reportTotals() {

            // total run time.
            final long elapsed = elapsed();

            final long nterms = db.getTermCount();

            final long nstmts = db.getStatementCount();

            final double tps = (long) (((double) nstmts) / ((double) elapsed) * 1000d);

            return "Database: #terms=" + nterms + ", #stmts=" + nstmts
                    + ", rate=" + tps + " in " + elapsed + " ms.";
                
        }
        
    }
    
}
