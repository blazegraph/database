/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 24, 2009
 */

package com.bigdata.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.service.DataService;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.NV;

/**
 * Suite of stress tests of the concurrency control mechanisms (without the
 * database implementation) - See {@link NonBlockingLockManager}.
 * <p>
 * Goals:
 * <p>
 * 1. higher concurrency of unisolated operations on the ds/journal with group
 * commit. this only requires a "lock" per writable named index, e.g., an
 * operation will lock exactly one resource. show consistency of the data in a
 * suite of stress tests with varying #s of threads, tasks, and resources. Each
 * task will lock exactly one resource - the unisolated named index on which it
 * would write. DO THIS W/O the TxDAG first and get group commit debugged before
 * trying to work through the tx commit stuff, which requires the WAITS_FOR
 * graph support.
 * <p>
 * 2. transaction processing integrated with unisolated operations. since a
 * transaction MAY write on more than one index this requires a TxDAG and a
 * queue of resources waiting to get into the granted group (which will always
 * be a singleton since writes on unisolated named indices require exclusive
 * locks). show that concurrency control never deadlocks in a suite of stress
 * tests with varying #s of threads, tasks, resources, and resource locked per
 * task. Again, a resource is a unisolated named index.
 * 
 * @todo verify more necessary outcomes of the different tests using assertions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractStressTestNonBlockingLockManager extends TestCase {

    protected static final Logger log = Logger
            .getLogger(StressTestNonBlockingLockManagerWithPredeclaredLocks.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * 
     */
    public AbstractStressTestNonBlockingLockManager() {
    }

    /**
     * @param arg0
     */
    public AbstractStressTestNonBlockingLockManager(String arg0) {
        super(arg0);
    }

    /**
     * Waits 10ms once it acquires its locks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Wait10ResourceTask<T> implements Callable<T> {

        public T call() throws Exception {

//            if (INFO)
//                log.info("Executing: "+this);
            
            synchronized (this) {

                try {
                    
                    if (DEBUG)
                        log.debug("Waiting: "+this);
                    
                    wait(10/* milliseconds */);
                    
//                    if (INFO)
//                        log.info("Done waiting: "+this);
                    
                } catch (InterruptedException e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }
            
//            if (INFO)
//                log.info("Done: "+this);
            
            return null;

        }

    }
    
    /**
     * Dies once it acquires its locks by throwing {@link HorridTaskDeath}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class DeathResourceTask<T> implements Callable<T> {
        
        public T call() throws Exception {

            if(DEBUG)
                log.debug("Arrgh!");
            
            throw new HorridTaskDeath();

        }

    }

    /**
     * Test driver. 
     * <p>
     * Note: A "resource" is a named index (partition), so set nresources based
     * on your expectations for the #of index partitions on a journal or
     * federation.
     * <p>
     * Note: The likelyhood of deadlock increases as (a) more locks are
     * requested per task; and (b) fewer resources are available to be locked.
     * When minLocks==maxLocks==nresources then tasks will be serialized since
     * each task requires all resources in order to proceed.
     * <p>
     * Note: At minLocks==maxLocks==1 this test can be used to explore the
     * behavior of tasks that lock only a single resource, eg., unisolated
     * operations on the {@link DataService}.
     */
    public Result doComparisonTest(final Properties properties) throws Exception {

        // if 0L use Long.MAX_VALUE else convert seconds to nanoseconds.
        final long testTimeout = (TimeUnit.SECONDS.toNanos(Long
                .parseLong(properties.getProperty(TestOptions.TIMEOUT,
                        TestOptions.DEFAULT_TIMEOUT))) == 0L) ? Long.MAX_VALUE
                : TimeUnit.SECONDS.toNanos(Long.parseLong(properties
                        .getProperty(TestOptions.TIMEOUT,
                                TestOptions.DEFAULT_TIMEOUT)));

        final int nthreads = Integer.parseInt(properties
                .getProperty(TestOptions.NTHREADS));

        final int ntasks = Integer.parseInt(properties
                .getProperty(TestOptions.NTASKS));

        final double percentTaskDeath = Double.parseDouble(properties
                .getProperty(TestOptions.PERCENT_TASK_DEATH,
                        TestOptions.DEFAULT_PERCENT_TASK_DEATHS));

        final int nresources = Integer.parseInt(properties
                .getProperty(TestOptions.NRESOURCES));

        final int minLocks = Integer.parseInt(properties
                .getProperty(TestOptions.MIN_LOCKS));

        final int maxLocks = Integer.parseInt(properties
                .getProperty(TestOptions.MAX_LOCKS));

        // if 0L use Long.MAX_VALUE else convert milliseconds to nanoseconds.
        final long taskTimeout = (Long.parseLong(properties.getProperty(
                TestOptions.TASK_TIMEOUT, TestOptions.DEFAULT_TASK_TIMEOUT)) == 0L) ? Long.MAX_VALUE
                : TimeUnit.MILLISECONDS.toNanos(Long.parseLong(properties
                        .getProperty(TestOptions.TASK_TIMEOUT,
                                TestOptions.DEFAULT_TASK_TIMEOUT)));

        final int maxLockTries = Integer
                .parseInt(properties.getProperty(TestOptions.MAX_LOCK_TRIES,
                        TestOptions.DEFAULT_MAX_LOCK_TRIES));

        final boolean predeclareLocks = Boolean.parseBoolean(properties
                .getProperty(TestOptions.PREDECLARE_LOCKS,
                        TestOptions.DEFAULT_PREDECLARE_LOCKS));
        
        final boolean sortLockRequests = Boolean.parseBoolean(properties
                .getProperty(TestOptions.SORT_LOCK_REQUESTS,
                        TestOptions.DEFAULT_SORT_LOCK_REQUESTS));
        
        /*
         * Note: without pre-declaration of locks, you can expect high deadlock
         * rates when minLocks=maxLocks=nresources since all tasks will contend
         * for all resources and locks will be assigned incrementally.
         */
        assert minLocks <= maxLocks;
        assert minLocks <= nresources;
        assert maxLocks <= nresources;

        assert maxLockTries >= 1;
        
        // used to execute the tasks.
        final ExecutorService delegateService;
        
        // fixed size thread pool w/ an unbounded queue.
//        delegateService = Executors.newFixedThreadPool(
//                nthreads, DaemonThreadFactory.defaultThreadFactory());
        
        // fixed size thread pool w/ zero capacity queue.
//        final BlockingQueue<Runnable> workQueue = new SynchronousQueue<Runnable>(
//                true/*fair*/);
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(1);
        delegateService = new ThreadPoolExecutor(nthreads/* corePoolSize */,
                nthreads/* maximumPoolSize */, Long.MAX_VALUE/* keepAliveTime */,
                TimeUnit.SECONDS/*keepAliveUnit*/, workQueue);

        final NonBlockingLockManager<String> lockManager = new NonBlockingLockManager<String>(
                nthreads/* multi-programming level */, predeclareLocks,
                sortLockRequests, delegateService);

        try {

            final Collection<LockCallableImpl<String, Object>> tasks = new ArrayList<LockCallableImpl<String, Object>>(
                    ntasks);

            // distinct resource names. references are reused by reach task.
            final String[] resources = new String[nresources];

            for (int i = 0; i < nresources; i++) {

                resources[i] = "resource" + i;

                // assertTrue(db.addResource(resources[i]));

            }

            Random r = new Random();

            // create tasks; each will use between minLocks and maxLocks
            // distinct
            // resources.
            for (int i = 0; i < ntasks; i++) {

                // #of locks that this task will seek to acquire.
                final int nlocks = (minLocks == maxLocks ? minLocks : r
                        .nextInt(maxLocks - minLocks)
                        + minLocks);

                // final int nlocks = maxLocks;

                final String[] resource = new String[nlocks];

                // find a [nlocks] unique resources to lock for this task.
                for (int j = 0; j < nlocks; j++) {

                    int t;
                    while (true) {

                        // random resource index.
                        t = r.nextInt(Integer.MAX_VALUE) % nresources;

                        // ensure distinct resources for this task.

                        boolean duplicate = false;

                        for (int k = 0; k < j; k++) {

                            if (resource[k] == resources[t]) {

                                duplicate = true;

                                break;

                            }

                        }

                        if (!duplicate)
                            break;

                    }

                    resource[j] = resources[t];

                }

                /*
                 * Create all tasks.  They will be submitted below.
                 */

                final LockCallableImpl<String, Object> task;

                if (r.nextDouble() < percentTaskDeath) {

                    task = new LockCallableImpl<String, Object>(resource,
                            new DeathResourceTask<Object>(),
                            TimeUnit.NANOSECONDS, taskTimeout, maxLockTries);

                } else {

                    task = new LockCallableImpl<String, Object>(resource,
                            new Wait10ResourceTask<Object>(),
                            TimeUnit.NANOSECONDS, taskTimeout, maxLockTries);

                }

                tasks.add(task);

            }
            
            // submit the tasks for execution.
            final long begin = System.nanoTime();
            final List<Future<Object>> futures = new LinkedList<Future<Object>>();
            int nsubmitted = 0;
            {
                if (INFO)
                    log.info("Submitting " + tasks.size()
                            + " tasks: testTimeout="
                            + TimeUnit.NANOSECONDS.toSeconds(testTimeout));
                long elapsed;
                {

                    final Iterator<LockCallableImpl<String, Object>> itr = tasks
                            .iterator();

                    while ((elapsed = (System.nanoTime() - begin)) < testTimeout
                            && itr.hasNext()) {

                        final LockCallableImpl<String, Object> task = itr
                                .next();

                        futures.add(lockManager.submit(task.resource,
                                task.task, task.unit, task.timeout,
                                task.maxtries));

                        nsubmitted++;

                    }

                }

                // normal shutdown (run to completion).
                if (INFO) {
                    
                    log.info("Submitting " + tasks.size() + " tasks in "
                            + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");
                    
                    log.info("Shutting down the lock manager.");
                    
                }
                
                lockManager.shutdown();
                
            }

            // check the futures, blocking until each is available.
            int nerrors = 0;
            int ndeadlock = 0;
            int ntimeout = 0;
            int ncomplete = 0;
            int nsuccess = 0;
            int ncancel = 0;
            int nhorriddeath = 0;
            final Iterator<Future<Object>> itr = futures.iterator();
            while (itr.hasNext()) {
                final Future<Object> future = itr.next();
                if (future.isCancelled()) {
                    ncancel++;
                } else {
                    ncomplete++;
                    try {
                        while (true) {
                            final long elapsed = System.nanoTime() - begin;
                            if (elapsed > taskTimeout) {
                                /*
                                 * task timeout.
                                 * 
                                 * @todo This is measured from when we start to
                                 * submit the tasks so they will all be
                                 * cancelled at once. Alternatively, we could
                                 * tunnel into the LockTaskFuture and compute
                                 * the timeout from the time the task was
                                 * accepted by the lock service, but that will
                                 * be different iff the lock service is using a
                                 * SynchronousQueue to accept tasks for
                                 * execution.
                                 */
                                future.cancel(true/* mayInterruptIfRunning */);
                                ntimeout++;
                                break;
                            }
                            try {
                                future.get(1, TimeUnit.SECONDS);
                                break;
                            } catch (TimeoutException ex) {
                                log.warn("Future not ready yet: task=" + future
                                        + ", service=" + lockManager);
                            }
                        }
                        nsuccess++;
                    } catch (ExecutionException ex) {
                        if (ex.getCause() instanceof DeadlockException) {
                            ndeadlock++;
//                        } else if (ex.getCause() instanceof TimeoutException) {
//                            ntimeout++;
                        } else if (ex.getCause() instanceof HorridTaskDeath) {
                            nhorriddeath++;
                        } else {
                            nerrors++;
                            log.error("Task threw: " + ex, ex);
                        }
                    }
                }
            }
            if (nerrors > 0) {
                log.warn("There were " + nerrors + " errors and " + ndeadlock
                        + " deadlocks");
            }

            // Done.
            System.out.println(lockManager.toString());

            final Result result = new Result();

            // elapsed ns from when we began to submit tasks.
            final long elapsed = System.nanoTime() - begin;
            
            // elapsed ms from when we began to submit tasks.
            final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(elapsed);
            
            final double perSec = (ncomplete * 1000d) / elapsedMillis;

            // these are based on examination of the futures.
            result.put("nsubmitted", "" + nsubmitted);
            result.put("nsuccess", "" + nsuccess);
            result.put("ncomplete", "" + ncomplete);
            result.put("ncancel", "" + ncancel);
            result.put("ndeadlock", "" + ndeadlock);
            result.put("nhorriddeath", "" + nhorriddeath); // Note: This is an expected error.
            
            // these are reporting from the lock manager.
            result.put("maxrunning", "" + lockManager.counters.maxRunning);
//            result.put("nstarted", "" + lockManager.counters.nstarted);
//            result.put("nended", "" + lockManager.counters.nended);
//            result.put("nerror", "" + lockManager.counters.nerror);
//            result.put("ndeadlock", "" + lockManager.counters.ndeadlock);
            result.put("ntimeout", "" + lockManager.counters.ntimeout);

            // throughput metrics.
            result.put("perSec", "" + perSec);
            result.put("elapsed", "" + elapsedMillis);

            System.err.println(result.toString(true/* newline */));

            return result;

        } finally {

            lockManager.shutdownNow();

            delegateService.shutdownNow();

        }

    }

    /**
     * Options for
     * {@link StressTestNonBlockingLockManagerWithPredeclaredLocks#doComparisonTest(Properties)}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestOptions {
        
        /**
         * Maximum amount of time that the test will run (seconds) or ZERO (0)
         * if there is no timeout.
         */
        public static final String TIMEOUT = "testTimeout";
        /**
         * The #of concurrent threads (multi-programming level).
         */
        public static final String NTHREADS = "nthreads";
        /**
         * Total #of tasks to execute.
         */
        public static final String NTASKS = "ntasks";
        /**
         * The percentage of tasks that will die a {@link HorridTaskDeath} in
         * [0.0:1.0] (default is 0.0). This is used to stress the error handling
         * mechanisms.
         */
        public static final String PERCENT_TASK_DEATH = "percentTaskDeath";
        /**
         * The #of declared resources.
         */
        public static final String NRESOURCES = "nresources";
        /**
         * The minimum #of locks that a task will seek to acquire.
         */
        public static final String MIN_LOCKS = "minLocks";
        /**
         * The maximum #of locks that a task will seek to acquire.
         */
        public static final String MAX_LOCKS = "maxLocks";
        /**
         * The timeout for the task in (milliseconds) -or- <code>0</code> iff
         * no timeout will be used.
         */
        public static final String TASK_TIMEOUT = "lockTimeout";
        /**
         * The maximum #of times that a task will attempt to acquire its locks
         * before failing. Temporary failures may occur due to deadlock or
         * timeout during lock acquisition. Such failures may be retried. The
         * minimum value is ONE (1) since that means that we make only one
         * attempt to obtain the necessary locks for the task.
         */
        public static final String MAX_LOCK_TRIES = "maxLockTries";

        /**
         * When true, operations MUST pre-declare their locks (default true).
         * <p>
         * Note: The {@link NonBlockingLockManager} uses this information to
         * avoid deadlocks by the simple expediency of sorting the resources in
         * each lock request into a common order. With this option deadlocks are
         * NOT possible but all locks MUST be pre-declared by the operation
         * before it begins to execute.
         */
        public static final String PREDECLARE_LOCKS = "predeclareLocks";
        
        /**
         * When true, the resources in a lock request are sorted before the lock
         * requests are issued (default true). This option is ONLY turned off
         * for testing purposes. Since predeclaration plus sorting makes
         * deadlocks impossible, this option MAY be turned off in order to
         * exercise the deadlock detection logic in {@link TxDag} and the
         * handling of deadlocks when they are detected.
         */
        public static final String SORT_LOCK_REQUESTS = "sortLockRequest";
        
        /**
         * The default is no timeout for the test.
         */
        public static final String DEFAULT_TIMEOUT = "0";

        /**
         * The default is 1 try for locks.
         */
        public static final String DEFAULT_MAX_LOCK_TRIES = "1";

        /**
         * The default is no timeout for tasks.
         */
        public static final String DEFAULT_TASK_TIMEOUT = "0";

        /**
         * By default we do not force any tasks to die.
         */
        public static final String DEFAULT_PERCENT_TASK_DEATHS = "0.0";
        
        /**
         * By default the operations will predeclare their locks.
         */
        public static final String DEFAULT_PREDECLARE_LOCKS = "true";
        
        /**
         * By default lock requests will be sorted.
         */
        public static final String DEFAULT_SORT_LOCK_REQUESTS = "true";
        
    }
    
    public void setUpComparisonTest(Properties properties) throws Exception {

    }

    public void tearDownComparisonTest() throws Exception {
        
    }

    /**
     * Generates an XML file that can be used to (re-)run the concurrency
     * control tests. The outputs are appended to a file so you can see how
     * performance and collected counters change from run to run.
     * 
     * @see ExperimentDriver
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo refactor the tests generated below to use apply() and run each
     *       basic condition with and without lock timeout and with and without
     *       predeclaration of locks (and without sorting when locks are NOT
     *       predeclared so that we can exercise the deadlock detection stuff).
     *       We could also run each condition at (2), (20), and (100) threads.
     */
    static public class Generate extends ExperimentDriver {
        
        /**
         * Generates an XML file that can be run by {@link ExperimentDriver}.
         * 
         * @param args
         */
        public static void main(String[] args) throws Exception {
            
            // this is the test to be run.
            String className = TestLockManager.class.getName();
            
            Map<String,String> defaultProperties = new HashMap<String,String>();

            /* 
             * Set defaults for each condition.
             */
            
            defaultProperties.put(TestOptions.TIMEOUT,"30"); // secs.
            defaultProperties.put(TestOptions.NTHREADS,"20");
            defaultProperties.put(TestOptions.NTASKS,"1000");
            defaultProperties.put(TestOptions.NRESOURCES,"100");
            defaultProperties.put(TestOptions.MIN_LOCKS,"1");
            defaultProperties.put(TestOptions.MAX_LOCKS,"3");
            defaultProperties.put(TestOptions.MAX_LOCK_TRIES,"1");
            defaultProperties.put(TestOptions.PREDECLARE_LOCKS,"false");
            defaultProperties.put(TestOptions.SORT_LOCK_REQUESTS,"false");
            defaultProperties.put(TestOptions.TASK_TIMEOUT,"1000"); // ms

            List<Condition>conditions = new ArrayList<Condition>();

            // low concurrency.
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.NTHREADS,
                            "2") }));
            // and with a non-zero lock timeout.
            conditions.add(getCondition(defaultProperties, new NV[] {
                    new NV(TestOptions.NTHREADS, "2"),
                    new NV(TestOptions.TASK_TIMEOUT, "1000") }));

            // default concurrency.
            conditions.add(getCondition(
                    defaultProperties, new NV[] {}));
            // and with a non-zero lock timeout.
            conditions.add(getCondition(defaultProperties, new NV[] { new NV(
                    TestOptions.TASK_TIMEOUT, "1000") }));

            // default concurrency with 10% task death.
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.PERCENT_TASK_DEATH,
                            ".10") }));
            // and with a non-zero lock timeout.
            conditions.add(getCondition(
                    defaultProperties, new NV[] {
                    new NV(TestOptions.PERCENT_TASK_DEATH, ".10"),
                    new NV(TestOptions.TASK_TIMEOUT, "1000") }));

            // high concurrency.
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.NTHREADS,
                            "100") }));
            // and with a non-zero lock timeout
            conditions.add(getCondition(defaultProperties, new NV[] {
                    new NV(TestOptions.NTHREADS, "100"),
                    new NV(TestOptions.TASK_TIMEOUT, "1000") }));

            // high concurrency with 10% task death.
            conditions.add(getCondition(defaultProperties, new NV[] {
                    new NV(TestOptions.NTHREADS, "100"),
                    new NV(TestOptions.PERCENT_TASK_DEATH, ".10") }));
            // and with a non-zero lock timeout.
            conditions.add(getCondition(defaultProperties, new NV[] {
                    new NV(TestOptions.NTHREADS, "100"),
                    new NV(TestOptions.PERCENT_TASK_DEATH, ".10"),
                    new NV(TestOptions.TASK_TIMEOUT, "1000") }));

            // force sequential execution by limiting
            // nresources==minlocks==maxlocks.
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.NRESOURCES,
                            "1"), new NV(TestOptions.MIN_LOCKS,
                            "1"), new NV(TestOptions.MAX_LOCKS,
                            "1") }));
            // and with a non-zero lock timeout
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.NRESOURCES,
                            "1"), new NV(TestOptions.MIN_LOCKS,
                            "1"), new NV(TestOptions.MAX_LOCKS,
                            "1"), new NV(TestOptions.TASK_TIMEOUT,"1000") }));
            
            // force sequential execution by limiting nresources==minlocks==maxlocks.
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.NRESOURCES,
                            "3"), new NV(TestOptions.MIN_LOCKS,
                            "3"), new NV(TestOptions.MAX_LOCKS,
                            "3") }));
            // and with a non-zero lock timeout
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.NRESOURCES,
                            "3"), new NV(TestOptions.MIN_LOCKS,
                            "3"), new NV(TestOptions.MAX_LOCKS,
                            "3"), new NV(TestOptions.TASK_TIMEOUT,"1000") }));

            Experiment exp = new Experiment(className,defaultProperties,conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }

    }
    
}
