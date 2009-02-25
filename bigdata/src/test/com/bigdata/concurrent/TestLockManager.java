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
 * Created on Oct 1, 2007
 */

package com.bigdata.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.service.DataService;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.NV;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Suite of stress tests of the concurrency control mechanisms (without the
 * database implementation) - See {@link LockManager}.
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
 * @todo test to verify that we can interrupt running tasks.
 * 
 * @todo refactor to have Thread != Tx using thread local variables ala
 *       {@link LockContext}. Change the {@link #lockedResources} to be a
 *       {@link ThreadLocal} variable. Borrow the test suite for the
 *       {@link LockContext}.
 * 
 * @todo verify more necessary outcomes of the different tests using assertions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLockManager extends TestCase implements IComparisonTest {

    public static final Logger log = Logger.getLogger(TestLockManager.class);

    /**
     * 
     */
    public TestLockManager() {
        super();
    }

    public TestLockManager(String name) {
        super(name);
    }

    /**
     * Waits 10ms once it acquires its locks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Wait10ResourceTask implements Callable<Object> {

        public Object call() throws Exception {

            synchronized (this) {

                try {
                    
                    wait(10/* milliseconds */);
                    
                } catch (InterruptedException e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }
            
            return null;

        }

    }
    
    /**
     * Dies once it acquires its locks by throwing {@link HorridTaskDeath}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class DeathResourceTask implements Callable<Object> {

        public Object call() throws Exception {

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
    public Result doComparisonTest(Properties properties) throws Exception {

        final long testTimeout = Integer.parseInt(properties.getProperty(
                TestOptions.TIMEOUT, TestOptions.DEFAULT_TIMEOUT));

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

        final long lockTimeout = Integer.parseInt(properties.getProperty(
                TestOptions.LOCK_TIMEOUT, TestOptions.DEFAULT_LOCK_TIMEOUT));

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
        
        ExecutorService execService = Executors.newFixedThreadPool(nthreads,
                DaemonThreadFactory.defaultThreadFactory());

        Collection<Callable<Object>> tasks = new ArrayList<Callable<Object>>(
                ntasks);

        LockManager<String> db = new LockManager<String>(
                nthreads/* multi-programming level */, predeclareLocks,
                sortLockRequests);

        // distinct resource names. references are reused by reach task.
        final String[] resources = new String[nresources];

        for (int i = 0; i < nresources; i++) {

            resources[i] = "resource" + i;

//            assertTrue(db.addResource(resources[i]));
            
        }

        Random r = new Random();

        // create tasks; each will use between minLocks and maxLocks distinct
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

            final LockManagerTask<String,Object> task;
            
            if(r.nextDouble()<percentTaskDeath) {
            
                task = new LockManagerTask<String,Object>(db,resource,new DeathResourceTask());
                
            } else {
                
                task = new LockManagerTask<String,Object>(db,resource,new Wait10ResourceTask());
                
            }
            
            task.setMaxLockTries(maxLockTries);
            
            task.setLockTimeout(lockTimeout);
            
            tasks.add( task );

        }

        // run everyone.
        
        log.info("invoking all tasks");
        
        final long begin = System.currentTimeMillis();
        
        final List<Future<Object>> futures;
        
        if(testTimeout==0L) {

            futures = execService.invokeAll(tasks);
        
        } else {

            futures = execService.invokeAll(tasks, testTimeout, TimeUnit.SECONDS);

        }
        
//        // normal shutdown (run to completion).
//        log.info("Shutting down service");
//        execService.shutdown();

        final long elapsed = System.currentTimeMillis() - begin;
        
        // terminate running and pending tasks.
        log.info("Shutting down service");
        execService.shutdownNow();

        // await termination (should be rapid unless there is a deadlock).
        log.info("Awaiting termination");
        execService.awaitTermination(1, TimeUnit.SECONDS);

        // check the futures.
        int nerrors = 0;
        int ndeadlock = 0;
        int ntimeout = 0;
        int ncomplete = 0;
        int ncancel = 0;
        int nhorriddeath = 0;
        Iterator<Future<Object>> itr = futures.iterator();
        while (itr.hasNext()) {
            Future<Object> future = itr.next();
            if (future.isCancelled()) {
                ncancel++;
            } else {
                ncomplete++;
                try {
                    future.get();
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof DeadlockException) {
                        ndeadlock++;
                    } else if (ex.getCause() instanceof TimeoutException) {
                        ntimeout++;
                    } else if( ex.getCause() instanceof HorridTaskDeath ) {
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
        System.out.println(db.toString());

        Result result = new Result();

        double perSec = (ncomplete *1000d) / elapsed;
        
        result.put("maxrunning", ""+db.maxrunning);
        result.put("nrunning", ""+db.nrunning);
        result.put("nstarted", ""+db.nstarted);
        result.put("nended", ""+db.nended);
        result.put("nerror", ""+db.nerror);
        result.put("ndeadlock", ""+db.ndeadlock);
        result.put("ntimeout", ""+db.ntimeout);
        result.put("ncomplete", ""+ncomplete);
        result.put("ncancel", ""+ncancel);
        result.put("nhorriddeath", ""+nhorriddeath); // Note: This is an expected task death.
        result.put("perSec", ""+perSec);
        result.put("elapsed", ""+elapsed);

        System.err.println(result.toString(true/*newline*/));
        
        return result;
        
    }

    /**
     * Options for {@link TestLockManager#doComparisonTest(Properties)}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestOptions {
        
        /**
         * Maximum amount of time that the test will run (seconds).
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
         * The timeout when attempting to acquire a lock (milliseconds) -or-
         * <code>0</code> iff no timeout will be used.
         */
        public static final String LOCK_TIMEOUT = "lockTimeout";
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
         * Note: The {@link LockManager} uses this information to avoid
         * deadlocks by the simple expediency of sorting the resources in each
         * lock request into a common order. With this option deadlocks are NOT
         * possible but all locks MUST be pre-declared by the operation before
         * it begins to execute.
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
         * The default is no timeout for locks.
         */
        public static final String DEFAULT_LOCK_TIMEOUT = "0";

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

    public void test_noResourcesDoesNotWait() throws Exception {
        
        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"5");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"10");
        properties.setProperty(TestOptions.MIN_LOCKS,"0");
        properties.setProperty(TestOptions.MAX_LOCKS,"0");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"true");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"true");
        
        Result result = doComparisonTest(properties);
        
        /*
         * Make sure that the tasks were not single threaded. ideally they will
         * run with full concurrency (NTHREADS == maxrunning).
         */
        assertTrue(Integer.parseInt(result.get("maxrunning"))==5);

    }
    
    /**
     * Test where each operation locks only a single resource (low concurrency
     * condition w/ 5 threads).
     */
    public void test_singleResourceLocking_lowConcurrency5() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"5");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }
    
    /**
     * Test where each operation locks only a single resource (default concurrency).
     */
    public void test_singleResourceLocking_defaultConcurrency20() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"20"); // whoops! was 5
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }
    
    /**
     * Test where each operation locks only a single resource (high concurrency).
     */
    public void test_singleResourceLocking_highConcurrency100() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"100");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }
    
    /**
     * Test where each operation locks only a single resource and there is only one
     * resource to be locked so that all operations MUST be serialized.
     */
    public void test_singleResourceLocking_serialized_lowConcurrency2() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"2");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized.
     */
    public void test_singleResourceLocking_serialized_lowConcurrency5() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"3");
        // Note: Small ntasks since this case otherwise is slow.
        properties.setProperty(TestOptions.NTASKS,"100");
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
//        properties.setProperty(TestOptions.LOCK_TIMEOUT,"0"); // Note: timeout==0 when debugging.
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized and
     * where 10% of all tasks die a horrid death.
     */
    public void test_singleResourceLocking_serialized_lowConcurrency5_withTaskDeath() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"3");
        // Note: Small ntasks since this case otherwise is slow.
        properties.setProperty(TestOptions.NTASKS,"100");
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
//        properties.setProperty(TestOptions.LOCK_TIMEOUT,"0"); // Note: timeout==0 when debugging.
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        properties.setProperty(TestOptions.PERCENT_TASK_DEATH,".10");
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized.
     */
    public void test_singleResourceLocking_serialized_highConcurrency() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"100");
        // Note: Small ntasks since otherwise this case takes very long.
        properties.setProperty(TestOptions.NTASKS,"100");
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
//        properties.setProperty(TestOptions.LOCK_TIMEOUT,"0");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized with
     * a non-zero lock timeout. This test stresses the logic in lock() that is
     * responsible for backing out a lock request on timeout.
     */
    public void test_singleResourceLocking_serialized_highConcurrency_lockTimeout() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"100");
        // Note: Small ntasks since otherwise this case takes very long.
        properties.setProperty(TestOptions.NTASKS,"100");
        properties.setProperty(TestOptions.NRESOURCES,"1");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        properties.setProperty(TestOptions.LOCK_TIMEOUT,"1000");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        Result result = doComparisonTest(properties);
        
        /*
         * Deadlocks should not be possible with only one resource.
         */
        assertEquals("ndeadlock","0",result.get("ndeadlock"));
        
    }

    /**
     * Test where each operation locks one or more resources.
     * <p>
     * Note: This condition provides the basis for deadlocks.
     * 
     * @throws Exception
     */
    public void test_multipleResourceLocking_resources3_locktries_3() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"20");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"3");
        properties.setProperty(TestOptions.MAX_LOCKS,"3");
        properties.setProperty(TestOptions.MAX_LOCK_TRIES,"3");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks one or more resources.
     * <p>
     * Note: This condition provides the basis for deadlocks. In fact, since we
     * have 10 resource locks for each operation and only 100 operations the
     * chances of a deadlock on any given operation are extremely high. However,
     * since we are predeclaring our locks and the lock requests are being
     * sorted NO deadlocks should result.
     * 
     * @throws Exception
     */
    public void test_multipleResourceLocking_resources10_locktries10_predeclareLocks() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"20");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"10");
        properties.setProperty(TestOptions.MAX_LOCKS,"10");
        properties.setProperty(TestOptions.MAX_LOCK_TRIES,"10");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"true");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"true");
                
        Result result = doComparisonTest(properties);
        
        /*
         * Deadlocks should not be possible when we predeclare and sort locks.
         */
        assertEquals("ndeadlock","0",result.get("ndeadlock"));

    }

    /**
     * Test where each operation locks one or more resources.
     * <p>
     * Note: This condition provides the basis for deadlocks. In fact, since we
     * have 10 resource locks for each operation and only 100 operations the
     * chances of a deadlock on any given operation are extremely high.
     * 
     * @throws Exception
     */
    public void test_multipleResourceLocking_resources10_locktries10() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"20");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"10");
        properties.setProperty(TestOptions.MAX_LOCKS,"10");
        properties.setProperty(TestOptions.MAX_LOCK_TRIES,"10");
        properties.setProperty(TestOptions.PREDECLARE_LOCKS,"false");
        properties.setProperty(TestOptions.SORT_LOCK_REQUESTS,"false");
                
        doComparisonTest(properties);
        
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
            defaultProperties.put(TestOptions.LOCK_TIMEOUT,"1000"); // ms

            List<Condition>conditions = new ArrayList<Condition>();

            // low concurrency.
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.NTHREADS,
                            "2") }));
            // and with a non-zero lock timeout.
            conditions.add(getCondition(defaultProperties, new NV[] {
                    new NV(TestOptions.NTHREADS, "2"),
                    new NV(TestOptions.LOCK_TIMEOUT, "1000") }));

            // default concurrency.
            conditions.add(getCondition(
                    defaultProperties, new NV[] {}));
            // and with a non-zero lock timeout.
            conditions.add(getCondition(defaultProperties, new NV[] { new NV(
                    TestOptions.LOCK_TIMEOUT, "1000") }));

            // default concurrency with 10% task death.
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.PERCENT_TASK_DEATH,
                            ".10") }));
            // and with a non-zero lock timeout.
            conditions.add(getCondition(
                    defaultProperties, new NV[] {
                    new NV(TestOptions.PERCENT_TASK_DEATH, ".10"),
                    new NV(TestOptions.LOCK_TIMEOUT, "1000") }));

            // high concurrency.
            conditions.add(getCondition(
                    defaultProperties, new NV[] { new NV(TestOptions.NTHREADS,
                            "100") }));
            // and with a non-zero lock timeout
            conditions.add(getCondition(defaultProperties, new NV[] {
                    new NV(TestOptions.NTHREADS, "100"),
                    new NV(TestOptions.LOCK_TIMEOUT, "1000") }));

            // high concurrency with 10% task death.
            conditions.add(getCondition(defaultProperties, new NV[] {
                    new NV(TestOptions.NTHREADS, "100"),
                    new NV(TestOptions.PERCENT_TASK_DEATH, ".10") }));
            // and with a non-zero lock timeout.
            conditions.add(getCondition(defaultProperties, new NV[] {
                    new NV(TestOptions.NTHREADS, "100"),
                    new NV(TestOptions.PERCENT_TASK_DEATH, ".10"),
                    new NV(TestOptions.LOCK_TIMEOUT, "1000") }));

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
                            "1"), new NV(TestOptions.LOCK_TIMEOUT,"1000") }));
            
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
                            "3"), new NV(TestOptions.LOCK_TIMEOUT,"1000") }));

            Experiment exp = new Experiment(className,defaultProperties,conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }

    }
    
}
