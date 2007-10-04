/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 1, 2007
 */

package com.bigdata.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.CognitiveWeb.concurrent.locking.DeadlockException;
import org.CognitiveWeb.concurrent.locking.TimeoutException;
import org.CognitiveWeb.concurrent.locking.TxDag;
import org.CognitiveWeb.concurrent.locking.LockContextManager.LockContext;
import org.apache.log4j.Logger;

import com.bigdata.concurrent.TestConcurrentJournal.ConcurrentJournal;
import com.bigdata.service.DataService;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Suite of stress tests of the concurrency control mechanisms (without the
 * database implementation).
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConcurrencyControl extends TestCase implements IComparisonTest {

    public static final Logger log = Logger.getLogger(TestConcurrencyControl.class);

    /**
     * 
     */
    public TestConcurrencyControl() {
        super();
    }

    public TestConcurrencyControl(String name) {
        super(name);
    }

    /**
     * Access to "resource"s is constrained using {@link ResourceQueue}s to
     * administer locks and {@link TxDag} to detect deadlocks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @param T
     *            The type of the object that represents an operation,
     *            transaction, etc. This is often a Thread.
     * 
     * @param R
     *            The type of the object that identifies a resource for the
     *            purposes of the locking system. This is typically the name of
     *            an index.
     * 
     * @todo abstract the relationship to the task and then refactor into the
     *       source tree for integration with {@link ConcurrentJournal}.
     */
    public static class LockManager</*T,*/R> {

        /**
         * Each resource that can be locked has an associated queue.
         * <p>
         * Note: This is a concurrent collection since new resources may be
         * added while concurrent operations resolve resources to their queues.
         */
        final Map<R, ResourceQueue<R, Thread>> resourceQueues = new ConcurrentHashMap<R, ResourceQueue<R, Thread>>(
                1000/* nresources */);

        /**
         * The set of locks held by each transaction.
         */
        final ConcurrentHashMap<Thread, Collection<R>> lockedResources;

//        /**
//         * Used to lock regions of code that add and drop resources.
//         * 
//         * @todo if used in lock()/releaseLocks() then we need to also use a
//         *       {@link Condition} so that we do not block progress by other
//         *       tasks while holding this lock.
//         */
//        final Lock resourceManagementLock = new ReentrantLock();

        /**
         * Used to track dependencies among transactions.
         */
        final TxDag waitsFor;

        // counters
        AtomicLong nstarted = new AtomicLong(0);

        AtomicLong nended = new AtomicLong(0);

        AtomicLong nerror = new AtomicLong(0);

        AtomicLong ndeadlock = new AtomicLong(0);

        AtomicLong ntimeout = new AtomicLong(0);

        // #of tasks holding all their locks.
        AtomicLong nrunning = new AtomicLong(0);
        
        // Maximum #of tasks all holding their locks at once.
        AtomicLong maxrunning = new AtomicLong(0); 
        
        /**
         * 
         * @param maxConcurrency
         *            The maximum multi-programming level.
         */
        LockManager(int maxConcurrency) {

            lockedResources = new ConcurrentHashMap<Thread, Collection<R>>(
                    maxConcurrency);

            waitsFor = new TxDag(maxConcurrency);

        }

        /**
         * Add a resource.
         * 
         * @param resource
         *            The resource.
         * 
         * @throws IllegalStateException
         *             if the resource already exists.
         */
        void addResource(R resource) {

//            resourceManagementLock.lock();
//
//            try {

            // synchronize before possible modification.
            synchronized(resourceQueues) {
            
                if (resourceQueues.containsKey(resource)) {

                    throw new IllegalStateException("Resource exists: "
                            + resource);

                }

                resourceQueues.put(resource, new ResourceQueue<R,Thread>(
                        resource, waitsFor));

            }
            
//            } finally {
//
//                resourceManagementLock.unlock();
//
//            }

        }

        /**
         * Drop a resource.
         * 
         * The caller must have lock on the resource. All tasks blocked waiting
         * for that resource will be aborted.
         * 
         * @param resource
         *            The resource.
         * 
         * @throws IllegalArgumentException
         *             if the resource does not exist.
         * @throws IllegalStateException
         *             if the caller does not have a lock on the resource.
         */
        void dropResource(R resource) {

//            resourceManagementLock.lock();

//            try {

            Thread tx = Thread.currentThread();

            // synchronize before possible modification.
            synchronized(resourceQueues) {
                
                ResourceQueue<R,Thread> resourceQueue = resourceQueues.get(resource);

                if (resourceQueue == null) {

                    throw new IllegalArgumentException("No such resource: "
                            + resource);
                    
                }

                /*
                 * If the caller has the lock then aborts anyone waiting on that
                 * resource and releases the lock; otherwise throws an
                 * exception.
                 */
                resourceQueue.clear(tx);

                resourceQueues.remove(resource);

            }
                
//            } finally {
//
//                resourceManagementLock.unlock();
//
//            }

        }

        /**
         * Lock resource(s).
         * <p>
         * Note: If you can not obtain the required lock(s) then you MUST use
         * {@link #releaseLocks()} to make sure that you release any locks that
         * you might have obtained.
         * 
         * @param resource
         *            The resource(s) to be locked.
         * @param timeout
         *            The lock timeout -or- 0L to wait forever.
         * 
         * @throws InterruptedException
         * @throws DeadlockException
         * @throws TimeoutException
         */
        public void lock(R[] resource, long timeout)
                throws InterruptedException, DeadlockException,
                TimeoutException {

            if(resource==null) throw new NullPointerException(); 
            
            for(int i=0; i<resource.length; i++) {
                
                if (resource[i] == null)
                    throw new NullPointerException();

            }

            if (timeout < 0)
                throw new IllegalArgumentException();
            
            log.info("Acquiring lock(s): " + resource);

            for (int i = 0; i < resource.length; i++) {

                lock(resource[i], timeout);

            }

            log.info("Acquired lock(s): " + resource);

        }
        
        /**
         * Obtain a lock on a resource.
         * 
         * @param resource
         *            The resource to be locked.
         * @param timeout
         *            The lock timeout -or- 0L to wait forever.
         * 
         * @throws InterruptedException
         */
        private void lock(R resource,long timeout) throws InterruptedException {

//            resourceManagementLock.lock();

//            try {

            Thread tx = Thread.currentThread();

            ResourceQueue<R,Thread> resourceQueue = resourceQueues.get(resource);

            if (resourceQueue == null)
                throw new IllegalArgumentException("No such resource: "
                        + resource);

            resourceQueue.lock(tx,timeout);

            Collection<R> resources = lockedResources.get(tx);
            
            if(resources==null) {

                resources = new LinkedList<R>();
            
                lockedResources.put(tx, resources);
                
            }
            
            resources.add(resource);

//            } finally {

//                resourceManagementLock.unlock();

//            }

        }

        /**
         * Release all locks.
         * 
         * @todo could be optimized to update the {@link TxDag} more efficiently
         *       on normal completion (success). The logic below works
         *       regardless of whether the tx completed normally or not but uses
         *       incremental updates of the WAITS_FOR graph.
         */
        void releaseLocks() {

//            resourceManagementLock.lock();

            Thread tx = Thread.currentThread();

            try {

                log.info("Releasing locks");

                Collection<R> resources = lockedResources.remove(tx);

                if (resources == null) {
                    
                    log.info("No locks: "+tx);

                    return;

                }

                /*
                 * @todo does releasing locks needs to be atomic so that blocked
                 * operations can not get moving until we have released all of
                 * our locks?  I would not think that this matters....
                 */

                log.info("Releasing resource locks: resources=" + resources);

                Iterator<R> itr = resources.iterator();

                while (itr.hasNext()) {

                    R resource = itr.next();

                    ResourceQueue<R, Thread> resourceQueue = resourceQueues
                            .get(resource);

                    if (resourceQueue == null)
                        throw new IllegalStateException(
                                "No queue for resource: " + resource);

                    try {

                        // release a lock on a resource.

                        resourceQueue.unlock(tx);

                    } catch (Throwable t) {

                        log.warn("Could not release lock", t);

                        // Note: release the rest of the locks anyway.

                        continue;

                    }

                }

            } catch (Throwable t) {

                log.error("Could not release locks: " + t, t);

            } finally {

                /*
                 * Release the vertex (if any) in the WAITS_FOR graph.
                 * 
                 * Note: A vertex is created iff a dependency chain is
                 * established. Therefore it is possible for a transaction to
                 * obtain a lock without a vertex begin created for that
                 * tranasaction. Hence it is Ok if this method returns [false].
                 */

                waitsFor.releaseVertex(tx);

//                resourceManagementLock.unlock();

            }

        }

        void didStart(Callable<Object> task) {

            nstarted.incrementAndGet();

            log.info("Started: nstarted=" + nstarted);

        }

        void didError(Callable<Object> task, Throwable t) {

            nerror.incrementAndGet();

            // log.warn("Error: #error="+nerror);

        }

        /**
         * Always invoked on task end regardless of success or error.
         */
        void didEnd(Callable<Object> task) {
            
            nended.incrementAndGet();

            try {

                /*
                 * Force release of locks (if any) and removal of the vertex (if
                 * any) from the WAITS_FOR graph.
                 */
                
                releaseLocks();
                
            } catch(Throwable t) {

                log.warn("Problem(s) releasing locks: "+t, t);
                
                didError(task,t);
                
            }

            log.info("Ended: nended=" + nended);

        }

        String status() {

            return "nthreads=" + waitsFor.capacity() + ", maxrunning="
                    + maxrunning + ", nrunning=" + nrunning + ", nstarted="
                    + nstarted + ", nended=" + nended + ", nerror=" + nerror
                    + ", ndeadlock=" + ndeadlock+", ntimeout="+ntimeout;
            
        }

    }

    /**
     * A test class used to model a task reading or writing on an index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @param R
     *            The type of the object that identifies a resource for the
     *            purposes of the locking system. This is typically the name of
     *            an index.
     */
    public static abstract class AbstractResourceTask<R> implements Callable<Object> {

        protected final LockManager<R> db;
        private final R[] resource;
        private int maxLockTries = 1;
        private long lockTimeout = 0L;

        public int setMaxLockTries(int newValue) {
            
            if(newValue<1) throw new IllegalArgumentException();
            
            int t = this.maxLockTries;
            
            this.maxLockTries = newValue;
            
            return t;
            
        }
        
        public int getMaxLockTries() {
            
            return maxLockTries;
            
        }

        public long setLockTimeout(long newValue) {
            
            long t = this.lockTimeout;
            
            this.lockTimeout = newValue;
            
            return t;
            
        }
        
        public long getLockTimeout() {
            
            return lockTimeout;
            
        }
        
        /**
         * 
         * @param db
         *            The lock manager.
         * 
         * @param resource
         *            The resource(s) to be locked.
         */
        protected AbstractResourceTask(LockManager<R> db, R[] resource) {

            if (db == null)
                throw new NullPointerException();

            if (resource == null)
                throw new NullPointerException();

            for (int i = 0; i < resource.length; i++) {

                if (resource[i] == null)
                    throw new NullPointerException();

            }
            
            this.db = db;
            
            this.resource = resource;
            
        }

        /**
         * Attempt to acquire locks on resources required by the task.
         * <p>
         * Up to {@link #getMaxLockTries()} attempts will be made.
         * 
         * @exception DeadlockException
         *                if the locks could not be acquired (last exception
         *                encountered only).
         * @exception TimeoutException
         *                if the locks could not be acquired (last exception
         *                encountered only).
         */
        protected void acquireLocks() throws Exception {
            
            boolean locked = false;
            
            RuntimeException ex2 = null;
            
            for (int i = 0; i < maxLockTries && !locked; i++) {
            
                try {

                    db.lock(resource,lockTimeout);

                    locked = true;

                } catch (DeadlockException ex) {

                    /*
                     * A deadlock is not the same as an error since we
                     * can try to obtain the locks again.
                     */

                    db.ndeadlock.incrementAndGet();

                    ex2 = ex;

                    db.releaseLocks();
                    
                } catch (TimeoutException ex) {

                    /*
                     * A timeout is not the same as an error since we can
                     * try to obtain the locks again.
                     */

                    db.ntimeout.incrementAndGet();

                    ex2 = ex;

                    db.releaseLocks();

                }
                
            }
            
            if (!locked) {

                db.didError(this, ex2);
                
                throw ex2;

            }

        }
        
        
        final public Object call() throws Exception {

            db.didStart(this);

            final Object ret;
            try {

                acquireLocks();
                
                /*
                 * Run the task now that we have the necessary lock(s).
                 */
                try {
                    
                    long nrunning = db.nrunning.incrementAndGet();

                    // Note: not really atomic and hence only approximate.
                    db.maxrunning.set(Math.max(db.maxrunning.get(), nrunning));
                    
//                    System.err.println("#running="+db.nrunning);

                    try {

                        log.info(toString() + ": run - start");
                        
                        ret = run();
                        
                        log.info(toString() + ": run - end");
                                                
                    } catch (Throwable t) {

                        if(t instanceof HorridTaskDeath) {

                            // An expected error.
                            
                            db.didError(this, t);

                            throw (HorridTaskDeath)t;
                            
                        }

                        // An unexpected error.

                        log.error("Problem running task: " + this, t);
                        
                        db.didError(this, t);

                        throw new RuntimeException(t);
                    
                    }

                } finally {
                
                    db.nrunning.decrementAndGet();
                    
                }

            } finally {

                db.didEnd(this);

            }

            log.info(toString() + ": done");

            return ret;

        }

        public String toString() {

            return super.toString() + " resources=" + Arrays.toString(resource);

        }

        /**
         * Run the task (resources have already been locked and will be unlocked
         * on completion).
         * 
         * @throws Exception
         */
        abstract protected Object run() throws Exception;

    }

    /**
     * Waits 10ms once it acquires its locks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Wait10ResourceTask<R> extends AbstractResourceTask<R> {

        Wait10ResourceTask(LockManager<R> db, R[] resource) {

            super(db,resource);

        }

        protected Object run() throws Exception {

            synchronized (this) {
                wait(10/* milliseconds */);
            }

            return null;

        }

    }
    
    /**
     * Dies once it acquires its locks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DeathResourceTask<R> extends AbstractResourceTask<R> {

        DeathResourceTask(LockManager<R> db, R[] resource) {

            super(db,resource);

        }

        protected Object run() throws Exception {

            throw new HorridTaskDeath();

        }

    }
    
    /**
     * Thrown by {@link DeathResourceTask} in some test cases.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class HorridTaskDeath extends Exception {

        /**
         * 
         */
        private static final long serialVersionUID = 6798406260395237402L;
        
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

        LockManager<String> db = new LockManager<String>(nthreads/* multi-programming level */);

        // distinct resource names. references are reused by reach task.
        final String[] resources = new String[nresources];

        for (int i = 0; i < nresources; i++) {

            resources[i] = "resource" + i;

            db.addResource(resources[i]);
            
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

            final AbstractResourceTask<String> task;
            
            if(r.nextDouble()<percentTaskDeath) {
            
                task = new DeathResourceTask<String>(db, resource);
                
            } else {
                
                task = new Wait10ResourceTask<String>(db, resource);
                
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
        System.out.println(db.status());

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
     * Options for {@link TestConcurrencyControl#doComparisonTest(Properties)}.
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
        
    }
    
    public void setUpComparisonTest(Properties properties) throws Exception {

    }

    public void tearDownComparisonTest() throws Exception {
        
    }

    /**
     * Test where each operation locks only a single resource (low concurrency).
     */
    public void test_singleResourceLocking_lowConcurrency5() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"5");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        
        doComparisonTest(properties);
        
    }
    
    /**
     * Test where each operation locks only a single resource (default concurrency).
     */
    public void test_singleResourceLocking_defaultConcurrency20() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"5");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        properties.setProperty(TestOptions.MAX_LOCKS,"1");
        
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
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks only a single resource and there is only
     * one resource to be locked so that all operations MUST be serialized with
     * a non-zero lock timeout.
     * 
     * @todo throws exceptions IFF the lock timeout is non-zero, e.g., 1000ms.
     * 
     * <pre>
     * WARN : com.bigdata.concurrent.ResourceQueue.unlock(ResourceQueue.java:481): unknown transaction: tx1=Thread[pool-1-thread-241,5,main]
     * java.lang.IllegalStateException: unknown transaction: tx1=Thread[pool-1-thread-241,5,main]
     *     at org.CognitiveWeb.concurrent.locking.TxDag.removeEdge(TxDag.java:1346)
     *     at com.bigdata.concurrent.ResourceQueue.unlock(ResourceQueue.java:469)
     *     at com.bigdata.concurrent.TestConcurrencyControl$LockManager.releaseLocks(TestConcurrencyControl.java:446)
     *     at com.bigdata.concurrent.TestConcurrencyControl$LockManager.didEnd(TestConcurrencyControl.java:513)
     *     at com.bigdata.concurrent.TestConcurrencyControl$AbstractResourceTask.call(TestConcurrencyControl.java:745)
     *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:269)
     *     at java.util.concurrent.FutureTask.run(FutureTask.java:123)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:650)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:675)
     *     at java.lang.Thread.run(Thread.java:595)
     * </pre>
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
        
        doComparisonTest(properties);
        
    }

    /**
     * Test where each operation locks one or more resources.
     * 
     * @throws Exception
     */
    public void test_multipleResourceLocking() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTHREADS,"20");
        properties.setProperty(TestOptions.NTASKS,"1000");
        properties.setProperty(TestOptions.NRESOURCES,"100");
        properties.setProperty(TestOptions.MIN_LOCKS,"3");
        properties.setProperty(TestOptions.MAX_LOCKS,"3");
        
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
     * @todo write tests above and here that test conditions with and without
     *       lock timeout to make sure that the timeout itself does not put the
     *       CC system into an unhappy state!
     */
    static public class Generate extends ExperimentDriver {
        
        /**
         * Generates an XML file that can be run by {@link ExperimentDriver}.
         * 
         * @param args
         */
        public static void main(String[] args) throws Exception {
            
            // this is the test to be run.
            String className = TestConcurrencyControl.class.getName();
            
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
