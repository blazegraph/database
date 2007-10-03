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

package com.bigdata.journal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase2;

import org.CognitiveWeb.concurrent.locking.DeadlockException;
import org.CognitiveWeb.concurrent.locking.TxDag;
import org.CognitiveWeb.concurrent.locking.LockContextManager.LockContext;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.service.DataService;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Stress test of concurrent unisolated operations on named indices.
 * 
 * FIXME Test suites and refactoring.
 * 
 * @todo add a stress/correctness test that mixes unisolated and isolated
 *       operations. Note that isolated operations (transactions) during commit
 *       simply acquire the corresponding unisolated index(s) (the transaction
 *       commit itself is just an unisolated operation on one or more indices).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConcurrentWritersOnNamedIndices extends TestCase2 {

    public static final Logger log = Logger.getLogger(TestConcurrentWritersOnNamedIndices.class);

    /**
     * 
     */
    public TestConcurrentWritersOnNamedIndices() {
        super();
    }

    public TestConcurrentWritersOnNamedIndices(String name) {
        super(name);
    }

    /**
     * Unbounded queue of tasks waiting to gain an exclusive lock on a resource.
     * Deadlocks are detected using a WAITS_FOR graph that is shared by all
     * resources and transactions for a given database instance.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo the queue elements could be strongly typed using a generic
     *       parameter.
     */
    static class ResourceQueue {

        /**
         * 
         */
        private static final long serialVersionUID = 6759702404466026405L;

        /**
         * The resource whose access is controlled by this object.
         */
        final private String resource;

        /**
         * The queue of transactions seeking access to the {@link #resource}.
         * The object at the head of the queue is the transaction with the lock
         * on the resource.
         */
        final BlockingQueue<Object/* tx */> queue = new LinkedBlockingQueue<Object/* tx */>(/* unbounded */);

        /**
         * Used to restrict access to {@link #queue}.
         */
        final Lock lock = new ReentrantLock();

        /**
         * The next transaction pending in the queue is
         * {@link Condition#signal() signaled} when a transaction releases its
         * lock.
         */
        final Condition available = lock.newCondition();

        /**
         * When true the {@link ResourceQueue} will refuse further operations.
         * This set by {@link #clear(Object)}.
         */
        final AtomicBoolean dead = new AtomicBoolean(false);

        /**
         * The WAITS_FOR graph shared by all transactions and all resources.
         */
        final TxDag waitsFor;

        /**
         * The resource whose locks are administeded by this object.
         */
        public String getResource() {
            
            return resource;
            
        }
        
        public String toString() {
            
            return getClass().getSimpleName()+"{resource="+resource+", queue="+queue.toString();
            
        }
        
        /**
         * 
         * @param resource
         *            The resource.
         * @param waitsFor
         *            The WAITS_FOR graph shared by all transactions and all
         *            resources.
         */
        ResourceQueue(String resource, TxDag waitsFor) {

            if (resource == null)
                throw new NullPointerException();

            if (waitsFor == null)
                throw new NullPointerException();

            this.resource = resource;

            this.waitsFor = waitsFor;

        }

        /**
         * Return iff the queue is alive.
         * <p>
         * Pre-condition: the caller owns {@link #lock}.
         * 
         * @exception IllegalStateException
         *                if the resource queue is dead.
         */
        private final void assertNotDead() {

            if (dead.get()) {

                throw new IllegalStateException("Dead");

            }

        }

        /**
         * Return iff the tx currently holds the lock on the resource.
         * <p>
         * Pre-condition: the caller owns {@link #lock}.
         * 
         * @exception IllegalStateException
         *                if the resource queue is dead.
         */
        private final void assertOwnsLock(Object tx) {

            if (queue.peek() != tx) {
                throw new IllegalStateException("Does not hold lock: " + tx);
            }

        }

//        public void lock() throws InterruptedException {
//            lock(Thread.currentThread());
//        }
//
//        public void unlock() {
//            unlock(Thread.currentThread());
//        }
//
//        public void clear() {
//            clear(Thread.currentThread());
//        }

        /**
         * Obtain a lock on the resource.
         * <p>
         * Note: Since blocked transactions do not run a transaction can be
         * pending a lock in at most one {@link ResourceQueue}.
         * 
         * @param tx
         *            The transaction.
         * 
         * @throws InterruptedException
         *             if the lock was interrupted (the transaction should
         *             handle this exception by aborting).
         * @throws DeadlockException
         *             if the request would cause a deadlock among the running
         *             transactions.
         */
        public void lock(Object tx) throws InterruptedException, DeadlockException {

            log.debug("enter");
            
            lock.lock();
            
            log.debug("have private lock");

            try {

                assertNotDead();

                // already locked.
                if (queue.peek() == tx) {
                 
                    log.info("Already owns lock");
                    
                    return;
                    
                }

                if(queue.isEmpty()) {

                    // the queue is empty so immediately grant the lock.
                    
                    queue.add(tx);
                    
                    log.info("Granted lock with empty queue");
                    
                    return;
                    
                }

                /*
                 * Update the WAITS_FOR graph since we are now going to wait on
                 * the tx that currently holds this lock. All we need to do is
                 * add an edge from this transaction to the transaction that
                 * currently holds the lock for this resource.
                 * 
                 * @todo should we add edges for each tx already in the queue
                 * since the new request in fact waits for each of those
                 * requests in turn since we are using exclusive locks?
                 */
                {
                
                    Object tgt = queue.peek();
                    
                    try {

                        waitsFor.addEdge(tx/*src*/, tgt);
                        
                    } catch(DeadlockException ex) {
                        
                        log.warn("Deadlock: "+this,ex);
                        
                        throw ex;
                        
                    }
                
                }

                // queue up a request to lock this resource.
                queue.add(tx);

                while (true) {

                    log.info("Awaiting resource");
                    
                    available.await();

                    if (dead.get()) {

                        throw new InterruptedException("Dead: " + resource);

                    }

                    if (queue.peek() == tx) {

                        // Note: tx remains at the head of the queue while
                        // it holds the lock.

                        log.info("Lock granted after wait");
                        
                        return;

                    }

                }

            } finally {

                lock.unlock();

                log.debug("released private lock");
                
            }

        }

        /**
         * Release the lock held by the tx on the resource.
         * 
         * @param tx
         *            The transaction.
         * 
         * @exception IllegalStateException
         *                if the transaction does not hold the lock.
         */
        public void unlock(Object tx) {

            log.debug("enter");
            
            lock.lock();

            log.debug("have private lock");
            
            try {

                assertNotDead();

                assertOwnsLock(tx);

                // remove the lock owner from the queue.
                if (queue.remove() != tx) {

                    throw new AssertionError();
                    
                }

                /*
                 * We just removed the granted lock. Now we have to update the
                 * WAITS_FOR graph to remove all edges whose source is a pending
                 * transaction (for this resource) since those transactions are
                 * waiting on the transaction that just released the lock.
                 */
                {

                    Iterator<Object> itr = queue.iterator();

                    while (itr.hasNext()) {

                        Object pendingTx = itr.next();

                        try {
                            waitsFor.removeEdge(pendingTx, tx);
                        } catch(Throwable t) {
                            /*
                             * Note: log the error but continue otherwise we
                             * will deadlock other tasks waiting on this
                             * resource since throwing the exception will mean
                             * that we do not invoke [available.signalAll()].
                             */
                            log.error(t.getMessage(), t);
                        }

                    }

                }

                if(queue.isEmpty()) {
                    
                    log.info("Nothing pending");
                    
                    return;
                    
                }
                
                log.info("Signaling blocked requestors");
                
                available.signalAll();

            } finally {

                lock.unlock();

                log.debug("released private lock");
                
            }

        }

        /**
         * Causes pending lock requests to abort (the threads that are blocked
         * will throw an {@link InterruptedException}) and releases the lock
         * held by the caller.
         * 
         * @param tx
         *            The transaction.
         * 
         * @exception IllegalStateException
         *                if the transaction does not hold the lock.
         */
        public void clear(Object tx) {

            lock.lock();

            try {

                assertNotDead();

                assertOwnsLock(tx);

                // FIXME This needs to remove any edge involving any pending tx.
                if (true)
                    throw new UnsupportedOperationException();

                queue.clear();

                dead.set(true);

                available.signalAll();

            } finally {

                lock.unlock();

            }

        }

    }

    /**
     * A test class used to model an instance of a database. The use of resource
     * names is constrained within the scope of the "database".
     * 
     * @todo do we need to use a lock on TxDag operations? They are all
     *       synchronized and atomic so maybe not? But maybe we do anyway for
     *       some things such as releasing a vertex.
     * 
     * @todo refactor to have Thread != Tx using thread local variables ala
     *       {@link LockContext}. Change the {@link #lockedResources} to be a
     *       {@link ThreadLocal} variable.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class Database {

        /**
         * Each resource that can be locked has an associated queue.
         */
        final ConcurrentHashMap<String/* resource */, ResourceQueue> resourceQueues = new ConcurrentHashMap<String, ResourceQueue>(
                1000/* nresources */);

        /**
         * The set of locks held by each transaction.
         */
        final ConcurrentHashMap<Thread, Collection<String/* resource */>> lockedResources;

        /**
         * Used to lock regions of code that add and drop resources.
         * 
         * @todo if used in lock()/releaseLocks() then we need to also use a
         *       {@link Condition} so that we do not block progress by other
         *       tasks while holding this lock.
         */
        final Lock resourceManagementLock = new ReentrantLock();

        /**
         * Used to track dependencies among transactions.
         */
        final TxDag waitsFor;

        // counters
        AtomicLong nstarted = new AtomicLong(0);

        AtomicLong nended = new AtomicLong(0);

        AtomicLong nerror = new AtomicLong(0);

        AtomicLong ndeadlock = new AtomicLong(0);

        // #of tasks holding all their locks.
        AtomicLong nrunning = new AtomicLong(0);
        
        // Maximum #of tasks all holding their locks at once.
        AtomicLong maxrunning = new AtomicLong(0); 
        
        /**
         * 
         * @param maxConcurrency
         *            The maximum multi-programming level.
         */
        Database(int maxConcurrency) {

            lockedResources = new ConcurrentHashMap<Thread, Collection<String>>(
                    maxConcurrency);

            waitsFor = new TxDag(maxConcurrency);

        }

        /**
         * Add a resource.
         * 
         * @param resource
         */
        void addResource(String resource) {

            resourceManagementLock.lock();

            try {

                if (resourceQueues.putIfAbsent(resource, new ResourceQueue(
                        resource, waitsFor)) != null) {

                    throw new IllegalStateException("Resource exists: "
                            + resource);

                }

            } finally {

                resourceManagementLock.unlock();

            }

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
        void dropResource(String resource) {

            resourceManagementLock.lock();

            try {

                Thread tx = Thread.currentThread();
                
                ResourceQueue resourceQueue = resourceQueues.get(resource);

                if (resourceQueue == null)
                    throw new IllegalArgumentException("No such resource: "
                            + resource);

                /*
                 * If the caller has the lock then aborts anyone waiting on that
                 * resource and releases the lock; otherwise throws an
                 * exception.
                 */
                resourceQueue.clear(tx);

                resourceQueues.remove(resource);

            } finally {

                resourceManagementLock.unlock();

            }

        }

        /**
         * FIXME make sure that we back out all locks on a deadlock exception.
         * 
         * @param resource
         * @throws InterruptedException
         */
        void lock(String resource) throws InterruptedException {

//            resourceManagementLock.lock();

            try {

                Thread tx = Thread.currentThread();

                ResourceQueue resourceQueue = resourceQueues.get(resource);

                if (resourceQueue == null)
                    throw new IllegalArgumentException("No such resource: "
                            + resource);

                resourceQueue.lock(tx);

                Collection<String> resources = lockedResources.get(tx);
                
                if(resources==null) {

                    resources = new LinkedList<String>();
                
                    lockedResources.put(tx, resources);
                    
                }
                
                resources.add(resource);

            } finally {

//                resourceManagementLock.unlock();

            }

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

            try {

                log.info("Releasing locks");

                Thread tx = Thread.currentThread();

                Collection<String> resources = lockedResources.remove(tx);

                if (resources == null) {

                    throw new AssertionError("Unknown tx: " + tx);

                }

                log.info("Releasing resource locks: resources="+resources);
                
                Iterator<String> itr = resources.iterator();

                while (itr.hasNext()) {

                    String resource = itr.next();

                    ResourceQueue resourceQueue = resourceQueues.get(resource);

                    if (resourceQueue == null)
                        throw new IllegalStateException(
                                "No queue for resource: " + resource);

                    resourceQueue.unlock(tx);

                }

                /*
                 * Release the vertex (if any) in the WAITS_FOR graph.
                 * 
                 * Note: A vertex is created iff a dependency chain is
                 * established. Therefore it is possible for a transaction to
                 * obtain a lock without a vertex begin created for that
                 * tranasaction. Hence it is Ok if this method returns [false].
                 */
                waitsFor.releaseVertex(tx);

            } catch (Throwable t) {

                log.error("Could not release locks: " + t, t);

            } finally {

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

            releaseLocks();

            log.info("Ended: nended=" + nended);

        }

        String status() {

            return "nthreads=" + waitsFor.capacity() + ", maxrunning="
                    + maxrunning + ", nrunning=" + nrunning + ", nstarted="
                    + nstarted + ", nended=" + nended + ", nerror=" + nerror
                    + ", ndeadlock=" + ndeadlock;
            
        }

    }

    /**
     * A test class used to model a task reading or writing on an index.
     */
    static abstract class ResourceTask implements Callable<Object> {

        final Database db;

        protected void lock(String resource) throws InterruptedException {
            log.info("Acquiring lock: " + resource);
            db.lock(resource);
            log.info("Acquired lock: " + resource);
        }

        protected void lock(String[] resource) throws InterruptedException {
            log.info("Acquiring locks: " + Arrays.toString(resource));
            for (int i = 0; i < resource.length; i++) {
                db.lock(resource[i]);
            }
            log.info("Acquired locks: " + Arrays.toString(resource));
        }

        public ResourceTask(Database db) {
            this.db = db;
        }

        final public Object call() throws Exception {

            db.didStart(this);

            final Object ret;
            try {

                /*
                 * Attempt to lock resources required by the task.
                 * 
                 * @todo experiment with N>1, but make sure that all locks on
                 * released when the deadlock exception is thrown.
                 */
                final int maxRetries = 1;
                boolean locked = false;
                DeadlockException ex2 = null;
                for (int i = 0; i < maxRetries && !locked; i++) {
                    try {

                        lock();
                        locked = true;

                    } catch (DeadlockException ex) {

                        /*
                         * A deadlock is not the same as an error since we
                         * can try to obtain the locks again.
                         */

                        db.ndeadlock.incrementAndGet();

                        ex2 = ex;

                    }
                }
                if (!locked) {

                    throw ex2;

                }
                
                /*
                 * Run the task now that we have the necessary lock(s).
                 */
                try {
                    
                    long nrunning = db.nrunning.incrementAndGet();

                    // Note: not really atomic and hence only approximate.
                    db.maxrunning.set(Math.max(db.maxrunning.get(), nrunning));
                    
                    System.err.println("#running="+db.nrunning);

                    try {

                        log.info(toString() + ": run - start");
                        
                        ret = run();
                        
                        log.info(toString() + ": run - end");
                                                
                    } catch (Throwable t) {
                        
                        // An error.
                        
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

        /**
         * Lock any resources required by the task. This is invoked before
         * {@link #run()}.
         * 
         * @throws Exception
         */
        abstract protected void lock() throws Exception;
        
        /**
         * Run the task (resources have already been locked and will be unlocked
         * on completion).
         * 
         * @throws Exception
         */
        abstract protected Object run() throws Exception;

    }

    static class SingleResourceTask extends ResourceTask {

        final String resource;

        SingleResourceTask(Database db, String name) {

            super(db);

            this.resource = name;

        }

        protected void lock() throws Exception {
            
            lock(resource);

        }
        
        protected Object run() throws Exception {

            synchronized (this) {
                wait(10/* milliseconds */);
            }

            return null;

        }

        public String toString() {

            return super.toString() + " resource=" + resource;

        }

    }

    static class MultipleResourceTask extends ResourceTask {

        final String[] resource;

        MultipleResourceTask(Database db, String[] resource) {

            super(db);

            this.resource = resource;

        }

        protected void lock() throws Exception {
            
            lock(resource);
            
        }

        protected Object run() throws Exception {

            synchronized (this) {
                wait(10/* milliseconds */);
            }

            return null;

        }

        public String toString() {

            return super.toString() + " resource=" + Arrays.toString(resource);

        }

    }

    /**
     * A test suite for single exclusive resource locking using only very simple
     * mechanisms.
     * 
     * @throws Exception
     */
    public void test_simpleSingleResourceLocking() throws Exception {

        final int nthreads = 10; // 100;
        final int ntasks = 100; // 10000;
        final int nresources = 10; // 100
        
        ExecutorService execService = Executors.newFixedThreadPool(nthreads,
                DaemonThreadFactory.defaultThreadFactory());
        
        Collection<Callable<Object>> tasks = new ArrayList<Callable<Object>>(ntasks);

        Database db = new Database(nthreads/* multi-programming level */);
        
        // distinct resource names. references are reused by reach task.
        final String [] resources = new String[nresources];

        for(int i=0; i<nresources; i++) {
            
            resources[i] = "resource"+i;
        
            db.addResource(resources[i]);
            
        }
        
        Random r = new Random();

        // create tasks
        for(int i=0; i<ntasks; i++) {

            String resource = "resource"+r.nextInt(Integer.MAX_VALUE) % nresources;
            
            tasks.add(new SingleResourceTask(db,resource));
            
        }

        // run everyone.
        log.info("invoking all tasks");
        List<Future<Object>> futures = execService.invokeAll(tasks);
        
        // normal shutdown (run to completion).
        log.info("Shutting down service");
        execService.shutdown();
        
        // await termination (should be rapid unless there is a deadlock).
        log.info("Awaiting termination");
        execService.awaitTermination(1, TimeUnit.SECONDS);

        // check the futures - deadlocks could be re-run.
        {
            int nerrors = 0;
            int ndeadlock = 0;
            Iterator<Future<Object>> itr = futures.iterator();
            while(itr.hasNext()) {
                Future<Object> future = itr.next();
                try {
                    future.get();
                } catch(ExecutionException ex) {
                    if(ex.getCause() instanceof DeadlockException) {
                        ndeadlock++;
                    } else {
                        nerrors++;
                        log.error("Task threw: "+ex);
                    }
                }
            }
            if(nerrors>0) {
                log.warn("There were "+nerrors+" errors and "+ndeadlock+" deadlocks");
            }
        }
        
        // Done.
        System.out.println(db.status());

        // deadlocks are impossible when tasks lock only a single resource.
        assertEquals("#deadlocks", 0L,db.ndeadlock.get());
        
    }

//    /**
//     * 
//     * @todo move this test into locking.concurrent.
//     * 
//     * @see LockContextManager
//     * 
//     * Goals:
//     * 
//     * 1. higher concurrency of unisolated operations on the ds/journal with
//     * group commit. this only requires a "lock" per writable named index, e.g.,
//     * an operation will lock exactly one resource. show consistency of the data
//     * in a suite of stress tests with varying #s of threads, tasks, and
//     * resources. Each task will lock exactly one resource - the unisolated
//     * named index on which it would write.  DO THIS W/O the TxDAG first and
//     * get group commit debugged before trying to work through the tx commit
//     * stuff, which requires the WAITS_FOR graph support.
//     * 
//     * 2. transaction processing integrated with unisolated operations. since a
//     * transaction MAY write on more than one index this requires a TxDAG and a
//     * queue of resources waiting to get into the granted group (which will
//     * always be a singleton since writes on unisolated named indices require
//     * exclusive locks). show that concurrency control never deadlocks in a
//     * suite of stress tests with varying #s of threads, tasks, resources, and
//     * resource locked per task. Again, a resource is a unisolated named index.
//     * 
//     * x. modify Schedule to use java.util.concurrent.locking.Lock and Condition
//     * rather than synchronized() and wait() so that the use of the latter
//     * mechanisms is does not interfere with Schedules. then try to modify Lock
//     * and Queue again to use a "granted" Condition so that we can avoid some of
//     * the problems arising from using the LockContext's monitor.
//     * 
//     * @todo test the ability to enforce locks on {@link Thread}s using
//     *       {@link ThreadLocal} variables based on the named indices that a
//     *       {@link Thread} seeks to acquire.
//     *       <p>
//     *       Competent decisions may be made using the Java concurrency
//     *       mechanisms for unisolated writers as long as the threads are
//     *       limited to writing on at most one named index. As soon as a thread
//     *       may require a lock on more than one named index we need a different
//     *       paradigm for granting locks.
//     *       <p>
//     *       The data service API restricts unisolated write tasks to a single
//     *       index at a time (the Journal does not), but transactions may still
//     *       write on multiple isolated indices. The transaction will run fine,
//     *       but deadlock becomes possible when it seeks to commit since that is
//     *       when it requires a lock on the unisolated version of each index on
//     *       which it has written during its isolated (active) phase.
//     * 
//     * @todo variation where multiple locks must be acquired before a task may
//     *       execute. ideally the task declares the locks that it needs rather
//     *       than requesting them one by one but the lock support that we have
//     *       only works on incremental declaration of locks (2PL).
//     * 
//     * @todo thread deadlock can appear with small #s of resources (1,2) and
//     *       modest to large #s of threads (20+). Eg, nthreads=20+,
//     *       ntasks=2000+, nresources=1.
//     * 
//     * Deadlock currently arises when a blocked transaction attempts to
//     * Queue.grantRequests() and another transaction is trying to synchronize()
//     * on the blocked transaction. The block transaction is being allowed to
//     * call grantRequests in an attempt to alleviate another source of deadlock -
//     * calling block() when you might already have a lock. A hand-over-hand lock
//     * might fix the latter.
//     * 
//     * @todo I have seen DeadlockExceptions at nthreads=100; ntasks=2000;
//     *       nresources=1. deadlock should be impossible with this test since we
//     *       never request more than a single lock, so there should not be any
//     *       WAITS_FOR cycles - everyone waits for the task with the lock!
//     */
//    public void test_singleResourceLocking() throws Exception {
//
//        final int nthreads = 20; //100;
//        final int ntasks = 100000; //10000;
//        final int nresources = 1;
//
//        /**
//         * A test class used to model an instance of a database.  The use of resource
//         * names is constrained within the scope of the "database".
//         * <p>
//         * Note: there is no concept of resource add/drop for this class.
//         * 
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//         * @version $Id$
//         */
//        class Database {
//
//            final LockContextManager lcm;
//            
//            /**
//             * 
//             * @param maxConcurrency
//             *            The maximum multi-programming level.
//             */
//            Database(int maxConcurrency) {
//                lcm = new LockContextManager(maxConcurrency);
//            }
//            
//            void lock(String resource) {
//                
//                LockContext.getCurrentThread().lock(resource, LockMode.X);
//                
//            }
//
////            void release(String resource) {
////
////                LockContext.getCurrentThread().unlock(resource);
////
////            }
//            
//        }
//        
//        /**
//         * A test class used to model a task reading or writing on an index.
//         */
//        abstract class ResourceTask implements Callable<Object> {
//
//            final Database db;
//            
//            protected void acquire(String resource) {
//                log.info("Acquiring lock: "+resource);
//                db.lock(resource);
//                log.info("Acquired lock: "+resource);
//            }
//
////            protected void release(String resource) {
////                log.info("Releasing lock: "+resource);
////                db.release(resource);
////                log.info("Released lock: "+resource);
////            }
//
//            public ResourceTask(Database db) {
//                this.db = db;
//            }
//
//            final public Object call() throws Exception {
//
//                log.info(toString()+": creating lock context");
//                db.lcm.createLockContext();
//                log.info(toString()+": created lock context");
//
//                final Object ret;
//                try {
//                
//                    log.info(toString()+": run - start");
//                    try {
//                        ret = run();
//                        log.info(toString()+": run - end");
//                    } catch(Throwable t) {
//                        log.error("Problem running task: "+this, t);
//                        throw new RuntimeException(t);
//                    }
//
//                } finally {
//                
//                    log.info(toString()+": releasing locks");
//                    try {
//                        LockContext.getCurrentThread().releaseLocks();
//                    } catch (Throwable t) {
//                        log.warn("" + t, t);
//                    }
//                    
//                }
//                
//                log.info(toString()+": done");
//
//                return ret;
//                
//            }
//            
//            abstract protected Object run() throws Exception;
//
//        }
//
//        class SingleResourceTask extends ResourceTask {
//
//            final String resource;
//            
//            SingleResourceTask(Database db,String name) {
//
//                super(db);
//                
//                this.resource = name;
//                
//            }
//            
//            protected Object run() throws Exception {
//
//                /*
//                 * @todo verify that no one has our monitor when we start. if
//                 * someone else is synchronized on us then the locking stuff has
//                 * failed to recognize that this thread released all of locks
//                 * the last time it ran a task.
//                 */
//
//                acquire(resource);
//                
//                synchronized(this) {
//                    wait(10/*milliseconds*/);
//                }
//
//                // This is not required - the caller handles the release of all locks.
////                release(resource);
//                
//                return null;
//                
//            }
//            
//            public String toString() {
//                
//                return super.toString()+" resource="+resource;
//                
//            }
//            
//        }
//        
//        ExecutorService execService = Executors.newFixedThreadPool(nthreads,
//                DaemonThreadFactory.defaultThreadFactory());
//        
//        Collection<Callable<Object>> tasks = new ArrayList<Callable<Object>>(ntasks);
//        
//        String [] resources = new String[nresources];
//        
//        for(int i=0; i<nresources; i++) {
//            
//            resources[i] = "resource"+i;
//            
//        }
//        
//        Database db = new Database(nthreads/*multi-programming level*/);
//        
//        for(int i=0; i<ntasks; i++) {
//            
//            tasks.add(new SingleResourceTask(db,""+resources[i%nresources]));
//            
//        }
//
//        // run everyone.
//        log.info("invoking all tasks");
//        List<Future<Object>> futures = execService.invokeAll(tasks);
//        
//        // normal shutdown (run to completion).
//        log.info("Shutting down service");
//        execService.shutdown();
//        
//        // await termination (should be rapid unless there is a deadlock).
//        log.info("Awaiting termination");
//        execService.awaitTermination(1, TimeUnit.SECONDS);
//
//        // check the futures.
//        {
//            int nerrors = 0;
//            Iterator<Future<Object>> itr = futures.iterator();
//            while(itr.hasNext()) {
//                Future<Object> future = itr.next();
//                try {
//                    future.get();
//                } catch(ExecutionException ex) {
//                    nerrors++;
//                    log.error("Task threw: "+ex);
//                }
//            }
//            if(nerrors>0) {
//                log.warn("There were "+nerrors+" errors");
//            }
//        }
//        
//        // Done.
//        log.info("Done");
//        
//    }
    
    /**
     * Stress test where each task may lock more than one resource (deadlocks
     * may arise).
     * <p>
     * Note: A "resource" is a named index (partition), so set nresources based
     * on your expectations for the #of index partitions on a journal or
     * federation.
     * <p>
     * Note: The likelyhood of deadlock increases as (a) more locks are
     * requested per task; and (b) fewer resources are available to be locked.
     * When minLocks==maxLocks==nresources then tasks will be serialized since
     * each task requires all resources in order to proceed.
     * 
     * @todo test to verify that we can interrupt running tasks.
     */
    public void test_multipleResourceLocking() throws Exception {

        final int nthreads = 2; // 100;
        final int ntasks = 100; // 10000;
        final int nresources = 3;
        final int minLocks = 3;
        final int maxLocks = 3;

        /*
         * Note: without pre-declaration of locks, you can expect high deadlock
         * rates when minLocks=maxLocks=nresources since all tasks will contend
         * for all resources and locks will be assigned incrementally.
         */
        assert minLocks <= maxLocks;
        assert minLocks <= nresources;
        assert maxLocks <= nresources;

        ExecutorService execService = Executors.newFixedThreadPool(nthreads,
                DaemonThreadFactory.defaultThreadFactory());

        Collection<Callable<Object>> tasks = new ArrayList<Callable<Object>>(
                ntasks);

        Database db = new Database(nthreads/* multi-programming level */);

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

            tasks.add(new MultipleResourceTask(db, resource));

        }

        // run everyone.
        log.info("invoking all tasks");
        List<Future<Object>> futures = execService.invokeAll(tasks);

        // normal shutdown (run to completion).
        log.info("Shutting down service");
        execService.shutdown();

        // await termination (should be rapid unless there is a deadlock).
        log.info("Awaiting termination");
        execService.awaitTermination(1, TimeUnit.SECONDS);

        // check the futures - deadlocks could be re-run.
        {
            int nerrors = 0;
            int ndeadlock = 0;
            Iterator<Future<Object>> itr = futures.iterator();
            while (itr.hasNext()) {
                Future<Object> future = itr.next();
                try {
                    future.get();
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof DeadlockException) {
                        ndeadlock++;
                    } else {
                        nerrors++;
                        log.error("Task threw: " + ex);
                    }
                }
            }
            if (nerrors > 0) {
                log.warn("There were " + nerrors + " errors and " + ndeadlock
                        + " deadlocks");
            }
        }

        // Done.
        System.out.println(db.status());

    }

//
//    /**
//     * Stress test where each task tries to lock one or more resources.
//     * 
//     * @todo Since transactions only acquire locks on the unisolated indices
//     *       during commit processing explore variant concurrency control
//     *       support that can leverage pre-declaration of locks. There is
//     *       probably a pre-declaration variant that can be designed that reuses
//     *       the {@link TxDag} and {@link Queue} but serializes and optimizes
//     *       the declaration of locks to one per transaction.
//     * 
//     * @todo this exception is thrown from time to time by this test. once it is
//     *       thrown the code develops an unresolvable condition in Queue#block
//     * 
//     * <pre>
//     *  final int nthreads = 3; //100;
//     *  final int ntasks = 1000; //10000;
//     *  final int nresources = 10;
//     *  final int minLocks = 1;
//     *  final int maxLocks = 3;
//     *   
//     *  java.lang.IllegalStateException: edge does not exist: src=org.CognitiveWeb.concurrent.locking.LockContextManager$LockContext@1a42792, tgt=org.CognitiveWeb.concurrent.locking.LockContextManager$LockContext@1a06e38
//     *      at org.CognitiveWeb.concurrent.locking.TxDag.removeEdge(TxDag.java:1352)
//     *      at org.CognitiveWeb.concurrent.locking.Queue.releaseLock(Queue.java:740)
//     *      at org.CognitiveWeb.concurrent.locking.Queue.unlock(Queue.java:828)
//     *      at org.CognitiveWeb.concurrent.locking.LockContextManager$LockContext.releaseLocks(LockContextManager.java:586)
//     *      at com.bigdata.journal.TestConcurrentWritersOnNamedIndices$2ResourceTask.call(TestConcurrentWritersOnNamedIndices.java:540)
//     * </pre>
//     * 
//     * @todo This exception gets thrown from time to time.
//     * 
//     * <pre>
//     *  (Same parameters as above).
//     *  
//     *  java.lang.IllegalStateException: unknown transaction: tx1=org.CognitiveWeb.concurrent.locking.LockContextManager$LockContext@29ac
//     *      at org.CognitiveWeb.concurrent.locking.TxDag.removeEdge(TxDag.java:1343)
//     *      at org.CognitiveWeb.concurrent.locking.Queue.releaseLock(Queue.java:745)
//     *      at org.CognitiveWeb.concurrent.locking.Queue.unlock(Queue.java:833)
//     *      at org.CognitiveWeb.concurrent.locking.LockContextManager$LockContext.releaseLocks(LockContextManager.java:586)
//     *      at com.bigdata.journal.TestConcurrentWritersOnNamedIndices$2ResourceTask.call(TestConcurrentWritersOnNamedIndices.java:559)
//     * </pre>
//     * 
//     * Since both errors appear when releasing locks perhaps the problem is that
//     * other transactions referenced in the WAITS_FOR graph have been
//     * asynchronously removed?
//     * 
//     * @throws Exception
//     */
//    public void test_multipleResourceLocking() throws Exception {
//
//        final int nthreads = 3; //100;
//        final int ntasks = 1000; //10000;
//        final int nresources = 10;
//        final int minLocks = 1;
//        final int maxLocks = 3;
//        
//        /*
//         * Note: when minLocks==maxLocks==nresources then tasks will be
//         * serialized since each task requires all resources in order to
//         * proceed.
//         * 
//         * Note: without pre-declaration of locks, you can expect high deadlock
//         * rates when minLocks=maxLocks=nresources since all tasks will contend
//         * for all resources and locks will be assigned incrementally.
//         */
//        assert minLocks<=maxLocks;
//        assert minLocks<=nresources;
//        assert maxLocks<=nresources;
//
//        /**
//         * A test class used to model an instance of a database. The use of
//         * resource names is constrained within the scope of the "database".
//         * <p>
//         * Note: there is no concept of resource add/drop for this class.
//         * 
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//         *         Thompson</a>
//         * @version $Id$
//         */
//        class Database {
//
//            final LockContextManager lcm;
//
//            // counters
//            AtomicLong nstarted = new AtomicLong(0);
//            AtomicLong nended = new AtomicLong(0);
//            AtomicLong nerror = new AtomicLong(0);
//            AtomicLong ndeadlock = new AtomicLong(0);
//            
//            /**
//             * 
//             * @param maxConcurrency
//             *            The maximum multi-programming level.
//             */
//            Database(int maxConcurrency) {
//                lcm = new LockContextManager(maxConcurrency);
//            }
//            
//            void lock(String resource) {
//                
//                LockContext.getCurrentThread().lock(resource, LockMode.X);
//                
//            }
//
////            void release(String resource) {
////
////                LockContext.getCurrentThread().unlock(resource);
////
////            }
//
//            void didStart(Callable<Object> task) {
//                long n = nstarted.incrementAndGet();
////                log.info("#started="+n);
//            }
//            
//            void didEnd(Callable<Object> task) {
//                long n = nended.incrementAndGet();
////                log.info("#ended="+n);
//            }
//
//            void didError(Callable<Object> task, Throwable t) {
//                long n = nerror.incrementAndGet();
////                log.warn("#error="+n);
//            }
//
//            String status() {
//                
//                return "nstarted="+nstarted+", nended="+nended+", nerror="+nerror+", ndeadlock="+ndeadlock;
//                
//            }
//            
//        }
//        
//        /**
//         * A test class used to model a task reading or writing on an index.
//         */
//        abstract class ResourceTask implements Callable<Object> {
//
//            final Database db;
//            
//            protected void acquire(String resource) {
//                log.info("Acquiring lock: "+resource);
//                db.lock(resource);
//                log.info("Acquired lock: "+resource);
//            }
//
//            protected void acquire(String[] resource) {
//                log.info("Acquiring locks: "+Arrays.toString(resource));
//                for(int i=0; i<resource.length; i++) {
//                    db.lock(resource[i]);
//                }
//                log.info("Acquired lock: "+resource);
//            }
//
////            protected void release(String resource) {
////                log.info("Releasing lock: "+resource);
////                db.release(resource);
////                log.info("Released lock: "+resource);
////            }
//
//            public ResourceTask(Database db) {
//                this.db = db;
//            }
//
//            final public Object call() throws Exception {
//
//                db.didStart(this);
//                log.info(toString()+": creating lock context");
//                db.lcm.createLockContext();
//                log.info(toString()+": created lock context");
//
//                final Object ret;
//                try {
//                
//                    log.info(toString()+": run - start");
//                    try {
//                        ret = run();
//                        log.info(toString()+": run - end");
//                    } catch(DeadlockException ex) {
//                        // A deadlock is not the same as an error - the tx can be retried.
//                        db.ndeadlock.incrementAndGet();
//                        throw ex;
//                    } catch(Throwable t) {
//                        // An error.
//                        log.error("Problem running task: "+this, t);
//                        db.didError(this,t);
//                        throw new RuntimeException(t);
//                    }
//
//                } finally {
//                
//                    log.info(toString()+": releasing locks");
//                    try {
//                        LockContext.getCurrentThread().releaseLocks();
//                    } catch (Throwable t) {
//                        log.error("Could not release locks: " + t, t);
//                    }
//                    db.didEnd(this);
//                    
//                }
//                
//                log.info(toString()+": done");
//
//                return ret;
//                
//            }
//            
//            abstract protected Object run() throws Exception;
//
//        }
//
//        class MultipleResourceTask extends ResourceTask {
//
//            final String[] resource;
//            
//            MultipleResourceTask(Database db,String[] resource) {
//
//                super(db);
//                
//                this.resource = resource;
//                
//            }
//            
//            protected Object run() throws Exception {
//
//                /*
//                 * @todo verify that no one has our monitor when we start. if
//                 * someone else is synchronized on us then the locking stuff has
//                 * failed to recognize that this thread released all of locks
//                 * the last time it ran a task.
//                 */
//
//                acquire(resource);
//                
//                synchronized(this) {
//                    wait(10/*milliseconds*/);
//                }
//
//                // This is not required - the caller handles the release of all locks.
////                release(resource);
//                
//                return null;
//                
//            }
//            
//            public String toString() {
//                
//                return super.toString()+" resource="+Arrays.toString(resource);
//                
//            }
//            
//        }
//        
//        ExecutorService execService = Executors.newFixedThreadPool(nthreads,
//                DaemonThreadFactory.defaultThreadFactory());
//        
//        Collection<Callable<Object>> tasks = new ArrayList<Callable<Object>>(ntasks);
//        
//        // distinct resource names. references are reused by reach task.
//        final String [] resources = new String[nresources];
//
//        for(int i=0; i<nresources; i++) {
//            
//            resources[i] = "resource"+i;
//            
//        }
//        
//        Database db = new Database(nthreads/*multi-programming level*/);
//        
//        Random r = new Random();
//
//        // create tasks; each will use between minLocks and maxLocks distinct
//        // resources.
//        for(int i=0; i<ntasks; i++) {
//
//            // #of locks that this task will seek to acquire.
//            final int nlocks = (minLocks == maxLocks ? minLocks : r.nextInt(maxLocks
//                    - minLocks)
//                    + minLocks);
//
////            final int nlocks = maxLocks;
//            
//            final String[] resource = new String[nlocks];
//            
//            // find a [nlocks] unique resources to lock for this task.
//            for(int j=0; j<nlocks; j++) {
//
//                int t;
//                while (true) {
//
//                    // random resource index.
//                    t = r.nextInt(Integer.MAX_VALUE) % nresources;
//
//                    // verify not duplicating existing resources for this task.
//                    
//                    boolean duplicate = false;
//
//                    for (int k = 0; k < j; k++) {
//
//                        if (resource[k] == resources[t]) {
//                         
//                            duplicate = true;
//                            
//                            break;
//                            
//                        }
//                        
//                    }
//
//                    if(!duplicate) break;
//                    
//                }
//                
//                resource[j] = resources[t];
//                
//            }
//            
//            tasks.add(new MultipleResourceTask(db,resource));
//            
//        }
//
//        // run everyone.
//        log.info("invoking all tasks");
//        List<Future<Object>> futures = execService.invokeAll(tasks);
//        
//        // normal shutdown (run to completion).
//        log.info("Shutting down service");
//        execService.shutdown();
//        
//        // await termination (should be rapid unless there is a deadlock).
//        log.info("Awaiting termination");
//        execService.awaitTermination(1, TimeUnit.SECONDS);
//
//        // check the futures - deadlocks could be re-run.
//        {
//            int nerrors = 0;
//            int ndeadlock = 0;
//            Iterator<Future<Object>> itr = futures.iterator();
//            while(itr.hasNext()) {
//                Future<Object> future = itr.next();
//                try {
//                    future.get();
//                } catch(ExecutionException ex) {
//                    if(ex.getCause() instanceof DeadlockException) {
//                        ndeadlock++;
//                    } else {
//                        nerrors++;
//                        log.error("Task threw: "+ex);
//                    }
//                }
//            }
//            if(nerrors>0) {
//                log.warn("There were "+nerrors+" errors and "+ndeadlock+" deadlocks");
//            }
//        }
//        
//        // Done.
//        log.info("Done: "+db.status());
//        
//    }
    
    /**
     * A refactor of some logic from {@link DataService} into the underlying
     * journal to support greater concurrency of operations on named indices.
     * 
     * @todo refactor into {@link AbstractJournal}.
     * 
     * @todo move the tx service into this class. this is used for running
     *       isolated transactions (read-only, read-committed, or read-write).
     *       since transactions never write on the unisolated indices during
     *       their "active" phase, transactions may be run with arbitrary
     *       concurrency. However, the commit phase of transactions must
     *       serialized. A transaction that requests a commit is placed onto a
     *       queue. Transactions are selected to commit once they have acquired
     *       a lock on the corresponding unisolated indices, thereby enforcing
     *       serialization of their write sets both among other transactions and
     *       among unisolated writers. The commit itself consists of the
     *       standard validation and merge phrases.
     * 
     * @todo tease apart "writePoolSize" (concurrent writers) from group commit
     *       (the commit point for a set of operations is deferred until the
     *       next scheduled commit and all operations then commit together).
     * 
     * @todo modify to extend AbstractJournal so that Journal will then extend
     *       this class (rename to AbstractConcurrentJournal). the layering is
     *       mainly important in terms of isolating the logic for concurrency
     *       control into a source file (this one). in fact, this should
     *       probably just be merged into {@link AbstractJournal} so that the
     *       concurrency contraints are always imposed on the application.
     * 
     * @todo move the status task into the AbstractJournal and just subclass for
     *       the DataService?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ConcurrentJournal extends Journal {

        /**
         * Pool of threads for handling concurrent transactions on named
         * indices. Transactions are not inherently limited in their
         * concurrency. The size of the thread pool for this service governs the
         * maximum practical concurrency for transactions.
         * <p>
         * Transactions always read from historical data and buffer their writes
         * until they commit. Transactions that commit MUST acquire unisolated
         * writable indices for each index on which the transaction has written.
         * Once the transaction has acquired those writable indices it then runs
         * its commit phrase as an unisolated operation on the
         * {@link #writeService}.
         */
        final protected ExecutorService txService;

        /**
         * Pool of threads for handling concurrent unisolated read operations on
         * named indices using <strong>historical</strong> data. Unisolated
         * read operations from historical data are not inherently limited in
         * their concurrency and do not conflict with unisolated writers. The
         * size of the thread pool for this service governs the maximum
         * practical concurrency for unisolated readers.
         * <p>
         * Note that unisolated read operations on the <strong>current</strong>
         * state of an index DO conflict with unisolated writes and such tasks
         * must be run as unisolated writers.
         * <p>
         * Note: unisolated readers of historical data do require the rention of
         * historical commit records (which may span more than one logical
         * journal) until the reader terminates.
         */
        final protected ExecutorService readService;

        /**
         * Pool of threads for handling concurrent unisolated write operations
         * on named indices. Unisolated writes are always performed against the
         * current state of the named index. Unisolated writes for the same
         * named index (or index partition) conflict and must be serialized. The
         * size of this thread pool and the #of distinct named indices together
         * govern the maximum practical concurrency for unisolated writers.
         * <p>
         * Serialization is acomplished by gaining a lock on the named index.
         */
//        final protected ExecutorService writeService;
        
        /**
         * Runs a {@link StatusTask} printing out periodic service status
         * information (counters).
         */
        final protected ScheduledExecutorService statusService;

        public void shutdown() {
            
            txService.shutdown();
            
            readService.shutdown();
            
            statusService.shutdown();
            
            super.shutdown();
            
        }
        
        public void shutdownNow() {
            
            txService.shutdownNow();
            
            readService.shutdownNow();

            statusService.shutdownNow();
            
            super.shutdownNow();
            
        }
        
        public ConcurrentJournal(Properties properties) {

            super(properties);

            String val;
            
            final int writeServicePoolSize;
            final boolean groupCommit;
            final long commitPeriod;

            final int txServicePoolSize;
            final int readServicePoolSize;
            
            // txServicePoolSize
            {
            
                val = properties.getProperty(Options.TX_SERVICE_POOL_SIZE,Options.DEFAULT_TX_SERVICE_POOL_SIZE);

                txServicePoolSize = Integer.parseInt(val);

                if (txServicePoolSize < 1 ) {

                    throw new RuntimeException("The '"
                            + Options.TX_SERVICE_POOL_SIZE
                            + "' must be at least one.");

                }

                log.info(Options.TX_SERVICE_POOL_SIZE+"="+txServicePoolSize);

            }
            
            // readServicePoolSize
            {

                val = properties.getProperty(Options.READ_SERVICE_POOL_SIZE,
                        Options.DEFAULT_READ_SERVICE_POOL_SIZE);

                readServicePoolSize = Integer.parseInt(val);

                if (readServicePoolSize < 1) {

                    throw new RuntimeException("The '"
                            + Options.READ_SERVICE_POOL_SIZE
                            + "' must be at least one.");

                }

                log.info(Options.READ_SERVICE_POOL_SIZE+"="+readServicePoolSize);
                
            }

            // groupCommit
            {

                val = properties.getProperty(Options.GROUP_COMMIT,
                        Options.DEFAULT_GROUP_COMMIT);

                groupCommit = Boolean.parseBoolean(val);

                log.info(Options.GROUP_COMMIT + "=" + groupCommit);

            }

            // commitPeriod
            {

                val = properties.getProperty(Options.COMMIT_PERIOD,
                        Options.DEFAULT_COMMIT_PERIOD);

                commitPeriod = Long.parseLong(val);

                if (commitPeriod < 1) {

                    throw new RuntimeException("The '" + Options.COMMIT_PERIOD
                            + "' must be at least one.");

                }

                log.info(Options.COMMIT_PERIOD + "=" + commitPeriod);

            }

            // writeServicePoolSize
            {

                if (groupCommit) {

                    val = properties.getProperty(
                            Options.WRITE_SERVICE_POOL_SIZE,
                            Options.DEFAULT_WRITE_SERVICE_POOL_SIZE);

                    writeServicePoolSize = Integer.parseInt(val);

                    if (writeServicePoolSize < 1) {

                        throw new RuntimeException("The '"
                                + Options.WRITE_SERVICE_POOL_SIZE
                                + "' must be at least one.");

                    }

                } else {

                    // only one thread when groupCommit is disabled.

                    writeServicePoolSize = 1;

                }

                log.info(Options.WRITE_SERVICE_POOL_SIZE + "="
                        + writeServicePoolSize);

            }

            // setup thread pool for concurrent transactions.
            txService = Executors.newFixedThreadPool(txServicePoolSize,
                    DaemonThreadFactory.defaultThreadFactory());

            // setup thread pool for unisolated read operations.
            readService = Executors.newFixedThreadPool(readServicePoolSize,
                    DaemonThreadFactory.defaultThreadFactory());

            /*
             * Setup thread pool for unisolated write operations.
             */

            if (!groupCommit) {

                /*
                 * Note: If groupCommit is disabled, then we always use a single
                 * threaded ExecutorService -- one that does not allow
                 * additional threads to be created.
                 * 
                 * @todo the tasks are currently being queued on the journal's
                 * write service, but they will need to be either queued on this
                 * write service instead or we will have to encapsulate the
                 * request to queue the writes such that the correct
                 * writeService is choose.
                 */

                ExecutorService writeService = Executors
                        .newSingleThreadExecutor(DaemonThreadFactory
                                .defaultThreadFactory());

            } else {

                /*
                 * When groupCommit is enabled we use a custom
                 * ThreadPoolExecutor. This class provides handshaking to (a)
                 * insure that at most one thread is running a task for a given
                 * named index (by acquiring a lock when a thread accepts a task
                 * for a named index and releasing the lock when the task
                 * completes); and (b) allows for the synchronization of worker
                 * tasks to support groupCommit (by forcing new tasks to pause
                 * if a groupCommit is scheduled).
                 * 
                 * Synchronization is accomplished by forcing threads to pause
                 * before execution if the commitPeriod has been exceeded.
                 * 
                 * Note: Task scheduling could be modified by changing the
                 * natural order of the work queue using a PriorityBlockingQueue
                 * (by ordering by transaction start time and placing start
                 * times of zero at the end of the order (with a secondary
                 * ordering on the time at which the request was queued so that
                 * the queue remains FIFO for unisolated writes). That might be
                 * necessary in order to have transactions commit with dispatch
                 * when there are heavy unisolated writes on the data service.
                 * 
                 * @todo create the class described here and initialize the
                 * writeService using that task. We should not need to otherwise
                 * modify the code in terms of how tasks are submitted to the
                 * write service. Write a test suite, perhaps for the class in
                 * isolation.
                 */
                ScheduledExecutorService writeService = new ConcurrentWritersAndGroupCommitThreadPoolExecutor(
                        writeServicePoolSize, DaemonThreadFactory
                                .defaultThreadFactory());

                /*
                 * Schedules a repeatable task that will perform group commit.
                 * 
                 * Note: The delay between commits is estimated based on the #of
                 * times per second (1000ms) that you can sync a backing file to
                 * disk.
                 * 
                 * @todo if groupCommit is used with forceOnCommit=No or with
                 * bufferMode=Transient then the delay can be smaller and that
                 * will reduce the latency of commit without any substantial
                 * penalty.
                 * 
                 * @todo The first ballpark estimates are (without task
                 * concurrency)
                 * 
                 * Transient := 63 operations per second.
                 * 
                 * Disk := 35 operations per second (w/ forceOnCommit).
                 * 
                 * Given those data, the maximum throughput for groupCommit
                 * would be around 60 operations per second (double the
                 * performance).
                 * 
                 * Enabling task concurrency could significantly raise the #of
                 * upper bound on the operations per second depending on the
                 * available CPU resources to execute those tasks and the extent
                 * to which the tasks operation on distinct indices and may
                 * therefore be parallelized.
                 */

                final long initialDelay = 100;
                final long delay = (long) (1000 / 35d);
                final TimeUnit unit = TimeUnit.MILLISECONDS;

                writeService.scheduleWithFixedDelay(new GroupCommitTask(),
                        initialDelay, delay, unit);

            }
        
            // schedule runnable for periodic status messages.
            // @todo status delay as config parameter.
            {
                
                final long initialDelay = 100;
                final long delay = 2500;
                final TimeUnit unit = TimeUnit.MILLISECONDS;
                
                statusService = Executors
                        .newSingleThreadScheduledExecutor(DaemonThreadFactory
                                .defaultThreadFactory());
        
                statusService.scheduleWithFixedDelay(new StatusTask(), initialDelay, delay, unit );
                
            }

        }
        
        /**
         * A {@link Runnable} that executes periodically, performing a commit of all
         * pending unisolated writes.
         * <p>
         * Note: If {@link #run()} throws an exception then the task will no longer
         * be run and commit processing WILL NOT be performed.
         * 
         * @see ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        protected class GroupCommitTask implements Runnable {

            public Object call() throws Exception {

                log.info("groupCommit");
                
                return null;
                
            }

            /**
             * Performs a commit of all pending unisolated writes.
             * 
             * @todo the operations that are pending commit all need to be on a
             *       queue somewhere - perhaps just paused awaiting a
             *       {@link Condition}. if the commit succeeds, then those
             *       operations all need to return normally. if the commit fails,
             *       then the operations all need to throw an exception (set an
             *       error message and then notify the {@link Condition}?).
             * 
             * @todo The ground state from which an unisolated operation begins
             *       needs to evolve after each unisolated operation that reaches
             *       its commit point successfully. this can be acomplished by
             *       holding onto the btree reference, or even just the address at
             *       which the metadata record for the btree was last written.
             *       <p>
             *       However, if an unisolated write fails for any reason on a given
             *       index then we MUST use the last successful check point for that
             *       index.
             *       <p>
             *       Due to the way in which the {@link BTree} class is written, it
             *       "steals" child references when cloning an immutable node or
             *       leaf prior to making modifications. This means that we must
             *       reload the btree from a metadata record if we have to roll back
             *       due to an abort of some unisolated operation.
             * 
             * @todo The requirement for global serialization of transactions may be
             *       relaxed only when it is known that the transaction writes on a
             *       limited set of data services, indices, or index partitions but
             *       it must obtain a lock on the appropriate resources before it
             *       may merge its write set onto the corresponding unisolated
             *       resources. Otherwise unisolated operations could be interleaved
             *       within a transaction commit and it would no longer be atomic.
             */
            public void run() {
                // TODO Auto-generated method stub
                
            }
            
        }
        
        /**
         * Writes out periodic status information.
         * 
         * @todo make extensible (StatusTaskFactory, but note that this is an
         *       inner class and requires access to the journal).
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        protected class StatusTask implements Runnable {
            
            public void run() {

                status();
                
            }
            
            /*
             * @todo Write out the queue depths, #of operations to date, etc.
             */
            public void status() {
                
                final long commitCounter = getCommitRecord().getCommitCounter();

                System.err.println("status: commitCounter=" + commitCounter);
                
            }
            
        }

    }
    
    /**
     * @todo refactor into {@link com.bigdata.journal.Options}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options extends com.bigdata.journal.Options {
        
        /**
         * <code>txServicePoolSize</code> - The #of threads in the pool
         * handling concurrent transactions.
         * 
         * @see #DEFAULT_TX_SERVICE_POOL_SIZE
         */
        public static final String TX_SERVICE_POOL_SIZE = "txServicePoolSize";
        
        /**
         * The default #of threads in the transaction service thread pool.
         */
        public final static String DEFAULT_TX_SERVICE_POOL_SIZE = "100";
        
        /**
         * <code>readServicePoolSize</code> - The #of threads in the pool
         * handling concurrent unisolated read requests on named indices.
         * 
         * @see #DEFAULT_READ_SERVICE_POOL_SIZE
         */
        public static final String READ_SERVICE_POOL_SIZE = "readServicePoolSize";
        
        /**
         * The default #of threads in the read service thread pool.
         */
        public final static String DEFAULT_READ_SERVICE_POOL_SIZE = "20";

        /**
         * The #of threads in the pool handling concurrent unisolated write on
         * named indices - this is forced to ONE (1) IFF {@link #GROUP_COMMIT}
         * is disabled.
         * 
         * @see #DEFAULT_WRITE_SERVICE_POOL_SIZE
         * @see #GROUP_COMMIT
         */
        public final static String WRITE_SERVICE_POOL_SIZE = "writeServicePoolSize"; 

        /**
         * The default #of threads in the write service thread pool (#of
         * concurrent index writers).
         */
        public final static String DEFAULT_WRITE_SERVICE_POOL_SIZE = "20";

        /**
         * <code>groupCommit</code> - A boolean property used to enable or
         * disable group commit. When <em>enabled</em>, concurrent unisolated
         * writes are allowed on named indices by a pool of worker threads (both
         * unisolated data service operations and transaction commit processing
         * are unisolated). Every {@link #COMMIT_PERIOD} milliseconds the writer
         * threads synchronize and unisolated writes are committed. When
         * <em>disabled</em>, only a single thread is available to process
         * writes on named indices.
         */
        public final static String GROUP_COMMIT = "groupCommit";
        
        /**
         * The default for {@link #GROUP_COMMIT}.
         */
        public final static String DEFAULT_GROUP_COMMIT = "false";
        
        /**
         * The #COMMIT_PERIOD determines the maximum latency (in milliseconds)
         * that a writer will block awaiting the next commit and is used IFF
         * {@link #GROUP_COMMIT} is enabled.
         */
        public final static String COMMIT_PERIOD = "commitPeriod";

        /**
         * The default {@link #COMMIT_PERIOD}.
         */
        public final static String DEFAULT_COMMIT_PERIOD = "100";
        
    }
    
    /**
     * A custom {@link ScheduledThreadPoolExecutor} used to support group
     * commit. This class provides handshaking to (a) insure that at most one
     * thread is running a task for a given named index (by acquiring a lock
     * when a thread accepts a task for a named index and releasing the lock
     * when the task completes); and (b) allows for the synchronization of
     * worker tasks to support groupCommit (the scheduled group commit task
     * acquires a {@link #pauseLock()} forcing new tasks to pause if a
     * groupCommit is scheduled).
     * <p>
     * Synchronization is accomplished by forcing threads to pause before
     * execution if the commitPeriod has been exceeded.
     * <p>
     * Note: Task scheduling could be modified by changing the natural order of
     * the work queue using a PriorityBlockingQueue (by ordering by transaction
     * start time and placing start times of zero at the end of the order (with
     * a secondary ordering on the time at which the request was queued so that
     * the queue remains FIFO for unisolated writes). That might be necessary in
     * order to have transactions commit with dispatch when there are heavy
     * unisolated writes on the data service.
     * 
     * @todo create the class descibed here and initialize the writeService
     *       using that task. We should not need to otherwise modify the code in
     *       terms of how tasks are submitted to the write service. Write a test
     *       suite, perhaps for the class in isolation.
     * 
     * FIXME we also need to make sure that concurrent writes are not permitted
     * on the same index.
     * 
     * @todo group commit can be realized on the {@link AbstractJournal} using
     *       (thread local?) locks on writable named indices. the lock is held
     *       until the abort or commit by the thread (or tx, which could use
     *       different threads but not more than one thread at once). write
     *       locks on indices do not conflict with read-committed or read-only
     *       transactions, but they do conflict with unisolated views of the
     *       index (both because the unisolated view is the writable view and
     *       because the index can not be read by another process concurrent
     *       with writing). adapt the test suites for the journal, esp. the
     *       StressTestConcurrent, to run on multiple indices at once.  develop
     *       a stress test that operates using all valid forms of isolation and
     *       verify that the correct locking is imposed.  group commit just adds
     *       a scheduled commit task on top of concurrent writers.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ConcurrentWritersAndGroupCommitThreadPoolExecutor extends ScheduledThreadPoolExecutor {

        public ConcurrentWritersAndGroupCommitThreadPoolExecutor(int corePoolSize) {
            super(corePoolSize);
        }

        public ConcurrentWritersAndGroupCommitThreadPoolExecutor(int corePoolSize,
                RejectedExecutionHandler handler) {
            super(corePoolSize, handler);
        }

        public ConcurrentWritersAndGroupCommitThreadPoolExecutor(int corePoolSize,
                ThreadFactory threadFactory) {
            super(corePoolSize, threadFactory);
        }

        public ConcurrentWritersAndGroupCommitThreadPoolExecutor(int corePoolSize,
                ThreadFactory threadFactory, RejectedExecutionHandler handler) {
            super(corePoolSize, threadFactory, handler);
        }

        /*
         * Support for pausing and resuming execution of new worker tasks.
         */
        private boolean isPaused;
        private ReentrantLock pauseLock = new ReentrantLock();
        private Condition unpaused = pauseLock.newCondition();

        /**
         * If task execution has been {@link #pause() paused} then
         * {@link Condition#await() awaits} someone to call {@link #resume()}.
         */
        protected void beforeExecute(Thread t, Runnable r) {
          super.beforeExecute(t, r);
          pauseLock.lock();
          try {
            while (isPaused) unpaused.await();
          } catch(InterruptedException ie) {
            t.interrupt();
          } finally {
            pauseLock.unlock();
          }
        }
      
        /**
         * Sets the flag indicating that new worker tasks must pause in
         * {@link #beforeExecute(Thread, Runnable)}.
         */
        public void pause() {
          pauseLock.lock();
          try {
            isPaused = true;
          } finally {
            pauseLock.unlock();
          }
        }

        /**
         * Notifies all paused tasks that they may now run.
         */
        public void resume() {
          pauseLock.lock();
          try {
            isPaused = false;
            unpaused.signalAll();
          } finally {
            pauseLock.unlock();
          }
        }

    }
    
}
