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
 * Created on Oct 3, 2007
 */

package com.bigdata.concurrent;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.CognitiveWeb.concurrent.locking.DeadlockException;
import org.CognitiveWeb.concurrent.locking.TimeoutException;
import org.CognitiveWeb.concurrent.locking.TxDag;
import org.apache.log4j.Logger;

import com.bigdata.cache.WeakValueCache;

/**
 * This class coordinates a schedule among concurrent operations requiring
 * exclusive access to shared resource. Whenever possible, the result is a
 * concurrent schedule - that is, operations having non-overlapping lock
 * requirements run concurrently while operations that have lock contentions
 * are queued behind operations that currently have locks on the relevant
 * resources. A {@link ResourceQueue} is created for each resource and used
 * to block operations that are awaiting a lock. When locks are not being
 * pre-declared, a {@link TxDag WAITS_FOR} graph is additionally used to
 * detect deadlocks.
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
 * @todo Support escalation of operation priority based on time and
 *       scheduling of higher priority operations. the latter is done by
 *       queueing lock requests in front of pending requests for each
 *       resource on which an operation attempt to gain a lock. The former
 *       is just a dynamic adjustment of the position of the operation in
 *       the resource queue where it is awaiting a lock (an operation never
 *       awaits more than one lock at a time). This facility could be used
 *       to give priority to distributed transactions over local unisolated
 *       operations and to priviledge certain operations that have low
 *       latency requirements. This is not quite a "real-time" guarentee
 *       since the VM is not (normally) providing real-time guarentees and
 *       since we are not otherwise attempting to ensure anything except
 *       lower latency when compared to other operations awaiting their own
 *       locks.
 */
public class LockManager</*T,*/R extends Comparable<R>> {

    protected static final Logger log = Logger.getLogger(LockManager.class);

    /**
     * Each resource that can be locked has an associated {@link ResourceQueue}.
     * <p>
     * Note: This is a concurrent collection since new resources may be added
     * while concurrent operations resolve resources to their queues.
     * 
     * @todo I expect the #of resources to be on the order of 1000s at most for
     *       a given data service, but it could be larger on a transaction
     *       server. Consider adding necessary synchronization for updates and
     *       using a {@link WeakValueCache} so that {@link ResourceQueue}s that
     *       are not in use are not taking up memory. Each operation that is
     *       using a resource would also have to hold a hard reference to the
     *       corresponding {@link ResourceQueue} so that {@link ResourceQueue}s
     *       are not cleared while they are in use.
     */
    final ConcurrentHashMap<R, ResourceQueue<R, Thread>> resourceQueues = new ConcurrentHashMap<R, ResourceQueue<R, Thread>>(
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
     * True iff locks MUST be predeclared by the operation - this is a special
     * case of 2PL (two-phrase locking) that allows significant optimizations
     * and avoids the possibility of deadlock altogether.
     */
    final private boolean predeclareLocks;

    /**
     * When true, the resources in a lock request are sorted before the lock
     * requests are made to the various resource queues. This option is ONLY
     * turned off for testing purposes as it ALWAYS reduces the chance of
     * deadlocks and eliminates it entirely when locks are also predeclared.
     */
    final private boolean sortLockRequests;

    /**
     * Used to track dependencies among transactions.
     */
    final private TxDag waitsFor;

    /*
     * counters
     */

    /**
     * The #of tasks that start execution (enter
     * {@link LockManagerTask#call()}). This counter is incremented
     * BEFORE the task attempts to acquire its resource lock(s).
     */
    AtomicLong nstarted = new AtomicLong(0);

    /**
     * The #of tasks that end execution (exit
     * {@link LockManagerTask#call()}).
     */
    AtomicLong nended = new AtomicLong(0);

    /**
     * The #of tasks that had an error condition.
     */
    AtomicLong nerror = new AtomicLong(0);

    /**
     * The #of tasks that deadlocked when they attempted to acquire their
     * locks. Note that a task MAY retry lock acquisition and this counter
     * will be incremented each time it does so and then deadlocks.
     */
    AtomicLong ndeadlock = new AtomicLong(0);

    /**
     * The #of tasks that timed out when they attempted to acquire their
     * locks. Note that a task MAY retry lock acquisition and this counter
     * will be incremented each time it does so and then times out.
     */
    AtomicLong ntimeout = new AtomicLong(0);

    /**
     * #of tasks that are concurrently executing in
     * {@link LockManagerTask#run()}.
     */
    AtomicLong nrunning = new AtomicLong(0);

    /**
     * The maximum observed value of {@link #nrunning}.
     */
    AtomicLong maxrunning = new AtomicLong(0);

    /**
     * Create a lock manager for resources and concurrent operations.
     * <p>
     * Note that there is no concurrency limit imposed by the
     * {@link LockManager} when predeclareLocks is true as deadlocks are
     * impossible and we do not maintain a WAITS_FOR graph.
     * 
     * @param maxConcurrency
     *            The maximum multi-programming level (ignored if
     *            predeclareLocks is true).
     * 
     * @param predeclareLocks
     *            When true operations MUST declare all locks before they
     *            begin to execute. This makes possible several efficiencies
     *            and by sorting the resources in each lock request into a
     *            common order we are able to avoid deadlocks entirely.
     */
    public LockManager(int maxConcurrency, boolean predeclareLocks) {

        this(maxConcurrency, predeclareLocks, true/* sortLockRequests */);

    }

    /**
     * Create a lock manager for resources and concurrent operations.
     * <p>
     * Note that there is no concurrency limit imposed by the
     * {@link LockManager} when predeclareLocks is true as deadlocks are
     * impossible and we do not maintain a WAITS_FOR graph.
     * 
     * @param maxConcurrency
     *            The maximum multi-programming level (ignored if
     *            predeclareLocks is true).
     * 
     * @param predeclareLocks
     *            When true operations MUST declare all locks before they
     *            begin to execute. This makes possible several efficiencies
     *            and by sorting the resources in each lock request into a
     *            common order we are able to avoid deadlocks entirely.
     * 
     * @param sortLockRequests
     *            This option indicates whether or not the resources in a
     *            lock request will be sorted before attempting to acquire
     *            the locks for those resources. Normally <code>true</code>
     *            this option MAY be disabled for testing purposes. It is an
     *            error to disable this option if <i>predeclareLocks</i> is
     *            <code>false</code>.
     */
    LockManager(int maxConcurrency, boolean predeclareLocks,
            boolean sortLockRequests) {

        if (maxConcurrency < 2 && !predeclareLocks) {

            throw new IllegalArgumentException(
                    "maxConcurrency: must be 2+ unless you are predeclaring locks, not "
                            + maxConcurrency);

        }

        if (predeclareLocks && !sortLockRequests) {

            /*
             * This is required since we do not maintain TxDag when locks
             * are predeclare and therefore can not detect deadlocks.
             * Sorting with predeclared locks avoids the possibility of
             * deadlocks so we do not need the TxDag (effectively, it means
             * that all locks that can be requested by an operation are
             * sorted since they are predeclared and acquired in one go).
             */

            throw new IllegalArgumentException(
                    "Sorting of lock requests MUST be enabled when locks are being predeclared.");

        }

        this.predeclareLocks = predeclareLocks;

        this.sortLockRequests = sortLockRequests;

        lockedResources = new ConcurrentHashMap<Thread, Collection<R>>(
                maxConcurrency);

        if (predeclareLocks) {

            /*
             * Note: waitsFor is NOT required if we will acquire all locks
             * at once for a given operation since we can simply sort the
             * lock requests for each operation into a common order, thereby
             * making deadlock impossible!
             * 
             * Note: waitsFor is also NOT required if we are using only a
             * single threaded system.
             * 
             * Note: if you allocate waitsFor here anyway then you can
             * measure the cost of deadlock detection. As far as I can tell
             * it is essentially zero when locks are predeclared.
             */

            waitsFor = null;

            //                waitsFor = new TxDag(maxConcurrency);

        } else {

            /*
             * Construct the directed graph used to detect deadlock cycles.
             */

            waitsFor = new TxDag(maxConcurrency);

        }

    }

    /**
     * Add resource(s).
     * 
     * @param resource
     *            The resource(s).
     */
    public void addResource(R[] resource) {
        
        for(int i=0; i<resource.length; i++) {
            
            addResource(resource[i]);
            
        }
        
    }
    
    /**
     * Add a resource.
     * 
     * @param resource
     *            The resource.
     * 
     * @return true if the resource was not already declared.
     */
    public boolean addResource(R resource) {

        //            resourceManagementLock.lock();
        //
        //            try {

//        // synchronize before possible modification.
//        synchronized (resourceQueues) {

            if (resourceQueues.containsKey(resource)) {

                /*
                 * test 1st to avoid creating a resource queue if we do not need
                 * one.
                 */
                
                return false;

            }

            ResourceQueue<R, Thread> resourceQueue = new ResourceQueue<R, Thread>(resource,
                waitsFor);
            
            ResourceQueue tmp = resourceQueues.putIfAbsent(resource, resourceQueue );

            return tmp == null;
            
//        }

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
        synchronized (resourceQueues) {

            ResourceQueue<R, Thread> resourceQueue = resourceQueues
                    .get(resource);

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
     *             If the operation is interrupted while awaiting a lock.
     * @throws DeadlockException
     *             If the lock request would cause a deadlock.
     * @throws TimeoutException
     *             If the lock request times out.
     * @throws IllegalStateException
     *             If locks are being predeclared and there are already
     *             locks held by the operation.
     */
    void lock(R[] resource, long timeout) throws InterruptedException,
            DeadlockException, TimeoutException {

        if (resource == null) {

            throw new NullPointerException();

        }

        for (int i = 0; i < resource.length; i++) {

            if (resource[i] == null) {

                throw new NullPointerException();

            }

        }

        if (timeout < 0)
            throw new IllegalArgumentException();

        if (resource.length == 0)
            return; // NOP.

        Thread tx = Thread.currentThread();

        if (predeclareLocks) {

            // verify that no locks are held for this operation.
            Collection<R> resources = lockedResources.get(tx);

            if (resources != null) {

                // the operation are already declared its locks.

                throw new IllegalStateException(
                        "Operation already has lock(s): " + tx);

            }

        }

        if (resource.length > 1 && sortLockRequests) {

            /*
             * Sort the resources in the lock request.
             * 
             * Note: Sorting the resources reduces the chance of a deadlock and
             * excludes it entirely when predeclaration of locks is also used.
             * 
             * Note: We clone the resources to avoid side-effects on the caller.
             * 
             * Note: This will throw an exception if the "resource" does not
             * implement Comparable.
             */

            resource = resource.clone();
            
            Arrays.sort(resource);

        }

        log.info("Acquiring lock(s): " + resource);

        for (int i = 0; i < resource.length; i++) {

            lock(tx, resource[i], timeout);

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
    private void lock(Thread tx, R resource, long timeout)
            throws InterruptedException {

        //            resourceManagementLock.lock();

        //            try {

        ResourceQueue<R, Thread> resourceQueue = resourceQueues.get(resource);

        if (resourceQueue == null)
            throw new IllegalArgumentException("No such resource: " + resource);

        resourceQueue.lock(tx, timeout);

        Collection<R> resources = lockedResources.get(tx);

        if (resources == null) {

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
     * @param waiting
     *            <code>false</code> iff the operation was <strong>known</strong>
     *            to be running. Otherwise <code>true</code> to indicate
     *            that the operation is awaiting a lock. An optimization is
     *            used to update the {@link TxDag} when the operation is NOT
     *            waiting. Since that optimization is invalid when the
     *            operation is waiting, always specify <code>true</code>
     *            if you are not sure and the less efficient technique will
     *            be used to update the {@link TxDag}.
     * 
     * @todo The [waiting] flag is not being used to optimize the removal of
     *       edges from the WAITS_FOR graph. Fixing this will require us to
     *       remove the operation from each {@link ResourceQueue} without
     *       updating the {@link TxDag} and then update the {@link TxDag}
     *       using {@link TxDag#removeEdges(Object, boolean)} and specifying
     *       "false" for "waiting".  Since this operation cuts across multiple
     *       queues at once additional synchronization MAY be required.
     */
    void releaseLocks(boolean waiting) {

        //            resourceManagementLock.lock();

        Thread tx = Thread.currentThread();

        try {

            log.info("Releasing locks");

            Collection<R> resources = lockedResources.remove(tx);

            if (resources == null) {

                log.info("No locks: " + tx);

                return;

            }

            /*
             * Note: The way this is written releasing locks is not atomic.
             * This means that blocked operations can start as soon as a
             * resource becomes available rather than waiting until the
             * operation has released all of its locks. I don't think that
             * there are any negative consequences to this.
             */

            log
                    .info("Releasing resource locks: resources=" + resources);

            Iterator<R> itr = resources.iterator();

            while (itr.hasNext()) {

                R resource = itr.next();

                ResourceQueue<R, Thread> resourceQueue = resourceQueues
                        .get(resource);

                if (resourceQueue == null)
                    throw new IllegalStateException("No queue for resource: "
                            + resource);

                try {

                    // release a lock on a resource.

                    resourceQueue.unlock(tx);

                } catch (Throwable t) {

                    log
                            .warn("Could not release lock", t);

                    // Note: release the rest of the locks anyway.

                    continue;

                }

            }

        } catch (Throwable t) {

            log
                    .error("Could not release locks: " + t, t);

        } finally {

            /*
             * Release the vertex (if any) in the WAITS_FOR graph.
             * 
             * Note: A vertex is created iff a dependency chain is
             * established. Therefore it is possible for a transaction to
             * obtain a lock without a vertex begin created for that
             * tranasaction. Hence it is Ok if this method returns [false].
             */

            if (waitsFor != null) {

                waitsFor.releaseVertex(tx);

            }

            //                resourceManagementLock.unlock();

        }

    }

    /**
     * Invoked when a task begins to run.
     * 
     * @param task
     */
    void didStart(Callable<Object> task) {

        nstarted.incrementAndGet();

        log.info("Started: nstarted=" + nstarted);

    }

    /**
     * Invoked on successful task completion.
     */
    void didSucceed(Callable<Object> task) {

        nended.incrementAndGet();

        try {

            /*
             * Force release of locks (if any) and removal of the vertex (if
             * any) from the WAITS_FOR graph.
             * 
             * Note: An operation that completes successfully is by definition
             * NOT awaiting a lock.
             */

            final boolean waiting = false;

            releaseLocks(waiting);

        } catch (Throwable t) {

            log.warn("Problem(s) releasing locks: " + t,
                    t);

        }

        log.info("Ended: nended=" + nended);

    }

    /**
     * Invoke if a task aborted.
     * 
     * @param task
     * @param t
     * @param waiting
     *            <code>false</code> iff the operation was <strong>known</strong>
     *            to be running. Otherwise <code>true</code> to indicate
     *            that the operation is awaiting a lock. An optimization is
     *            used to update the {@link TxDag} when the operation is NOT
     *            waiting. Since that optimization is invalid when the
     *            operation is waiting, always specify <code>true</code>
     *            if you are not sure and the less efficient technique will
     *            be used to update the {@link TxDag}.
     */
    void didAbort(Callable<Object> task, Throwable t, boolean waiting) {

        nerror.incrementAndGet();

        try {

            /*
             * Force release of locks (if any) and removal of the vertex (if
             * any) from the WAITS_FOR graph.
             */

            releaseLocks(waiting);

        } catch (Throwable t2) {

            log.warn(
                    "Problem(s) releasing locks: " + t2, t2);

        }

        log.info("Ended: nended=" + nended);

    }

    public String status() {

        return "maxrunning=" + maxrunning + ", nrunning=" + nrunning
                + ", nstarted=" + nstarted + ", nended=" + nended + ", nerror="
                + nerror + ", ndeadlock=" + ndeadlock + ", ntimeout="
                + ntimeout;

    }

}
