/*

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
package com.bigdata.concurrent;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * Unbounded queue of operations waiting to gain an exclusive lock on a
 * resource. By default, the queue imposes a "fair" schedule for access to the
 * resource. Deadlocks are detected using a WAITS_FOR graph that is shared by
 * all resources and transactions for a given database instance.
 * <p>
 * Note: deadlock detection MAY be disabled when all lock requests are (a)
 * pre-declared; and (b) sorted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param R
 *            The type of the object that correspond to the "resource".
 * 
 * @param T
 *            The type of the object that corresponds to the "operation". This
 *            is often the {@link Thread} in which the operation runs, but
 *            implementations MAY choose to let operations migrate from thread
 *            to thread.
 * 
 * @see LockManager
 * @see TxDag
 */
public class ResourceQueue<R, T> {

    public static final Logger log = Logger.getLogger(ResourceQueue.class);

    /**
     * 
     */
    private static final long serialVersionUID = 6759702404466026405L;

    /**
     * The resource whose access is controlled by this object.
     */
    final private R resource;

    /**
     * The queue of transactions seeking access to the {@link #resource}.
     * The object at the head of the queue is the transaction with the lock
     * on the resource.
     */
    final BlockingQueue<T/* tx */> queue = new LinkedBlockingQueue<T>(/* unbounded */);

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
     * <p>
     * Note: This MAY be null. When it is null deadlock detection is
     * <strong>disabled</strong>. This is Ok if the operations are (1)
     * pre-declaring their locks; and (2) the resources in each lock request are
     * sorted into a common order. Under those conditions deadlocks are
     * impossible and we do not need to maintain a WAITS_FOR graph.
     */
    final TxDag waitsFor;

    /**
     * The resource whose locks are administeded by this object.
     */
    public R getResource() {

        return resource;

    }

    /**
     * True iff a lock is granted.
     * 
     * @todo method signature may conflict with implicit use of the Thread
     *       as the transaction object.
     */
    public boolean isLocked() {

        return !queue.isEmpty();

    }

    /**
     * The #of pending requests for a lock on the resource.
     * 
     * @return
     */
    public int getQueueSize() {

        return Math.max(0, queue.size() - 1);

    }

    /**
     * Return true if the transaction currently holds the lock.
     * 
     * @param tx
     *            The transaction.
     */
    public boolean isGranted(T tx) {

        if (tx == null) {

            throw new NullPointerException();

        }

        //            lock.lock();
        //            
        //            try {

        return queue.peek() == tx;

        //            } finally {
        //                
        //                lock.unlock();
        //                
        //            }

    }

    /**
     * Note: This uses {@link LinkedBlockingQueue#toString()} to serialize
     * the state of the resource queue so the result will be consistent per
     * the contract of that method and
     * {@link LinkedBlockingQueue#iterator()}.
     */
    public String toString() {

        return getClass().getSimpleName() + "{resource=" + resource
                + ", queue=" + queue.toString() + "}";

    }

    /**
     * Create a queue of lock requests for a resource.
     * 
     * @param resource
     *            The resource.
     * @param waitsFor
     *            The WAITS_FOR graph shared by all transactions and all
     *            resources (optional). When NOT specified operations MUST
     *            pre-declare their locks and the {@link LockManager} MUST sort
     *            the resources in each lock request into a common order such
     *            that deadlocks CAN NOT occur.
     */
    public ResourceQueue(R resource, TxDag waitsFor) {

        if (resource == null)
            throw new NullPointerException();

//        if (waitsFor == null)
//            throw new NullPointerException();

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
    public void lock(T tx) throws InterruptedException, DeadlockException {

        lock(tx, 0L);

    }

    /**
     * Obtain a lock on the resource.
     * 
     * @param tx
     *            The transaction.
     * @param timeout
     *            The timeout (ms) -or- 0L to wait forever.
     * 
     * @throws InterruptedException
     *             if the lock was interrupted (the transaction should
     *             handle this exception by aborting).
     * @throws DeadlockException
     *             if the request would cause a deadlock among the running
     *             transactions.
     */
    public void lock(T tx, long timeout) throws InterruptedException,
            DeadlockException {

        /*
         * Note: Since blocked transactions do not run a transaction can be
         * pending a lock in at most one {@link ResourceQueue}.
         */

        if (tx == null)
            throw new NullPointerException();

        if (timeout < 0L)
            throw new IllegalArgumentException();

        log.debug("enter: tx=" + tx + ", queue=" + this);

        lock.lock();

        log.debug("have private lock: tx=" + tx + ", queue=" + this);

        final long begin = System.currentTimeMillis();

        try {

            assertNotDead();

            // already locked.
            if (queue.peek() == tx) {

                log.info("Already owns lock: tx=" + tx + ", queue=" + this);

                return;

            }

            if (queue.isEmpty()) {

                // the queue is empty so immediately grant the lock.

                queue.add(tx);

                log.info("Granted lock with empty queue: tx=" + tx + ", queue="
                        + this);

                return;

            }

            /*
             * Update the WAITS_FOR graph since we are now going to wait on the
             * tx that currently holds this lock.
             * 
             * We need to add an edge from this transaction to the transaction
             * that currently holds the lock for this resource.  This indicates
             * that [tx] WAITS_FOR the operation that holds the lock.
             * 
             * We need to do this for each predecessor in the queue so that the
             * correct WAITS_FOR edges remain when a predecessor is granted the
             * lock.
             */
            if (waitsFor != null) {

                Object[] predecessors = queue.toArray();
                
                try {

                    /*
                     * Note: this operation is atomic.  If it fails, then none
                     * of the edges were added.
                     */
                    
                    waitsFor.addEdges(tx/* src */, predecessors);

                } catch (DeadlockException ex) {

                    /*
                     * Reject the lock request since it would cause a deadlock.
                     */
                    
                    log.warn("Deadlock: tx=" + tx + ", queue=" + this/*, ex*/);

                    throw ex;

                }

            }

            /*
             * Now that we know that the request does not directly cause a
             * deadlock we add the request to the queue.
             */

            queue.add(tx);

            try {

                while (true) {
    
                    long elapsed = System.currentTimeMillis() - begin;
    
                    if (timeout != 0L && elapsed >= timeout) {
    
                        throw new TimeoutException("After " + elapsed + " ms: tx="
                                + tx + ", queue=" + this);
    
                    }
    
                    log.info("Awaiting resource: tx=" + tx + ", queue=" + this);
    
                    final long remaining = timeout - elapsed;
    
                    if (timeout == 0L) {
    
                        // wait w/o timeout.
    
                        available.await();
    
                    } else {
   
                        // wait w/ timeout.
                        
                        if (!available.await(remaining, TimeUnit.MILLISECONDS)) {

                            throw new TimeoutException("After " + elapsed
                                    + " ms: tx=" + tx + ", queue=" + this);

                        }
    
                    }
    
                    /*
                     * Note: We can continue here either because the Condition
                     * that we were awaiting has been signalled -or- because of
                     * a timeout (handled above) -or- for no reason.
                     */
    
                    log.info("Continuing after wait: tx=" + tx + ", queue=" + this);
    
                    if (dead.get()) {
    
                        throw new InterruptedException("Resource is dead: "
                                + resource);
    
                    }
    
                    if (queue.peek() == tx) {
    
                        /*
                         * Note: tx is at the head of the queue while it holds
                         * the lock.
                         */
    
                        log.info("Lock granted after wait: tx=" + tx + ", queue="
                                + this);
    
                        return;
    
                    }
    
                } // while(true)
                
            } catch(Throwable t) {

                /*
                 * At this point there are edges in the WAITS_FOR graph and the
                 * tx is on the queue. While we appended it to the end of the
                 * queue above it can have "moved" since both due to locks that
                 * have been granted to other transactions and due to other
                 * transactions that are now also waiting in the queue. In fact,
                 * it can even in the 1st position (the "granted" position).
                 * Regardless it is definately not running since it is still in
                 * "lock()".
                 * 
                 * What we need to do now is perform a correcting action to
                 * restore the WAITS_FOR graph, remove any edges that might
                 * depend on this tx since it is going to be removed, and then
                 * remove the tx from the pending queue.
                 * 
                 * Note: If we timeout this request then we need to back it out
                 * of the queue, including the edges that we added above. The
                 * same procedure should be used regardless of the error
                 * condition, e.g., if we are interrupted or if a spurious error
                 * occurs then we still need to perform this correcting action.
                 * 
                 * @todo If we get into adjusting schedules to suit priorities
                 * then it may make sense to write insert(int pos) and
                 * remove(int pos) that also update the WAITS_FOR graph
                 * atomically while we have a lock on the resource queue.
                 */
                if (waitsFor != null) {

                    synchronized (waitsFor) {

                        /*
                         * Note: If the transaction is at the head of the queue
                         * then it is probably NOT waiting. However, this case
                         * should be quite rare in lock().
                         */

                        try {
                            
                            /*
                             * Note: Assume that tx is waiting on something
                             * unless we have absolute proof to the contrary.
                             */
                            
                            final boolean waiting = true;
                            
                            waitsFor.removeEdges(tx, waiting);
                            
                        } catch (Throwable t2) {
                            
                            log.warn(t2);
                            
                        }

                    }
                }

                // and remove it from the queue - we are done w/ our correcting action.
                queue.remove(tx);
                
                // minimize masquerading of exceptions.
                
                if (t instanceof RuntimeException)
                    throw (RuntimeException) t;

                if (t instanceof InterruptedException)
                    throw (InterruptedException) t;

                throw new RuntimeException(t);
                
            }

        } finally {

            lock.unlock();

            log.debug("released private lock: tx=" + tx + ", queue=" + this);

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
    public void unlock(T tx) {

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
            
            if (waitsFor != null) {

                Iterator<T> itr = queue.iterator();

                synchronized (waitsFor) {

                    while (itr.hasNext()) {

                        T pendingTx = itr.next();

                        try {

                            waitsFor.removeEdge(pendingTx, tx);

                        } catch (Throwable t) {

                            /*
                             * Note: log the error but continue otherwise we
                             * will deadlock other tasks waiting on this
                             * resource since throwing the exception will mean
                             * that we do not invoke [available.signalAll()] and
                             * do not remove any edges that we think should be
                             * there.
                             */
                            log.warn(t.getMessage(), t);

                        }

                    }

                }

            }

            if (queue.isEmpty()) {

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
    public void clear(T tx) {

        lock.lock();

        try {

            assertNotDead();

            assertOwnsLock(tx);

            if (true) {
              
                // @todo This needs to remove any edge involving any pending tx.
                
                throw new UnsupportedOperationException();
                
            }

            queue.clear();

            dead.set(true);

            available.signalAll();

        } finally {

            lock.unlock();

        }

    }

}
