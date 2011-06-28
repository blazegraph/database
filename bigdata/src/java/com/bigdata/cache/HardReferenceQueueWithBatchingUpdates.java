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
 * Created on Jan 11, 2010
 */

package com.bigdata.cache;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.btree.BTree;

/**
 * A variant relying on thread-local {@link IHardReferenceQueue}s to batch
 * updates and thus minimize thread contention for the lock required to
 * synchronize calls to {@link #add(Object)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo This class was created to improve concurrency for the read-only
 *       {@link BTree} by batching updates from each thread using a thread-local
 *       queue. While SOME of the methods from the base class have been modified
 *       to preserve the {@link Queue} or {@link IHardReferenceQueue} semantics
 *       across both the thread-local and the backing shared queue, many methods
 *       have not been so modified and care MUST be taken if you wish to reuse
 *       this class for another purpose.
 */
public class HardReferenceQueueWithBatchingUpdates<T> implements
        IHardReferenceQueue<T> {

    /**
     * The thread-local queues.
     */
    private final ConcurrentHashMap<Thread, BatchQueue<T>> threadLocalQueues;

    /**
     * Scanning is done on the thread-local {@link BatchQueue}.
     */
    private final int threadLocalQueueNScan;
    
    /**
     * The capacity of the thread-local {@link BatchQueue}.
     */
    private final int threadLocalQueueCapacity;
    
    /**
     * When our inner queue has this many entries we will invoke tryLock()
     * and batch the updates if we can barge in on the lock.
     */
    private final int threadLocalTryLockSize;

    /**
     * Optional listener is notified when updates are batched through.
     */
    private final IBatchedUpdateListener batchedUpdatedListener;
    
    /**
     * Lock used to synchronize operations on the {@link #sharedQueue}.
     */
    private final ReentrantLock lock = new ReentrantLock(false/* fair */);

    /**
     * The shared queue. Touches are batched onto this queue by the
     * {@link #threadLocalQueues}.
     */
    private final IHardReferenceQueue<T> sharedQueue;

    /**
     * The listener to which cache eviction notices are reported by the thread-
     * local queues. This listener is responsible for adding the evicted
     * reference to the {@link #sharedQueue}.
     */
    private final HardReferenceQueueEvictionListener<T> threadLocalQueueEvictionListener;

    /**
     * @param listener
     *            The eviction listener (sees only evictions from the outer
     *            class).
     * @param capacity
     *            The capacity of this cache (does not include the capacity of
     *            the thread-local caches).
     */
    public HardReferenceQueueWithBatchingUpdates(
            final HardReferenceQueueEvictionListener<T> listener,
            final int capacity) {

            this(//
                    new HardReferenceQueue<T>(listener, capacity, 0/* nscan */),
//                    listener, capacity,
                IHardReferenceQueue.DEFAULT_NSCAN,// threadLocalNScan
                64,// threadLocalCapacity
                32, // threadLocalTryLockSize
                null // batched update listener
                );

    }

    /**
     * Designated constructor.
     * 
     * @param sharedQueue
     *            The backing {@link IHardReferenceQueue}.
     * @param threadLocalQueueNScan
     *            The #of references to scan on the thread-local queue.
     * @param threadLocalQueueCapacity
     *            The capacity of the thread-local queues in which the updates
     *            are gathered before they are batched into the shared queue.
     *            This must be at least
     * @param threadLocalTryLockSize
     *            Once the thread-local queue is this full an attempt will be
     *            made to barge in on the lock and batch the updates to the
     *            shared queue. This feature may be disabled by passing ZERO
     *            (0).
     */
    public HardReferenceQueueWithBatchingUpdates(
            final IHardReferenceQueue<T> sharedQueue,
//            final HardReferenceQueueEvictionListener<T> listener,
//            final int capacity,//
            final int threadLocalQueueNScan,//
            final int threadLocalQueueCapacity,//
            final int threadLocalTryLockSize,//
            final IBatchedUpdateListener batchedUpdateListener//
            ) {

        if (sharedQueue == null)
            throw new IllegalArgumentException();
        
        // @todo configure concurrency, initialCapacity (defaults are pretty good).
        threadLocalQueues = new ConcurrentHashMap<Thread, BatchQueue<T>>();
        
        this.sharedQueue = sharedQueue;
//        sharedQueue = new HardReferenceQueue<T>(listener, capacity, 0/* nscan */);

        if (threadLocalQueueCapacity <= 0)
            throw new IllegalArgumentException();

        if (threadLocalQueueNScan < 0 || threadLocalQueueNScan > threadLocalQueueCapacity)
            throw new IllegalArgumentException();

        if (threadLocalTryLockSize < 0
                || threadLocalTryLockSize > threadLocalQueueCapacity)
            throw new IllegalArgumentException();

        this.threadLocalQueueNScan = threadLocalQueueNScan;

        this.threadLocalQueueCapacity = threadLocalQueueCapacity;

        this.threadLocalTryLockSize = threadLocalQueueCapacity;

        this.batchedUpdatedListener = batchedUpdateListener;
        
        this.threadLocalQueueEvictionListener = new HardReferenceQueueEvictionListener<T>() {

            /**
             * Add the reference to the backing queue for the outer class. The
             * caller MUST hold the outer class lock.
             */
            public void evicted(final IHardReferenceQueue<T> cache, final T ref) {

                // Note: invokes add() on the shared inner queue.
                sharedQueue.add(ref);

            }

        };
        
    }
    
    /**
     * Return a thread-local queue which may be used to batch updates to this
     * {@link HardReferenceQueueWithBatchingUpdates}. The returned queue will
     * combine calls to {@link IHardReferenceQueue#add(Object)} in a
     * thread-local array, batching updates from the array to <i>this</i> queue
     * periodically. This can substantially reduce contention for the lock
     * required to synchronize before invoking {@link #add(Object)}.
     * <P>
     * Note: The returned queue handles synchronization for {@link #add(Object)}
     * internally using {@link Lock#tryLock()} and {@link Lock#lock()}.
     * <p>
     * Note: {@link IHardReferenceQueue#add(Object)} for the returned reference
     * will report <code>true</code> iff the object is already on the
     * thread-local queue and DOES NOT consider whether the object is already on
     * <i>this</i> queue. Therefore {@link #getThreadLocalQueue()} MUST NOT
     * be used if the value returned by {@link #add(Object)} is significant (for
     * example, do not use the thread-local batch queue for the mutable
     * {@link BTree}!).
     * 
     * @return The thread-local queue used to batch updates to <i>this</i>
     *         queue.
     */
    final private BatchQueue<T> getThreadLocalQueue() {

        final Thread t = Thread.currentThread();

        BatchQueue<T> tmp = threadLocalQueues.get(t);//id);

        if (tmp == null) {

            if (threadLocalQueues.put(t, tmp = new BatchQueue<T>(
                    threadLocalQueueNScan, threadLocalQueueCapacity,
                    threadLocalTryLockSize, lock,
                    threadLocalQueueEvictionListener,
                    batchedUpdatedListener
                    )) != null) {
                
                /*
                 * Note: Since the key is the thread it is not possible for there to
                 * be a concurrent put of an entry under the same key so we do not
                 * have to use putIfAbsent().
                 */

                throw new AssertionError();
                
            }

        }

        return tmp;

    }

    /*
     * IHardReferenceQueue
     */

    /**
     * The size of the shared queue (approximate).
     */
    public int size() {
        return sharedQueue.size();
    }
//    /**
//     * Reports the combined size of the thread-local queue plus the shared
//     * queue.
//     */
//    public int size() {
//        
//        return getThreadLocalQueue().size + size;
//        
//    }

    /**
     * The capacity of the shared queue.
     */
    public int capacity() {
        return sharedQueue.capacity();
    }

    /**
     * The nscan value of the shared queue.
     */
    public int nscan() {
        return sharedQueue.nscan();
    }

    /**
     * Not supported.
     */
    public boolean evict() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not supported.
     */
    public void evictAll(boolean clearRefs) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not supported.
     */
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
//        return innerQueue.isEmpty();
    }

    /**
     * Not supported.
     */
    public boolean isFull() {
        throw new UnsupportedOperationException();
//        return innerQueue.isFull();
    }

    /**
     * Not supported.
     */
    public T peek() {
        throw new UnsupportedOperationException();
    }

    /**
     * Adds the reference to the thread-local queue, returning <code>true</code>
     * iff the queue was modified as a result. This is non-blocking unless the
     * thread-local queue is full. If the thread-local queue is full, the
     * existing references will be batched first onto the shared queue.
     */
    final public boolean add(final T ref) {
        
        return getThreadLocalQueue().add(ref);
        
    }

    /**
     * Offers the reference to the thread-local queue, returning
     * <code>true</code> iff the queue was modified as a result. This is
     * non-blocking unless the thread-local queue is full. If the thread-local
     * queue is full, the existing references will be batched first onto the
     * shared queue.
     */
    final public boolean offer(final T ref) {
        
        throw new UnsupportedOperationException();
//        return getThreadLocalQueue().offer(ref);
        
    }

    /**
     * Discards the thread-local buffers and clears the backing ring buffer.
     * <p>
     * Note: This method can have side-effects from asynchronous operations if
     * the queue is still in use.
     */
    final public void clear(final boolean clearRefs) {

        lock.lock();

        try {

            for (BatchQueue<T> q : threadLocalQueues.values()) {
            
                // clear the thread local queues.
                q.clear(clearRefs);
                
            }

            // discard map entries.
            threadLocalQueues.clear();

            // clear the shared backing queue.
            sharedQueue.clear(true/* clearRefs */);
            
        } finally {
            
            lock.unlock();
            
        }

    }

    /**
     * Not supported.
     */
//    Tests the thread-local buffer first, then the shared buffer.
    final public boolean contains(final Object ref) {

        throw new UnsupportedOperationException();
        
//        if (getThreadLocalQueue().contains(ref)) {
//
//            // found in the thread-local queue.
//            return true;
//
//        }
//
//        // test the shared queue.
//        lock.lock();
//
//        try {
//
//            return sharedQueue.contains(ref);
//
//        } finally {
//            
//            lock.unlock();
//            
//        }

    }

    /**
     * Inner class provides thread-local batching of updates to the outer
     * {@link HardReferenceQueue}. This can substantially decrease the
     * contention for the lock required to provide safe access to the outer
     * {@link HardReferenceQueue} during updates.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <T>
     */
    static private class BatchQueue<T> extends RingBuffer<T> implements
            IHardReferenceQueue<T> {

        private final int nscan;
        private final int tryLockSize;
        private final Lock lock;
        private final HardReferenceQueueEvictionListener<T> listener;
        private final IBatchedUpdateListener batchedUpdatedListener;

        public BatchQueue(
                final int nscan,//
                final int capacity,//
                final int tryLockSize, //
                final ReentrantLock lock,//
                final HardReferenceQueueEvictionListener<T> listener,//
                final IBatchedUpdateListener batchedUpdateListener//
                ) {

            super(capacity);

            this.nscan = nscan;

            this.tryLockSize = tryLockSize;
            
            this.lock = lock;
            
            this.listener = listener;

            this.batchedUpdatedListener = batchedUpdateListener;

        }

        /**
         * Return the value on the outer class.
         */
        public int nscan() {
            
            return nscan;
            
        }

        /**
         * Add a reference to the cache. If the reference was recently added to
         * the cache then this is a NOP. Otherwise the reference is appended to
         * the cache. If a reference is appended to the cache and then cache is
         * at capacity, then the LRU reference is first evicted from the cache.
         * 
         * @param ref
         *            The reference to be added.
         * 
         * @return True iff the reference was added to the cache and false iff
         *         the reference was found in a scan of the nscan MRU cache
         *         entries.
         */
        @Override
        public boolean add(final T ref) {

            /*
             * Scan the last nscan references for this reference. If found,
             * return immediately.
             */
            if (nscan > 0 && scanHead(nscan, ref)) {

                return false;

            }

            // add to the thread-local queue.
            return super.add(ref);

        }

        @Override
        public boolean offer(final T ref) {

            /*
             * Scan the last nscan references for this reference. If found,
             * return immediately.
             */
            if (nscan > 0 && scanHead(nscan, ref)) {

                return false;

            }

            // offer to the thread-local queue.
            return super.offer(ref);

        }

        /**
         * Extended to batch the updates to the base class for the outer class
         * when the inner queue is half full (tryLock()) and when the inner
         * queue is full (lock()).
         */
        @Override
        protected void beforeOffer(final T ref) {

//            assert size <= capacity : "size=" + size + ", capacity=" + capacity;
            
            if (tryLockSize != 0 && size == tryLockSize) {

                if (lock.tryLock()) {

                    /*
                     * Batch evict all references to the outer class's queue.
                     */

                    try {

                        evictAll(true/* clearRefs */);

//                        assert size == 0 : "size=" + size;

                        if (batchedUpdatedListener != null) {

                            batchedUpdatedListener.didBatchUpdates();

                        }
                        
                    } finally {

                        lock.unlock();

                    }

                }

                return;

            }

            // @todo why does this fail if written as (size == capacity)???
            if (size + 1 == capacity) {

                /*
                 * If at capacity, batch evict all references to the outer
                 * class's queue.
                 */

                lock.lock();

                try {

                    evictAll(true/* clearRefs */);

//                    assert size == 0 : "size=" + size;

                    if (batchedUpdatedListener != null) {

                        batchedUpdatedListener.didBatchUpdates();

                    }

                } finally {

                    lock.unlock();

                }

            }
         
//            assert size < capacity : "size=" + size + ", capacity=" + capacity;
            
        }

        public boolean evict() {

//            assert lock.isHeldByCurrentThread();
            
            final T ref = poll();

            if (ref == null) {

                // buffer is empty.
                return false;

            }

            if (listener != null) {

                // report eviction notice to listener.
                listener.evicted(this, ref);

            }

            return true;
            
        }

        /**
         * Evict all references, starting with the LRU reference and proceeding to
         * the MRU reference.
         * 
         * @param clearRefs
         *            When true, the reference are actually cleared from the cache.
         *            This may be false to force persistence of the references in
         *            the cache without actually clearing the cache.
         */
//        synchronized
        final public void evictAll(final boolean clearRefs) {
//System.err.println((clearRefs?'T':'F')+" : "+Thread.currentThread());
//            assert lock.isHeldByCurrentThread();
            if (clearRefs) {

                /*
                 * Evict all references, clearing each as we go.
                 */

                while (!isEmpty()) { // count > 0 ) {

                    evict();

                }

            } else {

                /*
                 * Generate eviction notices in LRU to MRU order but do NOT clear
                 * the references.
                 */

                final int size = size();

                for (int n = 0; n < size; n++) {

                    final T ref = get(n); 

                    if (listener != null) {

                        // report eviction notice to listener.
                        listener.evicted(this, ref);
                        
                    }

                }

            }

//            assert size() == 0 : "size=" + size();
            
        }

    }

    /**
     * This callback is invoked when updates are batched through from the
     * thread-local queue to the shared queue. The default implementation does
     * nothing.
     */
    interface IBatchedUpdateListener {
       
        void didBatchUpdates();
        
    }
    
}
