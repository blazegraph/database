/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Sep 2, 2014
 */
package com.bigdata.btree;

import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.bigdata.journal.ICommitter;

/**
 * Base class for managing read/write locks for unisolated {@link ICommitter}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/855"> AssertionError: Child does
 *      not have persistent identity </a>
 */
public class ReadWriteLockManager implements IReadWriteLockManager {

//    private static final Logger log = Logger.getLogger(ReadWriteLockManager.class);

    /**
     * The #of milliseconds that the class will wait for a read or write lock. A
     * (wrapped) {@link InterruptedException} will be thrown if this timeout is
     * exceeded. The default is {@value #LOCK_TIMEOUT_MILLIS} milliseconds. Use
     * {@link Long#MAX_VALUE} for no timeout.
     * 
     * TODO There may be no reason to have a timeout when waiting for a lock in
     * which case we can get rid of this field. Also, there is no means
     * available to configure the timeout (in a similar fashion you can not
     * configure the fairness policy for the {@link ReentrantReadWriteLock}).
     * <p>
     * If we get rid of this field, then the {@link WrappedReadLock} and
     * {@link WrappedWriteLock} classes can be simplified to have normal lock
     * semantics rather than tryLock() based semantics.
     */
    private static final long LOCK_TIMEOUT_MILLIS = Long.MAX_VALUE;// 10000;
    
    /*
     * Note: This creates a hard reference that defeats the weak keys in the
     * hash map.
     */
//    /**
//     * The unisolated persistence capable data structure.
//     */
//    final private ICheckpointProtocol committer;
    
    /**
     * True iff the caller's {@link ICheckpointProtocol} object was read-only.
     */
    final private boolean readOnly;
    
    /**
     * The {@link Lock} used to permit concurrent readers on an unisolated index
     * while serializing access to that index when a writer must run.
     */
    final private WrappedWriteLock writeLock;

    /**
     * The {@link Lock} ensures that any code path that obtains the read lock
     * also maintains the per-thread read-lock counter.
     */
    final private Lock readLock;

    /**
     * Canonicalizing mapping for the {@link ReadWriteLockManager} objects.
     */
    static final private WeakHashMap<ICommitter, ReadWriteLockManager> locks = new WeakHashMap<ICommitter, ReadWriteLockManager>();

    @Override
    public int getReadLockCount() {

        if (readOnly) {

            // No locks are actually taken.
            return 0;

        }

        // Return the locks actually held by this thread.
        final Integer readLockCounter = ((WrappedReadLock) readLock).threadLockMap
                .get(Thread.currentThread().getId());

        if (readLockCounter == null) {

            // No read locks are held.
            return 0;

        }

        return readLockCounter.intValue();

    }

    @Override
    public Lock readLock() {

        return readLock;
        
    }

    @Override
    public Lock writeLock() {

        if (readOnly)
            throw new UnsupportedOperationException(
                    AbstractBTree.ERROR_READ_ONLY);
        
        return writeLock;
        
    }

    @Override
    public boolean isReadOnly() {
        
        /*
         * Note: This method is grounded out without delegation to avoid
         * recursion through the target persistence capable data structure.
         */
        
        return readOnly;
        
    }
    
    /**
     * {@link WrappedReadLock} is used to intercept lock/unlock calls to the
     * readLock to trigger calls to the logic that tracks the #of reentrant
     * read-locks by read and which can be used to identify whether the readlock
     * is held by the current thread.
     * <p>
     * This is tested in the touch() methods for the BTree and HTree classe to
     * determine whether the touch should be ignored or trigger potential
     * evictions.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/855"> AssertionError: Child
     *      does not have persistent identity </a>
     */
    private class WrappedReadLock implements Lock {
        
        private final Lock delegate;
        
        /**
         * Maintain count of readLocks on by Thread. This is used to avoid having
         * read-only operations protected by an {@link ReadWriteLockManager}
         * causing evictions of dirty nodes and leaves.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/855"> AssertionError: Child
         *      does not have persistent identity </a>
         */
        private final ConcurrentHashMap<Long, Integer> threadLockMap;

        /**
         * Track the #of read locks by thread IFF this is a read/write index
         * view.
         */
        private void readLockedThread() {
            final long thisThreadId = Thread.currentThread().getId();
            final Integer entry = threadLockMap.get(thisThreadId);
            final Integer newVal = entry == null ? 1 : 1 + entry.intValue();
            threadLockMap.put(thisThreadId, newVal);
        }

        /**
         * Track the #of read locks by thread IFF this is a read/write index
         * view.
         */
        private void readUnlockedThread() {
            final long thisThreadId = Thread.currentThread().getId();
            final Integer entry = threadLockMap.get(thisThreadId);
            assert entry != null;
            if (entry.intValue() == 1) {
                threadLockMap.remove(thisThreadId);
            } else {
                threadLockMap.put(thisThreadId, entry.intValue() - 1);
            }
        }

        WrappedReadLock(final Lock delegate) {

            if (delegate == null)
                throw new IllegalArgumentException();
            
            this.delegate = delegate;
            
            /*
             * Configure parallelism default.
             * 
             * Note: This CHM is ONLY used by mutable index views. So what
             * matters here is the #of threads that contend for a mutable index
             * view. I suspect that this is significantly fewer threads than we
             * observe for concurrent read-only index views. Therefore I have
             * set the parameters for the map based on the notion that only a
             * few threads are contending for the mutable index object in order
             * to reduce the heap burden associated with these CHM instances. If
             * this map is observed to be hot spot, then we can simply use the
             * defaults (initialCapacity = concurrencyLevel = 16). We only have
             * this for the mutable index views and there are typically not that
             * many instances of those open at the same time.
             */
            final int initialCapacity = 4;
            final int concurrencyLevel = initialCapacity;
            final float loadFactor = .75f;
            this.threadLockMap = new ConcurrentHashMap<Long, Integer>(
                    initialCapacity, loadFactor, concurrencyLevel);

        }

        @Override
        public void lock() {
            try {
                /*
                 * Note: The original UnisolatedReadWriteLock semantics are
                 * always those of a tryLock with a default timeout. Make sure
                 * that we keep this in place!
                 */
                lockInterruptibly();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            /*
             * Note: The order in which we obtain the real read lock and
             * increment (and decrement) the per-thread read lock counter on the
             * AbstractBTree is not critical because AbstractBTree.touch()
             * relies on the thread both owning the read lock and having the
             * per-thread read lock counter incremented for that thread.
             * 
             * Note: The original UnisolatedReadWriteLock semantics are always
             * those of a tryLock with a default timeout. Make sure that we keep
             * this in place!
             */
//            delegate.lock();
            if (!delegate.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw new RuntimeException("Timeout");
            }
            readLockedThread();
        }

        @Override
        public boolean tryLock() {
            final boolean ret = delegate.tryLock();
            if (ret) {
                readLockedThread();
            }
            return ret;
        }

        @Override
        public boolean tryLock(final long time, final TimeUnit unit)
                throws InterruptedException {
            final boolean ret = delegate.tryLock(time, unit);
            if (ret) {
                readLockedThread();
            }
            return ret;
        }

        @Override
        public void unlock() {
            /*
             * Note: The unlock order does not really matter. See the 
             * comments on lock() and AbstractBTree.touch().
             */
            delegate.unlock();
            /*
             * Do this after the unlock() in case the lock/unlock are not
             * correctly paired.
             */
            readUnlockedThread();
        }

        @Override
        public Condition newCondition() {
            return delegate.newCondition();
        }

    } // class WrappedReadLock

    /**
     * Wraps the write lock to provide interruptable tryLock() with timeout
     * semantics for all write lock acquisitions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/855"> AssertionError: Child
     *      does not have persistent identity </a>
     */
    private class WrappedWriteLock implements Lock {
        
        private final Lock delegate;

        WrappedWriteLock(final Lock delegate) {

            if (delegate == null)
                throw new IllegalArgumentException();
            
            this.delegate = delegate;
            
        }

        @Override
        public void lock() {
            try {
                /*
                 * Note: The original UnisolatedReadWriteLock semantics are
                 * always those of a tryLock with a default timeout. Make sure
                 * that we keep this in place!
                 */
                lockInterruptibly();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            /*
             * Note: The original UnisolatedReadWriteLock semantics are always
             * those of a tryLock with a default timeout. Make sure that we keep
             * this in place!
             */
//            delegate.lock();
            if (!delegate.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw new RuntimeException("Timeout");
            }
        }

        @Override
        public boolean tryLock() {
            return delegate.tryLock();
        }

        @Override
        public boolean tryLock(final long time, final TimeUnit unit)
                throws InterruptedException {
            return delegate.tryLock(time, unit);
        }

        @Override
        public void unlock() {
            delegate.unlock();
        }

        @Override
        public Condition newCondition() {
            return delegate.newCondition();
        }

    } // class WrappedWriteLock

    /**
     * Class used for read lock for read-only data structures.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class ConcurrentReaderLock implements Lock {

        @Override
        public void lock() {
            // NOP
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            // NOP
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit)
                throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {
            // NOP
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
        
    }
    private static final Lock READ_ONLY_LOCK = new ConcurrentReaderLock();
    
    /**
     * Canonicalizing factory for the {@link ReadWriteLock} for an
     * {@link ICommitter}.
     * <p>
     * Note: This method CAN NOT be exposed since that breaks encapsulation for
     * the {@link WrappedReadLock}.
     * 
     * @param index
     *            The btree.
     * @return The lock.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    static public ReadWriteLockManager getLockManager(
            final ICheckpointProtocol index) {

        if (index == null)
            throw new IllegalArgumentException();

        synchronized (locks) {

            ReadWriteLockManager lockManager = locks.get(index);

            if (lockManager == null) {

                lockManager = new ReadWriteLockManager(index);

                locks.put(index, lockManager);

            }

            return lockManager;
        }

    }

    /**
     * Note: ONLY accessed through the canonicalizing pattern!
     */
    private ReadWriteLockManager(final ICheckpointProtocol index) {

//        this.committer = index;
        
        if (this.readOnly = index.isReadOnly()) {

            /*
             * Since the index does not allow mutation, wrap with a NOP lock.
             * 
             * Note: Concurrent readers are automatically supported by our
             * persistent capable data structures so we return a NOP Lock
             * implementation if the data structure is read-only. Also note that
             * read-only data structures are (by definition) not mutable so we
             * do not need to track the #of reentrant locks held for a read-only
             * data structure (per above).
             */
            this.readLock = READ_ONLY_LOCK;

            this.writeLock = null;

        } else {

            /*
             * Note: fairness is NOT required for the locks. I believe that this
             * is supposed to provide better throughput, but that has not been
             * tested. Also, this has not been tested with a simple mutex lock
             * vs a read-write lock. The use case for which this class was
             * originally developed was computing the fix point of a set of
             * rules. In that use case, we do a lot of concurrent reading and
             * periodically flush the computed solutions onto the relations. It
             * is likely that a read-write lock will do well for this situation.
             */
            final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(
                    false/* fair */);

            /**
             * If the index allows mutation, then wrap with tryLock() and
             * lock-counting semantics. This allows us to test for the #of
             * reentrant locks held by the current thread in
             * AbstractBTree.touch() and is the primary basis for the fix the
             * ticket below.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/855">
             *      AssertionError: Child does not have persistent identity </a>
             */
            this.readLock = new WrappedReadLock(readWriteLock.readLock());

            // Wrap with tryLock() semantics.
            this.writeLock = new WrappedWriteLock(readWriteLock.writeLock());

        }

    }

//    @Override
//    final public String toString() {
//
//        return getClass().getName() + "{committer=" + committer + ",readOnly="
//                + readOnly + "}";
//        
//    }

}
