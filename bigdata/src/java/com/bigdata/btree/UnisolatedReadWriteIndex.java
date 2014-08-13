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
 * Created on Jan 10, 2008
 */

package com.bigdata.btree;

import java.util.Iterator;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.bop.cost.BTreeCostModel;
import com.bigdata.bop.cost.DiskCostModel;
import com.bigdata.bop.cost.ScanCostReport;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.btree.view.FusedView;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.service.Split;

import cutthecrap.utils.striterators.IFilter;

/**
 * <p>
 * A view onto an unisolated index partition which enforces the constraint that
 * either concurrent readers -or- a single writer may have access to the
 * unisolated index at any given time. This provides the maximum possible
 * concurrency for an unisolated index using an internal {@link ReadWriteLock}
 * to coordinate threads.
 * </p>
 * <p>
 * The possible concurrency with this approach is higher than that provided by
 * the {@link IConcurrencyManager} since the latter only allows a single process
 * access to the unisolated index while this class can allow multiple readers
 * concurrent access to the same unisolated index. <strong>The use of this class
 * is NOT compatible with the {@link IConcurrencyManager}</strong> (the
 * {@link IConcurrencyManager} does not respect the locks managed by this
 * class).
 * </p>
 * <p>
 * This class does NOT handle deadlock detection. However, it does not expose
 * the underlying lock and the scope of the acquired lock should always be
 * restricted to a single operation as defined by {@link IIndex}. If you
 * circumvent this by writing and submitting an {@link IIndexProcedure} that
 * attempts an operation on another {@link UnisolatedReadWriteIndex} then a
 * deadlock MAY occur.
 * </p>
 * <p>
 * The point test methods on this class (get, contains, lookup, remove) have
 * relatively high overhead since they need to acquire and release the lock per
 * point test. If you need to do a bunch of point tests, then submit an
 * {@link IIndexProcedure} that will run against the underlying index once it
 * has acquired the appropriate lock -- point tests from within the
 * {@link IIndexProcedure} will be very efficient.
 * </p>
 * 
 * <h2>Design notes</h2>
 * 
 * This class was developed to squeeze the maximum possible performance out of a
 * local database. The use of this class can provide correct interleaving of
 * readers and writers without the use of the {@link ConcurrencyManager} and the
 * group commit protocol which it imposes on writers. It also facilitates the
 * reuse of the buffers backing the unisolated index, which can reduce IO
 * associated with index operations when compared to reading on a read-committed
 * view of the index with concurrent writes and interleaved commits on the
 * corresponding unisolated index.
 * <p>
 * Reading on the read-committed index view has greater possible concurrency,
 * but requires that writes are committed before they become visible and must
 * read the data from the disk since it does not have access to the buffers for
 * the unisolated index. Requiring a commit in order for the writes to become
 * visible imposes significant latency, especially when computing the fix point
 * of a rule set which may take multiple rounds. Reading on the unisolated index
 * should do better in terms of buffer reuse and does NOT require commits or
 * checkpoints of the index for writes to become visible to readers but does
 * require a means to correctly interleave access to the unisolated index, which
 * is the purpose of this class.
 * <p>
 * While the lock manager could be modified to support Share vs Exclusive locks
 * and to use Share locks for readers and Exclusive locks for writers, writers
 * would still block until the next commit so the throughput (e.g., when
 * computing the fix point of a rule set) is significantly lower.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class UnisolatedReadWriteIndex implements IIndex, ILinearList {

    private static final Logger log = Logger.getLogger(UnisolatedReadWriteIndex.class);
  
    /**
     * The #of milliseconds that the class will wait for a read or write lock. A
     * (wrapped) {@link InterruptedException} will be thrown if this timeout is
     * exceeded. The default is {@value #LOCK_TIMEOUT_MILLIS} milliseconds. Use
     * {@link Long#MAX_VALUE} for no timeout.
     * 
     * @todo There may be no reason to have a timeout when waiting for a lock in
     *       which case we can get rid of this field. Also, there is no means
     *       available to configure the timeout (in a similar fashion you can
     *       not configure the fairness policy for the
     *       {@link ReentrantReadWriteLock}).
     */
    protected static final long LOCK_TIMEOUT_MILLIS = Long.MAX_VALUE;// 10000;
    
    /**
     * An exclusive write lock used (in the absence of other concurrency control
     * mechanisms) to serialize all processes accessing an unisolated index when
     * a writer must run. This is automatically obtained by methods on this
     * class which will write on the underlying {@link IIndex}. It is exposed
     * for processes which need to obtain the write lock to coordinate external
     * operations.
     * 
     * @return The acquired lock.
     */
    public Lock writeLock() {
       
        final Lock writeLock = readWriteLock.writeLock();
        
        try {
            
            if(log.isDebugEnabled()) {
                
                log.debug(ndx.toString());
                
            }
            
//            writeLock.lock();
            
            if(!writeLock.tryLock( LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                
                throw new RuntimeException("Timeout");
                
            }
            
        } catch(InterruptedException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return writeLock;
        
    }
    
    /**
     * A shared read lock used (in the absence of other concurrency control
     * mechanisms) to permit concurrent readers on an unisolated index while
     * serializing access to that index when a writer must run. This is
     * automatically obtained by methods on this class which will write on the
     * underlying {@link IIndex}. It is exposed for processes which need to
     * obtain the write lock to coordinate external operations.
     * 
     * @return The acquired lock.
     */
    public Lock readLock() {
        
        final Lock readLock = readWriteLock.readLock();

        try {

            if(log.isDebugEnabled()) {
                
                log.debug(ndx.toString()
//                        , new RuntimeException()
                        );
                
            }
            
//            readLock.lock();
            
            if(!readLock.tryLock( LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                
                throw new RuntimeException("Timeout");

            }
            
        } catch(InterruptedException ex) {
            
            throw new RuntimeException(ex);
            
        }
            
        return readLock;
        
    }
    
    /**
     * Acquire an appropriate lock depending on whether or not the procedure
     * asserts that it is read-only.
     * 
     * @param proc
     *            The procedure.
     *            
     * @return The acquired lock.
     */
    private Lock lock(final IIndexProcedure<?> proc) {
     
        if (proc == null)
            throw new IllegalArgumentException();
        
        if(proc.isReadOnly()) {
            
            return readLock();
            
        }
        
        return writeLock();
        
    }

    private void unlock(final Lock lock) {
        
        lock.unlock();
        
        if(log.isDebugEnabled()) {
            
            log.debug(ndx.toString());
            
        }

    }
    
    /**
     * The unisolated index partition. This is either a {@link BTree} or a
     * {@link FusedView}.
     */
    final private BTree ndx;
    
    /**
     * The {@link ReadWriteLock} used to permit concurrent readers on an
     * unisolated index while serializing access to that index when a writer
     * must run.
     */
    final private ReadWriteLock readWriteLock;

    /**
     * Canonicalizing mapping for the locks used to control access to the
     * unisolated index.
     */
    static final private WeakHashMap<ICommitter, ReentrantReadWriteLock> locks = new WeakHashMap<ICommitter,ReentrantReadWriteLock>();
    
    /**
     * The default capacity for iterator reads against the underlying index. The
     * main purpose of the capacity is to reduce the contention for the
     * {@link ReadWriteLock}.
     */
    final static private int DEFAULT_CAPACITY = 1000;// 10000;

    /**
     * The default capacity for iterator reads against the underlying index. The
     * main purpose of the capacity is to reduce the contention for the
     * {@link ReadWriteLock}.
     */
    final private int defaultCapacity;
    
    /**
     * Creates a view of an unisolated index that will enforce the concurrency
     * constraints of the {@link BTree} class, but only among other instances of
     * this class for the same underlying index.
     * 
     * @param ndx
     *            The underlying unisolated index.
     * 
     * @throws IllegalArgumentException
     *             if the index is <code>null</code>.
     */
    public UnisolatedReadWriteIndex(final BTree ndx) {
        
        this(ndx, DEFAULT_CAPACITY);
        
    }
    
    /**
     * Creates a view of an unisolated index that will enforce the concurrency
     * constraints of the {@link BTree} class, but only among other instances of
     * this class for the same underlying index.
     * 
     * @param ndx
     *            The underlying unisolated index.
     * @param defaultCapacity
     *            The capacity for iterator reads against the underlying index.
     *            The main purpose of the capacity is to reduce the contention
     *            for the {@link ReadWriteLock}. Relatively small values should
     *            therefore be fine. See {@link #DEFAULT_CAPACITY}.
     * 
     * @throws IllegalArgumentException
     *             if the index is <code>null</code>.
     * 
     * @todo This is using an internal canonicalizing map for the
     *       {@link ReadWriteLock}s. It would be as easy to provide a
     *       canonicalizing factory for these objects on the {@link Journal} and
     *       {@link TemporaryStore}.
     * 
     * @todo fairness is NOT required for the locks. I believe that this is
     *       supposed to provide better throughput, but that has not been
     *       tested. Also, this has not been tested with a simple mutex lock vs
     *       a read-write lock. The use case for which this class was originally
     *       developed was computing the fix point of a set of rules. In that
     *       use case, we do a lot of concurrent reading and periodically flush
     *       the computed solutions onto the relations. It is likely that a
     *       read-write lock will do well for this situation.
     */
    public UnisolatedReadWriteIndex(final BTree ndx, final int defaultCapacity) {

        if (ndx == null)
            throw new IllegalArgumentException();

        if (defaultCapacity <= 0)
            throw new IllegalArgumentException();

        this.ndx = ndx;

        this.defaultCapacity = defaultCapacity;

        this.readWriteLock = getReadWriteLock(ndx);
        
    }

	/**
	 * Canonicalizing factory for the {@link ReadWriteLock} for an {@link ICommitter}.
	 * 
	 * @param btree
	 *            The btree.
	 * @return The lock.
	 * 
	 * @throws IllegalArgumentException
	 *             if the argument is <code>null</code>.
	 */
    // Note: Exposed to HTree, at least for now.
	public static ReentrantReadWriteLock getReadWriteLock(final ICommitter btree) {

		if (btree == null)
			throw new IllegalArgumentException();
		
		synchronized (locks) {

		    ReentrantReadWriteLock readWriteLock = locks.get(btree);

			if (readWriteLock == null) {

				readWriteLock = new ReentrantReadWriteLock(false/* fair */);

				locks.put(btree, readWriteLock);

			}

			return readWriteLock;
		}

	}   
    
    @Override
    public String toString() {
        
        return getClass().getSimpleName() + "{" + ndx.toString() + "}";
        
    }
    
    @Override
    public IndexMetadata getIndexMetadata() {

        return ndx.getIndexMetadata();
        
    }

    @Override
    public IResourceMetadata[] getResourceMetadata() {

        return getIndexMetadata().getPartitionMetadata().getResources();

    }

    @Override
    public CounterSet getCounters() {

        return ndx.getCounters();
        
    }

    /**
     * This throws an exception. If you need access to the {@link ICounter} for
     * the index partition, then write and submit an {@link IIndexProcedure}.
     * 
     * @throws UnsupportedOperationException
     */
    @Override
    public ICounter getCounter() {
        
        throw new UnsupportedOperationException();
        
    }

    @Override
    public boolean contains(final Object key) {

        final Lock lock = readLock();
        
        try {
            
            return ndx.contains(key);
            
        } finally {
            
            unlock(lock);
            
        }
        
    }

    @Override
    public Object insert(final Object key, final Object value) {

        final Lock lock = writeLock();
        
        try {
            
            return ndx.insert(key,value);
            
        } finally {
            
            unlock(lock);
            
        }
    
    }

    @Override
    public Object lookup(final Object key) {
        
        final Lock lock = readLock();
        
        try {
            
            return ndx.lookup(key);
            
        } finally {
            
            unlock(lock);
            
        }
        
    }

    @Override
    public Object remove(final Object key) {

        final Lock lock = writeLock();
        
        try {
            
            return ndx.remove(key);
            
        } finally {
            
            unlock(lock);
            
        }
    
    }

    @Override
    public boolean contains(final byte[] key) {

        final Lock lock = readLock();
        
        try {
            
            return ndx.contains(key);
            
        } finally {
            
            unlock(lock);
            
        }
        
    }
    
    @Override
    public byte[] lookup(final byte[] key) {

        final Lock lock = readLock();
        
        try {
            
            return ndx.lookup(key);
            
        } finally {
            
            unlock(lock);
            
        }
        
    }

    @Override
    public byte[] insert(final byte[] key, final byte[] value) {

        final Lock lock = writeLock();
        
        try {
            
            return ndx.insert(key,value);
            
        } finally {
            
            unlock(lock);
            
        }
        
    }

    @Override
    public byte[] remove(final byte[] key) {

        final Lock lock = writeLock();
        
        try {
            
            return ndx.remove(key);
            
        } finally {
            
            unlock(lock);
            
        }

    }

    @Override
    public long rangeCount() {

        final Lock lock = readLock();

        try {
        
            return ndx.rangeCount();
            
        } finally {
            
            unlock(lock);
            
        }
        
    }
    
    @Override
    public long rangeCount(final byte[] fromKey, final byte[] toKey) {

        final Lock lock = readLock();

        try {
        
            return ndx.rangeCount(fromKey, toKey);
            
        } finally {
            
            unlock(lock);
            
        }

    }

    @Override
    public long rangeCountExact(final byte[] fromKey, final byte[] toKey) {

        final Lock lock = readLock();

        try {
        
            return ndx.rangeCountExact(fromKey, toKey);
            
        } finally {
            
            unlock(lock);
            
        }
        
    }

    @Override
    public long rangeCountExactWithDeleted(final byte[] fromKey, final byte[] toKey) {

        final Lock lock = readLock();

        try {
        
            return ndx.rangeCountExactWithDeleted(fromKey, toKey);
            
        } finally {
            
            unlock(lock);
            
        }
        
    }

    @Override
    @SuppressWarnings("rawtypes")
    final public ITupleIterator rangeIterator() {

        return rangeIterator(null, null);

    }
    
    @Override
    @SuppressWarnings("rawtypes")
    public ITupleIterator rangeIterator(final byte[] fromKey, final byte[] toKey) {

        return rangeIterator(fromKey, toKey, 0/* capacity */,
                IRangeQuery.DEFAULT, null/* filter */);

    }

    /**
     * The iterator will read on the underlying index in chunks, buffering
     * tuples as it goes. The buffer capacity is as specified by the caller and
     * will default to the capacity specified to the ctor. The iterator acquires
     * and releases the appropriate lock (either the shared read lock or the
     * exclusive write lock) before it fetches reads the next chunk of tuples
     * from the underlying index. Likewise, the mutation methods on the iterator
     * will acquire the exclusive write lock.
     */
    @Override
    @SuppressWarnings("rawtypes")
    public ITupleIterator rangeIterator(final byte[] fromKey, final byte[] toKey,
            int capacity, int flags, final IFilter filter) {

        if (capacity == 0) {
         
            /*
             * When the buffer capacity is not specified, use the default from
             * the constructor.
             */
            
            capacity = defaultCapacity;
            
        }

        if ((flags & IRangeQuery.REMOVEALL) != 0) {

            /*
             * AbstractChunkedIterator handles REMOVEALL by buffering the keys
             * for the tuples to be deleted and then deleting the keys in bulk
             * after each chunk. Therefore we need to ensure that the KEYS flag
             * is set here.
             */
            
            flags |= IRangeQuery.KEYS;
            
        }
        
        /*
         * Note: Accepts the delegate. The methods that access the delegate are
         * all overridden to acquire the appropriate lock.
         */
        
        return new ChunkedIterator(this.ndx, fromKey, toKey, capacity, flags,
                filter);

    }

    /**
     * Inner class provides a buffered iterator reading against the underlying
     * unisolated index. The class coordinates reads (and writes) with the outer
     * class using the appropriate {@link Lock}. Buffering means that the
     * iterator will read a chunk of tuples at a time, which reduces contention
     * for the {@link Lock}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private class ChunkedIterator<E> extends ChunkedLocalRangeIterator<E> {

        private ChunkedIterator(final IIndex ndx, final byte[] fromKey, final byte[] toKey,
                final int capacity, final int flags, final IFilter filter) {
            
            super(ndx, fromKey, toKey, capacity, flags, filter);
            
        }

        /**
         * Extended to acquire the exclusive write lock.
         */
        @Override
        protected void deleteBehind(final int n, final Iterator<byte[]> keys) {

            final Lock lock = writeLock();
            
            try {
            
                super.deleteBehind(n, keys);
                
            } finally {
                
                unlock(lock);
                
            }
            
        }

        /**
         * Extended to acquire the exclusive write lock.
         */
        @Override
        protected void deleteLast(final byte[] key) {

            final Lock lock = writeLock();
            
            try {
            
                super.deleteLast(key);
                
            } finally {
                
                unlock(lock);
                
            }
            
        }

        /**
         * Extended to acquire the shared read lock (or the exclusive write lock
         * if {@link IRangeQuery#REMOVEALL} was specified for the iterator).
         */
        @Override
        protected ResultSet getResultSet(final long timestamp, final byte[] fromKey,
                final byte[] toKey, final int capacity, final int flags, final IFilter filter) {

            final boolean mutation = (flags & IRangeQuery.REMOVEALL) != 0;

            final Lock lock = mutation ? writeLock() : readLock();

            try {

                return super.getResultSet(timestamp, fromKey, toKey, capacity,
                        flags, filter);
                
            } finally {
                
                unlock(lock);
                
            }
            
        }
        
    } // ChunkedIterator
    
    @Override
    public Object submit(final byte[] key, final ISimpleIndexProcedure proc) {

        final Lock lock = lock(proc);

        try {

            /*
             * Apply the procedure to the underlying index now that we are
             * holding the appropriate lock.
             */

            return ndx.submit(key, proc);

        } finally {

            unlock(lock);

        }

    }

    @Override
    @SuppressWarnings("rawtypes")
    public void submit(final byte[] fromKey, final byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler handler) {

        final Lock lock = lock(proc);

        try {

            /*
             * Apply the procedure to the underlying index now that we are
             * holding the appropriate lock.
             */

            ndx.submit(fromKey, toKey, proc, handler);

        } finally {

            unlock(lock);

        }

    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void submit(final int fromIndex, final int toIndex, final byte[][] keys,
            final byte[][] vals, final AbstractKeyArrayIndexProcedureConstructor ctor,
            final IResultHandler aggregator) {

        if (ctor == null)
            throw new IllegalArgumentException();

        final IIndexProcedure proc = ctor.newInstance(this, fromIndex, toIndex,
                keys, vals);

        final Lock lock = lock(proc);

        try {
            
            /*
             * Apply the procedure to the underlying index now that we are
             * holding the appropriate lock.
             */
            
            final Object result = proc.apply(ndx);

            if (aggregator != null) {

                aggregator.aggregate(result, new Split(null, fromIndex, toIndex));

            }
            
        } finally {
            
            unlock(lock);
            
        }
        
    }

    /**
     * Estimate the cost of a range scan.
     * 
     * @param diskCostModel
     *            The disk cost model.
     * @param rangeCount
     *            The #of tuples to be visited.
     *            
     * @return The estimated cost.
     */
    public ScanCostReport estimateCost(final DiskCostModel diskCostModel,
            final long rangeCount) {

        // BTree is its own statistics view.
        final IBTreeStatistics stats = (BTree) ndx;

        // Estimate cost based on random seek per node/leaf.
        final double cost = new BTreeCostModel(diskCostModel).rangeScan(
                rangeCount, stats.getBranchingFactor(), stats.getHeight(),
                stats.getUtilization().getLeafUtilization());

        return new ScanCostReport(rangeCount, cost);

    }

    @Override
    public long indexOf(final byte[] key) {
        final Lock lock = readLock();
        try {
            return ndx.indexOf(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] keyAt(final long index) {
        final Lock lock = readLock();
        try {
            return ndx.keyAt(index);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] valueAt(final long index) {
        final Lock lock = readLock();
        try {
            return ndx.valueAt(index);
        } finally {
            lock.unlock();
        }
    }

}
