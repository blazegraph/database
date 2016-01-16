/**

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
package com.bigdata.journal;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.bigdata.btree.BTree;
import com.bigdata.btree.DelegateBTree;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.UnisolatedReadWriteIndex;

/**
 * Abstract {@link BTree} mapping <em>commitTime</em> (long integers) to
 * {@link ICommitTimeEntry} objects.
 * <p>
 * This class is thread-safe for concurrent readers and writers.
 */
public class AbstractCommitTimeIndex<T extends ICommitTimeEntry> extends
        DelegateBTree implements ILinearList {

    /**
     * The underlying index. Access to this is NOT thread safe unless you take
     * the appropriate lock on the {@link #readWriteLock}.
     */
    private final BTree btree;

//    /**
//     * The {@link ReadWriteLock} used by the {@link UnisolatedReadWriteIndex} to
//     * make operations on the underlying {@link #btree} thread-safe.
//     */
//    private final ReadWriteLock readWriteLock;
    
    @SuppressWarnings("unchecked")
    private Tuple<T> getLookupTuple() {
        
        return btree.getLookupTuple();
        
    }
    
//    /**
//     * Instance used to encode the timestamp into the key.
//     */
//    final private IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

    protected AbstractCommitTimeIndex(final BTree ndx) {

        // Wrap B+Tree for read/write thread safety.
        super(new UnisolatedReadWriteIndex(ndx));
        
        this.btree = ndx;
        
    }
    
    /**
     * Encodes the commit time into a key.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @return The corresponding key.
     */
    public byte[] getKey(final long commitTime) {

        return getIndexMetadata().getKeyBuilder().reset().append(commitTime)
                .getKey();

    }

    /**
     * Returns (but does not take) the {@link ReadLock}.
     */
    public Lock readLock() {

        return btree.readLock();
        
    }
    
    /**
     * Returns (but does not take) the {@link WriteLock}.
     */
    public Lock writeLock() {

        return btree.writeLock();
        
    }

    public long getEntryCount() {

        return super.rangeCount();
        
    }
    
    @SuppressWarnings("unchecked")
    public Tuple<T> valueAt(final long index, final Tuple<T> t) {
        final Lock lock = readLock();
        lock.lock();
        try {
            return btree.valueAt(index, t);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return the {@link IRootBlock} identifying the journal having the largest
     * commitTime that is less than or equal to the given timestamp. This is
     * used primarily to locate the commit record that will serve as the ground
     * state for a transaction having <i>timestamp</i> as its start time. In
     * this context the LTE search identifies the most recent commit state that
     * not later than the start time of the transaction.
     * 
     * @param timestamp
     *            The given timestamp.
     * 
     * @return The description of the relevant journal resource -or-
     *         <code>null</code> iff there are no journals in the index that
     *         satisify the probe.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than ZERO (0L).
     */
     public T find(final long timestamp) {

        if (timestamp < 0L)
            throw new IllegalArgumentException();

        final Lock lock = readLock();

        lock.lock();
        
        try {

            // find (first less than or equal to).
            final long index = findIndexOf(timestamp);

            if (index == -1) {

                // No match.
                return null;

            }

            return valueAtIndex(index);

        } finally {

            lock.unlock();

        }
        
    }

    /**
     * Retrieve the entry from the index.
     */
    private T valueAtIndex(final long index) {

        return (T) valueAt(index, getLookupTuple()).getObject();

//        final byte[] val = super.valueAt(index);
//
//        assert val != null : "Entry has null value: index=" + index;
//        
//        final IRootBlockView entry = new RootBlockView(false/* rootBlock0 */,
//                ByteBuffer.wrap(val), ChecksumUtility.getCHK());
//
//        return entry;

    }
    
    /**
     * Return the first entry whose <em>commitTime</em> is strictly greater than
     * the timestamp.
     * 
     * @param timestamp
     *            The timestamp. A value of ZERO (0) may be used to find the
     *            first entry.
     * 
     * @return The root block of that entry -or- <code>null</code> if there is
     *         no entry whose timestamp is strictly greater than
     *         <i>timestamp</i>.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than ZERO (0L).
     */
    public T findNext(final long timestamp) {

        if (timestamp < 0L)
            throw new IllegalArgumentException();

        final Lock lock = readLock();

        lock.lock();

        try {

            // find first strictly greater than.
            final long index = findIndexOf(timestamp) + 1;

            if (index == rangeCount()) {

                // No match.

                return null;

            }

            return valueAtIndex(index);

        } finally {

            lock.unlock();

        }

    }

    /**
     * Find the index of the entry associated with the largest commitTime that
     * is less than or equal to the given timestamp.
     * 
     * @param commitTime
     *            The timestamp.
     * 
     * @return The index of the entry associated with the largest commitTime
     *         that is less than or equal to the given timestamp -or-
     *         <code>-1</code> iff the index is empty.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than ZERO (0L).
     */
    public long findIndexOf(final long commitTime) {
        
        if (commitTime < 0L)
            throw new IllegalArgumentException();

        /*
         * Note: Since this is the sole index access, we don't need to take the
         * lock to coordinate a consistent view of the index in this method.
         */
        long pos = indexOf(getKey(commitTime));
        
        if (pos < 0) {

            /*
             * the key lies between the entries in the index, or possible before
             * the first entry in the index. [pos] represents the insert
             * position. we convert it to an entry index and subtract one to get
             * the index of the first commit record less than the given
             * timestamp.
             */
            
            pos = -(pos+1);

            if (pos == 0) {

                // No entry is less than or equal to this timestamp.
                return -1;

            }
       
            pos--;

            return pos;
            
        } else {
            
            /*
             * exact hit on an entry.
             */
            
            return pos;
            
        }

    }
    
    /**
     * Add an entry under the commitTime associated with the entry.
     * 
     * @param entry
     *            The entry
     * 
     * @exception IllegalArgumentException
     *                if <i>commitTime</i> is <code>0L</code>.
     * @exception IllegalArgumentException
     *                if <i>rootBLock</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if there is already an entry registered under for the
     *                given timestamp.
     */
    public void add(final T entry) {

        if (entry == null)
            throw new IllegalArgumentException();

        final long commitTime = entry.getCommitTime();

        if (commitTime == 0L)
            throw new IllegalArgumentException();

        final Lock lock = writeLock();

        lock.lock();

        try {

            final byte[] key = getKey(commitTime);

            if (super.contains(key)) {

                throw new IllegalArgumentException("entry exists: timestamp="
                        + commitTime);

            }

            // add a serialized entry to the persistent index.
            super.insert(key, entry);

        } finally {

            lock.unlock();

        }

    }
   
    /**
     * Find and return the oldest entry (if any).
     * 
     * @return That entry -or- <code>null</code> if there are no entries.
     */
    public T getOldestEntry() {

        final Lock lock = readLock();

        lock.lock();
        
        try {

            if (rangeCount() == 0L) {

                // Empty index.
                return null;

            }

            // Lookup first tuple in index.
            final ITuple<T> t = valueAt(0L, getLookupTuple());

            final T r = t.getObject();

            return r;

        } finally {

            lock.unlock();
            
        }

    }
    
    /**
     * Find the the most recent entry (if any).
     * 
     * @return That entry -or- <code>null</code> if there are no entries.
     */
    public T getNewestEntry() {
        
        final Lock lock = readLock();

        lock.lock();

        try {

            final long entryCount = getEntryCount();

            if (entryCount == 0L)
                return null;

            return valueAt(entryCount - 1, getLookupTuple()).getObject();

        } finally {
            
            lock.unlock();
            
        }
        
    }
    
    /**
     * Find the oldest entry whose commit counter is LTE the specified commit
     * counter.
     * 
     * @return The entry -or- <code>null</code> if there is no such entry.
     * 
     * @throws IllegalArgumentException
     *             if <code>commitCounter LT ZERO (0)</code>
     * 
     *             TODO It is possible to improve the performance for this for
     *             large indices using a binary search. Each time we probe the
     *             index we discover a specific commit counter value. If the
     *             value is LT the target, we need to search above that probe.
     *             If GT the target, we need to search below that problem.
     */
    public T findByCommitCounter(final long commitCounter) {

        if (commitCounter < 0L)
            throw new IllegalArgumentException();

        final Lock lock = readLock();

        lock.lock();
        
        try {

            // Reverse scan.
            @SuppressWarnings("unchecked")
            final ITupleIterator<T> itr = rangeIterator(
                    null/* fromKey */, null/* toKey */, 0/* capacity */,
                    IRangeQuery.DEFAULT | IRangeQuery.REVERSE/* flags */, null/* filter */);

            while (itr.hasNext()) {

                final ITuple<T> t = itr.next();

                final T r = t.getObject();

                final IRootBlockView rb = r.getRootBlock();

                if (rb.getCommitCounter() <= commitCounter) {

                    // First entry LTE that commit counter.
                    return r;

                }

            }

            return null;

        } finally {

            lock.unlock();
            
        }
       
    }
    
    /**
     * Return the entry that is associated with the specified ordinal index
     * (origin ZERO) counting backwards from the most recent entry (0) towards
     * the earliest entry (nentries-1).
     * <p>
     * Note: The effective index is given by <code>(entryCount-1)-index</code>.
     * If the effective index is LT ZERO (0) then there is no such entry and
     * this method will return <code>null</code>.
     * 
     * @param index
     *            The index.
     * 
     * @return The entry -or- <code>null</code> if there is no such entry.
     * 
     * @throws IllegalArgumentException
     *             if <code>index LT ZERO (0)</code>
     */
    public T getEntryByReverseIndex(final int index) {

        if (index < 0)
            throw new IllegalArgumentException();
        
        final Lock lock = readLock();

        lock.lock();
        
        try {

            final long entryCount = rangeCount();
            
            if (entryCount > Integer.MAX_VALUE)
                throw new AssertionError();

            final int effectiveIndex = ((int) entryCount - 1) - index;
            
            if (effectiveIndex < 0) {

                // No such entry.
                return null;

            }

            final ITuple<T> t = valueAt(effectiveIndex,
                    getLookupTuple());

            final T r = t.getObject();
            
            return r;

        } finally {
            
            lock.unlock();
            
        }

    }

    public void removeAll() {

        final Lock lock = writeLock();

        lock.lock();

        try {

            btree.removeAll();

        } finally {

            lock.unlock();
            
        }

    }

}
