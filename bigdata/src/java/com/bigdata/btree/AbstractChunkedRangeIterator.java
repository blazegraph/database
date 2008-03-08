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
 * Created on Feb 1, 2008
 */

package com.bigdata.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.ITx;
import com.bigdata.rawstore.IBlock;
import com.bigdata.service.DataServiceRangeIterator;

/**
 * A chunked iterator that proceeds a {@link ResultSet} at a time.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractChunkedRangeIterator implements ITupleIterator {

    public static final transient Logger log = Logger
            .getLogger(DataServiceRangeIterator.class);

    /**
     * Error message used by {@link #getKey()} when the iterator was not
     * provisioned to request keys from the data service.
     */
    static protected transient final String ERR_NO_KEYS = "Keys not requested";

    /**
     * Error message used by {@link #getValue()} when the iterator was not
     * provisioned to request values from the data service.
     */
    static protected transient final String ERR_NO_VALS = "Values not requested";

    /**
     * The first key to visit -or- null iff no lower bound.
     */
    protected final byte[] fromKey;

    /**
     * The first key to NOT visit -or- null iff no upper bound.
     */
    protected final byte[] toKey;

    /**
     * This controls the #of results per data service query.
     */
    protected final int capacity;

    /**
     * These flags control whether keys and/or values are requested. If neither
     * keys nor values are requested, then this is just a range count operation
     * and you might as well use rangeCount instead.
     */
    protected final int flags;

    /**
     * Optional filter.
     */
    protected final ITupleFilter filter;

    /**
     * The #of range query operations executed.
     */
    protected int nqueries = 0;

    /**
     * The current result set. For each index partition spanned by the overall
     * key range supplied by the client, we will issue at least one range query
     * against the data service for that index partition. Once all entries in a
     * result set have been consumed by the client, we test the result set to
     * see whether or not it exhausted the entries that could be matched for
     * that index partition. If not, then we will issue another "continuation"
     * range query against the same index position starting from the successor
     * of the last key scanned. If it is exhausted, then we will issue a new
     * range query against the next index partition. If no more index partitions
     * remain that are spanned by the key range specified by the client then we
     * are done.
     * <p>
     * Note: A result set will be empty if there are no entries (including after
     * filtering) that lie within the key range in a given index partition. It
     * is possible for any of the result sets to be empty. Consider a case of
     * static partitioning of an index into N partitions. When the index is
     * empty, a range query of the entire index will still query each of the N
     * partitions. However, since the index is empty none of the partitions will
     * have any matching entries and all result sets will be empty.
     */
    protected ResultSet rset = null;

    /**
     * The timestamp for the operation as specified by the ctor (this is used
     * for remote index queries but when running against a local index).
     */
    protected abstract long getTimestamp();
    
    /**
     * Note: value is 0L until the first {@link ResultSet} has been read.
     */
    private long commitTime = 0L;
    
    /**
     * The timestamp returned by the initial {@link ResultSet}. 
     */
    public long getCommitTime() {
        
        return commitTime;
        
    }
    
    /**
     * When <code>true</code> the {@link #getCommitTime()} will be used to
     * ensure that {@link #continuationQuery()}s run against the same commit
     * point thereby producing a consistent view even when the iterator is
     * {@link ITx#UNISOLATED} or {@link ITx#READ_COMMITTED}. When
     * <code>false</code> {@link #continuationQuery()}s will use whatever
     * value is returned by {@link #getTimestamp()}.
     */
    abstract public boolean getReadConsistent();
    
    /**
     * Return the timestamp used for {@link #continuationQuery()}s. The value
     * returned depends on whether or not {@link #getReadConsistent()} is
     * <code>true</code>. When consistent reads are required the timestamp
     * will be the {@link ResultSet#getCommitTime()} for the initial
     * {@link ResultSet}. Otherwise it is the value returned by
     * {@link #getTimestamp()}.
     * 
     * @throws IllegalStateException
     *             if {@link #getReadConsistent()} is <code>true</code> and
     *             the initial {@link ResultSet} has not been read since the
     *             commitTime for that {@link ResultSet} is not yet available.
     */
    final public long getReadTime() {
        
        if(getReadConsistent() ) {
            
            if (commitTime == 0L) {

                /*
                 * The commitTime is not yet available (nothing has been read).
                 */
                
                throw new IllegalStateException();
                
            }
            
            return commitTime;
            
        }
        
        return getTimestamp();
        
    }
    
    /**
     * The #of enties visited so far.
     */
    protected long nvisited = 0;

    /**
     * The index of the last entry visited in the current {@link ResultSet}.
     * This is reset to <code>-1</code> each time we obtain a new
     * {@link ResultSet}.
     */
    protected int lastVisited = -1;

    /**
     * When true, the entire key range specified by the client has been visited
     * and the iterator is exhausted (i.e., all done).
     */
    protected boolean exhausted = false;

    /**
     * The #of queries issued so far.
     */
    public int getQueryCount() {

        return nqueries;

    }

    /**
     * The #of entries visited so far (not the #of entries scanned, which can be
     * much greater if a filter is in use).
     */
    public long getVisitedCount() {

        return nvisited;

    }

    /**
     * The capacity used by default when the caller specified <code>0</code>
     * as the capacity for the iterator.
     */
    protected int getDefaultCapacity() {
        
        return 100000;
        
    }
    
    public AbstractChunkedRangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, ITupleFilter filter) {

        if (capacity < 0) {

            throw new IllegalArgumentException();

        }

        this.fromKey = fromKey;
        
        this.toKey = toKey;
        
        this.capacity = capacity == 0 ? getDefaultCapacity() : capacity;
        
        this.flags = flags;
        
        this.filter = filter;

    }

    /**
     * Abstract method must return the next {@link ResultSet} based on the
     * supplied parameter values.
     * 
     * @param timestamp 
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filter
     * @return
     */
    abstract protected ResultSet getResultSet(long timestamp,byte[] fromKey, byte[] toKey,
            int capacity, int flags, ITupleFilter filter);

    /**
     * Issues the original range query.
     */
    protected void rangeQuery() {

        assert !exhausted;

        log.info("fromKey=" + BytesUtil.toString(fromKey) + ", toKey="
                + BytesUtil.toString(toKey));

        // initial query.
        rset = getResultSet(getTimestamp(), fromKey, toKey, capacity, flags, filter);

        // Note: will be 0L if reading on a local index.
        commitTime = rset.getCommitTime();
        
        // reset index into the ResultSet.
        lastVisited = -1;

        nqueries++;

    }

    /**
     * Issues a "continuation" query against the same index. This is invoked iff
     * the there are no entries left to visit in the current {@link ResultSet}
     * but {@link ResultSet#isExhausted()} is [false], indicating that there is
     * more data available.
     */
    protected void continuationQuery() {

        assert !exhausted;
        assert rset != null;
        assert !rset.isExhausted();

        /*
         * Save the last visited key for #remove(). 
         */
        lastVisitedKeyInPriorResultSet = tuple.getKeysRequested() ? tuple
                .getKey() : null;
        
        /*
         * Start from the successor of the last key scanned by the previous
         * result set.
         */

        final byte[] _fromKey = rset.successor();

        log.info("fromKey=" + BytesUtil.toString(_fromKey) + ", toKey="
                + BytesUtil.toString(toKey));

        // continuation query.
        rset = getResultSet(getReadTime(),_fromKey, toKey, capacity, flags, filter);

        // reset index into the ResultSet.
        lastVisited = -1;

        nqueries++;
        
        deleteBehind();

    }
    
    /**
     * This gets set by {@link #continuationQuery()} to the value of the key for
     * the then current {@link #tuple}. This is used by {@link #remove()} in
     * the edge case where {@link #lastVisited} is <code>-1</code> because a
     * continuation query has been issued but {@link #next()} has not yet been
     * invoked. It is cleared by {@link #next()} so that it does not hang
     * around.
     */
    protected byte[] lastVisitedKeyInPriorResultSet;

    /**
     * There are three levels at which we need to test in order to determine if
     * the total iterator is exhausted. First, we need to test to see if there
     * are more entries remaining in the current {@link ResultSet}. If not and
     * the {@link ResultSet} is NOT {@link ResultSet#isExhausted() exhausted},
     * then we issue a {@link #continuationQuery()} against the same index
     * partition. If the {@link ResultSet} is
     * {@link ResultSet#isExhausted() exhausted}, then we test to see whether
     * or not we have visited all index partitions. If so, then the iterator is
     * exhausted. Otherwise we issue a range query against the
     * {@link #nextPartition()}.
     * 
     * @return True iff the iterator is not exhausted.
     */
    public boolean hasNext() {

        if (nqueries == 0) {

            // Obtain the first result set.
            rangeQuery();

        }
        
        assert rset != null;

        if (exhausted) {

            return false;

        }

        final int ntuples = rset.getNumTuples();

        if (ntuples > 0 && lastVisited + 1 < ntuples) {
            /*
             * There is more data in the current ResultSet.
             */
            return true;
        }

        if (!rset.isExhausted()) {
            
            /*
             * This result set is empty but there is more data available.
             */
            
            continuationQuery();
            
            /*
             * Recursive query since the result set might be empty.
             * 
             * Note: The result set could be empty if we are: (a) unisolated; or
             * (b) in a read committed transaction; or (c) if a filter is being
             * applied.
             */
            
            return hasNext();
            
        }
        
        // Exausted.
        exhausted = true;
        
        // flush any buffered deletes.
        flush();
        
        return false;
        
    }

    public ITuple next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        lastVisitedKeyInPriorResultSet = null; // clear
        
        nvisited++; // total #visited.

        lastVisited++; // index of last visited tuple in current result set.

        return tuple;

    }

    final private ResultSetTuple tuple = new ResultSetTuple();

    /**
     * An {@link ITuple} that draws its data from a {@link ResultSet}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ResultSetTuple implements ITuple {

        protected ResultSetTuple() {

        }

        public int getSourceIndex() {
            
            if (lastVisited == -1)
                throw new IllegalStateException();

            return rset.getSourceIndex(lastVisited);
            
        }
        
        public int flags() {
            
            return flags;
            
        }
        
        public boolean getKeysRequested() {

            return (flags & IRangeQuery.KEYS) != 0;

        }

        public boolean getValuesRequested() {

            return (flags & IRangeQuery.VALS) != 0;

        }
        
        public byte[] getKey() {

            if (lastVisited == -1)
                throw new IllegalStateException();

            if (!getKeysRequested())
                throw new UnsupportedOperationException();

            return rset.getKey(lastVisited);

        }

        public ByteArrayBuffer getKeyBuffer() {

            return new ByteArrayBuffer(getKey());

        }

        public DataInputBuffer getKeyStream() {
            byte[] key = getKey();
            if (keyStream == null) {
                keyStream = new DataInputBuffer(key);
            } else {
                keyStream.setBuffer(key);
            }
            return keyStream;
        }

        private DataInputBuffer keyStream = null;

        public byte[] getValue() {

            if (lastVisited == -1)
                throw new IllegalStateException();

            if (!getValuesRequested())
                throw new UnsupportedOperationException();

            return rset.getValue(lastVisited);

        }

        public boolean isNull() {
            
            return getValue() == null;
            
        }
        
        public ByteArrayBuffer getValueBuffer() {

            final byte[] val = getValue();
            
            if (val == null)
                throw new UnsupportedOperationException();

            return new ByteArrayBuffer(val);

        }

        public DataInputBuffer getValueStream() {
            byte[] val = getValue();
            if (val == null)
                throw new UnsupportedOperationException();
            if (valStream == null) {
                valStream = new DataInputBuffer(val);
            } else {
                valStream.setBuffer(val);
            }
            return valStream;
        }

        private DataInputBuffer valStream = null;

        public long getVersionTimestamp() {

            if (lastVisited == -1)
                throw new IllegalStateException();

            if (rset.getVersionTimestamps() == null) {

                // Version timestamps not maintained by the index.

                return 0L;

            }

            return rset.getVersionTimestamps()[lastVisited];

        }

        public boolean isDeletedVersion() {

            if (lastVisited == -1)
                throw new IllegalStateException();

            if (rset.getDeleteMarkers() == null) {

                // Delete markers not maintained by the index.
                
                return false;
                
            }

            return rset.getDeleteMarkers()[lastVisited] == 0 ? false : true;

        }

        public long getVisitCount() {

            /*
             * The total #of tuples visited by itr across all result sets.
             */
            
            return nvisited;

        }

        public IBlock readBlock(long addr) {

            final int sourceIndex = getSourceIndex();
            
            return AbstractChunkedRangeIterator.this.readBlock(sourceIndex,
                    addr);
            
        }

        public String toString() {
            return super.toString()+
            "{nvisited="+nvisited+
            (isDeletedVersion()? ", deleted" : "")+
            (getVersionTimestamp() == 0L ? "" : ", timestamp="+ getVersionTimestamp())+
            ", key="+(getKeysRequested()?Arrays.toString(getKey()):"N/A")+
            ", val="+(getValuesRequested()?(isNull()?"null":Arrays.toString(getValue())):"N/A")+
            "}";
            }

    }

    /**
     * Queues a request to remove the entry under the most recently visited key.
     * If the iterator is exhausted then the entry will be deleted immediately.
     * Otherwise the requests will be queued until the current {@link ResultSet}
     * is exhausted and then a batch delete will be done for the queue.
     */
    synchronized public void remove() {

        if (nvisited == 0) {

            throw new IllegalStateException();

        }

        if (!tuple.getKeysRequested()) {

            throw new UnsupportedOperationException(ERR_NO_KEYS);

        }

        if (removeList == null) {

            removeList = new ArrayList<byte[]>(capacity);

        }

        final byte[] key = lastVisited == -1 ? lastVisitedKeyInPriorResultSet
                : tuple.getKey();

        assert key != null;

        /*
         * Test to see if the iterator is willing to _seek_ more results within
         * the _current_ result set. We need to do this without calling
         * hasNext() since that will cause the next result set to be fetched if
         * the current one has been consumed and that would make the iterator
         * eagerly fetch more result sets than the consumer has actually
         * demanded by calling #hasNext() on their end.
         */
        if (!exhausted && nvisited < capacity) {

            // queue up for batch delete.
            removeList.add(key);

        } else {

            // delete immediately since the iterator is exhausted.
            deleteLast(key);

        }

    }

    private ArrayList<byte[]> removeList;

    /**
     * Method flushes any queued deletes. You MUST do this if you are only
     * processing part of the buffered capacity of the iterator and you are are
     * deleting some index entries. Failure to {@link #flush()} under these
     * circumstances will result in some buffered deletes never being applied.
     */
    public void flush() {

        deleteBehind();
        
    }
    
    protected void deleteBehind() {
        
        if(removeList==null||removeList.isEmpty()) return;
        
        deleteBehind(removeList.size(),removeList.iterator());
        
        removeList.clear();
        
    }
    
    /**
     * Batch delete the index entries identified by <i>keys</i> and clear the
     * list.
     * 
     * @param n
     *            The #of keys to be deleted.
     * @param keys
     *            The keys to be deleted.
     */
    abstract protected void deleteBehind(int n,Iterator<byte[]> keys);

    /**
     * Delete the index entry identified by <i>key</i>.
     * 
     * @param key
     *            A key.
     */
    abstract protected void deleteLast(byte[] key);

    /**
     * Return an object that may be used to read the block from the backing
     * store per the contract for {@link ITuple#readBlock(long)}
     * 
     * @param sourceIndex
     *            The value from {@link ITuple#getSourceIndex()}.
     * 
     * @param addr
     *            The value supplied to {@link ITuple#readBlock(long)}.
     */
    abstract protected IBlock readBlock(int sourceIndex, long addr);

}
