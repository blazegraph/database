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
package com.bigdata.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DelegateTuple;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.AbstractKeyRangeIndexProcedure;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.util.InnerCause;

/**
 * Class supports range query across one or more index partitions. Each
 * partition is mapped onto a single {@link DataServiceTupleIterator} query. In
 * turn, the {@link DataServiceTupleIterator} may make several queries to the
 * data service per partition. The actual #of queries made to the data service
 * depends on the #of index entries that are visited per partition and the
 * capacity specified to the ctor.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionedTupleIterator<E> implements ITupleIterator<E> {

    protected static final transient Logger log = Logger
            .getLogger(PartitionedTupleIterator.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
//    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * The index on which the range query is being performed.
     */
    private final ClientIndexView ndx;
    
    /**
     * Iterator traversing the index partition locators spanned by the query.
     */
    private Iterator<PartitionLocator> locatorItr;
    
    /**
     * The timestamp from the ctor.
     */
    private final long ts;

    /**
     * <code>true</code> iff the {@link #ts} is a read-historical
     * transaction created specifically to give the iterator read-consistent
     * semantics. when <code>true</code>, this class will ensure that the
     * transaction is eventually aborted so that its read lock will be released.
     */
    private final boolean isReadConsistentTx;
    
    /**
     * The first key to visit -or- <code>null</code> iff no lower bound (from
     * the ctor).
     */
    private final byte[] fromKey;
    
    /**
     * The first key to NOT visit -or- <code>null</code> iff no upper bound
     * (from the ctor).
     */
    private final byte[] toKey;

    /**
     * This controls the #of results per data service query.
     */
    private final int capacity;

    /**
     * These flags control whether keys and/or values are requested. If
     * neither keys nor values are requested, then this is just a range
     * count operation and you might as well use rangeCount instead.
     */
    private final int flags;
    
    private final IFilterConstructor filter;
    
    /**
     * <code>true</code> iff {@link IRangeQuery#REVERSE} was specified by the
     * caller. When {@link IRangeQuery#REVERSE} was specified then we will use a
     * reverse locator scan so that we proceed in reverse order over the index
     * partitions as in reverse order within each index partition.
     */
    private final boolean reverseScan;

    /**
     * The {@link #currentFromKey} and {@link #currentToKey} are updated each
     * time we formulate a query against the partitioned index, which can occur
     * one or more times per index partition. The update of the fields depends
     * on whether we are doing a forward or reverse scan. If we have to handle a
     * {@link StaleLocatorException} then these fields will be used to restart
     * the locator scan.
     * <p>
     * The {@link #currentFromKey} is initially set by the ctor to the
     * {@link #fromKey}.
     * <p>
     * For a reverse scan, the {@link #currentFromKey} remains unchanged.
     * <p>
     * For a forward scan the {@link #currentFromKey} is set to the last key
     * visited in each {@link ResultSet}.
     * 
     * @todo {@link ResultSet#getLastKey()} will return <code>null</code> if
     *       there were no tuples to visit. This class does not appear to handle
     *       that condition, but it is able to pass a unit test where there is
     *       an empty partition in the middle of the key range scan for both
     *       forward and reverse traversal and a test where there is a non-empty
     *       partition whose tuples are filtered out such that it is effectively
     *       empty. see {@link TestRangeQuery#test_reverseScan()}
     * 
     * @todo if we restart a locator scan and the index partition boundaries
     *       have changed, then what do we need to do in order to make sure that
     *       we are issuing the query?
     */
    private byte[] currentFromKey;
    
    /**
     * The {@link #currentToKey} is initially set by the ctor to the
     * {@link #toKey}.
     * <p>
     * For a forward scan, the {@link #currentToKey} remains unchanged.
     * <p>
     * For a reverse scan, the {@link #currentToKey} is set to the last key
     * visited in each {@link ResultSet}.
     */
    private byte[] currentToKey;
    
    /**
     * The metadata for the current index partition.
     */
    private PartitionLocator locator = null;

    /**
     * The last locator for which we received a {@link StaleLocatorException}.
     * We note this so that we can avoid an endless retry if the same locator is
     * reported when we attempt to restart the {@link #locatorItr}.
     */
    private PartitionLocator lastStaleLocator = null;
    
    /**
     * The #of index partitions that have been queried so far. There will be one
     * {@link DataServiceTupleIterator} query issued per partition.
     * 
     * @deprecated The #of partitions is a bit tricky since splits and joins can
     *             change the #of index partitions dynamically (for unisolated
     *             or read-committed reads where read-consistent is not true).
     */
    private int nparts = 0;
    
    /**
     * The #of tuples visited so far.
     */
    private long nvisited = 0L;
    
    /**
     * The {@link DataServiceTupleIterator} reading from the current index
     * partition.
     */
    private DataServiceTupleIterator<E> src;
   
    /**
     * When true, the entire key range specified by the client has been
     * visited and the iterator is exhausted (i.e., all done).
     * 
     * @see #close()
     */
    private boolean exhausted = false;

    /**
     * The #of index partitions queried so far.
     * 
     * @deprecated The #of partitions is a bit tricky since splits and joins can
     *             introduce new partitions unless you are using a
     *             read-consistent view.
     */
    public int getPartitionCount() {
        
        return nparts;
        
    }
    
    /**
     * The #of entries visited so far (not the #of entries scanned, which
     * can be much greater if a filter is in use).
     */
    public long getVisitedCount() {
        
        return nvisited;
        
    }
    
    /**
     * <p>
     * Note: The {@link PartitionedTupleIterator} uses a sequential scan (rather
     * than mapping across the index partitions in parallel) and always picks up
     * from the successor of the last key visited. Read-consistent is achieved
     * by specifying a commitTime for the <i>timestamp</i> rather than
     * {@link ITx#READ_COMMITTED}. The latter will use dirty reads (each time a
     * {@link ResultSet} is fetched it will be fetched from the most recently
     * committed state of the database).
     * 
     * @param ndx
     * @param ts
     *            The timestamp for the view (may be a transaction).
     * @param isReadConsistentTx
     *            <code>true</code> iff the caller specified timestamp is a
     *            read-historical transaction created specifically to give the
     *            iterator read-consistent semantics. when <code>true</code>,
     *            this class will ensure that the transaction is eventually
     *            aborted so that its read lock will be released. This is done
     *            eagerly when the iterator is exhausted and with a
     *            {@link #finalize()} method otherwise.
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filter
     * 
     * @throws IllegalArgumentException
     *             if readConsistent is requested and the index view is
     *             {@link ITx#UNISOLATED}.
     */
    public PartitionedTupleIterator(final ClientIndexView ndx,
            final long ts, final boolean isReadConsistentTx, final byte[] fromKey,
            final byte[] toKey, final int capacity, final int flags,
            final IFilterConstructor filter) {

        if (ndx == null) {

            throw new IllegalArgumentException();
            
        }
        
        if (capacity < 0) {

            throw new IllegalArgumentException();
            
        }
        
        this.ndx = ndx;
        this.ts = ts;
        this.isReadConsistentTx = isReadConsistentTx;
        this.fromKey = this.currentFromKey = fromKey;
        this.toKey = this.currentToKey = toKey;
        this.capacity = capacity;
        
        this.flags = flags;
                
        this.filter = filter;

        this.reverseScan = (flags & IRangeQuery.REVERSE) != 0;

        // start locator scan
        this.locatorItr = ndx.locatorScan(ts, fromKey, toKey, reverseScan);

    }

    protected void finalize() {
        
        close();
        
    }

    /**
     * Marks the iterator as {@link #exhausted} and aborts the {@link #ts} iff
     * it was identified to the ctor as being created specifically to provide
     * read-consistent semantics for this iterator and hence our responsibility
     * to clean up.
     */
    synchronized private void close() {

        if (exhausted)
            return;

        exhausted = true;

        if (isReadConsistentTx) {

            try {

                ndx.getFederation().getTransactionService().abort(ts);

            } catch (IOException e) {

                // log and ignore since the caller was not directly affected.
                log.error(ClientIndexView.ERR_ABORT_TX + ts, e);

            }

        }

    }
    
    public boolean hasNext() {

        if (exhausted) {

            return false;
            
        }

        if (locator == null) {

            // Setup query for the first partition.
            
            if (!nextPartition()) {
                
                return false;
                
            }

        }
        
        assert src != null;

        try {

            if(src.hasNext()) {
            
                // More from the current source iterator.
                
                return true;
                
            }
            
        } catch(RuntimeException ex) {
            
            final StaleLocatorException cause = (StaleLocatorException) InnerCause
                    .getInnerCause(ex, StaleLocatorException.class);
            
            if(cause != null) {
                
                /*
                 * Handle StaleLocatorException. This exception indicates that
                 * we have a stale index partition locator. This can happen when
                 * index partitions are split, joined, or moved. It can only
                 * happen for UNISOLATED or READ_COMMITTED operations since we
                 * never change historical locators. Also, If the index view is
                 * read-committed and read-consistent operations are specified,
                 * then IIndexStore#getLastCommitTime() will be used so stale
                 * locators will not occur for that case either.
                 */
                
                if (lastStaleLocator != null) {

                    if (lastStaleLocator.getPartitionId() == locator
                            .getPartitionId()) {

                        /*
                         * This happens if we get a StaleLocatorException,
                         * restart the locator scan, and get another
                         * StaleLocatorException on the same index partition.
                         * Since a new index partition identifier is assigned
                         * every time there is a split/join/move this is a clear
                         * indication that something is wrong with either the
                         * locator, with the cached view of the metadata index
                         * used by the client, or the data service. For example,
                         * the client may have failed to refresh its cached view
                         * for the locator or the index partition might have
                         * been dropped on the data service (a no-no to be
                         * sure).
                         */

                        throw new RuntimeException(
                                "Missing index partition on data service? "
                                        + locator, ex);

                    }
                    
                }
                
                // notify the client so that it can refresh its cache.
                ndx.staleLocator(ts, locator,cause);
                
                // save reference
                lastStaleLocator = locator;
                
                // clear since invalid.
                locator = null;
                
                // Re-start the locator scan.
                locatorItr = ndx.locatorScan(ts, currentFromKey,
                        currentToKey, reverseScan);
                
                // Recursive query.
                return hasNext();
                
            } else throw ex;
            
        }
        
        /*
         * The current index partition is empty, but there are other index
         * partitions left to query.
         * 
         * Each source iterator reads from one index partition. (The source
         * iterator is itself a chunked iterator so it may issue multiple remote
         * requests to consume the data available on a given index partition).
         */
        
        if(nextPartition()) {
        
            /*
             * Recursive query since the index partition might be empty.
             */
            
            return hasNext();
            
        }
        
        /*
         * Exausted.
         */
        
        close();
        
        return false;
        
    }

    /**
     * Issues a new range query against the next index partititon.
     */
    private boolean nextPartition() {

        assert ! exhausted;
        
        if (Thread.interrupted()) {

            // notice an interrupt no later than the next partition.
            throw new RuntimeException(new InterruptedException());
            
        }

        if (!locatorItr.hasNext()) {

            if(INFO)
                log.info("No more locators");

            return false;
            
        }
       
        locator = locatorItr.next();
        
        if (INFO)
            log.info("locator=" + locator);
        
        // submit query to the next partition.
        rangeQuery();

        assert src != null;

        return true;

    }
    
    /**
     * Issues a range query against the current {@link #locator}.
     */
    private void rangeQuery() {

        assert ! exhausted;
        
        assert locator != null;

        if (Thread.interrupted()) {

            // notice an interrupt no later than the next chunk.
            throw new RuntimeException(new InterruptedException());
            
        }
        
        try {

            /*
             * Note: The range query request is formed such that it addresses
             * only those keys that actually lie within the partition and also
             * within the caller's given key range. This has two benefits:
             * 
             * (1) The data service can check the range and report an error for
             * clients that appear to be requesting data for index partitions
             * that have been relocated.
             * 
             * (2) It avoids double-counting (or possible under-counting) when
             * an index partition join (or split) causes the partition bounds to
             * be greater than was originally anticipated.
             */

            final byte[] _fromKey = AbstractKeyRangeIndexProcedure
                    .constrainFromKey(currentFromKey, locator);

            final byte[] _toKey = AbstractKeyRangeIndexProcedure
                    .constrainToKey(currentToKey, locator);
            
            final int partitionId = locator.getPartitionId();
            
            if (INFO)
                log.info("name=" + ndx.getName() //
                        + ", tx=" + ts //
                        + ", reverseScan=" + reverseScan //
                        + ", partition=" + partitionId //
                        + ", fromKey=" + BytesUtil.toString(_fromKey) //
                        + ", toKey=" + BytesUtil.toString(_toKey));
            
            /*
             * The data service for the current index partition.
             * 
             * @todo this should failover.
             */
            final IDataService dataService = ndx.getDataService(locator);
            
            /*
             * Iterator will visit all data on that index partition.
             * 
             * Note: This merely initializes the variables on the iterator, but
             * it DOES NOT send the request to the data service. That does not
             * happen until you call [src.hasNext()].
             */
            
            src = new DataServiceTupleIterator<E>(ndx, dataService, DataService
                    .getIndexPartitionName(ndx.getName(), partitionId),
                    ts, _fromKey, _toKey, capacity, flags, filter) {
                
                /**
                 * Overriden so that we observe each distinct result set
                 * obtained from the DataService.
                 */
                protected ResultSet getResultSet(final long timestamp,
                        final byte[] fromKey, final byte[] toKey,
                        final int capacity, final int flags,
                        final IFilterConstructor filter) {

                    final ResultSet tmp = super.getResultSet(timestamp,
                            fromKey, toKey, capacity, flags, filter);

                    if (INFO)
                        log.info("Got chunk: ntuples=" + tmp.getNumTuples()
                                + ", exhausted=" + tmp.isExhausted()
                                + ", lastKey="
                                + BytesUtil.toString(tmp.getLastKey()));

                    if (reverseScan) {

                        /*
                         * We are moving backwards through the key order so we
                         * take the last key visited and use it to restrict our
                         * exclusive upper bound. Without this the iterator will
                         * not "advance".
                         */
                        
                        currentToKey = tmp.getLastKey();

                        if (INFO)
                            log.info("New exclusive upper bound: "
                                    + BytesUtil.toString(currentToKey));

                        // assert currentToKey != null;

                    } else {

                        /*
                         * We are moving forwards through the key order so we
                         * take the last key visited and use it to advanced our
                         * inclusive lower bound. Without this the iterator will
                         * not advance.
                         */

                        currentFromKey = tmp.getLastKey();

                        if (INFO)
                            log.info("New inclusive lower bound: "
                                    + BytesUtil.toString(currentFromKey));

//                        assert currentFromKey != null;
                        
                    }
                    
                    return tmp;
                    
                }
                
            };
            
            // increment the #of partitions visited.
            nparts++;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    public ITuple<E> next() {

        if (!hasNext()) {

            throw new NoSuchElementException();
            
        }
        
        nvisited++;

        final long nvisited = this.nvisited;
        
        final ITuple<E> sourceTuple = src.next();

        /*
         * Override the visitCount.
         */
        return new DelegateTuple<E>( sourceTuple ) {
            
            public long getVisitCount() {
                
                return nvisited;
                
            }
            
        };
        
    }

    /**
     * Batch delete behind semantics.
     * 
     * @see DataServiceTupleIterator#remove()
     */
    public void remove() {
        
        if (src == null)
            throw new IllegalStateException();
        
        src.remove();
        
    }

    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());

        sb.append("{ flags=" + Tuple.flagString(flags));

        sb.append(", timestamp=" + ts);

        sb.append(", isReadConsistentTx=" + isReadConsistentTx);

        sb.append(", capacity=" + capacity);

        sb.append(", fromKey="
                + (fromKey == null ? "n/a" : BytesUtil.toString(fromKey)));

        sb.append(", toKey="
                + (toKey == null ? "n/a" : BytesUtil.toString(toKey)));

        sb.append(", filter=" + filter);

        // dynamic state.

        sb.append(", #visited=" + nvisited);

        sb.append(", exhausted=" + exhausted);

        sb.append(", locator=" + locator);

        sb.append(", lastStaleLocator=" + lastStaleLocator);

        // Note: [src] is the per index partition source (dynamic state).
        sb.append(", src=" + (src == null ? "N/A" : src.getClass()));

        sb.append("}");

        return sb.toString();

    }

}
