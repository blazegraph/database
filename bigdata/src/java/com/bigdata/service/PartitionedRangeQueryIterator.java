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

import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DelegateTuple;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.AbstractKeyRangeIndexProcedure;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.util.InnerCause;

/**
 * Class supports range query across one or more index partitions.
 * <p>
 * Each partition is mapped onto a single {@link DataServiceRangeIterator}
 * query. In turn, the {@link DataServiceRangeIterator} may make several queries
 * to the data service per partition. The actual #of queries made to the data
 * service depends on the #of index entries that are visited per partition and
 * the capacity specified to the ctor.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionedRangeQueryIterator implements ITupleIterator {

    protected static final transient Logger log = Logger
            .getLogger(PartitionedRangeQueryIterator.class);
    
    /**
     * The index on which the range query is being performed.
     */
    private final ClientIndexView ndx;
    
    /**
     * Iterator traversing the index partition locators spanned by the query.
     */
    private ITupleIterator<PartitionLocator> locatorItr;
    
    /**
     * The timestamp from the ctor.
     */
    private final long timestamp;

//    /**
//     * From the {@link ClientIndexView}.
//     */
//    private final boolean readConsistent;
    
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
    
//    /**
//     * The last key that was visited on the {@link #src} iterator. This is used
//     * in case we trap a {@link StaleLocatorxception}. It provides the
//     * exclusive lower bound for the query against the new index partitions
//     * identified when we refresh our {@link PartitionLocator}s. This is
//     * <code>null</code> initially and is set to the non-<code>null</code>
//     * value of each key that we visit by {@link #next()}.
//     * 
//     * This adds a requirement that the {@link IRangeQuery#KEYS} are
//     * always requested so that we can avoid double-counting. However,
//     * {@link StaleLocatorException}s can only arise when we request the next
//     * result set so we SHOULD be able to get by without the KEYS at this layer
//     * (which means that we do not have to send them across the network) by
//     * using {@link ResultSet#getLastKey()}.  Change this since we otherwise
//     * will always send KEYS which can add significant NIO costs.
//     */
//    private byte[] lastKeyVisited = null;

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
     *       forward and reverse traversal.
     *       <p>
     *       Try writing a test where the partition is non-empty, but a filter
     *       causes at least one chunk in that partition to report an empty
     *       result set. This might lead to either non-progression of the
     *       iterator, early termination, or a thrown exception.
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
     * {@link DataServiceRangeIterator} query issued per partition.
     * 
     * @deprecated The #of partitions is a bit tricky since splits and introduce
     *             new partitions.
     */
    private int nparts = 0;
    
    /**
     * The #of enties visited so far.
     */
    private long nvisited = 0;
    
    /**
     * The {@link DataServiceRangeIterator} reading from the current index
     * partition.
     */
    private DataServiceRangeIterator src;
   
    /**
     * When true, the entire key range specified by the client has been
     * visited and the iterator is exhausted (i.e., all done).
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
     * Note: The {@link PartitionedRangeQueryIterator} uses a sequential scan
     * (rather than mapping across the index partitions in parallel) and always
     * picks up from the successor of the last key visited. Read-consistent is
     * achieved by specifying a commitTime for the <i>timestamp</i> rather than
     * {@link ITx#READ_COMMITTED}.  The latter will use dirty reads (each time
     * a {@link ResultSet} is fetched it will be fetched from the most recently
     * committed state of the database).
     * 
     * @param ndx
     * @param timestamp
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
    public PartitionedRangeQueryIterator(ClientIndexView ndx, long timestamp,
            byte[] fromKey, byte[] toKey, int capacity, int flags,
            IFilterConstructor filter) {

        if (ndx == null) {

            throw new IllegalArgumentException();
            
        }
        
        if (capacity < 0) {

            throw new IllegalArgumentException();
            
        }

//        if (ndx.getTimestamp() == ITx.UNISOLATED && readConsistent) {
//            
//            throw new IllegalArgumentException(
//                    "Read-consistent not available for unisolated operations");
//            
//        }
        
        this.ndx = ndx;
        this.timestamp = timestamp;
//        this.readConsistent = readConsistent;
        this.fromKey = this.currentFromKey = fromKey;
        this.toKey = this.currentToKey = toKey;
        this.capacity = capacity;
        
//        // Note: need keys for REMOVEALL.
//        this.flags = ((flags & IRangeQuery.REMOVEALL)==0) ? flags : flags|IRangeQuery.KEYS;

        /*
         * we used to need the KEYS in order to keep [lastKeyVisited] up to date.
         */
//        this.flags = IRangeQuery.KEYS | flags;
        this.flags = flags;
                
        this.filter = filter;

        this.reverseScan = (flags & IRangeQuery.REVERSE) != 0;

        // start locator scan
        this.locatorItr = ndx.locatorScan(reverseScan, timestamp, fromKey,
                toKey);

    }

    public boolean hasNext() {

        if (Thread.interrupted())
            throw new RuntimeException(new InterruptedException());

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
            
            if(InnerCause.isInnerCause(ex, StaleLocatorException.class)) {
                
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
                
                if(Thread.interrupted()) throw new RuntimeException(new InterruptedException());

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
                         * locator or the data service. For example, the index
                         * partition might have been dropped on the data service
                         * (a no-no to be sure).
                         */

                        throw new RuntimeException(
                                "Missing index partition on data service? "
                                        + locator, ex);

                    }
                    
                }
                
                // save reference
                lastStaleLocator = locator;
                
                // clear since invalid.
                locator = null;
                
                // Re-start the locator scan.
                locatorItr = ndx.locatorScan(reverseScan, timestamp,
                        currentFromKey, currentToKey);
                
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
        
        exhausted = true;
        
        return false;
        
    }

//    /**
//     * The next [fromKey] that we are supposed to visit. If we have not visited
//     * anything, then we are still on the original {@link #fromKey}. Otherwise
//     * we take the successor of the {@link #lastKeyVisited}.
//     */
//    private byte[] currentFromKey() {
//
//        assert src != null;
//        
//        final byte[] currentFromKey = lastKeyVisited == null //
//            ? this.fromKey//
//            : BytesUtil.successor(lastKeyVisited)//
//            ;
//
//        return currentFromKey;
//        
//    }
    
    /**
     * Issues a new range query against the next index partititon.
     */
    private boolean nextPartition() {

        assert ! exhausted;
        
        if (!locatorItr.hasNext()) {

            log.info("No more locators");

            return false;
            
        }
       
        locator = locatorItr.next().getObject();
        
        if (log.isInfoEnabled())
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
            
            if (log.isInfoEnabled())
                log.info("name=" + ndx.getName() //
                        + ", tx=" + timestamp //
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
            
            src = new DataServiceRangeIterator(ndx, dataService, DataService
                    .getIndexPartitionName(ndx.getName(), partitionId),
                    timestamp, _fromKey, _toKey, capacity, flags, filter) {
                
                /**
                 * Overriden so that we observe each distinct result set
                 * obtained from the DataService.
                 */
                protected ResultSet getResultSet(long timestamp,
                        byte[] fromKey, byte[] toKey, int capacity, int flags,
                        IFilterConstructor filter) {

                    final ResultSet tmp = super.getResultSet(timestamp,
                            fromKey, toKey, capacity, flags, filter);

                    if (reverseScan) {

                        /*
                         * We are moving backwards through the key order so we
                         * take the last key visited and use it to restrict our
                         * exclusive upper bound. Without this the iterator will
                         * not "advance".
                         */
                        
                        currentToKey = tmp.getLastKey();

                        if (log.isInfoEnabled())
                            log.info("New exclusive upper bound: "
                                    + currentToKey);

//                        assert currentToKey != null;

                    } else {

                        /*
                         * We are moving forwards through the key order so we
                         * take the last key visited and use it to advanced our
                         * inclusive lower bound. Without this the iterator will
                         * not advance.
                         */

                        currentFromKey = tmp.getLastKey();

                        if (log.isInfoEnabled())
                            log.info("New inclusive lower bound: "
                                    + currentFromKey);

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
    
    public ITuple next() {

        if (!hasNext()) {

            throw new NoSuchElementException();
            
        }
        
        nvisited++;

        final long nvisited = this.nvisited;
        
        final ITuple sourceTuple = src.next();

//        this.lastKeyVisited = sourceTuple.getKey();
//
//        assert lastKeyVisited != null;
        
        return new DelegateTuple( sourceTuple ) {
            
            public long getVisitCount() {
                
                return nvisited;
                
            }
            
        };
        
    }

    /**
     * Batch delete behind semantics.
     * 
     * @see DataServiceRangeIterator#remove()
     */
    public void remove() {
        
        if (src == null)
            throw new IllegalStateException();
        
        src.remove();
        
    }
    
}
