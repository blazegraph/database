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

import com.bigdata.btree.AbstractKeyRangeIndexProcedure;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DelegateTuple;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ResultSet;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
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
     * The timestamp from the {@link ClientIndexView}.
     */
    private final long timestamp;

    /**
     * The first key to visit -or- null iff no lower bound.
     */
    private final byte[] fromKey;
    
    /**
     * The first key to NOT visit -or- null iff no upper bound.
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
    
    private final ITupleFilter filter;
    
    /**
     * Index of the first partition to be queried.
     */
    private final int fromIndex;
    
    /**
     * Index of the last partition to be queried.
     */
    private final int toIndex;

    /**
     * Index of the partition that is currently being queried.
     */
    private int index;

    /**
     * The #of partitions that have been queried so far. There will be one
     * {@link DataServiceRangeIterator} query issued per partition.
     */
    private int nparts = 0;
    
    /**
     * The metadata for the current index partition.
     */
    private PartitionLocator pmd = null;

    /**
     * The data service for the current index partition (this should
     * failover).
     */
    private IDataService dataService = null;

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
    
    public PartitionedRangeQueryIterator(ClientIndexView ndx, byte[] fromKey,
            byte[] toKey, int capacity, int flags, ITupleFilter filter) {

        if (ndx == null) {

            throw new IllegalArgumentException();
            
        }
        
        if (capacity < 0) {

            throw new IllegalArgumentException();
            
        }

        this.ndx = ndx;
        this.timestamp = ndx.getTimestamp();
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.capacity = capacity;
        this.flags = flags;
        this.filter = filter;

        final IMetadataIndex mdi = ndx.getMetadataIndex();

        {
            
            /*
             * FIXME The indices into the metadata index are not stable for
             * read-committed or unisolated queries. Either we have to cache the
             * actual partition locators up front, or we have to convert the
             * query for partition locators into a historical read as of a
             * recent commit time, or we have to re-compute the partition
             * locators each time before moving on to the next partition.
             */
            int a[] = mdi.findIndices(fromKey, toKey);
            
            fromIndex = a[0];
            
            toIndex = a[1];
            
        }
        
        // starting index is the lower bound.
        index = fromIndex;
        
    }

    /**
     * Issues a new range query against the current partition (the one
     * identified by {@link #index}. 
     */
    private void rangeQuery() {

        assert ! exhausted;

        pmd = ndx.getPartitionAtIndex(index);

        dataService = ndx.getDataService(pmd);
        
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

            final byte[] _fromKey = AbstractKeyRangeIndexProcedure.constrainFromKey(fromKey, pmd);

            final byte[] _toKey = AbstractKeyRangeIndexProcedure.constrainToKey(toKey, pmd);
            
//            final byte[] _fromKey = (index == fromIndex ? fromKey : pmd
//                    .getLeftSeparatorKey());
//
//            final byte[] _toKey = (index == toIndex ? toKey : pmd
//                    .getRightSeparatorKey());

            final int partitionId = pmd.getPartitionId();
            
            log.info("name=" + ndx.getName() + ", tx=" + timestamp + ", partition="
                    + partitionId + ", fromKey=" + BytesUtil.toString(_fromKey)
                    + ", toKey=" + BytesUtil.toString(_toKey));
            
            /*
             * Iterator will visit all data on that index partition.
             * 
             * Note: This merely initializes the variables on the iterator, but
             * it DOES NOT send the request to the data service. That does not
             * happen until you call [src.hasNext()].
             */
            
            src = new DataServiceRangeIterator(ndx, dataService, DataService
                    .getIndexPartitionName(ndx.getName(), partitionId),
                    ndx.getTimestamp(), _fromKey, _toKey, capacity, flags, filter);

            // increment the #of partitions visited.
            nparts++;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * Issues a new range query against the next index partititon.
     */
    private void nextPartition() {

        assert ! exhausted;
        assert src != null;
        
        // update the partition index.
        
        index++;

        // submit query to the next partition.
        
        rangeQuery();

    }
    
    /**
     * There are three levels at which we need to test in order to determine
     * if the total iterator is exhausted. First, we need to test to see if
     * there are more entries remaining in the current {@link ResultSet}.
     * If not and the {@link ResultSet} is NOT
     * {@link ResultSet#isExhausted() exhausted}, then we issue a
     * {@link #continuationQuery()} against the same index partition. If the
     * {@link ResultSet} is {@link ResultSet#isExhausted() exhausted}, then
     * we test to see whether or not we have visited all index partitions.
     * If so, then the iterator is exhausted. Otherwise we issue a range
     * query against the {@link #nextPartition()}.
     * 
     * @return True iff the iterator is not exhausted.
     */
    public boolean hasNext() {

        if (nparts == 0) {

            // Obtain the first result set.

            rangeQuery();

        }
        
        assert src != null;
        
        if (exhausted) {

            return false;
            
        }

        try {

            if(src.hasNext()) {
            
                // More from the current source iterator.
                
                return true;
                
            }
            
        } catch(RuntimeException ex) {
            
            if(InnerCause.isInnerCause(ex, NoSuchIndexException.class)) {
                
                /*
                 * FIXME Handle NoSuchIndexException.
                 * 
                 * 1. advance the fromKey to the next key that we are supposed
                 * to visit
                 * 
                 * 1. refresh locator(s) spanning the key range for the index
                 * partition for which we got this exception
                 */
                throw new UnsupportedOperationException(ex);
                
            }
            
        }
        
        /*
         * The source iterator is exhausted so we will issue a continuation
         * query. Each source iterator reads from one index partition. (The
         * source iterator is itself a chunked iterator so it may issue multiple
         * remote requests to consume the data available on a given index
         * partition).
         */
        
        if(index < toIndex) {
        
            /*
             * The current index partition is empty, but there are other
             * index partitions left to query.
             */
            
            nextPartition();
            
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

    /**
     * The byte[] containing the serialized value for the next matching
     * entry in the index -or- <code>null</code> iff the iterator was
     * provisioned to NOT visit values.
     */
    public ITuple next() {

        if (!hasNext()) {

            throw new NoSuchElementException();
            
        }
        
        nvisited++;

        final long nvisited = this.nvisited;
        
        return new DelegateTuple( src.next() ) {
            
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
