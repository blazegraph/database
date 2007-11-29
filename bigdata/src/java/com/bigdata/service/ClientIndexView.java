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
/*
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.MetadataIndex;
import com.bigdata.scaleup.PartitionMetadata;
import com.bigdata.scaleup.PartitionedIndexView;

/**
 * A client-side view of an index.
 * <p>
 *
 * @todo change the {@link IDataService} API so that we can provide custome
 *       serialization for the various methods. For example, we are relying on
 *       default marshalling of the arguments for the batch operations. This is
 *       especially bad since the arguments are things like byte[][] and not
 *       IKeyBuffer.
 *
 * @todo We should be able to transparently use either a hash mod N approach to
 *       distributed index partitions or a dynamic approach based on overflow.
 *       This could even be decided on a per-index basis. The different
 *       approaches would be hidden by appropriate implementations of this
 *       class.
 * 
 * @todo the client does not attempt to obtain a new data service proxy for a
 *       partition if the current proxy fails (no failover).
 * 
 * @todo the client does not notice deleted index partitions (which can arise
 *       from index partition joins). this case needs to be handled in any code
 *       that visits partitions using the entryIndex in the metadata index since
 *       some entries may be "deleted".
 * 
 * @todo It is a design goal (not yet obtained) that the client should interact
 *       with an interface rather than directly with {@link MetadataIndex} so
 *       that this code can look identical regardless of whether the metadata
 *       index is local (embedded) or remote. (We do in fact use the same code
 *       for both scenarios, but only because, at this time, the metadata index
 *       is fully cached in the remote case).
 * 
 * @todo note that it is possible (though unlikely) for an index partition split
 *       or join to occur during operations. Figure out how I want to handle
 *       that, and how I want to handle that with transactional isolation
 *       (presumably a read-only historical view of the metadata index would be
 *       used - in which case we need to pass the tx into the getPartition()
 *       method).
 * 
 * @todo cache leased information about index partitions of interest to the
 *       client. The cache will be a little tricky since we need to know when
 *       the client does not possess a partition definition. Index partitions
 *       are defined by the separator key - the first key that lies beyond that
 *       partition. the danger then is that a client will presume that any key
 *       before the first leased partition is part of that first partition. To
 *       guard against that the client needs to know both the separator key that
 *       represents the upper and lower bounds of each partition. If a lookup in
 *       the cache falls outside of any known partitions upper and lower bounds
 *       then it is a cache miss and we have to ask the metadata service for a
 *       lease on the partition. the cache itself is just a btree data structure
 *       with the proviso that some cache entries represent missing partition
 *       definitions (aka the lower bounds for known partitions where the left
 *       sibling partition is not known to the client).
 * 
 * @todo support partitioned indices by resolving client operations against each
 *       index partition as necessary and maintaining leases with the metadata
 *       service - the necessary logic is in the {@link PartitionedIndexView}
 *       and can be refactored for this purpose. Some operations can be made
 *       parallel (rangeCount), and we could offer parallelization with
 *       post-sort for range query.
 * 
 * @todo offer alternatives for the batch insert and remove methods that do not
 *       return the old values to the client so that the client may opt to
 *       minimize network traffic for data that it does not need.
 * 
 * @todo develop and offer policies for handling index partitions that are
 *       unavailable at the time of the request (continued operation during
 *       partial failure).
 * 
 * @todo rangeCount and the batch CRUD API (contains, insert, lookup, remove)
 *       could all execute operations in parallel against the index partitions
 *       that are relevant to their data. Explore parallelism solutions in which
 *       the client uses a worker thread pool that throttles the #of concurrent
 *       operations that it is allowed to execute, but make sure that it is not
 *       possible for the client to deadlock owing to dependencies required by
 *       the execution of those operations by the worker threads (I don't think
 *       that a deadlock is possible).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientIndexView implements IIndex {

    public static final transient Logger log = Logger
            .getLogger(ClientIndexView.class);
    
    private final IBigdataFederation fed;
    private final long tx;
    private final String name;
    
    /**
     * The name of the scale-out index.
     */
    public String getName() {
        
        return name;
        
    }

    /**
     * The unique index identifier.
     */
    private UUID indexUUID;

    /**
     * Obtain the proxy for a metadata service. if this instance fails, then we
     * can always ask for a new instance for the same federation (failover).
     */
    final protected IMetadataService getMetadataService() {
        
        return fed.getMetadataService();
        
    }
    
    /**
     * @todo define an interface and use a read-only view of the metadata index.
     *       this approach is forward looking to when the metadata index is only
     *       partly materialized on the client.
     */
    final protected MetadataIndex getMetadataIndex() {
        
        return fed.getMetadataIndex(name);
        
    }
    
    /**
     * Create a view on a scale-out index.
     * 
     * @param fed
     *            The federation containing the index.
     * @param tx
     *            The transaction identifier or zero(0L) iff the index view is
     *            not isolated by a transaction.
     * @param name
     *            The index name.
     */
    public ClientIndexView(IBigdataFederation fed, long tx, String name) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();
    
        if(tx != IBigdataFederation.UNISOLATED) {
            
            throw new UnsupportedOperationException(
                    "Only unisolated views are supported at this time");
            
        }
        
        this.fed = fed;

        this.tx = tx;
        
        this.name = name;
        
    }

    public UUID getIndexUUID() {

        if(indexUUID == null) {
        
            /*
             * obtain the UUID for the managed scale-out index.
             */
            
            try {

                this.indexUUID = getMetadataService().getManagedIndexUUID(name);
                
                if(indexUUID == null) {
                    
                    throw new NoSuchIndexException(name);
                    
                }
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }


        }
        
        return indexUUID;
        
    }

    public boolean contains(byte[] key) {
        
        IPartitionMetadata pmd = getPartition(tx,name, key);

        IDataService dataService = getDataService(pmd);

        final boolean[] ret;
        
        try {

            ret = dataService.batchContains(tx, name, pmd.getPartitionId(), 1,
                    new byte[][] { key });
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return ret[0];
        
    }

    public Object insert(Object key, Object value) {

        IPartitionMetadata pmd = getPartition(tx,name, (byte[])key);
        
        IDataService dataService = getDataService(pmd);

        final boolean returnOldValues = true;
        
        final byte[][] ret;
        
        try {
            
            ret = dataService.batchInsert(tx, name, pmd.getPartitionId(), 1,
                    new byte[][] { (byte[]) key },
                    new byte[][] { (byte[]) value }, returnOldValues);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return ret[0];

    }

    public Object lookup(Object key) {

        IPartitionMetadata pmd = getPartition(tx, name, (byte[]) key);

        IDataService dataService = getDataService(pmd);
        
        final byte[][] ret;
        
        try {
            
            ret = dataService.batchLookup(tx, name, pmd.getPartitionId(), 1,
                    new byte[][] { (byte[]) key });

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return ret[0];

    }

    public Object remove(Object key) {

        IPartitionMetadata pmd = getPartition(tx,name, (byte[])key);

        IDataService dataService = getDataService(pmd);
        
        final byte[][] ret;
        
        final boolean returnOldValues = true;
        
        try {
            
            ret = dataService.batchRemove(tx, name, pmd.getPartitionId(), 1,
                    new byte[][] { (byte[]) key }, returnOldValues );

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return ret[0];

    }

    /*
     * All of these methods need to divide up the operation across index
     * partitions.
     */

    /**
     * Returns the sum of the range count for each index partition spanned by
     * the key range.
     */
    public int rangeCount(byte[] fromKey, byte[] toKey) {

//        IMetadataService metadataService = getMetadataService();
        MetadataIndex mdi = getMetadataIndex();

        final int fromIndex;
        final int toIndex;
//        try {

            // index of the first partition to check.
            fromIndex = (fromKey == null ? 0 : mdi.findIndexOf(fromKey));

            // index of the last partition to check.
            toIndex = (toKey == null ? mdi.getEntryCount()-1 : mdi.findIndexOf(toKey));
            
//        } catch (IOException ex) {
//            
//            throw new RuntimeException(ex);
//            
//        }

        // per javadoc, keys out of order returns zero(0).
        if (toIndex < fromIndex)
            return 0;

        // use to counters so that we can look for overflow.
        int count = 0;
        int lastCount = 0;

        for (int index = fromIndex; index <= toIndex; index++) {

            PartitionMetadataWithSeparatorKeys pmd = getPartitionAtIndex(
                    name, index);

            // // The first key that would enter the nth partition.
            // byte[] separatorKey = mdi.keyAt(index);

            IDataService dataService = getDataService(pmd);

            try {

                /*
                 * Add in the count from that partition.
                 * 
                 * Note: The range count request is formed such that it
                 * addresses only those keys that actually lies within the
                 * partition. This has two benefits:
                 * 
                 * (1) The data service can check the range and notify clients
                 * that appear to be requesting data for index partitions that
                 * have been relocated.
                 * 
                 * (2) In order to avoid double-counting when multiple
                 * partitions for the same index are mapped onto the same data
                 * service we MUST query at most the key range for a specific
                 * partition (or we must provide the data service with the index
                 * partition identifier and it must restrict the range on our
                 * behalf).
                 */

                /*
                 * For the first (last) partition use the caller's fromKey
                 * (toKey) so that we do not count everything from the start
                 * of (up to the close) of the partition unless the caller
                 * specified fromKey := null (toKey := null).
                 */
                    
                final byte[] _fromKey = (index == fromIndex ? fromKey : pmd
                        .getLeftSeparatorKey());

                final byte[] _toKey = (index == toIndex ? toKey : pmd
                        .getRightSeparatorKey());
                
                final int partitionId = pmd.getPartitionId();
                
                count += dataService.rangeCount(tx, name, partitionId,
                        _fromKey, _toKey);
                
            } catch(Exception ex) {

                throw new RuntimeException(ex);
                
            }

            if(count<lastCount) {
            
                // more than would fit in an Integer.
                return Integer.MAX_VALUE;
                
            }
            
            lastCount = count;
            
        }

        return count;
        
    }

    /**
     * An {@link IEntryIterator} that kinds the use of a series of
     * {@link ResultSet}s to cover all index partitions spanned by the key
     * range.
     * 
     * @todo provide variant method so that the client can pass in a key and/or
     *       value filter.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        
        // @todo make this a configuration parameter for the client and/or
        // index.
        final int capacity = 1000;

        return new PartitionedRangeQueryIterator(this, tx, fromKey, toKey, capacity,
                IDataService.KEYS | IDataService.VALS);

    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags) {

        return new PartitionedRangeQueryIterator(this, tx, fromKey, toKey, capacity,
                flags);
        
    }

    /**
     * Class supports range query across one or more index partitions.
     * 
     * @todo support an optional key and/or value filter that is executed on the
     *       data service.
     * 
     * @todo if unisolated or read-committed, then we may need to re-assess the
     *       toIndex during the query.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class PartitionedRangeQueryIterator implements IEntryIterator {

        /**
         * Error message used by {@link #getKey()} when the iterator was not
         * provisioned to request keys from the data service.
         */
        static public transient final String ERR_NO_KEYS = "Keys not requested";
        
        /**
         * Error message used by {@link #getValue()} when the iterator was not
         * provisioned to request values from the data service.
         */
        static public transient final String ERR_NO_VALS = "Values not requested";

        /**
         * The index on which the range query is being performed.
         */
        private final ClientIndexView ndx;
        
        /**
         * The transaction identifier -or- zero iff the request is unisolated.
         */
        private final long tx;

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
         * The #of range query operations executed.
         */
        private int nqueries;
        
        /**
         * The #of partitions visited.
         */
        private int nparts;
        
        /**
         * The metadata for the current index partition.
         */
        PartitionMetadataWithSeparatorKeys pmd = null;

        /**
         * The data service for the current index partition (this should
         * failover).
         */
        IDataService dataService = null;

        /**
         * The current result set. For each index partition spanned by the
         * overall key range supplied by the client, we will issue at least one
         * range query against the data service for that index partition. Once
         * all entries in a result set have been consumed by the client, we test
         * the result set to see whether or not it exhausted the entries that
         * could be matched for that index partition. If not, then we will issue
         * another "continuation" range query against the same index position
         * starting from the successor of the last key scanned. If it is
         * exhausted, then we will issue a new range query against the next
         * index partition. If no more index partitions remain that are spanned
         * by the key range specified by the client then we are done.
         * <p>
         * Note: A result set will be empty if there are no entries (including
         * after filtering) that lie within the key range in a given index
         * partition. It is possible for any of the result sets to be empty.
         * Consider a case of static partitioning of an index into N partitions.
         * When the index is empty, a range query of the entire index will still
         * query each of the N partitions. However, since the index is empty
         * none of the partitions will have any matching entries and all result
         * sets will be empty.
         */
        private ResultSet rset = null;

        /**
         * The #of enties visited so far.
         */
        private long nvisited = 0;
        
        /**
         * The index of the last entry visited in the current {@link ResultSet}.
         * This is reset to <code>-1</code> each time we obtain a new
         * {@link ResultSet}.
         */
        private int lastVisited = -1;
        
        /**
         * The last key visited by the iterator.
         */
        private byte[] lastKey = null;

        /**
         * The last value visited by the iterator.
         */
        private byte[] lastVal = null;
        
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
         * The #of queries issued so far.
         */
        public int getQueryCount() {

            return nqueries;
            
        }
        
        /**
         * The #of entries visited so far (not the #of entries scanned, which
         * can be much greater if a filter is in use).
         */
        public long getVisitedCount() {
            
            return nvisited;
            
        }
        
        public PartitionedRangeQueryIterator(ClientIndexView ndx, long tx,
                byte[] fromKey, byte[] toKey, int capacity, int flags) {

            if (ndx == null) {

                throw new IllegalArgumentException();
                
            }
            
            if (capacity <= 0) {

                throw new IllegalArgumentException("capacity must be positive.");
                
            }

            this.ndx = ndx;
            this.tx = tx;
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.capacity = capacity;
            this.flags = flags;

//          IMetadataService metadataService = getMetadataService();
            MetadataIndex mdi = ndx.getMetadataIndex();

            final int fromIndex;
            final int toIndex;
//            try {

                // index of the first partition to check.
                fromIndex = (fromKey == null ? 0 : mdi.findIndexOf(fromKey));

                // index of the last partition to check.
                toIndex = (toKey == null ? mdi.getEntryCount()-1 : mdi.findIndexOf(toKey));
                
//            } catch (IOException ex) {
//                
//                throw new RuntimeException(ex);
//                
//            }

            // keys are out of order.
            if (fromIndex > toIndex ) {
            
                throw new IllegalArgumentException("fromKey > toKey");
                
            }

            this.fromIndex = fromIndex;
            this.toIndex = toIndex;

            // starting index is the lower bound.
            index = fromIndex;

            // Obtain the first result set.
            rangeQuery();
            
            nparts = 1;
            
        }

        /**
         * Issues a new range query against the current partition (the one
         * identified by {@link #index}. 
         */
        private void rangeQuery() {

            assert ! exhausted;

            pmd = ndx.getPartitionAtIndex(ndx.name, index);

            dataService = ndx.getDataService(pmd);
            
            try {

                /*
                 * Note: The range query request is formed such that it
                 * addresses only those keys that actually lie within the
                 * partition. This has two benefits:
                 * 
                 * (1) The data service can check the range and notify clients
                 * that appear to be requesting data for index partitions that
                 * have been relocated.
                 * 
                 * (2) In order to avoid double-counting when multiple
                 * partitions for the same index are mapped onto the same data
                 * service we MUST query at most the key range for a specific
                 * partition (or we must provide the data service with the index
                 * partition identifier and it must restrict the range on our
                 * behalf).
                 */

                /*
                 * For the first (last) partition use the caller's fromKey
                 * (toKey) so that we do not count everything from the start of
                 * (up to the close) of the partition unless the caller
                 * specified fromKey := null (toKey := null).
                 */

                final byte[] _fromKey = (index == fromIndex ? fromKey : pmd
                        .getLeftSeparatorKey());

                final byte[] _toKey = (index == toIndex ? toKey : pmd
                        .getRightSeparatorKey());

                final int partitionId = pmd.getPartitionId();
                
                log.info("name=" + ndx.name + ", partition=" + partitionId
                        + ", fromKey=" + BytesUtil.toString(_fromKey)
                        + ", toKey=" + BytesUtil.toString(_toKey));
                
                rset = dataService.rangeQuery(tx, ndx.name, partitionId,
                        _fromKey, _toKey, capacity, flags);
                
                // reset index into the ResultSet.
                lastVisited = -1;
                
                nqueries++;

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }
        
        /**
         * Issues a "continuation" query within the same index partition. This
         * is invoked iff the there are no entries left to visit in the current
         * {@link ResultSet} but {@link ResultSet#isExhausted()} is [false],
         * indicating that there is more data in that index partition.
         */
        private void continuationQuery() {
            
            assert ! exhausted;
            assert rset != null;
            assert ! rset.isExhausted();
            assert pmd != null;
            assert dataService != null;
            
            try {

                /*
                 * Note: The range query request is formed such that it
                 * addresses only those keys that actually lie within the
                 * partition. This has two benefits:
                 * 
                 * (1) The data service can check the range and notify clients
                 * that appear to be requesting data for index partitions that
                 * have been relocated.
                 * 
                 * (2) In order to avoid double-counting when multiple
                 * partitions for the same index are mapped onto the same data
                 * service we MUST query at most the key range for a specific
                 * partition (or we must provide the data service with the index
                 * partition identifier and it must restrict the range on our
                 * behalf).
                 */

                /*
                 * Start from the successor of the last key scanned by the
                 * previous result set.
                 */

                final byte[] _fromKey = rset.successor();

                /*
                 * The toKey is choosen in the same manner for each query issued
                 * against a given index partition.
                 */
                final byte[] _toKey = (index == toIndex ? toKey : pmd
                        .getRightSeparatorKey());

                final int partitionId = pmd.getPartitionId();
                
                log.info("name=" + ndx.name + ", partition=" + partitionId
                        + ", fromKey=" + BytesUtil.toString(_fromKey)
                        + ", toKey=" + BytesUtil.toString(_toKey));
                
                rset = dataService.rangeQuery(tx, ndx.name, partitionId,
                        _fromKey, _toKey, capacity, flags);
                
                // reset index into the ResultSet.
                lastVisited = -1;

                nqueries++;
                
            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }
        
        /**
         * Issues a new range query against the next index partititon.
         */
        private void nextPartition() {

            assert ! exhausted;
            assert rset != null;
            
            index++;

            rangeQuery();
            
            nparts++;

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
            
            if(!rset.isExhausted()) {
                /*
                 * This result set is empty but there is more data in the
                 * current partition.
                 */
                continuationQuery();
                /*
                 * Recursive query since the result set might be empty.
                 * 
                 * Note: The result set could be empty if we are: (a)
                 * unisolated; or (b) in a read committed transaction; or (c) if
                 * a filter is being applied.
                 */
                return hasNext();
            }
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
        public Object next() {

            if (!hasNext()) {

                throw new NoSuchElementException();
                
            }
            
            nvisited++;

            lastVisited++;
            
            lastKey = ((flags & IDataService.KEYS) == 0) ? null : rset
                    .getKeys()[lastVisited];

            lastVal = ((flags & IDataService.VALS) == 0) ? null : rset
                    .getValues()[lastVisited];
            
            return lastVal;
            
        }

        /**
         * The byte[] containing the key for the last matching entry in the
         * index.
         * 
         * @exception IllegalStateException
         *                if nothing has been visited.
         * 
         * @exception UnsupportedOperationException
         *                if the iterator was not provisioned to visit keys.
         */
        public byte[] getKey() {
            
            if(nvisited==0L) {
                
                // Nothing visited yet.
                throw new IllegalStateException();
                
            }
            
            if((flags & IDataService.KEYS)==0) {

                // Keys not requested.
                throw new UnsupportedOperationException(ERR_NO_KEYS);
                
            }
            
            return lastKey;
            
        }

        public Object getValue() {

            if(nvisited==0L) {
               
                // Nothing visited yet.
                throw new IllegalStateException();
                
            }
            
            if((flags & IDataService.VALS)==0) {
                
                // Values not requested.
                throw new UnsupportedOperationException(ERR_NO_VALS);
                
            }

            return lastVal;
            
        }

        /**
         * This operation is not supported.
         * 
         * @exception UnsupportedOperationException
         *                Always.
         */
        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }

    }

    /*
     * All of these methods need to divide up the operation across index
     * partitions. Like rangeCount, but unlike rangeQuery, these operations can
     * be parallelized. Unlike either rangeCount or rangeQuery, these operations
     * are likely to access only a "few" partitions in the most common cases.
     * The reason is that the client needs to buffer all data for the operation
     * in memory, so there is an inherent scale limit on the #of partitions that
     * will be touched. A worst case scenario would be a client using an index
     * whose keys were randomly choosen (e.g., UUIDs). In such a case the
     * operations may be expected to be distributed uniformly across the index
     * partitions.
     * 
     * @todo expose option to not return the old values for insert and remove()
     * in order to allow the client to minimize the network traffic.
     * 
     * @todo provide retry / ignore options for failures on individual index
     * partitions.
     * 
     * @todo all of the batch api operations can be parallelized.
     * 
     * @todo decide if the batch API will _require_ the keys to be presented in
     * sorted order - I think that this requirement makes good sense.
     */

    public void contains(BatchContains op) {

        if (op == null)
            throw new IllegalArgumentException();
        
        final List<Split> splits = splitKeys(op.ntuples, op.keys);
        
        final Iterator<Split> itr = splits.iterator();
        
        while(itr.hasNext()) {
            
            Split split = itr.next();
            
            IDataService dataService = getDataService(split.pmd);
            
            byte[][] _keys = new byte[split.ntuples][];
            boolean[] _vals;
            
            System.arraycopy(op.keys, split.fromIndex, _keys, 0, split.ntuples);
            
            try {

                _vals = dataService.batchContains(tx, name, split.pmd
                        .getPartitionId(), split.ntuples, _keys);
                
            } catch (Exception ex) {
                
                throw new RuntimeException(ex);
                
            }

            System.arraycopy(_vals, 0, op.contains, split.fromIndex,
                    split.ntuples);
            
        }
        
    }


    public void lookup(BatchLookup op) {

        if (op == null)
            throw new IllegalArgumentException();
        
        final List<Split> splits = splitKeys(op.ntuples, op.keys);
        
        final Iterator<Split> itr = splits.iterator();
        
        while(itr.hasNext()) {
            
            Split split = itr.next();
            
            IDataService dataService = getDataService(split.pmd);
            
            byte[][] _keys = new byte[split.ntuples][];
            byte[][] _vals;
            
            System.arraycopy(op.keys, split.fromIndex, _keys, 0, split.ntuples);
            
            try {

                _vals = dataService.batchLookup(tx, name, split.pmd
                        .getPartitionId(), split.ntuples, _keys);
                
            } catch (Exception ex) {
                
                throw new RuntimeException(ex);
                
            }

            System.arraycopy(_vals, 0, op.values, split.fromIndex,
                    split.ntuples);
            
        }
        
    }

    public void insert(BatchInsert op) {

        if (op == null)
            throw new IllegalArgumentException();
        
        final boolean returnOldValues = true;
        
        final List<Split> splits = splitKeys(op.ntuples, op.keys);
        
        final Iterator<Split> itr = splits.iterator();
        
        while(itr.hasNext()) {
            
            Split split = itr.next();
            
            IDataService dataService = getDataService(split.pmd);
            
            byte[][] _keys = new byte[split.ntuples][];
            byte[][] _vals = new byte[split.ntuples][];
            
            System.arraycopy(op.keys, split.fromIndex, _keys, 0, split.ntuples);
            System.arraycopy(op.values, split.fromIndex, _vals, 0, split.ntuples);
            
            byte[][] oldVals;
            
            try {

                oldVals = dataService.batchInsert(tx, name, split.pmd
                        .getPartitionId(), split.ntuples, _keys, _vals,
                        returnOldValues);
                
            } catch (Exception ex) {
                
                throw new RuntimeException(ex);
                
            }

            if(returnOldValues) {
                
                System.arraycopy(oldVals, 0, op.values, split.fromIndex,
                        split.ntuples);
                
            }
            
        }
        
    }

    public void remove(BatchRemove op) {

        if (op == null)
            throw new IllegalArgumentException();
        
        final boolean returnOldValues = true;
        
        final List<Split> splits = splitKeys(op.ntuples, op.keys);
        
        final Iterator<Split> itr = splits.iterator();
        
        while(itr.hasNext()) {
            
            Split split = itr.next();
            
            IDataService dataService = getDataService(split.pmd);
            
            byte[][] _keys = new byte[split.ntuples][];
            
            System.arraycopy(op.keys, split.fromIndex, _keys, 0, split.ntuples);
            
            byte[][] oldVals;
            
            try {

                oldVals = dataService.batchRemove(tx, name, split.pmd
                        .getPartitionId(), split.ntuples, _keys, 
                        returnOldValues);
                
            } catch (Exception ex) {
                
                throw new RuntimeException(ex);
                
            }

            if(returnOldValues) {
                
                System.arraycopy(oldVals, 0, op.values, split.fromIndex,
                        split.ntuples);
                
            }
            
        }

    }
    
    /**
     * FIXME Develop this as a helper method that accepts {@link Split}s paired
     * to {@link IProcedure}s. The helper should execute the operations in
     * parallel and should provide a means of combining the results of each
     * split operation into an overall operation. Perhaps the splitting of the
     * main operation into component operations can also be abstracted?  Failure
     * on individual partitions could be retried or not depending on the client
     * configuration or index view options.
     * 
     * @param op
     * @return
     */
    public Object submit(IPartitionMetadata pmd,IProcedure op) {
        
//        if (op == null)
//            throw new IllegalArgumentException();
//        
//        final boolean returnOldValues = true;
//        
//        final List<Split> splits = splitKeys(op.ntuples, op.keys);
//        
//        final Iterator<Split> itr = splits.iterator();
//        
//        while(itr.hasNext()) {
//            
//            Split split = itr.next();
//            
//            IDataService dataService = fed.getDataService(split.pmd);
//            
//            byte[][] _keys = new byte[split.ntuples][];
//
////         execute, gather results.
//        
//    }

        IDataService dataService = getDataService(pmd);
        
        try {

            Object ret = dataService.submit(tx, name, pmd.getPartitionId(), op);
        
            return ret;
            
        } catch(Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    /**
     * Utility method to split a set of ordered keys into partitions based the
     * index partitions defined for a scale-out index.
     * <p>
     * Find the partition for the first key. Check the last key, if it is in the
     * same partition then then this is the simplest case and we can just send
     * the data along, perhaps breaking it down into smaller batches (note that
     * batch break points MUST respect the "row" identity for a sparse row
     * store).
     * <p>
     * Otherwise, perform a binary search on the remaining keys looking for the
     * index of the first key GTE the right separator key for that partition.
     * The batch for this partition is formed from all keys from the first key
     * for that partition up to but excluding the index position identified by
     * the binary search (if there is a match; if there is a miss, then the
     * binary search result needs to be converted into a key index and that will
     * be the last key for the current partition).
     * <p>
     * Examine the next key and repeat the process until all keys have been
     * allocated to index partitions.
     * <p>
     * Form requests based on the identified first/last key and partition
     * identified by this process.
     * 
     * @param ntuples
     *            The #of keys.
     * @param keys
     *            An array of keys. Each key is an interpreted as an unsigned
     *            byte[]. All keys must be non-null. The keys must be in sorted
     *            order.
     * 
     * @see Arrays#sort(Object[], int, int, java.util.Comparator)
     * 
     * @see BytesUtil#compareBytes(byte[], byte[])
     * 
     * @todo refactor to accept {@link IKeyBuffer}, which should be the basis
     *       for the data interchange as well - along with an IValueBuffer. This
     *       will promote reuse of key compression and value compression
     *       techniques for the on the wire format.
     *       <p>
     *       I would need to modify the {@link IKeyBuffer#search(byte[])} method
     *       to accept a fromIndex (and perhaps a toIndex) so that we can search
     *       within only the remaining keys.
     */
    public List<Split> splitKeys(int ntuples, byte[][] keys ) {
        
        if (ntuples <= 0)
            throw new IllegalArgumentException();
        
//        MetadataIndex mdi = getMetadataIndex();
        
        List<Split> splits = new LinkedList<Split>();
        
        // start w/ the first key.
        int fromIndex = 0;

        while(fromIndex<ntuples) {
        
        // partition spanning that key.
            PartitionMetadataWithSeparatorKeys pmd = getPartition(tx, name,
                    keys[fromIndex]);

            final byte[] rightSeparatorKey = pmd.getRightSeparatorKey();

            if (rightSeparatorKey == null) {

                /*
                 * The last index partition does not have an upper bound and
                 * will absorb any keys that order GTE to its left separator
                 * key.
                 */
                final int toIndex = ntuples;

                splits.add(new Split(pmd, fromIndex, toIndex));

                fromIndex = toIndex;

            } else {

                /*
                 * Otherwise this partition has an upper bound, so figure out
                 * the index of the last key that would go into this partition.
                 */
                int toIndex = BytesUtil.binarySearch(keys, fromIndex, ntuples
                        - fromIndex, rightSeparatorKey);

                if (toIndex < 0) {

                    toIndex = -toIndex - 1;

                }

                assert toIndex > fromIndex;

                splits.add(new Split(pmd, fromIndex, toIndex));

                fromIndex = toIndex;

            }

        }

        return splits;

    }

    /**
     * Describes a "split" of keys for a batch operation that are spanned by the
     * same index partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Split {
        
        /**
         * The index partition that spans the keys in this split.
         */
        public final IPartitionMetadata pmd;

        /**
         * Index of the first key in this split.
         */
        public final int fromIndex;
        
        /**
         * Index of the first key NOT included in this split.
         */
        public final int toIndex;

        /**
         * The #of keys in this split (toIndex - fromIndex).
         */
        public final int ntuples;
        
        /**
         * Create a representation of a split point.
         * 
         * @param pmd
         *            The metadata for the index partition within which the keys
         *            in this split lie.
         * @param fromIndex
         *            The index of the first key that will enter that index
         *            partition (inclusive lower bound).
         * @param toIndex
         *            The index of the first key that will NOT enter that index
         *            partition (exclusive upper bound).
         */
        public Split(IPartitionMetadata pmd,int fromIndex,int toIndex) {
            
            assert pmd != null;
            assert fromIndex >= 0;
            assert toIndex >= fromIndex;
            
            this.pmd = pmd;
            
            this.fromIndex = fromIndex;
            
            this.toIndex = toIndex;
            
            this.ntuples = toIndex - fromIndex;
            
        }
        
        /**
         * Hash code is based on the {@link IPartitionMetadata} hash code.
         */
        public int hashCode() {
            
            return pmd.hashCode();
            
        }

        public boolean equals(Split o) {
            
            if( fromIndex != o.fromIndex ) return false;

            if( toIndex != o.toIndex ) return false;
            
            if( ntuples != o.ntuples ) return false;
            
            if( ! pmd.equals(o.pmd)) return false;
            
            return true;
            
        }
        
    }
    
    /**
     * 
     * @param tx
     * @param name
     * @param key
     * @return
     */
    public PartitionMetadataWithSeparatorKeys getPartition(long tx,
            String name, byte[] key) {

        MetadataIndex mdi = fed.getMetadataIndex(name);

        IPartitionMetadata pmd;

        //            final byte[][] data;

        try {

            final int index = mdi.findIndexOf(key);

            /*
             * The code from this point on is shared with getPartitionAtIndex() and
             * also by some of the index partition tasks (CreatePartition for one).
             */

            if (index == -1)
                return null;

            /*
             * The serialized index partition metadata record for the partition that
             * spans the given key.
             */
            byte[] val = (byte[]) mdi.valueAt(index);

            /*
             * The separator key that defines the left edge of that index partition
             * (always defined).
             */
            byte[] leftSeparatorKey = (byte[]) mdi.keyAt(index);

            /*
             * The separator key that defines the right edge of that index partition
             * or [null] iff the index partition does not have a right sibling (a
             * null has the semantics of no upper bound).
             */
            byte[] rightSeparatorKey;

            try {

                rightSeparatorKey = (byte[]) mdi.keyAt(index + 1);

            } catch (IndexOutOfBoundsException ex) {

                rightSeparatorKey = null;

            }

            //                return new byte[][] { leftSeparatorKey, val, rightSeparatorKey };
            //
            //                data = getMetadataService().getPartition(name, key);
            //                
            //                if (data == null)
            //                    return null;

            pmd = (IPartitionMetadata) SerializerUtil.deserialize(val);

            return new PartitionMetadataWithSeparatorKeys(leftSeparatorKey,
                    pmd, rightSeparatorKey);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * @todo this is subject to concurrent modification of the metadata index
     *       would can cause the index to identify a different partition. client
     *       requests that use {@link #findIndexOfPartition(String, byte[])} and
     *       {@link #getPartitionAtIndex(String, int)} really need to refer to
     *       the same historical version of the metadata index (this effects
     *       range count and range iterator requests and to some extent batch
     *       operations that span multiple index partitions).
     */
    public PartitionMetadataWithSeparatorKeys getPartitionAtIndex(String name,
            int index) {

        MetadataIndex mdi = fed.getMetadataIndex(name);

        /*
         * The code from this point on is shared with getPartition()
         */

        if (index == -1)
            return null;

        /*
         * The serialized index partition metadata record for the partition that
         * spans the given key.
         */
        byte[] val = (byte[]) mdi.valueAt(index);

        /*
         * The separator key that defines the left edge of that index partition
         * (always defined).
         */
        byte[] leftSeparatorKey = (byte[]) mdi.keyAt(index);

        /*
         * The separator key that defines the right edge of that index partition
         * or [null] iff the index partition does not have a right sibling (a
         * null has the semantics of no upper bound).
         */
        byte[] rightSeparatorKey;

        try {

            rightSeparatorKey = (byte[]) mdi.keyAt(index + 1);

        } catch (IndexOutOfBoundsException ex) {

            rightSeparatorKey = null;

        }

        return new PartitionMetadataWithSeparatorKeys(leftSeparatorKey,
                (PartitionMetadata) SerializerUtil.deserialize(val),
                rightSeparatorKey);

    }

    //        public PartitionMetadataWithSeparatorKeys getPartitionAtIndex(long tx, String name, int index) {
    //
    //            IPartitionMetadata pmd;
    //
    //            byte[][] data;
    //
    //            try {
    //                
    //                data = getMetadataService().getPartitionAtIndex(name, index);
    //                
    //                if (data == null)
    //                    return null;
    //                
    //                pmd = (IPartitionMetadata) SerializerUtil.deserialize(data[1]);
    //                
    //            } catch(Exception ex) {
    //                
    //                throw new RuntimeException(ex);
    //                
    //            }
    //
    //            return new PartitionMetadataWithSeparatorKeys(data[0],pmd,data[2]);
    //
    //        }

    //        private Map<String, Map<Integer, IDataService>> indexCache = new ConcurrentHashMap<String, Map<Integer, IDataService>>(); 
    //
    //        synchronized(indexCache) {
    //      
    //          Map<Integer,IDataService> partitionCache = indexCache.get(name);
    //       
    //          if(partitionCache==null) {
    //              
    //              partitionCache = new ConcurrentHashMap<Integer, IDataService>();
    //              
    //              indexCache.put(name, partitionCache);
    //              
    //          }
    //          
    //          IDataService dataService = 
    //          
    //      }

    /**
     * Resolve the data service to which the index partition was mapped.
     * <p>
     * This uses the lookup cache provided by
     * {@link IBigdataClient#getDataService(UUID))}
     */
    public IDataService getDataService(IPartitionMetadata pmd) {


        final UUID serviceID = pmd.getDataServices()[0];
        //ServiceID serviceID = JiniUtil.uuid2ServiceID(pmd.getDataServices()[0]);

        final IDataService dataService;

        try {

            dataService = fed.getClient().getDataService(serviceID);

//            dataService = fed.getClient().getDataService(
//                    JiniUtil.serviceID2UUID(serviceID));

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return dataService;

    }

    /**
     * This reports the response for the index partition that spans an key (this
     * index partition should always exist).
     */
    public boolean isIsolatable() {
        
        if (isolatable == null) {

            final byte[] key = new byte[] {};

            IPartitionMetadata pmd = getPartition(tx, name, key);

            IDataService dataService = getDataService(pmd);

            try {

                isolatable = Boolean.valueOf(dataService
                        .isIsolatable(DataService.getIndexPartitionName(name,
                                pmd.getPartitionId())));

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }
        
        return isolatable.booleanValue();
        
    }

    // cached value. @todo invalidate on drop/add.
    private Boolean isolatable = null;
    
}
