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
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
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
 * @todo change the {@link IDataService} API so that we can provide custom
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

    /**
     * The maximum #of tuples per request for the
     * {@link #rangeIterator(byte[], byte[])}
     * 
     * @todo make this a configuration parameter for the client and/or index
     */
    final int capacity = 50000;

    private final long tx;
    
    /**
     * The transaction identifier for this index view -or-
     * {@link IDataService#UNISOLATED} if the index view is not transactional.
     * 
     * @return The transaction identifier for the index view.
     */
    public long getTx() {
        
        return tx;
        
    }
    
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
     * Whether or not the index supports isolation - this is a cached value.
     * 
     * @todo invalidate on drop/add.
     */
    private Boolean isolatable = null;
    
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
             * 
             * @todo do we cache this when we cache the metadata index?
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

    /**
     * @todo report on both the metadata index and the individual index
     *       partitions. If we parallelize the index partition reporting then we
     *       need to use a thread-safe {@link StringBuffer} rather than a
     *       {@link StringBuilder}.
     */
    public String getStatistics() {

        StringBuilder sb = new StringBuilder();

        sb.append("scale-out index: name="+name);

        /*
         * Statistics for the metadata index.
         */
        try {
            
            String _name = MetadataService.getMetadataName(name);
            
            sb.append("\n" + _name + " : "
                    + getMetadataService().getStatistics(_name));

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        /*
         * Statistics for the index partitions (at least those that are cached
         * by the client).
         */
        {
            
            final MetadataIndex mdi = getMetadataIndex();
            
            final IEntryIterator itr = mdi.rangeIterator(null, null);
            
            while(itr.hasNext()) {
                
                final byte[] val = (byte[]) itr.next();
                
                final PartitionMetadata pmd = (PartitionMetadata) SerializerUtil.deserialize(val);
                
                final String _name = DataService.getIndexPartitionName(name, pmd.getPartitionId());
                
                sb.append("\npartition: " + _name);
                sb.append("\nresources: " + Arrays.toString(pmd.getResources()));
                sb.append("\nliveFiles: " + Arrays.toString(pmd.getLiveSegmentFiles()));
                sb.append("\ndataServices: " + Arrays.toString(pmd.getDataServices()));
                
                String _stats;
                try {
                    _stats = getDataService(pmd).getStatistics( _name );
                } catch (IOException e) {
                    _stats = "Could not obtain index partition statistics: "+e.toString();
                }
                
                sb.append( "\nindexStats: "+_stats);
                
            }
            
        }

        
        return sb.toString();
        
    }

    /**
     * This reports the response for the index partition that contains an byte[]
     * key (this index partition should always exist).
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
    
    public boolean contains(byte[] key) {
        
        IPartitionMetadata pmd = getPartition(tx,name, key);

        IDataService dataService = getDataService(pmd);

        final boolean[] ret;
        
        try {

            // the name of the index partition.
            final String name = DataService.getIndexPartitionName(this.name,
                    pmd.getPartitionId());

            ret = dataService.batchContains(tx, name, 1, new byte[][] { key });
            
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
            
            // the name of the index partition.
            final String name = DataService.getIndexPartitionName(this.name,
                    pmd.getPartitionId());

            ret = dataService.batchInsert(tx, name, 1,
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
            
            // the name of the index partition.
            final String name = DataService.getIndexPartitionName(this.name,
                    pmd.getPartitionId());

            ret = dataService.batchLookup(tx, name, 1,
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
            
            // the name of the index partition.
            final String name = DataService.getIndexPartitionName(this.name,
                    pmd.getPartitionId());

            ret = dataService.batchRemove(tx, name, 1,
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

        MetadataIndex mdi = getMetadataIndex();

        final int fromIndex;
        final int toIndex;

        // index of the first partition to check.
        fromIndex = (fromKey == null ? 0 : mdi.findIndexOf(fromKey));

        // index of the last partition to check.
        toIndex = (toKey == null ? mdi.getEntryCount() - 1 : mdi
                .findIndexOf(toKey));

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
                
                // the name of the index partition.
                final String name = DataService.getIndexPartitionName(this.name,
                        pmd.getPartitionId());

                count += dataService.rangeCount(tx, name, _fromKey, _toKey);
                
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
        
        return new PartitionedRangeQueryIterator(this, tx, fromKey, toKey, capacity,
                IDataService.KEYS | IDataService.VALS);

    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags) {

        return new PartitionedRangeQueryIterator(this, tx, fromKey, toKey, capacity,
                flags);
        
    }

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

                // the name of the index partition.
                final String name = DataService.getIndexPartitionName(this.name,
                        split.pmd.getPartitionId());

                _vals = dataService.batchContains(tx, name, split.ntuples, _keys);
                
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

                // the name of the index partition.
                final String name = DataService.getIndexPartitionName(this.name,
                        split.pmd.getPartitionId());

                _vals = dataService.batchLookup(tx, name, split.ntuples, _keys);
                
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

                // the name of the index partition.
                final String name = DataService.getIndexPartitionName(this.name,
                        split.pmd.getPartitionId());

                oldVals = dataService.batchInsert(tx, name, split.ntuples,
                        _keys, _vals, returnOldValues);
                
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

                // the name of the index partition.
                final String name = DataService.getIndexPartitionName(this.name,
                        split.pmd.getPartitionId());

                oldVals = dataService.batchRemove(tx, name, split.ntuples,
                        _keys, returnOldValues);
                
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
     * The procedure will be transparently broken down and executed against each
     * index partitions spanned by its keys.
     * 
     * @return The aggregated result of applying the procedure to the relevant
     *         index partitions.
     */
    public void submit(int n, byte[][] keys, byte[][] vals,
            IIndexProcedureConstructor ctor, IResultAggregator aggregator) {

        if (ctor == null) {

            throw new IllegalArgumentException();
        
        }
        
        if (aggregator == null) {

            throw new IllegalArgumentException();
            
        }
        
        /*
         * Run the procedure remotely.
         * 
         * @todo add counters for the #of procedures run and the execution time
         * for those procedures. add counters for the #of splits and the #of
         * tuples in each split, as well as the total #of tuples.
         */

        final List<Split> splits = splitKeys(n, keys);

        final Iterator<Split> itr = splits.iterator();

        while (itr.hasNext()) {

            final Split split = itr.next();

            final IDataService dataService = getDataService(split.pmd);

            final IIndexProcedure proc = ctor.newInstance(split.ntuples,
                    split.fromIndex, keys, vals);

            try {

                // the name of the index partition.
                final String name = DataService.getIndexPartitionName(
                        this.name, split.pmd.getPartitionId());

                final Object result = dataService.submit(tx, name, proc);

                aggregator.aggregate(result, split);

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

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
    public List<Split> splitKeys(int ntuples, byte[][] keys) {
        
        if (ntuples <= 0)
            throw new IllegalArgumentException();
        
//        MetadataIndex mdi = getMetadataIndex();
        
        List<Split> splits = new LinkedList<Split>();
        
        // start w/ the first key.
        int fromIndex = 0;

        while(fromIndex<ntuples) {
        
            // partition spanning that key.
            final PartitionMetadataWithSeparatorKeys pmd = getPartition(tx, name,
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

}
