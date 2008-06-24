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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.RangeCountProcedure;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.rawstore.IRawStore;

/**
 * This class encapsulates access to the services for a remote bigdata
 * federation - it is in effect a proxy object for the distributed set of
 * services that comprise the federation.
 * 
 * @todo Explore a variety of cached and uncached strategies for the metadata
 *       index. An uncached strategy is currently used. However, caching may be
 *       necessary for some kinds of application profiles, especially as the #of
 *       index partitions grows. If an application performs only unisolated and
 *       read-committed operations, then a single metadata index cache can be
 *       shared by the client for all operations against a given scale-out
 *       index. On the other hand, a client that uses transactions or performs
 *       historical reads will need to have a view of the metadata index as of
 *       the timestamp associated with the transaction or historical read.
 * 
 * @todo support failover metadata service discovery.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRemoteFederation extends AbstractFederation {

    /**
     * A temporary store used to cache various data in the client.
     */
    private final IRawStore clientTempStore = new TemporaryRawStore();

    /**
     * A per-index partition metadata cache.
     * 
     * @todo close out unused cache entries.
     */
    private final Map<String, MetadataIndex> partitions = new ConcurrentHashMap<String, MetadataIndex>();
    
    public AbstractRemoteFederation(IBigdataClient client) {

        super(client);

    }

    synchronized public void shutdownNow() {

        super.shutdownNow();
        
        if (clientTempStore.isOpen()) {

            clientTempStore.close();

        }

        partitions.clear();

    }
    
    synchronized public void shutdown() {

        super.shutdown();
        
        if (clientTempStore.isOpen()) {

            clientTempStore.close();

        }

        partitions.clear();

    }
    
    /**
     * This is a read-through view onto the metadata index.
     * <p>
     * The easiest way to have the view be correct is for the operations to all
     * run against the remote metadata index (no caching).
     * <p>
     * There are three kinds of queries that we do against the metadata index:
     * (1) get(key); (2) find(key); and (3) locatorScan(fromKey,toKey). The
     * first is only used by the unit tests. The second is used when we start a
     * locator scan, when we split a batch operation against the index
     * partitions, and when we map an index procedure over a key range or use a
     * key range iterator. This is the most costly of the queries, but it is
     * also the one that is the least easy to cache. The locator scan itself is
     * heavily buffered - a cache would only help for frequently scanned and
     * relatively small key ranges. For this purpose, it may be better to cache
     * the iterator result itself locally to the client (for historical reads or
     * transactional reads).
     * <p>
     * The difficulty with caching find(key) is that we need to use the
     * {@link ILinearList} API to locate the appropriate index partition.
     * However, since it is a cache, there can be cache misses. These would show
     * up as a "gap" in the (leftSeparator,rightSeparator) coverage.
     * <p>
     * If we do not cache access to the remote metadata index then we will
     * impose additional latency on clients, traffic on the network, and demands
     * on the metadata service. However, with high client concurrency mitigates
     * the increase in access latency to the metadata index.
     * 
     * @todo test with and without caching and see what works better. this can
     *       evolve over time, probably some configuration options to control
     *       cache behavior. note that caching of result sets also makes sense
     *       for historical reads or transactions.
     * 
     * @todo Rather than synchronizing all requests, this should queue requests
     *       for a specific metadata index iff there is a cache miss for that
     *       index.
     * @todo Use a weak-ref cache with an LRU (or hard reference cache) to evict
     *       cached {@link PartitionLocator}. The client needs access by {
     *       indexName, timestamp, key }. We need to eventually evict the cached
     *       locators to prevent the client from building up too much state
     *       locally. Also the cached locators can not be shared across
     *       different timestamps, so clients will build up a locator cache when
     *       working on a transaction but then never go back to that cache once
     *       the transaction completes.
     *       <p>
     *       While it may be possible to share cached locators between
     *       historical reads and transactions for the same point in history, we
     *       do not have enough information on hand to make those decisions.
     *       What we would need to know is the historical commit time
     *       corresponding to an assigned transaction startTime. This is not
     *       one-to-one since the start times for transactions must be unique
     *       (among those in play). See
     *       {@link ITransactionManager#newTx(com.bigdata.journal.IsolationEnum)}
     *       for more on this.
     * 
     * @todo cache leased information about index partitions of interest to the
     *       client. The cache will be a little tricky since we need to know
     *       when the client does not possess a partition definition. Index
     *       partitions are defined by the separator key - the first key that
     *       lies beyond that partition. the danger then is that a client will
     *       presume that any key before the first leased partition is part of
     *       that first partition. To guard against that the client needs to
     *       know both the separator key that represents the upper and lower
     *       bounds of each partition. If a lookup in the cache falls outside of
     *       any known partitions upper and lower bounds then it is a cache miss
     *       and we have to ask the metadata service for a lease on the
     *       partition. the cache itself is just a btree data structure with the
     *       proviso that some cache entries represent missing partition
     *       definitions (aka the lower bounds for known partitions where the
     *       left sibling partition is not known to the client).
     */
    synchronized public IMetadataIndex getMetadataIndex(String name,
            long timestamp) {

        assertOpen();

        final MetadataIndexMetadata mdmd = getMetadataIndexMetadata(name, timestamp);
        
        // No such index.
        if (mdmd == null) return null;
                
        if (true) {

            return new NoCacheMetadataIndexView(name, timestamp, mdmd);
            
        } else {

            // FIXME Cache per isolation time.
            if (timestamp != ITx.UNISOLATED)
                throw new UnsupportedOperationException();

            MetadataIndex tmp = partitions.get(name);

            if (tmp == null) {

                tmp = cacheMetadataIndex(name, timestamp, mdmd);

                if (tmp == null) {

                    // No such scale-out index.

                    return null;

                }

                // save reference to cached mdi.
                partitions.put(name, tmp);

            }

            return tmp;

        }
        
    }

    /**
     * Cache the index partition metadata in the client.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The cached partition metadata -or- <code>null</code> iff there
     *         is no such scale-out index.
     * 
     * FIXME Just create cache view when MDI is large and then cache on demand.
     */
    private MetadataIndex cacheMetadataIndex(String name, long timestamp,
            MetadataIndexMetadata mdmd) {

        assertOpen();

        // The UUID of the metadata index.
        final UUID metadataIndexUUID = mdmd.getIndexUUID();

        /*
         * Allocate a cache for the defined index partitions.
         */
        MetadataIndex mdi = MetadataIndex.create(//
                clientTempStore,//
                metadataIndexUUID,//
                mdmd.getManagedIndexMetadata()// the managed index's metadata.
        );

        /*
         * Bulk copy the partition definitions for the scale-out index into the
         * client.
         * 
         * Note: This assumes that the metadata index is NOT partitioned and
         * DOES NOT support delete markers.
         * 
         * @todo the easiest way to handle a scale-out metadata index is to
         * make it hash-partitioned (vs range-partitioned).  We can just flood
         * queries to the hash partitioned index.  For the iterator, we have to
         * buffer the results and place them back into order.  A fused view style
         * iterator could be used to merge the iterator results from each partition
         * into a single totally ordered iterator.
         */
        {
        
            final ITupleIterator itr = new RawDataServiceRangeIterator(
                    getMetadataService(),//
                    MetadataService.getMetadataIndexName(name), //
                    timestamp,//
                    true, // readConsistent
                    null, // fromKey
                    null, // toKey
                    0, // capacity
                    IRangeQuery.KEYS | IRangeQuery.VALS, //
                    null // filter
                    );
        
            while(itr.hasNext()) {
             
                ITuple tuple = itr.next();
                
                byte[] key = tuple.getKey();
                
                byte[] val = tuple.getValue();
                
                mdi.insert(key, val);
                
            }
            
        }

        return mdi;

    }

    /**
     * An implementation that performs NO caching. All methods read through to
     * the remote metadata index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class NoCacheMetadataIndexView implements IMetadataIndex {

        final String name;
        final long timestamp;
        final MetadataIndexMetadata mdmd;
        
        /**
         * 
         * @param name The name of the scale-out index.
         * @param timestamp
         */
        public NoCacheMetadataIndexView(String name,long timestamp,MetadataIndexMetadata mdmd) {
        
            assert name != null;
            assert mdmd != null;
            
            this.name = name;

            this.timestamp = timestamp;

            this.mdmd = mdmd;
            
        }
        
        // @todo re-fetch if READ_COMMITTED or UNISOLATED? it's very unlikely to change.
        public IndexMetadata getScaleOutIndexMetadata() {

            return mdmd;
            
        }

        // this could be cached easily, but its only used by the unit tests.
        public PartitionLocator get(byte[] key) {

            try {

                return getMetadataService().get(name, timestamp, key);
                
            } catch (Exception e) {
                
                throw new RuntimeException(e);
                
            }
            
        }

        // harder to cache - must look for "gaps"
        public PartitionLocator find(byte[] key) {

            try {

                return getMetadataService().find(name, timestamp, key);
                
            } catch (Exception e) {
                
                throw new RuntimeException(e);
                
            }
            
        }


        public long rangeCount() {

            return rangeCount(null,null);
            
        }

        // only used by unit tests.
        public long rangeCount(byte[] fromKey, byte[] toKey) {

            final IIndexProcedure proc = new RangeCountProcedure(
                    false/* exact */, fromKey, toKey);

            final Long rangeCount;
            try {

                rangeCount = (Long) getMetadataService().submit(timestamp,
                        MetadataService.getMetadataIndexName(name), proc);

            } catch (Exception e) {

                throw new RuntimeException(e);

            }

            return rangeCount.longValue();

        }

        public long rangeCountExact(byte[] fromKey, byte[] toKey) {

            final IIndexProcedure proc = new RangeCountProcedure(
                    true/* exact */, fromKey, toKey);

            final Long rangeCount;
            try {

                rangeCount = (Long) getMetadataService().submit(timestamp,
                        MetadataService.getMetadataIndexName(name), proc);

            } catch (Exception e) {

                throw new RuntimeException(e);

            }

            return rangeCount.longValue();
            
        }
        
        public ITupleIterator rangeIterator() {
            
            return rangeIterator(null,null);
            
        }
        
        public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey) {

            return rangeIterator(fromKey, toKey, 0/* capacity */,
                    IRangeQuery.DEFAULT, null/* filter */);

        }

        /**
         * Note: Since this view is read-only this method forces the use of
         * {@link ITx#READ_COMMITTED} IFF the timestamp for the view is
         * {@link ITx#UNISOLATED}. This produces the same results on read and
         * reduces contention for the {@link WriteExecutorService}. This is
         * already done automatically for anything that gets run as an index
         * procedure, so we only have to do this explicitly for the range
         * iterator method.
         */
        // not so interesting to cache, but could cache the iterator results on the scale-out index.
        public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
                int capacity, int flags, ITupleFilter filter) {

            return new RawDataServiceRangeIterator(getMetadataService(),//
                    MetadataService.getMetadataIndexName(name), //
                    (timestamp==ITx.UNISOLATED?ITx.READ_COMMITTED:timestamp),//
                    true, // read-consistent semantics.
                    null, // fromKey
                    null, // toKey
                    0, // capacity
                    IRangeQuery.KEYS | IRangeQuery.VALS, //
                    null // filter
            );

        }

    }

    /**
     * Return <code>true</code>.
     */
    public boolean isScaleOut() {
        
        return true;
        
    }

}
