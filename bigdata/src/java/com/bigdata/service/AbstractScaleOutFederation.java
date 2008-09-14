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
 * Created on Sep 13, 2008
 */

package com.bigdata.service;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.service.AbstractScaleOutClient.MetadataIndexCachePolicy;
import com.bigdata.service.AbstractScaleOutClient.Options;
import com.bigdata.util.InnerCause;

/**
 * Abstract base class for federation implementation uses the scale-out index
 * architecture (supporting key ranged partitioned indices).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractScaleOutFederation extends AbstractFederation {

    /**
     * @param client
     */
    public AbstractScaleOutFederation(IBigdataClient client) {
       
        super(client);
        
        indexCache = new IndexCache(this, client.getIndexCacheCapacity());

        metadataIndexCache = new MetadataIndexCache(this, client
                .getIndexCacheCapacity());
        
        final Properties properties = client.getProperties();
        
        metadataIndexCachePolicy = MetadataIndexCachePolicy.valueOf(properties
                .getProperty(Options.METADATA_INDEX_CACHE_POLICY,
                        Options.DEFAULT_METADATA_INDEX_CACHE_POLICY));

        if (INFO) {

            log.info(Options.METADATA_INDEX_CACHE_POLICY + "="
                    + metadataIndexCachePolicy);
            
        }
        
    }
    
    private final MetadataIndexCachePolicy metadataIndexCachePolicy;

    public synchronized void shutdown() {
        
        super.shutdown();

        indexCache.shutdown();

        metadataIndexCache.shutdown();
        
    }

    public synchronized void shutdownNow() {
    
        super.shutdownNow();
        
        indexCache.shutdown();

        metadataIndexCache.shutdown();
            
    }

    /**
     * Return a read-only view onto an {@link IMetadataIndex}.
     * 
     * @param name
     *            The name of the scale-out index.
     * @param timestamp
     *            The timestamp for the view.
     * 
     * @todo The easiest way to have the view be correct is for the operations
     *       to all run against the remote metadata index (no caching).
     *       <p>
     *       There are three kinds of queries that we do against the metadata
     *       index: (1) get(key); (2) find(key); and (3)
     *       locatorScan(fromKey,toKey). The first is only used by the unit
     *       tests. The second is used when we start a locator scan, when we
     *       split a batch operation against the index partitions, and when we
     *       map an index procedure over a key range or use a key range
     *       iterator. This is the most costly of the queries, but it is also
     *       the one that is the least easy to cache. The locator scan itself is
     *       heavily buffered - a cache would only help for frequently scanned
     *       and relatively small key ranges. For this purpose, it may be better
     *       to cache the iterator result itself locally to the client (for
     *       historical reads or transactional reads).
     *       <p>
     *       The difficulty with caching find(key) is that we need to use the
     *       {@link ILinearList} API to locate the appropriate index partition.
     *       However, since it is a cache, there can be cache misses. These
     *       would show up as a "gap" in the (leftSeparator, rightSeparator)
     *       coverage.
     *       <p>
     *       If we do not cache access to the remote metadata index then we will
     *       impose additional latency on clients, traffic on the network, and
     *       demands on the metadata service. However, with high client
     *       concurrency mitigates the increase in access latency to the
     *       metadata index.
     * 
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
    public IMetadataIndex getMetadataIndex(String name, long timestamp) {

        if (INFO)
            log.info("name="+name+" @ "+timestamp);
        
        assertOpen();

        return getMetadataIndexCache().getIndex(name, timestamp);
         
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
        final MetadataIndex mdi = MetadataIndex.create(//
                getTempStore(),//
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
        
            long n = 0;

            final ITupleIterator itr = new RawDataServiceTupleIterator(
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
        
            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final byte[] key = tuple.getKey();

                final byte[] val = tuple.getValue();

                mdi.insert(key, val);
                
                
                n++;

            }
            
            if(INFO) {
                
                log.info("Copied "+n+" locator records: name="+name);
                
            }
            
        }

        return mdi;
    
    }

    /**
     * Return <code>true</code>.
     */
    final public boolean isScaleOut() {
        
        return true;
        
    }

    private final IndexCache indexCache;
    private final MetadataIndexCache metadataIndexCache;
    
    protected AbstractIndexCache<ClientIndexView> getIndexCache() {
        
        return indexCache;
        
    }
    
    /**
     * Return the cache for {@link IMetadataIndex} objects.
     */
    protected MetadataIndexCache getMetadataIndexCache() {
        
        return metadataIndexCache;
        
    }
    
    /**
     * Concrete implementation for {@link IClientIndex} views.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     */
    public static class IndexCache extends AbstractIndexCache<ClientIndexView>{
        
        private final AbstractScaleOutFederation fed;
        
        public IndexCache(final AbstractScaleOutFederation fed, final int capacity) {
            
            super(capacity);

            if (fed == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;

        }
        
        @Override
        protected ClientIndexView newView(final String name, final long timestamp) {
            
            final IMetadataIndex mdi = fed.getMetadataIndex(name, timestamp);

            // No such index.
            if (mdi == null) {

                if (INFO)
                    log.info("name=" + name + " @ " + timestamp
                            + " : is not registered");

                return null;

            }

            // Index exists.
            return new ClientIndexView(fed, name, timestamp, mdi);
            
        }
        
    }
    
    /**
     * Concrete implementation for {@link IMetadataIndex} views.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     */
    public static class MetadataIndexCache extends AbstractIndexCache<IMetadataIndex>{

        private final AbstractScaleOutFederation fed;

        public MetadataIndexCache(final AbstractScaleOutFederation fed,
                final int capacity) {
            
            super(capacity);

            if (fed == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;

        }

        @Override
        protected IMetadataIndex newView(String name, long timestamp) {
            
            final MetadataIndexMetadata mdmd = getMetadataIndexMetadata(
                    name, timestamp);
            
            // No such index.
            if (mdmd == null) return null;
                    
            switch (fed.metadataIndexCachePolicy) {

            case NoCache:
                return new NoCacheMetadataIndexView(fed, name, timestamp, mdmd);

            case CacheAll:

                return fed.cacheMetadataIndex(name, timestamp, mdmd);

            default:
                throw new AssertionError("Unknown option: "
                        + fed.metadataIndexCachePolicy);
            }
            
        }
        
        /**
         * Return the metadata for the metadata index itself.
         * <p>
         * Note: This method always reads through!
         * 
         * @param name
         *            The name of the scale-out index.
         * 
         * @param timestamp
         * 
         * @return The metadata for the metadata index or <code>null</code>
         *         iff no scale-out index is registered by that name at that
         *         timestamp.
         */
        protected MetadataIndexMetadata getMetadataIndexMetadata(String name,
                long timestamp) {

            final MetadataIndexMetadata mdmd;
            try {

                // @todo test cache for this object as of that timestamp?
                mdmd = (MetadataIndexMetadata) fed.getMetadataService()
                        .getIndexMetadata(
                                MetadataService.getMetadataIndexName(name),
                                timestamp);
                
                assert mdmd != null;

            } catch( NoSuchIndexException ex ) {
                
                return null;
            
            } catch (ExecutionException ex) {
                
                if (InnerCause.isInnerCause(ex, NoSuchIndexException.class)) {

                    // per API.
                    return null;
                    
                }
                
                throw new RuntimeException(ex);
                
            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }
            
            if (mdmd == null) {

                // No such index.
                
                return null;

            }
            
            return mdmd;

        }
        
    }

}
