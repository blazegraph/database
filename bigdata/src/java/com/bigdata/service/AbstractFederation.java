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
 * Created on Mar 28, 2008
 */

package com.bigdata.service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.cache.ICacheEntry;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.QueueLengthTask;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.ITPV;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for common functionality for {@link IBigdataFederation}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractFederation implements IBigdataFederation {

    private IBigdataClient client;
    
    public IBigdataClient getClient() {
        
        assertOpen();
        
        return client;
        
    }
    
    /**
     * Normal shutdown allows any existing client requests to federation
     * services to complete but does not schedule new requests, disconnects from
     * the federation, and then terminates any background processing that is
     * being performed on the behalf of the client (service discovery, etc).
     * <p>
     * Note: concrete implementations MUST extend this method.
     * <p>
     * Note: Clients use {@link IBigdataClient#disconnect(boolean)} to disconnect
     * from a federation.  The federation implements that disconnect using either
     * {@link #shutdown()} or {@link #shutdownNow()}.
     */
    synchronized public void shutdown() {

        assertOpen();

        final long begin = System.currentTimeMillis();

        log.info("begin");

        // allow client requests to finish normally.
        threadPool.shutdown();

        try {

            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {

                log.warn("Timeout awaiting thread pool termination.");

            }

        } catch (InterruptedException e) {

            log.warn("Interrupted awaiting thread pool termination.", e);

        }

        log.info("done: elapsed=" + (System.currentTimeMillis() - begin));
        
        client = null;
        
    }

    /**
     * Immediate shutdown terminates any client requests to federation services,
     * disconnects from the federation, and then terminate any background
     * processing that is being performed on the behalf of the client (service
     * discovery, etc).
     * <p>
     * Note: concrete implementations MUST extend this method to either
     * disconnect from the remote federation or close the embedded federation
     * and then clear the {@link #fed} reference so that the client is no longer
     * "connected" to the federation.
     * <p>
     * Note: Clients use {@link IBigdataClient#disconnect(boolean)} to disconnect
     * from a federation.  The federation implements that disconnect using either
     * {@link #shutdown()} or {@link #shutdownNow()}.
     */
    synchronized public void shutdownNow() {
        
        assertOpen();
        
        final long begin = System.currentTimeMillis();
        
        log.info("begin");
        
        // stop client requests.
        threadPool.shutdownNow();
        
        log.info("done: elapsed="+(System.currentTimeMillis()-begin));
        
        client = null;
        
    }
    
    /**
     * @throws IllegalStateException
     *                if the client has disconnected from the federation.
     */
    protected void assertOpen() {

        if (client == null) {

            throw new IllegalStateException();

        }

    }

    /**
     * Used to run application tasks.
     */
    private final ThreadPoolExecutor threadPool;

    /**
     * Used to sample and report on the queue associated with the
     * {@link #threadPool}.
     */
    protected final ScheduledExecutorService sampleService = Executors
            .newSingleThreadScheduledExecutor(DaemonThreadFactory
                    .defaultThreadFactory());
    
    public ExecutorService getThreadPool() {
        
        assertOpen();
        
        return threadPool;
        
    }

    protected AbstractFederation(IBigdataClient client) {

        if (client == null)
            throw new IllegalArgumentException();

        this.client = client;

        final int threadPoolSize = client.getThreadPoolSize();

        if (threadPoolSize == 0) {

            threadPool = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(DaemonThreadFactory
                            .defaultThreadFactory());

        } else {

            threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    threadPoolSize, DaemonThreadFactory.defaultThreadFactory());

        }
        
        /*
         * indexCache
         */
        {

            indexCache = new WeakValueCache<NT, IClientIndex>(
                    new LRUCache<NT, IClientIndex>(client.getIndexCacheCapacity()));
            
        }
        
        /*
         * Setup sampling and reporting on the client's thread pool.
         * 
         * @todo report to the load balancer.
         * 
         * @todo config. See ConcurrencyManager, which also setups up some queue
         * monitors.
         */
        {

            final long initialDelay = 0; // initial delay in ms.
            final long delay = 1000; // delay in ms.
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            QueueLengthTask queueLengthTask = new QueueLengthTask(
                    "clientThreadPool", threadPool);

            sampleService.scheduleWithFixedDelay(queueLengthTask, initialDelay,
                    delay, unit);
        }

    }
    
    public void registerIndex(IndexMetadata metadata) {

        assertOpen();

        registerIndex(metadata, null);

    }

    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {

        assertOpen();

        if (dataServiceUUID == null) {
            
            final ILoadBalancerService loadBalancerService = getLoadBalancerService();

            if (loadBalancerService == null) {

                try {

                    /*
                     * As a failsafe (or at least a failback) we ask the client
                     * for ANY data service that it knows about and use that as
                     * the data service on which we will register this index.
                     * This lets us keep going if the load balancer is dead when
                     * this request comes through.
                     */

                    dataServiceUUID = getAnyDataService().getServiceUUID();

                } catch (Exception ex) {

                    log.error(ex);

                    throw new RuntimeException(ex);

                }
                
            } else {

                try {

                    dataServiceUUID = loadBalancerService
                            .getUnderUtilizedDataService();

                } catch (Exception ex) {

                    throw new RuntimeException(ex);

                }

            }
            
        }

        return registerIndex(//
                metadata, //
                new byte[][] { new byte[] {} },//
                new UUID[] { dataServiceUUID } //
            );

    }

    public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs) {

        assertOpen();

        try {

            UUID indexUUID = getMetadataService().registerScaleOutIndex(
                    metadata, separatorKeys, dataServiceUUIDs);

            return indexUUID;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * An index name and a timestamp.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class NT {
        
        public final String name;
        public final long timestamp;
        private final int hashCode;
       
        public NT(String name, long timestamp) {
            
            if (name == null)
                throw new IllegalArgumentException();
            
            this.name = name;
            
            this.timestamp = timestamp;
            
            this.hashCode = name.hashCode()<<32 + (Long.valueOf(timestamp).hashCode()>>>32);
            
        }
        
        public int hashCode() {
            
            return hashCode;
            
        }
        
        public boolean equals(Object o) {

            return equals((NT)o);
            
        }
        
        public boolean equals(NT o) {
            
            if(this==o) return true;
            
            if(!this.name.equals(o.name)) return false;

            if(this.timestamp != o.timestamp) return false;
            
            return true;
            
        }
        
    }
    
    /**
     * A canonicalizing cache for the client's {@link IIndex} proxy objects. The
     * keys are {@link NT} objects which represent both the name of the index
     * and the timestamp for the index view. The values are the {@link IIndex}
     * proxy objects.
     * <p>
     * Note: The "dirty" flag associated with the object in this cache is
     * ignored.
     */
    final protected WeakValueCache<NT, IClientIndex> indexCache;
    
    /**
     * @todo synchronization should be limited to the index resource and the
     *       cache when we actually touch the cache. synchronizing the entire
     *       method limits concurrency for access to other named resources or
     *       the same named resource as of a different timestamp.
     */
    synchronized public IIndex getIndex(String name,long timestamp) {

        log.info("name="+name+" @ "+timestamp);
        
        assertOpen();

        final NT nt = new NT(name,timestamp);
        
        IClientIndex ndx = indexCache.get(nt);

        if (ndx == null) {

            final MetadataIndexMetadata mdmd = getMetadataIndexMetadata(name,
                    timestamp);

            // No such index.
            if (mdmd == null) {

                log.info("name="+name+" @ "+timestamp+" : is not registered");
                
                return null;
                
            }

            // Index exists.
            ndx = new ClientIndexView(this, name, timestamp, mdmd);

            indexCache.put(nt, ndx, false/* dirty */);

            log.info("name="+name+" @ "+timestamp+" : index exists.");
            
        } else {
            
            log.info("name="+name+" @ "+timestamp+" : cache hit.");
            
        }

        return ndx;

    }

    public void dropIndex(String name) {

        log.info("name="+name);
        
        assertOpen();

        try {
            
            getMetadataService().dropScaleOutIndex(name);

            log.info("dropped scale-out index.");
            
            dropIndexFromCache(name);

        } catch (Exception e) {

            throw new RuntimeException( e );
            
        }

    }

    /**
     * Drops the entry for the named index from the {@link #indexCache}.
     * <p>
     * Historical and transactional reads are still allowed, but we remove the
     * the read-committed or unisolated views from the cache once the index has
     * been dropped. If a client wants them, it needs to re-request. If they
     * have been re-registered on the metadata service then they will become
     * available again.
     * <p>
     * Note: Operations against unisolated or read-committed indices will throw
     * exceptions if they execute after the index was dropped.
     */
    protected void dropIndexFromCache(String name) {
        
        synchronized(indexCache) {
            
            Iterator<ICacheEntry<NT,IClientIndex>> itr = indexCache.entryIterator();
            
            while(itr.hasNext()) {
                
                final ICacheEntry<NT,IClientIndex> entry = itr.next();
                
                final IClientIndex ndx = entry.getObject(); 
                
                if(name.equals(ndx.getName())) {
                    
                    final long timestamp = ndx.getTimestamp();

                    if (timestamp == ITx.UNISOLATED
                            || timestamp == ITx.READ_COMMITTED) {
                    
                        log.info("dropped from cache: "+name+" @ "+timestamp);
                        
                        // remove from the cache.
                        indexCache.remove(entry.getKey());

                    }
                    
                }
                
            }
            
        }
        
    }
    
    /**
     * Return the metadata for the metadata index itself.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @param timestamp
     * 
     * @return The metadata for the metadata index or <code>null</code> iff no
     *         scale-out index is registered by that name at that timestamp.
     */
    protected MetadataIndexMetadata getMetadataIndexMetadata(String name, long timestamp) {

        assertOpen();

        final MetadataIndexMetadata mdmd;
        try {

            // @todo test cache for this object as of that timestamp?
            mdmd = (MetadataIndexMetadata) getMetadataService()
                    .getIndexMetadata(
                            MetadataService.getMetadataIndexName(name),
                            timestamp);
            
            assert mdmd != null;

        } catch( NoSuchIndexException ex ) {
            
            return null;
        
        } catch (ExecutionException ex) {
            
            if(InnerCause.isInnerCause(ex, NoSuchIndexException.class)) return null;
            
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
    
    /*
     * thread-local key builder. 
     */

    /**
     * A {@link ThreadLocal} variable providing access to thread-specific
     * instances of a configured {@link IKeyBuilder}.
     * <p>
     * Note: this {@link ThreadLocal} is not static since we need configuration
     * properties from the constructor - those properties can be different for
     * different {@link IBigdataClient}s on the same machine.
     */
    private ThreadLocal<IKeyBuilder> threadLocalKeyBuilder = new ThreadLocal<IKeyBuilder>() {

        protected synchronized IKeyBuilder initialValue() {

            return KeyBuilder.newUnicodeInstance(client.getProperties());

        }

    };

    /**
     * Return a {@link ThreadLocal} {@link IKeyBuilder} instance configured
     * using the properties specified for the {@link IBigdataClient}.
     */
    public IKeyBuilder getKeyBuilder() {
        
        assertOpen();
        
        return threadLocalKeyBuilder.get();
        
    }
    
    /*
     * named records
     */
    
    private final String GLOBAL_ROW_STORE_INDEX = "__global_namespace_index";

    synchronized public SparseRowStore getGlobalRowStore() {
        
        log.info("");

        if (globalRowStore == null) {

            IIndex ndx = getIndex(GLOBAL_ROW_STORE_INDEX, ITx.UNISOLATED);

            if (ndx == null) {

                log.info("Global row store does not exist - will try to register now");
                
                try {

                    registerIndex(new IndexMetadata(GLOBAL_ROW_STORE_INDEX,
                            UUID.randomUUID()));

                } catch (Exception ex) {

                    throw new RuntimeException(ex);

                }

                ndx = getIndex(GLOBAL_ROW_STORE_INDEX, ITx.UNISOLATED);

                if (ndx == null) {

                    throw new RuntimeException("Could not find index?");

                }

            }

            globalRowStore = new SparseRowStore(ndx);

        }
        
        return globalRowStore;

    }
    private SparseRowStore globalRowStore;
    
    public Object getNamedRecord(final String primaryKey) {

        if (primaryKey == null)
            throw new IllegalArgumentException();

        final ITPS tps = getGlobalRowStore().read(getKeyBuilder(),//
                GlobalRowStoreSchema.INSTANCE, //
                primaryKey,//
                Long.MAX_VALUE, // most recent value only
                null // filter
                );
        
        final ITPV tpv = tps.get(GlobalRowStoreSchema.VALUE);

        if (tpv.getValue() == null) {

            // No value for that name.
            return null;        
            
        }
        
        return tpv;
        
    }

    public Object putNamedRecord(String primaryKey, Object value) {

        if (primaryKey == null)
            throw new IllegalArgumentException();

        final Map<String,Object> row = new HashMap<String,Object>();
        
        row.put(primaryKey,value);
        
        final ITPS tps = getGlobalRowStore().write(//
                getKeyBuilder(), //
                GlobalRowStoreSchema.INSTANCE, //
                row,//
                SparseRowStore.AUTO_TIMESTAMP_UNIQUE,//
                null // filter
                );

        final ITPV tpv = tps.get(GlobalRowStoreSchema.VALUE);

        if (tpv.getValue() == null) {

            // No value for that name.
            return null;

        }

        return tpv;

    }

}
