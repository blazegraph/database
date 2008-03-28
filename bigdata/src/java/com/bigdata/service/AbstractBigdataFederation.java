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

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.QueueLengthTask;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for common functionality for {@link IBigdataFederation}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBigdataFederation implements IBigdataFederation {

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

    protected AbstractBigdataFederation(IBigdataClient client) {

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
                    client.getThreadPoolSize(), DaemonThreadFactory
                            .defaultThreadFactory());

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
    
    public UUID registerIndex(IndexMetadata metadata) {

        assertOpen();

        return registerIndex(metadata, null);

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

                    log.error(ex);

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

            log.error(ex);

            throw new RuntimeException(ex);

        }

    }

    public void dropIndex(String name) {

        assertOpen();

        try {
            
            getMetadataService().dropScaleOutIndex(name);
            
        } catch (Exception e) {

            throw new RuntimeException( e );
            
        }

    }

    public IIndex getIndex(String name,long timestamp) {

        assertOpen();

        final MetadataIndexMetadata mdmd = getMetadataIndexMetadata(name, timestamp);
        
        // No such index.
        if (mdmd == null)
            return null;

        // Index exists.
        return new ClientIndexView(this, name, timestamp, mdmd);

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
    
}
