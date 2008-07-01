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
 * Created on Apr 1, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.util.InnerCause;
import com.bigdata.util.NT;

/**
 * Integration provides a view of a local {@link DataService} as if it were a
 * federation. The {@link LocalDataServiceFederation} runs its own embedded
 * {@link TimestampService} and {@link LoadBalancerService} to support its
 * embedded {@link DataService}.
 * 
 * @see LocalDataServiceClient
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LocalDataServiceFederation extends AbstractFederation {

    private TimestampService timestampService;
    private LoadBalancerService loadBalancerService;
    private LocalDataServiceImpl dataService;
    
    /**
     * 
     */
    public LocalDataServiceFederation(LocalDataServiceClient client) {
        
        super(client);

        final Properties properties = client.getProperties();
        
        timestampService = new EmbeddedTimestampService(UUID.randomUUID(),
                properties);
        
        /*
         * Note: This will expose the counters for the local data service.
         */
        loadBalancerService = new EmbeddedLoadBalancerService(
                UUID.randomUUID(), properties);
        
        /*
         * Note: The embedded data service does not support scale-out indices.
         * Use an embedded or distributed federation for that.
         * 
         * @todo the UUID of the data service might be best persisted with the
         * data service in case anything comes to rely on it, but as far as I
         * can tell nothing does or should.
         */

        // Disable overflow.
        properties.setProperty(Options.OVERFLOW_ENABLED,"false");
        
        // create the embedded data service.
        dataService = new LocalDataServiceImpl(properties).start();

        // notify join.
        loadBalancerService.join(dataService.getServiceUUID(),
                AbstractStatisticsCollector.fullyQualifiedHostName);
        
    }
    
    /**
     * The embedded {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class LocalDataServiceImpl extends AbstractEmbeddedDataService {
        
        LocalDataServiceImpl(Properties properties) {
            
            super(UUID.randomUUID(), properties);
            
        }

        @Override
        public LocalDataServiceFederation getFederation() {

            return LocalDataServiceFederation.this;
            
        }
        
        @Override
        public LocalDataServiceImpl start() {
            
            return (LocalDataServiceImpl) super.start();
            
        }
        
    }

    /**
     * Extended for type-safe return.
     */
    public LocalDataServiceClient getClient() {

        return (LocalDataServiceClient) super.getClient();
        
    }
    
    /**
     * Returns an array containing one element - the {@link UUID} of the local
     * {@link IDataService}.
     */
    public UUID[] getDataServiceUUIDs(int maxCount) {
        
        assertOpen();
        
        return new UUID[]{dataService.getServiceUUID()};
        
    }
    
    /**
     * Returns the local {@link IDataService}.
     */
    public LocalDataServiceImpl getAnyDataService() {

        assertOpen();
        
        return dataService;
        
    }

    /**
     * Return the local {@link DataService}.
     */
    public LocalDataServiceImpl getDataService() {
        
        assertOpen();
        
        return dataService;
        
    }

    /**
     * Return the {@link UUID} that identifies the local {@link IDataService}.
     */
    public UUID getDataServiceUUID() {

        assertOpen();
        
        return dataService.getServiceUUID();
        
    }
    
    /**
     * Registers an index that does not support scale-out.
     */
    public void registerIndex(IndexMetadata metadata) {
        
        assertOpen();
        
        try {

            registerIndex(metadata,getDataServiceUUID());
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    /**
     * Registers an index that does not support scale-out.
     */
    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {
        
        assertOpen();

        try {

            dataService.registerIndex(metadata.getName(), metadata);
            
        } catch (Exception e) {
            
            throw new RuntimeException(e);
            
        }
        
        return metadata.getIndexUUID();
    }

    /**
     * Since key-range partitioned indices are not supported this method will
     * log a warning and register the index on the local {@link IDataService} as
     * an unpartitioned index.
     */
    public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs) {

        log.warn("key-range partitioned indices not supported: "+metadata.getName());
        
        registerIndex(metadata);
        
        return metadata.getIndexUUID();
        
    }

    public void dropIndex(String name) {

        assertOpen();

        try {
            
            dataService.dropIndex(name);
            
            dropIndexFromCache(name);
            
        } catch (Exception e) {

            throw new RuntimeException(e);
            
        }
        
    }

    synchronized public IIndex getIndex(String name, long timestamp) {

        assertOpen();

        final NT nt = new NT(name, timestamp);

        // check the cache.
        IClientIndex ndx = indexCache.get(nt);

        if (ndx == null) {

            try {

                // test for existence.
                dataService.getIndexMetadata(name, timestamp);

            } catch (Exception ex) {

                if (InnerCause.isInnerCause(ex, NoSuchIndexException.class)) {

                    return null;

                }

                throw new RuntimeException(ex);
                
            }
            
            // exists, so create view.
            ndx = new DataServiceIndex( name, timestamp, dataService );
            
            // put view in the cache.
            indexCache.put(nt,ndx,false/*dirty*/);

        }
        
        // return view.
        return ndx;
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always since the metadata index is not used.
     */
    public IMetadataService getMetadataService() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always
     */
    public IMetadataIndex getMetadataIndex(String name, long timestamp) {

        throw new UnsupportedOperationException();
        
    }

    public ILoadBalancerService getLoadBalancerService() {

        assertOpen();

        return loadBalancerService;
        
    }
    
    public ITimestampService getTimestampService() {

        assertOpen();

        return timestampService;
        
    }

    /**
     * Returns the embedded data service IFF the given serviceUUID is
     * the UUID for the embedded data service and <code>null</code>
     * otherwise.
     */
    public IDataService getDataService(UUID serviceUUID) {

        assertOpen();
        
        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        if (getDataServiceUUID().equals(serviceUUID)) {
            
            return dataService;
            
        }

        return null; 
        
    }

    /**
     * Return <code>false</code>.
     */
    public boolean isScaleOut() {
        
        return false;
        
    }
    
    /**
     * Return <code>false</code>.
     */
    public boolean isDistributed() {
        
        return false;
        
    }
    
    /**
     * Extended to shutdown the embedded services.
     */
    synchronized public void shutdown() {
        
        super.shutdown();
        
        if (dataService != null) {

            dataService.shutdown();

            dataService = null;

        }

        if (loadBalancerService != null) {

            loadBalancerService.shutdown();

            loadBalancerService = null;

        }

        if (timestampService != null) {

            timestampService.shutdown();

            timestampService = null;

        }

    }
    
    /**
     * Extended to shutdown the embedded services.
     */
    synchronized public void shutdownNow() {

        super.shutdownNow();

        if (dataService != null) {

            dataService.shutdownNow();

            dataService = null;

        }

        if (loadBalancerService != null) {

            loadBalancerService.shutdownNow();

            loadBalancerService = null;

        }

        if (timestampService != null) {

            timestampService.shutdownNow();

            timestampService = null;

        }

    }

    /**
     * Destroys the embedded services and disconnects from the federation.
     */
    public void destroy() {

        assertOpen();

        try {
            
            dataService.destroy();

            dataService = null;
            
        } catch (IOException e) {

            throw new RuntimeException(e);
            
        }
        
        if (loadBalancerService != null) {

            loadBalancerService.shutdownNow();

            loadBalancerService = null;

        }

        if(timestampService!=null) {

            timestampService.shutdownNow();
        
            timestampService = null;
            
        }
     
    }
    
}
