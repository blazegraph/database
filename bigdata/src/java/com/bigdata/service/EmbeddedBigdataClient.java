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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A client for an embedded federation (the client and the data services all run
 * in the same process).
 * 
 * @see EmbeddedBigdataFederation
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmbeddedBigdataClient implements IBigdataClient {

    protected final Properties properties;

    /*
     * IBigdataClient state.
     */
    private final ExecutorService threadPool;
    private final int defaultRangeQueryCapacity;
    private final boolean batchApiOnly;

    /*
     * IBigdataClient API.
     */

    public ExecutorService getThreadPool() {
        
        assertConnected();
        
        return threadPool;
        
    }

    public int getDefaultRangeQueryCapacity() {
        
        return defaultRangeQueryCapacity;
        
    }
    
    public boolean getBatchApiOnly() {
        
        return batchApiOnly;
        
    }
    
    protected void assertConnected() {
        
        if (fed == null)
            throw new IllegalStateException("Not connected");
        
    }
    
    /**
     * 
     * @param properties See {@link EmbeddedBigdataFederation.Options}.
     */
    public EmbeddedBigdataClient(Properties properties) {
        
        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties;
        
        final int nthreads = Integer.parseInt(properties.getProperty(
                Options.CLIENT_THREAD_POOL_SIZE,
                Options.DEFAULT_CLIENT_THREAD_POOL_SIZE));
        
        threadPool = Executors.newFixedThreadPool(nthreads, DaemonThreadFactory
                .defaultThreadFactory());

        defaultRangeQueryCapacity = Integer.parseInt(properties.getProperty(
                Options.CLIENT_RANGE_QUERY_CAPACITY,
                Options.DEFAULT_CLIENT_RANGE_QUERY_CAPACITY));
        
        batchApiOnly = Boolean.valueOf(properties.getProperty(
                Options.CLIENT_BATCH_API_ONLY,
                Options.DEFAULT_CLIENT_BATCH_API_ONLY));

    }
    
    public IBigdataFederation connect() {

            if (fed == null) {

            fed = new EmbeddedBigdataFederation(this, properties);

        }

        return fed;

    }

    private EmbeddedBigdataFederation fed = null;

    public void shutdown() {

        log.info("");

        if(fed != null) {

            // allow client requests to finish normally.
            threadPool.shutdown();
            
            try {
            
                if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    
                    log.warn("Timeout awaiting thread pool termination.");
                    
                }
   
            } catch (InterruptedException e) {
                
                log.warn("Interrupted awaiting thread pool termination.", e);
                
            }
            
            // normal shutdown of the embedded federation as well.
            fed.shutdown();
        
            fed = null;
            
        }
        
    }

    public void shutdownNow() {

        log.info("");
        
        if(fed != null) {

            // stop client requests.
            threadPool.shutdownNow();
            
            // immediate shutdown of the embedded federation as well.
            fed.shutdownNow();
        
            fed = null;
            
        }
        
    }

    /**
     * Returns UUIDs for embedded {@link IDataService}s.
     */
    public UUID[] getDataServiceUUIDs(int maxCount) {

        assertConnected();
        
        if (maxCount < 0)
            throw new IllegalArgumentException();
        
        final int n = maxCount == 0 ? fed.ndataServices : Math.min(maxCount,
                fed.ndataServices);
        
        final UUID[] uuids = new UUID[ n ];
        
        for(int i=0; i<n; i++) {
            
            try {
            
                uuids[i] = fed.getDataService( i ).getServiceUUID();
            
            } catch (IOException e) {
                
                throw new RuntimeException( e );
                
            }
            
        }
        
        return uuids;
        
    }

    /**
     * Return the (in process) data service.
     * 
     * @param serviceUUID
     *            The data service identifier.
     */
    public IDataService getDataService(UUID serviceUUID) {

        assertConnected();

        return fed.getDataService(serviceUUID);
        
    }

    /**
     * The (in process) metadata service.
     */
    public IMetadataService getMetadataService() {

        assertConnected();
        
        return fed.getMetadataService();
        
    }

}
