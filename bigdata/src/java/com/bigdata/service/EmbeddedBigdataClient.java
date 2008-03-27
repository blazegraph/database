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

/**
 * A client for an embedded federation (the client and the data services all run
 * in the same process).
 * 
 * @see EmbeddedBigdataFederation
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmbeddedBigdataClient extends AbstractBigdataClient {

    /**
     * 
     * @param properties
     *            See {@link EmbeddedBigdataFederation.Options}.
     */
    public EmbeddedBigdataClient(Properties properties) {

        super(properties);
        
    }
    
    public IBigdataFederation connect() {

        if (fed == null) {

            fed = new EmbeddedBigdataFederation(this, properties);

        }

        return fed;

    }

    public void shutdown() {

        super.shutdown();

        if(fed != null) {
            
            // normal shutdown of the embedded federation as well.
            ((EmbeddedBigdataFederation)fed).shutdown();
        
            // clear reference now that the client is disconnected.
            fed = null;
            
        }
        
    }

    public void shutdownNow() {

        super.shutdownNow();
        
        if(fed != null) {

            // immediate shutdown of the embedded federation as well.
            ((EmbeddedBigdataFederation)fed).shutdownNow();
        
            fed = null;
            
        }
        
    }

    /**
     * Returns UUIDs for embedded {@link IDataService}s.
     */
    public UUID[] getDataServiceUUIDs(int maxCount) {

        assertConnected();
        
        final EmbeddedBigdataFederation fed = ((EmbeddedBigdataFederation)this.fed);
        
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

    public IDataService getDataService(UUID serviceUUID) {

        assertConnected();

        final EmbeddedBigdataFederation fed = ((EmbeddedBigdataFederation)this.fed);

        return fed.getDataService(serviceUUID);
        
    }

    public IMetadataService getMetadataService() {

        assertConnected();
        
        return fed.getMetadataService();
        
    }

    public ILoadBalancerService getLoadBalancerService() {
        
        assertConnected();
        
        return fed.getLoadBalancerService();
        
    }

    public IDataService getAnyDataService() {
        
        assertConnected();
        
        final EmbeddedBigdataFederation fed = ((EmbeddedBigdataFederation)this.fed);

        return fed.getDataService(0);
        
    }

}
