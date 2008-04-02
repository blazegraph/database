package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.journal.IResourceManager;

/**
 * A local (in process) data service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class EmbeddedDataService extends DataService {
    
    final private UUID serviceUUID;
    
    public EmbeddedDataService(UUID serviceUUID, Properties properties) {
        
        super(properties);
        
        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        this.serviceUUID = serviceUUID;
        
        log.info("uuid="+serviceUUID);
        
    }

    public UUID getServiceUUID() throws IOException {

        return serviceUUID;
        
    }
 
    public void destroy() throws IOException {

        log.info("");
        
        IResourceManager resourceManager = getResourceManager();

        shutdownNow();
        
        // destroy all resources.
        resourceManager.deleteResources();
        
    }
    
}
