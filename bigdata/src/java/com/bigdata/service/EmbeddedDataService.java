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
    
    public EmbeddedDataService(UUID serviceUUID, Properties properties) {
        
        super(properties);

        setServiceUUID(serviceUUID);
        
    }

    public void destroy() throws IOException {

        log.info("");
        
        IResourceManager resourceManager = getResourceManager();

        shutdownNow();
        
        // destroy all resources.
        resourceManager.deleteResources();
        
    }
    
}
