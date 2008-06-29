package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.journal.IResourceManager;

/**
 * A local (in process) metadata service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmbeddedMetadataService extends MetadataService {

    private EmbeddedFederation federation;
    
    public EmbeddedMetadataService(EmbeddedFederation federation,
            UUID serviceUUID, Properties properties) {
        
        super(properties);
    
        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        if (federation == null)
            throw new IllegalArgumentException();
        
        this.federation = federation;

        setServiceUUID(serviceUUID);
        
    }

//    @Override
//    public IDataService getDataService(UUID dataService) {
//
//        return federation.getDataService(dataService);
//        
//    }
//
//    @Override
//    public IMetadataService getMetadataService() {
//
//        return this;
//        
//    }
//    
//    public ITimestampService getTimestampService() {
//        
//        return federation.getTimestampService();
//        
//    }
//    
//    @Override
//    public ILoadBalancerService getLoadBalancerService() {
//        
//        return federation.getLoadBalancerService();
//        
//    }

    @Override
    public EmbeddedFederation getFederation() {

        return federation;
        
    }

    public void destroy() throws IOException {

        log.info("");
        
        IResourceManager resourceManager = getResourceManager();

        shutdownNow();
        
        // destroy all resources.
        resourceManager.deleteResources();
        
    }

}