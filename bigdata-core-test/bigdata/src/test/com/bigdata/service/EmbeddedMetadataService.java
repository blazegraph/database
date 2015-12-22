package com.bigdata.service;

import java.util.Properties;
import java.util.UUID;

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

    @Override
    public EmbeddedFederation getFederation() {

        return federation;
        
    }
    
}
