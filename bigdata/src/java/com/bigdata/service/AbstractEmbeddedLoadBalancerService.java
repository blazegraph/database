package com.bigdata.service;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

/**
 * Embedded {@link LoadBalancerService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractEmbeddedLoadBalancerService extends LoadBalancerService {

//    final private UUID serviceUUID;
    final private String hostname;
    
    public AbstractEmbeddedLoadBalancerService(UUID serviceUUID,
            Properties properties) {
        
        super( properties );
        
//        if (serviceUUID == null)
//            throw new IllegalArgumentException();

//        this.serviceUUID = serviceUUID;
        
        setServiceUUID(serviceUUID);
        
        String hostname;
        try {
            
            hostname = Inet4Address.getLocalHost().getCanonicalHostName();
            
        } catch (UnknownHostException e) {
            
            hostname = "localhost";
            
        }
        this.hostname = hostname;

    }

//    @Override
//    public AbstractFederation getFederation() {
//
//        return server.getClient().getFederation();
//        
//    }
    
//    public UUID getServiceUUID() {
//        
//        return serviceUUID;
//        
//    }

    protected String getClientHostname() {

        return hostname;
        
    }
    
}