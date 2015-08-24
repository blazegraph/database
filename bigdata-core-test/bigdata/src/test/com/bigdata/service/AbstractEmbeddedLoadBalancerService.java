package com.bigdata.service;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.util.config.NicUtil;

/**
 * Embedded {@link LoadBalancerService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractEmbeddedLoadBalancerService extends LoadBalancerService {

//    final private UUID serviceUUID;
    private String hostname = NicUtil.getIpAddressByLocalHost();//for now, maintain the same failure logic as in constructor
    
    public AbstractEmbeddedLoadBalancerService(UUID serviceUUID,
            Properties properties) {
        
        super( properties );
        
//        if (serviceUUID == null)
//            throw new IllegalArgumentException();

//        this.serviceUUID = serviceUUID;
        
        setServiceUUID(serviceUUID);
        
        try {
            this.hostname = NicUtil.getIpAddress("default.nic", "default", false);
        } catch(Throwable t) {
            t.printStackTrace();
        }

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