package com.bigdata.jini.start;

import java.util.UUID;

import net.jini.entry.AbstractEntry;

/**
 * An attribute giving the unique service token assigned by the process
 * which started the service. This token is used to recognize the service
 * instance when it starts.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see ServiceTokenFilter
 */
public class ServiceToken extends AbstractEntry {

    /**
     * 
     */
    private static final long serialVersionUID = -130803260479385799L;

    public UUID serviceToken;
    
    /**
     * De-serializator ctor.
     */
    public ServiceToken() {
        
    }
    
    public ServiceToken(final UUID serviceToken) {
        
        if (serviceToken == null)
            throw new IllegalArgumentException();
        
        this.serviceToken = serviceToken;
        
    }
    
}