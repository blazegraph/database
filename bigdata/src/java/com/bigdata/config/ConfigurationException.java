package com.bigdata.config;

/**
 * Instance thrown if there is a problem with a property value. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConfigurationException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 55363418935577963L;

    public ConfigurationException(String key, String val, String msg) {
        
        super(msg + ": " + key + "=" + val);
        
    }
    
}