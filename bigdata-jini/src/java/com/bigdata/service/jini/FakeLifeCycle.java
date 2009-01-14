package com.bigdata.service.jini;

import com.sun.jini.start.LifeCycle;

/**
 * A NOP implementation used when the service is not started by activation
 * (eg, when the service is run from a command line).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public final class FakeLifeCycle implements LifeCycle {

    public FakeLifeCycle() {
        
    }
    
    public boolean unregister(final Object arg0) {
        
        if (AbstractServer.INFO)
            AbstractServer.log.info("");
        
        return true;
        
    }
    
}