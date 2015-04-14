package com.bigdata.jini.lookup.entry;


import net.jini.core.entry.Entry;
import net.jini.entry.AbstractEntry;

/**
 * {@link Entry} giving the name of the host on which the service was created.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo could be hostname[]
 */
public class Hostname extends AbstractEntry {

    /**
     * 
     */
    private static final long serialVersionUID = 2002927830828788312L;
    
    public String hostname;
    
    /**
     * De-serialization ctor.
     */
    public Hostname() {
        
    }
    
    public Hostname(final String hostname) {
        
        this.hostname = hostname;
        
    }

}