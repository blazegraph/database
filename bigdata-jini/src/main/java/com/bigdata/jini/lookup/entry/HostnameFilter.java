package com.bigdata.jini.lookup.entry;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.Arrays;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.ServiceItemFilter;

/**
 * Filters for the host on which the filter is evaluated.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HostnameFilter implements ServiceItemFilter, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -973420571400694111L;
    
    final Hostname[] acceptHostname;

    public String toString() {

        return getClass().getName() + "{" + Arrays.toString(acceptHostname) + "}";

    }

    public HostnameFilter(final Hostname[] attr) throws UnknownHostException {

        if (attr == null)
            throw new IllegalArgumentException();
        
        for (Hostname a : attr) {

            if (a == null || a.hostname == null)
                throw new IllegalArgumentException();
            
        }
        
        this.acceptHostname = attr;
        
    }

    public boolean check(final ServiceItem serviceItem) {

        for (Entry e : serviceItem.attributeSets) {

            if (!(e instanceof Hostname)) {

                continue;

            }

            for (Hostname hostname : acceptHostname) {

                if (hostname.hostname.equals(((Hostname) e).hostname)) {

                    return true;

                }

            }

        }

        return false;

    }

}
