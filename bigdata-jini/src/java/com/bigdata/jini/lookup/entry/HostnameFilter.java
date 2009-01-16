package com.bigdata.jini.lookup.entry;

import java.net.InetAddress;
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
public class HostnameFilter implements ServiceItemFilter {

    final Hostname[] attr;

    public String toString() {

        return getClass().getName() + "{" + Arrays.toString(attr) + "}";

    }

    public HostnameFilter() throws UnknownHostException {

        final String hostname = InetAddress.getLocalHost().getHostName();

        final String canonicalHostname = InetAddress.getLocalHost()
                .getCanonicalHostName();

        attr = new Hostname[] {

        new Hostname(hostname),//
                new Hostname(canonicalHostname) //

        };

    }

    public boolean check(final ServiceItem serviceItem) {

        for (Entry e : serviceItem.attributeSets) {

            if (!(e instanceof Hostname)) {

                continue;

            }

            for (Hostname n : attr) {

                if (n.hostname.equals(((Hostname) e).hostname)) {

                    return true;

                }

            }

        }

        return false;

    }

}
