package com.bigdata.jini.lookup.entry;

import java.util.LinkedList;
import java.util.List;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.ServiceItemFilter;

/**
 * A chain of filters applied in the order in which they were added.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServiceItemFilterChain implements ServiceItemFilter {

    private final List<ServiceItemFilter> chain = new LinkedList<ServiceItemFilter>();

    public ServiceItemFilterChain() {

    }

    public void add(final ServiceItemFilter f) {

        if (f == null)
            throw new IllegalArgumentException();

        chain.add(f);

    }

    public boolean check(final ServiceItem serviceItem) {

        for (ServiceItemFilter f : chain) {

            if (!f.check(serviceItem))
                return false;

        }

        return true;

    }

}
