package com.bigdata.jini.start.config;

import java.net.InetAddress;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.LookupCache;

import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.jini.lookup.entry.HostnameFilter;
import com.bigdata.jini.lookup.entry.ServiceItemFilterChain;
import com.bigdata.service.IClientService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.config.NicUtil;

/**
 * Constraint on the #of {@link IClientService}s on the same host.
 */
public class MaxClientServicesPerHostConstraint extends
        MaxServicesPerHostConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 5440342559965030393L;

    /**
     * @param className
     * @param maxServices
     * @param timeout
     */
    public MaxClientServicesPerHostConstraint(int maxServices) {

        super(maxServices);

    }

    public boolean allow(final JiniFederation fed) throws Exception {

        if (fed == null) // required
            throw new IllegalArgumentException();

        final LookupCache lookupCache = fed.getClientServicesClient()
                .getLookupCache();

        final ServiceItemFilterChain filter = new ServiceItemFilterChain();

//        /*
//		 * consider only client services
//		 * 
//		 * This is not be necessary since we are using the lookup cache for the
//		 * client services.
//		 */
//        filter.add(ClientServiceFilter.INSTANCE);

        final String hostname = NicUtil.getIpAddress("default.nic", "default", false);
        final String canonicalHostname = hostname;

        // filters for _this_ host.
        filter.add(new HostnameFilter(new Hostname[] {//
                new Hostname(hostname),//
                new Hostname(canonicalHostname) //
                }));

        final ServiceItem[] serviceItems = lookupCache.lookup(filter,
                maxServices);

        final boolean allowed = serviceItems.length < maxServices;

             if (log.isInfoEnabled())
                log.info("New instance: allowed=" + allowed + ", maxServices="
                        + maxServices + ", #found=" + serviceItems.length
                        + ", host=" + canonicalHostname);

        return allowed;

    }

}
