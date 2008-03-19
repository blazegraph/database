/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Mar 18, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.rmi.RemoteException;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryManagement;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import org.apache.log4j.Logger;

/**
 * Class handles discovery of an {@link ILoadBalancerService}.  Clients are responsible
 * for generating notification events.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadBalancerClient {

    public static final transient Logger log = Logger
            .getLogger(LoadBalancerClient.class);

    private ServiceDiscoveryManager serviceDiscoveryManager = null;

    private LookupCache serviceLookupCache = null;
    
    private ServiceTemplate template = null;

    /**
     * Timeout for remote lookup on cache miss (milliseconds).
     */
    private final long timeout = 1000;
    
    /**
     * Provides direct cached lookup of {@link LoadBalancerService}s by their
     * {@link ServiceID}.
     */
    public ServiceCache serviceMap = new ServiceCache();

    /**
     * Begins discovery for the {@link ILoadBalancerService} service.
     * 
     * @param discoveryManagement
     */
    public LoadBalancerClient(DiscoveryManagement discoveryManagement) {

        /*
         * Setup a helper class that will be notified as services join or leave
         * the various registrars to which the data server is listening.
         */
        try {

            serviceDiscoveryManager = new ServiceDiscoveryManager(discoveryManagement,
                    new LeaseRenewalManager());
            
        } catch(IOException ex) {
            
            throw new RuntimeException(
                    "Could not initiate service discovery manager", ex);
            
        }

        /*
         * Setup a LookupCache that will be populated with all services that match a
         * filter. This is used to keep track of all metadata services registered
         * with any service registrar to which the data server is listening.
         */
        try {
            
            template = new ServiceTemplate(null,
                    new Class[] { ILoadBalancerService.class }, null);

            serviceLookupCache = serviceDiscoveryManager.createLookupCache(
                    template, null /*new LoadBalancerFilter()*/ /* filter */,
                    serviceMap/* ServiceDiscoveryListener */);

        } catch (RemoteException ex) {
            
            throw new RuntimeException("Could not setup LookupCache", ex);
            
        }

    }
    
    protected LookupCache getServiceLookupCache() {
        
        return serviceLookupCache;
        
    }
    
    protected void terminate() {
        
        serviceLookupCache.terminate();
        
        serviceDiscoveryManager.terminate();

    }

    /**
     * Return the {@link ILoadBalancerService} service from the cache -or-
     * <code>null</code> if there is no such service in the cache and a remote
     * lookup times out.
     */
    public ILoadBalancerService getLoadBalancerService() {

        ServiceItem item = serviceLookupCache.lookup(null);

        if (item == null) {

            log.info("Cache miss.");

            item = handleCacheMiss(null/*filter*/);
                        
            if (item == null) {

                log.warn("No matching service.");

                return null;

            }
            
        }
        
        return (ILoadBalancerService) item.service;

    }

    /**
     * Handles a cache miss by a remote query on the managed set of service
     * registrars.
     */
    protected ServiceItem handleCacheMiss(ServiceItemFilter filter) {

        ServiceItem item = null;

        try {

            item = serviceDiscoveryManager.lookup(template, filter, timeout);

        } catch (RemoteException ex) {

            log.error(ex);

            return null;

        } catch (InterruptedException ex) {

            log.info("Interrupted - no match.");

            return null;

        }

        if (item == null) {

            // Could not discover a matching service.

            log.warn("Could not discover matching service");

            return null;

        }

        log.info("Found: " + item);

        return item;

    }

//    /**
//     * Filter only matches a service item where {@link ILoadBalancer} is
//     * implemented.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class LoadBalancerFilter implements ServiceItemFilter {
//
//        public static final transient Logger log = Logger
//                .getLogger(LoadBalancerFilter.class);
//
//        public static final transient ServiceItemFilter INSTANCE = new LoadBalancerFilter();
//        
//        public boolean check(ServiceItem item) {
//
//            if(item.service==null) {
//                
//                log.warn("Service is null: "+item);
//
//                return false;
//                
//            }
//            
//            if (item.service instanceof ILoadBalancer) {
//               
//                log.info("Matched: "+item);
//                
//                return true;
//                
//            }
//
//            log.debug("Ignoring: "+item);
//            
//            return false;
//            
//        }
//        
//    }
    
}
