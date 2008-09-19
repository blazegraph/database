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

package com.bigdata.service.jini;

import java.io.IOException;
import java.rmi.RemoteException;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryManagement;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import org.apache.log4j.Logger;

import com.bigdata.journal.IResourceLockService;

/**
 * Class handles discovery of an {@link IResourceLockService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ResourceLockClient {

    protected static final transient Logger log = Logger
            .getLogger(ResourceLockClient.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
    private ServiceDiscoveryManager serviceDiscoveryManager = null;

    private LookupCache serviceLookupCache = null;
    
    private ServiceTemplate template = null;

    /**
     * Timeout for remote lookup on cache miss (milliseconds).
     */
    private final long timeout = 1000;
    
    /**
     * Provides direct cached lookup of {@link IResourceLockService}s by their
     * {@link ServiceID}.
     */
    private final ServiceCache serviceMap;

    /**
     * Begins discovery for the {@link IResourceLockService} service.
     * 
     * @param discoveryManagement
     * @param listener
     *            Optional listener will see {@link ServiceDiscoveryEvent}s.
     */
    public ResourceLockClient(DiscoveryManagement discoveryManagement,
            ServiceDiscoveryListener listener) {

        serviceMap = new ServiceCache(listener);
        
        /*
         * Setup a helper class that will be notified as services join or leave
         * the various registrars to which the client is listening.
         */
        try {

            serviceDiscoveryManager = new ServiceDiscoveryManager(discoveryManagement,
                    new LeaseRenewalManager());
            
        } catch(IOException ex) {
            
            throw new RuntimeException(
                    "Could not initiate service discovery manager", ex);
            
        }

        /*
         * Setup a LookupCache that will be populated with all services that
         * match a filter. This is used to keep track of all services registered
         * with any service registrar to which the client is listening.
         */
        try {
            
            template = new ServiceTemplate(null,
                    new Class[] { IResourceLockService.class }, null);

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
     * Return the {@link IResourceLockService} service from the cache -or-
     * <code>null</code> if there is no such service in the cache and a remote
     * lookup times out.
     * 
     * @todo handle multiple service instances for failover but always designate
     *       a primary.
     */
    public IResourceLockService getResourceLockService() {

        ServiceItem item = serviceLookupCache.lookup(null);

        if (item == null) {

            if(INFO)
                log.info("Cache miss.");

            item = handleCacheMiss(null/* filter */);
                        
            if (item == null) {

                log.warn("No matching service.");

                return null;

            }
            
        }
        
        return (IResourceLockService) item.service;

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

            if(INFO)
                log.info("Interrupted - no match.");

            return null;

        }

        if (item == null) {

            // Could not discover a matching service.

            log.warn("Could not discover matching service");

            return null;

        }

        if (INFO)
            log.info("Found: " + item);

        return item;

    }

}
