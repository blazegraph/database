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
import java.util.UUID;

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

import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.MetadataService;

/**
 * Class handles discovery, caching, and local lookup of {@link IDataService}s
 * and/or {@link IMetadataService}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServicesClient {

    protected static final transient Logger log = Logger
            .getLogger(DataServicesClient.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.isDebugEnabled();
    
    private ServiceDiscoveryManager serviceDiscoveryManager = null;

    private LookupCache serviceLookupCache = null;

    private ServiceTemplate template;

    /**
     * Timeout used when there is a cache miss (milliseconds).
     */
    final long timeout = 1000L;

    /**
     * Provides direct cached lookup {@link ServiceItem}s by {@link ServiceID}.
     */
    final ServiceCache serviceMap;

    /**
     * When <code>true</code> {@link ServiceItem}s for {@link DataService}s
     * will be cached.
     */
    private final boolean cacheDataServices;

    /**
     * When <code>true</code> {@link ServiceItem}s for
     * {@link MetadataDataService}s will be cached.
     */
    private final boolean cacheMetadataServices;

    /**
     * Begins discovery for {@link DataService}s and {@link MetadataService}s.
     * 
     * @param discoveryManagement
     * @para listener An optional listener that will see the
     *       {@link ServiceDiscoveryEvent}s observed by the internal
     *       {@link ServiceCache}.
     */
    public DataServicesClient(DiscoveryManagement discoveryManagement,
            ServiceDiscoveryListener listener) {

        this(discoveryManagement, listener, true, true);
        
    }
    
    /**
     * Begins discovery for {@link DataService}s and/or {@link MetadataService}s
     * as determined by the specified flags.
     * <p>
     * Note: Since {@link IMetadataService} extends {@link IDataService} we use
     * {@link ServiceItemFilter}s if necessary to exclude {@link DataService}s
     * or {@link MetadataService}s from the cache. If neither
     * {@link DataService}s nor {@link MetadataService}s have been excluded,
     * then the cache will contain both types of services. Regardless of the
     * cache state, {@link #getDataService()} and {@link #getMetadataService()}
     * will return only the matching service items and will throw an exception
     * if there is an attempt to request the {@link ServiceItem} for
     * {@link DataService} when those {@link ServiceItem}s are not being cached
     * or the {@link ServiceItem} for {@link MetadataService} when those
     * {@link ServiceItem}s are not being cached.
     * 
     * @param discoveryManagement
     * @param listener
     *            An optional listener that will see the
     *            {@link ServiceDiscoveryEvent}s observed by the internal
     *            {@link ServiceCache}.
     * @param cacheDataServices
     *            When <code>true</code> {@link ServiceItem}s for
     *            {@link DataService}s will be cached.
     * @param cacheMetadataServices
     *            When <code>true</code> {@link ServiceItem}s for
     *            {@link MetadataDataService}s will be cached.
     * 
     * @throws IllegalArgumentException
     *             if <i>discoveryManagment</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if neither data services nor metadata services are to be
     *             cached.
     */
    public DataServicesClient(//
            final DiscoveryManagement discoveryManagement,//
            final ServiceDiscoveryListener listener,//
            final boolean cacheDataServices, //
            final boolean cacheMetadataServices//
            ) {

        if (discoveryManagement == null)
            throw new IllegalArgumentException();

        serviceMap = new ServiceCache(listener);
        
        this.cacheDataServices = cacheDataServices;

        this.cacheMetadataServices = cacheMetadataServices;

        if (!cacheDataServices && !cacheMetadataServices) {
            
            throw new IllegalArgumentException("Neither data services nor metadata services are being cached?");
            
        }
        
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
         * Setup a LookupCache that will be populated with all services that
         * match a filter. This is used to keep track of services registered
         * with any service registrar to which the data server is listening.
         */
        try {
            
            template = new ServiceTemplate(null,
                    new Class[] { IDataService.class }, null);

            ServiceItemFilter filter = null;
            
            if(!cacheDataServices) {
                
                filter = DataServiceFilter.INSTANCE;
                
            } else if(!cacheMetadataServices) {
                
                filter = MetadataServiceFilter.INSTANCE;
                
            }
            
            serviceLookupCache = serviceDiscoveryManager.createLookupCache( //
                    template,  //
                    filter,    // MAY be null.
                    serviceMap // ServiceDiscoveryListener
                    );

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
     * Return an arbitrary {@link IDataService} instance from the cache -or-
     * <code>null</code> if there is none in the cache and a remote lookup
     * times out.
     * 
     * @throws UnsupportedOperationException
     *             if {@link DataService} discovery was disallowed by the
     *             constructor.
     */
    public IDataService getDataService() {

        if(!cacheDataServices) {
            
            throw new UnsupportedOperationException();
            
        }

        final ServiceItemFilter filter = DataServiceFilter.INSTANCE;
        
        ServiceItem item = serviceLookupCache.lookup(filter);

        if (item == null) {

            if(INFO)
                log.info("Cache miss.");

            item = handleCacheMiss(filter);
                        
            if (item == null) {

                log.warn("No matching service.");

                return null;

            }
            
        }

        return (IDataService) item.service;

    }

    /**
     * Return the {@link IMetadataService} from the cache -or- <code>null</code>
     * if there is none in the cache and a remote lookup times out.
     * 
     * @throws UnsupportedOperationException
     *             if {@link MetadataService} discovery was disallowed by the
     *             constructor.
     *             
     * @todo handle more than one metadata service. right now registering more
     *       than one will cause problems since different clients might discover
     *       different metadata services and the metadata services are not
     *       arranging themselves into a failover chain or a hash partitioned
     *       service.
     */
    public IMetadataService getMetadataService() {

        if(!cacheMetadataServices) {
            
            throw new UnsupportedOperationException();
            
        }

        final ServiceItemFilter filter = MetadataServiceFilter.INSTANCE;

        ServiceItem item = serviceLookupCache.lookup( filter );

        if (item == null) {

            if(INFO)
                log.info("Cache miss.");

            item = handleCacheMiss( filter );
            
            if (item == null) {

                log.warn("No matching service.");

                return null;

            }

        }
        
        return (IMetadataService) item.service;

    }

    /**
     * Return the proxy for an {@link IDataService} from the local cache.
     * 
     * @param serviceUUID
     *            The {@link UUID} for the {@link DataService}.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known {@link DataService}.
     * 
     * @throws IllegalArgumentException
     *             if <i>serviceUUID</i> is <code>null</code>.
     * @throws RuntimeException
     *             if <i>serviceUUID</i> identifies a {@link MetadataService}.
     * @throws UnsupportedOperationException
     *             if {@link DataService} discovery was disallowed by the
     *             constructor.
     */
    public IDataService getDataService(UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if(!cacheDataServices) {
            
            throw new UnsupportedOperationException();
            
        }

        final ServiceItem serviceItem = serviceMap.getServiceItemByID(JiniUtil
                .uuid2ServiceID(serviceUUID));

        if (serviceItem == null) {

            log.warn("No such service: uuid=" + serviceUUID);

            return null;

        }

        if(!DataServiceFilter.INSTANCE.check(serviceItem)) {
            
            throw new RuntimeException("Not a data service: "+serviceItem);
            
        }
        
        // return the data service.
        return (IDataService) serviceItem.service;
        
    }

    /**
     * Return the proxy for an {@link IMetadataService} from the local cache.
     * 
     * @param serviceUUID
     *            The {@link UUID} for the {@link MetadataService}.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known {@link MetadataService}.
     * 
     * @throws IllegalArgumentException
     *             if <i>serviceUUID</i> is <code>null</code>.
     * @throws RuntimeException
     *             if <i>serviceUUID</i> identifies a {@link DataService}.
     * @throws UnsupportedOperationException
     *             if {@link DataService} discovery was disallowed by the
     *             constructor.
     */
    public IMetadataService getMetadataService(UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if(!cacheMetadataServices) {
            
            throw new UnsupportedOperationException();
            
        }

        final ServiceItem serviceItem = serviceMap.getServiceItemByID(JiniUtil
                .uuid2ServiceID(serviceUUID));

        if (serviceItem == null) {

            log.warn("No such service: uuid=" + serviceUUID);

            return null;

        }

        if(!MetadataServiceFilter.INSTANCE.check(serviceItem)) {
            
            throw new RuntimeException("Not a metadata service: "+serviceItem);
            
        }

        // return the metadata service.
        return (IMetadataService) serviceItem.service;
        
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

        if(INFO)
        log.info("Found: " + item);

        return item;

    }

    /**
     * Return an array {@link UUID}s for {@link IDataService}s.
     * 
     * @param maxCount
     *            The maximum #of data services whose {@link UUID} will be
     *            returned. When zero (0) the {@link UUID} for all known data
     *            services will be returned.
     * 
     * @return An array of {@link UUID}s for data services.
     */
    public UUID[] getDataServiceUUIDs(int maxCount) {

        final ServiceItem[] items = serviceMap.getServiceItems(maxCount,
                DataServiceFilter.INSTANCE);

        if (INFO)
            log.info("There are at least " + items.length
                    + " data services : maxCount=" + maxCount);

        UUID[] uuids = new UUID[items.length];

        for (int i = 0; i < items.length; i++) {

            uuids[i] = JiniUtil.serviceID2UUID(items[i].serviceID);

        }

        return uuids;

    }
    
}
