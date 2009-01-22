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

import java.rmi.RemoteException;
import java.util.UUID;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.ServiceItemFilter;
import net.jini.lookup.entry.Name;

import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;

/**
 * Class handles discovery, caching, and local lookup of {@link IDataService}s
 * and {@link IMetadataService}s.
 * <p>
 * Note: Since {@link IMetadataService} extends {@link IDataService} this class
 * uses {@link ServiceItemFilter}s as necessary to exclude {@link IDataService}s
 * or {@link IMetadataService}s from responses.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServicesClient extends
        AbstractCachingServiceClient<IDataService> {

    /**
     * {@inheritDoc}
     */
    public DataServicesClient(final JiniFederation fed, final long timeout)
            throws RemoteException {

        /*
         * Note: No filter is imposed here. Instead there are type specific
         * methods if you want an IDataService vs an IMetadataService.
         */
        super(fed, IDataService.class, new ServiceTemplate(null,
                new Class[] { IDataService.class }, null), null/* filter */,
                timeout);

    }

    /**
     * Return an arbitrary {@link IDataService} instance from the cache -or-
     * <code>null</code> if there is none in the cache and a remote lookup
     * times out. This method will NOT return an {@link IMetadataService}.
     * 
     * @return The service.
     */
    final public IDataService getDataService() {

        return getService(DataServiceFilter.INSTANCE);
        
    }

    /**
     * Return an arbitrary {@link IMetadataService} from the cache -or-
     * <code>null</code> if there is none in the cache and a remote lookup
     * times out. This method will NOT return an {@link IDataService} unless it
     * also implements {@link IMetadataService}.
     * 
     * @todo handle more than one metadata service. right now registering more
     *       than one will cause problems since different clients might discover
     *       different metadata services and the metadata services are not
     *       arranging themselves into a failover chain or a hash partitioned
     *       service.
     */
    final public IMetadataService getMetadataService() {

        return (IMetadataService) getService(MetadataServiceFilter.INSTANCE);

    }

    /**
     * Return the proxy for an {@link IDataService} from the local cache.
     * 
     * @param serviceUUID
     *            The {@link UUID} for the {@link IDataService}.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known {@link IDataService}.
     * 
     * @throws IllegalArgumentException
     *             if <i>serviceUUID</i> is <code>null</code>.
     * @throws RuntimeException
     *             if <i>serviceUUID</i> identifies an {@link IMetadataService}.
     */
    public IDataService getDataService(final UUID serviceUUID) {

        final ServiceItem serviceItem = getServiceItem(serviceUUID);
        
        if (serviceItem == null) {

            log.warn("No such service: uuid=" + serviceUUID);

            return null;

        }

        if (!DataServiceFilter.INSTANCE.check(serviceItem)) {

            throw new RuntimeException("Not a data service: " + serviceItem);

        }
        
        // return the data service.
        return (IDataService) serviceItem.service;
        
    }
    
    /**
     * Return the proxy for an {@link IMetadataService} from the local cache.
     * 
     * @param serviceUUID
     *            The {@link UUID} for the {@link IMetadataService}.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known {@link IMetadataService}.
     * 
     * @throws IllegalArgumentException
     *             if <i>serviceUUID</i> is <code>null</code>.
     * @throws RuntimeException
     *             if <i>serviceUUID</i> identifies a {@link IDataService}.
     */
    public IMetadataService getMetadataService(final UUID serviceUUID) {

        final ServiceItem serviceItem = getServiceItem(serviceUUID);
        
        if (serviceItem == null) {

            log.warn("No such service: uuid=" + serviceUUID);

            return null;

        }

        if (!MetadataServiceFilter.INSTANCE.check(serviceItem)) {

            throw new RuntimeException("Not a metadata service: " + serviceItem);

        }

        // return the metadata service.
        return (IMetadataService) serviceItem.service;

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
    public UUID[] getDataServiceUUIDs(final int maxCount) {

        final ServiceItem[] items = serviceCache.getServiceItems(maxCount,
                DataServiceFilter.INSTANCE);

        if (INFO)
            log.info("There are at least " + items.length
                    + " data services : maxCount=" + maxCount);

        final UUID[] uuids = new UUID[items.length];

        for (int i = 0; i < items.length; i++) {

            uuids[i] = JiniUtil.serviceID2UUID(items[i].serviceID);

        }

        return uuids;

    }

    /**
     * Return an arbitrary {@link IDataService} having the specified service
     * name on an {@link Entry} for that service.
     * 
     * @param name
     *            The service name.
     * 
     * @return The {@link IDataService} -or- <code>null</code> if there is
     *         none in the cache and a remote lookup times out.
     * 
     * @todo refactor into the base class but keep semantics of only matching
     *       data services (vs metadata services) in this class.
     */
    public IDataService getDataServiceByName(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        return getService(new DataServiceFilter() {

            public boolean check(final ServiceItem item) {

                if (super.check(item)) {

                    for (Entry e : item.attributeSets) {

                        if (e instanceof Name) {

                            if (((Name) e).name.equals(name)) {

                                return true;

                            }

                        }

                    }
                    
                }

                return false;
                
            }
            
        });
        
    }
    
}
