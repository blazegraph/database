/*

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.journal.jini.ha;

import java.rmi.RemoteException;
import java.util.UUID;

import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;

import com.bigdata.ha.HAGlue;
import com.bigdata.service.jini.lookup.AbstractCachingServiceClient;

/**
 * Helper class for discovery management of {@link HAGlue} services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HAGlueServicesClient extends
        AbstractCachingServiceClient<HAGlue> {

    /**
     * Sets up service discovery for the {@link HAGlue} services.
     * 
     * @param serviceDiscoveryManager
     *            Used to discovery services matching the template and filter.
     * @param serviceDiscoveryListener
     *            Service discovery notices are delivered to this class
     *            (optional).
     * @param cacheMissTimeout
     *            The timeout in milliseconds that the client will await the
     *            discovery of a service if there is a cache miss.
     * 
     * @throws RemoteException
     *             if we could not setup the {@link LookupCache}
     */
    public HAGlueServicesClient(
            final ServiceDiscoveryManager serviceDiscoveryManager,
            final ServiceDiscoveryListener serviceDiscoveryListener,
            final long cacheMissTimeout)
            throws RemoteException {

        super(serviceDiscoveryManager, serviceDiscoveryListener,
                HAGlue.class, new ServiceTemplate(null,
                        new Class[] { HAGlue.class }, null),
                null/* filter */, cacheMissTimeout);

    }

    /**
     * Return the proxy for an {@link HAGlue} service from the local cache -or-
     * the reference to this service if the {@link UUID} identifies this service
     * (this avoids RMI requests from a service to itself).
     * 
     * @param serviceUUID
     *            The {@link UUID} for the {@link HAGlue} service.
     * 
     * @return The proxy or <code>null</code> if the {@link UUID} does not
     *         identify a known {@link HAGlue} service.
     * 
     * @throws IllegalArgumentException
     *             if <i>serviceUUID</i> is <code>null</code>.
     */
    public HAGlue getService(final UUID serviceUUID) {

        /*
         * Note: I have backed out this optimization as it raises concerns that
         * code written to assume RMI might rely on the deserialized objects
         * returned from the proxy being independent of the objects on the
         * remote service. Since the main optimization of interest is joins, I
         * will handle this explicitly from within the distributed join logic.
         */
//        if (serviceUUID.equals(thisServiceUUID)) {
//
//            /*
//             * Return the actual service reference rather than a proxy to avoid
//             * RMI when this service makes a request to itself.
//             */
//
//            return (IDataService) thisService;
//
//        }
        
        final ServiceItem serviceItem = getServiceItem(serviceUUID);
        
        if (serviceItem == null) {

            log.error("No such service: uuid=" + serviceUUID);

            return null;

        }
        
        // return the data service.
        return (HAGlue) serviceItem.service;
        
    }
    
}
