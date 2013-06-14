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
package com.bigdata.journal.jini.ha;

import java.rmi.RemoteException;

import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;

import com.bigdata.ha.HAGlue;
import com.bigdata.service.IClientService;
import com.bigdata.service.jini.lookup.AbstractCachingServiceClient;

/**
 * Helper class for discovery management of {@link HAGlue} services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HAJournalDiscoveryClient extends
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
    public HAJournalDiscoveryClient(
            final ServiceDiscoveryManager serviceDiscoveryManager,
            final ServiceDiscoveryListener serviceDiscoveryListener,
            final long cacheMissTimeout)
            throws RemoteException {

        super(serviceDiscoveryManager, serviceDiscoveryListener,
                HAGlue.class, new ServiceTemplate(null,
                        new Class[] { HAGlue.class }, null),
                null/* filter */, cacheMissTimeout);

    }

}
