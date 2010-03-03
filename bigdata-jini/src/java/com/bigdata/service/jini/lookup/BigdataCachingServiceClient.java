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
 * Created on Apr 24, 2009
 */

package com.bigdata.service.jini.lookup;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import com.bigdata.service.IDataService;
import com.bigdata.service.jini.JiniFederation;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataCachingServiceClient<S extends Remote> extends
        AbstractCachingServiceClient<S> {

    /**
     * The {@link UUID} of the service owing this cache instance.
     */
    protected final UUID thisServiceUUID;
    
    /**
     * The service owning this cache instance. For standard bigdata services
     * this will be the {@link IDataService}, etc.
     */
    protected final Object thisService;
    
    /**
     * {@inheritDoc}
     * 
     * @param fed
     *            The {@link JiniFederation}. This class will use the
     *            {@link ServiceDiscoveryManager} exposed by the
     *            {@link JiniFederation} and {@link ServiceDiscoveryEvent}s
     *            will be passed by this class to the {@link JiniFederation},
     *            which implements {@link ServiceDiscoveryListener}. Those
     *            events are used to notice service joins.
     */
    public BigdataCachingServiceClient(JiniFederation fed, Class serviceIface,
            ServiceTemplate template, ServiceItemFilter filter, long timeout)
            throws RemoteException {

        super(fed.getServiceDiscoveryManager(),
                fed/* serviceDiscoveryListener */, serviceIface, template,
                filter, timeout);

        thisServiceUUID = fed.getClient().getDelegate().getServiceUUID();

        thisService = fed.getClient().getDelegate().getService();

    }

}
