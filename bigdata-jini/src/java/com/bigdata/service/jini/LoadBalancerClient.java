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

import net.jini.core.lookup.ServiceTemplate;

import com.bigdata.service.ILoadBalancerService;

/**
 * Class handles discovery of an {@link ILoadBalancerService}.  Clients are responsible
 * for generating notification events.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadBalancerClient extends
        AbstractCachingServiceClient<ILoadBalancerService> {

    public LoadBalancerClient(final JiniFederation fed, final long timeout) throws RemoteException {

        super(fed, ILoadBalancerService.class, new ServiceTemplate(null,
                new Class[] { ILoadBalancerService.class }, null),
                null/* filter */, timeout);
        
    }
    
    /**
     * Return the {@link ILoadBalancerService} service from the cache -or-
     * <code>null</code> if there is no such service in the cache and a remote
     * lookup times out.
     * 
     * @todo handle multiple service instances for failover but always designate
     *       a primary.
     */
    public ILoadBalancerService getLoadBalancerService() {

        return super.getService();
        
    }

}
