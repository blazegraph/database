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
 * Created on Sep 17, 2008
 */

package com.bigdata.service;

import java.util.UUID;

/**
 * Basic delegate for services that need to override the service UUID and
 * service interface reported to the {@link ILoadBalancerService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultServiceFederationDelegate<T extends AbstractService>
        implements IFederationDelegate {

    final protected T service;
    
    public DefaultServiceFederationDelegate(T service) {
        
        if (service == null)
            throw new IllegalArgumentException();
        
        this.service = service;
        
    }
    
    public String getServiceName() {
        
        return service.getServiceName();
        
    }
    
    public UUID getServiceUUID() {
        
        return service.getServiceUUID();
        
    }
    
    public Class getServiceIface() {
       
        return service.getServiceIface();
        
    }

    /** NOP */
    public void reattachDynamicCounters() {

    }

    /**
     * Returns <code>true</code>
     */
    public boolean isServiceReady() {
        
        return true;

    }
    
    /**
     * NOP
     */
    public void didStart() {
        
    }
    
    /** NOP */
    public void serviceJoin(IService service, UUID serviceUUID) {

    }

    /** NOP */
    public void serviceLeave(UUID serviceUUID) {

    }

}
