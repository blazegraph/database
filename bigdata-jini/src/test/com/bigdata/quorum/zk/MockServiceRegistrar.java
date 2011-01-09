/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jul 6, 2010
 */

package com.bigdata.quorum.zk;

import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Trivial mock for a service registery.
 * 
 * @param <V>
 */
class MockServiceRegistrar<V extends Remote> {
    
    private final ConcurrentHashMap<UUID, V> services = new ConcurrentHashMap<UUID, V>();
    
    public MockServiceRegistrar() {
        
    }
    
    public void put(UUID serviceId,V service) {
        services.put(serviceId, service);
    }
    
    public V get(UUID serviceId) {
        return services.get(serviceId);
    }
    
}

