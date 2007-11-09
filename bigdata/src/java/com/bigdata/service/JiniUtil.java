/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Apr 23, 2007
 */

package com.bigdata.service;

import java.util.UUID;

import net.jini.core.lookup.ServiceID;

/**
 * Some utility methods that attempt to isolate the aspects of the Jini
 * architecture that would otherwise bleed into the bigdata architecture.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniUtil {

    /**
     * Convert a Jini {@link ServiceID} to a {@link UUID} (this changes the kind
     * of UUID implementation object but preserves the UUID data).
     * 
     * @param serviceID
     *            The {@link ServiceID}.
     * 
     * @return The {@link UUID}.
     */
    public static UUID serviceID2UUID(ServiceID serviceID) {

        if(serviceID==null) return null;
        
        return new UUID(serviceID.getMostSignificantBits(), serviceID
                .getLeastSignificantBits());

    }
    
    /**
     * Convert a {@link UUID} to a Jini {@link ServiceID} (this changes the kind
     * of UUID implementation object but preserves the UUID data).
     * 
     * @param uuid
     *            The {@link UUID}.
     * 
     * @return The Jini {@link ServiceID}.
     */
    public static ServiceID uuid2ServiceID(UUID uuid) {

        if(uuid==null) return null;
        
        return new ServiceID(uuid.getMostSignificantBits(), uuid
                .getLeastSignificantBits());

    }
    
}
