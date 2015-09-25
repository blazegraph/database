/**

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
/*
 * Created on Apr 23, 2007
 */

package com.bigdata.jini.util;

import java.net.MalformedURLException;
import java.util.UUID;

import javax.net.SocketFactory;

import net.jini.core.discovery.LookupLocator;
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

	/**
	 * Parse a comma delimited list of zero or more unicast URIs of the form
	 * <code>jini://host/</code> or <code>jini://host:port/</code>.
	 * <p>
	 * This MAY be an empty array if you want to use multicast discovery
	 * <strong>and</strong> you have specified the groups as
	 * {@link net.jini.discovery.LookupDiscovery#ALL_GROUPS} (a <code>null</code>).
	 * <p>
	 * Note: This method is intended for overrides expressed from scripts using
	 * environment variables where we need to parse an interpret the value
	 * rather than given the value directly in a {@link net.jini.config.Configuration} file. As
	 * a consequence, you can not specify the optional {@link SocketFactory} for
	 * the {@link net.jini.core.discovery.LookupLocator} with this method.
	 * 
	 * @param locators
	 *            The locators, expressed as a comma delimited list of URIs.
	 * 
	 * @return An array of zero or more {@link net.jini.core.discovery.LookupLocator}s.
	 * 
	 * @throws MalformedURLException
	 *             if any of the parse URLs is invalid.
	 * 
	 * @throws IllegalArgumentException
	 *             if the <i>locators</i> is <code>null</code>.
	 */
	public static LookupLocator[] getLocators(final String locators)
	        throws MalformedURLException {
	
	    if (locators == null)
	        throw new IllegalArgumentException();
	
	    final String[] a = locators.split(",");
	
	    final LookupLocator[] b = new LookupLocator[a.length];
	
	    if (a.length == 1 && a[0].trim().length() == 0) {
	
	        return new LookupLocator[0];
	
	    }
	    
	    for (int i = 0; i < a.length; i++) {
	
	        final String urlStr = a[i];
	
	        final LookupLocator locator = new LookupLocator(urlStr);
	
	        b[i] = locator;
	        
	    }
	    
	    return b;
	
	}
    
}
