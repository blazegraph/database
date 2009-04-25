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
 * Created on Jan 18, 2009
 */

package com.bigdata.jini.lookup.entry;

import java.util.UUID;

import net.jini.config.Configuration;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.entry.AbstractEntry;

import com.bigdata.jini.start.config.JiniServiceConfiguration;
import com.bigdata.jini.start.config.ManagedServiceConfiguration;
import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.util.JiniUtil;

/**
 * An {@link Entry} whose value is the {@link ServiceID} assigned to a service
 * represented as a standard {@link UUID} (which can be expressed as a
 * {@link String} and then parsed to regain the {@link UUID}). This attribute
 * is placed into a generated service {@link Configuration} in order to inform a
 * service of a pre-assigned {@link ServiceID}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see ManagedServiceConfiguration
 * @see JiniServiceConfiguration
 * @see AbstractServer
 * @see JiniUtil#uuid2ServiceID(UUID)
 * @see JiniUtil#serviceID2UUID(ServiceID)
 */
public class ServiceUUID extends AbstractEntry {

    /**
     * 
     */
    private static final long serialVersionUID = 7187162825927205358L;
    
    public UUID serviceUUID;

    public ServiceUUID() {
    }

    public ServiceUUID(final UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        this.serviceUUID = serviceUUID;
        
    }

}
