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
 * Created on Apr 28, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;

/**
 * Common service interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IService extends Remote {

    /**
     * The unique identifier for this data service.
     * <p>
     * Note: Some service discovery frameworks (Jini) will assign the service a
     * {@link UUID} asynchronously after a new service starts, in which case
     * this method will return <code>null</code> until the service
     * {@link UUID} has been assigned.
     * 
     * @return The unique data service identifier.
     * 
     * @throws IOException
     *             since you can use this method with RMI.
     */
    UUID getServiceUUID() throws IOException;
    
}
