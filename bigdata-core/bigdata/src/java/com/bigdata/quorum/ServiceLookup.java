/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jun 2, 2010
 */
package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.UUID;

public interface ServiceLookup<S extends Remote> {

    /**
     * Return the remote interface used to perform HA operations on a member of
     * quorum.
     * 
     * @param serviceId
     *            The {@link UUID} associated with the service.
     * 
     * @return The remote interface for that quorum member.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>
     * @throws QuorumException
     *             if there is no {@link Quorum} member with that
     *             <i>serviceId</i>.
     */
    S getService(UUID serviceId);

}
