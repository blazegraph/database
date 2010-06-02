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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.UUID;

import com.bigdata.journal.ha.QuorumException;

/**
 * A non-remote interface for a client which monitors the state of a quorum.
 * This interface adds the ability to receive notice of quorum state changes and
 * resolve the {@link Remote} interface for the member services of the quorum.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumClient<S extends Remote> extends QuorumListener {

    /**
     * The quorum that is being monitored.
     */
    Quorum getQuorum();

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

    /**
     * Return the remote interface used to perform HA operations on the quorum
     * leader.
     * 
     * @param token
     *            The quorum token for which the request was made.
     * 
     * @return The remote interface for the leader.
     * 
     * @throws QuorumException
     *             if the quorum token is no longer valid.
     */
    S getLeaderService(long token);

}
