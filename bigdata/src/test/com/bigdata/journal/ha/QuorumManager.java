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
 * Created on Apr 22, 2010
 */

package com.bigdata.journal.ha;

import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.WORMStrategy;

/**
 * A manager for a set of physical services designated as prospective members of
 * the same logical service. The physical services must met in a {@link Quorum}
 * before operations on the services can commence. Operations will be suspended
 * if the {@link Quorum} breaks and will resume if the quorum becomes satisfied
 * again. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see Quorum
 */
public interface QuorumManager {

    /**
     * Return <em>k</em>, the replication factor. The replication factor must be
     * a non-negative odd integer (1, 3, 5, 7, etc). A quorum exists only when
     * <code>(k + 1)/2</code> physical services for the same logical service
     * have an agreement on state. A single service with <code>k := 1</code> is
     * the degenerate case and has a minimum quorum size of ONE (1). High
     * availability is only possible when <code>k</code> is GT ONE (1). Thus
     * <code>k := 3</code> is the minimum value for which services can be highly
     * available and has a minimum quorum size of <code>2</code>.
     */
    int replicationFactor();

    /**
     * Return <code>true</code> if the {@link QuorumManager} is configured for
     * high availability. High availability exists (in principle) when the
     * {@link QuorumManager#replicationFactor()} <em>k</em> is greater than one.
     * High availability exists (in practice) when the {@link Quorum}
     * {@link Quorum#isQuorumMet() is met} for a {@link QuorumManager} that is
     * configured for high availability.
     * 
     * @return <code>true</code> if this {@link QuorumManager} is highly
     *         available <em>in principle</code>
     */
    boolean isHighlyAvailable();
    
    /**
     * Return the current quorum (non-blocking). The quorum may or may not be
     * "met".
     */
    Quorum getQuorum();
    
    /**
     * Await and return a met quorum.
     */
    Quorum awaitQuorum() throws InterruptedException;

    /**
     * Assert that the quorum associated with the token is still valid. This
     * test verifies that (a) the quorum token has not changed; and (b) that the
     * quorum is still "met".
     * 
     * @param token
     *            The token for the quorum.
     * 
     * @throws IllegalStateException
     *             if the quorum is invalid.
     */
    void assertQuorum(long token);

    /**
     * Terminate any asynchronous processing associated with the
     * {@link QuorumManager}.
     */
    void terminate();

    /**
     * Returns the BufferStrategy associated with this HA node
     */
	IBufferStrategy getLocalBufferStrategy();

	void setLocalBufferStrategy(IBufferStrategy strategy);
}
