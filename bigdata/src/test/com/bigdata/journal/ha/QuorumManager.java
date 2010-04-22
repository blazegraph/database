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
     * Return <em>k</em>, the replication factor. A quorum exists only when
     * <code>(k + 1)/2</code> physical services for the same logical service
     * have an agreement on state.
     */
    int replicationFactor();
    
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
    
}
