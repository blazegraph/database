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
package com.bigdata.quorum;

import java.util.UUID;


/**
 * Event data equivalent to the {@link QuorumStateChangeListener} API. This
 * interface makes it possible to enqueue these messages and then process them
 * asynchronously.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see QuorumStateChangeListener
 */
public interface QuorumStateChangeEvent {

    /**
     * The type of event and never <code>null</code>.
     */
    QuorumStateChangeEventEnum getEventType();

    /**
     * Return the old and new downstream {@link UUID}s.
     * 
     * @return An array of two elements. [0] is the old downstream service
     *         {@link UUID}. [1] is the new downstream service {@link UUID}.
     * 
     * @throws UnsupportedOperationException
     *             unless {@link #getEventType()} is
     *             {@link QuorumStateChangeEventEnum#PIPELINE_CHANGE}.
     * 
     * @see QuorumStateChangeListener#pipelineChange(UUID, UUID)
     */
    UUID[] getDownstreamOldAndNew();

    /**
     * The last commit time consensus.
     * 
     * @return The last commit time consensus value.
     * 
     * @throws UnsupportedOperationException
     *             unless {@link #getEventType()} is
     *             {@link QuorumStateChangeEventEnum#CONSENSUS}.
     * 
     * @see QuorumStateChangeListener#consensus(long)
     */
    long getLastCommitTimeConsensus();

    /*
     * quorumMeet(token,leaderId)
     */

    /**
     * Return the token on which the quorum met.
     * 
     * @return The token.
     * 
     * @throws UnsupportedOperationException
     *             unless {@link #getEventType()} is
     *             {@link QuorumStateChangeEventEnum#QUORUM_MEET}.
     * 
     * @see QuorumStateChangeListener#quorumMeet(long, UUID)
     */
    long getToken();

    /**
     * Return the {@link UUID} of the quorum leader.
     * 
     * @return The {@link UUID} of the quorum leader.
     * 
     * @throws UnsupportedOperationException
     *             unless {@link #getEventType()} is
     *             {@link QuorumStateChangeEventEnum#QUORUM_MEET}.
     * 
     * @see QuorumStateChangeListener#quorumMeet(long, UUID)
     */
    UUID getLeaderId();

}
