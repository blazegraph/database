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
 * Enumeration of event types for the events described by the
 * {@link QuorumStateChangeListener} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public enum QuorumStateChangeEventEnum {
    /**
     * This service was added as a quorum member.
     * 
     * @see QuorumStateChangeListener#memberAdd()
     */
    MEMBER_ADD,
    /**
     * This service was removed as a quorum member.
     * 
     * @see QuorumStateChangeListener#memberRemove()
     */
    MEMBER_REMOVE,
    /**
     * This service was added as a pipeline member.
     * 
     * @see QuorumStateChangeListener#pipelineAdd()
     */
    PIPELINE_ADD,
    /**
     * This service was removed as a pipeline member.
     * 
     * @see QuorumStateChangeListener#pipelineRemove()
     */
    PIPELINE_REMOVE,
    /**
     * Event generated when a service already in the pipeline becomes the first
     * service in the write pipeline because all previous services in the
     * pipeline order have been removed from the pipeline.
     * 
     * @see QuorumStateChangeListener#pipelineElectedLeader()
     */
    PIPELINE_ELECTED_LEADER,
    /**
     * Event generated when the downstream service in the write pipeline has
     * changed. Services always enter at the end of the write pipeline, but may
     * be removed at any position in the write pipeline.
     * 
     * @see QuorumStateChangeListener#pipelineChange(UUID, UUID)
     */
    PIPELINE_CHANGE,
    /**
     * Event generated when the upstream service in the write pipeline has been
     * removed. This hook provides an opportunity for the service to close out
     * its connection with the old upstream service and to prepare to establish
     * a new connection with the new downstream service.
     * 
     * @see QuorumStateChangeListener#pipelineUpstreamChange()
     */
    PIPELINE_UPSTREAM_CHANGE,
    /**
     * Event generated when a consensus has been achieved among
     * <code>(k+1)/2</code> services concerning a shared lastCommitTime (really,
     * this is not a consensus but a simple majority). This message is sent to
     * each member service regardless of whether or not they participated in the
     * consensus.
     * 
     * @see QuorumStateChangeListener#consensus(long)
     */
    CONSENSUS,
    /**
     * Event generated when the consensus is lost. Services do not withdraw
     * their cast votes until a quorum breaks and a new consensus needs to be
     * established.
     * 
     * @see QuorumStateChangeListener#lostConsensus()
     */
    LOST_CONSENSUS,
    /**
     * Event generated when this service joins the quorum.
     * 
     * @see QuorumStateChangeListener#serviceJoin()
     */
    SERVICE_JOIN,
    /**
     * Event generated when this service leaves the quorum.
     * 
     * @see QuorumStateChangeListener#serviceLeave()
     */
    SERVICE_LEAVE,
    /**
     * Event generated when a quorum meets.
     * 
     * @see QuorumStateChangeListener#quorumMeet(long, UUID)
     */
    QUORUM_MEET,
    /**
     * Event generated when a quorum breaks.
     * 
     * @see QuorumStateChangeListener#quorumBreak()
     */
    QUORUM_BREAK;
}
