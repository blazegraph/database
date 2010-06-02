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
 * A non-remote interface for a service which will participate in a quorum as a
 * member service (as opposed to a client service that merely watches the
 * quorum). The methods on this interface are aware of the service {@link UUID}
 * of the member service and can report on its role and relationships in the
 * {@link Quorum}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumMember<S extends Remote> extends QuorumClient<S> {

    /**
     * The identifier for this service.
     */
    UUID getServiceId();
    
    /**
     * Return <code>true</code> iff this node is the quorum leader. The
     * quorum leader is the only node which will accept writes. Note that is
     * always <code>true</code> when the {@link #replicationFactor()} is ONE
     * (1).
     * 
     * @param token
     *            The quorum token for which the request was made.
     */
    boolean isLeader(long token);

    /**
     * Return <code>true</code> iff this node is a quorum follower. This is
     * <code>true</code> of all nodes in a {@link Q} except for the leader.
     * 
     * @param token
     *            The quorum token for which the request was made.
     */
    boolean isFollower(long token);

    /**
     * Return <code>true</code> iff the quorum is highly available and this
     * node is last one in the write pipeline (it will not return true for a
     * singleton quorum where the only node is the master).
     * 
     * @param token
     *            The quorum token for which the request was made.
     */
    boolean isLastInChain(long token);

    /**
     * Return metadata used to communicate with the downstream node in the write
     * pipeline. When a quorum is met, the <i>leader</i> is always first in the
     * write pipeline since it is the node which receives writes from clients.
     * When a service joins the write pipeline, it always does so at the end of
     * the chain. Services may enter the write pipeline before joining a quorum
     * in order to synchronize with the quorum. If a service in the middle of
     * the chain leaves the pipeline, then the upstream node will reconfigure
     * and retransmit the current cache block to its new downstream node. This
     * prevent nodes which are "bouncing" during synchronization from causing
     * write sets to be discarded. However, if the leader leaves the write
     * pipeline, then the current token is invalidated and the write set will be
     * discarded.
     * 
     * @param token
     *            The quorum token for which the request was made.
     * 
     * @return The UUID of the downstream service in the write pipeline. This
     *         will return <code>null</code> for services which are not part of
     *         the write pipeline, for the leader, and for the last service in
     *         the write pipeline.
     * 
     * @return <code>null</code> if there is no downstream service.
     * 
     * @throws QuorumException
     *             if the quorum token is no longer valid.
     */
    UUID getDownstreamService(long token);

    /**
     * Invoked when the write pipeline has changed.
     * 
     * @param serviceId
     *            The {@link UUID} of a service which was either added to or
     *            removed from the write pipeline.
     */
    void pipelineChanged(UUID serviceId);

    /**
     * Invoked when <em>this</em> quorum member is elected as the quorum leader.
     * This event only occurs when the quorum meets.
     * 
     * @param token
     *            The newly assigned quorum token.
     */
    void electedLeader(long token);

    /**
     * Invoked when <em>this</em> quorum member is elected as a quorum follower.
     * This event occurs both when the quorum meets and when a quorum member is
     * becomes synchronized with and then joins an already met quorum.
     * 
     * @param token
     *            The newly assigned quorum token.
     * 
     *            FIXME Verify that we get this event both on meet and on join
     *            after meet.
     */
    void electedFollower(long token);

    /**
     * Invoked when this service was joined with the quorum and it leaves the
     * quorum.
     */
    void serviceLeft();

    /**
     * Invoked for all quorum members when the leader leaves the quorum.
     * 
     * @param leaderId
     *            The {@link UUID} of the leader which left the quorum.
     */
    void leaderLeft(UUID leaderLeft);

    /**
     * Invoked for all quorum member when the quorum breaks.
     */
    void quorumBroke();

}
