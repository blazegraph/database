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
import java.util.concurrent.Executor;

import com.bigdata.ha.HACommitGlue;
import com.bigdata.ha.QuorumCommit;

/**
 * A non-remote interface for a service which will participate in a quorum as a
 * member service (as opposed to a client service that merely watches the
 * quorum). The methods on this interface are aware of the service {@link UUID}
 * of the member service and can report on its role and relationships in the
 * {@link Quorum}. In order to <em>act</em> on the distributed quorum state, the
 * {@link QuorumMember} will issue various commands to its {@link QuorumActor}.
 * <p>
 * This interface defines a collection of messages which are used to inform the
 * {@link QuorumMember} of significant changes in the distributed quorum state
 * as it pertains to <i>this</i> quorum member. See {@link QuorumStateChangeListener}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumMember<S extends Remote> extends QuorumClient<S>,
        QuorumStateChangeListener {

    /**
     * The identifier for this service.
     */
    UUID getServiceId();

    /**
     * Return the local service implementation object (NOT the RMI proxy for
     * that object).
     * <p>
     * A service operating as a {@link QuorumMember} has both {@link Remote}
     * interface and a non-remote interface. These interfaces typically define
     * similar methods (for example, compare {@link HACommitGlue}, which is
     * {@link Remote}, with {@link QuorumCommit}, which is non-remote). The
     * {@link Remote} interface in turn will have a local implementation object
     * inside of the JVM and an exported proxy for that {@link Remote}
     * interface. This method returns the <em>local</em> implementation object
     * for the {@link Remote} interface and is intended to facilitate operations
     * which the {@link QuorumMember} executes against its own {@link Remote}
     * interface. While the {@link Remote} interface will be exposed to other
     * services using a proxy, how that happens is outside of the scope of this
     * method.
     */
    S getService();

    /**
     * An {@link Executor} which may be used by the {@link QuorumMember} to run
     * various asynchronous tasks.
     */
    Executor getExecutor();

    /**
     * Return the actor for this {@link QuorumMember}.
     */
    QuorumActor<S,QuorumMember<S>> getActor();
    
    /**
     * Return <code>true</code>if the quorum recognizes the service as a member
     * of that quorum. The quorum token is not required for this method because
     * membership status does not change with a quorum meet or break.
     */
    boolean isMember();

    /**
     * Return <code>true</code> if quorum recognizes the service is part of the
     * write pipeline. The quorum token is not required for this method because
     * pipeline status does not change with a quorum meet or break. Once a
     * service is receiving synchronous messages from a {@link Quorum} it will
     * notice when it enters and leaves the pipeline and when the downstream
     * service for this service changes in the quorum.
     */
    boolean isPipelineMember();
    
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
     * Return <code>true</code> if the service is joined with the quorum.
     * <p>
     * Note: This method DOES NOT throw an exception if the quorum is not met,
     * but it will not return <code>true</code> unless the quorum is met.
     * 
     * @param token
     *            The quorum token for which the request was made.
     */
    boolean isJoinedMember(long token);
    
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
     * Assert that this is the quorum leader.
     * 
     * @param token
     *            The quorum token for which the request was made.
     * @throws QuorumException
     *             if the quorum is not met
     * @throws QuorumException
     *             if the caller's token is not the current quorum token.
     * @throws QuorumException
     *             if this is not the quorum leader.
     */
    void assertLeader(final long token);

    /**
     * Return the {@link UUID} of the service (if any) downstream from this
     * service in the write pipeline.
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
    UUID getDownstreamServiceId();

}
