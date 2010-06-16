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
 * Created on Jun 6, 2010
 */
package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.UUID;

/**
 * An interface that causes various changes in the distributed quorum state
 * required to execute the intention of a {@link QuorumMember} service and its
 * cognizant {@link AbstractQuorum}. The {@link QuorumActor} is responsible for
 * making coherent changes in the distributed quorum state. However, when a
 * service terminates abnormally, is partitioned from the distributed quorum,
 * etc., then the distributed quorum state MAY temporary be inconsistent. For
 * example, when a service loses its zookeeper connection, zookeeper will remove
 * all <i>ephemeral</i> znodes for that service, but the order in which those
 * tokens are removed is not specified.
 * <p>
 * The {@link QuorumActor} will reject operations whose preconditions are not
 * met (on member add, pipeline add, vote cast, service join, set token, etc.)
 * and for take additional operations as necessary to ensure that postconditions
 * are satisfied (on remove). This asymmetry in the API is intended to make it
 * easier for a service to "remove" itself from the various collections
 * maintained in the distributed quorum state.
 * <p>
 * The preconditions for add are:
 * <dl>
 * <dt>{@link #memberAdd()}</dt>
 * <dd>none.</dd>
 * <dt>{@link #pipelineAdd()}</dt>
 * <dd>member.</dd>
 * <dt>{@link #castVote(long)}</dt>
 * <dd>member, pipeline.</dd>
 * <dt>{@link #serviceJoin()}</dt>
 * <dd>member, pipeline, consensus around cast vote, predecessor in the vote
 * order is joined</dd>
 * <dt>{@link #setLastValidToken(long)}</dt>
 * <dd>member, pipeline, consensus around cast vote, first in the vote order,
 * new value is GT the lastValidToken.</dd>
 * <dt>{@link #setToken(long)}</dt>
 * <dd>member, pipeline, consensus around cast vote, first in the vote order,
 * new value is EQ to the lastValidToken.</dd>
 * </dl>
 * <p>
 * The post-conditions for remove are:
 * <dl>
 * <dt>{@link #memberRemove()}</dt>
 * <dd>not joined, no vote cast, not in pipeline, not member.</dd>
 * <dt>{@link #pipelineRemove()}</dt>
 * <dd>not in pipeline, vote withdrawn, service not joined.</dd>
 * <dt>{@link #withdrawVote()}</dt>
 * <dd>vote withdrawn, service not joined.</dd>
 * <dt>{@link #serviceLeave()}</dt>
 * <dd>vote withdrawn, not in pipeline, service not joined.</dd>
 * <dt>{@link #clearToken()}</dt>
 * <dd>This operation does not have any pre- or post-conditions. However,
 * services will quickly recognize that the quorum token is no longer valid and
 * will initiate local state changes required to prepare themselves to form a
 * new quorum.</dd>
 * </dl>
 * 
 * @author thompsonbry@users.sourceforge.net
 * @see QuorumWatcher
 */
public interface QuorumActor<S extends Remote, C extends QuorumClient<S>> {
    
	/**
	 * The {@link Quorum}.
	 */
	public Quorum<S, C> getQuourm();

	/**
	 * The service on whose behalf this class is acting.
	 */
	public QuorumMember<S> getQuorumMember();

	/**
	 * The {@link UUID} of the service on whose behalf this class is acting.
	 */
	public UUID getServiceId();

    /**
     * Add the service to the set of quorum members.
     */
    void memberAdd();

    /**
     * Remove the service from the set of quorum members.
     */
    void memberRemove();
    
    /**
     * Add the service to the write pipeline.
     */
    void pipelineAdd();

    /**
     * Remove the service from the write pipeline.
     */
    void pipelineRemove();

    /**
     * Cast a vote on the behalf of the associated service. If the service has
     * already voted for some other lastCommitTime, then that vote is withdrawn
     * before the new vote is cast. Services do not withdraw their cast votes
     * until a quorum breaks and a new consensus needs to be established. When a
     * service needs to synchronize, it will have initially votes its current
     * lastCommitTime. Once the service is receiving writes from the write
     * pipeline and has synchronized any historical delta, it will update its
     * vote and join the quorum at the next commit point (or immediately if
     * there are no outstanding writes against the quorum).
     * 
     * @param lastCommitTime
     *            The lastCommitTime timestamp for which the service casts its
     *            vote.
     * 
     * @throws IllegalArgumentException
     *             if the lastCommitTime is negative.
     */
    void castVote(long lastCommitTime);

    /**
     * Withdraw the vote cast by the service (a service has only one vote).
     */
    void withdrawVote();

    /**
     * Add the associated service to the set of joined services for the quorum.
     */
    void serviceJoin();

    /**
     * Remove the associated service from the set of joined services for the
     * quorum.
     */
    void serviceLeave();

    /**
     * Set the lastValidToken on the quorum equal to the given token. When a new
     * leader will be elected, this method will be invoked to update the quorum
     * token, passing in <code>newToken := lastValidToken+1</code>.
     */
    void setLastValidToken(final long newToken);

    /**
     * Set the current token on the quorum equal to the given token. When a new
     * leader will be elected, this method will be invoked to update the quorum
     * token, passing in <code>newToken := lastValidToken</code>. Note that
     * {@link #setLastValidToken(long)} will have been invoked as a precondition
     * so this has the effect of updating the current token to the recently
     * assigned newToken.
     */
    void setToken();

    /**
     * Clear the quorum token.
     */
    void clearToken();

}
