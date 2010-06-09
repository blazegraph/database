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
 * cognizant {@link AbstractQuorum}. 
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
	public C getQuorumMember();

	/**
	 * The {@link UUID} of the service on whose behalf this class is acting.
	 */
	public UUID getServiceId();

    /**
     * Add the associated service to the set of quorum members.
     */
    void memberAdd();

    /**
     * Remove the associated service from the set of quorum members.
     */
    void memberRemove();
    
    /**
     * Add the associated service to the write pipeline.
     */
    void pipelineAdd();

    /**
     * Remove the associated service from write pipeline.
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
     * First set the lastValidToken on the quorum equal to the given token and
     * then set the current token to the given token. When the leader is
     * elected, it should invoke this method to update the quorum token, passing
     * in <code>newToken := lastValidToken+1</code>. This method may only be
     * invoked by the leader.
     */
    void setToken(final long newToken);

}
