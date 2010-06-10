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
 * Abstract base class for a {@link QuorumMember}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractQuorumMember<S extends Remote> extends
        AbstractQuorumClient<S> implements QuorumMember<S> {

    private final UUID serviceId;

    protected AbstractQuorumMember(final Quorum quorum, final UUID serviceId) {

        super(quorum);

        if (serviceId == null)
            throw new IllegalArgumentException();

        this.serviceId = serviceId;

    }

    public S getLeader(final long token) {
        final Quorum<?,?> q = getQuorum();
        q.assertQuorum(token);
        final UUID leaderId = q.getLeaderId();
        if (leaderId == null) {
            q.assertQuorum(token);
            throw new AssertionError();
        }
        return getService(leaderId);
    }

    abstract public S getService(UUID serviceId);

    public UUID getServiceId() {
        return serviceId;
    }

    public boolean isMember() {
        final UUID[] a = getQuorum().getMembers();
        for(UUID t : a) {
            if(serviceId.equals(t))
                return true;
        }
        return false;
    }

    public boolean isPipelineMember() {
        final UUID[] a = getQuorum().getPipeline();
        for(UUID t : a) {
            if(serviceId.equals(t))
                return true;
        }
        return false;
    }

    public boolean isJoinedMember(final long token) {
        final UUID[] a = getQuorum().getJoinedMembers();
        for(UUID t : a) {
            if(serviceId.equals(t)) {
                // verify the token is still valid.
                return getQuorum().token() == token;
            }
        }
        return false;
    }

    /**
     * @todo This method is a bit odd as it is open to two different semantics
     *       based on whether or not it will return false or throw a
     *       {@link QuorumException} if the condition is not true. What makes it
     *       odd is having the token passed in, but without the token we can not
     *       know that the test is valid as of the same met quorum.
     *       <p>
     *       Right now this is only used by unit tests. Maybe the method should
     *       go away?
     */
    public boolean isLeader(final long token) {
        if (!getServiceId().equals(getQuorum().getLeaderId())) {
            // Not the leader.
            return false;
        }
        // Verify quorum is still valid.
        getQuorum().assertQuorum(token);
        // Ok, we are the leader.
        return true;
    }

    /**
     * @todo This method is a bit odd as it is open to two different semantics
     *       based on whether or not it will return false or throw a
     *       {@link QuorumException} if the condition is not true. What makes it
     *       odd is having the token passed in, but without the token we can not
     *       know that the test is valid as of the same met quorum.
     *       <p>
     *       Right now this is only used by unit tests. Maybe the method should
     *       go away?
     */
    public boolean isFollower(final long token) {
        final UUID serviceId = getServiceId();
        final UUID[] joined = getQuorum().getJoinedMembers();
        for (int i = 0; i < joined.length; i++) {
            final boolean eq = serviceId.equals(joined[i]);
            if (!eq)
                continue;
            if (i == 0) {
                // Not a follower.
                return false;
            }
            // Verify quorum is still valid.
            getQuorum().assertQuorum(token);
            // Ok, we are a follower.
            return true;
        }
        return false;
    }

    /**
     * @todo This method is a bit odd as it is open to two different semantics
     *       based on whether or not it will return false or throw a
     *       {@link QuorumException} if the condition is not true. What makes it
     *       odd is having the token passed in, but without the token we can not
     *       know that the test is valid as of the same met quorum.
     *       <p>
     *       Right now this is only used by unit tests. Maybe the method should
     *       go away?
     */
    public boolean isLastInChain(final long token) {
        final UUID serviceId = getServiceId();
        final UUID[] pipeline = getQuorum().getPipeline();
        if (serviceId.equals(pipeline[pipeline.length - 1])) {
            // Verify quorum is still valid.
            getQuorum().assertQuorum(token);
            // Ok, we are at the end of the pipeline.
            return true;
        }
        return false;
    }

    public UUID getDownstreamService(final long token) {
        final UUID serviceId = getServiceId();
        final UUID[] pipeline = getQuorum().getPipeline();
        for (int i = 0; i < pipeline.length - 1; i++) {
            final boolean eq = serviceId.equals(pipeline[i]);
            if (!eq)
                continue;
            // The next service in the pipeline after this service.
            final UUID nextId = pipeline[i+1]; 
            // Verify quorum is still valid.
            getQuorum().assertQuorum(token);
            // Ok, we are at the end of the pipeline.
            return nextId;
        }
        // No such service.
        return null;
    }

    protected void assertQuorum(final long token) {
        getQuorum().assertQuorum(token);
    }

    protected void assertLeader(final long token) {
        if (!getServiceId().equals(getQuorum().getLeaderId())) {
            // Not the leader.
            throw new QuorumException();
        }
        // We are the leader, now verify quorum is still valid.
        getQuorum().assertQuorum(token);
    }

    protected void assertFollower(final long token) {
        final UUID serviceId = getServiceId();
        final UUID[] joined = getQuorum().getJoinedMembers();
        for (int i = 0; i < joined.length; i++) {
            final boolean eq = serviceId.equals(joined[i]);
            if (!eq)
                continue;
            if (i == 0) {
                // We are the leader (not a follower).
                throw new QuorumException();
            }
            // We are a follower.  Now verify quorum is still valid.
            getQuorum().assertQuorum(token);
        }
        // We are not a joined service.
        throw new QuorumException();
    }

    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void electedFollower() {
        if (log.isDebugEnabled())
            log.debug("");
    }

    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void electedLeader() {
        if (log.isDebugEnabled())
            log.debug("");
    }

    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void leaderLeft() {
        if (log.isDebugEnabled())
            log.debug("");
    }

    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void memberAdd() {
        if (log.isDebugEnabled())
            log.debug("");
    }

    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void memberRemove() {
        if (log.isDebugEnabled())
            log.debug("");
    }

    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void pipelineAdd() {
        if (log.isDebugEnabled())
            log.debug("");
    }

    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void pipelineRemove() {
        if (log.isDebugEnabled())
            log.debug("");
    }

	/**
	 * {@inheritDoc}
	 * 
	 * The default implementation logs the message but does not handle it.
	 */
	public void pipelineChange(final UUID oldDownStreamId,
			final UUID newDownStreamId) {
		if (log.isDebugEnabled())
			log.debug("oldDownStream=" + oldDownStreamId + ",newDownStream="
					+ newDownStreamId);
	}

	public void consensus(final long lastCommitTime) {
		if (log.isDebugEnabled())
			log.debug("lastCommitTime=" + lastCommitTime);
	}
    
    public void lostConsensus() {
        if (log.isDebugEnabled())
            log.debug("");
    }
    
    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void quorumBreak() {
        if (log.isDebugEnabled())
            log.debug("");
    }

    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void serviceJoin() {
        if (log.isDebugEnabled())
            log.debug("");
    }

    /**
     * {@inheritDoc}
     * 
     * The default implementation logs the message but does not handle it.
     */
    public void serviceLeave() {
        if (log.isDebugEnabled())
            log.debug("");
    }

}
