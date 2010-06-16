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
import java.util.concurrent.CopyOnWriteArraySet;


/**
 * Abstract base class for a {@link QuorumMember}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractQuorumMember<S extends Remote> extends
        AbstractQuorumClient<S> implements QuorumMember<S> {

    private final UUID serviceId;

    protected AbstractQuorumMember(final UUID serviceId) {

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

    /**
     * Return the actor for this {@link QuorumMember} (it is allocated by
     * {@link AbstractQuorum#start(QuorumClient)}).
     */
    public QuorumActor<S, QuorumMember<S>> getActor() {

        return (QuorumActor<S, QuorumMember<S>>) getQuorum().getActor();

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

    public UUID getDownstreamServiceId() {
        final UUID serviceId = getServiceId();
        final UUID[] pipeline = getQuorum().getPipeline();
        for (int i = 0; i < pipeline.length - 1; i++) {
            final boolean eq = serviceId.equals(pipeline[i]);
            if (!eq)
                continue;
            // The next service in the pipeline after this service.
            final UUID nextId = pipeline[i + 1];
            // Ok, we are at the end of the pipeline.
            return nextId;
        }
        // No such service.
        return null;
    }

    protected void assertQuorum(final long token) {
        getQuorum().assertQuorum(token);
    }

    public void assertLeader(final long token) {
        getQuorum().assertLeader(token);
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

    /*
     * QuorumStateChangeListener
     */
    
    /**
     * This is used to dispatch the {@link QuorumStateChangeListener} events.
     */
    private final CopyOnWriteArraySet<QuorumStateChangeListener> listeners = new CopyOnWriteArraySet<QuorumStateChangeListener>();

    /**
     * Add a delegate listener.
     * 
     * @param listener
     *            The listener.
     */
    protected void addListener(final QuorumStateChangeListener listener) {

        if (listener == null)
            throw new IllegalArgumentException();
        
        this.listeners.add(listener);
        
    }
    
    /**
     * Remove a delegate listener.
     * 
     * @param listener
     *            The listener.
     */
    protected void removeListener(final QuorumStateChangeListener listener) {

        if (listener == null)
            throw new IllegalArgumentException();
        
        this.listeners.remove(listener);
        
    }
    
    public void memberAdd() {
        for (QuorumStateChangeListener l : listeners) {
            l.memberAdd();
        }
    }

    public void memberRemove() {
        for (QuorumStateChangeListener l : listeners) {
            l.memberRemove();
        }
    }

    public void pipelineAdd() {
        for (QuorumStateChangeListener l : listeners) {
            l.pipelineAdd();
        }
    }

    public void pipelineRemove() {
        for (QuorumStateChangeListener l : listeners) {
            l.pipelineRemove();
        }
    }

    public void pipelineElectedLeader() {
        for (QuorumStateChangeListener l : listeners) {
            l.pipelineElectedLeader();
        }
    }

	public void pipelineChange(final UUID oldDownStreamId, final UUID newDownStreamId) {
        for (QuorumStateChangeListener l : listeners) {
            l.pipelineChange(oldDownStreamId, newDownStreamId);
        }
	}

    public void consensus(final long lastCommitTime) {
        for (QuorumStateChangeListener l : listeners) {
            l.consensus(lastCommitTime);
        }
    }

    public void lostConsensus() {
        for (QuorumStateChangeListener l : listeners) {
            l.lostConsensus();
        }
    }

    public void serviceJoin() {
        for (QuorumStateChangeListener l : listeners) {
            l.serviceJoin();
        }
    }

    public void serviceLeave() {
        for (QuorumStateChangeListener l : listeners) {
            l.serviceLeave();
        }
    }

    public void quorumBreak() {
        for (QuorumStateChangeListener l : listeners) {
            l.quorumBreak();
        }
    }

    public void quorumMeet(final long token, final UUID leaderId) {
        for (QuorumStateChangeListener l : listeners) {
            l.quorumMeet(token, leaderId);
        }
    }

}
