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

    public S getLeaderService(final long token) {
        final Quorum q = getQuorum();
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

    public boolean isLeader(final long token) {
        if (getServiceId().equals(getQuorum().getLeaderId())) {
            // Not the leader.
            return false;
        }
        // Verify quorum is still valid.
        getQuorum().assertQuorum(token);
        // Ok, we are the leader.
        return true;
    }

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
        if (!isLeader(token))
            throw new QuorumException();
    }

    protected void assertFollower(final long token) {
        if (!isFollower(token))
            throw new QuorumException();
    }

}
