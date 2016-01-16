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
 * Event implementation class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class QCE implements QuorumStateChangeEvent {

    private final QuorumStateChangeEventEnum eventType;
    private final UUID[] downstreamOldAndNew;
    private final Long lastCommitTimeConsensus;
    private final Long token;
    private final UUID leaderId;

    public QCE(final QuorumStateChangeEventEnum eventType) {

        this(eventType, null/* downstreamOldAndNew */,
                null/* lastCommitTimeConsensus */, null/* token */, null/* leaderId */);

    }

    public QCE(final QuorumStateChangeEventEnum eventType,//
            final UUID[] downstreamOldAndNew,//
            final Long lastCommitTimeConsensus,//
            final Long token,//
            final UUID leaderId//
    ) {
        if (eventType == null)
            throw new IllegalArgumentException();
        switch (eventType) {
        case PIPELINE_CHANGE:
            if (downstreamOldAndNew == null)
                throw new IllegalArgumentException();
            if (downstreamOldAndNew.length != 2)
                throw new IllegalArgumentException();
            break;
        case CONSENSUS:
            if (lastCommitTimeConsensus == null)
                throw new IllegalArgumentException();
            break;
        case QUORUM_MEET:
            if (token == null)
                throw new IllegalArgumentException();
            if (leaderId == null)
                throw new IllegalArgumentException();
            break;
        }
        this.eventType = eventType;
        this.downstreamOldAndNew = downstreamOldAndNew;
        this.lastCommitTimeConsensus = lastCommitTimeConsensus;
        this.token = token;
        this.leaderId = leaderId;
    }

    @Override
    public QuorumStateChangeEventEnum getEventType() {
        return eventType;
    }

    @Override
    public UUID[] getDownstreamOldAndNew() {

        if (eventType != QuorumStateChangeEventEnum.PIPELINE_CHANGE)
            throw new UnsupportedOperationException();

        return downstreamOldAndNew;

    }

    @Override
    public long getLastCommitTimeConsensus() {

        if (eventType != QuorumStateChangeEventEnum.CONSENSUS)
            throw new UnsupportedOperationException();

        return lastCommitTimeConsensus.longValue();

    }

    @Override
    public long getToken() {

        if (eventType != QuorumStateChangeEventEnum.QUORUM_MEET)
            throw new UnsupportedOperationException();

        return token.longValue();

    }

    @Override
    public UUID getLeaderId() {

        if (eventType != QuorumStateChangeEventEnum.QUORUM_MEET)
            throw new UnsupportedOperationException();

        return leaderId;

    }

    public String toString() {
        return "QuorumStateChangeEvent"
                + "{type="
                + eventType
                + (eventType == QuorumStateChangeEventEnum.PIPELINE_CHANGE ? ",oldDownstreamId="
                        + downstreamOldAndNew[0]
                        + ",newDownstreamId="
                        + downstreamOldAndNew[1]
                        : "")
                + (eventType == QuorumStateChangeEventEnum.CONSENSUS ? ",lastCommitTime="
                        + lastCommitTimeConsensus
                        : "") //
                + (eventType == QuorumStateChangeEventEnum.QUORUM_MEET ? ",token="
                        + token + ",leaderId=" + leaderId
                        : "") //
                + "}"//
        ;
    }

}
