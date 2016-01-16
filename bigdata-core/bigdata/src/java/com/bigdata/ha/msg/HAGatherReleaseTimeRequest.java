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
package com.bigdata.ha.msg;

import java.util.UUID;

public class HAGatherReleaseTimeRequest implements
        IHAGatherReleaseTimeRequest {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final long token;
    private final long timestampOnLeader;
    private final UUID leaderId;
    private final long newCommitCounter;
    private final long newCommitTime;

    public HAGatherReleaseTimeRequest(final long token,
            final long timestampOnLeader, final UUID leaderId,
            final long newCommitCounter, final long newCommitTime) {
        if (leaderId == null)
            throw new IllegalArgumentException();
        this.token = token;
        this.timestampOnLeader = timestampOnLeader;
        this.leaderId = leaderId;
        this.newCommitCounter = newCommitCounter;
        this.newCommitTime = newCommitTime;
    }

    @Override
    public String toString() {
        return super.toString() + "{token=" + token + ",timestampOnLeader="
                + timestampOnLeader + ", leaderId=" + leaderId
                + ", newCommitCounter=" + newCommitCounter + ", newCommitTime="
                + newCommitTime + "}";
    }
    
    @Override
    public long token() {
        return token;
    }

    @Override
    public long getTimestampOnLeader() {
        return timestampOnLeader;
    }
    
    @Override
    public UUID getLeaderId() {
        return leaderId;
    }

    @Override
    public long getNewCommitCounter() {
        return newCommitCounter;
    }

    @Override
    public long getNewCommitTime() {
        return newCommitTime;
    }

}
