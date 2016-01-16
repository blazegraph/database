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

public class HANotifyReleaseTimeRequest implements IHANotifyReleaseTimeRequest {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final UUID serviceUUID;
    private final long pinnedCommitTime;
    private final long pinnedCommitCounter;
    private final long timestamp;
    private final boolean isMock;
    private final long newCommitCounter;
    private final long newCommitTime;

    public HANotifyReleaseTimeRequest(final UUID serviceUUID,
            final long pinnedCommitTime, final long pinnedCommitCounter,
            final long timestamp, final boolean isMock,
            final long newCommitCounter, final long newCommitTime) {
        if (serviceUUID == null)
            throw new IllegalArgumentException();
        if (pinnedCommitTime < 0)
            throw new IllegalArgumentException();
        if (pinnedCommitCounter < 0)
            throw new IllegalArgumentException();
        this.serviceUUID = serviceUUID;
        this.pinnedCommitTime = pinnedCommitTime;
        this.pinnedCommitCounter = pinnedCommitCounter;
        this.timestamp = timestamp;
        this.isMock = isMock;
        this.newCommitCounter = newCommitCounter;
        this.newCommitTime = newCommitTime;
    }

    @Override
    public String toString() {
        return super.toString() + "{serviceUUID=" + serviceUUID
                + ",pinnedCommitTime=" + pinnedCommitTime
                + ",pinnedCommitCounter=" + pinnedCommitCounter + ",timestamp="
                + timestamp + ", isMock=" + isMock + "}";
    }

    @Override
    public UUID getServiceUUID() {
        return serviceUUID;
    }

    @Override
    public long getPinnedCommitTime() {
        return pinnedCommitTime;
    }

    @Override
    public long getPinnedCommitCounter() {
        return pinnedCommitCounter;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean isMock() {
        return isMock;
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
