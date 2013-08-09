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

    public HANotifyReleaseTimeRequest(final UUID serviceUUID,
            final long pinnedCommitTime, final long pinnedCommitCounter,
            final long timestamp) {
        if (serviceUUID == null)
            throw new IllegalArgumentException();
        this.serviceUUID = serviceUUID;
        this.pinnedCommitTime = pinnedCommitTime;
        this.pinnedCommitCounter = pinnedCommitCounter;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return super.toString() + "{serviceUUID=" + serviceUUID
                + ",pinnedCommitTime=" + pinnedCommitTime
                + ",pinnedCommitCounter=" + pinnedCommitCounter + ",timestamp="
                + timestamp + "}";
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

}
