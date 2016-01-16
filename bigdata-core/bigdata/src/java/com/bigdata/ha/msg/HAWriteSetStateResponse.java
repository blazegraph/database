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

public class HAWriteSetStateResponse implements IHAWriteSetStateResponse {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final long commitCounter;
    private final long lastCommitTime;
    private final long sequence;

    public HAWriteSetStateResponse(final long commitCounter,
            final long lastCommitTime, final long sequence) {
        this.commitCounter = commitCounter;
        this.lastCommitTime = lastCommitTime;
        this.sequence = sequence;
    }

    @Override
    public long getCommitCounter() {
        return commitCounter;
    }

    @Override
    public long getLastCommitTime() {
        return lastCommitTime;
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        
        return getClass().getSimpleName() + "{commitCounter=" + commitCounter
                + ", sequence=" + sequence + ", lastCommitTime="
                + lastCommitTime + "}";
        
    }
    
}
