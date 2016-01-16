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
package com.bigdata.ha;

import java.util.UUID;

public class HAPipelineResetRequest implements IHAPipelineResetRequest {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private long token;
    private UUID problemServiceId;
    private long timeoutNanos;

    public HAPipelineResetRequest(final long token,
            final UUID problemServiceId, final long timeoutNanos) {
        this.token = token;
        this.problemServiceId = problemServiceId;
        this.timeoutNanos = timeoutNanos;
    }

    @Override
    public long token() {
        return token;
    }

    @Override
    public String toString() {
        return super.toString() + "{token=" + token + ", problemServiceId="
                + problemServiceId + ", timeoutNanos=" + timeoutNanos + "}";
    }

    @Override
    public UUID getProblemServiceId() {
        return problemServiceId;
    }

    @Override
    public long getTimeoutNanos() {
        return timeoutNanos;
    }
    
}
