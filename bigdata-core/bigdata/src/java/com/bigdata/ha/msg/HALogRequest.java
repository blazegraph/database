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

public class HALogRequest implements IHALogRequest {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final UUID serviceId;
    private final long commitCounter;
//    private final boolean incremental;

    /**
     * @param serviceId
     *            The {@link UUID} of the service that made the request.
     * @param commitCounter
     *            The commit counter used to identify the desired commit point
     *            (the commit counter of the closing root block).
     */
    public HALogRequest(final UUID serviceId, final long commitCounter
//            , final boolean incremental
            ) {

        this.serviceId = serviceId;
        this.commitCounter = commitCounter;
//        this.incremental = incremental;

    }

    @Override
    public long getCommitCounter() {

        return commitCounter;

    }

    @Override
    public UUID getServiceId() {

        return serviceId;
        
    }

    public String toString() {

        return getClass() + "{serviceId=" + getServiceId() + ", commitCounter="
                + getCommitCounter() +
//                ", incremental=" + isIncremental() +
                "}";

    }

//    @Override
//    public boolean isIncremental() {
//
//        return incremental;
//        
//    }
    
}
