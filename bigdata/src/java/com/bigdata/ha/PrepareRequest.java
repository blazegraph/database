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
 * Created on Jun 13, 2010
 */
package com.bigdata.ha;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.bigdata.journal.IRootBlockView;

/**
 * A 2-phase request as coordinated by the leader (local object).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PrepareRequest {

    private final UUID[] joinedServiceIds;
    private final Set<UUID> nonJoinedPipelineServiceIds;
    private final IRootBlockView rootBlock;
    private final long timeout;
    private final TimeUnit unit;

    public UUID[] getJoinedServiceIds() {
        return joinedServiceIds;
    }

    public Set<UUID> getNonJoinedPipelineServiceIds() {
        return nonJoinedPipelineServiceIds;
    }

    public IRootBlockView getRootBlock() {
        return rootBlock;
    }

    public long getTimeout() {
        return timeout;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    /**
     * 
     * @param joinedServiceIds
     *            The services joined with the met quorum, in their join order.
     * @param nonJoinedPipelineServiceIds
     *            The non-joined services in the write pipeline (in any order).
     * @param isRootBlock0
     *            if this is rootBlock0.
     * @param rootBlock
     *            The new root block.
     * @param timeout
     *            How long to wait for the other services to prepare.
     * @param unit
     *            The unit for the timeout.
     */
    public PrepareRequest(
            final UUID[] joinedServiceIds, //
            final Set<UUID> nonJoinedPipelineServiceIds,//
            final IRootBlockView rootBlock, final long timeout,
            final TimeUnit unit) {
        
        if (rootBlock == null)
            throw new IllegalArgumentException();

        if (unit == null)
            throw new IllegalArgumentException();

        this.joinedServiceIds = joinedServiceIds;
        this.nonJoinedPipelineServiceIds = nonJoinedPipelineServiceIds;
        this.rootBlock = rootBlock;
        this.timeout = timeout;
        this.unit = unit;
    }

    @Override
    public String toString() {
        return super.toString() + "{isRootBlock0=" + rootBlock.isRootBlock0()
                + ", rootBlock=" + rootBlock + ", #joined="
                + joinedServiceIds.length + ", #nonJoined="
                + nonJoinedPipelineServiceIds.size() + ", joinedServices="
                + Arrays.toString(joinedServiceIds) + ", nonJoined="
                + nonJoinedPipelineServiceIds + ", timeout=" + timeout
                + ", unit=" + unit + "}";
    }

}
