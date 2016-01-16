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
/*
 * Created on Jun 13, 2010
 */
package com.bigdata.ha;

import java.util.concurrent.TimeUnit;

import com.bigdata.ha.msg.IHANotifyReleaseTimeResponse;
import com.bigdata.journal.IRootBlockView;

/**
 * A 2-phase request as coordinated by the leader (local object).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PrepareRequest {

    /** The consensus release time from the GATHER. */
    private final IHANotifyReleaseTimeResponse consensusReleaseTime;
    
    /**
     * The services joined and non-joined with the met quorum as of the atomic
     * decision point for the GATHER.
     */
    private final IJoinedAndNonJoinedServices gatherJoinedAndNonJoinedServices;
    /**
     * The services joined and non-joined with the met quorum as of the atomic
     * decision point for the PREPARE.
     */
    private final IJoinedAndNonJoinedServices prepareJoinedAndNonJoinedServices;
//    private final UUID[] joinedServiceIds;
//    private final Set<UUID> nonJoinedPipelineServiceIds;
    private final IRootBlockView rootBlock;
    private final long timeout;
    private final TimeUnit unit;

    /** The consensus release time from the GATHER. */
    public IHANotifyReleaseTimeResponse getConsensusReleaseTime() {
    
        return consensusReleaseTime;
        
    }
    
    /**
     * The services joined and non-joined with the met quorum as of the atomic
     * decision point for the GATHER.
     */
    public IJoinedAndNonJoinedServices getGatherJoinedAndNonJoinedServices() {

        return gatherJoinedAndNonJoinedServices;
        
    }

    /**
     * The services joined and non-joined with the met quorum as of the atomic
     * decision point for the PREPARE.
     */
    public IJoinedAndNonJoinedServices getPrepareAndNonJoinedServices() {
        
        return prepareJoinedAndNonJoinedServices;
        
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
     * @param consensusReleaseTime
     *            The consensus release time from the GATHER.
     * @param gatherJoinedAndNonJoinedServices
     *            The services joined and non-joined with the met quorum as of
     *            the atomic decision point for the GATHER.
     * @param prepareJoinedAndNonJoinedServices
     *            The services joined and non-joined with the met quorum as of
     *            the atomic decision point for the PREPARE.
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
            final IHANotifyReleaseTimeResponse consensusReleaseTime,
            final IJoinedAndNonJoinedServices gatherJoinedAndNonJoinedServices,
            final IJoinedAndNonJoinedServices prepareJoinedAndNonJoinedServices,
//            final UUID[] joinedServiceIds, //
//            final Set<UUID> nonJoinedPipelineServiceIds,//
            final IRootBlockView rootBlock, final long timeout,
            final TimeUnit unit) {
        
        if (consensusReleaseTime == null)
            throw new IllegalArgumentException();
        
        if (gatherJoinedAndNonJoinedServices == null)
            throw new IllegalArgumentException();

        if (prepareJoinedAndNonJoinedServices == null)
            throw new IllegalArgumentException();
        
        if (rootBlock == null)
            throw new IllegalArgumentException();

        if (unit == null)
            throw new IllegalArgumentException();

        this.consensusReleaseTime = consensusReleaseTime;
        this.gatherJoinedAndNonJoinedServices = gatherJoinedAndNonJoinedServices;
        this.prepareJoinedAndNonJoinedServices = prepareJoinedAndNonJoinedServices;
//        this.joinedServiceIds = joinedServiceIds;
//        this.nonJoinedPipelineServiceIds = nonJoinedPipelineServiceIds;
        this.rootBlock = rootBlock;
        this.timeout = timeout;
        this.unit = unit;
    }

    @Override
    public String toString() {
        return super.toString()
                + "{"//
                + "isRootBlock0="
                + rootBlock.isRootBlock0()//
                + ", rootBlock="
                + rootBlock//
                + ", consensusReleaseTime="
                + consensusReleaseTime//
                + ", gatherJoinedAndNonJoinedServices="
                + gatherJoinedAndNonJoinedServices//
                + ", prepareJoinedAndNonJoinedServices="
                + prepareJoinedAndNonJoinedServices //
                + ", timeout=" + timeout//
                + ", unit=" + unit //
                + "}";
    }

}
