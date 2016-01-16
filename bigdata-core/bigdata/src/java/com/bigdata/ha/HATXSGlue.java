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

import java.io.IOException;
import java.rmi.Remote;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Future;

import com.bigdata.ha.msg.IHAGatherReleaseTimeRequest;
import com.bigdata.ha.msg.IHANotifyReleaseTimeRequest;
import com.bigdata.ha.msg.IHANotifyReleaseTimeResponse;
import com.bigdata.journal.ITransactionService;

/**
 * RMI interface for the {@link ITransactionService} for HA.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a
 *      href="https://docs.google.com/document/d/14FO2yJFv_7uc5N0tvYboU-H6XbLEFpvu-G8RhAzvxrk/edit?pli=1#"
 *      > HA TXS Design Document </a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/623" > HA TXS
 *      / TXS Bottleneck </a>
 */
public interface HATXSGlue extends Remote {

    /**
     * Message used to request information about the earliest commit point that
     * is pinned on a follower. This is used by the leader to make a decision
     * about the new release time for the replication cluster.
     * <p>
     * Note: This message is used as part of a pattern where the leader
     * instructs the followers to message the leader with their earliest commit
     * point pinned by either a transaction or the minReleaseAge of their
     * {@link ITransactionService} using
     * {@link #notifyEarliestCommitTime(IHANotifyReleaseTimeResponse)}.
     * <p>
     * The message is a sync RMI call. The follower will clear an outcome and
     * execute a task which runs asynchronously and messages back to the leader
     * with its {@link IHANotifyReleaseTimeResponse}. The leader will report
     * back the consensus release time. The outcome of these on the follower is
     * not directly reported back to the leader, e.g., through a remote
     * {@link Future} because this causes a DGC thread leak on the follower. See
     * the ticket below. Instead, the follower notes the outcome of the gather
     * operation and will vote "NO" in
     * {@link HACommitGlue#prepare2Phase(IHA2PhasePrepareMessage)} unless it
     * completes its side of the release time consensus protocol without error
     * (that is, the otherwise unmonitored outcome of the asynchronous task for
     * {@link #gatherMinimumVisibleCommitTime(IHAGatherReleaseTimeRequest)}).
     * 
     * @param req
     *            The request from the leader.
     * 
     * @see #notifyEarliestCommitTime(IHANotifyReleaseTimeResponse)
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/673" >
     *      Native thread leak in HAJournalServer process </a>
     */
    void gatherMinimumVisibleCommitTime(IHAGatherReleaseTimeRequest req)
            throws IOException;

    /**
     * Message used by the follower to notify the leader about the new release
     * time that will be visible for new transaction starts. The leader then
     * chooses the mimimum across itself and the followers.
     * 
     * @param rsp
     *            The earliest pinned commit point on the follower.
     * 
     * @return The earliest pinned commit point across the services joined with
     *         the met quorum.
     * 
     * @throws BrokenBarrierException
     * @throws InterruptedException
     */
    IHANotifyReleaseTimeResponse notifyEarliestCommitTime(
            IHANotifyReleaseTimeRequest req) throws IOException,
            InterruptedException, BrokenBarrierException;

}
