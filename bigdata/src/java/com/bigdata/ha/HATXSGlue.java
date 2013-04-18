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
package com.bigdata.ha;

import java.io.IOException;
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
public interface HATXSGlue {

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
     * 
     * @param req
     *            The request from the leader.
     * 
     * @see #notifyEarliestCommitTime(IHANotifyReleaseTimeResponse)
     */
    Future<Void> gatherMinimumVisibleCommitTime(IHAGatherReleaseTimeRequest req)
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

//    /**
//     * Return an asynchronous {@link Future} for a task executing on the quorum
//     * leader that holds the {@link Lock} protecting the critical section
//     * contended by
//     * {@link #gatherMinimumVisibleCommitTime(IHAGatherReleaseTimeRequest)}.
//     * This lock is used by a service that wishes to join a met quorum to
//     * prevent a service join that is concurrent with the critical section in
//     * which the already joined services agree on the new <i>releaseTime</i>.
//     * <p>
//     * Note: The leader MAY impose a timeout on this lock such that a service
//     * which does not complete its work in a timely fashion will lose the lock.
//     * The service MUST check the {@link Future} and verify that it is still
//     * running. If the {@link Future#isCancelled()}, then the service MUST NOT
//     * complete its service join (or must do a service leave to exit the
//     * quorum).
//     * 
//     * @return The {@link Future} for the task holding that lock.
//     */
//    Future<Void> getTXSCriticalSectionLockOnLeader(IHATXSLockRequest req)
//            throws IOException;

}
