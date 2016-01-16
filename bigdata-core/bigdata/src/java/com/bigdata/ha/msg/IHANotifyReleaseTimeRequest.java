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

import com.bigdata.journal.ITransactionService;

/**
 * Message from a follower to the leader in which the follower specifies the
 * earliest commit point that is pinned on the follower by an active transaction
 * or the minReleaseAge associated with its local {@link ITransactionService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHANotifyReleaseTimeRequest extends IHAMessage {

    /**
     * The service that provided this information.
     */
    public UUID getServiceUUID();

    /**
     * The earliest pinned commit time on the follower.
     */
    public long getPinnedCommitTime();

    /**
     * The earliest pinned commit counter on the follower.
     */
    public long getPinnedCommitCounter();

//    /**
//     * The readsOnCommitTime of the earliest active transaction on the follower.
//     */
//    public long getReadsOnCommitTimeForEarliestActiveTx();
//
//    /**
//     * The minReleaseAge on the follower (this should be the same on all
//     * services in a quorum).
//     */
//    public long getMinReleaseAge();

    /**
     * A timestamp taken during the protocol used to agree on the new release
     * time. This is used to detect problems where the clocks are not
     * synchronized on the services.
     */
    public long getTimestamp();

    /**
     * Mock responses are used when a follow is unable to provide a correct
     * response (typically because the follower is not yet HAReady and hence is
     * not able to participate in the gather). The mock responses preserves
     * liveness since the GATHER protocol will terminate quickly. By marking the
     * response as a mock object, the leader can differentiate mock responses
     * from valid responses and discard the mock responeses. If the GATHER task
     * on the follower sends a mock response to the leader, then it will also
     * have thrown an exception out of its GatherTask which will prevent the
     * follower from voting YES on the PREPARE message for that 2-phase commit.
     * 
     * @see <href="https://sourceforge.net/apps/trac/bigdata/ticket/720" > HA3
     *      simultaneous service start failure </a>
     */
    public boolean isMock();

    /**
     * The commit counter that will be assigned to the new commit point (as
     * specified by the leader).
     */
    public long getNewCommitCounter();

    /**
     * The commit time that will be assigned to the new commit point (as
     * specified by the leader).
     */
    public long getNewCommitTime();
    
}
