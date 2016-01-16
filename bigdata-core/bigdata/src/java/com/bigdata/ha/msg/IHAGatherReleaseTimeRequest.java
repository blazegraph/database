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

/**
 * Message used to request information about the earliest commit point that is
 * pinned on a follower. This is used by the leader to make a decision about the
 * new release time for the replication cluster. The message causes the follower
 * to send an {@link IHANotifyReleaseTimeRequest} back to the leader. That
 * message is sent from within the thread on the follower that is handling the
 * RMI for the {@link IHAGatherReleaseTimeRequest} in order to synchronize the
 * protocol across the leader and the followers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHAGatherReleaseTimeRequest extends IHAMessage {

    /**
     * The token for which this request is valid.
     */
    public long token();
    
    /**
     * A timestamp on the leader at the start of the protocol used to agree on
     * the new release time (this can be the commitTime that will be assigned by
     * the leader to the new commit point). This is used to detect problems
     * where the clocks are not synchronized on the services.
     */
    public long getTimestampOnLeader();

    /**
     * The UUID of the leader. This may be used to discover the service that
     * is (or was) the leader even if the token has been invalidated.
     */
    public UUID getLeaderId();

    /**
     * The commit counter that will be assigned to the new commit point.
     */
    public long getNewCommitCounter();

    /**
     * The commit time that will be assigned to the new commit point.
     */
    public long getNewCommitTime();
    
}
