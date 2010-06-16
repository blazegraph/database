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
 * Created on Jun 14, 2010
 */

package com.bigdata.quorum;

import java.util.UUID;

/**
 * {@link QuorumStateChangeListener} provides NOP method implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QuorumStateChangeListenerBase implements QuorumStateChangeListener {

    public void memberAdd() {
    }

    public void memberRemove() {
    }

    public void pipelineAdd() {
    }

    public void pipelineRemove() {
    }

    public void pipelineElectedLeader() {
        
    }
    
    public void pipelineChange(final UUID oldDownStreamId,
            final UUID newDownStreamId) {
    }

    public void consensus(final long lastCommitTime) {
    }

    public void lostConsensus() {
    }

    public void quorumBreak() {
    }

    public void serviceJoin() {
    }

    public void serviceLeave() {
    }

    public void quorumMeet(long token, UUID leaderId) {
    }

}
