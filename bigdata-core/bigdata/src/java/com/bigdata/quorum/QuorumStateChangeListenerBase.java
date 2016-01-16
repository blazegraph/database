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

    @Override
    public void memberAdd() {
    }

    @Override
    public void memberRemove() {
    }

    @Override
    public void pipelineAdd() {
    }

    @Override
    public void pipelineRemove() {
    }

    @Override
    public void pipelineElectedLeader() {
    }
    
    @Override
    public void pipelineChange(final UUID oldDownStreamId,
            final UUID newDownStreamId) {
    }

    @Override
    public void pipelineUpstreamChange() {
    }

    @Override
    public void consensus(final long lastCommitTime) {
    }

    @Override
    public void lostConsensus() {
    }

    @Override
    public void quorumBreak() {
    }

    @Override
    public void serviceJoin() {
    }

    @Override
    public void serviceLeave() {
    }

    @Override
    public void quorumMeet(long token, UUID leaderId) {
    }

}
