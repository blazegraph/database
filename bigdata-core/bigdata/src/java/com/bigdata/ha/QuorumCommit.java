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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.bigdata.quorum.Quorum;

/**
 * A non-remote interface for a member service in a {@link Quorum} defining
 * methods to support service the 2-phase quorum commit protocol.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumCommit<S extends HACommitGlue> { //extends QuorumService<S> {

    /*
     * quorum commit.
     */

    /**
     * Used by the leader to send a message to each joined service in the quorum
     * instructing it to flush all writes to the backing channel, and
     * acknowledge "yes" if ready to commit. If the service can not prepare for
     * any reason, then it must return "no". The service must save a copy of the
     * root block for use with the next {@link #commit2Phase(long, long) commit}
     * message.
     * 
     * @return The outcome of the distributed PREPARE request, indicating
     *         whether each service is prepared to commit.
     */
    PrepareResponse prepare2Phase(PrepareRequest req)
            throws InterruptedException, TimeoutException, IOException;

    /**
     * Used by the leader to send a message to each joined service in the quorum
     * telling it to commit using the root block from the corresponding
     * {@link #prepare2Phase(PrepareRequest) prepare} message. The commit MAY
     * NOT go forward unless both the current quorum token and the
     * lastCommitTime on this message agree with the quorum token and
     * lastCommitTime in the root block from the last "prepare" message.
     */
    CommitResponse commit2Phase(CommitRequest req) throws IOException,
            InterruptedException;

    /**
     * Used by the leader to send a message to each service joined with the
     * quorum telling it to discard its write set, reloading all state from the
     * last root block. If the node has not observed the corresponding "prepare"
     * message then it should ignore this message.
     * 
     * @param token
     *            The quorum token.
     */
    void abort2Phase(long token) throws IOException, InterruptedException;

}
