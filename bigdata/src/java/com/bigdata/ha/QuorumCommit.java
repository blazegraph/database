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

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.journal.IRootBlockView;
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
     * @param isRootBlock0
     *            if this is rootBlock0.
     * @param rootBlock
     *            The new root block.
     * @param timeout
     *            How long to wait for the other services to prepare.
     * @param unit
     *            The unit for the timeout.
     * 
     * @return A {@link Future} which evaluates to a yes/no vote on whether the
     *         service is prepared to commit.
     */
    int prepare2Phase(boolean isRootBlock0, IRootBlockView rootBlock,
            long timeout, TimeUnit unit) throws InterruptedException,
            TimeoutException, IOException;

    /**
     * Used by the leader to send a message to each joined service in the quorum
     * telling it to commit using the root block from the corresponding
     * {@link #prepare2Phase(IRootBlockView, long, TimeUnit) prepare} message.
     * The commit MAY NOT go forward unless both the current quorum token and
     * the lastCommitTime on this message agree with the quorum token and
     * lastCommitTime in the root block from the last "prepare" message.
     * 
     * @param token
     *            The quorum token used in the prepare message.
     * @param commitTime
     *            The commit time that assigned to the new commit point.
     */
    void commit2Phase(long token, long commitTime) throws IOException,
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
    void abort2Phase(final long token) throws IOException, InterruptedException;

}
