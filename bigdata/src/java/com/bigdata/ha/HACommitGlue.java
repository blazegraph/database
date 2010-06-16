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
import java.rmi.Remote;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.concurrent.TimeoutException;
import com.bigdata.journal.AbstractJournal;

/**
 * A {@link Remote} interface supporting a 2-phase commit protocol for the
 * members of a highly available quorum.
 * 
 * @see QuorumCommit
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface HACommitGlue extends Remote {

    /**
     * Save a reference to the caller's root block, flush writes to the backing
     * channel and acknowledge "yes" if ready to commit. If the node can not
     * prepare for any reason, then it must return "no".
     * 
     * @param isRootBlock0
     *            <code>true</code> if this is rootBlock0 for the leader.
     * @param rootBlock
     *            The new root block.
     * @param timeout
     *            How long to wait for the other services to prepare.
     * @param unit
     *            The unit for the timeout.

     * @return A {@link Future} which evaluates to a yes/no vote on whether the
     *         service is prepared to commit.
     */
    Future<Boolean> prepare2Phase(boolean isRootBlock0, byte[] rootBlock,
            long timeout, TimeUnit unit) throws IOException;

    /**
     * Commit using the root block from the corresponding prepare message. It is
     * an error if a commit message is observed without the corresponding
     * prepare message.
     * 
     * @param commitTime
     *            The commit time that will be assigned to the new commit point.
     */
    Future<Void> commit2Phase(long commitTime) throws IOException;

    /**
     * Discard the current write set using {@link AbstractJournal#abort()},
     * reloading all state from the last root block, etc.
     * <p>
     * Note: A service automatically does a local abort() if it leaves the pool
     * of services joined with the quorum or if the leader fails over. Therefore
     * a service should reject this message if the token has been invalidated
     * since the service either has dine or will shortly do a low-level abort()
     * on its own initiative.
     * 
     * @param token
     *            The token for the quorum for which this request was made.
     */
    Future<Void> abort2Phase(long token) throws IOException;

}
