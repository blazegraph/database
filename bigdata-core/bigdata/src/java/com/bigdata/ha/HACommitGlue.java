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
import java.rmi.Remote;
import java.util.concurrent.Future;

import com.bigdata.ha.msg.IHA2PhaseAbortMessage;
import com.bigdata.ha.msg.IHA2PhaseCommitMessage;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
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
     * @param prepareMessage
     *            The message used to prepare for the commit.
     *            
     * @return A {@link Future} which evaluates to a yes/no vote on whether the
     *         service is prepared to commit.
     */
    Future<Boolean> prepare2Phase(IHA2PhasePrepareMessage prepareMessage)
            throws IOException;

    /**
     * Commit using the root block from the corresponding prepare message. It is
     * an error if a commit message is observed without the corresponding
     * prepare message.
     * 
     * @param commitMessage
     *            The commit message.
     */
    Future<Void> commit2Phase(IHA2PhaseCommitMessage commitMessage)
            throws IOException;

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
    Future<Void> abort2Phase(IHA2PhaseAbortMessage abortMessage)
            throws IOException;

}
