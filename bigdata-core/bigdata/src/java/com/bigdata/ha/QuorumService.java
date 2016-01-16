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
 * Created on Jun 1, 2010
 */

package com.bigdata.ha;

import java.io.File;

import com.bigdata.journal.IRootBlockView;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumMember;

/**
 * A non-remote interface for a member service in a {@link Quorum} defining
 * methods to support service specific high availability operations such as
 * reading on another member of the quorum, the 2-phase quorum commit protocol,
 * replicating writes, etc.
 * <p>
 * A {@link QuorumService} participates in a write pipeline, which replicates
 * cache blocks from the leader and relays them from service to service in the
 * {@link Quorum#getPipeline() pipeline order}. The write pipeline is
 * responsible for ensuring that the followers will have the same persistent
 * state as the leader at each commit point. Services which are synchronizing
 * may also join the write pipeline in order to receive updates before they join
 * a met quorum. See {@link QuorumPipeline}.
 * <p>
 * The leader uses a 2-phase commit protocol to ensure that the quorum enters
 * each commit point atomically and that followers can read historical commit
 * points, including on the prior commit point. See {@link QuorumCommit}.
 * <p>
 * Persistent services use pre-record checksums to detect read errors and can
 * handle read failures by reading on another service joined with the quorum.
 * See {@link QuorumRead}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumService<S extends HAGlue> extends QuorumMember<S>,
        QuorumRead<S>, QuorumCommit<S>, QuorumPipeline<S> {

    /**
     * Return the lastCommitTime for this service (based on its current root
     * block).
     */
    long getLastCommitTime();
    
    /**
     * Return the lastCommitCounter for this service (based on its current root
     * block).
     */
    long getLastCommitCounter();

    /**
     * Return the service directory. This directory has various metadata about
     * the service process, but it might not contain either the data or the HA
     * log files.
     */
    File getServiceDir();

//    /**
//     * Return the directory in which we are logging the write blocks.
//     */
//    File getHALogDir();

    /**
     * Return the best guess at the process identifier for this process.
     */
    int getPID();
    
    /**
     * Return the configured timeout in milliseconds that the leader will await
     * the other services to prepare for a 2-phase commit.
     */
    long getPrepareTimeout();

    /**
     * Install root blocks on the local service. This is used for a few
     * different conditions in HA.
     * <ol>
     * <li>When the quorum meets for the first time, we need to take the root
     * block from the leader and use it to replace both of our root blocks (the
     * initial root blocks are identical). That will make the root blocks the
     * same on all quorum members.</li>
     * <li>REBUILD: When a service goes through an automated disaster recovery,
     * we need to install new root blocks in order to make the local journal
     * logically empty. This prevents the service from attempting to interpret
     * the data on the backing file if there is a restart part way through the
     * rebuild operation.</li>
     * </ol>
     * <p>
     * Note: The initial root blocks on a service are identical, so this root
     * block will be installed as both root block ZERO (0) and root block ONE
     * (1).
     * 
     * @param rootBlock0
     *            Root block ZERO (0).
     * @param rootBlock1
     *            Root block ONE (1).
     */
    void installRootBlocks(final IRootBlockView rootBlock0,
            final IRootBlockView rootBlock1);

    /**
     * Enter an error state. The error state should take whatever corrective
     * actions are necessary in order to prepare the service for continued
     * operations.
     */
    void enterErrorState();

    /**
     * Discard all state associated with the current write set. 
     */
    void discardWriteSet();
    
}
