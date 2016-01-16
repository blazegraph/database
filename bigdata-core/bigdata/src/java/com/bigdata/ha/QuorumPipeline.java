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
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Future;

import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.msg.IHASendState;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.pipeline.HAReceiveService;
import com.bigdata.ha.pipeline.HASendService;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumMember;

/**
 * A non-remote interface for a member service in a {@link Quorum} defining
 * methods to replicating writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumPipeline<S extends HAPipelineGlue> {

    /**
     * Return a {@link Future} for a task which will replicate an NIO buffer
     * along the write pipeline. This method is only invoked by the quorum
     * leader. The payload is replicated to the first follower in the write
     * pipeline. That follower will accept the payload (and replicate it if
     * necessary) using {@link #receiveAndReplicate(IHAWriteMessage)}.
     * <p>
     * Note: The implementation of this method should be robust to changes in
     * the write pipeline. Specifically, if a follower leaves the write
     * pipeline, it should attempt to retransmit the message and the payload
     * while allowing time for the write pipeline to be reconfigured in response
     * to the related {@link QuorumMember} events.
     * 
     * @param req
     *            A synchronization request (optional). This is only non-null
     *            when historical {@link WriteCache} blocks are being replayed
     *            down the write pipeline in order to synchronize a service.
     * @param msg
     *            The RMI metadata about the payload.
     * @param b
     *            The payload. The bytes from the position to the limit will be
     *            transmitted (note that the #of bytes remaining in the buffer
     *            MUST agree with {@link IHAWriteMessage#getSize()}).
     */
    Future<Void> replicate(IHASyncRequest req, IHAWriteMessage msg, ByteBuffer b)
            throws IOException;

    /**
     * Return a {@link Future} for a task which will replicate an NIO buffer
     * along the write pipeline. This method is invoked for any node except the
     * master, including the last node in the failover chain.
     * 
     * @param req
     *            A synchronization request (optional). This is only non-null
     *            when historical {@link WriteCache} blocks are being replayed
     *            down the write pipeline in order to synchronize a service.
     * @param snd
     *            Metadata about the state of the sender and potentially the
     *            routing of the payload along the write replication pipeline.
     * @param msg
     *            The RMI metadata about the payload.
     */
    Future<Void> receiveAndReplicate(IHASyncRequest req, IHASendState snd,
            IHAWriteMessage msg) throws IOException;

    /**
     * Reset the pipeline (blocking). This message is used to handle an error in
     * pipeline replication. If replication fails, the socket connections both
     * upstream and downstream of the point of failure can be left in an
     * indeterminate state with partially buffered data. In order to bring the
     * pipeline back into a known state (without forcing a quorum break) we
     * message each service in the pipeline to reset its
     * {@link HAReceiveService} (including the inner {@link HASendService}). The
     * next message and payload relayed from the leader will cause new socket
     * connections to be established.
     * 
     * @param msg The request.
     * 
     * @return The {@link Future} for the operation on the local service.
     * 
     * @throws IOException
     */
    Future<IHAPipelineResetResponse> resetPipeline(IHAPipelineResetRequest req)
            throws IOException;

    /*
     * Note: Method removed since it does not appear necessary to let this
     * service out of the scope of the QuorumPipelineImpl and the send service
     * itself is not aware of pipeline state changes.
     */
//    /**
//     * The service used by the leader to transmit NIO buffers to the next node
//     * in the write pipeline. The returned object is valid as long as this node
//     * remains the quorum leader.
//     * 
//     * @throws UnsupportedOperationException
//     *             if the quorum is not highly available.
//     */
//    HASendService getHASendService();

    /*
     * Note: Method removed since it does not appear necessary to let this
     * service out of the scope of the QuorumPipelineImpl and the send service
     * itself is not aware of pipeline state changes.
     */
//    /**
//     * The service used by the followers to accept and relay NIO buffers along
//     * the write pipeline. The returned object is valid until the service
//     * becomes the leader or disconnects from the {@link Quorum}.
//     * 
//     * @throws UnsupportedOperationException
//     *             if the quorum is not highly available.
//     */
//    HAReceiveService<HAWriteMessage> getHAReceiveService();

    /**
     * Return the lastCommitTime for this service (based on its current root
     * block). This supports the {@link IHAWriteMessage} which requires this
     * information as part of the metadata about replicated {@link WriteCache}
     * blocks.
     */
    long getLastCommitTime();
    
    /**
     * Return the lastCommitCounter for this service (based on its current root
     * block). This supports the {@link IHAWriteMessage} which requires this
     * information as part of the metadata about replicated {@link WriteCache}
     * blocks.
     */
    long getLastCommitCounter();

    /**
     * The {@link UUID} of the backing store.
     */
    UUID getStoreUUID();
    
    /**
     * Log the {@link IHAWriteMessage} and the associated data (if necessary).
     * The log file for the current write set will be deleted if the quorum is
     * fully met at the next 2-phase commit.
     * 
     * @param msg
     *            The {@link IHAWriteMessage}.
     * @param data
     *            The {@link WriteCache} block.
     */
    void logWriteCacheBlock(final IHAWriteMessage msg,
            final ByteBuffer data) throws IOException;

    /**
     * Log the root block for the commit point that closes the current write set
     * onto the {@link HALogWriter}.
     * <p>
     * Note: This method is ONLY invoked as part of the 2-phase commit protocol.
     * Therefore, this method ONLY applies to the live HALog file. A service is
     * atomically either joined with the met quorum at a 2-phase commit point or
     * not joined. The PREPARE and COMMIT messages are ONLY generated for
     * services that were joined with the met quorum as of that atomic decision
     * point in the commit protocol. Therefore, this method is never called for
     * a service that was not joined as of that atomic decision point.
     * 
     * @param rootBlock
     *            The root block for the commit point that was just achieved.
     */
//    * @param isJoinedService
//    *            <code>true</code> iff the service was joined with the met
//    *            quorum at the atomic decision point in the 2-phase commit
//    *            protocol.
    void logRootBlock(//final boolean isJoinedService,
            final IRootBlockView rootBlock) throws IOException;

    /**
     * Purge the local HA log files. This should be invoked when the service
     * goes through a commit point in which the quorum is fully met. At that
     * moment, we no longer require these log files to resynchronize any
     * service.
     * <p>
     * Note: The open HALog file is NOT removed by this method.
     * 
     * @param token
     *            The quorum token. The quorum must remain fully met for this
     *            token across this operation. If that invariant is violated
     *            then the operation will terminate once this violation is
     *            noticed.
     */
    void purgeHALogs(final long token);
    
}
