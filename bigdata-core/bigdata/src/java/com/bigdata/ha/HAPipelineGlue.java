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
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.util.concurrent.Future;

import com.bigdata.ha.msg.HAWriteMessage;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHASendState;
import com.bigdata.ha.msg.IHASendStoreResponse;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteSetStateRequest;
import com.bigdata.ha.msg.IHAWriteSetStateResponse;
import com.bigdata.ha.pipeline.HAReceiveService;
import com.bigdata.ha.pipeline.HASendService;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.service.proxy.ThickFuture;

/**
 * A {@link Remote} interface supporting the write replication pipeline. The
 * quorum leader accepts writes from the application layer. The writes are
 * formatted onto low-level cache blocks. Those cache blocks are replicated from
 * the quorum leader to the quorum followers along the pipeline.
 * 
 * @see QuorumPipeline
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface HAPipelineGlue extends Remote {

    /**
     * Return the address at which this service will listen for write pipeline
     * messages sent from the upstream service.
     * 
     * @todo This should be handled as a smart proxy so this method does not
     *       actually perform RMI.
     */ 
    InetSocketAddress getWritePipelineAddr() throws IOException;

    /**
     * Instruct the service to move to the end of the write pipeline. The leader
     * MUST be the first service in the write pipeline since it is the service
     * to which the application directs all write requests. This message is used
     * when a leader will be elected and it needs to force other services before
     * it in the write pipeline to leave/add themselves from/to the write
     * pipeline such that the leader becomes the first service in the write
     * pipeline.
     * 
     * @todo It is possible for the leader to issue these messages in an
     *       sequence which is designed to arrange the write pipeline such that
     *       it has a good network topology. The leader must first analyze the
     *       network topology for the services in the pipeline, decide on a good
     *       order, and then issue the requests to the services in that order.
     *       Each service will move to the end of the pipeline in turn. Once all
     *       services have been moved to the end of the pipeline, the pipeline
     *       will reflect the desired total order. However, note that transient
     *       network partitioning, zookeeper session timeouts, etc. can all
     *       cause this order to be perturbed.
     * 
     * @todo A pipeline leave currently causes a service leave. Reconsider this
     *       because it would mean that reorganizing the pipeline would actually
     *       cause service leaves, which would temporarily break the quorum and
     *       certainly distribute the leader election. If we can do a pipeline
     *       leave without a service leave, if services which are joined always
     *       rejoin the write pipeline when the observe a pipeline leave, and if
     *       we can force a service leave (by instructing the service to
     *       bounce the zookeeper connection) if the service fails to
     *       rejoin the pipeline, then it would be easier to reorganize the
     *       pipeline. [This would still make it possible for services to add
     *       themselves to the pipeline without being joined with the quorum but
     *       the quorum watcher would now automatically add the service to the
     *       quorum before a join and add it back if it is removed from the
     *       pipeline while it was joined _if_ the quorum is not yet met (once
     *       the quorum is met we can not rejoin the pipeline if writes have
     *       propagated along the pipeline, and the pipeline is not clean at
     *       each commit - only when we have an exclusive lock on the
     *       {@link WriteExecutorService}).]
     * 
     * @todo The leader could also invoke this at any commit point where a
     *       service joins the quorum since we need to obtain an exclusive lock
     *       on the {@link WriteExecutorService} at that point (use as we do
     *       when we do a synchronous overflow).
     *       <p>
     *       Periodic re-optimizations should take into account that services
     *       which are bouncing should stay at the end of the pipeline even
     *       though they have higher network costs because loosing the service
     *       at the end of the pipeline causes minimal retransmission.
     */
    Future<Void> moveToEndOfPipeline() throws IOException;
    
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
     * @return The {@link Future} for the operation on the remote service.
     * 
     * @throws IOException
     */
    Future<IHAPipelineResetResponse> resetPipeline(IHAPipelineResetRequest req)
            throws IOException;

    /**
     * Accept metadata describing an NIO buffer transfer along the write
     * pipeline. This method is never invoked on the master. It is only invoked
     * on the failover nodes, including the last node in the failover chain.
     * 
     * @param req
     *            A request for an HALog (optional). This is only non-null when
     *            historical {@link WriteCache} blocks are being replayed down
     *            the write pipeline in order to synchronize a service.
     * @param snd
     *            Metadata about the state of the sender and potentially the
     *            routing of the payload along the write replication pipeline.
     * @param msg
     *            The metadata.
     * 
     * @return The {@link Future} which will become available once the buffer
     *         transfer is complete.
     */
    Future<Void> receiveAndReplicate(IHASyncRequest req, IHASendState snd,
            IHAWriteMessage msg) throws IOException;

    /**
     * Request metadata about the current write set from the quorum leader.
     * 
     * @param req
     *            The request.
     *            
     * @return The response.
     */
    IHAWriteSetStateResponse getHAWriteSetState(IHAWriteSetStateRequest req)
            throws IOException;

    /**
     * Request the root blocks for the HA Log for the specified commit point.
     * 
     * @param msg
     *            The request (specifies the desired HA Log by the commit
     *            counter of the closing root block).
     * 
     * @return The requested root blocks.
     */
    IHALogRootBlocksResponse getHALogRootBlocksForWriteSet(
            IHALogRootBlocksRequest msg) throws IOException;

    /**
     * The recipient will send the {@link WriteCache} blocks for the specified
     * write set on the write pipeline. These {@link WriteCache} blocks will be
     * visible to ALL services in the write pipeline. It is important that all
     * services <em>ignore</em> {@link WriteCache} blocks that are not in
     * sequence for the current write set unless they have explicitly requested
     * an HALog using this method.
     * <p>
     * Note: The {@link WriteCache} blocks are sent on the write pipeline.
     * Therefore, a service MUST NOT request the write set from a service that
     * is downstream from it in the write pipeline. It is always safe to request
     * the data from the leader.
     * <p>
     * Note: The {@link HAWriteMessage} includes a quorum token. When historical
     * {@link WriteCache} blocks are being replicated in response to this
     * method, the will be replicated using the quorum token that was in effect
     * for that write set. Implementations MUST NOT perform asserts on the
     * quorum token until after they have determined that the message is for the
     * <em>current</em> write set.
     * <p>
     * Note: DO NOT use a {@link ThickFuture} for the returned {@link Future}.
     * That will defeat the ability of the requester to cancel the
     * {@link Future}.
     * 
     * @param req
     *            A request for an HALog.
     *            
     * @return A {@link Future} that may be used to cancel the remote process
     *         sending the data through the write pipeline.
     */
    Future<Void> sendHALogForWriteSet(IHALogRequest msg) throws IOException;

    /**
     * Send the raw blocks for the requested backing store across the write
     * pipeline.
     * <p>
     * Note: This method supports disaster recovery of a service from a met
     * quorum. This procedure can only be used when a met quorum exists.
     * <p>
     * Note: DO NOT use a {@link ThickFuture} for the returned {@link Future}.
     * That will defeat the ability of the requester to cancel the
     * {@link Future}.
     * 
     * @param req
     *            A request to replicate a backing store.
     * 
     * @return A {@link Future} that may be used to cancel the remote process
     *         sending the data through the write pipeline. The {@link Future}
     *         will report the current root block in effect as of the moment
     *         when the <code>sendHAStore</code> operation was completed.
     */
    Future<IHASendStoreResponse> sendHAStore(IHARebuildRequest msg)
            throws IOException;

//    /**
//     * There is something for this on HAGlue right now.
//     *
//     * TODO Method to compute a digest for the committed allocations on a
//     * backing store as of the commit point on which the specified transaction
//     * is reading. This may be used to verify that the backing stores are
//     * logically consistent even when they may have some discarded writes that
//     * are not present on all stores (from aborted write sets).
//     * <p>
//     * The caller can get the root blocks for the commit counter associated with
//     * the txId (if we can the readsOnCommitTime).
//     * <p>
//     * The RWStore needs to snapshot the allocators while holding the allocation
//     * lock and that snapshot MUST be for the same commit point. Therefore, this
//     * operation needs to be submitted by the leader in code that can guarantee
//     * that the leader does not go through a commit point. The snapshots should
//     * be stored at the nodes, not shipped to the leader. The leader therefore
//     * needs to know when each snapshot is ready so it can exit the critical
//     * code and permit additional writes (releasing the allocLock or the
//     * semaphore on the journal).
//     * 
//     * <pre>
//     * IHAStoreChecksumRequest {storeUUID, txid (of readLock)}
//     * 
//     * IHAStoreCheckSumResponse {MD5Digest}
//     * </pre>
//     */
//    Future<IHASnapshotDigestResponse> computeSnapshotDigest(IHASnapshotDigestRequest req) throws IOException;

}
