package com.bigdata.quorum;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.rawstore.IRawStore;

/**
 * A {@link Remote} interface for methods supporting high availability for a set
 * of journals or data services having shared persistent state.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Scale-out needs to add {@link AbstractJournal#closeForWrites(long)} to
 *       this API and reconcile it with the methods to send index segments (and
 *       historical journal files) across the wire. Perhaps we need both an
 *       HAJournalGlue and an HADataServiceGlue interface?
 */
public interface HAGlue extends Remote {

    /**
     * The {@link UUID} of this service.
     * 
     * @todo This should be handled as a smart proxy so this method does not
     *       actually perform RMI.
     */
    UUID getServiceId();
    
    /**
     * Return the address at which this service will listen for write pipeline
     * messages sent from the upstream service.
     * 
     * @todo This should be handled as a smart proxy so this method does not
     *       actually perform RMI.
     */ 
    InetSocketAddress getWritePipelineAddr();

    /*
     * Administrative
     */

    /**
     * This method may be issued to force the service to close and then reopen
     * its zookeeper connection. This is a drastic action which will cause all
     * <i>ephemeral</i> tokens for that service to be retracted from zookeeper.
     * When the service reconnects, it will reestablish those connections.
     * 
     * @todo Good idea? Bad idea?
     */
    public Future<Void> bounceZookeeperConnection();
    
    /*
     * bad reads
     */

    /**
     * Read a record an {@link IRawStore} managed by the service. Services MUST
     * NOT refer this message to another service. If the read can not be
     * satisfied, for example because the {@link IRawStore} has been released or
     * because there is a checksum error when reading on the {@link IRawStore},
     * then that exception should be thrown back to the caller.
     * 
     * @param storeId
     *            The {@link UUID} identifying the {@link IRawStore} for which
     *            the record was requested.
     * @param addr
     *            The address of the record.
     * 
     * @return A runnable which will transfer the contents of the record into
     *         the buffer returned by the future.
     * 
     * @see HAService#readFromQuorum(UUID, long)
     */
    public RunnableFuture<byte[]> readFromDisk(UUID storeId, long addr)
            throws IOException;

    /*
     * quorum commit protocol.
     */

    /**
     * Save a reference to the caller's root block, flush writes to the backing
     * channel and acknowledge "yes" if ready to commit. If the node can not
     * prepare for any reason, then it must return "no".
     * 
     * @param rootBlock
     *            The new root block.
     * 
     * @return A {@link Future} which evaluates to a yes/no vote on whether the
     *         service is prepared to commit.
     */
    RunnableFuture<Boolean> prepare2Phase(IRootBlockView rootBlock)
            throws IOException;

    /**
     * Commit using the root block from the corresponding prepare message. It is
     * an error if a commit message is observed without the corresponding
     * prepare message.
     * 
     * @param commitTime
     *            The commit time that will be assigned to the new commit point.
     */
    RunnableFuture<Void> commit2Phase(long commitTime) throws IOException;

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
    RunnableFuture<Void> abort2Phase(long token) throws IOException;

    /*
     * Write replication pipeline.
     */

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
     *       {@link #bounceZookeeperConnection()}) if the service fails to
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
     * Accept metadata describing an NIO buffer transfer along the write
     * pipeline. This method is never invoked on the master. It is only invoked
     * on the failover nodes, including the last node in the failover chain.
     * 
     * @param msg
     *            The metadata.
     * 
     * @return The {@link Future} which will become available once the buffer
     *         transfer is complete.
     */
    Future<Void> receiveAndReplicate(HAWriteMessage msg) throws IOException;

    /*
     * Synchronization.
     * 
     * Various methods to support synchronization between services as they join
     * a quorum.
     */

    /**
     * Return the current root block for the persistence store. The initial root
     * blocks are identical, so this may be used to create a new journal in a
     * quorum by replicating the root blocks of the quorum leader.
     * 
     * @return The current root block.
     * 
     * @todo For the data service, we need a means to access the root blocks for
     *       the historical journals as well as the current live journal. The
     *       store UUID could be passed along for that purpose. When null, it
     *       could always be the root block of the current store in order to
     *       avoid a bootstrapping problem where we do not know the UUID of the
     *       current store and the currently store could have changed (via
     *       synchronous overflow) by the time we receive the response to that
     *       request.
     */
    IRootBlockView getRootBlock() throws IOException;

}
