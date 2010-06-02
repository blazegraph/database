package com.bigdata.quorum;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IRootBlockView;
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
    public RunnableFuture<ByteBuffer> readFromDisk(UUID storeId, long addr)
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
     */
    IRootBlockView getRootBlock() throws IOException;

}
