package com.bigdata.journal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.journal.IRootBlockView;

/**
 * An ordered collection of services organized from the perspective of one
 * member of that quorum. A quorum has a replication factor <em>k</em> and is
 * considered "met" only when the #of services in the quorum is at least
 * <code>(k + 1)/2</code>. Client reads and writes will block unless the quorum
 * is met.
 * <p>
 * The first service in the chain is the master. Client writes are directed to
 * the master and are streamed from service to service in the order specified by
 * the quorum. Clients may read from any service in the quorum.
 * <p>
 * If a quorum breaks, then a new quorum must be met before client operations
 * can proceed. The new quorum will have a distinct token from the old quorum.
 */
public interface Quorum {

    /** The #of members of the quorum. */
    int size();

    /** The replication factor <em>k</em> is the target replication count. */
    int replicationFactor();

    /**
     * The token for the quorum. Each met quorum has a unique token which
     * identifies the committed state on which the agreement was achieved for at
     * least <code>(k + 1)/2</code> services. While the quorum meets on an
     * agreement on the <em>lastCommitTime</em>, the quorum token reflects each
     * such distinct agreement which is achieved even when agreement is on the
     * same <i>lastCommitTime</i>.
     * <p>
     * If the quorum is broken, the token is set to {@link #NO_QUORUM}, which is
     * never a legal quorum token. Likewise, the initial token before any quorum
     * has been met should be {@link #NO_QUORUM}.
     * 
     * FIXME Does the quorum token change if services join an existing quorum by
     * catching up through resynchronization? (Probably not. Synchronization
     * with an existing quorum needs to be specified in detail, but probably
     * does not change the quorum token. However, this also implies that changes
     * in the write pipeline, etc. may occur without a corresponding change in
     * the quorum token and must be detected and handled without error. This
     * implies that we must test HA with k=5 in order to test a loss of a
     * service in the quorum membership which do not cause the quorum to break.)
     */
    long token();

    /**
     * The constant used to indicate that there is no quorum (@value
     * {@value #NO_QUORUM}).
     */
    long NO_QUORUM = -1;
    
    /** Return true iff size GTE (capacity + 1). */
    boolean isQuorumMet();

//    /**
//     * Submits the message for execution by the next node in the quorum.
//     */
//    <T> void applyNext(RunnableFuture<T> r);

    /**
     * Return <code>true</code> iff this node is the master.
     */
    boolean isMaster();

    /**
     * Return the index of this service in the ordered list of services in a met
     * quorum.
     * 
     * @throws IllegalStateException
     *             if the quorum is not met.
     */
    int getIndex();
    
    /**
     * Return the remote interface used to perform HA operations on the members
     * of quorum.
     * 
     * @param index
     *            The index of the quorum member in [0:{@link #size()}-1].
     *            
     * @return The remote interface for that quorum member.
     */
    HAGlue getHAGlue(int index);
    
    /*
     * bad reads
     */

    /**
     * Read a record from another member of the quorum.
     * <p>
     * Note: This is NOT the normal path for reading on a record from a service.
     * This is used to handle bad reads (when a checksum or IO error is reported
     * by the local disk) by reading the record from another member of the
     * quorum.
     * 
     * @return The record.
     * 
     * @throws IllegalStateException
     *             if the quorum is not highly available.
     * @throws RuntimeException
     *             if the quorum is highly available but the record could not be
     *             read.
     */
    ByteBuffer readFromQuorum(long addr) throws InterruptedException,
            IOException;

    /*
     * quorum commit.
     */

    /**
     * Send a message to each member of the quorum instructing it to flush all
     * writes to the backing channel, and acknowledge "yes" if ready to commit.
     * If the node can not prepare for any reason, then it must return "no".
     * 
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
    int prepare2Phase(IRootBlockView rootBlock, long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException, IOException;

    /**
     * Send a message to each member of the quorum telling it to commit using
     * the root block from the corresponding prepare message.
     * <p>
     * Note: The commitTime is a sufficient indicator of the corresponding
     * "prepare" message. The commit times for the database are monotonically
     * increasing and are never reused. Also, unlike the commitCounter, the
     * commitTimes are never reused, even in the case of an aborted commit.
     * Therefore, each prepare message is guaranteed to have a root block with a
     * distinct commit time, regardless of the quorum for which it was formed.
     * However, neither the quorum token nor the commitCounter provide this
     * guarantee. The implementation of the commit message MUST verify that the
     * current quorum token is the same as the quorum token in the root block
     * for the "prepare" message.
     * 
     * @param commitTime
     *            The commit time that assigned to the new commit point.
     */
    void commit2Phase(long commitTime) throws IOException, InterruptedException;

    /**
     * Send a message to each member of the quorum telling it to discard its
     * write set, reloading all state from the last root block. If the node has
     * not observed the corresponding "prepare" message then it should ignore
     * this message.
     */
    void abort2Phase() throws IOException, InterruptedException;

    /**
     * Return a {@link Future} for a task which will replicate an NIO buffer
     * along the write pipeline.
     * 
     * @param msg
     *            The RMI metadata about the payload.
     * @param b
     *            The payload.
     */
    Future<Void> replicate(HAWriteMessage msg, ByteBuffer b)
            throws IOException, InterruptedException;

    /**
     * The service used by the master to transmit NIO buffers to the next node
     * in the write pipeline. The life cycle of the {@link HASendService} is
     * scoped by quorum and quorum membership changes.
     * 
     * @throws UnsupportedOperationException
     *             if the quorum is not highly available.
     */
    HASendService getHASendService();

    /**
     * The service used by the secondaries to accept and relay NIO buffers along
     * the write pipeline. The life cycle of the {@link HASendService} is scoped
     * by quorum and quorum membership changes.
     * 
     * @throws UnsupportedOperationException
     *             if the quorum is not highly available.
     */
    HAReceiveService<HAWriteMessage> getHAReceiveService();
    
}
