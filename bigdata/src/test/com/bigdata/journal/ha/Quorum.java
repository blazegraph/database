package com.bigdata.journal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.sun.corba.se.impl.orbutil.closure.Future;

/**
 * An ordered collection of services. A quorum has a replication factor
 * <em>k</em> and is considered "met" only when the #of services in the quorum
 * is at least <code>(k + 1)/2</code>. Client reads and writes will block unless
 * the quorum is met.
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
     * The token for the quorum. Each met quorum has a distinct token which
     * identifies the committed state on which the agreement was achieved for at
     * least <code>(k + 1)/2</code> services. The token is based on an agreement
     * on the <em>lastCommitTime</em> and is monotonically increasing. A
     * collection of new services will first achieve agreement on a
     * lastCommitTime of ZERO (0), since that is the initial value as reported
     * by {@link AbstractJournal#getLastCommitTime()} before any commits have
     * been performed. For this reason, the initial token before any quorum has
     * been met should be <code>-1L</code> which is before all valid commit
     * times and also LT the initial value reported for the lastCommitTime,
     * which is ZERO (0).
     */
    long token();
    
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

    // /**
    // * The set of nodes which are discovered and have an agreement on the
    // * distributed state of the journal.
    // */
    // ServiceID[] getServiceIDsInQuorum();

    /*
     * consider: service leave()/join()
     * 
     * consider: quorum met()/broken()
     * 
     * These might be notification messages subscribed to by the individual
     * services.  The origin for this information is zk.
     */
    
    /*
     * Note: write() is handled at the WriteCache.  Entire WriteCache instances
     * are send down a configured pipeline at a time.
     */
//    /**
//     * Write a record on the failover chain. Only quorum members will accept the
//     * write.
//     * 
//     * @param newaddr
//     * @param oldaddr
//     * @param bd
//     * @return
//     */
//    public RunnableFuture<Void> write(long newaddr, long oldaddr, BufferDescriptor bd) {
//        
//        throw new UnsupportedOperationException();
//        
//    }

    /*
     * file extension.
     */

    /**
     * Set the file length on all the services in the quorum.
     * <p>
     * Note: This method is invoked automatically by the master if the
     * {@link IBufferStrategy} needs to extend the file length for its managed
     * backing file. The hook is within the {@link IBufferStrategy}
     * implementation since the request to extend the file to make room for
     * additional writes does not go through the {@link AbstractJournal}.
     */
    void truncate(long extent);

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
     * @todo hook for monitoring (nagios, etc). bad reads indicate a probable
     *       problem.
     */
    void readFromQuorum(long addr, ByteBuffer b);

    /**
     * Send a message to each member of the quorum instructing it to flush all
     * writes to the backing channel, and acknowledge "yes" if ready to commit.
     * If the node can not prepare for any reason, then it must return "no".
     * 
     * @param commitTime
     *            The commit time that will be assigned to the new commit point.
     *            The commit times for the database are monotonically increasing
     *            and are never reused. Also, unlike the commitCounter, the
     *            commitTimes are never reused, even in the case of an aborted
     *            commit.
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
    int prepare2Phase(long commitTime, IRootBlockView rootBlock, long timeout,
            TimeUnit unit) throws InterruptedException, TimeoutException,
            IOException;

    /**
     * Send a message to each member of the quorum telling it to commit using
     * the root block from the corresponding prepare message.
     * 
     * @param commitTime
     *            The commit time that assigned to the new commit point.
     */
    void commit2Phase(long commitTime) throws IOException;

    /**
     * Send a message to each member of the quorum telling it to discard its
     * write set, reloading all state from the last root block.
     */
    void abort2Phase() throws IOException;

}
