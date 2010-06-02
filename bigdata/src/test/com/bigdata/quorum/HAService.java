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
 * Created on Jun 1, 2010
 */

package com.bigdata.quorum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ha.HAReceiveService;
import com.bigdata.journal.ha.HASendService;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.rawstore.IRawStore;

/**
 * A non-remote interface for a member service in a {@link Quorum} defining
 * methods to support service specific high availability operations such as
 * reading on another member of the quorum, the 2-phase quorum commit protocol,
 * replicating writes, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface HAService<S extends HAGlue> extends QuorumMember<S> {

    /**
     * Return the local service implementation object (NOT the RMI proxy).
     */
    public S getService();
    
    /*
     * bad reads
     */

    /**
     * Used by any service joined with the quorum to read a record from another
     * service joined with the quorum in order to work around a "bad read" as
     * identified by a checksum error on the local service.
     * <p>
     * Note: This is NOT the normal path for reading on a record from a service.
     * This is used to handle bad reads (when a checksum or IO error is reported
     * by the local disk) by reading the record from another member of the
     * quorum.
     * 
     * @param storeId
     *            The {@link UUID} of the {@link IRawStore} from which the
     *            record should be read.
     * @param addr
     *            The address of a record on that store.
     * 
     * @return The record.
     * 
     * @throws IllegalStateException
     *             if the quorum is not highly available.
     * @throws RuntimeException
     *             if the quorum is highly available but the record could not be
     *             read.
     * 
     * @see HAGlue#readFromDisk(UUID, long)
     */
    ByteBuffer readFromQuorum(UUID storeId, long addr)
            throws InterruptedException, IOException;

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
     * @param rootBlock
     *            The new root block.
     * @param timeout
     *            How long to wait for the other services to prepare.
     * @param unit
     *            The unit for the timeout.
     * 
     * @return A {@link Future} which evaluates to a yes/no vote on whether the
     *         service is prepared to commit.
     * 
     * @todo {@link IRootBlockView} is not serializable. Send a byte[] instead.
     */
    int prepare2Phase(IRootBlockView rootBlock, long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException, IOException;

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

    /**
     * Return a {@link Future} for a task which will replicate an NIO buffer
     * along the write pipeline. This method is only invoked by the master.
     * The payload is replicated to the first follower in the write
     * pipeline. That follower will accept the payload (and replicate it if
     * necessary) using {@link #receiveAndReplicate(HAWriteMessage)}.
     * 
     * @param msg
     *            The RMI metadata about the payload.
     * @param b
     *            The payload.
     */
    Future<Void> replicate(HAWriteMessage msg, ByteBuffer b)
            throws IOException;

    /**
     * Return a {@link Future} for a task which will replicate an NIO buffer
     * along the write pipeline. This method is invoked for any node except
     * the master, including the last node in the failover chain.
     * 
     * @param msg
     *            The RMI metadata about the payload.
     */
    Future<Void> receiveAndReplicate(HAWriteMessage msg) throws IOException;

    /**
     * The service used by the leader to transmit NIO buffers to the next
     * node in the write pipeline. The returned object is valid as long as
     * this node remains the quorum leader.
     * 
     * @throws UnsupportedOperationException
     *             if the quorum is not highly available.
     */
    HASendService getHASendService();

    /**
     * The service used by the followers to accept and relay NIO buffers
     * along the write pipeline. The returned object is valid until the
     * service becomes the leader or disconnects from the {@link Quorum}.
     * 
     * @throws UnsupportedOperationException
     *             if the quorum is not highly available.
     */
    HAReceiveService<HAWriteMessage> getHAReceiveService();

    /**
     * The {@link Executor} used by the {@link HAService} to run various
     * asynchronous tasks.
     */
    Executor getExecutor();

} // HAService
