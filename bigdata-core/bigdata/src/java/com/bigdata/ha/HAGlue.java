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
package com.bigdata.ha;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.Remote;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.msg.IHAAwaitServiceJoinRequest;
import com.bigdata.ha.msg.IHADigestRequest;
import com.bigdata.ha.msg.IHADigestResponse;
import com.bigdata.ha.msg.IHALogDigestRequest;
import com.bigdata.ha.msg.IHALogDigestResponse;
import com.bigdata.ha.msg.IHANotifyReleaseTimeResponse;
import com.bigdata.ha.msg.IHARemoteRebuildRequest;
import com.bigdata.ha.msg.IHARootBlockRequest;
import com.bigdata.ha.msg.IHARootBlockResponse;
import com.bigdata.ha.msg.IHASnapshotDigestRequest;
import com.bigdata.ha.msg.IHASnapshotDigestResponse;
import com.bigdata.ha.msg.IHASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.QuorumException;
import com.bigdata.rdf.task.IApiTask;
import com.bigdata.service.IService;

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
 * 
 * @todo The naming convention for these interfaces should be changed to follow
 *       the standard jini smart proxy naming pattern.
 */
public interface HAGlue extends HAGlueBase, HAPipelineGlue, HAReadGlue,
        HACommitGlue, HATXSGlue, IService {

    /*
     * Administrative
     * 
     * @todo Move to an HAAdminGlue interface?
     */
    
    /**
     * Await the service being ready to partitipate in an HA quorum. The
     * preconditions include:
     * <ol>
     * <li>receiving notice of the quorum token via
     * {@link #setQuorumToken(long)}</li>
     * <li>The service is joined with the met quorum for that token</li>
     * <li>If the service is a follower and it's local root blocks were at
     * <code>commitCounter:=0</code>, then the root blocks from the leader have
     * been installed on the follower.</li>
     * <ol>
     * 
     * @param timeout
     *            The timeout to await this condition.
     * @param units
     *            The units for that timeout.
     *            
     * @return the quorum token for which the service became HA ready.
     */
    public long awaitHAReady(long timeout, TimeUnit unit) throws IOException,
            InterruptedException, TimeoutException, QuorumException,
            AsynchronousQuorumCloseException;

    /**
     * A follower uses this message to request that the quorum leader await the
     * visibility of the zookeeper event in which the service join becomes
     * visible to the leader. This is invoked while holding a lock that blocks
     * pipeline replication, so the leader can not flush the write replication
     * pipeline and enter the commit. The callback to the leader ensures that
     * the service join is visible to the leader before the leader makes an
     * atomic decision about the set of services that are joined with the met
     * quorum for a 2-phase commit.
     * 
     * @param req
     *            The request.
     *            
     * @return The most recent consensus release time for the quorum leader.
     *         This information is used to ensure that a service which joins
     *         after a gather and before a PREPARE will join with the correct
     *         release time for its local journal and thus will not permit
     *         transactions to start against commit points which have been
     *         recycled by the quorum leader.
     * 
     * @throws InterruptedException
     * @throws TimeoutException
     *             if the timeout is exceeded before the service join becomes
     *             visible to this service.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/681" >
     *      HAJournalServer deadlock: pipelineRemove() and getLeaderId()</a>
     */
    public IHANotifyReleaseTimeResponse awaitServiceJoin(
            IHAAwaitServiceJoinRequest req) throws IOException,
            InterruptedException, TimeoutException;
    
    /*
     * Synchronization.
     * 
     * Various methods to support synchronization between services as they join
     * a quorum.
     * 
     * @todo Move to an HASyncGlue interface?
     */

    /**
     * Return the then current root block for the persistence store.
     * <p>
     * Note: The initial root blocks are identical, so this may be used to
     * create a new journal in a quorum by replicating the root blocks of the
     * quorum leader.
     * 
     * @param msg
     *            The message requesting the then current root block.
     * 
     * @return The then current root block.
     */
    IHARootBlockResponse getRootBlock(IHARootBlockRequest msg)
            throws IOException;

    /**
     * The port that the NanoSparqlServer is running on.
     */
    int getNSSPort() throws IOException;
    
    /**
     * The {@link RunState} of the service - this does NOT tell you whether the
     * service is ready to act as a leader or follower.
     */
    RunState getRunState() throws IOException;

    /**
     * The extended run state of the service - this embeds more information but
     * is not designed for progamatic interpretation.
     */
    String getExtendedRunState() throws IOException;

    /**
     * A simplified summary of the HA status of the service. This may be used to
     * reliably decide whether the service is the {@link HAStatusEnum#Leader}, a
     * {@link HAStatusEnum#Follower}, or {@link HAStatusEnum#NotReady}. This is
     * exposed both here (an RMI interface) and by the REST API.
     */
    HAStatusEnum getHAStatus() throws IOException;
    
    /**
     * Compute the digest of the entire backing store - <strong>THIS METHOD IS
     * ONLY FOR DIAGNOSTIC PURPOSES.</strong>
     * <p>
     * The digest is useless if there are concurrent writes since it can not be
     * meaningfully compared with the digest of another store unless both stores
     * are known to be stable.
     */
    IHADigestResponse computeDigest(IHADigestRequest req) throws IOException,
            NoSuchAlgorithmException, DigestException;

    /**
     * Compute the digest of the entire HALog file - <strong>THIS METHOD IS ONLY
     * FOR DIAGNOSTIC PURPOSES.</strong>
     * <p>
     * The digest is useless if there are concurrent writes since it can not be
     * meaningfully compared with the digest of another store unless both stores
     * are known to be stable.
     * 
     * @throws FileNotFoundException
     *             if the HALog for the specified commit point does not exist.
     */
    IHALogDigestResponse computeHALogDigest(IHALogDigestRequest req) throws IOException,
            NoSuchAlgorithmException, DigestException;

    /**
     * Compute the digest of the entire snapshot file - <strong>THIS METHOD IS
     * ONLY FOR DIAGNOSTIC PURPOSES.</strong> This digest is computed for the
     * compressed data so it may be compared directly with the digest of the
     * backing store from which the snapshot was obtained.
     * 
     * @throws FileNotFoundException
     *             if no snapshot exists for that commit point.
     */
    IHASnapshotDigestResponse computeHASnapshotDigest(IHASnapshotDigestRequest req)
            throws IOException, NoSuchAlgorithmException, DigestException;

//    /**
//     * Obtain a global write lock on the leader. The lock only blocks writers.
//     * Readers may continue to execute without delay.
//     * <p>
//     * You can not obtain a coherent backup of the {@link Journal} while there
//     * are concurrent write operations. This method may be used to coordinate
//     * full backups of the {@link Journal} by suspending low level writes on the
//     * backing file.
//     * <p>
//     * This method will block until the lock is held, the lock request is
//     * interrupted, or the lock request timeout expires.
//     * 
//     * @param req
//     *            The request.
//     * 
//     * @return A {@link Future} for the lock. The lock may be released by
//     *         canceling the {@link Future}. The lock is acquired before this
//     *         method returns and is held while the {@link Future} is running.
//     *         If the {@link Future#isDone()} then the lock is no longer held.
//     * 
//     * @throws IOException
//     *             if there is an RMI problem.
//     * @throws TimeoutException
//     *             if a timeout expires while awaiting the global lock.
//     * @throws InterruptedException
//     *             if interrupted while awaiting the lock.
//     * 
//     * @deprecated This is no longer necessary to support backups since we can
//     *             now take snapshots without suspending writers.
//  * @see https://sourceforge.net/apps/trac/bigdata/ticket/566 (
//  *      Concurrent unisolated operations against multiple KBs on the
//  *      same Journal)
//     */
//    @Deprecated 
//    Future<Void> globalWriteLock(IHAGlobalWriteLockRequest req)
//            throws IOException, TimeoutException, InterruptedException;

    /**
     * Request that the service take a snapshot. If there is already a snapshot
     * in progress, then the {@link Future} for that request will be returned.
     * 
     * @param req
     *            The request (optional). When <code>null</code>, the
     *            {@link Future} for any existing snapshot operation will be
     *            returned but the request WILL NOT schedule a snapshot if none
     *            is running.
     * 
     * @return A {@link Future} for the snapshot -or- <code>null</code> if no
     *         snapshot is running and none will be taken for that request.
     */
    Future<IHASnapshotResponse> takeSnapshot(IHASnapshotRequest req)
            throws IOException;

    /**
     * Disaster recovery (REBUILD) of the local database instance from the
     * leader of a met quorum.
     * 
     * There are several preconditions:
     * <ul>
     * 
     * <li>The quorum must be met and there must be an
     * {@link HAStatusEnum#Ready} leader.</li>
     * 
     * <li>This service must be {@link HAStatusEnum#NotReady}.</li>
     * 
     * <li>This service MUST NOT be at the same commit point as the leader (if
     * it is, then the service could meet in a data race with the met quorum and
     * we do not permit RESTORE if the service is joined with the met quorum).</li>
     * 
     * <li>The {@link HAJournalServer} must not be running a RESTORE (we don't
     * want it to accidentally interrupt a RESTORE that is in progress).</li>
     * 
     * </ul>
     * 
     * @return The (asynchronous) {@link Future} of the REBUILD operation -or-
     *         <code>null</code> if any of the pre-conditions were violated.
     */
    Future<Void> rebuildFromLeader(IHARemoteRebuildRequest req)
            throws IOException;

    /**
     * Run the caller's task on the service.
     * <p>
     * Note: This interface provides direct access to the raw index manager.
     * Caller's requiring concurrency control should submit an {@link IApiTask}
     * in order to have their task queue for the necessary locks and run on the
     * appropriate executor service.
     * 
     * @param callable
     *            The task to run on the service.
     * @param asyncFuture
     *            <code>true</code> if the task will execute asynchronously
     *            and return a {@link Future} for the computation that may
     *            be used to inspect and/or cancel the computation.
     *            <code>false</code> if the task will execute synchronously
     *            and return a thick {@link Future}.
     */
    public <T> Future<T> submit(IIndexManagerCallable<T> callable,
            boolean asyncFuture) throws IOException;
	
}
