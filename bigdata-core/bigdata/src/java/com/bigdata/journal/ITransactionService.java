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
 * Created on Mar 15, 2007
 */

package com.bigdata.journal;

import java.io.IOException;

import com.bigdata.btree.isolation.IConflictResolver;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;

/**
 * <p>
 * An interface for managing transaction life cycles.
 * </p>
 * <h2>Concurrency control</h2>
 * <p>
 * The underlying concurrency control mechanism is Multi-Version Concurrency
 * Control (MVCC). There are no "write locks" per say. Instead, a transaction
 * reads from a historical commit point identified by its assigned start time
 * (abs(transactionIdentifier) is a timestamp which identifies the commit point)
 * and writes on an isolated write set visible only to that transaction. When a
 * read-write transaction commits, its write set is validated against the then
 * current committed state of the database. If validation succeeds, the isolated
 * write set is merged down onto the database. Otherwise the transaction is
 * aborted and its write set is discarded. (It is possible to register an index
 * with an {@link IConflictResolver} in order to present the application with an
 * opportunity to validate write-write conflicts using state-based techniques,
 * i.e., by looking at the records and timestamps and making an informed
 * decision).
 * </p>
 * <p>
 * A transaction imposes "read locks" on the resources required for the
 * historical state of the database from which that transaction is reading (both
 * read-only and read-write transactions impose read locks). Those resources
 * will not be released until the transaction is complete or aborted. The
 * transaction manager coordinates the release of resources by advancing the
 * global release time - the earliest commit time from which a transaction may
 * read. Under dire circumstances (disk shortage) the transaction manager MAY
 * choose to abort transactions and advance the release time in order to permit
 * the release of locked resources and the reclamation of their space on local
 * disk.
 * </p>
 * <h2>Centralized transaction manager service</h2>
 * <p>
 * When deployed as a distributed database there will be a centralized service
 * implementing this interface and clients will discover and talk with that
 * service. The centralized service in turn will coordinate the distributed
 * transactions with the various {@link IDataService}s using their local
 * implementations of this same interface. The centralized transaction service
 * SHOULD invoke the corresponding methods on a {@link IDataService} IFF it
 * knows that the {@link IDataService} is buffering writes for the transaction.
 * </p>
 * <h2>Timestamps</h2>
 * <p>
 * Both read-only and read-write transactions assert global read locks on the
 * resources required for that historical state of the database corresponding to
 * their start time. Those read locks are released when the transaction
 * completes. Periodically the transaction manager will advance the release
 * time, rendering views of earlier states of the database unavailable.
 * </p>
 * <p>
 * The transaction identifier codes the transaction <em>start time</em>. The
 * transaction start time is chosen from among those distinct timestamps
 * available between the specified commit time and the next commit time on the
 * database. Successive read-write transactions must be assigned transaction
 * identifiers whose absolute value is strictly increasing - this requirement is
 * introduced by the MVCC protocol.
 * </p>
 * <p>
 * The sign of the transaction identifier indicates whether the transaction is
 * read-only (positive) or read-write (negative). Read-only transaction
 * identifiers may be directly used as commit times when reading on a local
 * store. Read-write transaction identifiers must have their sign bit cleared in
 * order to read from their ground state (the commit point corresponding to
 * their transaction start time) but the unmodified transaction identifier is 
 * used to access their mutable view (the view comprised of the write set of the
 * transaction super imposed on the ground state such that writes, overwrites,
 * and deletes are visible in the view).
 * </p>
 * <p>
 * The symbolic value {@link ITx#READ_COMMITTED} and any <code>startTime</code>
 * MAY be used to perform a lightweight read-only operations either on a local
 * data service or on the distributed database without coordination with the
 * {@link ITransactionService}, but resources MAY be released at any time since
 * no read "locks" have been declared. While a read-write transaction may be
 * readily identified by the sign associated with the transaction identifier,
 * you CAN NOT differentiate between a read-only transaction (with read-locks)
 * and a lightweight read on a given commit time. In practice, it is only the
 * transaction manager which needs to recognize read-only transactions and then
 * only to constrain its assignment of distinct transaction identifiers and to
 * coordinate the advance of the release time as transactions end. There is no
 * other practical difference between read-only transactions and lightweight
 * reads from the perspective of either the client or the individual data
 * services as read-locks are managed solely through the advancement of the
 * release time by the transaction manager.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ITransactionService extends ITimestampService {

    /**
     * Create a new transaction.
     * 
     * @param timestamp
     *            The timestamp may be:
     *            <ul>
     *            <li>A timestamp (GT ZERO), which will result in a
     *            read-historical (read-only) transaction that reads from the
     *            most recent committed state whose commit timestamp is less
     *            than or equal to <i>timestamp</i>. (As a special case, a
     *            timestamp that is GT the <code>lastCommitTime</code> will
     *            produce a read-only transaction that is reading on the
     *            <code>lastCommitTime</code> with snapshot isolation (new
     *            writes will not become visible in the view). This is basically
     *            the same behavior as specifying {@link ITx#READ_COMMITTED}.
     *            While perhaps counter-intuitive, this behavior is preferred to
     *            throwing an exception when a user attempts to read from a
     *            commit time GT the most recent commit point.).</li>
     *            <li>The symbolic constant {@link ITx#READ_COMMITTED} to obtain
     *            a read-historical transaction reading from the most recently
     *            committed state of the database. The transaction will be
     *            assigned a start time corresponding to the most recent commit
     *            point of the database and will be a fully isolated read-only
     *            view of the state of the database as of that start time. (This
     *            is an atomic shorthand for newTx(getLastCommitTime())).</li>
     *            <li>{@link ITx#UNISOLATED} for a read-write transaction.</li>
     *            </ul>
     * 
     * @return The unique transaction identifier.
     * 
     * @throws IllegalStateException
     *             if the requested timestamp is for a commit point that is no
     *             longer preserved by the database (the resources for that
     *             commit point have been released).
     * @throws IOException
     *             RMI errors.
     * 
     * @todo specialize exception for a timestamp that is no longer preserved
     *       and for one that is in the future?
     */
//    * @throws IllegalStateException
//    *             if the requested timestamp is greater than
//    *             {@link #getLastCommitTime()}.
    long newTx(long timestamp) throws IOException;
    
    /**
     * Request commit of the transaction write set. Committing a read-only
     * transaction is necessary in order to release read locks (this is very
     * fast). If a transaction has a write set, then this method does not return
     * until that write set has been made restart safe or the transaction has
     * failed.
     * <p>
     * The commit of a transaction with a write set on a single
     * {@link IDataService} does not require either {@link ITx#UNISOLATED} tasks
     * or other transactions to wait. The latency for such commits is directly
     * related to the size of the transaction write set.
     * <p>
     * However, the commit of a transaction with writes on more than one
     * {@link IDataService} requires a distributed commit protocol. The
     * distributed commit protocol forces ALL tasks writing on those
     * {@link IDataService}s to wait until the transaction is complete. This is
     * necessary in order to obtain a global commit point that corresponds to
     * the atomic commit state of the transaction (without this we would not
     * have the Atomic property for distributed transaction commits).
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @return The commit time for the transaction -or- ZERO (0L) if the
     *         transaction was read-only or had an empty write set. This commit
     *         time identifies a global commit point on the database from which
     *         you may read the coherent post-commit state of the transaction.
     * 
     * @throws ValidationError
     *             if the transaction could not be validated.
     * @throws IllegalStateException
     *             if <i>tx</i> is not an active transaction.
     * @throws IOException
     *             RMI errors.
     */
    long commit(long tx) throws ValidationError, IOException;

    /**
     * Request abort of the transaction write set.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @throws IllegalStateException
     *             if <i>tx</i> is not an active transaction.
     * @throws IOException
     *             RMI errors.
     */
    void abort(long tx) throws IOException;
    
    /**
     * Notify the {@link ITransactionService} that a commit has been performed
     * with the given timestamp (which it assigned) and that it should update
     * its lastCommitTime iff the given commitTime is GT its current
     * lastCommitTime.
     * <p>
     * Note: This is used to inform the {@link ITransactionService} of commits
     * that DO NOT involve transaction commits. That is, local unisolated writes
     * on individual {@link IDataService}s in an {@link IBigdataFederation}.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @throws IOException
     */
    void notifyCommit(long commitTime) throws IOException;

    /**
     * Return the last commitTime reported to the {@link ITransactionService}.
     * 
     * @return The last known commit time.
     * 
     * @throws IOException
     */
    long getLastCommitTime() throws IOException;

    /**
     * Return the timestamp whose historical data MAY be released. This time is
     * derived from the minimum of the timestamp of the earliest running
     * transaction and <code>now-minReleaseAge</code> and is updated whenever
     * the earliest running transaction terminates. This value is monotonically
     * increasing. It will always be LT the last non-zero last commit time. It
     * will never be negative. It MAY be ZERO (0L) and will be ZERO (0L) on
     * startup (unless explicitly set by the database to the last known commit
     * time).
     * <p>
     * The returned values is used to identify the most recent commit point LTE
     * the releaseTime. This is the earliest commit point on whose data MAY be
     * released. (Consequently, the first commit point GT the releaseTime is the
     * earliest visible commit point.)
     */
    long getReleaseTime() throws IOException;
    
}
