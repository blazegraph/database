/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Mar 15, 2007
 */

package com.bigdata.journal;

import com.bigdata.isolation.IConflictResolver;
import com.bigdata.service.DataService;
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
 * and writes on an isolated write set visible only to that transaction. When a
 * read-write transaction commits, its write set is validated against the then
 * current committed state of the database. If validation succeeds, the isolated
 * write set is merged down onto the database. Otherwise the transaction is
 * aborted and its write set is discarded.
 * </p>
 * <p>
 * A transaction will impose "read locks" on the resources required for the
 * historical state of the database from which that transaction is reading.
 * Those resources will not be released until the transaction is complete or
 * aborted (the transaction manager MAY choose to abort a read in order to
 * release locked resources).
 * </p>
 * <h2>Centralized transaction manager service</h2>
 * <p>
 * When deployed as a distributed database there will be a centralized service
 * implementing this interface and clients will discover and talk with that
 * service. The centralized service in turn will coordinate the distributed
 * transactions with the various {@link IDataService}s using their local
 * implementations of this same interface. The centralized transactin service
 * SHOULD invoke the corresponding methods on a {@link IDataService} IFF it
 * knows that the {@link IDataService} is buffering writes for the transaction.
 * </p>
 * <h2>Timestamps</h2>
 * <p>
 * Timestamps are used to indicate whether an operation is a historical
 * read-only transaction (-startTime), a fully isolated read-write (startTime),
 * a read-committed operation ({@link ITx#READ_COMMITTED}), or an unisolated
 * read-write operation ({@link ITx#UNISOLATED}}. Timestamps for fully
 * isolated read-write transactions MUST be obtained from an
 * {@link ITransactionManager}. Likewise, a read-only transaction obtained from
 * an {@link ITransactionManager} will cause the resources necessary to support
 * that read to be retained until the transaction has completed. The value
 * <code>-startTime</code> MAY be used to perform a lightweight read-only
 * operation without coordination with the {@link ITransactionManager} but
 * resources MAY be released since no read "locks" have been declared.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Since the overall concurrency control algorithm is MVCC, the
 *       {@link ITransactionManagerService} becomes aware of the required locks
 *       during the active {@link ITx#isActive()} phase of the transaction. By
 *       the time the transaction is done executing and a COMMIT is requested by
 *       the client the {@link ITransactionManagerService} knows the set of
 *       resources (named indices) on which the transaction has written. As its
 *       first step in the commit protocol the
 *       {@link ITransactionManagerService} acquires the necessary exclusive
 *       locks on those resources. The locks are eventually released when the
 *       transaction either commits or aborts.
 * 
 * FIXME in order to support 2-/3-phase commit, the [commitTime] from the
 * transaction manager service must be passed through to the journal rather than
 * being returned from {@link #commit(long)}. There also needs to be a distinct
 * "prepare" message that validates the write set of the transaction and makes
 * it restart safe. finally, i have to coordinate the serialization of the wait
 * for the "commit" message. (The write set of the transaction also needs to be
 * restart safe when it indicates that it has "prepared" so that a commit will
 * eventually succeed.)
 */
public interface ITransactionManager extends ITimestampService {

    /**
     * Create a new transaction.
     * <p>
     * The concurrency control algorithm is MVCC, so readers never block and
     * only write-write conflicts can arise. It is possible to register an index
     * with an {@link IConflictResolver} in order to present the application
     * with an opportunity to validate write-write conflicts using state-based
     * techniques (i.e., by looking at the records and timestamps and making an
     * informed decision).
     * 
     * @param level
     *            The isolation level. The following isolation levels are
     *            supported:
     *            <dl>
     *            <dt>{@link IsolationEnum#ReadCommitted}</dt>
     *            <dd>A read-only transaction in which data become visible
     *            within the transaction as concurrent transactions commit. This
     *            is suitable for very long read processes that do not require a
     *            fully consistent view of the data.</dd>
     *            <dt>{@link IsolationEnum#ReadOnly}</dt>
     *            <dd>A fully isolated read-only transaction with "read locks"
     *            on the resources required for that historical state of the
     *            database.</dd>
     *            <dt>{@link IsolationEnum#ReadWrite}</dt>
     *            <dd>A fully isolated read-write transaction.</dd>
     *            </dl>
     * 
     * @return The transaction start time, which serves as the unique identifier
     *         for the transaction.
     * 
     * FIXME Modify this to accept an optional timestamp so that the caller can
     * request a historical read with "read locks" on the resources required for
     * that historical committed state. When the optional timestamp is 0L the
     * transaction manager will assign a timestamp. For a read-only transaction
     * the timestamp will identify the most recent committed state of the
     * database (it will in fact be a distinct timestamp not less than the last
     * assigned commit time). For a read-write transaction it will be a distinct
     * timestamp not less than the last assigned commit time. When a historical
     * read is specified for a caller supplied timestamp an exception (or other
     * indication) should be given to the caller to indicate that they are not
     * the only client reading from that timestamp - what is at issue here is
     * that the "read locks" will be released as soon as the historical read is
     * completed - and any client can cause it to be completed, so it two
     * clients are using the same timestamp then one could inadvertently cause
     * the read locks to be released too early. Another way to handle this is to
     * return a timestamp distinct from any active historical read tx, but we
     * could run out of timestamps under some very high concurrency scenarios in
     * which case the caller might be delayed the start of their historical
     * read.
     * 
     * FIXME Drop support for {@link IsolationEnum#ReadCommitted} - that is
     * handled with {@link ITx#READ_COMMITTED} now.
     * 
     * @todo a heartbeat could be sent to all clients (and data services) from
     *       the {@link ITransactionManager} giving the most recent commit time
     *       on the database. this is useful for clients that want a
     *       light-weight historical read (no read locks, not read-committed).
     *       <P>
     *       this could be extended for the RDF inference case to give the last
     *       read-behind closure time as well.
     */
    public long newTx(IsolationEnum level);
    
    /**
     * Request commit of the transaction write set (synchronous).
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @throws IllegalStateException
     *             if the tx is not a known active transaction.
     */
    public long commit(long tx) throws ValidationError;

    /**
     * Request abort of the transaction write set.
     * 
     * @param tx
     *            The transaction identifier.
     *            
     * @throws IllegalStateException
     *             if the tx is not a known active transaction.
     */
    public void abort(long tx);

//    /**
//     * Obtain a lock on a named index (synchronous).
//     * <p>
//     * Note: Clients DO NOT use this method. Locks are NOT required during the
//     * active phrase of a transaction. Locks are only required during the commit
//     * phase of a transaction where they are used to coordinate execution with
//     * concurrent unisolated operations. The transaction manager automatically
//     * acquires the necessary locks during the commit and will cause those locks
//     * to be released before the transaction is complete.
//     * 
//     * @param tx
//     *            The transaction identifier.
//     * @param name
//     *            The resource name(s).
//     */
//    public void lock(long tx,String resource[]);
//
//    /**
//     * Release all locks held by the transaction.
//     * 
//     * @param tx
//     *            The transaction identifier.
//     */
//    public void releaseLocks(long tx);
    
    /**
     * Return interesting statistics about the transaction manager.
     * 
     * @todo use object or xml for telemetry.
     */
    public String getStatistics();

    /**
     * Invoked by tasks executing a transaction to notify the transaction
     * manager that they have written on the named resource(s). This information
     * is used when submitting the task(s) that will handle the validation and
     * commit (or abort) of the transaction.
     * 
     * @param startTime
     *            The transaction identifier.
     * @param resource
     *            The named resources.
     * 
     * @todo pass the {@link DataService} identifier also (or instead) to make
     *       it easier for a distributed transaction manager to locate the
     *       {@link DataService}s holding the write sets for the transaction?
     *       <p>
     *       Note: In a failover situation we need to know the specific
     *       resources on which the transaction wrote on each
     *       {@link DataService} unless the media replication strategy for the
     *       data services also sends along this piece of otherwise transient
     *       state.
     */
    public void wroteOn(long startTime, String[] resource);
    
}
