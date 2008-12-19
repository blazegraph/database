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

package com.bigdata.service;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegment;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RunState;
import com.bigdata.journal.Tx;
import com.bigdata.journal.ValidationError;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.resources.StoreManager;
import com.bigdata.util.InnerCause;

/**
 * Centalized transaction manager service. In response to a client request, the
 * transaction manager will distribute prepare/commit or abort operations to all
 * data services on which writes were made by a transaction. The transaction
 * manager also provides global timestamps required for non-transactional commit
 * points and various other purposes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see OldTransactionServer, which has lots of code and notes that bear on this
 *      implementation.
 * 
 * @todo Note: The metadata state of a transaction is (a) abs(startTime); and
 *       (b) whether it is read-only or read-write (which is indicated by the
 *       sign of the transaction identifier).
 *       <p>
 *       The global transaction manager maintains additional state for
 *       transactions, including (a) their runState; and (b) the set of
 *       resources (names of index partitions) on which a read-write transaction
 *       has written;
 *       <p>
 *       The transaction state should also include a counter for the #of clients
 *       that have started work on a transaction in order to support distributed
 *       start/commit protocols. Alternatively, the same result could be
 *       achieved using a distributed barrier (ala zookeeper).
 * 
 * @todo Note: The correspondence between a transaction identifier and its start
 *       time and readOnly flag and the need to issue distinct (and for
 *       read-write transactions, strictly increasing) timestamps creates a
 *       contention for the distinct timestamps available to the transaction
 *       manager which can satisify the request for a new transaction
 *       identifier.
 *       <p>
 *       Under some circumstances the assignment of a read-only transaction
 *       identifier must be delayed until a distinct timestamp becomes available
 *       between the designed start time and the next commit point.
 *       <p>
 *       Likewise, there is an upper bound of one read-write transaction that
 *       may be created per millisecond (the resolution of the global timestamp
 *       service) and requests for new read-write transactions contend with
 *       request for global timestamps.
 * 
 * @todo Track which {@link IndexSegment}s and {@link Journal}s are required
 *       to support the {@link IsolatedFusedView}s in use by a {@link Tx}. The
 *       easiest way to do this is to name these by appending the transaction
 *       identifier to the name of the index partition, e.g., name#partId#tx. At
 *       that point the {@link StoreManager} will automatically track the
 *       resources. This also simplifies access control (write locks) for the
 *       isolated indices as the same {@link WriteExecutorService} will serve.
 *       However, with this approach {split, move, join} operations will have to
 *       be either deferred or issued against the isolated index partitions as
 *       well as the unisolated index partitions.
 *       <p>
 *       Make writes on the tx thread-safe (Temporary mode Journal rather than
 *       TemporaryStore).
 * 
 * @todo test for transactions that have already been completed? that would
 *       represent a protocol error. we could maintain an LRU cache of completed
 *       transactions for this purpose.
 * 
 * @todo The transaction server should make sure that time does not go backwards
 *       when it starts up (with respect to the last time that it issued). Note
 *       that {@link AbstractJournal#commit(long)} already protects against this
 *       problem.
 * 
 * @todo failover. the service instances will need to track active/committed
 *       transactions, complain if their clocks get out of alignment, and refuse
 *       to generate a timestamp that would go backwards when compared to the
 *       timestamp generated by the last master service.
 * 
 * @todo Read-only transactions do not have client-side state and clients are
 *       NOT notified when read-only transactions complete. However, a
 *       standalone {@link Journal} must keep running during shutdown until its
 *       read-only transactions complete. It achieves this goal using a simple
 *       counter of the #of open read-only transactions.
 */
abstract public class AbstractTransactionService extends TimestampService implements
        ITransactionService {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(AbstractTransactionService.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * Options understood by this service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends TimestampService.Options {
        
    }
    
    /**
     * If the transaction is read-only and a write operation was requested.
     */
    protected static final transient String ERR_READ_ONLY = "Read-only";
    
    /**
     * If the transaction is not known to this service.
     */
    protected static final transient String ERR_NO_SUCH = "Unknown transaction";
    
    /**
     * If a transaction is no longer active.
     */
    protected static final transient String ERR_NOT_ACTIVE = "Not active";
    
    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will remain in this collection until it has completed (aborted
     * or committed).
     * 
     * @todo config param for the initial capacity of the map.
     * @todo config for the concurrency rating of the map.
     */
    final protected ConcurrentHashMap<Long, TxState> activeTx = new ConcurrentHashMap<Long, TxState>();

    public AbstractTransactionService(final Properties properties) {
        
        super(properties);
        
    }

    /**
     * @todo We will need to distinguish more run states since some operations
     *       must continue during both {@link #shutdown()} and
     *       {@link #shutdownNow()}.
     */
    protected void assertOpen() {

        if (!isOpen())
            throw new IllegalStateException();

    }
    
    /**
     * Polite shutdown. New transactions will not start. This method will block
     * until existing transactions are complete (either aborted or committed).
     * 
     * FIXME implement
     * 
     * @todo Support a federation shutdown protocol. The transaction manager is
     *       notified that the federation will shutdown. At that point the
     *       transaction manager should refuse to start new transactions, but
     *       MUST continue to issue timestamps. Once no more transactions are
     *       active, a shutdown notice should be broadcast to the data services.
     *       Once the data services are down, the metadata service and the load
     *       balancer may be shutdown as well. When the transaction manager
     *       itself shuts down it must save the last assigned transaction commit
     *       time so that it can verify that time does not go backwards on
     *       restart.
     */
    synchronized public void shutdown() {
        
        super.shutdown();
        
    }
    
    /**
     * Fast shutdown (not immediate since it must abort active transactions).
     * <p>
     * New transactions will not start and active transactions will be aborted.
     * Transactions which are in the middle of a commit will execute normally
     * and may result in either commits or aborts (if the commit fails, e.g.,
     * due to validation errors).
     * 
     * FIXME implement.
     */
    synchronized public void shutdownNow() {

//        commitService.shutdownNow();

        super.shutdownNow();
        
    }
    
    /**
     * @todo write unit tests for fence posts for the assigned transaction
     *       identifers and the resolution of the correct commit time for both
     *       read-only and read-write transactions.
     * 
     * @todo write unit tests for distinct read-only transaction identifiers
     *       under heavy load (forces contention for the distinct identifiers
     *       and could lead to deadlocks if you try to hold more than one
     *       read-only tx at a time).
     */
    public long newTx(long timestamp) {

        if (timestamp == ITx.UNISOLATED) {

            /*
             * When timestamp is ZERO (0L), this simply returns the next
             * distinct timestamp (with its sign bit flipped).
             * 
             * Note: This is guarenteed to be a valid start time since it is LT
             * the next possible commit point for the database. When we
             * validate, we will read from [-startTime] and the journal will
             * identify the 1st commit point LTE [-startTime], which will be the
             * most recent commit point on the database as of the moment when we
             * assigned this transaction identifier.
             */

            final long startTime = -nextTimestamp();

            activeTx.put(startTime, new TxState(startTime));

            return startTime;

        }

        final long lastCommitTime;
        try {

            lastCommitTime = lastCommitTime();

        } catch (IOException ex) {

            /*
             * Note: This exception will never be thrown since we are the
             * service and we are just requesting a method on a concrete
             * subclass.
             */
            
            throw new RuntimeException(ex);

        }

        if (timestamp > lastCommitTime) {

            /*
             * You can't request a historical read for a timestamp which has not
             * yet been issued by this service!
             */
            
            throw new IllegalArgumentException("Timestamp is in the future.");

        }
        
        if (timestamp == ITx.READ_COMMITTED) {

            /*
             * This is a symbolic shorthand for a read-only transaction that
             * will read from the most recent commit point on the database.
             */

                timestamp = lastCommitTime;

            if (timestamp == 0L) {

                /*
                 * There are no commit points from which we can read.
                 */
                
                throw new RuntimeException("Nothing committed.");
                
            }

        }

        synchronized (startTimeLock) {

            /*
             * FIXME identify a distinct start time NOT in use by any
             * transaction that is LTE the specified timestamp and GT the first
             * commit point LT the specified timestamp (that is, any of the
             * timestamps which would read from the same commit point on the
             * database).
             * 
             * @todo we do not need to serialize all such requests, only those
             * requests that contend for timestamps reading from the same commit
             * point.
             * 
             * @todo in order to write this we need to maintain a log of the
             * historical commit times. That log can be in a transient BTree.
             * The head of the log can be truncated whenever we advance the
             * releaseTime (that is, we always remove an entry when a tx
             * completes and if the entry is the head of the log then it is the
             * earliest tx and we also advance the release time).
             */

            if (timestamp == lastCommitTime) {
              
                /*
                 * Special case.  We just return the next timestamp.
                 */
                
                return nextTimestamp();
                
            }
            
            throw new UnsupportedOperationException();

        }

    }

    /**
     * Lock serializes requests for a read-only transaction identifier.
     */
    private final Object startTimeLock = new Object();

    /**
     * Implementation must abort the tx on the journal (standalone) or on each
     * data service (federation) on which it has written.
     * 
     * @param tx
     *            The transaction identifier.
     */
    abstract protected void abortImpl(final long tx) throws Exception;

    /**
     * Implementation must either single-phase commit (standalone journal or a
     * transaction that only writes on a single data service) or 2-/3-phase
     * commit (distributed transaction running on a federation).
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @return The commit time for the transaction -or- ZERO (0L) if the
     *         transaction was read-only or had an empty write set.
     * 
     * @throws Exception
     *             if something else goes wrong. This will be (or will wrap) a
     *             {@link ValidationError} if validation fails.
     */
    abstract protected long commitImpl(final long tx) throws Exception;

    /**
     * FIXME Remove the transaction entry in the ordered set of running
     * transactions. If the transaction was the lowest entry in that ordered
     * set, then update the releaseTime to the now lowest member of that set.
     * Note that the ordered set only contains the absolute value of the
     * transaction identifers!
     * 
     * @todo edge case when the set is empty sets the releaseTime to zero, but
     *       minReleaseAge will prevent us from really releasing all state in
     *       the db.
     * 
     * @param tx
     *            The transaction identifier.
     */
    protected void updateReleaseTime(final long tx) {
        
    }
    
    /**
     * Abort the transaction (asynchronous).
     */
    public void abort(final long tx) {

        final TxState state = activeTx.get(tx);

        if (state == null)
            throw new IllegalStateException(ERR_NO_SUCH);
        
        state.lock.lock();

        try {

            if (!state.isActive()) {

                throw new IllegalStateException(ERR_NOT_ACTIVE);

            }

            if(!state.isReadOnly()) {
                
                try {

                    abortImpl(tx);
                    
                } catch (Throwable t) {
                    
                    throw new RuntimeException(t);
                    
                }
                
            }

            state.runState = RunState.Aborted;

            updateReleaseTime(tx);
            
        } finally {
            
            state.lock.unlock();

        }

    }

    public long commit(final long tx) throws ValidationError {

        final TxState state = activeTx.get(tx);

        if (state == null) {

            throw new IllegalStateException(ERR_NO_SUCH);

        }

        state.lock.lock();

        try {

            if (!state.isActive()) {

                throw new IllegalStateException(ERR_NOT_ACTIVE);

            }

            try {

                final long commitTime = commitImpl(tx);

                state.runState = RunState.Committed;

                updateReleaseTime(tx);
                
                return commitTime;
                
            } catch (Throwable t2) {

                try {

                    abortImpl(tx);
                    
                } catch (Throwable t3) {
                    
                    log.error(t3);
                    
                }
                
                if (InnerCause.isInnerCause(t2, ValidationError.class)) {

                    throw new ValidationError();

                }

                throw new RuntimeException(t2);

            }
             
        } finally {

            try {

                assert !state.isActive();

                assert state.isCommitted() || state.isAborted();
            
            } finally {

                state.lock.unlock();

            }
            
        }

    }

    /**
     * Notify the {@link ITransactionService} that a new transaction is being
     * activated on a {@link DataService} instance (the transaction is writing
     * on that {@link DataService}).
     * <p>
     * Note: The {@link DataService} is responsible for ensuring that notice of
     * writes is generated <strong>before</strong> the successful completion of
     * a task isolated by a read-write transaction which writes on an isolated
     * index. Notice MUST be received before the task commits so that we do not
     * risk lost writes by committing a tx before noticing its writes on a
     * {@link DataService}!
     * 
     * @param tx
     *            The transaction identifier (aka start time).
     * 
     * @param dataServiceUUID
     *            The UUID for the logical {@link DataService} instance on which
     *            the transaction is writing.
     */
    public void wroteOn(final long tx, final UUID dataServiceUUID)
            throws IllegalStateException {

        final TxState state = activeTx.get(tx);

        if (state == null) {

            throw new IllegalStateException(ERR_NO_SUCH);

        }

        state.lock.lock();

        try {

            if (state.isReadOnly()) {

                throw new IllegalStateException(ERR_READ_ONLY);

            }

            if (!state.isActive()) {

                throw new IllegalStateException(ERR_NOT_ACTIVE);

            }

            state.addDataService(dataServiceUUID);

        } finally {

            state.lock.unlock();

        }

    }

    /**
     * Metadata for the transaction state.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class TxState {

        /**
         * The transaction identifier.
         */
        public final long tx;
        
        /**
         * <code>true</code> iff the transaction is read-only.
         */
        public final boolean readOnly;

        /**
         * The run state of the transaction (only accessible while you are
         * holding the {@link #lock}.
         * 
         * @todo we don't need {@link RunState#Prepared} - that is captured by
         *       the act of prepare+commit.
         */
        private RunState runState = RunState.Active;
        
//        /**
//         * The commit time assigned to a read-write transaction iff it
//         * successfully commits and otherwise ZERO (0L).
//         */
//        private long commitTime = 0L;
        
        /**
         * The set of {@link DataService}s on which the transaction has written
         * and <code>null</code> if this is not a read-write transaction (we
         * only track data services on which the transaction writes).
         */
        private final Set<UUID/* logicalDataServiceUUID */> writtenOn;

        /**
         * The set of {@link DataService}s on which the transaction has
         * written.
         * 
         * @throws IllegalStateException
         *             if not a read-write transaction.
         */
        protected UUID[] getDataServiceUUIDs() {

            if (writtenOn == null)
                throw new IllegalStateException();
            
            return writtenOn.toArray(new UUID[] {});
            
        }
        
        /**
         * A per-transaction lock used to serialize operations on a given
         * transaction. You need to hold this lock for most of the operations on
         * this class, including any access to the {@link RunState}.
         */
        final protected ReentrantLock lock = new ReentrantLock();
        
        public TxState(final long startTime) {
            
            if (startTime == ITx.UNISOLATED)
                throw new IllegalArgumentException();
            
            this.tx = startTime;
            
            this.readOnly = startTime > 0;
                       
            // pre-compute the hash code for the transaction.
            this.hashCode = Long.valueOf(startTime).hashCode();

            this.writtenOn = readOnly ? null : new LinkedHashSet<UUID>();
            
        }

        /**
         * The hash code is based on the {@link #getStartTimestamp()}.
         */
        final public int hashCode() {
            
            return hashCode;

        }

        private final int hashCode;

        /**
         * True iff they are the same object or have the same start timestamp.
         * 
         * @param o
         *            Another transaction object.
         */
        final public boolean equals(ITx o) {

            return this == o || tx == o.getStartTimestamp();

        }

        /**
         * Declares a data service instance on which the transaction will write.
         * 
         * @param locator
         *            The locator for the data service instance.
         */
        final public void addDataService(final UUID dataServiceUUID) {

            assert lock.isHeldByCurrentThread();

            writtenOn.add(dataServiceUUID);

        }

        /*
         * Note: This will report true if there was an attempt to write on a
         * data service. If the write operation on the data service failed after
         * the transaction manager was notified, the write set would still be
         * empty. This is Ok as long as the prepare+commit protocol on the data
         * service does not reject empty write sets.
         */
        final boolean isEmptyWriteSet() {

            assert lock.isHeldByCurrentThread();

            return writtenOn.isEmpty();

        }

        final boolean isDistributed() {

            assert lock.isHeldByCurrentThread();

            return writtenOn.size() > 1;

        }

        /**
         * Returns a string representation of the transaction start time.
         */
        final public String toString() {

            return Long.toString(tx);

        }

        final public boolean isReadOnly() {

            return readOnly;

        }

        final public boolean isActive() {

            assert lock.isHeldByCurrentThread();

            return runState == RunState.Active;

        }

//        final public boolean isPrepared() {
//
//            assert lock.isHeldByCurrentThread();
//
//            return runState == RunState.Prepared;
//
//        }

        final public boolean isComplete() {

            assert lock.isHeldByCurrentThread();

            return runState == RunState.Committed
                    || runState == RunState.Aborted;

        }

        final public boolean isCommitted() {

            assert lock.isHeldByCurrentThread();

            return runState == RunState.Committed;

        }

        final public boolean isAborted() {

            assert lock.isHeldByCurrentThread();

            return runState == RunState.Aborted;

        }

    }

    /** NOP */
    @Override
    public AbstractTransactionService start() {
        
        return this;
        
    }

    @Override
    public Class getServiceIface() {

        return ITransactionService.class;
        
    }
    
}
