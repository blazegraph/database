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

import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegment;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RunState;
import com.bigdata.journal.Tx;
import com.bigdata.journal.ValidationError;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.resources.StoreManager;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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
 * 
 * @todo test for transactions that have already been completed? that would
 *       represent a protocol error. we could maintain an LRU cache of completed
 *       transactions for this purpose.
 * 
 * @todo support a shutdown protocol. The transaction manager is notified that
 *       the federation will shutdown. At that point the transaction manager
 *       should refuse to start new transactions. data services should quiese as
 *       transactions complete. after a timeout, a shutdown notice should be
 *       broadcast to the data services (and the metadata service). when the
 *       transaction manager itself shuts down it must save the last assigned
 *       transaction commit time so that it can verify that time does not go
 *       backwards on restart.
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
 */
abstract public class TransactionService extends TimestampService implements
        ITransactionManager, IServiceShutdown {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(TransactionService.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will remain in this collection until it has completed (aborted
     * or committed).
     * 
     * @todo config param for the initial capacity of the map.
     * @todo config for the concurrency rating of the map.
     */
    final protected ConcurrentHashMap<Long, TxMetadata> activeTx = new ConcurrentHashMap<Long, TxMetadata>();

    /**
     * A thread that serializes transaction commits.
     * 
     * @todo we do not need to use this thread. The serialization should come
     *       from ordered locks based on the resources (data services on which
     *       the tx has written).
     *       <p>
     *       Aborts do not need to be serialized at all, but aborts for a tx on
     *       a given data service can not proceed while a commit for another tx
     *       on that data service is in progress (part of the 2/3-phase
     *       protocol).
     */
    final protected ExecutorService commitService = Executors
            .newSingleThreadExecutor(new DaemonThreadFactory
                    (getClass().getName()+".commitService"));

    public TransactionService(Properties properties) {
        
        super(properties);
        
    }
    
    public boolean isOpen() {
        
        return ! commitService.isShutdown();
        
    }
    
    /**
     * Polite shutdown does not accept new requests and will shutdown once
     * the existing requests have been processed.
     */
    synchronized public void shutdown() {
        
        commitService.shutdown();

        // @todo await shutdown!
        
    }
    
    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon
     * as possible.
     */
    synchronized public void shutdownNow() {

        commitService.shutdownNow();
        
    }
    
    /*
     * ITransactionManager.
     */

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

            activeTx.put(startTime, new TxMetadata(startTime));

            return startTime;

        }

        if (timestamp == ITx.READ_COMMITTED) {

            /*
             * This is a symbolic shorthand for a read-only transaction that
             * will read from the most recent commit point on the database.
             */

            timestamp = lastCommitTime();

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
             * releaseTime.
             */

            throw new UnsupportedOperationException();

        }

    }

    /**
     * Lock serializes requests for a read-only transaction identifier.
     */
    private final Object startTimeLock = new Object();

    /**
     * Abort the transaction (asynchronous).
     */
    public void abort(final long startTime) {

        final TxMetadata tx = activeTx.get(startTime);

        if (tx == null)
            throw new IllegalStateException("Unknown: " + startTime);

        tx.lock.lock();

        try {

            if (!tx.isActive()) {

                throw new IllegalStateException("Not active: " + startTime);

            }

            try {

                commitService.submit(new AbortTask(tx)).get();

            } catch (Throwable t) {

                throw new RuntimeException(t);

            }

        } finally {

            tx.lock.unlock();

        }

    }

    /**
     * Commit the transaction (synchronous).
     * <p>
     * If a transaction has a write set, then this method does not return until
     * that write set has been made restart safe or the transaction has failed.
     */
    public long commit(final long startTime) throws ValidationError {

        final TxMetadata tx = activeTx.get(startTime);

        if (tx == null) {

            throw new IllegalStateException("Unknown: " + startTime);

        }

        tx.lock.lock();

        try {

            if (!tx.isActive()) {

                throw new IllegalStateException("Not active: " + startTime);

            }

            if (tx.isEmptyWriteSet()) {

                /*
                 * Empty write set.
                 */
                
                tx.runState = RunState.Committed;

                tx.commitTime = nextTimestamp();

                activeTx.remove(startTime);

                return tx.commitTime;
                
            }

            try {

                if (tx.isDistributed()) {

                    // wait for the commit.
                    return commitService.submit(new DistributedCommitTask(tx))
                            .get();

                } else {

                    // wait for the commit
                    return commitService.submit(new SimpleCommitTask(tx)).get();

                }

            } catch (InterruptedException ex) {

                // interrupted, perhaps during shutdown.
                throw new RuntimeException(ex);

            } catch (ExecutionException ex) {

                final Throwable cause = InnerCause.getInnerCause(ex,
                        ValidationError.class);

                if (cause != null) {

                    throw (ValidationError) cause;

                }

                // this is an unexpected error.
                throw new RuntimeException(cause);

            }

        } finally {

            tx.lock.unlock();

            assert !tx.isActive();

            assert tx.isCommitted() || tx.isAborted();

            if (!tx.isCommitted()) {

                // commitTime is zero unless commit was successful.
                assert tx.commitTime == 0L;
                
            }
            
        }

    }

    /**
     * Notify the journal that a new transaction is being activated on a data
     * service instance (starting to write on that data service).
     * 
     * @param tx
     *            The transaction identifier (aka start time).
     * 
     * @param locator
     *            The locator for the data service instance on which the
     *            transaction has begun writing.
     */
    public void wroteOn(final long tx, final UUID dataServiceUUID)
            throws IllegalStateException {

        final TxMetadata md = activeTx.get(tx);

        if (md == null) {

            throw new IllegalStateException("Unknown: tx=" + tx);

        }

        md.lock.lock();

        try {

            if (md.isReadOnly()) {

                throw new IllegalStateException("Read-only: tx=" + tx);

            }

            if (!md.isActive()) {

                throw new IllegalStateException("Not active: tx=" + tx);

            }

            md.addDataService(dataServiceUUID);

        } finally {

            md.lock.unlock();

        }

    }

    /**
     * Metadata for the transaction state.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class TxMetadata {

        /**
         * The transaction identifier. For a read-only transaction, this is the
         * start time. For a read-write transaction, the ground state from which
         * the transaction will read is <code>-startTime</code>.
         */
        public final long startTime;
        
        /**
         * <code>true</code> iff the transaction is read-only.
         */
        public final boolean readOnly;

        /**
         * The run state of the transaction (only accessible while you are
         * holding the {@link #lock}.
         */
        private RunState runState = RunState.Active;
        
        /**
         * The commit time assigned to a read-write transaction iff it
         * successfully commits and otherwise ZERO (0L).
         */
        private long commitTime = 0L;
        
        /**
         * The set of {@link DataService}s on which the transaction has written (<code>null</code>
         * if not a read-write transaction).
         */
        private final Set<UUID/* logicalDataServiceUUID */> writtenOn;

        /**
         * A per-transaction lock used to serialize operations on a given
         * transaction. You need to hold this lock for most of the operations on
         * this class, including any access to the {@link RunState}.
         */
        final protected ReentrantLock lock = new ReentrantLock();
        
        public TxMetadata(final long startTime) {
            
            if (startTime == ITx.UNISOLATED)
                throw new IllegalArgumentException();
            
            this.startTime = startTime;
            
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
            
            return this == o || startTime == o.getStartTimestamp();
            
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
            
            return Long.toString(startTime);
            
        }

        final public boolean isReadOnly() {
            
            return readOnly;
            
        }
        
        final public boolean isActive() {

            assert lock.isHeldByCurrentThread();

            return runState == RunState.Active;
            
        }
        
        final public boolean isPrepared() {

            assert lock.isHeldByCurrentThread();

            return runState == RunState.Prepared;
            
        }
        
        final public boolean isComplete() {

            assert lock.isHeldByCurrentThread();

            return runState == RunState.Committed || runState == RunState.Aborted;
            
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

    /**
     * Prepare and commit a read-write transactions that has written on a single
     * data service.
     */
    private class SimpleCommitTask implements Callable<Long> {

        private final TxMetadata tx;
        
        public SimpleCommitTask(final TxMetadata tx) {
            
            if (tx == null)
                throw new IllegalArgumentException();
            
            this.tx = tx;
            
        }
        
        /**
         * 
         * @return The assigned commit time.
         */
        public Long call() throws Exception {

            assert tx.lock.isHeldByCurrentThread();
            
            final UUID[] uuids = tx.writtenOn.toArray(new UUID[] {});

            if (uuids.length != 1)
                throw new AssertionError();

            final UUID serviceUUID = uuids[0];

            final IDataService dataService = getFederation().getDataService(
                    serviceUUID);

            /*
             * @todo In order to further reduce latency this could be combined
             * in a single prepare-commit message, or we could just issue the
             * commit() and have the prepare be implicit if the tx had not yet
             * prepared (so commit could also through a validation error).
             */

            try {

                dataService.prepare(tx.startTime);

                tx.runState = RunState.Prepared;

            } catch (Throwable t) {
            
                try {
                
                    dataService.abort(tx.startTime);
                    
                } catch (Throwable t2) {
                    
                    log.error(t2, t2);
                    
                }

                tx.runState = RunState.Aborted;
                
                if (t instanceof ValidationError)
                    throw (ValidationError) t;

                throw new RuntimeException(t);
            
            }

            try {

                final long commitTime = nextTimestamp();

                dataService.commit(tx.startTime, commitTime);

                tx.runState = RunState.Committed;

                tx.commitTime = commitTime;
                
                return commitTime;

            } catch (Throwable t) {
                
                try {
                
                    dataService.abort(tx.startTime);
                    
                } catch (Throwable t2) {
                    
                    log.error(t2, t2);
                    
                }

                tx.runState = RunState.Aborted;
                
                throw new RuntimeException(t);
            
            }
            
        }
        
    }
    
    /**
     * Prepare and commit a read-write transaction that has written on more than
     * one data service.
     * <p>
     * Note: read-write transactions that have written on multiple journals must
     * use a 2-/3-phase commit protocol. Latency is critical in multi-phase
     * commits since the journals will be unable to perform unisolated writes
     * until the transaction either commits or aborts.
     */
    private class DistributedCommitTask implements Callable<Long> {

        private final TxMetadata tx;
        
        public DistributedCommitTask(final TxMetadata tx) {

            if (tx == null)
                throw new IllegalArgumentException();
            
            this.tx = tx;
            
        }

        /**
         * 
         * @return The assigned commit time.
         */
        public Long call() throws Exception {

            assert tx.lock.isHeldByCurrentThread();
            
            final UUID[] uuids = tx.writtenOn.toArray(new UUID[]{});

            /*
             * @todo issue prepare messages concurrently to reduce latency,
             * collecting all exceptions and reporting them all back to the
             * client. If the exceptions are just validation errors then we can
             * summarize since the client does not need to know about the
             * details, just that the tx could be be validated and hence was
             * aborted.
             * 
             * @todo if any prepare messages fail, then ALL data services must
             * abort (since all services have write sets that need to be
             * discarded).
             * 
             * @todo prepare might well flush any writes not related to the tx
             * by gaining an exclusive write service lock, forcing a commit of
             * any running tasks, and then preparing and continuing to hold the
             * lock until a commit message is received or a timeout occurs.
             */
            try {

                for (UUID uuid : uuids) {

                    final IDataService dataService = getFederation()
                            .getDataService(uuid);

                    dataService.prepare(tx.startTime);

                }
                
                tx.runState = RunState.Prepared;
                
            } catch (Throwable t) {

                tx.runState = RunState.Aborted;
                
                for (UUID uuid : uuids) {

                    try {

                        final IDataService dataService = getFederation()
                                .getDataService(uuid);

                        dataService.abort(tx.startTime);
                        
                    } catch (Throwable t2) {
                        
                        log.error(t2, t2);
                        
                    }

                }

                if (t instanceof ValidationError)
                    throw (ValidationError) t;
                
                throw new RuntimeException(t);
                
            }

            /*
             * @todo issue commit messages concurrently to reduce latency
             * 
             * @todo if any commit messages fail, then we have a problem since
             * the data may be restart safe on some of the journals. A three
             * phase commit would prevent further commits by the journal until
             * all journals had successfully committed and would rollback the
             * journals to the prior commit points (touching the root block to
             * do this) if any journal failed to commit.
             */

            final long commitTime = nextTimestamp();

            try {

                for (UUID uuid : uuids) {

                    final IDataService dataService = getFederation()
                            .getDataService(uuid);

                    dataService.commit(tx.startTime, commitTime);

                }

                tx.runState = RunState.Committed;

                tx.commitTime = commitTime;

                return commitTime;

            } catch (Throwable t) {

                tx.runState = RunState.Aborted;

                for (UUID uuid : uuids) {

                    try {

                        final IDataService dataService = getFederation()
                                .getDataService(uuid);

                        dataService.abort(tx.startTime);

                    } catch (Throwable t2) {

                        log.error(t2, t2);

                    }

                }

                throw new RuntimeException(t);

            }

        }
        
    }
    
    /**
     * Abort for a read-write transaction.
     */
    private class AbortTask implements Callable<Void> {
        
        private final TxMetadata tx;
        
        public AbortTask(final TxMetadata tx) {
            
            if (tx == null)
                throw new IllegalArgumentException();
            
            this.tx = tx;
            
        }

        public Void call() throws Exception {

            assert tx.lock.isHeldByCurrentThread();
            
            assert tx.isActive();
            
            final UUID[] uuids = tx.writtenOn.toArray(new UUID[]{});

            tx.runState = RunState.Aborted;
            
            for (UUID uuid : uuids) {

                try {

                    final IDataService dataService = getFederation()
                            .getDataService(uuid);

                    dataService.abort(tx.startTime);

                } catch (Throwable t2) {

                    log.error(t2, t2);

                }
                
            }
            
            return null;
            
        }
        
    }

    /**
     * Return the {@link CounterSet}.
     */
    synchronized public CounterSet getCounters() {
        
        if (countersRoot == null) {

            countersRoot = new CounterSet();

            countersRoot.addCounter("#active", new Instrument<Integer>() {
                protected void sample() {
                    setValue(activeTx.size());
                }
            });

//            countersRoot.addCounter("#prepared", new Instrument<Integer>() {
//                protected void sample() {
//                    setValue(preparedTx.size());
//                }
//            });

        }
        
        return countersRoot;
        
    }
    private CounterSet countersRoot;

}
