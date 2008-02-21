package com.bigdata.journal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ReadOnlyFusedView;
import com.bigdata.btree.ReadOnlyIndex;
import com.bigdata.service.IDataService;

/**
 * Abstract base class for local transaction either when running a standalone
 * database.
 * 
 * @todo Most of the logic in this class can be reused to manage the local state
 *       for transactions executing on a specific {@link IDataService} when
 *       running a distributed database. However, you MUST override
 *       {@link #newTx(IsolationEnum)} so that it accept the given timestamp
 *       (presumably from a centralized transaction manager) rather than
 *       generating its own timestamps.
 *       <p>
 *       The distributed transaction manager itself requires somewhat more
 *       sophisticated logic.
 * 
 * @todo assert and coordinate read locks for transactions with the resource
 *       manager and release read locks once the transaction is no longer
 *       active.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractLocalTransactionManager implements
        ILocalTransactionManager, ITransactionManager {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(IJournal.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Object used to manage local resources.
     */
    private final IResourceManager resourceManager;

    private IConcurrencyManager concurrencyManager;
    
    /**
     * The object used to control access to the index resources.
     * 
     * @throws IllegalStateException
     *             if the object has not been set yet using
     *             {@link #setConcurrencyManager(IConcurrencyManager)}.
     */
    public IConcurrencyManager getConcurrencyManager() {
        
        if(concurrencyManager==null) {
            
            // Not assigned!
            
            throw new IllegalStateException();
            
        }
        
        return concurrencyManager;
        
    }

    public void setConcurrencyManager(IConcurrencyManager concurrencyManager) {

        if (concurrencyManager == null)
            throw new IllegalArgumentException();

        if (this.concurrencyManager != null)
            throw new IllegalStateException();

        this.concurrencyManager = concurrencyManager;
        
    }
    
    /**
     * Note: You MUST use {@link #setConcurrencyManager(IConcurrencyManager)}
     * after calling this constructor (the parameter can not be passed in since
     * there is a circular dependency between the {@link IConcurrencyManager}
     * and {@link #commit(long)} on this class, which requires access to the
     * {@link IConcurrencyManager} to submit a task).
     * 
     * @param resourceManager
     */
    public AbstractLocalTransactionManager(IResourceManager resourceManager) {

        if (resourceManager == null)
            throw new IllegalArgumentException();

        this.resourceManager = resourceManager;

    }

    /*
     * ILocalTransactionManager
     */
    
    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will remain in this collection until it has either successfully
     * prepared or aborted.
     */
    final Map<Long, ITx> activeTx = new ConcurrentHashMap<Long, ITx>();

    /**
     * A hash map containing all transactions that have prepared but not yet
     * either committed or aborted.
     * 
     * @todo A transaction will be in this map only while it is actively
     *       committing, so this is always a "map" of one and could be replaced
     *       by a scalar reference (except that we may allow concurrent prepare
     *       and commit of read-only transactions).
     */
    final Map<Long, ITx> preparedTx = new ConcurrentHashMap<Long, ITx>();

//    public int getActiveTxCount() {
//        
//        return activeTx.size();
//        
//    }
//
//    public int getPreparedTxCount() {
//        
//        return preparedTx.size();
//        
//    }
    
    /**
     * Notify the journal that a new transaction is being activated (starting on
     * the journal).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     * 
     * @todo test for transactions that have already been completed? that would
     *       represent a protocol error in the transaction manager service.
     */
    public void activateTx(ITx tx) throws IllegalStateException {

        Long timestamp = tx.getStartTimestamp();

        if (activeTx.containsKey(timestamp)) {

            throw new IllegalStateException("Already active: tx=" + tx);
            
        }

        if (preparedTx.containsKey(timestamp)) {

            throw new IllegalStateException("Already prepared: tx=" + tx);
            
        }

        activeTx.put(timestamp, tx);

    }

    /**
     * Notify the journal that a transaction has prepared (and hence is no
     * longer active).
     * 
     * @param tx
     *            The transaction
     * 
     * @throws IllegalStateException
     */
    public void prepared(ITx tx) throws IllegalStateException {

        Long id = tx.getStartTimestamp();

        ITx tx2 = activeTx.remove(id);

        if (tx2 == null)
            throw new IllegalStateException("Not active: tx=" + tx);

        assert tx == tx2;

        if (preparedTx.containsKey(id)) {

            throw new IllegalStateException("Already preparing: tx=" + tx);
        
        }

        preparedTx.put(id, tx);

    }

    /**
     * Notify the journal that a transaction is completed (either aborted or
     * committed).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     */
    public void completedTx(ITx tx) throws IllegalStateException {

        assert tx != null;
        assert tx.isComplete();

        Long id = tx.getStartTimestamp();

        ITx txActive = activeTx.remove(id);

        ITx txPrepared = preparedTx.remove(id);

        if (txActive == null && txPrepared == null) {

            throw new IllegalStateException(
                    "Neither active nor being prepared: tx=" + tx);

        }

    }

    /**
     * Lookup an active or prepared transaction (exact match).
     * 
     * @param startTime
     *            The start timestamp for the transaction.
     * 
     * @return The transaction with that start time or <code>null</code> if
     *         the start time is not mapped to either an active or prepared
     *         transaction.
     */
    public ITx getTx(long startTime) {

        ITx tx = activeTx.get(startTime);

        if (tx == null) {

            tx = preparedTx.get(startTime);

        }

        return tx;

    }

    public IIndex getIndex(String name, long timestamp) {
        
        if (name == null) {

            throw new IllegalArgumentException();

        }

        final boolean isTransaction = timestamp > ITx.UNISOLATED;
        
        final ITx tx; 
        {

            ITx tmp = activeTx.get(timestamp);
        
            if (tmp == null) {
    
                tmp = preparedTx.get(timestamp);

                if( tmp != null ) {
                    
                    log.warn("Transaction has prepared: index not usable: name="+name+", tx="+timestamp);
                    
                    return null;
                    
                }
                
            }
            
            tx = tmp;
        
        }
        
        if( isTransaction && tx == null ) {
        
            /*
             * Note: This will happen both if you attempt to use a transaction
             * identified that has not been registered or if you attempt to use
             * a transaction manager after the transaction has been either
             * committed or aborted.
             */
            
            log.warn("No such transaction: name=" + name + ", tx=" + tx);

            return null;
            
        }
        
        final boolean readOnly = (timestamp < ITx.UNISOLATED)
                || (isTransaction && tx.isReadOnly());

        final IIndex tmp;

        if (isTransaction) {

            /*
             * Isolated operation.
             * 
             * Note: The backing index is always a historical state of the named
             * index.
             */

            final IIndex isolatedIndex = tx.getIndex(name);

            if (isolatedIndex == null) {

                log.warn("No such index: name="+name+", tx="+timestamp);
                
                return null;

            }

            tmp = isolatedIndex;

        } else {
            
            /*
             * historical read -or- unisolated read operation.
             */

            if (readOnly) {

                final AbstractBTree[] sources = resourceManager
                        .getIndexSources(name, timestamp);

                if (sources == null) {

                    log.warn("No such index: name="+name+", timestamp="+timestamp);
                    
                    return null;

                }

                if (sources.length == 1) {

                    tmp = new ReadOnlyIndex(sources[0]);

                } else {

                    tmp = new ReadOnlyFusedView(sources);

                }
                            
            } else {
                
                /*
                 * Writable unisolated index.
                 * 
                 * Note: This is the "live" mutable index. This index is NOT
                 * thread-safe. A lock manager is used to ensure that at most
                 * one task has access to this index at a time.
                 */

                assert timestamp == ITx.UNISOLATED;
                
                final AbstractBTree[] sources = resourceManager
                        .getIndexSources(name, ITx.UNISOLATED);
                
                if (sources == null) {

                    log.warn("No such index: name="+name+", timestamp="+timestamp);
                    
                    return null;
                    
                }

                if (sources.length == 1) {

                    tmp = sources[0];
                    
                } else {
                    
                    tmp = new FusedView( sources );
                    
                }

            }

        }
        
        return tmp;

    }

//    public IIndex getIndex(String name, long ts) {
//
//        if (name == null) {
//
//            throw new IllegalArgumentException();
//
//        }
//
//        ITx tx = activeTx.get(ts);
//
//        if (tx == null) {
//
//            throw new IllegalStateException();
//
//        }
//
//        return tx.getIndex(name);
//
//    }

    /*
     * ITxCommitProtocol.
     */
    
    /*
     * ITxCommitProtocol
     */

    /**
     * FIXME modify to accept the desired start time for read-only transactions
     * and to assign an actual start time for read-only transactions that is GTE
     * to the most recent commit time that is NOT already in use by any active
     * transaction and that is strictly LT the current time (probed without
     * assignment). If necessary, this will cause the caller to block until a
     * suitable timestamp is available.
     */
    public long newTx(IsolationEnum level) {

        final ILocalTransactionManager transactionManager = this;
        
        switch (level) {
        
        case ReadOnly: {

            final long startTime = nextTimestamp();

            new Tx(transactionManager, resourceManager, startTime, true);

            return startTime;
        }

        case ReadWrite: {

            final long startTime = nextTimestamp();
            
            new Tx(transactionManager, resourceManager, startTime, false);
            
            return startTime;

        }

        default:
            throw new AssertionError("Unknown isolation level: " + level);
        }

    }
    
    /**
     * Abort a transaction (synchronous, low latency for read-only transactions
     * but aborts for read-write transactions are serialized since there may be
     * latency in communications with the transaction server or deletion of the
     * temporary backing store for the transaction).
     * 
     * @param ts
     *            The transaction identifier (aka start time).
     */
    public void abort(long ts) {

        ITx tx = getTx(ts);

        if (tx == null) {

            throw new IllegalStateException("No such tx: " + ts);
            
        }

        // abort is synchronous.
        tx.abort();

        /*
         * Note: We do not need to abort the pending group commit since nothing
         * is written by the transaction on the unisolated indices until it has
         * validated - and the validate/merge task is an unisolated write
         * operation.
         */

    }

    /**
     * Commit a transaction (synchronous).
     * <p>
     * Read-only transactions and transactions without write sets are processed
     * immediately and will have a commit time of ZERO (0L).
     * <p>
     * Transactions with non-empty write sets are placed onto the
     * {@link #writeService} and the caller will block until the transaction
     * either commits or aborts. For this reason, this method MUST be invoked
     * from within a worker thread for the transaction so that concurrent
     * transactions may continue to execute.
     * 
     * @param ts
     *            The transaction identifier (aka start time).
     * 
     * @return The transaction commit time -or- ZERO (0L) if the transaction was
     *         read-only or had empty write sets.
     * 
     * @exception ValidationError
     *                If the transaction could not be validated. A transaction
     *                that can not be validated is automatically aborted. The
     *                caller MAY re-execute the transaction.
     */
    public long commit(long ts) throws ValidationError {

        ITx tx = getTx(ts);

        if (tx == null) {

            throw new IllegalStateException("No such tx: " + ts);

        }

        /*
         * A read-only transaction can commit immediately since validation and
         * commit are basically NOPs.
         */

        if (tx.isReadOnly()) {

            // read-only transactions do not get a commit time.
            tx.prepare(0L);

            return tx.commit();

        }

        /*
         * A transaction with an empty write set can commit immediately since
         * validation and commit are basically NOPs (this is the same as the
         * read-only case.)
         */

        if (tx.isEmptyWriteSet()) {

            tx.prepare(0L);

            return tx.commit();

        }

        final IConcurrencyManager concurrencyManager = getConcurrencyManager();
        
        try {
            
            final AbstractTask task = new TxCommitTask(concurrencyManager, tx);

            final Long commitTime = (Long) concurrencyManager.getWriteService()
                    .submit(task).get();

            if (DEBUG) {

                log.debug("committed: startTime=" + tx.getStartTimestamp()
                        + ", commitTime=" + commitTime);

            }

            return commitTime;

        } catch (InterruptedException ex) {

            // interrupted, perhaps during shutdown.
            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            Throwable cause = ex.getCause();

            if (cause instanceof ValidationError) {

                throw (ValidationError) cause;

            }

            // this is an unexpected error.
            throw new RuntimeException(ex);

        }

    }

    public void wroteOn(long startTime, String[] resource) {
        
        /*
         * Ignored since the information is also in the local ITx.
         */
        
    }

    public String getStatistics() {
        
        return "#active=" + activeTx.size() +
             ", #prepared="+ preparedTx.size()
             ;
        
    }
    
    /**
     * This task is an UNISOLATED operation that validates and commits a
     * transaction known to have non-empty write sets.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class TxCommitTask extends AbstractTask {

        /**
         * The transaction that is being committed.
         */
        private final ITx tx;

        public TxCommitTask(IConcurrencyManager concurrencyManager, ITx tx) {

            super(concurrencyManager, ITx.UNISOLATED, tx.getDirtyResource());

            this.tx = tx;

        }

        /**
         * 
         * @return The commit time assigned to the transaction.
         */
        public Object doTask() throws Exception {

            /*
             * The commit time is assigned when we prepare the transaction.
             */

            final long commitTime = nextTimestamp();

            tx.prepare(commitTime);

            return Long.valueOf(tx.commit());

        }

    }

    public void shutdown() {
        
        // Note: currently a NOP.
        
    }

    public void shutdownNow() {

        // Note: currently a NOP.
        
    }
    
}
