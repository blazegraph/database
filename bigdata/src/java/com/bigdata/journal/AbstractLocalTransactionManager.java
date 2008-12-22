package com.bigdata.journal;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;

/**
 * Manages the client side of a transaction either for a standalone
 * {@link Journal} or for an {@link IDataService} in an
 * {@link IBigdataFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractLocalTransactionManager extends TimestampUtility
        implements ILocalTransactionManager {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(IJournal.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.isDebugEnabled();

    public AbstractLocalTransactionManager() {

    }

    /*
     * ILocalTransactionManager
     */
    
    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will remain in this collection until it has either successfully
     * prepared or aborted.
     * 
     * @todo config initial capacity and concurrency.
     */
    final protected Map<Long, ITx> activeTx = new ConcurrentHashMap<Long, ITx>();

    /**
     * A hash map containing all transactions that have prepared but not yet
     * either committed or aborted.
     * 
     * @todo use only a single map for all running tx?
     * 
     * @todo A transaction will be in this map only while it is actively
     *       committing, so this is always a "map" of one and could be replaced
     *       by a scalar reference (except that we may allow concurrent prepare
     *       and commit of read-only transactions).
     */
    final protected Map<Long, ITx> preparedTx = new ConcurrentHashMap<Long, ITx>();

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
    public void activateTx(final ITx tx) throws IllegalStateException {

        final Long timestamp = tx.getStartTimestamp();

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
    public void preparedTx(final ITx tx) throws IllegalStateException {

        final Long startTime = tx.getStartTimestamp();

        final ITx tx2 = activeTx.remove(startTime);

        if (tx2 == null)
            throw new IllegalStateException("Not active: tx=" + tx);

        assert tx == tx2;

        if (preparedTx.containsKey(startTime)) {

            throw new IllegalStateException("Already preparing: tx=" + tx);
        
        }

        preparedTx.put(startTime, tx);

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
    public void completedTx(final ITx tx) throws IllegalStateException {

        if (tx == null)
            throw new IllegalArgumentException();
        
        // @todo verify caller holds lock else IllegalMonitorStateException.
        
        if (!tx.isComplete())
            throw new IllegalStateException();

        final Long startTime = tx.getStartTimestamp();

        final ITx txActive = activeTx.remove(startTime);

        final ITx txPrepared = preparedTx.remove(startTime);

        if (txActive == null && txPrepared == null) {

            throw new IllegalStateException(
                    "Neither active nor being prepared: tx=" + tx);

        }

    }

    /**
     * Return the local state for a transaction.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @return The local state for the identified transaction -or-
     *         <code>null</code> if the start time is not mapped to either an
     *         active or prepared transaction.
     */
    public ITx getTx(final long tx) {

        /*
         * @todo lock for [activeTx] and [preparedTx] to prevent concurrent decl
         * or state change. the lock must also be used by the ITx so that both
         * lookup and state changes are atomic.
         */
        
        ITx localState = activeTx.get(tx);

        if (localState == null) {

            localState = preparedTx.get(tx);

        }

        return localState;

    }

    public boolean isOpen() {
        
        return open;
        
    }
    
    private boolean open = true;
    
    synchronized public void shutdown() {
        
        // Note: currently a NOP.
        
        if(!open) return;
        
        open = false;
        
    }

    synchronized public void shutdownNow() {

        // Note: currently a NOP.
        
        if(!open) return;
        
        open = false;
        
    }

    /**
     * Delay between attempts reach the remote service (ms).
     */
    final long delay = 10L;
    
    /**
     * #of attempts to reach the remote service.
     * 
     * Note: delay*maxtries == 1000ms of trying before we give up.
     * 
     * If this is not enough, then consider adding an optional parameter giving
     * the time the caller will wait and letting the StoreManager wait longer
     * during startup to discover the timestamp service.
     */
    final int maxtries = 100; 
    
    /**
     * Note: The reason for all this retry logic is to work around race
     * conditions during service startup (and possibly during service failover)
     * when the {@link ITimestampService} has not been discovered yet.
     */
    public long nextTimestamp() {

        final long begin = System.currentTimeMillis();
        
        IOException cause = null;

        int ntries;

        for (ntries = 1; ntries <= maxtries; ntries++) {

            try {

                final ITransactionService transactionService = getTransactionService();

                if (transactionService == null) {

                    log.warn("Service not discovered yet?");

                    try {

                        Thread.sleep(delay/* ms */);

                        continue;
                        
                    } catch (InterruptedException e2) {

                        throw new RuntimeException(
                                "Interrupted awaiting timestamp service discovery: "
                                        + e2);

                    }

                }

                return transactionService.nextTimestamp();
                
            } catch (IOException e) {

                log.warn("Problem with timestamp service? : ntries=" + ntries
                        + ", " + e, e);

                cause = e;

            }

        }

        final long elapsed = System.currentTimeMillis() - begin;

        final String msg = "Could not get timestamp after " + ntries
                + " tries and " + elapsed + "ms";

        log.error(msg, cause);

        throw new RuntimeException(msg, cause);
        
    }

    public void notifyCommit(final long commitTime) {
        
        final long begin = System.currentTimeMillis();
        
        IOException cause = null;

        int ntries;

        for (ntries = 1; ntries <= maxtries; ntries++) {

            try {

                final ITransactionService transactionService = getTransactionService();

                if (transactionService == null) {

                    log.warn("Service not discovered?");

                    try {

                        Thread.sleep(delay/* ms */);

                    } catch (InterruptedException e2) {

                        throw new RuntimeException(
                                "Interrupted awaiting timestamp service discovery: "
                                        + e2);

                    }

                    continue;

                }

                transactionService.notifyCommit(commitTime);

                return;

            } catch (IOException e) {

                log.warn("Problem with timestamp service? : ntries=" + ntries
                        + ", " + e, e);

                cause = e;

            }

        }

        final long elapsed = System.currentTimeMillis() - begin;

        final String msg = "Could not notify timestamp service after " + ntries
                + " tries and " + elapsed + "ms";

        log.error(msg, cause);

        throw new RuntimeException(msg, cause);
        
    }
    
    /**
     * Return interesting statistics about the transaction manager.
     */
    synchronized public CounterSet getCounters() {
        
        if (countersRoot == null) {

            countersRoot = new CounterSet();

            countersRoot.addCounter("#active", new Instrument<Integer>() {
                protected void sample() {
                    setValue(activeTx.size());
                }
            });

            countersRoot.addCounter("#prepared", new Instrument<Integer>() {
                protected void sample() {
                    setValue(preparedTx.size());
                }
            });

        }
        
        return countersRoot;
        
    }
    private CounterSet countersRoot;
    
}
