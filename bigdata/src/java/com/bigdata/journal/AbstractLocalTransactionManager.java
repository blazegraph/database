package com.bigdata.journal;

import java.io.IOException;
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
abstract public class AbstractLocalTransactionManager implements
        ILocalTransactionManager {

    /**
     * Logger.
     */
    protected static final Logger log = Logger
            .getLogger(AbstractLocalTransactionManager.class);

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
    final private ConcurrentHashMap<Long, Tx> activeTx = new ConcurrentHashMap<Long, Tx>();

    /**
     * Notify the journal that a new transaction is being activated (starting on
     * the journal).
     * 
     * @param localState
     *            The transaction.
     * 
     * @throws IllegalStateException
     */
    public void activateTx(final Tx localState) throws IllegalStateException {

        if (localState == null)
            throw new IllegalArgumentException();

        localState.lock.lock();

        try {

            if (activeTx
                    .putIfAbsent(localState.getStartTimestamp(), localState) != null) {

                throw new IllegalStateException("Already in local table: tx="
                        + localState);

            }

        } finally {

            localState.lock.unlock();

        }

    }

    /**
     * Removes the transaction from the local tables.
     * 
     * @param localState
     *            The transaction.
     */
    protected void deactivateTx(final Tx localState)
            throws IllegalStateException {

        if (localState == null)
            throw new IllegalArgumentException();

        localState.lock.lock();

        try {

            if (!localState.isComplete())
                throw new IllegalStateException("Not complete: "+localState);

            // release any local resources.
            localState.releaseResources();

            if (activeTx.remove(localState.getStartTimestamp()) == null) {

                throw new IllegalStateException("Not in local tables: tx="
                        + localState);

            }
            
        } finally {

            localState.lock.unlock();
            
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
    public Tx getTx(final long tx) {

        return activeTx.get(tx);

    }

    public boolean isOpen() {
        
        return open;
        
    }
    
    private volatile boolean open = true;
    
    synchronized public void shutdown() {
        
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

        }
        
        return countersRoot;
        
    }
    private CounterSet countersRoot;
    
}
