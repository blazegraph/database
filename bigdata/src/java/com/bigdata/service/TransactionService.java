/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 15, 2007
 */

package com.bigdata.service;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IsolationEnum;
import com.bigdata.journal.LocalTimestampService;
import com.bigdata.journal.RunState;
import com.bigdata.journal.ValidationError;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Centalized transaction manager service. In response to a client request, the
 * transaction manager will distribute prepare/commit or abort operations to all
 * data services on which writes were made by a transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see OldTransactionServer, which has lots of code and notes that bear on this
 *      implementation.
 * 
 * @todo robust/failover discoverable service.
 * 
 * @todo since data services can fail and start elsewhere, what we need to
 *       locate the data service is really the index name and key range (or
 *       perhaps the partition identifier) for the write. Based on that, we can
 *       always locate the correct data service instance. The
 *       {@link InetSocketAddress} is useful if we can invalidate it if the data
 *       service instance fails.
 * 
 * @todo test for transactions that have already been completed? that would
 *       represent a protocol error. we could maintain an LRU cache of completed
 *       transactions for this purpose.
 * 
 * @todo does the api need to be synchronized?
 * 
 * @todo track ground states so that we known when we can release old journals
 *       and index segments?
 */
public class TransactionService implements ITransactionManager, IServiceShutdown {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger(TransactionService.class);

    /**
     * Used to generate start time and commit time timestamps.
     */
    protected final ITimestampService timestampFactory = LocalTimestampService.INSTANCE;

    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will remain in this collection until it has completed (aborted
     * or committed).
     * 
     * @todo parametrize the capacity of the map.
     */
    final Map<Long, TxMetadata> activeTx = new ConcurrentHashMap<Long, TxMetadata>(
            10000);

    /**
     * A thread that serializes transaction commits.
     */
    final protected ExecutorService commitService = Executors
            .newSingleThreadExecutor(DaemonThreadFactory
                    .defaultThreadFactory());

    /**
     * Polite shutdown does not accept new requests and will shutdown once
     * the existing requests have been processed.
     */
    public void shutdown() {
        
        commitService.shutdown();
        
    }
    
    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon
     * as possible.
     */
    public void shutdownNow() {

        commitService.shutdownNow();
        
    }

    /*
     * ITransactionManager.
     */

    public long nextTimestamp() {
        
        return timestampFactory.nextTimestamp();
        
    }
    
    public long newTx(IsolationEnum level) {
        
        final long startTime = nextTimestamp();
        
        activeTx.put(startTime,new TxMetadata(level,startTime));
        
        return startTime;
        
    }

    /**
     * Abort the transaction (asynchronous).
     */
    public void abort(long startTime) {

        TxMetadata tx = activeTx.get(startTime);
        
        if (tx == null)
            throw new IllegalStateException("Unknown: " + startTime);

        if(!tx.isActive()) {
            throw new IllegalStateException("Not active: " + startTime);
        }

        // Note: do not wait for the task to run.
        commitService.submit(new AbortTask(tx));
        
    }

    /**
     * Commit the transaction (synchronous).
     * <p>
     * If a transaction has a write set, then this method does not return until
     * that write set has been made restart safe or the transaction has failed.
     */
    public long commit(long startTime) throws ValidationError {
        
        TxMetadata tx = activeTx.get(startTime);
        
        if (tx == null)
            throw new IllegalStateException("Unknown: " + startTime);

        if(!tx.isActive()) {
            throw new IllegalStateException("Not active: " + startTime);
        }

        final long commitTime = nextTimestamp();

        if(tx.isEmptyWriteSet()) {
            
            tx.runState = RunState.Committed;
         
            tx.commitTime = commitTime;
            
            activeTx.remove(startTime);
            
        }
        
        try {

            if (tx.isDistributed()) {

                // wait for the commit.
                commitService.submit(new DistributedCommitTask(tx)).get();

            } else {

                // wait for the commit.
                commitService.submit(new SimpleCommitTask(tx)).get();
                
            }

        } catch (InterruptedException ex) {

            // interrupted, perhaps during shutdown.
            throw new RuntimeException(ex);

        } catch (ExecutionException ex) {

            Throwable cause = ex.getCause();

            if (cause instanceof ValidationError) {

                throw (ValidationError) cause;

            }

            // this is an unexpected error.
            throw new RuntimeException(cause);

        }

        return commitTime;

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
     * 
     * @return true if the operation was successful. false if the transaction is
     *         not currently active (e.g., preparing, committed, or unknown).
     */
    public boolean activateTx(long tx, InetSocketAddress locator)
            throws IllegalStateException {

        Long timestamp = tx;

        TxMetadata md = activeTx.get(timestamp);
        
        if(md == null) {

            log.warn("Unknown: tx="+tx);

            return false;
            
        }
        
        if(!md.isActive()) {
            
            log.warn("Not active: tx="+tx);

            return false;
                        
        }

        md.addDataService(locator);
        
        return true;

    }

    /**
     * Metadata for the transaction state.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TxMetadata {
 
        public final IsolationEnum level;
        public final long startTime;
        public final boolean readOnly;
        private final int hashCode;
        private RunState runState = RunState.Active;
        private long commitTime = 0L;
        private Set<InetSocketAddress> writtenOn = new HashSet<InetSocketAddress>();
        
        public TxMetadata(IsolationEnum level, long startTime) {
            
            assert startTime != 0L;
            assert level != null;
            
            this.startTime = startTime;
            
            this.level = level;
            
            this.readOnly = level != IsolationEnum.ReadWrite;
            
            // pre-compute the hash code for the transaction.
            this.hashCode = Long.valueOf(startTime).hashCode();

        }

        /**
         * The hash code is based on the {@link #getStartTimestamp()}.
         */
        final public int hashCode() {
            
            return hashCode;
            
        }

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
        final public void addDataService(InetSocketAddress locator) {
            
            synchronized(writtenOn) {
                
                writtenOn.add(locator);
                
            }
            
        }

        final boolean isEmptyWriteSet() {
                        
            synchronized(writtenOn) {

                return writtenOn.isEmpty();
                
            }
            
        }
        
        final boolean isDistributed() {
            
            synchronized(writtenOn) {

                return writtenOn.size() > 1;
                
            }
            
        }
        
        /**
         * Returns a string representation of the transaction start time.
         */
        final public String toString() {
            
            return ""+startTime;
            
        }

        final public IsolationEnum getIsolationLevel() {
            
            return level;
            
        }
        
        final public boolean isReadOnly() {
            
            return readOnly;
            
        }
        
        final public boolean isActive() {
            
            return runState == RunState.Active;
            
        }
        
        final public boolean isPrepared() {
            
            return runState == RunState.Prepared;
            
        }
        
        final public boolean isComplete() {
            
            return runState == RunState.Committed || runState == RunState.Aborted;
            
        }

        final public boolean isCommitted() {
            
            return runState == RunState.Committed;
            
        }
     
        final public boolean isAborted() {
            
            return runState == RunState.Aborted;
            
        }
        
    }

    /*
     * TODO read-only transactions and read-write transactions that have not
     * declared any write sets should be handled exactly like abort.
     * 
     * read-write transactions that have written on a single journal can do
     * a simple (one phase) commit.
     * 
     * read-write transactions that have written on multiple journals must
     * use a 2-/3-phase commit protocol.  Once again, latency is critical
     * in multi-phase commits since the journals will be unable to perform
     * unisolated writes until the transaction either commits or aborts.
     */
    public static class SimpleCommitTask implements Callable<Object> {

        public SimpleCommitTask(TxMetadata tx) {
            
        }
        
        public Object call() throws Exception {

            // FIXME implement single point commit protocol.
            throw new UnsupportedOperationException();

        }
        
    }
    
    /*
     * @todo 2-/3-phase commit.
     */
    public static class DistributedCommitTask implements Callable<Object> {

        public DistributedCommitTask(TxMetadata tx) {
            
        }

        public Object call() throws Exception {

            // FIXME implement distributed commit protocol.
            throw new UnsupportedOperationException();

        }
        
    }
    
    /*
     * TODO read-only transactions can abort immediately since they do not
     * need to notify the journals synchronously (they only need to notify
     * them in order to guide resource reclaimation, which only applies for
     * a read-only tx).
     * 
     * fully isolated transactions can also abort immediately and simply
     * notify the journals asynchronously that they should abort that tx.
     * 
     * Note: fast notification is required in case the tx has already
     * prepared since the journal will be unable to process unisolated
     * writes until the tx either aborts or commits.
     * 
     * @todo who is responsible for changing the runState?  Are multiple
     * aborts silently ignored (presuming multiple clients)?
     */
    public static class AbortTask implements Callable<Object> {
        
        public AbortTask(TxMetadata tx) {
            
        }

        public Object call() throws Exception {

            // FIXME implement abort.
            throw new UnsupportedOperationException();
            
        }
        
    }

}
