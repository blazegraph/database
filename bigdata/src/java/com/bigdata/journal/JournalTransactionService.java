/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Dec 18, 2008
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.DataService;

/**
 * Implementation for a standalone journal using single-phase commits.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class JournalTransactionService extends
        AbstractTransactionService {

    protected final Journal journal;

    /**
     * @param properties
     */
    public JournalTransactionService(final Properties properties,
            final Journal journal) {

        super(properties);

        this.journal = journal;

    }
    
    /**
     * Extended to register the new tx in the
     * {@link AbstractLocalTransactionManager}.
     */
    protected void activateTx(final TxState state) {

        super.activateTx(state);

        if (TimestampUtility.isReadWriteTx(state.tx)) {

            /*
             * Register read-write transactions with the local transaction
             * manager.
             */

            new Tx(journal.getLocalTransactionManager(), journal, state.tx);

        }

    }

    protected void deactivateTx(final TxState state) {

        super.deactivateTx(state);
                
        if (TimestampUtility.isReadWriteTx(state.tx)) {

            /*
             * Unregister read-write transactions.
             */

            final AbstractTx localState = journal.getLocalTransactionManager()
                    .getTx(state.tx);

            if (localState != null) {

                journal.getLocalTransactionManager().deactivateTx(localState);

            }

        }

    }

    protected long findCommitTime(final long timestamp) {

        final ICommitRecord commitRecord = journal.getCommitRecord(timestamp);

        if (commitRecord == null) {

            return -1L;

        }

        return commitRecord.getTimestamp();
        
    }

    protected long findNextCommitTime(long commitTime) {
        
        final ICommitRecord commitRecord = journal.getCommitRecordIndex()
                .findNext(commitTime);
        
        if(commitRecord == null) {
            
            return -1L;
            
        }
        
        return commitRecord.getTimestamp();
        
    }
    
    protected void abortImpl(final TxState state) {

        if(state.isReadOnly()) {
            
            /*
             * A read-only transaction.
             * 
             * Note: We do not maintain state on the client for read-only
             * transactions. The state for a read-only transaction is captured
             * by its transaction identifier and by state on the transaction
             * service, which maintains a read lock.
             */

            state.setRunState(RunState.Aborted);
            
            return;
            
        }

        try {

            /*
             * The local (client-side) state for this tx.
             */
            final AbstractTx localState = (AbstractTx) journal
                    .getLocalTransactionManager().getTx(state.tx);

            if (localState == null) {

                /*
                 * The client should maintain the local state of the transaction
                 * until the transaction service either commits or aborts the
                 * tx.
                 */
                
                throw new AssertionError("Local tx state not found: tx="
                        + state);

            }

            /*
             * Update the local state of the tx to indicate that it is aborted.
             * 
             * Note: We do not need to issue an abort to the journal since
             * nothing is written by the transaction on the unisolated indices
             * until it has validated - and the validate/merge task is an
             * unisolated write operation, so the task's write set will be
             * automatically discarded if it fails.
             */

            localState.abort();

        } finally {
            
            state.setRunState(RunState.Aborted);
            
        }

    }

    /**
     * Single-phase commit for a transaction (synchronous).
     * <p>
     * Read-only transactions and transactions without write sets are processed
     * immediately and will have a commit time of ZERO (0L).
     * <p>
     * For a transaction with a non-empty write set, an {@link ITx#UNISOLATED}
     * task is placed onto the {@link WriteExecutorService} and the caller will
     * block until the transaction either commits or aborts. That task names the
     * indices that were isolated by the transaction on the so that the task
     * will have exclusive access to those indices during the commit protocol.
     * That task will:
     * <ol>
     * <li> Validate the write sets of the transaction;</li>
     * <li> Merge down the validated write sets onto the live (unisolated
     * indices).</li>
     * </ol>
     * If the task succeeds, then return the commit time for the task.
     * Otherwise, the write set of the transaction will have been discarded and
     * the transaction will be marked as aborted.
     */
    protected long commitImpl(final TxState state) throws ExecutionException,
            InterruptedException {

        if(state.isReadOnly()) {
            
            /*
             * A read-only transaction.
             * 
             * Note: We do not maintain state on the client for read-only
             * transactions. The state for a read-only transaction is captured
             * by its transaction identifier and by state on the transaction
             * service, which maintains a read lock.
             */
            
            state.setRunState(RunState.Committed);

            return 0L;
            
        }
        
        final AbstractTx localState = (AbstractTx) journal
                .getLocalTransactionManager().getTx(state.tx);

        if (localState == null) {

            throw new AssertionError("Not in local tables: " + state);

        }

        {

            /*
             * A transaction with an empty write set can commit immediately
             * since validation and commit are basically NOPs (this is the same
             * as the read-only case.)
             * 
             * Note: We lock out other operations on this tx so that this
             * decision will be atomic.
             */

            localState.lock.lock();

            try {

                if (localState.isEmptyWriteSet()) {

                    /*
                     * Sort of a NOP commit. 
                     */
                    
                    localState.setRunState(RunState.Committed);

                    state.setRunState(RunState.Committed);
                    
                    journal.getLocalTransactionManager().deactivateTx(
                            localState);
                    
                    return 0L;

                }

            } finally {

                localState.lock.unlock();

            }

        }

        final IConcurrencyManager concurrencyManager = journal.getConcurrencyManager();

        final AbstractTask<Void> task = new SinglePhaseCommit(
                concurrencyManager, journal.getLocalTransactionManager(),
                localState);

        try {
            
            // submit and wait for the result.
            concurrencyManager.getWriteService().submit(task).get();

            /*
             * @todo these state changes need to be made inside of
             * SinglePhaseTask while it is holding the lock.
             */
            state.setRunState(RunState.Committed);
            
            journal.getLocalTransactionManager().deactivateTx(localState);

        } catch(Throwable t) {
            
            log.error(t.getMessage(),t);//@todo remove.
            
            state.setRunState(RunState.Aborted);
            
            journal.getLocalTransactionManager().deactivateTx(localState);

        }

        /*
         * Note: This is returning the commitTime set on the task when it was
         * committed as part of a group commit.
         */

        return task.getCommitTime();

    }

    /**
     * This task is an UNISOLATED operation that validates and commits a
     * transaction known to have non-empty write sets.
     * <p>
     * Note: DO NOT {@link AbstractTx#lock} while you submit this task as it
     * could cause a deadlock if there is a task ahead of you in the queue for
     * the same tx!
     * <p>
     * NOte: DO NOT use this task for a distributed transaction (one with writes
     * on more than one {@link DataService}) since it will fail to obtain a
     * coherent commit time for the transaction as a whole.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SinglePhaseCommit extends AbstractTask<Void> {

        /**
         * The transaction that is being committed.
         */
        private final AbstractTx state;

        private final ILocalTransactionManager localTransactionManager;
        
        public SinglePhaseCommit(final IConcurrencyManager concurrencyManager,
                final ILocalTransactionManager localTransactionManager,
                final AbstractTx state) {

            super(concurrencyManager, ITx.UNISOLATED, state.getDirtyResource());

            if (localTransactionManager == null)
                throw new IllegalArgumentException();
        
            if (state == null)
                throw new IllegalArgumentException();
            
            this.localTransactionManager = localTransactionManager;
            
            this.state = state;

        }

        public Void doTask() throws Exception {

            /*
             * Note: In this case the [revisionTime] will be LT the
             * [commitTime]. That's Ok as long as the issued revision times are
             * strictly increasing, which they are.
             */
            final long revisionTime = localTransactionManager.nextTimestamp();

            /*
             * Lock out other operations on this tx.
             */

            state.lock.lock();

            try {

                // Note: throws ValidationError.
                state.prepare(revisionTime);

            } finally {

                state.lock.unlock();

            }

            return null;
            
        }

    }

    /**
     * The last commit time from the current root block.
     */
    final public long lastCommitTime() {
        
        return journal.getRootBlockView().getLastCommitTime();
        
    }

    /**
     * Ignored since the {@link Journal} records the last commit time
     * in its root blocks.
     */
    public void notifyCommit(long commitTime) throws IOException {
    
        // NOP
        
    }

    /**
     * Ignored since the {@link Journal} can not release history.
     */
    public void setReleaseTime(long releaseTime) throws IOException {
        
        // NOP
        
    }
    
    /**
     * Always returns ZERO (0L).
     */
    @Override
    protected long getReleaseTime() {

        return 0L;
        
    }


    /**
     * Throws exception since distributed transactions are not used for a single
     * {@link Journal}.
     */
    public long prepared(long tx, UUID dataService) throws IOException {

        throw new UnsupportedOperationException();

    }

    /**
     * Throws exception since distributed transactions are not used for a single
     * {@link Journal}.
     */
    public boolean committed(long tx, UUID dataService) throws IOException {

        throw new UnsupportedOperationException();
        
    }

}
