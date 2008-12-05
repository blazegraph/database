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
 * Created on Feb 27, 2007
 */

package com.bigdata.journal;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.resources.ResourceManager;

/**
 * An abstract base class that encapsulates the run state transitions and
 * constraints for transactions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTx implements ITx {

    protected static final Logger log = Logger.getLogger(AbstractTx.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
    /*
     * Text for error messages.
     */
    final static protected String NOT_ACTIVE = "Not active";
    final static protected String NOT_PREPARED = "Transaction is not prepared";
    final static protected String NOT_COMMITTED = "Transaction is not committed";
    final static protected String IS_COMPLETE = "Transaction is complete";
    
    /**
     * This {@link Lock} is used to obtain exclusive access during certain
     * operations, including creating the temporary store and isolating a view
     * of a named index. Exclusive access is required since multiple concurrent
     * operations MAY execute for the same transaction.
     */
    protected ReentrantLock lock = new ReentrantLock();
    
    /**
     * Used for some handshaking in the commit protocol.
     */
    final protected ILocalTransactionManager transactionManager;
    
    /**
     * Used to locate the named indices that the transaction isolates.
     */
    final protected IResourceManager resourceManager;
    
    /**
     * The start startTime assigned to this transaction.
     * <p>
     * Note: Transaction {@link #startTime} and {@link #commitTime}s are
     * assigned by a global time service. The time service must provide unique
     * times for transaction start and commit timestamps and commit times for
     * unisolated {@link Journal#commit()}s.
     */
    final protected long startTime;
    
    /**
     * The pre-computed hash code for the transaction (based on the start time).
     */
    final private int hashCode;
    
    /**
     * The commit time assigned to this transaction and ZERO (0L) if the
     * transaction has not prepared or is not writable.
     */
    private long commitTime = 0L;
    
    /**
     * True iff the transaction is read only and will reject writes.
     */
    final protected boolean readOnly;
    
    /**
     * The type-safe enumeration representing the isolation level of this
     * transaction.
     */
    final protected IsolationEnum level;
    
    private RunState runState;

    protected AbstractTx(//
            final ILocalTransactionManager transactionManager,
            final IResourceManager resourceManager,//
            final long startTime,//
            final IsolationEnum level//
            ) {
        
        if (transactionManager == null)
            throw new IllegalArgumentException();
        
        if (resourceManager == null)
            throw new IllegalArgumentException();
        
        if (startTime == ITx.UNISOLATED)
            throw new IllegalArgumentException();

        if (startTime == ITx.READ_COMMITTED)
            throw new IllegalArgumentException();
        
        this.transactionManager = transactionManager;
        
        this.resourceManager = resourceManager;
        
        this.startTime = startTime;

        this.readOnly = level != IsolationEnum.ReadWrite;;

        this.level = level;
        
        // pre-compute the hash code for the transaction.
        this.hashCode = Long.valueOf(startTime).hashCode();
        
        transactionManager.activateTx(this);

        this.runState = RunState.Active;

        // report event.
        ResourceManager.openTx(startTime, level);

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
    final public boolean equals(final ITx o) {
        
        return this == o || (o != null && startTime == o.getStartTimestamp());
        
    }
    
    final public long getStartTimestamp() {
        
        return startTime;
        
    }
    
    final public long getCommitTimestamp() {

        if(readOnly) {

            throw new UnsupportedOperationException();

        }
        
        switch(runState) {
        case Active:
        case Aborted:
            throw new IllegalStateException();
        case Prepared:
        case Committed:
            /*
             * Note: A committed tx will have a zero commit time if it was
             * readOnly, readCommitted, or readWrite but did not write any data.
             */
            return commitTime;
        }
        
        throw new AssertionError();
        
    }
    
    /**
     * Returns a string representation of the transaction start time.
     */
    final public String toString() {
        
        return Long.toString(startTime);
        
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

    final public void abort() {

        lock.lock();

        try {

            if (isComplete())
                throw new IllegalStateException(IS_COMPLETE);

            try {

                runState = RunState.Aborted;

                transactionManager.completedTx(this);

                ResourceManager.closeTx(startTime, commitTime, true);

            } finally {

                releaseResources();

            }

        } finally {

            lock.unlock();

        }

    }

    final public void prepare(long commitTime) {

        lock.lock();

        try {

            if (!isActive()) {

                if (!isComplete()) {

                    abort();

                }

                throw new IllegalStateException(NOT_ACTIVE);

            }

            if (readOnly || isEmptyWriteSet()) {

                /*
                 * A read-only tx does not have a commit time. Likewise, we do
                 * not assign a commitTime to a readWrite transaction that does
                 * not write any data.
                 */

                assert commitTime == 0L;

            } else {

                try {

                    assert commitTime != 0L;

                    // save the assigned commit time.
                    this.commitTime = commitTime;

                    /*
                     * Validate against the current state of the various indices
                     * on write the transaction has written.
                     */

                    if (!validateWriteSets()) {

                        abort();

                        throw new ValidationError();

                    }

                } catch (ValidationError ex) {

                    throw ex;

                } catch (Throwable t) {

                    abort();

                    throw new RuntimeException("Unexpected error: " + t, t);

                }

            }

            transactionManager.prepared(this);

            runState = RunState.Prepared;

        } finally {

            lock.unlock();

        }
        
    }
    
    /**
     * Note: This does NOT commit the backing store. That is handled by the
     * {@link WriteExecutorService}.
     */
    final public long commit() {

        lock.lock();

        try {

            if (!isPrepared()) {

                if (!isComplete()) {

                    abort();

                }

                throw new IllegalStateException(NOT_PREPARED);

            }

            // The commitTime is zero unless this is a writable transaction.
            final long commitTime = readOnly ? 0L : getCommitTimestamp();

            try {

                if (!readOnly && !isEmptyWriteSet()) {

                    /*
                     * Merge each isolated index into the global scope. This
                     * also marks the slots used by the versions written by the
                     * transaction as 'committed'. This operation MUST succeed
                     * (at a logical level) since we have already validated
                     * (neither read-write nor write-write conflicts exist).
                     * 
                     * @todo Non-transactional operations on the global scope
                     * should be either disallowed entirely or locked out during
                     * the prepare-commit protocol when using transactions since
                     * they (a) could invalidate the pre-condition for the
                     * merge; and (b) uncommitted changes would be discarded if
                     * the merge operation fails. One solution is to use batch
                     * operations or group commit mechanism to dynamically
                     * create transactions from unisolated operations.
                     */

                    mergeOntoGlobalState(commitTime);

                    // // Atomic commit.
                    // journal.commitNow(commitTime);

                }

                runState = RunState.Committed;

                transactionManager.completedTx(this);

                ResourceManager.closeTx(startTime, commitTime, false);

            } catch (Throwable t) {

                /*
                 * If the operation fails then we need to discard any changes
                 * that have been merged down into the global state. Failure to
                 * do this will result in those changes becoming restart-safe
                 * when the next transaction commits!
                 * 
                 * Discarding registered committers is legal if we are observing
                 * serializability; that is, if no one writes on the global
                 * state for a restart-safe btree except mergeOntoGlobalState().
                 * When this constraint is observed it is impossible for there
                 * to be uncommitted changes when we begin to merge down onto
                 * the store and any changes may simply be discarded.
                 * 
                 * Note: we can not simply reload the current root block (or
                 * reset the nextOffset to be assigned) since concurrent
                 * transactions may be writing non-restart safe data on the
                 * store in their own isolated btrees.
                 */

                // journal.abort();
                abort();

                if (t instanceof RuntimeException)
                    throw (RuntimeException) t;

                throw new RuntimeException(t);

            } finally {

                releaseResources();

            }

            return commitTime;

        } finally {

            lock.unlock();

        }

    }

    /**
     * Invoked when a writable transaction prepares in order to validate its
     * write sets (one per isolated index). The default implementation is NOP.
     * 
     * @return true iff the write sets were validated.
     */
    protected boolean validateWriteSets() {
    
        assert lock.isHeldByCurrentThread();

        // NOP.
        
        return true;
        
    }
    
    /**
     * Invoked during commit processing to merge down the write set from each
     * index isolated by this transactions onto the corresponding unisolated
     * index on the database. This method invoked iff a transaction has
     * successfully prepared and hence is known to have validated successfully.
     * The default implementation is a NOP.
     */
    protected void mergeOntoGlobalState(final long commitTime) {
    
        assert lock.isHeldByCurrentThread();
        
    }
    
    /**
     * This method must be invoked any time a transaction completes ({@link #abort()}s
     * or {@link #commit()}s) in order to release resources held by that
     * transaction. The default implementation is a NOP and must be extended if
     * a transaction holds state.
     */
    protected void releaseResources() {
        
        assert lock.isHeldByCurrentThread();
        
        if(!isComplete()) {

            throw new IllegalStateException();
            
        }
        
        // NOP.
        
    }
    
}
