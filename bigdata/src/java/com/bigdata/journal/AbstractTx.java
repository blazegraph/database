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
 * Created on Feb 27, 2007
 */

package com.bigdata.journal;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * An abstract base class that encapsulates the run state transitions and
 * constraints for transactions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTx implements ITx {

    protected static final Logger log = Logger.getLogger(AbstractTx.class);
    
    /*
     * Text for error messages.
     */
    final static String NOT_ACTIVE = "Not active";
    final static String NOT_PREPARED = "Transaction is not prepared";
    final static String NOT_COMMITTED = "Transaction is not committed";
    final static String IS_COMPLETE = "Transaction is complete";
    
    /**
     * This {@link Lock} is used to obtain exclusive access during certain
     * operations, including creating the temporary store and isolating a view
     * of a named index. Exclusive access is required since multiple concurrent
     * operations MAY execute for the same transaction.
     */
    protected ReentrantLock lock = new ReentrantLock();
    
    /**
     * The transaction uses the {@link Journal} for some handshaking in the
     * commit protocol and to locate the named indices that it isolates.
     */
    final protected AbstractJournal journal;
    
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
     * The commit time assigned to this transaction and zero if the transaction
     * has not prepared or is not writable.
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

    protected AbstractTx(AbstractJournal journal, long startTime, IsolationEnum level ) {
        
        if (journal == null)
            throw new IllegalArgumentException();
        
        assert startTime != 0L;
        
        this.journal = journal;
        
        this.startTime = startTime;

        this.readOnly = level != IsolationEnum.ReadWrite;;

        this.level = level;
        
        // pre-compute the hash code for the transaction.
        this.hashCode = Long.valueOf(startTime).hashCode();
        
        journal.activateTx(this);

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
    final public boolean equals(ITx o) {
        
        return this == o || startTime == o.getStartTimestamp();
        
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

    final public void abort() {

        lock.lock();

        try {

            if (isComplete())
                throw new IllegalStateException(IS_COMPLETE);

            try {

                runState = RunState.Aborted;

                journal.completedTx(this);

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

            journal.prepared(this);

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

                if (!readOnly) {

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

                    mergeOntoGlobalState();

                    // // Atomic commit.
                    // journal.commitNow(commitTime);

                }

                runState = RunState.Committed;

                journal.completedTx(this);

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
    protected void mergeOntoGlobalState() {
    
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
