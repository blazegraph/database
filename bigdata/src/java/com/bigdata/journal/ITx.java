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
package com.bigdata.journal;

import java.util.Date;

import com.bigdata.btree.IIndex;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.service.IDataService;

/**
 * <p>
 * Interface for transaction state on the client.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITx {

    /**
     * The constant that SHOULD used as the timestamp for an <em>unisolated</em>
     * read-write operation. The value of this constant is ZERO (0L) -- this
     * value corresponds to <code>Wed Dec 31 19:00:00 EST 1969</code> when
     * interpreted as a {@link Date}.
     */
    public static final long UNISOLATED = 0L;
    
    /**
     * A constant that SHOULD used as the timestamp for <em>read-committed</em>
     * (non-transactional dirty reads) operations. The value of this constant is
     * MINUS ONE (-1L) -- this value corresponds to
     * <code>Wed Dec 31 18:59:59 EST 1969</code> when interpreted as a
     * {@link Date}.
     * <p>
     * If you want a scale-out index to be read consistent over multiple
     * operations, then use {@link IIndexStore#getLastCommitTime()} when you
     * specify the timestamp for the view. The index will be as of the specified
     * commit time and more recent commit points will not become visible.
     * <p>
     * {@link AbstractTask}s that run with read-committed isolation provide a
     * read-only view onto the most recently committed state of the indices on
     * which they read. However, when a process runs a series of
     * {@link AbstractTask}s with read-committed isolation the view of the
     * index in each distinct task will change if concurrenct processes commit
     * writes on the index (some constructs, such as the scale-out iterators,
     * provide read-consistent views for the last commit time). Further, an
     * index itself can appear or disappear if concurrent processes drop or
     * register that index.
     * <p>
     * A read-committed transaction imposes fewer constraints on when old
     * resources (historical journals and index segments) may be released. For
     * this reason, a read-committed transaction is a good choice when a
     * very-long running read must be performed on the database. Since a
     * read-committed transaction does not allow writes, the commit and abort
     * protocols are identical.
     * <p>
     * However, split/join/move operations can cause locators to become invalid
     * for read-committed (and unisolated) operations. For this reason, it is
     * often better to specify "read-consistent" semantics by giving the
     * lastCommitTime for the {@link IIndexStore}.
     */
    public static final long READ_COMMITTED = -1L;
    
    /**
     * The start time for the transaction as assigned by a centralized
     * transaction manager service. Transaction start times are unique and also
     * serve as transaction identifiers. Note that this is NOT the time at which
     * a transaction begins executing on a specific journal as the same
     * transaction may start at different moments on different journals and
     * typically will only start on some journals rather than all.
     * 
     * @return The transaction start time.
     * 
     * @todo rename since the sign indicates read-only vs read-write?
     */
    public long getStartTimestamp();

//    /**
//     * Return the timestamp assigned to this transaction by a centralized
//     * transaction manager service during its prepare+commit protocol. This
//     * timestamp is written into the tuples modified by the transaction when
//     * they are merged down onto the unisolated indices. Write-write conflicts
//     * for transactions are detected (during validation) based on those revision
//     * timestamps.
//     * 
//     * @return The revision timestamp assigned to this transaction.
//     * 
//     * @exception UnsupportedOperationException
//     *                unless the transaction is writable.
//     * 
//     * @exception IllegalStateException
//     *                if the transaction is writable but has not yet prepared (
//     *                the commit time is assigned when the transaction is
//     *                prepared).
//     */
//    public long getRevisionTimestamp();
    
    /**
     * Validate the write set of the named indices isolated transaction and
     * merge down that write set onto the corresponding unisolated indices but
     * DOES NOT commit the data. As a post-condition, the {@link RunState} of
     * the transaction will be {@link RunState#Prepared} iff successful and
     * {@link RunState#Aborted} otherwise.
     * <p>
     * For a single-phase commit the caller MUST hold an exclusive lock on the
     * unisolated indices on which this operation will write.
     * <p>
     * For a distributed transaction, the caller MUST hold a lock on the
     * {@link WriteExecutorService} for each {@link IDataService} on which the
     * transaction has written.
     * 
     * @param revisionTime
     *            The revision time assigned by a centralized transaction
     *            manager service -or- ZERO (0L) IFF the transaction is
     *            read-only.
     * 
     * @throws IllegalStateException
     *             if the transaction is not active. If the transaction is not
     *             complete, then it will be aborted.
     * @throws ValidationError
     *             If the transaction can not be validated. If this exception is
     *             thrown, then the transaction was aborted.
     */
    public void prepare(long revisionTime);

//    /**
//     * Merge down the write set of a transaction that has already been
//     * {@link #prepare(long)}d onto the unisolated indices. The caller MUST
//     * hold an exclusive lock on at least the unisolated indices on which this
//     * operation will write. For a distributed transaction, the caller MUST hold
//     * a lock on the {@link WriteExecutorService} for each {@link IDataService}
//     * on which the transaction has written before invoking either
//     * {@link #prepare(long)} or this method.
//     * 
//     * @param revisionTime
//     *            The revision time assigned by a centralized transaction
//     *            manager service -or- ZERO (0L) IFF the transaction is
//     *            read-only.
//     * 
//     * @throws IllegalStateException
//     *             If the transaction has not {@link #prepare(long) prepared}.
//     *             If the transaction is not already complete, then it is
//     *             aborted.
//     * 
//     * FIXME Since this no longer commits the backing store it must not change
//     * the state from {@link RunState#Prepared} to {@link RunState#Committed}.
//     * Instead, {@link #prepare(long)} and {@link #mergeDown()} should be
//     * combined into a single {@link #prepare(long)} method and the caller must
//     * be responsible for handshaking with the {@link ILocalTransactionManager}
//     * and this interface to make sure that the state of the {@link ITx} is
//     * update to reflect success or failure (or that the {@link ITx} is just
//     * removed from the {@link ILocalTransactionManager}'s tables so that its
//     * state is no longer visible).
//     * 
//     * @todo also note that merely letting the {@link ITx} become weakly
//     *       reachable is enough for it to release its resources, including any
//     *       temporary store.
//     */
//    public void mergeDown(final long revisionTime);

    /**
     * Abort the transaction.
     * 
     * @throws IllegalStateException
     *             if the transaction is already complete.
     */
    public void abort();

    /**
     * When true, the transaction will reject writes.
     */
    public boolean isReadOnly();
    
    /**
     * When true, the transaction has an empty write set.
     */
    public boolean isEmptyWriteSet();
    
    /**
     * A transaction is "active" when it is created and remains active until it
     * prepares or aborts.  An active transaction accepts READ, WRITE, DELETE,
     * PREPARE and ABORT requests.
     * 
     * @return True iff the transaction is active.
     */
    public boolean isActive();

    /**
     * A transaction is "prepared" once it has been successfully validated and
     * has fulfilled its pre-commit contract for a multi-stage commit protocol.
     * An prepared transaction accepts COMMIT and ABORT requests.
     * 
     * @return True iff the transaction is prepared to commit.
     */
    public boolean isPrepared();

    /**
     * A transaction is "complete" once has either committed or aborted. A
     * completed transaction does not accept any requests.
     * 
     * @return True iff the transaction is completed.
     */
    public boolean isComplete();

    /**
     * A transaction is "committed" iff it has successfully committed. A
     * committed transaction does not accept any requests.
     * 
     * @return True iff the transaction is committed.
     */
    public boolean isCommitted();

    /**
     * A transaction is "aborted" iff it has successfully aborted. An aborted
     * transaction does not accept any requests.
     * 
     * @return True iff the transaction is aborted.
     */
    public boolean isAborted();

    /**
     * Return an isolated view onto a named index. The index will be isolated at
     * the same level as this transaction. Changes on the index will be made
     * restart-safe iff the transaction successfully commits. Writes on the
     * returned index will be isolated in an {@link IsolatedFusedView}. Reads that
     * miss on the {@link IsolatedFusedView} will read through named index as of the
     * ground state of this transaction. If the transaction is read-only then
     * the index will not permit writes.
     * <p>
     * During {@link #prepare(long)}, the write set of each
     * {@link IsolatedFusedView} will be validated against the then current commited
     * state of the named index.
     * <p>
     * During {@link #mergeDown()}, the validated write sets will be merged down
     * onto the then current committed state of the named index.
     * 
     * @param name
     *            The index name.
     * 
     * @return The named index or <code>null</code> if no index is registered
     *         under that name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     *                
     * @exception IllegalStateException
     *                if the transaction is not active.
     */
    public IIndex getIndex(String name);
    
    /**
     * Return an array of the resource(s) (the named indices) on which the
     * transaction has written (the isolated index(s) that absorbed the writes
     * for the transaction).
     */
    public String[] getDirtyResource();
    
}
