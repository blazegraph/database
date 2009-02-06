/*

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
 * Created on Oct 13, 2006
 */

package com.bigdata.journal;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexSegment;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;

/**
 * <p>
 * A read-write transaction.
 * <p>
 * A transaction is a context in which the application can access and perform
 * operations on fully named indices. Writes on the named indices accessed by
 * the transaction are accumulated in a {@link IsolatedFusedView}s. In order to
 * commit, the write set of the transaction must be validated against the then
 * current state of the corresponding unisolated indices and then merge down
 * onto those indices, applying the specified <i>revisionTime</i>. The
 * transaction MUST have an exclusive lock on the named indices on which it will
 * write from the that it begins validation until after the commit or abort of
 * the transaction. This is necessary in order to ensure that concurrent writers
 * can not invalidate the conditions under which validation is performed.
 * </p>
 * <p>
 * The write set of a transaction is written onto a {@link TemporaryRawStore}.
 * Therefore the size limit on the transaction write set is currently 2G, but
 * the transaction will be buffered in memory until the store exceeds its write
 * cache size and creates a backing file on the disk. The store is closed and
 * any backing file is deleted as soon as the transaction completes.
 * </p>
 * <p>
 * Each {@link IsolatedFusedView} is local to a transaction and is backed by the
 * temporary store for that transaction. This means that concurrent transactions
 * can execute without synchronization (real concurrency) up to the point where
 * they {@link #prepare()}. We do not need a read-lock on the indices isolated
 * by the transaction since they are <em>historical</em> states that will not
 * receive concurrent updates.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo javadoc update for temporary stores (shared temporary journals) and
 *       concurrency limits (concurrent operations on the same transaction with
 *       a partial order imposed by locks on the isolated indices and concurrent
 *       operations on different transactions).
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
 * @todo Modify the isolated indices use a delegation strategy so that I can
 *       trap attempts to access an isolated index once the transaction is no
 *       longer active? Define "active" as up to the point where a "commit" or
 *       "abort" is _requested_ for the tx. (Alternative, extend the close()
 *       model for an index to not permit the re-open of an isolated index after
 *       the tx commits and then just close the btree absorbing writes for the
 *       isolated index when we are releasing our various resources. The
 *       isolated index will thereafter be unusable, which is what we want.)
 */
public class Tx implements ITx {

    protected static final Logger log = Logger.getLogger(Tx.class);
    
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
     * <p>
     * Note: This is exposed to the {@link DataService}.
     */
    final public ReentrantLock lock = new ReentrantLock();
    
    /**
     * Used for some handshaking in the commit protocol.
     */
    final protected AbstractLocalTransactionManager localTransactionManager;
    
    /**
     * Used to locate the named indices that the transaction isolates.
     */
    final protected IResourceManager resourceManager;
    
    /**
     * The start startTime assigned to this transaction.
     * <p>
     * Note: Transaction {@link #startTime} and {@link #revisionTime}s are
     * assigned by the {@link ITransactionService}.
     */
    final protected long startTime;
    
    /**
     * The pre-computed hash code for the transaction (based on the start time).
     */
    final private int hashCode;
    
    /**
     * The revisionTime assigned to the transaction when it was validated and
     * merged down onto the global state.
     */
    private long revisionTime = 0L;

    /**
     * The <i>revisionTime</i> assigned to the transaction when it was
     * validated and merged down onto the global state.
     * 
     * @see #mergeOntoGlobalState(long)
     */
    public long getRevisionTime() {
        
        return revisionTime;
        
    }
    
    private RunState runState;

    /**
     * A temporary store used to hold write sets for read-write transactions. It
     * is null if the transaction is read-only and will remain null in any case
     * until its first use.
     */
    private IRawStore tmpStore = null;

    /**
     * Indices isolated by this transactions.
     * <p>
     * Note: This must be thread-safe to support concurrent operations for the
     * same transaction (however, those operations will be serialized if they
     * declare any named indices in common).
     */
    private ConcurrentHashMap<String, ILocalBTreeView> indices = new ConcurrentHashMap<String, ILocalBTreeView>();

    /**
     * Create a transaction reading from the most recent committed state not
     * later than the specified startTime.
     * <p>
     * Note: For an {@link IBigdataFederation}, a transaction does not start
     * execution on all {@link IDataService}s at the same moment. Instead, the
     * transaction startTime is assigned by the {@link ITransactionService} and
     * then provided each time an {@link ITx} must be created for isolatation of
     * resources accessible on a {@link IDataService}.
     * 
     * @param transactionManager
     *            The local (client-side) transaction manager.
     * @param resourceManager
     *            Provides access to named indices that are isolated by the
     *            transaction.
     * @param startTime
     *            The transaction identifier
     */
    public Tx(//
            final AbstractLocalTransactionManager localTransactionManager,//
            final IResourceManager resourceManager, //
            final long startTime//
    ) {

        if (localTransactionManager == null)
            throw new IllegalArgumentException();

        if (resourceManager == null)
            throw new IllegalArgumentException();

        if (!TimestampUtility.isReadWriteTx(startTime)) {

            /*
             * Note: We only maintain local state for read-write transactions.
             */

            throw new IllegalArgumentException();

        }

        this.localTransactionManager = localTransactionManager;

        this.resourceManager = resourceManager;

        this.startTime = startTime;

        this.runState = RunState.Active;

        // pre-compute the hash code for the transaction.
        this.hashCode = Long.valueOf(startTime).hashCode();

        localTransactionManager.activateTx(this);

        // report event.
        ResourceManager.openTx(startTime);

    }
    
    /**
     * Change the {@link RunState}.
     * 
     * @param newval
     *            The new {@link RunState}.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the state transition is not allowed.
     * 
     * @see RunState#isTransitionAllowed(RunState)
     */
    public void setRunState(final RunState newval) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (newval == null)
            throw new IllegalArgumentException();
        
        if (!runState.isTransitionAllowed(newval)) {

            throw new IllegalStateException("runState=" + runState
                    + ", newValue=" + newval);

        }

        this.runState = newval;
        
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
    
//    final public long getRevisionTimestamp() {
//
//        if(readOnly) {
//
//            throw new UnsupportedOperationException();
//
//        }
//        
//        switch(runState) {
//        case Active:
//        case Aborted:
//            throw new IllegalStateException();
//        case Prepared:
//        case Committed:
//            /*
//             * Note: A committed tx will have a zero revision time if it was
//             * read-only or if it was read-write but did not write any data.
//             */
//            return revisionTime;
//        }
//        
//        throw new AssertionError();
//        
//    }
    
    /**
     * Returns a string representation of the transaction start time.
     */
    final public String toString() {
        
        /*
         * Note: Representation MUST NOT have dependencies on lock state!
         */
        
//        return Long.toString(startTime);
        
        return "LocalTxState{startTime="+startTime+",runState="+runState+"}";
        
    }
    
    final public boolean isActive() {
        
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return runState == RunState.Active;
        
    }
    
    final public boolean isPrepared() {
        
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return runState == RunState.Prepared;
        
    }
    
    final public boolean isComplete() {
        
        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        
        return runState == RunState.Committed || runState == RunState.Aborted;
        
    }

    final public boolean isCommitted() {

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return runState == RunState.Committed;
        
    }
 
    final public boolean isAborted() {

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        return runState == RunState.Aborted;
        
    }

//    /**
//     * Abort the transaction.
//     * 
//     * @throws IllegalStateException
//     *             if the transaction is already complete.
//     */
//    public void abort() {
//
//        if (!lock.isHeldByCurrentThread())
//            throw new IllegalMonitorStateException();
//
//        if (INFO)
//            log.info("tx=" + this);
//
//        try {
//
//            setRunState(RunState.Aborted);
//
//            localTransactionManager.deactivateTx(this);
//
//            ResourceManager.closeTx(startTime, 0L/* commitTime */, true);
//
//        } finally {
//
//            releaseResources();
//
//        }
//
//    }

    /**
     * Validate the write set of the named indices isolated transaction and
     * merge down that write set onto the corresponding unisolated indices but
     * DOES NOT commit the data. The {@link RunState} is NOT changed by this
     * method.
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
     *             if the transaction is not active.
     * @throws ValidationError
     *             If the transaction can not be validated.
     * @throws IllegalMonitorStateException
     *             unless the caller holds the {@link #lock}.
     */
    public void prepare(final long revisionTime) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (INFO)
            log.info("tx=" + this);

        if (!isActive()) {

            throw new IllegalStateException(NOT_ACTIVE);

        }

        if (!isEmptyWriteSet()) {

            try {

                /*
                 * Validate against the current state of the various indices on
                 * write the transaction has written.
                 */

                if (!validateWriteSets()) {

                    throw new ValidationError();

                }

                /*
                 * Merge each isolated index into the global scope. This also
                 * marks the tuples on which the transaction has written with
                 * the [revisionTime]. This operation MUST succeed (at a logical
                 * level) since we have already validated (neither read-write
                 * nor write-write conflicts exist).
                 * 
                 * Note: This MUST be run as an AbstractTask which declares the
                 * unisolated indices so that has the appropriate locks on those
                 * indices when it executes. The AbstractTask will either
                 * succeed or fail. If it succeeds, then the tx will be made
                 * restart-safe at the group commit. If it fails or if the group
                 * commit fails, then the writes on the unisolated indices will
                 * be discarded.
                 */

                mergeOntoGlobalState(revisionTime);

            } catch (ValidationError ex) {

                throw ex;

            }

        }

    }
    
// final public void mergeDown(final long revisionTime) {
//
// lock.lock();
//
// try {
//
// if(INFO)
//                log.info("tx="+this);
//            
//            if (!isPrepared()) {
//
//                if (!isComplete()) {
//
//                    abort();
//
//                }
//
//                throw new IllegalStateException(NOT_PREPARED);
//
//            }
//
////            // The commitTime is zero unless this is a writable transaction.
////            final long commitTime = readOnly ? 0L : getCommitTimestamp();
//
//            try {
//
//                if (!readOnly && !isEmptyWriteSet()) {
//
//                    /*
//                     * Merge each isolated index into the global scope. This
//                     * also marks the tuples on which the transaction has
//                     * written with the [revisionTime]. This operation MUST
//                     * succeed (at a logical level) since we have already
//                     * validated (neither read-write nor write-write conflicts
//                     * exist).
//                     * 
//                     * Note: This MUST be run as an AbstractTask which declares
//                     * the unisolated indices so that has the appropriate locks
//                     * on those indices when it executes. The AbstractTask will
//                     * either succeed or fail. If it succeeds, then the tx will
//                     * be made restart-safe at the group commit. If it fails or
//                     * if the group commit fails, then the writes on the
//                     * unisolated indices will be discarded.
//                     */
//
//                    mergeOntoGlobalState(revisionTime);
//
//                    // // Atomic commit.
//                    // journal.commitNow(commitTime);
//
//                }
//
//                runState = RunState.Committed;
//
//                localTransactionManager.completedTx(this);
//
//                ResourceManager.closeTx(startTime, revisionTime, false);
//
//            } catch (Throwable t) {
//
//                /*
//                 * Note: If the operation fails then we need to discard any
//                 * changes that have been merged down into the global state.
//                 * Failure to do this will result in those changes becoming
//                 * restart-safe when the next transaction commits. This is
//                 * easily done simply by (a) running this operation as an
//                 * AbstractTask; and (b) throwing an exception. The AbstractTask
//                 * will automatically discard its write set such that there will
//                 * be no side-effect on the persistent state of the unisolated
//                 * indices.
//                 * 
//                 * Note: We do an abort() here just to set the appropriate
//                 * runState and other misc. handshaking.
//                 */
//
//                abort();
//
//                if (t instanceof RuntimeException)
//                    throw (RuntimeException) t;
//
//                throw new RuntimeException(t);
//
//            } finally {
//
//                releaseResources();
//
//            }
//
////            return revisionTime;
//
//        } finally {
//
//            lock.unlock();
//
//        }
//
//    }

    /**
     * This method must be invoked any time a transaction completes in order to
     * release resources held by that transaction.
     */
    protected void releaseResources() {
        
        assert lock.isHeldByCurrentThread();
        
        if(!isComplete()) {

            throw new IllegalStateException();
            
        }

        /*
         * Release hard references to any named btrees isolated within this
         * transaction so that the JVM may reclaim the space allocated to them
         * on the heap.
         */
        indices.clear();

        /*
         * Close and delete the TemporaryRawStore.
         * 
         * @todo when changing to use a shared temporary store modify this to
         * drop the BTree for the isolated indices on the temporary store. That
         * will reduce clutter in its Name2Addr object.
         */
        if (tmpStore != null && tmpStore.isOpen()) {

            tmpStore.close();

        }

    }

    /**
     * @todo This might need to be a full {@link Journal} using
     *       {@link BufferMode#Temporary} in order to have concurrency control
     *       for the isolated named indices. This would let us leverage the
     *       existing {@link WriteExecutorService} for handling concurrent
     *       operations within a transaction on the same named _isolated_
     *       resource. There are a lot of issues here, including the level of
     *       concurrency expected for transactions. Also, note that the write
     *       set of the tx is not restart safe, we never force writes to disk,
     *       etc. Those are good fits for the {@link BufferMode#Temporary}
     *       {@link BufferMode}. However, it might be nice to do without having
     *       a {@link WriteExecutorService} per transaction, e.g., by placing
     *       the named indices for a transaction within a namespace for that tx.
     * 
     * @todo Rather than creating a distinct {@link TemporaryStore} for each tx
     *       and then closing and deleting the store when the tx completes, just
     *       use the temporary store factory. Once there are no more tx's using
     *       a given temporary store it will automatically be finalized and
     *       deleted. However, it is important that we namespace the indices so
     *       that different transactions do not see one another's data.
     *       <p>
     *       We can do this just as easily with {@link BufferMode#Temporary},
     *       but {@link IIndexStore#getTempStore()} would have to be modified.
     *       However, that would give us more concurrency control in the tmp
     *       stores and we might need that for concurrent access to named
     *       indices (see above).
     */
    private IRawStore getTemporaryStore() {

        assert lock.isHeldByCurrentThread();

        if (tmpStore == null) {

            final int offsetBits = resourceManager.getLiveJournal()
                    .getOffsetBits();
            
            tmpStore = new TemporaryRawStore(offsetBits);

        }

        return tmpStore;

    }
    
    /**
     * Invoked when a writable transaction prepares in order to validate its
     * write sets (one per isolated index). The default implementation is NOP.
     * 
     * @return true iff the write sets were validated.
     */
    protected boolean validateWriteSets() {

        /*
         * for all isolated btrees, if(!validate()) return false;
         */

        final Iterator<Map.Entry<String, ILocalBTreeView>> itr = indices.entrySet()
                .iterator();

        while (itr.hasNext()) {

            final Map.Entry<String, ILocalBTreeView> entry = itr.next();

            final String name = entry.getKey();

            final IsolatedFusedView isolated = (IsolatedFusedView) entry.getValue();

            /*
             * Note: this is the live version of the named index. We need to
             * validate against the live version of the index, not some
             * historical state.
             */

            final AbstractBTree[] sources = resourceManager.getIndexSources(
                    name, UNISOLATED);

            if (sources == null) {

                log.warn("Index does not exist: " + name);

                return false;
                
            }
            
            if (!isolated.validate( sources )) {

                // Validation failed.

                if(INFO)
                    log.info("validation failed: " + name);

                return false;

            }

        }

        return true;

    }
    
    /**
     * Invoked during commit processing to merge down the write set from each
     * index isolated by this transactions onto the corresponding unisolated
     * index on the database. This method invoked iff a transaction has
     * successfully prepared and hence is known to have validated successfully.
     * The default implementation is a NOP.
     *
     * @param revisionTime
     */
    protected void mergeOntoGlobalState(final long revisionTime) {
        
        this.revisionTime = revisionTime;
        
        final Iterator<Map.Entry<String, ILocalBTreeView>> itr = indices.entrySet()
                .iterator();

        while (itr.hasNext()) {

            final Map.Entry<String, ILocalBTreeView> entry = itr.next();

            final String name = entry.getKey();

            final IsolatedFusedView isolated = (IsolatedFusedView) entry.getValue();

            /*
             * Note: this is the live version of the named index. We need to
             * merge down onto the live version of the index, not onto some
             * historical state.
             */

            final AbstractBTree[] sources = resourceManager.getIndexSources(
                    name, UNISOLATED);

            if (sources == null) {

                /*
                 * Note: This should not happen since we just validated the
                 * index.
                 */

                throw new AssertionError();

            }

            /*
             * Copy the validated write set for this index down onto the
             * corresponding unisolated index, updating version counters, delete
             * markers, and values as necessary in the unisolated index.
             */

            isolated.mergeDown(revisionTime, sources);

            /*
             * Write a checkpoint so that everything is on the disk. This
             * reduces both the latency for the commit and the possibilities for
             * error.
             */
            
            isolated.getWriteSet().writeCheckpoint();
            
        }

    }

    /**
     * Return a named index. The index will be isolated at the same level as
     * this transaction. Changes on the index will be made restart-safe iff the
     * transaction successfully commits.
     * 
     * @param name
     *            The name of the index.
     * 
     * @return The named index or <code>null</code> if no index is registered
     *         under that name.
     * 
     * @exception IllegalStateException
     *                if the transaction is not active.
     */
    public ILocalBTreeView getIndex(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        /*
         * @todo lock could be per index for higher concurrency rather than for
         * all indices which you might access through this tx.
         */
        lock.lock();

        try {

            if (!isActive()) {

                throw new IllegalStateException(NOT_ACTIVE);

            }

            /*
             * Test the cache - this is used so that we can recover the same
             * instance on each call within the same transaction.
             */
            
            if (indices.containsKey(name)) {

                // Already defined.
                
                return indices.get(name);

            }

            final ILocalBTreeView index;

            /*
             * See if the index was registered as of the ground state used by
             * this transaction to isolated indices.
             * 
             * Note: IResourceManager#getIndex(String name,long timestamp) calls
             * us when the timestamp identifies an active transaction so we MUST
             * NOT call that method ourselves! Hence there is some replication
             * of logic between that method and this one.
             */

            final AbstractBTree[] sources = resourceManager.getIndexSources(
                    name, startTime);

            if (sources == null) {

                /*
                 * The named index was not registered as of the transaction
                 * ground state.
                 */

                if (INFO)
                    log.info("No such index: " + name + ", startTime="
                            + startTime);

                return null;

            }

            if (!sources[0].getIndexMetadata().isIsolatable()) {

                throw new RuntimeException("Not isolatable: " + name);

            }

            /*
             * Isolate the named btree.
             */

//            if (readOnly) {
//
//                assert sources[0].isReadOnly();
//
//                if (sources.length == 1) {
//
//                    index = sources[0];
//
//                } else {
//
//                    index = new FusedView(sources);
//
//                }
//
//            } else {

                /*
                 * Setup the view. The write set is always the first element in
                 * the view.
                 */

                // the view definition.
                final AbstractBTree[] b = new AbstractBTree[sources.length + 1];

                // create the write set on a temporary store.
                b[0] = BTree.create(getTemporaryStore(), sources[0]
                        .getIndexMetadata().clone());

                System.arraycopy(sources, 0, b, 1, sources.length);

                // create view with isolated write set.
                index = new IsolatedFusedView(-startTime, b);

                // report event.
                ResourceManager.isolateIndex(startTime, name);

//            }

            indices.put(name, index);

            return index;

        } finally {

            lock.unlock();

        }

    }

    final public boolean isEmptyWriteSet() {

        lock.lock();

        try {

            final Iterator<ILocalBTreeView> itr = indices.values().iterator();

            while (itr.hasNext()) {

                final IsolatedFusedView ndx = (IsolatedFusedView) itr.next();

                if (!ndx.isEmptyWriteSet()) {

                    // At least one isolated index was written on.

                    return false;

                }

            }

            return true;

        } finally {

            lock.unlock();
            
        }

    }

    final public String[] getDirtyResource() {

        lock.lock();

        try {

            return indices.keySet().toArray(new String[indices.size()]);

        } finally {

            lock.unlock();

        }

    }

}
