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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;

/**
 * <p>
 * A transaction. A transaction is a context in which the application can access
 * and perform operations on named indices in which the operations will be
 * isolated according to the isolation level of the transaction. When using a
 * writable isolated transaction, writes are accumulated in an
 * {@link IsolatedFusedView}. The write set is validated when the transaction
 * {@link #prepare()}s. The write set is merged down onto the global state when
 * the transaction commits. When the transaction is read-only, writes will be
 * rejected and {@link #prepare()} and {@link #commit()} are NOPs.
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
 * @todo In order to support a distributed transaction commit protocol the write
 *       set of a validated transaction needs to be made restart safe without
 *       making it restart safe on the corresponding unisolated index on the
 *       journal. It may be that the right thing to do is to write the validated
 *       data onto the unisolated indices but not commit the journal and not
 *       permit other unisolated writes until the commit message arives, e.g.,
 *       block in the {@link WriteExecutorService} waiting on the commit
 *       message. A timeout would cause the buffered writes to be discarded (by
 *       an abort).
 * 
 * @todo track whether or not the transaction has written any isolated data (I
 *       currently rangeCount the isolated indices in {@link #isEmptyWriteSet()}).
 *       do this at the same time that I modify the isolated indices use a
 *       delegation strategy so that I can trap attempts to access an isolated
 *       index once the transaction is no longer active. define "active" as up
 *       to the point where a "commit" or "abort" is _requested_ for the tx.
 * 
 * @todo The various public methods on this API that have {@link RunState}
 *       constraints all eagerly force an abort when invoked from an illegal
 *       state. This is, perhaps, excessive. Futher, since this is used in a
 *       single-threaded server context (the {@link AbstractTask} needs to
 *       manage resource locks for the isolated indices when there are
 *       concurrent tasks executing for the same transaction), we are better off
 *       testing for illegal conditions and notifying clients without out
 *       generating expensive stack traces. This could be done by return flags
 *       or by the server checking pre-conditions itself and exceptions being
 *       thrown from here if the server failed to test the pre-conditions and
 *       they were not met
 */
public class Tx extends AbstractTx implements ITx {

//    /**
//     * The historical {@link ICommitRecord} choosen as the ground state for this
//     * transaction. All indices isolated by this transaction are isolated as of
//     * the discoverable root address based on this commit record.
//     */
//    final protected ICommitRecord commitRecord;

    /**
     * A temporary store used to hold write sets for read-write transactions. It
     * is null if the transaction is read-only and will remain null in any case
     * until its first use.
     */
    private IRawStore tmpStore = null;

    /**
     * Indices isolated by this transactions.
     * 
     * @todo this must be thread-safe to support concurrent operations on the
     *       same tx.
     */
    private Map<String, IIndex> indices = new HashMap<String, IIndex>();

    /**
     * Create a transaction reading from the most recent committed state not
     * later than the specified startTime.
     * 
     * @param journal
     *            The journal.
     * 
     * @param startTime
     *            The start time assigned to the transaction. Note that a
     *            transaction does not start execution on all {@link Journal}s
     *            at the same moment. Instead, the transaction start startTime
     *            is assigned by a centralized time service and then provided
     *            each time a transaction object must be created for isolatation
     *            of resources accessible on some {@link Journal}.
     * 
     * @param readOnly
     *            When true the transaction will reject writes and
     *            {@link #prepare()} and {@link #commit()} will be NOPs.
     */
    public Tx(//
            final ILocalTransactionManager transactionManager,//
            final IResourceManager resourceManager, //
            final long startTime,//
            final boolean readOnly//@todo remove this flag.
            ) {

        super(transactionManager, resourceManager, startTime);

//        /*
//         * The commit record serving as the ground state for the indices
//         * isolated by this transaction (MAY be null, in which case the
//         * transaction will be unable to isolate any indices).
//         */
//        this.commitRecord = journal.getCommitRecord(startTime);

    }

    /**
     * This method must be invoked any time a transaction completes ({@link #abort()}s
     * or {@link #commit()}s) in order to release resources held by that
     * transaction.
     */
    protected void releaseResources() {

        assert lock.isHeldByCurrentThread();
        
        super.releaseResources();

        /*
         * Release hard references to any named btrees isolated within this
         * transaction so that the JVM may reclaim the space allocated to them
         * on the heap.
         */
        indices.clear();

        /*
         * Close and delete the TemporaryRawStore.
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
     *       {@link BufferMode}.  However, it might be nice to do without having
     *       a {@link WriteExecutorService} per transaction, e.g., by placing
     *       the named indices for a transaction within a namespace for that
     *       tx. 
     */
    private IRawStore getTemporaryStore() {

        assert lock.isHeldByCurrentThread();

        if (tmpStore == null) {

            final int offsetBits = resourceManager.getLiveJournal()
                    .getOffsetBits();
            
            tmpStore = readOnly ? null : new TemporaryRawStore(offsetBits);

        }

        return tmpStore;

    }
    
    protected boolean validateWriteSets() {

        assert !readOnly;

        // Note: This is not true now that unisolated writers may be concurrent.
        //        
        // /*
        // * This compares the current commit counter on the journal with the
        // * commit counter as of the start time for the transaction. If they
        // are
        // * the same, then no intervening commits have occurred on the journal
        // * and there is nothing to validate.
        // */
        //        
        // if (commitRecord == null
        // || (journal.getRootBlockView().getCommitCounter() == commitRecord
        // .getCommitCounter())) {
        //            
        // return true;
        //            
        // }

        /*
         * for all isolated btrees, if(!validate()) return false;
         */

        final Iterator<Map.Entry<String, IIndex>> itr = indices.entrySet()
                .iterator();

        while (itr.hasNext()) {

            final Map.Entry<String, IIndex> entry = itr.next();

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

//    /**
//     * Return a read-only view of the current (unisolated) named index.
//     * 
//     * @param name
//     *            The index name.
//     *            
//     * @return The view -or- <code>null</code> iff the index is not registered
//     *         at this time.A
//     */
//    private IIndex getCurrentGroundState(String name) {
//
//        final AbstractBTree[] sources = resourceManager.getIndexSources(name,
//                UNISOLATED);
//
//        if (sources == null) {
//
//            // Index does not exist.
//            
//            return null;
//            
//        }
//        
//        assert sources.length > 0;
//
//        if (sources.length == 1) {
//
//            assert sources[0] != null;
//
//            return new ReadOnlyIndex(sources[0]);
//
//        } else {
//
//            return new ReadOnlyFusedView(sources);
//
//        }
//
//        // IIndex groundState = journal.getIndex(name);
//
//    }
    
    protected void mergeOntoGlobalState(final long commitTime) {

        assert !readOnly;

        super.mergeOntoGlobalState(commitTime);

        final Iterator<Map.Entry<String, IIndex>> itr = indices.entrySet()
                .iterator();

        while (itr.hasNext()) {

            final Map.Entry<String, IIndex> entry = itr.next();

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

            isolated.mergeDown(commitTime, sources );

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
    public IIndex getIndex(final String name) {

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

            // if (commitRecord == null) {
            //
            // /*
            // * This occurs when there are either no commit records or no
            // * commit records before the start time for the transaction.
            // */
            //
            // return null;
            //
            // }

            final IIndex index;

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

            // IIndex src = journal.getIndex(name, commitRecord);

            /*
             * Isolate the named btree.
             */

            if (readOnly) {

                assert sources[0].isReadOnly();

                if (sources.length == 1) {

                    index = sources[0];

                } else {

                    index = new FusedView(sources);

                }

//                if (sources.length == 1) {
//
//                    index = new ReadOnlyIndex(sources[0]);
//
//                } else {
//
//                    index = new ReadOnlyFusedView(sources);
//
//                }

            } else {

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
                index = new IsolatedFusedView(startTime, b);

                // report event.
                ResourceManager.isolateIndex(startTime, name);

            }

            indices.put(name, index);

            return index;

        } finally {

            lock.unlock();

        }

    }

    final public boolean isEmptyWriteSet() {

        lock.lock();

        try {

            if (isReadOnly()) {

                // Read-only transactions always have empty write sets.
                return true;

            }

            final Iterator<IIndex> itr = indices.values().iterator();

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

        if (isReadOnly()) {

            return EMPTY;

        }

        lock.lock();
        
        try {
        
            return indices.keySet().toArray(new String[indices.size()]);
            
        } finally {
            
            lock.unlock();
            
        }

    }

    private static transient final String[] EMPTY = new String[0];

}
