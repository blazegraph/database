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
import com.bigdata.btree.ReadOnlyIndex;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

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
 * the transaction will run in memory up to 100M. The {@link TemporaryRawStore}
 * is closed and any backing file is deleted as soon as the transaction
 * completes.
 * </p>
 * <p>
 * Each {@link IsolatedFusedView} is local to a transaction and is backed by its own
 * store. This means that concurrent transactions can execute without
 * synchronization (real concurrency) up to the point where they
 * {@link #prepare()}. We do not need a read-lock on the indices isolated by
 * the transaction since they are <em>historical</em> states that will not
 * receive concurrent updates. This might prove to be a nice way to leverage
 * multiple processors / cores on a data server.
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
 *       block in the {@link AbstractJournal#writeService} waiting on the commit
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
 * @todo Support transactions where the indices isolated by the transactions are
 *       {@link PartitionedIndexView}es.
 * 
 * @todo The various public methods on this API that have {@link RunState}
 *       constraints all eagerly force an abort when invoked from an illegal
 *       state. This is, perhaps, excessive. Futher, since this is used in a
 *       single-threaded server context, we are better off testing for illegal
 *       conditions and notifying clients without out generating expensive stack
 *       traces. This could be done by return flags or by the server checking
 *       pre-conditions itself and exceptions being thrown from here if the
 *       server failed to test the pre-conditions and they were not met
 */
public class Tx extends AbstractTx implements IIndexStore, ITx {

    /**
     * The historical {@link ICommitRecord} choosen as the ground state for this
     * transaction. All indices isolated by this transaction are isolated as of
     * the discoverable root address based on this commit record.
     */
    final protected ICommitRecord commitRecord;

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
    private Map<String, IIndex> btrees = new HashMap<String, IIndex>();

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
    public Tx(AbstractJournal journal, long startTime, boolean readOnly) {

        super(journal, startTime, readOnly ? IsolationEnum.ReadOnly
                : IsolationEnum.ReadWrite);

        /*
         * The commit record serving as the ground state for the indices
         * isolated by this transaction (MAY be null, in which case the
         * transaction will be unable to isolate any indices).
         */
        this.commitRecord = journal.getCommitRecord(startTime);

    }

    /**
     * This method must be invoked any time a transaction completes ({@link #abort()}s
     * or {@link #commit()}s) in order to release resources held by that
     * transaction.
     */
    protected void releaseResources() {

        super.releaseResources();

        /*
         * Release hard references to any named btrees isolated within this
         * transaction so that the JVM may reclaim the space allocated to them
         * on the heap.
         */
        btrees.clear();

        /*
         * Close and delete the TemporaryRawStore.
         */
        if (tmpStore != null && tmpStore.isOpen()) {

            tmpStore.close();

        }

    }

    private IRawStore getTemporaryStore() {

        assert lock.isHeldByCurrentThread();

        if (tmpStore == null) {

            tmpStore = readOnly ? null : new TemporaryRawStore(journal
                    .getOffsetBits(), Bytes.megabyte * 1, // initial
                    // in-memory
                    // size.
                    Bytes.megabyte * 10, // maximum in-memory size.
                    false // do NOT use direct buffers.
                    );

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

        Iterator<Map.Entry<String, IIndex>> itr = btrees.entrySet()
                .iterator();

        while (itr.hasNext()) {

            Map.Entry<String, IIndex> entry = itr.next();

            String name = entry.getKey();

            IsolatedFusedView isolated = (IsolatedFusedView) entry.getValue();

            /*
             * Note: this is the _current_ state for the named index. We need to
             * validate against the current state, not against some historical
             * state.
             */

            IIndex groundState = journal.getIndex(name);

            if (!isolated.validate(groundState)) {

                // Validation failed.

                log.info("validation failed: " + name);

                return false;

            }

        }

        return true;

    }

    protected void mergeOntoGlobalState(final long commitTime) {

        assert !readOnly;

        super.mergeOntoGlobalState(commitTime);

        final Iterator<Map.Entry<String, IIndex>> itr = btrees.entrySet()
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

            final BTree groundState = (BTree) journal.getIndex(name);

            /*
             * Copy the validated write set for this index down onto the
             * corresponding unisolated index, updating version counters, delete
             * markers, and values as necessary in the unisolated index.
             */

            isolated.mergeDown(commitTime, groundState);

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
    public IIndex getIndex(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        lock.lock();

        try {

            if (!isActive()) {

                throw new IllegalStateException(NOT_ACTIVE);

            }

            /*
             * Store the btrees in hash map so that we can recover the same
             * instance on each call within the same transaction.
             */
            
            IIndex index = btrees.get(name);

            if (commitRecord == null) {

                /*
                 * This occurs when there are either no commit records or no
                 * commit records before the start time for the transaction.
                 */

                return null;

            }

            if (index == null) {

                /*
                 * See if the index was registered as of the ground state used
                 * by this transaction to isolated indices.
                 */
                
                IIndex src = journal.getIndex(name, commitRecord);

                if (!src.getIndexMetadata().isIsolatable()) {

                    throw new RuntimeException("Not isolatable: " + name);
                    
                }
                
                if (name == null) {

                    /*
                     * The named index was not registered as of the transaction
                     * ground state.
                     */

                    return null;

                }

                /*
                 * Isolate the named btree.
                 */

                if (readOnly) {

                    index = new ReadOnlyIndex(src);

                } else {

                    // create the write set on a temporary store.
                    final BTree writeSet = BTree.create(getTemporaryStore(),
                            src.getIndexMetadata().clone());
                    
                    /*
                     * Setup the view. The write set is always the first element
                     * in the view.
                     */
                    
                    final AbstractBTree[] sources;
                    
                    if(src instanceof AbstractBTree) {
                        
                        sources = new AbstractBTree[] { 
                                writeSet,
                                (AbstractBTree) src
                                };
                        
                    } else {

                        AbstractBTree[] a = ((FusedView) src).getSources();

                        sources = new AbstractBTree[a.length+1];
                        
                        sources[0] = writeSet;
                        
                        System.arraycopy(a, 0, sources, 1, a.length);
                        
                    }
                    
                    // create view with isolated write set.
                    index = new IsolatedFusedView(startTime, sources);

                    // report event.
                    ResourceManager.isolateIndex(startTime, name);

                }

                btrees.put(name, index);

            }

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

            Iterator<IIndex> itr = btrees.values().iterator();

            while (itr.hasNext()) {

                IsolatedFusedView ndx = (IsolatedFusedView) itr.next();

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
        
            return btrees.keySet().toArray(new String[btrees.size()]);
            
        } finally {
            
            lock.unlock();
            
        }

    }

    private static transient final String[] EMPTY = new String[0];

}
