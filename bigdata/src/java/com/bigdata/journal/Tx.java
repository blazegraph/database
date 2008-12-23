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
import com.bigdata.btree.IndexSegment;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;

/**
 * <p>
 * A transaction. A transaction is a context in which the application can access
 * and perform operations on named indices in which the operations will be
 * isolated according to the isolation level of the transaction. When using a
 * writable isolated transaction, writes are accumulated in an
 * {@link IsolatedFusedView}. The write set is validated when the transaction
 * {@link #prepare()}s. The write set is merged down onto the global state when
 * the transaction commits. When the transaction is read-only, writes will be
 * rejected and {@link #prepare()} and {@link #mergeDown()} are NOPs.
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
public class Tx extends AbstractTx {

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
     *       same tx. the temporary store for the transaction's isolated state
     *       must also be thread safe.
     */
    private Map<String, IIndex> indices = new HashMap<String, IIndex>();

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
            final AbstractLocalTransactionManager transactionManager,//
            final IResourceManager resourceManager, //
            final long startTime//
            ) {

        super(transactionManager, resourceManager, startTime);

    }

    /**
     * This method must be invoked any time a transaction completes in order to
     * release resources held by that transaction.
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
            
            tmpStore = readOnly ? null : new TemporaryRawStore(offsetBits);

        }

        return tmpStore;

    }
    
    protected boolean validateWriteSets() {

        assert !readOnly;

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
    
    protected void mergeOntoGlobalState(final long revisionTime) {

        assert !readOnly;

        super.mergeOntoGlobalState(revisionTime);

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
                index = new IsolatedFusedView(-startTime, b);

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
