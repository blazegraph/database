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
 * Created on Aug 26, 2010
 */
package com.bigdata.bop;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IKeyOrder;

/**
 * Base class for the bigdata operation evaluation context (NOT serializable).
 * 
 * @param <E>
 *            The generic type of the objects processed by the operator.
 */
public class BOpContextBase {

    static private final transient Logger log = Logger.getLogger(BOpContextBase.class);

//    private final QueryEngine queryEngine;
    
    private final IBigdataFederation<?> fed;
    private final IIndexManager indexManager;

    /**
     * The <strong>local</strong> {@link IIndexManager}. Query evaluation occurs
     * against the local indices. In scale-out, query evaluation proceeds shard
     * wise and this {@link IIndexManager} MUST be able to read on the
     * {@link ILocalBTreeView}.
     */
    final public IIndexManager getIndexManager() {
        return indexManager;
    }

    /**
     * The {@link IBigdataFederation} IFF the operator is being evaluated on an
     * {@link IBigdataFederation} and otherwise <code>null</code>. When
     * evaluating operations against an {@link IBigdataFederation}, this
     * reference provides access to the scale-out view of the indices and to
     * other bigdata services.
     */
    final public IBigdataFederation<?> getFederation() {
        return fed;
    }

    /**
     * Return the {@link Executor} on to which the operator may submit tasks.
     * <p>
     * Note: The is the {@link ExecutorService} associated with the
     * <em>local</em> {@link #getIndexManager() index manager}.
     */
    public final Executor getExecutorService() {
        return indexManager.getExecutorService();
    }

    public BOpContextBase(final QueryEngine queryEngine) {
        
        this(queryEngine.getFederation(), queryEngine.getIndexManager());
        
    }

    /**
     * Core constructor.
     * @param fed
     * @param indexManager
     */
    public BOpContextBase(final IBigdataFederation<?> fed,
            final IIndexManager indexManager) {

        /*
         * @todo null is permitted here for the unit tests, but we should really
         * mock the IIndexManager and pass in a non-null object here and then
         * verify that the reference is non-null.
         */
//        if (indexManager == null)
//            throw new IllegalArgumentException();

        this.fed = fed;
        
        this.indexManager = indexManager;
        
    }
    
    /**
     * Locate and return the view of the relation(s) identified by the
     * {@link IPredicate}.
     * <p>
     * Note: This method is responsible for returning a fused view when more
     * than one relation name was specified for the {@link IPredicate}. It
     * SHOULD be used whenever the {@link IRelation} is selected based on a
     * predicate in the tail of an {@link IRule} and could therefore be a fused
     * view of more than one relation instance. (The head of the {@link IRule}
     * must be a simple {@link IRelation} and not a view.)
     * <p>
     * Note: The implementation should choose the read timestamp for each
     * relation in the view using {@link #getReadTimestamp(String)}.
     * 
     * @param pred
     *            The {@link IPredicate}, which MUST be a tail from some
     *            {@link IRule}.
     * 
     * @return The {@link IRelation}.
     * 
     * @todo Replaces {@link IJoinNexus#getTailRelationView(IPredicate)}. In
     *       order to support mutation operator we will also have to pass in the
     *       {@link #writeTimestamp} or differentiate this in the method name.
     */
    @SuppressWarnings("unchecked")
    public <E> IRelation<E> getRelation(final IPredicate<E> pred) {

        /*
         * Note: This uses the federation as the index manager when locating a
         * resource for scale-out since that let's us look up the relation in
         * the global row store, which is being used as a catalog.
         */
        final IIndexManager tmp = getFederation() == null ? getIndexManager()
                : getFederation();
        
        final long timestamp = (Long) pred
                .getRequiredProperty(BOp.Annotations.TIMESTAMP);

        return (IRelation<E>) tmp.getResourceLocator().locate(
                pred.getOnlyRelationName(), timestamp);

    }

//  /**
//  * Return a writable view of the relation.
//  * 
//  * @param namespace
//  *            The namespace of the relation.
//  *            
//  * @return A writable view of the relation.
//  * 
//  * @deprecated by getRelation()
//  */
// public IRelation getWriteRelation(final String namespace) {
//
//     /*
//      * @todo Cache the resource locator?
//      * 
//      * @todo This should be using the federation as the index manager when
//      * locating a resource for scale-out, right?  But s/o writes must use
//      * the local index manager when actually obtaining the index view for
//      * the relation.
//      */        
//     return (IRelation) getIndexManager().getResourceLocator().locate(
//             namespace, getWriteTimestamp());
//
// }

    /**
     * Obtain an access path reading from relation for the specified predicate
     * (from the tail of some rule).
     * <p>
     * Note: Passing in the {@link IRelation} is important since it otherwise
     * must be discovered using the {@link IResourceLocator}. By requiring the
     * caller to resolve it before hand and pass it into this method the
     * contention and demand on the {@link IResourceLocator} cache is reduced.
     * <p>
     * <h2>Scale-Out</h2>
     * <p>
     * Note: You MUST be extremely careful when using expanders with a local
     * access path for a shared-partitioned or hash-partitioned index. Only
     * expanders whose semantics remain valid with a partial view of the index
     * will behave as expected. Here are some examples that DO NOT work:
     * <ul>
     * <li>"DISTINCT" on a partitioned local access path is not coherent</li>
     * <li>Expanders which generate reads against keys not found on that shard
     * are not coherent.</li>
     * </ul>
     * If you have requirements such as these, then either use a remote access
     * path or change your query plan design more radically to take advantage of
     * efficient shard-wise scans in scale-out.
     * 
     * @param relation
     *            The relation.
     * @param pred
     *            The predicate. When {@link IPredicate#getPartitionId()} is
     *            set, the returned {@link IAccessPath} MUST read on the
     *            identified local index partition (directly, not via RMI).
     * 
     * @return The access path.
     * 
     * @todo replaces
     *       {@link IJoinNexus#getTailAccessPath(IRelation, IPredicate)}.
     * 
     * @todo Reconcile with IRelation#getAccessPath(IPredicate) once the bop
     *       conversion is done. It has much of the same logic (this also
     *       handles remote access paths now).
     * 
     * @todo Support mutable relation views.
     */
//    @SuppressWarnings("unchecked")
    public <E> IAccessPath<E> getAccessPath(final IRelation<E> relation,
            final IPredicate<E> predicate) {

        if (relation == null)
            throw new IllegalArgumentException();

        if (predicate == null)
            throw new IllegalArgumentException();

        /*
         * FIXME This should be as assigned by the query planner so the query is
         * fully declarative.
         */
        final IKeyOrder<E> keyOrder;
        {
            final IKeyOrder<E> tmp = predicate.getKeyOrder();
            if (tmp != null) {
                // use the specified index.
                keyOrder = tmp;
            } else {
                // ask the relation for the best index.
                keyOrder = relation.getKeyOrder(predicate);
            }
        }

        if (keyOrder == null)
            throw new RuntimeException("No access path: " + predicate);

        final int partitionId = predicate.getPartitionId();

        final long timestamp = (Long) predicate
                .getRequiredProperty(BOp.Annotations.TIMESTAMP);
        
        final int flags = predicate.getProperty(
                IPredicate.Annotations.FLAGS,
                IPredicate.Annotations.DEFAULT_FLAGS)
                | (TimestampUtility.isReadOnly(timestamp) ? IRangeQuery.READONLY
                        : 0);
        
        final int chunkOfChunksCapacity = predicate.getProperty(
                BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY,
                BufferAnnotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);

        final int chunkCapacity = predicate.getProperty(
                BufferAnnotations.CHUNK_CAPACITY,
                BufferAnnotations.DEFAULT_CHUNK_CAPACITY);

        final int fullyBufferedReadThreshold = predicate.getProperty(
                IPredicate.Annotations.FULLY_BUFFERED_READ_THRESHOLD,
                IPredicate.Annotations.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD);
        
        if (partitionId != -1) {

            /*
             * Note: This handles a read against a local index partition. For
             * scale-out, the [indexManager] will be the data service's local
             * index manager.
             * 
             * Note: Expanders ARE NOT applied in this code path. Expanders
             * require a total view of the relation, which is not available
             * during scale-out pipeline joins. Likewise, the [backchain]
             * property will be ignored since it is handled by an expander.
             * 
             * @todo Replace this with IRelation#getAccessPathForIndexPartition()
             */
//            return ((AbstractRelation<?>) relation)
//                    .getAccessPathForIndexPartition(indexManager,
//                            (IPredicate) predicate);

            /*
             * @todo This is an error since expanders are currently ignored on
             * shard-wise access paths. While it is possible to enable expanders
             * for shard-wise access paths.
             */
            if (predicate.getSolutionExpander() != null)
                throw new IllegalArgumentException();
            
            final String namespace = relation.getNamespace();//predicate.getOnlyRelationName();

            // The name of the desired index partition.
            final String name = DataService.getIndexPartitionName(namespace
                    + "." + keyOrder.getIndexName(), partitionId);

            // MUST be a local index view.
            final ILocalBTreeView ndx = (ILocalBTreeView) indexManager
                    .getIndex(name, timestamp);

            return new AccessPath<E>(relation, indexManager, timestamp,
                    predicate, keyOrder, ndx, flags, chunkOfChunksCapacity,
                    chunkCapacity, fullyBufferedReadThreshold).init();

        }

//          accessPath = relation.getAccessPath((IPredicate) predicate);

        // Decide on a local or remote view of the index.
        final IIndexManager indexManager;
        if (predicate.isRemoteAccessPath()) {
            // use federation in scale-out for a remote access path.
            indexManager = fed != null ? fed : this.indexManager;
        } else {
            indexManager = this.indexManager;
        }

        // Obtain the index.
        final String fqn = AbstractRelation.getFQN(relation, keyOrder);
        final IIndex ndx = AbstractRelation.getIndex(indexManager, fqn, timestamp);

        if (ndx == null) {

            throw new IllegalArgumentException("no index? relation="
                    + relation.getNamespace() + ", timestamp=" + timestamp
                    + ", keyOrder=" + keyOrder + ", pred=" + predicate
                    + ", indexManager=" + getIndexManager());

        }

        // Obtain the access path for that relation and index.
        final IAccessPath<E> accessPath = new AccessPath<E>(
                relation, indexManager, timestamp,
                predicate, keyOrder, ndx, flags,
                chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold).init();

        // optionally wrap with an expander pattern.
        return expander(predicate, accessPath);

    }

    /**
     * Optionally wrap with an expander pattern.
     * 
     * @param predicate
     * @param accessPath
     * @return
     * @param <E>
     */
    private <E> IAccessPath<E> expander(final IPredicate<E> predicate,
            final IAccessPath<E> accessPath) {

        final ISolutionExpander<E> expander = predicate.getSolutionExpander();

        if (expander != null) {

            // allow the predicate to wrap the access path
            return expander.getAccessPath(accessPath);

        }

        return accessPath;
        
    }

}
