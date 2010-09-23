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
 * The evaluation context for the operator (NOT serializable).
 * 
 * @param <E>
 *            The generic type of the objects processed by the operator.
 */
public class BOpContextBase {

    static private final transient Logger log = Logger.getLogger(BOpContextBase.class);

    private final QueryEngine queryEngine;

    /**
     * The <strong>local</strong> {@link IIndexManager}. Query evaluation occurs
     * against the local indices. In scale-out, query evaluation proceeds shard
     * wise and this {@link IIndexManager} MUST be able to read on the
     * {@link ILocalBTreeView}.
     */
    final public IIndexManager getIndexManager() {
        return queryEngine.getIndexManager();
    }
    
    /**
     * The {@link IBigdataFederation} IFF the operator is being evaluated on an
     * {@link IBigdataFederation}. When evaluating operations against an
     * {@link IBigdataFederation}, this reference provides access to the
     * scale-out view of the indices and to other bigdata services.
     */
    final public IBigdataFederation<?> getFederation() {
        return queryEngine.getFederation();
    }

    /**
     * Return the {@link Executor} on to which the operator may submit tasks.
     * <p>
     * Note: The is the {@link ExecutorService} associated with the
     * <em>local</em> {@link #getIndexManager() index manager}.
     */
    public final Executor getExecutorService() {
        return getIndexManager().getExecutorService();
    }

    /**
     * 
     * @param indexManager
     *            The <strong>local</strong> {@link IIndexManager}. Query
     *            evaluation occurs against the local indices. In scale-out,
     *            query evaluation proceeds shard wise and this
     *            {@link IIndexManager} MUST be able to read on the
     *            {@link ILocalBTreeView}.
     * 
     */
    public BOpContextBase(final QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
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
    public IRelation getRelation(final IPredicate<?> pred) {

        /*
         * Note: This uses the federation as the index manager when locating a
         * resource for scale-out. However, s/o reads must use the local index
         * manager when actually obtaining the index view for the relation.
         */
        final IIndexManager tmp = getFederation() == null ? getIndexManager()
                : getFederation();
        
        final long timestamp = (Long) pred
                .getRequiredProperty(BOp.Annotations.TIMESTAMP);

        return (IRelation<?>) tmp.getResourceLocator().locate(
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
     * Note that passing in the {@link IRelation} is important since it
     * otherwise must be discovered using the {@link IResourceLocator}. By
     * requiring the caller to resolve it before hand and pass it into this
     * method the contention and demand on the {@link IResourceLocator} cache is
     * reduced.
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
     */
    @SuppressWarnings("unchecked")
    public IAccessPath<?> getAccessPath(final IRelation<?> relation,
            final IPredicate<?> predicate) {

        if (relation == null)
            throw new IllegalArgumentException();

        if (predicate == null)
            throw new IllegalArgumentException();
        // FIXME This should be as assigned by the query planner so the query is fully declarative.
        final IKeyOrder keyOrder = relation.getKeyOrder((IPredicate) predicate);

        if (keyOrder == null)
            throw new RuntimeException("No access path: " + predicate);

        final int partitionId = predicate.getPartitionId();

        final long timestamp = (Long) predicate
                .getRequiredProperty(BOp.Annotations.TIMESTAMP);
        
        final int flags = predicate.getProperty(
                PipelineOp.Annotations.FLAGS,
                PipelineOp.Annotations.DEFAULT_FLAGS)
                | (TimestampUtility.isReadOnly(timestamp) ? IRangeQuery.READONLY
                        : 0);
        
        final int chunkOfChunksCapacity = predicate.getProperty(
                PipelineOp.Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                PipelineOp.Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);

        final int chunkCapacity = predicate.getProperty(
                PipelineOp.Annotations.CHUNK_CAPACITY,
                PipelineOp.Annotations.DEFAULT_CHUNK_CAPACITY);

        final int fullyBufferedReadThreshold = predicate.getProperty(
                PipelineOp.Annotations.FULLY_BUFFERED_READ_THRESHOLD,
                PipelineOp.Annotations.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD);
        
        final IIndexManager indexManager = getIndexManager();
        
        if (predicate.getPartitionId() != -1) {

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
             * @todo This condition should probably be an error since the expander
             * will be ignored.
             */
//            if (predicate.getSolutionExpander() != null)
//                throw new IllegalArgumentException();
            
            final String namespace = relation.getNamespace();//predicate.getOnlyRelationName();

            // The name of the desired index partition.
            final String name = DataService.getIndexPartitionName(namespace
                    + "." + keyOrder.getIndexName(), partitionId);

            // MUST be a local index view.
            final ILocalBTreeView ndx = (ILocalBTreeView) indexManager
                    .getIndex(name, timestamp);

            return new AccessPath(relation, indexManager, timestamp,
                    predicate, keyOrder, ndx, flags, chunkOfChunksCapacity,
                    chunkCapacity, fullyBufferedReadThreshold).init();

        }

        /*
         * Find the best access path for the predicate for that relation.
         * 
         * @todo Replace this with IRelation#getAccessPath(IPredicate) once the
         * bop conversion is done. It is the same logic.
         */
        IAccessPath accessPath;
        {

//          accessPath = relation.getAccessPath((IPredicate) predicate);

            final IIndex ndx = relation.getIndex(keyOrder);

            if (ndx == null) {
            
                throw new IllegalArgumentException("no index? relation="
                        + relation.getNamespace() + ", timestamp="
                        + timestamp + ", keyOrder=" + keyOrder + ", pred="
                        + predicate + ", indexManager=" + getIndexManager());

            }

            accessPath = new AccessPath((IRelation) relation, indexManager,
                    timestamp, (IPredicate) predicate,
                    (IKeyOrder) keyOrder, ndx, flags, chunkOfChunksCapacity,
                    chunkCapacity, fullyBufferedReadThreshold).init();

        }
        
        /*
         * @todo No expander's for bops, at least not right now. They could be
         * added in easily enough, which would support additional features for
         * standalone query evaluation (runtime materialization of some
         * entailments).
         * 
         * FIXME temporarily enabled expanders (mikep)
         */
         final ISolutionExpander<?> expander = predicate.getSolutionExpander();
                    
         if (expander != null) {
                        
             // allow the predicate to wrap the access path
             accessPath = expander.getAccessPath(accessPath);
                        
         }

        // return that access path.
        return accessPath;
    }

}
