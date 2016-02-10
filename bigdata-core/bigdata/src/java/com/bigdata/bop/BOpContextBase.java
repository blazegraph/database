/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.IBigdataFederation;

/**
 * Base class for the bigdata operation evaluation context (NOT serializable).
 */
public class BOpContextBase {

//    static private final transient Logger log = Logger.getLogger(BOpContextBase.class);

    /**
     * The federation iff running in scale-out.
     */
    private final IBigdataFederation<?> fed;

    /**
     * The <strong>local</strong> index manager.
     */
    private final IIndexManager indexManager;

    /** 
     * The executor service.
     */
    private final Executor executor;
    
    /**
     * The <strong>local</strong> {@link IIndexManager}. Query evaluation occurs
     * against the local indices. In scale-out, query evaluation proceeds
     * shard-wise and this {@link IIndexManager} MUST be able to read on the
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

        return executor;
        
    }

    public BOpContextBase(final QueryEngine queryEngine) {
        
        this(queryEngine.getFederation(), queryEngine.getIndexManager());

    }

    /**
     * Core constructor.
     * 
     * @param fed
     *            The federation iff running in scale-out.
     * @param localIndexManager
     *            The <strong>local</strong> index manager.
     */
    public BOpContextBase(final IBigdataFederation<?> fed,
            final IIndexManager localIndexManager) {

        /*
         * @todo null is permitted here for the unit tests, but we should really
         * mock the IIndexManager and pass in a non-null object here and then
         * verify that the reference is non-null.
         */
//        if (localIndexManager == null)
//            throw new IllegalArgumentException();

        this.fed = fed;
        
        this.indexManager = localIndexManager;
     
        this.executor = localIndexManager == null ? null : localIndexManager
                .getExecutorService();
        
    }
    
    /**
     * Locate and return the view of the relation identified by the
     * {@link IPredicate}.
     * 
     * @param pred
     *            The {@link IPredicate}, which MUST be a tail from some
     *            {@link IRule}.
     * 
     * @return The {@link IRelation} -or- <code>null</code> if the relation did
     *         not exist for that timestamp.
     * 
     * @todo Replaces {@link IJoinNexus#getTailRelationView(IPredicate)}. In
     *       order to support mutation operator we will also have to pass in the
     *       {@link #writeTimestamp} or differentiate this in the method name.
     */
    @SuppressWarnings("unchecked")
    public <E> IRelation<E> getRelation(final IPredicate<E> pred) {

        final String namespace = pred.getOnlyRelationName();
        
        final long timestamp = pred.getTimestamp();

        return (IRelation<E>) getResource(namespace, timestamp);

    }
    
    /**
     * Locate and return the view of the identified relation.
     * 
     * @param namespace
     *            The namespace of the relation.
     * @param timestamp
     *            The timestamp of the view of that relation.
     * 
     * @return The {@link ILocatableResource} -or- <code>null</code> if the
     *         relation did not exist for that timestamp.
     */
    public ILocatableResource<?> getResource(final String namespace,
            final long timestamp) {

        /*
         * Note: This uses the federation as the index manager when locating a
         * resource for scale-out since that let's us look up the relation in
         * the global row store, which is being used as a catalog.
         */
        final IIndexManager tmp = getFederation() == null ? getIndexManager()
                : getFederation();

        return (ILocatableResource<?>) tmp.getResourceLocator().locate(
                namespace, timestamp);

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
     * Obtain an access path reading from the identified {@link IRelation} using
     * the specified {@link IPredicate}.
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
     * @todo Support mutable relation views (no - just fix truth maintenance).
     */
    public <E> IAccessPath<E> getAccessPath(final IRelation<E> relation,
            final IPredicate<E> predicate) {

        if (relation == null)
            throw new IllegalArgumentException();

        if (predicate == null)
            throw new IllegalArgumentException();

        return relation.getAccessPath(indexManager/* localIndexManager */,
                relation.getKeyOrder(predicate), predicate);

    }

}
