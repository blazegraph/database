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
 * Created on Jun 23, 2008
 */

package com.bigdata.relation;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.striterator.IKeyOrder;

/**
 * An abstraction corresponding to a set of elements using some schema (think a
 * table containing a set of rows). Each relation is backed by one or more
 * indices and knows how to return the {@link IAccessPath} that is most
 * efficient given an {@link IPredicate} expressing a query against the
 * {@link IRelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param E
 *            The generic type for the elements in the relation.
 */
public interface IRelation<E> extends ILocatableResource<IRelation<E>>{

    /**
     * The {@link IIndexManager} for the {@link IRelation}.
     */
    IIndexManager getIndexManager();

    /**
     * The service used to run asynchronous or parallel tasks for the
     * {@link IRelation}.
     */
    ExecutorService getExecutorService();

    /**
     * Return the class for the generic type of this relation. This information
     * is used to dynamically create arrays of that generic type.
     */
    Class<E> getElementClass();
    
//    /**
//     * Create and return a new element. The element is constructed from the
//     * predicate given the bindings. Typically, this is used when generating an
//     * {@link ISolution} for an {@link IRule} during either a query or mutation
//     * operations. The element is NOT inserted into the relation.
//     * 
//     * @param predicate
//     *            The predicate that is the head of some {@link IRule}.
//     * @param bindingSet
//     *            A set of bindings for that {@link IRule}.
//     * 
//     * @return The new element.
//     * 
//     * @throws IllegalArgumentException
//     *             if any parameter is <code>null</code>.
//     * @throws IllegalStateException
//     *             if the predicate is not fully bound given those bindings.
//     */
//    E newElement(IPredicate<E> predicate, IBindingSet bindingSet);

    /**
     * Create and return a new element. The element is constructed from the
     * ordered list of variables and constants. Variables are replaced using the
     * given the bindings. The element is NOT inserted into the relation.
     * 
     * @param a
     *            An ordered list of variables and/or constants.
     * @param bindingSet
     *            A set of bindings.
     * 
     * @return The new element.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     * @throws IllegalStateException
     *             if there exists a variable which is not bound given those
     *             bindings.
     */
    E newElement(List<IVariableOrConstant<?>> a, IBindingSet bindingSet);

    /**
     * Return the fully qualified name of each index maintained by this
     * relation.
     * 
     * @return An immutable set of the index names for the relation.
     * 
     * @deprecated by {@link #getKeyOrders()}. However, note that some odd ball
     *             cases exist such as the free text index of the
     *             {@link LexiconRelation} and the justifications index of the
     *             {@link SPORelation}.  These are currently enumerated by
     *             {@link #getIndexNames()} but they are not enumerated by
     *             {@link #getKeyOrders()} since they are not a simple 
     *             transform of the original relation.
     */
    Set<String> getIndexNames();

    /**
     * Return the {@link IKeyOrder} for the primary index for the relation.
     */
    IKeyOrder<E> getPrimaryKeyOrder();

    /**
     * Return the {@link IKeyOrder}s corresponding to the registered indices for
     * this relation. [rather than getIndexNames?]
     */
    Iterator<IKeyOrder<E>> getKeyOrders();

    /**
     * Return the {@link IKeyOrder} for the predicate corresponding to the
     * perfect access path. A perfect access path is one where the bound values
     * in the predicate form a prefix in the key space of the corresponding
     * index.
     * 
     * @param p
     *            The predicate.
     * 
     * @return The {@link IKeyOrder} for the perfect access path -or-
     *         <code>null</code> if there is no index which provides a perfect
     *         access path for that predicate.
     * 
     * @todo What about "best" versus "perfect"? Perfect is more a concept from
     *       RDF with covering indices. For other schemas we will often just
     *       have "best".  If you only have one index then it is always "best".
     *       <p>
     *       Note that one of the main uses for this is query optimization.
     *       However, runtime query optimization can just work through the
     *       possible indices and join orders and get to a "best" query plan
     *       given the actual indices and foreign keys.
     */
    IKeyOrder<E> getKeyOrder(IPredicate<E> p);

    /**
     * Return the best {@link IAccessPath} for a relation given a predicate with
     * zero or more unbound variables.
     * <p>
     * If there is an {@link IIndex} that directly corresponds to the natural
     * order implied by the variable pattern on the predicate then the access
     * path should use that index. Otherwise you should choose the best index
     * given the constraints and make sure that the {@link IAccessPath}
     * incorporates additional filters that will allow you to filter out the
     * irrelevant {@link ITuple}s during the scan - this is very important when
     * the index is remote!
     * <p>
     * If there are any {@link IElementFilter}s then the access path MUST
     * incorporate those constraints such that only elements that satisfy the
     * constraints may be visited.
     * <p>
     * Whether the constraints arise because of the lack of a perfect index for
     * the access path or because they were explicitly specified for the
     * {@link IPredicate}, those constraints should be translated into
     * constraints imposed on the underlying {@link ITupleIterator} and sent
     * with it to be evaluated local to the data.
     * <p>
     * Note: Filters should be specified when the {@link IAccessPath} is
     * constructed so that they will be evaluated on the data service rather
     * than materializing the elements and then filtering then. This can be
     * accomplished by adding the filter as a constraint on the predicate when
     * specifying the access path.
     * 
     * @param predicate
     *            The constraint on the elements to be visited.
     * 
     * @return The best {@link IAccessPath} for that {@link IPredicate}.
     */
    IAccessPath<E> getAccessPath(IPredicate<E> predicate);

    /**
     * This handles a request for an access path that is restricted to a
     * specific index partition. This access path is used with the scale-out
     * JOIN strategy, which distributes join tasks onto each index partition
     * from which it needs to read. Those tasks constrain the predicate to only
     * read from the index partition which is being serviced by that join task.
     * <p>
     * Note: Expanders ARE NOT applied in this code path. Expanders require a
     * total view of the relation, which is not available during scale-out
     * pipeline joins.
     * 
     * @param indexManager
     *            This MUST be the data service local index manager so that the
     *            returned access path will read against the local shard.
     * @param predicate
     *            The predicate. {@link IPredicate#getPartitionId()} MUST return
     *            a valid index partition identifier.
     * 
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             unless the {@link IIndexManager} is a <em>local</em> index
     *             manager providing direct access to the specified shard.
     * @throws IllegalArgumentException
     *             unless the predicate identifies a specific shard using
     *             {@link IPredicate#getPartitionId()}.
     */
    IAccessPath<E> getAccessPathForIndexPartition(IIndexManager indexManager,
            IPredicate<E> predicate);
    /**
     * The fully qualified name of the index.
     * 
     * @param keyOrder
     *            The natural index order.
     * 
     * @return The index name.
     */
    String getFQN(IKeyOrder<? extends E> keyOrder);

    /**
     * Return the index for the {@link IKeyOrder} the timestamp for this view of
     * the relation.
     * 
     * @param keyOrder
     *            The natural index order.
     * 
     * @return The index -or- <code>null</code> iff the index does not exist as
     *         of the timestamp for this view of the relation.
     * 
     * @see #getIndex(String)
     */
    IIndex getIndex(IKeyOrder<? extends E> keyOrder);
    
}
