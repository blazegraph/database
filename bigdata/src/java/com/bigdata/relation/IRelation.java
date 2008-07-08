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

import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ISolution;

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
public interface IRelation<E> {

    /**
     * The relation identifier.
     */
    public IRelationName getRelationName();
    
    /**
     * The namespace for the indices used by the relation.
     */
    public String getNamespace();

    /**
     * The timestamp associated with the view of the relation.
     */
    public long getTimestamp();

    /**
     * The {@link IIndexManager} for the {@link IRelation}.
     */
    public IIndexManager getIndexManager();
    
    /**
     * The service used to run asynchronous or parallel tasks for the {@link IRelation}.
     */
    public ExecutorService getExecutorService();
    
    /**
     * The #of elements in the relation.
     * 
     * @param exact
     *            When <code>true</code> an exact count is reported. An exact
     *            count will require a key-range scan if delete markers are in
     *            use, in which case it will be more expensive. See
     *            {@link IRangeQuery}.
     */
    long getElementCount(boolean exact);

    /**
     * Return the best {@link IAccessPath} for a relation given a predicate with
     * zero or more unbound variables.
     * <p>
     * If there is an {@link IIndex} that directly corresponeds to the natural
     * order implied by the variable pattern on the predicate then the access
     * path should use that index. Otherwise you should choose the best index
     * given the constraints and make sure that the {@link IAccessPath}
     * incorporates additional filters that will allow you to filter out the
     * irrelevant {@link ITuple}s during the scan - this is very important when
     * the index is remote!
     * <p>
     * If there are any {@link IElementFilter}s then the access path MUST
     * incorporate those constraints such that only elements that satisify the
     * constraints may be visited.
     * <p>
     * Whether the constraints arise because of the lack of a perfect index for
     * the access path or because they were explicitly specified for the
     * {@link IPredicate}, those constraints should be translated into
     * constraints imposed on the underlying {@link ITupleIterator} and sent
     * with it to be evaluated local to the data.
     * <p>
     * Note: Filters should be specified when the {@link IAccessPath} is
     * constructed so that they will be evalated on the data service rather than
     * materializing the elements and then filtering then. This can be
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
     * Return the fully qualified name of each index maintained by this
     * relation.
     * 
     * @return An immutable set of the index names for the relation.
     */
    Set<String> getIndexNames();
    
    /**
     * Create and return a new element. The element is constructed from the
     * predicate given the bindings. Typically, this is used when generating an
     * {@link ISolution} for an {@link IRule} during either a query or mutation
     * operations. The element is NOT inserted into the relation.
     * 
     * @param predicate
     *            The predicate that is the head of some {@link IRule}.
     * @param bindingSet
     *            A set of bindings for that {@link IRule}.
     * 
     * @return The new element.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     * @throws IllegalStateException
     *             if the predicate is not fully bound given those bindings.
     */
    Object newElement(IPredicate predicate, IBindingSet bindingSet);

}
