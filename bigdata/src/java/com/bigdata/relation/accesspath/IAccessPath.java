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
 * Created on Oct 24, 2007
 */

package com.bigdata.relation.accesspath;

import com.bigdata.bop.IPredicate;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.rule.IQueryOptions;
import com.bigdata.relation.rule.IRule;
import com.bigdata.service.IDataService;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.IFilter;

/**
 * An abstraction for efficient reads on an {@link IRelation} using the index
 * selected by an {@link IPredicate} constraint. Like their {@link #iterator()},
 * implementations of this interface are NOT required to be thread-safe. They
 * are designed for a single-threaded consumer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <R>
 *            The generic type of the [R]elation elements of the
 *            {@link IRelation}.
 */
public interface IAccessPath<R> extends Iterable<R> {

    /**
     * The constraints on the {@link IAccessPath}.
     */
    public IPredicate<R> getPredicate();

    /**
     * The order in which the elements will be visited.
     */
    public IKeyOrder<R> getKeyOrder();
    
    /**
     * True iff the access path is empty (there are no matches for the
     * {@link IPredicate}) This is more conclusive than {@link #rangeCount()}
     * since you MAY have a non-zero range count when the key range is in fact
     * empty (there may be "deleted" index entries within the key range).
     */
    public boolean isEmpty();

    /**
     * Return the maximum #of elements spanned by the {@link IPredicate}.
     * <p>
     * Note: When there is an {@link IFilter} on the {@link IPredicate} the
     * exact range count MUST apply that {@link IFilter}, which means that it
     * will be required to traverse the index counting tuples which pass the
     * {@link IFilter}. However, {@link IFilter}s are ignored for the fast
     * range count.
     * 
     * @param exact
     *            When <code>true</code>, the result will be an exact count and
     *            may require a key-range scan. When <code>false</code>, the
     *            result will be an upper bound IFF delete markers are
     *            provisioned for the backing index (delete markers are required
     *            for transactions and for scale-out indices).
     * 
     * @see IRangeQuery
     */
    public long rangeCount(boolean exact);

    /**
     * The index selected for the access path.
     * <p>
     * Note: The access path may incorporate additional constraints from the
     * specified {@link IPredicate} that are not present on the {@link IIndex}
     * returned by this method.
     */
    public IIndex getIndex();
    
    /**
     * The raw iterator for traversing the selected index within the key range
     * implied by {@link IPredicate}.
     * <p>
     * Note: The access path may incorporate additional constraints from the
     * specified {@link IPredicate} that are not present on the raw
     * {@link ITupleIterator} returned by this method.
     */
    public ITupleIterator<R> rangeIterator();

    /**
     * An iterator visiting elements using the natural order of the index
     * selected for the {@link IPredicate}. This is equivalent to
     * 
     * <pre>
     * iterator(0L, 0L, 0)
     * </pre>
     * 
     * since an <i>offset</i> of ZERO (0L) means no offset, a <i>limit</i> of
     * ZERO (0L) means no limit and a <i>capacity</i> of ZERO (0) means
     * whatever is the default capacity.
     * <p>
     * Note: Filters should be specified when the {@link IAccessPath} is
     * constructed so that they will be evaluated on the {@link IDataService}
     * rather than materializing the elements and then filtering then. This can
     * be accomplished by adding the filter as an {@link IElementFilter} on the
     * {@link IPredicate} when requesting {@link IAccessPath}.
     * 
     * @return The iterator.
     * 
     * @see IRelation#getAccessPath(IPredicate)
     */
    public IChunkedOrderedIterator<R> iterator();

    /**
     * An iterator visiting elements using the natural order of the index
     * selected for the {@link IPredicate}.
     * 
     * @param limit
     *            The maximum #of elements that will be visited -or- ZERO (0) if
     *            there is no limit.
     * 
     * @param capacity
     *            The maximum capacity for the buffer used by the iterator. When
     *            ZERO(0), a default capacity will be used. When a <i>limit</i>
     *            is specified, the capacity will never exceed the <i>limit</i>.
     * 
     * @return The iterator.
     * 
     * @deprecated by {@link #iterator(long, long, int)}. Also, [limit] should
     *             have been a long, not an int.
     */
    public IChunkedOrderedIterator<R> iterator(int limit, int capacity);

    /**
     * An iterator visiting elements using the natural order of the index
     * selected for the {@link IPredicate}.
     * <p>
     * The <i>offset</i> and <i>limit</i> together describe an optional
     * <em>slice</em> that will be visited by the iterator. When a slice is
     * specified, the iterator will count off the elements accepted by the
     * {@link IPredicate} up to the <i>offset</i>, but not materialize them.
     * Elements by the {@link IPredicate} starting with the <i>offset</i> and up
     * to (but not including) <i>offset+limit</i> will be materialized for the
     * client. The iterator will halt processing after observing
     * <i>offset+limit</i> accepted elements. Note that slices for JOINs (vs a
     * simple {@link IAccessPath} scan) are handled by {@link IQueryOptions} for
     * an {@link IRule}.
     * <p>
     * The meaning of "accepted" is that: (a) the elements lie in the key-range
     * constraint implied by the {@link IPredicate}; and (b) the elements pass
     * any optional constraints that the {@link IPredicate} imposes.
     * 
     * @param offset
     *            The first element accepted by the iterator that it will visit
     *            (materialize for the client). The offset must be non-negative.
     *            This is ZERO (0L) to visit all accepted elements.
     * @param limit
     *            The last element accepted by the iterator that it will visit
     *            (materialize for the client). The limit must be non-negative.
     *            This is ZERO (0L) to visit all accepted elements (the value
     *            {@link Long#MAX_VALUE} is interpreted exactly like ZERO(0L)).
     * @param capacity
     *            The maximum capacity for the buffer used by the iterator. When
     *            ZERO(0), a default capacity will be used. When a <i>limit</i>
     *            is specified, the capacity will never exceed the <i>limit</i>.
     * 
     * @return The iterator.
     * 
     *         FIXME The offset and limit should probably be rolled into the
     *         predicate and removed from the {@link IAccessPath}. This way they
     *         will be correctly applied when {@link #isEmpty()} is implemented
     *         using the {@link #iterator()} to determine if any elements can be
     *         visited.
     */
    public IChunkedOrderedIterator<R> iterator(long offset, long limit,
            int capacity);

    /**
     * Remove all elements selected by the {@link IPredicate} (optional
     * operation).
     * 
     * @return The #of elements that were removed.
     */
    public long removeAll();
    
}
