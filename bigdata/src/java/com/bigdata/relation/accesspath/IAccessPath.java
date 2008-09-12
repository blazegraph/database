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

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

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
     * 
     * @param exact
     *            When <code>true</code>, the result will be an exact count
     *            and may require a key-range scan. When <code>false</code>,
     *            the result will be an upper bound IFF delete markers are
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
     * iterator(0, 0)
     * </pre>
     * 
     * since a <i>limit</i> of ZERO (0) means no limit and a <i>capacity</i>
     * of ZERO (0) means whatever is the default capacity.
     * <p>
     * Note: Filters should be specified when the {@link IAccessPath} is
     * constructed so that they will be evalated on the data service rather than
     * materializing the elements and then filtering then. This can be
     * accomplished by adding the filter as an {@link IElementFilter} on
     * the {@link IPredicate} when requesting access path.
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
     */
    public IChunkedOrderedIterator<R> iterator(int limit, int capacity);

    /**
     * Remove all elements selected by the {@link IPredicate}.
     * 
     * @return The #of elements that were removed.
     */
    public long removeAll();
    
}
