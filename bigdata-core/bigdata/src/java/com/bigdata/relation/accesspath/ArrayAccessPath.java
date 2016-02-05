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
package com.bigdata.relation.accesspath;

import java.util.Collections;

import com.bigdata.bop.IPredicate;
import com.bigdata.btree.IIndex;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * An access path over an array of elements.
 */
public class ArrayAccessPath<E> implements IAccessPath<E> {

    private final IPredicate<E> predicate;

    private final IKeyOrder<E> keyOrder;
    
    /**
     * Array of elements
     */
    private final E[] e;

    /**
     * Ctor variant does not specify the {@link #getPredicate()} or the
     * {@link #getKeyOrder()} and those methods will throw an
     * {@link UnsupportedOperationException} if invoked.
     */
    public ArrayAccessPath(final E[] e) {

        this(e, null/* predicate */, null/* keyOrder */);
        
    }
    
    /**
     * Note: the {@link #getPredicate()} and {@link #getKeyOrder()} and methods
     * will throw an {@link UnsupportedOperationException} if the corresponding
     * argument is null.
     */
    public ArrayAccessPath(final E[] e, 
    		final IPredicate<E> predicate, final IKeyOrder<E> keyOrder) {

        this.predicate = predicate;
      
        this.keyOrder = keyOrder;
        
        this.e = e;
        
    }
    
    /**
     * {@inheritDoc}
     * 
     * @throws UnsupportedOperationException
     *             unless the caller specified an {@link IPredicate} to the
     *             ctor.
     */
    @Override
    public IPredicate<E> getPredicate() {

        if (predicate == null)
            throw new UnsupportedOperationException();

        return predicate;

    }

    /**
     * {@inheritDoc}
     * 
     * @throws UnsupportedOperationException
     *             unless the caller specified an {@link IKeyOrder} to the ctor.
     */
    @Override
    public IKeyOrder<E> getKeyOrder() {

        if (keyOrder == null)
            throw new UnsupportedOperationException();

        return keyOrder;

    }

    /**
     * {@inheritDoc}
     * 
     * @throws UnsupportedOperationException
     *             since no index is associated with this array
     */
    @Override
    public IIndex getIndex() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns <code>true</code> when the array of elements is empty.
     */
    @Override
    public boolean isEmpty() {

        return e.length == 0;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the size of the array of elements.
     */
    @Override
    public long rangeCount(boolean exact) {

        return e.length;

    }

//    /**
//     * @throws UnsupportedOperationException
//     *             since no index is associated with this array
//     */
//    @Override
//    public ITupleIterator<E> rangeIterator() {
//
//        throw new UnsupportedOperationException();
//        
//    }

    /**
     * {@inheritDoc}
     * <p>
     * Visits the entire array of elements.
     */
    @Override
	public IChunkedOrderedIterator<E> iterator() {

        return iterator(0L/* offset */, 0L/* limit */, 0/* capacity */);

    }

//    /**
//     * Visits the array of elements up to the specified limit.
//     */
//    public IChunkedOrderedIterator<E> iterator(final int limit, 
//    		final int capacity) {
//
//        return iterator(0L/* offset */, limit, capacity);
//        
//    }

    /**
     * {@inheritDoc}
     * <p>
     * Visits the array of elements from the specified offset up to the
     * specified limit.
     */
    @Override
    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<E> iterator(final long offset, 
    		long limit, final int capacity) {

		if (offset < 0)
			throw new IllegalArgumentException();

		if (offset > e.length || e.length == 0) {
		
			// Nothing to visit.

			return new ChunkedWrappedIterator<E>(
					Collections.EMPTY_LIST.iterator());

		}

		if (limit >= Long.MAX_VALUE) {
			// Treat MAX_VALUE as meaning NO limit.
			limit = 0L;
		}

		if (limit >= offset+e.length) {
			/*
			 * The caller requested more data than is available. Treat as
			 * meaning NO limit since we will read everything after the
			 * [offset].
			 */
			limit = 0L;
		}

		final int n;

		if (limit == 0L) {

			// No limit. Deliver everything after the offset.

			n = e.length - (int) offset;

		} else {

			// Limit. Deliver no more than [limit] elements.
			
			n = Math.min((int) limit, e.length - (int) offset);

		}

		if (offset == 0 && n == e.length) {

			/*
			 * Array is already dense. No allocation is required.
			 */

			// Wrap as iterator and return.
			return new ChunkedArrayIterator<E>(e);

		}

		// Allocate dense array.
		final E[] a = (E[]) java.lang.reflect.Array.newInstance(e.getClass()
				.getComponentType(), n);

		// Copy into array.
		System.arraycopy(e/* src */, (int) offset/* srcPos */, a/* dst */,
				0/* dstPos */, n/* length */);

		// Wrap as iterator and return.
		return new ChunkedArrayIterator<E>(a);

	}

    /**
     * {@inheritDoc}
     * <P>
     * Does nothing and always returns ZERO(0).
     */
    @Override
    public long removeAll() {

        return 0L;

    }

}
