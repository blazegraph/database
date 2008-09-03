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
 * Created on Aug 7, 2008
 */

package com.bigdata.striterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Chunk-at-a-time filter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <I>
 *            The parameterized generic type of the source iterator.
 * @param <E>
 *            The generic type of the elements visited by the source iterator.
 * @param <F>
 *            The generic type of the elements after applying this filter. This
 *            will differ from {@link E} iff the filter is transforming the
 *            element type.
 *            
 * @todo This class is redundent with the {@link ChunkedConvertingIterator}
 */
abstract public class ChunkedFilter<I extends IChunkedIterator<E>, E, F>
        implements IFilter<I, E, F> {

    /**
     * Caller's object.
     */
    protected Object state;

    /**
     * The natural order of the filtered chunks. This field is set iff specified
     * explicitly to the ctor and is otherwise <code>null</code>. When
     * <code>null</code> the filtered iterator is <strong>assumed to be
     * unordered</strong>. This assumption is made since chunk-at-a-time
     * transforms can change the natural order of the chunk in many, many
     * different ways, e.g., by changing the type of the elements in the chunk.
     */
    final protected IKeyOrder<F> keyOrder;

    public ChunkedFilter() {

        this(null/* state */, null/* keyOrder */);

    }

    public ChunkedFilter(Object state) {

        this(state, null/* keyOrder */);

    }

    /**
     * 
     * @param state
     *            Application state (optional).
     * @param keyOrder
     *            The natural sort order for the filtered iterator (optional).
     */
    public ChunkedFilter(Object state, IKeyOrder<F> keyOrder) {

        this.state = state;

        this.keyOrder = keyOrder;

    }

    public IChunkedOrderedIterator<F> filter(I src) {

        return new ChunkedFilteringIterator<I, E, F>(src, this);

    }
    
    /**
     * @todo the need for this variant is worrysome - it is required if you do
     *       NOT specify the generic types and then try to use this class.
     */
    public IChunkedOrderedIterator<F> filter(Iterator src) {
        
        return new ChunkedFilteringIterator<I, E, F>((I) src, this);
        
    }

    /**
     * Process a chunk of elements, returning another chunk of elements.
     * 
     * @param chunk
     *            A chunk of elements from the source iterator.
     * 
     * @return The next chunk of elements to be visited by the filtered
     *         iterator. If all elements are filtered then this method should
     *         return either an empty chunk or <code>null</code>.
     */
    abstract protected F[] filterChunk(E[] chunk);

    /**
     * Implementation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <I>
     * @param <E>
     * @param <F>
     */
    private static class ChunkedFilteringIterator<I extends IChunkedIterator<E>, E, F>
            implements IChunkedOrderedIterator<F> {

        /**
         * The source iterator.
         */
        private final I src;

        /**
         * The chunk-at-a-time filter.
         */
        private final ChunkedFilter<I, E, F> filter;
        
        /**
         * A chunk of filtered elements (initially <code>null</code>). If
         * this chunk is <code>null</code> or is exhausted and the source
         * iterator is also exhausted then the filtered iterator is exhausted.
         */
        private F[] chunk = null;

        /**
         * The index of the next element to be visited in {@link #chunk}. This
         * is initially ZERO (0). It is incremented each time {@link #next()} is
         * called. The current {@link #chunk} is exhausted when {@link #index}
         * equals the <code>chunk.length</code>.
         */
        private int index = 0;
        
        /**
         * @param src
         * @param filter
         */
        public ChunkedFilteringIterator(I src, ChunkedFilter<I, E, F> filter) {
        
            this.src = src;
            
            this.filter = filter;
            
        }

        public boolean hasNext() {

            while ((chunk == null || index == chunk.length) && src.hasNext()) {

                // fetch and filter another chunk.
                chunk = filter.filterChunk(src.nextChunk());
                
                // reset the index into the current chunk.
                index = 0;

            }

            if (chunk == null || index == chunk.length) {

                // nothing left.
                return false;
                
            }
            
            return true;
            
        }
        
        public F next() {
            
            if (!hasNext())
                throw new NoSuchElementException();
            
            return chunk[index++];
            
        }

        public F[] nextChunk() {
            
            if (!hasNext())
                throw new NoSuchElementException();

            if (index == 0) {

                // return the entire chunk.
                
                final F[] tmp = chunk;
                
                chunk = null;
                
                return tmp;

            }
            
            /*
             * Make a dense copy of the rest of the chunk and return that.
             */
            
            final int nremaining = chunk.length - index;
            
            /*
             * Dynamically instantiation an array of the same component type
             * as the objects that we are visiting.
             */
            final F[] tmp = (F[]) java.lang.reflect.Array.newInstance(
                    chunk[index].getClass(), nremaining);
            
            for (int i = 0; i < nremaining; i++) {

                tmp[i] = chunk[index + i];
                
            }
            
            // we need another chunk after this.
            chunk = null;
            
            return tmp;
            
        }

        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }

        public void close() {
            
            src.close();
            
        }

        public IKeyOrder<F> getKeyOrder() {

            return filter.keyOrder;
            
        }

        public F[] nextChunk(IKeyOrder<F> keyOrder) {

            if (keyOrder == null)
                throw new IllegalArgumentException();

            final F[] chunk = nextChunk();

            if (!keyOrder.equals(getKeyOrder())) {

                // sort into the required order.

                Arrays.sort(chunk, 0, chunk.length, keyOrder.getComparator());

            }

            return chunk;

        }

    }

}
