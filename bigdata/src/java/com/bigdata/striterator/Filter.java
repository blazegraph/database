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
 * Element-at-a-time filter with generics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <I>
 *            The generic type of the iterator.
 * @param <E>
 *            The generic type of the elements visited by the iterator.
 */
abstract public class Filter<I extends Iterator<E>,E> implements IFilter<I,E,E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final int chunkSize;
    
    protected Object state;
    
    public Filter() {

        this( null );
        
    }

    public Filter(Object state) {

        this(IChunkedIterator.DEFAULT_CHUNK_SIZE, state);

    }

    public Filter(int chunkSize, Object state) {

        this.chunkSize = chunkSize;

        this.state = state;

    }

//    @SuppressWarnings("unchecked")
    public IChunkedIterator<E> filter(I src) {

        return new FilteredIterator<I, E>(chunkSize, src, this);
        
    }

    /**
     * Return <code>true</code> iff the element should be visited.
     * 
     * @param e
     *            The element.
     */
    abstract protected boolean isValid(E e);

    /**
     * Applies filter to the source iterator.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <I>
     * @param <E>
     */
    private static class FilteredIterator<I extends Iterator<E>, E> implements
            IChunkedIterator<E> {

        /** The source iterator. */
        private final I src;

        /** The filter. */
        private final Filter<I, E> filter;

        /** One step lookahead. */
        private E next;
        
        /** The chunk size. */
        private final int chunkSize;

        /** <code>null</code> unless the source iterator is ordered. */
        private final IKeyOrder<E> keyOrder;
        
        /**
         * @param chunkSize The chunk size.
         * @param src
         *            The source iterator.
         * @param filter
         *            The filter.
         */
        public FilteredIterator(int chunkSize, I src, Filter<I, E> filter) {

            this.chunkSize = chunkSize;
            
            this.src = src;
            
            this.filter = filter;

            if(src instanceof IChunkedOrderedIterator<?>) {
                
                keyOrder = ((IChunkedOrderedIterator<E>) src).getKeyOrder();
                
            } else {
                
                keyOrder = null;
                
            }
            
        }

        public boolean hasNext() {

            if (next != null)
                return true;
                
            while(src.hasNext()) {
                
                final E e = src.next();
                
                if(filter.isValid(e)) {
                    
                    next = e;
                    
                    return true;
                    
                }
                
            }
            
            return false;

        }

        public E next() {
            
            if (!hasNext())
                throw new NoSuchElementException();

            final E tmp = next;

            next = null;
            
            return tmp;
        }

        /**
         * Not supported since the one-step lookahead means that we would have
         * to delete the previous element from the source.
         */
        public void remove() {

            throw new UnsupportedOperationException();
            
        }

        /**
         * The next chunk of elements in whatever order the were visited by
         * {@link #next()}.
         */
        @SuppressWarnings("unchecked")
        public E[] nextChunk() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            int n = 0;

            E[] chunk = null;
            
            while (hasNext() && n < chunkSize) {

                E t = next();

                if (chunk == null) {

                    /*
                     * Dynamically instantiation an array of the same component type
                     * as the objects that we are visiting.
                     */

                    chunk = (E[]) java.lang.reflect.Array.newInstance(t.getClass(),
                            chunkSize);

                }

                // add to this chunk.
                chunk[n++] = t;
                
            }
            
            if (n != chunkSize) {

                // make it dense.
                
                E[] tmp = (E[])java.lang.reflect.Array.newInstance(
                        chunk[0].getClass(), n);
                
                System.arraycopy(chunk, 0, tmp, 0, n);
                
                chunk = tmp;
             
            }
            
            return chunk;
            
        }

        /**
         * The {@link IKeyOrder} iff the source iterator implements
         * {@link IChunkedOrderedIterator}.
         */
        public IKeyOrder<E> getKeyOrder() {

            return keyOrder;
            
        }

        public E[] nextChunk(IKeyOrder<E> keyOrder) {

            if (keyOrder == null)
                throw new IllegalArgumentException();

            final E[] chunk = nextChunk();

            if (!keyOrder.equals(getKeyOrder())) {

                // sort into the required order.

                Arrays.sort(chunk, 0, chunk.length, keyOrder.getComparator());

            }

            return chunk;

        }

        public void close() {
            
            if(src instanceof ICloseableIterator<?>) {

                ((ICloseableIterator<E>)src).close();
                
            }
            
        }

    }

}
