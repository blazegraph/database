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
 * Pattern for resolving elements of an iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <I>
 *            The generic type of the source iterator.
 * @param <E>
 *            The generic type of the elements visited by the source iterator.
 * @param <F>
 *            The generic type of the elements visited by the filtered iterator.
 */
abstract public class Resolver<I extends Iterator<E>, E, F> implements
        IFilter<I, E, F> {

    /** The chunk size if the source is not chunked. */
    private final int chunkSize;
    
    /** The order for the resolved elements (optional). */
    private final IKeyOrder<F> keyOrder;

    public Resolver() {

        this(IChunkedIterator.DEFAULT_CHUNK_SIZE, null);

    }

    /**
     * @param chunkSize
     *            The chunk size to use if the source is not chunked.
     * @param keyOrder
     *            The order for the resolved elements (optional).
     */
    public Resolver(int chunkSize, IKeyOrder<F> keyOrder) {

        this.chunkSize = chunkSize;
        
        this.keyOrder = keyOrder;
        
    }
    
    public IChunkedIterator<F> filter(I src) {

        return new ChunkedResolvingIterator<I, E, F>(src, this);
        
    }

    /**
     * Resolve an element visited by the source iterator into an element of the
     * type visitable by this iterator.
     * 
     * @param e
     *            An element visited by the source iterator.
     *            
     * @return The element to be visited by this iterator.
     */
    abstract protected F resolve(E e);
    
    /**
     * Converts the type of the source iterator using
     * {@link Resolver#resolve(Object)}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @param <I>
     *            The generic type of the source iterator.
     * @param <E>
     *            The generic type of the elements visited by the source
     *            iterator.
     * @param <F>
     *            The generic type of the elements visited by the filtered
     *            iterator.
     */
    static private class ChunkedResolvingIterator<I extends Iterator<E>, E, F>
            implements IChunkedOrderedIterator<F> {

        /** The source iterator. */
        private final I src;

        /** The resolver. */
        private final Resolver<I,E,F> filter;
        
        /**
         * Ctor variant when (a) you KNOW the sort order for the source iterator and
         * (b) the source elements and the resolved elements have the same natural
         * sort order (so the sort order is naturally preserved when the elements
         * are resolved).
         * 
         * @param src
         *            The source iterator.
         * @param keyOrder
         *            The sort order for the resolved elements.
         */
        public ChunkedResolvingIterator(I src, Resolver<I,E,F> filter) {

            if (src == null)
                throw new IllegalArgumentException();
            
            this.src = src;
            
            this.filter = filter;
            
        }
        
        public IKeyOrder<F> getKeyOrder() {

            return filter.keyOrder;
            
        }

        public boolean hasNext() {

            return src.hasNext();
            
        }

        public F next() {
            
            return filter.resolve( src.next() );
            
        }

        @SuppressWarnings("unchecked")
        public F[] nextChunk() {

            if (!src.hasNext())
                throw new NoSuchElementException();
            
            if(src instanceof IChunkedIterator) {
             
                final E[] a = ((IChunkedIterator<E>) src).nextChunk();

                return resolveChunk(a, a.length);
                
            } else {

                E[] a = null;

                int n = 0;
                
                for (int i = 0; i < filter.chunkSize && src.hasNext(); i++) {

                    final E e = src.next();
                    
                    if (a == null) {

                        /*
                         * Dynamically instantiation an array of the same
                         * component type as the objects that we are visiting.
                         */

                        a = (E[]) java.lang.reflect.Array.newInstance(e
                                .getClass(), filter.chunkSize);

                    }

                    a[i] = e;
                    
                    n++;

                }

                return resolveChunk(a, n);
                
            }
            
        }
        
        /**
         * Resolve a chunk of elements.
         * 
         * @param a
         *            The chunk of elements.
         * @param n
         *            The #of elements in that chunk.
         *            
         * @return The chunk of resolved elements.
         */
        @SuppressWarnings("unchecked")
        protected F[] resolveChunk(E[] a, int n) {
        
            F[] b = null;
            
            for (int i = 0; i <n; i++) { 
                
                // resolve 
                final F e = filter.resolve(a[i]);
                
                if (b == null) {

                    /*
                     * Dynamically instantiation an array of the same
                     * component type as the objects that we are visiting.
                     */

                    b = (F[]) java.lang.reflect.Array.newInstance(e.getClass(),
                            n);
                    
                }
                
                // save reference
                b[i] = e;
                
            }

            // done.
            return b;
            
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

        public void remove() {

            src.remove();
            
        }

        public void close() {

            if (src instanceof ICloseableIterator) {

                ((ICloseableIterator<E>) src).close();
                
            }
            
        }
        
    }
   
//    abstract static public class ChunkedResolvingIterator2<I extends Iterator<E>, E, F>
//            extends ChunkedResolvingIterator<I, E, F> {
//
//        /**
//         * @param src
//         * @param filter
//         */
//        public ChunkedResolvingIterator2(I src) {
//
//            super(src, new Resolver<I,E,F>() {
//
//                @Override
//                protected F resolve(E e) {
//                    return ChunkedResolvingIterator2.this.resolve( e );
//                }
//
//            });
//
//        }
//
//        /**
//         * Resolve an element visited by the source iterator into an element of
//         * the type visitable by this iterator.
//         * 
//         * @param e
//         *            An element visited by the source iterator.
//         * 
//         * @return The element to be visited by this iterator.
//         */
//        abstract protected F resolve(E e);
//
//    }
    
}
