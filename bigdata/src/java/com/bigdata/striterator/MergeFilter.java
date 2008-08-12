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

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Merge sort of two iterators with duplicates suppressed. The elements must be
 * comparable. Each source iterator must deliver its elements in a total
 * ordering in order for the aggregate iterator to maintain a total ordering.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MergeFilter<I extends Iterator<E>, E extends Comparable<E>>
        implements IFilter<I, E, E> {

    private static final long serialVersionUID = -3593674406822688011L;

    private final int chunkSize;
    private final I src2;
    
    public MergeFilter(I src2) {

        this( IChunkedIterator.DEFAULT_CHUNK_SIZE, src2 );
        
    }
    
    public MergeFilter(int chunkSize, I src2) {

        if (chunkSize <= 0)
            throw new IllegalArgumentException();
        
        if (src2 == null)
            throw new IllegalArgumentException();
        
        this.chunkSize = chunkSize;
        
        this.src2 = src2;
        
    }
    
    @SuppressWarnings("unchecked")
    public I filter(I src) {

        return (I) new MergedIterator(chunkSize, src, src2);

    }

    /**
     * Reads on two iterators visiting elements in some natural order and visits
     * their order preserving merge (no duplicates).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    private static class MergedIterator<I extends Iterator<E>, E extends Comparable<E>>
            implements IChunkedIterator<E> {
       
        private final int chunkSize;
        private final I src1;
        private final I src2;
        
        public MergedIterator(int chunkSize, I src1, I src2) {

            this.chunkSize = chunkSize;
            
            this.src1 = src1;
            
            this.src2 = src2;
            
        }
        
        @SuppressWarnings("unchecked")
        public void close() {
            
            if(src1 instanceof ICloseableIterator) {
                
                ((ICloseableIterator)src1).close();
                
            }
            
            if(src2 instanceof ICloseableIterator) {
                
                ((ICloseableIterator)src2).close();
                
            }
            
        }

        public boolean hasNext() {

            return tmp1 != null || tmp2 != null || src1.hasNext()
                    || src2.hasNext();
            
        }
        
        private E tmp1;
        private E tmp2;
        
        public E next() {

            if (!hasNext())
                throw new NoSuchElementException();
            
            if (tmp1 == null && src1.hasNext()) {

                tmp1 = src1.next();

            }
 
            if (tmp2 == null && src2.hasNext()) {

                tmp2 = src2.next();

            }
            
            if (tmp1 == null) {

                // src1 is exhausted so deliver from src2.
                final E tmp = tmp2;

                tmp2 = null;

                return tmp;

            }
            
            if (tmp2 == null) {

                // src2 is exhausted so deliver from src1.
                final E tmp = tmp1;

                tmp1 = null;

                return tmp;

            }

            final int cmp = tmp1.compareTo(tmp2);

            if (cmp == 0) {

                final E tmp = tmp1;

                tmp1 = tmp2 = null;

                return tmp;

            } else if (cmp < 0) {

                final E tmp = tmp1;

                tmp1 = null;

                return tmp;

            } else {

                final E tmp = tmp2;

                tmp2 = null;

                return tmp;

            }
            
        }

        public void remove() {

            throw new UnsupportedOperationException();
            
        }

        /**
         * The next chunk of elements in whatever order they were visited by
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

                final E t = next();

                if (chunk == null) {

                    /*
                     * Dynamically instantiation an array of the same component
                     * type as the objects that we are visiting.
                     */

                    chunk = (E[]) java.lang.reflect.Array.newInstance(t
                            .getClass(), chunkSize);

                }

                // add to this chunk.
                chunk[n++] = t;

            }

            if (n != chunkSize) {

                // make it dense.

                final E[] tmp = (E[]) java.lang.reflect.Array.newInstance(
                        chunk[0].getClass(), n);
                
                System.arraycopy(chunk, 0, tmp, 0, n);
                
                chunk = tmp;
             
            }
            
            return chunk;
            
        }

    }

}
