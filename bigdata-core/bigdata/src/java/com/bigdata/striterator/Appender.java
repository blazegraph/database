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
 * Created on Aug 7, 2008
 */

package com.bigdata.striterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Appender pattern tacks on another iterator when the source iterator is
 * exhausted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Appender<I extends Iterator<E>,E> implements IFilter<I,E,E> {

    private static final long serialVersionUID = 1307691066685808103L;

    private final int chunkSize;
    private final I src2;

    public Appender(final I src2) {

        this(IChunkedIterator.DEFAULT_CHUNK_SIZE, src2);

    }

    public Appender(final int chunkSize, final I src2) {

        if (chunkSize <= 0)
            throw new IllegalArgumentException();

        if (src2 == null)
            throw new IllegalArgumentException();

        this.chunkSize = chunkSize;

        this.src2 = src2;

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public I filter(final I src) {

        return (I) new AppendingIterator(chunkSize, src, src2);

    }

    /**
     * Appending iterator implementation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <I>
     * @param <E>
     */
    private static class AppendingIterator<I extends Iterator<E>, E>
            implements IChunkedIterator<E> {

        private final int chunkSize;
        private final I src1;
        private final I src2;

        /**
         * Initially set to the value supplied to the ctor. When that source is
         * exhausted, this is set to {@link #src2}. When the second source is
         * exhausted then the total iterator is exhausted.
         */
        private I src;

        private boolean firstSource = true;

        public AppendingIterator(final int chunkSize, final I src, final I src2) {

            this.chunkSize = chunkSize;
            
            this.src = src;
            
            this.src1 = src;

            this.src2 = src2;
            
        }

        @Override
        public boolean hasNext() {

            if (src == null) {

                // exhausted.
                return false;

            }

            if (src.hasNext())
                return true;

            if (firstSource) {

                // start on the 2nd source.
                src = src2;
                firstSource = false;
                
            } else {

                // exhausted.
                src = null;

            }

            return hasNext();

        }

        @Override
        public E next() {

            if (src == null)
                // exhausted.
                throw new NoSuchElementException();

            return src.next();

        }

        @Override
        public void remove() {

            if (src == null)
                throw new IllegalStateException();

            src.remove();

        }

        @Override
        public void close() {

            // exhausted.
            src = null;
            
            if (src1 instanceof ICloseableIterator) {

                ((ICloseableIterator<?>) src1).close();

            }

            if (src2 instanceof ICloseableIterator) {

                ((ICloseableIterator<?>) src2).close();

            }

        }

        /**
         * The next chunk of elements in whatever order they were visited by
         * {@link #next()}.
         */
        @Override
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
//                        chunk[0].getClass(),
                        chunk.getClass().getComponentType(),//
                        n);
                
                System.arraycopy(chunk, 0, tmp, 0, n);
                
                chunk = tmp;
             
            }
            
            return chunk;
            
        }

    }

}
