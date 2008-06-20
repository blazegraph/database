/**

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
 * Created on Nov 11, 2007
 */
package com.bigdata.join;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Converts an <code>Iterator</code> into chunked iterator.
 * <p>
 * Note: The visitation order is whatever the order was for the source
 * iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkedWrappedIterator<T> implements IChunkedIterator<T> {
    
    private boolean open = true;
    
    private final Iterator<T> src;
    
    private final int chunkSize;
    
    /**
     * Create an iterator that reads from the source.
     * 
     * @param src
     *            The source iterator.
     */
    public ChunkedWrappedIterator(Iterator<T>src) {
        
        this(src, 10000 );
        
    }

    /**
     * Create an iterator that reads from the source.
     * 
     * @param src
     *            The source iterator.
     * @param chunkSize
     *            The desired chunk size.
     */
    public ChunkedWrappedIterator(Iterator<T>src, int chunkSize) {
        
        if (src == null)
            throw new IllegalArgumentException();

        if (chunkSize <= 0)
            throw new IllegalArgumentException();
     
        this.src = src;
        
        this.chunkSize = chunkSize;
        
    }
    
    public void close() {

        if (!open)
            return;
        
        open = false;

        if(src instanceof IChunkedIterator) {
            
            ((IChunkedIterator<T>)src).close();
            
        }

    }

    /**
     * Return <code>true</code> if there are {@link SPO}s in the source
     * iterator.
     */
    public boolean hasNext() {

        if(!open) return false;

        return src.hasNext();
        
    }

    public T next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        return src.next();

    }

    /**
     * The next chunk of in whatever order the were visited by {@link #next()}.
     */
    @SuppressWarnings("unchecked")
    public T[] nextChunk() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        int n = 0;

        T[] chunk = null;
        
        while (hasNext() && n < chunkSize) {

            T t = next();

            if (chunk == null) {

                /*
                 * Dynamically instantiation an array of the same component type
                 * as the objects that we are visiting.
                 */

                chunk = (T[]) java.lang.reflect.Array.newInstance(t.getClass(),
                        chunkSize);

            }

            // add to this chunk.
            chunk[n++] = t;
            
        }
        
        if (n != chunkSize) {

            // make it dense.
            
            T[] tmp = (T[])java.lang.reflect.Array.newInstance(
                    chunk[0].getClass(), n);
            
            System.arraycopy(chunk, 0, tmp, 0, n);
            
            chunk = tmp;
         
        }
        
        return chunk;
        
    }

    /**
     * Delegated to the source iterator.
     */
    public void remove() {
        
        src.remove();
        
    }
    
}
