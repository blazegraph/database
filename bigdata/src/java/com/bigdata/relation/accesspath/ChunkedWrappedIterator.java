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
package com.bigdata.relation.accesspath;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Converts an <code>Iterator</code> into chunked iterator.
 * <p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkedWrappedIterator<E> implements IChunkedOrderedIterator<E> {

    protected static transient final int DEFAULT_CHUNK_SIZE = 10000;
    
    private boolean open = true;

    private final Iterator<E> src;

    private final int chunkSize;
    
    private final IKeyOrder<E> keyOrder;

    /**
     * Create an iterator that reads from the source.
     * 
     * @param src
     *            The source iterator.
     */
    public ChunkedWrappedIterator(Iterator<E> src) {

        this(src, DEFAULT_CHUNK_SIZE, null);
        
    }

    /**
     * Create an iterator that reads from the source.
     * 
     * @param src
     *            The source iterator.
     * @param chunkSize
     *            The desired chunk size.
     * @param keyOrder
     *            The order in which the elements will be visited by the source
     *            iterator if known and <code>null</code> otherwise.
     */
    public ChunkedWrappedIterator(Iterator<E> src, int chunkSize, IKeyOrder<E> keyOrder) {
        
        if (src == null)
            throw new IllegalArgumentException();

        if (chunkSize <= 0)
            throw new IllegalArgumentException();
     
        this.src = src;
        
        this.chunkSize = chunkSize;
        
        this.keyOrder = keyOrder;
        
    }
    
    public void close() {

        if (!open)
            return;
        
        open = false;

        if(src instanceof IChunkedIterator) {
            
            ((IChunkedIterator<E>)src).close();
            
        }

    }

    /**
     * Return <code>true</code> if there are elements in the source iterator.
     */
    public boolean hasNext() {

        if(!open) return false;

        return src.hasNext();
        
    }

    /**
     * The next element from the source iterator.
     */
    public E next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        return src.next();

    }

    /**
     * The next chunk of in whatever order the were visited by {@link #next()}.
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
     * Delegated to the source iterator.
     */
    public void remove() {
        
        src.remove();
        
    }

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
    
}
