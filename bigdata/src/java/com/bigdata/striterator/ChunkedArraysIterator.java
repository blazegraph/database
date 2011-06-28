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
 * Created on Oct 24, 2007
 */

package com.bigdata.striterator;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Fully buffered iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ChunkedArrayIterator.java 2265 2009-10-26 12:51:06Z thompsonbry $
 */
public class ChunkedArraysIterator<E> implements IChunkedOrderedIterator<E> {

    private boolean open = true;
    
    /** buffer iterator. */
    private ICloseableIterator<E[]> bufferIt;

    /** current buffer. */
    private E[] buffer;

    /** The order of the elements in the buffer or <code>null</code> iff not known. */
    private final IKeyOrder<E> keyOrder;
    
    /**
     * The index of the next entry in {@link #buffer} that will be returned by
     * {@link #next()}.
     */
    private int i = 0;

//    /**
//     * The element most recently returned by {@link #next()}.
//     */
//    private E current = null;
    
//    /**
//     * The #of elements that this iterator buffered.
//     */
//    public int getBufferCount() {
//
//        return bufferCount;
//        
//    }

    /**
     * An iterator that visits the elements in the given iterator of arrays.
     * 
     * @param a
     *            The iterator of arrays of elements.
     */
    public ChunkedArraysIterator(final ICloseableIterator<E[]> a) {
    
        this(a, null);
        
    }
    
    /**
     * An iterator that visits the elements in the given iterator of arrays.
     * 
     * @param a
     *            The iterator of arrays of elements.
     * @param keyOrder
     *            The order of the elements in the buffer or <code>null</code>
     *            iff not known.
     */
    public ChunkedArraysIterator(final ICloseableIterator<E[]> a,
            final IKeyOrder<E> keyOrder) {
    
        if (a == null)
            throw new IllegalArgumentException();

        this.bufferIt = a;
        
        this.keyOrder = keyOrder;

    }

    public boolean hasNext() {

        if(!open) return false;
        
        if (buffer == null) {
            
            return bufferIt.hasNext();
            
        } 
//        else {
//            
//            assert i <= buffer.length;
//            
//            if (i == buffer.length) {
//    
//                return false;
//                
//            }
//            
//        }
    
        return true;
        
    }

    public E next() {
        
        if (!hasNext()) {

            throw new NoSuchElementException();
        
        }
        
        if (buffer == null) {
            
            buffer = bufferIt.next();
            
        }
        
        E e = buffer[i++];
        
        if (i == buffer.length) {
            
            buffer = null;
            
            i = 0;
            
        }
        
        return e;
        
//        current = buffer[i++];
//        
//        return current;
        
    }

    /**
     * @throws UnsupportedOperationException
     */
    public void remove() {
        
        throw new UnsupportedOperationException();
        
    }

//    /**
//     * Return the backing array.
//     * 
//     * @see #getBufferCount()
//     */
//    public E[] array() {
//
//        assertOpen();
//
//        return buffer;
//        
//    }

    /**
     * Returns the remaining statements.
     * 
     * @throws NoSuchElementException
     *             if {@link #hasNext()} returns false.
     */
    @SuppressWarnings("unchecked")
    public E[] nextChunk() {
       
        if (!hasNext()) {

            throw new NoSuchElementException();
            
        }

        final E[] ret;
        
        if (buffer == null) {
            
            /*
             * We need to fetch the next buffer from the source iterator, and
             * then we can just return it directly.
             */
            buffer = bufferIt.next();
            
            ret = buffer;
            
        } else if (i == 0) {
            
            /*
             * Nothing has been returned to the caller by next() so we can just 
             * return the current buffer in this case.
             */
            ret = buffer;
            
        } else {
            
            /*
             * We have a buffer but we've already started return elements from
             * it via next(), so we need to create a new buffer to return.
             */
            final int remaining = buffer.length - i;
            
            /*
             * Dynamically instantiation an array of the same component type
             * as the objects that we are visiting.
             */

            ret = (E[]) java.lang.reflect.Array.newInstance(buffer.getClass()
                    .getComponentType(), remaining);

            
            System.arraycopy(buffer, i, ret, 0, remaining);
            
        }
        
        // reset the current buffer
        
        buffer = null;
        
        i = 0;
        
        return ret;
        
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
    
    /*
     * Note: Do NOT eagerly close the iterator since the makes it impossible to
     * implement {@link #remove()}.
     */
    public void close() {

        if (!open) {
            
            // already closed.
            
            return;
            
        }

        bufferIt.close();
        
        open = false;
        
        buffer = null;
        
        i = 0;
        
    }

//    private final void assertOpen() {
//        
//        if (!open) {
//
//            throw new IllegalStateException();
//            
//        }
//        
//    }
    
}
