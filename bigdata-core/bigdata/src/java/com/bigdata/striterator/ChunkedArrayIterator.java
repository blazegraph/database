/**

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
 * Created on Oct 24, 2007
 */

package com.bigdata.striterator;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Fully buffered iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkedArrayIterator<E> implements IChunkedOrderedIterator<E> {

    private boolean open = true;
    
    /** buffer. */
    private E[] buffer;

    /** #of valid entries in {@link #buffer}. */
    private int bufferCount;

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
    
    /**
     * The #of elements that this iterator buffered.
     */
    public int getBufferCount() {

        return bufferCount;
        
    }

    /**
     * An iterator that visits the elements in the given array.
     * 
     * @param a
     *            The array of elements.
     */
    public ChunkedArrayIterator(final E[] a) {
    	
        this(a.length, a, null);
    	
    }
    
    /**
     * An iterator that visits the elements in the given array.
     * 
     * @param a
     *            The array of elements.
     * @param n
     *            The #of entries in <i>a</i> that are valid.
     */
    public ChunkedArrayIterator(final int n, final E[] a) {
        
        this(n, a, null);
        
    }
    
    /**
     * An iterator that visits the elements in the given array.
     * 
     * @param a
     *            The array of elements.
     * @param n
     *            The #of entries in <i>a</i> that are valid.
     * @param keyOrder
     *            The order of the elements in the buffer or <code>null</code>
     *            iff not known.
     */
    public ChunkedArrayIterator(final int n, final E[] a,
            final IKeyOrder<E> keyOrder) {
    
        if (a == null)
            throw new IllegalArgumentException();

        if (n < 0 || n > a.length)
            throw new IllegalArgumentException();
        
        this.buffer = a;
        
        this.bufferCount = n;
        
        this.keyOrder = keyOrder;

    }

    @Override
    public boolean hasNext() {

        if(!open) return false;
        
        assert i <= bufferCount;
        
        if (i == bufferCount) {

            return false;
            
        }

        return true;
        
    }

    @Override
    public E next() {
        
        if (!hasNext()) {

            throw new NoSuchElementException();
        
        }
        
        return buffer[i++];
        
//        current = buffer[i++];
//        
//        return current;
        
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void remove() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Return the backing array.
     * 
     * @see #getBufferCount()
     */
    public E[] array() {

        assertOpen();

        return buffer;
        
    }

    /**
     * Returns the remaining statements.
     * 
     * @throws NoSuchElementException
     *             if {@link #hasNext()} returns false.
     */
    @Override
    @SuppressWarnings("unchecked")
    public E[] nextChunk() {
       
        if (!hasNext()) {

            throw new NoSuchElementException();
            
        }

        final E[] ret;
        
        if (i == 0 && bufferCount == buffer.length) {
            
            /*
             * The SPO[] does not have any unused elements and nothing has been
             * returned to the caller by next() so we can just return the
             * backing array in this case.
             * 
             * Note: If the caller then sorts the chunk it will have a
             * side-effect on the original SPO[] since we are returning a
             * reference to that array.
             */
            
            ret = buffer;
            
        } else {

            /*
             * Create and return a new SPO[] containing only the statements
             * remaining in the iterator.
             */
            
            final int remaining = bufferCount - i;
            
            /*
             * Dynamically instantiation an array of the same component type
             * as the objects that we are visiting.
             */

            ret = (E[]) java.lang.reflect.Array.newInstance(buffer.getClass()
                    .getComponentType(), remaining);

            
            System.arraycopy(buffer, i, ret, 0, remaining);
            
        }
        
        // indicate that all statements have been consumed.
        
        i = bufferCount;
        
        return ret;
        
    }
    
    @Override
    public IKeyOrder<E> getKeyOrder() {
        
        return keyOrder;
        
    }
    
    @Override
    public E[] nextChunk(final IKeyOrder<E> keyOrder) {

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
    @Override
    public void close() {

        if (!open) {
            
            // already closed.
            
            return;
            
        }
        
        open = false;
        
//        db = null;
        
        buffer = null;
        
//        current = null;
        
        i = bufferCount = 0;
        
    }

    private final void assertOpen() {
        
        if (!open) {

            throw new IllegalStateException();
            
        }
        
    }
    
}
