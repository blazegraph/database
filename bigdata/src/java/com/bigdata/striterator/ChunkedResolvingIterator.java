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
 * Created on Jun 26, 2008
 */

package com.bigdata.striterator;

import java.util.Arrays;

/**
 * Converts the type of the source iterator using #resolve().
 * <p>
 * Note: This class correctly passes {@link ICloseableIterator#close()} through
 * to the source iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <S>
 *            The generic type of the elements visited by the [S]ource iterator.
 * @param <E>
 *            The generic type of the [E]lements visited by this iterator.
 */
abstract public class ChunkedResolvingIterator<E,S> implements IChunkedOrderedIterator<E> {

    private final IChunkedOrderedIterator<S> src;
    
    private final IKeyOrder<E> keyOrder;

    /**
     * Ctor variant when resolving changes the sort order such that the resolved
     * elements would need to be sorted to put them into a known order.
     * 
     * @param src
     *            The source iterator.
     */
    public ChunkedResolvingIterator(IChunkedOrderedIterator<S> src) {

        this(src, null/* keyOrder */);
        
    }

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
    public ChunkedResolvingIterator(IChunkedOrderedIterator<S> src, IKeyOrder<E> keyOrder) {

        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
        this.keyOrder = keyOrder;
        
    }
    
    public IKeyOrder<E> getKeyOrder() {

        return keyOrder;
        
    }

    public boolean hasNext() {

        return src.hasNext();
        
    }

    public E next() {
        
        return resolve( src.next() );
        
    }

    @SuppressWarnings("unchecked")
    public E[] nextChunk() {

        final S[] a = src.nextChunk();
        
        E[] b = null;
        
        for( int i=0; i<a.length; i++) {
    
            E e = resolve(a[i]);
            
            if(b == null) {
                
                /*
                 * Dynamically instantiation an array of the same component type
                 * as the objects that we are visiting.
                 */

                b = (E[]) java.lang.reflect.Array.newInstance(e.getClass(),
                        a.length);
                
            }
            
            b[i] = e;
            
        }
        
        return b;
        
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

    public void remove() {

        src.remove();
        
    }

    public void close() {

        src.close();
        
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
    abstract protected E resolve(S e);
    
}
