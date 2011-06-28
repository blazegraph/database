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

/**
 * Chunked ordered streaming iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkedOrderedStriterator<I extends IChunkedOrderedIterator<E>, E>
        extends ChunkedStriterator<I, E> implements
        IChunkedOrderedStriterator<I, E> {

    public ChunkedOrderedStriterator(I src) {

        super(src);
        
    }

    /**
     * Wraps the source iterator as a chunked ordered iterator.
     * 
     * @param chunkSize
     *            The chunk size.
     * @param src
     *            The source iterator.
     */
    @SuppressWarnings("unchecked")
    public ChunkedOrderedStriterator(int chunkSize,Iterator<E> src) {
     
        this((I) new ChunkedWrappedIterator<E>(src, chunkSize,
                null/* keyOrder */, null/*filter*/));
        
    }

    final public IKeyOrder<E> getKeyOrder() {

        return src.getKeyOrder();
        
    }

    final public E[] nextChunk(IKeyOrder<E> keyOrder) {
        
        return src.nextChunk(keyOrder);
        
    }

    /**
     * Strengthened return type.
     */
    public IChunkedOrderedStriterator<I, E> addFilter(IFilter<I, ?, E> filter) {
        
        return (IChunkedOrderedStriterator<I, E>) super.addFilter(filter);
        
    }

}
