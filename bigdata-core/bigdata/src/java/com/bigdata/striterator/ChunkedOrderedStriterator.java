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

/**
 * Chunked ordered streaming iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkedOrderedStriterator<I extends IChunkedOrderedIterator<E>, E>
        extends ChunkedStriterator<I, E> implements
        IChunkedOrderedStriterator<I, E> {

    public ChunkedOrderedStriterator(final I src) {

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
    public ChunkedOrderedStriterator(final int chunkSize, final Iterator<E> src) {

        this((I) new ChunkedWrappedIterator<E>(src, chunkSize,
                null/* keyOrder */, null/*filter*/));
        
    }
    
    @Override
    final public IKeyOrder<E> getKeyOrder() {

        return src.getKeyOrder();
        
    }

    @Override
    final public E[] nextChunk(final IKeyOrder<E> keyOrder) {
        
        return src.nextChunk(keyOrder);
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Strengthened return type.
     */
    @Override
    public IChunkedOrderedStriterator<I, E> addFilter(
            final IFilter<I, ?, E> filter) {

        return (IChunkedOrderedStriterator<I, E>) super.addFilter(filter);

    }

}
