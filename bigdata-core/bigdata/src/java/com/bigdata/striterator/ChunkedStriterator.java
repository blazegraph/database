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

import com.bigdata.btree.ITupleIterator;
//import com.bigdata.btree.filter.IFilter;

/**
 * Chunked streaming iterator.
 * 
 * @todo Like the {@link ITupleIterator}, the {@link IChunkedIterator}s can be
 *       executed on the server and them stream results back to the client. We
 *       really need to introduce custom compression for chunk-at-a-time results
 *       streamed back from the server. If the filters are to be specified on
 *       the client and applied on the server, then an
 *       {@link IFilter} will be needed to construct the filter stack
 *       on the server.
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkedStriterator<I extends IChunkedIterator<E>, E> extends
        Striterator<I, E> implements IChunkedStriterator<I, E> {

    public ChunkedStriterator(final I src) {

        super(src);

        this.src = src;

    }
    
    /**
     * Wraps the source iterator as a chunked iterator using a default chunk
     * size.
     * 
     * @param src
     *            The source iterator.
     */
    public ChunkedStriterator(final Iterator<E> src) {
     
        this(IChunkedIterator.DEFAULT_CHUNK_SIZE, src);
        
    }

    /**
     * Wraps the source iterator as a chunked iterator.
     * 
     * @param chunkSize
     *            The chunk size.
     * @param src
     *            The source iterator.
     */
    @SuppressWarnings("unchecked")
    public ChunkedStriterator(final int chunkSize, final Iterator<E> src) {

        this((I) new ChunkedWrappedIterator<E>(src, chunkSize,
                null/* keyOrder */, null/* filter */));

    }

    @Override
    final public E[] nextChunk() {

        return src.nextChunk();

    }

//    @Override
//    final public void close() {
//
//        ((ICloseableIterator<?>) src).close();
//
//    }

    /**
     * Strengthened return type.
     */
    @Override
    public IChunkedStriterator<I, E> addFilter(final IFilter<I, ?, E> filter) {

        return (IChunkedStriterator<I, E>) super.addFilter(filter);

    }

}
