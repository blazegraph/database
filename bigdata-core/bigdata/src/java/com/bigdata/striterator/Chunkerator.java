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
 * Created on Oct 14, 2011
 */

package com.bigdata.striterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.relation.accesspath.IElementFilter;

import cutthecrap.utils.striterators.ICloseable;
import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Iterator "chunks" up another iterator, visiting arrays of elements at a time.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Chunkerator<E> implements ICloseableIterator<E[]> {

    private final IChunkedIterator<E> src;

    private boolean open = true;
    
    public Chunkerator(final Iterator<E> src) {

        this(src, ChunkedWrappedIterator.DEFAULT_CHUNK_SIZE);

    }

    public Chunkerator(final Iterator<E> src, final int chunkSize) {

        this(src, chunkSize, null/* elementClass */);

    }

    public Chunkerator(final Iterator<E> src, final int chunkSize,
            Class<? extends E> elementClass) {

        this(src, chunkSize, elementClass, null/* keyOrder */, null/* filter */);

    }

    public Chunkerator(final Iterator<E> src, final int chunkSize,
            final Class<? extends E> elementClass, final IKeyOrder<E> keyOrder,
            final IElementFilter<E> filter) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = new ChunkedWrappedIterator<E>(src, chunkSize, elementClass,
                keyOrder, filter);

    }

    @Override
    public boolean hasNext() {
        if (!open)
            return false;
        if (!src.hasNext()) {
            close();
            return false;
        }
        return true;
    }

    @Override
    public E[] next() {
        if (!hasNext())
            throw new NoSuchElementException();
        return src.nextChunk();
    }

    /**
     * Unsupported operation.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (open) {
            open = false;
            if (src instanceof ICloseable) {
                ((ICloseable) src).close();
            }
        }
    }

}
