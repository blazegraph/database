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
 * Created on Sep 18, 2009
 */

package com.bigdata.striterator;

import java.util.Iterator;

import cutthecrap.utils.striterators.ICloseable;
import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A fly weight conversion that wraps a normal {@link IChunkedIterator} as an
 * {@link ICloseableIterator} visiting the source iterator's chunks, i.e., this
 * converts between an {@link IChunkedIterator} and an {@link Iterator} visiting
 * chunks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: CloseableIteratorWrapper.java 7396 2013-09-12 13:53:51Z
 *          thompsonbry $
 * @param <E>
 *            The generic type of the chunk elements in the source iterator.
 * 
 * @see Dechunkerator
 */
public class CloseableChunkedIteratorWrapperConverter<E> implements
        ICloseableIterator<E[]> {

    private final IChunkedIterator<E> src;

    public CloseableChunkedIteratorWrapperConverter(
            final IChunkedIterator<E> src) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;

    }

    /** Delegate to the source iff the source implements {@link ICloseable}. */
    @Override
    public void close() {

        if (src instanceof ICloseable) {

            ((ICloseable) src).close();

        }

    }

    @Override
    public boolean hasNext() {
        return src.hasNext();
    }

    @Override
    public E[] next() {
        return src.nextChunk();
    }

    @Override
    public void remove() {
        src.remove();
    }

}
