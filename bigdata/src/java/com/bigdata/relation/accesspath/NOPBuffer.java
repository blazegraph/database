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
 * Created on Aug 12, 2008
 */

package com.bigdata.relation.accesspath;

import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedIterator;

/**
 * Buffer that accumulates elements until it fills up. It will throw an
 * exception if you attempt to write more elements on it until you have
 * {@link #reset()} the buffer. The elements in the buffer are exposed for
 * consumption by other patterns.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class NOPBuffer<E> extends AbstractUnsynchronizedArrayBuffer<E> {

    /**
     * @param capacity
     * @param target
     */
    public NOPBuffer(int capacity) {

        super(capacity);

    }

    @Override
    protected void overflow() {

        throw new UnsupportedOperationException();

    }

    /** NOP. */
    public long flush() {

        return 0;

    }

    /**
     * Resets the #of elements in the buffer to ZERO (0).
     */
    public void reset() {

        size = 0;

    }

    /**
     * An iterator that visits the elements in the buffer. You MUST use
     * {@link #reset()} to clear the buffer after consuming its contents
     * with this iterator.
     */
    public IChunkedIterator<? extends E> iterator() {

        return new ChunkedArrayIterator<E>(size, buffer, null/* keyOrder */);

    }

}
