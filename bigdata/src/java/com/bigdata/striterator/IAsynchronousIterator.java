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
 * Created on Aug 27, 2008
 */

package com.bigdata.striterator;

import java.util.concurrent.TimeUnit;

/**
 * Interface for iterators that are running asynchronously.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAsynchronousIterator<E> extends ICloseableIterator<E> {

    /**
     * Wait up to timeout units for the next chunk. The method will return
     * before the timeout if there are no more elements available from the
     * producer or if the <i>minChunkSize</i> has been satisified.
     * 
     * @param minChunkSize
     *            The #of elements in the desired chunk. There may be fewer than
     *            this many elements if the timeout expires first. There may be
     *            fewer than this many elements if the desired chunk size
     *            exceeds the internal capacity of the iterator (that is, if the
     *            iterator would block).
     * @param timeout
     *            The timeout.
     * @param unit
     *            The unit for that timeout.
     * 
     * @return The next chunk.
     */
    public E[] nextChunk(int minChunkSize, long timeout, TimeUnit unit);

}
