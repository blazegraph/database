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
 * Created on Nov 14, 2007
 */

package com.bigdata.join;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator that is able visit items in chunks. The elements in the chunk
 * will be in the same order that they would be visited by
 * {@link Iterator#next()}. The size of the chunk is up to the implementation.
 * <p>
 * Note: Chunked iterators are designed to make it easier to write methods that
 * use the batch APIs but do not require the data that will be visited by the
 * iterator to be fully materialized. You can use {@link #nextChunk()} instead
 * of {@link Iterator#next()} to break down the operation into N chunks, where N
 * is determined dynamically based on how much data the iterator returns in each
 * chunk and how much data there is to be read.
 * 
 * @todo verify that all {@link IChunkedIterator}s are being closed within a
 *       <code>finally</code> clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IChunkedIterator<T> extends Iterator<T> {

    /**
     * Closes the iterator, releasing any associated resources. This method MAY
     * be invoked safely if the iterator is already closed.
     * <p>
     * Note: Implementations MUST NOT eagerly close the iterator when it is
     * exhausted since that would make it impossible to remove the last visited
     * statement. Instead they MUST wait for an explicit {@link #close()} by the
     * application.
     */
    public void close();

    /**
     * The next {@link T} available from the iterator.
     * 
     * @throws NoSuchElementException
     *             if the iterator is exhausted.
     */
    public T next();
    
    /**
     * Return the next "chunk" from the iterator.
     * 
     * @return The next chunk.
     * 
     * @throws NoSuchElementException
     *             if the iterator is exhausted.
     */
    public T[] nextChunk();

    /**
     * Removes the last element visited by {@link #next()} (optional operation).
     * <p>
     * Note: This method is not readily suited for use with {@link #nextChunk()}
     * since the latter has already visited everything in the chunk and
     * {@link #remove()} would only remove the last item in the chunk. Normally
     * you will want to accumulate items to be removed in a buffer and then
     * submit the buffer to some batch api operation when it overflows.
     */
    public void remove();
    
}
