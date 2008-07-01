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
 * Created on Jun 21, 2008
 */

package com.bigdata.relation.accesspath;

import java.util.NoSuchElementException;

/**
 * An extension of {@link IChunkedIterator} interface that knows about natural
 * traversal orders and how to re-order the elements that are being visited to
 * support JOINs where the natural order for the access paths is different for
 * the left- and right-hand side of the JOIN.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <R>
 *            The generic type for the [R]elation elements.
 */
public interface IChunkedOrderedIterator<R> extends IChunkedIterator<R> {

    /**
     * The natural order in which elements are being visited.
     * 
     * @return The natural order in which the elements are being visited -or-
     *         <code>null</code> if not known.
     */
    public IKeyOrder<R> getKeyOrder();
    
    /**
     * Return the next "chunk" of elements. The elements will be in the
     * specified order. If {@link #getKeyOrder()} would return non-<code>null</code>
     * and the request order corresponds to the value that would be returned by
     * {@link #getKeyOrder()} then the elements in the next chunk are NOT
     * sorted. Otherwise the elements in the next chunk are sorted before they
     * are returned. The size of the chunk is up to the implementation.
     * 
     * @param keyOrder
     *            The natural order for the elements in the chunk.
     * 
     * @return The next chunk of elements in the specified order.
     * 
     * @throws NoSuchElementException
     *             if the iterator is exhausted.
     * @throws IllegalArgumentException
     *             if the <i>keyOrder</i> is <code>null</code>.
     */
    public R[] nextChunk(IKeyOrder<R> keyOrder);
    
}
