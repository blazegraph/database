/*

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
 * Created on Oct 24, 2007
 */

package com.bigdata.rdf.spo;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Iterator visits {@link SPO}s.
 * 
 * @todo verify that all {@link ISPOIterator}s are being closed within a
 *       <code>finally</code> clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISPOIterator extends Iterator<SPO> {

    public final long NULL = IRawTripleStore.NULL;
    public final long N = IRawTripleStore.N;
    
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
     * The next {@link SPO} available from the iterator.
     * 
     * @throws NoSuchElementException
     *             if the iterator is exhausted.
     */
    public SPO next();
    
    /**
     * Return the next "chunk" of statements from the iterator. The statements
     * will be in the same order that they would be visited by
     * {@link Iterator#next()}. The size of the chunk is up to the
     * implementation.
     * <p>
     * This is designed to make it easier to write methods that use the batch
     * APIs but do not require the statements matching some triple pattern to be
     * fully materialized. You can use {@link #nextChunk()} instead of
     * {@link Iterator#next()} to break down the operation into N chunks, where
     * N is determined dynamically based on how much data the iterator returns
     * in each chunk and how much data there is to be read.
     * 
     * @return The next chunk of statements.
     * 
     * @see #nextChunk(KeyOrder)
     * 
     * @throws NoSuchElementException
     *             if the iterator is exhausted.
     */
    public SPO[] nextChunk();

    /**
     * Return the next "chunk" of statements from the iterator. The statements
     * will be in the specified order. If {@link #getKeyOrder()} would return
     * non-<code>null</code> and the request order corresponds to the value
     * that would be returned by {@link #getKeyOrder()} then the statements in
     * the next chunk are NOT sorted. Otherwise the statements in the next chunk
     * are sorted before they are returned. The size of the chunk is up to the
     * implementation.
     * 
     * @param keyOrder
     *            The order for the statements in the chunk.
     * 
     * @return The next chunk of statements in the specified order.
     * 
     * @throws NoSuchElementException
     *             if the iterator is exhausted.
     */
    public SPO[] nextChunk(KeyOrder keyOrder);

    /**
     * The {@link KeyOrder} in which statements are being visited and
     * <code>null</code> if not known.
     * 
     * @return The order in which statemetns are being visited -or-
     *         <code>null</code> if not known.
     */
    public KeyOrder getKeyOrder();
    
}
