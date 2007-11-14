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

import java.util.NoSuchElementException;

import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Iterator visits {@link SPO}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISPOIterator extends IChunkedIterator<SPO> {

    public final long NULL = IRawTripleStore.NULL;

    public final long N = IRawTripleStore.N;
    
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
     * @throws IllegalArgumentException
     *             if the <i>keyOrder</i> is <code>null</code>.
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
