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
 * Created on Dec 11, 2006
 */

package com.bigdata.btree;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Interface exposes the key and value associated with the object most recently
 * visited an {@link Iterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IEntryIterator extends Iterator {

    /**
     * The value for the next entry.
     * 
     * @return The value for the next entry -or- <code>null</code> if values
     *         were not requested when the iterator was provisioned.
     * 
     * @throws NoSuchElementException
     *             if there is no next entry.
     */
    public Object next();
    
    /**
     * The key for the last entry visited by {@link Iterator#next()}.
     * <p>
     * Note: {@link Tuple#getKeyBuffer()} is potentially much more efficient.
     * 
     * @throws IllegalStateException
     *             if no entries have been visited.
     * 
     * @throws UnsupportedOperationException
     *             if the iterator was not provisioned to return the keys.
     * 
     * @see #getTuple()
     * @see Tuple#getKeyBuffer()
     */
    public byte[] getKey();
    
    /**
     * The value associated with the last entry visited by
     * {@link Iterator#next()}.
     * 
     * @throws IllegalStateException
     *             if no entries have been visited.
     * 
     * @throws UnsupportedOperationException
     *             if the iterator was not provisioned to return the values.
     */
    public Object getValue();
    
    /**
     * The {@link Tuple} exposes a lower-level interface to the keys and values
     * that may be used to access them without causing allocations on the heap.
     * 
     * @return The current {@link Tuple}.
     * 
     * @throws IllegalStateException
     *             if no entries have been visited.
     */
    public ITuple getTuple();
    
}
