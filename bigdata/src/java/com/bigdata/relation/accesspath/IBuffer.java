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
 * Created on Jun 20, 2008
 */

package com.bigdata.relation.accesspath;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;

/**
 * A buffer abstraction.
 * <p>
 * An {@link AbstractArrayBuffer} is generally used to write on an
 * {@link IRelation} while {@link BlockingBuffer} may be used to feed an
 * iterator on which another process will read asynchronously. An
 * {@link UnsynchronizedArrayBuffer} may be used in single-threaded contexts and
 * offers reduced synchronization overhead.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBuffer<E> {

    /**
     * The #of elements currently in the buffer.
     */
    public int size();

    /**
     * True iff there are no elements in the buffer.
     */
    public boolean isEmpty();
    
    /**
     * Add an element to the buffer.
     * 
     * @param e
     *            The element
     */
    public void add(E e);

    /**
     * Add a set of elements to the buffer. This operation is potentially more
     * efficient than invoking {@link #add(Object)} for each element in turn.
     * 
     * @param n
     *            The #of elements in the set.
     * 
     * @param a
     *            The set of elements.
     */
    public void add(int n, E[] a);
    
    /**
     * Flush the buffer and return the #of elements written on the backing
     * {@link IRelation} since the counter was last {@link #reset()} (the
     * <i>mutationCount</i>).
     * <p>
     * Note: If the buffer does not write on an {@link IRelation} then it SHOULD
     * return ZERO(0).
     * 
     * @return The #of elements written on the backing {@link IRelation}.
     * 
     * See {@link IMutableRelation}
     */
    public long flush();

    /**
     * Reset the state of the buffer, including the counter whose value is
     * reported by {@link #flush()}. Any data in the buffer will be discarded.
     */
    public void reset();
    
}
