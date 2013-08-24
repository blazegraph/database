/*
Striterator - transformation and mapping patterns over java Iterators

Copyright (C) SYSTAP, LLC 2010.  All rights reserved.

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

package cutthecrap.utils.striterators;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Supports standard iteration over an object Array, allowing this to be used as
 * a source for a <code>Striterator</code>.
 */
public class ArrayIterator<T> implements Iterator<T> {

    /** Source array. */
    private final T[] m_src;
    
    /** Current index (next to be visited). */
	private int m_index;
	
	/** Exclusive upper bound. */
	private final int m_last;

    /** Constructor takes source object array **/
    public ArrayIterator(final T[] src) {
        this(src, 0, src.length);
    }

    /** Constructor takes source object array **/
    public ArrayIterator(final T[] src, final int off, final int len) {
        if (src == null)
            throw new NullPointerException();
        if (off < 0)
            throw new IllegalArgumentException();
        if (len < 0)
            throw new IllegalArgumentException();
        if (off + len > src.length)
            throw new IllegalArgumentException();
        m_src = src;
        m_index = off;
        m_last = off + len;
    }

	/** checks with current index and array size **/
	public boolean hasNext() {
		return m_last > m_index;
	}

    /** @return current index from array **/
    public T next() {
        if (m_index < m_last)
            return m_src[m_index++];
        else
            throw new NoSuchElementException();
    }

    /** void .. does nothing **/
    public void remove() {
    }
}
