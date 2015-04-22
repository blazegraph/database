/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
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
