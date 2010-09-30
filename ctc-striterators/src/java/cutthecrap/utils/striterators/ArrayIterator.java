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
 * Supports standard iteration over an object Array, allowing this to
 *	be used as a source for a <code>Striterator</code>.
 */
public class ArrayIterator implements Iterator {
	Object[] m_src = null;
	int m_index = 0;

	/** Constructor takes source object array **/
	public ArrayIterator(Object[] src) {
		m_src = src;
	}

	/** checks with current index and array size **/
	public boolean hasNext() {
		return m_src != null && m_src.length > m_index;
	}

	/** @return current index from array **/
	public Object next() {
		if (m_index < m_src.length)
			return m_src[m_index++];
		else
			throw new NoSuchElementException("ArrayIterator");
	}

	/** void .. does nothing **/
	public void remove() {
	}
}
