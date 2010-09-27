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

import java.util.*;

/**
 * <p>Example usage</p>
 * <pre>
 * Iterator src = new Filterator(old, new Filter(state)) {
 *		boolean isValid(Object object) {
 *			return object == m_state;
 *		}
 * } );
 * </pre>
 *
 * <p>The Filterator provide the protocol support to utulise Filter objects.</p>
 */

public class Filterator implements Iterator {

	Iterator m_src;
	Object m_value = null;

	protected Filter m_filter = null;

	public Filterator(Iterator src, Filter filter) {
		m_src = src;
		m_filter = filter;

		m_value = getNext();
	}

	//-------------------------------------------------------------

	public boolean hasNext() {
		return m_value != null;
	}

	//-------------------------------------------------------------
	// must call hasNext() to ensure m_child is setup
	public Object next() {
		if (hasNext()) {
			Object val = m_value;
			m_value = getNext();

			return val;
		}

		throw new NoSuchElementException("FilterIterator");
	}

	//-------------------------------------------------------------

	public void remove() {
		m_src.remove();
	}

	//-------------------------------------------------------------

	protected Object getNext() {
		while (m_src.hasNext()) {
			Object next = m_src.next();

			if (m_filter.isValid(next)) {
				return next;
			}
		}

		return null;
	}
}