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
 * <p>Example usage</p>
 * <pre>
 * Iterator src = new Filterator(old, new Filter(state)) {
 *		boolean isValid(Object object) {
 *			return object == m_state;
 *		}
 * } );
 * </pre>
 *
 * <p>The Filterator provide the protocol support to utilize Filter objects.</p>
 */

public class Filterator implements Iterator {

	final private Iterator m_src;
	
	/**
	 * Flag set once {@link #getNext()} is invoked at least once.
	 */
	private boolean didInit = false;
	
	/**
	 * Pre-fetched, but initial prefetch must not be done in the constructor.
	 */
	private Object m_value = null;

	final protected Object m_context;
	final protected Filter m_filter;

	public Filterator(final Iterator src, final Object context, final Filter filter) {
		m_src = src;
		m_context = context;
		m_filter = filter;

        /*
         * Note: eager initialization causes problems when we are stacking
         * filters.
         */
//		m_value = getNext();
	}

	//-------------------------------------------------------------

    public boolean hasNext() {
        if (!didInit) {
            m_value = getNext();
        }
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
        didInit = true;
		while (m_src.hasNext()) {
			final Object next = m_src.next();

			if (m_filter.isValid(next)) {
				return next;
			}
		}

		return null;
	}
}