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

public class Filterator extends Prefetch {

	final private Iterator m_src;
	
	final protected Object m_context;
	final protected Filter m_filter;

	public Filterator(final Iterator src, final Object context, final Filter filter) {
		m_src = src;
		m_context = context;
		m_filter = filter;
	}


	//-------------------------------------------------------------

	public void remove() {
		m_src.remove();
	}

	//-------------------------------------------------------------

	protected Object getNext() {
		while (m_src.hasNext()) {
			final Object next = m_src.next();

			if (m_filter.isValid(next)) {
				return next;
			}
		}

		return null;
	}
}