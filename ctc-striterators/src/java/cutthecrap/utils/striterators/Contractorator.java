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
 * A Contractorator is the reverse of an Expanderator. A Contractorator takes a
 * source Iterator, and consumes elements from it in batches by passing to the
 * Contractor which returns an object when ready. It is upto the Contractor to
 * handle termination conditions. For example, it might batch consume the
 * objects ten at a time with some summary info, but handle that there may be
 * less than ten elements in the final summary.
 * 
 * A Contractor could also be used to chunk up a number of objects for
 * serialization, perhaps for network transmission. Such a pattern might be used
 * with an Expander on the other side to deserialize into an iterator.
 * 
 * @author Martyn Cutcher
 */
public class Contractorator implements Iterator {
	private final Iterator m_src;
	protected final Object   m_ctx;
	private final Contractor m_contractor;
	private Object m_next;

	public Contractorator(Iterator src, final Object ctx, Contractor contractor) {
		m_src = src;
		m_ctx = ctx;
		m_contractor = contractor;

		m_next = m_contractor.contract(m_src);
	}

	public boolean hasNext() {
		return m_next != null;
	}

	public Object next() {
		if (m_next == null) {
			throw new NoSuchElementException();
		}

		Object ret = m_next;
		m_next = m_contractor.contract(m_src);

		return ret;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

}
