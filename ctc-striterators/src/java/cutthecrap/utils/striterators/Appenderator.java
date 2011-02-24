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

import cutthecrap.utils.striterators.IStriterator.ITailOp;

/**
 * Appenderator
 **/

public class Appenderator extends Prefetch implements ITailOp {

	private final Iterator m_src;
	protected final Object m_ctx;
	private final Iterator m_xtra;

	private Iterator m_current;
	private boolean m_isxtra = false;

	public Appenderator(Iterator src, Object ctx, Iterator xtra) {
		m_src = src;
		m_ctx = ctx;
		m_xtra = xtra;

		m_current = m_src;
	}

	// -------------------------------------------------------------

	protected Object getNext() {
		Object ret = null;
		if (m_current.hasNext()) {
			ret = m_current.next();
		} else if (m_isxtra) { // no need to call twice
			return null;
		} else {		
			m_current = m_xtra;
			m_isxtra = true;
	
			if (m_current.hasNext()) {
				ret = m_current.next();
			}
		}
		// experimental tail optimisation
		if (m_current instanceof ITailOp) {
			m_current = ((ITailOp) m_current).availableTailOp();
		}
		
		return ret;
	}

	public Iterator availableTailOp() {
		if (m_isxtra) {
			return m_current;
		} else {
			return this;
		}
	}

	// -------------------------------------------------------------

	public void remove() {
		m_current.remove();
	}
}