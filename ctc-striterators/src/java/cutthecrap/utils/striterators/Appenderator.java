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