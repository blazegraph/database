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

import cutthecrap.utils.striterators.IStriterator.ITailOp;

public class Expanderator extends Prefetch implements ITailOp {

	private final Iterator m_src;
	private Iterator m_child = null;
	protected final Object m_context;
	private final Expander m_expander;
	private final IContextMgr m_contextMgr;

	public Expanderator(Iterator src, Object context, Expander expander) {
		m_src = src;
		m_context = context;
		m_expander = expander;
		m_contextMgr = m_expander.getContextMgr();
	}

	// -------------------------------------------------------------

	protected Object getNext() {
		if (m_child != null && m_child.hasNext()) {
			final Object ret = m_child.next();
			
			// experimental tail optimisation
			if (m_child instanceof ITailOp) {
				m_child = ((ITailOp) m_child).availableTailOp();
			}

			return ret;
		} else {
			if (m_child != null && m_contextMgr != null)
				m_contextMgr.popContext();
			
			if (m_src.hasNext()) {
				final Object nxt = m_src.next();
				if (m_contextMgr != null)
					m_contextMgr.pushContext(nxt);
				
				m_child = m_expander.expand(nxt);

				return getNext();
			} else {
				return null;
			}
		}
	}

	// -------------------------------------------------------------

	public void remove() {
		m_child.remove();
	}

	public Iterator availableTailOp() {
		// do NOT optimize if m_contextMhr is non null
		if (m_contextMgr == null && (!ready()) && !m_src.hasNext()) {
			return m_child;
		} else {
			return this;
		}
	}
}
