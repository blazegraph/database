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