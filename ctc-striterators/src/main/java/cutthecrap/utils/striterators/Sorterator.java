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

/**
 * Initialized with a Sorter object, wraps a standard iterator and resolves each
 * returned object
 */

public class Sorterator implements Iterator {

	private final Iterator m_src;
	private Iterator m_iter;
	private final Sorter m_sorter;
	protected final Object m_context;
	private boolean m_doneSort = false;

	public Sorterator(Iterator iter, Object context, Sorter sorter) {

		m_context = context;
		m_src = iter;
		m_sorter = sorter;

	}

	private void sort() {
		if (!m_doneSort) {
			// materialize the objects to be sorted.
			LinkedList tmp = new LinkedList();

			while (m_src.hasNext()) {
				tmp.add(m_src.next());
			}

			Object[] a = tmp.toArray();

			Arrays.sort(a, m_sorter/* .getComparator() */);

			m_iter = Arrays.asList(a).iterator();
			m_doneSort = true;
		}
	}

	public boolean hasNext() {
		sort();
		
		return m_iter.hasNext();
	}

	public Object next() {
		if (hasNext())
			return m_iter.next();
		else
			throw new NoSuchElementException();
	}

	public void remove() {
		m_iter.remove();
	}
}