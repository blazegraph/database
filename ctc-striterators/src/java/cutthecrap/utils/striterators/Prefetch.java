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

public abstract class Prefetch implements Iterator {
	private Object m_next;
	private boolean m_ready = false;

	final private void checkInit() {
		if (!m_ready) {
			m_next = getNext();
			m_ready = true;
		}
	}

	abstract protected Object getNext();

	public boolean hasNext() {
		checkInit();

		return m_next != null;
	}

	public Object next() {
		checkInit(); // check prefetch is ready

		if (m_next == null) {
			throw new NoSuchElementException();
		}

		Object ret = m_next;

		// do not prefetch on next() since this may cause problems with
		// side-effecting
		// overides
		m_next = null;
		m_ready = false;

		return ret;
	}
	
	protected boolean ready() {
		return m_ready;
	}
}
