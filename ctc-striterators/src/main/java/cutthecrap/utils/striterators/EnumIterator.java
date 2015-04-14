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
import java.util.Enumeration;

/**
 * Wrapper for Enumeration objects to produce an Iterator.
 */

public class EnumIterator implements Iterator {

	protected Enumeration m_enum = null;

	public EnumIterator(Enumeration p_enum)	{
		m_enum = p_enum;
	}

	public boolean hasNext() {
		return m_enum.hasMoreElements();
	}

	public Object next() {
		return m_enum.nextElement();
	}

	public void remove() {
		throw new UnsupportedOperationException("Remove Not Supported by underlying Enumeration");
	}
}
