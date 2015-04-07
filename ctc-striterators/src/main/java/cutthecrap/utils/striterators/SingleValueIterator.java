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



/***************************************************************************
 * SingleValueIterator
 *
 * Only one value but need to return an iterator?  This makes it easy.
 */

 public class SingleValueIterator<E> implements ICloseableIterator<E> {
	private E m_value;
	private boolean m_hasNext = true;
	
	public SingleValueIterator(final E value) {
		m_value = value;
	}
	public boolean hasNext() {
		return m_hasNext;
	}
	public E next() {
    if (!m_hasNext) {
      m_value = null;
    } else {
		  m_hasNext = false;
    }
	
		return m_value;
	}
	public void remove() {
		m_hasNext = false;
		m_value = null;
	}

	@Override
    public void close() {
	    m_hasNext = false;
    }
}


