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
public class Contractorator extends Prefetch {
	private final Iterator m_src;
	protected final Object   m_ctx;
	private final Contractor m_contractor;

	public Contractorator(Iterator src, final Object ctx, Contractor contractor) {
		m_src = src;
		m_ctx = ctx;
		m_contractor = contractor;
	}
	
	protected Object getNext() {
		if (m_src.hasNext()) {
			return m_contractor.contract(m_src);
		} else {
			return null;
		}
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

}
