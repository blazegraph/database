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
import java.util.Comparator;

/**
 * Used with Mergerator by Striterator to merge returned objects.
 */

public class Merger extends FilterBase {
	
    protected Iterator m_xtra = null; // @todo Non-Serializable.
	protected Comparator m_comparator = null;

	public Merger()	{}

	public Merger(Iterator xtra, Comparator comparator) {
		m_xtra = xtra;
		m_comparator = comparator;
	}
	
	//-------------------------------------------------------------

	@Override
	final public Iterator filterOnce(Iterator src, Object context) {
		return new Mergerator(src, m_xtra, context, m_comparator);
	}

	//-------------------------------------------------------------
}
