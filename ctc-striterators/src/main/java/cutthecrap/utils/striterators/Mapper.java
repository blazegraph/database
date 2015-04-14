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

import java.lang.reflect.Method;
import java.util.Iterator;

/**
 * Mapper
 *
 * Used with Mapperator by Striterator to map instance methods against member objects.
 */

public class Mapper extends FilterBase {
	protected Object m_client = null;
	protected Method m_method = null; // @todo Not serializable. Defer reflection?
	protected Object[] m_args = {null};
	
	public Mapper(Object client, Method method) {
		m_client = client;
		m_method = method;
	}
	
	//-------------------------------------------------------------

	@Override
    final public Iterator filterOnce(Iterator src, Object context) {
        return new Mapperator(src, context, this);
    }

	//-------------------------------------------------------------

	protected void apply(Object obj) {
		try {
			m_args[0] = obj;
			
			m_method.invoke(m_client, m_args);
		} catch (Exception e) {
			throw new RuntimeException("Error on mapping method", e);
		}
	}
}