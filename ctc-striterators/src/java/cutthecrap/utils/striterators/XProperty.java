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

/**
 * A property key/value pair
 */

public class XProperty implements IXProperty {
	private String m_key;
	private Object m_value;

	public XProperty(String key, Object value) {
		m_key = key;
		m_value = value;
	}

	//------------------------------------------------------------------------

	public String getKey() {
		return m_key;
	}

	//------------------------------------------------------------------------

	public Object getValue() {
		return m_value;
	}
}
