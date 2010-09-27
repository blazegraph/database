/*
Striterator - transformation and mapping patterns over java Iterators

Copyright (C) SYSTAP, LLC 2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
