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

import java.util.Iterator;
import java.lang.reflect.Method;

/**
 * Mapper
 *
 * Used with Mapperator by Striterator to map instance methods against member objects.
 */

public class Mapper implements IFilter {
	protected Object m_client = null;
	protected Method m_method = null;
	protected Object[] m_args = {null};
	
	public Mapper(Object client, Method method) {
		m_client = client;
		m_method = method;
	}
	
	//-------------------------------------------------------------

	final public Iterator filter(Iterator src) {
		return new Mapperator(src, this);
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