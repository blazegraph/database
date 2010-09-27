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
