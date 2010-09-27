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

/**
 * Resolver
 *
 * Used with Resolverator by Striterator to resolve returned objects.
 *
 * Cannot be instantiated directly since an implementation of the resolve method is required.
 */

public abstract class Resolver implements IFilter {
	protected Object m_state = null;
	
	public Resolver()	{}

	public Resolver(Object state) {
		m_state = state;
	}
	
	//-------------------------------------------------------------

	final public Iterator filter(Iterator src) {
		return new Resolverator(src, this);
	}

	//-------------------------------------------------------------

	protected abstract Object resolve(Object obj);
}