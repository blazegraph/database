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

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Enumeration;

/**
 * IStriterator
 *
 * Extends the Iterator interface to expose methods to add IFilter objects and a specific Type filter
 */

public interface IStriterator extends Iterator, Enumeration {
	/** Adds a Discriminating IFilter object **/
	public IStriterator addFilter(IFilter filter);
	
	/** check each object against cls.isInstance(object) **/
	public IStriterator addTypeFilter(Class cls);
	
	/** check each object against object.getClass() == cls **/
	public IStriterator addInstanceOfFilter(Class cls);

	/** exclude the passed object from the iteration **/
	public IStriterator exclude(Object object);
	
	/** append the passed iteration **/
	public IStriterator append(Iterator iter);
	
	/** Ensures the returned values appear only once **/
	public IStriterator makeUnique();
	
	/** map the clients method against the Iteration, the Method MUST take a single Object valued parameter.
	 *	can be called by :
	 *		iter.map(this, MyClass.aMethod);
	 **/
	public IStriterator map(Object client, Method method);
	
	public interface ITailOp {
		/** 
		 * Opportunity for a Striterator to provide a "tail iterator" to
		 * shorten the call stack.  For example, an Appenderator would return
		 * the second iterator if current. Or an Expanderator the child iterator
		 * if there were no more source objects.
		 * 
		 * @return a tail optimizing iterator if possible
		 */
		public Iterator availableTailOp();
	}
}
