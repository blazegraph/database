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
import java.util.Enumeration;

/**
 * Striterator - transformation and mapping patterns over java {@link Iterator}
 * s.
 * <p>
 * Extends the {@link Iterator} interface to expose methods to add
 * {@link IFilter} objects and a specific Type filter
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
