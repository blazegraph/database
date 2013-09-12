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
import java.io.Serializable;



/**
 * Provides the hook interface that allows use by Striterators
 * 
 * TODO The {@link Striterator} protocol does not support a close() method for
 * {@link Filter}s. That method should be invoked by an
 * {@link ICloseableIterator}.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/582">
 *      IStriterator does not support close() protocol for Ifilter </a>
 */
public interface IFilter extends Serializable, IPropertySet {
	/**
	 * <p>The filter method is provided to allow the creation of the filtering iterator.</p>
	 *
	 * <p>Any implementation should follow the following pattern:</p>
	 * <pre>
	 *  public Iterator filter(Iterator src, Object context) {
	 *		return new Filterator(src, context, this);
	 *	}
	 *  </pre>
	 *  This pattern makes the source iterator, the evaluation context, and the
	 *  {@link IPropertySet} annotations visible to the runtime striterator
	 *  implementation.
	 **/
	public abstract Iterator filter(Iterator src, Object context);
	
}