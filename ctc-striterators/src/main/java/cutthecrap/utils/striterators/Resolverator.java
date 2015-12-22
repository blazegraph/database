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

import java.util.*;

/**
 * Resolverator
 *
 * Initialized with a Resolver object, wraps a standard iterator and resolves each returned object
 */

public class Resolverator implements Iterator {

	private final Iterator m_iter;
	protected final Object m_context;
	private final Resolver m_resolver;
	
  public Resolverator(Iterator iter, Object context, Resolver resolver) {
    m_iter = iter;
    m_context = context;
    m_resolver = resolver;
  }

  public boolean hasNext() {
  	return m_iter.hasNext();
  }
  
  public Object next() {
	  if (hasNext())
		  return m_resolver.resolve(m_iter.next());
	  else
		  throw new NoSuchElementException("Resolverator");
  }
  
  public void remove() {
  	m_iter.remove();
  }
}