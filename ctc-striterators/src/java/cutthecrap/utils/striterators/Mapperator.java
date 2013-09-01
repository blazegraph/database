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
 * Mapperator
 *
 * Initialized with a Mapper object, wraps a standard iterator and calls apply on each object as it is iterated
 */

public class Mapperator implements Iterator {

	final private Iterator m_iter;
	final protected Object m_context;
	final private Mapper m_mapper;
	
  public Mapperator(Iterator iter, Object context, Mapper mapper) {
    m_iter = iter;
    m_context = context;
    m_mapper = mapper;
  }

  public boolean hasNext() {
  	return m_iter.hasNext();
  }
  
  public Object next() {
  	Object obj = m_iter.next();
  	
  	m_mapper.apply(obj);
  	
  	return obj;
  }
  
  public void remove() {
  	m_iter.remove();
  }
}