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
 * Merges two sorted lists removing equal objects.  For example, two sorted
 *	lists of Strings to create a single list with no duplicates.
 **/
public class Mergerator implements Iterator {

	final private Iterator m_setA;
	final private Iterator m_setB;
	final protected Object m_context;
	final private Comparator m_comparator;
	
	private Object m_valA = null;
	private Object m_valB = null;
	private boolean m_init = false;
	
  public Mergerator(Iterator setA, Iterator setB, Object context, Comparator comparator) {
    m_context = context;
    m_setA = setA;
    m_setB = setB;

    m_comparator = comparator;   
  }
  
  private void init() {
	  if (!m_init) {
		    if (m_setA.hasNext()) {
		    	m_valA = m_setA.next();
		    }
		    
		    if (m_setB.hasNext()) {
		    	m_valB = m_setB.next();
		    }
		    
		    m_init = true;
	  }
  }

  //-------------------------------------------------------------

  public boolean hasNext() {
	  init();
	  
  	return m_valA != null || m_valB != null;
  }

  //-------------------------------------------------------------
  // must call hasNext() to ensure m_current is correct
  public Object next() {
		if (hasNext()) {
			Object retVal = null;
			
			if (m_valB == null || (m_valA != null && (m_comparator.compare(m_valA, m_valB) <= 0))) {
				retVal = m_valA;
				m_valA = m_setA.hasNext() ? m_setA.next() : null;
			} else {
				retVal = m_valB;
				m_valB = m_setB.hasNext() ? m_setB.next() : null;
			}
			return retVal;
		} else {
  		throw new NoSuchElementException("Mergerator");
  	}
  }

  //-------------------------------------------------------------

  public void remove() {
  	throw new RuntimeException("Cannot remove object from merged set");
  }
}