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
	
  public Mergerator(Iterator setA, Iterator setB, Object context, Comparator comparator) {
    m_context = context;
    m_setA = setA;
    m_setB = setB;

    m_comparator = comparator;
    
    if (m_setA.hasNext()) {
    	m_valA = m_setA.next();
    }
    
    if (m_setB.hasNext()) {
    	m_valB = m_setB.next();
    }
  }

  //-------------------------------------------------------------

  public boolean hasNext() {
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