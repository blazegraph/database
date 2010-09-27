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
 * Expanderator
 *
 * Flattens out a two-level iteration.  By combining Expanderators recursively a general tree
 *	iteration is provided.
 *
 * Provides resolution for both the child object and also the nested iterator.
 *	The actual expansion is via an Expander object that is passed in at construction.
 */

public class Expanderator implements Iterator {

	Iterator m_src;
	Iterator m_child = null;
	
	Expander m_expander;
	
  public Expanderator(Iterator src, Expander expander) {
    m_src = src;
    m_expander = expander;
  }

  //-------------------------------------------------------------
  
  public boolean hasNext() {
  	if (m_child != null && m_child.hasNext()) {
  		return true;
  	} else if (m_src.hasNext()) {
  		m_child = m_expander.expand(m_src.next());
  		
  		return hasNext();
  	} else {
  		return false;
  	}
  }
  
  //-------------------------------------------------------------
  // must call hasNext() to ensure m_child is setup
  public Object next() {
  	if (hasNext()) {
  		return m_child.next();
  	}
  	
  	throw new NoSuchElementException("Expanderator");
  }
  
  //-------------------------------------------------------------
  
  public void remove() {
  	m_child.remove();
  }
}