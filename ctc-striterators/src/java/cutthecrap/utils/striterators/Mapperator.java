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
 * Mapperator
 *
 * Initialized with a Mapper object, wraps a standard iterator and calls apply on each object as it is iterated
 */

public class Mapperator implements Iterator {

	Iterator m_iter;
	Mapper m_mapper;
	
  public Mapperator(Iterator iter, Mapper mapper) {
    m_iter = iter;
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