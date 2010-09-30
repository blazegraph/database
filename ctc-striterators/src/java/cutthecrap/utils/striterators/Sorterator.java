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
 * Initialized with a Sorter object, wraps a standard iterator and resolves each returned object
 */

public class Sorterator implements Iterator {

    private final Iterator m_iter;
    protected final Object m_context;
    
    public Sorterator(Iterator iter, Object context, Sorter sorter) {

        this.m_context = context;

        // materialize the objects to be sorted.
        LinkedList tmp = new LinkedList();
        
        while(iter.hasNext()) {
            tmp.add(iter.next());
        }
        
        Object[] a = tmp.toArray();
        
        Arrays.sort(a, sorter/*.getComparator()*/);
        
        m_iter = Arrays.asList(a).iterator();
        
//        TreeSet set = new TreeSet(sorter);
//
//        while (iter.hasNext()) {
//            set.add(iter.next());
//        }
//
//        m_iter = set.iterator();

    }

  public boolean hasNext() {
    return m_iter.hasNext();
  }
  
  public Object next() {
    return m_iter.next();
  }
  
  public void remove() {
    m_iter.remove();
  }
}