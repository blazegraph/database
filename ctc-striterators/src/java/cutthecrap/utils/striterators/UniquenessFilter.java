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

/******************************************************************************
 * UniquenessFilter
 *
 * Derived from Filter, and ensures no duplication, not to be used with large sets!
 */
public class UniquenessFilter extends Filter {
	ArrayList m_members = new ArrayList();
	
  public UniquenessFilter() {
  	super(null);
  }
  
	/***********************************************************************
	 * Just make sure that the current object has not already been returned
	 **/
  protected boolean isValid(Object obj) {
  	if (m_members.contains(obj)) {
  		return false;
  	}
  	m_members.add(obj);
  	
  	return true;
  }
}
