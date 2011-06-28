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

/******************************************************************************
 * Derived from Filter, and excludes a single object from the iteration.
 */
public class ExclusionFilter extends Filter {
  private Object m_exclude;
  
  public ExclusionFilter(Object exclude) {
  	m_exclude = exclude;
  }
  
	/***********************************************************************
	 * Just make sure that the current object is not the one to be excluded.
	 **/
  public boolean isValid(Object obj) {
  	return obj != m_exclude;
  }
}
