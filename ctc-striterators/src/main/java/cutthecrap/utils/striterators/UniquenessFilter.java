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

import java.util.HashSet;
import java.util.Set;

/******************************************************************************
 * UniquenessFilter
 * 
 * Derived from Filter, and ensures no duplication, not to be used with large
 * sets!
 * 
 * @todo Side effects can occur via {@link #m_members} across invocations with
 *       this filter.
 */
public class UniquenessFilter extends Filter {

    private static final long serialVersionUID = 424197241022124358L;
//    ArrayList m_members = new ArrayList();
    private final Set<Object> m_members = new HashSet<Object>();
	
  public UniquenessFilter() {
  }
  
	/***********************************************************************
	 * Just make sure that the current object has not already been returned
	 **/
  public boolean isValid(Object obj) {
      return m_members.add(obj);
//  	if (m_members.contains(obj)) {
//  		return false;
//  	}
//  	m_members.add(obj);
//  	
//  	return true;
  }
}
