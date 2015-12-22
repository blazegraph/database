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
