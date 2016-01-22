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

/***************************************************************************
 * <p>Need to return an iterator to indicate that there's nothing there?  Here's
 *  one ready made.</p>
 *
 * <p>It allows calls to be made without needing to check for a null iterator.</p>
 */

public final class EmptyIterator<T> implements Iterator<T> {
  public boolean hasNext() {
    return false;
  }
  
  public T next() {
    return null;
  }
  
  public void remove() {}
  
  final static public EmptyIterator DEFAULT = new EmptyIterator();
}

