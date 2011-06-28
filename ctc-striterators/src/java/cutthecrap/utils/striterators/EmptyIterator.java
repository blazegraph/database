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

/***************************************************************************
 * <p>Need to return an iterator to indicate that there's nothing there?  Here's
 *  one ready made.</p>
 *
 * <p>It allows calls to be made without needing to check for a null iterator.</p>
 */

public final class EmptyIterator implements Iterator {
  public boolean hasNext() {
    return false;
  }
  
  public Object next() {
    return null;
  }
  
  public void remove() {}
  
  final static public EmptyIterator DEFAULT = new EmptyIterator();
}

