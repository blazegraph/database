/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.service.fts;

import java.util.Iterator;

/**
 * Visits external fulltext index search results in order of decreasing
 * relevance.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class FulltextSearchHiterator<A extends IFulltextSearchHit> implements
      Iterator<A> {

   /**
    * The index into the array of hits wrapped by this iterator.
    */
   private int rank = 0;

   /**
    * The array of hits wrapped by this iterator.
    */
   private final A[] hits;

   /**
    * 
    * @param hits
    */
   public FulltextSearchHiterator(final A[] hits) {

      if (hits == null)
         throw new IllegalArgumentException();

      this.hits = hits;

   }

   public boolean hasNext() {

      return rank < hits.length;

   }

   public A next() {

      return hits[rank++];

   }

   /**
    * @throws UnsupportedOperationException
    */
   public void remove() {

      throw new UnsupportedOperationException();

   }

   public String toString() {

      return "FulltextSearchHiterator{nhits=" + hits.length + "} : " + hits;

   }

}
