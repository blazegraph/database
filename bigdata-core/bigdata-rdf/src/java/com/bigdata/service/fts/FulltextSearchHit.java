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

import com.bigdata.bop.IBindingSet;
import com.bigdata.service.fts.FTS.SearchResultType;


/**
 * Metadata about a search result against an external fulltext index.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class FulltextSearchHit<V extends Comparable<V>> implements IFulltextSearchHit<V>,
        Comparable<FulltextSearchHit<V>> {

   protected final String res;
   protected final Double score;
   protected final String snippet;
   protected final IBindingSet incomingBindings;
   protected final SearchResultType searchResultType;

   
   
   public FulltextSearchHit(
         final String res, final Double score, final String snippet,
         final IBindingSet incomingBindings, 
         final SearchResultType searchResultType) {
      this.res = res;
      this.score = score;
      this.snippet = snippet;
      this.incomingBindings = incomingBindings;
      this.searchResultType = searchResultType;
   }

   public String toString() {

      return "FulltextSearchHit{res=" + res + ",score=" + score + ",snippet=" + snippet + "}";

   }

   /**
    * Sorts {@link FulltextSearchHit}s into decreasing cosine order with ties broken
    * by the the <code>docId</code>.
    */
   public int compareTo(final FulltextSearchHit<V> o) {

      if (score==null) {
         if (o.score==null) {
            return 0;
         } else {
            return 1;
         }
      } else if (o.score==null) {
         
         return -1;
      }
      
      if (score < o.score)
         return 1;
      else if (score > o.score)
         return -1;
      
      return res.compareTo(o.res);

   }
   
   @Override
   public String getRes() {
      return res;
   }

   @Override
   public Double getScore() {
      return score;
   }

   @Override
   public String getSnippet() {
      return snippet;
   }

   @Override
   public IBindingSet getIncomingBindings() {
      return incomingBindings;
   }
   
   @Override
   public SearchResultType getSearchResultType() {
      return searchResultType;
   }

}
