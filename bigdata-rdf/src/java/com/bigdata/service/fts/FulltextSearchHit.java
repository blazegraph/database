/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
import com.bigdata.service.fts.FTS.TargetType;


/**
 * Metadata about a search result against an external fulltext index.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class FulltextSearchHit<V extends Comparable<V>> implements IFulltextSearchHit<V>,
        Comparable<FulltextSearchHit<V>> {

   protected final String res;
   protected final double score;
   protected final String snippet;
   protected final IBindingSet incomingBindings;
   protected final TargetType targetType;

   
   
   public FulltextSearchHit(
         final String res, final double score, final String snippet,
         final IBindingSet incomingBindings, final TargetType targetType) {
      this.res = res;
      this.score = score;
      this.snippet = snippet;
      this.incomingBindings = incomingBindings;
      this.targetType = targetType;
   }

   public String toString() {

      return "SolrSearchHit{score=" + score + ",snippet=" + snippet + "}";

   }

   /**
    * Sorts {@link FulltextSearchHit}s into decreasing cosine order with ties broken
    * by the the <code>docId</code>.
    */
   public int compareTo(final FulltextSearchHit<V> o) {

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
   public double getScore() {
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
   public TargetType getTargetType() {
      return targetType;
   }

}
