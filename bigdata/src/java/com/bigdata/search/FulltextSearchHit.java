package com.bigdata.search;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.store.FTS.TargetType;


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
