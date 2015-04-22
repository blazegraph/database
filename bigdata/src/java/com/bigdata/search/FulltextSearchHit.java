package com.bigdata.search;

import org.apache.log4j.Logger;

/**
 * Metadata about a search result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FulltextSearchHit<V extends Comparable<V>> implements IFulltextSearchHit<V>,
        Comparable<FulltextSearchHit<V>> {

   final private static transient Logger log = Logger
         .getLogger(FulltextSearchHit.class);

   protected String res;
   
   protected double score;

   protected String snippet;

   public FulltextSearchHit(final String res, final double score, final String snippet) {
      this.res = res;
      this.score = score;
      this.snippet = snippet;
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

}
