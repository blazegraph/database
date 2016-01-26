package com.bigdata.search;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Metadata about a search result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Hit<V extends Comparable<V>> implements IHit<V>,
        Comparable<Hit<V>> {

    final private static transient Logger log = Logger.getLogger(Hit.class);
   
    /** note: defaults to an illegal value. */
    private V docId = null;
    
//    /** #of terms reporting. */
//    private int nterms;

    /** Array of whether each search term appears or does not appear in the hit. **/
//    private LongArrayBitVector searchTerms;
    private final boolean[] searchTerms;
    
    /** Net cosine for the reporting terms. */
    private double cosine;

    /** Rank order for this hit */
    private int rank;

    /**
     * Ctor used in conjunction with a {@link ConcurrentHashMap} to insert
     * objects into the result set.
     * <p>
     * Note: You must call {@link #setDocId(long)} after using this ctor.
     * 
     * @see ReadIndexTask2
     */
    Hit(final int numSearchTerms) {

//    	this.searchTerms = LongArrayBitVector.ofLength(numSearchTerms);
    	this.searchTerms = new boolean[numSearchTerms];
    	
    }
    
    synchronized void setDocId(final V docId) {
        
        if(docId == null)
            throw new IllegalArgumentException();
        
        this.docId = docId;

    }

    synchronized void setRank(final int rank) {
        
        this.rank = rank;

    }
    
//    synchronized void setNumSearchTerms(final int numSearchTerms) {
//    	
////    	this.searchTerms = LongArrayBitVector.ofLength(numSearchTerms);
//    	this.searchTerms = new boolean[numSearchTerms];
//    	
//    }
    
    /**
     * The #of terms for which a hit was reported for this document.
     */
    synchronized public int getTermCount() {
        
//    	if (searchTerms.size() == 0)
    	if (searchTerms.length == 0)
    		return 0;
    	
    	int nterms = 0;
    	for (boolean b : searchTerms)
    		if (b) nterms++;
    	
        return nterms;
        
    }
    
    synchronized public double getCosine() {
        
        return cosine;

    }

    synchronized public int getRank() {
        
        return rank;

    }
    
    synchronized public V getDocId() {
     
        return docId;
        
    }

    /**
     * Adds another component to the cosine.
     */
    public void add(final int termNdx, final double weight) {
        
        synchronized (this) {

            cosine += weight;

//            nterms++;
            
//            searchTerms.set(termNdx, true);
            searchTerms[termNdx] = true;

        }

//        if(log.isDebugEnabled()) {
//        
//            log.debug("docId=" + docId + ", term: " + term + ", nterms="
//                    + nterms + ", weight=" + weight + ", cosine=" + cosine);
//            
//        }

    }

    public String toString() {
        
        return "Hit{docId"+docId+",nterms="+getTermCount()+",cosine="+cosine+"}";
        
    }

    /**
     * Sorts {@link Hit}s into decreasing cosine order with ties broken by the
     * the <code>docId</code>.
     */
    public int compareTo(final Hit<V> o) {

        if (cosine < o.cosine)
            return 1;

        if (cosine > o.cosine)
            return -1;

        return docId.compareTo(o.docId);
        
//        if (docId < o.docId)
//            return -1;
//
//        if (docId > o.docId)
//            return 1;
//
//        return 0;
        
    }
    
}
