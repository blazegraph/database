package com.bigdata.search;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Metadata about a search result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Hit implements IHit, Comparable<Hit>{

    final private static transient Logger log = Logger.getLogger(Hit.class);
   
    /** note: defaults to an illegal value. */
    private long docId = -1;
    
    /** #of terms reporting. */
    private int nterms;
    
    /** Net cosine for the reporting terms. */
    private double cosine;
    
    /**
     * Ctor used in conjunction with a {@link ConcurrentHashMap} to insert
     * objects into the result set.
     * <p>
     * Note: You must call {@link #setDocId(long)} after using this ctor.
     * 
     * @see ReadIndexTask
     */
    Hit() {

    }
    
    synchronized void setDocId(final long docId) {
        
        this.docId = docId;

    }

    /**
     * The #of terms for which a hit was reported for this document.
     */
    synchronized public int getTermCount() {
        
        return nterms;
        
    }
    
    synchronized public double getCosine() {
        
        return cosine;

    }

    synchronized public long getDocId() {
     
        return docId;
        
    }

    /**
     * Adds another component to the cosine.
     */
    public void add(final String term, final double weight) {
        
        synchronized (this) {

            cosine += weight;

            nterms++;

        }

        if(log.isDebugEnabled()) {
        
            log.debug("docId=" + docId + ", term: " + term + ", nterms="
                    + nterms + ", weight=" + weight + ", cosine=" + cosine);
            
        }

    }

    public String toString() {
        
        return "Hit{docId"+docId+",nterms="+nterms+",cosine="+cosine+"}";
        
    }

    /**
     * Sorts {@link Hit}s into decreasing cosine order with ties broken by the
     * the <code>docId</code>.
     */
    public int compareTo(final Hit o) {

        if (cosine < o.cosine)
            return 1;

        if (cosine > o.cosine)
            return -1;

        if (docId < o.docId)
            return -1;

        if (docId > o.docId)
            return 1;

        return 0;
        
    }
    
}
