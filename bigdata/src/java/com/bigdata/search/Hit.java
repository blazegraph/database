package com.bigdata.search;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Metadata about a search result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add integer rank?
 */
public class Hit implements IHit, Comparable<Hit>{

    final protected static Logger log = Logger.getLogger(Hit.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();
   
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
    
    void setDocId(long docId) {
        
        this.docId = docId;

    }

    /**
     * The #of terms for which a hit was reported for this document.
     */
    public int getTermCount() {
        
        return nterms;
        
    }
    
    public double getCosine() {
        
        return cosine;

    }

    public long getDocId() {
     
        return docId;
        
    }

    /**
     * Adds another component to the cosine.
     */
    public void add(String term, double weight) {
        
        cosine += weight;

        nterms ++;

        if(DEBUG) {
        
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
    public int compareTo(Hit o) {

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
