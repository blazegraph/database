package com.bigdata.text;

/**
 * Metadata about a search result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add integer rank?
 */
public class Hit implements IHit {

    private final long docId;
    
    /** #of terms reporting. */
    private int nterms;
    
    /** Net cosine for the reporting terms. */
    private double cosine;
    
    /**
     * 
     * @param docId
     *            A document with a match on at least one term.
     */
    Hit(long docId) {

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
    public void add(double weight) {
        
        cosine += weight;

        nterms ++;

    }

    public String toString() {
        
        return "Hit{docId"+docId+",nterms="+nterms+",cosine="+cosine+"}";
        
    }
    
}
