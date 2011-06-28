package com.bigdata.search;

/**
 * Mutable metadata for the occurrences of a term within a field of some
 * document.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TermMetadata implements ITermMetadata {

//    /**
//     * The token.
//     */
//    private final String token;
    
    /**
     * The local term weight, which may be computed by a variety of methods.
     */
    private double localTermWeight;
    private int noccurrences;

//    public TermMetadata(final String token) {
//        
//        this.token = token;
//    }
//    
//    public String termText() {
//        
//        return token;
//        
//    }
    
    public int termFreq() {
    
        return noccurrences;
        
    }
    
    final public double getLocalTermWeight() {

        return localTermWeight;
        
    }

    final public void setLocalTermWeight(final double d) {
        
        localTermWeight = d;
        
    }
    
    public void add() {

        noccurrences++;
        
    }

    public String toString() {

        return "{noccur=" + noccurrences + ",weight=" + localTermWeight + "}";

    }
    
}