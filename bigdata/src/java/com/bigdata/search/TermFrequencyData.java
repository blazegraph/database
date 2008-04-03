package com.bigdata.search;

import java.util.HashMap;

import org.apache.lucene.analysis.Token;

/**
 * Models the term-frequency data associated with a single field of some
 * document.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TermFrequencyData {

    /** The document identifier. */
    final public long docId;
    
    /** The field identifier. */
    final public int fieldId;
    
    /** The #of terms added (includes duplicates). */
    private int totalTermCount = 0;
    
    /**
     * The set of distinct tokens and their {@link TermMetadata}.
     */
    final public HashMap<String,TermMetadata> terms = new HashMap<String,TermMetadata>(); 
    
    public TermFrequencyData(long docId, int fieldId, Token token) {
        
        this.docId = docId;
        
        this.fieldId = fieldId;
        
        add( token );
        
    }

    /**
     * Add a {@link Token}.
     * 
     * @param token
     *            The token.
     * 
     * @return true iff the termText did not previously exist for this {@link TermFrequencyData}.
     */
    public boolean add(Token token) {
        
        final String termText = token.termText();
        
        final boolean newTerm;
        
        TermMetadata termMetadata = terms.get(termText);

        if (termMetadata == null) {
            
            termMetadata = new TermMetadata();
            
            terms.put(termText, termMetadata);
            
            newTerm = true;
            
        } else {
            
            newTerm = false;
            
        }
        
        termMetadata.add( token );

        totalTermCount++;
        
        return newTerm;
        
    }
 
    /**
     * The #of distinct terms.
     */
    public int distinctTermCount() {
     
        return terms.size();
        
    }

    /**
     * The total #of terms, including duplicates.
     */
    public int totalTermCount() {

        return totalTermCount;
        
    }
    
    /**
     * Computes the normalized term-frequency vector. This a unit vector whose
     * magnitude is <code>1.0</code>. The magnitude of the term frequency
     * vector is computed using the integer term frequency values reported by
     * {@link TermMetadata#termFreq()}. The normalized values are then set on
     * {@link TermMetadata#localTermWeight}.
     * 
     * @return The magnitude of the un-normalized
     *         {@link TermMetadata#termFreq()} vector.
     */
    public double normalize() {
     
        /*
         * Compute magnitude.
         */
        double magnitude = 0d;
        
        for(TermMetadata md : terms.values()) { 
            
            int termFreq = md.termFreq();

            // sum of squares.
            magnitude += (termFreq * termFreq);
            
        }

        magnitude = Math.sqrt(magnitude);
     
        /*
         * normalizedWeight = termFreq / magnitude for each term.
         */

        for(TermMetadata md : terms.values()) { 
            
            int termFreq = md.termFreq();

            md.localTermWeight = (double)termFreq / magnitude;
            
        }

        return magnitude;
        
    }
    
}
