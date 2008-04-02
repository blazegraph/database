package com.bigdata.text;

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
    
    /**
     * The set of distinct tokens and their {@link TermMetadata}.
     */
    final public HashMap<String,TermMetadata> tokens = new HashMap<String,TermMetadata>(); 
    
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
        
        TermMetadata termMetadata = tokens.get(termText);

        if (termMetadata == null) {
            
            termMetadata = new TermMetadata();
            
            tokens.put(termText, termMetadata);
            
            newTerm = true;
            
        } else {
            
            newTerm = false;
            
        }
        
        termMetadata.add( token );
        
        return newTerm;
        
    }
    
}