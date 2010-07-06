package com.bigdata.rdf.lexicon;

import java.io.Serializable;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Interface for filtering base term identifiers, typically based on the bit
 * flags defined {@link ITermIdCodes}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITermIdFilter extends Serializable {
    
    /**
     * Return <code>true</code> iff the term identifier should be visited.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @see AbstractTripleStore#isURI(long)
     * @see AbstractTripleStore#isBNode(long)
     * @see AbstractTripleStore#isLiteral(long)
     * @see AbstractTripleStore#isStatement(IV)
     */
    public boolean isValid(TermId termId);
    
}