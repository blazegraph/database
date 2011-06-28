package com.bigdata.rdf.lexicon;

import java.io.Serializable;
import com.bigdata.rdf.internal.IV;

/**
 * Interface for filtering internal values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITermIVFilter extends Serializable {
    
    /**
     * Return <code>true</code> iff the term {@link IV} should be visited.
     * 
     * @param iv
     *            The internal value
     */
    public boolean isValid(IV iv);
    
}