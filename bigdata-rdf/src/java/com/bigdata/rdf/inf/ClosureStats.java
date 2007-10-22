package com.bigdata.rdf.inf;

/**
 * Statistics collected when performing inference.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClosureStats {
    
    /**
     * The #of entailments that were added to the database (this includes axioms
     * and statements entailed given the data). This reports only the actual
     * change in the #of statements in the database across the closure
     * operation.
     */
    public int numComputed;

    /**
     * Time to compute the entailments and store them within the database
     * (milliseconds).
     */
    public long elapsed;

}
