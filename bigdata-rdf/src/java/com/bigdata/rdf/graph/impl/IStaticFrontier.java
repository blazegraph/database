package com.bigdata.rdf.graph.impl;

import com.bigdata.rdf.internal.IV;

/**
 * Interface abstracts the fixed frontier as known on entry into a new
 * round.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
@SuppressWarnings("rawtypes")
interface IStaticFrontier extends Iterable<IV> {

    /**
     * The number of vertices in the frontier.
     * 
     * TODO Long? Or just do not allow in scale-out?
     */
    int size();

    /**
     * Return <code>true</code> if the frontier is known to be empty.
     */
    boolean isEmpty();
    
}

