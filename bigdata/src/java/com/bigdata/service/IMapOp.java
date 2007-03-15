package com.bigdata.service;

import com.bigdata.objndx.IEntryIterator;

/**
 * Interface for client-defined mapped operators.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMapOp {
    
    /**
     * The name of the reducer service.
     */
    public String getReducer();

    /**
     * Apply the operator to the key/value stream, writing results onto the
     * reducer service.
     * 
     * @param src
     *            The key/value stream.
     * @param reducer
     *            The reducer service.
     */
    public void apply(IEntryIterator src,IReducer reducer);
    
}