package com.bigdata.rdf.spo;

import com.bigdata.relation.accesspath.IElementFilter;

public abstract class SPOFilter<E extends ISPO> implements IElementFilter<E> {
        
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public boolean canAccept(final Object o) {
        
        return o instanceof ISPO;
        
    }
    
}
