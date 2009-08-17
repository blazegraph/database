package com.bigdata.rdf.spo;

import com.bigdata.relation.accesspath.IElementFilter;

public abstract class SPOFilter implements IElementFilter<ISPO> {
    
    public boolean canAccept(final Object o) {
        
        return o instanceof ISPO;
        
    }
    
}
