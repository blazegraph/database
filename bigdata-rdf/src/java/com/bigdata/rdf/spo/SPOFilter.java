package com.bigdata.rdf.spo;

import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.eval.ISolution;

public abstract class SPOFilter<E extends ISPO> implements IElementFilter<E> {
        
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public boolean canAccept(final Object o) {
        
        if (o instanceof ISolution) {
            
            ISolution solution = (ISolution) o;
            
            return solution.get() instanceof ISPO;
            
        }
        
        return false;
        
    }
    
}
