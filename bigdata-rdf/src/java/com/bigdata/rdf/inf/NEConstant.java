package com.bigdata.rdf.inf;

import com.bigdata.rdf.inf.Rule.IConstraint;
import com.bigdata.rdf.inf.Rule.State;
import com.bigdata.rdf.inf.Rule.Var;

/**
 * Imposes the constraint <code>var != constant</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NEConstant implements IConstraint {
    
    private final Var var;
    private final long constant;

    public NEConstant(Var var, long constant) {
        
        this.var = var;
        
        this.constant = constant;
        
    }
    
    public boolean accept(State s) {
        
        // get binding for the variable.
        long var = s.get(this.var);
    
        if(var==RuleOwlSameAs2.NULL) return true; // not yet bound.
    
        return var != constant; 

   }

}