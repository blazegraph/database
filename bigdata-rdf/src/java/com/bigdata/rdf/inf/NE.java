package com.bigdata.rdf.inf;

import com.bigdata.rdf.inf.Rule.IConstraint;
import com.bigdata.rdf.inf.Rule.State;
import com.bigdata.rdf.inf.Rule.Var;

/**
 * Imposes the constraint <code>x != y</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NE implements IConstraint {

    private final Var x;
    private final Var y;
    
    public NE(Var x, Var y) {
        
        if (x == null || y == null)
            throw new IllegalArgumentException();

        if (x == y)
            throw new IllegalArgumentException();
        
        this.x = x;
        
        this.y = y;
        
    }
    
    public boolean accept(State s) {
        
        // get binding for "x".
        long x = s.get(this.x);
       
        if(x==RuleOwlSameAs2.NULL) return true; // not yet bound.

        // get binding for "y".
        long y = s.get(this.y);
    
        if(y==RuleOwlSameAs2.NULL) return true; // not yet bound.
    
        return x != y; 

   }

}