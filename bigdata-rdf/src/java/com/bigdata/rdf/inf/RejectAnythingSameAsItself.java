package com.bigdata.rdf.inf;

import com.bigdata.rdf.inf.Rule.IConstraint;
import com.bigdata.rdf.inf.Rule.State;
import com.bigdata.rdf.inf.Rule.Var;

/**
 * Rejects (x y z) iff x==z and y==owl:sameAs, where x, y, and z are variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RejectAnythingSameAsItself implements IConstraint {

    private final Var s;
    private final Var p;
    private final Var o;
    private final long owlSameAs;
    
    /**
     * 
     * @param s
     * @param p
     * @param o
     * @param owlSameAs
     */
    public RejectAnythingSameAsItself(Var s, Var p, Var o, Id owlSameAs) {
        
        if (s == null || p == null || o == null || owlSameAs == null)
            throw new IllegalArgumentException();

        if (s == p || p == o || s == o)
            throw new IllegalArgumentException();
        
        this.s = s;

        this.p = p;

        this.o = o;
        
        this.owlSameAs = owlSameAs.id;
        
    }
    
    public boolean accept(State state) {
        
        // get binding for "x".
        long s = state.get(this.s);
       
        if(s==NULL) return true; // not yet bound.

        // get binding for "y".
        long p = state.get(this.p);
    
        if(p==NULL) return true; // not yet bound.
    
        // get binding for "z".
        long o = state.get(this.o);
    
        if(o==NULL) return true; // not yet bound.
    
        if (s == o && p == owlSameAs) {

            // reject this case.
            
            return false;
            
        }

        return true;
        
   }

}
