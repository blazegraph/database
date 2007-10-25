package com.bigdata.rdf.inf;

/**
 * Rule for step 13 of {@link InferenceEngine#fastForwardClosure()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleFastClosure13 extends AbstractRuleFastClosure_11_13 {

    /**
     * @param inf
     * @param x
     * @param y
     * @param z
     * @param a
     * @param b
     */
    public RuleFastClosure13(InferenceEngine inf, Var x, Var y, Var z, Var a, Var b) {
        
        super(inf, x, y, z, a, b, inf.rdfsRange);
        
    }
    
}