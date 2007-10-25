package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.SPO;

/**
 * Rule for step 11 of {@link InferenceEngine#fastForwardClosure()}.
 * 
 * <pre>
 *       (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b)
 *          -&gt; (?x, rdf:type, ?b).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleFastClosure11 extends AbstractRuleFastClosure_11_13 {

    /**
     * @param inf
     * @param x
     * @param y
     * @param z
     * @param a
     * @param b
     */
    public RuleFastClosure11(InferenceEngine inf, Var x, Var y, Var z, Var a, Var b) {
        
        super(inf, x, y, z, a, b, inf.rdfsDomain);
        
    }
    
    /**
     * Chooses "x" from the 1st term.
     */
    protected long getSubjectForHead(SPO spo){

        return spo.s;
        
    }

}
