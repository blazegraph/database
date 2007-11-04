package com.bigdata.rdf.inf;



/**
 * Rule for step 13 of {@link InferenceEngine#fastForwardClosure()}.
 * 
 * <pre>
 * (?z, rdf:type, ?b ) :-
 *       (?x, ?y, ?z),
 *       (?y, rdfs:subPropertyOf, ?a),
 *       (?a, rdfs:range, ?b ).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleFastClosure13 extends AbstractRuleFastClosure_11_13 {

    /**
     * @param inf
     */
    public RuleFastClosure13(InferenceEngine inf) {
        
        super(inf.database, 
                new Triple(var("z"), inf.rdfType, var("b")),//
                new Pred[] {//
                    new Triple(var("x"), var("y"), var("z")),//
                    new Triple(var("y"), inf.rdfsSubPropertyOf, var("a")),//
                    new Triple(var("a"), inf.rdfsRange, var("b"))//
                },
                new IConstraint[] {
                    new NE(var("y"),var("a"))
                });
        
    }
    
}
