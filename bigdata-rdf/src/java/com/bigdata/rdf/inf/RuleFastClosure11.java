package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.SPO;

/**
 * Rule for step 11 of {@link InferenceEngine#fastForwardClosure()}.
 * 
 * <pre>
 * (?x, rdf:type, ?b) :-
 *     (?x, ?y, ?z),
 *     (?y, rdfs:subPropertyOf, ?a),
 *     (?a, rdfs:domain, ?b).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleFastClosure11 extends AbstractRuleFastClosure_11_13 {

    /**
     * @param inf
     */
    public RuleFastClosure11(InferenceEngine inf) {
        
        super(inf,
                new Triple(var("x"), inf.rdfType, var("b")),//
                new Pred[] {//
                    new Triple(var("x"), var("y"), var("z")),//
                    new Triple(var("y"), inf.rdfsSubPropertyOf, var("a")),//
                    new Triple(var("a"), inf.rdfsDomain, var("b"))//
                });
        
    }

}
