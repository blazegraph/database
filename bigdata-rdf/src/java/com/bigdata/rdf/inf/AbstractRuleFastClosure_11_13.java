package com.bigdata.rdf.inf;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Rule for steps 11 and 13 of {@link InferenceEngine#fastForwardClosure()}.
 * <p>
 * Note: As long as the binding patterns (the positions in which correlated
 * variables appear) in the tails are the same the rules can have different
 * binding patterns in the head and use the same logic for their execution.
 * Differences in which constants are used do not matter.
 * <p>
 * Note: this rule is not very selective and does not produce new entailments
 * unless your ontology and your application both rely on domain/range to confer
 * type information. If you explicitly type your instances then this will not
 * add information during closure.
 * <p>
 * Step 11.
 * 
 * <pre>
 * (?x, rdf:type, ?b) :-
 *     (?x, ?y, ?z),
 *     (?y, rdfs:subPropertyOf, ?a),
 *     (?a, rdfs:domain, ?b).
 * </pre>
 * 
 * Step 13.
 * 
 * <pre>
 * (?z, rdf:type, ?b ) :-
 *       (?x, ?y, ?z),
 *       (?y, rdfs:subPropertyOf, ?a),
 *       (?a, rdfs:range, ?b ).
 * </pre>
 * 
 * @see TestRuleFastClosure_11_13
 */
abstract public class AbstractRuleFastClosure_11_13 extends AbstractRuleNestedSubquery {

    protected final long propertyId;
    
    final Var x, y, z, a, b;
    final Id C1, C2;
    
    /**
     * 
     * @param db
     * @param head
     * @param body
     */
    public AbstractRuleFastClosure_11_13(AbstractTripleStore db, Triple head,
            Pred[] body, IConstraint[] constraints) {

        super(db, head, body, constraints );

        // validate the binding pattern for the tail of this rule.
        assert body.length == 3;
        
        // (x,y,z)
        x = (Var)body[0].s;
        y = (Var)body[0].p;
        z = (Var)body[0].o;

        // (y,C1,a)
        assert y == (Var)body[1].s;
        C1 = (Id)body[1].p;
        a = (Var)body[1].o;

        // (a,C2,b)
        assert a == (Var)body[2].s;
        C2 = (Id)body[2].p;
        b = (Var)body[2].o;

        this.propertyId = C2.id;
        
    }

}
