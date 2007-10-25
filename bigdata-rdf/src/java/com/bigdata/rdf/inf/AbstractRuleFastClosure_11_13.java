package com.bigdata.rdf.inf;

import java.util.Arrays;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Rule for steps 11 and 13 of {@link InferenceEngine#fastForwardClosure()}.
 * <p>
 * Note: this rule is not very selective and does not produce new
 * entailments unless your ontology and your application both rely on
 * domain/range to confer type information. If you explicitly type your
 * instances then this will not add information during closure.
 * <p>
 * 11.
 * 
 * <pre>
 *      (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b)
 *         -&gt; (?x, rdf:type, ?b).
 * </pre>
 * 
 * 13.
 * 
 * <pre>
 *      (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:range, ?b )
 *         -&gt; (?x, rdf:type, ?b )
 * </pre>
 * 
 * @todo Consider whether the head of the rule (?x, rdf:type, ?b) could be
 *       queried since it is two bound to identify bindings of ?x for which
 *       the entailment is already known and thereby reduce the amount of
 *       work, or at least the #of false entailments, that this rule will
 *       produce.
 * 
 * @see TestRuleFastClosure_11_13
 */
abstract public class AbstractRuleFastClosure_11_13 extends AbstractRuleRdf {

    protected final long propertyId;
    
    /**
     * 
     * @param inf
     * @param x
     * @param y
     * @param z
     * @param a
     * @param b
     * @param propertyId Use [rdfs:domain] for #11 and [rdfs:range] for #13.
     */
    public AbstractRuleFastClosure_11_13(InferenceEngine inf, Var x, Var y, Var z,
            Var a, Var b, final Id propertyId) {

        super(inf, new Triple(x, inf.rdfType, b),
                new Pred[] {
                new Triple(x, y, z),
                new Triple(y, inf.rdfsSubPropertyOf, a),
                new Triple(a, propertyId, b)
                });

        this.propertyId = propertyId.id;
        
    }

    /**
     * Do either one-bound sub-query first, depending on the range count for
     * (?y, rdfs:subPropertyOf, ?a) vs (?a, propertyId, ?b).
     * <p>
     * Then do the other term as a two-bound sub-query (joining on ?a). The
     * join binds ?y and ?b, which we use for the output tuples.
     * <p>
     * Finally, for each ?y, do the 1-bound query (?x, ?y, ?z) and generate
     * (?x, rdf:type, ?b).
     * 
     * @todo refactor to choose the evaluation order based on the range
     *       count for (?y, rdfs:subPropertyOf, ?a) vs (?a, propertyId, ?b).
     */
    public RuleStats apply( final RuleStats stats, final SPOBuffer buffer) {
        
        final long computeStart = System.currentTimeMillis();
        
        // in SPO order.
        SPO[] stmts1 = getStmts1();
        
        stats.stmts1 += stmts1.length;

        /*
         * Subquery is two bound: (a, propertyId, ?b). What we want out of
         * the join is stmt1.s, which is ?y.
         */
        
        long lastS = NULL;
        
        SPO[] stmts2 = null;
        
        for (int i = 0; i < stmts1.length; i++) {

            SPO stmt1 = stmts1[i];
            
            if(lastS==NULL || lastS!=stmt1.s) {
                
                lastS = stmt1.s;
            
                // Subquery on the POS index using ?a := stmt2.p := stmt1.s.

                stmts2 = getStmts2(stmt1);
                
                stats.stmts2 += stmts2.length;
                
                stats.numSubqueries1++;
                
            }
            
            for (int j = 0; j < stmts2.length; j++) {
            
                SPO stmt2 = stmts2[j];
                
                /* join on ?a
                 * 
                 * ?y := stmt1.s
                 * 
                 * ?b := stmt2.o
                 */
                if(stmt1.o != stmt2.s) continue;

                SPO[] stmts3 = getStmts3(stmt1);
                
                stats.stmts3 += stmts3.length;
                
                stats.numSubqueries2++;
                
                for(SPO stmt3: stmts3) {

                    // generate (?x, rdf:type, ?b).
                    
                    SPO newSPO = new SPO(stmt3.s, inf.rdfType.id, stmt2.o,
                            StatementEnum.Inferred);
                    
                    Justification jst = null;
                    
                    if(justify) {
                        
                        jst = new Justification(this,newSPO,new SPO[]{
                                stmt1,
                                stmt2,
                                stmt3
                        });
                        
                    }
                    
                    buffer.add( newSPO, jst );
                                            
                    stats.numComputed++;
                    
                }
                
            }
            
        }
        
        stats.elapsed += System.currentTimeMillis() - computeStart;

        return stats;

    }
    
    /**
     * Use POS index to match (?y, rdfs:subPropertyOf, ?a) with one bound
     * (the predicate). The statements are buffered and then sorted into SPO
     * order.
     */
    public SPO[] getStmts1() {
        
        final long p = inf.rdfsSubPropertyOf.id;
        
        byte[] fromKey = db.getKeyBuilder().statement2Key(p, NULL, NULL);

        byte[] toKey = db.getKeyBuilder().statement2Key(p + 1, NULL, NULL);

        SPO[] stmts = db.getStatements(KeyOrder.POS, fromKey, toKey);
        
        /*
         * Sort into SPO order.
         * 
         * Note: you can comment this out to compare with POS order.  The JOIN
         * is still correct, but the logic to reuse subqueries in apply() is
         * mostly defeated when the statements are not sorted into SPO order.
         */
        Arrays.sort(stmts,SPOComparator.INSTANCE);
        
        return stmts;
        
    }
    
    /**
     * Two bound subquery <code>(?a, propertyId, ?b)</code> using the SPO
     * index with ?a bound to stmt1.o.
     * 
     * @return The data in SPO order.
     */
    public SPO[] getStmts2(SPO stmt1) {

        final long a = stmt1.o;
        
        byte[] fromKey = db.getKeyBuilder().statement2Key(a, propertyId,
                NULL);

        byte[] toKey = db.getKeyBuilder().statement2Key(a, propertyId + 1,
                NULL);

        return db.getStatements(KeyOrder.SPO, fromKey, toKey);
    
    }
    
    /**
     * One bound subquery <code>(?x, ?y, ?z)</code> using the POS
     * index with ?y bound to stmt1.s.
     * 
     * @return The data in POS order.
     */
    public SPO[] getStmts3(SPO stmt1) {

        final long y = stmt1.s;
        
        byte[] fromKey = db.getKeyBuilder().statement2Key(y, NULL, NULL);

        byte[] toKey = db.getKeyBuilder().statement2Key(y + 1, NULL, NULL);

        return db.getStatements(KeyOrder.POS, fromKey, toKey);
    
    }
    
}