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
    public AbstractRuleFastClosure_11_13(AbstractTripleStore db, Triple head, Pred[] body) {

        super(db, head, body );

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

//    /**
//     * Do either one-bound sub-query first, depending on the range count for
//     * (?y, rdfs:subPropertyOf, ?a) vs (?a, propertyId, ?b).
//     * <p>
//     * Then do the other term as a two-bound sub-query (joining on ?a). The join
//     * binds ?y and ?b, which we use for the output tuples.
//     * <p>
//     * Finally, for each ?y, do the 1-bound query (?x, ?y, ?z) and generate (?x,
//     * rdf:type, ?b).
//     * 
//     * @todo refactor to choose the evaluation order based on the range count
//     *       for (?y, rdfs:subPropertyOf, ?a) vs (?a, propertyId, ?b).
//     *       <p>
//     *       Note:This needs to preserve the logic to reuse the subquery using
//     *       iterators even as it is refactored to be run against (new,
//     *       new+old). See {@link AbstractRuleRdfs_2_3_7_9} which does this in
//     *       for a 2-term tail.
//     */
//    public RuleStats apply( final boolean justify, final SPOBuffer buffer) {
//        
//        if(true) return apply0(justify, buffer);
//        
//        final long computeStart = System.currentTimeMillis();
//
//        resetBindings();
//        
//        // Query (?y, rdfs:subPropertyOf, ?a).
//        ISPOIterator itr1 = getAccessPath(1/*pred*/).iterator();
//        
//        assert itr1.getKeyOrder() == KeyOrder.POS;
//        
//        while(itr1.hasNext()) {
//            
//            /*
//             * Note: The query used POS, but we reorder each chunk to OSP so
//             * that the join on ?a to (?a propertyId ?b) will progress through
//             * the SPO index in the subquery (?a will be sorted).
//             */
//            
//            SPO[] stmts1 = itr1.nextChunk(KeyOrder.OSP);
//            
//            if(DEBUG) {
//                
//                log.debug("stmts1: chunk="+stmts1.length+"\n"+Arrays.toString(stmts1));
//                
//            }
//
//            stats.nstmts[1] += stmts1.length;
//
//            /*
//             * Two bound subquery (?a, propertyId, ?b), where ?a := stmt1.o.
//             * What we want out of the join is stmt1.s, which is ?y.
//             */
//            
//            for (SPO stmt1 : stmts1) {
//                
//                bind(1,stmt1);
//                
////                set(y,stmt1.s);
////                
////                set(a,stmt1.o);
//                
//                // Subquery on the SPO index joining on stmt1.o == stmt2.s (a).
//
//                ISPOIterator itr2 = getAccessPath(2/*pred*/).iterator();
//                
//                assert itr2.getKeyOrder() == KeyOrder.SPO;
//
//                stats.nsubqueries[1]++;
//                
//                while(itr2.hasNext()) {
//                    
//                    SPO[] stmts2 = itr2.nextChunk();
//
//                    if(DEBUG) {
//                        
//                        log.debug("stmts2: chunk="+stmts2.length+"\n"+Arrays.toString(stmts2));
//                        
//                    }
//
//                    stats.nstmts[2] += stmts2.length;
//                    
//                    for (SPO stmt2 : stmts2) {
//                        
//                        /*
//                         * join was on ?a
//                         */
//                        
//                        assert stmt2.s == get(a);
//
//                        bind(2,stmt2);
////                        set(b,stmt2.o);
//                        
//                        // One bound subquery <code>(?x, ?y, ?z)</code> using POS
//                        
//                        ISPOIterator itr3 = getAccessPath(0/*pred*/).iterator();
//                        
//                        stats.nsubqueries[0]++;
//                        
//                        while(itr3.hasNext()) {
//                            
//                            SPO[] stmts0 = itr3.nextChunk();
//                            
//                            if(DEBUG) {
//                                
//                                log.debug("stmts3: chunk="+stmts0.length+"\n"+Arrays.toString(stmts0));
//                                
//                            }
//                            
//                            stats.nstmts[0] += stmts0.length;
//                            
//                            for(SPO stmt3: stmts0) {
//
//                                assert stmt3.p == get(y);
//
//                                bind(0,stmt3);
//                                
////                                set(x,stmt3.s);
////                                
////                                set(z,stmt3.o);
//                                
//                                // generate ([?x|?z] , rdf:type, ?b).
//                                emit(justify,buffer);
//                                
//                            }
//
//                        }
//                                                
//                    }
//
//                }
//                                
//            }
//
//        }
//        
//        assert checkBindings();
//        
//        stats.elapsed += System.currentTimeMillis() - computeStart;
//
//        return stats;
//
//    }
    
}
