package com.bigdata.rdf.inf;

import java.util.Arrays;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Rule for steps 11 and 13 of {@link InferenceEngine#fastForwardClosure()}.
 * <p>
 * Note: this rule is not very selective and does not produce new
 * entailments unless your ontology and your application both rely on
 * domain/range to confer type information. If you explicitly type your
 * instances then this will not add information during closure.
 * <p>
 * Step 11.
 * 
 * <pre>
 *      (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b)
 *         -&gt; (?x, rdf:type, ?b).
 * </pre>
 * 
 * Step 13.
 * 
 * <pre>
 *      (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:range, ?b )
 *         -&gt; (?z, rdf:type, ?b )
 * </pre>
 * 
 * @see TestRuleFastClosure_11_13
 *
 * FIXME modify to use {@link Rule#var(String)} and {@link Rule#emit(SPOBuffer)}
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
     * Then do the other term as a two-bound sub-query (joining on ?a). The join
     * binds ?y and ?b, which we use for the output tuples.
     * <p>
     * Finally, for each ?y, do the 1-bound query (?x, ?y, ?z) and generate (?x,
     * rdf:type, ?b).
     * 
     * @todo refactor to choose the evaluation order based on the range count
     *       for (?y, rdfs:subPropertyOf, ?a) vs (?a, propertyId, ?b).
     * 
     * FIXME This needs to preserve the logic to reuse the subquery using
     * iterators even as it is refactored to be run against (new, new+old). See
     * {@link AbstractRuleRdfs_2_3_7_9} which does this in for a 2-term tail.
     */
    public RuleStats apply( final RuleStats stats, final SPOBuffer buffer) {
        
        final long computeStart = System.currentTimeMillis();

        // Query (?y, rdfs:subPropertyOf, ?a).

        ISPOIterator itr1 = getStmts1();
        
        assert itr1.getKeyOrder() == KeyOrder.POS;
        
        while(itr1.hasNext()) {
            
            /*
             * Note: The query used POS, but we reorder each chunk to OSP so
             * that the join on ?a to (?a propertyId ?b) will progress through
             * the SPO index in the subquery (?a will be sorted).
             */
            
            SPO[] stmts1 = itr1.nextChunk(KeyOrder.OSP);
            
            if(DEBUG) {
                
                log.debug("stmts1: chunk="+stmts1.length+"\n"+Arrays.toString(stmts1));
                
            }

            stats.stmts1 += stmts1.length;

            /*
             * Two bound subquery (?a, propertyId, ?b), where ?a := stmt1.o.
             * What we want out of the join is stmt1.s, which is ?y.
             */
            
            for (SPO stmt1 : stmts1) {
                
                // Subquery on the SPO index joining on stmt1.o == stmt2.s (a).

                ISPOIterator itr2 = getStmts2(stmt1);
                
                assert itr2.getKeyOrder() == KeyOrder.SPO;
                
                while(itr2.hasNext()) {
                    
                    SPO[] stmts2 = itr2.nextChunk();
                    
                    if(DEBUG) {
                        
                        log.debug("stmts2: chunk="+stmts2.length+"\n"+Arrays.toString(stmts2));
                        
                    }

                    stats.stmts2 += stmts2.length;
                    
                    stats.numSubqueries1++;
                    
                    for (SPO stmt2 : stmts2) {
                        
                        /*
                         * join was on ?a
                         */
                        
                        assert stmt1.o == stmt2.s;

                        /* ?y := stmt1.s
                         * 
                         * ?b := stmt2.o
                         */
                        
                        final long y = stmt1.s;
                        
                        final long b = stmt2.o;
                        
                        // One bound subquery <code>(?x, ?y, ?z)</code> using POS
                        
                        ISPOIterator itr3 = getStmts3(stmt1/*y*/);
                        
                        while(itr3.hasNext()) {
                            
                            SPO[] stmts3 = itr3.nextChunk();
                            
                            if(DEBUG) {
                                
                                log.debug("stmts3: chunk="+stmts3.length+"\n"+Arrays.toString(stmts3));
                                
                            }
                            
                            stats.stmts3 += stmts3.length;
                            
                            stats.numSubqueries2++;
                            
                            for(SPO stmt3: stmts3) {

                                // generate ([?x|?z] , rdf:type, ?b).
                                
                                SPO newSPO = new SPO(getSubjectForHead(stmt3),
                                        inf.rdfType.id, b,
                                        StatementEnum.Inferred);
                                
                                Justification jst = null;
                                
                                if(justify) {
                                    
                                    jst = new Justification(this, newSPO, new SPO[] {
                                    /*
                                     * Note: this is the order in which the rule was written
                                     * in the paper.
                                     */
                                            stmt3,
                                            stmt1,
                                            stmt2,
                                    });
                                    
                                }
                                
                                buffer.add( newSPO, jst );
                                                        
                                stats.numComputed++;
                                
                            }

                        }
                                                
                    }

                }
                                
            }

        }
        
        stats.elapsed += System.currentTimeMillis() - computeStart;

        return stats;

    }
    
    /**
     * Use POS index to match (?y, rdfs:subPropertyOf, ?a) with one bound (the
     * predicate).
     */
    public ISPOIterator getStmts1() {
        
        final long p = inf.rdfsSubPropertyOf.id;

        return db.getAccessPath(NULL, p, NULL).iterator();
        
    }
    
    /**
     * Two bound subquery <code>(?a, propertyId, ?b)</code> using the SPO
     * index with ?a bound to stmt1.o.
     * 
     * @return The data in SPO order.
     */
    public ISPOIterator getStmts2(SPO stmt1) {

        final long a = stmt1.o;

        return db.getAccessPath(a, propertyId, NULL).iterator();
        
    }
    
    /**
     * One bound subquery <code>(?x, ?y, ?z)</code> using the POS
     * index with ?y bound to stmt1.s.
     * 
     * @return The data in POS order.
     */
    public ISPOIterator getStmts3(SPO stmt1) {

        final long y = stmt1.s;

        return db.getAccessPath(NULL/*x*/, y, NULL/*z*/).iterator();
    
    }

//    public RuleStats apply( final RuleStats stats, final SPOBuffer buffer) {
//        
//        final long computeStart = System.currentTimeMillis();
//        
//        // (?y, rdfs:subPropertyOf, ?a) in SPO order.
//        SPO[] stmts1 = getStmts1();
//        
//        stats.stmts1 += stmts1.length;
//
//        /*
//         * Subquery is two bound: (a, propertyId, ?b). What we want out of
//         * the join is stmt1.s, which is ?y.
//         */
//        
//        long lastS = NULL;
//        
//        SPO[] stmts2 = null;
//        
//        for (int i = 0; i < stmts1.length; i++) {
//
//            SPO stmt1 = stmts1[i];
//            
//            if(lastS==NULL || lastS!=stmt1.s) {
//                
//                lastS = stmt1.s;
//            
//                // Subquery on the POS index using ?a := stmt2.p := stmt1.s.
//
//                stmts2 = getStmts2(stmt1);
//                
//                stats.stmts2 += stmts2.length;
//                
//                stats.numSubqueries1++;
//                
//            }
//            
//            for (int j = 0; j < stmts2.length; j++) {
//            
//                SPO stmt2 = stmts2[j];
//                
//                /* join on ?a
//                 * 
//                 * ?y := stmt1.s
//                 * 
//                 * ?b := stmt2.o
//                 */
//                if(stmt1.o != stmt2.s) continue;
//
//                // One bound subquery <code>(?x, ?y, ?z)</code> using the POS
//                SPO[] stmts3 = getStmts3(stmt1);
//                
//                stats.stmts3 += stmts3.length;
//                
//                stats.numSubqueries2++;
//                
//                for(SPO stmt3: stmts3) {
//
//                    // generate (?z , rdf:type, ?b).
//                    
//                    SPO newSPO = new SPO(getSubjectForHead(stmt3), inf.rdfType.id, stmt2.o,
//                            StatementEnum.Inferred);
//                    
//                    Justification jst = null;
//                    
//                    if(justify) {
//                        
//                        jst = new Justification(this, newSPO, new SPO[] {
//                        /*
//                         * Note: this is the order in which the rule was written
//                         * in the paper.
//                         */
//                                stmt3,
//                                stmt1,
//                                stmt2,
//                        });
//                        
//                    }
//                    
//                    buffer.add( newSPO, jst );
//                                            
//                    stats.numComputed++;
//                    
//                }
//                
//            }
//            
//        }
//        
//        stats.elapsed += System.currentTimeMillis() - computeStart;
//
//        return stats;
//
//    }
//
//    /**
//     * Use POS index to match (?y, rdfs:subPropertyOf, ?a) with one bound
//     * (the predicate). The statements are buffered and then sorted into SPO
//     * order.
//     */
//    public SPO[] getStmts1() {
//        
//        final long p = inf.rdfsSubPropertyOf.id;
//
//        SPO[] stmts = ((SPOArrayIterator)db.getAccessPath(NULL, p, NULL).iterator()).array();
//        
//        /*
//         * Sort into SPO order.
//         * 
//         * Note: you can comment this out to compare with POS order.  The JOIN
//         * is still correct, but the logic to reuse subqueries in apply() is
//         * mostly defeated when the statements are not sorted into SPO order.
//         */
//        Arrays.sort(stmts,SPOComparator.INSTANCE);
//        
//        return stmts;
//        
//    }
//    
//    /**
//     * Two bound subquery <code>(?a, propertyId, ?b)</code> using the SPO
//     * index with ?a bound to stmt1.o.
//     * 
//     * @return The data in SPO order.
//     */
//    public SPO[] getStmts2(SPO stmt1) {
//
//        final long a = stmt1.o;
//
//        SPO[] stmts = ((SPOArrayIterator)db.getAccessPath(a, propertyId, NULL).iterator()).array();
//
//        return stmts;
//        
//    }
//    
//    /**
//     * One bound subquery <code>(?x, ?y, ?z)</code> using the POS
//     * index with ?y bound to stmt1.s.
//     * 
//     * @return The data in POS order.
//     */
//    public SPO[] getStmts3(SPO stmt1) {
//
//        final long y = stmt1.s;
//
//        SPO[] stmts = ((SPOArrayIterator)db.getAccessPath(NULL/*x*/, y, NULL/*z*/).iterator()).array();
//
//        return stmts;
//    
//    }

    /**
     * The two rules choose different bindings for the subject of the head. This
     * method makes that selection.
     * 
     * @param spo The 1st term.
     */
    abstract protected long getSubjectForHead(SPO spo);
    
}
