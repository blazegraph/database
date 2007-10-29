/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.rdf.inf;

import java.util.Arrays;

import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * <p>
 * Abstract base class for rules with two terms in the tail where one term is
 * one-bound and the other is one bound (or two bound) by virtue of a join
 * variable.  The examples are:
 * </p>
 * <pre>
 * rdfs2: (u rdf:type x) :- (a rdfs:domain x),        (u a y).
 * 
 * rdfs3: (v rdf:type x) :- (a rdfs:range  x),        (u a v).
 * -----> (y rdf:type x) :- (a rdfs:range  x),        (u a y).
 * 
 * rdfs7: (u b        y) :- (a rdfs:subPropertyOf b), (u a y).
 * -----> (u x        y) :- (a rdfs:subPropertyOf x), (u a y).
 * </pre>
 * <p>
 * The second form for each rule above is merely rewritten to show that the
 * same variable binding patterns are used by each rule.
 * </p>
 * <p>
 * While the following rule can be evaluated by the same logic, it has a
 * different variable binding pattern such that it is not possible to make
 * a static decision concerning which position from each predicate in the
 * tail will be assigned to a given variable.  Therefore this class MUST
 * use {@link #bind(int, SPO)} to make dynamic decision mapping materialized
 * statements onto the variables declared by the rule.
 * </p>
 * <pre>
 * rdfs9: (v rdf:type x) :- (u rdfs:subClassOf x),    (v rdf:type u).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractRuleRdfs_2_3_7_9 extends AbstractRuleNestedSubquery {

    public AbstractRuleRdfs_2_3_7_9
        ( AbstractTripleStore db, 
          Triple head, 
          Pred[] body
          ) {

        super( db, head, body );

        // only two predicates in the tail.
        assert body.length == 2;
        
        // only one shared variable.
        assert getSharedVars(0/*body[0]*/, 1/*body[1]*/).size() == 1;
        
    }
    
    /**
     * Note: The evaluation logic reuses subqueries. This is accomplised by
     * placing the results for the 1st term into SPO order and then applying the
     * results of each subquery to the next N tuples for the first term having
     * the same value for their subject position.
     */
    public RuleStats apply( final boolean justify, final SPOBuffer buffer) {
        
        if(true) return apply0(justify, buffer);
        
        final long computeStart = System.currentTimeMillis();
        
        resetBindings();
        
        /*
         * This is a one-bound query on the POS index.
         * 
         * For example: (a rdfs:domain x)
         */

        ISPOIterator itr = getAccessPath(0).iterator();
        // @todo remove paranoid asserts.
        assert getAccessPath(0).getTriplePattern()[0]==NULL;
        assert getAccessPath(0).getTriplePattern()[1]!=NULL;
        assert getAccessPath(0).getTriplePattern()[2]==NULL;
        
        assert itr.getKeyOrder() == KeyOrder.POS;
        
        while(itr.hasNext()) {
            
            /*
             * Sort the chunk into SPO order.
             * 
             * Note: If you leave this in POS order then the JOIN is still
             * correct, but the logic to reuse subqueries in apply() is mostly
             * defeated since the statements are not grouped by their subjects.
             * 
             * @todo measure the cost/benefit of this tweak on some real data
             * sets. It appears to be of some benefit but I do not have any
             * clean data collected on this point.
             */
            
            SPO[] stmts0 = itr.nextChunk(KeyOrder.SPO);
//            SPO[] stmts1 = itr.nextChunk(); // Leave in POS order (comparison only).
            
            if(DEBUG) {
                
                log.debug("stmts1: chunk="+stmts0.length+"\n"+Arrays.toString(stmts0));
                
            }
            
            stats.nstmts[0] += stmts0.length;

            /*
             * Subquery is one bound: ?u:a:?y.
             * 
             * For example, rdfs7:
             * 
             * triple(?u,?b,?y) :- triple(?a,rdfs:subPropertyOf,?b), triple(?u,?a,?y).
             * 
             * Since stmt1.s := stmt2.p, we only execute N distinct subqueries
             * for N distinct values of stmt1.s. This works because stmts1 is in
             * SPO order, so the subject values are clustered into an ascending
             * order.
             */
            
            for(int i=0; i<stmts0.length; /*inc below*/) {

                final long lastS = stmts0[i].s;
                
                log.debug("subquery: subject="+lastS);

                /*
                 * Note: bind before subquery!!! Otherwise the binding is not
                 * used to select the access path for the subquery and you will
                 * read too much data (all triples).
                 * 
                 * Note: You have to bind again below in order to copy the
                 * bindings for each specific statement visited -- this will
                 * only copy the bindings for the 1st statement having a given
                 * subject.
                 */
                bind(0,stmts0[i]);
                
                // New subquery on the POS index using stmt2.p := stmt1.s.
                ISPOIterator itr2 = getAccessPath(1).iterator(); //(stmts1[i]/*lastS*/);

                stats.nsubqueries[0]++;

                if(!itr2.hasNext()) {

                    /*
                     * Nothing matched the subquery so consume all rows of stmt1
                     * where stmts1[i].s == lastS.
                     */
                    
                    while(i<stmts0.length && stmts0[i].s == lastS) {
                     
                        i++;
                        
                    }
                    
                    continue;
                    
                }
                
                // Apply the subquery while stmt1.s is unchanged.

                while (itr2.hasNext()) {

                    SPO[] stmts1 = itr2.nextChunk(KeyOrder.POS);

                    if(DEBUG) {
                        
                        log.debug("stmts2: chunk="+stmts1.length+"\n"+Arrays.toString(stmts1));
                        
                    }

                    stats.nstmts[1] += stmts1.length;

                    // for each row in stmt1s with the same [s].
                    for (; i < stmts0.length && stmts0[i].s == lastS; i++) {

                        final SPO stmt1 = stmts0[i];

                        bind(0,stmt1);
                        
                        // join to each row of stmt2
                        for (SPO stmt2 : stmts1) {

                            bind(1,stmt2);

                            emit(justify,buffer);
                        
                        }
                        
                    }

                }
                
            }

        }

        assert checkBindings();
        
        stats.elapsed += System.currentTimeMillis() - computeStart;

        return stats;

    }
    
//    /**
//     * The default behavior is to use POS index to match body[0] with one bound
//     * (the predicate). The statements are buffered and then sorted into SPO
//     * order.
//     */
//    final protected ISPOIterator getStmts1() {
//        
//        /*
//         * For example: rdfs7
//         * 
//         * triple(?u,?b,?y) :- triple(?a,rdfs:subPropertyOf,?b), triple(?u,?a,?y).
//         */
//        
//        return db.getAccessPath(NULL/* a */, body[0].p.id, NULL/* b */)
//                .iterator();
//        
//    }
//    
//    /**
//     * A one bound subquery using the POS index with the subject of stmt1 as the
//     * predicate of body[1]. The object and subject positions in the subquery
//     * are unbound.
//     * 
//     * @see RuleRdfs09#getStmts2(SPO)
//     */
//    protected ISPOIterator getStmts2(SPO stmt1) {
//
//        /*
//         * The subject from stmt1 is in the predicate position for this query.
//         * 
//         * For example: rdfs7
//         * 
//         * triple(?u,?b,?y) :- triple(?a,rdfs:subPropertyOf,?b), triple(?u,?a,?y).
//         */
//
//        return db.getAccessPath(NULL/*u*/, stmt1.s/*a*/, NULL/*y*/).iterator();
//          
//    }
//    
//    /**
//     * Builds the entailed triple from the matched triples.
//     * 
//     * @param stmt1
//     *            The match on the 1st triple pattern.
//     * @param stmt2
//     *            The match on the 2nd triple pattern.
//     *            
//     * @return The entailed triple.
//     */
//    protected abstract SPO buildStmt3( SPO stmt1, SPO stmt2 );

}
