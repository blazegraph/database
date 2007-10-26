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
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Abstract base class for rules with two terms in the tail where one term is
 * one-bound and the other is one bound (or two bound) by virtue of a join
 * variable.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractRuleRdfs_2_3_7_9 extends AbstractRuleRdf {

    public AbstractRuleRdfs_2_3_7_9
        ( InferenceEngine inf, 
          Triple head, 
          Pred[] body
          ) {

        super( inf, head, body );

    }
    
    /**
     * Note: The evaluation logic reuses subqueries. This is accomplised by
     * placing the results for the 1st term into SPO order and then applying the
     * results of each subquery to the next N tuples for the first term having
     * the same value for their subject position.
     */
    public RuleStats apply( final RuleStats stats, final SPOBuffer buffer) {
        
        final long computeStart = System.currentTimeMillis();
        
        // this is a one-bound query on the POS index.
        ISPOIterator itr = getStmts1();
        
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
            
            SPO[] stmts1 = itr.nextChunk(KeyOrder.SPO);
//            SPO[] stmts1 = itr.nextChunk(); // Leave in POS order (comparison only).
            
            if(DEBUG) {
                
                log.debug("stmts1: chunk="+stmts1.length+"\n"+Arrays.toString(stmts1));
                
            }
            
            stats.stmts1 += stmts1.length;

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
            
            for(int i=0; i<stmts1.length; /*inc below*/) {

                final long lastS = stmts1[i].s;
                
                log.debug("subquery: subject="+lastS);
                
                // New subquery on the POS index using stmt2.p := stmt1.s.
                ISPOIterator itr2 = getStmts2(stmts1[i]/*lastS*/);

                stats.numSubqueries1++;

                if(!itr2.hasNext()) {

                    /*
                     * Nothing matched the subquery so consume all rows of stmt1
                     * where stmts1[i].s == lastS.
                     */
                    
                    while(i<stmts1.length && stmts1[i].s == lastS) {
                     
                        i++;
                        
                    }
                    
                    continue;
                    
                }
                
                // Apply the subquery while stmt1.s is unchanged.

                while (itr2.hasNext()) {

                    SPO[] stmts2 = itr2.nextChunk(KeyOrder.POS);

                    if(DEBUG) {
                        
                        log.debug("stmts2: chunk="+stmts2.length+"\n"+Arrays.toString(stmts2));
                        
                    }

                    stats.stmts2 += stmts2.length;

                    // for each row in stmt1s with the same [s].
                    for (; i < stmts1.length && stmts1[i].s == lastS; i++) {

                        final SPO stmt1 = stmts1[i];

                        // join to each row of stmt2
                        for (SPO stmt2 : stmts2) {
                            
                            SPO newSPO = buildStmt3(stmt1, stmt2);

                            Justification jst = null;

                            if (justify) {

                                jst = new Justification(this, newSPO,//
                                        new SPO[] { stmt1, stmt2 });

                            }

                            buffer.add(newSPO, jst);

                            stats.numComputed++;

                        }
                        
                    }

                }
                
            }

        }

        stats.elapsed += System.currentTimeMillis() - computeStart;

        return stats;

    }
    
    /**
     * The default behavior is to use POS index to match body[0] with one bound
     * (the predicate). The statements are buffered and then sorted into SPO
     * order.
     */
    final protected ISPOIterator getStmts1() {
        
        /*
         * For example: rdfs7
         * 
         * triple(?u,?b,?y) :- triple(?a,rdfs:subPropertyOf,?b), triple(?u,?a,?y).
         */
        
        return db.getAccessPath(NULL/* a */, body[0].p.id, NULL/* b */)
                .iterator();
        
    }
    
    /**
     * A one bound subquery using the POS index with the subject of stmt1 as the
     * predicate of body[1]. The object and subject positions in the subquery
     * are unbound.
     * 
     * @see RuleRdfs09#getStmts2(SPO)
     */
    protected ISPOIterator getStmts2(SPO stmt1) {

        /*
         * The subject from stmt1 is in the predicate position for this query.
         * 
         * For example: rdfs7
         * 
         * triple(?u,?b,?y) :- triple(?a,rdfs:subPropertyOf,?b), triple(?u,?a,?y).
         */

        return db.getAccessPath(NULL/*u*/, stmt1.s/*a*/, NULL/*y*/).iterator();
          
    }
    
    /**
     * Builds the entailed triple from the matched triples.
     * 
     * @param stmt1
     *            The match on the 1st triple pattern.
     * @param stmt2
     *            The match on the 2nd triple pattern.
     *            
     * @return The entailed triple.
     */
    protected abstract SPO buildStmt3( SPO stmt1, SPO stmt2 );

}
