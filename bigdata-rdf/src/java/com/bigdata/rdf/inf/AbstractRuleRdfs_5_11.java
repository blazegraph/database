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

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Abstract rule for chain triple patterns where the object position in the
 * first triple pattern is the same variable as the subject position in the
 * second triple pattern and where the predicate is bound to the same constant
 * for both triple patterns and also appears in the predicate position in the
 * entailed triple.
 * 
 * <pre>
 *  triple(?u,C,?x) :-
 *           triple(?u,C,?v),
 *           triple(?v,C,?x).
 * </pre>
 * 
 * where C is a constant.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractRuleRdfs_5_11 extends AbstractRuleRdf {

    final Var u, x, v;
    
    final Id C;

    public AbstractRuleRdfs_5_11(InferenceEngine inf, Id C) {

        super( inf, //
                new Triple(var("u"), C, var("x")), //
                new Pred[] { //
                    new Triple(var("u"), C, var("v")),//
                    new Triple(var("v"), C, var("x")) //
                });

        this.u = var("u");
        this.v = var("v");
        this.x = var("x");

        this.C = C;
        
    }
    
    public RuleStats apply( final RuleStats stats, SPOBuffer buffer) {
        
        final long computeStart = System.currentTimeMillis();
        
        resetBindings();
        
        /*
         * Query for the 1st part of the rule.
         * 
         * Note that it does not matter which half of the rule we execute first
         * since they are both 2-unbound with the same predicate bound and will
         * therefore have exactly the same results.
         * 
         * Further note that we can perform a self-join on the returned triples
         * without going back to the database IFF all triples in the database
         * for the specified predicate can be materialized at once. This is a
         * huge performance savings, but it will not work for extremely large
         * subClassOf or subPropertyOf ontologies.
         */

        /*
         * Explicitly set the upper bound on the capacity so that we can read
         * every very large ontologies directly into memory. This lets us read
         * up to 1M subClassOf or subPropertyOf statements into RAM. If there
         * are more than that many in the database then the iterator will break
         * things into chunks.
         */
        
        final int capacity = 1 * Bytes.megabyte32;
        
        ISPOIterator itr = db.getAccessPath(NULL, C.id, NULL).iterator(
                0/* limit */, capacity);

        try {
        
            int nchunks = 0;

            while (itr.hasNext()) {

                // Note: The data will be in POS order, so we reorder to SPO.

                SPO[] stmts1 = itr.nextChunk(KeyOrder.POS);

                if (DEBUG) {

                    log.debug("stmts1: chunk=" + stmts1.length + "\n"
                            + Arrays.toString(stmts1));

                }

                stats.stmts1 += stmts1.length;

                if (nchunks == 0 && !itr.hasNext()) {

                    /*
                     * Apply an in-memory self-join.
                     * 
                     * Note: The self-join trick only works if we can fully
                     * buffer the statements. If we are getting more than one
                     * chunk of statements then we MUST process this using
                     * subqueries.
                     */

                    return fullyBufferedSelfJoin(stats, buffer, stmts1);

                }

                /*
                 * FIXME The self-join requires that we fully buffer the
                 * statements. If they are not fully buffered then the self-join
                 * within a chunk can fail since the statement index is being
                 * traversed in POS order but we are joining stmt1.o := stmt2.s,
                 * which requires SPO order.
                 */

                if (true)
                    throw new UnsupportedOperationException();

                nchunks++;

            } // while(itr.hasNext())
            
        } finally {

            itr.close();

        }
        
        assert checkBindings();
        
        stats.elapsed += System.currentTimeMillis() - computeStart;

        return stats;
        
    }

    /**
     * Do a fully buffered self-join.
     * 
     * @param stats
     * @param buffer
     * @param stmts1
     * @return
     */
    private RuleStats fullyBufferedSelfJoin(RuleStats stats, SPOBuffer buffer, SPO[] stmts1) {
        
        // in SPO order.
        Arrays.sort(stmts1,SPOComparator.INSTANCE);
        
        // self-join using binary search.
        for (int i = 0; i < stmts1.length; i++) {

            SPO left = stmts1[i];
            
            /*
             * Search for the index of the first statement having left.s as its
             * subject. Note that the object is NULL, so this should always
             * return a negative index which we then convert to the insert
             * position. The insert position is the first index at which a
             * matching statement would be found. We then scan statements from
             * that point. As soon as there is no match (and it may be that
             * there is no match even on the first statement tested) we break
             * out of the inner loop and continue with the outer loop.
             */ 
            
            // Note: The StatementEnum is ignored by the SPOComparator.
            SPO key = new SPO(left.o, C.id, ITripleStore.NULL,
                    StatementEnum.Explicit);
            
            // Find the index of that key (or the insert position).
            int j = Arrays.binarySearch(stmts1, key, SPOComparator.INSTANCE);

            if (j < 0) {

                // Convert the position to obtain the insertion point.
                j = -j - 1;
                
            }
            
            // process only the stmts with left.s as their subject.
            for (; j < stmts1.length; j++) {

                SPO right = stmts1[j];

                if (left.o != right.s) break;

                set(u,left.s);

                set(v,left.o);
                
                set(x,right.o);
                
                emit(buffer);
                
//                SPO newSPO = new SPO(left.s, c.id, right.o, StatementEnum.Inferred);
//
//                Justification jst = null;
//                
//                if(justify) {
//                    
//                    jst = new Justification(this, newSPO, new SPO[] { left,
//                            right });
//                    
//                }
//                
//                buffer.add(newSPO, jst);

                stats.numComputed++;
                
            }

        }

        return stats;
        
    }
    
}
