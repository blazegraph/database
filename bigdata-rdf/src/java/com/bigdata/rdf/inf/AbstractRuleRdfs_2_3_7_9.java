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

import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.util.KeyOrder;

public abstract class AbstractRuleRdfs_2_3_7_9 extends AbstractRuleRdf {

    public AbstractRuleRdfs_2_3_7_9
        ( InferenceEngine store, 
          Triple head, 
          Pred[] body
          ) {

        super( store, head, body );

    }
    
    public Stats apply( final Stats stats, final SPOBuffer buffer) {
        
        final long computeStart = System.currentTimeMillis();
        
        // in SPO order.
        SPO[] stmts1 = getStmts1();
        
        stats.stmts1 += stmts1.length;

 /* For example, rdfs7:
  * 
  *       triple(?u,?b,?y) :-
  *          triple(?a,rdfs:subPropertyOf,?b),
  *          triple(?u,?a,?y). 
  */
        /*
         * Subquery is one bound: ?u:a:?y.
         * 
         * Since stmt1.s := stmt2.p, we only execute N distinct subqueries for N
         * distinct values of stmt1.s. This works because stmts1 is in SPO
         * order, so the subject values are clustered into an ascending order.
         * 
         * Note: I have observed very little or _possibly_ a slight negative
         * impact on performance from the attempt to reuse subqueries for the
         * same subject. Presumably this is because the subjects are already
         * mostly distinct, so we just pay for the cost of sorting them and do
         * not normally get a reduction in the #of subqueries. The alternative
         * is to have getStmts1() return the statements without sorting them so
         * that they will be in POS order since that is the index that we are
         * querying. The conditional tests on lastS here are still Ok, it is
         * just much less likely that we will ever reuse a subquery.
         */
        
        long lastS = NULL;
        
        SPO[] stmts2 = null;
        
        for (int i = 0; i < stmts1.length; i++) {

            SPO stmt1 = stmts1[i];
            
            if(lastS==NULL || lastS!=stmt1.s) {
                
                lastS = stmt1.s;
            
                // Subquery on the POS index using stmt2.p := stmt1.s.
                stmts2 = getStmts2(stmt1);
                
                stats.stmts2 += stmts2.length;
                
                stats.numSubqueries++;
                
            }
            
            for (int j = 0; j < stmts2.length; j++) {
            
                buffer.add(buildStmt3(stmt1, stmts2[j]));
                
                stats.numComputed++;
                
            }
            
        }
        
        stats.computeTime += System.currentTimeMillis() - computeStart;

        return stats;

    }
    
    /**
     * The default behavior is to use POS index to match body[0] with one bound
     * (the predicate). The statements are buffered and then sorted into SPO
     * order.
     */
    final protected SPO[] getStmts1() {
        
        // use the POS index to look up the matches for body[0], the more
        // constrained triple
        
        byte[] fromKey = db.getKeyBuilder().statement2Key(body[0].p.id, NULL,
                NULL);

        byte[] toKey = db.getKeyBuilder().statement2Key(body[0].p.id + 1, NULL,
                NULL);

        SPO[] stmts = db.getStatements(db.getPOSIndex(), KeyOrder.POS, fromKey,
                toKey);
        
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
     * A one bound subquery using the POS index with the subject of stmt1 as the
     * predicate of body[1]. The object and subject positions in the subquery
     * are unbound.
     * 
     * @see RuleRdfs09#getStmts2(SPO)
     */
    protected SPO[] getStmts2(SPO stmt1) {

        /*
         * The subject from stmt1 is in the predicate position for this query.
         */

        byte[] fromKey = db.getKeyBuilder().statement2Key(stmt1.s, NULL, NULL);

        byte[] toKey = db.getKeyBuilder().statement2Key(stmt1.s + 1, NULL, NULL);

        return db.getStatements(db.getPOSIndex(), KeyOrder.POS, fromKey,
                toKey);
    
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
