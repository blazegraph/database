/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.Set;

import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Rule used in steps 3, 5, 6, 7, and 9 of
 * {@link InferenceEngine#fastForwardClosure()}.
 * 
 * <pre>
 *    (?x, {P}, ?y) -&gt; (?x, propertyId, ?y)
 * </pre>
 * 
 * where <code>{P}</code> is the closure of the subproperties of one of the
 * FIVE (5) reserved keywords:
 * <ul>
 * <li><code>rdfs:subPropertyOf</code></li>
 * <li><code>rdfs:subClassOf</code></li>
 * <li><code>rdfs:domain</code></li>
 * <li><code>rdfs:range</code></li>
 * <li><code>rdf:type</code></li>
 * </ul>
 * 
 * The caller MUST provide a current version of "{P}" when they instantiate this
 * rule.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractRuleFastClosure_3_5_6_7_9 extends AbstractRuleRdf {

    private final Set<Long> P;

    private final long propertyId;
    
//    private final Var x, y, SetP;

    /**
     * @param inf
     * @param propertyId
     * @param P
     */
    public AbstractRuleFastClosure_3_5_6_7_9(Id propertyId, Set<Long> P) {

        super(  new Triple(var("x"), propertyId, var("y")), //
                new Pred[] {//
                new Triple(var("x"), var("{P}"), var("y")) //
                });

        this.P = P;

        this.propertyId = propertyId.id;
        
//        this.x = var("x");
//        this.y = var("y");
//        this.SetP = var("{P}");

    }

    /**
     * <code>(?x, {P}, ?y) -> (?x, propertyId, ?y)</code>
     * 
     * @param database
     *            The database.
     * @param buffer
     *            A buffer used to accumulate entailments. The buffer is flushed
     *            to the database if this method returns normally.
     * @param P
     *            A set of term identifiers.
     * @param propertyId
     *            The propertyId to be used in the assertions.
     */
    public void apply(State state) {

        final long begin = System.currentTimeMillis();
        
        final long[] a = getSortedArray(P);

        /*
         * @todo execute subqueries in parallel against shared thread pool.
         */

        for (long p : a) {

            if (p == propertyId) {

                /*
                 * The rule refuses to consider triple patterns where the
                 * predicate for the subquery is the predicate for the
                 * generated entailments since the support would then entail
                 * itself.
                 */

                continue;

            }

            state.stats.nsubqueries[0]++;

            ISPOIterator itr2 = (state.focusStore == null ? state.database
                    .getAccessPath(NULL, p, NULL).iterator() : state.focusStore
                    .getAccessPath(NULL, p, NULL).iterator());

            try {

                while (itr2.hasNext()) {

                    SPO[] stmts0 = itr2.nextChunk(KeyOrder.POS);

                    if (DEBUG) {

                        log.debug("stmts1: chunk=" + stmts0.length + "\n"
                                + Arrays.toString(stmts0));

                    }

                    state.stats.nstmts[0] += stmts0.length;

                    for (SPO spo : stmts0) {

                        /*
                         * Note: since P includes rdfs:subPropertyOf (as well as
                         * all of the sub properties of rdfs:subPropertyOf)
                         * there are going to be some axioms in here that we
                         * really do not need to reassert and generally some
                         * explicit statements as well.
                         */

                        assert spo.p == p;

                        if(state.bind(0,spo)) {

                            state.emit();
                            
                        }

                    } // next stmt

                } // while(itr2)

            } finally {

                itr2.close();

            }

        } // next p in P

        state.stats.elapsed += System.currentTimeMillis() - begin;

    }

    /**
     * Convert a {@link Set} of term identifiers into a sorted array of term
     * identifiers.
     * <P>
     * Note: When issuing multiple queries against the database, it is generally
     * faster to issue those queries in key order.
     * 
     * @return The sorted term identifiers.
     */
    public long[] getSortedArray(Set<Long> ids) {
        
        int n = ids.size();
        
        long[] a = new long[n];
        
        int i = 0;
        
        for(Long id : ids) {
            
            a[i++] = id;
            
        }
        
        Arrays.sort(a);
        
        return a;
        
    }

}
