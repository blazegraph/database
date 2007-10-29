package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.Set;

import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
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
    public AbstractRuleFastClosure_3_5_6_7_9(AbstractTripleStore db,
            Id propertyId, Set<Long> P) {

        super(db, //
                new Triple(var("x"), propertyId, var("y")), //
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
     *            A buffer used to accumulate entailments. The buffer is
     *            flushed to the database if this method returns normally.
     * @param P
     *            A set of term identifiers.
     * @param propertyId
     *            The propertyId to be used in the assertions.
     */
    public RuleStats apply(final boolean justify, final SPOBuffer buffer) {

        final long begin = System.currentTimeMillis();

        resetBindings();
        
        final long[] a = getSortedArray(P);

//        // Note: counting the passed in array as stmts[0].
//        stats.nstmts[0] += a.length;

        /*
         * @todo execute subqueries in parallel using inference engine thread
         * pool, but note that the bindings[] need to be per-thread or thread
         * local!  SPOBuffer also needs to be thread-safe.
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

            stats.nsubqueries[0]++;

            ISPOIterator itr2 = db.getAccessPath(NULL, p, NULL).iterator();

            try {

                while (itr2.hasNext()) {

                    SPO[] stmts0 = itr2.nextChunk(KeyOrder.POS);

                    if (DEBUG) {

                        log.debug("stmts1: chunk=" + stmts0.length + "\n"
                                + Arrays.toString(stmts0));

                    }

                    stats.nstmts[0] += stmts0.length;

                    for (SPO spo : stmts0) {

                        /*
                         * Note: since P includes rdfs:subPropertyOf (as well as
                         * all of the sub properties of rdfs:subPropertyOf)
                         * there are going to be some axioms in here that we
                         * really do not need to reassert and generally some
                         * explicit statements as well.
                         */

                        assert spo.p == p;

                        bind(0,spo);
//                        set(x, spo.s);
//                        set(SetP, p);
//                        set(y, spo.o);

                        emit(justify,buffer);

                    } // next stmt

                } // while(itr2)

            } finally {

                itr2.close();

            }

        } // next p in P

        assert checkBindings();
        
        stats.elapsed += System.currentTimeMillis() - begin;

        return stats;

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
