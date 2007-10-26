package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.Set;

import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
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
    
    private final Var x, y, SetP;

    /**
     * @param inf
     * @param propertyId
     * @param P
     */
    public AbstractRuleFastClosure_3_5_6_7_9(InferenceEngine inf,
            Id propertyId, Set<Long> P) {

        super(inf, //
                new Triple(var("x"), propertyId, var("y")), //
                new Pred[] {//
                new Triple(var("x"), var("{P}"), var("y")) //
                });

        this.P = P;

        this.propertyId = propertyId.id;
        
        this.x = var("x");
        this.y = var("y");
        this.SetP = var("{P}");

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
    public RuleStats apply(final RuleStats stats, final SPOBuffer buffer) {

        final long begin = System.currentTimeMillis();

        resetBindings();
        
        final long[] a = inf.getSortedArray(P);

        // Note: counting the passed in array as stmts1.
        stats.stmts1 += a.length;

        // @todo execute subqueries in parallel using inference engine thread pool.
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

            stats.numSubqueries1++;

            ISPOIterator itr2 = db.getAccessPath(NULL, p, NULL).iterator();

            try {

                while (itr2.hasNext()) {

                    SPO[] stmts = itr2.nextChunk(KeyOrder.POS);

                    if (DEBUG) {

                        log.debug("stmts1: chunk=" + stmts.length + "\n"
                                + Arrays.toString(stmts));

                    }

                    stats.stmts2 += stmts.length;

                    for (SPO spo : stmts) {

                        /*
                         * Note: since P includes rdfs:subPropertyOf (as well as
                         * all of the sub properties of rdfs:subPropertyOf)
                         * there are going to be some axioms in here that we
                         * really do not need to reassert and generally some
                         * explicit statements as well.
                         */

                        assert spo.p == p;

                        set(x, spo.s);
                        set(SetP, p);
                        set(y, spo.o);

                        emit(buffer);

                        stats.numComputed++;

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

}
