package com.bigdata.rdf.inf;

import java.util.Set;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.util.KeyOrder;

/**
 * <code>(?x, P, ?y) -> (?x, propertyId, ?y)</code>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractRuleFastClosure_3_5_6_7_9 extends AbstractRuleRdf {

    protected final Set<Long> P;

    protected final long propertyId;

    /**
     * @param inf
     * @param P
     * @param propertyId
     * 
     * @todo refactor [P] into the rule execution and refactor the unit
     *       tests as well.
     */
    public AbstractRuleFastClosure_3_5_6_7_9(InferenceEngine inf, Var x,
            Id propertyId, Var y, Set<Long> P) {

        super(inf, new Triple(x, propertyId, y), new Pred[] {
        //                    new Triple(x, P, y)
                });

        this.P = P;

        this.propertyId = propertyId.id;

    }

    /**
     * <code>(?x, P, ?y) -> (?x, propertyId, ?y)</code>
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

            ISPOIterator itr = db.getAccessPath(NULL, p, NULL).iterator();

            while (itr.hasNext()) {

                SPO[] stmts = itr.nextChunk(KeyOrder.POS);

                stats.stmts2 += stmts.length;

                for (SPO spo : stmts) {

                    /*
                     * Note: since P includes rdfs:subPropertyOf (as well as
                     * all of the sub properties of rdfs:subPropertyOf)
                     * there are going to be some axioms in here that we
                     * really do not need to reassert and generally some
                     * explicit statements as well.
                     */

                    SPO newSPO = new SPO(spo.s, propertyId, spo.o,
                            StatementEnum.Inferred);

                    Justification jst = null;

                    if (justify) {

                        jst = new Justification(this, newSPO, new long[] {//
                                NULL, p, NULL,// stmt1
                                        spo.s, spo.p, spo.o // stmt2
                                });
                    }

                    buffer.add(newSPO, jst);

                    stats.numComputed++;

                }

            }

        }

        stats.elapsed += System.currentTimeMillis() - begin;

        return stats;

    }

}
