package com.bigdata.rdf.sail;

import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;

import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IRule;

/**
 * Uses range counts to give cost estimates based on the size of the expected
 * results. Note that this class is NOT used when {@link Options#NATIVE_JOINS}
 * is enabled since the {@link IRule} execution will automatically assign its
 * own evaluation plan and that evaluation plan will be informed by the range
 * counts.
 * 
 * See
 * {@link BigdataSailConnection#evaluate(TupleExpr, Dataset, BindingSet, boolean)}
 * 
 * FIXME quads : if a {@link StatementPattern} is to read against the default
 * context and that is a merge of pre-defined contexts then we need to use the
 * union of the range counts for those contexts.
 */
public class BigdataEvaluationStatistics extends EvaluationStatistics {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(BigdataEvaluationStatistics.class);

    private final BigdataSailConnection conn;

    /**
     * When <code>true</code>, range counts will be obtained and used to
     * influence the join order.
     */
    private final boolean useRangeCounts = true;

    public BigdataEvaluationStatistics(BigdataSailConnection conn) {

        this.conn = conn;

    }

    @Override
    protected CardinalityCalculator createCardinalityCalculator() {

        return new BigdataCardinalityCalculator();

    }

    /**
     * These uses range counts on the various {@link StatementPattern}s to
     * influence the cardinality.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class BigdataCardinalityCalculator extends CardinalityCalculator {

        public BigdataCardinalityCalculator() {

        }

        @Override
        public void meet(StatementPattern sp) {

            /*
             * Figure out which positions in the pattern are bound to
             * constants, obtaining the corresponding internal Value object.
             * 
             * Note: This assumes that the term identifier on the Value was
             * already resolved as part of the query optimization (the
             * constants should have been re-written into internal _Value
             * objects and their term identifiers resolved against the
             * database).
             */

            final BigdataResource subj = (BigdataResource) getConstantValue(sp
                    .getSubjectVar());

            final BigdataURI pred = (BigdataURI) getConstantValue(sp
                    .getPredicateVar());

            final BigdataValue obj = (BigdataValue) getConstantValue(sp
                    .getObjectVar());

            final BigdataResource context = (BigdataResource) getConstantValue(sp
                    .getContextVar());

            if ((subj != null && subj.getIV() == BigdataSail.NULL)
                    || (pred != null && pred.getIV() == BigdataSail.NULL)
                    || (obj != null && obj.getIV() == BigdataSail.NULL)
                    || (context != null && context.getIV() == BigdataSail.NULL)) {

                // non-existent subject, predicate, object or context

                if (log.isDebugEnabled())
                    log
                            .debug("One or more constants not found in the lexicon: "
                                    + sp);

                cardinality = 0;

                return;

            }

            final long rangeCount;
            if (useRangeCounts) {

                /*
                 * Get the most efficient access path.
                 */

                final IAccessPath<?> accessPath = conn.database.getSPORelation().getAccessPath(
                        (subj == null ? BigdataSail.NULL : subj.getIV()),//
                        (pred == null ? BigdataSail.NULL : pred.getIV()),//
                        (obj == null ? BigdataSail.NULL : obj.getIV()),//
                        (context == null ? BigdataSail.NULL : context.getIV())//
                        );

                /*
                 * The range count for that access path based on the data. The
                 * variables will be unbound at this point so the selectivity
                 * will depend mostly on the #of SPOC positions that were bound
                 * to constants in the query.
                 */

                rangeCount = accessPath.rangeCount(false/* exact */);

                cardinality = rangeCount;

            } else {

                /*
                 * Fake range count, e.g., because we are going to translate the
                 * query into a native Rule and the Rule will self-optimize when
                 * it is executed so we want to avoid getting the range count
                 * data twice.
                 */ 

                rangeCount = 1000;

            }
            
            cardinality = rangeCount;
            
//            final int boundVarCount = countBoundVars(sp);
//
//            final int sqrtFactor = 2 * boundVarCount;

            List<Var> vars = sp.getVarList();
            
            final int constantVarCount = countConstantVars(vars);
            
			final double unboundVarFactor = 
				(double)(vars.size() - constantVarCount) / vars.size();
            
            cardinality = Math.pow(rangeCount, unboundVarFactor);

            if (log.isInfoEnabled())
                log.info("cardinality=" + cardinality + ", nconstant="
                        + constantVarCount + ", rangeCount=" + rangeCount
                        + ", pattern=" + sp);

        }

        /**
         * Return the value of the variable iff bound to a constant and
         * otherwise <code>null</code>.
         * 
         * @param var
         *            The variable (MAY be <code>null</code>, in which
         *            case <code>null</code> is returned).
         * 
         * @return Its constant value -or- <code>null</code>.
         */
        protected Value getConstantValue(final Var var) {

            if (var != null) {

                return var.getValue();

            }

            return null;

        }

    }

}
