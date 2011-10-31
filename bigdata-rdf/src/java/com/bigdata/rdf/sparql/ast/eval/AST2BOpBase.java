/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Sep 28, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.cost.SubqueryCostReport;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.spo.DefaultGraphSolutionExpander;
import com.bigdata.rdf.spo.NamedGraphSolutionExpander;

/**
 * Base class provides support for triples, sids, and quads mode joins which
 * was refactored from the {@link Rule2BOpUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AST2BOpBase {

    private static final Logger log = Logger.getLogger(AST2BOpBase.class);

    protected AST2BOpBase() {
        
    }
    
    /**
     * Flag to conditionally enable the new named and default graph support.
     * <p>
     * Note: When enabled, the {@link NamedGraphSolutionExpander} and
     * {@link DefaultGraphSolutionExpander} must be stripped from the
     * {@link IPredicate.Annotations#ACCESS_PATH_EXPANDER}. In the long term, we
     * will simply no longer generate them in
     * {@link BigdataEvaluationStrategyImpl}.
     * <p>
     * Note: If you want to test just the named graph stuff, then the default
     * graph processing could be handed off to the
     * {@link DefaultGraphSolutionExpander}.
     * 
     * @deprecated This is always used so it can just go away. So can all of the
     *             related classes.
     */
    protected static final boolean enableDecisionTree = true;

    /**
     * Flag to conditionally force the use of REMOTE access paths in scale-out
     * joins. This is intended as a tool when analyzing query patterns in
     * scale-out. It should normally be <code>false</code>.
     * 
     * FIXME Make this [false]. It is currently enabled so we can go to native
     * SPARQL evaluation in CI.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/380#comment:4
     */
    protected static final boolean forceRemoteAPs = true;

    /**
     * The #of samples to take when comparing the cost of a SCAN with an IN
     * filter to subquery for each graph in the data set.
     *
     * @todo Add query hint to override this default.
     */
    protected static final int SAMPLE_LIMIT = 100;

    /**
     * Annotations used by the {@link BigdataEvaluationStrategyImpl} to
     * communicate with the {@link Rule2BOpUtility}.
     * <p>
     * <h3>Quads Mode</h3>
     * Several annotations are used to mark named and default graph patterns on
     * the {@link IPredicate}s. Rather than attaching a named or default graph
     * expander, we annotate the predicate with the metadata for the access path
     * and then convert that annotation to the appropriate bop pattern in
     * {@link Rule2BOpUtility}.
     */
    public interface Annotations {

        /**
         * Boolean flag indicates that the database is operating in quads mode.
         */
        String QUADS = Rule2BOpUtility.class.getName() + ".quads";

        boolean DEFAULT_QUADS = false;

        /**
         * The {@link Dataset} associated with the access path (quads mode
         * only). The {@link Dataset} is only provided by openrdf when FROM or
         * FROM NAMED was used in the query. Otherwise the {@link Dataset} will
         * be <code>null</code> and is not attached as an annotation.
         * <p>
         * Note: This annotation MUST be stripped from the query plan to prevent
         * an attempt to serialized it for RMI in scale-out (the {@link Dataset}
         * is not {@link Serializable}, can be quite large, and is captured by
         * other constructions in the generated query plan).
         */
        String DATASET = Rule2BOpUtility.class.getName() + ".dataset";

        /**
         * The {@link Scope} of the access path (quads mode only). In quads mode
         * the {@link Scope} is always provided by openrdf.
         *
         * @see Scope#NAMED_CONTEXTS
         * @see Scope#DEFAULT_CONTEXTS
         */
        String SCOPE = Rule2BOpUtility.class.getName() + ".scope";

        /*
         * Query planner and cost estimates.
         */

        /**
         * The original index assigned to the access path by the static query
         * optimizer.
         * <p>
         * Note: The actual index will be chosen at runtime based on the asBound
         * predicate. In scale-out, the binding sets are send to the node having
         * the shard on which the asBound predicate would read.
         */
        String ORIGINAL_INDEX = Rule2BOpUtility.class.getName()
                + ".originalIndex";

        /**
         * The estimated cardinality of an access path as determined during
         * static query optimization. This is the fast range count if the
         * predicate and {@link Long#MAX_VALUE} if the predicate is part of an
         * optional join (this is used by the query optimized to order the
         * optional joins to the end since they can not increase the selectivity
         * of the query).
         */
        String ESTIMATED_CARDINALITY = Rule2BOpUtility.class.getName()
                + ".estimatedCardinality";

        /**
         * The estimated cost of a SCAN + FILTER approach to a default graph or
         * named graph query.
         */
        String COST_SCAN = Rule2BOpUtility.class.getName() + ".cost.scan";

        /**
         * A {@link SubqueryCostReport} on the estimated cost of a SUBQUERY
         * approach to a default graph or named graph query.
         */
        String COST_SUBQUERY = Rule2BOpUtility.class.getName()
                + ".cost.subquery";

        /**
         * The #of known graphs in the {@link Dataset} for a default graph or
         * named graph query.
         */
        String NKNOWN = Rule2BOpUtility.class.getName() + ".nknown";

        /**
         * Query hint to use a hash join against the access path for a given
         * predicate. Hash joins should be enabled once it is recognized that
         * the #of as-bound probes of the predicate will approach or exceed the
         * range count of the predicate.
         * <p>
         * Note: {@link HashJoinAnnotations#JOIN_VARS} MUST also be specified
         * for the predicate. The join variable(s) are variables which are (a)
         * bound by the predicate and (b) are known bound in the source
         * solutions. The query planner has the necessary context to figure this
         * out based on the structure of the query plan and the join evaluation
         * order.
         */
        String HASH_JOIN = AST2BOpBase.class.getPackage().getName()
                + ".hashJoin";

        boolean DEFAULT_HASH_JOIN = false;
        
    }

    /**
     * Return either <i>left</i> wrapped as the sole member of an array or
     * {@link BOp#NOARGS} iff <i>left</i> is <code>null</code>.
     * 
     * @param left
     *            The prior operator in the pipeline (optional).
     * @return The array.
     */
    static protected BOp[] leftOrEmpty(final PipelineOp left) {

        return left == null ? BOp.NOARGS : new BOp[] { left };

    }

    /**
     * Apply any query hints to the operator as annotations of that operator.
     * 
     * @param op
     *            The operator.
     * @param queryHints
     *            The query hints.
     * 
     * @return A copy of that operator to which the query hints (if any) have
     *         been applied. If there are no query hints then the original
     *         operator is returned.
     * 
     *         TODO This is being phased out by the
     *         {@link ASTQueryHintOptimizer}
     */
    public static PipelineOp applyQueryHints(PipelineOp op,
            final Properties queryHints) {

        if (queryHints == null)
            return op;

        final Enumeration<?> pnames = queryHints.propertyNames();

        while (pnames.hasMoreElements()) {

            final String name = (String) pnames.nextElement();

            final String value = queryHints.getProperty(name);

            if (log.isInfoEnabled())
                log.info("Query hint: [" + name + "=" + value + "]");

            op = (PipelineOp) op.setProperty(name, value);

        }

        return op;

    }

}
