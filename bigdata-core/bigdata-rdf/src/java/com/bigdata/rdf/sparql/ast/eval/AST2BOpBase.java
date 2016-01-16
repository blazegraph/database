/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;

/**
 * Base class provides support for triples, sids, and quads mode joins which
 * was refactored from the old Rule2BOpUtility class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AST2BOpBase {

    private static final Logger log = Logger.getLogger(AST2BOpBase.class);

    protected AST2BOpBase() {
        
    }
    
    /**
     * Annotations used to mark named and default graph patterns on the
     * {@link IPredicate}s. Rather than attaching a named or default graph
     * expander, we annotate the predicate with the metadata for the access path
     * and then convert that annotation to the appropriate pipeline operators.
     */
    public interface Annotations {

        /**
         * Boolean flag indicates that the database is operating in quads mode.
         */
        String QUADS = AST2BOpBase.class.getName() + ".quads";

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
        String DATASET = AST2BOpBase.class.getName() + ".dataset";

        /**
         * The {@link Scope} of the access path (quads mode only). In quads mode
         * the {@link Scope} is always provided by openrdf.
         *
         * @see Scope#NAMED_CONTEXTS
         * @see Scope#DEFAULT_CONTEXTS
         */
        String SCOPE = AST2BOpBase.class.getName() + ".scope";

        /**
         * Boolean annotation indicates whether the generated JOIN is simple (a
         * single JOIN operator with optional constraints but without any
         * variable materialization requirements) or complex (a JOIN operator
         * associated with at least one constraint which requires the
         * materialization of variables that are not already known to be
         * materialized).
         */
        String SIMPLE_JOIN = AST2BOpBase.class.getName() + ".simpleJoin";
        
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
         * 
         * TODO It would make sense to lift this annotation into a different AST
         * optimizer so it is always present. An optimization for index locality
         * for as-bound evaluation depends on the presence of this annotation.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/150" >
         *      Choosing the index for testing fully bound access paths based on
         *      index locality</a>
         */
        String ORIGINAL_INDEX = AST2BOpBase.class.getName()
                + ".originalIndex";

        /**
         * The estimated cardinality of an access path as determined during
         * static query optimization. This is the fast range count if the
         * predicate and {@link Long#MAX_VALUE} if the predicate is part of an
         * optional join (this is used by the query optimized to order the
         * optional joins to the end since they can not increase the selectivity
         * of the query).
         */
        String ESTIMATED_CARDINALITY = AST2BOpBase.class.getName()
                + ".estimatedCardinality";

        /**
         * The estimated cost of a SCAN + FILTER approach to a default graph or
         * named graph query.
         */
        String COST_SCAN = AST2BOpBase.class.getName() + ".cost.scan";

        /**
         * A {@link SubqueryCostReport} on the estimated cost of a SUBQUERY
         * approach to a default graph or named graph query.
         */
        String COST_SUBQUERY = AST2BOpBase.class.getName()
                + ".cost.subquery";

        /**
         * The #of known graphs in the {@link Dataset} for a default graph or
         * named graph query.
         */
        String NKNOWN = AST2BOpBase.class.getName() + ".nknown";
        
        
        /**
         * Boolean indicating whether the AST2BOpUpdate should autocommit.  Default
         * value is TRUE.
         */
        
        String AUTO_COMMIT = AST2BOpBase.class.getName() + "AUTO_COMMIT";

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

//    /**
//     * Apply any query hints to the operator as annotations of that operator.
//     * <p>
//     * Note: This method is responsible for transferring query hints from
//     * {@link ASTBase#getQueryHints()} onto a generated {@link PipelineOp}.
//     * 
//     * @param op
//     *            The operator.
//     * @param queryHints
//     *            The query hints (from {@link ASTBase#getQueryHints()}).
//     * 
//     * @return A copy of that operator to which the query hints (if any) have
//     *         been applied. If there are no query hints then the original
//     *         operator is returned.
//     * 
//     * @deprecated by
//     *             {@link #applyQueryHints(PipelineOp, ASTBase, AST2BOpContext)}
//     *             which allows by global and AST node specific query hints to
//     *             be applied.
//     *             
//     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
//     *      Clean up query hints </a>
//     */
//    @Deprecated
//    protected static PipelineOp applyQueryHints(PipelineOp op,
//            final Properties queryHints) {
//
//        return _applyQueryHints(op, queryHints);
//
//    }

    /**
     * Apply any query hints to the operator as annotations of that operator.
     * <p>
     * Note: This method is responsible for transferring query hints from
     * {@link ASTBase#getQueryHints()} onto a generated {@link PipelineOp}.
     * 
     * @param op
     *            The pipeline operator generated from some AST node.
     * @param node
     *            The AST node from which the pipeline operator was generated
     *            (required). The query hints (from
     *            {@link ASTBase#getQueryHints()}) will be applied to that
     *            pipeline operator.
     * @param ctx
     *            The evaluation context (ignored). Global query hints declared
     *            here will be applied to the generated pipeline operator.
     *            Global hints are applied <strong>first</strong> so they can be
     *            override by AST node specific hints.
     * 
     * @return A copy of that operator to which the query hints (if any) have
     *         been applied. If there are no query hints then the original
     *         operator is returned.
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
     *      Clean up query hints </a>
     */
    protected static PipelineOp applyQueryHints(PipelineOp op,
            final ASTBase node, final AST2BOpContext ctx) {

        /*
         * Note: The global query hints are transferred onto the AST nodes by
         * the ASTQueryHintOptimizer and the registered IQueryHint classes. They
         * do NOT need to be reapplied here.
         */
//        // Apply global query hints from ASTContext.
//        op = _applyQueryHints(op, ctx.queryHints);

//        if (node != null) {
            // Apply ASTBase node specific query hints.
            op = _applyQueryHints(op, node.getQueryHints());
//        }

        return op;
        
    }

    /**
     * Apply any query hints to the operator as annotations of that operator.
     * <p>
     * Note: This method is responsible for transferring query hints from
     * {@link ASTBase#getQueryHints()} onto a generated {@link PipelineOp}.
     * <p>
     * Note: This alternative form is for use within caller contexts in which
     * (due to historical reasons) we have the query hints object for the AST
     * node, but not the AST node itself. This pattern shows up in the join()
     * methods since the {@link StatementPatternNode} is not passed through.
     * 
     * @param op
     *            The pipeline operator generated from some AST node (required).
     * @param nodeQueryHints
     *            The query hints for the AST node from which the pipeline
     *            operator was generated or its dominating operator context
     *            since not all operators have query hints applied (required).
     * @param ctx
     *            The evaluation context (ignored). Global query hints declared
     *            here will be applied to the generated pipeline operator.
     *            Global hints are applied <strong>first</strong> so they can be
     *            override by AST node specific hints.
     * 
     * @return A copy of that operator to which the query hints (if any) have
     *         been applied. If there are no query hints then the original
     *         operator is returned.
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
     *      Clean up query hints </a>
     */
    protected static PipelineOp applyQueryHints(PipelineOp op,
            final Properties nodeQueryHints, final AST2BOpContext ctx) {

        /*
         * Note: The global query hints are transferred onto the AST nodes by
         * the ASTQueryHintOptimizer and the registered IQueryHint classes. They
         * do NOT need to be reapplied here.
         */
//        // Apply global query hints from ASTContext.
//        op = _applyQueryHints(op, ctx.queryHints);

//        if (nodeQueryHints != null) {
            // Apply ASTBase node specific query hints.
            op = _applyQueryHints(op, nodeQueryHints);
//        }

        return op;
        
    }

    /**
     * Apply any query hints to the operator as annotations of that operator.
     * <p>
     * Note: This method is responsible for transferring query hints from
     * {@link ASTBase#getQueryHints()} onto a generated {@link PipelineOp}.
     * 
     * @param op
     *            The operator.
     * @param queryHints
     *            The query hints (optional).
     * 
     * @return A copy of that operator to which the query hints (if any) have
     *         been applied. If there are no query hints then the original
     *         operator is returned.
     */
    private static PipelineOp _applyQueryHints(PipelineOp op,
            final Properties queryHints) {

        if (queryHints == null)
            return op;

        final Enumeration<?> pnames = queryHints.propertyNames();

        while (pnames.hasMoreElements()) {

            final String name = (String) pnames.nextElement();

            final String value = queryHints.getProperty(name);

            if (log.isDebugEnabled())
                log.debug("Query hint: op=" + (op.getClass().getSimpleName())
                        + " [" + name + "=" + value + "]");

            op = (PipelineOp) op.setProperty(name, value);

        }

        return op;

    }

}
