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
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.filter.DistinctFilter;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.cost.ScanCostReport;
import com.bigdata.bop.cost.SubqueryCostReport;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.AccessPathJoinAnnotations;
import com.bigdata.bop.join.HTreeHashJoinOp;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.JVMHashJoinOp;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.filter.HTreeDistinctFilter;
import com.bigdata.bop.rdf.filter.StripContextFilter;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.bop.rdf.join.InlineMaterializeOp;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.internal.constraints.IsInlineBOp;
import com.bigdata.rdf.internal.constraints.IsMaterializedBOp;
import com.bigdata.rdf.internal.constraints.NeedsMaterializationBOp;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.TryBeforeMaterializationConstraint;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.lexicon.LexPredicate;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sop.SOp2BOpUtility;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.spo.DefaultGraphSolutionExpander;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.InGraphHashSetFilter;
import com.bigdata.rdf.spo.NamedGraphSolutionExpander;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.EmptyAccessPathExpander;

/**
 * Base class provides support for triples, sids, and quads mode joins which
 * was refactored from the {@link Rule2BOpUtility}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AST2BOpBase {

    private static final Logger log = Logger.getLogger(AST2BOpBase.class);

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
    private static final boolean enableDecisionTree = true;

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
    private static final int SAMPLE_LIMIT = 100;

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

    /**
     * @deprecated This is part of the {@link SOp2BOpUtility} integration.
     */
    @SuppressWarnings("rawtypes")
    public static PipelineOp join(final AbstractTripleStore db,
            final QueryEngine queryEngine,
            final PipelineOp left, final Predicate pred,
            final Collection<IConstraint> constraints,
            final AtomicInteger idFactory, final Properties queryHints) {

        return join(db, queryEngine, left, pred,
                new LinkedHashSet<IVariable<?>>(), constraints,
                new BOpContextBase(queryEngine), idFactory, queryHints);

    }

    /**
     * Adds a series of materialization steps to materialize terms needed
     * downstream.
     * 
     * To materialize the variable ?term, the pipeline looks as follows:
     * 
     * <pre>
     * left
     * ->
     * ConditionalRoutingOp1 (condition=!IsMaterialized(?term), alt=right)
     * ->
     * ConditionalRoutingOp2 (condition=IsInline(?term), alt=PipelineJoin)
     * ->
     * InlineMaterializeOp (predicate=LexPredicate(?term), sink=right)
     * ->
     * PipelineJoin (predicate=LexPredicate(?term))
     * ->
     * right
     * </pre>
     * 
     * @param db
     *            the database
     * @param queryEngine
     *            the query engine
     * @param left
     *            the left (upstream) operator that immediately proceeds the
     *            materialization steps
     * @param right
     *            the right (downstream) operator that immediately follows the
     *            materialization steps
     * @param c
     *            the constraint to run on the IsMaterialized op to see if the
     *            materialization pipeline can be bypassed (bypass if true and
     *            no {@link NotMaterializedException} is thrown).
     * @param varsToMaterialize
     *            the terms to materialize
     * @param idFactory
     *            the bop id factory
     * @param queryHints
     *            the query hints
     * @return the final bop added to the pipeline by this method
     * 
     * @see AST2BOpUtility#addMaterializationSteps(PipelineOp, int,
     *      IValueExpression, Collection, AST2BOpContext)
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static PipelineOp addMaterializationSteps(
            final AbstractTripleStore db,
            final QueryEngine queryEngine, PipelineOp left, final int right,
            final IConstraint c,
            final Collection<IVariable<IV>> varsToMaterialize,
            final AtomicInteger idFactory, final Properties queryHints) {

        final AST2BOpContext context = new AST2BOpContext(
                null/* astContainer */, idFactory, db, queryEngine, queryHints);

        final IValueExpression<IV> ve = (IValueExpression) c.get(0);

        return addMaterializationSteps(left, right, ve, varsToMaterialize,
                context);

    }
    
    /**
     * If the value expression that needs the materialized variables can run
     * without a {@link NotMaterializedException} then just route to the
     * <i>rightId</i> (around the rest of the materialization pipeline steps).
     * This happens in the case of a value expression that only "sometimes"
     * needs materialized values, but not always (i.e. materialization
     * requirement depends on the data flowing through). A good example of this
     * is {@link CompareBOp}, which can sometimes work on internal values and
     * sometimes can't.
     * 
     * TODO Consider the efficiency of the steps which are being taken. Should
     * we test for the most common cases first, or for those with the least
     * latency to "fix"?
     * 
     * @see TryBeforeMaterializationConstraint
     */
    @SuppressWarnings("rawtypes")
    public static PipelineOp addMaterializationSteps(PipelineOp left,
            final int rightId, final IValueExpression<IV> ve,
            final Collection<IVariable<IV>> vars, final AST2BOpContext ctx) {

        /*
         * If the constraint "c" can run without a NotMaterializedException then
         * bypass the rest of the pipeline by routing the solutions to rightId.
         */
        final IConstraint c2 = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                new NeedsMaterializationBOp(ve));

        left = applyQueryHints(new ConditionalRoutingOp(leftOrEmpty(left),
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(ConditionalRoutingOp.Annotations.CONDITION, c2),//
                new NV(PipelineOp.Annotations.ALT_SINK_REF, rightId)//
                ), ctx.queryHints);

        return addMaterializationSteps(left, rightId, vars, ctx);

    }

    /**
     * Adds a series of materialization steps to materialize terms needed
     * downstream. To materialize the variable <code>?term</code>, the pipeline
     * looks as follows:
     * 
     * <pre>
     * (A) leftId      : The upstream operator
     * (B) condId1     : if !materialized then condId2 (fall through) else rightId 
     * (C) condId2     : if inline then inlineMatId (fall through) else lexJoinId
     * (D) inlineMatId : InlineMaterializeOp; goto rightId.
     * (E) lexJoinId   : LexJoin (fall through).
     * (F) rightId     : the downstream operator.
     * </pre>
     * 
     * <pre>
     * left 
     * ->
     * ConditionalRoutingOp1 (condition=!IsMaterialized(?term), alt=right)
     * ->
     * ConditionalRoutingOp2 (condition=IsInline(?term), alt=PipelineJoin)
     * ->
     * InlineMaterializeOp (predicate=LexPredicate(?term), sink=right)
     * ->
     * PipelineJoin (predicate=LexPredicate(?term))
     * ->
     * right
     * </pre>
     * 
     * @param left
     *            the left (upstream) operator that immediately proceeds the
     *            materialization steps
     * @param rightId
     *            the id of the right (downstream) operator that immediately
     *            follows the materialization steps
     * @param vars
     *            the terms to materialize
     * 
     * @return the final bop added to the pipeline by this method
     * 
     * @see TryBeforeMaterializationConstraint
     * 
     * TODO make [vars] a Set.
     */
    @SuppressWarnings("rawtypes")
    protected static PipelineOp addMaterializationSteps(PipelineOp left,
            final int rightId, final Collection<IVariable<IV>> vars,
            final AST2BOpContext ctx) {

        final Iterator<IVariable<IV>> it = vars.iterator();

        int firstId = ctx.nextId();

        while (it.hasNext()) {

            final IVariable<IV> v = it.next();

            // Generate bopIds for the routing targets for this variable.
            final int condId1 = firstId;
            final int condId2 = ctx.nextId();
            final int inlineMaterializeId = ctx.nextId();
            final int lexJoinId = ctx.nextId();

            final int endId;

            if (!it.hasNext()) {

                /*
                 * If there are no more terms to materialize, the terminus of
                 * this materialization pipeline is the "right" (downstream)
                 * operator that was passed in.
                 */
                endId = rightId;

            } else {

                /*
                 * If there are more terms, the terminus of this materialization
                 * pipeline is the 1st operator of the next materialization
                 * pipeline.
                 */
                endId = firstId = ctx.nextId();

            }

            final IConstraint c1 = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    new IsMaterializedBOp(v, false/* materialized */));

            final PipelineOp condOp1 = applyQueryHints(
                    new ConditionalRoutingOp(leftOrEmpty(left), //
                            new NV(BOp.Annotations.BOP_ID, condId1),//
                            new NV(ConditionalRoutingOp.Annotations.CONDITION,
                                    c1),//
                            new NV(PipelineOp.Annotations.SINK_REF, condId2),//
                            new NV(PipelineOp.Annotations.ALT_SINK_REF, endId)//
                    ), ctx.queryHints);

            if (log.isDebugEnabled()) {
                log.debug("adding 1st conditional routing op: " + condOp1);
            }

            final IConstraint c2 = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    new IsInlineBOp(v, true/* inline */));

            final PipelineOp condOp2 = applyQueryHints(
                    new ConditionalRoutingOp(leftOrEmpty(condOp1), //
                            new NV(BOp.Annotations.BOP_ID, condId2), //
                            new NV(ConditionalRoutingOp.Annotations.CONDITION,
                                    c2),//
                            new NV(PipelineOp.Annotations.SINK_REF,
                                    inlineMaterializeId), //
                            new NV(PipelineOp.Annotations.ALT_SINK_REF,
                                    lexJoinId)//
                    ), ctx.queryHints);

            if (log.isDebugEnabled()) {
                log.debug("adding 2nd conditional routing op: " + condOp2);
            }

            final Predicate lexPred;
            {

                long timestamp = ctx.db.getTimestamp();

                if (TimestampUtility.isReadWriteTx(timestamp)) {
                    /*
                     * Note: Use the timestamp of the triple store view unless
                     * this is a read/write transaction, in which case we need
                     * to use the last commit point in order to see any writes
                     * which it may have performed (lexicon writes are always
                     * unisolated).
                     */
                    timestamp = ITx.UNISOLATED;
                }

                final String ns = ctx.db.getLexiconRelation().getNamespace();

                /*
                 * An anonymous variable whose name is based on the variable in
                 * the query whose Value we are trying to materialize from the
                 * IV.
                 */
                @SuppressWarnings("unchecked")
                final IVariable<BigdataValue> anonvar = Var.var("--"
                        + v.getName() + "-" + ctx.nextId());
                
                lexPred = LexPredicate.reverseInstance(ns, timestamp, anonvar,
                        v);

            }

            if (log.isDebugEnabled()) {
                log.debug("lex pred: " + lexPred);
            }

            final PipelineOp inlineMaterializeOp = applyQueryHints(
                    new InlineMaterializeOp(
                            leftOrEmpty(condOp2),//
                            new NV(BOp.Annotations.BOP_ID, inlineMaterializeId),//
                            new NV(InlineMaterializeOp.Annotations.PREDICATE,
                                    lexPred.clone()),//
                            new NV(PipelineOp.Annotations.SINK_REF, endId)//
                    ), ctx.queryHints);

            if (log.isDebugEnabled()) {
                log.debug("adding inline materialization op: "
                        + inlineMaterializeOp);
            }

            {
                
                // annotations for this join.
                final List<NV> anns = new LinkedList<NV>();

                // TODO Why is lexPred being cloned?
                Predicate<?> pred = (Predicate) lexPred.clone();
                
                anns.add(new NV(BOp.Annotations.BOP_ID, lexJoinId));
                anns.add(new NV(PipelineOp.Annotations.SINK_REF, endId));
                
                if (ctx.isCluster() && !AST2BOpBase.forceRemoteAPs) {
                    // use a partitioned join.
                    anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.SHARDED));
                    pred = (Predicate) pred.setProperty(
                            Predicate.Annotations.REMOTE_ACCESS_PATH, false);
                }

                anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

                // Join against the lexicon to materialize the Value.
                final PipelineOp lexJoinOp = applyQueryHints(//
                        new PipelineJoin(leftOrEmpty(inlineMaterializeOp), //
                                anns.toArray(new NV[anns.size()])),
                        ctx.queryHints);
//                final PipelineOp lexJoinOp = newJoin(inlineMaterializeOp, anns,
//                        ctx.queryHints);

                if (log.isDebugEnabled()) {
                    log.debug("adding lex join op: " + lexJoinOp);
                }

                left = lexJoinOp;

            }
            
        }

        return left;

    }

    /**
     * Add a join for a statement pattern. This handles triples-mode,
     * named-graph and default graph join patterns whether on a single machine
     * or on a cluster.
     * <p>
     * If there are join constraints, then their materialization requirements
     * are considered. Constraints are attached directly to the join if they do
     * not have materialization requirements, or if those materialization
     * requirements either have been or *may* have been satisified (
     * {@link Requirement#SOMETIMES}).
     * <p>
     * Constraints which can not be attached to the join, or which *might* not
     * have their materialization requirements satisified by the time the join
     * runs ({@link Requirement#SOMETIMES}) cause a materialization pattern to
     * be appended to the pipeline. Once the materialization requirements have
     * been satisified, the constraint is then imposed by a
     * {@link ConditionalRoutingOp}.
     * 
     * @param db
     * @param queryEngine
     * @param left
     * @param pred
     *            The predicate describing the statement pattern.
     * @param doneSet
     *            The set of variables already known to be materialized.
     * @param constraints
     *            Constraints on that join (optional).
     * @param context
     * @param idFactory
     * @param queryHints
     *            Query hints associated with that statement pattern.
     * @return
     * 
     *         FIXME Pull this out into a method. It is generic and can be used
     *         for build a materialization pipeline after any kind of join and
     *         applying join filters as soon as their data is materialized.
     * 
     *         -OR- Can I just handle this in ASTAttachJoinFiltersOptimizer (or
     *         a follow on optimizer) by moving filter nodes which require
     *         materialization to after the join, at which point the logic to
     *         add the filter to the pipeline will take care of the
     *         materialization steps automatically?
     * 
     *         TODO The RTO will need to assign the ids to joins so it can
     *         correlate them with the {@link IJoinNode}s.
     */
    @SuppressWarnings("rawtypes")
    public static PipelineOp join(//
            final AbstractTripleStore db,//
            final QueryEngine queryEngine,//
            PipelineOp left,//
            Predicate pred,//
            final Set<IVariable<?>> doneSet,// variables known to be materialized.
            final Collection<IConstraint> constraints,//
            final BOpContextBase context, //
            final AtomicInteger idFactory,//
            final Properties queryHints) {

        final int joinId = idFactory.incrementAndGet();

        // annotations for this join.
        final List<NV> anns = new LinkedList<NV>();

        anns.add(new NV(BOp.Annotations.BOP_ID, joinId));

        /*
         * Some constraints need to be detached from the join so that we can
         * add a materialization step in between the join and the constraint
         * evaluation.
         */
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization =
            new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();
        
        if (constraints != null && !constraints.isEmpty()) {

            // create a mutable version
            final Collection<IConstraint> tmp = new LinkedList<IConstraint>();

            tmp.addAll(constraints);

            final Collection<IConstraint> tryBeforeMaterialization =
                new LinkedList<IConstraint>();

            final Iterator<IConstraint> it = tmp.iterator();

            while (it.hasNext()) {

                final IConstraint c = it.next();

                /*
                 * If this constraint needs materialized variables, remove it
                 * from the join and run it as a ConditionalRoutingOp later.
                 */

                final Set<IVariable<IV>> terms =
                    new LinkedHashSet<IVariable<IV>>();

                final Requirement req = StaticAnalysis
                        .gatherVarsToMaterialize(c, terms);

                if (req != Requirement.NEVER) {

                    it.remove();

                    if (req == Requirement.SOMETIMES) {

                        tryBeforeMaterialization.add(c);

                    }

                    needsMaterialization.put(c, terms);

                }

            }

            for (IConstraint c : tryBeforeMaterialization) {

                // need to make a clone so that BOpUtility doesn't complain
                c = (IConstraint) c.clone();

                tmp.add(new TryBeforeMaterializationConstraint(c));

            }

            // add constraints to the join for that predicate.
            anns.add(new NV(
                    PipelineJoin.Annotations.CONSTRAINTS,
                    tmp.toArray(new IConstraint[tmp.size()])));

        }

        /*
         * Pull off annotations before we clear them from the predicate.
         */
        final Scope scope = (Scope) pred.getProperty(Annotations.SCOPE);

        // true iff this is a quads access path.
        final boolean quads = pred.getProperty(Annotations.QUADS,
                Annotations.DEFAULT_QUADS);

        // pull of the Sesame dataset before we strip the annotations.
        final DatasetNode dataset = (DatasetNode) pred
                .getProperty(Annotations.DATASET);

        // strip off annotations that we do not want to propagate.
        pred = pred.clearAnnotations(new String[] { Annotations.SCOPE,
                Annotations.QUADS, Annotations.DATASET });

        if (quads) {

            /*
             * Quads mode.
             */

            if (enableDecisionTree) {
                /*
                 * Strip off the named graph or default graph expander (in the
                 * long term it will simply not be generated.)
                 */
                pred = pred
                        .clearAnnotations(new String[] { IPredicate.Annotations.ACCESS_PATH_EXPANDER });

                switch (scope) {
                case NAMED_CONTEXTS:
                    left = namedGraphJoin(queryEngine, context, idFactory,
                            left, anns, pred, dataset, queryHints);
                    break;
                case DEFAULT_CONTEXTS:
                    left = defaultGraphJoin(queryEngine, context, idFactory,
                            left, anns, pred, dataset, queryHints);
                    break;
                default:
                    throw new AssertionError();
                }

            } else {

                /*
                 * This is basically the old way of handling quads query
                 * using expanders which were attached by toPredicate() in
                 * BigdataEvaluationStrategyImpl.
                 */

                final boolean scaleOut = queryEngine.isScaleOut();

                if (scaleOut)
                    throw new UnsupportedOperationException();

                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.ANY));

                anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

                left = newJoin(left, anns, queryHints);

            }

        } else {

            /*
             * Triples or provenance mode.
             */

            left = triplesModeJoin(queryEngine, left, anns, pred, queryHints);

        }

        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        if (!needsMaterialization.isEmpty()) {

            final Set<IVariable<?>> alreadyMaterialized = doneSet;

            for (Map.Entry<IConstraint, Set<IVariable<IV>>> e :
                needsMaterialization.entrySet()) {

                final IConstraint c = e.getKey();

                final Set<IVariable<IV>> terms = e.getValue();

                // remove any terms already materialized
                terms.removeAll(alreadyMaterialized);

                // add any new terms to the list of already materialized
                alreadyMaterialized.addAll(terms);

                final int condId = idFactory.incrementAndGet();

                // we might have already materialized everything we need
                if (!terms.isEmpty()) {

                    // Add materialization steps for remaining variables.
                    left = addMaterializationSteps(db, queryEngine, left,
                            condId, c, terms, idFactory, queryHints);

                }

                left = Rule2BOpUtility.applyQueryHints(//
                    new ConditionalRoutingOp(leftOrEmpty(left),//
                        new NV(BOp.Annotations.BOP_ID, condId),//
                        new NV(ConditionalRoutingOp.Annotations.CONDITION,c)//
                    ), queryHints);

            }

        }

        return left;

    }

    /**
     * Generate a {@link PipelineJoin} for a triples mode access path.
     *
     * @param queryEngine
     * @param left
     * @param anns
     * @param pred
     *
     * @return The join operator.
     */
    private static PipelineOp triplesModeJoin(final QueryEngine queryEngine,
            final PipelineOp left, final List<NV> anns, Predicate<?> pred,
            final Properties queryHints) {

        final boolean scaleOut = queryEngine.isScaleOut();
        if (scaleOut && !forceRemoteAPs) {
            /*
             * All triples queries can run shard-wise in scale-out.
             */
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.SHARDED));
            pred = (Predicate) pred.setProperty(
                    Predicate.Annotations.REMOTE_ACCESS_PATH, false);
        } else {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.ANY));
        }

        anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

//        return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                .toArray(new NV[anns.size()])), queryHints);
        return newJoin(left, anns, queryHints);

    }

    /**
     * Generate a named graph join (quads mode).
     *
     * @param queryEngine
     * @param left
     * @param anns
     * @param pred
     * @param cvar
     * @return
     *
     * @todo If the context position is shared by some other variable which we
     *       know to be bound based on the selected join order, then we need to
     *       treat the context variable as during this analysis.
     *
     * @todo Since we do not know the specific asBound values, but only that
     *       they will be bound, we should defer the SCAN versus SUBQUERY
     *       decision until we actually evaluate that access path. This is
     *       basically a special case of runtime query optimization.
     */
    private static PipelineOp namedGraphJoin(final QueryEngine queryEngine,
            final BOpContextBase context, final AtomicInteger idFactory,
            final PipelineOp left, final List<NV> anns, Predicate<?> pred,
            final DatasetNode dataset, final Properties queryHints) {

        final boolean scaleOut = queryEngine.isScaleOut();
        if (scaleOut && !forceRemoteAPs) {
            /*
             * All named graph patterns in scale-out are partitioned (sharded).
             */
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.SHARDED));
            pred = (Predicate) pred.setProperty(
                    Predicate.Annotations.REMOTE_ACCESS_PATH, false);
        } else {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.ANY));
        }

        if (dataset == null || dataset.getNamedGraphs()==null) {

            /*
             * The dataset is all graphs. C is left unbound and the unmodified
             * access path is used.
             */

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])), queryHints);
            return newJoin(left, anns, queryHints);

        }

        if (pred.get(3/* c */).isConstant()) {

            /*
             * C is already bound.  The unmodified access path is used.
             */

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])), queryHints);
            return newJoin(left, anns, queryHints);

        }

        /*
         * @todo raise this into the caller and do one per rule rather than once
         * per access path. While a query can mix default and named graph access
         * paths, there is only one named graph collection and one default graph
         * collection within the scope of that query.
         */
        final DataSetSummary summary = dataset.getNamedGraphs();

        anns.add(new NV(Annotations.NKNOWN, summary.nknown));

        if (summary.nknown == 0) {

            /*
             * The data set is empty (no graphs). Return a join backed by an
             * empty access path.
             */

            // force an empty access path for this predicate.
            pred = (Predicate<?>) pred.setUnboundProperty(
                    IPredicate.Annotations.ACCESS_PATH_EXPANDER,
                    EmptyAccessPathExpander.INSTANCE);

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])), queryHints);
            return newJoin(left, anns, queryHints);

        }

        if (summary.nknown == 1) {

            /*
             * The dataset contains exactly one graph. Bind C.
             *
             * Note: This uses the 2 argument Constant constructor, which
             * accepts the name of the variable bound to the constant as its
             * first argument. BOpContext#bind() takes care of propagating the
             * binding onto the variable for solutions which join.
             *
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/359
             *
             * Fixed by changing to the two-arg constructor for Constant.
             */

            pred = pred.asBound((IVariable<?>) pred.get(3),
                    new Constant<IV<?, ?>>((IVariable) pred.get(3),
                            summary.firstContext));

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])), queryHints);
            return newJoin(left, anns, queryHints);

        }

        /*
         * Estimate cost of SCAN with C unbound.
         *
         * Note: We need to use global index view in order to estimate the cost
         * of the scan even though the scan will be shard-wise when we actually
         * run the query.
         *
         * @todo must pass estimateCost() to the underlying access path plus
         * layer on any cost for the optional expander.
         */
        @SuppressWarnings("rawtypes")
        final IRelation r = context.getRelation(pred);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final ScanCostReport scanCostReport = ((AccessPath) context
                .getAccessPath(r, (Predicate<?>) pred.setProperty(
                        IPredicate.Annotations.REMOTE_ACCESS_PATH, true)))
                .estimateCost();

        anns.add(new NV(Annotations.COST_SCAN, scanCostReport));

        /*
         * Estimate cost of SUBQUERY with C bound (sampling).
         *
         * Note: Again, we need to use a remote index view in order to estimate
         * the cost of the subqueries even though we will use sharded joins when
         * actually running the query.
         */
        final SubqueryCostReport subqueryCostReport = summary
                .estimateSubqueryCost(context, SAMPLE_LIMIT, (Predicate<?>) pred.setProperty(
                        IPredicate.Annotations.REMOTE_ACCESS_PATH, true));

        anns.add(new NV(Annotations.COST_SUBQUERY, subqueryCostReport));

        if (scanCostReport.cost < subqueryCostReport.cost) {

            /*
             * Scan and filter. C is left unbound. We do a range scan on the
             * index and filter using an IN constraint.
             */

            // IN filter for the named graphs.
            final IElementFilter<ISPO> test = new InGraphHashSetFilter<ISPO>(
                    summary.nknown, summary.graphs);

            // layer filter onto the predicate.
            pred = pred.addIndexLocalFilter(ElementFilter.newInstance(test));

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])), queryHints);
            return newJoin(left, anns, queryHints);

        } else {

            /*
             * Parallel Subquery.
             */

            /*
             * Setup the data set join.
             *
             * @todo When the #of named graphs is large we need to do something
             * special to avoid sending huge graph sets around with the query.
             * For example, we should create named data sets and join against
             * them rather than having an in-memory DataSetJoin.
             *
             * @todo The historical approach performed parallel subquery using
             * an expander pattern rather than a data set join. The data set
             * join should have very much the same effect, but it may need to
             * emit multiple chunks to have good parallelism.
             */

            // The variable to be bound.
            final IVariable<?> var = (IVariable<?>) pred.get(3);

            // The data set join.
            final DataSetJoin dataSetJoin = new DataSetJoin(leftOrEmpty(left),
                    NV.asMap(new NV[] {//
                                    new NV(DataSetJoin.Annotations.VAR, var),//
                                    new NV(DataSetJoin.Annotations.BOP_ID,
                                            idFactory.incrementAndGet()),//
                                    new NV(DataSetJoin.Annotations.GRAPHS,
                                            summary.getGraphs()) //
                            }));

//            if (scaleOut) {
//                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
//                        BOpEvaluationContext.SHARDED));
//                pred = (Predicate) pred.setProperty(
//                        Predicate.Annotations.REMOTE_ACCESS_PATH, false);
//            } else {
//                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
//                        BOpEvaluationContext.ANY));
//            }

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(dataSetJoin),
//                    anns.toArray(new NV[anns.size()])), queryHints);
            return newJoin(dataSetJoin, anns, queryHints);

        }

    }

    /**
     * Generate a default graph join (quads mode).
     *
     * @param queryEngine
     * @param left
     * @param anns
     * @param pred
     * @return
     *
     * @todo Since we do not know the specific asBound values, but only that
     *       they will be bound, we should defer the SCAN versus SUBQUERY
     *       decision until we actually evaluate that access path. This is
     *       basically a special case of runtime query optimization.
     */
    @SuppressWarnings("rawtypes")
    private static PipelineOp defaultGraphJoin(final QueryEngine queryEngine,
            final BOpContextBase context, final AtomicInteger idFactory,
            final PipelineOp left, final List<NV> anns, Predicate<?> pred,
            final DatasetNode dataset, final Properties queryHints) {

        final DataSetSummary summary = dataset == null ? null
                : dataset.getDefaultGraphs();

        final boolean scaleOut = queryEngine.isScaleOut();

        if (dataset != null && summary == null) {
            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());
            // Filter for distinct SPOs.
            if (true) { // summary.nknown < 10 * Bytes.kilobyte32) {
                /*
                 * JVM Based DISTINCT filter.
                 */
                pred = pred.addAccessPathFilter(DistinctFilter.newInstance());
            } else {
                /*
                 * FIXME The HTree variant of the distinct filter is more
                 * scalable but it does not have access to the memory manager
                 * for the IRunningQuery and hence will allocate its own memory
                 * manager, which means that it will use at a minimum of 1M of
                 * native memory. We need to look at the integration of the
                 * filters with the IRunningQuery to fix this.
                 * 
                 * TODO The HTree benefits very much from vectoring. However, in
                 * order to vector the filter we need to be operating on IV
                 * chunks. Maybe we can resolve this issue and the issue of
                 * access to the memory manager at the same time?
                 */
                pred = pred.addAccessPathFilter(HTreeDistinctFilter.newInstance());
            }

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])), queryHints);
            return newJoin(left, anns, queryHints);
        }

        if (summary != null && summary.nknown == 0) {

            /*
             * The data set is empty (no graphs). Return a join backed by an
             * empty access path.
             */

            // force an empty access path for this predicate.
            pred = (Predicate<?>) pred.setUnboundProperty(
                    IPredicate.Annotations.ACCESS_PATH_EXPANDER,
                    EmptyAccessPathExpander.INSTANCE);

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])), queryHints);
            return newJoin(left, anns, queryHints);

        }

        if (summary != null && summary.nknown == 1) {

            /*
             * The dataset contains exactly one graph. Bind C. Add a filter to
             * strip off the context position.
             */

            // Bind C.
            pred = pred.asBound((IVariable<?>) pred.get(3),
                    new Constant<IV<?, ?>>(summary.firstContext));

            if (scaleOut && !forceRemoteAPs) {
                // use a partitioned join.
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.SHARDED));
                pred = (Predicate) pred.setProperty(
                        Predicate.Annotations.REMOTE_ACCESS_PATH, false);
            }

            // Strip of the context position.
            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])), queryHints);
            return newJoin(left, anns, queryHints);

        }

        /*
         * @todo This optimization can only be applied at runtime. It can not be
         * decided statically because the actual index used may change as
         * variable bindings propagate [it could be decided statically if we
         * examined the predicate as it would be evaluated by propagating fake
         * variable bindings except when some joins are optional in which case
         * the actual index can not be known until runtime.]
         */
//        if (pred.getKeyOrder().getIndexName().endsWith("C")) {
//
//            /*
//             * C is not bound. An advancer is imposed on the AP to skip to the
//             * next possible triple after each match. Impose filter on AP to
//             * strip off the context position. Distinct filter is not required
//             * since the advancer pattern used will not report duplicates.
//             */
//
//            // Set the CURSOR flag.
//            pred = (Predicate<?>) pred.setProperty(IPredicate.Annotations.FLAGS,
//                    pred.getProperty(IPredicate.Annotations.FLAGS,
//                            IPredicate.Annotations.DEFAULT_FLAGS)
//                            | IRangeQuery.CURSOR); // @todo also READONLY
//
//            // Set Advancer (runs at the index).
//            pred = pred.addIndexLocalFilter(new ContextAdvancer());
//
//            // Filter to strip off the context position.
//            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());
//
//            if(scaleOut) {
//
//                /*
//                 * When true, an ISimpleSplitHandler guarantees that no triple
//                 * on that index spans more than one shard.
//                 */
//                final SPORelation r = (SPORelation)context.getRelation(pred);
//                final boolean shardTripleConstraint = r.getContainer().isConstrainXXXCShards();
//
//                if (shardTripleConstraint) {
//
//                    // JOIN is SHARDED.
//                    anns.add(new NV(
//                            BOp.Annotations.EVALUATION_CONTEXT,
//                            BOpEvaluationContext.SHARDED));
//
//                    // AP is LOCAL.
//                    pred = (Predicate<?>) pred.setProperty(
//                            IPredicate.Annotations.REMOTE_ACCESS_PATH, false);
//
//                } else {
//
//                    // JOIN is ANY.
//                    anns.add(new NV(
//                            BOp.Annotations.EVALUATION_CONTEXT,
//                            BOpEvaluationContext.ANY));
//
//                    // AP is REMOTE.
//                    pred = (Predicate<?>) pred.setProperty(
//                            IPredicate.Annotations.REMOTE_ACCESS_PATH, true);
//
//                }
//
//            }
//
//            return applyQueryHints(new PipelineJoin(new BOp[] { left, pred }, anns
//                    .toArray(new NV[anns.size()])),queryHints);
//
//        }

        /*
         * Estimate cost of SCAN with C unbound.
         * 
         * Note: We need to use the global index view in order to estimate the
         * cost of the scan regardless of whether the query runs with
         * partitioned or global index views when it is evaluated.
         * 
         * FIXME We need a higher threshold (and a cheaper test) to decide when
         * we should SCAN+FILTER. For a journal, the #of elements in the filter
         * needs to be probably 25% of the named graphs, which is probably too
         * much data to have in memory anyway.
         */
        final IRelation r = context.getRelation(pred);
        @SuppressWarnings("unchecked")
        final ScanCostReport scanCostReport = ((AccessPath) context
                .getAccessPath(r, (Predicate<?>) pred.setProperty(
                        IPredicate.Annotations.REMOTE_ACCESS_PATH, true)))
                .estimateCost();
        anns.add(new NV(Annotations.COST_SCAN, scanCostReport));

        /*
         * Estimate cost of SUBQUERY with C bound (sampling).
         *
         * Note: We need to use the global index view in order to estimate the
         * cost of the scan regardless of whether the query runs with
         * partitioned or global index views when it is evaluated.
         */
        final SubqueryCostReport subqueryCostReport = dataset == null ? null
                : summary.estimateSubqueryCost(context, SAMPLE_LIMIT, (Predicate<?>) pred.setProperty(
                        IPredicate.Annotations.REMOTE_ACCESS_PATH, true));

        anns.add(new NV(Annotations.COST_SUBQUERY, subqueryCostReport));

        if (subqueryCostReport == null
                || scanCostReport.cost < subqueryCostReport.cost) {

            /*
             * SCAN AND FILTER. C is not bound. Unless all graphs are used,
             * layer IN filter on the AP to select for the desired graphs. Layer
             * a filter on the AP to strip off the context position. Layer a
             * DISTINCT filter on top of that.
             */

            if (dataset != null) {

                // IN filter for the named graphs.
                final IElementFilter<ISPO> test = new InGraphHashSetFilter<ISPO>(
                        summary.nknown, summary.graphs);

                // layer filter onto the predicate.
                pred = pred
                        .addIndexLocalFilter(ElementFilter.newInstance(test));

            }

            // Filter to strip off the context position.
            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());

            // Filter for distinct SPOs.
            pred = pred.addAccessPathFilter(DistinctFilter.newInstance());

            if (scaleOut) {
                /*
                 * Use the global index view so we can impose the distinct
                 * filter.
                 */
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.ANY));
                pred = (Predicate) pred.setProperty(
                        Predicate.Annotations.REMOTE_ACCESS_PATH, true);
            } else {
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.ANY));
            }

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])),queryHints);
            return newJoin(left, anns, queryHints);

        } else {

            /*
             * PARALLEL SUBQUERY. Bind each value of C in turn, issuing parallel
             * subqueries against the asBound access paths using an expander
             * pattern and layer on a filter to strip off the context position.
             * The asBound access paths write on a shared buffer. That shared
             * buffer is read from by the expander.
             *
             * Scale-out: JOIN is ANY or HASHED. AP is REMOTE.
             * 
             * FIXME This is still using an expander pattern. Rewrite it to use
             * a join against the default contexts in the data set.
             */

            final long estimatedRangeCount = subqueryCostReport.rangeCount;

            final Set<IV> graphs = summary.getGraphs();

            // @todo default with query hint to override and relate to ClientIndexView limit in scale-out.
            final int maxParallel = 10;

            // Set subquery expander.
            pred = (Predicate<?>) pred.setUnboundProperty(
                    IPredicate.Annotations.ACCESS_PATH_EXPANDER,
                    new DGExpander(maxParallel, graphs, estimatedRangeCount));

            // Filter to strip off the context position.
            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());

            // Filter for distinct SPOs.
            pred = pred.addAccessPathFilter(DistinctFilter.newInstance());

            if (scaleOut) {
                /*
                 * Use the global index view so we can impose the distinct
                 * filter.
                 */
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.ANY));
                pred = (Predicate) pred.setProperty(
                        Predicate.Annotations.REMOTE_ACCESS_PATH, true);
            } else {
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.ANY));
            }

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

//            return applyQueryHints(new PipelineJoin(leftOrEmpty(left), anns
//                    .toArray(new NV[anns.size()])),queryHints);            
            return newJoin(left, anns, queryHints);
            
        }

    }

    /**
     * Create and return an appropriate type of join. The default is the
     * pipeline join. A hash join can be selected using the appropriate query
     * hint. The query hints which control this decision must be annotated on
     * the {@link IPredicate} by the caller.
     * 
     * @param left
     * @param anns
     * @param queryHints
     * @return
     * 
     * @see Annotations#HASH_JOIN
     * @see HashJoinAnnotations#JOIN_VARS
     * @see Annotations#ESTIMATED_CARDINALITY
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static private PipelineOp newJoin(PipelineOp left, final List<NV> anns,
            final Properties queryHints) {

        final Map<String, Object> map = NV.asMap(anns.toArray(new NV[anns
                .size()]));

        // Look up the predicate for the access path.
        final IPredicate<?> pred = (IPredicate<?>) map
                .get(AccessPathJoinAnnotations.PREDICATE);

        // MAX_LONG unless we are doing cutoff join evaluation.
        final long limit = pred.getProperty(JoinAnnotations.LIMIT,
                JoinAnnotations.DEFAULT_LIMIT);

        // True iff a hash join was requested for this predicate.
        final boolean hashJoin = limit == JoinAnnotations.DEFAULT_LIMIT
                && pred.getProperty(Annotations.HASH_JOIN,
                        Annotations.DEFAULT_HASH_JOIN);
        
        if (hashJoin) {

            /*
             * TODO Choose HTree versus JVM hash join operator based on the the
             * estimated input cardinality to the join.
             * 
             * TODO If we partition the hash join on a cluster then we should
             * divide the estimated input cardinality by the #of partitions to
             * get the estimated input cardinality per partition.
             */
            
//            final long fastRangeCount = (Long) pred
//                    .getRequiredProperty(Annotations.ESTIMATED_CARDINALITY);
            
            final long estimatedInputCardinality = Long.MAX_VALUE; // FIXME From the RTO. 
            
            final boolean useHTree = estimatedInputCardinality > 20 * Bytes.megabyte;

            /*
             * The join variable(s) are variables which are (a) bound by the
             * predicate and (b) are known bound in the source solutions.
             */
            final IVariable<?>[] joinVars = (IVariable<?>[]) pred
                    .getRequiredProperty(HashJoinAnnotations.JOIN_VARS);

            map.put(HashJoinAnnotations.JOIN_VARS, joinVars);
            
            /*
             * Choose the evaluation context.
             * 
             * Note: On a cluster this MUST be consistent with the decision made
             * for handling named and default graphs, except that we are free to
             * choose either SHARDED or HASHED for a hash join. Also, while the
             * pipeline join can use ANY on a cluster, the hash joins MUST run
             * on the controller or be either sharded or hash partitioned.
             */
            BOpEvaluationContext evaluationContext = (BOpEvaluationContext) map
                    .get(BOp.Annotations.EVALUATION_CONTEXT);

            if (evaluationContext == null) {
                // TODO Should be SHARDED or HASHED on a cluster.
                evaluationContext = BOpEvaluationContext.CONTROLLER;
            } else if(evaluationContext == BOpEvaluationContext.ANY) {
                // ANY is not permitted for a hash join.
                evaluationContext = BOpEvaluationContext.CONTROLLER;
            }
            map.put(BOp.Annotations.EVALUATION_CONTEXT, evaluationContext);

            map.put(PipelineOp.Annotations.MAX_PARALLEL, 1);

            if (useHTree) {

                map.put(PipelineOp.Annotations.MAX_MEMORY, Long.MAX_VALUE);
                
                map.put(PipelineOp.Annotations.LAST_PASS, true);

                left = new HTreeHashJoinOp(leftOrEmpty(left), map);
                
            } else {
                
                map.put(PipelineOp.Annotations.PIPELINED, false);
                
                left = new JVMHashJoinOp(leftOrEmpty(left), map);
                
            }

        } else {

            left = new PipelineJoin(leftOrEmpty(left), map);

        }

        left = applyQueryHints(left, queryHints);

        return left;

    }

}
