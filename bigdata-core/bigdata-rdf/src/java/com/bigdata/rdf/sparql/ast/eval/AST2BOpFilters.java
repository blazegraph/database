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
 * Created on Oct 31, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.join.ChunkedMaterializationOp;
import com.bigdata.bop.rdf.join.InlineMaterializeOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
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
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;

/**
 * Class handles the materialization pattern for filters by adding a series of
 * materialization steps to materialize terms needed downstream.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AST2BOpFilters extends AST2BOpBase {

    private static final Logger log = Logger.getLogger(AST2BOpFilters.class);

    /**
     * 
     */
    protected AST2BOpFilters() {
    }

    /*
     * Note: There was only one caller left for this method and it was from
     * within this same class, so I rewrote the calling context very slightly
     * and removed this method. The documentation on the general approach has
     * been moved to the delegate method (immediate below). bbt.
     */
//    /**
//     * Adds a series of materialization steps to materialize terms needed
//     * downstream.
//     * 
//     * To materialize the variable ?term, the pipeline looks as follows:
//     * 
//     * <pre>
//     * left
//     * ->
//     * ConditionalRoutingOp1 (condition=!IsMaterialized(?term), alt=right)
//     * ->
//     * ConditionalRoutingOp2 (condition=IsInline(?term), alt=PipelineJoin)
//     * ->
//     * InlineMaterializeOp (predicate=LexPredicate(?term), sink=right)
//     * ->
//     * PipelineJoin (predicate=LexPredicate(?term))
//     * ->
//     * right
//     * </pre>
//     * 
//     * @param context
//     * @param left
//     *            the left (upstream) operator that immediately proceeds the
//     *            materialization steps
//     * @param right
//     *            the right (downstream) operator that immediately follows the
//     *            materialization steps
//     * @param c
//     *            the constraint to run on the IsMaterialized op to see if the
//     *            materialization pipeline can be bypassed (bypass if true and
//     *            no {@link NotMaterializedException} is thrown).
//     * @param varsToMaterialize
//     *            the terms to materialize
//     * @param queryHints
//     *            the query hints
//     * @return the final bop added to the pipeline by this method
//     * 
//     * @see AST2BOpUtility#addMaterializationSteps(PipelineOp, int,
//     *      IValueExpression, Collection, AST2BOpContext)
//     */
//    @SuppressWarnings({ "rawtypes", "unchecked" })
//    protected static PipelineOp addMaterializationSteps(
//            final AST2BOpContext context,//
//            PipelineOp left, //
//            final int right,//
//            final IConstraint c,//
//            final Collection<IVariable<IV>> varsToMaterialize,//
////            final AtomicInteger idFactory, 
//            final Properties queryHints) {
//
////        final AST2BOpContext context = new AST2BOpContext(
////                null/* astContainer */, idFactory, db, queryEngine, queryHints);
//
//        final IValueExpression<IV> ve = (IValueExpression) c.get(0);
//
//        return addMaterializationSteps(left, right, ve, varsToMaterialize,
//                context);
//
//    }
    
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
     * To materialize the variable <code>?term</code>, the pipeline looks as
     * follows:
     * 
     * <pre>
     * left
     * ->
     * ConditionalRoutingOp1 (condition=!IsMaterialized(?term), alt=rightId)
     * ->
     * ConditionalRoutingOp2 (condition=IsInline(?term), alt=PipelineJoin)
     * ->
     * InlineMaterializeOp (predicate=LexPredicate(?term), sink=rightId)
     * ->
     * PipelineJoin (predicate=LexPredicate(?term))
     * ->
     * rightId
     * </pre>
     * 
     * @param left
     *            The left (upstream) operator that immediately proceeds the
     *            materialization steps.
     * @param rightId
     *            The id for the right (downstream) operator that immediately
     *            follows the materialization steps. This needs to be
     *            pre-reserved by the caller.
     * @param ve
     *            The {@link IValueExpression} for the {@link SPARQLConstraint}.
     * @param varsToMaterialize
     *            The variables to be materialize.
     * @param queryHints
     *            The query hints to be applied from the dominating operator
     *            context.
     * @param ctx
     *            The evaluation context.
     * 
     * @return The final bop added to the pipeline by this method
     * 
     * @see TryBeforeMaterializationConstraint
     * @see AST2BOpUtility#addMaterializationSteps1(PipelineOp, int,
     *      IValueExpression, Collection, AST2BOpContext)
     */
    @SuppressWarnings("rawtypes")
    protected static PipelineOp addMaterializationSteps1(//
            PipelineOp left,//
            final int rightId, //
            final IValueExpression<IV> ve,//
            final Set<IVariable<IV>> vars, //
            final Properties queryHints,
            final AST2BOpContext ctx) {

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
                ), queryHints, ctx);

        return addMaterializationSteps2(left, rightId, vars, queryHints, ctx);

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
     * ConditionalRoutingOp1 (condition=!IsMaterialized(?term), alt=rightId)
     * ->
     * ConditionalRoutingOp2 (condition=IsInline(?term), alt=PipelineJoin)
     * ->
     * InlineMaterializeOp (predicate=LexPredicate(?term), sink=rightId)
     * ->
     * PipelineJoin (predicate=LexPredicate(?term))
     * ->
     * rightId
     * </pre>
     * 
     * @param left
     *            The left (upstream) operator that immediately proceeds the
     *            materialization steps.
     * @param rightId
     *            The id of the right (downstream) operator that immediately
     *            follows the materialization steps.
     * @param vars
     *            The terms to materialize
     * @param queryHints
     *            The query hints from the dominating AST node.
     * @param ctx
     *            The evaluation context.
     * 
     * @return The final bop added to the pipeline by this method. If there are
     *         no variables that require materialization, then this just returns
     *         <i>left</i>.
     * 
     * @see TryBeforeMaterializationConstraint
     */
    @SuppressWarnings("rawtypes")
    protected static PipelineOp addMaterializationSteps2(//
            PipelineOp left,//
            final int rightId, //
            final Set<IVariable<IV>> vars,//
            final Properties queryHints, //
            final AST2BOpContext ctx) {

        final int nvars = vars.size();

        if (nvars == 0)
            return left;

        if (nvars >= 1) {

            /*
             * Materializes multiple variables at once.
             * 
             * Note: This code path does not reorder the solutions (no
             * conditional routing).
             */

            return addChunkedMaterializationStep(
                    left,
                    vars,
                    ChunkedMaterializationOp.Annotations.DEFAULT_MATERIALIZE_INLINE_IVS,
                    null, // cutoffLimit
                    queryHints, ctx);

        }

        /*
         * Materialize a single variable.
         * 
         * Note: This code path can reorder the solutions (it uses conditional
         * routing).
         * 
         * TODO We should drop the more complicated materialization pipeline
         * logic unless a performance advantage can be demonstrated either on a
         * Journal or a cluster.
         */
        final long timestamp = ctx.getLexiconReadTimestamp();

        final String ns = ctx.getLexiconNamespace();

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
                    ), queryHints, ctx);

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
                    ), queryHints, ctx);

            if (log.isDebugEnabled()) {
                log.debug("adding 2nd conditional routing op: " + condOp2);
            }

            final Predicate lexPred;
            {

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
                    ), queryHints, ctx);

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
                
                if (ctx.isCluster() && !ctx.remoteAPs) {
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
                        queryHints, ctx);
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
     * Use a pipeline operator which uses the chunked materialization pattern
     * for solution sets. This is similar to the pattern which is used when the
     * IVs in the final solutions are materialized as RDF Values.
     * <p>
     * Note: The RTO uses this method since it does not use conditional routing
     * and does not reorder the solutions.
     * 
     * @param left
     *            The left (upstream) operator that immediately proceeds the
     *            materialization steps.
     * @param vars
     *            The terms to materialize.
     * @param materializeInlineIvs
     *            When <code>true</code>, inline IVs are also materialized.
     * @param queryHints
     *            The query hints from the dominating AST node.
     * @param ctx
     *            The evaluation context.
     * 
     * @return The final bop added to the pipeline by this method. If there are
     *         no variables that require materialization, then this just returns
     *         <i>left</i>.
     * 
     * @see ChunkedMaterializationOp
     */
    protected static PipelineOp addChunkedMaterializationStep(//
            PipelineOp left,//
            final Set<IVariable<IV>> vars,//
            final boolean materializeInlineIvs,//
            final Long cutoffLimit,//
            final Properties queryHints,//
            final AST2BOpContext ctx//
            ) {

        final int nvars = vars.size();

        if (nvars == 0)
            return left;

        final long timestamp = ctx.getLexiconReadTimestamp();

        final String ns = ctx.getLexiconNamespace();

        /*
         * If we are doing cutoff join evaluation, then limit the parallelism of
         * this operator to prevent reordering of solutions.
         * 
         * TODO RTO: If query hints are allowed to override MAX_PARALLEL and
         * this is being invoked for cutoff join evaluation, then that will
         * break the "no reordering" guarantee.
         */

        // disable reordering of solutions for cutoff joins.
        final boolean reorderSolutions = (cutoffLimit != null) ? false
                : PipelineJoin.Annotations.DEFAULT_REORDER_SOLUTIONS;

        // disable operator parallelism for cutoff joins.
        final int maxParallel = cutoffLimit != null ? 1
                : PipelineOp.Annotations.DEFAULT_MAX_PARALLEL;

        return (PipelineOp) applyQueryHints(new ChunkedMaterializationOp(leftOrEmpty(left),
            new NV(ChunkedMaterializationOp.Annotations.VARS, vars.toArray(new IVariable[nvars])),//
            new NV(ChunkedMaterializationOp.Annotations.RELATION_NAME, new String[] { ns }), //
            new NV(ChunkedMaterializationOp.Annotations.TIMESTAMP, timestamp), //
            new NV(ChunkedMaterializationOp.Annotations.MATERIALIZE_INLINE_IVS, materializeInlineIvs), //
            new NV(PipelineOp.Annotations.SHARED_STATE, !ctx.isCluster()),// live stats, but not on the cluster.
            new NV(PipelineOp.Annotations.REORDER_SOLUTIONS,reorderSolutions),//
            new NV(PipelineOp.Annotations.MAX_PARALLEL,maxParallel),//
            new NV(BOp.Annotations.BOP_ID, ctx.nextId())//
            ), queryHints, ctx);
        
    }

//    /**
//     * Wrapper for handling the {@link AST2BOpContext} / {@link BOpContextBase}
//     * API mismatch.
//     * 
//     * @param left
//     * @param doneSet
//     * @param needsMaterialization
//     * @param queryHints
//     * @param ctx
//     */
//    protected static PipelineOp addMaterializationSteps(PipelineOp left,
//            final Set<IVariable<?>> doneSet,
//            final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization,
//            final Properties queryHints,
//            final AST2BOpContext ctx) {
//
//        return addMaterializationSteps(left, doneSet, needsMaterialization, ctx.db,
//                ctx.queryEngine, ctx.idFactory, new BOpContextBase(
//                        ctx.queryEngine), queryHints);
//    
//    }

    /**
     * For each filter which requires materialization steps, add the
     * materializations steps to the pipeline and then add the filter to the
     * pipeline.
     * 
     * @param left
     * @param doneSet
     *            The set of variables already known to be materialized. This is
     *            populated as a side-effect with any variables that will be
     *            materialized by the materialization steps added by this
     *            method.
     * @param needsMaterialization
     *            A map of constraints and their variable materialization
     *            requirements.
     * @param queryHints
     *            Query hints from the dominating AST node.
     * @param ctx
     *            The evaluation context.
     * 
     *            TODO This treats each filter in turn rather than handling all
     *            variable materializations for all filters at once. Is this
     *            deliberate? If so, maybe we should pay attention to the order
     *            of the filters. I.e., those constraints should be ordered
     *            based on an expectation that they can reduce the total work by
     *            first eliminating solutions with less materialization effort
     *            (run constraints without materialization requirements before
     *            those with materialization requirements, run constraints that
     *            are more selective before others, etc.).
     */
    @SuppressWarnings("rawtypes")
    protected static PipelineOp addMaterializationSteps3(//
            PipelineOp left,//
            final Set<IVariable<?>> doneSet,//
            final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization,
            final Properties queryHints,//
            final AST2BOpContext ctx//
            ) {

        if (needsMaterialization.isEmpty()) {
            // Nothing to do.
            return left;
        }

        final Set<IVariable<?>> alreadyMaterialized = doneSet;

        for (Map.Entry<IConstraint, Set<IVariable<IV>>> e :
            needsMaterialization.entrySet()) {

            // The constraint.
            final IConstraint c = e.getKey();

            // The set of variables associated with that constraint.
            final Set<IVariable<IV>> terms = e.getValue();

            // remove any terms already materialized
            terms.removeAll(alreadyMaterialized);

            if (c instanceof INeedsMaterialization
                    && ((INeedsMaterialization) c).getRequirement() == Requirement.ALWAYS) {

                // add any new terms to the list of already materialized
                alreadyMaterialized.addAll(terms);
                
            }

            final int condId = ctx.nextId();

            // we might have already materialized everything we need
            if (!terms.isEmpty()) {

                // Add materialization steps for remaining variables.

                @SuppressWarnings("unchecked")
                final IValueExpression<IV> ve = (IValueExpression) c.get(0);

                left = addMaterializationSteps1(//
                        left, //
                        condId, // right
                        ve, // value expression
                        terms,// varsToMaterialize,
                        queryHints,//
                        ctx);
                
            }

            left = applyQueryHints(//
                new ConditionalRoutingOp(leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, condId),//
                    new NV(ConditionalRoutingOp.Annotations.CONDITION,c)//
                ), queryHints, ctx);

        }

        return left;

    }

    /**
     * The RTO requires that we do not reorder solutions. This means that it
     * must use an un-conditional approach to variable materialization for
     * constraints with SOMETIMES materialization requirements. This has two
     * practical impacts:
     * <p>
     * 1. We can not attach a filter with SOMETIMES requirements to a JOIN and
     * wrap it with a {@link TryBeforeMaterializationConstraint} since this
     * requires a {@link ConditionalRoutingOp} with an altSink and that will
     * reorder solutions.
     * <p>
     * 2. We can not use a pattern which involves an {@link InlineMaterializeOp}
     * followed by a {@link ConditionalRoutingOp} with an altSink followed by a
     * {@link PipelineJoin} against the lexicon. This also reorders the
     * solutions (primarily because of the {@link ConditionalRoutingOp} since we
     * can force the {@link PipelineJoin} to not reorder solutions).
     * <p>
     * The code below uses the {@link ChunkedMaterializationOp}. This does not
     * reorder the solutions. It can also materialize inline IVs giving us a
     * single operator that prepare the solutions for filter evaluation.
     */
    @SuppressWarnings("rawtypes")
    protected static PipelineOp addNonConditionalMaterializationSteps(//
            PipelineOp left,//
            final Set<IVariable<?>> doneSet,//
            final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization,
            final Long cutoffLimit,//
            final Properties queryHints,//
            final AST2BOpContext ctx//
            ) {
        
        if (needsMaterialization.isEmpty()) {
         
            // No filters.
            return left;
            
        }

        // Collect variables that require materialization.
        final Set<IVariable<IV>> matvars = new LinkedHashSet<IVariable<IV>>();

        for (Map.Entry<IConstraint, Set<IVariable<IV>>> e : needsMaterialization
                .entrySet()) {

            matvars.addAll(e.getValue());

        }

        if (!matvars.isEmpty()) {

            // Materialize those variables.
            left = addChunkedMaterializationStep(left, matvars,
                    true/* materializeInlineIVs */, cutoffLimit, queryHints,
                    ctx);

            // Add them to the doneSet.
            doneSet.addAll(matvars);

        }

        // Attach all constraints.
        for(IConstraint c : needsMaterialization.keySet()) {
        
            /*
             * Note: While this is using a ConditionalRoutingOp, it is NOT
             * use the altSink. All solutions flow through the default sink.
             * This use does not cause the solutions to be reordered.
             * Parallel evaluation is also disabled.
             */

            left = applyQueryHints(//
                new ConditionalRoutingOp(leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    // disallow reordering of solutions by the query engine.
                    new NV(PipelineJoin.Annotations.REORDER_SOLUTIONS, Boolean.FALSE),//
                    new NV(ConditionalRoutingOp.Annotations.CONDITION, c)//
                ), queryHints, ctx);
        
        }
        
        return left;

    }
    
    /**
     * Partition the constraints for a join into those which can (or might) be
     * able to run attached to that join and those which must (or might) need to
     * materialize some variables before they can be evaluated. Constraints
     * which might be able to run attached to a join actually wind up both
     * attached to the join (in the return value) where they are wrapped by a
     * {@link TryBeforeMaterializationConstraint} which will ignore errors
     * caused by unmaterialized values and in the <i>needsMaterialization</i>
     * map. This allows such constraints to run attached to the join iff that is
     * possible and otherwise to be evaluated as soon as the materialization
     * pipeline can satisify their materialization requirements.
     * 
     * @param constraints
     *            The constraints (if any) for the join (optional). This
     *            collection is NOT modified.
     * @param needsMaterialization
     *            A map providing for each constraint the set of variables which
     *            must be materialized before that constraint can be evaluated.
     *            This map is populated as a side-effect. It will be empty iff
     *            there are no constraints that might or must require variable
     *            materialization.
     * 
     * @return Constraints which can (or might) be able to run attached to that
     *         join -or- <code>null</code> iff there are no constraints that can
     *         be attached to the join.
     */
    @SuppressWarnings("rawtypes")
    static protected IConstraint[] getJoinConstraints(
            final Collection<IConstraint> constraints,
            final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization) {
        
        return getJoinConstraints2(constraints, needsMaterialization, true/* conditionalRouting */);

    }

    /**
     * Partition the constraints for a join into those which can (or might) be
     * able to run attached to that join and those which must (or might) need to
     * materialize some variables before they can be evaluated. Constraints
     * which might be able to run attached to a join actually wind up both
     * attached to the join (in the return value) where they are wrapped by a
     * {@link TryBeforeMaterializationConstraint} which will ignore errors
     * caused by unmaterialized values and in the <i>needsMaterialization</i>
     * map. This allows such constraints to run attached to the join iff that is
     * possible and otherwise to be evaluated as soon as the materialization
     * pipeline can satisify their materialization requirements.
     * <p>
     * Note: The RTO requires that solutions are not reordered during cutoff
     * JOIN evaluation in order to obtain accurate estimates of the cardinality
     * of a join based on a known number of solutions in producing a known
     * number of solutions out. Conditional materialization can cause solutions
     * to be reordered since some solutions may pass the constraint attached to
     * the join and be routed along one path in the query plan while other
     * solutions may pass the join but fail the constraint due to a
     * materialization requirement and are routed along another path. Thus it is
     * necessary to disable conditional routing for cutoff JOIN evaluation.
     * 
     * @param constraints
     *            The constraints (if any) for the join (optional). This
     *            collection is NOT modified.
     * @param needsMaterialization
     *            A map providing for each constraint the set of variables which
     *            either might or must be materialized before that constraint
     *            can be evaluated. This map is populated as a side-effect. It
     *            will be empty iff there are no constraints that might or must
     *            require variable materialization.
     * @param conditionalRouting
     *            When <code>true</code>, constraints that
     *            {@link Requirement#SOMETIMES} are able to run attached are
     *            wrapped by a {@link TryBeforeMaterializationConstraint} and
     *            appear both attached to the JOIN and will also run after the
     *            JOIN using a {@link ConditionalRoutingOp} to route solutions
     *            that fail the attached contraint through a materialization
     *            pipeline and then into a 2nd copy of the constraint once the
     *            variable(s) are known to be materialized.<br/>
     *            When <code>false</code>, only constraints that
     *            {@link Requirement#NEVER} require materialization will be
     *            attached to the JOIN. All other constraints will use
     *            non-conditional materialization.
     * 
     * @return Constraints which can (or might) be able to run attached to that
     *         join -or- <code>null</code> iff there are no constraints that can
     *         be attached to the join.
     */
    @SuppressWarnings("rawtypes")
    static protected IConstraint[] getJoinConstraints2(
                final Collection<IConstraint> constraints,
                final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization,
                final boolean conditionalRouting) {
            
        if (constraints == null || constraints.isEmpty()) {

            // No constraints for this join.
            return null;
            
        }
        
        // create a mutable version
        final Collection<IConstraint> joinConstraints = new LinkedList<IConstraint>();

        joinConstraints.addAll(constraints);

        final Collection<IConstraint> tryBeforeMaterialization = new LinkedList<IConstraint>();

        final Iterator<IConstraint> it = joinConstraints.iterator();

        while (it.hasNext()) {

            final IConstraint c = it.next();

            /*
             * If this constraint needs materialized variables, remove it from
             * the join and run it as a ConditionalRoutingOp later.
             */

            final Set<IVariable<IV>> terms = new LinkedHashSet<IVariable<IV>>();

            final Requirement req = StaticAnalysis.gatherVarsToMaterialize(c,
                    terms);

            if (req != Requirement.NEVER) {

                it.remove();

                if (req == Requirement.SOMETIMES && conditionalRouting) {

                    tryBeforeMaterialization.add(c);

                }

                needsMaterialization.put(c, terms);

            }

        }

        for (IConstraint c : tryBeforeMaterialization) {

            // need to make a clone so that BOpUtility doesn't complain
            c = (IConstraint) c.clone();

            joinConstraints.add(new TryBeforeMaterializationConstraint(c));

        }

        return joinConstraints.toArray(new IConstraint[joinConstraints.size()]);

    }
    
    /**
     * Convert the attached join filters into a list of {@link IConstraint}s.
     * 
     * @param joinNode
     *            The {@link IJoinNode}.
     * 
     * @return The constraints -or- <code>null</code> if there are no attached
     *         join filters for that {@link IJoinNode}.
     */
    static protected List<IConstraint> getJoinConstraints(final IJoinNode joinNode) {

        final List<FilterNode> joinFilters = joinNode.getAttachedJoinFilters();

        if (joinFilters == null || joinFilters.isEmpty()) {

            return null;

        }

        final List<IConstraint> constraints = new LinkedList<IConstraint>();
        
        for (FilterNode filter : joinFilters) {
        
            constraints
                    .add(new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                            filter.getValueExpression()));
            
        }
        
        return constraints;

    }
    
}
