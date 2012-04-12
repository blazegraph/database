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
 * Class handles the materialization pattern for filters.
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
     * @param queryHints
     *            the query hints
     * @return the final bop added to the pipeline by this method
     * 
     * @see AST2BOpUtility#addMaterializationSteps(PipelineOp, int,
     *      IValueExpression, Collection, AST2BOpContext)
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static PipelineOp addMaterializationSteps(
            final AST2BOpContext context,
//            final AbstractTripleStore db,
//            final QueryEngine queryEngine, 
            PipelineOp left, final int right,
            final IConstraint c,
            final Collection<IVariable<IV>> varsToMaterialize,
//            final AtomicInteger idFactory, 
            final Properties queryHints) {

//        final AST2BOpContext context = new AST2BOpContext(
//                null/* astContainer */, idFactory, db, queryEngine, queryHints);

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
     * @see TryBeforeMaterializationConstraint
     */
    @SuppressWarnings("rawtypes")
    protected static PipelineOp addMaterializationSteps(PipelineOp left,
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

        final int nvars = vars.size();

        if (nvars == 0)
            return left;

        final long timestamp = ctx.getLexiconReadTimestamp();

        final String ns = ctx.getLexiconNamespace();

        if (nvars >= 1) {
            /*
             * Use a pipeline operator which uses the chunked materialization
             * pattern for solution sets. This is similar to the pattern which
             * is used when the IVs in the final solutions are materialized as
             * RDF Values.
             * 
             * TODO We should drop the more complicated materialization pipeline
             * logic unless a performance advantage can be demonstrated either
             * on a Journal or a cluster.
             */
            return (PipelineOp) new ChunkedMaterializationOp(leftOrEmpty(left),
                    new NV(ChunkedMaterializationOp.Annotations.VARS, vars.toArray(new IVariable[nvars])),//
                    new NV(ChunkedMaterializationOp.Annotations.RELATION_NAME, new String[] { ns }), //
                    new NV(ChunkedMaterializationOp.Annotations.TIMESTAMP, timestamp), //
                    new NV(PipelineOp.Annotations.SHARED_STATE, !ctx.isCluster()),// live stats, but not on the cluster.
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId())//
                    );
//                    vars.toArray(new IVariable[nvars]), ns, timestamp)
//                    .setProperty(BOp.Annotations.BOP_ID, ctx.nextId());
        }

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
     *            The set of variables already known to be materialized.
     * @param needsMaterialization
     *            A map of constraints and their variable materialization
     *            requirements.
     * @param context
     * @param queryHints
     */
    @SuppressWarnings("rawtypes")
    protected static PipelineOp addMaterializationSteps(
            final AST2BOpContext ctx,
            PipelineOp left,
            final Set<IVariable<?>> doneSet,
            final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization,
//            final AbstractTripleStore db,//
//            final QueryEngine queryEngine,//
//            final AtomicInteger idFactory,//
//            final BOpContextBase context,//
            final Properties queryHints) {

        if (!needsMaterialization.isEmpty()) {

            final Set<IVariable<?>> alreadyMaterialized = doneSet;

            for (Map.Entry<IConstraint, Set<IVariable<IV>>> e :
                needsMaterialization.entrySet()) {

                final IConstraint c = e.getKey();
                
                final Set<IVariable<IV>> terms = e.getValue();

                // remove any terms already materialized
                terms.removeAll(alreadyMaterialized);

                if (c instanceof INeedsMaterialization && ((INeedsMaterialization) c).getRequirement() == Requirement.ALWAYS) {
                	
	                // add any new terms to the list of already materialized
	                alreadyMaterialized.addAll(terms);
	                
                }

                final int condId = ctx.nextId();

                // we might have already materialized everything we need
                if (!terms.isEmpty()) {

                    // Add materialization steps for remaining variables.
                    left = addMaterializationSteps(
                            ctx,
//                            db, queryEngine, 
                            left,
                            condId, c, terms, 
                            // idFactory, 
                            queryHints
                            );

                }

                left = applyQueryHints(//
                    new ConditionalRoutingOp(leftOrEmpty(left),//
                        new NV(BOp.Annotations.BOP_ID, condId),//
                        new NV(ConditionalRoutingOp.Annotations.CONDITION,c)//
                    ), queryHints);

            }

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
     *            This map is populated as a side-effect.
     * 
     * @return Constraints which can (or might) be able to run attached to that
     *         join.
     */
    static protected IConstraint[] getJoinConstraints(
            final Collection<IConstraint> constraints,
            final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization) {
        
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

            @SuppressWarnings("rawtypes")
            final Set<IVariable<IV>> terms = new LinkedHashSet<IVariable<IV>>();

            final Requirement req = StaticAnalysis.gatherVarsToMaterialize(c,
                    terms);

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
