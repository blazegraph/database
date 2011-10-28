/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 5, 2010
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.bset.EndOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.controller.AbstractSubqueryOp;
import com.bigdata.bop.controller.Steps;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.joinGraph.IRangeCountFactory;
import com.bigdata.bop.joinGraph.PartitionedJoinGroup;
import com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlan2;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.striterator.IKeyOrder;

/**
 * Utility class converts {@link IRule}s to {@link BOp}s.
 * <p>
 * Note: This is a stopgap measure designed to allow us to evaluate SPARQL
 * queries and verify the standalone {@link QueryEngine} while we develop a
 * direct translation from Sesame's SPARQL operator tree onto {@link BOp}s and
 * work on the scale-out query buffer transfer mechanisms.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Rule2BOpUtility extends AST2BOpBase {

    protected static final Logger log = Logger.getLogger(Rule2BOpUtility.class);

//    private static final transient IConstraint[][] NO_ASSIGNED_CONSTRAINTS = new IConstraint[0][];

    /**
     * Convert an {@link IStep} into an operator tree. This should handle
     * {@link IRule}s and {@link IProgram}s as they are currently implemented
     * and used by the {@link BigdataSail}.
     *
     * @param step
     *            The step.
     *
     * @return
     */
    public static PipelineOp convert(final IStep step,
            final AtomicInteger idFactory, final AbstractTripleStore db,
            final QueryEngine queryEngine, final Properties queryHints) {

        if (step instanceof IRule<?>) {

            // Convert the step to a bigdata operator tree.
            PipelineOp tmp = convert((IRule<?>) step, idFactory, db,
                    queryEngine, queryHints);

            if (!tmp.getEvaluationContext().equals(
                    BOpEvaluationContext.CONTROLLER)) {
                /*
                 * Wrap with an operator which will be evaluated on the query
                 * controller so the results will be streamed back to the query
                 * controller in scale-out.
                 */
                tmp = new EndOp(leftOrEmpty(tmp), NV.asMap(//
                        new NV(BOp.Annotations.BOP_ID, idFactory
                                .incrementAndGet()), //
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER)//
//                        new NV(PipelineOp.Annotations.SHARED_STATE,true)//
                        ));

            }

            return applyQueryHints(tmp, queryHints);

        }

        return convert((IProgram) step, idFactory, db, queryEngine, queryHints);

    }

    /**
     * Convert a rule into an operator tree.
     *
     * @param rule
     *
     * @return
     */
    public static PipelineOp convert(final IRule<?> rule,
            final AtomicInteger idFactory, final AbstractTripleStore db,
            final QueryEngine queryEngine, final Properties queryHints) {

        // TODO StartOp is not required.
        final PipelineOp startOp = applyQueryHints(new StartOp(BOp.NOARGS,
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, idFactory
                                .incrementAndGet()),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                })),queryHints);

        if (rule.getTailCount() == 0) {
            // TODO Why would we every evaluation an empty rule?
            throw new RuntimeException("Empty rule?");
//        	return startOp;
        }

    	return convert(rule,
    			startOp,
    			null/* known bound variables */,
    			idFactory, db, queryEngine, queryHints);

    }

    /**
     * Extend a query plan to include the evaluation of the required joins and
     * constraints described by the {@link IRule}.
     * 
     * @param rule
     *            The rule to be converted. This may consist of only required
     *            joins. The rule MUST NOT contain any OPTIONAL predicates. The
     *            caller is responsible for evaluating OPTIONAL predicates,
     *            optional subqueries, and constraints for variables which might
     *            not be bound by the required joins.
     * @param left
     *            The upstream operator (if any) in the pipeline (may be
     *            <code>null</code>).
     * @param knownBound
     *            The set of variables which are known to be bound. This is used
     *            to determine when the constraints associated with the rule
     *            will be evaluated.
     * @param idFactory
     *            Used to assign IDs to generated operators.
     * @param db
     * @param queryEngine
     * @param queryHints
     * @return
     */
    public static PipelineOp convert(//
            final IRule<?> rule,//
            PipelineOp left,//
            final Set<IVariable<?>> knownBound,//
            final AtomicInteger idFactory, //
            final AbstractTripleStore db,//
            final QueryEngine queryEngine, //
            final Properties queryHints) {

        /*
         * First put the tails in the correct order based on the logic in
         * DefaultEvaluationPlan2.
         *
         * @todo Consider making order[] disappear such that all of the arrays
         * (preds[], cardinality[], keyOrder[]) are indexed directly by the
         * array index rather than by order[i]. Alternatively, make sure that
         * the runtime query optimizer reports the permutation array (order[])
         * so we can maintain information about the relationship between the
         * given joins and the evaluation order.
         */
        final BOpContextBase context = new BOpContextBase(queryEngine);

        final QueryOptimizerEnum optimizer = queryHints == null ? QueryOptimizerEnum.Static
                : QueryOptimizerEnum.valueOf(queryHints.getProperty(
                        QueryHints.OPTIMIZER, QueryOptimizerEnum.Static
                                .toString()));

        // The evaluation plan order.
        final int[] order;
        // The estimated cardinality of each tail (if the optimizer provides it)
        final long[] cardinality;
        // The index assigned to each tail of the rule by static analysis.
        final IKeyOrder[] keyOrder;

        switch(optimizer) {
        case None: {
            /*
             * Do not run the join optimizer.
             */
            order = new int[rule.getTailCount()];
            for (int i = 0; i < order.length; i++) {
                order[i] = i;
            }
            cardinality = null;
            keyOrder = null;
            break;
        }
        case Static: {
            /*
             * Static query optimizer.
             */
            final DefaultEvaluationPlan2 plan = new DefaultEvaluationPlan2(
                    new IRangeCountFactory() {

                        public long rangeCount(final IPredicate pred) {
                            return context.getRelation(pred)
                                    .getAccessPath(pred).rangeCount(false);
                        }

                    }, rule);

            order = plan.getOrder();

            /*
             * The index assigned to each tail of the rule by static analysis
             * (this is often not the index which is actually used when we
             * evaluate a given predicate since we always choose the best index
             * and that can depend on whether or not we are binding the context
             * position for a default or named graph query. When optional joins
             * are involved, some variables may not become bound for some
             * solutions. A different index will often be chosen for access
             * paths using the unbound variable.
             */

            // the #of variables in each tail of the rule (set by side-effect).
            final int[] nvars = new int[rule.getTailCount()];

            cardinality = new long[rule.getTailCount()];
            for (int i = 0; i < cardinality.length; i++) {
                cardinality[i] = plan.cardinality(i);
            }

            keyOrder = computeKeyOrderForEachTail(rule, context, order, nvars);

            break;

        }
        case Runtime: {
            /*
             * The runtime query optimizer.
             *
             * FIXME We can not optimize quads here using the runtime query
             * optimizer since we have not yet generated the full query plan. In
             * order to get the runtime query optimizer working for quads we
             * need to replace the DataSetJoin with a PipelineJoin against an
             * inline "relation" containing the named or default graphs IVs. The
             * runtime query optimizer does not accept the JOIN operators so the
             * annotations which are being applied there will be lost which is
             * another problem, especially in scale-out. Both of these issues
             * need to be resolved before quads can be used with the runtime
             * query optimizer. [Actually, a DataSetJoin is not a problem if
             * we handle the RTO integration in terms of the AST, but we still
             * might want to replace it with an inline access path.]
             *
             * @todo In fact, we should be able to write in a JoinGraph operator
             * which optimizes the join graph and then evaluates it rather than
             * explicitly doing the optimization and evaluation steps here.
             *
             * FIXME The runtime query optimizer can not be run against an
             * IPredicate[] extracted from the IRule, even for triples, because
             * those IPredicates lack some critical annotations, such as the
             * bopId, which are only added in the logic below this point. Thus,
             * while we can run the static optimizer first, the runtime
             * optimizer needs to be run after we convert to bops (or as a bop
             * at runtime). [This all runs into trouble because we are creating
             * the JOIN operators in the code below rather than inferring the
             * correct JOIN annotations based on the IPredicates.]
             *
             * @todo Make sure that a summary of the information collected by
             * the runtime query optimizer is attached as an annotation to the
             * query.
             *
             * @todo query hints for [limit] and [nedges].
             */

//            // The initial sampling limit.
//            final int limit = 100;
//
//            // The #of edges considered for the initial paths.
//            final int nedges = 2;
//
//            // isolate/extract the join graph.
//            final IPredicate[] preds = new IPredicate[rule.getTailCount()];
//            for (int i = 0; i < preds.length; i++) {
//                preds[i] = rule.getTail(i);
//            }
//
//            final JGraph g = new JGraph(preds);
//
//            final Path p;
//            try {
//                p = g.runtimeOptimizer(queryEngine, limit, nedges);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//
//            // the permutation order.
//            order = g.getOrder(p);
//
//            keyOrder = null;
//
//            cardinality = null;
//
//            break;

            throw new UnsupportedOperationException("Runtime optimizer is not supported yet.");

        }
        default:
            throw new AssertionError("Unknown option: " + optimizer);
        }

        // the variables to be retained for each join.
//        final IVariable<?>[][] selectVars = RuleState
//                .computeRequiredVarsForEachTail(rule, order);

        /*
         * Create an array of predicates in the decided evaluation order with
         * various annotations providing details from the query optimizer.
         */
        final Predicate<?>[] preds = new Predicate[rule.getTailCount()];
        for (int i = 0; i < order.length; i++) {

            Predicate<?> pred = (Predicate<?>) rule.getTail(order[i]);
            
            // Verify that the predicate has the required ID annotation.
            pred.getId();

            /*
             * Decorate the predicate with the assigned index (this is purely
             * informative).
             */
            if (keyOrder != null && keyOrder[order[i]] != null) {
                // pred = pred.setKeyOrder(keyOrder[order[i]]);
                pred = (Predicate<?>) pred.setProperty(
                        Annotations.ORIGINAL_INDEX, keyOrder[order[i]]);
            }

    		// decorate the predicate with the cardinality estimate.
            if (cardinality != null) {
                pred = (Predicate<?>) pred.setProperty(
                        Annotations.ESTIMATED_CARDINALITY,
                        cardinality[order[i]]);
            }

            // save reference into array in evaluation order.
            preds[i] = pred;

        }

        /*
         * Analyze the predicates and constraints to decide which constraints
         * will run with which predicates.
         * 
         * Two comments from MikeP on Rule2BOp which may bear on the RTO
         * integration:
         * 
         * 1. I no longer pass optional tails into Rule2BOp, I handle them
         * directly in SOp2BOp. Has to do with making sure they run after
         * non-optional subqueries (nested unions). So the static/runtime
         * optimizers never even see them anymore.
         * 
         * 2. Constraints that use variables not bound by the time the required
         * predicates are run are not passed into Rule2BOp either – those are
         * handled as ConditionalRoutingOps? after the optionals and subqueries
         * have run. So you don't need to worry about those either.
         */
        final IConstraint[][] assignedConstraints;
		{

			final int nconstraints = rule.getConstraintCount();

			// Extract IConstraint[] from the rule.
			final IConstraint[] constraints = new IConstraint[nconstraints];
			for (int i = 0; i < constraints.length; i++) {
				constraints[i] = rule.getConstraint(i);
			}

			final int nknownBound = knownBound != null ? knownBound.size() : 0;

			// figure out which constraints are attached to which
			// predicates.
			assignedConstraints = PartitionedJoinGroup.getJoinGraphConstraints(
					preds, constraints,
					nknownBound == 0 ? IVariable.EMPTY : knownBound
							.toArray(new IVariable<?>[nknownBound]),
					true// pathIsComplete
					);
		}

		/*
         *
         */
		final Set<IVariable<?>> doneSet = new LinkedHashSet<IVariable<?>>();
        for (int i = 0; i < preds.length; i++) {

            // assign a bop id to the predicate
            final Predicate<?> pred = (Predicate<?>) preds[i];

            // need to make a modifiable collection
            final Collection<IConstraint> c = new LinkedList<IConstraint>
            	(Arrays.asList(assignedConstraints[i]));

            left = join(db, queryEngine, left, pred,//
                    doneSet,//
                    c, context, idFactory, queryHints);

        }

        if (log.isInfoEnabled()) {
            // just for now while i'm debugging
            log.info("rule: " + rule);
            log.info("query:\n" + BOpUtility.toString(left));
        }

        return left;

    }

    /**
     * Convert a program into an operator tree. Programs can be either UNIONs or
     * STEPS. They can also be STARs (transitive closure) of a UNION or STEPS.
     *
     * @param program
     *            The program.
     *
     * @return The operator tree.
     *
     * @todo This does not handle STAR ({@link IProgram#isClosure()}). However,
     *       that is not currently generated by the SAIL. STAR is used for truth
     *       maintenance and could also be useful for property path queries.
     */
    public static PipelineOp convert(final IProgram program,
            final AtomicInteger idFactory, final AbstractTripleStore db,
            final QueryEngine queryEngine, final Properties queryHints) {

        // When parallel, the program is translated to a UNION. Else STEPS.
        final boolean isParallel = program.isParallel();

        // The bopId for the UNION or STEP.
        final int thisId = idFactory.incrementAndGet();

       final IStep[] steps = program.toArray();

        final BOp[] subqueries = new BOp[steps.length];

        for (int i = 0; i < steps.length; i++) {

            // convert the child IStep
            final BOp tmp = convert(steps[i], idFactory, db, queryEngine,
                    queryHints);

            /*
             * @todo Route binding sets around the UNION/STEPS operator. We need
             * the id of the parent of the UNION/STEPS operator to do this. This
             * only matters if the UNION / STEPS operator feeds something which
             * does not execute on the query controller.
             */
//            tmp = tmp.setProperty(PipelineOp.Annotations.SINK_REF, thisId);

            subqueries[i] = tmp;

        }

        final LinkedList<NV> anns = new LinkedList<NV>();

        anns.add(new NV(BOp.Annotations.BOP_ID, thisId));

        // the subqueries.
        anns.add(new NV(AbstractSubqueryOp.Annotations.SUBQUERIES, subqueries));

//        anns.add(new NV(Union.Annotations.EVALUATION_CONTEXT,
//                BOpEvaluationContext.CONTROLLER));
//
//        anns.add(new NV(Union.Annotations.CONTROLLER, true));

        if (!isParallel)
            anns.add(new NV(Union.Annotations.MAX_PARALLEL_SUBQUERIES, 1));

        final PipelineOp thisOp;
        if (isParallel) {
            thisOp = new Union(BOp.NOARGS, NV
                    .asMap(anns.toArray(new NV[anns.size()])));
        } else {
            thisOp = new Steps(BOp.NOARGS, NV
                    .asMap(anns.toArray(new NV[anns.size()])));
        }

        return thisOp;

    }

    /**
     * Return an array indicating the {@link IKeyOrder} that will be used when
     * reading on each of the tail predicates. The array is formed using a
     * private {@link IBindingSet} and propagating fake bindings to each
     * predicate in turn using the given evaluation order.
     *
     * @param order
     *            The evaluation order.
     * @param nvars
     *            The #of unbound variables for each tail predicate is assigned
     *            by side-effect.
     *
     * @return An array of the {@link IKeyOrder}s for each tail predicate. The
     *         array is correlated with the predicates index in the tail of the
     *         rule NOT its evaluation order.
     */
    @SuppressWarnings("unchecked")
    static private IKeyOrder[] computeKeyOrderForEachTail(final IRule rule,
            final BOpContextBase context, final int[] order, final int[] nvars) {

        if (order == null)
            throw new IllegalArgumentException();

        if (order.length != rule.getTailCount())
            throw new IllegalArgumentException();

        final int tailCount = rule.getTailCount();

        final IKeyOrder[] a = new IKeyOrder[tailCount];

        final IBindingSet bindingSet = new ListBindingSet();

        for (int orderIndex = 0; orderIndex < tailCount; orderIndex++) {

            final int tailIndex = order[orderIndex];

            final IPredicate pred = rule.getTail(tailIndex);

            final IRelation rel = context.getRelation(pred);

            final IPredicate asBound = pred.asBound(bindingSet);

            final IKeyOrder keyOrder = context.getAccessPath(
                    rel, asBound).getKeyOrder();

            if (log.isDebugEnabled())
                log.debug("keyOrder=" + keyOrder + ", orderIndex=" + orderIndex
                        + ", tailIndex=" + orderIndex + ", pred=" + pred
                        + ", bindingSet=" + bindingSet + ", rule=" + rule);

            // save results.
            a[tailIndex] = keyOrder;
            nvars[tailIndex] = keyOrder == null ? asBound.getVariableCount()
                    : asBound.getVariableCount((IKeyOrder) keyOrder);

            final int arity = pred.arity();

            for (int j = 0; j < arity; j++) {

                final IVariableOrConstant<?> t = pred.get(j);

                if (t.isVar()) {

                    final IVariable<?> var = (IVariable<?>) t;

                    if (log.isDebugEnabled()) {

                        log.debug("Propagating binding: pred=" + pred
                                        + ", var=" + var + ", bindingSet="
                                        + bindingSet);

                    }

                    bindingSet.set(var, fakeTermId);

                }

            }

        }

        if (log.isDebugEnabled()) {

            log.debug("keyOrder[]=" + Arrays.toString(a) + ", nvars="
                    + Arrays.toString(nvars) + ", rule=" + rule);

        }

        return a;

    }

    /**
     * A fake value that is propagated when we compute the {@link IKeyOrder} for
     * a series of joins based on an assigned join evaluation order.
     *
     * @todo This has to be of the appropriate data type or we run into class
     * cast exceptions.
     */
    final private static transient IConstant<IV> fakeTermId = new Constant<IV>(
            TermId.mockIV(VTE.URI)
            );

}
