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

package com.bigdata.rdf.sail;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
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
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.filter.DistinctFilter;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.controller.AbstractSubqueryOp;
import com.bigdata.bop.controller.Steps;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.cost.ScanCostReport;
import com.bigdata.bop.cost.SubqueryCostReport;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.joinGraph.IRangeCountFactory;
import com.bigdata.bop.joinGraph.PartitionedJoinGroup;
import com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlan2;
import com.bigdata.bop.rdf.filter.StripContextFilter;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
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
public class Rule2BOpUtility {

    protected static final Logger log = Logger.getLogger(Rule2BOpUtility.class);

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
     */
    private static boolean enableDecisionTree = true;

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

    }

//    /**
//     * A list of annotations to be cleared from {@link Predicate} when they are
//     * copied into a query plan.
//     */
//    private static final String[] ANNS_TO_CLEAR_FROM_PREDICATE = new String[] {
//            Annotations.QUADS,//
//            Annotations.DATASET,//
//            Annotations.SCOPE,//
//            IPredicate.Annotations.OPTIONAL //
//    };
    
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
                tmp = new SliceOp(new BOp[] { tmp }, NV.asMap(//
                        new NV(BOp.Annotations.BOP_ID, idFactory
                                .incrementAndGet()), //
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.SHARED_STATE,true)//
                        ));

            }

            return applyQueryHints(tmp, queryHints);
            
        }
        
        return convert((IProgram) step, idFactory, db, queryEngine, queryHints);

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
     * @todo It would be nice if this would only apply those query hints to an
     *       operator which are known to be annotations understood by that
     *       operator. This information is basically available from the inner
     *       Annotation interface for a given operator class, but that is not
     *       really all that accessible.
     */
    private static PipelineOp applyQueryHints(PipelineOp op,
            Properties queryHints) {

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
     * Convert a rule into an operator tree.
     * 
     * @param rule
     * 
     * @return
     */
    public static PipelineOp convert(final IRule<?> rule,
            final AtomicInteger idFactory, final AbstractTripleStore db,
            final QueryEngine queryEngine, final Properties queryHints) {

    	return convert(rule, 
    			null/* conditionals */, 
    			null/* known bound variables */, 
    			idFactory, db, queryEngine, queryHints);
    	
    }
    
    public static PipelineOp convert(final IRule<?> rule,
    		final Collection<IConstraint> conditionals,
    		final Set<IVariable<?>> knownBound,
            final AtomicInteger idFactory, final AbstractTripleStore db,
            final QueryEngine queryEngine, final Properties queryHints) {

//        // true iff the database is in quads mode.
//        final boolean isQuadsQuery = db.isQuads();
        
        final PipelineOp startOp = applyQueryHints(new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, idFactory
                                .incrementAndGet()),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                })),queryHints);
        
        if (rule.getTailCount() == 0) {
        	return startOp;
        }

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
        
        final QueryOptimizerEnum optimizer = QueryOptimizerEnum
                .valueOf(queryHints.getProperty(QueryHints.OPTIMIZER,
                        QueryOptimizerEnum.Static.toString()));

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
             * 
             * @todo Do we need to move any of the joins to the front, e.g.,
             * magic search, or should everything just be left the way it is?
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
             * FIXME MikeP: I have modified the JoinGraph so that it can report
             * the permutation order. However, the code here needs to isolate
             * the join graph rather than running against all predicates in the
             * tail. As it is, it will reorder optionals.
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
             * query optimizer.
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
        
        PipelineOp left = startOp;
        
        if (conditionals != null) { // @todo lift into CONDITION on SubqueryOp
        	for (IConstraint c : conditionals) {
        		final int condId = idFactory.incrementAndGet();
                final PipelineOp condOp = applyQueryHints(
                	new ConditionalRoutingOp(new BOp[]{left},
                        NV.asMap(new NV[]{//
                            new NV(BOp.Annotations.BOP_ID,condId),
                            new NV(ConditionalRoutingOp.Annotations.CONDITION, c),
                        })), queryHints);
                left = condOp;
                if (log.isDebugEnabled()) {
                	log.debug("adding conditional routing op: " + condOp);
                }
        	}
        }

        /*
         * Create an array of predicates in the decided evaluation with various
         * annotations providing details from the query optimizer.
         */
        final Predicate<?>[] preds = new Predicate[rule.getTailCount()];
        for (int i = 0; i < order.length; i++) {
            
            // assign a bop id to the predicate
            Predicate<?> pred = (Predicate<?>) rule.getTail(order[i]).setBOpId(
                    idFactory.incrementAndGet());
            
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
         * will run with which predicates.  @todo does not handle optionals
         * correctly, but we do not pass optionals in to Rule2BOpUtility
         * from SOp2BOpUtility anymore so ok for now
         */
        final IConstraint[][] assignedConstraints;
        {
            // Extract IConstraint[] from the rule.
            final IConstraint[] constraints = new IConstraint[rule.getConstraintCount()];
            for(int i=0; i<constraints.length; i++) {
                constraints[i] = rule.getConstraint(i);
            }
            
            // figure out which constraints are attached to which predicates.
            assignedConstraints = PartitionedJoinGroup.getJoinGraphConstraints(
                    preds, constraints, 
                    knownBound.toArray(new IVariable<?>[knownBound.size()]));
        }

        /*
         * 
         */
        for (int i = 0; i < preds.length; i++) {
            
            // assign a bop id to the predicate
            final Predicate<?> pred = (Predicate<?>) preds[i];

            left = join(queryEngine, left, pred,//
                    Arrays.asList(assignedConstraints[i]), //
                    context, idFactory, queryHints);

        }
        
        if (log.isInfoEnabled()) {
            // just for now while i'm debugging
            log.info("rule=" + rule + ":::query="
                    + BOpUtility.toString(left));
        }
        
        return left;
        
    }
    
    public static PipelineOp join(final QueryEngine queryEngine, 
			PipelineOp left, Predicate pred, final AtomicInteger idFactory,
			final Properties queryHints) {
    	
		return join(queryEngine, left, pred, null, 
				new BOpContextBase(queryEngine), idFactory, queryHints);
    	
    }
    
    public static PipelineOp join(final QueryEngine queryEngine, 
    		PipelineOp left, Predicate pred, 
    		final Collection<IConstraint> constraints, 
    		final BOpContextBase context, final AtomicInteger idFactory, 
    		final Properties queryHints) {
    	
        final int joinId = idFactory.incrementAndGet();
        
        // annotations for this join.
        final List<NV> anns = new LinkedList<NV>();
        
        anns.add(new NV(BOp.Annotations.BOP_ID, joinId));

//        anns.add(new NV(PipelineJoin.Annotations.SELECT,
//                selectVars[order[i]]));
        
        // No. The join just looks at the Predicate's optional annotation.
//        if (pred.isOptional())
//            anns.add(new NV(PipelineJoin.Annotations.OPTIONAL, pred
//                    .isOptional()));
        
        if (constraints != null && !constraints.isEmpty()) {
//			// decorate the predicate with any constraints.
//			pred = (Predicate<?>) pred.setProperty(
//					IPredicate.Annotations.CONSTRAINTS, constraints
//							.toArray(new IConstraint[constraints.size()]));
				
				// add constraints to the join for that predicate.
			anns.add(new NV(
					PipelineJoin.Annotations.CONSTRAINTS,
					constraints.toArray(new IConstraint[constraints.size()])));

		}

        /*
         * Pull off annotations before we clear them from the predicate.
         */
        final Scope scope = (Scope) pred.getProperty(Annotations.SCOPE);

        // true iff this is a quads access path.
        final boolean quads = pred.getProperty(Annotations.QUADS,
                Annotations.DEFAULT_QUADS);

        // pull of the Sesame dataset before we strip the annotations.
        final Dataset dataset = (Dataset) pred
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
                 * Strip off the named graph or default graph expander (in
                 * the long term it will simply not be generated.)
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
                 * using expanders which were attached by
                 * BigdataEvaluationStrategyImpl.
                 */
                
                final boolean scaleOut = queryEngine.isScaleOut();
                if (scaleOut)
                    throw new UnsupportedOperationException();
                
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.ANY));

                anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

                left = applyQueryHints(new PipelineJoin(new BOp[] { left },
                        anns.toArray(new NV[anns.size()])), queryHints);

            }

        } else {

            /*
             * Triples or provenance mode.
             */

            left = triplesModeJoin(queryEngine, left, anns, pred, queryHints);

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
        if (scaleOut) {
            /*
             * All triples queries can run shard-wise.
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

        return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
                .toArray(new NV[anns.size()])), queryHints);

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
            final Dataset dataset, final Properties queryHints) {

        final boolean scaleOut = queryEngine.isScaleOut();
        if (scaleOut) {
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

        if (dataset == null) {

            /*
             * The dataset is all graphs. C is left unbound and the unmodified
             * access path is used.
             */

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

            return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
                    .toArray(new NV[anns.size()])), queryHints);

        }

        if (pred.get(3/* c */).isConstant()) {

            /*
             * C is already bound.  The unmodified access path is used. 
             */

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

            return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
                    .toArray(new NV[anns.size()])), queryHints);

        }

        /*
         * @todo raise this into the caller and do one per rule rather than once
         * per access path. While a query can mix default and named graph access
         * paths, there is only one named graph collection and one default graph
         * collection within the scope of that query.
         */
        final DataSetSummary summary = new DataSetSummary(dataset
                .getNamedGraphs());

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

            return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
                    .toArray(new NV[anns.size()])), queryHints);

        }

        if (summary.nknown == 1) {

            /*
             * The dataset contains exactly one graph. Bind C.
             */

            pred = pred.asBound((IVariable<?>) pred.get(3),
                    new Constant<IV<?, ?>>(summary.firstContext));

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

            return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
                    .toArray(new NV[anns.size()])), queryHints);

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
        final IRelation r = context.getRelation(pred);
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

            return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
                    .toArray(new NV[anns.size()])), queryHints);

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
            final DataSetJoin dataSetJoin = new DataSetJoin(new BOp[] { left },
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

            return applyQueryHints(new PipelineJoin(new BOp[] { dataSetJoin },
                    anns.toArray(new NV[anns.size()])), queryHints);

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
    private static PipelineOp defaultGraphJoin(final QueryEngine queryEngine,
            final BOpContextBase context, final AtomicInteger idFactory,
            final PipelineOp left, final List<NV> anns, Predicate<?> pred,
            final Dataset dataset, final Properties queryHints) {

        /*
         * @todo raise this into the caller and do one per rule rather than once
         * per access path. While a query can mix default and named graph access
         * paths, there is only one named graph collection and one default graph
         * collection within the scope of that query.
         */
        final DataSetSummary summary = dataset == null ? null
                : new DataSetSummary(dataset.getDefaultGraphs());

        final boolean scaleOut = queryEngine.isScaleOut();

        if (dataset != null && summary.nknown == 0) {

            /*
             * The data set is empty (no graphs). Return a join backed by an
             * empty access path.
             */

            // force an empty access path for this predicate.
            pred = (Predicate<?>) pred.setUnboundProperty(
                    IPredicate.Annotations.ACCESS_PATH_EXPANDER,
                    EmptyAccessPathExpander.INSTANCE);

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

            return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
                    .toArray(new NV[anns.size()])), queryHints);

        }
        
        if (dataset != null && summary.nknown == 1) {

            /*
             * The dataset contains exactly one graph. Bind C. Add a filter to
             * strip off the context position.
             */

            // Bind C.
            pred = pred.asBound((IVariable<?>) pred.get(3),
                    new Constant<IV<?, ?>>(summary.firstContext));

            if (scaleOut) {
                // use a partitioned join.
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.SHARDED));
                pred = (Predicate) pred.setProperty(
                        Predicate.Annotations.REMOTE_ACCESS_PATH, false);
            }

            // Strip of the context position.
            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());
            
			anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

            return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
                    .toArray(new NV[anns.size()])), queryHints);

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
         */
        final IRelation r = context.getRelation(pred);
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

			return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
					.toArray(new NV[anns.size()])),queryHints);

        } else {

            /*
             * PARALLEL SUBQUERY. Bind each value of C in turn, issuing parallel
             * subqueries against the asBound access paths using an expander
             * pattern and layer on a filter to strip off the context position.
             * The asBound access paths write on a shared buffer. That shared
             * buffer is read from by the expander.
             * 
             * Scale-out: JOIN is ANY or HASHED. AP is REMOTE.
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

			return applyQueryHints(new PipelineJoin(new BOp[] { left }, anns
					.toArray(new NV[anns.size()])),queryHints);

        }

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
            final BOpBase tmp = convert(steps[i], idFactory, db, queryEngine,
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
            thisOp = new Union(new BOp[]{}, NV
                    .asMap(anns.toArray(new NV[anns.size()])));
        } else {
            thisOp = new Steps(new BOp[]{}, NV
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
        
        final IBindingSet bindingSet = new HashBindingSet();
        
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

                    final Var<?> var = (Var<?>) t;

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
            new TermId(VTE.URI, -1L));

}
