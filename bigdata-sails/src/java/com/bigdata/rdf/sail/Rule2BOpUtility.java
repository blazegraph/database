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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.controller.Steps;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.cost.ScanCostReport;
import com.bigdata.bop.cost.SubqueryCostReport;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
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
import com.bigdata.relation.rule.eval.DefaultEvaluationPlan2;
import com.bigdata.relation.rule.eval.IRangeCountFactory;
import com.bigdata.relation.rule.eval.RuleState;
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
            final QueryEngine queryEngine) {

        if (step instanceof IRule<?>) {

            // Convert the step to a bigdata operator tree.
            PipelineOp tmp = convert((IRule<?>) step, idFactory, db,
                    queryEngine);

            if (!tmp.getEvaluationContext().equals(
                    BOpEvaluationContext.CONTROLLER)) {
                /*
                 * Wrap with an operator which will be evaluated on the query
                 * controller.
                 */
                tmp = new SliceOp(new BOp[] { tmp }, NV.asMap(//
                        new NV(BOp.Annotations.BOP_ID, idFactory
                                .incrementAndGet()), //
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER)));

            }

            return tmp;
            
        }
        
        return convert((IProgram) step, idFactory, db, queryEngine);

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
            final QueryEngine queryEngine) {

//        // true iff the database is in quads mode.
//        final boolean isQuadsQuery = db.isQuads();
        
        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, idFactory
                                .incrementAndGet()),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                }));
        
        /*
         * First put the tails in the correct order based on the logic in
         * DefaultEvaluationPlan2.
         */
        final BOpContextBase context = new BOpContextBase(queryEngine);
        final DefaultEvaluationPlan2 plan = new DefaultEvaluationPlan2(
                new IRangeCountFactory() {

            public long rangeCount(final IPredicate pred) {
                return context.getRelation(pred).getAccessPath(pred)
                                .rangeCount(false);
            }
            
        }, rule);
        
        // evaluation plan order.
        final int[] order = plan.getOrder();
        
        // the #of variables in each tail of the rule.
        final int[] nvars = new int[rule.getTailCount()];

        // the index assigned to each tail of the rule.
        final IKeyOrder[] keyOrder = computeKeyOrderForEachTail(rule, context,
                order, nvars);

        // the variables to be retained for each join.
        final IVariable<?>[][] selectVars = RuleState
                .computeRequiredVarsForEachTail(rule, order);
        
        /*
         * Map the constraints from the variables they use.  This way, we can
         * properly attach constraints to only the first tail in which the
         * variable appears.  This way we only run the appropriate constraint
         * once, instead of for every tail. 
         */
        final Map<IVariable<?>, Collection<IConstraint>> constraintsByVar = 
            new HashMap<IVariable<?>, Collection<IConstraint>>();
        for (int i = 0; i < rule.getConstraintCount(); i++) {
            final IConstraint c = rule.getConstraint(i);
            
            if (log.isDebugEnabled()) {
                log.debug(c);
            }
            
            final Set<IVariable<?>> uniqueVars = new HashSet<IVariable<?>>();
            final Iterator<IVariable<?>> vars = BOpUtility.getSpannedVariables(c);
            while (vars.hasNext()) {
                final IVariable<?> v = vars.next();
                uniqueVars.add(v);
            }
            
            for (IVariable<?> v : uniqueVars) {

                if (log.isDebugEnabled()) {
                    log.debug(v);
                }
                
                Collection<IConstraint> constraints = constraintsByVar.get(v);
                if (constraints == null) {
                    constraints = new LinkedList<IConstraint>();
                    constraintsByVar.put(v, constraints);
                }
                constraints.add(c);
            }
        }
        
        PipelineOp left = startOp;
        
        for (int i = 0; i < order.length; i++) {
            
            final int joinId = idFactory.incrementAndGet();
            
            // assign a bop id to the predicate
            Predicate<?> pred = (Predicate<?>) rule.getTail(order[i]).setBOpId(
                    idFactory.incrementAndGet());

            // decorate the predicate with the assigned index.
//            pred = pred.setKeyOrder(keyOrder[order[i]]);
            pred.setProperty(Annotations.ORIGINAL_INDEX, keyOrder[order[i]]);

            /*
             * Collect all the constraints for this predicate based on which
             * variables make their first appearance in this tail
             */
            final Collection<IConstraint> constraints = 
                new LinkedList<IConstraint>();
            
            /*
             * Peek through the predicate's args to find its variables. Use
             * these to attach constraints to the join based on the variables
             * that make their first appearance in this tail.
             */
            for (BOp arg : pred.args()) {
                if (arg instanceof IVariable<?>) {
                    final IVariable<?> v = (IVariable<?>) arg;
                    /*
                     * We do a remove because we don't ever need to run these
                     * constraints again during subsequent joins once they have
                     * been run once at the initial appearance of the variable.
                     * 
                     * @todo revisit this when we dynamically re-order running
                     * joins
                     */ 
                    if (constraintsByVar.containsKey(v))
                        constraints.addAll(constraintsByVar.remove(v));
                }
            }
           
            // annotations for this join.
            final List<NV> anns = new LinkedList<NV>();
            
            anns.add(new NV(BOp.Annotations.BOP_ID, joinId));

            anns.add(new NV(PipelineJoin.Annotations.SELECT,
                    selectVars[order[i]]));
            
            if (pred.isOptional())
                anns.add(new NV(PipelineJoin.Annotations.OPTIONAL, pred
                        .isOptional()));
            
            if (!constraints.isEmpty())
                anns.add(new NV(PipelineJoin.Annotations.CONSTRAINTS,
                        constraints
                                .toArray(new IConstraint[constraints.size()])));

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
                                left, anns, pred, dataset);
                        break;
                    case DEFAULT_CONTEXTS:
                        left = defaultGraphJoin(queryEngine, context, idFactory,
                                left, anns, pred, dataset);
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

                    left = new PipelineJoin(new BOp[] { left, pred }, anns
                            .toArray(new NV[anns.size()]));

                }

            } else {

                /*
                 * Triples or provenance mode.
                 */

                left = triplesModeJoin(queryEngine, left, anns, pred);

            }

        }
        
        if (true||log.isInfoEnabled()) {
            // just for now while i'm debugging
            log.info("rule=" + rule + ":::query="
                    + BOpUtility.toString(left));
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
            final PipelineOp left, final List<NV> anns, Predicate<?> pred) {

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

        return new PipelineJoin(new BOp[] { left, pred }, anns
                .toArray(new NV[anns.size()]));

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
            final Dataset dataset) {

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

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        }

        if (pred.get(3/* c */).isConstant()) {

            /*
             * C is already bound.  The unmodified access path is used. 
             */

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

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

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        }

        if (summary.nknown == 1) {

            /*
             * The dataset contains exactly one graph. Bind C.
             */

            pred = pred.asBound((IVariable<?>) pred.get(3),
                    new Constant<IV<?, ?>>(summary.firstContext));

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

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

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

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

            return new PipelineJoin(new BOp[] { dataSetJoin, pred }, anns
                    .toArray(new NV[anns.size()]));

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
            final Dataset dataset) {

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

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

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
            
            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

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
//            return new PipelineJoin(new BOp[] { left, pred }, anns
//                    .toArray(new NV[anns.size()]));
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
            
            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

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
            
            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));
            
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
            final QueryEngine queryEngine) {

        // When parallel, the program is translated to a UNION. Else STEPS.
        final boolean isParallel = program.isParallel();
        
        // The bopId for the UNION or STEP.
        final int thisId = idFactory.incrementAndGet();

        final int arity = program.stepCount();

        final IStep[] steps = program.toArray();
        
        final BOp[] args = new BOp[arity];

        for (int i = 0; i < arity; i++) {

            // convert the child IStep
            BOpBase tmp = convert(steps[i], idFactory, db, queryEngine);

            /*
             * @todo Route binding sets around the UNION/STEPS operator. We need
             * the id of the parent of the UNION/STEPS operator to do this. This
             * only matters if the UNION / STEPS operator feeds something which
             * does not execute on the query controller.
             */
//            tmp = tmp.setProperty(PipelineOp.Annotations.SINK_REF, thisId);

            args[i] = tmp;
            
        }
        
        final LinkedList<NV> anns = new LinkedList<NV>();
        
        anns.add(new NV(Union.Annotations.BOP_ID, thisId));
        
        anns.add(new NV(Union.Annotations.EVALUATION_CONTEXT,
                BOpEvaluationContext.CONTROLLER));
        
        anns.add(new NV(Union.Annotations.CONTROLLER, true));
        
        if (!isParallel)
            anns.add(new NV(Union.Annotations.MAX_PARALLEL, 1));

        final PipelineOp thisOp;
        if (isParallel) {
            thisOp = new Union(args, NV
                    .asMap(anns.toArray(new NV[anns.size()])));
        } else {
            thisOp = new Steps(args, NV
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
