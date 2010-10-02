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

package com.bigdata.bop.engine;

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

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
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
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.fed.FederatedQueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.filter.StripContextFilter;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sail.BigdataEvaluationStrategyImpl;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.spo.ContextAdvancer;
import com.bigdata.rdf.spo.DefaultGraphSolutionExpander;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.InGraphHashSetFilter;
import com.bigdata.rdf.spo.NamedGraphSolutionExpander;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.EmptyAccessPathExpander;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlan2;
import com.bigdata.relation.rule.eval.IRangeCountFactory;
import com.bigdata.relation.rule.eval.RuleState;
import com.bigdata.service.ResourceService;
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
     * {@link IPredicate.Annotations#ACCESS_PATH_EXPANDER}. In the long term, we will simply
     * no longer generate them in {@link BigdataEvaluationStrategyImpl}.
     * <p>
     * Note: If you want to test just the named graph stuff, then the default
     * graph processing could be handed off to the
     * {@link DefaultGraphSolutionExpander}.
     */
    private static boolean enableDecisionTree = false;

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

        /**
         * The graph variable specified in the query (quads mode only). This is
         * <p>
         * Note: This is not used for SIDs mode because we use the standard
         * triple store access paths.
         * 
         * @see org.openrdf.query.algebra.Var
         * 
         * @todo can we just use pred.get(3)?
         */
        String CVAR = Rule2BOpUtility.class.getName() + ".cvar";

        /*
         * Cost estimates.
         */

        /**
         * The estimated cost of a SCAN + FILTER approach to a default graph or
         * named graph query.
         */
        String COST_SCAN = Rule2BOpUtility.class.getName() + ".cost.scan";

        /**
         * The estimated cost of a SUBQUERY approach to a default graph or named
         * graph query.
         */
        String COST_SUBQUERY = Rule2BOpUtility.class.getName()
                + ".cost.subquery";

        /**
         * The #of samples used when estimating the cost of a SUBQUERY approach
         * to a default graph or named graph query.
         */
        String COST_SUBQUERY_SAMPLE_COUNT = Rule2BOpUtility.class.getName()
                + ".cost.subquerySampleCount";

        /**
         * The #of known graphs in the {@link Dataset} for a default graph or
         * named graph query.
         */
        String NKNOWN = Rule2BOpUtility.class.getName() + ".nknown";

    }

    /**
     * A list of annotations to be cleared from {@link Predicate} when they are
     * copied into a query plan.
     */
    private static final String[] ANNS_TO_CLEAR_FROM_PREDICATE = new String[] {
            Annotations.QUADS,//
            Annotations.DATASET,//
            Annotations.SCOPE,//
            Annotations.CVAR,//
            IPredicate.Annotations.OPTIONAL //
    };
    
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
    public static PipelineOp convert(final IStep step, final int startId,
            final AbstractTripleStore db, final QueryEngine queryEngine) {

        if (step instanceof IRule)
            return convert((IRule) step, startId, db, queryEngine);
        
        return convert((IProgram) step, startId, db, queryEngine);

    }

    /**
     * Convert a rule into an operator tree.
     * 
     * @param rule
     * 
     * @return
     */
    public static PipelineOp convert(final IRule rule, final int startId,
            final AbstractTripleStore db, final QueryEngine queryEngine) {

//        // true iff the database is in quads mode.
//        final boolean isQuadsQuery = db.isQuads();
        
        int bopId = startId;

        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, bopId++),//
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
        final IVariable[][] selectVars = RuleState
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
            
            final int joinId = bopId++;
            
            // assign a bop id to the predicate
            Predicate<?> pred = (Predicate<?>) rule.getTail(order[i]).setBOpId(
                    bopId++);
            
            // decorate the predicate with the assigned index.
            pred = pred.setKeyOrder(keyOrder[order[i]]);

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
            
            final Scope scope = (Scope) pred.getProperty(Annotations.SCOPE);

            // @todo can we just use pred.get(3)?
            final org.openrdf.query.algebra.Var cvar = (org.openrdf.query.algebra.Var) pred
                    .getProperty(Annotations.CVAR);

            // true iff this is a quads access path.
            final boolean quads = pred.getProperty(Annotations.QUADS,
                    Annotations.DEFAULT_QUADS);

            // pull of the Sesame dataset before we strip the annotations.
            final Dataset dataset = (Dataset) pred.getProperty(Annotations.DATASET);

            // strip off annotations that we do not want to propagate.
            pred = pred.clearAnnotations(ANNS_TO_CLEAR_FROM_PREDICATE);

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
                        left = namedGraphJoin(queryEngine, context, left, anns,
                                pred, dataset, cvar);
                        break;
                    case DEFAULT_CONTEXTS:
                        left = defaultGraphJoin(queryEngine, context, left,
                                anns, pred, dataset, cvar);
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
        
        // just for now while i'm debugging
        System.err.println(toString(left));
        
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
            final PipelineOp left, final List<NV> anns, final Predicate pred) {

        final boolean scaleOut = queryEngine.isScaleOut();
        if (scaleOut) {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.SHARDED));
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
     */
    private static PipelineOp namedGraphJoin(final QueryEngine queryEngine,
            final BOpContextBase context, final PipelineOp left,
            final List<NV> anns, Predicate pred, final Dataset dataset,
            final org.openrdf.query.algebra.Var cvar) {

        final boolean scaleOut = queryEngine.isScaleOut();
        if (scaleOut) {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.SHARDED));
        } else {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.ANY));
        }

        // true iff C is bound to a constant.
        final boolean isCBound = cvar.getValue() != null;

        if (dataset == null) {

            /*
             * The dataset is all graphs. C is left unbound and the unmodified
             * access path is used.
             */

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        }

        if (isCBound) {

            /*
             * C is already bound.  The unmodified access path is used. 
             */

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        }
        
        /*
         * @todo raise this into the caller and do one per rule rather than once
         * per access path.
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
            pred = (Predicate) pred.setUnboundProperty(
                    IPredicate.Annotations.ACCESS_PATH_EXPANDER,
                    EmptyAccessPathExpander.INSTANCE);

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        }

        if (summary.nknown == 1) {

            /*
             * The dataset contains exactly one graph. Bind C.
             */

            pred = pred.asBound((IVariable) pred.get(3), new Constant(
                    summary.firstContext));

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        }

        /*
         * Estimate cost of SCAN with C unbound.
         */
        final double scanCost = queryEngine.estimateCost(context, pred);
        anns.add(new NV(Annotations.COST_SCAN, scanCost));

        // Estimate cost of SUBQUERY with C bound (sampling).
        final double subqueryCost = summary.estimateSubqueryCost(queryEngine,
                context, SAMPLE_LIMIT, pred, anns);

        if (scanCost < subqueryCost) {

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
            final IVariable var = (IVariable) pred.get(3);

            // The data set join.
            final DataSetJoin dataSetJoin = new DataSetJoin(new BOp[] { var },
                    NV.asMap(new NV[] {
                            new NV(DataSetJoin.Annotations.VAR, var),
                            new NV(DataSetJoin.Annotations.GRAPHS, summary
                                    .getGraphs()) }));

            if (scaleOut) {
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.SHARDED));
                anns
                        .add(new NV(Predicate.Annotations.REMOTE_ACCESS_PATH,
                                false));
            } else {
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.ANY));
                anns
                        .add(new NV(Predicate.Annotations.REMOTE_ACCESS_PATH,
                                false));
            }

            return new PipelineJoin(new BOp[] { left, pred }, anns
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
     */
    private static PipelineOp defaultGraphJoin(final QueryEngine queryEngine,
            final BOpContextBase context, final PipelineOp left,
            final List<NV> anns, Predicate pred, final Dataset dataset,
            final org.openrdf.query.algebra.Var cvar) {

        /*
         * @todo raise this into the caller and do one per rule rather than once
         * per access path.
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
            pred = (Predicate) pred.setUnboundProperty(
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

            // bind C.
            pred = pred.asBound((IVariable) pred.get(3), new Constant(
                    summary.firstContext));

            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());
            
            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        }
        
        if (pred.getKeyOrder().getIndexName().endsWith("C")) {

            /*
             * C is not bound. An advancer is imposed on the AP to skip to the
             * next possible triple after each match. Impose filter on AP to
             * strip off the context position. Distinct filter is not required
             * since the advancer pattern used will not report duplicates.
             */
            
            // Set the CURSOR flag.
            pred = (Predicate) pred.setProperty(IPredicate.Annotations.FLAGS,
                    pred.getProperty(IPredicate.Annotations.FLAGS,
                            IPredicate.Annotations.DEFAULT_FLAGS)
                            | IRangeQuery.CURSOR);

            // Set Advancer (runs at the index).
            pred = pred.addIndexLocalFilter(new ContextAdvancer());
            
            // Filter to strip off the context position.
            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());

            if(scaleOut) {

                /*
                 * When true, an ISimpleSplitHandler guarantees that no triple
                 * on that index spans more than one shard.
                 * 
                 * @todo Implement the split handler and detect when it is being
                 * used.
                 */
                final boolean shardTripleConstraint = false;

                if (shardTripleConstraint) {

                    // JOIN is SHARDED.
                    pred = (Predicate) pred.setProperty(
                            BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.SHARDED);

                    // AP is LOCAL.
                    pred = (Predicate) pred.setProperty(
                            IPredicate.Annotations.REMOTE_ACCESS_PATH, false);

                } else {

                    // JOIN is ANY.
                    pred = (Predicate) pred.setProperty(
                            BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.ANY);

                    // AP is REMOTE.
                    pred = (Predicate) pred.setProperty(
                            IPredicate.Annotations.REMOTE_ACCESS_PATH, true);

                }

            }

            return new PipelineJoin(new BOp[] { left, pred }, anns
                    .toArray(new NV[anns.size()]));

        }

        // Estimate cost of SCAN with C unbound.
        final double scanCost = queryEngine.estimateCost(context, pred);
        anns.add(new NV(Annotations.COST_SCAN, scanCost));

        /*
         * Estimate cost of SUBQUERY with C bound (sampling). A large value is
         * used if the dataset is null since the default graph query will run
         * against all contexts and we are better off doing a SCAN.
         */
        final double subqueryCost = dataset == null ? Double.MAX_VALUE
                : summary.estimateSubqueryCost(queryEngine, context,
                        SAMPLE_LIMIT, pred, anns);

        if (scanCost < subqueryCost) {

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
             * 
             * FIXME This needs to be implemented based on an expander pattern
             * which we can capture from DefaultGraphExpander.
             */

            throw new UnsupportedOperationException();
            
//            /*
//             * Setup the data set join.
//             * 
//             * @todo When the #of named graphs is large we need to do something
//             * special to avoid sending huge graph sets around with the query.
//             * For example, we should create named data sets and join against
//             * them rather than having an in-memory DataSetJoin.
//             * 
//             * @todo The historical approach performed parallel subquery using
//             * an expander pattern rather than a data set join. The data set
//             * join should have very much the same effect, but it may need to
//             * emit multiple chunks to have good parallelism.
//             */
//
//            // The variable to be bound.
//            final IVariable var = (IVariable) pred.get(3);
//
//            // The data set join.
//            final DataSetJoin dataSetJoin = new DataSetJoin(new BOp[] { var },
//                    NV.asMap(new NV[] {
//                            new NV(DataSetJoin.Annotations.VAR, var),
//                            new NV(DataSetJoin.Annotations.GRAPHS, summary
//                                    .getGraphs()) }));
//
//            if (scaleOut) {
//                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
//                        BOpEvaluationContext.SHARDED));
//                anns
//                        .add(new NV(Predicate.Annotations.REMOTE_ACCESS_PATH,
//                                false));
//            } else {
//                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
//                        BOpEvaluationContext.ANY));
//                anns
//                        .add(new NV(Predicate.Annotations.REMOTE_ACCESS_PATH,
//                                false));
//            }
//
//            return new PipelineJoin(new BOp[] { left, pred }, anns
//                    .toArray(new NV[anns.size()]));

        }

    }

    /**
     * Pretty print (aspects of) a bop.
     * 
     * @param bop
     *            The bop.
     *            
     * @return The formatted representation.
     */
    private static String toString(final BOp bop) {
        
        StringBuilder sb = new StringBuilder();
        
        toString(bop, sb, 0);
        
        // chop off the last \n
        sb.setLength(sb.length() - 1);

        return sb.toString();

    }

    private static void toString(final BOp bop, final StringBuilder sb,
            final int indent) {

        for (int i = 0; i < indent; i++) {
            sb.append(' ');
        }
        sb.append(bop).append('\n');

        if (bop != null) {
            final List<BOp> args = bop.args();
            for (BOp arg : args) {
                toString(arg, sb, indent + 4);
            }
            final IConstraint[] constraints = (IConstraint[]) bop
                    .getProperty(PipelineJoin.Annotations.CONSTRAINTS);
            if (constraints != null) {
                for (IConstraint c : constraints) {
                    toString(c, sb, indent + 4);
                }
            }
        }

    }
    
    /**
     * Convert a program into an operator tree.
     * 
     * @param program
     * 
     * @return
     * 
     * FIXME What is the pattern for UNION?
     */
    public static PipelineOp convert(final IProgram rule, final int startId,
            final AbstractTripleStore db, final QueryEngine queryEngine) {

        throw new UnsupportedOperationException();

    }

    /**
     * Helper class summarizes the named graphs for a quads mode query.
     * 
     * @todo This could be used for either named or default graphs. All it does
     *       not report the #of URIs known to the database.
     * 
     * @todo This summary could be computed once for a given query for its named
     *       graphs and once for its default graph. We do not need to do this
     *       for each predicate in the query.
     */
    private static class DataSetSummary {

        /**
         * The set of graphs. The {@link URI}s MUST have been resolved against
         * the appropriate {@link LexiconRelation} such that their term
         * identifiers (when the exist) are known. If any term identifier is
         * {@link IRawTripleStore#NULL}, then the corresponding graph does not
         * exist and no access path will be queried for that graph. However, a
         * non- {@link IRawTripleStore#NULL} term identifier may also identify a
         * graph which does not exist, in which case an access path will be
         * created for that {@link URI}s but will not visit any data.
         */
        public final Iterable<? extends URI> graphs;

        /**
         * The #of graphs in {@link #graphs} whose term identifier is known.
         * While this is not proof that there is data in the quad store for a
         * graph having the corresponding {@link URI}, it does allow the
         * possibility that a graph could exist for that {@link URI}.
         */
        public final int nknown;
//        * <p>
//        * If {@link #nknown} is ZERO (0), then the access path is empty.
//        * <p>
//        * If {@link #nknown} is ONE (1), then the caller's {@link IAccessPath}
//        * should be used and filtered to remove the context information. If
//        * {@link #graphs} is <code>null</code>, which implies that ALL graphs
//        * in the quad store will be used as the default graph, then
//        * {@link #nknown} will be {@link Integer#MAX_VALUE}.

        /**
         * The term identifier for the first graph and
         * {@link IRawTripleStore#NULL} if no graphs were specified having a
         * term identifier.
         */
        public final IV firstContext;

        /**
         * 
         * @param graphs
         *            The set of named graphs in the SPARQL DATASET (optional).
         *            A runtime exception will be thrown during evaluation of
         *            the if the {@link URI}s are not {@link BigdataURI}s. If
         *            <code>graphs := null</code>, then the set of named graphs
         *            is understood to be ALL graphs in the quad store.
         */
        public DataSetSummary(final Iterable<? extends URI> graphs) {

            this.graphs = graphs;

            IV firstContext = null;

            if (graphs == null) {

                nknown = Integer.MAX_VALUE;

            } else {

                final Iterator<? extends URI> itr = graphs.iterator();

                int nknown = 0;

                while (itr.hasNext()) {

                    final BigdataURI uri = (BigdataURI) itr.next();

                    if (uri.getIV() != null) {

                        if (++nknown == 1) {

                            firstContext = uri.getIV();

                        }

                    }

                } // while

                this.nknown = nknown;

            }

            this.firstContext = firstContext;

        }

        /**
         * Return a dense array of the {@link IV}s for the graphs known to the
         * database.
         */
        public IV[] getGraphs() {
            
            final IV[] a = new IV[nknown];
            
            final Iterator<? extends URI> itr = graphs.iterator();

            int nknown = 0;

            while (itr.hasNext()) {

                final BigdataURI uri = (BigdataURI) itr.next();

                final IV id = uri.getIV();

                if (id != null) {

                    a[nknown++] = id;

                }

            } // while
            
            return a;
            
        }

        /**
         * Estimate cost of SUBQUERY with C bound (sampling).
         * 
         * @param queryEngine
         * @param context
         * @param limit
         *            The maximum #of samples to take.
         * @param pred
         *            The predicate.
         * @param anns
         *            A list of annotations to which the cost estimate data will
         *            be added.
         * 
         * @return The estimated cost. This is adjusted based on the sample size
         *         and the #of graphs against which the query was issued and
         *         represents the total expected cost of the subqueries against
         *         all of the graphs in the {@link Dataset}.
         * 
         * @todo Subquery will be less efficient than a scan when the access
         *       path is remote since there will be remote requests. This model
         *       does not capture that additional overhead. We need to measure
         *       the overhead using appropriate data sets and queries and then
         *       build it into the model. The overhead itself could be changed
         *       dramatically by optimizations in the
         *       {@link FederatedQueryEngine} and the {@link ResourceService}.
         * 
         * @todo This should randomly sample in case there is bias in the order
         *       in which the URIs are presented here. However, the only thing
         *       which would be likely to create a strong bias is if someone
         *       sorted them on the IVs or if the URIs were in the same ordering
         *       in which their IVs were assigned AND the data were somehow
         *       correlated with that order. I rate the latter as pretty
         *       unlikely and the former is not true, so this sampling approach
         *       should be pretty good.
         */
        public double estimateSubqueryCost(QueryEngine queryEngine,
                BOpContextBase context, int limit, Predicate pred, List<NV> anns) {
            double subqueryCost = 0d;
            int nsamples = 0;
            for (URI uri : graphs) {
                if (nsamples == limit)
                    break;
                final IV graph = ((BigdataURI) uri).getIV();
                subqueryCost += queryEngine.estimateCost(context, pred.asBound(
                        (IVariable) pred.get(3), new Constant(graph)));
                nsamples++;
            }
            subqueryCost = (subqueryCost * nknown) / nsamples;

            anns.add(new NV(Annotations.COST_SUBQUERY, subqueryCost));
            anns.add(new NV(Annotations.COST_SUBQUERY_SAMPLE_COUNT, nsamples));
            return subqueryCost;
        }

    } // DataSetSummary

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
