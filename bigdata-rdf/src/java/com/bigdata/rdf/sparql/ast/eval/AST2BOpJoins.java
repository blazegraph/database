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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.filter.BOpFilterBase;
import com.bigdata.bop.ap.filter.DistinctFilter;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.cost.ScanCostReport;
import com.bigdata.bop.cost.SubqueryCostReport;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.AccessPathJoinAnnotations;
import com.bigdata.bop.join.HTreeHashJoinAnnotations;
import com.bigdata.bop.join.HTreeHashJoinOp;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.JVMHashJoinOp;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.filter.HTreeDistinctFilter;
import com.bigdata.bop.rdf.filter.StripContextFilter;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.InGraphHashSetFilter;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.EmptyAccessPathExpander;

/**
 * Class handles join patterns.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AST2BOpJoins extends AST2BOpFilters {

    private static final Logger log = Logger.getLogger(AST2BOpFilters.class);

    /**
     * 
     */
    protected AST2BOpJoins() {
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
                new LinkedHashSet<IVariable<?>>()/* doneSet */, constraints,
                new BOpContextBase(queryEngine), idFactory, queryHints);

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

        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization =
                new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        // Add constraints to the join for that predicate.
        anns.add(new NV(JoinAnnotations.CONSTRAINTS, getJoinConstraints(
                constraints, needsMaterialization)));
        
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
                 * This is basically the old way of handling quads query using
                 * expanders which were attached by toPredicate() in
                 * BigdataEvaluationStrategyImpl.
                 * 
                 * FIXME Remove this code path and the expander patterns from
                 * the code base.
                 */

                final boolean scaleOut = queryEngine.isScaleOut();

                if (scaleOut)
                    throw new UnsupportedOperationException();

                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.ANY));

                anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

                left = newJoin(left, anns, queryHints,
                        false/* defaultGraphFilter */, null/* summary */);

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
        left = addMaterializationSteps(left, doneSet, needsMaterialization, db,
                queryEngine, idFactory, context, queryHints);

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

        return newJoin(left, anns, queryHints, false/* defaultGraphFilter */,
                null/* summary */);

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

            return newJoin(left, anns, queryHints,
                    false/* defaultGraphFilter */, null/* summary */);

        }

        if (pred.get(3/* c */).isConstant()) {

            /*
             * C is already bound. The unmodified access path is used.
             */

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

            return newJoin(left, anns, queryHints,
                    false/* defaultGraphFilter */, null/* summary */);
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

            return newJoin(left, anns, queryHints,
                    false/* defaultGraphFilter */, summary);

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

            return newJoin(left, anns, queryHints,
                    false/* defaultGraphFilter */, summary);

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

            return newJoin(left, anns, queryHints,
                    false/* defaultGraphFilter */, summary);

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

            return newJoin(dataSetJoin, anns, queryHints,
                    false/* defaultGraphFilter */, summary);

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
            
            anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

            return newJoin(left, anns, queryHints, true/* defaultGraphFilter */,
                    summary);
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

            return newJoin(left, anns, queryHints,
                    false/* defaultGraphFilter */, summary);

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

            return newJoin(left, anns, queryHints,
                    false/* defaultGraphFilter */, summary);

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

//            // Filter for distinct SPOs.
//            pred = pred.addAccessPathFilter(newDistinctFilter(pred, summary));

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

            return newJoin(left, anns, queryHints,
                    true/* defaultGraphFilter */, summary);

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

//            // Filter for distinct SPOs.
//            pred = pred.addAccessPathFilter(newDistinctFilter(pred, summary));

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
           
            return newJoin(left, anns, queryHints,
                    true/* defaultGraphFilter */, summary);
            
        }

    }

    /**
     * Return the distinct filter used for a default graph join (distinct SPOs).
     * <p>
     * Note: The native memory based DISTINCT filter MUST NOT be used for
     * pipelined joins. Pipelined joins run "as-bound" and the per-as-bound
     * cardinality is typically very small (1s to 1000s). However, a hash join
     * can hit a very large cardinality for the default graph distinct filter
     * since it sees all SPOs at once.
     * 
     * @param pred
     *            The predicate.
     * @param summary
     *            The {@link DataSetSummary} (when available).
     * @param hashJoin
     *            <code>true</code> iff a hash join was chosen for this
     *            predicate.
     * @return
     * 
     *         TODO The HTree variant of the distinct filter is more scalable
     *         but it does not have access to the memory manager for the
     *         IRunningQuery and hence will allocate its own memory manager,
     *         which means that it will use at a minimum of 1M of native memory.
     *         We need to look at the integration of the filters with the
     *         IRunningQuery to fix this.
     * 
     *         TODO The HTree benefits very much from vectoring. However, in
     *         order to vector the filter we need to be operating on IV chunks.
     *         Maybe we can resolve this issue and the issue of access to the
     *         memory manager at the same time?
     */
    static private BOpFilterBase newDistinctFilter(final Predicate<?> pred,
            final DataSetSummary summary, final boolean hashJoin) {
        
        // Never use native distinct for as-bound "pipeline" joins.
        boolean nativeDistinct = hashJoin && AST2BOpBase.nativeDefaultGraph;
        
        if (nativeDistinct) {
            /*
             * Examine the cardinality of the predicate to determine whether or
             * not we should use a DISTINCT SPO filter backed by a persistence
             * capable data structure against native memory.
             */
            final Long rangeCount = (Long) pred
                    .getProperty(Annotations.ESTIMATED_CARDINALITY);
            if (rangeCount != null) {
                if (rangeCount.longValue() < nativeDefaultGraphThreshold) {
                    // Small range count.
                    nativeDistinct = false;
                }
            } else {
                log.warn("No rangeCount? : " + pred);
            }
        }
        /*
         * Note: I think that the predicate cardinality is probably much more
         * important than the #of different contexts in the default graph. You
         * can have two contexts and 2B cardinality on the range count and wind
         * up with a DISTINCT 2B SPOs. Therefore I have disabled the following
         * code path.
         */
        if (false && nativeDistinct && summary != null) {
            /*
             * Examine the cardinality of the defaultGraph *contexts*.
             */
            if (summary.nknown < nativeDefaultGraphThreshold) {
                // Only a few graphs in the defaultGraph.
                nativeDistinct = false;
            }
        }
        if (nativeDistinct) {
            // Native memory based DISTINCT filter.
            return HTreeDistinctFilter.newInstance();
        } else {
            // JVM Based DISTINCT filter.
            return DistinctFilter.newInstance();
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
     * @param defaultGraphFilter
     *            <code>true</code> iff a DISTINCT filter must be imposed on the
     *            SPOs. This is never done for a named graph query. It is
     *            normally done for default graph queries, but there are some
     *            edge cases where the SPOs are provably distinct and we do not
     *            need to bother.
     * @param summary
     *            The {@link DataSetSummary} (when available).
     * @return
     * 
     * @see Annotations#HASH_JOIN
     * @see HashJoinAnnotations#JOIN_VARS
     * @see Annotations#ESTIMATED_CARDINALITY
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static private PipelineOp newJoin(PipelineOp left, final List<NV> anns,
            final Properties queryHints, final boolean defaultGraphFilter,
            final DataSetSummary summary) {

        final Map<String, Object> map = NV.asMap(anns.toArray(new NV[anns
                .size()]));

        // Look up the predicate for the access path.
        Predicate<?> pred = (Predicate<?>) map
                .get(AccessPathJoinAnnotations.PREDICATE);

        // MAX_LONG unless we are doing cutoff join evaluation.
        final long limit = pred.getProperty(JoinAnnotations.LIMIT,
                JoinAnnotations.DEFAULT_LIMIT);

        // True iff a hash join was requested for this predicate.
        final boolean hashJoin = limit == JoinAnnotations.DEFAULT_LIMIT
                && pred.getProperty(Annotations.HASH_JOIN,
                        Annotations.DEFAULT_HASH_JOIN);
        
        if (defaultGraphFilter) {

            /*
             * Filter for distinct SPOs.
             */

            pred = pred.addAccessPathFilter(newDistinctFilter(pred, summary,
                    hashJoin));
            
        }
        
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
            
            // FIXME From query hint / AST2BaseContext default.
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
            if (evaluationContext == BOpEvaluationContext.CONTROLLER) {
                // This is not necessary, but it makes the hash join stats
                // immediately visible.
                map.put(PipelineOp.Annotations.SHARED_STATE, true);
            }
            map.put(BOp.Annotations.EVALUATION_CONTEXT, evaluationContext);

            map.put(PipelineOp.Annotations.MAX_PARALLEL, 1);

            if (useHTree) {

                map.put(PipelineOp.Annotations.MAX_MEMORY, Long.MAX_VALUE);
                
                map.put(PipelineOp.Annotations.LAST_PASS, true);

                map.put(HTreeHashJoinAnnotations.RELATION_NAME,
                        pred.getRequiredProperty(Predicate.Annotations.RELATION_NAME));

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
