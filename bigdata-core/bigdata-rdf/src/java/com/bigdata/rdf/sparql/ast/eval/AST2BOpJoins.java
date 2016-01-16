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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.ap.filter.BOpFilterBase;
import com.bigdata.bop.ap.filter.DistinctFilter;
import com.bigdata.bop.cost.ScanCostReport;
import com.bigdata.bop.cost.SubqueryCostReport;
import com.bigdata.bop.join.AccessPathJoinAnnotations;
import com.bigdata.bop.join.DistinctTermScanOp;
import com.bigdata.bop.join.FastRangeCountOp;
import com.bigdata.bop.join.HTreeHashJoinAnnotations;
import com.bigdata.bop.join.HTreeHashJoinOp;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.HashJoinOp;
import com.bigdata.bop.join.JVMHashJoinOp;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.filter.NativeDistinctFilter;
import com.bigdata.bop.rdf.filter.StripContextFilter;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.InGraphHashSetFilter;
import com.bigdata.rdf.spo.SPOKeyOrder;
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
     * Add a join for a statement pattern. This handles triples-mode,
     * named-graph and default graph join patterns whether on a single machine
     * or on a cluster.
     * 
     * @param left
     * @param pred
     *            The predicate describing the statement pattern.
     * @param doneSet
     *            The set of variables already known to be materialized.
     * @param constraints
     *            Constraints on that join (optional).
     * @param cutoffLimit
     *            When non-null, this is the limit for a cutoff join (RTO).
     * @param queryHints
     *            Query hints associated with that {@link StatementPatternNode}.
     * @param ctx
     *            The evaluation context.
     */
    @SuppressWarnings("rawtypes")
    public static PipelineOp join(//
            PipelineOp left,//
            Predicate pred,//
            final Set<IVariable<?>> doneSet,// 
            final Collection<IConstraint> constraints,//
            final Long cutoffLimit, //
            final Properties queryHints,//
            final AST2BOpContext ctx//
            ) {

        final int joinId = ctx.nextId();

        // annotations for this join.
        final List<NV> anns = new LinkedList<NV>();

        anns.add(new NV(BOp.Annotations.BOP_ID, joinId));

        /*
         * A map containing the materialization requirements for each constraint
         * attached to this join. This is populated as a side-effect by
         * getJoinConstraints() (immediately below). If this map is NOT empty
         * then the JOIN operator will be followed by either manditory or
         * conditional materialization of variable bindings in order to evaluate
         * one or more constraints.
         */
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization =
                new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        /*
         * Add constraints to the join for that predicate.
         * 
         * Note: If we are performing cutoff evaluation of a JOIN
         * [cutoffLimit!=null], then this disables the conditional routing logic
         * for constraints with SOMETIMES materialization requirements. This is
         * necessary in order to preserve the order of evaluation. Conditional
         * routing of solutions causes them to be reordered and that breaks the
         * ability to accurately estimate the cardinality of the JOIN using
         * cutoff evaluation.
         */
        anns.add(new NV(JoinAnnotations.CONSTRAINTS,
                getJoinConstraints2(constraints, needsMaterialization,
                        cutoffLimit == null/* conditionalRouting */)));

        // true iff there are no constraints that require materialization.
        anns.add(new NV(Annotations.SIMPLE_JOIN, needsMaterialization.isEmpty()));
        
        /*
         * Pull off annotations before we clear them from the predicate.
         */
        final Scope scope = (Scope) pred.getProperty(Annotations.SCOPE);

        // true iff this is a quads access path.
        final boolean quads = pred.getProperty(Annotations.QUADS,
                Annotations.DEFAULT_QUADS);

        // when non-null use distinct-term-scan. see #1035.
		final VarNode distinctTermScanVar = (VarNode) pred
				.getProperty(StatementPatternNode.Annotations.DISTINCT_TERM_SCAN_VAR);

        // when non-null use fast-range-count. see #1037.
		final VarNode fastRangeCountVar = (VarNode) pred
				.getProperty(StatementPatternNode.Annotations.FAST_RANGE_COUNT_VAR);
        
        // pull off the Sesame dataset before we strip the annotations.
        final DatasetNode dataset = (DatasetNode) pred
                .getProperty(Annotations.DATASET);

        // strip off annotations that we do not want to propagate.
		pred = pred.clearAnnotations(new String[] { Annotations.SCOPE,
				Annotations.QUADS, Annotations.DATASET,
				StatementPatternNode.Annotations.DISTINCT_TERM_SCAN_VAR,
				StatementPatternNode.Annotations.FAST_RANGE_COUNT_VAR });

		if (fastRangeCountVar != null) {

			// fast-range-count. see #1037.
			left = fastRangeCountJoin(left, anns, pred, dataset, cutoffLimit,
					fastRangeCountVar, queryHints, ctx);
			
			return left;
			
		}

		if (distinctTermScanVar != null) {
			
			// distinct-term-scan. see #1035.
			left = distinctTermScanJoin(left, anns, pred, dataset, cutoffLimit,
					distinctTermScanVar, queryHints, ctx);
			
			return left;
			
		}
		
        if (quads) {

            /*
             * Quads mode.
             */

            // TODO Verifying that the expanders are not present.  Take this  
            // assert out once we have proven that the expanders are not present.
            assert pred.getProperty(IPredicate.Annotations.ACCESS_PATH_EXPANDER) == null;

            switch (scope) {
            case NAMED_CONTEXTS:
                left = namedGraphJoin(left, anns, pred, dataset, cutoffLimit,
                        queryHints, ctx);
                break;
            case DEFAULT_CONTEXTS:
                left = defaultGraphJoin(left, anns, pred, dataset, cutoffLimit,
                        queryHints, ctx);
                break;
            default:
                throw new AssertionError();
            }

        } else {

            /*
             * Triples or provenance mode.
             */

            left = triplesModeJoin(left, anns, pred, cutoffLimit, queryHints,
                    ctx);

        }

        if (needsMaterialization.isEmpty()) {
         
            // No filters.
            return left;
            
        }

        /*
         * Add operators to materialization variables (as necessary) and
         * evaluate filters.
         */
        if (cutoffLimit != null) {

            left = addNonConditionalMaterializationSteps(left, doneSet,
                    needsMaterialization, cutoffLimit, queryHints, ctx);

        } else {

            /*
             * For each filter which requires materialization steps, add the
             * materializations steps to the pipeline and then add the
             * filter to the pipeline.
             * 
             * Note: This is the old code path. This code path not support
             * cutoff evaluation of joins because it can reorder the
             * solutions.
             */

            left = addMaterializationSteps3(left, doneSet,
                    needsMaterialization, queryHints, ctx);

        }

        return left;

    }

    /**
	 * FIXME We need to handle cutoff joins here or the distinct-term-scan will
	 * not work with the RTO (alternatively, make sure the RTO is only using
	 * pipeline joins when sampling the join graph).
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1035" > DISTINCT PREDICATEs
	 *      query is slow </a>
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
    private static PipelineOp distinctTermScanJoin(//
			final PipelineOp left,//
			final List<NV> anns, //
			Predicate pred,//
			final DatasetNode dataset, //
			final Long cutoffLimitIsIgnored,//
			final VarNode distinctTermScanVar, //
			final Properties queryHints, //
			final AST2BOpContext ctx//
			) {

		final IVariable distinctVar = distinctTermScanVar.getValueExpression();

		anns.add(new NV(DistinctTermScanOp.Annotations.DISTINCT_VAR,
				distinctVar));

		// A mock constant used for predicate in which the distinctVar is not
		// yet bound.
		final Constant<IV> mockConst = new Constant<IV>(TermId.mockIV(VTE.URI));

		// ensure distinctVar is bound in mockPred.
		final IPredicate mockPred = pred.asBound(distinctVar, mockConst);

		final SPOKeyOrder keyOrder = SPOKeyOrder.getKeyOrder(mockPred,
				ctx.isQuads() ? 4 : 3);

		// Override the key order.
		pred = (Predicate) pred.setProperty(IPredicate.Annotations.KEY_ORDER,
				keyOrder);

        anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

		return applyQueryHints(
				new DistinctTermScanOp(leftOrEmpty(left), NV.asMap(anns
						.toArray(new NV[anns.size()]))), queryHints, ctx);
		
	}

    /**
	 * Use the {@link FastRangeCountOp} rather than a key-range scan.
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1037" > Rewrite SELECT
	 *      COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern} as ESTCARD
	 *      </a>
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static PipelineOp fastRangeCountJoin(//
			final PipelineOp left,//
			final List<NV> anns, //
			final Predicate pred,//
			final DatasetNode dataset, //
			final Long cutoffLimitIsIgnored,//
			final VarNode fastRangeCountVar, //
			final Properties queryHints, //
			final AST2BOpContext ctx//
			) {

		if ( ctx.gpuEvaluation != null
		     && pred.getProperty(GpuAnnotations.EVALUATE_ON_GPU,
		                         GpuAnnotations.DEFAULT_EVALUATE_ON_GPU) )
		{
         return ctx.gpuEvaluation.fastRangeCountJoin( left,
                                                      anns,
                                                      pred,
                                                      dataset,
                                                      cutoffLimitIsIgnored,
                                                      fastRangeCountVar,
                                                      queryHints,
                                                      ctx );
		}

		anns.add(new NV(FastRangeCountOp.Annotations.COUNT_VAR,
				fastRangeCountVar.getValueExpression()));

        anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

		return applyQueryHints(
				new FastRangeCountOp(leftOrEmpty(left), NV.asMap(anns
						.toArray(new NV[anns.size()]))), queryHints, ctx);
		
	}

	/**
     * Generate a {@link PipelineJoin} for a triples mode access path.
     * 
     * @param ctx
     * @param left
     * @param anns
     * @param pred
     * @param queryHints
     * 
     * @return The join operator.
     */
    private static PipelineOp triplesModeJoin(//
            final PipelineOp left, //
            final List<NV> anns, //
            Predicate<?> pred,//
            final Long cutoffLimit,//
            final Properties queryHints,//
            final AST2BOpContext ctx) {

        final boolean scaleOut = ctx.isCluster();

        if (scaleOut && !ctx.remoteAPs) {

            /*
             * All triples queries can run shard-wise in scale-out.
             */

            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.SHARDED));

            pred = (Predicate<?>) pred.setProperty(
                    Predicate.Annotations.REMOTE_ACCESS_PATH, false);

        } else {

            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.ANY));

        }

        anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

        return newJoin(left, anns, false/* defaultGraphFilter */,
                null/* summary */, cutoffLimit, queryHints, ctx);

    }

    /**
     * Generate a named graph join (quads mode).
     *
     * @param ctx
     * @param left
     * @param anns
     * @param pred
     * @param dataset
     * @return
     *
     * @todo If the context position is shared by some other variable which we
     *       know to be bound based on the selected join order, then we need to
     *       treat the context variable as bound during this analysis.
     *
     * @todo Since we do not know the specific asBound values, but only that
     *       they will be bound, we should defer the SCAN versus SUBQUERY
     *       decision until we actually evaluate that access path. This is
     *       basically a special case of runtime query optimization.
     */
    private static PipelineOp namedGraphJoin(//
            PipelineOp left, //
            final List<NV> anns, //
            Predicate<?> pred,//
            final DatasetNode dataset, //
            final Long cutoffLimit,//
            final Properties queryHints,//
            final AST2BOpContext ctx) {

        final boolean scaleOut = ctx.isCluster();
        if (scaleOut && !ctx.remoteAPs) {
            /*
             * All named graph patterns in scale-out are partitioned (sharded).
             */
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.SHARDED));
            pred = (Predicate<?>) pred.setProperty(
                    Predicate.Annotations.REMOTE_ACCESS_PATH, false);
        } else {
            anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.ANY));
        }

		if (dataset == null || dataset.getNamedGraphs() == null) {

            /*
             * The dataset is all graphs. C is left unbound and the unmodified
             * access path is used.
             */

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

            return newJoin(left, anns, false/* defaultGraphFilter */,
                    null/* summary */, cutoffLimit, queryHints, ctx);

        }

        if (pred.get(3/* c */).isConstant()) {

            /*
             * C is already bound. The unmodified access path is used.
             */

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

            return newJoin(left, anns, false/* defaultGraphFilter */,
                    null/* summary */, cutoffLimit, queryHints, ctx);
        }

        /*
         * Note: While a query can mix default and named graph access paths,
         * there is only one named graph collection and one default graph
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

            return newJoin(left, anns, false/* defaultGraphFilter */, summary,
                    cutoffLimit, queryHints, ctx);

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
             */

            pred = pred.asBound((IVariable<?>) pred.get(3),
                    new Constant<IV<?, ?>>((IVariable) pred.get(3),
                            summary.firstContext));

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));

            return newJoin(left, anns, false/* defaultGraphFilter */, summary,
                    cutoffLimit, queryHints, ctx);

        }

        /*
         */
        final int accessPathSampleLimit = pred.getProperty(
                QueryHints.ACCESS_PATH_SAMPLE_LIMIT, ctx.accessPathSampleLimit);
        final boolean estimateCosts = accessPathSampleLimit >= 0;
        @SuppressWarnings("rawtypes")
        final IRelation r = ctx.context.getRelation(pred);
        final ScanCostReport scanCostReport;
        final SubqueryCostReport subqueryCostReport;
        final boolean scanAndFilter;

        if (estimateCosts) {

            /*
             * Estimate cost of SCAN with C unbound.
             * 
             * Note: We need to use global index view in order to estimate the
             * cost of the scan even though the scan will be shard-wise when we
             * actually run the query.
             */
            scanCostReport = ((AccessPath) ctx.context.getAccessPath(r,
                    (Predicate<?>) pred.setProperty(
                            IPredicate.Annotations.REMOTE_ACCESS_PATH, true)))
                    .estimateCost();

            anns.add(new NV(Annotations.COST_SCAN, scanCostReport));

            /*
             * Estimate cost of SUBQUERY with C bound (sampling).
             * 
             * Note: Again, we need to use a remote index view in order to
             * estimate the cost of the subqueries even though we will use
             * sharded joins when actually running the query.
             */
            subqueryCostReport = summary.estimateSubqueryCost(ctx.context,
                    accessPathSampleLimit, (Predicate<?>) pred.setProperty(
                            IPredicate.Annotations.REMOTE_ACCESS_PATH, true));

            anns.add(new NV(Annotations.COST_SUBQUERY, subqueryCostReport));
            
            scanAndFilter = subqueryCostReport == null
                    || scanCostReport.cost < subqueryCostReport.cost;
            
        } else {
            
            scanCostReport = null;

            subqueryCostReport = null;
            
            scanAndFilter = pred.getProperty(
                    QueryHints.ACCESS_PATH_SCAN_AND_FILTER,
                    ctx.accessPathScanAndFilter);
            
        }

        if (scanAndFilter) {

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

            return newJoin(left, anns, false/* defaultGraphFilter */, summary,
                    cutoffLimit, queryHints, ctx);

        } else {

            /*
             * Setup the data set join (aka parallel subquery).
             */

            // The variable to be bound.
            final IVariable<?> var = (IVariable<?>) pred.get(3);

            // The data set join.
            left = new DataSetJoin(leftOrEmpty(left),
                    new NV(DataSetJoin.Annotations.VAR, var),//
                    new NV(DataSetJoin.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(DataSetJoin.Annotations.GRAPHS, summary.getGraphs()) //
            );

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

            return newJoin(left, anns, false/* defaultGraphFilter */, summary,
                    cutoffLimit, queryHints, ctx);

        }

    }

    /**
     * Generate a default graph join (quads mode).
     *
     * @param ctx
     * @param left
     * @param anns
     * @param pred
     * @param dataset
     * @return
     *
     * @todo Since we do not know the specific asBound values, but only that
     *       they will be bound, we should defer the SCAN versus SUBQUERY
     *       decision until we actually evaluate that access path. This is
     *       basically a special case of runtime query optimization.
     */
    @SuppressWarnings("rawtypes")
    private static PipelineOp defaultGraphJoin(//
            PipelineOp left, //
            final List<NV> anns, //
            Predicate<?> pred,//
            final DatasetNode dataset, //
            final Long cutoffLimit,//
            final Properties queryHints,//
            final AST2BOpContext ctx) {

        final DataSetSummary summary = dataset == null ? null
                : dataset.getDefaultGraphs();

        final boolean scaleOut = ctx.isCluster();

        if (dataset != null && summary == null) {

            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());
            
            anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

            return newJoin(left, anns, ctx.defaultGraphDistinctFilter, summary,
                    cutoffLimit, queryHints, ctx);

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

            return newJoin(left, anns, false/* defaultGraphFilter */, summary,
                    cutoffLimit, queryHints, ctx);

        }

        if (summary != null && summary.nknown == 1) {

            /*
             * The dataset contains exactly one graph. Bind C. Add a filter to
             * strip off the context position.
             */

            // Bind C.
            pred = pred.asBound((IVariable<?>) pred.get(3),
                    new Constant<IV<?, ?>>(summary.firstContext));

            if (scaleOut && !ctx.remoteAPs) {
                // use a partitioned join.
                anns.add(new NV(Predicate.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.SHARDED));
                pred = (Predicate) pred.setProperty(
                        Predicate.Annotations.REMOTE_ACCESS_PATH, false);
            }

            // Strip of the context position (do not project C from the join).
            pred = pred.addAccessPathFilter(StripContextFilter.newInstance());

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE, pred));

            return newJoin(left, anns, false/* defaultGraphFilter */, summary,
                    cutoffLimit, queryHints, ctx);

        }

        /*
         * TODO This optimization COULD be decided statically if we marked the
         * predicate with the index would would be used when it was evaluated.
         * That is known in advance EXCEPT when some joins are optional,
         * in which case the actual index can not be known until runtime. The
         * code which attaches the "as-bound" index to the predicate MUST also
         * consider the exogenous variables (if any). This might be done in the
         * static join order optimizer, which does consider each join group even
         * if it does not reorder the joins in a given group (because it was
         * disabled for that group).
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
         * Decide on a SCAN+FILTER or PARALLEL SUBQUERY plan for the default
         * graph AP.
         * 
         * Note: The DataSetJoin approach used for named graph access paths does
         * not work for default graph access paths because it changes the scope
         * of the DISTINCT SPO filter and winds up letting through duplicate
         * SPOs.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/407
         */
        final int accessPathSampleLimit = pred.getProperty(
                QueryHints.ACCESS_PATH_SAMPLE_LIMIT, ctx.accessPathSampleLimit);
        final boolean estimateCosts = accessPathSampleLimit >= 0;
        final ScanCostReport scanCostReport;
        final SubqueryCostReport subqueryCostReport;
        final boolean scanAndFilter;
    
        if(true) {
        
            /*
             * TODO The "DGExpander" code appears to function correctly, but it
             * can do way too much work and take way too long as the #of graphs
             * in the data set increases for at least some shapes of the data
             * and the queries. However, we currently lack a means to detect
             * cases where the PARALLEL SUBQUERY plan is faster than the
             * SCAN+FILTER. The approach coded here does not make the correct
             * decisions for reasons which seem to have more to do with the data
             * density / sparsity for the APs which would be used for
             * SCAN+FILTER versus PARALLEL SUBQUERY. Therefore the PARALLEL
             * SUBQUERY path for default graph access paths is currently
             * disabled.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/407
             */
            
            scanCostReport = null;
            subqueryCostReport = null;
            scanAndFilter = true;
        
        } else {
        
            if (estimateCosts) {
    
                /*
                 * Estimate cost of SCAN with C unbound.
                 * 
                 * Note: We need to use the global index view in order to estimate
                 * the cost of the scan regardless of whether the query runs with
                 * partitioned or global index views when it is evaluated.
                 */
                final IRelation r = ctx.context.getRelation(pred);
                scanCostReport = ((AccessPath) ctx.context.getAccessPath(r,
                        (Predicate<?>) pred.setProperty(
                                IPredicate.Annotations.REMOTE_ACCESS_PATH, true)))
                        .estimateCost();
    
                anns.add(new NV(Annotations.COST_SCAN, scanCostReport));
    
                /*
                 * Estimate cost of SUBQUERY with C bound (sampling).
                 * 
                 * Note: We need to use the global index view in order to estimate
                 * the cost of the scan regardless of whether the query runs with
                 * partitioned or global index views when it is evaluated.
                 */
                subqueryCostReport = dataset == null ? null : summary
                        .estimateSubqueryCost(ctx.context,
                                accessPathSampleLimit,
                                (Predicate<?>) pred.setProperty(
                                        IPredicate.Annotations.REMOTE_ACCESS_PATH,
                                        true));
    
                anns.add(new NV(Annotations.COST_SUBQUERY, subqueryCostReport));
    
                scanAndFilter = subqueryCostReport == null
                        || scanCostReport.cost < subqueryCostReport.cost;
                
            } else {
    
                scanCostReport = null;
                
                subqueryCostReport = null;
                
                scanAndFilter = pred.getProperty(
                        QueryHints.ACCESS_PATH_SCAN_AND_FILTER,
                        ctx.accessPathScanAndFilter);
                
            }

        }
    
        if (scanAndFilter) {

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

//            // Filter for distinct SPOs. (moved inside of newJoin).
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

            return newJoin(left, anns, ctx.defaultGraphDistinctFilter, summary,
                    cutoffLimit, queryHints, ctx);

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
            final boolean dataSetJoin = false;

            if (dataSetJoin) {
                
                /*
                 * Setup the data set join (aka parallel subquery).
                 * 
                 * TODO This code path can not be made to work correctly. The
                 * problem is that we wind up with duplicate SPOs, even after
                 * filtering. This is because the scope of the DISTINCT SPO
                 * filter winds up being the JOIN against the B+Tree statement
                 * index but it would need to be ALL joins for a given source
                 * solution flowing through the DataSetJoin.
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/407
                 */

                // The variable to be bound.
                final IVariable<?> var = (IVariable<?>) pred.get(3);

                // The data set join.
                left = new DataSetJoin(leftOrEmpty(left), new NV(
                        DataSetJoin.Annotations.VAR, var),//
                        new NV(DataSetJoin.Annotations.BOP_ID, ctx.nextId()),//
                        new NV(DataSetJoin.Annotations.GRAPHS,
                                summary.getGraphs()) //
                );

            } else {

                /*
                 * Parallel subquery using the DGExpander.
                 * 
                 * NOte: See the notes on the DataSetJoin code path for why we
                 * can not use that approach for parallel subquery for default
                 * graph APs.
                 */
                
                final long estimatedRangeCount = subqueryCostReport.rangeCount;

                // @todo default with query hint to override and relate to
                // ClientIndexView limit in scale-out.
                final int maxParallel = 1; //PipelineJoin.Annotations.DEFAULT_MAX_PARALLEL;

                // Set subquery expander.
                pred = (Predicate<?>) pred.setUnboundProperty(
                        IPredicate.Annotations.ACCESS_PATH_EXPANDER,
                        new DGExpander(maxParallel, summary.getGraphs(),
                                estimatedRangeCount));

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

            anns.add(new NV(PipelineJoin.Annotations.PREDICATE,pred));
           
            return newJoin(left, anns, ctx.defaultGraphDistinctFilter, summary,
                    cutoffLimit, queryHints, ctx);

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
     */
    static private BOpFilterBase newDistinctFilter(final AST2BOpContext ctx,
            final Predicate<?> pred, final DataSetSummary summary,
            final boolean hashJoin) {
        
        // Never use native distinct for as-bound "pipeline" joins.
        boolean nativeDistinct = hashJoin && ctx.nativeDistinctSPO;
        
        if (nativeDistinct) {
            /*
             * Examine the cardinality of the predicate to determine whether or
             * not we should use a DISTINCT SPO filter backed by a persistence
             * capable data structure against native memory.
             */
            final Long rangeCount = (Long) pred
                    .getProperty(Annotations.ESTIMATED_CARDINALITY);
            if (rangeCount != null) {
                if (rangeCount.longValue() < ctx.nativeDistinctSPOThreshold) {
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
            if (summary.nknown < ctx.nativeDistinctSPOThreshold) {
                // Only a few graphs in the defaultGraph.
                nativeDistinct = false;
            }
        }
        if (nativeDistinct) {
            /*
             * The index that will be used to read on the B+Tree access path.
             */
            @SuppressWarnings({ "unchecked", "rawtypes" })
            final SPOKeyOrder indexKeyOrder = SPOKeyOrder.getKeyOrder(
                    (IPredicate) pred, 4/* keyArity */);
            // Native memory based DISTINCT filter.
            return NativeDistinctFilter.newInstance(indexKeyOrder);
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
     * @param defaultGraphFilter
     *            <code>true</code> iff a DISTINCT filter must be imposed on the
     *            SPOs. This is never done for a named graph query. It is
     *            normally done for default graph queries, but there are some
     *            edge cases where the SPOs are provably distinct and we do not
     *            need to bother.
     * @param summary
     *            The {@link DataSetSummary} (when available).
     * @param queryHints
     *            The query hints from the dominating operator context.
     * @param ctx
     *            The evaluation context.
     * @return
     * 
     * @see Annotations#HASH_JOIN
     * @see HashJoinAnnotations#JOIN_VARS
     * @see Annotations#ESTIMATED_CARDINALITY
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static private PipelineOp newJoin(//
            PipelineOp left, //
            final List<NV> anns,//
            final boolean defaultGraphFilter,//
            final DataSetSummary summary,//
            final Long cutoffLimit,//
            final Properties queryHints, //
            final AST2BOpContext ctx) {

        // Convert join annotations to a map so we can lookup some stuff.
        final Map<String, Object> map = NV.asMap(anns.toArray(new NV[anns
                .size()]));

        // Look up the predicate for the access path.
        Predicate<?> pred = (Predicate<?>) map
                .get(AccessPathJoinAnnotations.PREDICATE);

        // True iff a hash join was requested for this predicate.
        final boolean hashJoin = cutoffLimit == null
                && pred.getProperty(QueryHints.HASH_JOIN,
                        QueryHints.DEFAULT_HASH_JOIN);

        if (cutoffLimit != null) {

            /*
             * Cutoff join (RTO).
             */

            /*
             * true iff there are no variable materialization requirements for
             * this join.
             */
            final boolean simpleJoin = ((Boolean) map
                    .get(Annotations.SIMPLE_JOIN)).booleanValue()
                    && !AST2BOpRTO.runAllJoinsAsComplexJoins;

            // disallow reordering of solutions by the query engine.
            map.put(PipelineJoin.Annotations.REORDER_SOLUTIONS, Boolean.FALSE);

            // disallow parallel evaluation of tasks
            map.put(PipelineOp.Annotations.MAX_PARALLEL, Integer.valueOf(1));

            // disallow parallel evaluation of chunks.
            map.put(PipelineJoin.Annotations.MAX_PARALLEL_CHUNKS,
                    Integer.valueOf(0));

            // disable access path coalescing
            map.put(PipelineJoin.Annotations.COALESCE_DUPLICATE_ACCESS_PATHS,
                    Boolean.FALSE);

            /*
             * Disable access path reordering.
             * 
             * Note: Reordering must be disabled for complex joins since we will
             * correlate the input solutions and output solutions using a row
             * identifier. If the solutions are reordered as they flow through
             * the pipeline, then it will break this correlation and we will no
             * longer have accurate information about the #of input solutions
             * required to produce a given number of output solutions. [Simple
             * joins might not have this requirement since the PipelineJoin is
             * internally doing the accounting for the #of solutions in and out
             * of the join.]
             */
            map.put(PipelineJoin.Annotations.REORDER_ACCESS_PATHS,
                    Boolean.FALSE);

            if (simpleJoin) {

//                // disable access path coalescing
//                map.put(PipelineJoin.Annotations.COALESCE_DUPLICATE_ACCESS_PATHS,
//                        Boolean.FALSE);

                /*
                 * Note: We need to annotation the JOIN operator to eliminate
                 * parallelism, eliminate access path coalescing, and limit the
                 * output of the join.
                 */

                // cutoff join.
                map.put(PipelineJoin.Annotations.LIMIT,
                        Long.valueOf(cutoffLimit));

                /*
                 * Note: In order to have an accurate estimate of the join hit
                 * ratio we need to make sure that the join operator runs using
                 * a single PipelineJoinStats instance which will be visible to
                 * us when the query is cutoff. In turn, this implies that the
                 * join must be evaluated on the query controller.
                 * 
                 * FIXME RTO: This implies that sampling of scale-out joins must
                 * be done using remote access paths. This assumption and
                 * approach needs to be reviewed. This is probably NOT the case
                 * if we are using a complex pipline (i.e., with chunked
                 * materialization of some variables and/or conditional routing
                 * operations). In fact, if the pipeline is complex, we do not
                 * want to set LIMIT on the JOIN since that could cause the
                 * pipeline to underproduce if the filters wind up eliminating
                 * some solutions. This suggests that we either need to treat
                 * all cutoff joins as the general and NOT put the LIMIT on the
                 * JOIN -or- we need to pass in more information so newJoin()
                 * understands whether it will be required to impose the
                 * cutoffLimit or whether that limit will be imposed by a SLICE
                 * and injecting a column to correlate input and output
                 * solutions.
                 */
                map.put(PipelineJoin.Annotations.SHARED_STATE, Boolean.TRUE);//
                map.put(PipelineJoin.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER);//

            } else {

                /*
                 * Complex join.
                 * 
                 * Note: Complex joins may include operators to materialize of
                 * IVs as RDF Values and evaluate FILTERs that must (or might)
                 * operate on RDF Values.
                 * 
                 * FIXME RTO: Are there additional predicate annotations that we
                 * need to override if we are generating a complex query plan to
                 * evaluate the cutoff JOIN?
                 */
                
                /*
                 * FIXME RTO: This appears to be necessary to get reliable
                 * reporting for the sum of the fast range counters over the APs
                 * and the #of tuples read. Why? If we need to use shared state
                 * for reliable computation of cutoff joins then we must use
                 * remote APs for scale-out.
                 */
                map.put(PipelineJoin.Annotations.SHARED_STATE, Boolean.TRUE);//

            }

        } // cutoffJoin

        if (defaultGraphFilter) {

            /*
             * Filter for distinct SPOs.
             */

            pred = pred.addAccessPathFilter(newDistinctFilter(ctx, pred,
                    summary, hashJoin));
            
            // Update the annotation map with the predicate now that we have
            // attached the appropriate distinct filter.
            map.put(AccessPathJoinAnnotations.PREDICATE, pred);
            
        }
        
        if (hashJoin) {
            
            /*
             * TODO Choose HTree versus JVM hash join operator based on the the
             * estimated input cardinality to the join. The RTO can give us that
             * (the static join optimizer does not really provide a decent
             * estimate of the input/output cardinality of a join).
             * 
             * TODO If we partition the hash join on a cluster then we should
             * divide the estimated input cardinality by the #of partitions to
             * get the estimated input cardinality per partition.
             */
            
//            final long estimatedInputCardinality = Long.MAX_VALUE; 
//            
//            final boolean useHTree = estimatedInputCardinality > 20 * Bytes.megabyte;

            final boolean useHTree = ctx.nativeHashJoins;
            
            /*
             * The join variable(s) are variables which are (a) bound by the
             * predicate and (b) are known bound in the source solutions.
             */
            final IVariable<?>[] joinVars = (IVariable<?>[]) pred
                    .getRequiredProperty(HashJoinAnnotations.JOIN_VARS);

            map.put(HashJoinAnnotations.JOIN_VARS, joinVars);

            /*
             * Publish the NamedSolutionSetRef. This makes updates in the state
             * of the hash join visible from the NanoSparqlServer. (Both
             * versions of the operator require this attribute.)
             * 
             * BLZG-1608: this needs to be locally scoped, since the named subquery
             * may be executed multiple times (e.g., when part of an inner subquery
             * executed through the PipelinedHashIndexAndSolutionSetJoinOp.
             */
            map.put(HashJoinOp.Annotations.NAMED_SET_REF,
                    NamedSolutionSetRefUtility.newInstance(null,
                            "--namedSet-" + ctx.nextId(), joinVars));

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

        left = applyQueryHints(left, queryHints, ctx);

        return left;

    }

}
