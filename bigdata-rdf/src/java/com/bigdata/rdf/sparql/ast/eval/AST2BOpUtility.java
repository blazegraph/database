package com.bigdata.rdf.sparql.ast.eval;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.CopyOp;
import com.bigdata.bop.bset.EndOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.bset.Tee;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.JVMNamedSubqueryOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.controller.ServiceCallJoin;
import com.bigdata.bop.controller.Steps;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.HTreeHashIndexOp;
import com.bigdata.bop.join.HTreeSolutionSetHashJoinOp;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.JVMHashIndexOp;
import com.bigdata.bop.join.JVMSolutionSetHashJoinOp;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.bop.solutions.DistinctBindingSetOp;
import com.bigdata.bop.solutions.DistinctBindingSetsWithHTreeOp;
import com.bigdata.bop.solutions.GroupByOp;
import com.bigdata.bop.solutions.GroupByRewriter;
import com.bigdata.bop.solutions.GroupByState;
import com.bigdata.bop.solutions.IGroupByRewriteState;
import com.bigdata.bop.solutions.IGroupByState;
import com.bigdata.bop.solutions.ISortOrder;
import com.bigdata.bop.solutions.IVComparator;
import com.bigdata.bop.solutions.MemoryGroupByOp;
import com.bigdata.bop.solutions.MemorySortOp;
import com.bigdata.bop.solutions.PipelinedAggregationOp;
import com.bigdata.bop.solutions.ProjectionOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SortOrder;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.BindingConstraint;
import com.bigdata.rdf.internal.constraints.ConditionalBind;
import com.bigdata.rdf.internal.constraints.InBOp;
import com.bigdata.rdf.internal.constraints.ProjectedConstraint;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.TryBeforeMaterializationConstraint;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ASTUtil;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.ExistsNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ServiceCall;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.optimizers.ASTNamedSubqueryOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSetValueExpressionsOptimizer;
import com.bigdata.rdf.spo.DistinctTermAdvancer;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.ElementFilter;

import cutthecrap.utils.striterators.FilterBase;
import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.NOPFilter;

/**
 * Query plan generator converts an AST into a query plan made up of
 * {@link PipelineOp}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see <a href=
 *      "https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=QueryEvaluation"
 *      >Query Evaluation</a>.
 */
public class AST2BOpUtility extends Rule2BOpUtility {

    private static final transient Logger log = Logger
            .getLogger(AST2BOpUtility.class);

    /**
     * Top-level entry point converts an AST query model into an executable
     * query plan.
     */
    public static PipelineOp convert(final AST2BOpContext ctx) {

        /*
         * FIXME Pass in zero or more binding sets to use when optimizing the
         * query. This is mainly for rewriting variables as constants (when
         * there is one source binding set) or for adding IN filters (where
         * there are several and we can constrain a variable to the known values
         * which it can take on). This could be passed through using the [ctx]
         * or directly.
         * 
         * Do we still want the ability to pass in an IBindingSet[] to query
         * evaluation or we are planning to handle this entirely via a "java"
         * service interface. If so, then we need the ability to run the
         * optimizer once we have evaluated that service request in order to do
         * things like set an IN filter based on the returned solution set.
         * 
         * If we model this with an "inline access path", then that is really
         * very close to the Htree / hash join. The IN constraint could just be
         * the evaluation of the join. We could also do column projections,
         * which would give us something like the hash-set based IN constraint.
         */
        final IBindingSet[] bindingSets = null;

        // The AST query model.
        final ASTContainer astContainer = ctx.astContainer;

        // The AST model as produced by the parser.
        final QueryRoot originalQuery = astContainer.getOriginalAST();
        
        // Run the AST query rewrites / query optimizers.
        final QueryRoot optimizedQuery = (QueryRoot) ctx.optimizers.optimize(ctx,
                originalQuery, bindingSets);
        
        // Set the optimized AST model on the container.
        astContainer.setOptimizedAST(optimizedQuery);

        // Final static analysis object for the optimized query.
        ctx.sa = new StaticAnalysis(optimizedQuery);

        // The executable query plan.
        PipelineOp queryPlan = convertQueryBaseWithScopedVars(null/* left */,
                optimizedQuery, new LinkedHashSet<IVariable<?>>()/* doneSet */,
                ctx);

        if (queryPlan == null) {

            /*
             * An empty query plan. Just copy anything from the input to the
             * output.
             */

            queryPlan = addStartOp(ctx);

        }

        /*
         * Set the queryId on the top-level of the query plan.
         * 
         * Note: The queryId is necessary to coordinate access to named subquery
         * result sets when they are consumed within a subquery rather than the
         * main query.
         */

        queryPlan = (PipelineOp) queryPlan.setProperty(
                QueryEngine.Annotations.QUERY_ID, ctx.queryId);

        // Attach the query plan to the ASTContainer.
        astContainer.setQueryPlan(queryPlan);

        if (log.isInfoEnabled()) {
            log.info(astContainer);
        }

        return queryPlan;

    }

    /**
     * Convert a query (or subquery) into a query plan (pipeline).
     * <p>
     * Note: Only the variables in the parent which are projected by the
     * subquery are visible within the named subquery. Therefore, we have to
     * create a temporary set of the known materialized variables, and remove
     * anything which is not projected by the subquery on entry. On exit from
     * the subquery, we need to add into the parents set of known materialized
     * variables any variables which the subquery projects and which are known
     * to have been materialized by the subquery.
     * 
     * @param query
     *            Either a {@link QueryRoot}, a {@link SubqueryRoot}, or a
     *            {@link NamedSubqueryRoot}.
     * @param doneSet
     *            The set of variables known to be materialized in the parent
     *            when the {@link QueryBase} is evaluated. Materialized
     *            variables which are not visible (not projected by) the
     *            subquery are hidden by this method. On exit, all variables
     *            known to be materialized within the {@link QueryBase} which
     *            are also projected by the {@link QueryBase} will be added into
     *            the caller's set.
     * @param ctx
     *            The evaluation context.
     * 
     * @return The query plan.
     */
    private static PipelineOp convertQueryBase(PipelineOp left,
            final QueryBase query, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        final ProjectionNode projection = query.getProjection();
        
        final IVariable<?>[] projectedVars = projection == null
                || projection.isEmpty() ? null : projection.getProjectionVars();
        
        // The variables projected by the subquery.
        final List<IVariable<?>> projectedVarList = projectedVars == null ? new LinkedList<IVariable<?>>()
                : Arrays.asList(projectedVars);

        // Temporary set scoped to the subquery.
        final Set<IVariable<?>> tmp = new LinkedHashSet<IVariable<?>>();

        // Add everything known to have been materialized up to now.
        tmp.addAll(doneSet);

        // Retain only those variables projected by the subquery.
        tmp.retainAll(projectedVarList);

        // DO SUBQUERY PLAN HERE.
        left = convertQueryBaseWithScopedVars(left, query, tmp/* doneSet */,
                ctx);

        // Retain only those variables projected by the subquery.
        tmp.retainAll(projectedVarList);

        // Add any variables known to be materialized into this scope.
        doneSet.addAll(tmp);

        return left;

    }

    /**
     * Core method to convert a {@link QueryBase}.
     * <p>
     * Note: This assumes that the caller has correctly scoped <i>doneSet</i>
     * with respect to the projection of a subquery.
     * 
     * @param left
     * @param query
     * @param doneSet
     * @param ctx
     * @return
     */
    private static PipelineOp convertQueryBaseWithScopedVars(PipelineOp left,
            final QueryBase query, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {
        
        final GraphPatternGroup<?> root = query.getWhereClause();

        if (root == null)
            throw new IllegalArgumentException("No group node");

        if (left == null) {

            left = addStartOpOnCluster(ctx);
            
        }

        /*
         * Named subqueries.
         */
        if (query instanceof QueryRoot) {

            final NamedSubqueriesNode namedSubqueries = ((QueryRoot) query)
                    .getNamedSubqueries();

            if (namedSubqueries != null && !namedSubqueries.isEmpty()) {

                // WITH ... AS [name] ... INCLUDE style subquery declarations.
                left = addNamedSubqueries(left, namedSubqueries,
                        (QueryRoot) query, doneSet, ctx);

            }

        }

        // The top-level "WHERE" clause.
        left = convertJoinGroupOrUnion(left, root, doneSet, ctx);

        final ProjectionNode projection = query.getProjection() == null ? null
                : query.getProjection().isEmpty() ? null : query
                        .getProjection();

        if (projection != null) {

            /**
             * Add any evaluated projections
             * 
             * Note: If any of the projected value expressions is an an
             * aggregate then ALL must be aggregates (which includes value
             * expressions involving aggregate functions, bare variables
             * appearing on the group by clause, variables bound by the group by
             * clause, and constants).
             * 
             * Note: If GROUP BY or HAVING is used then the projected value
             * expressions MUST be aggregates. However, if these do not appear
             * and any of the projected value expressions is an aggregate then
             * we are still going to use a GROUP BY where all solutions form a
             * single implicit group.
             * 
             * Note: The combination of the IVs, the math and comparison
             * operators, and the materialization pipe MUST ensure that any IVs
             * which need to be materialized have been materialized BEFORE we
             * run ORDER BY or GROUP BY. Because those operators must act on all
             * solutions "at once", implementations of those operators CAN NOT
             * use the conditional routing logic to handle IVs which are not
             * materialized.
             */

            if (projection.isWildcard())
                throw new AssertionError(
                        "Wildcard projection was not rewritten.");

            final GroupByNode groupBy = query.getGroupBy() == null ? null
                    : query.getGroupBy().isEmpty() ? null : query.getGroupBy();

            final HavingNode having = query.getHaving() == null ? null : query
                    .getHaving().isEmpty() ? null : query.getHaving();

            // true if this is an aggregation query.
            final boolean isAggregate = isAggregate(projection, groupBy, having);

            if (isAggregate) {

                left = addAggregation(left, projection, groupBy, having, ctx);

            } else {

                for (AssignmentNode assignmentNode : projection
                        .getAssignmentProjections()) {

                    left = addAssignment(left, assignmentNode, doneSet, ctx,
                            true/* projection */);

                }

            }

            /*
             * Note: The DISTINCT operators also enforce the projection.
             * 
             * Note: REDUCED allows, but does not require, either complete or
             * partial filtering of duplicates. It is part of what openrdf does
             * for a DESCRIBE query.
             * 
             * Note: We do not currently have special operator for REDUCED. One
             * could be created using chunk wise DISTINCT. Note that REDUCED may
             * not change the order in which the solutions appear (but we are
             * evaluating it before ORDER BY so that is Ok.)
             * 
             * TODO If there is an ORDER BY and a DISTINCT then the sort can be
             * used to impose the distinct without the overhead of a hash index
             * by filtering out the duplicate solutions after the sort.
             */

            if (projection.isDistinct() || projection.isReduced()) {

                left = addDistinct(left, query, ctx);

            }

        }

        final OrderByNode orderBy = query.getOrderBy();

        if (orderBy != null && !orderBy.isEmpty()) {

            left = addOrderBy(left, orderBy, ctx);

        }

        if (projection != null) {
         
            /*
             * Append operator to drop variables which are not projected by the
             * subquery.
             * 
             * Note: We need to retain all variables which were visible in the
             * parent group plus anything which was projected out of the
             * subquery. Since there can be exogenous variables, the easiest way
             * to do this correctly is to drop variables from the subquery plan
             * which are not projected by the subquery. (This is not done at the
             * top-level query plan because it would cause exogenous variables
             * to be dropped.)
             */

            // The variables projected by the subquery.
            final IVariable<?>[] projectedVars = projection.getProjectionVars();

            left = new ProjectionOp(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(ProjectionOp.Annotations.SELECT, projectedVars)//
            );

        }
        
        final SliceNode slice = query.getSlice();

        if (slice != null) {

            left = addSlice(left, slice, ctx);

        }

        left = addEndOp(left, ctx);

        /*
         * Set a timeout on a query or subquery.
         */
        {

            final long timeout = query.getTimeout();

            if (timeout > 0 && timeout != Long.MAX_VALUE) {

                left = (PipelineOp) left.setProperty(BOp.Annotations.TIMEOUT,
                        timeout);

            }

        }

        /*
         * Note: we do NOT want to force materialization for variables projected
         * by subqueries since those variables might not need to be materialized
         * in the parent query. Projected variables SHOULD stay as IV as long as
         * possible.
         */

        if (log.isInfoEnabled())
            log.info("\nsubquery: " + query + "\nplan="
                    + BOpUtility.toString(left));

        return left;

    }

    /**
     * Add pipeline operators for named subquery solution sets. The solution
     * sets will be computed before the rest of the query plan runs. They may be
     * referenced from other parts of the query plan.
     * <p>
     * Each solution set has a name. For each {@link NamedSubqueryInclude}, we
     * also determine the join variables which are used to build a hash index
     * (this can be different for different includes of the same named solution
     * set in the same query). Together, the join variables and the name of the
     * solution set form a unique name for each hash index. That hash index (or
     * hash indices if we have more than one combination of join variables) will
     * be generated by the {@link HTreeNamedSubqueryOp}.
     * <p>
     * The main use case for WITH AS INCLUDE is a heavy subquery which should be
     * run first. However, in principle, we could run these where the INCLUDE
     * appears rather than up front as long as we are careful not to project
     * bindings into the subquery (other than those it projects out). Whether or
     * not this is more efficient depends on the subquery and how it is combined
     * into the rest of the query. (Running it first should be the default
     * policy.)
     * 
     * @param left
     * @param namedSubquerieNode
     * @param queryRoot
     *            The top-level query. This is required because we need to
     *            understand the join variables which will be used when the
     *            subquery result is joined back into the query for WITH AS
     *            INCLUDE style subqueries.
     * @param ctx
     * @return
     * 
     * @see ASTNamedSubqueryOptimizer
     */
    private static PipelineOp addNamedSubqueries(PipelineOp left,
            final NamedSubqueriesNode namedSubquerieNode,
            final QueryRoot queryRoot, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        /*
         * If there is more than one named subquery whose DEPENDS_ON attribute
         * is an empty String[], then run them in parallel using STEPS (and
         * filter them out in the 2nd pass). Then run the remaining named
         * subqueries in their current sequence.
         */

        // All named subqueries with NO dependencies.
        final List<NamedSubqueryRoot> runFirst = new LinkedList<NamedSubqueryRoot>();

        // Remaining named subqueries, which MUST already be in an ordering
        // consistent with their dependencies.
        final List<NamedSubqueryRoot> remainder = new LinkedList<NamedSubqueryRoot>();

        for (NamedSubqueryRoot subqueryRoot : namedSubquerieNode) {

            final String[] dependsOn = subqueryRoot.getDependsOn();

            /*
             * TODO Parallelism for the named subqueries with no dependenceies
             * has been (at least temporarily) disabled. The problem with a
             * UNION of these no-dependency named subqueries is that each of
             * them sees an input solution which is empty (no bindings) and
             * copies that to its output. Since the outputs are UNIONed, we are
             * getting N empty solutions out when we run N named subqueries with
             * no dependencies. This is demonstrated by TestSubQuery in the
             * "named-subquery-scope" test case. While that test was written to
             * test something else, it happens to trigger this bug as well.
             */
            if (dependsOn.length == 0 && false)
                runFirst.add(subqueryRoot);
            else
                remainder.add(subqueryRoot);

        }

        final int nfirst = runFirst.size();

        if (nfirst > 0) {

            if (nfirst == 1) {

                left = addNamedSubquery(left, runFirst.get(0), doneSet, ctx);

            } else {

                final PipelineOp[] steps = new PipelineOp[nfirst];

                int i = 0;

                for (NamedSubqueryRoot subqueryRoot : runFirst) {

                    steps[i++] = addNamedSubquery(null/* left */, subqueryRoot,
                            doneSet, ctx);

                }

                // Do not run the subqueries with unlimited parallelism.
                final int maxParallelSubqueries = Math.min(steps.length, 10);

                // Run the steps in parallel.
                left = new Union(leftOrEmpty(left), //
                        new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(Steps.Annotations.SUBQUERIES, steps),//
                        new NV(Steps.Annotations.MAX_PARALLEL_SUBQUERIES,
                                maxParallelSubqueries)//
                );

            }
            
        }

        // Run the remaining steps in sequence.
        for (NamedSubqueryRoot subqueryRoot : remainder) {

            left = addNamedSubquery(left, subqueryRoot, doneSet, ctx);

        }

        return left;

    }

    /**
     * Convert a {@link NamedSubqueryRoot} into a pipeline operator plan.
     * 
     * @param left
     *            The left or <code>null</code>.
     * @param subqueryRoot
     *            The {@link NamedSubqueryRoot}.
     * @param ctx
     *            The context.
     * @return The operator plan.
     */
    static private PipelineOp addNamedSubquery(PipelineOp left,
            final NamedSubqueryRoot subqueryRoot,
            final Set<IVariable<?>> doneSet, final AST2BOpContext ctx) {

        final PipelineOp subqueryPlan = convertQueryBase(null/* left */,
                subqueryRoot, doneSet, ctx);

        /*
         * Annotate the named subquery with the set of variables known to be
         * materialized after it has run. This annotation is looked up when we
         * do the NamedSubqueryInclude.
         */
        subqueryRoot.setProperty(NamedSubqueryRoot.Annotations.DONE_SET,
                doneSet);
        
        if (log.isInfoEnabled())
            log.info("\nsubquery: " + subqueryRoot + "\nplan=" + subqueryPlan);

        final IVariable<?>[] joinVars = ASTUtil.convert(subqueryRoot
                .getJoinVars());

        final NamedSolutionSetRef namedSolutionSet = new NamedSolutionSetRef(
                ctx.queryId, subqueryRoot.getName(), joinVars);

        if(ctx.nativeHashJoins) {
            left = new HTreeNamedSubqueryOp(leftOrEmpty(left), //
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(HTreeNamedSubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                new NV(HTreeNamedSubqueryOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HTreeNamedSubqueryOp.Annotations.NAMED_SET_REF,
                        namedSolutionSet)//
        );
        } else {
            left = new JVMNamedSubqueryOp(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(HTreeNamedSubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                    new NV(HTreeNamedSubqueryOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HTreeNamedSubqueryOp.Annotations.NAMED_SET_REF,
                            namedSolutionSet)//
            );
        }

        return left;

    }

    /**
     * Add an operator to evaluate a {@link ServiceCall}.
     * 
     * @param left
     * @param serviceNode
     * @param ctx
     * @return
     * 
     *         TODO Pass BindingsClause from QueryRoot through to Service.
     * 
     *         TODO When we provide an integration for Sesame aware services
     *         (whether remote or local) the [doneSet] needs to be expanded by
     *         the variables within the SERVICE's graph pattern which are known
     *         bound after the SERVICE call since those variables will have been
     *         (of necessity) materialized by the service.
     */
    static private PipelineOp addServiceCall(PipelineOp left,
            final ServiceNode serviceNode, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(serviceNode), needsMaterialization);

        left = new ServiceCallJoin(leftOrEmpty(left), //
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(ServiceCallJoin.Annotations.SERVICE_URI,
                        serviceNode.getServiceURI()),//
                new NV(ServiceCallJoin.Annotations.GROUP_NODE,
                        serviceNode.getGroupNode()),//
                new NV(ServiceCallJoin.Annotations.NAMESPACE,
                        ctx.db.getNamespace()),//
                new NV(ServiceCallJoin.Annotations.TIMESTAMP,
                        ctx.db.getTimestamp()),//
                new NV(SubqueryOp.Annotations.CONSTRAINTS, joinConstraints)//
        );

        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps(left, doneSet, needsMaterialization,
                serviceNode.getQueryHints(), ctx);

        return left;

    }

    /**
     * Add a join against a pre-computed temporary solution set into a join
     * group.
     * <p>
     * Note: Since the subquery solution set has already been computed and only
     * contains bindings for solutions projected by the subquery, we do not need
     * to adjust the visibility of bindings when we execute the hash join
     * against the named solution set.
     */
    private static PipelineOp addNamedSubqueryInclude(PipelineOp left,
            final NamedSubqueryInclude subqueryInclude,
            final Set<IVariable<?>> doneSet, final AST2BOpContext ctx) {

        if (log.isInfoEnabled())
            log.info("include: solutionSet=" + subqueryInclude.getName());

        final VarNode[] joinvars = subqueryInclude.getJoinVars();

        if (joinvars == null) {

            /*
             * The most likely explanation is not running the
             * ASTNamedSubqueryOptimizer.
             */

            throw new AssertionError();

        }

        if (joinvars.length == 0) {

            /*
             * Note: If there are no join variables then the join will examine
             * the full N x M cross product of solutions. That is very
             * inefficient, so we are logging a warning.
             */

            log.warn("No join variables: " + subqueryInclude.getName());

        }

        final IVariable<?>[] joinVars = ASTUtil.convert(joinvars);

        final NamedSolutionSetRef namedSolutionSetRef = new NamedSolutionSetRef(
                ctx.queryId, subqueryInclude.getName(), joinVars);

        /*
         * Get the known materialized variables from the named subquery and
         * combine them with those which are known to be materialized in the
         * parent to get the full set of variables known to be materialized once
         * we join in the named subquery solution set.
         */
        {

            // Resolve the NamedSubqueryRoot for this INCLUDE.
            final NamedSubqueryRoot nsr = subqueryInclude
                    .getNamedSubqueryRoot(ctx.sa.getQueryRoot());

            if (nsr == null) {
                // Paranoia check.
                throw new AssertionError("NamedSubqueryRoot not found: "
                        + subqueryInclude.getName());
            }
            
            @SuppressWarnings("unchecked")
            final Set<IVariable<?>> nsrDoneSet = (Set<IVariable<?>>) nsr
                    .getProperty(NamedSubqueryRoot.Annotations.DONE_SET);

            if (nsrDoneSet == null) {
                // Make sure the doneSet was attached to the named subquery.
                throw new AssertionError(
                        "NamedSubqueryRoot doneSet not found: "
                                + subqueryInclude.getName());
            }

            // Add in everything known to be materialized by the named subquery.
            doneSet.addAll(nsrDoneSet);
            
        }

        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(subqueryInclude), needsMaterialization);

        /*
         * TODO This should be set based on the #of INCLUDEs for the named
         * subquery. We can not release the associated hash index until all
         * includes are done.
         */
        final boolean release = false;
        
        if (ctx.nativeHashJoins) {
            
            left = new HTreeSolutionSetHashJoinOp(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF,
                            namedSolutionSetRef),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS,
//                            joinVars),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.CONSTRAINTS,
//                                    joinConstraints),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE,
                                release)//
            );

        } else {
            
            left = new JVMSolutionSetHashJoinOp(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.NAMED_SET_REF,
                            namedSolutionSetRef),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.JOIN_VARS,
                            joinVars),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.CONSTRAINTS,
                            joinConstraints),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.RELEASE,
                            release)//
            );
            
        }
        
        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps(left, doneSet, needsMaterialization,
                subqueryInclude.getQueryHints(), ctx);

        return left;

    }

    /**
     * Add an explicit SPARQL 1.1 subquery into a join group.
     * <p>
     * Rule: A variable within a subquery is distinct from the same name
     * variable outside of the subquery unless the variable is projected from
     * the subquery. This is handled by a pattern in which variables in the
     * parent context which are not projected by the subquery are hidden from
     * view during its evaluation scope.
     * <p>
     * Note: We evaluate SPARQL 1.1 style sub-queries using a pipelined subquery
     * join. For each solution which flows through the pipeline, we issue the
     * sub-query for the as-bound solution and join the subquery results against
     * that as-bound solution.
     * <p>
     * Note: Some SPARQL 1.1 subqueries may be more efficiently run once to
     * produce a temporary solution set which is then joined into the query for
     * each solution which flows through the query. Such subqueries should be
     * translated into named subqueries with an include by a query optimizer
     * step. When a subquery is rewritten like this is will no longer appear as
     * a {@link SubqueryRoot} node in the AST.
     * <p>
     * Note: This also handles ASK subqueries, which are automatically created
     * for {@link ExistsNode} and {@link NotExistsNode}.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/232
     */
    private static PipelineOp addSparql11Subquery(PipelineOp left,
            final SubqueryRoot subqueryRoot, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        final ProjectionNode projection = subqueryRoot.getProjection();

        // The variables projected by the subquery.
        final IVariable<?>[] projectedVars = projection.getProjectionVars();

        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(subqueryRoot), needsMaterialization);

        switch (subqueryRoot.getQueryType()) {
        case ASK: {

            /*
             * The projection should have just one variable, which is the
             * variable that will be bound to true if there is a solution to the
             * subquery and to false if there is no solution to the subquery.
             * 
             * FIXME ASK should not run "as-bound" with SubqueryOp. Use a hash
             * join pattern instead. Also, review the variable scope rules for
             * the embedded graph pattern of an EXIST filter (which is what is
             * being mapped on an ASK filter here).
             */

            final boolean aggregate = isAggregate(subqueryRoot);

            if (projectedVars == null || projectedVars.length != 1)
                throw new RuntimeException(
                        "Expecting ONE (1) projected variable for ASK subquery.");

            final IVariable<?> askVar = projectedVars[0];

            final PipelineOp subqueryPlan = convertQueryBase(null/* left */,
                    subqueryRoot, doneSet, ctx);

            left = new SubqueryOp(leftOrEmpty(left),// SUBQUERY
                    new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(SubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                    new NV(SubqueryOp.Annotations.OPTIONAL, false),//
                    new NV(SubqueryOp.Annotations.ASK_VAR, askVar),//
                    new NV(SubqueryOp.Annotations.SELECT, null),//
                    new NV(SubqueryOp.Annotations.CONSTRAINTS, joinConstraints),//
                    new NV(SubqueryOp.Annotations.IS_AGGREGATE, aggregate)//
            );
            break;
        }
        case SELECT: {
            
            /*
             * Model a sub-group by building a hash index at the start of the
             * group. We then run the group.  Finally, we do a hash join of
             * the hash index against the solutions in the group.  If the
             * group is optional, then the hash join is also optional.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/377#comment:4
             */
            final String solutionSetName = "--set-" + ctx.nextId(); // Unique name.

            final Set<IVariable<?>> joinVarSet = ctx.sa.getJoinVars(
                    subqueryRoot, new LinkedHashSet<IVariable<?>>());

            @SuppressWarnings("rawtypes")
            final IVariable[] joinVars = joinVarSet.toArray(new IVariable[0]);
            
            final NamedSolutionSetRef namedSolutionSet = new NamedSolutionSetRef(
                    ctx.queryId, solutionSetName, joinVars);

            // Sub-Select is not optional.
            final boolean optional = false;

            // lastPass is required if the join is optional.
            final boolean lastPass = optional; // iff optional.

            // true if we will release the HTree as soon as the join is done.
            // Note: also requires lastPass.
            final boolean release = lastPass;

            if(ctx.nativeHashJoins) {
                left = new HTreeHashIndexOp(leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),// required
                    new NV(HTreeHashIndexOp.Annotations.OPTIONAL, optional),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HTreeHashIndexOp.Annotations.SELECT, projectedVars),//
                    new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
            } else {
                left = new JVMHashIndexOp(leftOrEmpty(left),//
                        new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                        new NV(PipelineOp.Annotations.LAST_PASS, true),// required
                        new NV(HTreeHashIndexOp.Annotations.OPTIONAL, optional),//
                        new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                        new NV(HTreeHashIndexOp.Annotations.SELECT, projectedVars),//
                        new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
                ); 
            }

            // Append the subquery plan.
            left = convertQueryBase(left, subqueryRoot, doneSet, ctx);
            
            if(ctx.nativeHashJoins) {
                left = new HTreeSolutionSetHashJoinOp(
                    leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.SELECT, null/*all*/),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
                    );
            } else {
                left = new JVMSolutionSetHashJoinOp(
                    leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.SELECT, null/*all*/),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
                    );
            }

            break;

        }
        default:
            throw new UnsupportedOperationException();
        }

        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps(left, doneSet, needsMaterialization,
                subqueryRoot.getQueryHints(), ctx);

        return left;

    }

    /**
     * Return <code>true</code> if any of the {@link ProjectionNode},
     * {@link GroupByNode}, or {@link HavingNode} indicate that this is an
     * aggregation query.
     * 
     * @param query
     *            The query.
     * 
     * @return <code>true</code>if it is an aggregation query.
     */
    public static boolean isAggregate(final QueryBase query) {

        return isAggregate(query.getProjection(), query.getGroupBy(),
                query.getHaving());

    }

    /**
     * Return <code>true</code> if any of the {@link ProjectionNode},
     * {@link GroupByNode}, or {@link HavingNode} indicate that this is an
     * aggregation query. All arguments are optional.
     */
    private static boolean isAggregate(final ProjectionNode projection,
            final GroupByNode groupBy, final HavingNode having) {

        if (groupBy != null && !groupBy.isEmpty())
            return true;

        if (having != null && !having.isEmpty())
            return true;

        if (projection != null) {

            for (IValueExpressionNode exprNode : projection) {

                final IValueExpression<?> expr = exprNode.getValueExpression();

                if (isObviousAggregate(expr)) {

                    return true;

                }

            }

        }

        return false;

    }

    /**
     * Return <code>true</code> iff the {@link IValueExpression} is an obvious
     * aggregate (it uses an {@link IAggregate} somewhere within it). This is
     * used to identify projections which are aggregates when they are used
     * without an explicit GROUP BY or HAVING clause.
     * <p>
     * Note: Value expressions can be "non-obvious" aggregates when considered
     * in the context of a GROUP BY, HAVING, or even a SELECT expression where
     * at least one argument is a known aggregate. For example, a constant is an
     * aggregate when it appears in a SELECT expression for a query which has a
     * GROUP BY clause. Another example: any value expression used in a GROUP BY
     * clause is an aggregate when the same value expression appears in the
     * SELECT clause.
     * <p>
     * This method is only to find the "obvious" aggregates which signal that a
     * bare SELECT clause is in fact an aggregation.
     * 
     * @param expr
     *            The expression.
     * 
     * @return <code>true</code> iff it is an obvious aggregate.
     */
    private static boolean isObviousAggregate(final IValueExpression<?> expr) {

        if (expr instanceof IAggregate<?>)
            return true;

        final Iterator<BOp> itr = expr.argIterator();

        while (itr.hasNext()) {

            final IValueExpression<?> arg = (IValueExpression<?>) itr.next();

            if (arg != null) {

                if (isObviousAggregate(arg)) // recursion.
                    return true;

            }

        }

        return false;

    }

    /**
     * Generate the query plan for a join group or union. This is invoked for
     * the top-level "WHERE" clause and may be invoked recursively for embedded
     * join groups.
     * 
     * @param left
     * @param groupNode
     * @param ctx
     * @return
     */
    private static PipelineOp convertJoinGroupOrUnion(final PipelineOp left,
            final IGroupNode<? extends IGroupMemberNode> groupNode,
            final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        if (groupNode instanceof UnionNode) {

            return convertUnion(left, (UnionNode) groupNode, doneSet, ctx);

        } else if (groupNode instanceof JoinGroupNode) {

            return convertJoinGroup(left, (JoinGroupNode) groupNode, doneSet,
                    ctx, true/* needsEndOp */);

        } else {

            throw new IllegalArgumentException();

        }

    }

    /**
     * Generate the query plan for a union.
     * <p>
     * Note: A UNION operation is converted into a plan using the {@link Tee}
     * operator. This allows us to run the sub-plans "inlined" within the main
     * pipeline. This pattern is significantly faster than old evaluation
     * pattern which issued one sub-query per child of the UNION per source
     * solution.
     * 
     * @param left
     * @param unionNode
     * @param ctx
     * @return
     */
    private static PipelineOp convertUnion(PipelineOp left,
            final UnionNode unionNode, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        if (unionNode.isOptional()) {
            /*
             * This in fact shows up in the TCK which is a bit weird.
             */
            log.warn("Optional union? : " + ctx.astContainer);
        }

        final int arity = unionNode.size();

        if (arity == 0) {
            /*
             * TODO We can probably just return [left]. The old evaluation
             * strategy code would translate an empty union into an empty
             * iterator. That could be captured by a rewrite of the AST which
             * recognizes and removes empty UNION nodes.
             */
            throw new IllegalArgumentException();

        }

        /*
         * We are going to route all the subqueries here when they're done,
         * by replacing the SINK_REF on the topmost operator in the subquery.
         */
        final int downstreamId = ctx.nextId();

        /*
         * Pre-generate the ids for the Tee operators.
         */
        final int[] subqueryIds = new int[arity];

        for (int i = 0; i < arity; i++) {

            subqueryIds[i] = ctx.nextId();

        }

        /*
         * Should kinda look like this:
         * 
         *       copy := CopyOp( lastBOp4 )[bopId=5]
         *       subquery4 := firstBOp4( lastBOp3 )[bopId=41]->...->lastBOp4(...)[sinkRef=5]
         *       subquery3 := firstBOp3( lastBOp2 )[bopId=31]->...->lastBOp3(...)[sinkRef=5]
         *       subquery2 := firstBOp2( lastBOp1 )[bopId=21]->...->lastBOp2(...)[sinkRef=5]
         *       subquery1 := firstBOp1( tee3 )[bopId=11]->...->lastBOp1(...)[sinkRef=5]
         *       tee3   := TEE( tee2 )[bopId=03;sinkRef=31;altSinkRef=41]
         *       tee2   := TEE( tee1 )[bopId=02;sinkRef=03;altSinkRef=21]
         *       tee1   := TEE( left )[bopId=01;sinkRef=02;altSinkRef=11]
         *       left
         */

        int thisTeeId = ctx.nextId();
        int nextTeeId = ctx.nextId();
        
        /*
         * We need one less Tee than we have subqueries.
         */
        for (int j = 0; j < (arity - 1); j++) {
        	
            final LinkedList<NV> anns = new LinkedList<NV>();

            anns.add(new NV(BOp.Annotations.BOP_ID, thisTeeId));
            
            if (j < (arity - 2)) {
            
            	/* 
            	 * Not the last one - send the Tee to the next Tee and the next 
            	 * subquery.
            	 */
            	anns.add(new NV(PipelineOp.Annotations.SINK_REF, nextTeeId));
            	anns.add(new NV(PipelineOp.Annotations.ALT_SINK_REF, subqueryIds[j]));
            	
    			thisTeeId = nextTeeId;
            	nextTeeId = ctx.nextId();
            	
            } else {
            	
            	/*
            	 * Last one - send the Tee to the last two subqueries.
            	 */
            	anns.add(new NV(PipelineOp.Annotations.SINK_REF, subqueryIds[j]));
            	anns.add(new NV(PipelineOp.Annotations.ALT_SINK_REF, subqueryIds[j+1]));
            	
            }
            
        	left = applyQueryHints(new Tee(leftOrEmpty(left),
                    NV.asMap(anns.toArray(new NV[anns.size()]))), ctx.queryHints);
        	
        }
        
        /*
         * Since this is a UNION, the only known materialized variables which we
         * can rely on are those which are known materialized for ALL of the
         * child join groups (the intersection). That intersection is then added
         * to what was known materialized on entry to the UNION.
         */
        int i = 0;
        // Start with everything already known to be materialized.
        final Set<IVariable<?>> doneSetsIntersection = new LinkedHashSet<IVariable<?>>(doneSet);
        for (IGroupMemberNode child : unionNode) {

            // convert the child
            if (!(child instanceof JoinGroupNode))
                throw new RuntimeException("Illegal child type for union: "
                        + child.getClass());
 
            /*
             * Need to make sure the first operator in the group has the right
             * Id.
             */
            left = new CopyOp(leftOrEmpty(left), NV.asMap(new NV[] {//
                    new NV(Predicate.Annotations.BOP_ID, subqueryIds[i++]),//
                    }));

            // Start with everything already known to be materialized.
            final Set<IVariable<?>> tmp = new LinkedHashSet<IVariable<?>>(
                    doneSet);

            // Convert the child join group.
            final PipelineOp subquery = convertJoinGroup(left,
                    (JoinGroupNode) child, tmp/* doneSet */, ctx, false/* needsEndOp */);

            /*
             * Retain anything known materialized on entry to the subgroup plus
             * anything which the subgroup is known to have materialized. Unless
             * all subgroups materialize a variable which was not known to be
             * materialized on entry, that variable will not be known
             * materialized outside of the UNION.
             */
            doneSetsIntersection.retainAll(tmp);

            /*
             * Route all "subqueries" to the same place. This works because the
             * subqueries array is passed in as references to the topmost (last)
             * operator in the subquery pipeline, and this is the one whose
             * SINK_REF we need to change.
             */
            left = (PipelineOp) subquery.setProperty(
                    PipelineOp.Annotations.SINK_REF, downstreamId);

        }

        /*
         * All the subqueries get routed here when they are done.
         */
        left = applyQueryHints(new CopyOp(leftOrEmpty(left),//
                new NV(Predicate.Annotations.BOP_ID, downstreamId),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER)//
                ), ctx.queryHints);

        // Add in anything which was known materialized for all child groups.
        doneSet.addAll(doneSetsIntersection);

        return left;

    }

    /**
     * Join group consists of: statement patterns, constraints, and sub-groups
     * <p>
     * Sub-groups can be either join groups (optional) or unions (non-optional)
     * <p>
     * No such thing as a non-optional sub join group (in Sparql 1.0)
     * <p>
     * No such thing as an optional statement pattern, only optional sub-groups
     * <p>
     * Optional sub-groups with at most one statement pattern can be lifted into
     * this group using an optional PipelineJoin, but only if that sub-group has
     * no constraints that need materialized variables. (why? because if we put
     * in materialization steps in between the join and the constraint, the
     * constraint won't have the same optional semantics as the join and will
     * lose its association with that join and its "optionality".)
     * 
     * <pre>
     * 1. Partition the constraints:
     *    -preConditionals: all variables already bound.
     *    -joinConditionals: variables bound by statement patterns in this group.
     *    -postConditionals: variables bound by sub-groups of this group (or not at all).
     *    
     * 2. Pipeline the preConditionals. Add materialization steps as needed.
     * 
     * 3. Non-optional joins and non-optional subqueries.
     * 
     *   3a. Join the statement patterns. Use the static optimizer to attach
     *       constraints to joins. Lots of funky stuff with materialization and
     *       named / default graph joins.
     * 
     *   3b. Pipeline the SPARQL 1.1 subquery.
     * 
     *   3c. Hash join for each named subquery include. 
     * 
     * 4. Pipeline the optional sub-groups (join groups). Lift single optional
     * statement patterns into this group (avoid the subquery op) per the
     * instructions above regarding materialization and constraints. Unions are
     * handled here because they do not always produce bindings for a given
     * variable and hence have semantics similar to optionals for the purposes
     * of attaching constraints and reordering join graphs. For union, make sure 
     * it's not an empty union.
     * 
     * 5. Pipeline the postConditionals. Add materialization steps as needed.
     * </pre>
     * 
     * @param joinGroup
     *            The join group.
     * @param doneSet
     *            The set of variables which are already known to be
     *            materialized.
     */
    private static PipelineOp convertJoinGroup(PipelineOp left,
            final JoinGroupNode joinGroup,
            final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx, 
            final boolean needsEndOp) {

        final StaticAnalysis sa = ctx.sa;

        // /*
        // * Place the StartOp at the beginning of the pipeline.
        // *
        // * TODO We only need a start op on a cluster if there is a requirement
        // * to marshall all solutions from a parent's evaluation context onto
        // the
        // * top-level query controller. I am not sure that we ever have to do
        // * this for a subquery, but there might be cases where it is required.
        // * For all other cases, we should begin with left := null. However,
        // * there are cases where getParent() is running into a null reference
        // * during query evaluation if [left := null] here. Look into and
        // resolve
        // * those issues.
        // */
        // if(left == null)
        // left = addStartOp(ctx);

        /*
         * This checks to make sure that join filters were attached to join
         * nodes, in which case they should no longer be present as direct
         * children of this group.
         * 
         * @see ASTAttachJoinFilterOptimizer
         */
        assert sa.getJoinFilters(joinGroup).isEmpty() : "Unattached join filters: "
                + joinGroup;
        
        /*
         * FIXME We need to move away from the DataSetJoin class and replace it
         * with an IPredicate to which we have attached an inline access path.
         * That transformation needs to happen in a rewrite rule, which means
         * that we will wind up removing the IN filter and replacing it with an
         * AST node for that inline AP (something conceptually similar to a
         * statement pattern but for a column projection of the variable for the
         * IN expression). That way we do not have to magically "subtract" the
         * known "IN" filters out of the join- and post- filters.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Replace
         * DataSetJoin with an "inline" access path.)
         * 
         * @see JoinGroupNode#getInFilters()
         */
        final Set<FilterNode> inFilters = new LinkedHashSet<FilterNode>();
        inFilters.addAll(joinGroup.getInFilters());
        
        for (IGroupMemberNode child : joinGroup) {

            if (child instanceof StatementPatternNode) {
                final StatementPatternNode sp = (StatementPatternNode) child;
                /*
                 * Add statement pattern joins and the filters on those joins.
                 * 
                 * Note: This handles both required and optional statement
                 * pattern joins, but not optional join groups.
                 * 
                 * Note: This winds up handling materialization steps as well
                 * (it calls through to Rule2BOpUtility).
                 * 
                 * TODO The RTO will need to assign the ids to joins so it can
                 * correlate them with the {@link IJoinNode}s.
                 */
                final Predicate<?> pred = toPredicate(sp, ctx);
                left = join(ctx.db, ctx.queryEngine, left, pred, doneSet,
                        getJoinConstraints(sp), new BOpContextBase(
                                ctx.queryEngine), ctx.idFactory,
                        sp.getQueryHints());
                continue;
            } else if (child instanceof ServiceNode) {
                // SERVICE
                left = addServiceCall(left, (ServiceNode) child, doneSet, ctx);
                continue;
            } else if (child instanceof NamedSubqueryInclude) {
                /*
                 * INCLUDE
                 */
                left = addNamedSubqueryInclude(left,
                        (NamedSubqueryInclude) child, doneSet, ctx);
                continue;
            } else if (child instanceof SubqueryRoot) {
                /*
                 * SPARQL 1.1 style subqueries which were not lifted out into
                 * named subqueries.
                 */
                left = addSparql11Subquery(left, (SubqueryRoot) child, doneSet,
                        ctx);
                continue;
            } else if (child instanceof GraphPatternGroup<?>) {
                /*
                 * Sub-groups, OPTIONAL groups, and UNION.
                 * 
                 * This is all handled by addSubgroup. The plan within the
                 * subgroup will be somewhat different if the subgroup is a
                 * JoinGroupNode or a UnionNode, but join filter attachment is
                 * handled by the same code in both cases.
                 * 
                 * FIXME For optional SPs and optional child groups, the same
                 * optimizer which attaches the join filters to the required
                 * joins should order the join filters after the last optional
                 * in which any variable required by that filter MIGHT be bound.
                 * I think that the getJoinGroupConstraints() code might already
                 * handle this, but ASTAttachJoinFiltersOptimizer only looks at
                 * the required joins right now. Once we do this we will no
                 * longer have "post-" conditionals in the join group as all
                 * "post-" filters will not be run at the earliest allowable
                 * moment.
                 */
                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> subgroup = (GraphPatternGroup<IGroupMemberNode>) child;
                left = addSubgroup(left, subgroup, doneSet, ctx);
                continue;
            } else if (child instanceof FilterNode) {
                final FilterNode filter = (FilterNode) child;
                if (inFilters.contains(filter)) {
                    left = addKnownInConditional(left, filter, ctx);
                    continue;
                }
                // FILTER
                left = addConditional(left, filter, doneSet, ctx);
                continue;
            } else if (child instanceof AssignmentNode) {
                // LET / BIND
                left = addAssignment(left, (AssignmentNode) child, doneSet,
                        ctx, false/* projection */);
                continue;
            } else {
                throw new UnsupportedOperationException("child: " + child);
            }
        } // next child.
        
        /*
         * Add the end operator if necessary.
         */
        if (needsEndOp && joinGroup.getParent() != null) {
            left = addEndOp(left, ctx);
        }

        return left;

    }

    /**
     * Conditionally add a {@link StartOp} iff the query will rin on a cluster.
     * 
     * @param ctx
     * 
     * @return The {@link StartOp} iff this query will run on a cluster and
     *         otherwise <code>null</code>.
     * 
     *         TODO This is experimental (it is currently disabled) and does not
     *         appear to be necessary.
     */
	private static final PipelineOp addStartOpOnCluster(final AST2BOpContext ctx) {
	
		if (false && ctx.isCluster())
			return addStartOp(ctx);
		
    	return null;
    	
    }
    
    /**
     * Adds a {@link StartOp}.
     * <p>
     * Note: {@link StartOp} is not necessary in query plans. It is just a
     * convenient concept. When not using {@link StartOp}, the first operator in
     * the plan just needs to have an empty <code>args[]</code>. The only time
     * this is really necessary is when the top-level query plan would otherwise
     * be empty (a <code>null</code>). In this case, the {@link StartOp} just
     * copies its inputs to its outputs (which is all it ever does).
     * 
     * @return The {@link StartOp}.
     */
    private static final PipelineOp addStartOp(final AST2BOpContext ctx) {

        final PipelineOp start = applyQueryHints(
                new StartOp(BOp.NOARGS, NV.asMap(new NV[] {//
                                new NV(Predicate.Annotations.BOP_ID, ctx
                                        .nextId()),
                                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                        BOpEvaluationContext.CONTROLLER), })),
                ctx.queryHints);

        return start;

    }

    /**
     * Add an assignment to the query plan.
     * 
     * @param left
     * @param assignmentNode
     * @param doneSet
     * @param ctx
     * @param projection
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final PipelineOp addAssignment(PipelineOp left,
            final AssignmentNode assignmentNode,
            final Set<IVariable<?>> doneSet, final AST2BOpContext ctx,
            final boolean projection) {

        final IValueExpression ve = assignmentNode.getValueExpression();

        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        /*
         * Get the vars this filter needs materialized.
         */
        vars.addAll(assignmentNode.getMaterializationRequirement()
                .getVarsToMaterialize());

        /*
         * Remove the ones we've already done.
         */
        vars.removeAll(doneSet);

        final int bopId = ctx.nextId();

        final ConditionalBind b = new ConditionalBind(
                assignmentNode.getVar(),
                assignmentNode.getValueExpression(), projection);

        IConstraint c = projection ? new ProjectedConstraint(b)
                : new BindingConstraint(b);

        /*
         * We might have already materialized everything we need for this
         * filter.
         */
        if (vars.size() > 0) {

            left = addMaterializationSteps(left, bopId, ve, vars, ctx);

            /*
             * Add all the newly materialized variables to the set we've
             * already done.
             */
            doneSet.addAll(vars);

            c = new TryBeforeMaterializationConstraint(c);

        }

        left = applyQueryHints(
                new ConditionalRoutingOp(
                        leftOrEmpty(left),
                        NV.asMap(new NV[] {//
                                new NV(BOp.Annotations.BOP_ID, bopId),
                                new NV(
                                        ConditionalRoutingOp.Annotations.CONDITION,
                                        c), })), ctx.queryHints);

        return left;

    }

    /**
     * Add a FILTER which will not be attached to a required join to the
     * pipeline.
     * 
     * @param left
     * @param filter
     *            The filter.
     * @param doneSet
     *            The set of variables which are already known to be
     *            materialized.
     * @param ctx
     * @return
     */
    @SuppressWarnings("rawtypes")
    private static final PipelineOp addConditional(PipelineOp left,
            final FilterNode filter,
            final Set<IVariable<?>> doneSet, final AST2BOpContext ctx) {

        @SuppressWarnings("unchecked")
        final IValueExpression<IV> ve = (IValueExpression<IV>) filter
                .getValueExpression();

        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        /*
         * Get the variables that this filter needs materialized.
         */
        vars.addAll(filter.getMaterializationRequirement()
                .getVarsToMaterialize());

        /*
         * Remove the ones we've already done.
         */
        vars.removeAll(doneSet);

        final int bopId = ctx.nextId();

        /*
         * If we have not materialized everything we need for this filter then
         * add those materialization steps now.
         */
        if (!vars.isEmpty()) {

            // Add materialization steps for those variables.
            left = addMaterializationSteps(left, bopId, ve, vars, ctx);

            // Add the newly materialized vars to the set we've already done.
            doneSet.addAll(vars);

        }

        final IConstraint c = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                ve);

        left = applyQueryHints(new ConditionalRoutingOp(leftOrEmpty(left),//
                new NV(BOp.Annotations.BOP_ID, bopId), //
                new NV(ConditionalRoutingOp.Annotations.CONDITION, c)//
                ), ctx.queryHints);

        return left;

    }

    /**
     * Convert an IN filter to a join with an inline access path.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Replace
     *      DataSetJoin with an "inline" access path.)
     * 
     *      TODO This mechanism is not currently being used. If we decide to
     *      keep it, we could use a {@link JVMSolutionSetHashJoinOp}. Just push
     *      the data into the hash index and then just that operator to join it
     *      into the pipeline. Then we could ditch the {@link DataSetJoin}.
     */
    @SuppressWarnings("rawtypes")
    private static final PipelineOp addKnownInConditional(PipelineOp left,
            final FilterNode filter, final AST2BOpContext ctx) {

        final InBOp bop = (InBOp) filter.getValueExpression();
        
        final IConstant<IV>[] set = bop.getSet();
        
        final LinkedHashSet<IV> ivs = new LinkedHashSet<IV>();
        
        for (IConstant<IV> iv : set) {
        
            ivs.add(iv.get());
            
        }

        final IVariable var = (IVariable) bop.getValueExpression();

        left = new DataSetJoin(leftOrEmpty(left), NV.asMap(new NV[] {//
                new NV(DataSetJoin.Annotations.VAR, var),//
                new NV(DataSetJoin.Annotations.BOP_ID, ctx.idFactory
                        .incrementAndGet()),//
                new NV(DataSetJoin.Annotations.GRAPHS, ivs) //
                }));

        return left;

    }
    
    /**
     * This method handles sub-groups, including UNION, required sub-groups, and
     * OPTIONAL subgroups.
     * <p>
     * The basic pattern for efficient sub-group evaluation is to build a hash
     * index before the sub-group, run the sub-group against the solutions in
     * that hash index, and then hash join the sub-group solutions back into the
     * solutions in the hash index. This last step reunites the sub-group
     * solutions with the solutions in the parent group. The hash index and hash
     * join steps use the same join variables. This pattern works best when one
     * or more join variables can be identified for the hash index / hash join
     * steps.
     * 
     * @param left
     * @param subgroup
     * @param ctx
     * @return
     */
	private static PipelineOp addSubgroup(//
			PipelineOp left,//
			final GraphPatternGroup<IGroupMemberNode> subgroup,//
			final Set<IVariable<?>> doneSet,
			final AST2BOpContext ctx//
			) {

		if (ctx.isCluster()
				&& BOpUtility.visitAll((BOp) subgroup,
						NamedSubqueryInclude.class).hasNext()) {
			/*
			 * Since something in the subgroup (or recursively in some
			 * sub-subgroup) will require access to a named solution set, we
			 * add a CopyOp() to force the solutions to be materialized on
			 * the top-level query controller before issuing the subquery.
			 * 
			 * @see https://sourceforge.net/apps/trac/bigdata/ticket/379#comment:1
			 */
			left = new CopyOp(leftOrEmpty(left), NV.asMap(new NV[] {//
				new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
				new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
						BOpEvaluationContext.CONTROLLER),//
				}));

        }

		// true iff the sub-group is OPTIONAL.
        final boolean optional = subgroup.isOptional();

        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(subgroup), needsMaterialization);

        if (optional && joinConstraints != null) {
            /*
             * Note: Join filters should only be attached to required joins, not
             * to join OPTIONAL joins (except for a simple optional statement
             * pattern node) or OPTIONAL join groups.
             */
            throw new AssertionError(
                    "OPTIONAL group has attached join filters: " + subgroup);
        }

	    /*
	     * Model a sub-group by building a hash index at the start of the
	     * group. We then run the group.  Finally, we do a hash join of
	     * the hash index against the solutions in the group.  If the
	     * group is optional, then the hash join is also optional.
	     * 
	     * @see https://sourceforge.net/apps/trac/bigdata/ticket/377#comment:4
	     */
        final String solutionSetName = "--set-" + ctx.nextId(); // Unique name.

        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = subgroup.getJoinVars();
        if (joinVars == null) {
            /*
             * The AST optimizer which assigns the join variables did not
             * run.
             * 
             * Note: It is Ok if there are no join variables (an empty[]).
             * Sometimes there are no join variables and we wind up doing a
             * cross-product compare in the hash join. However, it is NOT Ok
             * if the join variables annotation was never set (a null).
             */
            throw new RuntimeException("Join variables not specified: "
                    + subgroup);
        }
        
	    // Pass all variable bindings along.
        @SuppressWarnings("rawtypes")
        final IVariable[] selectVars = null;
        
        final NamedSolutionSetRef namedSolutionSet = new NamedSolutionSetRef(
                ctx.queryId, solutionSetName, joinVars);

        final PipelineOp op;
        if(ctx.nativeHashJoins) {
            op = new HTreeHashIndexOp(leftOrEmpty(left),//
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),// required
                new NV(HTreeHashIndexOp.Annotations.OPTIONAL, optional),//
                new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HTreeHashIndexOp.Annotations.SELECT, selectVars),//
                new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );
        } else {
            op = new JVMHashIndexOp(leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.LAST_PASS, true),// required
                    new NV(HTreeHashIndexOp.Annotations.OPTIONAL, optional),//
                    new NV(HTreeHashIndexOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(HTreeHashIndexOp.Annotations.SELECT, selectVars),//
                    new NV(HTreeHashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            ); 
        }

        final PipelineOp subqueryPlan = convertJoinGroupOrUnion(op/* left */,
                subgroup, doneSet, ctx);

        // lastPass is required if the join is optional.
        final boolean lastPass = optional; // iff optional.

        // true if we will release the HTree as soon as the join is done.
        // Note: also requires lastPass.
        final boolean release = lastPass && true;
        
        if(ctx.nativeHashJoins) {
            left = new HTreeSolutionSetHashJoinOp(
                new BOp[] { subqueryPlan },//
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
//                new NV(HTreeSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
//                new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                new NV(HTreeSolutionSetHashJoinOp.Annotations.SELECT, selectVars),//
//                new NV(HTreeSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                new NV(HTreeSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
        );
        } else {
            left = new JVMSolutionSetHashJoinOp(
                    new BOp[] { subqueryPlan },//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.SELECT, selectVars),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
            );
        }

        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps(left, doneSet, needsMaterialization,
                subgroup.getQueryHints(), ctx);

        return left;
        
    }
    
    /**
     * Wrap with an operator which will be evaluated on the query controller so
     * the results will be streamed back to the query controller in scale-out.
     * <p>
     * Note: For scale-out, we need to stream the results back to the node from
     * which the subquery was issued. If the subquery is issued against the
     * local query engine where the {@link IBindingSet} was produced, then the
     * that query engine is the query controller for the subquery and an
     * {@link EndOp} on the subquery would bring the results for the subquery
     * back to that query controller.
     * <p>
     * Note: This should be conditional based on whether or not we are running
     * on a cluster, but also see
     * https://sourceforge.net/apps/trac/bigdata/ticket/227.
     */
    private static final PipelineOp addEndOp(PipelineOp left,
            final AST2BOpContext ctx) {

		if (left != null
				&& ctx.isCluster()
                && !left.getEvaluationContext().equals(
                        BOpEvaluationContext.CONTROLLER)) {

            left = new EndOp(leftOrEmpty(left),//
                    NV.asMap(
                            //
                            new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                            new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER)//
                    ));

        }

        return left;

    }

    /**
     * Add DISTINCT semantics to the pipeline.
     * 
     * TODO Support parallel decomposition of distinct on a cluster (DISTINCT
     * can be run on each node if we hash partition the DISTINCT operator based
     * on the variables on which DISTINCT will be imposed and the results when
     * streamed back to the controller will still be distinct.)
     */
    private static final PipelineOp addDistinct(PipelineOp left,
            final QueryBase query, final AST2BOpContext ctx) {

        final int bopId = ctx.nextId();

        final ProjectionNode projection = query.getProjection();

        if (projection.isWildcard())
            throw new AssertionError("Wildcard projection was not rewritten.");
        
        final IVariable<?>[] vars = projection.getProjectionVars();

        final PipelineOp op;
        if (ctx.nativeDistinct) {
            /*
             * DISTINCT on the JVM heap.
             */
            op = new DistinctBindingSetOp(
                    leftOrEmpty(left),//
                    new NV(DistinctBindingSetOp.Annotations.BOP_ID, bopId),
                    new NV(DistinctBindingSetOp.Annotations.VARIABLES, vars),
                    new NV(DistinctBindingSetOp.Annotations.EVALUATION_CONTEXT,
                           BOpEvaluationContext.CONTROLLER),
                    new NV(DistinctBindingSetOp.Annotations.SHARED_STATE, true)//
            );
        } else {
            /*
             * DISTINCT on the native heap.
             */
            op = new DistinctBindingSetsWithHTreeOp(leftOrEmpty(left),//
                    new NV(DistinctBindingSetsWithHTreeOp.Annotations.BOP_ID,
                           bopId),//
                    new NV(DistinctBindingSetsWithHTreeOp.Annotations.VARIABLES,
                           vars),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                           BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1)//
            );
        }

        left = applyQueryHints(op, ctx.queryHints);

        return left;

    }

    /**
     * Add an aggregation operator. It will handle the grouping (if any),
     * optional having filter, and projected select expressions. A generalized
     * aggregation operator will be used unless the aggregation corresponds to
     * some special case.
     * 
     * @param left
     *            The previous operator in the pipeline.
     * @param projection
     *            The projected select expressions (MUST be aggregates when
     *            interpreted in context with groupBy and having).
     * @param groupBy
     *            The group by clause (optional).
     * @param having
     *            The having clause (optional).
     * @param ctx
     * 
     * @return The left-most operator in the pipeline.
     */
    @SuppressWarnings("rawtypes")
    private static final PipelineOp addAggregation(PipelineOp left,
            final ProjectionNode projection, final GroupByNode groupBy,
            final HavingNode having, final AST2BOpContext ctx) {

        final IValueExpression<?>[] projectExprs = projection
                .getValueExpressions();

        final IValueExpression<?>[] groupByExprs = groupBy == null ? null
                : groupBy.getValueExpressions();

        final IConstraint[] havingExprs = having == null ? null : having
                .getConstraints();

        final IGroupByState groupByState = new GroupByState(projectExprs,
                groupByExprs, havingExprs);

        final IGroupByRewriteState groupByRewrite = new GroupByRewriter(
                groupByState);

        final int bopId = ctx.nextId();

        final GroupByOp op;

        /*
         * We need to materialize variables which are used in value expressions
         * where the functions applied to the variable have materialization
         * requirements.
         * 
         * We also need to materialize variables which appear in ORDER BY
         * expressions.
         * 
         * We do not need to materialize the variable on which the value
         * expression becomes bound.
         * 
         * We do not need to materialize a bare variable which appears in a
         * GROUP BY or SELECT expression.
         * 
         * TODO Review. I believe that AssignmentNode.getValueExpression()
         * should always return the Bind().
         */
        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        if (projectExprs != null) {

            for (IValueExpression expr : projectExprs) {

                if (expr instanceof Bind) {

                    // We do not need to materialize the variable on which the
                    // computed expression is bound.
//                    vars.add(((Bind) expr).getVar());

                    // Note: side-effect on expr!
                    expr = ((Bind) expr).getExpr();

                }

                if (expr instanceof IVariable<?>) {

                    // We do not need to materialize a bare variable.
//                    vars.add((IVariable<IV>) expr);

                } else {

                    StaticAnalysis.gatherVarsToMaterialize(expr, vars);

                }

            }

        }

        if (groupByExprs != null) {

            for (IValueExpression expr : groupByExprs) {

                if (expr instanceof Bind) {

                    // We do not need to materialize the variable on which the
                    // computed expression is bound.
//                    vars.add(((Bind) expr).getVar());

                    // Note: side-effect on expr.
                    expr = ((Bind) expr).getExpr();

                }

                if (expr instanceof IVariable<?>) {

                    // We do not need to materialize a bare variable.
//                    vars.add((IVariable<IV>) expr);

                } else {

                    StaticAnalysis.gatherVarsToMaterialize(expr, vars);

                }

            }

        }

        left = addMaterializationSteps(left, bopId, vars, ctx);

        if (!groupByState.isAnyDistinct() && !groupByState.isSelectDependency()
                && !groupByState.isNestedAggregates()) {

            /*
             * Extremely efficient pipelined aggregation operator.
             * 
             * TODO This operation can be parallelized on a cluster either if it
             * does not use any operators which can not be decomposed (such as
             * AVERAGE) or if we rewrite such operators into AVG(X) :=
             * SUM(X)/COUNT(X). When it is parallelized, we need to run it on
             * each node and add another instance of this operator on the query
             * controller which aggregates the aggregates from the nodes in the
             * cluster. If we have done rewrites of things like AVERAGE, then we
             * can not compute the AVERAGE until after we have computed the
             * aggregate of aggregates. The simplest way to do that is using a
             * LET operator after the aggregates have been combined.
             */

            op = new PipelinedAggregationOp(leftOrEmpty(left),//
                    NV.asMap(new NV[] {//
                            new NV(BOp.Annotations.BOP_ID, bopId),//
                            new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(PipelineOp.Annotations.PIPELINED, true),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                            new NV(GroupByOp.Annotations.GROUP_BY_STATE,
                                    groupByState), //
                            new NV(GroupByOp.Annotations.GROUP_BY_REWRITE,
                                    groupByRewrite), //
                            new NV(PipelineOp.Annotations.LAST_PASS, true),//
                    }));

        } else {

            /*
             * General aggregation operator on the JVM heap.
             * 
             * TODO There is a sketch of a generalized aggregation operator for
             * the native heap but it needs to be reworked (the control
             * structure is incorrect). This generalized aggregation operator
             * for the native heap would be a better only when the #of solutions
             * to be grouped is large and we can not pipeline the aggregation.
             * (The code for this is either on the CI machine or the
             * workstation). BBT 8/17/2011.
             */

            op = new MemoryGroupByOp(leftOrEmpty(left), NV.asMap(new NV[] {//
                            new NV(BOp.Annotations.BOP_ID, bopId),//
                            new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(PipelineOp.Annotations.PIPELINED, false),//
                            new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                            new NV(GroupByOp.Annotations.GROUP_BY_STATE,
                                    groupByState), //
                            new NV(GroupByOp.Annotations.GROUP_BY_REWRITE,
                                    groupByRewrite), //
                    }));

        }

        left = applyQueryHints(op, ctx.queryHints);

        return left;

    }

    /**
     * Add an ORDER BY operator.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final PipelineOp addOrderBy(PipelineOp left,
            final OrderByNode orderBy, final AST2BOpContext ctx) {

        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        final ISortOrder<IV>[] sortOrders = new ISortOrder[orderBy.size()];

        final Iterator<OrderByExpr> it = orderBy.iterator();

        for (int i = 0; it.hasNext(); i++) {

            final OrderByExpr orderByExpr = it.next();

            IValueExpression<?> expr = orderByExpr.getValueExpression();

            if (!(expr instanceof IVariableOrConstant<?> && !(expr instanceof IBind))) {

                /*
                 * Wrap the expression with a BIND of an anonymous variable.
                 */

                expr = new Bind(Var.var(), expr);

            }

            if (expr instanceof IVariable<?>) {

                vars.add((IVariable<IV>) expr);

            } else {

                StaticAnalysis.gatherVarsToMaterialize(
                        expr, vars);

            }

            sortOrders[i] = new SortOrder(expr, orderByExpr.isAscending());

        }

        final int sortId = ctx.nextId();

        left = addMaterializationSteps(left, sortId, vars, ctx);

        left = applyQueryHints(
                new MemorySortOp(
                        leftOrEmpty(left),
                        NV.asMap(new NV[] {//
                                new NV(MemorySortOp.Annotations.BOP_ID, sortId),//
                                new NV(MemorySortOp.Annotations.SORT_ORDER,
                                        sortOrders),//
                                new NV(
                                        MemorySortOp.Annotations.VALUE_COMPARATOR,
                                        new IVComparator()),//
                                new NV(
                                        MemorySortOp.Annotations.EVALUATION_CONTEXT,
                                        BOpEvaluationContext.CONTROLLER),//
                                new NV(MemorySortOp.Annotations.PIPELINED, true),//
                                new NV(MemorySortOp.Annotations.MAX_PARALLEL, 1),//
//                                new NV(MemorySortOp.Annotations.SHARED_STATE,
//                                        true),//
                                new NV(MemorySortOp.Annotations.LAST_PASS, true),//
                        })), ctx.queryHints);

        return left;

    }

    /**
     * Impose an OFFSET and/or LIMIT on a query.
     */
    private static final PipelineOp addSlice(PipelineOp left,
            final SliceNode slice, final AST2BOpContext ctx) {

        final int bopId = ctx.nextId();

        left = applyQueryHints(new SliceOp(leftOrEmpty(left),//
                new NV(SliceOp.Annotations.BOP_ID, bopId),//
                new NV(SliceOp.Annotations.OFFSET, slice.getOffset()),//
                new NV(SliceOp.Annotations.LIMIT, slice.getLimit()),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(SliceOp.Annotations.PIPELINED, true),//
                new NV(SliceOp.Annotations.MAX_PARALLEL, 1),//
                new NV(MemorySortOp.Annotations.SHARED_STATE, true)//
                ), ctx.queryHints);

        return left;

    }

    /**
     * Method produces a {@link Predicate} which captures the semantics of the
     * {@link StatementPatternNode}.
     * <p>
     * Note: This method is responsible for layering in the default graph and
     * named graph semantics on the access path associated with a statement
     * pattern.
     * <p>
     * Note: This method is NOT responsible for selecting the statement index to
     * use for the access path. That is decided when we determine the join
     * ordering.
     * 
     * @param sp
     *            The statement pattern.
     * @param ctx
     * 
     * @return The {@link Predicate} which models the access path constraints
     *         for that statement pattern.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Replace
     *      DataSetJoin with an "inline" access path.)
     */
    @SuppressWarnings("rawtypes")
    private static final Predicate toPredicate(final StatementPatternNode sp,
            final AST2BOpContext ctx) {

        final QueryRoot query = ctx.astContainer.getOptimizedAST();
        
        final AbstractTripleStore database = ctx.db;

        final DatasetNode dataset = query.getDataset();

        final Value predValue = sp.p().getValue();
        if (log.isDebugEnabled()) {
            log.debug(predValue);
        }

        final IVariableOrConstant<IV> s = sp.s().getValueExpression();
        final IVariableOrConstant<IV> p = sp.p().getValueExpression();
        final IVariableOrConstant<IV> o = sp.o().getValueExpression();

        // The graph term/variable iff specified by the query.
        final TermNode cvar = sp.c();
        final IVariableOrConstant<IV> c = cvar == null ? null : cvar
                .getValueExpression();

        // The annotations for the predicate.
        final List<NV> anns = new LinkedList<NV>();

        anns.add(new NV(IPredicate.Annotations.RELATION_NAME,
                new String[] { database.getSPORelation().getNamespace() }));

        // timestamp
        anns.add(new NV(IPredicate.Annotations.TIMESTAMP, database
                .getSPORelation().getTimestamp()));

        // bopId for the predicate.
        anns.add(new NV(IPredicate.Annotations.BOP_ID, ctx.idFactory
                .incrementAndGet()));

        // Propagate the estimated cardinality to the Predicate.
        anns.add(new NV(Annotations.ESTIMATED_CARDINALITY,
                sp.getProperty(Annotations.ESTIMATED_CARDINALITY)));

        // Propagate the index which would be used if we do not run the
        // predicate "as-bound".
        anns.add(new NV(Annotations.ORIGINAL_INDEX,
                sp.getProperty(Annotations.ORIGINAL_INDEX)));

        // Propagate the optional query hint override for the index to use.
        anns.add(new NV(IPredicate.Annotations.KEY_ORDER,
                sp.getProperty(IPredicate.Annotations.KEY_ORDER)));

        if (sp.isOptional()) {
            // Mark the join as optional.
            anns.add(new NV(IPredicate.Annotations.OPTIONAL, Boolean.TRUE));
        }

        final Properties queryHints = sp.getQueryHints();
        
        if (queryHints != null
                && Boolean.parseBoolean(queryHints.getProperty(
                        Annotations.HASH_JOIN, "false"))) {

            /*
             * Use a hash join for this predicate.
             * 
             * Note: In order to run the predicate using a hash join we need to
             * figure out what the JOIN_VARS[] will be for the predicate. The
             * join variables must be (a) bound by the predicate when we run the
             * access path; and (b) known incoming bound for the predicate when
             * we run that join in the current evaluation order.
             */

            // Start with everything known bound on entry.
            final Set<IVariable<?>> joinVars = ctx.sa
                    .getDefinitelyIncomingBindings(sp,
                            new LinkedHashSet<IVariable<?>>());

            // Find all variables which this predicate will bind.
            final Set<IVariable<?>> predVars = ctx.sa
                    .getDefinitelyProducedBindings(sp,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */);
            
            // Retain only those variables which this predicate will bind.
            joinVars.retainAll(predVars);
            
            if (!joinVars.isEmpty()) {

                /*
                 * A hash join can only be used when there is at least one
                 * variable which is known to be bound and which will also be
                 * bound by the predicate.
                 */

                anns.add(new NV(Annotations.HASH_JOIN, true));

                anns.add(new NV(HashJoinAnnotations.JOIN_VARS, joinVars
                        .toArray(new IVariable[joinVars.size()])));

            }
            
        }
        
        if (!database.isQuads()) {
            /*
             * Either triple store mode or provenance mode.
             */
            if (cvar != null && database.isStatementIdentifiers()
                    && cvar.getValue() != null) {
                /*
                 * Note: The context position is used as a statement identifier
                 * (SID). SIDs may be used to retrieve provenance statements
                 * (statements about statement) using high-level query. SIDs are
                 * represented as blank nodes and is not possible to have them
                 * bound in the original query. They only become bound during
                 * query evaluation.
                 */
                throw new IllegalArgumentException(
                        "Context position is a statement identifier and may not be bound in the original query: "
                                + sp);
            }
        } else {
            /*
             * Quad store mode.
             */

            // quads mode.
            anns.add(new NV(Rule2BOpUtility.Annotations.QUADS, true));
            // attach the Scope.
            anns.add(new NV(Rule2BOpUtility.Annotations.SCOPE, sp.getScope()));

            /*
             * Note: For a default graph access path, [cvar] can not become
             * bound since the context will is stripped from the visited ISPOs.
             */
            if (dataset != null) {
                // attach the DataSet.
                anns.add(new NV(Rule2BOpUtility.Annotations.DATASET, dataset));
                switch (sp.getScope()) {
                case DEFAULT_CONTEXTS: {
                    if (dataset.getDefaultGraphFilter() != null) {
                        anns.add(new NV(
                                IPredicate.Annotations.INDEX_LOCAL_FILTER,
                                ElementFilter.newInstance(dataset
                                        .getDefaultGraphFilter())));
                    }
                    break;
                }
                case NAMED_CONTEXTS: {
                    if (dataset.getNamedGraphFilter() != null) {
                        anns.add(new NV(
                                IPredicate.Annotations.INDEX_LOCAL_FILTER,
                                ElementFilter.newInstance(dataset
                                        .getNamedGraphFilter())));
                    }
                    break;
                }
                default:
                    throw new AssertionError();
                }
            }
        } // quads

        /*
         * Layer on filters which can run on the local access path (close to the
         * data).
         * 
         * Note: We can now stack filters so there may be other things which can
         * be leveraged here.
         * 
         * FIXME Handle StatementPatternNode#EXISTS here (This turns into an
         * iterator with a limit of ONE (1) on the {@link SPOAccessPath}. This
         * should turn into a LIMIT annotation on the access path (which does
         * not currently recognize an offset/limit annotation; instead you use
         * an alternative iterator call).
         */
        {

            final List<IFilter> filters = new LinkedList<IFilter>();

            if (Boolean.valueOf(sp.getProperty(
                    StatementPatternNode.Annotations.DISTINCT,
                    StatementPatternNode.Annotations.DEFAULT_DISTINCT))) {

                /*
                 * Visit only the distinct values for the first key component.
                 * 
                 * FIXME We MUST pin the choice of the index for this filter. In
                 * order to do that we need to interact with the join ordering
                 * since we typically want to run this first in a join group.
                 * 
                 * TODO GRAPH ?g {} can use this to visit the distinct named
                 * graphs for which there exists a statement in the database.
                 * 
                 * FIXME One exception where we can not use PARALLEL without
                 * also imposing DISTINCT is the @DISTINCT annotation in
                 * scale-out. This is because a given IV in the first key
                 * component could span more than one shard.
                 */
                filters.add(new DistinctTermAdvancer(database.isQuads() ? 4 : 3));
                
            }

            if (!query.getIncludeInferred()) {
                /*
                 * Filter out anything which is not an Explicit statement.
                 */
                filters.add(ElementFilter
                        .newInstance(ExplicitSPOFilter.INSTANCE));
            }

            final int nfilters = filters.size();
            final IFilter filter;
            if (nfilters > 0) {
                if (nfilters == 1) {
                    // Just the one filter.
                    filter = filters.get(0);
                } else {
                    // Stack multiple filters.
                    final FilterBase tmp = new NOPFilter();
                    for (IFilter f : filters) {
                        tmp.addFilter(f);
                    }
                    filter = tmp;
                }
                /*
                 * Attach a filter on elements visited by the access path.
                 */
                anns.add(new NV(IPredicate.Annotations.INDEX_LOCAL_FILTER,
                        filter));
            }

        }
        
        /*
         * Explicitly set the access path / iterator flags.
         * 
         * Note: High level query generally permits iterator level parallelism.
         * We set the PARALLEL flag here so it can be used if a global index
         * view is chosen for the access path.
         * 
         * Note: High level query for SPARQL always uses read-only access paths.
         * If you are working with a SPARQL extension with UPDATE or INSERT INTO
         * semantics then you will need to remote the READONLY flag for the
         * mutable access paths.
         */
        {
         
            final int flags = IRangeQuery.DEFAULT | IRangeQuery.PARALLEL
                    | IRangeQuery.READONLY;
            
            anns.add(new NV(IPredicate.Annotations.FLAGS, flags));
            
        }

        {
        
            // Decide on the correct arity for the predicate.
            final BOp[] vars;
            if (!database.isQuads() && !database.isStatementIdentifiers()) {
                vars = new BOp[] { s, p, o };
            } else if (c == null) {
                vars = new BOp[] { s, p, o, com.bigdata.bop.Var.var() };
            } else {
                vars = new BOp[] { s, p, o, c };
            }

            return new SPOPredicate(vars, anns.toArray(new NV[anns.size()]));

        }

    }

    /**
     * Convert an {@link IValueExpressionNode} (recursively) to an
     * {@link IValueExpression}.
     * 
     * @param lex
     *            The lexicon namespace.
     * @param node
     *            The expression to convert.
     * 
     * @return The converted expression.
     * 
     * @see ASTSetValueExpressionsOptimizer
     */
    public static final IValueExpression<? extends IV> toVE(final String lex,
            final IValueExpressionNode node) {

        /*
         * Check to see if value expression has already been created and cached
         * on node.
         */
        if (node.getValueExpression() != null) {

            return node.getValueExpression();

        }

        if (node instanceof VarNode) {

            return node.getValueExpression();

        } else if (node instanceof ConstantNode) {

            return node.getValueExpression();

        } else if (node instanceof AssignmentNode) {

            final AssignmentNode assignment = (AssignmentNode) node;

            final IValueExpressionNode valueExpr = assignment
                    .getValueExpressionNode();

            @SuppressWarnings("rawtypes")
            final IValueExpression<? extends IV> ve = toVE(lex, valueExpr);

            return ve;

        } else if (node instanceof FunctionNode) {

            final FunctionNode functionNode = (FunctionNode) node;

            final URI functionURI = functionNode.getFunctionURI();

            final Map<String, Object> scalarValues = functionNode.getScalarValues();

            final ValueExpressionNode[] args = functionNode.args().toArray(
                    new ValueExpressionNode[functionNode.arity()]);

            @SuppressWarnings("rawtypes")
            final IValueExpression<? extends IV> ve = FunctionRegistry.toVE(
                    lex, functionURI, scalarValues, args);

            functionNode.setValueExpression(ve);

            return ve;

        } else {

            throw new IllegalArgumentException(node.toString());

        }

    }

    /*
     * TODO Integrate code to attach RangeBOps to predicates. (This code is from
     * the old BigdataEvaluationStrategyImpl3 class. It should be moved into an
     * IASTOptimizer for recognizing range constraints.)
     * 
     * TODO Ranges can be just upper or lower. They do not need to be both. They
     * can even be an excluded middle.
     * 
     * TODO The big thing about handling range constraints is make sure that we
     * query each part of the OS(C)P index which corresponds to a datatype which
     * could have a value legally promotable within the context in which the
     * LT/LTE/GT/GTE filter(s) occur. For example, x>5 && x<10 needs to do a
     * key-range scan for xsd:int, xsd:integer, .... The big win comes when we
     * can recognize a datatype constraint at the same time such that we only
     * touch one part of the index for that key range of values.
     */
//  private void attachRangeBOps(final SOpGroup g) {
//
//      final Map<IVariable,Collection<IValueExpression>> lowerBounds =
//          new LinkedHashMap<IVariable,Collection<IValueExpression>>();
//      final Map<IVariable,Collection<IValueExpression>> upperBounds =
//          new LinkedHashMap<IVariable,Collection<IValueExpression>>();
//      
//      for (SOp sop : g) {
//          final BOp bop = sop.getBOp();
//          if (!(bop instanceof SPARQLConstraint)) {
//              continue;
//          }
//          final SPARQLConstraint c = (SPARQLConstraint) bop;
//          if (!(c.getValueExpression() instanceof CompareBOp)) {
//              continue;
//          }
//          final CompareBOp compare = (CompareBOp) c.getValueExpression();
//          final IValueExpression left = compare.get(0);
//          final IValueExpression right = compare.get(1);
//          final CompareOp op = compare.op();
//          if (left instanceof IVariable) {
//              final IVariable var = (IVariable) left;
//              final IValueExpression ve = right;
//              if (op == CompareOp.GE || op == CompareOp.GT) {
//                  // ve is a lower bound
//                  Collection bounds = lowerBounds.get(var);
//                  if (bounds == null) {
//                      bounds = new LinkedList<IValueExpression>();
//                      lowerBounds.put(var, bounds);
//                  }
//                  bounds.add(ve);
//              } else if (op == CompareOp.LE || op == CompareOp.LT) {
//                  // ve is an upper bound
//                  Collection bounds = upperBounds.get(var);
//                  if (bounds == null) {
//                      bounds = new LinkedList<IValueExpression>();
//                      upperBounds.put(var, bounds);
//                  }
//                  bounds.add(ve);
//              }
//          } 
//          if (right instanceof IVariable) {
//              final IVariable var = (IVariable) right;
//              final IValueExpression ve = left;
//              if (op == CompareOp.LE || op == CompareOp.LT) {
//                  // ve is a lower bound
//                  Collection bounds = lowerBounds.get(var);
//                  if (bounds == null) {
//                      bounds = new LinkedList<IValueExpression>();
//                      lowerBounds.put(var, bounds);
//                  }
//                  bounds.add(ve);
//              } else if (op == CompareOp.GE || op == CompareOp.GT) {
//                  // ve is an upper bound
//                  Collection bounds = upperBounds.get(var);
//                  if (bounds == null) {
//                      bounds = new LinkedList<IValueExpression>();
//                      upperBounds.put(var, bounds);
//                  }
//                  bounds.add(ve);
//              }
//          }
//      }
//      
//      final Map<IVariable,RangeBOp> rangeBOps = 
//          new LinkedHashMap<IVariable,RangeBOp>();
//      
//      for (IVariable v : lowerBounds.keySet()) {
//          if (!upperBounds.containsKey(v))
//              continue;
//          
//          IValueExpression from = null;
//          for (IValueExpression ve : lowerBounds.get(v)) {
//              if (from == null)
//                  from = ve;
//              else
//                  from = new MathBOp(ve, from, MathOp.MAX,this.tripleSource.getDatabase().getNamespace());
//          }
//
//          IValueExpression to = null;
//          for (IValueExpression ve : upperBounds.get(v)) {
//              if (to == null)
//                  to = ve;
//              else
//                  to = new MathBOp(ve, to, MathOp.MIN,this.tripleSource.getDatabase().getNamespace());
//          }
//          
//          final RangeBOp rangeBOp = new RangeBOp(v, from, to); 
//          
//          if (log.isInfoEnabled()) {
//              log.info("found a range bop: " + rangeBOp);
//          }
//          
//          rangeBOps.put(v, rangeBOp);
//      }
//      
//      for (SOp sop : g) {
//          final BOp bop = sop.getBOp();
//          if (!(bop instanceof IPredicate)) {
//              continue;
//          }
//          final IPredicate pred = (IPredicate) bop;
//          final IVariableOrConstant o = pred.get(2);
//          if (o.isVar()) {
//              final IVariable v = (IVariable) o;
//              if (!rangeBOps.containsKey(v)) {
//                  continue;
//              }
//              final RangeBOp rangeBOp = rangeBOps.get(v);
//              final IPredicate rangePred = (IPredicate)
//                  pred.setProperty(SPOPredicate.Annotations.RANGE, rangeBOp);
//              if (log.isInfoEnabled())
//                  log.info("range pred: " + rangePred);
//              sop.setBOp(rangePred);
//          }
//      }
//  }
//  
//    public void testSimpleRange() throws Exception {
//        
////      final Sail sail = new MemoryStore();
////      sail.initialize();
////      final Repository repo = new SailRepository(sail);
//
//      final BigdataSail sail = getSail();
//      try {
//      sail.initialize();
//      final BigdataSailRepository repo = new BigdataSailRepository(sail);
//      
//      final RepositoryConnection cxn = repo.getConnection();
//      
//      try {
//          cxn.setAutoCommit(false);
//  
//          final ValueFactory vf = sail.getValueFactory();
//
//          /*
//           * Create some terms.
//           */
//          final URI mike = vf.createURI(BD.NAMESPACE + "mike");
//          final URI bryan = vf.createURI(BD.NAMESPACE + "bryan");
//          final URI person = vf.createURI(BD.NAMESPACE + "person");
//          final URI age = vf.createURI(BD.NAMESPACE + "age");
//          final Literal _1 = vf.createLiteral(1);
//          final Literal _2 = vf.createLiteral(2);
//          final Literal _3 = vf.createLiteral(3);
//          final Literal _4 = vf.createLiteral(4);
//          final Literal _5 = vf.createLiteral(5);
//          
//          /*
//           * Create some statements.
//           */
//          cxn.add(mike, age, _2);
//          cxn.add(mike, RDF.TYPE, person);
//          cxn.add(bryan, age, _4);
//          cxn.add(bryan, RDF.TYPE, person);
//          
//          /*
//           * Note: The either flush() or commit() is required to flush the
//           * statement buffers to the database before executing any operations
//           * that go around the sail.
//           */
//          cxn.commit();
//          
//          {
//              
//              String query =
//                  QueryOptimizerEnum.queryHint(QueryOptimizerEnum.None) +
//                  "prefix bd: <"+BD.NAMESPACE+"> " +
//                  "prefix rdf: <"+RDF.NAMESPACE+"> " +
//                  "prefix rdfs: <"+RDFS.NAMESPACE+"> " +
//                  
//                  "select * " +
//                  "where { " +
////                    "bd:productA bd:property1 ?origProperty1 . " +
////                    "?product bd:property1 ?simProperty1 . " +
////                    "FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120)) " +
//                  "  ?x bd:age ?age . " +
//                  "  ?x rdf:type bd:person . " +
//                  "  filter(?age > 1 && ?age < 3) " +
//                  "}"; 
//  
//              final SailTupleQuery tupleQuery = (SailTupleQuery)
//                  cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//              tupleQuery.setIncludeInferred(false /* includeInferred */);
//              
////            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
////            answer.add(createBindingSet(
////                    new BindingImpl("a", paul),
////                    new BindingImpl("b", mary)
////                    ));
////            answer.add(createBindingSet(
////                    new BindingImpl("a", brad),
////                    new BindingImpl("b", john)
////                    ));
////
////            final TupleQueryResult result = tupleQuery.evaluate();
////              compare(result, answer);
//
//          }
//          
//      } finally {
//          cxn.close();
//      }
//      } finally {
//          sail.__tearDownUnitTest();//shutDown();
//      }
//
//  }

}
