package com.bigdata.rdf.sparql.ast.eval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.Constant;
import com.bigdata.bop.ContextBindingSet;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bindingSet.EmptyBindingSet;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.CopyOp;
import com.bigdata.bop.bset.EndOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.bset.Tee;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.controller.JVMNamedSubqueryOp;
import com.bigdata.bop.controller.NamedSetAnnotations;
import com.bigdata.bop.controller.ServiceCallJoin;
import com.bigdata.bop.controller.Steps;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.StaticAnalysisStats;
import com.bigdata.bop.join.HTreeHashJoinUtility;
import com.bigdata.bop.join.HTreeMergeJoin;
import com.bigdata.bop.join.HTreePipelinedHashJoinUtility;
import com.bigdata.bop.join.HTreeSolutionSetHashJoinOp;
import com.bigdata.bop.join.HashIndexOp;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.IHashJoinUtilityFactory;
import com.bigdata.bop.join.JVMHashJoinUtility;
import com.bigdata.bop.join.JVMMergeJoin;
import com.bigdata.bop.join.JVMPipelinedHashJoinUtility;
import com.bigdata.bop.join.JVMSolutionSetHashJoinOp;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.bop.join.JoinTypeEnum;
import com.bigdata.bop.join.NestedLoopJoinOp;
import com.bigdata.bop.join.PipelinedHashIndexAndSolutionSetJoinOp;
import com.bigdata.bop.join.SolutionSetHashJoinOp;
import com.bigdata.bop.paths.ArbitraryLengthPathOp;
import com.bigdata.bop.paths.ZeroLengthPathOp;
import com.bigdata.bop.rdf.join.ChunkedMaterializationOp;
import com.bigdata.bop.rdf.join.DataSetJoin;
import com.bigdata.bop.rdf.join.MockTermResolverOp;
import com.bigdata.bop.rdf.join.VariableUnificationOp;
import com.bigdata.bop.solutions.DropOp;
import com.bigdata.bop.solutions.GroupByOp;
import com.bigdata.bop.solutions.GroupByRewriter;
import com.bigdata.bop.solutions.GroupByState;
import com.bigdata.bop.solutions.HTreeDistinctBindingSetsOp;
import com.bigdata.bop.solutions.IGroupByRewriteState;
import com.bigdata.bop.solutions.IGroupByState;
import com.bigdata.bop.solutions.ISortOrder;
import com.bigdata.bop.solutions.IVComparator;
import com.bigdata.bop.solutions.JVMDistinctBindingSetsOp;
import com.bigdata.bop.solutions.MemoryGroupByOp;
import com.bigdata.bop.solutions.MemorySortOp;
import com.bigdata.bop.solutions.PipelinedAggregationOp;
import com.bigdata.bop.solutions.ProjectionOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SortOrder;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.CoalesceBOp;
import com.bigdata.rdf.internal.constraints.ConditionalBind;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.internal.constraints.InBOp;
import com.bigdata.rdf.internal.constraints.IsBoundBOp;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.constraints.ProjectedConstraint;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.TryBeforeMaterializationConstraint;
import com.bigdata.rdf.internal.constraints.UUIDBOp;
import com.bigdata.rdf.internal.constraints.XSDBooleanIVValueExpression;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.NumericIV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ASTUtil;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.ComputedMaterializationRequirement;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.FilterExistsModeEnum;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.FunctionRegistry.UnknownFunctionBOp;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.RangeNode;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.SolutionSetStatserator;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.ZeroLengthPathNode;
import com.bigdata.rdf.sparql.ast.optimizers.ASTExistsOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTJoinOrderByTypeOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTNamedSubqueryOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSetValueExpressionsOptimizer;
import com.bigdata.rdf.sparql.ast.service.MockIVReturningServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallUtility;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.spo.DistinctTermAdvancer;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.striterator.Chunkerator;

import cutthecrap.utils.striterators.FilterBase;
import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.NOPFilter;

/**
 * Query plan generator converts an AST into a query plan made up of
 * {@link PipelineOp}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href=
 *      "https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=QueryEvaluation"
 *      >Query Evaluation</a>.
 */
public class AST2BOpUtility extends AST2BOpRTO {

    private static final transient Logger log = Logger
            .getLogger(AST2BOpUtility.class);

    /**
     * Top-level entry point converts an AST query model into an executable
     * query plan.
     * <p>
     * <strong>NOTE:</strong> This is the entry for {@link ASTEvalHelper}. Do
     * NOT use this entry point directly. It will evolve when we integrate the
     * RTO. Applications should use public entry points on {@link ASTEvalHelper}
     * instead.
     * 
     * @param ctx
     *            The evaluation context.
     * @param globallyScopedBindings
     *            Bindings that are considered as "globally scoped", i.e.
     *            are valid also in sub scopes.
     * 
     * @return The query plan which may be used to evaluate the query.
     * 
     *         TODO We could handle the IBindingSet[] by stuffing the data into
     *         a named solution set during the query rewrite and attaching that
     *         named solution set to the AST. This could allow for very large
     *         solution sets to be passed into a query. Any such change would
     *         have to be deeply integrated with the SPARQL parser in order to
     *         provide any benefit for the Java heap.
     * 
     *         TODO This logic is currently single-threaded. If we allow
     *         internal concurrency or when we integrate the RTO, we will need
     *         to ensure that the logic remains safely cancelable by an
     *         interrupt of the thread in which the query was submitted. See <a
     *         href="https://sourceforge.net/apps/trac/bigdata/ticket/715" >
     *         Interrupt of thread submitting a query for evaluation does not
     *         always terminate the AbstractRunningQuery </a>.
     */
    static PipelineOp convert(final AST2BOpContext ctx, 
       final IBindingSet[] globallyScopedBindings) {

        // The AST query model.
        final ASTContainer astContainer = ctx.astContainer;

        // The AST model as produced by the parser.
        final QueryRoot originalQuery = astContainer.getOriginalAST();
        
        /**
         * The summary stats based on the globally scoped bindings are important
         * input to the join optimizer etc. Note that the 
         * ASTStaticBindingsOptimzer might modify/extend this set and update the
         * solution set stats.
         */
        ctx.setSolutionSetStats(SolutionSetStatserator.get(globallyScopedBindings));

        final StaticAnalysisStats stats = new StaticAnalysisStats();
        // register parser call if parse information is available
		if (astContainer != null) {
			stats.registerParserCall(astContainer);
        }
        ctx.setStaticAnalysisStats(stats);
        
        /**
         * By definition of the API, the mappings passed in from Sesame
         * (i.e., the bindingSets input to this method) is having the special
         * semantics of "globally scoped vars". We need to record and treat
         * them separately in some places. See also the discussion at
         * https://groups.google.com/forum/#!topic/sesame-devel/Di_ZLtTVuZA.
         * 
         * Also note that, unless the solution set stats (which might be
         * adjusted by optimizers as the input binding set is modified),
         * the globally scoped vars set is not intended to be modified.
         */
        ctx.setGloballyScopedVariables(ctx.getSolutionSetStats().getAlwaysBound());
        
        // Run the AST query rewrites / query optimizers.
        final QueryNodeWithBindingSet optRes = 
           ctx.optimizers.optimize(ctx, 
                 new QueryNodeWithBindingSet(originalQuery, globallyScopedBindings));
        
        // Set the optimized AST model on the container.
        final QueryRoot optimizedQuery = (QueryRoot)optRes.getQueryNode();
        astContainer.setOptimizedAST(optimizedQuery);
        
        // Final static analysis object for the optimized query.
        ctx.sa = new StaticAnalysis(optimizedQuery, ctx);

        // The set of known materialized variables.
        final LinkedHashSet<IVariable<?>> doneSet = new LinkedHashSet<IVariable<?>>();

        // true IFF the query plan should handle materialize the projection.
        final boolean materializeProjection = ctx.materializeProjectionInQuery
                && !optimizedQuery.hasSlice();

        // The executable query plan.
        PipelineOp left = convertQueryBaseWithScopedVars(null/* left */,
                optimizedQuery, doneSet, materializeProjection, ctx);

        if (left == null) {

            /*
             * An empty query plan. Just copy anything from the input to the
             * output.
             */

            left = addStartOp(optimizedQuery, ctx);

        }
        
        /*
         * Set the queryId on the top-level of the query plan.
         * 
         * Note: The queryId is necessary to coordinate access to named subquery
         * result sets when they are consumed within a subquery rather than the
         * main query.
         */

        left = (PipelineOp) left.setProperty(
                QueryEngine.Annotations.QUERY_ID, ctx.queryId);

        if (!ctx.isCluster()) {

            /*
             * For standalone, allow a query hint to override the chunk handler.
             * 
             * Note: scale-out is using a different chunk handler, so we would
             * not want to override it here (in fact, the FederatedRunningQuery
             * does not permit an override in its getChunkHandler() method).
             * 
             * @see BLZG-533 (Vector query engine on native heap)
             */
            left = (PipelineOp) left.setProperty(
                    QueryEngine.Annotations.CHUNK_HANDLER,
                    ctx.queryEngineChunkHandler
                    );

        }

        // Attach the query plan to the ASTContainer.
        astContainer.setQueryPlan(left);
        
        // Set the optimized binding set on the container.
        astContainer.setOptimizedASTBindingSets(optRes.getBindingSets());

        if (log.isInfoEnabled()) {
            log.info(astContainer);
        }

        return left;

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
                false /* materializeProjection */, ctx);

        // Retain only those variables projected by the subquery.
        tmp.retainAll(projectedVarList);

        // Add any variables known to be materialized into this scope.
        doneSet.addAll(tmp);

        if( left != null) {
        	left = (PipelineOp) left.setProperty(BOp.Annotations.NAMESPACE, ctx.getNamespace());
        }

        return left;

    }

    /**
     * Core method to convert a {@link QueryBase}.
     * <p>
     * Note: This assumes that the caller has correctly scoped <i>doneSet</i>
     * with respect to the projection of a subquery.
     * 
     * @param left
     * @param queryBase
     *            The query.
     * @param doneSet
     *            The set of variables which are already known to be
     *            materialized.
     * @param materializeProjection
     *            When <code>true</code>, the projection of the
     *            {@link QueryBase} solutions will be materialized.
     * @param ctx
     * 
     * @return The query plan.
     */
    private static PipelineOp convertQueryBaseWithScopedVars(PipelineOp left,
            final QueryBase queryBase, final Set<IVariable<?>> doneSet,
            final boolean materializeProjection, final AST2BOpContext ctx) {
        
        final GraphPatternGroup<?> root = queryBase.getWhereClause();

        if (root == null)
            throw new IllegalArgumentException("No group node");

        if (left == null) {

            left = addStartOpOnCluster(queryBase, ctx);
            
        }
        
        
        // add bindings clause object
        final Object bindingsClause = 
              queryBase.annotations().get(QueryBase.Annotations.BINDINGS_CLAUSE);
        if (bindingsClause!=null && bindingsClause instanceof BindingsClause) {
           left = addValues(left,
                 (BindingsClause)bindingsClause, doneSet, ctx);
        }
        

        /*
         * Named subqueries.
         */
        if (queryBase instanceof QueryRoot) {

            final NamedSubqueriesNode namedSubqueries = ((QueryRoot) queryBase)
                    .getNamedSubqueries();

            if (namedSubqueries != null && !namedSubqueries.isEmpty()) {

                // WITH ... AS [name] ... INCLUDE style subquery declarations.
                left = addNamedSubqueries(left, namedSubqueries,
                        (QueryRoot) queryBase, doneSet, ctx);

            }

        }

        // The top-level "WHERE" clause.
        left = convertJoinGroupOrUnion(left, root, doneSet, ctx);

        final OrderByNode orderBy = queryBase.getOrderBy();

        final ProjectionNode projection = queryBase.getProjection() == null ? null
                : queryBase.getProjection().isEmpty() ? null : queryBase
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

            final GroupByNode groupBy = queryBase.getGroupBy() == null ? null
                    : queryBase.getGroupBy().isEmpty() ? null : queryBase.getGroupBy();

            final HavingNode having = queryBase.getHaving() == null ? null : queryBase
                    .getHaving().isEmpty() ? null : queryBase.getHaving();

            // true if this is an aggregation query.
            final boolean isAggregate = StaticAnalysis.isAggregate(projection,
                    groupBy, having);

            if (isAggregate) {

                final Set<IVariable<IV>> vars = new HashSet<IVariable<IV>>();
                
                StaticAnalysis.gatherVarsToMaterialize(having, vars, true /* includeAnnotations */);
                vars.removeAll(doneSet);
                if (!vars.isEmpty()) {
                    left = addChunkedMaterializationStep(
                        left, vars, ChunkedMaterializationOp.Annotations.DEFAULT_MATERIALIZE_INLINE_IVS, 
                        null /* cutOffLimit */, having.getQueryHints(), ctx);
                }
                
                left = addAggregation(left, projection, groupBy, having, ctx);

            } else {

                for (AssignmentNode assignmentNode : projection
                        .getAssignmentProjections()) {

                    left = addAssignment(left, assignmentNode, doneSet,
                            projection.getQueryHints(), ctx, true/* projection */);

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

            // When true, DISTINCT must preserve ORDER BY ordering.
            final boolean preserveOrder;

            if (orderBy != null && !orderBy.isEmpty()) {

                /*
                 * Note: ORDER BY before DISTINCT, so DISTINCT must preserve
                 * order.
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/563
                 * (ORDER BY + DISTINCT)
                 */
                
                preserveOrder = true;

                left = addOrderBy(left, queryBase, orderBy, ctx);

            } else {
                
                preserveOrder = false;
                
            }

            if (projection.isDistinct() || projection.isReduced()) {

                left = addDistinct(left, queryBase, preserveOrder, ctx);

            }

        } else {

            /*
             * TODO Under what circumstances can the projection be [null]?
             */
            
            if (orderBy != null && !orderBy.isEmpty()) {

                left = addOrderBy(left, queryBase, orderBy, ctx);

            }

        }

        if (projection != null) {

        	/**
			 * The projection after the ORDER BY needs to preserve the ordering.
			 * So does the chunked materialization operator. The code above
			 * handles this for ORDER_BY + DISTINCT, but does not go far enough
			 * to impose order preserving evaluation on the PROJECTION and
			 * chunked materialization, both of which are downstream from the
			 * ORDER_BY operator.
			 * 
			 * @see #1044 (PROJECTION after ORDER BY does not preserve order)
			 */
        	final boolean preserveOrder = orderBy != null;
        	
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

			{
				// The variables projected by the subquery.
				final IVariable<?>[] projectedVars = projection.getProjectionVars();

//				/**
//				 * BLZG-1958: we only need a projection op if the set of projected vars
//				 * differs from the variables bound inside the query.
//               *				
//               * NOTE: The following code sets the top-level projection conditionally, only if it
//               * is needed. However, there are some problems related to temporary and anonymous
//               * variables being included where they should not. See BLZG-1958. This is something
//               * we may need to re-enable once the optimization proposed in BLZG-1901 is in place;
//               * the code is currently commented out.				 
//				 */
//				final Set<IVariable<?>> maybeBoundVars = 
//				    ctx.sa.getSpannedVariables(root, new HashSet<IVariable<?>>());
//				
//				final Set<String> varNamesProjected = new LinkedHashSet<String>();
//				for (final IVariable<?> projectedVar : projectedVars) {
//				    varNamesProjected.add(projectedVar.getName());
//				}
//				
//				final Set<String> varNamesMaybeBound = new LinkedHashSet<String>();
//				for (final IVariable<?> maybeBoundVar : maybeBoundVars) {
//				    varNamesMaybeBound.add(maybeBoundVar.getName());
//				}
//
//				// if the set of projected vars is a superset of those possibly bound
//				// (e.g. projected: ?s ?p ?o, possibly bound ?s ?p) we can safely skip
//				// the final projection, as it won't change the query result
//				if (!varNamesProjected.containsAll(varNamesMaybeBound)) {
				
				final List<NV> anns = new LinkedList<NV>();
				anns.add(new NV(BOp.Annotations.BOP_ID, ctx.nextId()));
				anns.add(new NV(BOp.Annotations.EVALUATION_CONTEXT, BOpEvaluationContext.CONTROLLER));
				anns.add(new NV(PipelineOp.Annotations.SHARED_STATE, true));// live stats
				anns.add(new NV(ProjectionOp.Annotations.SELECT, projectedVars));
				if (preserveOrder) {
					/**
					 * @see #563 (ORDER BY + DISTINCT)
					 * @see #1044 (PROJECTION after ORDER BY does not preserve
					 *      order)
					 */
					anns.add(new NV(PipelineOp.Annotations.MAX_PARALLEL, 1));
					anns.add(new NV(SliceOp.Annotations.REORDER_SOLUTIONS, false));
				}
				left = applyQueryHints(new ProjectionOp(leftOrEmpty(left),//
						anns.toArray(new NV[anns.size()])//
						), queryBase, ctx);
//				}
			}
            
            if (materializeProjection) {
                
                /*
                 * Note: Materialization done from within the query plan needs
                 * to occur before the SLICE operator since the SLICE will
                 * interrupt the query evaluation when it is satisfied, which
                 * means that downstream operators will be canceled. Therefore a
                 * materialization operator can not appear after a SLICE.
                 * However, doing materialization within the query plan is not
                 * efficient when the query uses a SLICE since we will
                 * materialize too many solutions. This is true whether the
                 * OFFSET and/or the LIMIT was specified. Therefore, as an
                 * optimization, the caller should take responsible for
                 * materialization when the top-level query uses a SLICE.
                 */
                
                final Set<IVariable<?>> tmp = projection
                        .getProjectionVars(new LinkedHashSet<IVariable<?>>());
                
                // do not materialize anything which was already materialized.
                tmp.removeAll(doneSet);
                
                if (!tmp.isEmpty()) {

                    final long timestamp = ctx.getLexiconReadTimestamp();

                    final String ns = ctx.getLexiconNamespace();

                    final IVariable<?>[] vars = tmp.toArray(new IVariable[tmp
                            .size()]);
                    
    				final List<NV> anns = new LinkedList<NV>();
    				anns.add(new NV(ChunkedMaterializationOp.Annotations.VARS, vars));
    				anns.add(new NV(ChunkedMaterializationOp.Annotations.RELATION_NAME, new String[] { ns })); //
    				anns.add(new NV(ChunkedMaterializationOp.Annotations.TIMESTAMP, timestamp)); //
    				anns.add(new NV(ChunkedMaterializationOp.Annotations.MATERIALIZE_INLINE_IVS, true));
    				anns.add(new NV(PipelineOp.Annotations.SHARED_STATE, !ctx.isCluster()));// live stats, but not on the cluster.
    				anns.add(new NV(BOp.Annotations.BOP_ID, ctx.nextId()));
    				if (preserveOrder) {
    					/**
    					 * @see #563 (ORDER BY + DISTINCT)
    					 * @see #1044 (PROJECTION after ORDER BY does not preserve
    					 *      order)
    					 */
    					anns.add(new NV(PipelineOp.Annotations.MAX_PARALLEL, 1));
    					anns.add(new NV(SliceOp.Annotations.REORDER_SOLUTIONS, false));
    				}
    				left = applyQueryHints(new ChunkedMaterializationOp(leftOrEmpty(left),//
    						anns.toArray(new NV[anns.size()])//
    						), queryBase, ctx);

//                    left = applyQueryHints(new ChunkedMaterializationOp(leftOrEmpty(left),
//                            new NV(ChunkedMaterializationOp.Annotations.VARS, vars),//
//                            new NV(ChunkedMaterializationOp.Annotations.RELATION_NAME, new String[] { ns }), //
//                            new NV(ChunkedMaterializationOp.Annotations.TIMESTAMP, timestamp), //
//                            new NV(ChunkedMaterializationOp.Annotations.MATERIALIZE_INLINE_IVS, true), //
//                            new NV(PipelineOp.Annotations.SHARED_STATE, !ctx.isCluster()),// live stats, but not on the cluster.
//                            new NV(BOp.Annotations.BOP_ID, ctx.nextId())//
//                            ), queryBase, ctx);
//                    left = (PipelineOp) new ChunkedMaterializationOp(
//                            leftOrEmpty(left), vars, ns, timestamp)
//                            .setProperty(BOp.Annotations.BOP_ID, ctx.nextId());

                    // Add to the set of known materialized variables.
                    doneSet.addAll(tmp);
                    
                }
                
            }

        }
        
        if(queryBase.hasSlice()) {

            left = addSlice(left, queryBase, queryBase.getSlice(), ctx);

        }

        left = addEndOp(left, ctx);

        /*
         * Set a timeout on a query or subquery.
         */
        if (left!=null) {

            final long timeout = queryBase.getTimeout();

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
            log.info("\nqueryOrSubquery:\n" + queryBase + "\nplan:\n"
                    + BOpUtility.toString(left));
        
        if (left != null) {
        	left = (PipelineOp) left.setProperty(BOp.Annotations.NAMESPACE, ctx.getNamespace());
        }

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
             * test something else, it happens to trigger this bug as well. This
             * could be fixed by a count down latch operator which also gets the
             * original inputs, waits until N named subqueries have completed,
             * and then sends the original inputs to the next operator in the
             * pipeline. This is the signal that the main query (or the next
             * named subquery) can now execute.
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

        final IVariable<?>[] subqueryProjVars = 
           subqueryRoot!=null && subqueryRoot.getProjection()!=null && 
           subqueryRoot.getProjection().getProjectionVars()!=null ?
           subqueryRoot.getProjection().getProjectionVars() :  new IVariable[] {};
       
        final PipelineOp subqueryBase = 
           addDistinctProjectionOp(null, ctx, subqueryRoot, subqueryProjVars);

        PipelineOp subqueryPlan = 
           convertQueryBase(subqueryBase,subqueryRoot, doneSet, ctx);
        // inherit the namespace property (which is needed for the CPU/GPU),
        // see https://github.com/SYSTAP/bigdata-gpu/issues/343
        subqueryPlan = (PipelineOp) subqueryPlan.setProperty(BOp.Annotations.NAMESPACE, ctx.getNamespace());

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

        final INamedSolutionSetRef namedSolutionSet = NamedSolutionSetRefUtility
                .newInstance(ctx.queryId, subqueryRoot.getName(), joinVars);

        if(ctx.nativeHashJoins) {
            left = applyQueryHints(new HTreeNamedSubqueryOp(leftOrEmpty(left), //
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.PIPELINED, false),// at-once evaluation
                new NV(PipelineOp.Annotations.MAX_MEMORY, Long.MAX_VALUE),// at-once evaluation
                new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                new NV(HTreeNamedSubqueryOp.Annotations.RELATION_NAME, new String[]{ctx.getLexiconNamespace()}),//
                new NV(HTreeNamedSubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                new NV(HTreeNamedSubqueryOp.Annotations.JOIN_VARS, joinVars),//
                new NV(NamedSetAnnotations.NAMED_SET_REF,
                                namedSolutionSet)//
                ), subqueryRoot, ctx);
        } else {
            left = applyQueryHints(new JVMNamedSubqueryOp(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.PIPELINED, false),// at-once evaluation
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                    new NV(HTreeNamedSubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                    new NV(HTreeNamedSubqueryOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(NamedSetAnnotations.NAMED_SET_REF,
                                    namedSolutionSet)//
                    ), subqueryRoot, ctx);
        }

        return left;

    }

    /**
     * Add an operator to evaluate a {@link ServiceCall}. This handles both
     * services which are evaluated by direct method call within the same JVM
     * and SPARQL 1.1 Federated Query.
     * 
     * @param left
     * @param serviceNode
     * @param ctx
     * @return
     */
    static private PipelineOp addServiceCall(PipelineOp left,
            final ServiceNode serviceNode, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        // The query hints are taken from the SERVICE node.
        final Properties queryHints = serviceNode.getQueryHints();
        
        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(serviceNode), needsMaterialization);

        /*
         * Note: If the serviceRef is a Variable then we MUST add the
         * materialization step since we can not know in advance whether any
         * given binding for the serviceRef will be a REMOTE SERVICE or an
         * internal bigdata aware service.
         */
        final IVariableOrConstant<?> serviceRef = serviceNode.getServiceRef()
                .getValueExpression();

        // true if we need to materialize variables before running the SERVICE.
        final boolean isMaterialize;
        // variables that are generated inside the service and may carry mock IVs
        final Set<IVariable<IV>> varsToMockResolve = new HashSet<IVariable<IV>>();
        if(serviceRef instanceof IConstant) {

            final BigdataURI serviceURI = ServiceCallUtility
                    .getConstantServiceURI(serviceRef);
            
            final ServiceCall<?> serviceCall = ServiceRegistry.getInstance()
                    .toServiceCall(ctx.db,
                            ctx.queryEngine.getClientConnectionManager(),
                            serviceURI, serviceNode, null /* BOpStats not yet available */);

            /*
             * true IFF this is a registered bigdata aware service running in
             * the same JVM.
             */
            final boolean isBigdata = serviceCall.getServiceOptions()
                    .isBigdataNativeService();

            /*
             * In case we are dealing with a run last service (either implicit
             * by the type of service or explicitly enforced by a query hint),
             * we may need to use variable bindings inside, so materialization
             * might be required here as well.
             */
            isMaterialize = !isBigdata ||
                !serviceCall.getServiceOptions().isRunFirst() ||
                !serviceNode.getProperty(QueryHints.RUN_FIRST, false);
            
            /*
             * For service calls returning mocked IVs, we need to resolve
             * the mocked IVs in order to make them usable in subsequent joins.
             */
            if (serviceCall instanceof MockIVReturningServiceCall) {
               MockIVReturningServiceCall esc = (MockIVReturningServiceCall)serviceCall;
               varsToMockResolve.addAll(esc.getMockVariables());
            }

        } else {

            /*
             * Since the service reference can not be evaluated to a constant we
             * have to assume that we need to do value materialization before
             * the service call.
             * 
             * Note: If the service call winds up being a native bigdata
             * service, then this will do more work. But, since the service
             * reference does not evaluate to a URI constant we are not able to
             * figure that out in advance.
             */
            
            isMaterialize = true;
            
        }
        
        /*
         * SERVICEs do not have explicit "projections" (there is no capacity to
         * rename variables) but they still must hide variables which are not in
         * scope. Also, if we have to materialize RDF Values for the SERVICE,
         * then we have to ensure that anything gets materialized which is (a)
         * used by the SERVICE; and (b) MIGHT be incoming bound to the SERVICE.
         * 
         * Note: The materialization requirement is only on entry to the
         * SERVICE. Solutions which flow out of the SERVICE will already be
         * materialized (though they might use mock IVs) unless the service is
         * bigdata aware (in which case it is responsible for getting the IVs
         * right).
         */
        
        // Anything which can flow out of the SERVICE is "projected".
        final Set<IVariable<?>> projectedVars = ctx.sa
                .getMaybeProducedBindings(serviceNode);
        {

            /*
             * FIXME Prune any variables which are not used outside of the
             * SERVICE's graph pattern by removing them from its "projection".
             * To do this we need to check any downstream siblings of the
             * SERVICE node (in its parent join group), and any downstream
             * siblings of parents of that join group. This proceeds recursively
             * until we reach the top level join group for the WHERE clause in
             * question. If we pass through a Sub-Select, then we can stop if
             * the variable(s) in question are not projected into that
             * Sub-Select.
             * 
             * The same logic could be used to prune out variables which are not
             * required by downstream joins.
             * 
             * There is some logic similar to this in ASTBottomUpOptimizer. Look
             * for callers of StaticAnalysis.findParent().
             * 
             * There is some logic similar to this in
             * ASTComplexOptionalOptimizer. However, I believe that it fails to
             * recursively examine the downstream siblings of the parent
             * group(s).
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/368 (Prune
             * variables during query evaluation)
             */
            
            /*
             * FIXME This is a cheap hack which fixes the problem where a blank
             * node is used inside of a SERVICE graph.  However, this hack is
             * WRONG if the blank node is correlated within the query outside
             * of that SERVICE graph.
             * 
             * @see http://sourceforge.net/apps/trac/bigdata/ticket/510 (Blank
             * nodes in SERVICE graph patterns)
             */
            final Iterator<IVariable<?>> itr = projectedVars.iterator();

            while (itr.hasNext()) {

                final IVariable<?> v = itr.next();

                if (v.getName().startsWith("-anon-")) {

                    itr.remove();

                }

            }

        }

        // Set on the ServiceNode
        serviceNode.setProjectedVars(projectedVars);
        
        final int rightId = ctx.nextId();
        
        if (isMaterialize) {

            /*
             * Setup materialization steps for any projected variables which
             * MIGHT be incoming bound into the SERVICE call.
             */

            // Anything which MIGHT be bound by the time the SERVICE runs.
            final Set<IVariable<?>> maybeBound = ctx.sa
                    .getMaybeIncomingBindings(serviceNode,
                            new LinkedHashSet<IVariable<?>>());
            
            final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
            vars.addAll(projectedVars); // start with everything "projected".
            vars.retainAll(maybeBound); // retain "maybe" incoming bound vars.
            if(serviceRef instanceof IVariable) {
                /*
                 * If the serviceRef is a bare variable, then that variable
                 * needs to be materialized.
                 */
                vars.add((IVariable<?>) serviceRef);
            }
            vars.removeAll(doneSet); // remove already materialized vars.

            if (!vars.isEmpty()) {

                // Add the materialization step.
                left = addMaterializationSteps2(left, rightId,
                        (Set<IVariable<IV>>) (Set) vars, queryHints, ctx);

                // These variables have now been materialized.
                doneSet.addAll(vars);
                
            }
            
        }

        /*
         * Identify the join variables for the SERVICE solution set. This is
         * based on the variables which are definitely bound on entry and the
         * variables which are definitely bound on exit from the SERVICE. It
         * is Ok if there are no join variables, but it means that we will do
         * a full cross product of the solutions vectored into a SERVICE call
         * and the solutions flowing out of that SERVICE call.
         */
        final Set<IVariable<?>> joinVarSet = ctx.sa.getJoinVars(
                serviceNode, new LinkedHashSet<IVariable<?>>());
        
        /*
         * Note: For overall sanity, the ServiceCallJoin is being run on the
         * query controller. This will prevent HTTP connections from being
         * originated on the data services in a cluster.
         * 
         * Note: In order to have better throughput, this operator is annotated
         * as "at-once" (PIPELINED:=false) by default (it can be overridden by
         * QueryHints#AT_ONCE). This forces the query engine to wait until all
         * source solutions are available and then run the ServiceCallJoin
         * exactly once. The ServiceCallJoin will internally vector the
         * solutions per target service and will use limited parallelism (based
         * on MAX_PARALLEL) to reduce the latency of the service requests across
         * multiple service targets.
         * 
         * TODO Unit test where join constraints wind up attached to the
         * ServiceCallJoin operator.
         */
        final Map<String, Object> anns = new LinkedHashMap<String, Object>();
        anns.put(BOp.Annotations.BOP_ID, rightId);
        anns.put(BOp.Annotations.EVALUATION_CONTEXT,
                BOpEvaluationContext.CONTROLLER);
        anns.put(PipelineOp.Annotations.PIPELINED, false); // at-once by default.
//      anns.put(PipelineOp.Annotations.MAX_PARALLEL, 1);
        anns.put(PipelineOp.Annotations.SHARED_STATE, true);// live stats.
        anns.put(ServiceCallJoin.Annotations.SERVICE_NODE, serviceNode);
        anns.put(ServiceCallJoin.Annotations.NAMESPACE, ctx.getNamespace());
        anns.put(ServiceCallJoin.Annotations.TIMESTAMP, ctx.getTimestamp());
        anns.put(ServiceCallJoin.Annotations.JOIN_VARS,
                joinVarSet.toArray(new IVariable[] {}));
        anns.put(JoinAnnotations.CONSTRAINTS, joinConstraints);

        left = applyQueryHints(new ServiceCallJoin(leftOrEmpty(left), anns),
                queryHints, ctx);

        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps3(left, doneSet, needsMaterialization,
                queryHints, ctx);

        /**
         * This is a special handling for external services, which might create
         * values that are reused/joined with IVs. We need to properly resolve
         * them in order to make subsequent joins successful.
         */
        if (!varsToMockResolve.isEmpty()) {
           
           left = addMockTermResolverOp(
                 left,//
                 varsToMockResolve,//
                 ChunkedMaterializationOp.Annotations.DEFAULT_MATERIALIZE_INLINE_IVS,
                 null,//
                 queryHints,//
                 ctx//
                 ); 
        }

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
	 * 
	 * @see ASTNamedSubqueryOptimizer
	 * 
	 *      TODO If the named solution set has variables which are not
	 *      materialized but those variables are visible in this query, then we
	 *      can no longer conclude that they are materialized. We MUST either
	 *      (A) remove them from the doneSet; (B) we need to materialize them
	 *      when reading from the named solution set (or after the join) such
	 *      that we have a consistent doneSet (only variables which are
	 *      materialized in all solutions) after the INCLUDE join; (C) remove
	 *      them from the solutions (if they are no longer required after the
	 *      join).
	 * 
	 *      TODO If we have mock IVs in solutions for a join variable then we
	 *      need to materialize that join variable in all source solutions
	 *      flowing into the join. This is <a
	 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/490"> MockIVs
	 *      in hash joins</a>. The {@link ISolutionSetStats} should track this
	 *      information. Static analysis should also flag this information based
	 *      on inspection (if the variable is bound to a computed expression and
	 *      that expression is not known to be a fully inline data type, then we
	 *      could have mock IVs for the variable binding and we must resolve
	 *      those IVs against the database). (There could be an exception when
	 *      the variable is known to be based on a computed expression in both
	 *      the left and right hand solutions. That suggests that we *might*
	 *      also be able to handle this by converting the variable binding into
	 *      MockIVs for all solutions, but it seems likely that this would
	 *      produce some unpleasant surprises - for example, the hash join would
	 *      have the same hash code for all values for that variable since the
	 *      termId would always be zero).
	 */
    private static PipelineOp addNamedSubqueryInclude(PipelineOp left,
            final NamedSubqueryInclude nsi,
            final Set<IVariable<?>> doneSet, final AST2BOpContext ctx) {

    		final String name = nsi.getName();
    	
        if (log.isInfoEnabled())
            log.info("include: solutionSet=" + name);

        // The join variables.
        final IVariable<?>[] joinVars;

        /*
		 * When true, the named solution set will be released after the INCLUDE.
		 * 
		 * Note: We MUST NOT [release] a named solution set whose scope is
		 * outside of the query (durable or cached).
		 */
        boolean release;
        
		{
        	
            // Resolve the NamedSubqueryRoot for this INCLUDE.
            final NamedSubqueryRoot nsr = ctx.sa
                    .getNamedSubqueryRoot(name);

            if(nsr != null) {

                /*
                 * We will do a hash join against the named solution set
                 * generated by that named subquery root.
                 * 
                 * TODO We run into problems here where the join variables were
                 * not predicted correctly, especially when there are multiple
                 * INCLUDEs for the same solution set and when the INCLUDE is
                 * the first operator in the query plan. We should built an
                 * appropriate index for each such INCLUDE, either up front or
                 * immediately before we evaluate the hash join against the
                 * named solution set.
                 */

                @SuppressWarnings("unchecked")
                final Set<IVariable<?>> nsrDoneSet = (Set<IVariable<?>>) nsr
                        .getProperty(NamedSubqueryRoot.Annotations.DONE_SET);

				if (nsrDoneSet == null) {

					// Make sure the doneSet was attached to the named subquery.
					throw new AssertionError(
							"NamedSubqueryRoot doneSet not found: " + name);

				}

				/*
				 * Get the known materialized variables from the named subquery
				 * and combine them with those which are known to be
				 * materialized in the parent to get the full set of variables
				 * known to be materialized once we join in the named subquery
				 * solution set.
				 * 
				 * TODO Should add all variables not in scope in the parent
				 * which are materialized in the named solution set and retain
				 * only the variables which are in scope in the parent which are
				 * materialized in the named solution set. Or we should
				 * materialize things which were materialized in the pipeline
				 * solutions and no longer possess that property because they
				 * were not materialized in the named solution set (especially
				 * if they will need to be materialized later in the query; if
				 * they will not need to be materialized later in the query then
				 * we should just drop them from the doneSet since they are only
				 * partly materialized).
				 */
				
				doneSet.addAll(nsrDoneSet);

		        final VarNode[] joinvars = nsi.getJoinVars();

		        if (joinvars == null) {

		            /*
					 * The most likely explanation is not running the
					 * ASTNamedSubqueryOptimizer.
					 */

		            throw new AssertionError();

		        }

				// Convert to IVariable[].
		        joinVars = ASTUtil.convert(joinvars);

				/*
				 * TODO This should be set based on the #of INCLUDEs for the
				 * named subquery. We can not release the associated hash index
				 * until all includes are done.
				 */

		        	release = false;
				
			} else {

				/**
				 * Attempt to resolve a pre-existing named solution set.
				 * 
				 * If we find the named solution set, then we will handle it in
				 * one of two ways.
				 * 
				 * 1) If the cardinality of solutions flowing into this
				 * operation on the pipeline is known to be small, then we will use
				 * a SCAN of the named solution set (it does not have an index,
				 * at least, not at this time) and use a nested inner loop to
				 * join against the left solutions. This provides a critical
				 * optimization for the case where the INCLUDE appears at the
				 * head of the WHERE clause. It also provides the guarantee that
				 * the ORDER of the solutions in the named solution set is
				 * preserved, which we depend on when SLICING a pre-existing
				 * solution set.
				 * 
				 * 2) Otherwise, we build a hash index over the named solution
				 * set. The join variables for that hash index will be the
				 * intersection of what is known bound on entry to the INCLUDE
				 * operator and what is known bound in the named solution set
				 * itself. We will then do a hash join against the generated
				 * hash index.
				 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/531"
                 *      > SPARQL UPDATE for NAMED SOLUTION SETS </a>
				 */

                final ISolutionSetStats stats = ctx.sa
                        .getSolutionSetStats(name);

                /*
                 * Note: This is all variables whose IVCache association is
                 * always present when that variable is bound in a solution
                 * (that is, all variables which we do not need to materialize
                 * for the solution set).
                 * 
                 * TODO Should add all variables not in scope in the parent
                 * which are materialized in the named solution set and retain
                 * only the variables which are in scope in the parent which are
                 * materialized in the named solution set. Or we should
                 * materialize things which were materialized in the pipeline
                 * solutions and no longer possess that property because they
                 * were not materialized in the named solution set (especially
                 * if they will need to be materialized later in the query; if
                 * they will not need to be materialized later in the query then
                 * we should just drop them from the doneSet since they are only
                 * partly materialized).
                 */

                doneSet.addAll(stats.getMaterialized());

                if (isNamedSolutionSetScan(ctx, nsi)) {

                    /*
                     * Optimize with SCAN and nested loop join.
                     * 
                     * Note: This optimization also preserves the ORDER in the
                     * named solution set.
                     */

                    return convertNamedSolutionSetScan(left, nsi, doneSet, ctx);

                }

                /*
                 * Find the join variables. This is the set of variables which
                 * are both definitely bound on entry to the INCLUDE and which
                 * are known to be bound by all solutions in the named solution
                 * set.
                 * 
                 * TODO If we provide CREATE SOLUTIONS semantics to specify the
                 * type of the data structure in which the named solution set is
                 * stored *AND* it is a hash index (or a BTree with a primary
                 * key which is either to our join variables or a sub-set of
                 * those join variables) then we can use that index without
                 * building a hash index on the desired join variables.
                 */
                final Set<IVariable<?>> joinvars = ctx.sa.getJoinVars(nsi,
                        name/* solutionSet */,
                        new LinkedHashSet<IVariable<?>>());

                // flatten into IVariable[].
                joinVars = joinvars.toArray(new IVariable[] {});

                /*
                 * Pass all variable bindings along.
                 * 
                 * Note: If we restrict the [select] annotation to only those
                 * variables projected by the subquery, then we will wind up
                 * pruning any variables used in the join group which are NOT
                 * projected into the subquery. [We are not building the index
                 * from the solutions flowing through the pipeline. Is this
                 * comment true for an index build from a source other than the
                 * pipeline?]
                 */
                @SuppressWarnings("rawtypes")
                final IVariable[] selectVars = null;

                /*
                 * The identifier for the source solution set.
                 * 
                 * Note: This is a pre-existing solution set. It is located
                 * using the KB view (namespace and timestamp).
                 */
                final INamedSolutionSetRef sourceSet = NamedSolutionSetRefUtility
                        .newInstance(ctx.getNamespace(), ctx.getTimestamp(),
                                name, IVariable.EMPTY/* joinVars */);

                /*
                 * The identifier for the generated index.
                 * 
                 * Note: This is a dynamically generated index scoped to the
                 * query. it is located using the queryId.
                 */
                final INamedSolutionSetRef generatedSet = NamedSolutionSetRefUtility
                        .newInstance(ctx.queryId, name, joinVars);

                final JoinTypeEnum joinType = JoinTypeEnum.Normal;

                final IHashJoinUtilityFactory joinUtilFactory;
                if (ctx.nativeHashJoins) {
                    joinUtilFactory = HTreeHashJoinUtility.factory;
                } else {
                    joinUtilFactory = JVMHashJoinUtility.factory;
                }

                left = applyQueryHints(new HashIndexOp(
                            leftOrEmpty(left),//
                            new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                            new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.CONTROLLER),//
                            new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
//                            new NV(PipelineOp.Annotations.CHUNK_CAPACITY, 1),// TODO Why ONE (1)?  Is this because there is a single source solution?  Is that true? Why not use the default?
                            new NV(PipelineOp.Annotations.LAST_PASS, true),// required
                            new NV(PipelineOp.Annotations.SHARED_STATE, true),// live
                                                                              // stats.
                            new NV(HashIndexOp.Annotations.JOIN_TYPE, joinType),//
                            new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                            new NV(HashIndexOp.Annotations.SELECT, selectVars),//
                            new NV(HashIndexOp.Annotations.HASH_JOIN_UTILITY_FACTORY,
                                    joinUtilFactory),//
                            new NV(HashIndexOp.Annotations.NAMED_SET_SOURCE_REF,
                                    sourceSet),//
                            new NV(HashIndexOp.Annotations.NAMED_SET_REF,
                                    generatedSet),//
                            new NV(IPredicate.Annotations.RELATION_NAME, 
                                    new String[]{ctx.getLexiconNamespace()})
                        ), nsi, ctx);

                // true since we are building the hash index.
                release = true;

            }
            
        }

//      if (joinVars.length == 0) {
//
//          /*
//           * Note: If there are no join variables then the join will examine
//           * the full N x M cross product of solutions. That is very
//           * inefficient, so we are logging a warning.
//           */
//
//          log.warn("No join variables: " 
//                  + subqueryInclude.getName()
//                  + ", subquery="
//                  + subqueryInclude.getNamedSubqueryRoot(ctx.sa
//                          .getQueryRoot())
//                          );
//
//      }
        
		/*
		 * Common code path.
		 * 
		 * At this point, we have either have a hash index which was generated
		 * by a named subquery or we have build a hash index from a pre-existing
		 * named solution set. Now we can do the hash join.
		 */
        final INamedSolutionSetRef namedSolutionSetRef = NamedSolutionSetRefUtility
                .newInstance(ctx.queryId, name, joinVars);

		@SuppressWarnings("rawtypes")
		final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(nsi), needsMaterialization);

		/*
		 * TODO In order to release the named solution set, we need to specify
		 * [lastPass=true] and [maxParallel=1].
		 */
        release = false;
        
        if (ctx.nativeHashJoins) {
            
            left = applyQueryHints(new HTreeSolutionSetHashJoinOp(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF,
                            namedSolutionSetRef),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS,
//                            joinVars),//
					new NV(HTreeSolutionSetHashJoinOp.Annotations.CONSTRAINTS,
							joinConstraints),//
					new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE,
							release)//
//					new NV(HTreeSolutionSetHashJoinOp.Annotations.LAST_PASS,
//							isLastPassRequested)//
                ), nsi, ctx);

        } else {
            
            left = applyQueryHints(new JVMSolutionSetHashJoinOp(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                    new NV(JVMSolutionSetHashJoinOp.Annotations.NAMED_SET_REF,
                            namedSolutionSetRef),//
//                    new NV(JVMSolutionSetHashJoinOp.Annotations.JOIN_VARS,
//                            joinVars),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.CONSTRAINTS,
                            joinConstraints),//
                    new NV(JVMSolutionSetHashJoinOp.Annotations.RELEASE,
                            release)//
//    					new NV(JVMSolutionSetHashJoinOp.Annotations.LAST_PASS,
//    							isLastPassRequested)//
                ), nsi, ctx);
            
        }
        
        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps3(left, doneSet, needsMaterialization,
                nsi.getQueryHints(), ctx);

        return left;

    }

    /**
     * This handles a VALUES clause. It grabs the binding sets from the
     * BindingsClause, attach them to the query as a named subquery with a hash
     * index, and then add a named subquery include to the pipeline right here.
     * <p>
     * The VALUES are interpreted using a solution set hash join. The "plan" for
     * the hash join of the VALUES with the solutions flowing through the
     * pipeline is: (a) we take the IBindingSet[] and use a {@link HashIndexOp}
     * to generate the hash index; and (b) we use a
     * {@link SolutionSetHashJoinOp} to join the solutions from the pipeline
     * with those in the hash index. Both JVM and HTree versions of this plan
     * are supported.
     * <p>
     * 1. {@link HashIndexOp} (JVM or HTree): Specify the IBindingSet[] as the
     * source. When the HashIndexOp runs, it will build a hash index from the
     * IBindingSet[].
     * <p>
     * Note: The join variables need to be set based on the known bound
     * variables in the context where we will evaluate the solution set hash
     * join (head of the sub-SELECT, OPTIONAL) and those that are bound by the
     * solution set hash join.
     * <p>
     * Note: The static analysis code needs to examine the definitely, and maybe
     * produced bindings for the {@link BindingsClause}. See the
     * {@link ISolutionSetStats} interface and
     * {@link SolutionSetStatserator#get(IBindingSet[])} for a convenience
     * method.
     * <p>
     * 2. {@link SolutionSetHashJoinOp} (JVM or HTree): Joins the solutions
     * flowing into the sub-query or update with the solutions from the
     * HashIndexOp. This will take each solution from the pipeline, probe the
     * hash index for solutions specified by the VALUES clause, and then do a
     * JOIN for each such solution that is discovered.
     */
    private static PipelineOp addValues(PipelineOp left,
            final BindingsClause bindingsClause,
            final Set<IVariable<?>> doneSet, final AST2BOpContext ctx) {

        final boolean usePipelinedHashJoin = usePipelinedHashJoin(ctx, bindingsClause) ;
       
        // Convert solutions from VALUES clause to an IBindingSet[].
        final IBindingSet[] bindingSets = BOpUtility.toArray(
                new Chunkerator<IBindingSet>(bindingsClause.getBindingSets().iterator()),//
                null/*stats*/
                );
        
        // Static analysis of the VALUES solutions.
        final ISolutionSetStats bindingsClauseStats = SolutionSetStatserator
                .get(bindingSets);
        
        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        /*
         * BindingsClause is an IBindingsProducer, but it should also be
         * an IJoinNode. That will let us attach constraints
         * (getJoinConstraints()) and identify the join variables for the VALUES
         * sub-plan (getJoinVars()).
         */
        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(bindingsClause), needsMaterialization);

        /*
         * Model the VALUES JOIN by building a hash index over the IBindingSet[]
         * from the VALUES clause. Then use a solution set hash join to join the
         * solutions flowing through the pipeline with those in the hash index.
         */
        final String solutionSetName = "--values-" + ctx.nextId(); // Unique name.

        final Set<IVariable<?>> joinVarSet = ctx.sa.getJoinVars(bindingsClause,
                bindingsClauseStats, new LinkedHashSet<IVariable<?>>());

        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = joinVarSet.toArray(new IVariable[0]);

//            if (joinVars.length == 0) {
//
//                /*
//                 * Note: If there are no join variables then the join will
//                 * examine the full N x M cross product of solutions. That is
//                 * very inefficient, so we are logging a warning.
//                 */
//
//                log.warn("No join variables: " + subqueryRoot);
//                
//            }
        
        final INamedSolutionSetRef namedSolutionSet = 
            NamedSolutionSetRefUtility.newInstance(
               usePipelinedHashJoin ? null : ctx.queryId, solutionSetName, joinVars);

        // VALUES is not optional.
        final JoinTypeEnum joinType = JoinTypeEnum.Normal;

        // lastPass is required except for normal joins.
        final boolean lastPass = false; 

        // true if we will release the HTree as soon as the join is done.
        // Note: also requires lastPass.
        final boolean release = lastPass;

        // join can be pipelined unless last pass evaluation is required
        final int maxParallel = lastPass ? 1
                : ctx.maxParallelForSolutionSetHashJoin;

        left = addHashIndexOp(left, usePipelinedHashJoin, ctx, bindingsClause, 
              joinType, joinVars,  joinConstraints, 
              joinVars /* projectInVars == joinVars -> inline projection */,
              namedSolutionSet, bindingSets,
              null /* askVar */, null /* subquery */);
        
        // Generate the solution set hash join operator.
        if (!usePipelinedHashJoin) {

            if(ctx.nativeHashJoins) {
                left = applyQueryHints(new HTreeSolutionSetHashJoinOp(
                    leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, maxParallel),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
    //                    new NV(HTreeSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
    //                    new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
    //                    new NV(HTreeSolutionSetHashJoinOp.Annotations.SELECT, null/*all*/),// 
    //                    new NV(HTreeSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
                ), bindingsClause, ctx);
            } else {
               
               left = applyQueryHints(new JVMSolutionSetHashJoinOp(
                   leftOrEmpty(left),//
                   new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                   new NV(BOp.Annotations.EVALUATION_CONTEXT,
                           BOpEvaluationContext.CONTROLLER),//
                   new NV(PipelineOp.Annotations.MAX_PARALLEL, maxParallel),//
                   new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
   //                    new NV(JVMSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
   //                    new NV(JVMSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
   //                    new NV(JVMSolutionSetHashJoinOp.Annotations.SELECT, null/*all*/),//
   //                    new NV(JVMSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                   new NV(JVMSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                   new NV(JVMSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                   new NV(JVMSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
               ), bindingsClause, ctx);

            }
            
        }  // else: hash join is built-in, nothing to do here

        
        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps3(left, doneSet, needsMaterialization,
                bindingsClause.getQueryHints(), ctx);

        return left;
        
    }

    /**
	 * Return <code>true</code> if we can optimize this INCLUDE with a SCAN of
	 * the named solution set and a nested inner loop to join against left
	 * solutions from the pipeline having a known low cardinality.
	 * 
	 * @param ctx
	 * @param nsi
	 *            The INCLUDE operator.
	 * @return <code>true</code> if this optimization can be used.
	 */
	static private boolean isNamedSolutionSetScan(final AST2BOpContext ctx,
			final NamedSubqueryInclude nsi) {

		// The immediately dominating join group (never null).
		final JoinGroupNode parentJoinGroup = nsi.getParentJoinGroup();

		// True iff the INCLUDE is the first operator in the group
		// (the group is in its evaluation order by this point).
		final boolean firstInJoinGroup = parentJoinGroup.get(0) == nsi;

		// The parent of that join group. Null if the join group is
		// the WHERE clause of some (Sub-)Query.
		if (firstInJoinGroup && parentJoinGroup.getParent() == null) {

			/*
			 * The INCLUDE is the first operator in the join group.
			 * 
			 * There is no direct parent for that join group, so this is a
			 * top-level INCLUDE in some WHERE clause of the query.
			 * 
			 * If the query for that WHERE clause is a top-level query or a
			 * NamedSubqueryRoot, then we consider the exogenous solutions
			 * (exogenous solutions are not directly visible in a Sub-Select
			 * unless it gets lifted out as a NamedSubqueryRoot).
			 * 
			 * If the exogenous solutions are (effectively) empty, then we
			 * optimize the INCLUDE and just stream the named solution set
			 * directly into the pipeline.
			 */

			final QueryBase dominatingQuery = (QueryBase) ctx.sa
					.findParent(parentJoinGroup);

			if (dominatingQuery instanceof QueryRoot
					|| dominatingQuery instanceof NamedSubqueryRoot) {

				// The stats associated with the exogenous solutions.
				final ISolutionSetStats exogenousStats = ctx
						.getSolutionSetStats();

				// TODO Extract threshold to AST2BOpContext and QueryHint.
				if (exogenousStats.getSolutionSetSize() <= 100) {

					return true;

				}
		
			}
		
		}
		
		return false;

	}

	/**
	 * If the cardinality of the exogenous solutions is low, then we can SCAN
	 * the named solution set and use an inner loop to test each solution read
	 * from the named solution set against each exogenous solution.
	 * <p>
	 * Note: This code path MUST NOT change the order of the solutions read from
	 * the named solution set. We rely on that guarantee to provide fast ordered
	 * SLICEs from a pre-computed named solution set.
	 * 
	 * @see #isNamedSolutionSetScan(AST2BOpContext, NamedSubqueryInclude)
	 */
	private static PipelineOp convertNamedSolutionSetScan(PipelineOp left,
			final NamedSubqueryInclude nsi, final Set<IVariable<?>> doneSet,
			final AST2BOpContext ctx) {

		@SuppressWarnings("rawtypes")
		final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(nsi), needsMaterialization);

        // The name of the named solution set.
        final String name = nsi.getName();
        
        final INamedSolutionSetRef namedSolutionSet = NamedSolutionSetRefUtility
                .newInstance(ctx.getNamespace(), ctx.getTimestamp(), name,
                        IVariable.EMPTY/* joinVars */);

//        ctx.addQueryAttribute(namedSolutionSet, name);

        left = new NestedLoopJoinOp(leftOrEmpty(left), new NV(
                BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                new NV(NestedLoopJoinOp.Annotations.NAMED_SET_REF,
                        namedSolutionSet),//
//				new NV( NestedLoopJoinOp.Annotations.NAMEX,
//						nsi.getName()),//
				new NV( NestedLoopJoinOp.Annotations.CONSTRAINTS,
						joinConstraints)//
		);
		
        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps3(left, doneSet, needsMaterialization,
                nsi.getQueryHints(), ctx);

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
     * Note: We evaluate SPARQL 1.1 style sub-queries using the same pattern
     * which is used for sub-groups. However, we must also handle variable
     * hiding based on the variables projected by the sub-select.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/232 (Support
     *      bottom-up evaluation semantics)
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/397 (AST Optimizer
     *      for queries with multiple complex optional groups)
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/414 (SPARQL 1.1
     *      EXISTS, NOT EXISTS, and MINUS)
     */
    private static PipelineOp addSparql11Subquery(PipelineOp left,
            final SubqueryRoot subqueryRoot, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

       final boolean usePipelinedHashJoin = usePipelinedHashJoin(ctx, subqueryRoot);

        final ProjectionNode projection = subqueryRoot.getProjection();

        
        
        // The variables projected by the subquery.
        final Set<IVariable<?>> projectedVars = 
            projection.getProjectionVars(new HashSet<IVariable<?>>());
        
        final Set<IVariable<?>> maybeIncomingBindings = 
                ctx.sa.getMaybeIncomingBindings(subqueryRoot, new HashSet<IVariable<?>>());

        projectedVars.retainAll(maybeIncomingBindings);

        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(subqueryRoot), needsMaterialization);

        // Only Sub-Select is supported by this code path.
        switch (subqueryRoot.getQueryType()) {
        case SELECT:
            break;
        default:
            throw new UnsupportedOperationException();
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

        final Set<IVariable<?>> joinVarSet = ctx.sa.getJoinVars(
                subqueryRoot, new LinkedHashSet<IVariable<?>>());

        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = joinVarSet.toArray(new IVariable[0]);

//            if (joinVars.length == 0) {
//
//                /*
//                 * Note: If there are no join variables then the join will
//                 * examine the full N x M cross product of solutions. That is
//                 * very inefficient, so we are logging a warning.
//                 */
//
//                log.warn("No join variables: " + subqueryRoot);
//                
//            }
        
        final INamedSolutionSetRef namedSolutionSet = 
            NamedSolutionSetRefUtility.newInstance(
                usePipelinedHashJoin ? null : ctx.queryId, solutionSetName, joinVars);

        // Sub-Select is not optional.
//            final boolean optional = false;
        final JoinTypeEnum joinType = JoinTypeEnum.Normal;

        // lastPass is required except for normal joins.
        final boolean lastPass = false; 

        // true if we will release the HTree as soon as the join is done.
        // Note: also requires lastPass.
        final boolean release = lastPass;

        // join can be pipelined unless last pass evaluation is required
        final int maxParallel = lastPass ? 1
                : ctx.maxParallelForSolutionSetHashJoin;

        PipelineOp subqueryPlan = null;
        if (usePipelinedHashJoin) {
           /**
            * Subquery execution is controlled by the pipelined hash join
            * itself, so for the pipelined variant we translate the subplan
            * as a standalone pipeline op and attach it to the annotation.
            */
           subqueryPlan = 
               convertQueryBase(null, subqueryRoot, doneSet, ctx);
           // inherit the namespace property (which is needed for the CPU/GPU) to the top-level query
           // see https://github.com/SYSTAP/bigdata-gpu/issues/343           
           subqueryPlan = (PipelineOp) subqueryPlan.setProperty(BOp.Annotations.NAMESPACE, ctx.getNamespace());

        }
        
        left = addHashIndexOp(left, usePipelinedHashJoin, ctx, subqueryRoot, 
              joinType, joinVars, 
              joinConstraints, projectedVars.toArray(new IVariable<?>[0]),
              namedSolutionSet, null /* bindingsSetSource */,
              null /* askVar */, subqueryPlan);
        
        // in case the subquery has not been inlined, append it to the pipeline
        if (!usePipelinedHashJoin) {
           
           left = convertQueryBase(left, subqueryRoot, doneSet, ctx);
           
        }
        
        if (!usePipelinedHashJoin) {
            
            if(ctx.nativeHashJoins) {
                
                left = applyQueryHints(new HTreeSolutionSetHashJoinOp(
                    leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, maxParallel),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.SELECT, null/*all*/),// 
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
                ), subqueryRoot, ctx);
                
            } else {
              
               left = applyQueryHints(new JVMSolutionSetHashJoinOp(
                   leftOrEmpty(left),//
                   new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                   new NV(BOp.Annotations.EVALUATION_CONTEXT,
                           BOpEvaluationContext.CONTROLLER),//
                   new NV(PipelineOp.Annotations.MAX_PARALLEL, maxParallel),//
                   new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
//                   new NV(JVMSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
//                   new NV(JVMSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                   new NV(JVMSolutionSetHashJoinOp.Annotations.SELECT, null/*all*/),//
//                   new NV(JVMSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                   new NV(JVMSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                   new NV(JVMSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                   new NV(JVMSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
               ), subqueryRoot, ctx);
           }
               
        } // else: hash join is built-in, nothing to do here
           
        
        // BLZG-1899: for analytic hash joins, we don't cache materialized values
        //            -> already variables that have not been projected into the subgroup
        //               need to be marked as not done, in order to enforce re-materialization
        //               where needed in later steps of the query plan
        //
        // Note: we always have joinType==JoinTypeEnum.NORMAL for SPARQL 1.1 subqueries,
        //       so we don't need to special case (as we do for subgroups, for instance)
        maybeIncomingBindings.removeAll(projectedVars); // variables that are *not* projected in
        if (ctx.nativeHashJoins)
            doneSet.removeAll(maybeIncomingBindings);
        
        
        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps3(left, doneSet, needsMaterialization,
                subqueryRoot.getQueryHints(), ctx);

        return left;

    }
    
    
    
    /**
     * Add operators to the query plan in support of the evaluation of the graph
     * pattern for (NOT) EXISTS. An ASK subquery is used to model (NOT) EXISTS.
     * The subquery is used to decide, for each source solution, whether the
     * (NOT) EXISTS graph pattern is satisified. An anonymous variable becomes
     * bound to the outcome of this test evaluation. The FILTER then tests the
     * value of the anonymous variable.
     * 
     * @param left
     * @param subqueryRoot
     * @param doneSet
     * @param ctx
     * @return
     * 
     *         TODO If there are no shared variables between the (NOT) EXISTS
     *         graph pattern and join group in which it appears (i.e., no join
     *         variables) then the (NOT) EXISTS should be handled as a RUN_ONCE
     *         ASK Subquery. It will produce a single solution in whicn the
     *         anonymous variable is either true or false. As an optimization
     *         based on the value of the anonymous variable, the entire join
     *         group may either be failed once the solution to the ASK SUBQUERY
     *         is known.
     * 
     *         TODO Mark the ASK SUBQUERY and the MINUS join group with an
     *         annotation which identifies the "FILTER variables" which must be
     *         bound before they can run (if bound by a required join) and which
     *         thereby govern when they are to be evaluated. Use the presence of
     *         that annotation to guide the reordering of the joins by the
     *         {@link ASTJoinOrderByTypeOptimizer} such that we run those nodes
     *         as early as possible and (in the case of (NOT) EXISTS before the
     *         FILTER which references the anonymous variable which is bound by
     *         the ASK subquery).
     *         <p>
     *         Note: While that while the change for ticket 515 fixes that
     *         query, it is possible that we still could get bad join orderings
     *         when the variables used by the filter are only bound by OPTIONAL
     *         joins. It is also possible that we could run the ASK subquery for
     *         FILTER (NOT) EXISTS earlier if the filter variables are bound by
     *         required joins. This is really identical to the join filter
     *         attachment problem. The problem in the AST is that both the ASK
     *         subquery and the FILTER are present. It seems that the best
     *         solution would be to attach the ASK subquery to the FILTER and
     *         then to run it immediately before the FILTER, letting the
     *         existing filter attachment logic decide where to place the
     *         filter. We would also have to make sure that the FILTER was never
     *         attached to a JOIN since the ASK subquery would have to be run
     *         before the FILTER was evaluated.
     * 
     * 
     *         TODO isAggregate() is probably no longer necessary as we always
     *         lift an aggregation subquery into a named subquery. Probably turn
     *         it into an assert instead to verify that an aggregation subquery
     *         is not being run otherwise.
     * 
     * @see ASTExistsOptimizer
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/414">
     *      SPARQL 1.1 EXISTS, NOT EXISTS, and MINUS </a>
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/515">
     *      Query with two "FILTER NOT EXISTS" expressions returns no
     *      results</a>
     * @see <a href="http://trac.blazegraph.com/ticket/988"> bad performance for
     *      FILTER EXISTS </a>
     * @see http://www.w3.org/2009/sparql/wiki/Design:Negation
     */
    private static PipelineOp addExistsSubquery(PipelineOp left,
            final SubqueryRoot subqueryRoot, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        final FilterExistsModeEnum filterExistsMode = subqueryRoot
                .getFilterExistsMode();

        switch (filterExistsMode) {
        case VectoredSubPlan:
            // Vectored sub-plan evaluation.
            return addExistsSubqueryFast(left, subqueryRoot, doneSet, ctx);
        case SubQueryLimitOne:
            // Non-vectored sub-query evaluation.
            return addExistsSubquerySubquery(left, subqueryRoot, doneSet, ctx);
        default:
            throw new UnsupportedOperationException(QueryHints.FILTER_EXISTS
                    + "=" + filterExistsMode);
        }
        
    }

    /**
     * (NOT) EXISTS code path using a vectored sub-plan.
     * 
     * @param left
     * @param subqueryRoot
     * @param doneSet
     * @param ctx
     * @return
     */
    private static PipelineOp addExistsSubqueryFast(PipelineOp left,
            final SubqueryRoot subqueryRoot, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        final boolean usePipelinedHashJoin = usePipelinedHashJoin(ctx, subqueryRoot) ;
       
        // Only Sub-Select is supported by this code path.
        switch (subqueryRoot.getQueryType()) {
        case ASK:
            break;
        default:
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(subqueryRoot), needsMaterialization);

        /*
         * Model a sub-group by building a hash index at the start of the
         * group. We then run the group.  Finally, we do a hash join of
         * the hash index against the solutions in the group.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/377#comment:4
         */
        final String solutionSetName = "--set-" + ctx.nextId(); // Unique name.

        final Set<IVariable<?>> joinVarSet = ctx.sa.getJoinVars(
                subqueryRoot, new LinkedHashSet<IVariable<?>>());

        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = joinVarSet.toArray(new IVariable[0]);

        /**
         * Compute the set of variables we need to project in. This is typically
         * identical to the set of join vars, but my differ for queries such
         * as
         * 
         * <code>
            SELECT * WHERE {
              ?a :p ?n
              FILTER NOT EXISTS {
                ?a :q ?m .
                FILTER(?n = ?m)
              }
            }
           </code>
         *
         * , where we join on ?a, but need to project in ?n as well (since
         * the scope of the FILTER includes the outer scope).
         */
        final Set<IVariable<?>> projectInVars = ctx.sa.getMaybeIncomingBindings(
              subqueryRoot, new LinkedHashSet<IVariable<?>>());
        final Set<IVariable<?>> spannedVars = 
              ctx.sa.getSpannedVariables(subqueryRoot, new HashSet<IVariable<?>>());
//        projectInVars.retainAll(alpVars);
        projectInVars.retainAll(spannedVars);
        final IVariable<?>[] projectInVarsArr =
              projectInVars.toArray(new IVariable<?>[projectInVars.size()]);
        
        
        final INamedSolutionSetRef namedSolutionSet = 
            NamedSolutionSetRefUtility.newInstance(
                usePipelinedHashJoin ? null : ctx.queryId, solutionSetName, joinVars);

        /*
         * Note: We also need to set the "asksVar" annotation to get back T/F
         * for each source solution depending on whether or not it passed the
         * graph pattern. If order for this to work, we need to output both the
         * joinSet (askVar:=true) and the set of things which did not join
         * (askVar:=false). The set of things which did not join is identified
         * in the same manner that we identify the "OPTIONAL" solutions, so this
         * is really more like an "OPTIONAL" join.
         */
        final JoinTypeEnum joinType = JoinTypeEnum.Exists;

        // lastPass is required except for normal joins.
        final boolean lastPass = true; 

        // true if we will release the HTree as soon as the join is done.
        // Note: also requires lastPass.
        final boolean release = lastPass;

        /*
         * Pass all variable bindings along.
         * 
         * Note: If we restrict the [select] annotation to only those variables
         * projected by the subquery, then we will wind up pruning any variables
         * used in the join group which are NOT projected into the subquery.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/515
         */
//        
//        // The variables projected by the subquery.
//        final IVariable<?>[] projectedVars = subqueryRoot.getProjection()
//                .getProjectionVars();
        
        // The variable which gets bound if the solutions "exist".
        final IVariable<?> askVar = subqueryRoot.getAskVar();

        if (askVar == null)
            throw new UnsupportedOperationException();

        PipelineOp subqueryPlan = null;
        if (usePipelinedHashJoin) {
           /**
            * Subquery execution is controlled by the pipelined hash join
            * itself, so for the pipelined variant we translate the subplan
            * as a standalone pipeline op and attach it to the annotation.
            */
           subqueryPlan = 
              convertJoinGroupOrUnion(
                 null /* standalone */, subqueryRoot.getWhereClause(),
                 new LinkedHashSet<IVariable<?>>(doneSet)/* doneSet */, ctx);
           // inherit the namespace property (which is needed for the CPU/GPU),
           // see https://github.com/SYSTAP/bigdata-gpu/issues/343
           subqueryPlan = (PipelineOp) subqueryPlan.setProperty(BOp.Annotations.NAMESPACE, ctx.getNamespace());           
        } 

        
        left = addHashIndexOp(left, usePipelinedHashJoin, ctx, subqueryRoot, 
              joinType, joinVars, joinConstraints, projectInVarsArr, namedSolutionSet, 
              null /* bindingsSetSource */, askVar, subqueryPlan);


        // Everything is visible inside of the EXISTS graph pattern.
//        /*
//         * Only the variables which are projected by the subquery may flow
//         * into the subquery. Adding a projection operator before the
//         * subquery plan ensures that variables which are not visible are
//         * dropped out of the solutions flowing through the subquery.
//         * However, those variables are already present in the hash index so
//         * they can be reunited with the solutions for the subquery in the
//         * solution set hash join at the end of the subquery plan.
//         */
//        left = new ProjectionOp(leftOrEmpty(left), //
//                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
//                new NV(BOp.Annotations.EVALUATION_CONTEXT,
//                        BOpEvaluationContext.CONTROLLER),//
//                new NV(PipelineOp.Annotations.SHARED_STATE,true),// live stats
//                new NV(ProjectionOp.Annotations.SELECT, projectedVars)//
//        );
        
        /*
         * FIXME EXISTS: Try DISTINCT in the sub-plan and compare to correctness
         * without for (NOT) EXISTS and to performance of the non-vectored code
         * path for EXISTS>
         */

        // in case the subquery has not been inlined, append it to the pipeline
        if (!usePipelinedHashJoin) {
           left = convertJoinGroupOrUnion(left, subqueryRoot.getWhereClause(),
                   new LinkedHashSet<IVariable<?>>(doneSet)/* doneSet */, ctx);
        }
        
        
        if (!usePipelinedHashJoin) {
            
            if(ctx.nativeHashJoins) {
                
                left = applyQueryHints(new HTreeSolutionSetHashJoinOp(
                    leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.SELECT, null/*all*/),// 
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
                ), subqueryRoot, ctx);
                
            } else {
                
               left = applyQueryHints(new JVMSolutionSetHashJoinOp(
                   leftOrEmpty(left),//
                   new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                   new NV(BOp.Annotations.EVALUATION_CONTEXT,
                           BOpEvaluationContext.CONTROLLER),//
                   new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                   new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
//                   new NV(JVMSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
//                   new NV(JVMSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                   new NV(JVMSolutionSetHashJoinOp.Annotations.SELECT, null/*all*/),//
//                   new NV(JVMSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                   new NV(JVMSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                   new NV(JVMSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                   new NV(JVMSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
               ), subqueryRoot, ctx);
               
            }
        }

        // BLZG-1899: for analytic hash joins, we don't cache materialized values
        //            -> for joinType==JoinTypeEnum.EXISTS we don't have any guarantees
        //            that materialized values are preserved, so we need to clear the doneSet
        doneSet.clear();                
        
        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps3(left, doneSet, needsMaterialization,
                subqueryRoot.getQueryHints(), ctx);

        return left;

    }
    
    /**
     * A non-vectored implementation for (NOT) EXISTS using one
     * {@link SubqueryOp} per source solution.
     */
    private static PipelineOp addExistsSubquerySubquery(PipelineOp left,
            final SubqueryRoot subqueryRoot, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        // Only "ASK" subqueries are allowed.
        switch (subqueryRoot.getQueryType()) {
        case ASK:
            break;
        default:
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(subqueryRoot), needsMaterialization);

        final boolean aggregate = StaticAnalysis.isAggregate(subqueryRoot);
        
        /*
         * The anonymous variable which gets bound based on the (NOT) EXISTS
         * graph pattern.
         */
        final IVariable<?> askVar = subqueryRoot.getAskVar();

        if (askVar == null)
            throw new UnsupportedOperationException();

        /*
         * Impose LIMIT ONE on the non-vectored sub-query.
         * 
         * Note: This reduces the amount of work for the sub-query.
         * 
         * For EXISTS, this means that we stop if we find at least one solution.
         * The askVar becomes bound to true. The IConstraint associated with the
         * EXISTS FILTER will therefore evaluate to true.
         * 
         * For NOT EXISTS, this means that we stop if we find at least one
         * solution. The askVar becomes bound to true (this is the same as for
         * EXISTS). The IConstraint associated with the NOT EXISTS FILTER will
         * therefore evaluate to false since it tests !askVar.
         */
        subqueryRoot.setSlice(new SliceNode(0L/* offset */, 1L/* limit */));
        
        PipelineOp subqueryPlan = convertQueryBase(null/* left */, subqueryRoot, doneSet, ctx);
        // inherit the namespace property (which is needed for the CPU/GPU) to the top-level query
        // see https://github.com/SYSTAP/bigdata-gpu/issues/343      
        subqueryPlan = (PipelineOp) subqueryPlan.setProperty(BOp.Annotations.NAMESPACE, ctx.getNamespace());        

        left = new SubqueryOp(leftOrEmpty(left),// SUBQUERY
                new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
                new NV(SubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                new NV(SubqueryOp.Annotations.JOIN_TYPE, JoinTypeEnum.Normal),//
                new NV(SubqueryOp.Annotations.ASK_VAR, askVar),//
                new NV(SubqueryOp.Annotations.SELECT, subqueryRoot.getProjection().getProjectionVars()),//
                new NV(SubqueryOp.Annotations.CONSTRAINTS, joinConstraints),//
                new NV(SubqueryOp.Annotations.IS_AGGREGATE, aggregate)//
        );

        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps3(left, doneSet, needsMaterialization,
                subqueryRoot.getQueryHints(), ctx);

        return left;

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
            
            left = applyQueryHints(
                    new Tee(leftOrEmpty(left), NV.asMap(anns
                            .toArray(new NV[anns.size()]))), unionNode, ctx);

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
             * 
             * FIXME Rolling back r7319 which broke UNION processing. 
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
                ), unionNode, ctx);

        // Add in anything which was known materialized for all child groups.
        doneSet.addAll(doneSetsIntersection);

        return left;

    }

    /**
     * Generate the query plan for an arbitrary length path.
     */
    private static PipelineOp convertArbitraryLengthPath(PipelineOp left,
            final ArbitraryLengthPathNode alpNode, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        final boolean usePipelinedHashJoin = usePipelinedHashJoin(ctx, alpNode);
       
        /**
         * The inner operations is quite complex, we definitely want to avoid
         * executing it for the same variable bindings over and over again.
         * Furthermore, the current version of the operator is not capable of
         * joining with unknown variables. Therefore, we need to compute the
         * distinct projection over the variables that are passed in. We do
         * this through the common HashJoinPattern, i.e. pass in the distinct
         * variables that are bound inside the ALP and re-join in the end using
         * the hash index. This can help significantly to reduce efforts.
         */

        /**
         * Calculate the join variables as the intersection of the maybe
         * incoming bindings of the ALP node with the variables bound inside.
         * For short, this is the subset of the ALP node's variable that
         * is possibly also bound from previous computations.
         */
        final Set<IVariable<?>> alpVars =
              ctx.sa.getDefinitelyProducedBindings
              (alpNode, new LinkedHashSet<IVariable<?>>(), true);
        
        final Set<IVariable<?>> joinVarsSet = ctx.sa.getDefinitelyIncomingBindings(
              alpNode, new LinkedHashSet<IVariable<?>>());
        joinVarsSet.retainAll(alpVars);

        final IVariable<?>[] joinVars = 
              joinVarsSet.toArray(new IVariable<?>[joinVarsSet.size()]);

        /**
         * We project in everything that might help in binding the variables
         * of the ALP node.
         */
        final Set<IVariable<?>> alpUsedVars = alpNode.getUsedVars();
        
        // compute the variables that we project into the inner subgroup and those
        // that we do *not* project in - both calculations start out with the set 
        // of possibly bind variables
        final Set<IVariable<?>> projectInVars = ctx.sa.getMaybeIncomingBindings(
                alpNode, new LinkedHashSet<IVariable<?>>());
        final Set<IVariable<?>> nonProjectInVars = new HashSet<IVariable<?>>(projectInVars);

        // we project in whatever's used inside the ALP
        projectInVars.retainAll(alpUsedVars);
        IVariable<?>[] projectInVarsArr =
              projectInVars.toArray(new IVariable<?>[projectInVars.size()]);

        // the remaining variables are those that are not projected in
        nonProjectInVars.removeAll(projectInVars);
        
        if (log.isDebugEnabled()) {
            log.debug(alpNode.getUsedVars());
            log.debug(ctx.sa.getMaybeIncomingBindings(
              alpNode, new LinkedHashSet<IVariable<?>>()));
            log.debug(projectInVars);
        }
        
        /**
         * Set up a named solution set associated with a hash index operation.
         * Note that we pass in the project in variables, so the hash join
         * will output a distinct projection over these variables (which is
         * a prerequisite to guarantee correctness and efficiency of the ALP op.
         */
        final String solutionSetName = "--set-" + ctx.nextId(); // Unique name.
              
		// See BLZG-1493 (queryId is null to use the then running query rather
		// than the parent to resolve the IHashJoinUtility instance for the
        // sub-queries evaluated by the property path operator.
        final INamedSolutionSetRef namedSolutionSet = 
              NamedSolutionSetRefUtility.newInstance( 
              null/*ctx.queryId*/, solutionSetName, joinVars);
        
        /**
         * Next, convert the child join group into a subquery
         */
        final JoinGroupNode subgroup = (JoinGroupNode) alpNode.subgroup();

        PipelineOp subquery = convertJoinGroup(null/*left*/,
                subgroup, doneSet, ctx, false/* needsEndOp */);

        if (ctx.isCluster()) {
            
            /**
             * Note: This is necessary if the first operator in the query plan
             * is a sharded join and we want it to run using a sharded index
             * view. Without this, the operator will actually run against the
             * global index view.
             * <p>
             * Note: There may be other ways to "fix" this. See the ticket for
             * more information.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/478" > Cluster does
             *      not map input solution(s) across shards </a>
             * @see <a href="http://trac.blazegraph.com/ticket/942" > Property path
             *      errors in scale-out </a>
             */
            
            subquery = applyQueryHints(
                    new StartOp(BOp.NOARGS, NV.asMap(new NV[] {//
                                    new NV(Predicate.Annotations.BOP_ID, ctx
                                            .nextId()),
                                    new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                            BOpEvaluationContext.CONTROLLER), })),
                    alpNode, ctx);
            
        }
        
        // inherit the namespace property (which is needed for the CPU/GPU),
        // see https://github.com/SYSTAP/bigdata-gpu/issues/343
        subquery = (PipelineOp) subquery.setProperty(BOp.Annotations.NAMESPACE, ctx.getNamespace());
        


        /**
         * Now, we're ready to set up the ALPOp at the core. 
         */
        final IVariableOrConstant<?> leftTerm = 
        		alpNode.left().getValueExpression();
        
        final IVariableOrConstant<?> rightTerm = 
        		alpNode.right().getValueExpression();
        
        final IVariable<?> tVarLeft = 
        		alpNode.tVarLeft().getValueExpression();
        
        final IVariable<?> tVarRight = 
        		alpNode.tVarRight().getValueExpression();
        
        final IVariable<?> edgeVar = alpNode.edgeVar() != null ? 
                alpNode.edgeVar().getValueExpression() : null;
           	
        final IVariableOrConstant<?> middleTerm = alpNode.middle() != null ? 
                alpNode.middle().getValueExpression() : null;
                
        final List<IVariable<?>> dropVars = new ArrayList<>();
        for (VarNode v : alpNode.dropVars()) {
            dropVars.add(v.getValueExpression());
        }

        PipelineOp alpOp = null;
        if (usePipelinedHashJoin) {

           /**
            * Subquery execution is controlled by the pipelined hash join
            * itself, so for the pipelined variant we translate the subplan
            * as a standalone pipeline op and attach it to the annotation.
            */
           alpOp = applyQueryHints(new ArbitraryLengthPathOp(leftOrEmpty(null),//
                 new NV(ArbitraryLengthPathOp.Annotations.SUBQUERY, subquery),
                 new NV(ArbitraryLengthPathOp.Annotations.LEFT_TERM, leftTerm),
                 new NV(ArbitraryLengthPathOp.Annotations.RIGHT_TERM, rightTerm),
                 new NV(ArbitraryLengthPathOp.Annotations.TRANSITIVITY_VAR_LEFT, tVarLeft),
                 new NV(ArbitraryLengthPathOp.Annotations.TRANSITIVITY_VAR_RIGHT, tVarRight),
                     new NV(ArbitraryLengthPathOp.Annotations.EDGE_VAR, edgeVar),
                     new NV(ArbitraryLengthPathOp.Annotations.MIDDLE_TERM, middleTerm),
                 new NV(ArbitraryLengthPathOp.Annotations.LOWER_BOUND, alpNode.lowerBound()),
                 new NV(ArbitraryLengthPathOp.Annotations.UPPER_BOUND, alpNode.upperBound()),
                     new NV(ArbitraryLengthPathOp.Annotations.PROJECT_IN_VARS, projectInVarsArr),
                     new NV(ArbitraryLengthPathOp.Annotations.DROP_VARS, dropVars),
                 new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
                 new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER)//
                 ), alpNode, ctx);
           // inherit the namespace property (which is needed for the CPU/GPU) to the top-level query
           // see https://github.com/SYSTAP/bigdata-gpu/issues/343                 
           alpOp = (PipelineOp) alpOp.setProperty(BOp.Annotations.NAMESPACE, ctx.getNamespace());           

        }

        left = addHashIndexOp(left, usePipelinedHashJoin, ctx, alpNode, 
              JoinTypeEnum.Normal, joinVars, null, projectInVarsArr, 
              namedSolutionSet, null /* bindingsSetSource */,
              null /* askVar */, alpOp);

        
        // in case the alpOp has not been inlined, append it to the pipeline
        if (!usePipelinedHashJoin) {
           
           left = applyQueryHints(new ArbitraryLengthPathOp(leftOrEmpty(left),//
                 new NV(ArbitraryLengthPathOp.Annotations.SUBQUERY, subquery),
                 new NV(ArbitraryLengthPathOp.Annotations.LEFT_TERM, leftTerm),
                 new NV(ArbitraryLengthPathOp.Annotations.RIGHT_TERM, rightTerm),
                 new NV(ArbitraryLengthPathOp.Annotations.TRANSITIVITY_VAR_LEFT, tVarLeft),
                 new NV(ArbitraryLengthPathOp.Annotations.TRANSITIVITY_VAR_RIGHT, tVarRight),
                     new NV(ArbitraryLengthPathOp.Annotations.EDGE_VAR, edgeVar),
                     new NV(ArbitraryLengthPathOp.Annotations.MIDDLE_TERM, middleTerm),
                 new NV(ArbitraryLengthPathOp.Annotations.LOWER_BOUND, alpNode.lowerBound()),
                 new NV(ArbitraryLengthPathOp.Annotations.UPPER_BOUND, alpNode.upperBound()),
                     new NV(ArbitraryLengthPathOp.Annotations.PROJECT_IN_VARS, projectInVarsArr),
                     new NV(ArbitraryLengthPathOp.Annotations.DROP_VARS, dropVars),
                 new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
                 new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER)//
                 ), alpNode, ctx);
         }

        /**
         * Finally, re-join the inner result with the hash index.
         */
        if (!usePipelinedHashJoin) {

            if(ctx.nativeHashJoins) {
               
                left = applyQueryHints(new HTreeSolutionSetHashJoinOp(
                    leftOrEmpty(left),//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                           BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.CONSTRAINTS, null),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE, true),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.LAST_PASS, true),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
                    ), subgroup, ctx);
               
            } else {

                left = applyQueryHints(new JVMSolutionSetHashJoinOp(
                    leftOrEmpty(left),//
                      new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                      new NV(BOp.Annotations.EVALUATION_CONTEXT,
                              BOpEvaluationContext.CONTROLLER),//
                      new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                      new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                      new NV(JVMSolutionSetHashJoinOp.Annotations.CONSTRAINTS, null),//
                      new NV(JVMSolutionSetHashJoinOp.Annotations.RELEASE, true),//
                      new NV(JVMSolutionSetHashJoinOp.Annotations.LAST_PASS, true),//
                      new NV(JVMSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
                ), subgroup, ctx);
              
            } 
        } // else: hash join is built-in, nothing to do here
       
        // BLZG-1899: for analytic hash joins, we don't cache materialized values
        //            -> already variables that have not been projected into the subgroup
        //               need to be marked as not done, in order to enforce re-materialization
        //               where needed in later steps of the query plan
        //
        // Note: we always have joinType==JoinTypeEnum.NORMAL for arbitrary length paths
        //       so we don't need to special case (as we do for subgroups, for instance)        
        if (ctx.nativeHashJoins)
            doneSet.removeAll(nonProjectInVars);
        
        return left;

    }

    /**
     * Generate the query plan for a zero length path.
     */
    private static PipelineOp convertZeroLengthPath(PipelineOp left,
            final ZeroLengthPathNode zlpNode, final Set<IVariable<?>> doneSet,
            final AST2BOpContext ctx) {

        final IVariableOrConstant<?> leftTerm = 
        		(IVariableOrConstant<?>) zlpNode.left().getValueExpression();
        
        final IVariableOrConstant<?> rightTerm = 
        		(IVariableOrConstant<?>) zlpNode.right().getValueExpression();
        
        left = applyQueryHints(new ZeroLengthPathOp(leftOrEmpty(left),//
    			new NV(ZeroLengthPathOp.Annotations.LEFT_TERM, leftTerm),
    			new NV(ZeroLengthPathOp.Annotations.RIGHT_TERM, rightTerm),
                new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER)//
                ), zlpNode, ctx);

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
     * @param ctx
     *            The evaluation context.
     * @param needsEndOp
     *            When <code>true</code> and the parent is not-
     *            <code>null</code>, adds an {@link EndOp} to the plan.
     */
    private static PipelineOp convertJoinGroup(//
            PipelineOp left,//
            final JoinGroupNode joinGroup,//
            final Set<IVariable<?>> doneSet,//
            final AST2BOpContext ctx,//
            final boolean needsEndOp) {

//        final StaticAnalysis sa = ctx.sa;
//
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
         * Anonymous variable(s) (if any) which were injected into the solutions
         * in support of (NOT) EXISTS and which need to be dropped before we
         * exit this group.
         */
        final LinkedList<IVariable<?>> dropVars = new LinkedList<IVariable<?>>();
        
        /*
         * This checks to make sure that join filters were attached to join
         * nodes, in which case they should no longer be present as direct
         * children of this group.
         * 
         * @see ASTAttachJoinFilterOptimizer
         * 
         * I commented this out because this is allowed now - because of
         * property paths we now have filters that are not attached to joins,
         * but this is perfectly fine.
         * 
         * TODO Allow the ArbitraryLengthPathOp to accept filters.
         */
//        assert sa.getJoinFilters(joinGroup).isEmpty() : "Unattached join filters: "
//                + joinGroup;
        
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
         * This could be done now using a query local named solution set as an
         * inline access path. However, named solution sets are not currently
         * shipped around on a cluster, so that would not yet work on a cluster
         * for default graphs joins unless those joins are marked as running on
         * the query controller (I think that they might be).
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Replace
         * DataSetJoin with an "inline" access path.)
         * 
         * @see JoinGroupNode#getInFilters()
         */
        final Set<FilterNode> inFilters = new LinkedHashSet<FilterNode>();
        inFilters.addAll(joinGroup.getInFilters());

        final AtomicInteger start = new AtomicInteger(0);

        if (ctx.gpuEvaluation != null
        	&& joinGroup.getProperty(com.bigdata.rdf.sparql.ast.eval.GpuAnnotations.EVALUATE_ON_GPU,
        			com.bigdata.rdf.sparql.ast.eval.GpuAnnotations.DEFAULT_EVALUATE_ON_GPU)) {
        	
        	left = ctx.gpuEvaluation.convertJoinGroup(left, joinGroup, doneSet, start, ctx);
        	
        }
        else if (joinGroup.getQueryHintAsBoolean(QueryHints.MERGE_JOIN,
                ctx.mergeJoin)) {

            /*
             * Attempt to interpret the leading sequence in the group as a merge
             * join.
             */
            
            left = doMergeJoin(left, joinGroup, doneSet, start, ctx);
            
        }

        if (QueryOptimizerEnum.Runtime.equals(joinGroup.getQueryOptimizer())) {

            /*
             * Inspect the remainder of the join group. If we can isolate a join
             * graph and filters, then we will push them down into an RTO
             * JoinGroup. Since the joins have already been ordered by the
             * static optimizer, we can accept them in sequence along with any
             * attachable filters.
             */
            
            left = convertRTOJoinGraph(left, joinGroup, doneSet, ctx, start);
            
            /*
             * Fall through. Anything not handled in this section will be
             * handled as part of normal join group processing below.
             */

        }
        
        /*
         * Translate the remainder of the group. 
         */
        final int arity = joinGroup.arity();
        for (int i = start.get(); i < arity; i++) {

            final IGroupMemberNode child = (IGroupMemberNode) joinGroup.get(i);

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
                 */
                final Predicate<?> pred = toPredicate(sp, ctx);
                final boolean optional = sp.isOptional();
                left = join(left, //
                        pred,//
                        optional ? new LinkedHashSet<IVariable<?>>(doneSet)
                                : doneSet,//
                        getJoinConstraints(sp), //
                        null, // cutoffLimit
                        sp.getQueryHints(), //
                        ctx);
                continue;
            } else if (child instanceof ArbitraryLengthPathNode) {
                final ArbitraryLengthPathNode alpNode = (ArbitraryLengthPathNode) child;
                left = convertArbitraryLengthPath(left, alpNode, doneSet, ctx);
                continue;
            } else if (child instanceof ZeroLengthPathNode) {
                final ZeroLengthPathNode zlpNode = (ZeroLengthPathNode) child;
                left = convertZeroLengthPath(left, zlpNode, doneSet, ctx);
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
            } else if (child instanceof BindingsClause) {
                /*
                 * VALUES clause
                 */
                left = addValues(left,
                        (BindingsClause) child, doneSet, ctx);
                continue;
            } else if (child instanceof SubqueryRoot) {
                final SubqueryRoot subquery = (SubqueryRoot) child;
                switch (subquery.getQueryType()) {
                case ASK:
                    /*
                     * The graph pattern for (NOT) EXISTS. An anonymous variable
                     * becomes bound to the truth state of the graph pattern
                     * (whether or not a solution exists). The FILTER in which
                     * the (NOT) EXISTS appears then tests the truth state of
                     * that anonmyous variable. The graph pattern must be
                     * evaluated before the FILTER in order for the anonymous
                     * variable to be bound.
                     */
                    dropVars.add(subquery.getAskVar());
                    left = addExistsSubquery(left, subquery, doneSet, ctx);
                    break;
                case SELECT:
                    /*
                     * SPARQL 1.1 style subqueries which were not lifted out
                     * into named subqueries.
                     */
                    left = addSparql11Subquery(left, subquery, doneSet, ctx);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Subquery has queryType=" + subquery.getQueryType());
                }
                continue;
            } else if (child instanceof UnionNode) {
//                @SuppressWarnings("unchecked")
                final UnionNode unionNode = (UnionNode) child;
                left = convertUnion(left, unionNode, doneSet, ctx);
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
                final boolean required = !(subgroup.isOptional() || subgroup
                        .isMinus());
                
                final Set<IVariable<?>> groupLocalDoneSet = 
                    required ? doneSet : new LinkedHashSet<IVariable<?>>(doneSet);
                left = addSubgroup(left, subgroup, groupLocalDoneSet, ctx);
                
                /**
                 * BLZG-1688: in the inner group, we may actuallt not only extend the (passed in)
                 *            groupLocalDoneSet, but also remove variables from it again. Removing
                 *            variables has global visibility, so we need to propagate this information
                 *            to the doneSet.
                 */
                doneSet.retainAll(groupLocalDoneSet);
                continue;
            } else if (child instanceof FilterNode) {
                final FilterNode filter = (FilterNode) child;
                if (inFilters.contains(filter)) {
                    left = addKnownInConditional(left, filter, ctx);
                    continue;
                }
                // FILTER
                left = addConditional(left, joinGroup, filter, doneSet, ctx);
                continue;
            } else if (child instanceof AssignmentNode) {
               
               if (mustBeResolvedInContext((AssignmentNode)child,ctx)) {
                  
                  left = addResolvedAssignment(left, (AssignmentNode) child, 
                        doneSet, joinGroup.getQueryHints(), ctx);
                  
               } else {
                  
                  left = addAssignment(left, (AssignmentNode) child, doneSet,
                        joinGroup.getQueryHints(), ctx, false/* projection */);
                  
               }

               continue;
               
            } else if (child instanceof BindingsClause) {
//                // LET / BIND
//                left = addAssignment(left, (AssignmentNode) child, doneSet,
//                        joinGroup.getQueryHints(), ctx, false/* projection */);
                continue;
            } else {
                throw new UnsupportedOperationException("child: " + child);
            }
        } // next child.
        
        if (!dropVars.isEmpty()) {
            final IVariable<?>[] a = dropVars.toArray(new IVariable[dropVars
                    .size()]);
            left = applyQueryHints(new DropOp(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()), //
                    new NV(DropOp.Annotations.DROP_VARS, a)//
            ), joinGroup, ctx);
        }
        
        /*
         * Add the end operator if necessary.
         */
        if (needsEndOp && joinGroup.getParent() != null) {
            left = addEndOp(left, ctx);
        }

        return left;

    }

    /**
     * Attempt to translate the join group using a merge join.
     * <P>
     * We recognize a merge join when there an INCLUDE followed by either a
     * series of INCLUDEs -or- a series of <code>OPTIONAL {INCLUDE}</code>s in
     * the group. The initial INCLUDE becomes the primary source for the merge
     * join (the hub). Each INCLUDE after the first must have the same join
     * variables. If the <code>OPTIONAL {INCLUDE}</code>s pattern is recognized
     * then the MERGE JOIN is itself OPTIONAL. The sequences of such INCLUDEs in
     * this group is then translated into a single MERGE JOIN operator.
     * <p>
     * Note: The critical pattern for a merge join is that we have a hash index
     * against which we may join several other hash indices. To join, of course,
     * the hash indices must all have the same join variables. The pattern
     * recognized here is based on an initial INCLUDE (defining the first hash
     * index) followed by several either required or OPTIONAL INCLUDEs, which
     * are the additional hash indices. The merge join itself is the N-way
     * solution set hash join and replaces the sequence of 2-way solution set
     * hash joins which we would otherwise do for those solution sets. It is
     * more efficient because the output of each 2-way join is fed into the next
     * 2-way join, which blows up the input cardinality for the next 2-way hash
     * join. While the merge join will consider the same combinations of
     * solutions, it does so with a very efficient linear pass over the hash
     * indices.
     * <p>
     * For the JVM Merge Join operator we have to do a SORT first, but the HTree
     * imposes a consistent ordering by the hash bits so we can go directly to
     * the linear pass. (It is actually log linear due to the tree structure of
     * the HTree, which we presume has basically the same costs as a function of
     * depth as a B+Tree, but it is against main memory and it is a sequential
     * scan of the index so it should be effectively linear.)
     * 
     * @param left
     * @param joinGroup
     * @param doneSet
     * @param start
     *            Modified by side-effect to indicate how many children were
     *            absorbed by the "merge-join" IFF a merge join was used.
     * @param ctx
     * 
     * @return <i>left</i> if no merge join was recognized and otherwise the
     *         merge join plan.
     * 
     *         TODO We also need to handle join filters which appear between
     *         INCLUDES. That should not prevent a merge join. Instead, those
     *         join filters can just be moved to after the MERGE JOIN. (The join
     *         filters wind up not attached to the INCLUDE when there is a
     *         materialization requirement for the filter. The materialization
     *         step can not occur in the merge join, so we either have to move
     *         the filter beyond the merge-join or we have to stop collecting
     *         INCLUDEs at the first unattached join filter.)
     * 
     *         TODO Pre-filters could also mess this up. The should be lifted
     *         out into the parent join group.
     * 
     *         TODO This should be done in the AST where we can manage the join
     *         order, verify the join variables, etc.
     */
    private static PipelineOp doMergeJoin(PipelineOp left,
            final JoinGroupNode joinGroup,
            final Set<IVariable<?>> doneSet,
            final AtomicInteger start,
            final AST2BOpContext ctx) {
        
//        final StaticAnalysis sa = ctx.sa;
        
        boolean mergeJoin = true; // until proven otherwise.
        
        final int arity = joinGroup.arity();

        /*
         * The named solution set which is the hub of the merge join and null if
         * we do not find such a hub.
         */
        final NamedSubqueryInclude firstInclude;
//        final NamedSubqueryRoot hub;
        if (arity >= 2 && joinGroup.get(0) instanceof NamedSubqueryInclude) {
            firstInclude = (NamedSubqueryInclude) joinGroup.get(0);
//            hub = firstInclude.getNamedSubqueryRoot(sa.getQueryRoot());
        } else {
            return left;
        }
        
        final List<NamedSubqueryInclude> requiredIncludes = new LinkedList<NamedSubqueryInclude>();
        final List<NamedSubqueryInclude> optionalIncludes = new LinkedList<NamedSubqueryInclude>();

        // Collect the join constraints from each INCLUDE that we will use.
        final List<FilterNode> joinConstraints = new LinkedList<FilterNode>();
        
        /*
         * Join variables must be the same for all secondary sources. They are
         * set once we find the first secondary source.
         */
        final Set<IVariable<?>> joinVars = new LinkedHashSet<IVariable<?>>();

        // Start at the 2nd member in the group.
        int j;
        for (j = 1; j < arity && mergeJoin; j++) {

            final IGroupMemberNode child = (IGroupMemberNode) joinGroup
                    .get(j);

            if (requiredIncludes.isEmpty()
                    && (child instanceof JoinGroupNode)
                    && ((JoinGroupNode) child).isOptional()
                    && ((JoinGroupNode) child).arity() == 1
                    && (((JoinGroupNode) child).get(0) instanceof NamedSubqueryInclude)) {

                /*
                 * OPTIONAL {INCLUDE x}.
                 */

                final NamedSubqueryInclude nsi = (NamedSubqueryInclude) ((JoinGroupNode) child)
                        .get(0);

//                final NamedSubqueryRoot nsr = nsi.getNamedSubqueryRoot(sa
//                        .getQueryRoot());

                final Set<IVariable<?>> theJoinVars = nsi.getJoinVarSet();

                if (joinVars.isEmpty()) {
                    joinVars.addAll(theJoinVars);
                } else if (!joinVars.equals(theJoinVars)) {
                    /*
                     * The join variables are not the same.
                     * 
                     * TODO It is possible to fix this for some queries since
                     * the there is some flexibility in how we choose the join
                     * variables. However, we need to model the MERGE JOIN in
                     * the AST in order to do that since, by this time, we have
                     * already generated the physical query plan for the named
                     * subquery and the join vars are baked into that plan.
                     */
                    break;
                }

                optionalIncludes.add(nsi);

                // Combine the attached join filters (if any).
                joinConstraints.addAll(nsi.getAttachedJoinFilters());

            } else if (optionalIncludes.isEmpty()
                    && child instanceof NamedSubqueryInclude) {

                /*
                 * INCLUDE %namedSet JOIN ON (theJoinVars)
                 */

                final NamedSubqueryInclude nsi = (NamedSubqueryInclude) child;

//                final NamedSubqueryRoot nsr = nsi.getNamedSubqueryRoot(sa
//                        .getQueryRoot());

                final Set<IVariable<?>> theJoinVars = nsi.getJoinVarSet();

                if (joinVars.isEmpty()) {

                    if (theJoinVars.isEmpty()) {

                        /**
                         * If the 2nd INCLUDE does not have any join variables
                         * either then we can not do a merge join. However, see
                         * the comment block immediately below. We should be
                         * doing better in join variable assignment for
                         * sub-selects!
                         */
                        
                        break;
                        
                    }
                    
                    joinVars.addAll(theJoinVars);
                    
                } else if (!joinVars.equals(theJoinVars)) {

                    /**
                     * The join variables are not the same.
                     * 
                     * TODO It is possible to fix this for some queries since
                     * the there is some flexibility in how we choose the join
                     * variables. However, we need to model the MERGE JOIN in
                     * the AST in order to do that since, by this time, we have
                     * already generated the physical query plan for the named
                     * subquery and the join vars are baked into that plan.
                     * 
                     * TODO In fact, we probably can be more aggressive about
                     * putting join variables onto sub-selects and named
                     * subqueries. If static analysis can predict that some
                     * variable(s) are known bound on exit, then those should be
                     * annotated as join variables for the sub-select. I've
                     * looked into this a few times, but have not yet wrestled
                     * it to the ground. It would probably make a big difference
                     * for some queries.
                     * 
                     * @see <a
                     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/534#comment:2">
                     *      BSBM BI Q5 Error when using MERGE JOIN </a>
                     */
                    
                    break;
                    
                }

                requiredIncludes.add(nsi);

                // Combine the attached join filters (if any).
                joinConstraints.addAll(nsi.getAttachedJoinFilters());

            } else {

                // End of the INCLUDEs.
                break;

            }

        }

        final int minIncludes = 1;
        if (requiredIncludes.size() < minIncludes
                && optionalIncludes.size() < minIncludes) {
            /*
             * The primary source will be the [firstInclude] identified above.
             * In addition, we need at least one other INCLUDE which satisify
             * the criteria to do a MERGE JOIN. If we do not have that then we
             * can not do a MERGE JOIN.
             */
            mergeJoin = false;
        }

        if (!mergeJoin) {
            
            return left;
            
        }

        /*
         * We will do a merge join.
         */

        // Figure out if the join is required or optional.
        final boolean optional = requiredIncludes.isEmpty();
        final JoinTypeEnum joinType = optional ? JoinTypeEnum.Optional
                : JoinTypeEnum.Normal;
        
        // The list of includes that we will process.
        final List<NamedSubqueryInclude> includes = optional ? optionalIncludes
                : requiredIncludes;
        
        // The join variables as an IVariable[].
        final IVariable<?>[] joinvars2 = joinVars
                .toArray(new IVariable[joinVars.size()]);
        
        // The hash index for the first source.
        final INamedSolutionSetRef firstNamedSolutionSetRef = NamedSolutionSetRefUtility
                .newInstance(ctx.queryId, firstInclude.getName(), joinvars2);

        if (!firstInclude.getJoinVarSet().equals(joinVars)) {
            
            /*
             * If the primary source does not share the same join variables,
             * then we need to build a hash index on the join variables which
             * are used by the other sources.
             */
            
            left = addNamedSubqueryInclude(left, firstInclude, doneSet, ctx);
            
            final IHashJoinUtilityFactory joinUtilFactory;
            if (ctx.nativeHashJoins) {
                joinUtilFactory = HTreeHashJoinUtility.factory;
            } else {
                joinUtilFactory = JVMHashJoinUtility.factory;
            }
            
            left = applyQueryHints(new HashIndexOp(leftOrEmpty(left),//
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                new NV(PipelineOp.Annotations.LAST_PASS, true),// required
                new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
    //                new NV(HashIndexOp.Annotations.OPTIONAL, optional),//
                new NV(HashIndexOp.Annotations.JOIN_TYPE, joinType),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, joinvars2),//
    //                new NV(HashIndexOp.Annotations.SELECT, selectVars),//
                new NV(HashIndexOp.Annotations.HASH_JOIN_UTILITY_FACTORY,
                        joinUtilFactory),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, firstNamedSolutionSetRef),//
                new NV(IPredicate.Annotations.RELATION_NAME, 
                      new String[]{ctx.getLexiconNamespace()})
            ), joinGroup, ctx);
        }
        
        /*
         * Setup the sources (one per INCLUDE).
         */
        final INamedSolutionSetRef[] namedSolutionSetRefs;
        {
            
            final List<INamedSolutionSetRef> list = new LinkedList<INamedSolutionSetRef>();

            list.add(firstNamedSolutionSetRef);

            for (NamedSubqueryInclude nsi : includes) {

                list.add(NamedSolutionSetRefUtility.newInstance(
                        ctx.queryId, nsi.getName(), joinvars2));

            }
            
            namedSolutionSetRefs = list
                    .toArray(new INamedSolutionSetRef[list.size()]);

        }

        /*
         * TODO Figure out if we can release the named solution sets.
         * 
         * Note: We are very likely to be able to release the named
         * solution sets after the merge join, but it is not guaranteed
         * that none of the source solution sets is being INCLUDEd more
         * than once.
         */
        boolean release = false;

        /*
         * FIXME This needs to be an IConstraint[][] which causes the join
         * constraints to be attached to the same joins that they were attached
         * to in the original query plan.  I need to change the code in the
         * merge join operators first.  Right now, the merge join does not
         * properly handle join constraints.
         */

        final List<IConstraint> constraints = new LinkedList<IConstraint>();

        // convert constraints to join constraints (BLZG-1648).
        for (FilterNode filter : joinConstraints) {
        
            constraints
                    .add(new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                            filter.getValueExpression()));
            
        }
        final IConstraint[] c = constraints.toArray(new IConstraint[0]);
        
        /*
         * FIXME Update the doneSet *after* the merge join based on the doneSet
         * for each INCLUDE which is folded into the merge join.
         */
        if (ctx.nativeHashJoins) {
            
            left = applyQueryHints(new HTreeMergeJoin(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),// required?
                    new NV(PipelineOp.Annotations.LAST_PASS, true),// required?
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                    new NV(HTreeMergeJoin.Annotations.NAMED_SET_REF,
                            namedSolutionSetRefs),//
//                    new NV(HTreeMergeJoin.Annotations.JOIN_TYPE, joinType),//
                    new NV(HTreeMergeJoin.Annotations.CONSTRAINTS, c),//
                    new NV(HTreeMergeJoin.Annotations.RELEASE,
                                release)//
            ), joinGroup, ctx);

        } else {
            
            left = applyQueryHints(new JVMMergeJoin(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),// required?
                    new NV(PipelineOp.Annotations.LAST_PASS, true),// required?
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                    new NV(JVMMergeJoin.Annotations.NAMED_SET_REF,
                            namedSolutionSetRefs),//
//                    new NV(JVMMergeJoin.Annotations.JOIN_TYPE, joinType),//
                    new NV(JVMMergeJoin.Annotations.CONSTRAINTS, c),//
                    new NV(JVMMergeJoin.Annotations.RELEASE,
                            release)//
            ), joinGroup, ctx);

        }

        // Advance beyond the last consumed INCLUDE.
        start.set(j);

        return left;

    }
    
    /**
     * Conditionally add a {@link StartOp} iff the query will rin on a cluster.
     * 
     * @param queryBase
     *            The {@link QueryBase} for which a {@link StartOp} might be
     *            required.
     * @param ctx
     * 
     * @return The {@link StartOp} iff this query will run on a cluster and
     *         otherwise <code>null</code>.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/478">
     *      Cluster does not map input solution(s) across shards</a>
     */
    private static final PipelineOp addStartOpOnCluster(
            final QueryBase queryBase, final AST2BOpContext ctx) {

		if (ctx.isCluster()) {
		
			/*
			 * Note: This is necessary if the first operator in the query plan
			 * is a sharded join and we want it to run using a sharded index
			 * view. Without this, the operator will actually run against the
			 * global index view.
			 * 
			 * Note: There may be other ways to "fix" this.  See the ticket for
			 * more information.
			 * 
			 * @see https://sourceforge.net/apps/trac/bigdata/ticket/478
			 */
			
			return addStartOp(queryBase, ctx);
			
		}

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
    private static final PipelineOp addStartOp(final QueryBase queryBase,
            final AST2BOpContext ctx) {

        final PipelineOp start = applyQueryHints(
                new StartOp(BOp.NOARGS, NV.asMap(new NV[] {//
                                new NV(Predicate.Annotations.BOP_ID, ctx
                                        .nextId()),
                                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                        BOpEvaluationContext.CONTROLLER), })),
                queryBase, ctx);

        return start;

    }

    /**
     * Add an assignment to the query plan. Note that this method does not
     * resolve the IV of the bound variable (which might be bound to a mocked
     * IV) against the dictionary. Actually, this can be considered a simple,
     * performance optimized version to translate assignment nodes that can
     * be used whenever resolving of the IV is not necessary (e.g., because
     * the IV is filtered away or directly passed to the result).
     * 
     * See addAndResolveAssignment method for an alternative that also resolves
     * constructed (mocked) IVs against the dictionary.
     * 
     * @param left
     * @param assignmentNode
     *            The {@link AssignmentNode} (LET() or BIND()).
     * @param doneSet
     * @param queryHints
     *            The query hints from the AST node that dominates the
     *            assignment and from which we will take any query hints. E.g.,
     *            a PROJECTION or a JOIN GROUP.
     * @param ctx
     * @param projection
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final PipelineOp addAssignment(PipelineOp left,
//            final ASTBase dominatingASTNode,//
            final AssignmentNode assignmentNode,//
            final Set<IVariable<?>> doneSet, //
            final Properties queryHints,//
            final AST2BOpContext ctx,//
            final boolean projection) {

        final IValueExpression ve = assignmentNode.getValueExpression();

        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        /*
         * Get the vars this filter needs materialized.
         */
        final ComputedMaterializationRequirement req = assignmentNode
                .getMaterializationRequirement();

        vars.addAll(req.getVarsToMaterialize());

        /*
         * Remove the ones we've already done.
         */
        vars.removeAll(doneSet);

        final int bopId = ctx.nextId();

        final ConditionalBind b = new ConditionalBind(
                assignmentNode.getVar(),
                assignmentNode.getValueExpression(), projection);

        IConstraint c = new ProjectedConstraint(b);

        /*
         * We might have already materialized everything we need for this
         * filter.
         */
        if (vars.size() > 0) {

            left = addMaterializationSteps1(left, bopId, ve, vars,
                    queryHints, ctx);

            if(req.getRequirement()==Requirement.ALWAYS) {

                /*
                 * Add all the newly materialized variables to the set we've
                 * already done.
                 */

                doneSet.addAll(vars);
                
            }

            c = new TryBeforeMaterializationConstraint(c);

        }

        left = applyQueryHints(//
                new ConditionalRoutingOp(leftOrEmpty(left), //
                new NV(BOp.Annotations.BOP_ID, bopId), //
                new NV(ConditionalRoutingOp.Annotations.CONDITION, c)//
            ), queryHints, ctx);

        return left;

    }

    /**
     * Add an assignment to the query plan and resolve the IV of the bound
     * variable (which might be bound to a mocked IV) against the dictionary. 
     * Using this method for translating assignments. This might come with a
     * performance overhead, since a dictionary join is involved. However,
     * using this method is always safe in the sense that the variable that
     * is bound may be used in subsequent joins.
     * 
     * See addAssignment for a performance optimized method that does not
     * resolve constructed (mocked) IVs against the dictionary.
     * 
     * @param left
     * @param assignmentNode
     *            The {@link AssignmentNode} (LET() or BIND()).
     * @param doneSet variable set containing 
     * @param queryHints
     *            The query hints from the AST node that dominates the
     *            assignment and from which we will take any query hints. E.g.,
     *            a PROJECTION or a JOIN GROUP.
     * @param ctx The evaluation context.
     * 
     * @return the resulting PipelineOp
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })    
    private static PipelineOp addResolvedAssignment(PipelineOp left,
        final AssignmentNode assignmentNode,//
        final Set<IVariable<?>> doneSet, //
        final Properties queryHints,//
        final AST2BOpContext ctx) {
       
       final IValueExpression ve = assignmentNode.getValueExpression();

       final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

       /*
        * Get the vars this filter needs materialized.
        */
       final ComputedMaterializationRequirement req = assignmentNode
               .getMaterializationRequirement();

       vars.addAll(req.getVarsToMaterialize());

       /*
        * Remove the ones we've already done.
        */
       vars.removeAll(doneSet);

       final int bopId = ctx.nextId();

       /*
        * Construct an IConstraint operating over a fresh variable, to avoid
        * conflicts with existing (already bounb variables): this is necessary,
        * because the values constructed in the assignment node might be mocked
        * values, which cannot be joined to existing values prior to being
        * resolved against the dictionary. This join will be built on top in
        * a later step, see comment below.
        */
       final Var freshVar = Var.var();
       final ConditionalBind b = new ConditionalBind(
             freshVar,
             assignmentNode.getValueExpression(), false);

       final IConstraint c = new ProjectedConstraint(b);

       /*
        * We might have already materialized everything we need for this
        * filter.
        */
       if (vars.size() > 0) {

           left = addMaterializationSteps1(left, bopId, ve, vars,
                   queryHints, ctx);

           if(req.getRequirement()==Requirement.ALWAYS) {

               /*
                * Add all the newly materialized variables to the set we've
                * already done.
                */

               doneSet.addAll(vars);
               
           }

       }

       /**
        * The pipeline we construct in the following works as follows, from
        * bottom to top:
        * 
        * A. Innermost is a conditional routing op that processes the condition
        *   and computes the bound values. We feed in the ConditionalBind
        *   expression c, which operates over freshVar and thus does not
        *   conflict with any mappings in the mapping set.
        *   
        * B. On top, we place a MockTermResolverOp, which batch joins the
        *    values for freshVar against the dictionary.
        *   
        * C. The resolved values for freshVar now coexist with potential
        *    prior mappings for the assignment node variable, so we use the
        *    VariableUnificationOp to calculate kind of an implicit join over
        *    the mapping sets (thereby collating freshVar with the assignment
        *    node variable again).
        */
       
       // operator A.
       left = applyQueryHints(//
               new ConditionalRoutingOp(leftOrEmpty(left), //
               new NV(BOp.Annotations.BOP_ID, bopId), //
               new NV(ConditionalRoutingOp.Annotations.CONDITION, c)//
           ), queryHints, ctx);

       // operator B.
       final Set<IVariable<IV>> iVars = new LinkedHashSet<IVariable<IV>>();
       iVars.add(freshVar);
       left = addMockTermResolverOp(
             left,//
             iVars,//
             ChunkedMaterializationOp.Annotations.DEFAULT_MATERIALIZE_INLINE_IVS,
             null,//
             assignmentNode.getQueryHints(),//
             ctx//
             ); 
       
       // operator C.
       left = addVariableUnificationOp(
                left, assignmentNode.getVar(), freshVar, ctx);
       
       return left;       

   }

   /**
     * Add a FILTER which will not be attached to a required join to the
     * pipeline.
     * 
     * @param left
     * @param joinGroup
     *            The parent join group.
     * @param filter
     *            The filter.
     * @param doneSet
     *            The set of variables which are already known to be
     *            materialized.
     * @param ctx
     * @return
     */
    @SuppressWarnings("rawtypes")
    private static final PipelineOp addConditional(//
            PipelineOp left,//
            final JoinGroupNode joinGroup,//
            final FilterNode filter,//
            final Set<IVariable<?>> doneSet, //
            final AST2BOpContext ctx) {

        @SuppressWarnings("unchecked")
        final IValueExpression<IV> ve = (IValueExpression<IV>) filter
                .getValueExpression();

        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        /*
         * Get the variables that this filter needs materialized.
         */
        final ComputedMaterializationRequirement req = filter
                .getMaterializationRequirement();

        vars.addAll(req.getVarsToMaterialize());

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
            left = addMaterializationSteps1(left, bopId, ve, vars,
                    joinGroup.getQueryHints(), ctx);

            if (req.getRequirement() == Requirement.ALWAYS) {
                
                /*
                 * Add all the newly materialized variables to the set we've
                 * already done.
                 */

                doneSet.addAll(vars);
                
            }

        }

        final IConstraint c = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                ve);

        left = applyQueryHints(new ConditionalRoutingOp(leftOrEmpty(left),//
                new NV(BOp.Annotations.BOP_ID, bopId), //
                new NV(ConditionalRoutingOp.Annotations.CONDITION, c)//
                ), joinGroup, ctx);

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
     *      <p>
     *      See {@link JoinGroupNode#getInFilters()} for how and where this is
     *      currently disabled.
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
                new NV(DataSetJoin.Annotations.BOP_ID, ctx.nextId()),//
                new NV(DataSetJoin.Annotations.GRAPHS, ivs) //
                }));

        return left;

    }
    
    /**
     * This method handles sub-groups, including UNION, required sub-groups,
     * OPTIONAL subgroups, and MINUS.
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
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/397
     */
	private static PipelineOp addSubgroup(//
			PipelineOp left,//
			final GraphPatternGroup<IGroupMemberNode> subgroup,//
			final Set<IVariable<?>> doneSet,
			final AST2BOpContext ctx//
			) {

	   final boolean usePipelinedHashJoin = usePipelinedHashJoin(ctx, subgroup) ;
	   
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
        
        // true iff the sub-group is MINUS
        final boolean minus = subgroup instanceof JoinGroupNode
                && ((JoinGroupNode) subgroup).isMinus();
        
        // The type of join.
        final JoinTypeEnum joinType = optional ? JoinTypeEnum.Optional
                : minus ? JoinTypeEnum.NotExists : JoinTypeEnum.Normal;

        @SuppressWarnings("rawtypes")
        final Map<IConstraint, Set<IVariable<IV>>> needsMaterialization = 
           new LinkedHashMap<IConstraint, Set<IVariable<IV>>>();

        final IConstraint[] joinConstraints = getJoinConstraints(
                getJoinConstraints(subgroup), needsMaterialization);

        if (!joinType.isNormal() && joinConstraints != null) {
            /*
             * Note: Join filters should only be attached to required joins, not
             * to OPTIONAL joins (except for a simple optional statement pattern
             * node) or OPTIONAL join groups.
             */
            throw new AssertionError(
                    "Non-required group has attached join filters: " + subgroup);
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
        
//        if (joinVars.length == 0) {
//
//            /*
//             * Note: If there are no join variables then the join will examine
//             * the full N x M cross product of solutions. That is very
//             * inefficient, so we are logging a warning.
//             */
//
//            log.warn("No join variables: " + subgroup);
//
//        }
                
        final INamedSolutionSetRef namedSolutionSet = 
            NamedSolutionSetRefUtility.newInstance(
                usePipelinedHashJoin ? null : ctx.queryId, solutionSetName, joinVars);

        final IVariable<?>[] projectInVars = subgroup.getProjectInVars();
           

        PipelineOp subqueryPlan = null;
        if (usePipelinedHashJoin) {
           /**
            * Subquery execution is controlled by the pipelined hash join
            * itself, so for the pipelined variant we translate the subplan
            * as a standalone pipeline op and attach it to the annotation.
            */
           subqueryPlan = 
              convertJoinGroupOrUnion(null /* standalone */, subgroup, doneSet, ctx);
           // inherit the namespace property (which is needed for the CPU/GPU),
           // see https://github.com/SYSTAP/bigdata-gpu/issues/343           
           subqueryPlan = (PipelineOp) subqueryPlan.setProperty(BOp.Annotations.NAMESPACE, ctx.getNamespace());
          
        } 
        
        left = addHashIndexOp(left, usePipelinedHashJoin, ctx, subgroup, 
              joinType, joinVars, joinConstraints, projectInVars, 
              namedSolutionSet, null /* bindingsSetSource */,
              null /* askVar */, subqueryPlan);

        // in case the subquery has not been inlined, append it to the pipeline
        if (!usePipelinedHashJoin) {
           left = convertJoinGroupOrUnion(left, subgroup, doneSet, ctx);
        }
        
        // lastPass unless this is a normal join.
        final boolean lastPass = !joinType.isNormal();
        
        // true if we will release the HTree as soon as the join is done.
        // Note: also requires lastPass.
        final boolean release = lastPass && true;

        // join can be pipelined unless last pass evaluation is required
        final int maxParallel = lastPass ? 1
                : ctx.maxParallelForSolutionSetHashJoin;

        if (!usePipelinedHashJoin) {

            if(ctx.nativeHashJoins) {
                left = applyQueryHints(new HTreeSolutionSetHashJoinOp(
                    new BOp[] { left },//
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, maxParallel),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                    new NV(HTreeSolutionSetHashJoinOp.Annotations.SELECT, selectVars),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                    new NV(HTreeSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
                    ), subgroup, ctx);
            } else {
               
               left = applyQueryHints(new JVMSolutionSetHashJoinOp(
                  new BOp[] { left },
                  new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                  new NV(BOp.Annotations.EVALUATION_CONTEXT,
                          BOpEvaluationContext.CONTROLLER),//
                  new NV(PipelineOp.Annotations.MAX_PARALLEL, maxParallel),//
                  new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
//                    new NV(JVMSolutionSetHashJoinOp.Annotations.OPTIONAL, optional),//
//                    new NV(JVMSolutionSetHashJoinOp.Annotations.JOIN_VARS, joinVars),//
//                    new NV(JVMSolutionSetHashJoinOp.Annotations.SELECT, selectVars),//
                  new NV(JVMSolutionSetHashJoinOp.Annotations.CONSTRAINTS, joinConstraints),//
                  new NV(JVMSolutionSetHashJoinOp.Annotations.RELEASE, release),//
                  new NV(JVMSolutionSetHashJoinOp.Annotations.LAST_PASS, lastPass),//
                  new NV(JVMSolutionSetHashJoinOp.Annotations.NAMED_SET_REF, namedSolutionSet)//                  
                  ), subgroup, ctx);
            } 
            
        } // else: hash join is built-in, nothing to do here

        // BLZG-1899: for analytic hash joins, we don't cache materialized values
        //            -> already variables that have not been projected into the subgroup
        //               need to be marked as not done, in order to enforce re-materialization
        //               where needed in later steps of the query plan
        if (ctx.nativeHashJoins) {
            
            if (joinType.equals(JoinTypeEnum.Normal)) {
            
                final Set<IVariable<?>> nonProjectInVariables = 
                        ctx.sa.getMaybeIncomingBindings(subgroup, new LinkedHashSet<IVariable<?>>());
                
                for (int i=0; i< projectInVars.length; i++) {
                    nonProjectInVariables.remove(projectInVars[i]);
        	    }
    
                doneSet.removeAll(nonProjectInVariables);        
                
            } else {
                
                // for non normal joins (such as OPTIONALs) we don't have any
                // materialization guarantees; non-join solutions for OPTIONALs,
                // for instance are taken from the hash index, which does not
                // cache the materialized values (see BLZG-1899)
                doneSet.clear();
            }
        }
        
        /*
         * For each filter which requires materialization steps, add the
         * materializations steps to the pipeline and then add the filter to the
         * pipeline.
         */
        left = addMaterializationSteps3(left, doneSet, needsMaterialization,
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
     * @param preserveOrder
     *            When <code>true</code> the solution set order must be
     *            preserved by the DISTINCT operator. This is necessary when
     *            both ORDER BY and DISTINCT are specified in a query.
     * 
     * TODO Support parallel decomposition of distinct on a cluster (DISTINCT
     * can be run on each node if we hash partition the DISTINCT operator based
     * on the variables on which DISTINCT will be imposed and the results when
     * streamed back to the controller will still be distinct.)
     */
    private static final PipelineOp addDistinct(PipelineOp left,
            final QueryBase query, final boolean preserveOrder,
            final AST2BOpContext ctx) {

        final int bopId = ctx.nextId();

        final ProjectionNode projection = query.getProjection();

        if (projection.isWildcard())
            throw new AssertionError("Wildcard projection was not rewritten.");

        final IVariable<?>[] vars = projection.getProjectionVars();

        final PipelineOp op;
        if (!ctx.nativeDistinctSolutions || preserveOrder) {
            /*
             * DISTINCT on the JVM heap.
             */
            final List<NV> anns = new LinkedList<NV>();
            anns.add(new NV(JVMDistinctBindingSetsOp.Annotations.BOP_ID, bopId));
            anns.add(new NV(JVMDistinctBindingSetsOp.Annotations.VARIABLES,
                    vars));
            anns.add(new NV(
                    JVMDistinctBindingSetsOp.Annotations.EVALUATION_CONTEXT,
                    BOpEvaluationContext.CONTROLLER));
            anns.add(new NV(JVMDistinctBindingSetsOp.Annotations.SHARED_STATE,
                    true));
            if (preserveOrder) {
                /*
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/563 (ORDER
                 * BY + DISTINCT)
                 */
                anns.add(new NV(PipelineOp.Annotations.MAX_PARALLEL, 1));
                anns.add(new NV(SliceOp.Annotations.REORDER_SOLUTIONS, false));
            }
            op = new JVMDistinctBindingSetsOp(leftOrEmpty(left),//
                    anns.toArray(new NV[anns.size()])//
            );
        } else {
            /*
             * DISTINCT on the native heap.
             */
            if(preserveOrder) {
                /*
                 * TODO The HTree DISTINCT operator is not preserving the input
                 * order. This is probably due to vectoring. This limits the
                 * scalablity of DISTINCT + ORDER BY since the HTree is not
                 * being used.
                 * 
                 * Note: It is possible to implement a DISTINCT operator which
                 * relies on the fact that the solutions are already ordered IFF
                 * the ordering is consistent with the projected variables such
                 * that we can compare the last solution output with the current
                 * solution and decide (without maintaining a hash index)
                 * whether the current solution has distinct bindings for the
                 * projected variables. However, this operator would not handle
                 * all cases of DISTINCT + ORDER BY for analytic query. In order
                 * to have a scalable solution which covers all cases, we would
                 * have to ensure that the HTree DISTINCT + ORDER BY operator
                 * DID NOT reorder solutions. That probably means that we would
                 * have to turn off vectoring in the HTree DISTINCT + ORDER BY
                 * operator.
                 */
                throw new UnsupportedOperationException();
            }
            final INamedSolutionSetRef namedSolutionSet = NamedSolutionSetRefUtility.newInstance(
                    ctx.queryId, "--distinct-"+ctx.nextId(), vars);
            op = new HTreeDistinctBindingSetsOp(leftOrEmpty(left),//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.BOP_ID,
                           bopId),//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.VARIABLES,
                           vars),//
                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
                           BOpEvaluationContext.CONTROLLER),//
                    new NV(HTreeDistinctBindingSetsOp.Annotations.NAMED_SET_REF,
                           namedSolutionSet),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),// for live stat updates.
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(IPredicate.Annotations.RELATION_NAME, 
                            new String[]{ctx.getLexiconNamespace()})            );
        }

        // Note: applies query hints to JVM or HTree based DISTINCT.
        left = applyQueryHints(op, query, ctx);

        return left;

    }

    /**
     * Add an aggregation operator. It will handle the <code>GROUP BY</code> (if
     * any), the optional <code>HAVING</code> filter, and projected
     * <code>SELECT</code> expressions. A generalized aggregation operator will
     * be used unless the aggregation corresponds to some special case, e.g.,
     * pipelined aggregation.
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

        // The query hints are taken from the PROJECTION.
        final Properties queryHints = projection.getQueryHints();
        
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

        left = addMaterializationSteps2(left, bopId, vars, queryHints, ctx);

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

        left = applyQueryHints(op, queryHints, ctx);

        return left;

    }

    /**
     * Add an ORDER BY operator.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final PipelineOp addOrderBy(PipelineOp left,
            final QueryBase queryBase, final OrderByNode orderBy,
            final AST2BOpContext ctx) {

        // The query hints are taken from the QueryBase
        final Properties queryHints = queryBase.getQueryHints();

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

                expr = new Bind(Var.var("--anon" + ctx.nextId()), expr);

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

        left = addMaterializationSteps2(left, sortId, vars, queryHints, ctx);

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
                                new NV(MemorySortOp.Annotations.REORDER_SOLUTIONS, false),//
//                                new NV(MemorySortOp.Annotations.SHARED_STATE,
//                                        true),//
                                new NV(MemorySortOp.Annotations.LAST_PASS, true),//
                        })), queryHints, ctx);

        return left;

    }

    /**
     * Impose an OFFSET and/or LIMIT on a query.
     */
    private static final PipelineOp addSlice(PipelineOp left,
            final QueryBase queryBase, final SliceNode slice,
            final AST2BOpContext ctx) {

        final int bopId = ctx.nextId();

        left = applyQueryHints(new SliceOp(leftOrEmpty(left),//
                new NV(SliceOp.Annotations.BOP_ID, bopId),//
                new NV(SliceOp.Annotations.OFFSET, slice.getOffset()),//
                new NV(SliceOp.Annotations.LIMIT, slice.getLimit()),//
                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                        BOpEvaluationContext.CONTROLLER),//
                new NV(SliceOp.Annotations.PIPELINED, true),//
                new NV(SliceOp.Annotations.MAX_PARALLEL, 1),//
                new NV(SliceOp.Annotations.REORDER_SOLUTIONS,false),//
                new NV(SliceOp.Annotations.SHARED_STATE, true)//
                ), queryBase, ctx);

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
    protected static final Predicate toPredicate(final StatementPatternNode sp,
            final AST2BOpContext ctx) {

        final QueryRoot query = ctx.astContainer.getOptimizedAST();
        
        final AbstractTripleStore database = ctx.getAbstractTripleStore();

        final DatasetNode dataset = query.getDataset();

//        final Value predValue = sp.p().getValue();
//        if (log.isDebugEnabled()) {
//            log.debug(predValue);
//        }

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
        anns.add(new NV(IPredicate.Annotations.BOP_ID, ctx.nextId()));

        // Propagate the estimated cardinality to the Predicate.
        anns.add(new NV(Annotations.ESTIMATED_CARDINALITY,
                sp.getProperty(Annotations.ESTIMATED_CARDINALITY)));

        // Propagate the index which would be used if we do not run the
        // predicate "as-bound".
        anns.add(new NV(Annotations.ORIGINAL_INDEX,
                sp.getProperty(Annotations.ORIGINAL_INDEX)));

        /*
         * BufferAnnotations are used by all PipelineOps to control their
         * vectoring. However, some BufferAnnotations are *also* used by the
         * AccessPath associated with a Predicate. We now copy those annotations
         * from the StatementPatternNode onto the Predicate. See
         * AccessPath<init>() for the set of annotations that it pulls from the
         * Predicate asssociated with the AccessPath.
         * 
         * Note: These annotations are *also* present on the PipelineOp
         * generated from the StatementPatternNode. On the Predicate, they
         * control the behavior of the AccessPath. On the PipelineOp, e.g.,
         * PipelineJoin, the control the vectoring of the pipeline operator.
         */
        {

            final Properties queryHints = sp.getQueryHints();
            
            conditionalCopy(anns, queryHints, BufferAnnotations.CHUNK_CAPACITY);
            
            conditionalCopy(anns, queryHints,
                    BufferAnnotations.CHUNK_OF_CHUNKS_CAPACITY);
            
            conditionalCopy(anns, queryHints,
                    IPredicate.Annotations.FULLY_BUFFERED_READ_THRESHOLD);

            conditionalCopy(anns, queryHints, IPredicate.Annotations.KEY_ORDER);

            // Note: moved up from below and modified to use conditionalCopy().
            conditionalCopy(anns, queryHints,
                    IPredicate.Annotations.CUTOFF_LIMIT);
            
        }

//        {
//            // Propagate the optional query hint override for the index to use.
//            final Object tmp = sp.getQueryHint(IPredicate.Annotations.KEY_ORDER);
//            if (tmp != null) {
//                anns.add(new NV(IPredicate.Annotations.KEY_ORDER, tmp));
//            }
//        }

        if (sp.isOptional()) {
            // Mark the join as optional.
            anns.add(new NV(IPredicate.Annotations.OPTIONAL, Boolean.TRUE));
        }

		if (sp.getProperty(StatementPatternNode.Annotations.DISTINCT_TERM_SCAN_VAR) != null) {
			// propagate annotation for distinct-term-scan. see #1035
			anns.add(new NV(
					StatementPatternNode.Annotations.DISTINCT_TERM_SCAN_VAR,
					sp.getProperty(StatementPatternNode.Annotations.DISTINCT_TERM_SCAN_VAR)));
		}
        
		if (sp.getProperty(StatementPatternNode.Annotations.FAST_RANGE_COUNT_VAR) != null) {
			// propagate annotation for fast-range-count. see #1037
			anns.add(new NV(
					StatementPatternNode.Annotations.FAST_RANGE_COUNT_VAR,
					sp.getProperty(StatementPatternNode.Annotations.FAST_RANGE_COUNT_VAR)));
		}
        
        /*
		 * Statements about statements.
		 * 
		 * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/526">
		 * Reification Done Right</a>
		 */
		final VarNode sidVar = sp.sid();
		if (QueryHints.DEFAULT_REIFICATION_DONE_RIGHT && sidVar != null) {

			anns.add(new NV(SPOPredicate.Annotations.SID, sidVar
					.getValueExpression()));

		}
        
        final RangeNode range = sp.getRange();
        if (range != null) {
            // Add the RangeBOp
            anns.add(new NV(IPredicate.Annotations.RANGE, range.getRangeBOp()));
        }

//        Note: Move above and modified to use conditionalCopy.
//        
//        final String cutoffLimit = sp.getQueryHint(QueryHints.CUTOFF_LIMIT);
//        if (cutoffLimit != null) {
//            // Add the cutoff limit
//            anns.add(new NV(IPredicate.Annotations.CUTOFF_LIMIT, Long
//                    .valueOf(cutoffLimit)));
//        }

        if ( sp.getProperty(GpuAnnotations.EVALUATE_ON_GPU) != null) {
           // propagate annotation for GPU acceleration
           anns.add( new NV(GpuAnnotations.EVALUATE_ON_GPU,
                            sp.getProperty(GpuAnnotations.EVALUATE_ON_GPU)) );
        }

        final Properties queryHints = sp.getQueryHints();
        
        if (queryHints != null
                && Boolean.parseBoolean(queryHints.getProperty(
                        QueryHints.HASH_JOIN, "false"))) {

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

                anns.add(new NV(QueryHints.HASH_JOIN, true));

                anns.add(new NV(HashJoinAnnotations.JOIN_VARS, joinVars
                        .toArray(new IVariable[joinVars.size()])));

            }
            
        }
        
        if (queryHints != null && Boolean.parseBoolean(queryHints.getProperty(
                QueryHints.HISTORY, "false"))) {
            
            anns.add(new NV(SPOPredicate.Annotations.INCLUDE_HISTORY, true));
            
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
            anns.add(new NV(AST2BOpJoins.Annotations.QUADS, true));
            // attach the Scope.
            anns.add(new NV(AST2BOpJoins.Annotations.SCOPE, sp.getScope()));

            /*
             * Note: For a default graph access path, [cvar] can not become
             * bound since the context is stripped from the visited ISPOs.
             */
            if (dataset != null) {
                // attach the DataSet.
                anns.add(new NV(AST2BOpJoins.Annotations.DATASET, dataset));
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
		 * TODO Handle StatementPatternNode#EXISTS here in support of GRAPH uri
		 * {}, which is an existence test for a non-empty context. (This turns
		 * into an iterator with a limit of ONE (1) on the {@link
		 * SPOAccessPath}. This should turn into a LIMIT annotation on the
		 * access path (which does not currently recognize an offset/limit
		 * annotation; instead you use an alternative iterator call).
		 * 
		 * We may require an inline access path attached to a predicate in order
		 * to support GRAPH ?g {}, which is basically an inline AP for the
		 * dataset. That should probably be another annotation on the
		 * StatementPatternNode, perhaps COLUMN_PROJECTION (var,vals) or
		 * IN(var,vals)? There are also several StatementPatternNode annotation
		 * concepts which have been sketched out for DISTINCT, EXISTS, IN, and
		 * RANGE. Those annotations could be used to do the right thing in
		 * toPredicate().
		 * 
		 * Remove the "knownIN" stuff contributed by Matt.
		 * 
		 * @see ASTGraphGroupOptimizer
		 * 
		 * @see TestNamedGraphs
		 * 
		 * @see Inline AP + filter if AP is not perfect issues.
		 * 
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/429
		 * (Optimization for GRAPH uri {} and GRAPH ?foo {})
		 * 
		 * TODO Can this be reworked into physical operators similar to how we
		 * are now handling fast-range-count (#1037) and distinct-term scan
		 * (#1037)?
		 */
        {

            final List<IFilter> filters = new LinkedList<IFilter>();

            if (Boolean.valueOf(sp.getProperty(
                    StatementPatternNode.Annotations.DISTINCT,
                    StatementPatternNode.Annotations.DEFAULT_DISTINCT))) {

                /*
                 * Visit only the distinct values for the first key component.
                 * 
                 * TODO  We MUST pin the choice of the index for this filter. In
                 * order to do that we need to interact with the join ordering
                 * since we typically want to run this first in a join group.
                 * 
                 * TODO GRAPH ?g {} can use this to visit the distinct named
                 * graphs for which there exists a statement in the database.
                 * 
                 * TODO One exception where we can not use PARALLEL without
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
                vars = new BOp[] { s, p, o, Var.var("--anon-"+ctx.nextId()) };
            } else {
                vars = new BOp[] { s, p, o, c };
            }

            return new SPOPredicate(vars, anns.toArray(new NV[anns.size()]));

        }

    }

    /**
     * Conditionally copy a query hint, adding it to the caller's list.
     * 
     * @param anns
     *            The caller's list.
     * @param queryHints
     *            The query hints (optional).
     * @param name
     *            The name of a query hint to copy. It will be added to
     *            <code>anns</code> if a non-default value is found in the given
     *            <code>queryHints</code>.
     */
    private static void conditionalCopy(final List<NV> anns,
            final Properties queryHints, final String name) {
        
        if (queryHints == null)
            return;

        final Object val = queryHints.getProperty(name);

        if (val != null) {
        
            anns.add(new NV(name, val));
            
        }
        
    }
    
//    /**
//     * Convert an {@link IValueExpressionNode} (recursively) to an
//     * {@link IValueExpression}. If the {@link IValueExpression} can be reduced
//     * to an {@link IConstant}, then that {@link IConstant} will be be returned
//     * instead. Either way, the {@link IValueExpression} is set on the
//     * {@link IValueExpressionNode} as a side effect.
//     * 
//     * @param globals
//     *            The global annotations, including the lexicon namespace.
//     * @param node
//     *            The expression to convert.
//     * 
//     * @return The converted expression.
//     * 
//     * @see ASTSetValueExpressionsOptimizer
//     * 
//     * @see BLZG-1343 (MathBOp and DateTime abstraction)
//     * 
//     * @deprecated by the version that accepts the BOpContextBase. See
//     *             BLZG-1372:: Callers should be refactored to pass in the
//     *             BOpContextBase from {@link AST2BOpContext#context} in order
//     *             to allow correct resolution of the {@link LexiconRelation}
//     *             and {@link ILexiconConfiguration} required to properly
//     *             evaluate {@link IValueExpression}s.
//     */
//    @SuppressWarnings("rawtypes")
//    public static final IValueExpression<? extends IV> toVE(
//            final GlobalAnnotations globals,
//            final IValueExpressionNode node) {
//        return toVE(null/*context*/,globals,node);
//    }
    
    /**
     * Convert an {@link IValueExpressionNode} (recursively) to an
     * {@link IValueExpression}. If the {@link IValueExpression} can be reduced
     * to an {@link IConstant}, then that {@link IConstant} will be be returned
     * instead. Either way, the {@link IValueExpression} is set on the
     * {@link IValueExpressionNode} as a side effect.
     * 
     * @param context
     *            The {@link BOpContextBase} is required in order to resolve the
     *            {@link ILexiconConfiguration} and {@link LexiconRelation}.
     *            These are required for the evaluation of some
     *            {@link IValueExpression}s.
     * @param globals
     *            The global annotations, including the lexicon namespace.
     * @param node
     *            The expression to convert.
     * 
     * @return The converted expression.
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * 
     * @see ASTSetValueExpressionsOptimizer
     * 
     * @see BLZG-1372 toVE() was refactored to pass in the
     *      {@link BOpContextBase} to allow correct resolution of the
     *      {@link LexiconRelation} and {@link ILexiconConfiguration} in order
     *      to properly evaluate {@link IValueExpression}s during query
     *      optimization.
     */
    @SuppressWarnings("rawtypes")
    public static final IValueExpression<? extends IV> toVE(
            final BOpContextBase context,
    		final GlobalAnnotations globals,
            final IValueExpressionNode node) {

        if (context == null)
            throw new IllegalArgumentException();
        if (globals == null)
            throw new IllegalArgumentException();
        if (node == null)
            throw new IllegalArgumentException();

        // Convert AST value expr node => IValueExpressionNode.
        final IValueExpression<? extends IV> ve1 = toVE1(context, globals, node);

        // Reduce IValueExpressionNode to constant when possible.
        try {

            final IValueExpression<? extends IV> ve2 = toVE2(context, ve1);

            if (ve2 != ve1) {

                // The IValueExpression was evaluated to an IConstant.
                node.setValueExpression(ve2);

                return ve2;

            }

            return ve1;

//        } catch (ContextNotAvailableException ex) {
//            
//            /*
//             * The value expression could not be evaluated because it could not
//             * resolve the ILexiconConfiguration from the EmptyBindingSet.
//             * 
//             * TODO This is thrown during query optimization since the necessary
//             * context is not available at that point. That should be fixed, but
//             * it is a static method invocation so we would have to touch a lot
//             * of code. [Fixed in BLZG-1343].
//             */
//        
//            return ve1;

        } catch (SparqlTypeErrorException ex) {

            /*
             * The value expression could not be evaluated to a constant at this
             * time.
             */

            return ve1;

        }
        
    }

    /**
     * Attempt to evaluate complex {@link IValueExpression}s (those which are
     * not a simple {@link IVariable} or {@link IConstant}) against an empty
     * binding set. If we can evaluate the {@link IValueExpression}, then return
     * the result as an {@link IConstant}. Otherwise, return the original
     * {@link IValueExpression}.
     * 
     * @param ve
     *            The {@link IValueExpression}.
     * 
     * @return A new {@link IConstant} if a complex {@link IValueExpression} was
     *         an effective constant and otherwise the argument.
     * 
     * @throws SparqlTypeErrorException
     *             if the evaluation of the value expression at this time
     *             resulted in a SPARQL type error.
     */
    @SuppressWarnings("rawtypes")
    private static final IValueExpression<? extends IV> toVE2(
            final BOpContextBase context,//
            final IValueExpression<? extends IV> ve) {

        if (ve instanceof IVariableOrConstant) {

            /*
             * Variables and constants can not be further reduced.
             */

            return ve;
        }

        /*
         * Look for things that we can not resolve now.
         * 
         * Note: We need to scan the value expression recursively before we
         * evaluate it in order to catch things like aggregates, BOUND(), and
         * COALESCE() when nested inside of other value expressions.
         */
        {

            final Iterator<BOp> itr = BOpUtility.preOrderIterator((BOp) ve);

            while (itr.hasNext()) {

                final BOp op = itr.next();
                
                if (op instanceof IAggregate) {

                    /*
                     * Aggregates can not be evaluated at this time.
                     */

                    return ve;

                }

                if (op instanceof IsBoundBOp || op instanceof CoalesceBOp) {

                    /*
                     * These handle unbound variables with special semantics and
                     * can not be evaluated until we have an actual solution as
                     * they will not throw a SparqlTypeErrorException for an
                     * unbound variable.
                     */

                    return ve;

                }
                
                if (op instanceof UnknownFunctionBOp) {
                	
                	/*
                	 * We want to defer on unknown functions until execution
                	 * time (to allow simple parsing to succeed).
                	 */
                	
                	return ve;
                	
                }
                
                if (op instanceof UUIDBOp) {// || op instanceof NowBOp) {
                    
                    /*
                     * We cannot pre-generate these, they need to be unique
                     * for each call.
                     */
                    return ve;
                    
                }

            }

        }

        // BLZG-1343 (MathBOp and DateTime abstraction)
        final IValueExpression<? extends IV> ve2 = new Constant<IV>(
                ve.get(new ContextBindingSet(context, EmptyBindingSet.INSTANCE)));

//        System.err.println("ve=" + ve + " => " + ve2);

        return ve2;

    }

    /**
     * Convert an {@link IValueExpressionNode} (recursively) to an
     * {@link IValueExpression}. The {@link IValueExpression} is set on the
     * {@link IValueExpressionNode} as a side effect.
     * 
     * @param globals
     *            The global annotations, including the lexicon namespace.
     * @param node
     *            The expression to convert.
     * 
     * @return The converted expression.
     * 
     * @see ASTSetValueExpressionsOptimizer
     */
    @SuppressWarnings("rawtypes")
    private static final IValueExpression<? extends IV> toVE1(
            final BOpContextBase context,
    		final GlobalAnnotations globals,
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

            final IValueExpression<? extends IV> ve = toVE(context, globals, valueExpr);

            return ve;

        } else if (node instanceof FunctionNode) {

            final FunctionNode functionNode = (FunctionNode) node;

            final URI functionURI = functionNode.getFunctionURI();

            final Map<String, Object> scalarValues = functionNode.getScalarValues();

            final ValueExpressionNode[] args = functionNode.args().toArray(
                    new ValueExpressionNode[functionNode.arity()]);

            final IValueExpression<? extends IV> ve = FunctionRegistry.toVE(
                    context, globals, functionURI, scalarValues, args);

            functionNode.setValueExpression(ve);

            return ve;

        } else {

            throw new IllegalArgumentException(node.toString());

        }

    }

    
    
    /**
     * Use a pipeline operator which resolves mocked terms in the binding set
     * against the dictionary.
     * 
     * @param left
     *            The left (upstream) operator that immediately proceeds the
     *            materialization steps.
     * @param vars
     *            The variables pointing to mocked IVs to be resolved.
     * @param queryHints
     *            The query hints from the dominating AST node.
     * @param ctx
     *            The evaluation context.
     * 
     * @return The final bop added to the pipeline by this method. If there are
     *         no variables that require resolving, then this just returns
     *         <i>left</i>.
     * 
     * @see MockTermResolverOp
     */
    private static PipelineOp addMockTermResolverOp(//
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

        return (PipelineOp) applyQueryHints(new MockTermResolverOp(leftOrEmpty(left),
            new NV(MockTermResolverOp.Annotations.VARS, vars.toArray(new IVariable[nvars])),//
            new NV(MockTermResolverOp.Annotations.RELATION_NAME, new String[] { ns }), //
            new NV(MockTermResolverOp.Annotations.TIMESTAMP, timestamp), //
            new NV(PipelineOp.Annotations.SHARED_STATE, !ctx.isCluster()),// live stats, but not on the cluster.
            new NV(BOp.Annotations.BOP_ID, ctx.nextId())//
            ), queryHints, ctx);
    }
    
    /**
     * Constructs an operator that unifies variables in a mapping set, thereby
     * filtering mappings out for which the variables cannot be unified, see
     * {@link VariableUnificationOp} for detailed documentation.
     * 
     * @param left
     *            The left (upstream) operator that immediately proceeds the
     *            materialization steps.
     * @param targetVar
     *            the target variable for unification, which will remain bound
     *            in mappings passing the unification process
     * @param tmpVar
     *            the variable for unification that will be removed/renamed in
     *            mappings passing the unification process
     * @param ctx The evaluation context.
     * 
     * @return The final bop added to the pipeline by this method.
     * 
     * @see VariableUnificationOp
     */
    private static PipelineOp addVariableUnificationOp(//
          PipelineOp left,//
          final IVariable<IV> targetVar,
          final IVariable<IV> tmpVar,
          final AST2BOpContext ctx) {
       
       return new VariableUnificationOp(leftOrEmpty(left),
             new NV(VariableUnificationOp.Annotations.VARS, 
                      new IVariable[] { targetVar, tmpVar }),
             new NV(BOp.Annotations.BOP_ID, ctx.nextId()));
    }

    
    /**
     * 
     * @param left the left-side pipeline op
     * @param usePipelinedHashJoin whether or not to use the 
     *            {@link PipelinedHashIndexAndSolutionSetJoinOp} or not
     * @param ctx the evaluation context
     * @param node current query node
     * @param joinType type of the join
     * @param joinVars the variables on which the join is performed
     * @param joinConstraints the join constraints
     * @param projectInVars the variables to be projected into the right op;
     *            this parameter is optional, if set to null, the full result
     *            set will be returned by the hash index op
     * @param namedSolutionSet the named solution set
     * @param bindingSetsSource bindings sets to be consumed by the hash index
     *           (may be null, set as HashIndexOp.Annotations.BINDING_SETS_SOURCE)
     * @param an additional ASK var, to be bound for the EXISTS case indicating
     *        whether the returned result matched or not
     * @param subqueryPlan the subquery plan, to be inlined if
     *           usePipelinedHashJoin equals <code>true</code> only
     * 
     * @return the new pipeline op
     */
    private static PipelineOp addHashIndexOp(
        PipelineOp left,
        final boolean usePipelinedHashJoin,
        final AST2BOpContext ctx,
        final ASTBase node,
        final JoinTypeEnum joinType,
        final IVariable<?>[] joinVars,
        final IConstraint[] joinConstraints,
        final IVariable<?>[] projectInVars,
        final INamedSolutionSetRef namedSolutionSet,
        final IBindingSet[] bindingSetsSource,
        final IVariable<?> askVar,
        final PipelineOp subqueryPlan) {
      
       
       final Set<IVariable<?>> joinVarsSet = new HashSet<IVariable<?>>();
       if (joinVars!=null) {
          for (int i=0; i<joinVars.length; i++) {
             joinVarsSet.add(joinVars[i]);
          }
       }

       final Set<IVariable<?>> projectInVarsSet = new HashSet<IVariable<?>>();
       if (projectInVars!=null) {
          for (int i=0; i<projectInVars.length; i++) {
             projectInVarsSet.add(projectInVars[i]);
          }
       }
       
       /** 
        * If the set of variables we want to project in equals the join
        * variables, then we can inline the projection into thee hash index
        * operation, which provides an efficient way to calculate the
        * DISTINCT projection over the join variables. 
        */
       boolean inlineProjection = joinVarsSet.equals(projectInVarsSet);
       
       final IHashJoinUtilityFactory joinUtilFactory;
       if (ctx.nativeHashJoins) {
           if (usePipelinedHashJoin) {
               joinUtilFactory = HTreePipelinedHashJoinUtility.factory;               
           } else {
               joinUtilFactory = HTreeHashJoinUtility.factory;
           }
       } else {
          
          if (usePipelinedHashJoin) {
             joinUtilFactory = JVMPipelinedHashJoinUtility.factory;             
          } else {
             joinUtilFactory = JVMHashJoinUtility.factory;             
          }
       }
       
       if (usePipelinedHashJoin) {
          
          left = applyQueryHints(new PipelinedHashIndexAndSolutionSetJoinOp(leftOrEmpty(left),//
              new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
              new NV(BOp.Annotations.EVALUATION_CONTEXT,
                     BOpEvaluationContext.CONTROLLER),//
              new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),// required for lastPass
              new NV(PipelineOp.Annotations.LAST_PASS, true),// required
              new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
              new NV(HashIndexOp.Annotations.JOIN_TYPE, joinType),//
              new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
              new NV(HashIndexOp.Annotations.BINDING_SETS_SOURCE, bindingSetsSource),
              /**
               * For the pipelined hash index op, the projection is handled inline,
               * so we need to pass in this information; note that, for this 
               * variant, there is no projection at the end of this method.
               * Note that, in turn, this operator does not support the 
               * OUTPUT_DISTINCT_JVs annotation, since this is a "built-in"
               * functionality of the operator.
               */
              new NV(PipelinedHashIndexAndSolutionSetJoinOp.Annotations.PROJECT_IN_VARS, projectInVars),//
              new NV(HashIndexOp.Annotations.CONSTRAINTS, joinConstraints),//
              new NV(HashIndexOp.Annotations.ASK_VAR, askVar),//
              new NV(HashIndexOp.Annotations.HASH_JOIN_UTILITY_FACTORY, joinUtilFactory),//
              new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet),//
              // the pipelined hash index may also contain a subquery for inner evaluation
              new NV(PipelinedHashIndexAndSolutionSetJoinOp.Annotations.SUBQUERY, subqueryPlan),
              new NV(IPredicate.Annotations.RELATION_NAME, 
                    new String[]{ctx.getLexiconNamespace()})              
          ), node, ctx);
          
       } else {
          
          left = applyQueryHints(new HashIndexOp(leftOrEmpty(left),//
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT,
                       BOpEvaluationContext.CONTROLLER),//
                new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),// required for lastPass
                new NV(PipelineOp.Annotations.LAST_PASS, true),// required
                new NV(PipelineOp.Annotations.SHARED_STATE, true),// live stats.
                new NV(HashIndexOp.Annotations.JOIN_TYPE, joinType),//
                new NV(HashIndexOp.Annotations.JOIN_VARS, joinVars),//
                new NV(HashIndexOp.Annotations.BINDING_SETS_SOURCE, bindingSetsSource),
                new NV(HashIndexOp.Annotations.OUTPUT_DISTINCT_JVs, inlineProjection),//
                new NV(HashIndexOp.Annotations.CONSTRAINTS, joinConstraints),//
                new NV(HashIndexOp.Annotations.ASK_VAR, askVar),//
                new NV(HashIndexOp.Annotations.HASH_JOIN_UTILITY_FACTORY, joinUtilFactory),//
                new NV(HashIndexOp.Annotations.NAMED_SET_REF, namedSolutionSet),//
                new NV(IPredicate.Annotations.RELATION_NAME, 
                      new String[]{ctx.getLexiconNamespace()})
            ), node, ctx);

       }
       
       
        /** 
         * If the projection was not inlined, we need to apply it on top.
         * Note that, for the pipelined hash join, everything is "all-in-one",
         * so we must not project here but the projection is inlined by default.
         */
        if (!inlineProjection && !usePipelinedHashJoin) {
           /*
            * Adding a projection operator before the
            * subquery plan ensures that variables which are not visible are
            * dropped out of the solutions flowing through the subquery.
            * However, those variables are already present in the hash index so
            * they can be reunited with the solutions for the subquery in the
            * solution set hash join at the end of the subquery plan.
            * 
            * Note that, when projecting variables inside the subquery, we need
            * to remove duplicates in the outer solution, to avoid a blowup in
            * the result size (the inner result is joined with the complete
            * outer result retained though the previous HashIndexOp at a later
            * point anyway, what we're doing here just serves the purpose to avoid
            * the computation of unneeded bindings inside the subclause, in the
            * sense that we pass in every possible binding once), cf. ticket #835.
            * 
            * A query where this happens is, for instance:
            * 
            * select *
            * where {
            *   ?a :knows ?b .
            *   OPTIONAL {
            *     ?b :knows ?c .
            *     ?c :knows ?d .
            *     filter(?a != :paul) # Note: filter applies to variable in the outer group.
            *    }
            *  }
            *  
            *  The join variable for the subgroup/optional is ?b, but we also
            *  need to project in ?a, since the variable is visible inside the
            *  filter. See TestOptionals.test_optionals_emptyWhereClause.
            */
           left = addDistinctProjectionOp(left, ctx, node, projectInVars);        
         
        }
      
        return left;      
    }

    /**
     * Appends a distinct projection to the current pipeline
     * 
     * @param left the current pipeline
     * @param ctx the context
     * @param node the node associated with the distinct projection
     * @param projectionVars the variables to distinct-project
     * 
     * @return the pipeline extended by a disctint binding sets op
     */
    private static PipelineOp addDistinctProjectionOp(PipelineOp left,
       final AST2BOpContext ctx, final ASTBase node,
       final IVariable<?>[] projectionVars) {
      
       if (!ctx.nativeDistinctSolutions) {
          
          final List<NV> anns = new LinkedList<NV>();
          anns.add(new NV(
              JVMDistinctBindingSetsOp.Annotations.BOP_ID, ctx.nextId()));
          anns.add(new NV(
              JVMDistinctBindingSetsOp.Annotations.VARIABLES, projectionVars));
          anns.add(new NV(
              JVMDistinctBindingSetsOp.Annotations.EVALUATION_CONTEXT,
              BOpEvaluationContext.CONTROLLER));
          anns.add(new NV(
              JVMDistinctBindingSetsOp.Annotations.SHARED_STATE, true));
     
          left = new JVMDistinctBindingSetsOp(leftOrEmpty(left),//
              anns.toArray(new NV[anns.size()]));
          
       } else {

          final INamedSolutionSetRef namedSolutionSet = 
             NamedSolutionSetRefUtility.newInstance(
                ctx.queryId, "--distinct-"+ctx.nextId(), projectionVars);
          
          final List<NV> anns = new LinkedList<NV>();
          anns.add(new NV(
              HTreeDistinctBindingSetsOp.Annotations.BOP_ID, ctx.nextId()));
          anns.add(new NV(
              HTreeDistinctBindingSetsOp.Annotations.VARIABLES, projectionVars));
          anns.add(new NV(
              HTreeDistinctBindingSetsOp.Annotations.EVALUATION_CONTEXT,
              BOpEvaluationContext.CONTROLLER));
          anns.add(new NV(
              HTreeDistinctBindingSetsOp.Annotations.SHARED_STATE, true));
          anns.add(new NV(PipelineOp.Annotations.MAX_PARALLEL, 1));
          anns.add(new NV(HTreeDistinctBindingSetsOp.Annotations.NAMED_SET_REF,
                       namedSolutionSet));
          anns.add(new NV(IPredicate.Annotations.RELATION_NAME, 
                  new String[]{ctx.getLexiconNamespace()}));

          
          left = new HTreeDistinctBindingSetsOp(leftOrEmpty(left),//
              anns.toArray(new NV[anns.size()]));

       }
      
       return left;
   }

   /**
     * Makes a static (pessimistic) best effort decision whether or not the
     * assignment node needs to be resolved (i.e., its possibly mocked IV must
     * be resolved against the dictionary). 
     * 
     * @param ass the assignment node to investigate
     * @param ctx the query evaluation context
     * 
     * @return false if it can be guaranteed that the IVs are not mocked or
     *               do not need to be resolved, true otherwise
     */
    private static boolean mustBeResolvedInContext(
       final AssignmentNode ass, final AST2BOpContext ctx) {

       final IValueExpression<?> vexp = ass.getValueExpression();
       
       /* APPROACH: SCAN FOR SAFE CASES, RETURN true IF NOT ENCOUNTERED */
       
       /*
        * We're always fine with numerical and boolean expressions.
        */
       if (vexp instanceof MathBOp ||
           vexp instanceof XSDBooleanIVValueExpression) {
           
           // need to take care of resolution if inlining of literals is disabled
           return !ctx.getAbstractTripleStore().isInlineLiterals();
            
       }
       
       /*
        * Handle cases where the assignment node is a constant with 
        * known bound value.
        */
       if (vexp instanceof IConstant) {
          final IConstant<?> vexpAsConst = (IConstant<?>)vexp;
          final Object val = vexpAsConst.get();
          
          if (val instanceof TermId) {    
             
             // whenever IV is not mocked, we're fine
             final TermId<?> valAsTermId = (TermId<?>)val;
             if (!valAsTermId.isNullIV()) {
                return false;
             }
   
          } else if (val instanceof NumericIV) {

             // also, numerics don't pose problems
             return false;
          }
       }
          
       
       /*
        * In case of more complex assignment expressions, we may still not
        * require to resolve the IV in case the bound variable is not used
        * anywhere else in the query. To do so, we count the number of 
        * occurrences of the variable in the root query; if it occurs more
        * than once in the non-select clause (i.e., more often than the binding
        * in the assignment node, it is (in the general case) not safe to
        * suppress the resolving.
        * 
        * Note: this again is overly broad. However, there are only rare cases
        * where the same variable is used twice independently (e.g. in
        * unconnected unions), which we ignore here.
        */
       final IVariable<?> assVar = ass.getVar();
       final QueryRoot root =  ctx.sa.getQueryRoot();

       if (BOpUtility.countVarOccurrencesOutsideProjections(root, assVar) <= 1) {
          return false;
       }
          
       /*
        * Fallback: we can't be sure that resolval is not required
        */
       return true;
   }
    

   /**
    * Decides, based on the parameters, whether we prefer a pipelined hash
    * join or not. The decision is made based on global configuration,
    * query hints (which may override the global setting), and finally
    * the structure of the query (LIMIT queries are good candidates for
    * pipelined joins).
    * 
    * @param ctx the context
    * @param node the node for which to possibly use the pattern 
    * @return true if a pipeled hash join is preferred
    */
   private static boolean usePipelinedHashJoin(
      AST2BOpContext ctx, QueryNodeBase node) {

      // query hints override global setting
      final String queryHint = node.getQueryHint(QueryHints.PIPELINED_HASH_JOIN);
      if (queryHint!=null && !queryHint.isEmpty()) {
         return Boolean.valueOf(queryHint);
      }

      // apply global setting if enabled
      if (ctx.pipelinedHashJoins) {
         return true;
      }
      
      // otherwise, investigate the query plan
      final ASTContainer container = ctx.astContainer;
      final QueryRoot queryRoot = container.getOptimizedAST();

      // check for LIMIT
      final SliceNode slice = queryRoot.getSlice();
      if (slice == null || slice.getLimit() == SliceNode.Annotations.DEFAULT_LIMIT) {
         return false; // no LIMIT specified
      }
     
      // if we end up here: there's a LIMIT clause in the query
      // -> assert that there's no ORDER BY (order by will require full result
      //    computation, so it rules out the benefits of the pipelined hash join
      final OrderByNode orderBy = queryRoot.getOrderBy();
      boolean noOrderBy = orderBy==null || orderBy.isEmpty();
      
      return noOrderBy;
   }

}
