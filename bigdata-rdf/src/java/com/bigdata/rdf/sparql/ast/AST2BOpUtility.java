package com.bigdata.rdf.sparql.ast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.EndOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.bop.controller.NamedSubqueryIncludeOp;
import com.bigdata.bop.controller.NamedSubqueryOp;
import com.bigdata.bop.controller.Steps;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.join.InlineMaterializeOp;
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
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SortOrder;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.constraints.BindingConstraint;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.ConditionalBind;
import com.bigdata.rdf.internal.constraints.IsInlineBOp;
import com.bigdata.rdf.internal.constraints.IsMaterializedBOp;
import com.bigdata.rdf.internal.constraints.NeedsMaterializationBOp;
import com.bigdata.rdf.internal.constraints.ProjectedConstraint;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.TryBeforeMaterializationConstraint;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.lexicon.LexPredicate;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sail.FreeTextSearchExpander;
import com.bigdata.rdf.sail.Rule2BOpUtility;
import com.bigdata.rdf.spo.DefaultGraphSolutionExpander;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.NamedGraphSolutionExpander;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;

/**
 * Query plan generator converts an AST into a query plan made up of
 * {@link PipelineOp}s
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME https://sourceforge.net/apps/trac/bigdata/ticket/368 (Prune
 *          variable bindings during query evaluation).
 *          <p>
 *          Note: Sesame allows externally given bindings to flow through the
 *          query even if they are projected by the query. This means that the
 *          "projection" can not remove all bindings except those which are
 *          projected. Either we need to also not prune exogenous bindings or we
 *          need to strip out bindings introduced by anonymous variables which
 *          are part of query rewrites. The former seems easier.
 * 
 *          TODO What about the backchain access path stuff? Are we going prolog
 *          / datalog or bringing that stuff forward?
 */
public class AST2BOpUtility {

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
        final QueryRoot query = ctx.query;
        
        /*
         * Run the AST query rewrites / query optimizers.
         * 
         * FIXME For improved visibility, the optimizer should be run outside of
         * this method and/or we should publish the optimizer AST. Otherwise
         * things like DESCRIBE can be rewritten pretty broadly and we do not
         * get a chance to see the rewritten AST model.
         */
        if (ctx.optimizedQuery == null) {

            ctx.optimizedQuery = (QueryRoot) ctx.optimizers.optimize(ctx,
                    query, bindingSets);
            
        }
        
        // The executable query plan.
        PipelineOp queryPlan = convert(ctx.optimizedQuery, ctx);

        /*
         * Set the queryId on the top-level of the query plan.
         * 
         * Note: The queryId is necessary to coordinate access to named subquery
         * result sets when they are consumed within a subquery rather than the
         * main query.
         */

        queryPlan = (PipelineOp) queryPlan.setProperty(
                QueryEngine.Annotations.QUERY_ID, ctx.queryId);

        if (log.isInfoEnabled()) {
            log.info("\noriginal :\n" + query);
            log.info("\noptimized:\n" + ctx.optimizedQuery);
            log.info("\npipeline :\n" + BOpUtility.toString2(queryPlan));
        }

        return queryPlan;

    }
    
    /**
     * Convert a query (or subquery) into a query plan (pipeline).
     * 
     * @param query
     *            Either a {@link QueryRoot}, a {@link SubqueryRoot}, or a
     *            {@link NamedSubqueryRoot}.
     * @param ctx
     *            The evaluation context.
     * 
     * @return The query plan.
     */
    private static PipelineOp convert(final QueryBase query,
            final AST2BOpContext ctx) {
    	
        /*
         * Visit all the value expression nodes and convert them into value
         * expressions.
         */
        {
            
            final String lex = ctx.db.getLexiconRelation().getNamespace();
            
            final Iterator<IValueExpressionNode> it = BOpUtility.visitAll(
                    query, IValueExpressionNode.class);

            final ArrayList<IValueExpressionNode> allNodes = new ArrayList<IValueExpressionNode>();

            while (it.hasNext()) {

                allNodes.add(it.next());

            }

            for (IValueExpressionNode ven : allNodes) {

                /*
                 * Convert and cache the value expression on the node as a
                 * side-effect.
                 */
                toVE(lex, ven);

            }

        }

        final IGroupNode<IGroupMemberNode> root = query.getWhereClause();

        if (root == null)
            throw new IllegalArgumentException("No group node");

		PipelineOp left = null;

        /*
         * Named subqueries.
         */
        if (query instanceof QueryRoot) {

            final NamedSubqueriesNode namedSubqueries = ((QueryRoot) query)
                    .getNamedSubqueries();

            if (namedSubqueries != null && !namedSubqueries.isEmpty()) {

                // WITH ... AS [name] ... INCLUDE style subquery declarations.
                left = addNamedSubqueries(left, namedSubqueries,
                        (QueryRoot) query, ctx);

            }

        }

        // The top-level "WHERE" clause.
        left = convertJoinGroupOrUnion(left, root, ctx);

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

                left = addProjectedAssigments(left,
                        projection.getAssignmentProjections(), ctx);

            }

            /*
             * Note: REDUCED allows, but does not require, either complete or
             * partial filtering of duplicates. It is part of what openrdf does
             * for a DESCRIBE query.
             * 
             * Note: We do not currently have special operator for REDUCED. One
             * could be created using chunk wise DISTINCT. Note that REDUCED may
             * not change the order in which the solutions appear (but we are
             * evaluating it before ORDER BY so that is Ok.)
             */
  
            if (projection.isDistinct() || projection.isReduced()) {

                left = addDistinct(left, query, ctx);

            }

        }
        
        final OrderByNode orderBy = query.getOrderBy();
        
        if (orderBy != null && ! orderBy.isEmpty()) {

    		left = addOrderBy(left, orderBy, ctx);
        	
        }

        final SliceNode slice = query.getSlice();

        if (slice != null) {

            left = addSlice(left, slice, ctx);

        }

//        left = addEndOp(left, ctx);
        
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
         * TODO Do we need to add operators for materialization of any remaining
         * variables which are being projected out of the query? Note that we do
         * not want to force materialization for subqueries. They should stay as
         * IVs. (I think that materialization is still handled by the bulk IV
         * resolution iterators).
         */
        
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
     * be generated by the {@link NamedSubqueryOp}.
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
     *         TODO It is possible to run the named subqueries in parallel since
     *         the do not have any mutual dependencies. {@link Steps} could be
     *         used to do this. Whether this is a wise use of resources is
     *         another matter. It probably is on a cluster. It might not be on a
     *         single machine depending on how heavy the subqueries are and what
     *         the concurrent workload is on the machine.
     */
    private static PipelineOp addNamedSubqueries(PipelineOp left,
            final NamedSubqueriesNode namedSubquerieNode,
            final QueryRoot queryRoot, final AST2BOpContext ctx) {

        for(NamedSubqueryRoot subqueryRoot : namedSubquerieNode) {
            
            final PipelineOp subqueryPlan = convert(subqueryRoot, ctx);
            
            if (log.isInfoEnabled())
                log.info("\nsubquery: " + subqueryRoot + "\nplan="
                        + subqueryPlan);

            final IVariable<?>[] joinVars = ASTUtil.convert(subqueryRoot
                    .getJoinVars());

            final NamedSolutionSetRef namedSolutionSet = new NamedSolutionSetRef(
                    ctx.queryId, subqueryRoot.getName(), joinVars);

            left = new NamedSubqueryOp(leftOrEmpty(left), //
                    new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT, BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(NamedSubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                    new NV(NamedSubqueryOp.Annotations.JOIN_VARS, joinVars),//
                    new NV(NamedSubqueryOp.Annotations.NAMED_SET_REF, namedSolutionSet)//
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
     */
    private static PipelineOp addSubqueryInclude(PipelineOp left,
            final NamedSubqueryInclude subqueryInclude, final AST2BOpContext ctx) {

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

        left = new NamedSubqueryIncludeOp(leftOrEmpty(left), //
                new NV(BOp.Annotations.BOP_ID, ctx.nextId()),//
                new NV(BOp.Annotations.EVALUATION_CONTEXT, BOpEvaluationContext.CONTROLLER),//
                new NV(NamedSubqueryIncludeOp.Annotations.NAMED_SET_REF, namedSolutionSetRef),//
                new NV(NamedSubqueryIncludeOp.Annotations.JOIN_VARS, joinVars)//
                );

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
            final SubqueryRoot subquery, final AST2BOpContext ctx) {

        final boolean aggregate = isAggregate(subquery);
        
        final PipelineOp subqueryPlan = convert(subquery, ctx);

        if (log.isInfoEnabled())
            log.info("\nsubquery: " + subquery + "\nplan=" + subqueryPlan);

        switch (subquery.getQueryType()) {
        case ASK: {
            
            /*
             * The projection should have just one variable, which is the
             * variable that will be bound to true if there is a solution to the
             * subquery and to false if there is no solution to the subquery.
             */

            final ProjectionNode projection = subquery.getProjection();

            if (projection == null)
                throw new RuntimeException("No projection for ASK subquery.");

            final IVariable<?>[] vars = projection.getProjectionVars();

            if (vars == null || vars.length != 1)
                throw new RuntimeException(
                        "Expecting ONE (1) projected variable for ASK subquery.");

            final IVariable<?> askVar = vars[0];
            
            left = new SubqueryOp(leftOrEmpty(left),// SUBQUERY
                    new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(SubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                    new NV(SubqueryOp.Annotations.OPTIONAL, false),//
                    new NV(SubqueryOp.Annotations.ASK_VAR, askVar),//
                    new NV(SubqueryOp.Annotations.SELECT, null),//
                    new NV(SubqueryOp.Annotations.IS_AGGREGATE, aggregate)//
            );
            break;
        }
        case SELECT: {

            final ProjectionNode projection = subquery.getProjection();
            
            final IVariable<?>[] vars = projection.getProjectionVars();
            
            left = new SubqueryOp(leftOrEmpty(left),// SUBQUERY
                    new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),//
                    new NV(SubqueryOp.Annotations.SUBQUERY, subqueryPlan),//
                    new NV(SubqueryOp.Annotations.OPTIONAL, false),//
                    new NV(SubqueryOp.Annotations.SELECT, vars),//
                    new NV(SubqueryOp.Annotations.IS_AGGREGATE, aggregate)//
            );
            
            break;
            
        }
        default:
            throw new UnsupportedOperationException();
        }

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
    private static boolean isAggregate(final QueryBase query) {

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

        if(expr instanceof IAggregate<?>)
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
            final AST2BOpContext ctx) {

		if (groupNode instanceof UnionNode) {
			
			return convertUnion(left, (UnionNode) groupNode, ctx);
			
		} else if (groupNode instanceof JoinGroupNode) {
			
			return convertJoinGroup(left, (JoinGroupNode) groupNode, ctx);
			
		} else {
		
			throw new IllegalArgumentException();
			
		}
		
	}

    /**
     * Generate the query plan for a union.
     * 
     * @param unionNode
     * @param ctx
     * @return
     * 
     *         TODO Could recognize
     * 
     *         <pre>
     * optional {
     *   { a } union { b }
     * }     *
     * </pre>
     * 
     *         as an optional union, but we are not handling the optional
     *         property for a union right now.
     */
	private static PipelineOp convertUnion(PipelineOp left,
	        final UnionNode unionNode,
			final AST2BOpContext ctx) {

        if (unionNode.isOptional()) {
            /*
             * This in fact shows up in the TCK which is a bit weird.
             */
            log.warn("Optional union? : " + ctx.query.getQueryString());
        }
	    
        final int arity = unionNode.size();
        
        if (arity == 0) {
            /*
             * TODO We can probably just return [left]. The old evaluation
             * strategy code would translate an empty union into an empty
             * iterator. That could be captured by a rewrite of the AST
             * which recognizes and removes empty UNION nodes.
             */
		    throw new IllegalArgumentException();
		    
		}
		
        // The bopId for the UNION or STEP.
        final int thisId = ctx.nextId();
        
        final BOp[] subqueries = new BOp[arity];

        int i = 0;
        for (IGroupMemberNode child : unionNode) {
        	
            // convert the child
        	if (child instanceof JoinGroupNode) {
        		
                subqueries[i++] = convertJoinGroup(null/* left */,
                        (JoinGroupNode) child, ctx);
        		
//        	} else if (child instanceof StatementPatternNode) {
//        		
//        		// allow lone statement patterns as children for unions,
//        		// just use a dummy join group to ease the conversion process
//        		
//        		final IGroupNode dummyGroup = new JoinGroupNode(false);
//        		dummyGroup.addChild(child);
//
//        		subqueries[i++] = convert(dummyGroup, ctx);

        	} else {
        		
        		throw new RuntimeException(
        				"Illegal child type for union: " + child.getClass());
        		
        	}
			
        }
        
        final LinkedList<NV> anns = new LinkedList<NV>();
        anns.add(new NV(BOp.Annotations.BOP_ID, thisId));
        anns.add(new NV(Union.Annotations.SUBQUERIES,subqueries));
        
//      if (union.getParent() == null) {
			anns.add(new NV(Union.Annotations.EVALUATION_CONTEXT,
					BOpEvaluationContext.CONTROLLER));
			anns.add(new NV(Union.Annotations.CONTROLLER, true));
//      }
        
        final PipelineOp union = applyQueryHints(
                new Union(leftOrEmpty(left),
                        NV.asMap(anns.toArray(new NV[anns.size()]))),
                ctx.queryHints);
        
        return union;
		
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
     * TODO Think about how hash joins fit into this.
     */
	private static PipelineOp convertJoinGroup(PipelineOp left,
	        final JoinGroupNode joinGroup,
			final AST2BOpContext ctx) {

        /*
         * Place the StartOp at the beginning of the pipeline.
         * 
         * TODO We only need a start op on a cluster if there is a requirement
         * to marshall all solutions from a parent's evaluation context onto the
         * top-level query controller. I am not sure that we ever have to do
         * this for a subquery, but there might be cases where it is required.
         * For all other cases, we should begin with left := null. However,
         * there are cases where getParent() is running into a null reference
         * during query evaluation if [left := null] here. Look into and resolve
         * those issues.
         */
	    if(left == null)
	        left = addStartOp(ctx);
        
        /*
         * Add the pre-conditionals to the pipeline.
         * 
         * FIXME Does this force anything which will be needed by any filter in
         * the query to become materialized before we do anything else? If so,
         * then how do we ever handle materialization for filters against
         * variables are not bound until we run a required join later in the
         * group? Are those filters being moved to post-conditionals? If so,
         * then that is very expensive. They should be tightly interwoven with
         * the required joins such that we evaluate them as soon as possible to
         * maximize the selectivity of the pipeline, right?
         */
        left = addConditionals(left, joinGroup.getPreFilters(), ctx);

        /*
         * Required joins and non-optional subqueries.
         * 
         * TODO SPARQL 1.1 style subqueries are currently always pipelined. Like
         * named subquery includes, they are also never optional. However, there
         * is no a-priori reason why we should run pipelined subqueries before
         * named subquery includes and, really, no reason why we can not mix
         * these with the required joins (above). I believe that this is being
         * done solely for expediency (because the static query optimizer can
         * not handle it).
         * 
         * Also, note that named subquery includes are hash joins. We have an
         * index. If the operator supported cutoff evaluation then we could
         * easily reorder them with the other required joins using the RTO.
         * 
         * Ditto for pipelined SPARQL 1.1 subquery. If it supported cutoff
         * evaluation, then the RTO could reorder them with the required joins.
         * This is even true when the subquery uses GROUP BY or ORDER BY, which
         * imply the use of at once operators. While we must fully materialize
         * the solutions for each evaluation of the subquery, the evaluation is
         * based on the as-bound solutions flowing into the subquery. If the
         * subquery is unselective, then clearly this will be painful and it
         * might be better to lift such unselective subqueries into named
         * subqueries in order to obtain a hash index over the entire subquery
         * solution set when evaluated with an empty source binding set.
         */
        {

            /*
             * Add the joins (statement patterns) and the filters on those
             * joins.
             * 
             * Note: This winds up handling materialization steps as well.
             * 
             * TODO Materialization steps are currently handled via recursion
             * into Rule2BOpUtility, but there is a FIXME for that.
             */
            left = addJoins(left, joinGroup, ctx);

            /*
             * Add SPARQL 1.1 style subqueries.
             */
            for (IGroupMemberNode child : joinGroup) {
                if (child instanceof SubqueryRoot) {
                    left = addSparql11Subquery(left, (SubqueryRoot) child, ctx);
                }
            }

            /*
             * Add joins against named solution sets from WITH AS INCLUDE style
             * subqueries.
             */
            for (IGroupMemberNode child : joinGroup) {
                if (child instanceof NamedSubqueryInclude)
                    left = addSubqueryInclude(left,
                            (NamedSubqueryInclude) child, ctx);
            }

        }
        
        /*
         * Add the subqueries (individual optional statement patterns, optional
         * join groups, and nested union).
         */
        left = addSubqueries(left, joinGroup, ctx);
        
        /*
         * Add the LET assignments to the pipeline.
         * 
         * TODO Review as generated query plans: A LET/BIND for an expression
         * should be run as soon as the variables for that expression are known
         * bound. This is the same rule which is used for filters. Running them
         * here could even cause incorrect evaluation depending on when
         * assigned-to variable is being consumed.
         */
        left = addAssignments(left, joinGroup.getAssignments(), ctx, false/* projection */);

        /*
         * Add the post-conditionals to the pipeline.
         */
        left = addConditionals(left, joinGroup.getPostFilters(), ctx);

        /*
         * Add the end operator if necessary.
         */
        if (joinGroup.getParent() != null) {
            left = addEndOp(left, ctx);
        }

        return left;
		
	}
	
	private static final PipelineOp addStartOp(final AST2BOpContext ctx) {
		
        final PipelineOp start = applyQueryHints(
        		new StartOp(BOp.NOARGS,
			        NV.asMap(new NV[] {//
			              new NV(Predicate.Annotations.BOP_ID, 
			            		  ctx.nextId()),
			              new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
			                      BOpEvaluationContext.CONTROLLER),
			        })), ctx.queryHints);

        return start;
        
	}
	
    /**
     * Project select expressions (non-aggregation case).
     * 
     * FIXME https://sourceforge.net/apps/trac/bigdata/ticket/368 (Prune
     * variable bindings during query evaluation). This is basically the last
     * place where we can prune out variables which are not being projected.
     * We need to do that here, but also incrementally.
     */
    private static final PipelineOp addProjectedAssigments(PipelineOp left,
	        final List<AssignmentNode> assignments,
	        final AST2BOpContext ctx){
	    
        left = addAssignments(left, assignments, ctx, true/* projection */);
        
        return left;

    }
	
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private static final PipelineOp addAssignments(PipelineOp left,    		
    		final List<AssignmentNode> assignments,
            final AST2BOpContext ctx,
            final boolean projection) {
	    
		final Set<IVariable<IV>> done = new LinkedHashSet<IVariable<IV>>();

		for (AssignmentNode assignmentNode : assignments) {

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
			vars.removeAll(done);
			
            final int bopId = ctx.nextId();

            final ConditionalBind b = new ConditionalBind(
                    assignmentNode.getVar(),
                    assignmentNode.getValueExpression(),
                    projection);

            IConstraint c = projection ? new ProjectedConstraint(b)
                    : new BindingConstraint(b);

            /*
             * We might have already materialized everything we need for this
             * filter.
             */
            if (vars.size() > 0) {

                left = addMaterializationSteps(left, bopId, ve, vars, ctx);

                /*
                 * All the newly materialized vars to the set we've already
                 * done.
                 */
                done.addAll(vars);

                c = new TryBeforeMaterializationConstraint(c);

            }

			left = applyQueryHints(
              	    new ConditionalRoutingOp(new BOp[]{ left },
                        NV.asMap(new NV[]{//
                            new NV(BOp.Annotations.BOP_ID, bopId),
                            new NV(ConditionalRoutingOp.Annotations.CONDITION, c),
                        })), ctx.queryHints);
			
		}
		
		return left;
    	
    }
	
    @SuppressWarnings("rawtypes")
    private static final PipelineOp addConditionals(PipelineOp left,    		
    		final Collection<FilterNode> filters,
            final AST2BOpContext ctx) {

		final Set<IVariable<IV>> done = new LinkedHashSet<IVariable<IV>>();

		for (FilterNode filter : filters) {

            @SuppressWarnings("unchecked")
            final IValueExpression<IV> ve = (IValueExpression<IV>) filter
                    .getValueExpression();
			
			final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();
			
			/*
			 * Get the vars this filter needs materialized.
			 */
            vars.addAll(filter.getMaterializationRequirement()
                    .getVarsToMaterialize());

			/*
			 * Remove the ones we've already done.
			 */
			vars.removeAll(done);
			
			final int bopId = ctx.nextId();

			/*
			 * We might have already materialized everything we need for this filter.
			 */
			if (vars.size() > 0) {
				
				left = addMaterializationSteps(left, bopId, ve, vars, ctx);
			
				/*
				 * All the newly materialized vars to the set we've already done.
				 */
				done.addAll(vars);
				
			}
		
            final IConstraint c = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    ve);
			
			left = applyQueryHints(
              	    new ConditionalRoutingOp(new BOp[]{ left },
                        NV.asMap(new NV[]{//
                            new NV(BOp.Annotations.BOP_ID, bopId),
                            new NV(ConditionalRoutingOp.Annotations.CONDITION, c),
                        })), ctx.queryHints);
			
		}
		
		return left;
    	
    }
    
    private static final PipelineOp addJoins(PipelineOp left,
    		final JoinGroupNode joinGroup, final AST2BOpContext ctx) {

        @SuppressWarnings("rawtypes")
    	final List<IPredicate> preds = new LinkedList<IPredicate>();
    	
    	for (StatementPatternNode sp : joinGroup.getStatementPatterns()) {
    		preds.add(toPredicate(sp, ctx));
    	}

    	// sometimes we get empty groups
    	if (preds.size() == 0) {
    		return left;
    	}
    	
    	final List<IConstraint> constraints = new LinkedList<IConstraint>();
    	
        for (FilterNode filter : joinGroup.getJoinFilters()) {

            constraints.add(new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    filter.getValueExpression()));
            
        }
    	
    	@SuppressWarnings("rawtypes")
        final IRule rule = new Rule(
    			"null", // name
    			null, // head
    			preds.toArray(new IPredicate[preds.size()]), // tails
    			constraints.size() > 0 // constraints 
    					? constraints.toArray(new IConstraint[constraints.size()]) 
						: null
				);
    	
        /*
         * FIXME Pull code up for this. (note that Rule2BOpUtility is also
         * handling materialization steps for joins.
         */
    	left = Rule2BOpUtility.convert(
    			rule,
    			left,
    			joinGroup.getIncomingBindings(), // knownBound
    			ctx.idFactory,
    			ctx.db,
    			ctx.queryEngine,
    			ctx.queryHints
    			);
    	
    	return left;
    	
    }
    
    /**
     * Adds an optional join (prepared by the caller) in place of an optional
     * group having a join as its sole group member.
     * 
     * @param left
     * @param pred
     * @param filters
     * @param ctx
     * @return
     * 
     *         TODO This could be handled by an AST rewrite which replaces the
     *         optional subquery with an optional {@link StatementPatternNode}.
     *         There is no annotation for that at this time on the statement
     *         pattern node. Also, if we allow that annotation then we have to
     *         check statement pattern nodes to make sure that they are not
     *         optional when they appear within a group which we currently do
     *         not have to do as they are never optional.
     */
    private static final PipelineOp addOptionalJoinForSingletonOptionalSubquery(
            PipelineOp left, final Predicate<?> pred,
            final Collection<FilterNode> filters, final AST2BOpContext ctx) {
    	
//    	final Predicate pred = toPredicate(sp);
    	
    	final Collection<IConstraint> constraints = new LinkedList<IConstraint>();
    	
        for (FilterNode filter : filters) {
         
            constraints.add(new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    filter.getValueExpression()));
            
        }
    	
    	// just for now
    	left = Rule2BOpUtility.join(
    			ctx.db, 
    			ctx.queryEngine, 
    			left, 
    			pred,
    			constraints,
    			ctx.idFactory, 
    			ctx.queryHints
    			);
    	
    	return left;
    	
    }

    /**
     * Adds subqueries for join groups ({@link JoinGroupNode}) and unions (
     * {@link UnionNode}) to the pipeline (this is NOT SPARQL 1.1 subquery).
     * Non-optional group groups and unions are handled first, then optionals.
     * 
     * @param left
     * @param joinGroup
     * @param ctx
     * @return
     */
    private static final PipelineOp addSubqueries(PipelineOp left,
    		final JoinGroupNode joinGroup, final AST2BOpContext ctx) {

    	/*
    	 * First do the non-optional subgroups
    	 */
    	for (IGroupMemberNode child : joinGroup) {
    		
            if (!(child instanceof GraphPatternGroup<?>)) {
                continue;
            }
    		
            @SuppressWarnings("unchecked")
            final IGroupNode<IGroupMemberNode> subgroup = (IGroupNode<IGroupMemberNode>) child;

    		if (subgroup.isOptional()) {
    			continue;
    		}
    		
            final PipelineOp subquery = convertJoinGroupOrUnion(null/* left */, subgroup, ctx);
    		
    		left = new SubqueryOp(leftOrEmpty(left), 
                    new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),
                    new NV(SubqueryOp.Annotations.SUBQUERY, subquery),
                    new NV(SubqueryOp.Annotations.OPTIONAL, false)
            );

    	}
    	
    	/*
    	 * Next do the optional sub-group 
    	 */
    	for (IGroupMemberNode child : joinGroup) {
    		
            if (!(child instanceof GraphPatternGroup<?>)) {
    			continue;
    		}
    		
            @SuppressWarnings("unchecked")
            final IGroupNode<IGroupMemberNode> subgroup = (IGroupNode<IGroupMemberNode>) child;
    		
    		if (!subgroup.isOptional()) {
    			continue;
    		}
    		
            if (subgroup instanceof JoinGroupNode
                    && ((JoinGroupNode) subgroup).isSimpleOptional()) {

                /*
                 * Optimize a simple join group as an optional join rather than
                 * a subquery.
                 */
    		    
    			final JoinGroupNode subJoinGroup = (JoinGroupNode) subgroup;
    			
    			final StatementPatternNode sp = subJoinGroup.getSimpleOptional();
    			
    			final Collection<FilterNode> filters = subJoinGroup.getFilters();
    			
                final Predicate<?> pred = (Predicate<?>) toPredicate(sp, ctx)
                        .setProperty(IPredicate.Annotations.OPTIONAL,
                                Boolean.TRUE);
    			
//    			final StatementPatternNode sp2 = new StatementPatternNode(pred);
    			
                left = addOptionalJoinForSingletonOptionalSubquery(left, pred,
                        filters, ctx);

    		} else {
    		
                final PipelineOp subquery = convertJoinGroupOrUnion(
                        null/* left */, subgroup, ctx);

	    		left = new SubqueryOp(leftOrEmpty(left), 
	                    new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),
	                    new NV(SubqueryOp.Annotations.SUBQUERY, subquery),
	                    new NV(SubqueryOp.Annotations.OPTIONAL, true)
	            );
	    		
    		}
    		
    	}
    	
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
     * 
     * TODO Should be conditional based on whether or not we are running on a
     * cluster, but also see https://sourceforge.net/apps/trac/bigdata/ticket/227.
     */
	private static final PipelineOp addEndOp(PipelineOp left,
			final AST2BOpContext ctx) {
		
        if (!left.getEvaluationContext().equals(
                        BOpEvaluationContext.CONTROLLER)) {
			
            left = new EndOp(new BOp[] { left },//
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
     * TODO If the expected cardinality is large then prefer
     * {@link DistinctBindingSetsWithHTreeOp} rather than
     * {@link DistinctBindingSetOp}.
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
//        /*
//         * TODO This will find variables within subqueries as well. Those
//         * variables are not really in scope. We should have a modified
//         * getSpannedVariables() method which handles this (or maybe just modify
//         * the version we already have to be scope aware).
//         */
//        final IVariable<?>[] vars = projection.isWildcard() ? BOpUtility
//                .toArray(BOpUtility.getSpannedVariables((BOp) query
//                        .getWhereClause())) : projection.getProjectionVars();

		final PipelineOp op;
		if(ctx.nativeDistinct) {
		    /*
		     * DISTINCT on the JVM heap.
		     */
		    op = new DistinctBindingSetOp(//
		            new BOp[] { left },// 
	                NV.asMap(new NV[]{//
	                    new NV(DistinctBindingSetOp.Annotations.BOP_ID, bopId),
	                    new NV(DistinctBindingSetOp.Annotations.VARIABLES, vars),
	                    new NV(DistinctBindingSetOp.Annotations.EVALUATION_CONTEXT,
	                            BOpEvaluationContext.CONTROLLER),
	                    new NV(DistinctBindingSetOp.Annotations.SHARED_STATE,
	                            true),
	                }));
		} else {
		    /*
		     * DISTINCT on the native heap.
		     */
	        op = new DistinctBindingSetsWithHTreeOp(//
	                new BOp[]{left},//
	                NV.asMap(new NV[]{//
	                    new NV(DistinctBindingSetsWithHTreeOp.Annotations.BOP_ID,bopId),//
	                    new NV(DistinctBindingSetsWithHTreeOp.Annotations.VARIABLES,vars),//
	                    new NV(PipelineOp.Annotations.EVALUATION_CONTEXT,
	                            BOpEvaluationContext.CONTROLLER),//
	                    new NV(PipelineOp.Annotations.SHARED_STATE, true),//
	                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
	                }));
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final PipelineOp addAggregation(PipelineOp left,
            final ProjectionNode projection, final GroupByNode groupBy,
            final HavingNode having, final AST2BOpContext ctx) {
        
        final IValueExpression<?>[] projectExprs = projection
                .getValueExpressions();

        final IValueExpression<?>[] groupByExprs = groupBy == null ? null
                : groupBy.getValueExpressions();

        final IConstraint[] havingExprs = having == null ? null
                : having.getConstraints();

        final IGroupByState groupByState = new GroupByState(projectExprs,
                groupByExprs, havingExprs);

        final IGroupByRewriteState groupByRewrite = new GroupByRewriter(
                groupByState);

        final int bopId = ctx.nextId();

        final GroupByOp op;
        
        /*
         * TODO Review. I believe that AssignmentNode.getValueExpression()
         * should always return the Bind().
         */
        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        if (projectExprs != null) {
            
            for (IValueExpression expr : projectExprs) {
                
                if (expr instanceof Bind) {
                
                    vars.add(((Bind) expr).getVar());
                
                    // Note: side-effect on expr!
                    expr = ((Bind) expr).getExpr();
                    
                }
                
                if (expr instanceof IVariable<?>) {

                    vars.add((IVariable<IV>) expr);

                } else {

                    ComputedMaterializationRequirement.gatherVarsToMaterialize(
                            expr, vars);

                }

            }

        }

        if (groupByExprs != null) {

            for (IValueExpression expr : groupByExprs) {

                if (expr instanceof Bind) {

                    vars.add(((Bind) expr).getVar());

                    // Note: side-effect on expr.
                    expr = ((Bind) expr).getExpr();

                } 
                
                if (expr instanceof IVariable<?>) {

                    vars.add((IVariable<IV>) expr);

                } else {

                    ComputedMaterializationRequirement.gatherVarsToMaterialize(
                            expr, vars);

                }

            }

        }

        left = addMaterializationSteps(left, bopId, vars, ctx);

        if (!groupByState.isAnyDistinct() && !groupByState.isSelectDependency()) {

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

            op = new PipelinedAggregationOp(new BOp[] {left}, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, bopId),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.PIPELINED, true),//
                    new NV(PipelineOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(PipelineOp.Annotations.SHARED_STATE, true),//
                    new NV(GroupByOp.Annotations.GROUP_BY_STATE, groupByState), //
                    new NV(GroupByOp.Annotations.GROUP_BY_REWRITE, groupByRewrite), //
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
            
            op = new MemoryGroupByOp(new BOp[] {left}, NV.asMap(new NV[] {//
                    new NV(BOp.Annotations.BOP_ID, bopId),//
                    new NV(BOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),//
                    new NV(PipelineOp.Annotations.PIPELINED, false),//
                    new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                    new NV(GroupByOp.Annotations.GROUP_BY_STATE, groupByState), //
                    new NV(GroupByOp.Annotations.GROUP_BY_REWRITE, groupByRewrite), //
            }));

        }
        
        left = applyQueryHints(op, ctx.queryHints);

        return left;
        
    }
    

    /**
     * TODO Minor optimization: A bare constant in the ORDER BY value expression
     * list should be dropped. If there are no remaining value expressions, then
     * the entire ORDER BY operation should be dropped.
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
    		
    		if(!(expr instanceof IVariableOrConstant<?> && !(expr instanceof IBind))) {
    		
                /*
                 * Wrap the expression with a BIND of an anonymous variable.
                 */
    		    
                expr = new Bind(Var.var(), expr);
    		    
    		}
    		
    		if (expr instanceof IVariable<?>) {

                vars.add((IVariable<IV>) expr);

            } else  {

                ComputedMaterializationRequirement.gatherVarsToMaterialize(expr, vars);
                
            }

            sortOrders[i] = new SortOrder(expr, orderByExpr.isAscending());
    		
		}
		
		final int sortId = ctx.nextId();
		
		left = addMaterializationSteps(left, sortId, vars, ctx);
		
    	left = applyQueryHints(new MemorySortOp(new BOp[] { left }, 
    			NV.asMap(new NV[] {//
	                new NV(MemorySortOp.Annotations.BOP_ID, sortId),//
                    new NV(MemorySortOp.Annotations.SORT_ORDER, sortOrders),//
                    new NV(MemorySortOp.Annotations.VALUE_COMPARATOR, new IVComparator()),//
                    new NV(MemorySortOp.Annotations.EVALUATION_CONTEXT,
	                       BOpEvaluationContext.CONTROLLER),//
	                new NV(MemorySortOp.Annotations.PIPELINED, true),//
	                new NV(MemorySortOp.Annotations.MAX_PARALLEL, 1),//
                    new NV(MemorySortOp.Annotations.SHARED_STATE, true),//
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
		
    	left = applyQueryHints(new SliceOp(new BOp[] { left },
    			NV.asMap(new NV[] { 
					new NV(SliceOp.Annotations.BOP_ID, bopId),
					new NV(SliceOp.Annotations.OFFSET, slice.getOffset()),
					new NV(SliceOp.Annotations.LIMIT, slice.getLimit()),
	                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
	                        BOpEvaluationContext.CONTROLLER),//
	                new NV(SliceOp.Annotations.PIPELINED, false),//
	                new NV(SliceOp.Annotations.MAX_PARALLEL, 1),//
	                new NV(MemorySortOp.Annotations.SHARED_STATE, true),//
    			})), ctx.queryHints);

    	return left;
		
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
     *         TODO It would be nice if this would only apply those query hints
     *         to an operator which are known to be annotations understood by
     *         that operator. This information is basically available from the
     *         inner Annotation interface for a given operator class, but that
     *         is not really all that accessible. [The way it is now, the query
     *         hints get sprayed onto every operator and that will make the
     *         query much fatter for NIO on a cluster.]
     */
    private static PipelineOp applyQueryHints(PipelineOp op,
            final Properties queryHints) {

        if (queryHints == null)
            return op;

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
     * If the value expression that needs the materialized variables can run
     * without a {@link NotMaterializedException} then just bypass the pipeline.
     * This happens in the case of a value expression that only "sometimes"
     * needs materialized values, but not always (i.e. materialization
     * requirement depends on the data flowing through). A good example of this
     * is {@link CompareBOp}, which can sometimes work on internal values and
     * sometimes can't.
     */
    @SuppressWarnings("rawtypes")
    private static PipelineOp addMaterializationSteps(
    		PipelineOp left, final int rightId, final IValueExpression<IV> ve,
    		final Collection<IVariable<IV>> vars, final AST2BOpContext ctx) {

        final IConstraint c2 = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                new NeedsMaterializationBOp(ve));

		left = applyQueryHints(
          	    new ConditionalRoutingOp(new BOp[]{left},
                    NV.asMap(new NV[]{//
                        new NV(BOp.Annotations.BOP_ID, ctx.nextId()),
                        new NV(ConditionalRoutingOp.Annotations.CONDITION, c2),
                        new NV(PipelineOp.Annotations.ALT_SINK_REF, rightId),
                    })), ctx.queryHints);
    	
    	return addMaterializationSteps(left, rightId, vars, ctx);
    	
    }
    
    /**
     * Adds a series of materialization steps to materialize terms needed
     * downstream. To materialize the variable <code>?term</code>, the pipeline
     * looks as follows:
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
     */
    @SuppressWarnings("rawtypes")
    private static PipelineOp addMaterializationSteps(
    		PipelineOp left, final int rightId,
    		final Collection<IVariable<IV>> vars, final AST2BOpContext ctx) {

    	final Iterator<IVariable<IV>> it = vars.iterator();
    	
    	int firstId = ctx.nextId();
    	
    	while (it.hasNext()) {
    		
    		final IVariable<IV> v = it.next();
    	
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
              	    new ConditionalRoutingOp(new BOp[]{left},
                        NV.asMap(new NV[]{//
                            new NV(BOp.Annotations.BOP_ID, condId1),
                            new NV(ConditionalRoutingOp.Annotations.CONDITION, c1),
                            new NV(PipelineOp.Annotations.SINK_REF, condId2),
                            new NV(PipelineOp.Annotations.ALT_SINK_REF, endId),
                        })), ctx.queryHints);
         
            if (log.isDebugEnabled()) {
          	    log.debug("adding 1st conditional routing op: " + condOp1);
            }
        	
            final IConstraint c2 = new SPARQLConstraint<XSDBooleanIV<BigdataLiteral>>(
                    new IsInlineBOp(v, true/* inline */));

            final PipelineOp condOp2 = applyQueryHints(
              	    new ConditionalRoutingOp(new BOp[]{condOp1},
                        NV.asMap(new NV[]{//
                            new NV(BOp.Annotations.BOP_ID, condId2),
                            new NV(ConditionalRoutingOp.Annotations.CONDITION, c2),
                            new NV(PipelineOp.Annotations.SINK_REF, inlineMaterializeId),
                            new NV(PipelineOp.Annotations.ALT_SINK_REF, lexJoinId),
                        })), ctx.queryHints);
         
            if (log.isDebugEnabled()) {
          	    log.debug("adding 2nd conditional routing op: " + condOp2);
            }
            
			final Predicate lexPred;
			{
				
				/*
				 * Note: Use the timestamp of the triple store view unless this
				 * is a read/write transaction, in which case we need to use the
				 * unisolated view in order to see any writes which it may have
				 * performed (lexicon writes are always unisolated).
				 */
				
				long timestamp = ctx.db.getTimestamp();
				if (TimestampUtility.isReadWriteTx(timestamp))
					timestamp = ITx.UNISOLATED;
				
				final String ns = ctx.db.getLexiconRelation().getNamespace();
				
				lexPred = LexPredicate.reverseInstance(ns, timestamp, v);
				
			}
            
            if (log.isDebugEnabled()) {
          	    log.debug("lex pred: " + lexPred);
            }
            
            final PipelineOp inlineMaterializeOp = applyQueryHints(
              	    new InlineMaterializeOp(new BOp[]{condOp2},
                        NV.asMap(new NV[]{//
                            new NV(BOp.Annotations.BOP_ID, inlineMaterializeId),
                            new NV(InlineMaterializeOp.Annotations.PREDICATE, lexPred.clone()),
                            new NV(PipelineOp.Annotations.SINK_REF, endId),
                        })), ctx.queryHints);

            if (log.isDebugEnabled()) {
          	    log.debug("adding inline materialization op: " + inlineMaterializeOp);
            }
            
            final PipelineOp lexJoinOp = applyQueryHints(
		      	    new PipelineJoin(new BOp[]{inlineMaterializeOp},
		                NV.asMap(new NV[]{//
		                    new NV(BOp.Annotations.BOP_ID, lexJoinId),
		                    new NV(PipelineJoin.Annotations.PREDICATE, lexPred.clone()),
                            new NV(PipelineOp.Annotations.SINK_REF, endId),
		                })), ctx.queryHints);

            if (log.isDebugEnabled()) {
          	    log.debug("adding lex join op: " + lexJoinOp);
            }
            
            left = lexJoinOp;
            
    	}
        
    	return left;
    	
    }

//    /**
//     * Return an array indicating the {@link IKeyOrder} that will be used when
//     * reading on each of the tail predicates. The array is formed using a
//     * private {@link IBindingSet} and propagating fake bindings to each
//     * predicate in turn using the given evaluation order.
//     * 
//     * @param order
//     *            The evaluation order.
//     * @param nvars
//     *            The #of unbound variables for each tail predicate is assigned
//     *            by side-effect.
//     * 
//     * @return An array of the {@link IKeyOrder}s for each tail predicate. The
//     *         array is correlated with the predicates index in the tail of the
//     *         rule NOT its evaluation order.
//     */
//    @SuppressWarnings({ "rawtypes" })
//    static private IKeyOrder[] computeKeyOrderForEachTail(final List<Predicate> preds,
//            final BOpContextBase context, final int[] order, final int[] nvars) {
//
//        if (order == null)
//            throw new IllegalArgumentException();
//
//        if (order.length != preds.size())
//            throw new IllegalArgumentException();
//
//        final int tailCount = preds.size();
//
//        final IKeyOrder[] a = new IKeyOrder[tailCount];
//        
//        final IBindingSet bindingSet = new HashBindingSet();
//        
//        final IConstant<IV> fakeTermId = new Constant<IV>(TermId.mockIV(VTE.URI));
//        
//        for (int orderIndex = 0; orderIndex < tailCount; orderIndex++) {
//
//            final int tailIndex = order[orderIndex];
//
//            final IPredicate pred = preds.get(tailIndex);
//
//            final IRelation rel = context.getRelation(pred);
//            
//            final IPredicate asBound = pred.asBound(bindingSet);
//            
//            final IKeyOrder keyOrder = context.getAccessPath(
//                    rel, asBound).getKeyOrder();
//
//            if (log.isDebugEnabled())
//                log.debug("keyOrder=" + keyOrder + ", orderIndex=" + orderIndex
//                        + ", tailIndex=" + orderIndex + ", pred=" + pred
//                        + ", bindingSet=" + bindingSet + ", preds=" +
//                        Arrays.toString(preds.toArray()));
//
//            // save results.
//            a[tailIndex] = keyOrder;
//            nvars[tailIndex] = keyOrder == null ? asBound.getVariableCount()
//                    : asBound.getVariableCount((IKeyOrder) keyOrder);
//
//            final int arity = pred.arity();
//
//            for (int j = 0; j < arity; j++) {
//
//                final IVariableOrConstant<?> t = pred.get(j);
//
//                if (t.isVar()) {
//
//                    final IVariable<?> var = (IVariable<?>) t;
//
//                    if (log.isDebugEnabled()) {
//
//                        log.debug("Propagating binding: pred=" + pred
//                                        + ", var=" + var + ", bindingSet="
//                                        + bindingSet);
//                        
//                    }
//                    
//                    bindingSet.set(var, fakeTermId);
//
//                }
//
//            }
//
//        }
//
//        if (log.isDebugEnabled()) {
//
//            log.debug("keyOrder[]=" + Arrays.toString(a) + ", nvars="
//                    + Arrays.toString(nvars) + ", preds=" + 
//                    Arrays.toString(preds.toArray()));
//
//        }
//
//        return a;
//
//    }

    @SuppressWarnings("rawtypes")
    private static final Predicate toPredicate(
    		final StatementPatternNode sp, final AST2BOpContext ctx) {
    	
    	final AbstractTripleStore database = ctx.db;
    	
    	final DatasetNode dataset = ctx.query.getDataset();
    	
        // create a solution expander for free text search if necessary
        IAccessPathExpander<ISPO> expander = null;
        
        final Value predValue = sp.p().getValue();
        if (log.isDebugEnabled()) {
            log.debug(predValue);
        }
        if (predValue != null && BD.SEARCH.equals(predValue)) {
            final Value objValue = sp.o().getValue();
            if (log.isDebugEnabled()) {
                log.debug(objValue);
            }
            if (objValue != null && objValue instanceof Literal) {
                expander = new FreeTextSearchExpander(database,
                        (Literal) objValue);
            }
        }
        
        // @todo why is [s] handled differently?
        // because [s] is the variable in free text searches, no need to test
        // to see if the free text search expander is in place
        final IVariableOrConstant<IV> s = sp.s().getValueExpression();
        if (s == null) {
            return null;
        }
        
        final IVariableOrConstant<IV> p;
        if (expander == null) {
            p = sp.p().getValueExpression();
        } else {
            p = new Constant<IV>(TermId.mockIV(VTE.BNODE));
        }
        if (p == null) {
            return null;
        }
        
        final IVariableOrConstant<IV> o;
        if (expander == null) {
            o = sp.o().getValueExpression();
        } else {
            o = new Constant<IV>(TermId.mockIV(VTE.BNODE));
        }
        if (o == null) {
            return null;
        }
        
        // The annotations for the predicate.
        final List<NV> anns = new LinkedList<NV>();
        
        final IVariableOrConstant<IV> c;
        
        if (!database.isQuads()) {
            /*
             * Either triple store mode or provenance mode.
             */
            final TermNode cTerm = sp.c();
            if (cTerm == null) {
                // context position is not used.
                c = null;
            } else {
                final Value val = cTerm.getValue();
                if (val != null && database.isStatementIdentifiers()) {
                    /*
                     * Note: The context position is used as a statement
                     * identifier (SID). SIDs may be used to retrieve provenance
                     * statements (statements about statement) using high-level
                     * query. SIDs are represented as blank nodes and is not
                     * possible to have them bound in the original query. They
                     * only become bound during query evaluation.
                     */
                    throw new IllegalArgumentException(
                            "Context position is a statement identifier and may not be bound in the original query: "
                                    + sp);
                }
                c = ((VarNode) cTerm).getValueExpression();
            }
        } else {
            /*
             * Quad store mode.
             */
            if (expander != null) {
                /*
                 * This code path occurs when we are doing a free text search
                 * for this access path using the FreeTestSearchExpander. There
                 * is no need to do any named or default graph expansion work on
                 * a free text search access path.
                 */
                c = null;
            } else {
                // the graph variable iff specified by the query.
                final TermNode cvar = sp.c();
                // quads mode.
                anns.add(new NV(Rule2BOpUtility.Annotations.QUADS, true));
                // attach the Scope.
                anns.add(new NV(Rule2BOpUtility.Annotations.SCOPE, sp
                        .getScope()));
                if (dataset == null) {
                    // attach the appropriate expander : @todo drop expanders. 
                    if (cvar == null) {
                        /*
                         * There is no dataset and there is no graph variable,
                         * so the default graph will be the RDF Merge of ALL
                         * graphs in the quad store.
                         * 
                         * This code path uses an "expander" which strips off
                         * the context information and filters for the distinct
                         * (s,p,o) triples to realize the RDF Merge of the
                         * source graphs for the default graph.
                         */
                        c = null;
                        expander = new DefaultGraphSolutionExpander(null/* ALL */);
                    } else {
                        /*
                         * There is no data set and there is a graph variable,
                         * so the query will run against all named graphs and
                         * [cvar] will be to the context of each (s,p,o,c) in
                         * turn. This handles constructions such as:
                         * 
                         * "SELECT * WHERE {graph ?g {?g :p :o } }"
                         */
                        expander = new NamedGraphSolutionExpander(null/* ALL */);
                        c = cvar.getValueExpression();
                    }
                } else { // dataset != null
                    // attach the DataSet.
                    anns.add(new NV(Rule2BOpUtility.Annotations.DATASET,
                            dataset));
                    // attach the appropriate expander : @todo drop expanders. 
                    switch (sp.getScope()) {
                    case DEFAULT_CONTEXTS: {
                        if(dataset.getDefaultGraphs()!=null){
                        /*
                         * Query against the RDF merge of zero or more source
                         * graphs.
                         */
                        expander = new DefaultGraphSolutionExpander(dataset
                                .getDefaultGraphs().getGraphs());
                        }else if(dataset.getDefaultGraphFilter()!=null){
                            anns.add(new NV(IPredicate.Annotations.INDEX_LOCAL_FILTER,
                                    ElementFilter.newInstance(dataset.getDefaultGraphFilter())));
                        }
                        /*
                         * Note: cvar can not become bound since context is
                         * stripped for the default graph.
                         */
                        if (cvar == null)
                            c = null;
                        else
                            c = cvar.getValueExpression();
                        break;
                    }
                    case NAMED_CONTEXTS: {
                        if(dataset.getNamedGraphs()!=null){
                        /*
                         * Query against zero or more named graphs.
                         */
                        expander = new NamedGraphSolutionExpander(dataset
                                .getNamedGraphs().getGraphs());
                        }else if(dataset.getNamedGraphFilter()!=null){
                            anns.add(new NV(IPredicate.Annotations.INDEX_LOCAL_FILTER,
                                    ElementFilter.newInstance(dataset.getNamedGraphFilter())));
                        }
                        if (cvar == null) {// || !cvar.hasValue()) {
                            c = null;
                        } else {
                            c = cvar.getValueExpression();
                        }
                        break;
                    }
                    default:
                        throw new AssertionError();
                    }
                }
            }
        }

        /*
         * This applies a filter to the access path to remove any inferred
         * triples when [includeInferred] is false.
         * 
         * @todo We can now stack filters so are we missing out here by not
         * layering in other filters as well? [In order to rotate additional
         * constraints onto an access path we would need to either change
         * IPredicate and AbstractAccessPath to process an IConstraint[] or
         * write a delegation pattern that let's us wrap one filter inside of
         * another.]
         */
        final IElementFilter<ISPO> filter = !ctx.query.getIncludeInferred() ? ExplicitSPOFilter.INSTANCE
                : null;

        // Decide on the correct arity for the predicate.
        final BOp[] vars;
        if (!database.isQuads() && !database.isStatementIdentifiers()) {
            vars = new BOp[] { s, p, o };
        } else if (c == null) {
            vars = new BOp[] { s, p, o, com.bigdata.bop.Var.var() };
        } else {
            vars = new BOp[] { s, p, o, c };
        }

        anns.add(new NV(IPredicate.Annotations.RELATION_NAME,
                new String[] { database.getSPORelation().getNamespace() }));//
        
        // filter on elements visited by the access path.
        if (filter != null)
            anns.add(new NV(IPredicate.Annotations.INDEX_LOCAL_FILTER,
                    ElementFilter.newInstance(filter)));

        // free text search expander or named graphs expander
        if (expander != null)
            anns.add(new NV(IPredicate.Annotations.ACCESS_PATH_EXPANDER, expander));

        // timestamp
        anns.add(new NV(Annotations.TIMESTAMP, database
                .getSPORelation().getTimestamp()));

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
        anns.add(new NV(IPredicate.Annotations.FLAGS, IRangeQuery.DEFAULT
                | IRangeQuery.PARALLEL | IRangeQuery.READONLY));
        
        return new SPOPredicate(vars, anns.toArray(new NV[anns.size()]));
//        return new SPOPredicate(
//                new String[] { database.getSPORelation().getNamespace() },
//                -1, // partitionId
//                s, p, o, c,
//                optional, // optional
//                filter, // filter on elements visited by the access path.
//                expander // free text search expander or named graphs expander
//                );
    	
    }

    /**
     * Return either <i>left</i> wrapped as the sole member of an array or
     * {@link BOp#NOARGS} iff <i>left</i> is <code>null</code>.
     * 
     * @param left
     *            The prior operator in the pipeline (optional).
     * @return The array.
     */
    static private BOp[] leftOrEmpty(final PipelineOp left) {

        return left == null ? BOp.NOARGS : new BOp[] { left };
        
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
     */
    public static final IValueExpression<? extends IV> toVE(
    		final String lex, final IValueExpressionNode node) {

    	/*
    	 * Check to see if value expression has already been created and
    	 * cached on node.
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
    		
    		final IValueExpressionNode child = assignment.getValueExpressionNode();
    		
            @SuppressWarnings("rawtypes")
    		final IValueExpression<? extends IV> ve = toVE(lex, child);
    		
    		return ve;
    		
    	} else if (node instanceof FunctionNode) {
    		
    		final FunctionNode function = (FunctionNode) node;
    		
    		final URI functionURI = function.getFunctionURI();
    		
    		final Map<String,Object> scalarValues = function.getScalarValues();
    		
    		final ValueExpressionNode[] args = 
    			function.args().toArray(new ValueExpressionNode[function.arity()]); 
    		
    		@SuppressWarnings("rawtypes")
            final IValueExpression<? extends IV> ve =
    			FunctionRegistry.toVE(lex, functionURI, scalarValues, args);
    		
    		node.setValueExpression(ve);
    		
    		return ve;
    		
    	} else {
    		
    		throw new IllegalArgumentException();
    		
    	}
    	
    }
	
}
