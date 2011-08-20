package com.bigdata.rdf.sparql.ast;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
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
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.EndOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.bop.controller.Union;
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
import com.bigdata.bop.solutions.ProjectionOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SortOrder;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.htree.HTree;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSDBooleanIV;
import com.bigdata.rdf.internal.constraints.BindingConstraint;
import com.bigdata.rdf.internal.constraints.ConditionalBind;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IsInlineBOp;
import com.bigdata.rdf.internal.constraints.IsMaterializedBOp;
import com.bigdata.rdf.internal.constraints.NeedsMaterializationBOp;
import com.bigdata.rdf.internal.constraints.ProjectedConstraint;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.TryBeforeMaterializationConstraint;
import com.bigdata.rdf.lexicon.LexPredicate;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sail.FreeTextSearchExpander;
import com.bigdata.rdf.sail.Rule2BOpUtility;
import com.bigdata.rdf.spo.DefaultGraphSolutionExpander;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.NamedGraphSolutionExpander;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;
import com.bigdata.striterator.IKeyOrder;

/**
 * Query plan generator converts an AST into a query plan made up of
 * {@link PipelineOp}s
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AST2BOpUtility {

    private static final transient Logger log = Logger
            .getLogger(AST2BOpUtility.class);

	/**
	 * Convert an AST query plan into a set of executable pipeline operators.
	 */
	public static PipelineOp convert(final AST2BOpContext ctx) {
		
		final QueryRoot query = ctx.query;
		
        final IGroupNode root = query.getRoot();

        if (root == null)
            throw new IllegalArgumentException("No group node");

		PipelineOp left = null;

        final NamedSubqueriesNode namedSubqueries = query.getNamedSubqueries();

        if (namedSubqueries != null && !namedSubqueries.isEmpty()) {

            // WITH ... AS [name] ... INCLUDE style subquery declarations.
            left = addNamedSubqueries(left, namedSubqueries, root, ctx);
            
		}

        // The top-level "WHERE" clause.
        left = convert(left, root, ctx);

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

            if (projection.isDistinct()) {

                left = addDistinct(left, projection.getProjectionVars(), ctx);

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

        left = addEndOp(left, ctx);
        
        /*
         * TODO Do we need to add operators for materialization of any remaining
         * variables which are being projected out of the query?
         * 
         * TODO We do not want to force materialization for subqueries.  They
         * should stay as IVs.
         */
        
        if (log.isInfoEnabled()) {
            log.info("ast:\n" + query);
            log.info("pipeline:\n" + BOpUtility.toString(left));
        }
        
        return left;
		
	}

    /**
     * Add pipeline operators for named subquery solution sets. The solution
     * sets will be computed before the rest of the query plan runs. They may be
     * referenced from other parts of the query plan.
     * 
     * @param left
     * @param subqueries
     * @param root
     *            The top-level {@link IGroupNode} for the query. This is
     *            required because we need to understand the join variables
     *            which will be used when the subquery result is joined back
     *            into the query For WITH AS INCLUDE style subqueries.
     * @param ctx
     * @return
     * 
     *         FIXME Integrate named subquery support here. We need to examine
     *         the places where the named subquery is used to identify the best
     *         join vars to use when buffering the temporary solution set. The
     *         named subquery solutions will be joined into the query by a
     *         {@link NamedSubqueryInclude} within one or more
     *         {@link IGroupNode}s. The main use case for WITH AS INCLUDE is a
     *         heavy subquery which should be run first. However, in priciple,
     *         we could run these where the INCLUDE appears rather than up front
     *         as long as we are careful not to project bindings into the
     *         subquery (other than those it projects out). Whether or not this
     *         is more efficient depends on the subquery and how it is combined
     *         into the rest of the query. (Running it first should be the
     *         default policy.)
     */
    private static PipelineOp addNamedSubqueries(PipelineOp left,
            NamedSubqueriesNode subqueries, IGroupNode root, AST2BOpContext ctx) {

        log.error("NAMED SUBQUERY NOT SUPPORTED YET: " + subqueries);
        
        return left;
        
    }
    
    /**
     * Add a join against a pre-computed temporary solution set into a join
     * group.
     * 
     * FIXME This will be a hash join against the named materialized solution
     * set (which will be on an {@link HTree}).
     */
    private static PipelineOp addSubqueryInclude(PipelineOp left,
            final NamedSubqueryInclude subquery, final AST2BOpContext ctx) {

        log.error("NAMED SUBQUERY NOT SUPPORTED YET: " + subquery);

        return left;

    }

    /**
     * Add an explicit SPARQL 1.1 subquery into a join group.
     * 
     * FIXME That's basically how we need to run the SPARQL 1.1 subqueries. We
     * can handle the bottom up evaluation semantics by stripping out any
     * bindings which are not being projected out of the subquery with the first
     * operator in the subquery. That way the original bindings flow through the
     * parent, the subquery runs with only those things bound which it projects,
     * and the join happens in the parent (SubqueryOp) based on only those
     * variables projected out of the subquery.
     */
    private static PipelineOp addSparqlSubquery(PipelineOp left,
            final SubqueryRoot subquery, final AST2BOpContext ctx) {
        
        log.error("EXPLICIT SUBQUERY NOT SUPPORTED YET: " + subquery);
        
        return left;
        
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
            
            if(isObviousAggregate(arg)) // recursion.
                return true;
            
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
    private static PipelineOp convert(PipelineOp left, final IGroupNode groupNode,
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
     */
	private static PipelineOp convertUnion(PipelineOp left,
	        final UnionNode unionNode,
			final AST2BOpContext ctx) {
		
        final int arity = unionNode.getChildCount();
        
		if (arity == 0) {
			throw new IllegalArgumentException();
		}
		
        // The bopId for the UNION or STEP.
        final int thisId = ctx.idFactory.incrementAndGet();
        
        final BOp[] subqueries = new BOp[arity];

        int i = 0;
        for (IQueryNode child : unionNode) {
        	
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
        				"illegal child type for union: " + child.getClass());
        		
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
                new Union((left == null ? new BOp[] {} : new BOp[] { left }),
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
     * 3. Join the statement patterns. Use the static optimizer to attach
     * constraints to joins. Lots of funky stuff with materialization and
     * named / default graph joins.
     * 
     * 4. Pipeline the non-optional sub-groups (unions). Make sure it's not
     * an empty union.
     * 
     * 5. Pipeline the optional sub-groups (join groups). Lift single optional
     * statement patterns into this group (avoid the subquery op) per the
     * instructions above regarding materialization and constraints.
     * 
     * 6. Pipeline the postConditionals. Add materialization steps as needed.
     * </pre>
     * 
     * TODO Think about how hash joins fit into this.
     * 
     * FIXME Integrate support for references to the named solution set. Those
     * references are {@link NamedSubqueryInclude}s. They may appear within any
     * {@link IGroupNode}.
     * 
     * FIXME Integrate support for SPARQL 1.1 style subqueries. Those are
     * modeled by {@link SubqueryRoot} (versus {@link NamedSubqueryRoot}).
     */
	private static PipelineOp convertJoinGroup(PipelineOp left,
	        final JoinGroupNode joinGroup,
			final AST2BOpContext ctx) {

		/*
		 * Place the StartOp at the beginning of the pipeline.
		 */
//        PipelineOp left = addStartOp(ctx);
	    if(left == null)
	        left = addStartOp(ctx);
        
        /*
         * Add the pre-conditionals to the pipeline.
         */
        left = addConditionals(left, joinGroup.getPreFilters(), ctx);
        
        /*
         * Add the joins (statement patterns) and the filters on those joins.
         */
        left = addJoins(left, joinGroup, ctx);

        /*
         * Add SPARQL 1.1 style subqueries. 
         */
        for (IQueryNode child : joinGroup) {
            if (child instanceof SubqueryRoot)
                left = addSparqlSubquery(left, (SubqueryRoot) child, ctx);
        }
        
        /*
         * Add joins against named solution sets from WITH AS INCLUDE style
         * subqueries. 
         */
        for (IQueryNode child : joinGroup) {
            if (child instanceof NamedSubqueryInclude)
                left = addSubqueryInclude(left, (NamedSubqueryInclude) child,
                        ctx);
        }

        /*
         * Add the subqueries (individual optional statement patterns, optional
         * join groups, and nested union).
         */
        left = addSubqueries(left, joinGroup, ctx);
        
        /*
         * Add the LET assignments to the pipeline.
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
        		new StartOp(BOpBase.NOARGS,
			        NV.asMap(new NV[] {//
			              new NV(Predicate.Annotations.BOP_ID, 
			            		  ctx.idFactory.incrementAndGet()),
			              new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
			                      BOpEvaluationContext.CONTROLLER),
			        })), ctx.queryHints);

        return start;
        
	}
	
    /**
     * Project select expressions (non-aggregation case).
     * 
     * TODO Should we use or discard the {@link ProjectionOp} here? Does it
     * offer any advantage?
     */
    private static final PipelineOp addProjectedAssigments(PipelineOp left,
	        final List<AssignmentNode> assignments,
	        final AST2BOpContext ctx){
	    
//	    PipelineOp subq=left;
        
//        left=addStartOp(ctx);
//        
//        left = new SubqueryOp(new BOp[]{left}, 
//                new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),
//                new NV(SubqueryOp.Annotations.SUBQUERY, subq),
//                new NV(SubqueryOp.Annotations.OPTIONAL, false));
//   
        left = addAssignments(left, assignments, ctx, true/* projection */);
        
//        left = addEndOp(left, ctx);
        
        return left;
	}
	
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
			vars.addAll(assignmentNode.getVarsToMaterialize());
			
			/*
			 * Remove the ones we've already done.
			 */
			vars.removeAll(done);
			
            final int bopId = ctx.idFactory.incrementAndGet();

            final ConditionalBind b = new ConditionalBind(
                    assignmentNode.getVar(),
                    assignmentNode.getValueExpression());

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
	
    private static final PipelineOp addConditionals(PipelineOp left,    		
    		final Collection<FilterNode> filters,
            final AST2BOpContext ctx) {

		final Set<IVariable<IV>> done = new LinkedHashSet<IVariable<IV>>();

		for (FilterNode filter : filters) {

			final IValueExpression ve = filter.getValueExpression();
			
			final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();
			
			/*
			 * Get the vars this filter needs materialized.
			 */
			vars.addAll(filter.getVarsToMaterialize());
			
			/*
			 * Remove the ones we've already done.
			 */
			vars.removeAll(done);
			
			final int bopId = ctx.idFactory.incrementAndGet();

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
		
			final IConstraint c = new SPARQLConstraint(ve);
			
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
    		constraints.add(new SPARQLConstraint(filter.getValueExpression()));
    	}
    	
    	final IRule rule = new Rule(
    			"null", // name
    			null, // head
    			preds.toArray(new IPredicate[preds.size()]), // tails
    			constraints.size() > 0 // constraints 
    					? constraints.toArray(new IConstraint[constraints.size()]) 
						: null
				);
    	
    	// just for now
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
    
    private static final PipelineOp addJoin(PipelineOp left,
    		final Predicate<?> pred, final Collection<FilterNode> filters,
    		final AST2BOpContext ctx) {
    	
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
     * Adds subqueries for join groups to the pipeline (this is NOT SPARQL 1.1
     * subquery).
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
    	for (IQueryNode child : joinGroup) {
    		
    		if (!(child instanceof IGroupNode)) {
    			continue;
    		}
    		
    		final IGroupNode subgroup = (IGroupNode) child;
    		
    		if (subgroup.isOptional()) {
    			continue;
    		}
    		
            final PipelineOp subquery = convert(null/* left */, subgroup, ctx);
    		
    		left = new SubqueryOp(new BOp[]{left}, 
                    new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),
                    new NV(SubqueryOp.Annotations.SUBQUERY, subquery),
                    new NV(SubqueryOp.Annotations.OPTIONAL, false)
            );

    	}
    	
    	/*
    	 * Next do the optional sub-group 
    	 */
    	for (IQueryNode child : joinGroup) {
    		
    		if (!(child instanceof IGroupNode)) {
    			continue;
    		}
    		
    		final IGroupNode subgroup = (IGroupNode) child;
    		
    		if (!subgroup.isOptional()) {
    			continue;
    		}
    		
    		if (subgroup instanceof JoinGroupNode &&
    				((JoinGroupNode) subgroup).isSimpleOptional()) {
    			
    			final JoinGroupNode subJoinGroup = (JoinGroupNode) subgroup;
    			
    			final StatementPatternNode sp = subJoinGroup.getSimpleOptional();
    			
    			final Collection<FilterNode> filters = subJoinGroup.getFilters();
    			
                final Predicate<?> pred = (Predicate<?>) toPredicate(sp, ctx)
                        .setProperty(IPredicate.Annotations.OPTIONAL,
                                Boolean.TRUE);
    			
//    			final StatementPatternNode sp2 = new StatementPatternNode(pred);
    			
    			left = addJoin(left, pred, filters, ctx);
    			
    		} else {
    		
                final PipelineOp subquery = convert(null/* left */, subgroup,
                        ctx);
	    		
	    		left = new SubqueryOp(new BOp[]{left}, 
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
            final IVariable<?>[] vars, final AST2BOpContext ctx) {
		
		final int bopId = ctx.idFactory.incrementAndGet();
		
		final PipelineOp op;
		if(true) {
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

        final int bopId = ctx.idFactory.incrementAndGet();

        final GroupByOp op;
        
        /* FIXME Review. I believe that AssignmentNode.getValueExpression() should
         * always return the Bind().
         */
        final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();

        if (projectExprs != null) {
            for (IValueExpression expr : projectExprs) {
                if(expr instanceof Bind){
                    vars.add(((Bind)expr).getVar());
                    expr=((Bind)expr).getExpr();
                }
                if (expr instanceof IVariable<?>) {

                    vars.add((IVariable<IV>) expr);

                } else if (expr instanceof INeedsMaterialization) {

                    vars.addAll(((INeedsMaterialization) expr).getTermsToMaterialize());

                }
            }

        }

        if (groupByExprs != null) {
            for (IValueExpression expr : groupByExprs) {
                if(expr instanceof Bind){
                    vars.add(((Bind)expr).getVar());
                    expr=((Bind)expr).getExpr();
                }
                if (expr instanceof IVariable<?>) {

                    vars.add((IVariable<IV>) expr);

                } else if (expr instanceof INeedsMaterialization) {

                    vars.addAll(((INeedsMaterialization) expr).getTermsToMaterialize());

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
     * NOT YET TESTED.
     * 
     * TODO TEST
     * 
     * TODO Minor optimization: A bare constant in the ORDER BY value expression
     * list should be dropped. If there are no remaining value expressions, then
     * the entire ORDER BY operation should be dropped.
     */
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

            } else if (expr instanceof INeedsMaterialization) {

                vars.addAll(((INeedsMaterialization) expr)
                        .getTermsToMaterialize());
                
            }

    		sortOrders[i++] = new SortOrder(expr, orderByExpr.isAscending());
    		
		}
		
		final int sortId = ctx.idFactory.incrementAndGet();
		
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

		final int bopId = ctx.idFactory.incrementAndGet();
		
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
     * @todo It would be nice if this would only apply those query hints to an
     *       operator which are known to be annotations understood by that
     *       operator. This information is basically available from the inner
     *       Annotation interface for a given operator class, but that is not
     *       really all that accessible. [The way it is now, the query hints get
     *       sprayed onto every operator and that will make the query much
     *       fatter for NIO on a cluster.]
     */
    public static PipelineOp applyQueryHints(PipelineOp op,
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

    public static PipelineOp addMaterializationSteps(
    		PipelineOp left, final int rightId, final IValueExpression ve,
    		final Collection<IVariable<IV>> vars, final AST2BOpContext ctx) {

		/*
		 * If the value expression that needs the materialized variables can run
		 * without a NotMaterializedException then just bypass the pipeline.
		 * This happens in the case of a value expression that only "sometimes"
		 * needs materialized values, but not always (i.e. materialization
		 * requirement depends on the data flowing through). A good example of
		 * this is CompareBOp, which can sometimes work on internal values and
		 * sometimes can't.
		 */
		final IConstraint c2 = 
    			new SPARQLConstraint(new NeedsMaterializationBOp(ve));
		
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
     * downstream.
     * 
     * To materialize the variable ?term, the pipeline looks as follows:
     * 
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
     * 
     * @param left
     * 			the left (upstream) operator that immediately proceeds the
     * 			materialization steps
     * @param rightId
     * 			the id of the right (downstream) operator that immediately 
     * 			follows the materialization steps
     * @param vars
     * 			the terms to materialize
     * @return
     * 			the final bop added to the pipeline by this method
     */
    public static PipelineOp addMaterializationSteps(
    		PipelineOp left, final int rightId,
    		final Collection<IVariable<IV>> vars, final AST2BOpContext ctx) {

    	final AtomicInteger idFactory = ctx.idFactory;
    	
    	final Iterator<IVariable<IV>> it = vars.iterator();
    	
    	int firstId = idFactory.incrementAndGet();
    	
    	while (it.hasNext()) {
    		
    		final IVariable<IV> v = it.next();
    	
    		final int condId1 = firstId;
    		final int condId2 = idFactory.incrementAndGet();
    		final int inlineMaterializeId = idFactory.incrementAndGet();
            final int lexJoinId = idFactory.incrementAndGet();
            
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
    			endId = firstId = idFactory.incrementAndGet();
    			
    		}
    		
    		final IConstraint c1 = new SPARQLConstraint(new IsMaterializedBOp(v, false));
    		
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
        	
    		final IConstraint c2 = new SPARQLConstraint(new IsInlineBOp(v, true));
    		
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
    static private IKeyOrder[] computeKeyOrderForEachTail(final List<Predicate> preds,
            final BOpContextBase context, final int[] order, final int[] nvars) {

        if (order == null)
            throw new IllegalArgumentException();

        if (order.length != preds.size())
            throw new IllegalArgumentException();

        final int tailCount = preds.size();

        final IKeyOrder[] a = new IKeyOrder[tailCount];
        
        final IBindingSet bindingSet = new HashBindingSet();
        
        final IConstant<IV> fakeTermId = new Constant<IV>(TermId.mockIV(VTE.URI));
        
        for (int orderIndex = 0; orderIndex < tailCount; orderIndex++) {

            final int tailIndex = order[orderIndex];

            final IPredicate pred = preds.get(tailIndex);

            final IRelation rel = context.getRelation(pred);
            
            final IPredicate asBound = pred.asBound(bindingSet);
            
            final IKeyOrder keyOrder = context.getAccessPath(
                    rel, asBound).getKeyOrder();

            if (log.isDebugEnabled())
                log.debug("keyOrder=" + keyOrder + ", orderIndex=" + orderIndex
                        + ", tailIndex=" + orderIndex + ", pred=" + pred
                        + ", bindingSet=" + bindingSet + ", preds=" +
                        Arrays.toString(preds.toArray()));

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
                    + Arrays.toString(nvars) + ", preds=" + 
                    Arrays.toString(preds.toArray()));

        }

        return a;

    }

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
            p = new Constant(TermId.mockIV(VTE.BNODE));
        }
        if (p == null) {
            return null;
        }
        
        final IVariableOrConstant<IV> o;
        if (expander == null) {
            o = sp.o().getValueExpression();
        } else {
            o = new Constant(TermId.mockIV(VTE.BNODE));
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
                c = ((VarNode) cTerm).getVar();
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

//        /*
//         * This applies a filter to the access path to remove any inferred
//         * triples when [includeInferred] is false.
//         * 
//         * @todo We can now stack filters so are we missing out here by not
//         * layering in other filters as well? [In order to rotate additional
//         * constraints onto an access path we would need to either change
//         * IPredicate and AbstractAccessPath to process an IConstraint[] or
//         * write a delegation pattern that let's us wrap one filter inside of
//         * another.]
//         */
//        final IElementFilter<ISPO> filter = 
//            !tripleSource.includeInferred ? ExplicitSPOFilter.INSTANCE
//                : null;

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
        
//        // filter on elements visited by the access path.
//        if (filter != null)
//            anns.add(new NV(IPredicate.Annotations.INDEX_LOCAL_FILTER,
//                    ElementFilter.newInstance(filter)));

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
	
}
