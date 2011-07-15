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

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.bset.EndOp;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.rdf.join.InlineMaterializeOp;
import com.bigdata.bop.solutions.ComparatorOp;
import com.bigdata.bop.solutions.DistinctBindingSetOp;
import com.bigdata.bop.solutions.ISortOrder;
import com.bigdata.bop.solutions.MemorySortOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.bop.solutions.SortOrder;
import com.bigdata.bop.solutions.SparqlBindingSetComparatorOp;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.constraints.IsInlineBOp;
import com.bigdata.rdf.internal.constraints.IsMaterializedBOp;
import com.bigdata.rdf.internal.constraints.NeedsMaterializationBOp;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.lexicon.LexPredicate;
import com.bigdata.rdf.sail.Rule2BOpUtility;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;
import com.bigdata.striterator.IKeyOrder;

public class AST2BOpUtility {

	private static final transient Logger log = Logger.getLogger(AST2BOpUtility.class);
	
	
	/**
	 * Convert an AST query plan into a set of executable pipeline operators.
	 */
	public static PipelineOp convert(final QueryRoot query,
			final AST2BOpContext ctx) {
		
		final IGroupNode root = query.getRoot();

		PipelineOp left = convert(root, ctx);
		
		/*
		 * distinct first, then order by, then limit/offset
		 * 
		 * none of these have been tested yet
		 */
		
		if (query.isDistinct()) {
			
			left = addDistinct(left, query.getProjection(), ctx);
			
		}
		
        if (query.hasOrderBy()) {
        	
    		left = addOrderBy(left, query.getOrderBy(), ctx);
        	
        }

        if (query.hasSlice()) {

        	left = addSlice(
        			left, query.getOffset(), query.getLimit(), ctx);
        	
        }
        
        return left;
		
	}
	
	private static PipelineOp convert(final IGroupNode groupNode,
			final AST2BOpContext ctx) {
		
		if (groupNode instanceof UnionNode) {
			
			return convert((UnionNode) groupNode, ctx);
			
		} else if (groupNode instanceof JoinGroupNode) {
			
			return convert((JoinGroupNode) groupNode, ctx);
			
		} else {
		
			throw new IllegalArgumentException();
			
		}
		
	}
		
	private static PipelineOp convert(final UnionNode unionNode,
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
        		
        		subqueries[i++] = convert((JoinGroupNode) child, ctx);
        		
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
        
        final PipelineOp union = applyQueryHints(new Union(new BOp[]{}, 
        		NV.asMap(anns.toArray(new NV[anns.size()]))
			), ctx.queryHints);
        
        return union;
		
	}
		
	/**
	 * Join group consists of: statement patterns, constraints, and sub-groups
	 * 
	 * Sub-groups can be either join groups (optional) or unions (non-optional)
	 * 
	 * No such thing as a non-optional sub join group (in Sparql 1.0)
	 * 
	 * No such thing as an optional statement pattern, only optional sub-groups
	 * 
	 * Optional sub-groups with at most one statement pattern can be lifted into
	 * this group using an optional PipelineJoin, but only if that sub-group
	 * has no constraints that need materialized variables. 
	 * (why? because if we put in materialization steps in between the join
	 * and the constraint, the constraint won't have the same optional semantics
	 * as the join and will lose its association with that join and its "optionality".)
	 * 
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
	 * 
	 * TODO Think about how hash joins fit into this.
	 */
	private static PipelineOp convert(final JoinGroupNode joinGroup,
			final AST2BOpContext ctx) {

		/*
		 * Place the StartOp at the beginning of the pipeline.
		 */
        PipelineOp left = addStartOp(ctx);
        
        /*
         * Add the pre-conditionals to the pipeline.
         */
        left = addConditionals(left, joinGroup.getPreFilters(), ctx);
        
        /*
         * Add the joins (statement patterns) and the filters on those joins.
         */
        left = addJoins(left, joinGroup, ctx);
        
        /*
         * Add the subqueries (individual optional statement patterns, optional
         * join groups, and nested union).
         */
        left = addSubqueries(left, joinGroup, ctx);
        
        /*
         * Add the post-conditionals to the pipeline.
         */
        left = addConditionals(left, joinGroup.getPostFilters(), ctx);

        /*
         * Add the end operator if necessary.
         */
        left = addEndOp(left, ctx);
        
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
    		preds.add(sp.getPredicate());
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
    		final StatementPatternNode sp, final Collection<FilterNode> filters,
    		final AST2BOpContext ctx) {
    	
    	final Predicate pred = sp.getPredicate();
    	
    	final Collection<IConstraint> constraints = new LinkedList<IConstraint>();
    	
    	for (FilterNode filter : filters) {
    		constraints.add(new SPARQLConstraint(filter.getValueExpression()));
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
    		
    		final PipelineOp subquery = convert(subgroup, ctx);
    		
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
    			
    			final Predicate pred = (Predicate) sp.getPredicate().setProperty(
						IPredicate.Annotations.OPTIONAL, Boolean.TRUE);
    			
    			final StatementPatternNode sp2 = new StatementPatternNode(pred);
    			
    			left = addJoin(left, sp2, filters, ctx);
    			
    		} else {
    		
	    		final PipelineOp subquery = convert(subgroup, ctx);
	    		
	    		left = new SubqueryOp(new BOp[]{left}, 
	                    new NV(Predicate.Annotations.BOP_ID, ctx.nextId()),
	                    new NV(SubqueryOp.Annotations.SUBQUERY, subquery),
	                    new NV(SubqueryOp.Annotations.OPTIONAL, true)
	            );
	    		
    		}
    		
    	}
    	
    	return left;
    	
    }
		
	private static final PipelineOp addEndOp(PipelineOp left,
			final AST2BOpContext ctx) {
		
		if (!left.getEvaluationContext().equals(BOpEvaluationContext.CONTROLLER)) {
	//			&& !(left instanceof SubqueryOp)) {
			
			/*
			 * Wrap with an operator which will be evaluated on the query
			 * controller so the results will be streamed back to the query
			 * controller in scale-out.
			 * 
			 * @todo For scale-out, we probably need to stream the results back
			 * to the node from which the subquery was issued. If the subquery
			 * is issued against the local query engine where the IBindingSet
			 * was produced, then the that query engine is the query controller
			 * for the subquery and a SliceOp on the subquery would bring the
			 * results for the subquery back to that query controller. There is
			 * no requirement that the query controller for the subquery and the
			 * query controller for the parent query be the same node. [I am not
			 * doing this currently in order to test whether there is a problem
			 * with SliceOp which interactions with SubqueryOp to allow
			 * incorrect termination under some circumstances.
			 */
	        left = new EndOp(new BOp[] { left }, 
	        		NV.asMap(
						new NV(BOp.Annotations.BOP_ID, ctx.nextId()),
						new NV(BOp.Annotations.EVALUATION_CONTEXT,
								BOpEvaluationContext.CONTROLLER)
	//					new NV(PipelineOp.Annotations.SHARED_STATE, true)
	        		));
	        
	    }
		
		return left;
        
	}
	
	/**
	 * NOT YET TESTED.
	 * 
	 * TODO TEST
	 */
	private static final PipelineOp addDistinct(PipelineOp left,
			final IVariable<?>[] vars, final AST2BOpContext ctx) {
		
//		DistinctBindingSetOp
		
		final int bopId = ctx.idFactory.incrementAndGet();
		
    	left = applyQueryHints(new DistinctBindingSetOp(new BOp[] { left }, 
                NV.asMap(new NV[]{//
                    new NV(DistinctBindingSetOp.Annotations.BOP_ID, bopId),
                    new NV(DistinctBindingSetOp.Annotations.VARIABLES, vars),
                    new NV(DistinctBindingSetOp.Annotations.EVALUATION_CONTEXT,
                            BOpEvaluationContext.CONTROLLER),
                    new NV(DistinctBindingSetOp.Annotations.SHARED_STATE,
                            true),
                })), ctx.queryHints);
				
    	return left;
		
	}
			
	/**
	 * NOT YET TESTED.
	 * 
	 * TODO TEST
	 */
	private static final PipelineOp addOrderBy(PipelineOp left,
			final List<OrderByNode> orderBys, final AST2BOpContext ctx) {
		
//		MemorySortOp
		
		final Set<IVariable<IV>> vars = new LinkedHashSet<IVariable<IV>>();
		
		final ISortOrder[] sortOrders = new ISortOrder[orderBys.size()];
		
		final Iterator<OrderByNode> it = orderBys.iterator();
		
		for (int i = 0; it.hasNext(); i++) {
			
    		final OrderByNode orderBy = it.next();
    		
    		final IVariable<IV> var = orderBy.getVar();
    		
    		final boolean ascending = orderBy.isAscending();
    		
    		vars.add(var);

    		sortOrders[i++] = new SortOrder(var, ascending);
    		
		}
		
		final int sortId = ctx.idFactory.incrementAndGet();
		
		left = addMaterializationSteps(left, sortId, vars, ctx);
		
		final ComparatorOp compareOp = 
			new SparqlBindingSetComparatorOp(sortOrders);

    	left = applyQueryHints(new MemorySortOp(new BOp[] { left }, 
    			NV.asMap(new NV[] {//
	                new NV(MemorySortOp.Annotations.BOP_ID, sortId),//
					new NV(MemorySortOp.Annotations.COMPARATOR, compareOp),//
	                new NV(MemorySortOp.Annotations.EVALUATION_CONTEXT,
	                        BOpEvaluationContext.CONTROLLER),//
	                new NV(MemorySortOp.Annotations.PIPELINED, false),//
	                new NV(MemorySortOp.Annotations.MAX_PARALLEL, 1),//
    			})), ctx.queryHints);

		
    	return left;
		
	}
			
	/**
	 * NOT YET TESTED.
	 * 
	 * TODO TEST
	 */
	private static final PipelineOp addSlice(PipelineOp left,
			final long offset, final long limit, final AST2BOpContext ctx) {
		
		final int bopId = ctx.idFactory.incrementAndGet();
		
    	left = applyQueryHints(new SliceOp(new BOp[] { left },
    			NV.asMap(new NV[] { 
					new NV(SliceOp.Annotations.BOP_ID, bopId),
					new NV(SliceOp.Annotations.OFFSET, offset),
					new NV(SliceOp.Annotations.LIMIT, limit),
	                new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
	                        BOpEvaluationContext.CONTROLLER),//
	                new NV(SliceOp.Annotations.PIPELINED, false),//
	                new NV(SliceOp.Annotations.MAX_PARALLEL, 1),//
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
     *       really all that accessible.
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

	
}
