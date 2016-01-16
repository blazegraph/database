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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlan2;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IReorderableNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;

/**
 * This is an AST optimizer port of the old "static" optimizer - 
 * {@link DefaultEvaluationPlan2}.  This optimizer uses range counts and simple
 * shared variable heuristics to order the statement patterns within a particular
 * join group.  This optimizer extends the old static optimizer in that child
 * join groups consider the ordering of statement patterns in their parent and
 * ancestors when deciding how their own order.
 * <p>
 * We want to optimize all {@link JoinGroupNode}s recursively, from the top down.
 * This is because the join group needs to take into account its ancestral join
 * ordering when deciding its own join ordering. A statement pattern with a 
 * shared variable with the ancestral groups should be preferred over one with
 * no shared variables.
 */
public class ASTStaticJoinOptimizer implements IASTOptimizer {

    public static final Logger log = Logger
            .getLogger(ASTStaticJoinOptimizer.class);

    public interface Annotations extends AST2BOpBase.Annotations {
    	
        /**
         * The value of this query hint determines how optimistic the optimizer
         * will be in selecting the join cardinality for its joins. Basically
         * when there is a join that has both shared and unshared variables, the
         * join cardinality will be somewhere in between the cardinality of the
         * two sides (range count for a statement pattern, previous join
         * cardinality for a predecessor join). The default value is
         * <code>1.0</code>, which is the historical behavior.
         * <p>
         * For a value of <code>0.67</code> the optimizer takes a mostly
         * optimistic view by default - the join cardinality will be <code>0.67
         * * the MIN + 0.33 * the MAX</code>. This settting will eliminate some
         * of the worst possible outcomes (ones where we guess wrong and get a
         * very bad join order as a result).
         * <p>
         * BSBM BI Q1 is a good example of a query that benefits from the
         * pessimistic approach, and LUBM Q2 is a good example of a query that
         * benefits from the optimistic approach.
         */
    	String OPTIMISTIC = ASTStaticJoinOptimizer.class.getName()+".optimistic";
    	
    	/**
    	 * See {@link #OPTIMISTIC}.
    	 */
    	Double DEFAULT_OPTIMISTIC = 1.0d;
    	
    }
    
    /**
     * Return the exogenous bindings.
     * <p>
     * Note: This is considering only a single exogenous solution. It can not
     * really use more than one solution to estimate the range counts unless it
     * does the sum across all exogenous solutions and then somehow combines
     * that information in order to make a decision on a single query plan which
     * is "best" overall for those solutions.
     * <p>
     * This takes the simplifying assumption that each solution will have the
     * same pattern of bindings. This is not true of necessity, but it will be
     * true (for example) if the BINDINGS are from the openrdf API (just one
     * exogenous solution) or if the BINDINGS are being sent with a SERVICE call
     * and were generated by some pattern of non-optional JOINs.
     * <p>
     * This can get things wrong if there are variables which are only bound in
     * some of the solutions. The RTO is insensitive to that because it will
     * feed all source solutions into the first cutoff joins and thus capture
     * the estimated costs for the data, the query, and the source bindings.
     * 
     * @param bindingSets
     *            The given solutions (optional).
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/412
     *      (StaticAnalysis#getDefinitelyBound() ignores exogenous variables.)
     */
    static private IBindingSet getExogenousBindings(
            final IBindingSet[] bindingSets) {

        if (bindingSets == null || bindingSets.length == 0)
            return null;

        return bindingSets[0];

    }
    
    @Override
    public QueryNodeWithBindingSet optimize(
       final AST2BOpContext context, final QueryNodeWithBindingSet input) {

       final IQueryNode queryNode = input.getQueryNode();
       final IBindingSet[] bindingSets = input.getBindingSets();     


//    	{
//    		
//	    	final QueryOptimizerEnum optimizer = 
//	    		context == null || context.queryHints == null 
//	    			? QueryOptimizerEnum.Static
//	                : QueryOptimizerEnum.valueOf(context.queryHints.getProperty(
//	                        QueryHints.OPTIMIZER, QueryOptimizerEnum.Static
//	                                .toString()));
//	    	
//	    	if (optimizer != QueryOptimizerEnum.Static)
//	    		return queryNode;
//	    	
//    	}
    	
        if (!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);

        if (log.isDebugEnabled()) {
        	log.debug("before:\n"+queryNode);
        }
        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final IBindingSet exogenousBindings = getExogenousBindings(bindingSets);
        
        // Named subqueries
        if (queryRoot.getNamedSubqueries() != null) {

            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            /*
             * Note: This loop uses the current size() and get(i) to avoid
             * problems with concurrent modification during visitation.
             */
            for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                @SuppressWarnings("unchecked")
				final GraphPatternGroup<IGroupMemberNode> whereClause = 
                	(GraphPatternGroup<IGroupMemberNode>) namedSubquery.getWhereClause();

                if (whereClause != null) {

                    optimize(context, exogenousBindings, queryRoot, new IJoinNode[] { }, whereClause);

                }

            }

        }

        // Main WHERE clause
        {

            @SuppressWarnings("unchecked")
			final GraphPatternGroup<IGroupMemberNode> whereClause = 
            	(GraphPatternGroup<IGroupMemberNode>) queryRoot.getWhereClause();

            if (whereClause != null) {

                optimize(context, exogenousBindings, queryRoot, new IJoinNode[] { }, whereClause);
                
            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        if (log.isDebugEnabled()) {
        	log.debug("after:\n"+queryNode);
        }
        
        return new QueryNodeWithBindingSet(queryNode, bindingSets);

    }

    /**
     * Return <code>true</code> if the static join optimizer is enabled for the
     * given join group.
     */
    static boolean isStaticOptimizer(final IEvaluationContext context,
            final JoinGroupNode joinGroup) {

        return QueryOptimizerEnum.Static.equals(joinGroup.getQueryOptimizer());

//        QueryOptimizerEnum optimizer = null;
//
//        if (joinGroup.getProperty(QueryHints.OPTIMIZER) != null) {
//
////            optimizer = QueryOptimizerEnum.valueOf(joinGroup
////                    .getQueryHint(QueryHints.OPTIMIZER));
//            optimizer = (QueryOptimizerEnum) joinGroup
//                    .getProperty(QueryHints.OPTIMIZER);
//            
//            return optimizer == QueryOptimizerEnum.Static;
//            
////        } else {
////
////            optimizer = context == null || context.queryHints == null ? QueryOptimizerEnum.Static
////                    : QueryOptimizerEnum.valueOf(context.queryHints
////                            .getProperty(QueryHints.OPTIMIZER,
////                                    QueryOptimizerEnum.Static.toString()));
//
//        }
//
////        return optimizer == QueryOptimizerEnum.Static;
//        return QueryHints.DEFAULT_OPTIMIZER == QueryOptimizerEnum.Static;

    }
    
    
    abstract private class GroupNodeOptimizer<T extends GraphPatternGroup<?>> {
    	final T op;
    	final AST2BOpContext ctx;
    	private final IBindingSet exogenousBindings;
    	final QueryRoot queryRoot;
		public GroupNodeOptimizer(AST2BOpContext ctx,
				IBindingSet exogenousBindings, QueryRoot queryRoot,
				IBindingProducerNode[] ancestry, T op) {
			this.op = op;
			this.ctx = ctx;
			this.exogenousBindings = exogenousBindings;
			this.queryRoot = queryRoot;
		}

		public void optimizex() {
			optimizeThisLevel();
			optimizeRecursively();
		}

		private void optimizeRecursively() {
	    	
	        /*
	         * Recursion, but only into group nodes (including within subqueries).
	         */
	        for (int i = 0; i < op.arity(); i++) {

	            final BOp child = op.get(i);

	            if (child instanceof GraphPatternGroup<?>) {

	                @SuppressWarnings("unchecked")
	                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) child;

	                optimize(ctx, exogenousBindings, queryRoot, getAncestry(), childGroup);
	                
	            } else if (child instanceof QueryBase) {

	                final QueryBase subquery = (QueryBase) child;

	                @SuppressWarnings("unchecked")
	                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) subquery
	                        .getWhereClause();

	                /*
	                 * Only the projected variables are in scope in the subquery.
	                 */

	                final Set<IVariable<?>> projectedVars = subquery
	                        .getProjectedVars(new LinkedHashSet<IVariable<?>>());

	                final IVariable<?>[] variablesToKeep = BOpUtility
	                        .toArray(projectedVars.iterator());
	                
	                final IBindingSet tmp = exogenousBindings == null ? null
	                        : exogenousBindings.copy(variablesToKeep);
	                
	                optimize(ctx, tmp, queryRoot, getAncestry(), childGroup);

	            }
	            
	            afterOptimizingChild(child);
	        }
			
		}

		abstract void afterOptimizingChild(BOp child);

		abstract IBindingProducerNode[] getAncestry();

		abstract void optimizeThisLevel() ;
    	
    }
    
    private class JoinGroupNodeOptimizer extends GroupNodeOptimizer<JoinGroupNode> {

		final List<IBindingProducerNode> ancestry;
		public JoinGroupNodeOptimizer(AST2BOpContext ctx,
				IBindingSet exogenousBindings, QueryRoot queryRoot,
				IBindingProducerNode[] ancestry, JoinGroupNode joinGroup) {
			super(ctx,exogenousBindings,queryRoot,ancestry,joinGroup);
			this.ancestry = new LinkedList<IBindingProducerNode>(Arrays.asList(ancestry));
			/*
			 * Look for service calls and named subquery includes, since they 
			 * will get run before the statement pattern nodes. Add them into 
			 * the ancestry.
			 */
			addToAncestry(joinGroup.getServiceNodes(),"service node");
			addToAncestry(joinGroup.getNamedSubqueryIncludes(),"named subquery include");
		}

		private void addToAncestry(List<? extends IBindingProducerNode> moreAncestors, String dbgMessage) {
			for (IBindingProducerNode ancestor : moreAncestors) {
				if (log.isDebugEnabled()) {
					log.debug("adding a "+dbgMessage+" to ancestry:" +ancestor);
				}
				ancestry.add(ancestor);
			}
		}

		@Override
		void afterOptimizingChild(BOp child) {

            /*
             * Update the ancestry for recursion. Only add the
             * non-optional statement pattern nodes - the ones that we
             * can count on to bind their variables.
             */
            if (child instanceof IBindingProducerNode) {
            	if (child instanceof IJoinNode) {
            		IJoinNode ijn = (IJoinNode)child;
            		if (ijn.isOptional() || ijn.isMinus() ) {
            			return;
            		}
            			
            	}
            	ancestry.add((IBindingProducerNode)child);
            	
            }
		}

		@Override
		IBindingProducerNode[] getAncestry() {
			return ancestry.toArray(new IBindingProducerNode[ancestry.size()]);
		}

		@Override
		void optimizeThisLevel() {

            if (isStaticOptimizer(ctx, op)) {

	    		optimizeJoinGroup(ctx, queryRoot, getAncestry(), op); 
	        }
		}
    	
    }
    private class UnionNodeOptimizer extends GroupNodeOptimizer<UnionNode> {

    	final IBindingProducerNode[] ancestry;
		public UnionNodeOptimizer(AST2BOpContext ctx,
				IBindingSet exogenousBindings, QueryRoot queryRoot,
				IBindingProducerNode[] ancestry, UnionNode op) {
			super(ctx,exogenousBindings,queryRoot,ancestry,op);
			this.ancestry = ancestry;
		}

		@Override
		void afterOptimizingChild(BOp child) {
			// nothing to do
		}

		@Override
		IBindingProducerNode[] getAncestry() {
			return ancestry;
		}

		@Override
		void optimizeThisLevel() {
			// don't.
		}
    	
    }
    
    
    private GroupNodeOptimizer<?> createGroupNodeOptimizer(final AST2BOpContext ctx,
            final IBindingSet exogenousBindings, final QueryRoot queryRoot,
            IBindingProducerNode[] ancestry, final GraphPatternGroup<?> op) {
    	if (op instanceof JoinGroupNode) {
    		return new JoinGroupNodeOptimizer(ctx,exogenousBindings,queryRoot,ancestry,(JoinGroupNode)op);
    	} else if (op instanceof UnionNode) {
    		return new UnionNodeOptimizer(ctx,exogenousBindings,queryRoot,ancestry,(UnionNode)op);
    	} else {
    		throw new IllegalArgumentException("Unexpected subclass of GraphPatternGroup");
    	}
    }
    
    /**
     * 
     * @param ctx
     * @param exogenousBindings
     *            The exogenous bindings -or- <code>null</code> iff there are
     *            none.
     * @param queryRoot
     * @param ancestry  The nodes that are known to have already bound their variables.
     * @param op
     */
    private void optimize(final AST2BOpContext ctx,
            final IBindingSet exogenousBindings, final QueryRoot queryRoot,
            IBindingProducerNode[] ancestry, final GraphPatternGroup<?> op) {

    	createGroupNodeOptimizer(ctx,exogenousBindings,queryRoot,ancestry,op).optimizex();

    }


	private void optimizeJoinGroup(final AST2BOpContext ctx,
			final QueryRoot queryRoot, IBindingProducerNode[] ancestry,
			final JoinGroupNode joinGroup) {
		/*
		 * Let the optimizer handle the simple optionals too.
		 */
		final List<IReorderableNode> nodes = joinGroup.getReorderableChildren();
		
		if (!nodes.isEmpty()) {

		    /*
		     * Find the "slots" where the reorderable nodes currently
		     * show up in the join group. We will later fill in these
		     * slots with the same nodes, but in a
		     * different (optimized) ordering.
		     */
		    final int[] slots = new int[nodes.size()];
		    {
		        int j = 0;
		        for (int i = 0; i < joinGroup.arity() && j < nodes.size(); i++) {

		            if (joinGroup.get(i) == nodes.get(j)) {

		                slots[j++] = i;

		            }

		        }
		    }


		    final double optimistic = joinGroup.getProperty(
		            Annotations.OPTIMISTIC,
		            Annotations.DEFAULT_OPTIMISTIC);
		    
		    final List<IReorderableNode> required =
		    	new LinkedList<IReorderableNode>();
		    
		    
		    IReorderableNode runLast = null;
		    
		    for (IReorderableNode sp : nodes) {
		    	
		    	if (runLast == null && sp.getProperty(QueryHints.RUN_LAST, false)) {

		    		runLast = sp;
		    		
		    	} else {
		    		
		    		required.add(sp);
		    		
		    	}
		    	
		    }
		    
		    /*
		     * Calculate the optimized join ordering for the required
		     * tails.
		     */
		    final StaticOptimizer opt = new StaticOptimizer(queryRoot,
		            ctx, ancestry, required, optimistic);

		    final int[] order = opt.getOrder();

		    /*
		     * Reorder the statement pattern nodes within the join
		     * group.
		     */
		    int i = 0;
		    for (int j = 0; j < required.size(); j++) {

		        final IReorderableNode sp = required.get(order[j]);

		        joinGroup.setArg(slots[i++], sp);

		    }
		    
		    if (runLast != null) {
		    	
		    	joinGroup.setArg(slots[i++], runLast);
		    	
		    }
		}
	}
    
//    /**
//     * Use the SPORelation from the database to grab the appropriate range
//     * counts for the {@link StatementPatternNode}s.  Only tries to attach them
//     * if the annotation {@link Annotations#ESTIMATED_CARDINALITY} is not
//     * already attached to the node.  This makes it possible to write unit
//     * tests without real data.
//     */
//    private final void attachRangeCounts(final AST2BOpContext ctx,
//            final List<StatementPatternNode> spNodes,
//            final IBindingSet exogenousBindings) {
//
//        final AbstractTripleStore db = ctx.getAbstractTripleStore();
//        
//    	for (StatementPatternNode sp : spNodes) {
//    		
//    		if (sp.getProperty(Annotations.ESTIMATED_CARDINALITY) == null) {
//    			
//                final IV<?, ?> s = getIV(sp.s(), exogenousBindings);
//                final IV<?, ?> p = getIV(sp.p(), exogenousBindings);
//                final IV<?, ?> o = getIV(sp.o(), exogenousBindings);
//                final IV<?, ?> c = getIV(sp.c(), exogenousBindings);
//                
//                final RangeNode rangeNode = sp.getRange();
//                final RangeBOp range = rangeNode != null ? rangeNode.getRangeBOp() : null;
//    			
//                final IAccessPath<?> ap = db.getAccessPath(s, p, o, c, range);
//                
//                final long cardinality = ap.rangeCount(false/* exact */);
//
//                // Annotate with the fast range count.
//    			sp.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
//    			
//                /*
//                 * Annotate with the index which would be used if we did not run
//                 * access path "as-bound". This is the index that will be used
//                 * if we wind up doing a hash join for this predicate.
//                 * 
//                 * TODO It would make sense to lift this annotation into a
//                 * different AST optimizer so it is always present. An
//                 * optimization for index locality for as-bound evaluation
//                 * depends on the presence of this annotation.
//                 * 
//                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/150"
//                 * (Choosing the index for testing fully bound access paths
//                 * based on index locality)
//                 */
//                sp.setProperty(Annotations.ORIGINAL_INDEX, ap.getKeyOrder());
//
//    		}
//    		
//    	}
//    	
//    }
//
//    /**
//     * Helper method grabs the IV out of the TermNode, doing the appropriate
//     * NULL and constant/var checks.
//     * 
//     * @param term
//     * @param exogenousBindings
//     *            The externally given bindings (optional).
//     */
//    @SuppressWarnings("rawtypes")
//    private final IV getIV(final TermNode term,
//            final IBindingSet exogenousBindings) {
//
//        if (term != null && term.isVariable() && exogenousBindings != null) {
//
//            @SuppressWarnings("unchecked")
//            final IConstant<IV> c = (IConstant<IV>) exogenousBindings
//                    .get((IVariable) term.getValueExpression());
//            
//            if(c != null) {
//                
//                return c.get();
//                
//            }
//            
//        }
//        
//    	if (term != null && term.isConstant()) {
//    		
//    		final IV iv = ((IConstant<IV>) term.getValueExpression()).get();
//    		
//    		if (iv == null) {
//    			
//    			throw new AssertionError("this optimizer cannot run with unknown IVs in statement patterns");
//    			
//    		}
//    		
//    		return iv;
//    		
//    	} else {
//    		
//    		return null;
//    		
//    	}
//    	
//    }
//    
    
}

