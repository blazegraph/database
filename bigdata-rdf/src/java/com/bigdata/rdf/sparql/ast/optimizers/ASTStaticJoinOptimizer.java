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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlan2;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BD;
import com.bigdata.relation.accesspath.IAccessPath;

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

    private static final Logger log = Logger
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
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

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
            return queryNode;

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

        return queryNode;

    }

    /**
     * Return <code>true</code> if the static join optimizer is enabled for the
     * given join group.
     */
    static boolean isStaticOptimizer(final AST2BOpContext context,
            final JoinGroupNode joinGroup) {

        QueryOptimizerEnum optimizer = null;

        if (joinGroup.getProperty(QueryHints.OPTIMIZER) != null) {

//            optimizer = QueryOptimizerEnum.valueOf(joinGroup
//                    .getQueryHint(QueryHints.OPTIMIZER));
            optimizer = (QueryOptimizerEnum) joinGroup
                    .getProperty(QueryHints.OPTIMIZER);
            
            return optimizer == QueryOptimizerEnum.Static;
            
//        } else {
//
//            optimizer = context == null || context.queryHints == null ? QueryOptimizerEnum.Static
//                    : QueryOptimizerEnum.valueOf(context.queryHints
//                            .getProperty(QueryHints.OPTIMIZER,
//                                    QueryOptimizerEnum.Static.toString()));

        }

//        return optimizer == QueryOptimizerEnum.Static;
        return QueryHints.DEFAULT_OPTIMIZER == QueryOptimizerEnum.Static;

    }
    
    /**
     * Eliminate a parent join group whose only child is another join group by
     * lifting the child (it replaces the parent).
     * 
     * @param ctx
     * @param exogenousBindings
     *            The exogenous bindings -or- <code>null</code> iff there are
     *            none.
     * @param queryRoot
     * @param ancestry
     * @param op
     */
    private void optimize(final AST2BOpContext ctx,
            final IBindingSet exogenousBindings, final QueryRoot queryRoot,
            IJoinNode[] ancestry, final GraphPatternGroup<?> op) {

    	if (op instanceof JoinGroupNode) {
    		
    		final JoinGroupNode joinGroup = (JoinGroupNode) op;
    		
    		/*
    		 * Look for service calls and named subquery includes, since they 
    		 * will get run before the statement pattern nodes. Add them into 
    		 * the ancestry.
    		 */
    		final List<ServiceNode> serviceNodes = joinGroup.getServiceNodes();
    		
    		final List<NamedSubqueryInclude> namedSubqueryIncludes =
    			joinGroup.getNamedSubqueryIncludes();
    		
    		if (serviceNodes.size() > 0 || namedSubqueryIncludes.size() > 0) {
    			
    			final List<IJoinNode> tmp = new LinkedList<IJoinNode>();
    			
    			for (IJoinNode ancestor : ancestry) {
    				
    				tmp.add(ancestor);
    				
    			}
    			
    			for (ServiceNode service : serviceNodes) {
    				
        			if (log.isDebugEnabled()) {
        				log.debug("adding a service node to ancestry:" + service);
        			}
        			
    				tmp.add(service);
    				
    			}
    			
    			for (NamedSubqueryInclude nsi : namedSubqueryIncludes) {
    				
        			if (log.isDebugEnabled()) {
        				log.debug("adding a named subquery include to ancestry:" + nsi);
        			}
        			
    				tmp.add(nsi);
    				
    			}
    			
    			ancestry = tmp.toArray(new IJoinNode[tmp.size()]);
    			
    		}
    		
	    	/*
	    	 * First optimize this group.
	    	 */
//	        @SuppressWarnings("rawtypes")
//	        final List<StatementPatternNode> spNodes = new LinkedList<StatementPatternNode>();
//	
//	        for (StatementPatternNode sp : joinGroup.getStatementPatterns()) {
//	        	
//	            if(!sp.isSimpleOptional()) {
//	            
//	            	// Only required statement patterns.
//	            	spNodes.add(sp);
//	            	
//	            }
//	            
//	        }

    		/*
    		 * Let the optimizer handle the simple optionals too.
    		 */
    		final List<StatementPatternNode> spNodes = joinGroup.getStatementPatterns();
    		
	        if (!spNodes.isEmpty()) {
	        
	            // Always attach the range counts.
	        	attachRangeCounts(ctx, spNodes, exogenousBindings);
	        	
                if (isStaticOptimizer(ctx, joinGroup)) {

                    /*
                     * Find the "slots" where the statement patterns currently
                     * show up in the join group. We will later fill in these
                     * slots with the same statement patterns, but in a
                     * different (optimized) ordering.
                     */
                    final int[] slots = new int[spNodes.size()];
                    {
	                    int j = 0;
	                    for (int i = 0; i < joinGroup.arity(); i++) {
	
	                        if (joinGroup.get(i) instanceof StatementPatternNode) {
	
	                            slots[j++] = i;
	
	                        }
	
	                    }
                    }

//                  final double optimistic = Double.parseDouble( 
//                  joinGroup.getQueryHint(Annotations.OPTIMISTIC, 
//                          Annotations.DEFAULT_OPTIMISTIC));

                    final double optimistic = joinGroup.getProperty(
                            Annotations.OPTIMISTIC,
                            Annotations.DEFAULT_OPTIMISTIC);
                    
                    final List<StatementPatternNode> required =
                    	new LinkedList<StatementPatternNode>();
                    
                    final List<StatementPatternNode> optional =
                    	new LinkedList<StatementPatternNode>();
                    
                    StatementPatternNode runLast = null;
                    
                    for (StatementPatternNode sp : spNodes) {
                    	
                    	if (sp.getProperty(QueryHints.RUN_LAST, false)) {

                    		runLast = sp;
                    		
                    	} else if (sp.isOptional()) {
                    		
                    		optional.add(sp);
                    		
                    	} else {
                    		
                    		required.add(sp);
                    		
                    	}
                    	
                    }
                    
                    /*
                     * Calculate the optimized join ordering for the required
                     * tails.
                     */
                    final StaticOptimizer opt = new StaticOptimizer(queryRoot,
                            ancestry, required, optimistic);

                    final int[] order = opt.getOrder();

                    /*
                     * Reorder the statement pattern nodes within the join
                     * group.
                     */
                    int i = 0;
                    for (int j = 0; j < required.size(); j++) {

                        final StatementPatternNode sp = required.get(order[j]);

                        joinGroup.setArg(slots[i++], sp);

                    }
                    
                    if (runLast != null && !runLast.isOptional()) {
                    	
                    	joinGroup.setArg(slots[i++], runLast);
                    	
                    }
                    
                    for (StatementPatternNode sp : optional) {

                        joinGroup.setArg(slots[i++], sp);
                        
                    }

                    if (runLast != null && runLast.isOptional()) {
                    	
                    	joinGroup.setArg(slots[i++], runLast);
                    	
                    }
                    
	        	} 
	        	    
                /*
                 * Update the ancestry for recursion. Only add the
                 * non-optional statement pattern nodes - the ones that we
                 * can count on to bind their variables.
                 */
                final List<IJoinNode> tmp = new LinkedList<IJoinNode>();
                
                for (IJoinNode ancestor : ancestry) {
                    
                    tmp.add(ancestor);
                    
                }
                
                for(StatementPatternNode sp : spNodes) {
                    
                    if (!sp.isOptional()) {
                        
                        tmp.add(sp);
                        
                    }
                    
                }
                
                if (tmp.size() > ancestry.length) {
                    
                    ancestry = tmp.toArray(new IJoinNode[tmp.size()]);
                    
                }
	        
	        }
	        
    	}
    	
        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GraphPatternGroup<?>) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) child;

                optimize(ctx, exogenousBindings, queryRoot, ancestry, childGroup);
                
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
                
                optimize(ctx, tmp, queryRoot, ancestry, childGroup);

            }
            
        }

    }
    
    /**
     * Use the SPORelation from the database to grab the appropriate range
     * counts for the {@link StatementPatternNode}s.  Only tries to attach them
     * if the annotation {@link Annotations#ESTIMATED_CARDINALITY} is not
     * already attached to the node.  This makes it possible to write unit
     * tests without real data.
     */
    private final void attachRangeCounts(final AST2BOpContext ctx,
            final List<StatementPatternNode> spNodes,
            final IBindingSet exogenousBindings) {

    	for (StatementPatternNode sp : spNodes) {
    		
    		if (sp.getProperty(Annotations.ESTIMATED_CARDINALITY) == null) {
    			
                final IV<?, ?> s = getIV(sp.s(), exogenousBindings);
                final IV<?, ?> p = getIV(sp.p(), exogenousBindings);
                final IV<?, ?> o = getIV(sp.o(), exogenousBindings);
                final IV<?, ?> c = getIV(sp.c(), exogenousBindings);
    			
                final IAccessPath<?> ap = ctx.db.getAccessPath(s, p, o, c);
                
                final long cardinality = ap.rangeCount(false/* exact */);

                // Annotate with the fast range count.
    			sp.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
    			
                /*
                 * Annotate with the index which would be used if we did not run
                 * access path "as-bound". This is the index that will be used
                 * if we wind up doing a hash join for this predicate.
                 */
                sp.setProperty(Annotations.ORIGINAL_INDEX, ap.getKeyOrder());

    		}
    		
    	}
    	
    }

    /**
     * Helper method grabs the IV out of the TermNode, doing the appropriate
     * NULL and constant/var checks.
     * 
     * @param term
     * @param exogenousBindings
     *            The externally given bindings (optional).
     */
    @SuppressWarnings("rawtypes")
    private final IV getIV(final TermNode term,
            final IBindingSet exogenousBindings) {

        if (term != null && term.isVariable() && exogenousBindings != null) {

            @SuppressWarnings("unchecked")
            final IConstant<IV> c = (IConstant<IV>) exogenousBindings
                    .get((IVariable) term.getValueExpression());
            
            if(c != null) {
                
                return c.get();
                
            }
            
        }
        
    	if (term != null && term.isConstant()) {
    		
    		final IV iv = ((IConstant<IV>) term.getValueExpression()).get();
    		
    		if (iv == null) {
    			
    			throw new AssertionError("this optimizer cannot run with unknown IVs in statement patterns");
    			
    		}
    		
    		return iv;
    		
    	} else {
    		
    		return null;
    		
    	}
    	
    }
    
    /**
     * This is the old static optimizer code, taken directly from
     * {@link DefaultEvaluationPlan2}, but lined up with the AST API instead of
     * the Rule and IPredicate API.
     * 
     */
    private static final class StaticOptimizer {
    	
    	private final QueryRoot queryRoot;
    	
    	private final IJoinNode[] ancestry;
    	
        private final List<StatementPatternNode> spNodes;

        private final int arity;
        
        private static final transient long BOTH_OPTIONAL = Long.MAX_VALUE-1;
        
        private static final transient long ONE_OPTIONAL = Long.MAX_VALUE-2;
        
        private static final transient long NO_SHARED_VARS = Long.MAX_VALUE-3;
        
        /**
         * The computed evaluation order. The elements in this array are the order
         * in which each tail predicate will be evaluated. The index into the array
         * is the index of the tail predicate whose evaluation order you want. So
         * <code>[2,0,1]</code> says that the predicates will be evaluated in the
         * order tail[2], then tail[0], then tail[1]. 
         */
        private int[/* order */] order;

        public int[] getOrder() {

            if (order == null) {
             
                /*
                 * This will happen if you try to use toString() during the ctor
                 * before the order has been computed.
                 */

                throw new IllegalStateException();
                
            }
//            calc();
            
            return order;
            
        }

        /**
         * Cache of the computed range counts for the predicates in the tail. The
         * elements of this array are initialized to -1L, which indicates that the
         * range count has NOT been computed. Range counts are computed on demand
         * and MAY be zero. Only an approximate range count is obtained. Such
         * approximate range counts are an upper bound on the #of elements that are
         * spanned by the access pattern. Therefore if the range count reports ZERO
         * (0L) it is a real zero and the access pattern does not match anything in
         * the data. The only other caveat is that the range counts are valid as of
         * the commit point on which the access pattern is reading. If you obtain
         * them for {@link ITx#READ_COMMITTED} or {@link ITx#UNISOLATED} views then
         * they could be invalidated by concurrent writers.
         */
        private long[/*tailIndex*/] rangeCount;
        
        /**
         * Keeps track of which tails have been used already and which still need
         * to be evaluated.
         */
        private transient boolean[/*tailIndex*/] used;
        
//        /**
//         * <code>true</code> iff the rule was proven to have no solutions.
//         */
//        private boolean empty = false;
        
        /**
         * See {@link Annotations#OPTIMISTIC}.
         */
        private final double optimistic;
        
//        public boolean isEmpty() {
//            
//            return empty;
//            
//        }
        
        /**
         * Computes an evaluation plan for the rule.
         * 
         * @param rangeCountFactory
         *            The range count factory.
         * @param rule
         *            The rule.
         */
        public StaticOptimizer(final QueryRoot queryRoot,
        		final IJoinNode[] ancestry, 
        		final List<StatementPatternNode> spNodes,
        		final double optimistic) {
            
            if (queryRoot == null)
                throw new IllegalArgumentException();
            
            if (ancestry == null)
                throw new IllegalArgumentException();
            
            if (spNodes == null)
                throw new IllegalArgumentException();
            
            this.queryRoot = queryRoot;
            
            this.ancestry = ancestry;
            
            this.spNodes = spNodes;
            
            this.arity = spNodes.size();
            
            this.optimistic = optimistic;
        
            calc();
            
            if (log.isDebugEnabled()) {
                for (int i = 0; i < arity; i++) {
                    log.debug(order[i]);
                }
            }
            
        }
        
        /**
         * Compute the evaluation order.
         */
        private void calc() {

            if (order != null)
                return;

            order = new int[arity];
            rangeCount = new long[arity];
            used = new boolean[arity];
            
            // clear arrays.
            for (int i = 0; i < arity; i++) {
                order[i] = -1; // -1 is used to detect logic errors.
                rangeCount[i] = -1L;  // -1L indicates no range count yet.
                used[i] = false;  // not yet evaluated
            }

            if (arity == 1) {
                order[0] = 0;
                return;
            }
            
            /*
            if (tailCount == 2) {
                order[0] = cardinality(0) <= cardinality(1) ? 0 : 1;
                order[1] = cardinality(0) <= cardinality(1) ? 1 : 0;
                return;
            }
            */
            
            final Set<IVariable<?>> runFirstVars = new HashSet<IVariable<?>>();
            
            int startIndex = 0;
            
            /*
             * DEAD CODE.  See ASTSearchOptimizer, which lifts the full text
             * search into a ServiceNode.  This is captured by the ancestry
             * code just below.
             */
            if (false) {
            	
            for (int i = 0; i < arity; i++) {
                final StatementPatternNode pred = spNodes.get(i);
//            	final IAccessPathExpander expander = pred.getAccessPathExpander();
                final TermNode p = pred.p();
                if (p != null && p.isConstant() && p.getValue() != null && p.getValue().equals(BD.SEARCH)) {
                    if (log.isDebugEnabled()) log.debug("found a run first, tail " + i);
//                    final Iterator<IVariable<?>> it = BOpUtility.getArgumentVariables(pred);
//                    while (it.hasNext()) {
//                    	runFirstVars.add(it.next());
//                    }
                    final TermNode s = pred.s();
                    if (s != null && s.isVariable()) {
                    	runFirstVars.add((IVariable<?>) s.getValueExpression());
                    }
                    order[startIndex++] = i;
                    used[i] = true;
                }
            }
            
            }
            
            /*
             * Seems like the easiest way to handle the ancestry is the exact
             * same way we handle the "run first" statement patterns (text search),
             * which is that we collect up the variables that are bound and then
             * give preferential treatment to the predicates that can join
             * on those variables.
             */
        	final StaticAnalysis sa = new StaticAnalysis(queryRoot);
            for (IJoinNode join : ancestry) {
            	if (log.isDebugEnabled()) {
            		log.debug("considering join node from ancestry: " + join);
            	}
//            	final Iterator<IVariable<?>> it = BOpUtility.getArgumentVariables((BOp) join);
//        		while (it.hasNext()) {
//        			runFirstVars.add(it.next());
//        		}
            	sa.getDefinitelyProducedBindings(join, runFirstVars, false/* recursive */);
            }
            if (log.isDebugEnabled()) {
            	log.debug("bindings from ancestry: " + Arrays.toString(runFirstVars.toArray()));
            }
            
            // if there are no more tails left after the expanders, we're done
            if (startIndex == arity) {
            	return;
            }
            
            // if there is only one tail left after the expanders
            if (startIndex == arity-1) {
                if (log.isDebugEnabled()) log.debug("one tail left");
                for (int i = 0; i < arity; i++) {
                    // only check unused tails
                    if (used[i]) {
                        continue;
                    }
                    order[arity-1] = i;
                    used[i] = true;
                    return;
                }            
            }
            
            int preferredFirstTail = -1;
            long minCardinality = Long.MAX_VALUE;
            // give preferential treatment to a tail that shares variables with the
            // runFirst expanders. collect all of them up and then choose the one
            // that has the lowest cardinality
            for (int i = 0; i < arity; i++) {
                // only check unused tails
                if (used[i]) {
                    continue;
                }
                final StatementPatternNode pred = spNodes.get(i);
                
                /*
                 * We need the optimizer to play nice with the run first hint.
                 */
                if (pred.getProperty(QueryHints.RUN_FIRST, false)) {
                	preferredFirstTail = i;
                	break;
                }
                
                if (log.isDebugEnabled()) {
                	log.debug("considering pred against ancestry: " + pred);
                }
                // only test the required joins
                if (pred.isOptional()) {
                	continue;
                }
                final Set<IVariable<?>> vars = sa.getDefinitelyProducedBindings(
                		pred, new LinkedHashSet<IVariable<?>>(), false/* recursive */);
//                Iterator<IVariable<?>> it = BOpUtility.getArgumentVariables(pred);
        		final Iterator<IVariable<?>> it = vars.iterator();
                while (it.hasNext()) {
                	if (runFirstVars.contains(it.next())) {
//                		preferredFirstTail = i;
                		/*
                		 * We have a shared var with either the run first
                		 * predicates or with the ancestry.
                		 */
                		final long tailCardinality = cardinality(i);
                		if (tailCardinality < minCardinality) {
                			preferredFirstTail = i;
                			minCardinality = tailCardinality; 
                		}
                	}
                }
//                if (preferredFirstTail != -1)
//                	break;
            }
            if (log.isDebugEnabled()) {
            	log.debug("preferred first tail: " + preferredFirstTail);
            }
            
            // if there are only two tails left after the expanders
            if (startIndex == arity-2) {
                if (log.isDebugEnabled()) log.debug("two tails left");
                int t1 = -1;
                int t2 = -1;
                for (int i = 0; i < arity; i++) {
                    // only check unused tails
                    if (used[i]) {
                        continue;
                    }
                    // find the two unused tail indexes
                    if (t1 == -1) {
                        t1 = i;
                    } else {
                        t2 = i;
                        break;
                    }
                }
                if (log.isDebugEnabled()) log.debug(t1 + ", " + t2);
                if (preferredFirstTail != -1) {
                	order[arity-2] = preferredFirstTail;
                	order[arity-1] = preferredFirstTail == t1 ? t2 : t1;
                } else {
    	            order[arity-2] = cardinality(t1) <= cardinality(t2) ? t1 : t2;
    	            order[arity-1] = cardinality(t1) <= cardinality(t2) ? t2 : t1;
                }
                return;
            }
            
            /*
             * There will be (tails-1) joins, we just need to figure out what
             * they should be.
             */
            Join join = preferredFirstTail == -1 ? getFirstJoin() : getFirstJoin(preferredFirstTail);
            int t1 = ((Tail) join.getD1()).getTail();
            int t2 = ((Tail) join.getD2()).getTail();
            if (preferredFirstTail == -1) {
    	        order[startIndex] = cardinality(t1) <= cardinality(t2) ? t1 : t2;
    	        order[startIndex+1] = cardinality(t1) <= cardinality(t2) ? t2 : t1;
            } else {
            	order[startIndex] = t1;
            	order[startIndex+1] = t2;
            }
            used[order[startIndex]] = true;
            used[order[startIndex+1]] = true;
            for (int i = startIndex+2; i < arity; i++) {
                join = getNextJoin(join);
                order[i] = ((Tail) join.getD2()).getTail();
                used[order[i]] = true;
            }
            
        }
        
        /**
         * Start by looking at every possible initial join. Take every tail and
         * match it with every other tail to find the lowest possible cardinality.
         * See {@link #computeJoinCardinality(com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlan2.IJoinDimension, com.bigdata.bop.joinGraph.fast.DefaultEvaluationPlan2.IJoinDimension)}
         * for more on this. 
         */
        private Join getFirstJoin() {
            if (log.isDebugEnabled()) {
                log.debug("evaluating first join");
            }
            long minJoinCardinality = Long.MAX_VALUE;
            long minTailCardinality = Long.MAX_VALUE;
            long minOtherTailCardinality = Long.MAX_VALUE;
            Tail minT1 = null;
            Tail minT2 = null;
            for (int i = 0; i < arity; i++) {
                // only check unused tails
                if (used[i]) {
                    continue;
                }
                Tail t1 = new Tail(i, rangeCount(i), getVars(i));
                long t1Cardinality = cardinality(i);
                for (int j = 0; j < arity; j++) {
                    // check only non-same and unused tails
                    if (i == j || used[j]) {
                        continue;
                    }
                    Tail t2 = new Tail(j, rangeCount(j), getVars(j));
                    long t2Cardinality = cardinality(j);
                    long joinCardinality = computeJoinCardinality(t1, t2);
                    long tailCardinality = Math.min(t1Cardinality, t2Cardinality);
                    long otherTailCardinality = Math.max(t1Cardinality, t2Cardinality);
                    if(log.isDebugEnabled()) log.debug("evaluating " + i + " X " + j + ": cardinality= " + joinCardinality);
                    if (joinCardinality < minJoinCardinality) {
                        if(log.isDebugEnabled()) log.debug("found a new min: " + joinCardinality);
                        minJoinCardinality = joinCardinality;
                        minTailCardinality = tailCardinality;
                        minOtherTailCardinality = otherTailCardinality;
                        minT1 = t1;
                        minT2 = t2;
                    } else if (joinCardinality == minJoinCardinality) {
                        if (tailCardinality < minTailCardinality) {
                            if(log.isDebugEnabled()) log.debug("found a new min: " + joinCardinality);
                            minJoinCardinality = joinCardinality;
                            minTailCardinality = tailCardinality;
                            minOtherTailCardinality = otherTailCardinality;
                            minT1 = t1;
                            minT2 = t2;
                        } else if (tailCardinality == minTailCardinality) {
                            if (otherTailCardinality < minOtherTailCardinality) {
                                if(log.isDebugEnabled()) log.debug("found a new min: " + joinCardinality);
                                minJoinCardinality = joinCardinality;
                                minTailCardinality = tailCardinality;
                                minOtherTailCardinality = otherTailCardinality;
                                minT1 = t1;
                                minT2 = t2;
                            }
                        }
                    }
                }
            }
            // the join variables is the union of the join dimensions' variables
            Set<String> vars = new HashSet<String>();
            vars.addAll(minT1.getVars());
            vars.addAll(minT2.getVars());
            return new Join(minT1, minT2, minJoinCardinality, vars);
        }
        
        private Join getFirstJoin(final int preferredFirstTail) {
            if (log.isDebugEnabled()) {
                log.debug("evaluating first join");
            }
            
            
            long minJoinCardinality = Long.MAX_VALUE;
            long minOtherTailCardinality = Long.MAX_VALUE;
            Tail minT2 = null;
            final int i = preferredFirstTail;
            final Tail t1 = new Tail(i, rangeCount(i), getVars(i));
            for (int j = 0; j < arity; j++) {
                // check only non-same and unused tails
                if (i == j || used[j]) {
                    continue;
                }
                Tail t2 = new Tail(j, rangeCount(j), getVars(j));
                long t2Cardinality = cardinality(j);
                long joinCardinality = computeJoinCardinality(t1, t2);
                if(log.isDebugEnabled()) log.debug("evaluating " + i + " X " + j + ": cardinality= " + joinCardinality);
                if (joinCardinality < minJoinCardinality) {
                    if(log.isDebugEnabled()) log.debug("found a new min: " + joinCardinality);
                    minJoinCardinality = joinCardinality;
                    minOtherTailCardinality = t2Cardinality;
                    minT2 = t2;
                } else if (joinCardinality == minJoinCardinality) {
                    if (t2Cardinality < minOtherTailCardinality) {
                        if(log.isDebugEnabled()) log.debug("found a new min: " + joinCardinality);
                        minJoinCardinality = joinCardinality;
                        minOtherTailCardinality = t2Cardinality;
                        minT2 = t2;
                    }
                }
            }

            // the join variables is the union of the join dimensions' variables
            Set<String> vars = new HashSet<String>();
            vars.addAll(t1.getVars());
            vars.addAll(minT2.getVars());
            return new Join(t1, minT2, minJoinCardinality, vars);
        }
        
        /**
         * Similar to {@link #getFirstJoin()}, but we have one join dimension
         * already calculated.
         * 
         * @param d1 
         *          the first join dimension
         * @return 
         *          the new join with the lowest cardinality from the remaining tails
         */
        private Join getNextJoin(IJoinDimension d1) {
            if (log.isDebugEnabled()) {
                log.debug("evaluating next join");
            }
            long minJoinCardinality = Long.MAX_VALUE;
            long minTailCardinality = Long.MAX_VALUE;
            Tail minTail = null;
            for (int i = 0; i < arity; i++) {
                // only check unused tails
                if (used[i]) {
                    continue;
                }
                Tail tail = new Tail(i, rangeCount(i), getVars(i));
                long tailCardinality = cardinality(i);
                long joinCardinality = computeJoinCardinality(d1, tail);
                if(log.isDebugEnabled()) log.debug("evaluating " + d1.toJoinString() + " X " + i + ": cardinality= " + joinCardinality);
                if (joinCardinality < minJoinCardinality) {
                    if(log.isDebugEnabled()) log.debug("found a new min: " + joinCardinality);
                    minJoinCardinality = joinCardinality;
                    minTailCardinality = tailCardinality;
                    minTail = tail;
                } else if (joinCardinality == minJoinCardinality) {
                    if (tailCardinality < minTailCardinality) {
                        if(log.isDebugEnabled()) log.debug("found a new min: " + joinCardinality);
                        minJoinCardinality = joinCardinality;
                        minTailCardinality = tailCardinality;
                        minTail = tail;
                    }
                }
            }
            // if we are at the "no shared variables" tails, order by range count
            if (minJoinCardinality == NO_SHARED_VARS) {
                minJoinCardinality = Long.MAX_VALUE;
                for (int i = 0; i < arity; i++) {
                    // only check unused tails
                    if (used[i]) {
                        continue;
                    }
                    Tail tail = new Tail(i, rangeCount(i), getVars(i));
                    long tailCardinality = cardinality(i);
                    if (tailCardinality < minJoinCardinality) {
                        if(log.isDebugEnabled()) log.debug("found a new min: " + tailCardinality);
                        minJoinCardinality = tailCardinality;
                        minTail = tail;
                    }
                }            
            }
            // the join variables is the union of the join dimensions' variables
            Set<String> vars = new HashSet<String>();
            vars.addAll(d1.getVars());
            vars.addAll(minTail.getVars());
            return new Join(d1, minTail, minJoinCardinality, vars);
        }
        
        /**
         * Return the range count for the predicate, ignoring any bindings. The
         * range count for the tail predicate is cached the first time it is
         * requested and returned from the cache thereafter. The range counts are
         * requested using the "non-exact" range count query, so the range counts
         * are actually the upper bound. However, if the upper bound is ZERO (0)
         * then the range count really is ZERO (0).
         * 
         * @param tailIndex
         *            The index of the predicate in the tail of the rule.
         * 
         * @return The range count for that tail predicate.
         */
        public long rangeCount(final int tailIndex) {

            if (rangeCount[tailIndex] == -1L) {

//                final IPredicate predicate = spNodes.getTail(tailIndex);
//                
//                final IAccessPathExpander expander = predicate.getAccessPathExpander();
//
//                if (expander != null && expander.runFirst()) {
//
//                    /*
//                     * Note: runFirst() essentially indicates that the cardinality
//                     * of the predicate in the data is to be ignored. Therefore we
//                     * do not request the actual range count and just return -1L as
//                     * a marker indicating that the range count is not available.
//                     */
//                    
//                    return -1L;
//                    
//                }
//                
//                final long rangeCount = rangeCountFactory
//                        .rangeCount(spNodes.getTail(tailIndex));

                final long rangeCount = (long) spNodes.get(tailIndex).getProperty(
                			Annotations.ESTIMATED_CARDINALITY, -1l);
                
                this.rangeCount[tailIndex] = rangeCount;

            }

            return rangeCount[tailIndex];

        }

        /**
         * Return the cardinality of a particular tail, which is the range count
         * if not optional and infinite if optional.
         */
        public long cardinality(final int tailIndex) {
//            IPredicate tail = spNodes.getTail(tailIndex);
        	final StatementPatternNode tail = spNodes.get(tailIndex);
            if (tail.isOptional()/* || tail instanceof IStarJoin */) {
                return Long.MAX_VALUE;
            } else {
                return rangeCount(tailIndex);
            }
        }
        
        public String toString() {
            return Arrays.toString(getOrder());
        }
        
        /**
         * This is the secret sauce.  There are three possibilities for computing
         * the join cardinality, which we are defining as the upper-bound for
         * solutions for a particular join.  First, if there are no shared variables
         * then the cardinality will just be the simple sum of the cardinality of
         * each join dimension.  If there are shared variables but no unshared
         * variables, then the cardinality will be the minimum cardinality from
         * the join dimensions.  If there are shared variables but also some
         * unshared variables, then the join cardinality will be the maximum
         * cardinality from each join dimension.
         * <p>
         * Any join involving an optional will have infinite cardinality, so that
         * optionals get placed at the end.
         *  
         * @param d1
         *          the first join dimension
         * @param d2
         *          the second join dimension
         * @return
         *          the join cardinality
         */
        protected long computeJoinCardinality(IJoinDimension d1, IJoinDimension d2) {
            // two optionals is worse than one
            if (d1.isOptional() && d2.isOptional()) {
                return BOTH_OPTIONAL;
            }
            if (d1.isOptional() || d2.isOptional()) {
                return ONE_OPTIONAL;
            }
            final boolean sharedVars = hasSharedVars(d1, d2);
            final boolean unsharedVars = hasUnsharedVars(d1, d2);
            final long joinCardinality;
            if (sharedVars == false) {
                // no shared vars - take the sum
                // joinCardinality = d1.getCardinality() + d2.getCardinality();
                // different approach - give preference to shared variables
                joinCardinality = NO_SHARED_VARS;
            } else {
                if (unsharedVars == false) {
                    // shared vars and no unshared vars - take the min
                    joinCardinality = 
                        Math.min(d1.getCardinality(), d2.getCardinality());
                } else {
                    // shared vars and unshared vars - take the max
					/*
					 * This modification to the join planner results in
					 * significantly faster queries for the bsbm benchmark (3x -
					 * 5x overall). It takes a more optimistic perspective on
					 * the intersection of two statement patterns, predicting
					 * that this will constraint, rather than increase, the
					 * multiplicity of the solutions. However, this COULD lead
					 * to pathological cases where the resulting join plan is
					 * WORSE than it would have been otherwise. For example,
					 * this change produces a 3x to 5x improvement in the BSBM
					 * benchmark results. However, it has a negative effect on
					 * LUBM Q2.
					 * 
					 * Update: Ok so just to go into a little detail -
					 * yesterday's change means we choose the join ordering
					 * based on an optimistic view of the cardinality of any
					 * particular join. If you have two triple patterns that
					 * share variables but that also have unshared variables,
					 * then technically the maximum cardinality of the join is
					 * the maximum range count of the two tails. But often the
					 * true cardinality of the join is closer to the minimum
					 * range count than the maximum. So yesterday we started
					 * assigning an expected cardinality for the join of the
					 * minimum range count rather than the maximum. What this
					 * means is that a lot of the time when those joins move
					 * toward the front of the line the query will do a lot
					 * better, but occasionally (LUBM 2), the query will do much
					 * much worse (when the true cardinality is closer to the
					 * max range count).
					 * 
					 * Today we put in an extra tie-breaker condition. We
					 * already had one tie-breaker - if two joins have the same
					 * expected cardinality we chose the one with the lower
					 * minimum range count. But the new tie-breaker is that if
					 * two joins have the same expected cardinality and minimum
					 * range count, we now chose the one that has the minimum
					 * range count on the other tail (the minimum maximum if
					 * that makes sense).
					 * 
					 * 11/14/2011:
					 * The static join order optimizer should consider the swing
					 * in stakes when choosing between either the MIN or the MAX
					 * of the cardinality of two join dimensions in order to
					 * decide which join to schedule next. Historically it took
					 * the MAX, but there are counter examples to that decision
					 * such as LUBM Q2. Subsequently it was modified to take the
					 * MIN, but BSBM BI Q1 is a counter example for that. 
					 * 
					 * Modify the static optimizer to consider the swing in
					 * stakes between the choice of MAX versus MIN. I believe
					 * that this boils down to something like "If an incorrect
					 * guess of MIN would cause us to suffer a very bad MAX,
					 * then choose based on the MAX to avoid paying that
					 * penalty."
					 */
                    joinCardinality = (long) ((long)
                        (optimistic * Math.min(d1.getCardinality(), d2.getCardinality())) +
                        ((1.0d-optimistic) * Math.max(d1.getCardinality(), d2.getCardinality())));
                }
            }
            return joinCardinality;
        }
        
        /**
         * Get the named variables for a given tail.  Is there a better way to do
         * this?
         * 
         * @param tail
         *          the tail
         * @return
         *          the named variables
         */
        protected Set<String> getVars(int tail) {
            final Set<String> vars = new HashSet<String>();
//            IPredicate pred = spNodes.getTail(tail);
//            for (int i = 0; i < pred.arity(); i++) {
//                IVariableOrConstant term = pred.get(i);
//                if (term.isVar()) {
//                    vars.add(term.getName());
//                }
//            }
            final StatementPatternNode sp = spNodes.get(tail);
            if (log.isDebugEnabled()) {
            	log.debug(sp);
            }
            for (int i = 0; i < sp.arity(); i++) {
            	final TermNode term = (TermNode) sp.get(i);
                if (log.isDebugEnabled()) {
                	log.debug(term);
                }
            	if (term != null && term.isVariable()) {
            		vars.add(term.getValueExpression().getName());
            	}
            }
            
            return vars;
        }
        
        /**
         * Look for shared variables.
         * 
         * @param d1
         *          the first join dimension
         * @param d2
         *          the second join dimension
         * @return
         *          true if there are shared variables, false otherwise
         */
        protected boolean hasSharedVars(IJoinDimension d1, IJoinDimension d2) {
            for(String var : d1.getVars()) {
                if (d2.getVars().contains(var)) {
                    return true;
                }
            }
            return false;
        }
        
        /**
         * Look for unshared variables.
         * 
         * @param d1
         *          the first join dimension
         * @param d2
         *          the second join dimension
         * @return
         *          true if there are unshared variables, false otherwise
         */
        protected boolean hasUnsharedVars(IJoinDimension d1, IJoinDimension d2) {
            for(String var : d1.getVars()) {
                if (d2.getVars().contains(var) == false) {
                    return true;
                }
            }
            for(String var : d2.getVars()) {
                if (d1.getVars().contains(var) == false) {
                    return true;
                }
            }
            return false;
        }
        
        /**
         * A join dimension can be either a tail, or a previous join.  Either way
         * we need to know its cardinality, its variables, and its tails.
         */
        private interface IJoinDimension {
            long getCardinality();
            Set<String> getVars();
            String toJoinString();
            boolean isOptional();
        }

        /**
         * A join implementation of a join dimension. The join can consist of two
         * tails, or one tail and another join.  Theoretically it could be two
         * joins as well, which might be a future optimization worth thinking about.
         */
        private static class Join implements IJoinDimension {
            
            private final IJoinDimension d1, d2;
            private final long cardinality;
            private final Set<String> vars;
            
            public Join(IJoinDimension d1, IJoinDimension d2, 
                        long cardinality, Set<String> vars) {
                this.d1 = d1;
                this.d2 = d2;
                this.cardinality = cardinality;
                this.vars = vars;
            }

            public IJoinDimension getD1() {
                return d1;
            }

            public IJoinDimension getD2() {
                return d2;
            }

            public Set<String> getVars() {
                return vars;
            }
            
            public long getCardinality() {
                return cardinality;
            }
            
            public boolean isOptional() {
                return false;
            }
            
            public String toJoinString() {
                return d1.toJoinString() + " X " + d2.toJoinString();
            }
            
        }
        
        /**
         * A tail implementation of a join dimension. 
         */
        private class Tail implements IJoinDimension {

            private final int tail;
            private final long cardinality;
            private final Set<String> vars;
            
            public Tail(int tail, long cardinality, Set<String> vars) {
                this.tail = tail;
                this.cardinality = cardinality;
                this.vars = vars;
            }

            public int getTail() {
                return tail;
            }

            public long getCardinality() {
                return cardinality;
            }

            public Set<String> getVars() {
                return vars;
            }
            
            public boolean isOptional() {
                return spNodes.get(tail).isOptional();
            }
            
            public String toJoinString() {
                return String.valueOf(tail);
            }
            
        }

    }
    
}
