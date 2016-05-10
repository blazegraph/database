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
 * Created on Sep 14, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.internal.constraints.IPassesMaterialization;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.sparql.ast.cache.CacheConnectionImpl;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;
import com.bigdata.rdf.sparql.ast.optimizers.ASTBottomUpOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTLiftPreFiltersOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTOptimizerList;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.ssets.ISolutionSetManager;

/**
 * Methods for static analysis of a query. There is one method which looks "up".
 * This corresponds to how we actually evaluation things (left to right in the
 * query plan). There are two methods which look "down". This corresponds to the
 * bottom-up evaluation semantics of SPARQL.
 * <p>
 * When determining the "known" bound variables on entry to a node we have to
 * look "up" the tree until we reach the outer most group. Note that named
 * subqueries DO NOT receive bindings from the places where they are INCLUDEd
 * into the query.
 * 
 * <h3>Analysis of Incoming "Known" Bound Variables (Looking Up)</h3>
 * 
 * Static analysis of the incoming "known" bound variables does NOT reflect
 * bottom up evaluation semantics. If a variable binding would not be observed
 * for bottom up evaluation semantics due to a badly designed left join pattern
 * then the AST MUST be rewritten to lift the badly designed left join into a
 * named subquery where it will enjoy effective bottom up evaluation semantics.
 * 
 * <h3>Analysis of "must" and "maybe" Bound Variables (Looking Down).</h3>
 * 
 * The following classes are producers of bindings and need to be handled by
 * static analysis when looking down the AST tree:
 * <dl>
 * <dt>{@link QueryBase}</dt>
 * <dd>The static analysis of the definitely and maybe bound variables depends
 * on the projection and where clauses.</dd>
 * 
 * <dt>{@link SubqueryRoot}</dt>
 * <dd>SPARQL 1.1 subquery. This is just the static analysis of the QueryBase
 * for that subquery.</dd>
 * 
 * <dt>{@link NamedSubqueryRoot}</dt>
 * <dd>This is just the static analysis of the QueryBase for that named
 * subquery. Named subqueries are run without any visible bindings EXCEPT those
 * which are exogenous.</dd>
 * 
 * <dt>{@link NamedSubqueryInclude}</dt>
 * <dd>The static analysis of the INCLUDE is really the static analysis of the
 * NamedSubqueryRoot which produces that solution set. The incoming known
 * variables are ignored when doing the static analysis of the named subquery
 * root.</dd>
 * 
 * <dt>{@link ServiceNode}</dt>
 * <dd>The static analysis of the definitely and maybe bound variables depends
 * on the graph pattern for that service call. This is analyzed like a normal
 * graph pattern. Everything visible in the graph pattern is considered to be
 * projected. As far as I can tell, ServiceNodes are not run "as-bound" and
 * their static analysis is as if they were named subqueries (they have no known
 * bound incoming variables other than those communicated by their
 * BindingsClause).</dd>
 * 
 * <dt>{@link StatementPatternNode}</dt>
 * <dd>All variables are definitely bound UNLESS
 * {@link StatementPatternNode#isOptional()} is <code>true</code>.
 * <p>
 * Note: we sometimes attach a simple optional join to the parent group for
 * efficiency, at which point it becomes an "optional" statement pattern. An
 * optional statement pattern may also have zero or more {@link FilterNode}s
 * associated with it.</dd>
 * 
 * <dt>{@link JoinGroupNode}</dt>
 * <dd></dd>
 * 
 * <dt>{@link UnionNode}</dt>
 * <dd>The definitely bound variables is the intersection of the definitely
 * bound variables in the child join groups. The maybe bound variables is the
 * union of the maybe bound variables in the child join groups.</dd>
 * 
 * <dt>{@link AssignmentNode}</dt>
 * <dd>BIND(expr AS var) in a group will not bind the variable if there is an
 * error when evaluating the value expression and does not fail the solution.
 * Thus BIND() in a group contributes to "maybe" bound variables.
 * <p>
 * Note: BIND() in a PROJECTION is handled differently as it is non-optional (if
 * the value expression results in an error the solution is dropped).
 * Projections are handled when we do the analysis of a QueryBase node since we
 * can see both the WHERE clause and the PROJECTION clauses at the same time.
 * <p>
 * See <a href="http://www.w3.org/TR/sparql11-query/#assignment"> If the
 * evaluation of the expression produces an error, the variable remains unbound
 * for that solution.</a></dd>
 * 
 * <dt><code>IF()</code></dt>
 * <dd>
 * * <code>IF</code> semantics : If evaluating the first argument raises an
 * error, then an error is raised for the evaluation of the IF expression. (This
 * greatly simplifies the analysis of the EBV of the IF value expressions, but
 * there is still uncertainty concerning whether the THEN or the ELSE is
 * executed for a given solution.) However, <code>IF</code> is not allowed to
 * conditionally bind a variable in the THEN/ELSE expressions so we do not have
 * to consider it here.</dd>
 * 
 * <dt><code>BOUND(var)</code></dt>
 * <dd>Filters which use BOUND() can not be pruned unless we can prove that the
 * variable is not (or is not) bound and also collapse the filter to a constant
 * after substituting either <code>true</code> or <code>false</code> in for the
 * BOUND() expression.</dd>
 * 
 * </dl>
 * 
 * <h3>FILTERs</h3>
 * 
 * FILTERs are groups based on whether they can run before any required joins
 * (pre-), with the required join (join-), or after all joins (post-).
 * <dl>
 * <dt>pre-</dt>
 * <dd>The pre-filters have all their required variables bound on entry to the
 * join group. They should be lifted into the parent join group.</dd>
 * <dt>join-</dt>
 * <dd>The join-filters will have all their required variables bound by the time
 * the required joins are done. These filters will wind up attached to the
 * appropriate required join. The specific filter/join attachments depend on the
 * join evaluation order.</dd>
 * <dt>post-</dt>
 * <dd>The post-filters might not have all of their required variables bound. We
 * have to wait until the last of the optionals joins has been evaluated before
 * we can evaluate any post-filters, so they run "last".</dd>
 * <dt>prune-</dt>
 * <dd>The prune-filters are those whose required variables CAN NOT be bound.
 * They should be pruned from the AST.</dd>
 * </dl>
 * 
 * TODO We can probably cache the heck out of things on this class. There is no
 * reason to recompute the SA of the know or maybe/must bound variables until
 * there is an AST change, and the caller can build a new SA when that happens.
 * However, note that we must make the cache sets unmodifiable since there are a
 * lot of patterns which rely on computing the difference between two sets and
 * those can not have a side-effect on the cache.
 * <p>
 * We could also attach the {@link StaticAnalysis} as an annotation on the
 * {@link QueryRoot} and provide a factory method for accessing it. That way we
 * would have reuse of the cached static analysis data. Each AST optimizer (or
 * the {@link ASTOptimizerList}) would have to clear the cached
 * {@link StaticAnalysis} when producing a new {@link QueryRoot}. Do this when
 * we add an ASTContainer to provide a better home for the queryStr, the parse
 * tree, the original AST, and the optimized AST.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StaticAnalysis extends StaticAnalysis_CanJoin {

    private static final Logger log = Logger.getLogger(StaticAnalysis.class);

    /**
     * 
     * @param queryRoot
     *            The root of the query. We need to have this on hand in order
     *            to resolve {@link NamedSubqueryInclude}s during static
     *            analysis.
     * 
     * @deprecated By the other form of this constructor. The constructor should
     *             have access to the {@link ISolutionSetStats}, which are on the
     *             {@link AST2BOpContext}. It also needs access to the
     *             {@link CacheConnectionImpl} for named solution sets.
     */
    // Note: Only exposed to the same package for unit tests.
    StaticAnalysis(final QueryRoot queryRoot) {
        
        this(queryRoot, null/* evaluationContext */);

    }

    /**
	 * 
	 * @param queryRoot
	 *            The root of the query. We need to have this on hand in order
	 *            to resolve {@link NamedSubqueryInclude}s during static
	 *            analysis.
	 * @param evaluationContext
	 *            The evaluation context provides access to the
	 *            {@link ISolutionSetStats} and the {@link ISolutionSetManager} for
	 *            named solution sets.
	 * 
	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/412
	 *      (StaticAnalysis#getDefinitelyBound() ignores exogenous variables.)
	 */
    public StaticAnalysis(final QueryRoot queryRoot,
            final IEvaluationContext evaluationContext) {

        super(queryRoot, evaluationContext);

    }

    /**
     * Find and return the parent {@link JoinGroupNode} which is the lowest such
     * {@link JoinGroupNode} dominating the given {@link GraphPatternGroup}.
     * This will search the tree to locate the parent when the
     * {@link GraphPatternGroup} appears as the annotation of a
     * {@link QueryBase}, {@link ServiceNode}, or a {@link FilterNode} having a
     * {@link ExistsNode} or {@link NotExistsNode}.
     * 
     * @param group
     *            The given group.
     * 
     * @return The lowest dominating {@link JoinGroupNode} above that group.
     */
    public JoinGroupNode findParentJoinGroup(final GraphPatternGroup<?> group) {

        final IQueryNode p = findParent(group);

        if (p instanceof JoinGroupNode) {

            return (JoinGroupNode) p;

        } else if (p instanceof UnionNode) {

            return ((UnionNode) p).getParentJoinGroup();

        } else if (p instanceof SubqueryRoot) {

            return ((SubqueryRoot) p).getParentJoinGroup();

        } else if (p instanceof NamedSubqueryRoot || p instanceof QueryRoot) {

            // top level.
            return null;

        } else if (p instanceof ServiceNode) {

            return ((ServiceNode) p).getParentJoinGroup();

        } else if (p instanceof FilterNode) {

            return ((FilterNode) p).getParentJoinGroup();

        }

        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Return the parent of the {@link GraphPatternGroup}. When the group has an
     * explicit parent reference, that reference is returned immediately.
     * Otherwise the {@link QueryRoot} is searched for a node having the given
     * group as an annotation. This makes it possible to locate a
     * {@link QueryBase}, {@link ServiceNode}, {@link ExistsNode}, or
     * {@link NotExistsNode} given its {@link GraphPatternGroup}.
     * <p>
     * Note: The parent of a {@link SubqueryRoot} is obtained by
     * {@link SubqueryRoot#getParent()} and is simply the {@link JoinGroupNode}
     * in which the {@link SubqueryRoot} appears.
     * 
     * @param group
     *            The group.
     * 
     * @return The parent of that group. This can be any of
     *         {@link GraphPatternGroup}, {@link QueryBase}, {@link ServiceNode}
     *         , or a {@link FilterNode}. This will be <code>null</code> iff the
     *         group does not appear anywhere in the {@link QueryRoot}.
     * 
     *         TODO The parent of a {@link NamedSubqueryRoot} is less well
     *         defined. A {@link NamedSubqueryRoot} may be included in multiple
     *         positions within the AST. Each of those could be considered a
     *         parent of the {@link NamedSubqueryRoot} in the sense that it
     *         provides a context within which the result of the query may be
     *         included. However, for the purposes of bottom up analysis, there
     *         is no parent of a {@link NamedSubqueryRoot}. It runs as if it
     *         were a top-level query (except that it might not have visibility
     *         into exogenous variables?).
     */
    public IQueryNode findParent(final GraphPatternGroup<?> group) {

        return findParent(queryRoot, group);

    }

    public static IQueryNode findParent(final QueryRoot queryRoot,
            final GraphPatternGroup<?> group) {

        if (group == null)
            throw new IllegalArgumentException();

        IQueryNode p = group.getParentGraphPatternGroup();

        if (p != null) {

            return p;

        }

        if (queryRoot.getNamedSubqueries() != null) {

            for (NamedSubqueryRoot namedSubquery : queryRoot
                    .getNamedSubqueries()) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> whereClause = (GraphPatternGroup<IGroupMemberNode>) namedSubquery
                        .getWhereClause();

                if (whereClause == group) {

                    return namedSubquery;

                }

                // Check the where clause.
                if ((p = findParent2(whereClause, group)) != null) {

                    return p;

                }

            }

        }

        {

            @SuppressWarnings("unchecked")
            final GraphPatternGroup<IGroupMemberNode> whereClause = (GraphPatternGroup<IGroupMemberNode>) queryRoot
                    .getWhereClause();

            if (whereClause == group) {

                return queryRoot;

            }

            // Check the where clause.
            if ((p = findParent2(whereClause, group)) != null) {

                return p;

            }

        }

        // Not found.
        return p;
    }

    /**
     * Search in aGroup for theGroup, peeking into
     * {@link QueryBase#getWhereClause()}, {@link ServiceNode#getGraphPattern()},
     * and all {@link SubqueryFunctionNodeBase} instances for any
     * {@link FilterNode}s.
     * 
     * @param aGroup
     *            A group which might be the "parent" of the group you are
     *            looking for.
     * @param theGroup
     *            The group which you are looking for.
     * 
     * @return The {@link QueryBase}, {@link ServiceNode}, or {@link FilterNode}
     *         which is the "parent" of <i>theGroup</i>.
     */
    static public IQueryNode findParent2(
            final GraphPatternGroup<IGroupMemberNode> aGroup,
            final GraphPatternGroup<?> theGroup) {

        if (aGroup == theGroup) {
            /*
             * The caller should have reported this. Now we no longer have the
             * context on hand.
             */
            throw new AssertionError();
        }
        
        final int arity = aGroup.arity();

        for (int i = 0; i < arity; i++) {

            final IGroupMemberNode child = (IGroupMemberNode) aGroup.get(i);            
            
            if (child instanceof QueryBase) {

                final QueryBase queryBase = (QueryBase) child;

                if (queryBase.getWhereClause() == theGroup) {

                    return queryBase;
                    
                }

            } else if (child instanceof ServiceNode) {

                final ServiceNode serviceNode = (ServiceNode) child;

                if (serviceNode.getGraphPattern() == theGroup) {

                    return serviceNode;
                    
                }

            } else if (child instanceof FilterNode) {

                final FilterNode filter = (FilterNode) child;
                
                final Iterator<SubqueryFunctionNodeBase> itr = BOpUtility
                        .visitAll(filter, SubqueryFunctionNodeBase.class);

                while (itr.hasNext()) {

                    final SubqueryFunctionNodeBase tmp = itr.next();

                    if (tmp.getGraphPattern() == theGroup) {

                        return filter;
                        
                    }
                
                }

            } else if (child instanceof ArbitraryLengthPathNode) {

                final ArbitraryLengthPathNode alpNode = (ArbitraryLengthPathNode) child;
                
                if (alpNode.subgroup() == theGroup) {
                    
                    return alpNode;
                    
                }
                
            }

        }
        
        // Not found.
        return null;

    }

//    /**
//     * Return the set of variables which are "in-scope" for a given node. This
//     * is based on bottom up evaluation semantics rather than the top-down,
//     * left-to-right evaluation order. The "in-scope" variables are the
//     * variables which are locally produced, which are produced in a child
//     * group, or which are produced in the parent when the parent's variables
//     * are in scope for the child (e.g., a FILTER in an OPTIONAL group can see
//     * the variables in the parent group).
//     * <p>
//     * Note: This method does NOT need to consider exogenous bindings. The scope
//     * of a variable is a completely different thing from whether or not the
//     * variable is must be bound in a given scope. If a variable has an
//     * exogenous binding but is not projected into a query, then it is still not
//     * visible in that query. If it is projected into the query, then it is in
//     * scope regardless of whether or not it has an exogenous binding and
//     * regardless of whether it MUST or MIGHT be bound.
//     * <p>
//     * This method should be used for bottom up analysis. It SHOULD NOT be used
//     * when you have a specific evaluation order and want to know whether or not
//     * a given variable is incoming bound or produced by a node in the query.
//     * 
//     * @param node
//     *            The node.
//     * @param vars
//     *            The caller's collection.
//     * 
//     * @return The caller's collection.
//     * 
//     * @see http://www.w3.org/TR/sparql11-query/#variableScope
//     * 
//     *      FIXME Test suite and implementation for "in-scope".
//     */
//    public Set<IVariable<?>> getInScopeVariables(final IGroupMemberNode node,
//            final Set<IVariable<?>> vars) {
//
//        final GraphPatternGroup<IGroupMemberNode> tmp;
//        
//        if (node instanceof GraphPatternGroup<?>) {
//
//            /*
//             * When given a group, report on the in-scope variable for this
//             * group.
//             */
//            tmp = (GraphPatternGroup<IGroupMemberNode>) node;
//
//        } else {
//
//            /*
//             * Report on the in-scope variables
//             */
//            tmp = (GraphPatternGroup<IGroupMemberNode>) node
//                .getParent();
//
//        }
//
//        getInScopeVars(tmp, vars);
//
//        return vars;
//        
//    }
//
//    /**
//     * Reports on all in-scope variables for a {@link JoinGroupNode} or
//     * {@link UnionNode}.
//     */
//    private Set<IVariable<?>> getInScopeVars(
//            final GraphPatternGroup<IGroupMemberNode> group,
//            final Set<IVariable<?>> vars) {
//
//        for(IGroupMemberNode child : group ) {
//
//            // TODO In scope means produced locally or in scope in the parent
//            // and visible locally.
//            getDefinitelyProducedBindings(sp, vars, false/* recursive */);
//
//        }
//
//        // Plus anything which is in scope in the parent.
//        {
//            
//            final JoinGroupNode p = findParentJoinGroup(group);
//
//            if (p != null) {
//
//                getInScopeVars(p, vars);
//
//            }
//
//        }
//        
//        return vars;
//
//    }
    
    /**
     * Return the set of variables which MUST be bound coming into this group
     * during top-down, left-to-right evaluation. The returned set is based on a
     * non-recursive analysis of the definitely (MUST) bound variables in each
     * of the parent groups. The analysis is non-recursive for each parent
     * group, but all parents of this group are considered. This approach
     * excludes information about variables which MUST or MIGHT be bound from
     * both <i>this</i> group and child groups.
     * <p>
     * This method DOES NOT pay attention to bottom up variable scoping rules.
     * Queries which are badly designed MUST be rewritten (by lifting out named
     * subqueries) such that they become well designed and adhere to bottom-up
     * evaluation semantics.
     * 
     * @param vars
     *            Where to store the "MUST" bound variables.
     * 
     * @return The argument.
     * 
     *         FIXME Both this and
     *         {@link #getMaybeIncomingBindings(IGroupMemberNode, Set)} need to
     *         consider the exogenous variables. Perhaps modify the
     *         StaticAnalysis constructor to pass in the exogenous
     *         IBindingSet[]?
     * 
     *         FIXME For some purposes we need to consider the top-down,
     *         left-to-right evaluation order. However, for others, such as when
     *         considering whether a variable appearing in a filter will be in
     *         scope, we need to consider whether there exists some evaluation
     *         order for which the variable would be in scope.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/412
     *      (StaticAnalysis#getDefinitelyBound() ignores exogenous variables.)
     */
    public Set<IVariable<?>> getDefinitelyIncomingBindings(
            final IGroupMemberNode node, final Set<IVariable<?>> vars) {
    
    	/*
    	 * Start by adding globally scoped and exogenous variables.
    	 */
    	if (evaluationContext != null) {
    	   
    	   vars.addAll(evaluationContext.getGloballyScopedVariables());
    		
    	   if (locatedInToplevelQuery(node)) {
       		final ISolutionSetStats stats = evaluationContext.getSolutionSetStats();
       		
       		// only add the vars that are always bound
       		vars.addAll(stats.getAlwaysBound());
    	   }    		
    	}
    	
        final GraphPatternGroup<?> parent = node.getParentGraphPatternGroup();
        
        /*
         * We've reached the root.
         */
        if (parent == null) {
            
            /*
             * FIXME This is unable to look upwards when the group is the graph
             * pattern of a subquery, a service, or a (NOT) EXISTS filter. Unit
             * tests. This could be fixed using a method which searched the
             * QueryRoot for the node having a given join group as its
             * annotation. However, that would not resolve the question of
             * evaluation order versus "in scope" visibility.
             * 
             * Use findParent(...) to fix this, but build up the test coverage
             * before making the code changes.
             */
            return vars;
            
        }

        /*
         * Do the siblings of the node first.  Unless it is a Union.  Siblings
         * don't see each other's bindings in a Union. 
         */
        if (!(parent instanceof UnionNode)) {
            
            for (IGroupMemberNode child : parent) {
                
                /*
                 * We've found ourself. Stop collecting vars.
                 */
                if (child == node) {
                    
                    break;
                    
                }
                
                if (child instanceof IBindingProducerNode) {
                    
                    final boolean optional = child instanceof IJoinNode
                            && ((IJoinNode) child).isOptional();

                    final boolean minus = child instanceof IJoinNode
                            && ((IJoinNode) child).isMinus();
                    
                    if (!optional && !minus) {
                        getDefinitelyProducedBindings(
                                (IBindingProducerNode) child, vars, true/* recursive */);
                    }
                    
                }
                
            }
            
        }
        
        /*
         * Next we recurse upwards to figure out what is definitely bound 
         * coming into the parent.  
         */
        return getDefinitelyIncomingBindings(parent, vars);
        
    }

    /**
     * Returns true if the current node is located (recursively) inside the
     * top-level query, false if it is nested inside a subquery or a
     * named subquery. The method does not look into {@link FilterNode}s,
     * but only recurses into {@link GroupNodeBase} nodes. 
     * 
     * @param node
     * @return
     */
    public boolean locatedInToplevelQuery(IGroupMemberNode node) {
       
       return locatedInGroupNode(queryRoot.getWhereClause(), node);

   }

    /**
     * Returns true if the current node is identical or (recursively) located
     * inside the given group scope or is the group node itself, but not a
     * subquery referenced in the node. The method does not look into
     * {@link FilterNode}s, but only recurses into {@link GroupNodeBase} nodes.
     * 
     * @param theNode the group we're looking in
     * @param theNode the node we're looking for
     * @return
     */
   public boolean locatedInGroupNode(
      final GroupNodeBase<?> theGroup, IGroupMemberNode theNode) {
      
      if (theGroup==null || theNode==null) {
         return false; // not found
      }
      
      if (theGroup==theNode)
         return true;
      
      for (IGroupMemberNode child : theGroup) {
         
         if (child instanceof GroupNodeBase<?>) {
            
            if (locatedInGroupNode((GroupNodeBase<?>)child, theNode))
               return true;
         }
      }
      
      return false; // not found
   }

   
   /**
     * Return the set of variables which MIGHT be bound coming into this group
     * during top-down, left-to-right evaluation. The returned set is based on a
     * non-recursive analysis of the "maybe" bound variables in each of the
     * parent groups. The analysis is non-recursive for each parent group, but
     * all parents of this group are considered. This approach excludes
     * information about variables which MUST or MIGHT be bound from both
     * <i>this</i> group and child groups.
     * <p>
     * This method DOES NOT pay attention to bottom up variable scoping rules.
     * Queries which are badly designed MUST be rewritten (by lifting out named
     * subqueries) such that they become well designed and adhere to bottom-up
     * evaluation semantics.
     * 
     * @param vars
     *            Where to store the "maybe" bound variables. This includes ANY
     *            variable which MIGHT or MUST be bound.
     * 
     * @return The argument.
     * 
     *         FIXME Both this and
     *         {@link #getDefinitelyIncomingBindings(IGroupMemberNode, Set)}
     *         need to consider the exogenous variables. Perhaps modify the
     *         StaticAnalysis constructor to pass in the exogenous
     *         IBindingSet[]?
     * 
     *         FIXME This is unable to look upwards when the group is the graph
     *         pattern of a subquery, a service, or a (NOT) EXISTS filter.
     *         
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/412
     */
    public Set<IVariable<?>> getMaybeIncomingBindings(
            final IGroupMemberNode node, final Set<IVariable<?>> vars) {

    	/*
    	 * Start by adding the exogenous variables.
    	 */
    	if (evaluationContext != null) {
    	   
    	   vars.addAll(evaluationContext.getGloballyScopedVariables());

    	   if (locatedInToplevelQuery(node)) {
    	      
       		final ISolutionSetStats stats = evaluationContext.getSolutionSetStats();
       		
       		// add the vars that are always bound and those that might be bound
       		vars.addAll(stats.getAlwaysBound());
       		vars.addAll(stats.getNotAlwaysBound());
    	   }
    		
    	}
    	
        final GraphPatternGroup<?> parent = node.getParentGraphPatternGroup();
        
        /*
         * We've reached the root.
         */
        if (parent == null) {
            
            return vars;
            
        }

        /*
         * Do the siblings of the node first.  Unless it is a Union.  Siblings
         * don't see each other's bindings in a Union.
         */
        if (!(parent instanceof UnionNode)) {
            
            for (IGroupMemberNode child : parent) {
                
                /*
                 * We've found ourself. Stop collecting vars.
                 */
                if (child == node) {
                    
                    break;
                    
                }
                
                if (child instanceof IBindingProducerNode) {
                    
//                    final boolean optional = child instanceof IJoinNode
//                            && ((IJoinNode) child).isOptional();

                    final boolean minus = child instanceof IJoinNode
                            && ((IJoinNode) child).isMinus();

                    if (/* !optional && */!minus) {
                        /*
                         * MINUS does not produce any bindings, it just removes
                         * solutions. On the other hand, OPTIONAL joins DO
                         * produce bindings, they are just "maybe" bindings.
                         */
                        getMaybeProducedBindings(
                                (IBindingProducerNode) child, vars, true/* recursive */);
                    }
                    
                }
                
            }
            
        }
        
        /*
         * Next we recurse upwards to figure out what is definitely bound 
         * coming into the parent.  
         */
        return getMaybeIncomingBindings(parent, vars);
        
    }

    /**
     * Return the set of variables which MUST be bound for solutions after the
     * evaluation of this group. A group will produce "MUST" bindings for
     * variables from its statement patterns and a LET based on an expression
     * whose variables are known bound.
     * <p>
     * The returned collection reflects "bottom-up" evaluation semantics. This
     * method does NOT consider variables which are already bound on entry to
     * the group.
     * <p>
     * Note: When invoked for an OPTIONAL or MINUS join group, the variables
     * which would become bound during the evaluation of the join group are
     * reported. Caller's who wish to NOT have variables reported for OPTIONAL
     * or MINUS groups MUST NOT invoke this method for those groups.
     * <p>
     * Note: The recursive analysis does not throw out variables when part of
     * the tree will provably fail to bind anything. It is the role of query
     * optimizers to identify those situations and prune the AST appropriately.
     * 
     * @param node
     *            The node to be analyzed.
     * @param vars
     *            Where to store the "MUST" bound variables.
     * @param recursive
     *            When <code>true</code>, the child groups will be recursively
     *            analyzed. When <code>false</code>, only <i>this</i> group will
     *            be analyzed.
     * 
     * @return The argument.
     */
    public Set<IVariable<?>> getDefinitelyProducedBindings(
            final IBindingProducerNode node, final Set<IVariable<?>> vars,
            final boolean recursive) {

        if (node instanceof GraphPatternGroup<?>) {
        
            if (node instanceof JoinGroupNode) {
            
                getDefinitelyProducedBindings((JoinGroupNode) node, vars,
                        recursive);
                
            } else if (node instanceof UnionNode) {
                
                getDefinitelyProducedBindings((UnionNode) node, vars, recursive);
                
            } else {
                
                throw new AssertionError(node.toString());
                
            }

        } else if(node instanceof StatementPatternNode) {

            final StatementPatternNode sp = (StatementPatternNode) node;
            
//            if(!sp.isOptional()) {
//
//                // Only if the statement pattern node is a required join.
                vars.addAll(sp.getProducedBindings());
//                
//            }
        } else if (node instanceof PropertyPathNode) {
            
            final PropertyPathNode ppn = (PropertyPathNode) node;
            vars.addAll(ppn.getProducedBindings());
            
        } else if (node instanceof ArbitraryLengthPathNode) {
        	
        	vars.addAll(((ArbitraryLengthPathNode) node).getDefinitelyProducedBindings());
        	
        } else if (node instanceof ZeroLengthPathNode) {
        	
        	vars.addAll(((ZeroLengthPathNode) node).getProducedBindings());
        	
        } else if(node instanceof SubqueryRoot) {

            final SubqueryRoot subquery = (SubqueryRoot) node;

            vars.addAll(getDefinitelyProducedBindings(subquery));

        } else if (node instanceof NamedSubqueryInclude) {

            final NamedSubqueryInclude nsi = (NamedSubqueryInclude) node;

            final String name = nsi.getName();
            
			final NamedSubqueryRoot nsr = getNamedSubqueryRoot(name);

			if (nsr != null) {

				vars.addAll(getDefinitelyProducedBindings(nsr));

			} else {

                final ISolutionSetStats stats = getSolutionSetStats(name);

                /*
                 * Note: This is all variables which are bound in ALL solutions.
                 */

                vars.addAll(stats.getAlwaysBound());

			}

        } else if(node instanceof ServiceNode) {

            final ServiceNode service = (ServiceNode) node;

            vars.addAll(getDefinitelyProducedBindings(service));

        } else if(node instanceof AssignmentNode) {
            
            /*
             * Note: BIND() in a group is only a "maybe" because the spec says
             * that an error when evaluating a BIND() in a group will not fail
             * the solution.
             * 
             * @see http://www.w3.org/TR/sparql11-query/#assignment (
             * "If the evaluation of the expression produces an error, the
             * variable remains unbound for that solution.")
             */

        } else if(node instanceof FilterNode) {

            // NOP.

        } else if(node instanceof BindingsClause) {

            final BindingsClause bc = (BindingsClause) node;
            
            vars.addAll(bc.getDeclaredVariables());
            
        } else {

            throw new AssertionError(node.toString());
            
        }

        return vars;
      
    }

    /**
     * Collect all variables appearing in the group. This DOES NOT descend
     * recursively into groups. It DOES report variables projected out of named
     * subqueries, SPARQL 1.1 subqueries, and SERVICE calls.
     * <p>
     * This has the same behavior as a non-recursive call obtain the definitely
     * bound variables PLUS the variables used by the filters in the group.
     * 
     * @param vars
     *            The variables are added to this set.
     * @param group
     *            The group whose variables will be reported.
     * @param includeFilters
     *            When <code>true</code>, variables appearing in FILTERs are
     *            also reported.
     * 
     * @return The caller's set.
     */
    public Set<IVariable<?>> getDefinitelyProducedBindingsAndFilterVariables( 
            final IGroupNode<? extends IGroupMemberNode> group,
            final Set<IVariable<?>> vars) {

        getDefinitelyProducedBindings((IBindingProducerNode) group, vars, false/* recursive */);

        for (IGroupMemberNode op : group) {

            if (op instanceof FilterNode) {

                addAll(vars, op);

            }
            
        }

        return vars;
        
    }

    /**
     * Return the set of variables which MUST or MIGHT be bound after the
     * evaluation of this join group.
     * <p>
     * The returned collection reflects "bottom-up" evaluation semantics. This
     * method does NOT consider variables which are already bound on entry to
     * the group.
     * 
     * @param vars
     *            Where to store the "MUST" and "MIGHT" be bound variables.
     * @param recursive
     *            When <code>true</code>, the child groups will be recursively
     *            analyzed. When <code>false</code>, only <i>this</i> group will
     *            be analyzed.
     *            
     * @return The caller's set.
     */
    public Set<IVariable<?>> getMaybeProducedBindings(
            final IBindingProducerNode node,//
            final Set<IVariable<?>> vars,//
            final boolean recursive) {

        if (node instanceof GraphPatternGroup<?>) {
        
            if (node instanceof JoinGroupNode) {
            
                getMaybeProducedBindings((JoinGroupNode) node, vars,
                        recursive);

            } else if (node instanceof UnionNode) {

                getMaybeProducedBindings((UnionNode) node, vars, recursive);

            } else {

                throw new AssertionError(node.toString());
                
            }

        } else if( node instanceof StatementPatternNode) {

            final StatementPatternNode sp = (StatementPatternNode) node;

//            if(sp.isOptional()) {
//
//                // Only if the statement pattern node is an optional join.
                vars.addAll(sp.getProducedBindings());
//                
//            }

        } else if (node instanceof PropertyPathNode) {
            
            final PropertyPathNode ppn = (PropertyPathNode) node;
            vars.addAll(ppn.getProducedBindings());
            
        } else if (node instanceof ArbitraryLengthPathNode) {
        	
        	vars.addAll(((ArbitraryLengthPathNode) node).getMaybeProducedBindings());
        	
        } else if (node instanceof ZeroLengthPathNode) {
        	
        	vars.addAll(((ZeroLengthPathNode) node).getProducedBindings());
        	
        } else if(node instanceof SubqueryRoot) {

            final SubqueryRoot subquery = (SubqueryRoot) node;

            vars.addAll(getMaybeProducedBindings(subquery));

        } else if (node instanceof NamedSubqueryInclude) {

            final NamedSubqueryInclude nsi = (NamedSubqueryInclude) node;

            final String name = nsi.getName();
            
			final NamedSubqueryRoot nsr = getNamedSubqueryRoot(name);

			if (nsr != null) {

				vars.addAll(getMaybeProducedBindings(nsr));
				
			} else {
				
                final ISolutionSetStats stats = getSolutionSetStats(name);

                /*
                 * Note: This is all variables bound in ANY solution. It MAY
                 * include variables which are NOT bound in some solutions.
                 */

                vars.addAll(stats.getUsedVars());

			}

        } else if(node instanceof ServiceNode) {

            final ServiceNode service = (ServiceNode) node;

            vars.addAll(getMaybeProducedBindings(service));

        } else if(node instanceof AssignmentNode) {

            /*
             * Note: BIND() in a group is only a "maybe" because the spec says
             * that an error when evaluating a BIND() in a group will not fail
             * the solution.
             * 
             * @see http://www.w3.org/TR/sparql11-query/#assignment (
             * "If the evaluation of the expression produces an error, the
             * variable remains unbound for that solution.")
             */

            vars.add(((AssignmentNode) node).getVar());
            
        } else if(node instanceof FilterNode) {

            // NOP

        } else if(node instanceof BindingsClause) {

            final BindingsClause bc = (BindingsClause) node;
            
            vars.addAll(bc.getDeclaredVariables());

        } else {
            
            throw new AssertionError(node.toString());
            
        }

        return vars;
      
    }

    /*
     * Private type specific helper methods.
     */

    // MUST : JOIN GROUP
    Set<IVariable<?>> getDefinitelyProducedBindings(
            final JoinGroupNode node, final Set<IVariable<?>> vars,
            final boolean recursive) {
        // Note: always report what is bound when we enter a group. The caller
        // needs to avoid entering a group which is optional if they do not want
        // it's bindings.
//        if(node.isOptional())
//            return vars;
        
        for (IGroupMemberNode child : node) {

            if(!(child instanceof IBindingProducerNode))
                continue;
            
            if (child instanceof StatementPatternNode) {

                final StatementPatternNode sp = (StatementPatternNode) child;

                if (!sp.isOptional()) {
                    
                    /*
                     * Required JOIN (statement pattern).
                     */

                    getDefinitelyProducedBindings(sp, vars, recursive);

                }
                
            } else if (child instanceof ArbitraryLengthPathNode) {
            	
            	vars.addAll(((ArbitraryLengthPathNode) child).getDefinitelyProducedBindings());
            	
            } else if (child instanceof ZeroLengthPathNode) {
            	
            	vars.addAll(((ZeroLengthPathNode) child).getProducedBindings());
            	
            } else if (child instanceof NamedSubqueryInclude
                    || child instanceof SubqueryRoot
                    || child instanceof ServiceNode) {

                /*
                 * Required JOIN (Named solution set, SPARQL 1.1 subquery,
                 * EXISTS, or SERVICE).
                 * 
                 * Note: We have to descend recursively into these structures in
                 * order to determine anything.
                 */

                vars.addAll(getDefinitelyProducedBindings(
                        (IBindingProducerNode) child,
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            } else if (child instanceof GraphPatternGroup<?>) {

                if (recursive) {

                    // Add anything bound by a child group.

                    final GraphPatternGroup<?> group = (GraphPatternGroup<?>) child;

                    if (!group.isOptional() && !group.isMinus()) {

                        getDefinitelyProducedBindings(group, vars, recursive);

                    }

                }
                
            } else if (child instanceof AssignmentNode) {

                /*
                 * Note: BIND() in a group is only a "maybe" because the spec says
                 * that an error when evaluating a BIND() in a group will not fail
                 * the solution.
                 * 
                 * @see http://www.w3.org/TR/sparql11-query/#assignment (
                 * "If the evaluation of the expression produces an error, the
                 * variable remains unbound for that solution.")
                 */

            } else if(child instanceof FilterNode) {

                // NOP
                
            } else if(child instanceof BindingsClause) {

                final BindingsClause bc = (BindingsClause) child;
                
                vars.addAll(bc.getDeclaredVariables());

            } else if (child instanceof PropertyPathNode) {
                
                getDefinitelyProducedBindings((PropertyPathNode)child, vars, recursive);

            } else {

                throw new AssertionError(child.toString());

            }

        }

        /*
         * Note: Assignments which have an error cause the variable to be left
         * unbound rather than failing the solution. Therefore assignment nodes
         * are handled as "maybe" bound, not "must" bound.
         */

        return vars;

    }

    // MAYBE : JOIN GROUP
    private Set<IVariable<?>> getMaybeProducedBindings(
            final JoinGroupNode node, final Set<IVariable<?>> vars,
            final boolean recursive) {

        // Add in anything definitely produced by this group (w/o recursion).
        getDefinitelyProducedBindings(node, vars, false/* recursive */);

        /*
         * Note: Assignments which have an error cause the variable to be left
         * unbound rather than failing the solution. Therefore assignment nodes
         * are handled as "maybe" bound, not "must" bound.
         */

        for (AssignmentNode bind : node.getAssignments()) {

            vars.add(bind.getVar());

        }

        if (recursive) {

            /*
             * Add in anything "maybe" produced by a child group.
             */

            for (IGroupMemberNode child : node) {

                if (child instanceof IBindingProducerNode) {

                    final IBindingProducerNode tmp = (IBindingProducerNode) child;
                    
                    if(tmp instanceof IJoinNode && ((IJoinNode)tmp).isMinus()) {
                        
                        // MINUS never contributes bindings, it only removes
                        // solutions.
                        continue;
                        
                    }

//                    vars.addAll(
                    getMaybeProducedBindings(tmp, vars, recursive)
//                            )
                    ;

                }
                
            }

        }

        return vars;

    }

    // MUST : UNION
    private Set<IVariable<?>> getDefinitelyProducedBindings(
            final UnionNode node,
            final Set<IVariable<?>> vars, final boolean recursive) {

        if (!recursive || node.isOptional() || node.isMinus()) {

            // Nothing to contribute
            return vars;
            
        }

        /*
         * Collect all definitely produced bindings from each of the children.
         */
        final Set<IVariable<?>> all = new LinkedHashSet<IVariable<?>>();

        final List<Set<IVariable<?>>> perChildSets = new LinkedList<Set<IVariable<?>>>();

        for (JoinGroupNode child : node) {

            final Set<IVariable<?>> childSet = new LinkedHashSet<IVariable<?>>();
            
            perChildSets.add(childSet);

            getDefinitelyProducedBindings(child, childSet, recursive);

            all.addAll(childSet);
            
        }

        /*
         * Now retain only those bindings which are definitely produced by each
         * child of the union.
         */
        for(Set<IVariable<?>> childSet : perChildSets) {
            
            all.retainAll(childSet);
            
        }
        
        // These are the variables which are definitely bound by the union.
        vars.addAll(all);
        
        return vars;

    }

    // MAYBE : UNION
    private Set<IVariable<?>> getMaybeProducedBindings(final UnionNode node,
            final Set<IVariable<?>> vars, final boolean recursive) {

        if (!recursive) {

            // Nothing to contribute.
            return vars;

        }

        /*
         * Collect all "maybe" bindings from each of the children.
         */
        for (JoinGroupNode child : node) {

            getMaybeProducedBindings(child, vars, recursive);

        }

        return vars;

    }

    /**
     * Report "MUST" bound bindings projected by the query. This involves
     * checking the WHERE clause and the {@link ProjectionNode} for the query.
     * Note that the projection can rename variables. It can also bind a
     * constant on a variable. Variables which are not projected by the query
     * will NOT be reported.
     * 
     * FIXME For a top-level query, any exogenously bound variables are also
     * definitely bound (in a subquery they are definitely bound if they are
     * projected into the subquery).
     * 
     * TODO  In the case when the variable is bound to an expression
     *       and the expression may execute with an error, this
     *       method incorrectly reports that variable as definitely bound
     *       see trac 750
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/412
     *      (StaticAnalysis#getDefinitelyBound() ignores exogenous variables.)
     *      
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/430 (StaticAnalysis
     *      does not follow renames of projected variables)
     *      
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/750
     *      artificial test case fails, currently wontfix
     */
    // MUST : QueryBase
    public Set<IVariable<?>> getDefinitelyProducedBindings(final QueryBase queryBase) {

        final ProjectionNode projection = queryBase.getProjection();
        
        if(projection == null) {

            // If there is no projection then there is nothing to report.
            return new LinkedHashSet<IVariable<?>>();

        }

        // The set of definitely bound variables in the query.
        final Set<IVariable<?>> definitelyBound = new LinkedHashSet<IVariable<?>>();
        
        @SuppressWarnings("unchecked")
        final GraphPatternGroup<IGroupMemberNode> whereClause = queryBase.getWhereClause();

        if (whereClause != null) {

            getDefinitelyProducedBindings(whereClause, definitelyBound, true/* recursive */);
            
            if (log.isInfoEnabled()) {
            	log.info(whereClause);
            	log.info(definitelyBound);
            }

        }

        /*
         * Now, we need to consider each select expression in turn. There are
         * several cases:
         * 
         * 1. Projection of a constant.
         * 
         * 2. Projection of a variable under the same name.
         * 
         * 3. Projection of a variable under a different name.
         * 
         * 4. Projection of a select expression which is not an aggregate.
         * 
         * This case is the one explored in trac750, and the code
         * below while usually correct is incorrect if the expression
         * can evaluate with an error - in which case the variable
         * will remain unbound.
         * 
         * 5. Projection of a select expression which is an aggregate. This case
         * is tricky. A select expression that is an aggregate which evaluates
         * to an error will cause an unbound value for to be reported for the
         * projected variable for the solution in which the error is computed.
         * Therefore, we must not assume that aggregation expressions MUST be
         * bound. (Given the schema flexible nature of RDF data, it is very
         * difficult to prove that an aggregate expression will never result in
         * an error without actually running the aggregation query.)
         * 
         * 6. Projection of an exogenously bound variable which is in scope.
         * 
         * TODO (6) is not yet handled! We need to know what variables are in
         * scope at each level as we descend into subqueries. Even if we know
         * the set of exogenous variables, the in scope exogenous varaibles are
         * not available in the typical invocation context.
         */
        {

            final boolean isAggregate = isAggregate(queryBase);
            
            /*
             * The set of projected variables which are definitely bound.
             */
            final Set<IVariable<?>> tmp = new LinkedHashSet<IVariable<?>>();

            for (AssignmentNode bind : projection) {

                if (bind.getValueExpression() instanceof IConstant<?>) {

                    /*
                     * 1. The projection of a constant.
                     * 
                     * Note: This depends on pre-evaluation of constant
                     * expressions. If the expression has not been reduced to a
                     * constant then it will not be detected by this test!
                     */

                    tmp.add(bind.getVar());

                    continue;

                }

                if (bind.getVar().equals(bind.getValueExpression())) {

                    if (definitelyBound.contains(bind.getVar())) {

                        /*
                         * 2. The projection of a definitely bound variable
                         * under the same name.
                         */

                        tmp.add(bind.getVar());

                    }
                    
                    continue;

                }

                if (bind.getValueExpression() instanceof IVariable<?>) {

                    if (definitelyBound.contains(bind.getValueExpression())) {

                        /*
                         * 3. The projection of a definitely bound variable
                         * under a different name.
                         */

                        tmp.add(bind.getVar());

                    }

                    continue;

                }

                if (!isAggregate) {

                    /*
                     * 4. The projection of a select expression which is not an
                     * aggregate. Normally, the projected variable will be 
                     * bound if all components of the select expression are
                     * definitely bound: this comment ignores the possibility
                     * that the expression may raise an error, in which case
                     * this block of code is incorrect.
                     * As of Oct 11, 2013 - we are no-fixing this
                     * because of caution about the performance impact, 
                     * and it seeming to be a corner case. See trac 750.
                     * 
                     * TODO Does coalesce() change the semantics for this
                     * analysis? If any of the values for coalesce() is
                     * definitely bound, then the coalesce() will produce a
                     * value. Can coalesce() be used to propagate an unbound
                     * value? If so, then we must either not assume that any
                     * value expression involving coalesce() is definitely bound
                     * or we must do a more detailed analysis of the value
                     * expression.
                     */
                    final Set<IVariable<?>> usedVars = getSpannedVariables(
                            (BOp) bind.getValueExpression(),
                            new LinkedHashSet<IVariable<?>>());

                    usedVars.removeAll(definitelyBound);

                    if (!usedVars.isEmpty()) {

                        /*
                         * There is at least one variable which is used by the
                         * select expression which is not definitely bound.
                         */
                        continue;

                    }

                    /*
                     * All variables used by the select expression are
                     * definitely bound so the projected variable for that
                     * select expression will be definitely bound.
                     */
                    tmp.add(bind.getVar());

                } else {
                	/* 5. Projection of a select expression which is an aggregate.
                	 * We do nothing
                	 */
                }
            	/* 6. Projection of an exogenously bound variable which is in scope.
            	 * We incorrectly do nothing
            	 */
                
            }

            return tmp;

        }

    }

    /**
     * Report the "MUST" and "MAYBE" bound bindings projected by the query. This
     * reduces to reporting the projected variables. We do not need to analyze
     * the whereClause or projection any further in order to know what "might"
     * be projected.
     */
    // MAYBE : QueryBase
    public Set<IVariable<?>> getMaybeProducedBindings(final QueryBase node) {

        final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();

        final ProjectionNode projection = node.getProjection();
        
        if(projection == null) {

            // If there is no projection then there is nothing to report.
            return vars;

        }

        return projection.getProjectionVars(vars);
        
    }

    /**
     * Report "MUST" bound bindings projected by the SERVICE. This involves
     * checking the graph pattern reported by
     * {@link ServiceNode#getGraphPattern()}.
     * <p>
     * Note: If the SERVICE URI is a variable, then it can only become bound
     * through some other operation. If the SERVICE variable never becomes
     * bound, then the SERVICE call can not run.
     */
    // MUST : ServiceNode
    public Set<IVariable<?>> getDefinitelyProducedBindings(
            final ServiceNode node) {

        final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();

        final GraphPatternGroup<IGroupMemberNode> graphPattern = (GraphPatternGroup<IGroupMemberNode>) node
                .getGraphPattern();

        if (graphPattern != null) {

            getDefinitelyProducedBindings(graphPattern, vars, true/* recursive */);

        }

        return vars;

    }

    /**
     * Report the "MUST" and "MAYBE" bound variables projected by the service.
     * This involves checking the graph pattern reported by
     * {@link ServiceNode#getGraphPattern()}. A SERVICE does NOT have an
     * explicit PROJECTION so it can not rename the projected bindings.
     */
    // MAY : ServiceNode
    public Set<IVariable<?>> getMaybeProducedBindings(final ServiceNode node) {

        final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
        
        final GraphPatternGroup<IGroupMemberNode> graphPattern = (GraphPatternGroup<IGroupMemberNode>) node.getGraphPattern();

        if (graphPattern != null) {

            getMaybeProducedBindings(graphPattern, vars, true/* recursive */);

        }

        return vars;

    }

    /*
     * FILTERS analysis for JoinGroupNodes
     */
    
    /**
     * Return only the filter child nodes in this group that will be fully bound
     * before running any of the joins in this group.
     * <p>
     * Note: Anything returned by this method should be lifted into the parent
     * group since it can be run before this group is evaluated. By lifting the
     * pre-filters into the parent group we can avoid issuing as many as-bound
     * subqueries for this group since those which fail the filter will not be
     * issued.
     * 
     * @param group
     *            The {@link JoinGroupNode}.
     * 
     * @return The filters which should either be run before the non-optional
     *         join graph or (preferably) lifted into the parent group.
     * 
     * @see ASTLiftPreFiltersOptimizer
     */
    public List<FilterNode> getPreFilters(final JoinGroupNode group) {

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> knownBound = getDefinitelyIncomingBindings(group,
                new LinkedHashSet<IVariable<?>>());

        /*
         * Get the filters that are bound by this set of known bound variables.
         */
        final List<FilterNode> filters = getBoundFilters(group,
                knownBound);

        return filters;

    }

    /**
     * Return only the filter child nodes in this group whose variables were not
     * fully bound on entry into the join group but which will be fully bound no
     * later than once we have run the required joins in this group.
     * 
     * @param group
     *            The {@link JoinGroupNode}.
     * 
     * @return The filters to be attached to the non-optional join graph for
     *         this group.
     */
    public List<FilterNode> getJoinFilters(final JoinGroupNode group) {

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> knownBound = getDefinitelyIncomingBindings(group,
                new LinkedHashSet<IVariable<?>>());

        /*
         * Add all the "must" bound variables for this group.
         * 
         * Note: We do not recursively compute the "must" bound variables for
         * this step because we are only interested in a FILTER which can be
         * attached to a non-optional JOIN run within this group.
         */
        getDefinitelyProducedBindings(group, knownBound, false/* recursive */);
        
        /*
         * Get the filters that are bound by this set of known bound variables.
         */
        final List<FilterNode> filters = getBoundFilters(group,
                knownBound);

        /*
         * Remove the preConditional filters (those fully bound by just incoming
         * bindings).
         */
        filters.removeAll(getPreFilters(group));
        
        return filters;
        
    }

    /**
     * Return only the filter child nodes in this group that will not be fully
     * bound even after running the <em>required</em> joins in this group.
     * <p>
     * Note: It is possible that some of these filters will be fully bound due
     * to nested optionals and unions.
     * <p>
     * Note: This will report any filters which are not pre-filters and are
     * not-join filters, including filters which are prune-filters. An AST
     * optimizer is responsible for identifying and removing filters which
     * should be pruned. Until they have been pruned, they will continue to be
     * reported by this method.
     * 
     * @param group
     *            The {@link JoinGroupNode}.
     * 
     * @return The filters to be run last in the group (after the nested
     *         optionals and unions).
     */
    public List<FilterNode> getPostFilters(final JoinGroupNode group) {

        /*
         * Start with all the filters in this group.
         */
        final List<FilterNode> filters = group.getAllFiltersInGroup();

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> knownBound = getDefinitelyIncomingBindings(group,
                new LinkedHashSet<IVariable<?>>());

        /*
         * Add all the "must" bound variables for this group.
         * 
         * Note: We do not recursively compute the "must" bound variables for
         * this step because we are only interested in FILTERs which can be
         * attached to a required JOIN run within this group. However, this
         * SHOULD consider statement pattern joins, named subquery include
         * joins, SPARQL 1.1 subquery joins, and service call joins -- all of
         * which are required joins.
         */
        getDefinitelyProducedBindings(group, knownBound, false/* recursive */);

        /*
         * Get the filters that are bound by this set of known bound variables.
         */
        final Collection<FilterNode> preAndJoinFilters = getBoundFilters(group,
                knownBound);

        /*
         * Remove the preFilters and joinFilters, leaving only the postFilters.
         * 
         * Note: This approach deliberately will report any filter which would
         * not have already been run for the group.
         */
        filters.removeAll(preAndJoinFilters);

        return filters;
        
    }

    /**
     * Return any filters can not succeed based on the "incoming", "must" and
     * "may" bound variables for this group. These filters are candidates for
     * pruning.
     * <p>
     * Note: Filters containing a {@link FunctionNode} for
     * {@link FunctionRegistry#BOUND} MUST NOT be pruned and are NOT reported by
     * this method.
     * 
     * @param group
     *            The {@link JoinGroupNode}.
     * 
     * @return The filters which are known to fail.
     * 
     *         TODO It is possible to prune a BOUND(?x) or NOT BOUND(?x) filter
     *         through a more detailed analysis of the value expression. If the
     *         variable <code>?x</code> simply does not appear in the group or
     *         any child of that group, then BOUND(?x) can be replaced by
     *         <code>false</code> and NOT BOUND(?x) by <code>true</code>.
     *         <p>
     *         However, in order to do this we must also look at any exogenous
     *         solution(s) (those supplied with the query when it is being
     *         evaluated). If the variable is bound in some exogenous solutions
     *         then it could be bound when the FILTER is run and the filter can
     *         not be pruned.
     * 
     * @deprecated This is now handled by {@link ASTBottomUpOptimizer}. I think
     *             that we will not need this method (it is only invoked from
     *             the test suite at this point).
     */
    public List<FilterNode> getPruneFilters(final JoinGroupNode group) {

        /*
         * Start with all the filters in this group.
         */
        final List<FilterNode> filters = group.getAllFiltersInGroup();

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> maybeBound = getDefinitelyIncomingBindings(group, new LinkedHashSet<IVariable<?>>());

        /*
         * Add all "must" / "may" bound variables for this group (recursively).
         */
        getMaybeProducedBindings(group, maybeBound, true/* recursive */);

        /*
         * Get the filters that are bound by this set of "maybe" bound variables.
         */
        final Collection<FilterNode> maybeFilters = getBoundFilters(group,
                maybeBound);

        /*
         * Remove the maybe bound filters, leaving only those which can not
         * succeed.
         */
        filters.removeAll(maybeFilters);
        
        /*
         * Collect all maybeFilters which use BOUND(). These can not be failed
         * as easily.
         */
        
        final Set<FilterNode> isBoundFilters = new LinkedHashSet<FilterNode>();
        
        for (FilterNode filter : maybeFilters) {

            final IValueExpressionNode node = filter.getValueExpressionNode();
            
            if (node instanceof FunctionNode) {
            
                if (((FunctionNode) node).isBound()) {
                
                    isBoundFilters.add(filter);
                    
                }
                
            }
            
        }

        // Remove filters which use BOUND().
        filters.removeAll(isBoundFilters);
        
        return filters;
        
    }
    
    /**
     * Helper method to determine the set of filters that will be fully bound
     * assuming the specified set of variables is bound.
     */
    private final List<FilterNode> getBoundFilters(
            final JoinGroupNode group, final Set<IVariable<?>> knownBound) {

        final List<FilterNode> filters = new LinkedList<FilterNode>();

        for (IQueryNode node : group) {

            if (!(node instanceof FilterNode))
                continue;

            final FilterNode filter = (FilterNode) node;

            final Set<IVariable<?>> filterVars = filter.getConsumedVars();

            boolean allBound = true;

            for (IVariable<?> v : filterVars) {

                allBound &= knownBound.contains(v);

            }

            if (allBound) {

                filters.add(filter);

            }

        }

        return filters;

    }

    /*
     * Materialization pipeline support.
     */
    
    /**
     * Use the {@link INeedsMaterialization} interface to find and collect
     * variables that need to be materialized for this constraint.
     */
    @SuppressWarnings("rawtypes")
    public static boolean requiresMaterialization(final IConstraint c) {
    
        return StaticAnalysis.gatherVarsToMaterialize(c,
                new LinkedHashSet<IVariable<IV>>()) != Requirement.NEVER;
    
    }
    
    /**
     * Static helper used to determine materialization requirements.
     */
    public static INeedsMaterialization.Requirement gatherVarsToMaterialize(
        final BOp c, final Set<IVariable<IV>> terms) {
        
        return gatherVarsToMaterialize(c, terms, false /* includeVarsInAnnotations */);
    }
    
    /**
     * Static helper used to determine materialization requirements.
     * 
     * TODO This should also reason about datatype constraints on variables. If
     * we know that a variable is constrained in a given scope to only take on a
     * data type which is associated with an {@link FullyInlineTypedLiteralIV}
     * or a specific numeric data type, then some operators may be able to
     * operate directly on that {@link IV}. This is especially interesting for
     * aggregates.
     */
    @SuppressWarnings("rawtypes")
    public static INeedsMaterialization.Requirement gatherVarsToMaterialize(
            final BOp c, final Set<IVariable<IV>> terms, final boolean includeVarsInAnnotations) {
    
        boolean materialize = false;
        boolean always = false;
        
        final Iterator<BOp> it = 
                includeVarsInAnnotations ? 
                BOpUtility.preOrderIteratorWithAnnotations(c) : BOpUtility.preOrderIterator(c);
        
        while (it.hasNext()) {
            
            final BOp bop = it.next();
            
            if (bop instanceof INeedsMaterialization) {
                
                final INeedsMaterialization bop2 = (INeedsMaterialization) bop;
                
                final Set<IVariable<IV>> t = getVarsFromArguments(bop);
                
                if (t.size() > 0) {
                    
                    terms.addAll(t);
                    
                    materialize = true;
                    
                    // if any bops have terms that always needs materialization
                    // then mark the whole constraint as such
                    if (bop2.getRequirement() == Requirement.ALWAYS) {
                        
                        always = true;
                        
                    }
                    
                }
                
            }
    
        }
    
        return materialize ? (always ? Requirement.ALWAYS
                : Requirement.SOMETIMES) : Requirement.NEVER;
    
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Set<IVariable<IV>> getVarsFromArguments(final BOp c) {
    
        final int arity = c.arity();
        
        final Set<IVariable<IV>> terms = new LinkedHashSet<IVariable<IV>>(arity);
    
        for (int i = 0; i < arity; i++) {
    
            final BOp arg = c.get(i);
    
            if (arg != null) {
    
                if (arg instanceof IValueExpression
                        && arg instanceof IPassesMaterialization) {
    
                    terms.addAll(getVarsFromArguments(arg));
    
                } else if (arg instanceof IVariable) {
    
                    terms.add((IVariable<IV>) arg);
    
                }
    
            }
    
        }
    
        return terms;
    
    }

    /**
     * Identify the join variables for the specified INCLUDE for the position
     * within the query in which it appears.
     * 
     * @param aNamedSubquery
     *            The named subquery.
     * @param anInclude
     *            An include for that subquery.
     */
    public Set<IVariable<?>> getJoinVars(
            final NamedSubqueryRoot aNamedSubquery,
            final NamedSubqueryInclude anInclude, final Set<IVariable<?>> vars) {

        return _getJoinVars(aNamedSubquery, anInclude, vars);

    }
    
    /**
     * Identify the join variables for the specified subquery for the position
     * within the query in which it appears.
     * 
     * @param aSubquery
     *            The subquery.
     * @param vars
     * 
     * @return The join variables.
     */
    public Set<IVariable<?>> getJoinVars(final SubqueryRoot subquery,
            final Set<IVariable<?>> vars) {

        return _getJoinVars(subquery, subquery, vars);

    }
    
    /**
     * Identify the join variables for the specified subquery for the position
     * within the query in which it appears. For a named subquery, it considers
     * the position in which the INCLUDE appears.
     * 
     * @param aSubquery
     *            Either a {@link NamedSubqueryRoot} or a {@link SubqueryRoot}.
     * @param theNode
     *            The node which represents the subquery in the join group. For
     *            a named subquery, this will be a {@link NamedSubqueryInclude}.
     *            For a {@link SubqueryRoot}, it is just the
     *            {@link SubqueryRoot} itself.
     * 
     * @return The join variables.
     */
    private Set<IVariable<?>> _getJoinVars(final SubqueryBase aSubquery,
            final IGroupMemberNode theNode, final Set<IVariable<?>> vars) {

        /*
         * The variables which are projected by the subquery which will be
         * definitely bound based on an analysis of the subquery.
         */
        final Set<IVariable<?>> boundBySubquery = getDefinitelyProducedBindings(aSubquery);

        if (log.isInfoEnabled()) {
        	log.info(boundBySubquery);
        }
        
        /*
         * The variables which are possibly bound on entry to the join group
         * in which the subquery appears.
         */
        final Set<IVariable<?>> incomingBindings = getDefinitelyIncomingBindings(
                theNode, new LinkedHashSet<IVariable<?>>());
        
        if (log.isInfoEnabled()) {
        	log.info(incomingBindings);
        }
        
        /*
         * This is only those variables which are bound on entry into the group
         * in which the subquery join appears *and* which are "must" bound
         * variables projected by the subquery.
         */
        boundBySubquery.retainAll(incomingBindings);
            
        if (log.isInfoEnabled()) {
        	log.info(boundBySubquery);
        }
        
        vars.addAll(boundBySubquery);

        if (log.isInfoEnabled()) {
        	log.info(vars);
        }
        
        return vars;

    }
    
    /**
     * Return the join variables for a SERVICE.
     * 
     * @param serviceNode
     * @param vars
     * @return 
     */
    public Set<IVariable<?>> getJoinVars(final ServiceNode serviceNode,
            final Set<IVariable<?>> vars) {

        /*
         * The variables which will be definitely bound based on an analysis of
         * the SERVICE.
         */
        final Set<IVariable<?>> boundByService = getDefinitelyProducedBindings(serviceNode);

        /*
         * The variables which are definitely bound on entry to the join group
         * in which the SERVICE appears.
         */
        final Set<IVariable<?>> incomingBindings = getDefinitelyIncomingBindings(
                serviceNode, new LinkedHashSet<IVariable<?>>());
        
        /*
         * This is only those variables which are bound on entry into the group
         * in which the SERVICE join appears *and* which are "must" bound
         * variables projected by the SERVICE.
         */
        boundByService.retainAll(incomingBindings);
            
        vars.addAll(boundByService);

        return vars;

    }
    
    /**
     * Return the join variables for a VALUES clause (embedded only - not
     * top-level).
     * 
     * @param bc The VALUES clause (a bunch of solutions)
     * @param stats A static analysis of those solutions.
     * @param vars
     * @return 
     */
    public Set<IVariable<?>> getJoinVars(final BindingsClause bc,
            final ISolutionSetStats stats,
            final Set<IVariable<?>> vars) {

        /*
         * The variables which will be definitely bound based on the solutions
         * in the VALUES clause.
         * 
         * Note: Collection is not modifyable, so we copy it.
         */
        final Set<IVariable<?>> boundByBindingsClause = new LinkedHashSet<IVariable<?>>(
                stats.getAlwaysBound());

        /*
         * The variables which are definitely bound on entry to the join group
         * in which the VALUES clause appears.
         */
        final Set<IVariable<?>> incomingBindings = getDefinitelyIncomingBindings(
                bc, new LinkedHashSet<IVariable<?>>());
        
        /*
         * This is only those variables which are bound on entry into the group
         * in which the VALUES join appears *and* which are "must" bound
         * variables projected by the VALUES.
         * 
         * FIXME Is this the correct semantics? I followed the pattern for SERVICE.
         */
        boundByBindingsClause.retainAll(incomingBindings);
            
        vars.addAll(boundByBindingsClause);

        return vars;

    }
    

    /**
	 * Return the join variables for an INCLUDE of a pre-existing named solution
	 * set.
	 * 
	 * @param nsi
	 *            The {@link NamedSubqueryInclude}
	 * @param solutionSet
	 *            The name of a pre-existing solution set.
	 * @param vars
	 *            The caller's collection.
	 *            
	 * @return The caller's collection.
	 */
	public Set<IVariable<?>> getJoinVars(final NamedSubqueryInclude nsi,
			final String solutionSet, final Set<IVariable<?>> vars) {

		final String name = solutionSet;
		
        /*
         * The variables which will be definitely bound based on the statistics
         * collected for that solution set.
         */
		final ISolutionSetStats stats = getSolutionSetStats(name);
		
		/*
		 * All variables which are bound in each solution of this solution set.
		 * 
		 * Note: The summary data for a named solution set is typically
		 * immutable, so we insert the variables into a mutable collection in
		 * order to make changes to that collection below.
		 */
		final Set<IVariable<?>> boundInSolutionSet = new LinkedHashSet<IVariable<?>>(
				stats.getAlwaysBound());

		/*
		 * The variables which are definitely bound on entry to the INCLUDE
		 * operator based on the static analysis of the query, including where
		 * it appears in the join order of the query.
		 */
		final Set<IVariable<?>> incomingBindings = getDefinitelyIncomingBindings(
				nsi, new LinkedHashSet<IVariable<?>>());
        
        /*
		 * This is only those variables which are bound on entry into the
		 * INCLUDE *and* which are "must" bound variables projected by the
		 * pre-existing named solution set.
		 */
        boundInSolutionSet.retainAll(incomingBindings);
            
        vars.addAll(boundInSolutionSet);

        return vars;

    }
    
    /**
     * Return any variables which are used after the given node in the current
     * ordering of its parent {@link JoinGroupNode} but DOES NOT consider the
     * parent or the PROJECTION for the query in which this group appears.
     * 
     * @param node
     *            A node which is a direct child of some {@link JoinGroupNode}.
     * @param vars
     *            Where to store the variables.
     * 
     * @return The caller's set.
     * 
     * @throws IllegalArgumentException
     *             if the <i>node</i> is not the direct child of some
     *             {@link JoinGroupNode}.
     */
    public Set<IVariable<?>> getAfterVars(final IGroupMemberNode node,
            final Set<IVariable<?>> vars) {

        if (node.getParent() == null) {
            // Immediate parent MUST be defined.
            throw new IllegalArgumentException();
        }

        if (!(node.getParent() instanceof JoinGroupNode)) {
            // Immediate parent MUST be a join group node.
            throw new IllegalArgumentException();
        }
        
        final JoinGroupNode p = node.getParentJoinGroup();
        
        boolean found = false;
        
        for (IGroupMemberNode c : p) {
        
            if (found) {
            
                // Add in any variables referenced after this proxy node.
                getSpannedVariables((BOp) c, true/* filters */, vars);
                
            }

            if (c == node) {
            
                // Found the position of the proxy node in the group.
                found = true;
                
            }

        }

        assert found;
        
        return vars;
        
    }

    /**
     * Return the set of variables which must be projected if the group is to be
     * converted into a sub-query. This method identifies variables which are
     * either MUST or MIGHT bound outside of the group which are also used
     * within the group and includes them in the projection. It also identified
     * variables used after the group (in the current evaluation order) which
     * are also used within the group and include them in the projection.
     * <p>
     * When considering the projection of the (sub-)query in which the group
     * appears, the SELECT EXPRESSIONS are consulted to identify variables which
     * we need to project out of the group.
     * 
     * @param proxy
     *            The join group which will be replaced by a sub-query. This is
     *            used to decide which variables are known bound (and hence
     *            should be projected into the WHERE clause if they are used
     *            within that WHERE clause). It is also used to decide which
     *            variables which become bound in the WHERE clause will be used
     *            outside of its scope and hence must be projected out of the
     *            WHERE clause. (The parent of this proxy MUST be a
     *            {@link JoinGroupNode}, not a {@link UnionNode} and not
     *            <code>null</code>. This condition is readily satisified if the
     *            rewrite is considering the children of some join group node as
     *            the parent of the proxy will be that join group node.)
     * @param groupToLift
     *            The group which is being lifted out and whose projection will
     *            be computed.
     * @param query
     *            The query (or sub-query) in which that proxy node exists. This
     *            is used to identify anything which is PROJECTed out of the
     *            query.
     * @param exogenousVars
     *            Any variables which are bound outside of the query AND known
     *            to be in scope (exogenous variables in a sub-select are only
     *            in scope if they are projected into the sub-select).
     * @param projectedVars
     *            The variables which must be projected will be added to this
     *            collection.
     * @return The projection.
     * 
     *         TODO We should recognize conditions under which this can be made
     *         into a DISTINCT projection. This involves a somewhat tricky
     *         analysis of the context in which each projected variable is used.
     *         There is *substantial* benefit to be gained from this analysis as
     *         a DISTINCT projection can radically reduce the size of the
     *         intermediate solution sets and the work performed by the overall
     *         query. However, if the analysis is incorrect and we mark the
     *         PROJECTION as DISTINCT when that is not allowed by the semantics
     *         of the query, then the query will not have the same behavior. So,
     *         getting this analysis correct is very important.
     */
    public Set<IVariable<?>> getProjectedVars(
            final IGroupMemberNode proxy,
            final GraphPatternGroup<?> groupToLift,//
            final QueryBase query,// 
            final Set<IVariable<?>> exogenousVars,//
            final Set<IVariable<?>> projectedVars) {

        // All variables which are used within the WHERE clause.
        final Set<IVariable<?>> groupVars = getSpannedVariables(groupToLift,
                new LinkedHashSet<IVariable<?>>());

        /*
         * Figure out what we need to project INTO the group.
         */
        
        // All variables which might be incoming bound into the proxy node.
        final Set<IVariable<?>> beforeVars = getMaybeIncomingBindings(
                proxy, new LinkedHashSet<IVariable<?>>());

        // Add in anything which is known to be bound outside of the query.
        beforeVars.addAll(exogenousVars);

        // Drop anything not used within the group.
        beforeVars.retainAll(groupVars);

        /*
         * Figure out what we need to project FROM the group.
         */

        // All variables used after the proxy node in its's parent join group.
        final Set<IVariable<?>> afterVars = getAfterVars(proxy,
                new LinkedHashSet<IVariable<?>>());
        
        // Gather the variables used by the SELECT EXPRESSIONS which are
        // projected out of the query in which this group appears.
        query.getSelectExprVars(afterVars);

        // Drop anything not used within the group.
        afterVars.retainAll(groupVars);
        
        /*
         * The projection for the group is anything MAYBE bound on entry to the
         * group which is also used within the group PLUS anything used after
         * the group which is used within the group.
         */
        projectedVars.addAll(beforeVars);
        projectedVars.addAll(afterVars);
        
        return projectedVars;

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
    public static boolean isAggregate(final ProjectionNode projection,
            final GroupByNode groupBy, final HavingNode having) {

        if (groupBy != null && !groupBy.isEmpty())
            return true;

        if (having != null && !having.isEmpty())
            return true;

        if (projection != null) {

            for (IValueExpressionNode exprNode : projection) {

                if (isAggregateExpressionNode(exprNode)) {
                    return true;
                }

            }

        }

        return false;

    }

    /**
     * Checks if given expression node is or contains any aggregates
     * <br><br>
     * After refactoring of SPARQL parser (https://jira.blazegraph.com/browse/BLZG-1176),
     * AggregationNode needs to be checked recuresively, as its value expression is not completely parsed, but could be an aggregate, that should result in failing checks while preparing queries.
     * For example, following test is failing without this check: com.bigdata.rdf.sail.sparql.BigdataSPARQL2ASTParserTest.test_agg10() 
     * 
     * @param exprNode - expression node to be checked
     */
    private static boolean isAggregateExpressionNode(IValueExpressionNode exprNode) {
        
                final IValueExpression<?> expr = exprNode.getValueExpression();

                if (expr == null) {
                    
                    /*
                     * The value expression is not cached....
                     */
                    
                    if (exprNode instanceof AssignmentNode) {
                        return isAggregateExpressionNode(((AssignmentNode) exprNode).getValueExpressionNode());
                    }

                    if (exprNode instanceof FunctionNode) {

                        /*
                         * Hack used when the BigdataExprBuilder needs to decide
                         * on the validity of aggregate expressions before we
                         * get around to caching the value expressions during
                         * evaluation (i.e., to pass the compliance tests for
                         * the parser).
                         */
                        final FunctionNode functionNode = (FunctionNode) exprNode;

                        if (FunctionRegistry.isAggregate(functionNode
                                .getFunctionURI()))

                            return true;

                    }
                    
                    return false;

                }

                if (isObviousAggregate(expr)) {

                    return true;

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
     * Extract the set of variables contained in a binding set.
     * @param bss
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Set<IVariable<?>> getVarsInBindingSet(final List<IBindingSet> bss) {
       Set<IVariable<?>> bssVars = new HashSet<IVariable<?>>();
       for (int i=0; i<bss.size(); i++) {
          
          final IBindingSet bs = bss.get(i);
          
          final Iterator<IVariable> bsVars = bs.vars();
          
          while (bsVars.hasNext()) {
             bssVars.add(bsVars.next());
          }
          
       }
       return bssVars;
    }
    
    /**
     * Checks whether the filter node's value expression node is in CNF.
     */
    static public boolean isCNF(final FilterNode filter) {
       return isCNF(filter.getValueExpressionNode());
    }

    /**
     * Checks whether the given value expression node is in CNF.
     * 
     * @param vexpr
     * 
     */
    static public boolean isCNF(final IValueExpressionNode vexpr) {
       
       if(!(vexpr instanceof FunctionNode)) {
          return true; 
       }
       
       final FunctionNode functionNode = (FunctionNode)vexpr;
       final URI functionURI = functionNode.getFunctionURI();
       
       if (functionURI.equals(FunctionRegistry.NOT)) {
          
          return isCNFNegationOrTerminal(functionNode);
          
       } else if (functionURI.equals(FunctionRegistry.OR)) {
          
          return isCNFDisjunct(functionNode);
          
       } else if (functionURI.equals(FunctionRegistry.AND)) {
          
          return isCNF((ValueExpressionNode)functionNode.get(0)) &&
                   isCNF((ValueExpressionNode)functionNode.get(1));
          
       } else {

          return true;  // everything else is a terminal

       }
    }


    /**
     * Check if filter node is an inner disjunct within a CNF. In particular,
     * it must not contain any other conjunctive nodes.
     * 
     * @param functionNode
     * @return
     */
    static public boolean isCNFDisjunct(final FunctionNode functionNode) {

       final URI functionURI = functionNode.getFunctionURI();
       
       if (functionURI.equals(FunctionRegistry.NOT)) {
          
          return isCNFNegationOrTerminal(functionNode);
          
       } else if (functionURI.equals(FunctionRegistry.OR)) {
          
          boolean isCNFDisjunct = 
             !(functionNode.get(0) instanceof FunctionNode) ||
             isCNFDisjunct((FunctionNode)functionNode.get(0));
          
          isCNFDisjunct &= 
                !(functionNode.get(1) instanceof FunctionNode) ||
                isCNFDisjunct((FunctionNode)functionNode.get(1));
          
          return isCNFDisjunct;        
          
       } else if (functionURI.equals(FunctionRegistry.AND)) {
          
          return false; // not allowed
          
       } else {
          
          return true; // everything else is a terminal
       }
    }


    /**
     * Check if filter node is a negation (possibly recursive) or terminal
     * within a CNF. In particular, it must not contain any other disjuncts
     * or conjuncts.
     * 
     * @param functionNode
     */
    static public boolean isCNFNegationOrTerminal(final FunctionNode functionNode) {

       final URI functionURI = functionNode.getFunctionURI();
       if (functionURI.equals(FunctionRegistry.AND) || 
           functionURI.equals(FunctionRegistry.OR)) {
          
          return false;
          
       } else if (functionURI.equals(FunctionRegistry.NOT)) {
          
          final BOp bop = functionNode.get(0);
          if (!(bop instanceof FunctionNode)) {

             return true; // terminal
             
          } else {
          
             return isCNFNegationOrTerminal((FunctionNode)bop);
             
          }
          
       } else {
          
          return true;  // everything else is a terminal
       }
    }


    /**
     * Returns the corresponding (equivalent) value expression in CNF. Makes
     * a copy of the original value expression, leaving it unmodified.
     * 
     * @param vexpr
     * @return null if the value expression is already in CNF, an equivalent
     *         value expression in CNF otherwise
     */
    static public IValueExpressionNode toCNF(final IValueExpressionNode vexpr) {
       
       final IValueExpressionNode copy = 
          (IValueExpressionNode)BOpUtility.deepCopy((BOp) vexpr);
       
       return pushDisjuncts(pushNegations(copy));
    }


    /**
     * Recursively pushes negations down the operator tree, such that in the
     * returned node, negations are always at the bottom of the tree. In
     * particular, all AND and OR value expressions will be situated above
     * negations. 
     * 
     * The resulting {@link IValueExpressionNode} is logically equivalent.
     */
    static public IValueExpressionNode pushNegations(IValueExpressionNode vexp) {

       if(!(vexp instanceof FunctionNode)) {
          return vexp;
       }
       
       final FunctionNode functionNode = (FunctionNode)vexp;
       final URI functionURI = functionNode.getFunctionURI();
       
       if (functionURI.equals(FunctionRegistry.NOT)) {
          
          final IValueExpressionNode inner = 
             (IValueExpressionNode) functionNode.get(0);
          
          if(inner instanceof FunctionNode) {
             
             final FunctionNode innerFunctionNode = (FunctionNode)inner;
             final URI innerFunctionURI = innerFunctionNode.getFunctionURI();
             
             if (innerFunctionURI.equals(FunctionRegistry.AND)) {

                final IValueExpressionNode negLeft = 
                   pushNegations(
                      FunctionNode.NOT(
                         (ValueExpressionNode)innerFunctionNode.get(0)));
                final IValueExpressionNode negRight = 
                   pushNegations(
                      FunctionNode.NOT(
                         (ValueExpressionNode)innerFunctionNode.get(1)));
                
                return FunctionNode.OR(
                   (ValueExpressionNode)negLeft, 
                   (ValueExpressionNode)negRight);
                
             } else if (innerFunctionURI.equals(FunctionRegistry.OR)) {

                final IValueExpressionNode negLeft = 
                   pushNegations(
                      FunctionNode.NOT(
                         (ValueExpressionNode)innerFunctionNode.get(0)));
                final IValueExpressionNode negRight = 
                   pushNegations(
                      FunctionNode.NOT(
                         (ValueExpressionNode)innerFunctionNode.get(1)));
                   
                return FunctionNode.AND(
                   (ValueExpressionNode)negLeft, 
                   (ValueExpressionNode)negRight);
                   
             } else if (innerFunctionURI.equals(FunctionRegistry.NOT)) {
               
                // drop double negation
                final BOp innerInner = innerFunctionNode.get(0);
                functionNode.setArg(0, innerInner);
                
                // recurse if necessary
                if (innerInner instanceof IValueExpressionNode) {
                   return pushNegations((IValueExpressionNode)innerInner);
                }
                
             } else if (innerFunctionURI.equals(FunctionRegistry.EQ)) {
                
                // invert: = -> !=
                return FunctionNode.NE(
                   (ValueExpressionNode)innerFunctionNode.get(0), 
                   (ValueExpressionNode)innerFunctionNode.get(1));
                
             } else if (innerFunctionURI.equals(FunctionRegistry.NE)) {
                
                // invert: != -> =
                return FunctionNode.EQ(
                   (ValueExpressionNode)innerFunctionNode.get(0), 
                   (ValueExpressionNode)innerFunctionNode.get(1));
                              
             } else if (innerFunctionURI.equals(FunctionRegistry.LE)) {
                
                // invert: <= -> >
                return FunctionNode.GT(
                   (ValueExpressionNode)innerFunctionNode.get(0), 
                   (ValueExpressionNode)innerFunctionNode.get(1));

                
             } else if (innerFunctionURI.equals(FunctionRegistry.LT)) {
                
                // invert: < -> >=
                return FunctionNode.GE(
                   (ValueExpressionNode)innerFunctionNode.get(0), 
                   (ValueExpressionNode)innerFunctionNode.get(1));
                
             } else if (innerFunctionURI.equals(FunctionRegistry.GE)) {
                
                // invert: >= -> <
                return FunctionNode.LT(
                   (ValueExpressionNode)innerFunctionNode.get(0), 
                   (ValueExpressionNode)innerFunctionNode.get(1));
                
             } else if (innerFunctionURI.equals(FunctionRegistry.GT)) {
                
                // invert: > -> <=
                return FunctionNode.LE(
                   (ValueExpressionNode)innerFunctionNode.get(0), 
                   (ValueExpressionNode)innerFunctionNode.get(1));

             }
          }
          
       } else if (functionURI.equals(FunctionRegistry.AND)) {

          return FunctionNode.AND(
                (ValueExpressionNode)pushNegations(
                   (IValueExpressionNode) functionNode.get(0)),
                (ValueExpressionNode)pushNegations(
                   (IValueExpressionNode) functionNode.get(1)));  
          
       } else if (functionURI.equals(FunctionRegistry.OR)) {

          return FunctionNode.OR(
             (ValueExpressionNode)pushNegations(
                (IValueExpressionNode) functionNode.get(0)),
             (ValueExpressionNode)pushNegations(
                (IValueExpressionNode) functionNode.get(1)));  
          
       } // else: nothing to be done
       
       return vexp;
    }

    
    /**
     * Recursively pushes logical ORs below logical ANDs in the operator tree, 
     * such that in the returned node all OR expressions are situated below 
     * AND expressions. Expectes that all NOT expressions have been pushed
     * down to the bottom already (otherwise, the behavior is undertermined).
     * 
     * The resulting {@link IValueExpressionNode} is logically equivalent.
     */
    static public IValueExpressionNode pushDisjuncts(
       final IValueExpressionNode vexp) {
       
       if(!(vexp instanceof FunctionNode)) {
          return vexp;
       }
       
       final FunctionNode functionNode = (FunctionNode)vexp;
       final URI functionURI = functionNode.getFunctionURI();
       
       if (functionURI.equals(FunctionRegistry.OR)) {

          // first, recurse, making sure that AND is propagated up in the subtrees
          final IValueExpressionNode left = 
             pushNegations(
                pushDisjuncts((IValueExpressionNode) functionNode.get(0)));
          final IValueExpressionNode right =
             pushNegations(
                pushDisjuncts((IValueExpressionNode) functionNode.get(1)));
          
          /*
           * New conjuncts are basically the cross product disjuncts of the left
           * and right subtree. Note that the special case (where neither the
           * left nor the right subtree has an AND at the top nicely fits in:
           * in that case, leftConjuncts and rightConjuncts have one element,
           * say x and y, and we compute x OR y as the one and only conjunct
           * (thus not changing the tree).
           */
          final List<IValueExpressionNode> leftConjuncts = 
             extractToplevelConjuncts(
                left, new ArrayList<IValueExpressionNode>());
          final List<IValueExpressionNode> rightConjuncts = 
             extractToplevelConjuncts(
                right, new ArrayList<IValueExpressionNode>());
          
          final List<IValueExpressionNode> newConjuncts = 
             new ArrayList<IValueExpressionNode>();
          for (IValueExpressionNode leftConjunct : leftConjuncts) {
             for (IValueExpressionNode rightConjunct : rightConjuncts) {
                
                final IValueExpressionNode newConjunct = 
                   FunctionNode.OR(
                      (ValueExpressionNode)leftConjunct,
                      (ValueExpressionNode)rightConjunct);
                newConjuncts.add(newConjunct);
             }
          }
          
          return toConjunctiveValueExpression(newConjuncts);
          
       } else if (functionURI.equals(FunctionRegistry.AND)) {
          
          // just recurse
          return FunctionNode.AND(
             (ValueExpressionNode)pushDisjuncts(
                (IValueExpressionNode) functionNode.get(0)),
             (ValueExpressionNode)pushDisjuncts(
                (IValueExpressionNode) functionNode.get(1)));
          
       }  // we're done recursing, no disjuncts will be found below this point


       return vexp; // return the (possibly modified) vexp
    }
    

    /** 
     * Extracts all AND-connected conjuncts located at the top of a given
     * value expression node (recursively, unless an operator different from
     * AND is encountered). 
     * 
     * @param vexpNode the value expression node
     * @param nodes set where to store the top level conjuncts in
     * 
     * @return the array of filters
     */
    static public List<IValueExpressionNode> extractToplevelConjuncts(
          final IValueExpressionNode vexp, List<IValueExpressionNode> nodes) {
       
       if (vexp instanceof FunctionNode) {

          final FunctionNode functionNode = (FunctionNode)vexp;
          final URI functionURI = functionNode.getFunctionURI();
          
          if (functionURI.equals(FunctionRegistry.AND)) {
             
             extractToplevelConjuncts(
                (ValueExpressionNode)functionNode.get(0), nodes);
             extractToplevelConjuncts(
                (ValueExpressionNode)functionNode.get(1), nodes);
             
             return nodes; // don't record this (complex AND) node
          }
       }

       nodes.add(vexp); // record conjunct (don't recurse)
       return nodes;
       
    }
    
    
    /**
     * Constructs an (unbalanced) tree out of the list of conjuncts.
     * If the conjuncts that are passed in are null or empty, null is returned.
     * 
     * @param conjuncts
     * @return
     */
    static public IValueExpressionNode toConjunctiveValueExpression(
          final List<IValueExpressionNode> conjuncts) {
       
       if (conjuncts==null || conjuncts.isEmpty()) {
          return null; 
       }

       
       // if the list is unary, we return the one and only conjunct
       if (conjuncts.size()==1) {
          
          return conjuncts.get(0);
          
       } else {
          
          IValueExpressionNode tmp = 
             FunctionNode.AND(
                (ValueExpressionNode)conjuncts.get(0), 
                (ValueExpressionNode)conjuncts.get(1));
          
          for (int i=2; i<conjuncts.size(); i++) {
             tmp = FunctionNode.AND(
                (ValueExpressionNode)tmp, 
                (ValueExpressionNode)conjuncts.get(i));            
          }
          
          return tmp;
       }
    }
    
    
    /**
     * Resolves the {@link NamedSubqueryInclude} in the given context,
     * returning the associated {@link NamedSubqueryRoot} object. Returns
     * null if resolval fails.
     */
    public NamedSubqueryRoot resolveNamedSubqueryInclude(
          final NamedSubqueryInclude nsi,
          final StaticAnalysis sa) {
       
       return sa==null || nsi==null || nsi.getName()==null ?
          null : sa.getNamedSubqueryRoot(nsi.getName());
    }
    
    /**
     * Checks whether a given node is an OPTIONAL node.
     */
    public static boolean isOptional(BOp node) {
       return node instanceof IJoinNode && ((IJoinNode)node).isOptional();
    }
    
    /**
     * Checks whether a given node is a MINUS node.
     */
    public static boolean isMinus(BOp node) {
       return node instanceof IJoinNode && ((IJoinNode)node).isMinus();
    }
    
    /**
     * Checks whether a given node is a MINUS or OPTIONAL node.
     */
    public static boolean isMinusOrOptional(BOp node) {
       return isOptional(node) || isMinus(node);
    }
    

    
}
