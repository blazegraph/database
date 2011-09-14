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
 * Created on Sep 14, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.optimizers.ASTBottomUpOptimizer;

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
 * <dd>All variables are definitely bound. (Note that we sometimes attach a
 * simple optional join to the parent group for efficiency, at which point it
 * becomes an "optional" statement pattern, but we only do that in the physical
 * query plan so it does not effect the static analysis.)</dd>
 * 
 * <dt>{@link JoinGroupNode}</dt>
 * <dd></dd>
 * 
 * <dt>{@link UnionNode}</dt>
 * <dd>The definitely bound variables is the intersection of the definitely
 * bound variables in the child join groups. The maybe bound variables is the
 * union of the definitely bound variables in the child join groups.</dd>
 * 
 * <dt>{@link AssignmentNode}</dt>
 * <dd>BIND(expr AS var) in a group will not bind the variable if there is an
 * error when evaluating the value expression and does not fail the solution.
 * Thus BIND() in a group contributes to "maybe" bound variables.
 * <p>
 * Note: BIND() in a PROJECTION is handled differently as it is non-optional (if
 * the value expression results in an error the solution is dropped).
 * Projections are handled when we do the analysis of a QueryBase node since we
 * can see both the WHERE clause and the PROJECTION clauses at the same time.</dd>
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
 * FIXME The main "gotcha" when looking "up" the tree is that we need to know
 * the position at which everything above us in the tree is being evaluated.
 * Historically, the evaluation order for binding producers was organized into
 * fairly simple regions : required pipelined joins of statement pattern nodes
 * and optional join groups. Life is more complex now that we also have SPARQL
 * 1.1 subqueries, SERVICE calls, and named subquery includes. We probably need
 * a concept which reflects these different evaluation regions within a group,
 * but even then it is not enough since we would have to specify this for each
 * parent group in the AST.
 * 
 * FIXME Filter attachment (and javadoc). The analysis of filters depends on the
 * analysis of variables so we need to have the {@link QueryRoot} on hand in
 * order to resolve {@link NamedSubqueryInclude}s. This means that we either
 * pass {@link StaticAnalysis} into the methods on {@link JoinGroupNode} or move
 * those methods here and pass in the {@link JoinGroupNode}.
 * 
 * FIXME {@link GraphPatternGroup}, {@link JoinGroupNode}, {@link UnionNode},
 * and {@link QueryBase}: Remove the static analysis methods from these classes
 * once we have captured all of that in {@link StaticAnalysis}.  Replace it with
 * this class and then test for new bugs.
 * 
 * FIXME Fold in the logic to detect a badly designed left join and finish the
 * {@link ASTBottomUpOptimizer}.
 * 
 * TODO We can probably cache the heck out of things on this class. There is no
 * reason to recompute the SA of the know or maybe/must bound variables until
 * there is an AST change, and the caller can build a new SA when that happens.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StaticAnalysis {

    private final QueryRoot queryRoot;
    
    /**
     * 
     * @param queryRoot
     *            The root of the query. We need to have this on hand in order
     *            to resolve {@link NamedSubqueryInclude}s during static
     *            analysis.
     */
    public StaticAnalysis(final QueryRoot queryRoot) {
        
        if(queryRoot == null)
            throw new IllegalArgumentException();
        
        this.queryRoot = queryRoot;
        
    }
    
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
     */
    public Set<IVariable<?>> getIncomingBindings(
            final IBindingProducerNode node, final Set<IVariable<?>> vars) {
    
        GraphPatternGroup<?> parent = node instanceof IGroupMemberNode ? ((IGroupMemberNode) node)
                .getParentGraphPatternGroup() : null;
        
        while (parent != null) {

            /*
             * Note: This needs to be a non-recursive definition of the
             * definitely produced bindings. Just those for *this* group for
             * each parent considered.
             */

            getDefinatelyProducedBindings(parent, vars, false/* recursive */);

            parent = parent.getParentGraphPatternGroup();
            
        }
        
        return vars;
        
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
     * Note: The recursive analysis does not throw out variables when part of
     * the tree will provably fail to bind anything. It is the role of query
     * optimizers to identify those situations and prune the AST appropriately.
     * 
     * @param vars
     *            Where to store the "MUST" bound variables.
     * @param recursive
     *            When <code>true</code>, the child groups will be recursively
     *            analyzed. When <code>false</code>, only <i>this</i> group will
     *            be analyzed.
     * 
     * @return The argument.
     */
    public Set<IVariable<?>> getDefinatelyProducedBindings(
            final IBindingProducerNode node, final Set<IVariable<?>> vars,
            final boolean recursive) {

        if (node instanceof GraphPatternGroup<?>) {
        
            if (node instanceof JoinGroupNode) {
            
                getDefinatelyProducedBindings((JoinGroupNode) node, vars,
                        recursive);
                
            } else if (node instanceof UnionNode) {
                
                getDefinatelyProducedBindings((UnionNode) node, vars, recursive);
                
            } else {
                
                throw new AssertionError(node.toString());
                
            }

        } else if(node instanceof StatementPatternNode) {

            final StatementPatternNode sp = (StatementPatternNode) node;

            vars.addAll(sp.getProducedBindings());

        } else if(node instanceof SubqueryRoot) {

            final SubqueryRoot subquery = (SubqueryRoot) node;

            vars.addAll(getDefinatelyProducedBindings(subquery));

        } else if (node instanceof NamedSubqueryInclude) {

            final NamedSubqueryInclude nsi = (NamedSubqueryInclude) node;

            final NamedSubqueryRoot nsr = nsi.getNamedSubqueryRoot(queryRoot);

            if (nsr == null)
                throw new RuntimeException("No named subquery declared: name="
                        + nsi.getName());

            vars.addAll(getDefinatelyProducedBindings(nsr));

        } else {
            
            throw new AssertionError(node.toString());
            
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
     *            Where to store the "MUST" bound variables.
     * @param recursive
     *            When <code>true</code>, the child groups will be recursively
     *            analyzed. When <code>false</code>, only <i>this</i> group will
     *            be analyzed.
     *            
     * @return The argument.
     */
    public Set<IVariable<?>> getMaybeProducedBindings(
            final IBindingProducerNode node, final Set<IVariable<?>> vars,
            boolean recursive) {

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
            
            // NOP
            
        } else if(node instanceof SubqueryRoot) {

            final SubqueryRoot subquery = (SubqueryRoot) node;

            vars.addAll(getMaybeProducedBindings(subquery));

        } else if (node instanceof NamedSubqueryInclude) {

            final NamedSubqueryInclude nsi = (NamedSubqueryInclude) node;

            final NamedSubqueryRoot nsr = nsi.getNamedSubqueryRoot(queryRoot);

            if (nsr == null)
                throw new RuntimeException("No named subquery declared: name="
                        + nsi.getName());

            vars.addAll(getMaybeProducedBindings(nsr));

        } else {
            
            throw new AssertionError(node.toString());
            
        }

        return vars;
      
    }

    /*
     * Private type specific helper methods.
     */

    // MUST : JOIN GROUP
    private Set<IVariable<?>> getDefinatelyProducedBindings(
            final JoinGroupNode node, final Set<IVariable<?>> vars,
            final boolean recursive) {

        for (IGroupMemberNode child : node) {

            if(!(child instanceof IBindingProducerNode))
                continue;
            
            if (child instanceof StatementPatternNode) {

                /*
                 * Required JOIN (statement pattern).
                 */

                getDefinatelyProducedBindings((IBindingProducerNode) child,
                        vars, recursive);

            } else if (child instanceof NamedSubqueryInclude
                    || child instanceof SubqueryRoot) {

                /*
                 * Required JOIN (named solution set or subquery).
                 * 
                 * Note: We have to descend recursively into these structures in
                 * order to determine anything.
                 */

                vars.addAll(getDefinatelyProducedBindings(
                        (IBindingProducerNode) child,
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            } else if (child instanceof GraphPatternGroup<?>) {

                if (recursive) {

                    // Add anything bound by a child group.

                    final GraphPatternGroup<?> group = (GraphPatternGroup<?>) child;

                    if (!group.isOptional()) {

                        getDefinatelyProducedBindings(group, vars, recursive);

                    }

                }
                
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
        getDefinatelyProducedBindings(node, vars, false/* recursive */);

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

                    vars.addAll(getMaybeProducedBindings(
                            (IBindingProducerNode) child, vars, recursive));                
                
                }
                
            }

        }

        return vars;

    }

    // MUST : UNION
    private Set<IVariable<?>> getDefinatelyProducedBindings(
            final UnionNode node,
            final Set<IVariable<?>> vars, final boolean recursive) {

        if (!recursive || node.isOptional()) {

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

            getDefinatelyProducedBindings(child, childSet, recursive);

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

            getDefinatelyProducedBindings(child, vars, recursive);

        }

        return vars;

    }

    /**
     * Report "MUST" bound bindings projected by the query. This involves
     * checking the WHERE clause and the {@link ProjectionNode} for the query.
     * Note that the projection can rename variables. It can also bind a
     * constant on a variable. Variables which are not projected by the query
     * will NOT be reported.
     */
    // MUST : QueryBase
    public Set<IVariable<?>> getDefinatelyProducedBindings(final QueryBase node) {

        final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
        
        final ProjectionNode projection = node.getProjection();
        
        if(projection == null) {

            // If there is no projection then there is nothing to report.
            return vars;

        }

        @SuppressWarnings("unchecked")
        final GraphPatternGroup<IGroupMemberNode> whereClause = node.getWhereClause();

        if (whereClause != null) {

            getDefinatelyProducedBindings(whereClause, vars, true/* recursive */);

        }

        /*
         * The set of projected variables.
         */
        final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
        
        for(AssignmentNode bind : projection) {
        
            if(bind.getValueExpression() instanceof IConstant<?>) {
                
                /*
                 * If there is a BIND of a constant expression onto a variable,
                 * then that variable is "MUST" bound by the query.
                 * 
                 * Note: This depends on pre-evaluation of constant expressions.
                 * If the expression has not been reduced to a constant then it
                 * will not be detected by this test!
                 */
                
                vars.add(bind.getVar());               
                
            }
            
            projectedVars.add(bind.getVar());
            
        }

        // Remove anything which is not projected out of the query.
        vars.retainAll(projectedVars);
        
        return vars;

    }

    /**
     * Report "MAYBE" bound bindings projected by the query. This involves
     * checking the WHERE clause and the {@link ProjectionNode} for the query.
     * Note that the projection can rename variables. It can also bind a
     * constant on a variable. Variables which are not projected by the query
     * will NOT be reported.
     */
    // MAYBE : QueryBase
    public Set<IVariable<?>> getMaybeProducedBindings(final QueryBase node) {

        final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
        
        final ProjectionNode projection = node.getProjection();
        
        if(projection == null) {

            // If there is no projection then there is nothing to report.
            return vars;

        }

        @SuppressWarnings("unchecked")
        final GraphPatternGroup<IGroupMemberNode> whereClause = node.getWhereClause();

        if (whereClause != null) {

            getMaybeProducedBindings(whereClause, vars, true/* recursive */);

        }

        /*
         * The set of projected variables.
         */
        final Set<IVariable<?>> projectedVars = new LinkedHashSet<IVariable<?>>();
        
        for(AssignmentNode bind : projection) {
        
            if(bind.getValueExpression() instanceof IConstant<?>) {
                
                /*
                 * If there is a BIND of a constant expression onto a variable,
                 * then that variable is "MUST" bound by the query.
                 * 
                 * Note: This depends on pre-evaluation of constant expressions.
                 * If the expression has not been reduced to a constant then it
                 * will not be detected by this test!
                 */
                
                vars.add(bind.getVar());               
                
            }
            
            projectedVars.add(bind.getVar());
            
        }

        // Remove anything which is not projected out of the query.
        vars.retainAll(projectedVars);
        
        return vars;

    }

    /*
     * FIXME FILTERS analysis from JoinGroupNode
     */
    
}
