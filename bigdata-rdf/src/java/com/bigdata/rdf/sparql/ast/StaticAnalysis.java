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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.internal.constraints.IPassesMaterialization;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.sparql.ast.optimizers.ASTBottomUpOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTLiftPreFiltersOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTOptimizerList;

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
 * {@link StatementPatternNode#isSimpleOptional()} is <code>true</code>.
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
 * FIXME [This should all change once we move to a more dynamic approach to
 * ordering the joins.] The main "gotcha" when looking "up" the tree is that we
 * need to know the position at which everything above us in the tree is being
 * evaluated. Historically, the evaluation order for binding producers was
 * organized into fairly simple regions : required pipelined joins of statement
 * pattern nodes and optional join groups. Life is more complex now that we also
 * have SPARQL 1.1 subqueries, SERVICE calls, and named subquery includes. We
 * probably need a concept which reflects these different evaluation regions
 * within a group, but even then it is not enough since we would have to specify
 * this for each parent group in the AST.
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
public class StaticAnalysis {

    private final QueryRoot queryRoot;
    
    /**
     * Return the {@link QueryRoot} parameter given to the constructor.
     */
    public QueryRoot getQueryRoot() {
       
        return queryRoot;
        
    }
    
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

            getDefinitelyProducedBindings(parent, vars, false/* recursive */);

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
            
            if(!sp.isSimpleOptional()) {

                // Only if the statement pattern node is a required join.
                vars.addAll(sp.getProducedBindings());
                
            }

        } else if(node instanceof SubqueryRoot) {

            final SubqueryRoot subquery = (SubqueryRoot) node;

            if(true) {

                /*
                 * FIXME Although the sub-select is also a required join, this
                 * code is being used to find the join variables for the
                 * sub-select so we can not consider the sub-select's
                 * contribution.
                 * 
                 * Note: By failing to consider the sub-select's contribution
                 * here we are letting in any number of nasty consequences which
                 * can range from not predicting join variables in other
                 * circumstances to failing to bind filters early enough.
                 * 
                 * @see https://sourceforge.net/apps/trac/bigdata/ticket/398
                 * (Convert the static optimizer into an AST rewrite)
                 */

                vars.addAll(getDefinatelyProducedBindings(subquery));
                
            }

        } else if (node instanceof NamedSubqueryInclude) {

            final NamedSubqueryInclude nsi = (NamedSubqueryInclude) node;

            final NamedSubqueryRoot nsr = nsi.getNamedSubqueryRoot(queryRoot);

            if (nsr == null)
                throw new RuntimeException("No named subquery declared: name="
                        + nsi.getName());

            vars.addAll(getDefinatelyProducedBindings(nsr));

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

//            } else if (op instanceof IBindingProducerNode) {
//
//                if (op instanceof StatementPatternNode) {
//
//                    addAll(vars, op);
//
//                } else if (op instanceof SubqueryRoot) {
//
//                    final SubqueryRoot subqueryRoot = (SubqueryRoot) op;
//
//                    vars.addAll((List) Arrays.asList(subqueryRoot
//                            .getProjection().getProjectionVars()));
//
//                } else if (op instanceof NamedSubqueryInclude) {
//
//                    final NamedSubqueryInclude nsi = (NamedSubqueryInclude) op;
//
//                    final NamedSubqueryRoot nsr = nsi
//                            .getNamedSubqueryRoot(queryRoot);
//
//                    vars.addAll((List) Arrays.asList(nsr.getProjection()
//                            .getProjectionVars()));
//
//                } else if (op instanceof ServiceNode) {
//
//                    /*
//                     * todo Add all maybe bound variables from the service
//                     * node's graph pattern.
//                     */
//
//                } else {
//                    throw new AssertionError(op + " in " + group);
//
//                }
//                
            }
            
        }

        return vars;
        
    }
    
    /**
     * Add all variables spanned by the operator.
     * 
     * @param bindings
     *            The set to which the variables will be added.
     * @param op
     *            The operator.
     */
    private void addAll(final Set<IVariable<?>> bindings,
            final IGroupMemberNode op) {

        final Iterator<IVariable<?>> it = BOpUtility
                .getSpannedVariables((BOp) op);

        while (it.hasNext()) {

            bindings.add(it.next());

        }

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

            final StatementPatternNode sp = (StatementPatternNode) node;

            if(sp.isSimpleOptional()) {

                // Only if the statement pattern node is an optional join.
                vars.addAll(sp.getProducedBindings());
                
            }

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
            
        } else {
            
            throw new AssertionError(node.toString());
            
        }

        return vars;
      
    }

    /*
     * Private type specific helper methods.
     */

    // MUST : JOIN GROUP
    private Set<IVariable<?>> getDefinitelyProducedBindings(
            final JoinGroupNode node, final Set<IVariable<?>> vars,
            final boolean recursive) {

        for (IGroupMemberNode child : node) {

            if(!(child instanceof IBindingProducerNode))
                continue;
            
            if (child instanceof StatementPatternNode) {

                final StatementPatternNode sp = (StatementPatternNode) child;

                if (!sp.isSimpleOptional()) {
                    
                    /*
                     * Required JOIN (statement pattern).
                     */

                    getDefinitelyProducedBindings(sp, vars, recursive);

                }

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

                    if (!group.isOptional()) {

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

                    vars.addAll(getMaybeProducedBindings(
                            (IBindingProducerNode) child, vars, recursive));                
                
                }
                
            }

        }

        return vars;

    }

    // MUST : UNION
    private Set<IVariable<?>> getDefinitelyProducedBindings(
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

            getDefinitelyProducedBindings(child, childSet, recursive);

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

            getDefinitelyProducedBindings(child, vars, recursive);

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

            getDefinitelyProducedBindings(whereClause, vars, true/* recursive */);

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

    /**
     * Report "MUST" bound bindings projected by the service. This involves
     * checking the graph pattern reported by {@link ServiceNode#getGroupNode()}
     * . Bindings visible in the parent group are NOT projected into a SERVICE.
     * A SERVICE does NOT have an explicit PROJECTION so it can not rename the
     * projected bindings.
     * <p>
     * Note: This assumes that services do not run "as-bound". If this is
     * permitted, then this code needs to be reviewed.
     */
    // MUST : ServiceNode
    public Set<IVariable<?>> getDefinitelyProducedBindings(final ServiceNode node) {

        final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
        
        final GraphPatternGroup<IGroupMemberNode> graphPattern = (GraphPatternGroup<IGroupMemberNode>) node.getGroupNode();

        if (graphPattern != null) {

            getDefinitelyProducedBindings(graphPattern, vars, true/* recursive */);

        }

        return vars;

    }

    /**
     * Report "MAYBE" bound bindings projected by the service. This involves
     * checking the graph pattern reported by {@link ServiceNode#getGroupNode()}
     * . Bindings visible in the parent group are NOT projected into a SERVICE.
     * A SERVICE does NOT have an explicit PROJECTION so it can not rename the
     * projected bindings.
     * <p>
     * Note: This assumes that services do not run "as-bound". If this is
     * permitted, then this code needs to be reviewed.
     */
    // MAY : ServiceNode
    public Set<IVariable<?>> getMaybeProducedBindings(final ServiceNode node) {

        final Set<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
        
        final GraphPatternGroup<IGroupMemberNode> graphPattern = (GraphPatternGroup<IGroupMemberNode>) node.getGroupNode();

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
        final Set<IVariable<?>> knownBound = getIncomingBindings(group,
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
        final Set<IVariable<?>> knownBound = getIncomingBindings(group,
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
        final List<FilterNode> filters = group.getFilters();

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> knownBound = getIncomingBindings(group,
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
        final List<FilterNode> filters = group.getFilters();

        /*
         * Get the variables known to be bound starting out.
         */
        final Set<IVariable<?>> maybeBound = getIncomingBindings(group, new LinkedHashSet<IVariable<?>>());

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

    /**
     * Return the distinct variables in the operator tree, including on those on
     * annotations attached to operators. Variables projected by a subquery are
     * included, but not variables within the WHERE clause of the subquery.
     * Variables projected by a {@link NamedSubqueryInclude} are also reported,
     * but not those used within the WHERE clause of the corresponding
     * {@link NamedSubqueryRoot}.
     * 
     * @param op
     *            An operator.
     * @param varSet
     *            The variables are inserted into this {@link Set}.
     *            
     * @return The caller's {@link Set}.
     */
    public Set<IVariable<?>> getSpannedVariables(final BOp op,
            final Set<IVariable<?>> varSet) {

        if (op == null) {

            return varSet;
            
        } else if (op instanceof IVariable<?>) {
         
            varSet.add((IVariable<?>)op);
            
        } else if(op instanceof IConstant<?>) {
            
            final IConstant<?> c = (IConstant<?>)op;
            
            final IVariable<?> var = (IVariable<?> )c.getProperty(Constant.Annotations.VAR);
                
            if( var != null) {
                
                varSet.add(var);
                
            }
            
        } else if (op instanceof SubqueryRoot) {

            /*
             * Do not recurse into a subquery, but report any variables
             * projected by that subquery.
             */

            final SubqueryRoot subquery = (SubqueryRoot) op;

            addProjectedVariables(subquery, varSet);
            
            // DO NOT RECURSE INTO THE SUBQUERY!
            return varSet;

        } else if (op instanceof NamedSubqueryInclude) {

            final NamedSubqueryInclude namedInclude = (NamedSubqueryInclude) op;

            final NamedSubqueryRoot subquery = namedInclude
                    .getRequiredNamedSubqueryRoot(queryRoot);

            addProjectedVariables(subquery, varSet);
            
            // DO NOT RECURSE INTO THE SUBQUERY!
            return varSet;
            
        }
        
        /*
         * Recursion.
         */

        final int arity = op.arity();

        for (int i = 0; i < arity; i++) {

            getSpannedVariables(op.get(i), varSet);

        }

        return varSet;

    }

    /**
     * Add all variables on the {@link ProjectionNode} of the subquery to the
     * set of distinct variables visible within the scope of the parent query.
     * 
     * @param subquery
     * @param varSet
     */
    private void addProjectedVariables(final SubqueryBase subquery,
            final Set<IVariable<?>> varSet) {
        
        final ProjectionNode proj = subquery.getProjection();

        if (proj.isWildcard()) {
            /* The subquery's projection should already have been rewritten. */
            throw new AssertionError();
        }

        for (IVariable<?> var : proj.getProjectionVars()) {

            varSet.add(var);

        }
        
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
            final BOp c, final Set<IVariable<IV>> terms) {
    
        boolean materialize = false;
        boolean always = false;
        
        final Iterator<BOp> it = BOpUtility.preOrderIterator(c);
        
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
     * 
     *            FIXME This is currently disabled and will always return an
     *            empty set. This has the effect of forcing named subquery
     *            INCLUDEs to run before the required statement pattern joins in
     *            a join group. This needs to be changed as part of the RTO
     *            integration.
     */
    public Set<IVariable<?>> getJoinVars(
            final NamedSubqueryRoot aNamedSubquery,
            final NamedSubqueryInclude anInclude, final Set<IVariable<?>> vars) {

        if (true) {

            return Collections.emptySet();

        } else {

            return _getJoinVars(aNamedSubquery, anInclude, vars);

        }

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
     * within the query in which it appears.
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
     * 
     *         FIXME This code must figure out which variables "must" be bound
     *         by both the the subquery and context in which the INCLUDE appears
     *         and return just those variables. The problem is that we can not
     *         really decide this until we decide the evaluation order since the
     *         named subquery include could run at any point in the required
     *         joins. That includes the pipelined statement pattern joins, the
     *         inline access path joins, the named subquery joins, the service
     *         node joins, etc. The code will currently only report join
     *         variables based on those variables which are known bound on entry
     *         to the group. Thus it completely ignores the order of evaluation
     *         of the required joins in the join group. This is an RTO
     *         integration issue.
     */
    private Set<IVariable<?>> _getJoinVars(final SubqueryBase aSubquery,
            final IGroupMemberNode theNode, final Set<IVariable<?>> vars) {

        /*
         * The variables which are projected by the subquery which will be
         * definitely bound based on an analysis of the subquery.
         */
        final Set<IVariable<?>> boundBySubquery = getDefinatelyProducedBindings(aSubquery);

        /*
         * The variables which are definitely bound on entry to the join group
         * in which the subquery appears.
         */
        final Set<IVariable<?>> incomingBindings = getIncomingBindings(
                theNode.getParentJoinGroup(), new LinkedHashSet<IVariable<?>>());
        
        /*
         * This is only those variables which are bound on entry into the group
         * in which the subquery join appears *and* which are "must" bound
         * variables projected by the subquery.
         */
        boundBySubquery.retainAll(incomingBindings);

        vars.addAll(boundBySubquery);

        return vars;

    }

}
