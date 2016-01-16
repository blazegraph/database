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
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.ExistsNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGraphPatternContainer.Annotations;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryFunctionNodeBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Optimizer identifies value expressions using EXISTS or NOT EXISTS and
 * rewrites them in a form suitable for evaluation by the query engine. The main
 * problem with (NOT) EXIST is that the query engine is not written to evaluate
 * graph patterns within value expressions. Therefore the graph pattern is
 * extracted into a subquery which must be evaluated before the FILTER may be
 * evaluated.
 * <p>
 * Like the FILTER in which the (NOT EXISTS) appears (and like MINUS) the order
 * in which graph pattern must be evaluated is determined by the same general
 * principles which govern the attachment of FILTERs to joins. That is, when a
 * variable on which the filter depends is bound by a required join they must
 * not run before that required join. And, when a variable on which the filter
 * depends is only bound by an optional join, then they must not run until after
 * that optional join.
 * <p>
 * For (NOT) EXISTS evaluation we need to bind the outcome of the graph pattern
 * (that is, whether or not the graph pattern is satisified) onto an anonymous
 * variable (assigned by the SPARQL parser). It is the truth state of that
 * anonymous variable which is tested by the filter. This is handled by special
 * handshaking with the join in which we declare the anonymous variable to the
 * join (this is the "ASK_VAR") and project only that anonymous variable out of
 * the join.
 * <p>
 * All variables which are in scope when the (NOT) EXISTS graph pattern is
 * evaluated must be projected into the subquery (since they must be visible to
 * it), but the bindings produced by the subquery (other than the anonymous
 * variable indicating whether or not the graph pattern "exists") must be
 * discarded. Again, this is handled by projecting out only the "anonymous"
 * variable and the join variables (which are bound on entry to the join and are
 * used within the (NOT) EXISTS graph pattern). Other bindings are discarded.
 * <p>
 * The interpretation of the truth state of the variable by the FILTER provides
 * the "exists" or "not exists" semantics. The hash index containing the ASK VAR
 * bindings must remain visible until the corresponding FILTER has been
 * evaluated.
 * <p>
 * Note: This rewrite must run relatively early to ensure that other optimizers
 * are able to run against the graph pattern once it has been lifted out of the
 * (NOT) EXISTS onto a subquery.
 * 
 * @see ExistsNode
 * @see NotExistsNode
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTExistsOptimizer implements IASTOptimizer {

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     
          

        if (!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);
        
        final Set<IVariable<?>> exogenousVars = context.getSolutionSetStats()
                .getUsedVars();
        
        if (queryRoot.getNamedSubqueries() != null) {

            for (NamedSubqueryRoot subqueryRoot : queryRoot
                    .getNamedSubqueries()) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> whereClause = subqueryRoot
                        .getWhereClause();

                rewrite(sa, exogenousVars, subqueryRoot, whereClause);

            }

        }
        
        @SuppressWarnings("unchecked")
        final GraphPatternGroup<IGroupMemberNode> whereClause = queryRoot
                .getWhereClause();

        if (whereClause == null) {

            throw new RuntimeException("Missing where clause? : " + queryNode);

        }

        rewrite(sa, exogenousVars, queryRoot, whereClause);

        return new QueryNodeWithBindingSet(queryRoot, bindingSets);
        
    }

    /**
     * Look for FILTER.
     * 
     * @param p
     *            The parent.
     */
    @SuppressWarnings("unchecked")
    private void rewrite(final StaticAnalysis sa, 
            final Set<IVariable<?>> exogenousVars, 
            final QueryBase query,
            final GraphPatternGroup<IGroupMemberNode> p) {

        final int arity = p.size();

        for (int i = 0; i < arity; i++) {

            final IGroupMemberNode child = (IGroupMemberNode) p.get(i);

            if (child instanceof FilterNode) {

                final FilterNode filter = (FilterNode) child;

                /**
                 * BLZG-1475: there are cases where we have nested FILTER
                 * EXISTS or FILTER NOT EXISTS expressions; in such cases, we
                 * rewrite the inner expressions first
                 */
                final IValueExpressionNode vexp = filter.getValueExpressionNode();
                if (vexp!=null) {
                   final Object gpGroup = 
                      child.get(0).getProperty(Annotations.GRAPH_PATTERN, null);
                   if (gpGroup instanceof GraphPatternGroup) {
                      rewrite(sa, exogenousVars, query,
                            (GraphPatternGroup<IGroupMemberNode>) gpGroup);
                   }
                }
                
                // rewrite filter.
                rewrite(sa, exogenousVars, query, p, filter, vexp);

            }

            if (child instanceof GraphPatternGroup<?>) {

                // Recursion.
                rewrite(sa, exogenousVars, query,
                        (GraphPatternGroup<IGroupMemberNode>) child);

            }

            if (child instanceof SubqueryRoot) {
                
                // Recursion.
                final SubqueryRoot subquery = (SubqueryRoot) child;

                rewrite(sa, exogenousVars, subquery, subquery.getWhereClause());
                
            }

        }

    }

    /**
     * Look for {@link ExistsNode} or {@link NotExistsNode} in FILTER. If we
     * find such a node, we lift its group graph pattern onto the parent.
     * 
     * @param p
     *            The group in which the filter was found (aka the parent).
     * @param filter
     *            The FILTER in which an {@link ExistsNode} or
     *            {@link NotExistsNode} might appears.
     * @param ve
     *            Part of the value expression for that filter.
     */
    private void rewrite(final StaticAnalysis sa,
            final Set<IVariable<?>> exogenousVars,
            final QueryBase query,
            final GraphPatternGroup<IGroupMemberNode> p,
            final FilterNode filter,
            final IValueExpressionNode ve) {

        if (ve instanceof SubqueryFunctionNodeBase) {

            final SubqueryFunctionNodeBase subqueryFunction = (SubqueryFunctionNodeBase) ve;

            final GraphPatternGroup<IGroupMemberNode> graphPattern = subqueryFunction
                    .getGraphPattern();

            if (graphPattern != null) {

                if ((subqueryFunction instanceof ExistsNode)
                        || (subqueryFunction instanceof NotExistsNode)) {

                    final SubqueryRoot subquery = new SubqueryRoot(QueryType.ASK);

                    /**
                     * Propagate the FILTER EXISTS mode query hint to the ASK
                     * subquery.
                     * 
                     * @see <a href="http://trac.blazegraph.com/ticket/988"> bad
                     *      performance for FILTER EXISTS </a>
                     */
                    subquery.setFilterExistsMode(subqueryFunction.getFilterExistsMode());
                    
                    // delegate pipelined hash join annotation to subquery
                    final String pipelinedHashJoinHint = 
                        filter.getQueryHint(QueryHints.PIPELINED_HASH_JOIN);
                    if (pipelinedHashJoinHint!=null) {
                       subquery.setQueryHint(
                          QueryHints.PIPELINED_HASH_JOIN, pipelinedHashJoinHint);
                    }
                    
                    final ProjectionNode projection = new ProjectionNode();
                    subquery.setProjection(projection);

                    /*
                     * The anonymous variable used to communicate the outcome of
                     * the graph pattern.
                     */
                    final VarNode anonVar = (VarNode) subqueryFunction.get(0);
                    subquery.setAskVar(anonVar.getValueExpression());
                    //projection.addProjectionVar((VarNode) subqueryFunction.get(0));

                    /*
                     * Anything which is visible in the scope in which the
                     * FILTER appears. All we need to know is anything
                     * exogenous, plus anything MAYBE incoming, plus anything
                     * MAYBE bound in the graphPattern, retaining anything used
                     * within the EXISTS graphPattern.
                     */ 
                    final LinkedHashSet<IVariable<?>> vars = new LinkedHashSet<IVariable<?>>();
                    {
                        vars.addAll(exogenousVars);
                        sa.getMaybeIncomingBindings(p, vars);
                        sa.getMaybeProducedBindings(p, vars, true/* recursive */);
                        final Set<IVariable<?>> usedVars = sa
                                .getSpannedVariables(graphPattern,
                                        new LinkedHashSet<IVariable<?>>());
                        vars.retainAll(usedVars);
                    }
//                    * Note: This may not be the best way to gather those
//                    * variables. First, [query] is not being passed into this
//                    * method in a manner which is sensitive to how we enter
//                    * SPARQL 1.1 subqueries nodes. Second, the order in which
//                    * the FILTER appears with respect to the other nodes in the
//                    * parent has not yet been settled, but getProjectedVars()
//                    * is paying attention to that order. Really, 
//                    
//                    final Set<IVariable<?>> vars = sa.getProjectedVars(filter,
//                            graphPattern, query, exogenousVars,
//                            new LinkedHashSet<IVariable<?>>());

                    for(IVariable<?> var : vars) {

                        projection.addProjectionVar(new VarNode(var.getName()));
                        
                    }
                    
                    /*
                     * Note: This makes the anonymous variable appear as if it
                     * is used by the ASK subquery. That is important for the
                     * bottom up analysis, which will otherwise identify the
                     * anonymous variable as one which is provably not bound in
                     * the filter.
                     */
                    projection.addProjectionVar(anonVar);
                    
                    subquery.setWhereClause(graphPattern);

                    // lift the SubqueryRoot into the parent.
                    p.addChild(subquery);
                }

            }

        }

        final int arity = ((BOp) ve).arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = ((BOp) ve).get(i);

            if (child instanceof IValueExpressionNode) {

                // Recursion.
                rewrite(sa, exogenousVars, query, p, filter,
                        (IValueExpressionNode) child);

            }

        }

    }

}
