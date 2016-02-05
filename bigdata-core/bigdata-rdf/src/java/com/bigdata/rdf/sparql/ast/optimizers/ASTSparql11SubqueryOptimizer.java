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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

import cutthecrap.utils.striterators.Striterator;

/**
 * Lift {@link SubqueryRoot}s into named subqueries when appropriate. This
 * includes the following cases:
 * <ul>
 * <li>Lift out SPARQL 1.1 subqueries which use both LIMIT and ORDER BY. Due to
 * the interaction of the LIMIT and ORDER BY clause, these subqueries MUST be
 * run first since they can produce different results if they are run
 * "as-bound".</li>
 * <li>Lift out SPARQL 1.1 subqueries involving aggregates. This typically
 * provides more efficient evaluation than repeated as-bound evaluation of the
 * sub-select. It also prevents inappropriate sharing of the internal state of
 * the {@link IAggregate} functions.</li>
 * <li>Lift out SPARQL 1.1 subqueries if there are no incoming bound variables
 * which are also projected by the subquery. Such subqueries must be lifted or
 * we will simply be doing the same work over and over since no bindings will be
 * projected into the subquery.</li>
 * <li>Lift out SPARQL 1.1 subqueries if {@link SubqueryRoot#isRunOnce()} is
 * <code>true</code>.
 * </ul>
 * 
 * FIXME The code to lift out sub-selects if there are no join variables has
 * been disabled since we are not correctly computing the join variables at this
 * point. Deciding the join variables requires that we either apply heuristics
 * or the RTO to decide on a join ordering. Once the join order is known we can
 * then recognize which variables will be definitely bound at the point in the
 * join group where the subquery would be evaluated.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTSparql11SubqueryOptimizer.java 5193 2011-09-15 14:18:56Z
 *          thompsonbry $
 */
public class ASTSparql11SubqueryOptimizer implements IASTOptimizer {

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

        // First, process any pre-existing named subqueries.
        {
            
            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            if (namedSubqueries != null) {

                // Note: works around concurrent modification error.
                final List<NamedSubqueryRoot> list = BOpUtility.toList(
                        namedSubqueries, NamedSubqueryRoot.class);
                
                for (NamedSubqueryRoot namedSubquery : list) {

                    liftSubqueries(context, sa, namedSubquery.getWhereClause());

                }

            }

        }
        
        // Now process the main where clause.
        liftSubqueries(context, sa, queryRoot.getWhereClause());
        
        if(false) {
            
            /*
             * Note: This may be enabled to lift all SPARQL 1.1 subqueries into
             * named subqueries. However, I think that the better way to handle
             * this is to run the subqueries either as-bound "chunked" or with
             * ALL solutions from the parent as their inputs (the extreme case
             * of chunked). This can also be applied to handling OPTIONAL groups.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/377
             */
            
            rewriteSparql11Subqueries(context, sa, queryRoot);
            
        }

        return new QueryNodeWithBindingSet(queryRoot, bindingSets);
        
    }

    /**
     * Apply all optimizations.
     */
    private void liftSubqueries(final AST2BOpContext context,
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group) {

        final int arity = group.arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = (BOp) group.get(i);
 
            if (child instanceof GraphPatternGroup<?>) {

                /*
                 * Note: Do recursion *before* we do the rewrite so we will
                 * rewrite Sub-Sub-Selects.
                 * 
                 * FIXME Unit test for sub-sub-select optimization.
                 */
                liftSubqueries(context, sa,
                        ((GraphPatternGroup<IGroupMemberNode>) child));

            } else if (child instanceof SubqueryRoot) {

                // Recursion into subqueries.

                final SubqueryRoot subqueryRoot = (SubqueryRoot) child;

                liftSubqueries(context, sa,
                        subqueryRoot.getWhereClause());

            }else if (child instanceof ServiceNode) {
            
                // Do not rewrite things inside of a SERVICE node.
                continue;
                
            }

            if (!(child instanceof SubqueryRoot)) {
                continue;
            }

            final SubqueryRoot subqueryRoot = (SubqueryRoot) child;

            if (subqueryRoot.getQueryType() == QueryType.ASK) {

                /*
                 * FIXME Look at what would be involved in lifting an ASK
                 * sub-query. There are going to be at least two cases. If there
                 * is no join variable, then we always want to lift the ASK
                 * sub-query as it is completely independent of the parent
                 * group. If there is a join variable, then we need to project
                 * solutions which include the join variables from the subquery
                 * and the "ASK". At that point we can hash join against the
                 * projected solutions and the ASK succeeds if the hash join
                 * succeeds. [Add unit tests for this too.]
                 */
                
                continue;
                
            }

            if (subqueryRoot.hasSlice()) {

                /*
                 * Lift out SPARQL 1.1 subqueries which use LIMIT and/or OFFSET.
                 * 
                 * The SliceOp in the subquery will cause the IRunningQuery in
                 * which it appears to be interrupted. Therefore, when a SLICE
                 * is required for a subquery we need to lift it out to run it
                 * as a named subquery.
                 * 
                 * TODO There may well be other cases that we have to handle
                 * with as-bound evaluation of a Subquery with a LIMIT/OFFSET.
                 * If so, then the subquery will have to be run using the
                 * SubqueryOp.
                 */

                liftSparql11Subquery(context, sa, subqueryRoot);

                continue;

            }
            if (subqueryRoot.hasSlice() 
                    && subqueryRoot.getOrderBy() != null) {

                /*
                 * Lift out SPARQL 1.1 subqueries which use both LIMIT and ORDER
                 * BY. Due to the interaction of the LIMIT and ORDER BY clause,
                 * these subqueries MUST be run first since they can produce
                 * different results if they are run "as-bound".
                 */

                liftSparql11Subquery(context, sa, subqueryRoot);

                continue;

            }

            if (StaticAnalysis.isAggregate(subqueryRoot)) {

                /*
                 * Lift out SPARQL 1.1 subqueries which use {@link IAggregate}s.
                 * This typically provides more efficient evaluation than
                 * repeated as-bound evaluation of the sub-select. It also
                 * prevents inappropriate sharing of the internal state of the
                 * {@link IAggregate} functions.
                 */

                liftSparql11Subquery(context, sa, subqueryRoot);

                continue;

            }

            if (subqueryRoot.isRunOnce()) {

                /*
                 * Lift out SPARQL 1.1 subqueries for which the RUN_ONCE
                 * annotation was specified.
                 */

                liftSparql11Subquery(context, sa, subqueryRoot);

                continue;

            }

            /*
             * FIXME We can not correctly predict the join variables at this
             * time because that depends on the actual evaluation order. This
             * has been commented out for now because it will otherwise cause
             * all sub-selects to be lifted out.
             */
            if(false) {
            final Set<IVariable<?>> joinVars = sa.getJoinVars(
                    subqueryRoot, new LinkedHashSet<IVariable<?>>());

            if (joinVars.isEmpty()) {

                /*
                 * Lift out SPARQL 1.1 subqueries for which the RUN_ONCE
                 * annotation was specified.
                 */

                liftSparql11Subquery(context, sa, subqueryRoot);

                continue;

            }
            }

        }
        
    }

    private void rewriteSparql11Subqueries(final AST2BOpContext context,
            final StaticAnalysis sa, final QueryRoot queryRoot) {

        final Striterator itr2 = new Striterator(
                BOpUtility.postOrderIterator((BOp) queryRoot.getWhereClause()));

        itr2.addTypeFilter(SubqueryRoot.class);

        final List<SubqueryRoot> subqueries  = new LinkedList<SubqueryRoot>();

        while (itr2.hasNext()) {

            subqueries.add((SubqueryRoot)itr2.next());

        }
        
        for(SubqueryRoot subquery : subqueries) {
            
            liftSparql11Subquery(context, sa, subquery);
            
        }
        
    }
    
    private void liftSparql11Subquery(final AST2BOpContext context,
            final StaticAnalysis sa, final SubqueryRoot subqueryRoot) {

        final IGroupNode<?> parent = subqueryRoot.getParent();

        final String newName = "-subSelect-" + context.nextId();

        final NamedSubqueryInclude include = new NamedSubqueryInclude(newName);

        /**
         * Set query hints from the parent join group.
         * 
         * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
         * Clean up query hints </a>
         */
        include.setQueryHints((Properties) parent
                .getProperty(ASTBase.Annotations.QUERY_HINTS));

        /**
         * Copy across attached join filters.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/796"
         *      >Filter assigned to sub-query by query generator is dropped from
         *      evaluation</a>
         */
        include.setAttachedJoinFilters(subqueryRoot.getAttachedJoinFilters());

        /*
         * Note: A SubqueryRoot normally starts out as the sole child of a
         * JoinGroupNode. However, other rewrites may have written out that
         * JoinGroupNode and it does not appear to be present for an ASK
         * subquery.
         * 
         * Therefore, when the parent of the SubqueryRoot is a JoinGroupNode
         * having the SubqueryRoot as its only child, we use the parent's parent
         * in order to replace the JoinGroupNode when we lift out the
         * SubqueryRoot. Otherwise we use the parent since there is no wrapping
         * JoinGroupNode (or if there is, it has some other stuff in there as
         * well).
         * 
         * BLZG-1542 -> there is an additional thing we need to take care of:
         *              whenever the parent node is an OPTIONAL or MINUS, we 
         *              must not remove it, otherwise we would just "drop" an
         *              OPTIONAL or MINUS, thus changing the query's semantics
         *              
         */
         
        if ((parent instanceof JoinGroupNode) && !((JoinGroupNode)parent).isOptional()
                && !((JoinGroupNode)parent).isMinus() && ((BOp) parent).arity() == 1
                && parent.getParent() != null &&
                !((IGroupNode<?>)parent.getParent() instanceof UnionNode)) {
            
            final IGroupNode<IGroupMemberNode> pp = parent.getParent();

            // Replace the sub-select with the include.
            if (((ASTBase) pp).replaceWith((BOp) parent, include) == 0)
                throw new AssertionError();

        } else {

            // Replace the sub-select with the include.
            if (((ASTBase) parent).replaceWith((BOp) subqueryRoot, include) == 0)
                throw new AssertionError();
            
        }

        final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                subqueryRoot.getQueryType(), newName);

        /**
         * Copy across query hints from the original subquery.
         * 
         * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
         *      Clean up query hints </a>
         */
        nsr.setQueryHints(subqueryRoot.getQueryHints());

        nsr.setConstruct(subqueryRoot.getConstruct());
        nsr.setGroupBy(subqueryRoot.getGroupBy());
        nsr.setHaving(subqueryRoot.getHaving());
        nsr.setOrderBy(subqueryRoot.getOrderBy());
        nsr.setProjection(subqueryRoot.getProjection());
        nsr.setSlice(subqueryRoot.getSlice());
        nsr.setWhereClause(subqueryRoot.getWhereClause());
        nsr.setBindingsClause(subqueryRoot.getBindingsClause());

        sa.getQueryRoot().getNamedSubqueriesNotNull().add(nsr);

    }

}
