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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

import cutthecrap.utils.striterators.Striterator;

/**
 * Lift {@link SubqueryRoot}s into named subqueries when appropriate.
 * 
 * FIXME We should do this if there are no join variables for the SPARQL 1.1
 * subquery. That will prevent multiple evaluations of the SPARQL 1.1 subquery
 * since the named subquery is run once before the main WHERE clause.
 * <p>
 * This optimizer needs to examine the required statement patterns and decide
 * whether we are better off running the named subquery pipelined after the
 * required statement patterns, using a hash join after the required statement
 * patterns, or running it as a named subquery and then joining in the named
 * solution set (and again, either before or after everything else).
 * 
 * FIXME If a subquery does not share ANY variables which MUST be bound in the
 * parent's context then rewrite the subquery into a named/include pattern so it
 * will run exactly once. (If it does not share any variables at all then it
 * will produce a cross product and, again, we want to run that subquery once.)
 * 
 * @see SubqueryRoot.Annotations#RUN_ONCE
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTSparql11SubqueryOptimizer.java 5193 2011-09-15 14:18:56Z
 *          thompsonbry $
 */
public class ASTSparql11SubqueryOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {
        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        /*
         * Lift out SPARQL 1.1 subqueries which use both LIMIT and ORDER BY. Due
         * to the interaction of the LIMIT and ORDER BY clause, these subqueries
         * MUST be run first since they can produce different results if they
         * are run "as-bound".
         */
        
        liftSubqueryWithLimitAndOrderBy(sa, queryRoot.getWhereClause());
        
        if(false) {
            
            rewriteSparql11Subqueries(queryRoot);
            
        }

        return queryRoot;
        
    }

    /**
     * Lift out SPARQL 1.1 subqueries which use both LIMIT and ORDER BY. Due to
     * the interaction of the LIMIT and ORDER BY clause, these subqueries MUST
     * be run first since they can produce different results if they are run
     * "as-bound".
     */
    private void liftSubqueryWithLimitAndOrderBy(
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group) {

        final int arity = group.arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = (BOp) group.get(i);

            if (child instanceof SubqueryRoot) {

                final SubqueryRoot subquery = (SubqueryRoot) child;

                final SliceNode slice = subquery.getSlice();
                
                if(slice == null)
                    continue;

                final OrderByNode orderBy = subquery.getOrderBy();
                
                if(orderBy == null)
                    continue;
                
                liftSparql11Subquery(sa, subquery);
                
            } else if (child instanceof GraphPatternGroup<?>) {

                // recursion.
                liftSubqueryWithLimitAndOrderBy(sa,
                        ((GraphPatternGroup<IGroupMemberNode>) child));

            } else if (child instanceof ServiceNode) {
            
                // Do not rewrite things inside of a SERVICE node.
                continue;
                
            }

        }
        
    }

    static private void rewriteSparql11Subqueries(final QueryRoot queryRoot){

        final Striterator itr2 = new Striterator(
                BOpUtility.postOrderIterator((BOp) queryRoot.getWhereClause()));

        itr2.addTypeFilter(SubqueryRoot.class);

        final List<SubqueryRoot> subqueries  = new ArrayList<SubqueryRoot>();

        while (itr2.hasNext()) {

            subqueries.add((SubqueryRoot)itr2.next());

        }
        
    }
    
    private void liftSparql11Subquery(final StaticAnalysis sa,
            final SubqueryRoot root) {

        final IGroupNode<IGroupMemberNode> parent = root.getParent();

        parent.removeChild(root);

        final String newName = UUID.randomUUID().toString();

        final NamedSubqueryInclude include = new NamedSubqueryInclude(newName);

        parent.addChild(include);

        final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                root.getQueryType(), newName);

        nsr.setConstruct(root.getConstruct());
        nsr.setGroupBy(root.getGroupBy());
        nsr.setHaving(root.getHaving());
        nsr.setOrderBy(root.getOrderBy());
        nsr.setProjection(root.getProjection());
        nsr.setSlice(root.getSlice());
        nsr.setWhereClause(root.getWhereClause());

        NamedSubqueriesNode namedSubqueries = sa.getQueryRoot()
                .getNamedSubqueries();

        if (namedSubqueries == null) {

            namedSubqueries = new NamedSubqueriesNode();

            sa.getQueryRoot().setNamedSubqueries(namedSubqueries);

        }

        namedSubqueries.add(nsr);

    }

}
