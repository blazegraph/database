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
 * Created on Oct 20, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Rewrites join groups having one or more joins which would involve a full
 * cross product as hash joins of sub-groups where there is a constraint imposed
 * indirectly via a FILTER operating across the variables bound by the joins.
 * This handles queries such as BSBM Q5.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/253
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTHashJoinOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        // First, process any pre-existing named subqueries.
        {
            
            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            if (namedSubqueries != null) {

                for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                    optimizeJoinGroups(context, sa, namedSubquery.getWhereClause());

                }

            }

        }
        
        // Now process the main where clause.
        optimizeJoinGroups(context, sa, queryRoot.getWhereClause());

        return queryRoot;
        
    }

    /**
     * Identify sets of joins which share variables only indirectly through a
     * constraint (FILTER). Such joins are pushed down into a sub-group along
     * with the constraint. The sub-group can be efficiently joined back to the
     * parent group (using a hash join) as long as there is a shared variable
     * between the sub-group and the parent (this condition is satisified if one
     * of the joins shares a variable with the parent group).
     */
    private void optimizeJoinGroups(final AST2BOpContext context,
            final StaticAnalysis sa,
            final GraphPatternGroup<IGroupMemberNode> group) {

        final int arity = group.arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = (BOp) group.get(i);
 
            if (child instanceof GraphPatternGroup<?>) {

                /*
                 * Note: Do recursion *before* we do the rewrite.
                 */
                optimizeJoinGroups(context, sa,
                        ((GraphPatternGroup<IGroupMemberNode>) child));

            } else if (child instanceof ServiceNode) {
            
                // Do not rewrite things inside of a SERVICE node.
                continue;
                
            }

        }
        
        /*
         * FIXME The logic to support this is in
         * PartitionedJoinGroup.canJoinUsingConstraints() and in
         * JGraph.expand(). That logic should be captured in the StaticAnalysis
         * class along with the test suite so we can apply it here and also 
         * apply it to the refactored RTO.
         */
        
    }

}
