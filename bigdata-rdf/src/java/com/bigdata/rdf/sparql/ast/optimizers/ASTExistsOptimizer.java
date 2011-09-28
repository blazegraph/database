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
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ExistsNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.SubqueryFunctionNodeBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTExistsOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;
        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final IGroupNode<IGroupMemberNode> whereClause = queryRoot
                .getWhereClause();

        if (whereClause == null) {

            throw new RuntimeException("Missing where clause? : " + queryNode);

        }

        rewrite((GroupNodeBase<IGroupMemberNode>) whereClause);

        return queryRoot;
        
    }

    /**
     * Look for FILTER.
     * 
     * @param p
     *            The parent.
     */
    @SuppressWarnings("unchecked")
    private void rewrite(final GroupNodeBase<IGroupMemberNode> p) {

        final int arity = p.size();

        for (int i = 0; i < arity; i++) {

            final IGroupMemberNode child = (IGroupMemberNode) p.get(i);

            if (child instanceof FilterNode) {

                rewrite(p, ((FilterNode) child).getValueExpressionNode());

            }

            if (child instanceof GroupNodeBase<?>) {

                // Recursion.
                rewrite((GroupNodeBase<IGroupMemberNode>) child);
                
            }

        }

    }

    /**
     * Look for {@link ExistsNode} or {@link NotExistsNode} in FILTER. If we
     * find such a node, we lift its group graph pattern onto the parent.
     * 
     * @param p
     *            The group in which the filter was found (aka the parent).
     * @param ve
     *            Part of the value expression for that filter.
     */
    private void rewrite(final GroupNodeBase<IGroupMemberNode> p,
            final IValueExpressionNode ve) {

        if (ve instanceof SubqueryFunctionNodeBase) {

            final SubqueryFunctionNodeBase subqueryFunction = (SubqueryFunctionNodeBase) ve;

            final GraphPatternGroup<IGroupMemberNode> graphPattern = subqueryFunction
                    .getGraphPattern();

            if (graphPattern != null) {

                if ((subqueryFunction instanceof ExistsNode)
                        || (subqueryFunction instanceof NotExistsNode)) {

                    final SubqueryRoot subquery = new SubqueryRoot(QueryType.ASK);

                    final ProjectionNode projection = new ProjectionNode();
                    subquery.setProjection(projection);
                    projection.addProjectionVar((VarNode) subqueryFunction
                            .get(0));
                    
                    subquery.setWhereClause(graphPattern);

                    // lift the SubqueryRoot into the parent.
                    p.addChild(subquery);

                }

            }

        }

        final int arity = ((BOp) ve).arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = ((BOp) ve).get(i);

            if(child instanceof IValueExpressionNode) {

                // Recursion.
                rewrite(p, (IValueExpressionNode) child);
                
            }

        }

    }
    
}
