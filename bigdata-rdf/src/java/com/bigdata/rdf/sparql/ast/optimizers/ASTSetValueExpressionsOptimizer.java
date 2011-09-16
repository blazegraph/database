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

import java.util.Iterator;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNodeContainer;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Visit all the value expression nodes and convert them into value expressions
 * using {@link AST2BOpUtility#toVE(String, IValueExpressionNode)}.
 * <p>
 * Note: <code>toVE()</code> is a NOP for {@link VarNode}s and
 * {@link ConstantNode}s. In fact, in only acts on the value expression of an
 * {@link AssignmentNode} and {@link FunctionNode}s.
 * <p>
 * Note: This has to be done carefully to avoid a side-effect during traversal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTSetValueExpressionsOptimizer.java 5193 2011-09-15 14:18:56Z
 *          thompsonbry $
 */
public class ASTSetValueExpressionsOptimizer implements IASTOptimizer {

    /**
     * 
     */
    public ASTSetValueExpressionsOptimizer() {
    }

    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        final QueryRoot query = (QueryRoot) queryNode;

        final String lex = context.db.getLexiconRelation().getNamespace();

//        convert1(lex, query); // Works around a concurrent modification.

        convert2(lex, query); // Should be faster.
        
        return query;
        
    }

//    /**
//     * Original version caches to avoid side-effects and visits all VENs both
//     * explicitly and recursively through toVE().
//     */
//    private void convert1(final String lex, final QueryRoot query) {
//
//        final Iterator<IValueExpressionNode> it = BOpUtility.visitAll(query,
//                IValueExpressionNode.class);
//
//        final ArrayList<IValueExpressionNode> allNodes = new ArrayList<IValueExpressionNode>();
//
//        while (it.hasNext()) {
//
//            allNodes.add(it.next());
//
//        }
//
//        for (IValueExpressionNode ven : allNodes) {
//
//            /*
//             * Convert and cache the value expression on the node as a
//             * side-effect.
//             */
//
//            AST2BOpUtility.toVE(lex, ven);
//
//        }
//
//    }

    /**
     * "Optimized" version visits only the nodes in the AST which can have VENs
     * and then invokes toVE() only for each top-level VEN. This works out to be
     * just {@link AssignmentNode}s, {@link FilterNode}s, and
     * {@link OrderByExpr}s. All of those are marked by the
     * {@link IValueExpressionNodeContainer} interface.
     * 
     * @param lex
     * @param query
     */
    private void convert2(final String lex, final QueryRoot query) {
        
        /*
         * Visit nodes that require modification.
         */
//        final IStriterator it = new Striterator(
//                BOpUtility.preOrderIteratorWithAnnotations(query))
//                .addFilter(new Filter() {
//
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public boolean isValid(Object obj) {
//                        if (obj instanceof AssignmentNode)
//                            return true;
//                        if (obj instanceof FilterNode)
//                            return true;
//                        if (obj instanceof OrderByExpr)
//                            return true;
//                        return false;
//                    }
//                });

        final Iterator<IValueExpressionNodeContainer> it = BOpUtility.visitAll(
                query, IValueExpressionNodeContainer.class);

        while (it.hasNext()) {

            /*
             * Convert and cache the value expression on the node as a
             * side-effect.
             * 
             * PROJECT's BIND()s (ALL projections, anywhere in the query
             * including subqueries and named subqueries).
             * 
             * BIND() in GROUP BY, HAVING.
             * 
             * FILTER()/BIND() in groups (ALL groups, anywhere in the query, including
             * EXISTS and SERVICE).
             * 
             */
            final IValueExpressionNodeContainer op = it.next();

            AST2BOpUtility.toVE(lex, op.getValueExpressionNode());
            
//            if(op instanceof AssignmentNode) {
//                
//                AssignmentNode bind = (AssignmentNode) op;
//                
//                AST2BOpUtility.toVE(lex, bind.getValueExpressionNode());
//
//            } else if(op instanceof FilterNode) {
//                
//                final FilterNode filter = (FilterNode) op;
//                
//                AST2BOpUtility.toVE(lex,
//                        filter.getValueExpressionNode());
//                
//            } else if(op instanceof OrderByExpr) {
//                
//                final OrderByExpr orderBy = (OrderByExpr) op;
//                
//                AST2BOpUtility.toVE(lex,
//                        orderBy.getValueExpressionNode());
//                
//            } else {
//
//                throw new AssertionError(op.toString());
//                
//            }
            
        }

    }
    
}
