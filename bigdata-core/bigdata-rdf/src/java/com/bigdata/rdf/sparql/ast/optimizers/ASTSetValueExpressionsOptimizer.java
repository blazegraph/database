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

import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNodeContainer;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.RangeNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Visit all the value expression nodes and convert them into value expressions
 * using {@link AST2BOpUtility#toVE(String, IValueExpressionNode)}. If a value
 * expression can be evaluated to a constant, then it is replaced by that
 * constant.
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

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     

        final QueryRoot query = (QueryRoot) queryNode;

        final GlobalAnnotations globals = new GlobalAnnotations(
        		context.getLexiconNamespace(),
        		context.getTimestamp()
        		);
        
//        final String lex = context.db.getLexiconRelation().getNamespace();

//        convert1(lex, query); // Works around a concurrent modification.

        convert2(context.context, globals, query); // Should be faster.
        
        return new QueryNodeWithBindingSet(query, bindingSets);
        
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
    private void convert2(final BOpContextBase context, final GlobalAnnotations globals, final QueryRoot query) {
        
        /*
         * Visit nodes that require modification.
         */
        final IStriterator it = new Striterator(
                BOpUtility.preOrderIteratorWithAnnotations(query))
                .addFilter(new Filter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean isValid(Object obj) {
//                        if (obj instanceof AssignmentNode)
//                            return true;
//                        if (obj instanceof FilterNode)
//                            return true;
//                        if (obj instanceof OrderByExpr)
//                            return true;
                        if (obj instanceof IValueExpressionNodeContainer)
                            return true;
                        if (obj instanceof HavingNode)
                            return true;
                        if (obj instanceof StatementPatternNode)
                            return true;
                        return false;
                    }
                });

//        final Iterator<IValueExpressionNodeContainer> it = BOpUtility.visitAll(
//                query, IValueExpressionNodeContainer.class);

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
             * HAVING : its children are top-level IValueExpressionNodes.
             */
            final Object op = it.next();

            if (op instanceof IValueExpressionNodeContainer) {

                // AssignmentNode, FilterNode, OrderByExpr
        		AST2BOpUtility.toVE(context, globals, 
        				((IValueExpressionNodeContainer) op).getValueExpressionNode());
                
            } else if (op instanceof HavingNode) {
                
                final HavingNode havingNode = (HavingNode)op;
                
                for(IValueExpressionNode node : havingNode) {
                
                    AST2BOpUtility.toVE(context, globals, node);
                    
                }
                
            } else if (op instanceof StatementPatternNode) {
                
            	final StatementPatternNode sp = (StatementPatternNode) op;
            	
            	final RangeNode range = sp.getRange();
            	
            	if (range != null) {
            		
            		if (range.from() != null)
            			AST2BOpUtility.toVE(context, globals, range.from());
            			
            		if (range.to() != null)
            			AST2BOpUtility.toVE(context, globals, range.to());
            		
            	}
                
            }
            
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
