/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.math.BigInteger;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for {@link ASTSetValueExpressionsOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTSetValueExpressionOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTSetValueExpressionOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTSetValueExpressionOptimizer(String name) {
        super(name);
    }

    /**
     * Given
     * 
     * <pre>
     * SELECT ?s (?o as 12+1) where {?s ?p ?o}
     * </pre>
     * 
     * Verify that the AST is rewritten as:
     * 
     * <pre>
     * SELECT ?s (?o as 13) where {?s ?p ?o}
     * </pre>
     * 
     * TODO unit test with FILTER(sameTerm(var,constExpr)) in {@link ASTBindingAssigner} 
     */
    public void test_reduceFunctionToConstant() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */

        @SuppressWarnings("rawtypes")
        final IV c1 = makeIV(store.getValueFactory().createLiteral(1));
        @SuppressWarnings("rawtypes")
        final IV c12 = makeIV(store.getValueFactory().createLiteral(12));
        @SuppressWarnings("rawtypes")
        final IV c13 = store.getLexiconRelation().getInlineIV(
                store.getValueFactory().createLiteral("13", XSD.INTEGER));
        
        final IBindingSet[] bsets = new IBindingSet[] { //
        new ListBindingSet(//
//                new IVariable[] { Var.var("p") },//
//                new IConstant[] { new Constant<IV>(mockIV) }
                ) //
        };

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("s"));

            final FunctionNode fn = FunctionNode.add(new ConstantNode(c12),
                    new ConstantNode(c1));

            projection.addProjectionExpression(new AssignmentNode(new VarNode(
                    "o"), fn));

            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);

            projection.addProjectionVar(new VarNode("s"));

            final FunctionNode fn = FunctionNode.add(new ConstantNode(c12),
                    new ConstantNode(c1));

            fn.setValueExpression(new Constant<IV<?, ?>>(c13));

            projection.addProjectionExpression(new AssignmentNode(new VarNode(
                    "o"), fn));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        final IASTOptimizer rewriter = new ASTSetValueExpressionsOptimizer();
        
        final IQueryNode actual = rewriter.optimize(new AST2BOpContext(
                new ASTContainer(given), store), given/* queryNode */, bsets);

        assertSameAST(expected, actual);
        
    }

}
