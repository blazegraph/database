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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.BD;

/**
 * Test suite for {@link ASTEmptyGroupOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTEmptyGroupOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTEmptyGroupOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTEmptyGroupOptimizer(String name) {
        super(name);
    }

    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(subj)
     *     JoinGroupNode {
     *       JoinGroupNode [context=VarNode(g)] {
     *         StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), NAMED_CONTEXTS)
     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), NAMED_CONTEXTS)
     *       }
     *     }
     * </pre>
     * 
     * Replace the outer {@link JoinGroupNode} with the inner
     * {@link JoinGroupNode}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_eliminateJoinGroup01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV bdSearchIV = TermId.mockIV(VTE.URI);
        bdSearchIV.setValue(store.getValueFactory().createURI(
                BD.SEARCH.toString()));

        final IV mikeIV = TermId.mockIV(VTE.LITERAL);
        bdSearchIV.setValue(store.getValueFactory().createLiteral("mike"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("subj"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final JoinGroupNode graphGroup = new JoinGroupNode();
            whereClause.addChild(graphGroup);
            
            graphGroup.setContext(new VarNode("g"));
            
            graphGroup.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

            graphGroup.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("subj"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            
            whereClause.setContext(new VarNode("g"));
            
            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * Unit test for
     * 
     * <pre>
     * { {} } => {}
     * </pre>
     * 
     * but not
     * 
     * <pre>
     * { GRAPH foo {} } => {}
     * </pre>
     * 
     * TODO also test for removal of empty UNIONs.
     */
    public void test_eliminateJoinGroup02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final JoinGroupNode graphGroup = new JoinGroupNode();
            graphGroup.setContext(new VarNode("g"));
            whereClause.addChild(graphGroup);

            final JoinGroupNode joinGroup = new JoinGroupNode();
            whereClause.addChild(joinGroup);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            final JoinGroupNode graphGroup = new JoinGroupNode();
            graphGroup.setContext(new VarNode("g"));
            whereClause.addChild(graphGroup);

//            final JoinGroupNode joinGroup = new JoinGroupNode();
//            whereClause.addChild(joinGroup);
            
        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

}
