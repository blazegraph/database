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
 * Created on Oct 14, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Unit tests for {@link ASTWildcardProjectionOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTWildcardProjectionOptimizer extends
        AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTWildcardProjectionOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTWildcardProjectionOptimizer(String name) {
        super(name);
    }
    

    /**
     * <pre>
     * SELECT * 
     *   JoinGroupNode {
     *     UnionNode {
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(2U)[http://example/p]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(3U)[http://example/q]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *     }
     *   }
     * </pre>
     */
    public void test_wildcardProjectionOptimizer00() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://example/p"));

        @SuppressWarnings("rawtypes")
        final IV q = makeIV(new URIImpl("http://example/q"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);
            
        }

        final IASTOptimizer rewriter = new ASTWildcardProjectionOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * <pre>
     * SELECT DISTINCT * 
     *   JoinGroupNode {
     *     UnionNode {
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(2U)[http://example/p]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(3U)[http://example/q]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *     }
     *   }
     * </pre>
     */
    public void test_wildcardProjectionOptimizer01() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://example/p"));

        @SuppressWarnings("rawtypes")
        final IV q = makeIV(new URIImpl("http://example/q"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);
            
        }

        final IASTOptimizer rewriter = new ASTWildcardProjectionOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * <pre>
     * SELECT REDUCED * 
     *   JoinGroupNode {
     *     UnionNode {
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(2U)[http://example/p]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), ConstantNode(TermId(3U)[http://example/q]), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *     }
     *   }
     * </pre>
     */
    public void test_wildcardProjectionOptimizer02() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://example/p"));

        @SuppressWarnings("rawtypes")
        final IV q = makeIV(new URIImpl("http://example/q"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.setReduced(true);
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.setReduced(true);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);
            
        }

        final IASTOptimizer rewriter = new ASTWildcardProjectionOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

}
