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

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.Annotations;
import com.bigdata.rdf.store.BD;

/**
 * Test suite for {@link ASTStaticJoinOptimizer}.
 */
public class TestASTStaticJoinOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTStaticJoinOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTStaticJoinOptimizer(String name) {
        super(name);
    }

    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *   }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_simpleReorder01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *     JoinGroupNode { }
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *     JoinGroupNode { }
     *   }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_simpleReorder02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(new JoinGroupNode());
            
            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(new JoinGroupNode());
            
            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(x), ConstantNode(e), ConstantNode(e))
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *     StatementPatternNode(VarNode(x), ConstantNode(d), ConstantNode(d))
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *     StatementPatternNode(VarNode(x), ConstantNode(c), ConstantNode(c))
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *     StatementPatternNode(VarNode(x), ConstantNode(c), ConstantNode(c))
     *     StatementPatternNode(VarNode(x), ConstantNode(d), ConstantNode(d))
     *     StatementPatternNode(VarNode(x), ConstantNode(e), ConstantNode(e))
     *   }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_simpleReorder03() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        @SuppressWarnings("rawtypes")
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        @SuppressWarnings("rawtypes")
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        @SuppressWarnings("rawtypes")
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 5l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 5l));
            
            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(e), ConstantNode(e))
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(d), ConstantNode(d))
     *     JoinGroupNode { }
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(c), ConstantNode(c))
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(c), ConstantNode(c))
     *     JoinGroupNode { }
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(d), ConstantNode(d))
     *     JoinGroupNode { }
     *     StatementPatternNode(VarNode(x), ConstantNode(e), ConstantNode(e))
     *   }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_simpleReorder04() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        @SuppressWarnings("rawtypes")
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        @SuppressWarnings("rawtypes")
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        @SuppressWarnings("rawtypes")
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 5l));

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(new JoinGroupNode());
            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            whereClause.addChild(new JoinGroupNode());
            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(new JoinGroupNode());
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 5l));
            
            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(x), ConstantNode(e), ConstantNode(e))
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *     StatementPatternNode(VarNode(x), ConstantNode(f), ConstantNode(f)) [OPTIONAL]
     *     StatementPatternNode(VarNode(x), ConstantNode(d), ConstantNode(d))
     *     StatementPatternNode(VarNode(x), ConstantNode(g), ConstantNode(g)) [OPTIONAL]
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *     StatementPatternNode(VarNode(x), ConstantNode(c), ConstantNode(c))
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a))
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b))
     *     StatementPatternNode(VarNode(x), ConstantNode(c), ConstantNode(c))
     *     StatementPatternNode(VarNode(x), ConstantNode(d), ConstantNode(d))
     *     StatementPatternNode(VarNode(x), ConstantNode(e), ConstantNode(e))
     *     StatementPatternNode(VarNode(x), ConstantNode(f), ConstantNode(f)) [OPTIONAL]
     *     StatementPatternNode(VarNode(x), ConstantNode(g), ConstantNode(g)) [OPTIONAL]
     *   }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_simpleOptional01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        @SuppressWarnings("rawtypes")
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        @SuppressWarnings("rawtypes")
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        @SuppressWarnings("rawtypes")
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        @SuppressWarnings("rawtypes")
        final IV f = makeIV(new URIImpl("http://example/f"));
        
        @SuppressWarnings("rawtypes")
        final IV g = makeIV(new URIImpl("http://example/g"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 5l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(g), new ConstantNode(g), 1l, true));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 5l));
            
            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(g), new ConstantNode(g), 1l, true));

            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     ServiceNode {
     *       StatementPatternNode(VarNode(x), ConstantNode(bd:search), ConstantNode("foo")
     *     }
     *     StatementPatternNode(VarNode(y), ConstantNode(b), ConstantNode(b)) [CARDINALITY=1]
     *     StatementPatternNode(VarNode(x), ConstantNode(a), VarNode(y)) [CARDINALITY=2]
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     ServiceNode {
     *       StatementPatternNode(VarNode(x), ConstantNode(bd:search), ConstantNode("foo")
     *     }
     *     StatementPatternNode(VarNode(x), ConstantNode(a), VarNode(y)) [CARDINALITY=2]
     *     StatementPatternNode(VarNode(y), ConstantNode(b), ConstantNode(b)) [CARDINALITY=1]
     *   }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_ServiceNode01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV search = makeIV(BD.SEARCH);

        @SuppressWarnings("rawtypes")
        final IV foo = makeIV(new LiteralImpl("foo"));

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
//        @SuppressWarnings("rawtypes")
//        final IV c = makeIV(new URIImpl("http://example/c"));
//        
//        @SuppressWarnings("rawtypes")
//        final IV d = makeIV(new URIImpl("http://example/d"));
//        
//        @SuppressWarnings("rawtypes")
//        final IV e = makeIV(new URIImpl("http://example/e"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            final ServiceNode serviceNode = new ServiceNode(BD.SEARCH,
                    new JoinGroupNode(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(search), new ConstantNode(foo))));
            
            whereClause.addChild(serviceNode);

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(b), new ConstantNode(b), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new VarNode("y"), 2l));

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            final ServiceNode serviceNode = new ServiceNode(BD.SEARCH,
                    new JoinGroupNode(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(search), new ConstantNode(foo))));
            
            whereClause.addChild(serviceNode);

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new VarNode("y"), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(b), new ConstantNode(b), 1l));

            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     ServiceNode {
     *       StatementPatternNode(VarNode(x), ConstantNode(bd:search), ConstantNode("foo")
     *     }
     *     StatementPatternNode(VarNode(x), ConstantNode(f), ConstantNode(f)) [CARDINALITY=1, OPTIONAL]
     *     StatementPatternNode(VarNode(y), ConstantNode(d), ConstantNode(d)) [CARDINALITY=3]
     *     StatementPatternNode(VarNode(y), ConstantNode(g), ConstantNode(g)) [CARDINALITY=1, OPTIONAL]
     *     StatementPatternNode(VarNode(y), ConstantNode(c), ConstantNode(c)) [CARDINALITY=2]
     *     StatementPatternNode(VarNode(y), ConstantNode(e), ConstantNode(e)) [CARDINALITY=4]
     *     StatementPatternNode(VarNode(x), ConstantNode(a), VarNode(y)) [CARDINALITY=1000]
     *     StatementPatternNode(VarNode(y), ConstantNode(b), ConstantNode(b)) [CARDINALITY=1]
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     ServiceNode {
     *       StatementPatternNode(VarNode(x), ConstantNode(bd:search), ConstantNode("foo")
     *     }
     *     StatementPatternNode(VarNode(x), ConstantNode(a), VarNode(y)) [CARDINALITY=1000]
     *     StatementPatternNode(VarNode(y), ConstantNode(b), ConstantNode(b)) [CARDINALITY=1]
     *     StatementPatternNode(VarNode(y), ConstantNode(c), ConstantNode(c)) [CARDINALITY=2]
     *     StatementPatternNode(VarNode(y), ConstantNode(d), ConstantNode(d)) [CARDINALITY=3]
     *     StatementPatternNode(VarNode(y), ConstantNode(e), ConstantNode(e)) [CARDINALITY=4]
     *     StatementPatternNode(VarNode(x), ConstantNode(f), ConstantNode(f)) [CARDINALITY=1, OPTIONAL]
     *     StatementPatternNode(VarNode(y), ConstantNode(g), ConstantNode(g)) [CARDINALITY=1, OPTIONAL]
     *   }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_ServiceNode02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV search = makeIV(BD.SEARCH);

        @SuppressWarnings("rawtypes")
        final IV foo = makeIV(new LiteralImpl("foo"));

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        @SuppressWarnings("rawtypes")
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        @SuppressWarnings("rawtypes")
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        @SuppressWarnings("rawtypes")
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        @SuppressWarnings("rawtypes")
        final IV f = makeIV(new URIImpl("http://example/f"));
        
        @SuppressWarnings("rawtypes")
        final IV g = makeIV(new URIImpl("http://example/g"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            final ServiceNode serviceNode = new ServiceNode(BD.SEARCH,
                    new JoinGroupNode(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(search), new ConstantNode(foo))));
            
            whereClause.addChild(serviceNode);

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(d), new ConstantNode(d), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(g), new ConstantNode(g), 1l, true));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(c), new ConstantNode(c), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(e), new ConstantNode(e), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new VarNode("y"), 1000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(b), new ConstantNode(b), 1l));

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            final ServiceNode serviceNode = new ServiceNode(BD.SEARCH,
                    new JoinGroupNode(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(search), new ConstantNode(foo))));
            
            whereClause.addChild(serviceNode);

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new VarNode("y"), 1000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(b), new ConstantNode(b), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(c), new ConstantNode(c), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(d), new ConstantNode(d), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(e), new ConstantNode(e), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(g), new ConstantNode(g), 1l, true));

            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b)) [CARDINALITY=2]
     *     StatementPatternNode(VarNode(x), ConstantNode(c), ConstantNode(c)) [CARDINALITY=3]
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a)) [CARDINALITY=1]
     *     JoinGroupNode {
     *       StatementPatternNode(VarNode(x), ConstantNode(d), ConstantNode(d)) [CARDINALITY=10]
     *       StatementPatternNode(VarNode(y), ConstantNode(f), ConstantNode(f)) [CARDINALITY=5]
     *       StatementPatternNode(VarNode(y), ConstantNode(e), ConstantNode(e)) [CARDINALITY=4]
     *     }
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(x), ConstantNode(a), ConstantNode(a)) [CARDINALITY=1]
     *     StatementPatternNode(VarNode(x), ConstantNode(b), ConstantNode(b)) [CARDINALITY=2]
     *     StatementPatternNode(VarNode(x), ConstantNode(c), ConstantNode(c)) [CARDINALITY=3]
     *     JoinGroupNode {
     *       StatementPatternNode(VarNode(x), ConstantNode(d), ConstantNode(d)) [CARDINALITY=10]
     *       StatementPatternNode(VarNode(y), ConstantNode(e), ConstantNode(e)) [CARDINALITY=4]
     *       StatementPatternNode(VarNode(y), ConstantNode(f), ConstantNode(f)) [CARDINALITY=5]
     *     }
     *   }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_nestedOptionals01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        @SuppressWarnings("rawtypes")
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        @SuppressWarnings("rawtypes")
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        @SuppressWarnings("rawtypes")
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        @SuppressWarnings("rawtypes")
        final IV f = makeIV(new URIImpl("http://example/f"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            final JoinGroupNode subgroup = new JoinGroupNode();
            
            subgroup.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(f), new ConstantNode(f), 5l));

            subgroup.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(e), new ConstantNode(e), 4l));

            subgroup.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 10l));

            whereClause.addChild(subgroup);

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            final JoinGroupNode subgroup = new JoinGroupNode();
            
            subgroup.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 10l));

            subgroup.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(e), new ConstantNode(e), 4l));

            subgroup.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(f), new ConstantNode(f), 5l));

            whereClause.addChild(subgroup);
            
            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_nestedOptionals02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        @SuppressWarnings("rawtypes")
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        @SuppressWarnings("rawtypes")
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        @SuppressWarnings("rawtypes")
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        @SuppressWarnings("rawtypes")
        final IV f = makeIV(new URIImpl("http://example/f"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            final JoinGroupNode subgroup = new JoinGroupNode();
            
            subgroup.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new VarNode("y"), 10l));

            subgroup.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(e), new ConstantNode(e), 4l));

            whereClause.addChild(subgroup);

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            final JoinGroupNode subgroup = new JoinGroupNode();
            
            subgroup.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new VarNode("y"), 10l));

            subgroup.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(e), new ConstantNode(e), 4l));

            whereClause.addChild(subgroup);
            
            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_nestedOptionals03() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        @SuppressWarnings("rawtypes")
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        @SuppressWarnings("rawtypes")
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        @SuppressWarnings("rawtypes")
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        @SuppressWarnings("rawtypes")
        final IV f = makeIV(new URIImpl("http://example/f"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            final JoinGroupNode subgroup1 = new JoinGroupNode();
            
            subgroup1.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(a), new ConstantNode(a), 1l));
            
            subgroup1.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(b), new ConstantNode(b), 1l));
            
            whereClause.addChild(subgroup1);

            final JoinGroupNode subgroup2 = new JoinGroupNode();
            
            subgroup2.addChild(newStatementPatternNode(new VarNode("z"),
                    new ConstantNode(a), new ConstantNode(a), 1l));
            
            subgroup2.addChild(newStatementPatternNode(new VarNode("z"),
                    new ConstantNode(b), new ConstantNode(b), 2l));
            
            final JoinGroupNode subgroup3 = new JoinGroupNode();
            
            subgroup3.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(d), new ConstantNode(d), 1l));
            
            subgroup3.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 10l));
            
            subgroup2.addChild(subgroup3);
            
            whereClause.addChild(subgroup2);

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            final JoinGroupNode subgroup1 = new JoinGroupNode();
            
            subgroup1.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(a), new ConstantNode(a), 1l));
            
            subgroup1.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(b), new ConstantNode(b), 1l));
            
            whereClause.addChild(subgroup1);

            final JoinGroupNode subgroup2 = new JoinGroupNode();
            
            subgroup2.addChild(newStatementPatternNode(new VarNode("z"),
                    new ConstantNode(a), new ConstantNode(a), 1l));
            
            subgroup2.addChild(newStatementPatternNode(new VarNode("z"),
                    new ConstantNode(b), new ConstantNode(b), 2l));
            
            final JoinGroupNode subgroup3 = new JoinGroupNode();
            
            subgroup3.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 10l));
            
            subgroup3.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(d), new ConstantNode(d), 1l));
            
            subgroup2.addChild(subgroup3);
            
            whereClause.addChild(subgroup2);
            
            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     ServiceNode {
     *       StatementPatternNode(VarNode(x), ConstantNode(bd:search), ConstantNode("foo")
     *     }
     *     StatementPatternNode(VarNode(y), ConstantNode(b), ConstantNode(b)) [CARDINALITY=1]
     *     StatementPatternNode(VarNode(x), ConstantNode(a), VarNode(y)) [CARDINALITY=2]
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   SELECT VarNode(x)
     *   JoinGroupNode {
     *     ServiceNode {
     *       StatementPatternNode(VarNode(x), ConstantNode(bd:search), ConstantNode("foo")
     *     }
     *     StatementPatternNode(VarNode(x), ConstantNode(a), VarNode(y)) [CARDINALITY=2]
     *     StatementPatternNode(VarNode(y), ConstantNode(b), ConstantNode(b)) [CARDINALITY=1]
     *   }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_NSI01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV search = makeIV(BD.SEARCH);

        @SuppressWarnings("rawtypes")
        final IV foo = makeIV(new LiteralImpl("foo"));

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(new URIImpl("http://example/a"));

        @SuppressWarnings("rawtypes")
        final IV b = makeIV(new URIImpl("http://example/b"));
        
        @SuppressWarnings("rawtypes")
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        @SuppressWarnings("rawtypes")
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        @SuppressWarnings("rawtypes")
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

        	final NamedSubqueryRoot namedSubquery = new NamedSubqueryRoot(QueryType.SELECT, "_set1");
        	{
        		
                final ProjectionNode projection = new ProjectionNode();
                namedSubquery.setProjection(projection);
                projection.addProjectionExpression(new AssignmentNode(new VarNode("_var1"), new VarNode("_var1")));
        		
                final JoinGroupNode joinGroup1 = new JoinGroupNode(
                        newStatementPatternNode(new VarNode("_var1"),
                                new ConstantNode(a), new ConstantNode(b), 1l));
                
                namedSubquery.setWhereClause(joinGroup1);
                
        	}
        	
            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("_var1"));
            projection.addProjectionVar(new VarNode("_var2"));
            projection.addProjectionVar(new VarNode("_var4"));
            
            final JoinGroupNode joinGroup1 = new JoinGroupNode();

            joinGroup1.addChild(new NamedSubqueryInclude("_set1"));
            
            joinGroup1.addChild(newStatementPatternNode(new VarNode("_var1"),
                    new ConstantNode(c), new VarNode("_var2"), 1l, true));
            
            final JoinGroupNode joinGroup2 = new JoinGroupNode(true);
            
            joinGroup2.addChild(newStatementPatternNode(new VarNode("_var12"),
                    new ConstantNode(e), new VarNode("_var4"), 10l));
            joinGroup2.addChild(newStatementPatternNode(new VarNode("_var12"),
                    new ConstantNode(d), new VarNode("_var1"), 100l));
            
            joinGroup1.addChild(joinGroup2);
            
            given.setWhereClause(joinGroup1);
            given.getNamedSubqueriesNotNull().add(namedSubquery);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
        	final NamedSubqueryRoot namedSubquery = new NamedSubqueryRoot(QueryType.SELECT, "_set1");
        	{
        		
                final ProjectionNode projection = new ProjectionNode();
                namedSubquery.setProjection(projection);
                projection.addProjectionExpression(new AssignmentNode(new VarNode("_var1"), new VarNode("_var1")));
        		
                final JoinGroupNode joinGroup1 = new JoinGroupNode(
                        newStatementPatternNode(new VarNode("_var1"),
                                new ConstantNode(a), new ConstantNode(b), 1l));
                
                namedSubquery.setWhereClause(joinGroup1);
                
        	}
        	
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("_var1"));
            projection.addProjectionVar(new VarNode("_var2"));
            projection.addProjectionVar(new VarNode("_var4"));
            
            final JoinGroupNode joinGroup1 = new JoinGroupNode();

            joinGroup1.addChild(new NamedSubqueryInclude("_set1"));
            
            joinGroup1.addChild(newStatementPatternNode(new VarNode("_var1"),
                    new ConstantNode(c), new VarNode("_var2"), 1l, true));
            
            final JoinGroupNode joinGroup2 = new JoinGroupNode(true);
            
            joinGroup2.addChild(newStatementPatternNode(new VarNode("_var12"),
                    new ConstantNode(d), new VarNode("_var1"), 100l));
            joinGroup2.addChild(newStatementPatternNode(new VarNode("_var12"),
                    new ConstantNode(e), new VarNode("_var4"), 10l));
            
            joinGroup1.addChild(joinGroup2);
            
            expected.setWhereClause(joinGroup1);
            expected.getNamedSubqueriesNotNull().add(namedSubquery);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    private StatementPatternNode newStatementPatternNode(
            final TermNode s, final TermNode p, final TermNode o, 
            final long cardinality) {
        
        return newStatementPatternNode(s, p, o, cardinality, false);
        
    }
        
    private StatementPatternNode newStatementPatternNode(
            final TermNode s, final TermNode p, final TermNode o, 
            final long cardinality, final boolean optional) {
        
        final StatementPatternNode sp = new StatementPatternNode(s, p, o);
        
        sp.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
        
        if (optional) {
            
            sp.setOptional(true);
            
        }
        
        return sp;
        
    }

}
