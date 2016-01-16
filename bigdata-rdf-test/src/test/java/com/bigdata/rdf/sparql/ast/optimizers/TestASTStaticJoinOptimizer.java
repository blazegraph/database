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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.DISTINCT;
import static com.bigdata.rdf.sparql.ast.optimizers.AbstractOptimizerTestCase.HelperFlag.OPTIONAL;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BDS;
/**
 * Test suite for {@link ASTStaticJoinOptimizer}.
 */
public class TestASTStaticJoinOptimizer extends AbstractOptimizerTestCase
{


	@Override
	protected ASTStaticJoinOptimizer newOptimizer() {
		return new ASTStaticJoinOptimizer();
	}
	
    public void test_simpleOptional01A() {
    	new Helper() {{
    		given = select( varNode(x), 
    				where (
    						statementPatternNode(varNode(x), constantNode(e), constantNode(e),5),
    						statementPatternNode(varNode(x), constantNode(b), constantNode(b),2),
    						statementPatternNode(varNode(x), constantNode(d), constantNode(d),4),
    						statementPatternNode(varNode(x), constantNode(a), constantNode(a),1),
    						statementPatternNode(varNode(x), constantNode(c), constantNode(c),3),
    						statementPatternNode(varNode(x), constantNode(f), constantNode(f),1,OPTIONAL),
    						statementPatternNode(varNode(x), constantNode(g), constantNode(g),1,OPTIONAL)
    						) );
    		expected = select( varNode(x), 
    				where (
    						statementPatternNode(varNode(x), constantNode(a), constantNode(a),1),
    						statementPatternNode(varNode(x), constantNode(b), constantNode(b),2),
    						statementPatternNode(varNode(x), constantNode(c), constantNode(c),3),
    						statementPatternNode(varNode(x), constantNode(d), constantNode(d),4),
    						statementPatternNode(varNode(x), constantNode(e), constantNode(e),5),
    						statementPatternNode(varNode(x), constantNode(f), constantNode(f),1,OPTIONAL),
    						statementPatternNode(varNode(x), constantNode(g), constantNode(g),1,OPTIONAL)
    						) );


    	}}.test();
    }
    /**
     * 
     */
    public TestASTStaticJoinOptimizer() {
    	super();
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
    @SuppressWarnings("rawtypes")
    public void test_simpleReorder01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);
        
        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
    @SuppressWarnings("rawtypes")
    public void test_simpleReorder02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
    @SuppressWarnings("rawtypes")
    public void test_simpleReorder03() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));

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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
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
    @SuppressWarnings("rawtypes")
        public void test_simpleReorder04() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));
        
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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
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
     *     StatementPatternNode(VarNode(x), ConstantNode(f), ConstantNode(f)) [OPTIONAL]
     *     StatementPatternNode(VarNode(x), ConstantNode(g), ConstantNode(g)) [OPTIONAL]
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
    @SuppressWarnings("rawtypes")
    public void test_simpleOptional01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        final IV f = makeIV(new URIImpl("http://example/f"));
        
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
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(g), new ConstantNode(g), 1l, true));

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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
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
    @SuppressWarnings("rawtypes")
    public void test_ServiceNode01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV search = makeIV(BDS.SEARCH);

        final IV foo = makeIV(new LiteralImpl("foo"));

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
        final IV serviceRef = makeIV(BDS.SEARCH);
        
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

            final ServiceNode serviceNode = new ServiceNode(new ConstantNode(serviceRef),
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

            final ServiceNode serviceNode = new ServiceNode(new ConstantNode(serviceRef),
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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
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
    @SuppressWarnings("rawtypes")
    public void test_ServiceNode02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV search = makeIV(BDS.SEARCH);

        final IV foo = makeIV(new LiteralImpl("foo"));

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        final IV f = makeIV(new URIImpl("http://example/f"));
        
        final IV g = makeIV(new URIImpl("http://example/g"));

        final IV serviceRef = makeIV(BDS.SEARCH);

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            final ServiceNode serviceNode = new ServiceNode(new ConstantNode(serviceRef),
                    new JoinGroupNode(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(search), new ConstantNode(foo))));
            
            whereClause.addChild(serviceNode);

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(d), new ConstantNode(d), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(c), new ConstantNode(c), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(e), new ConstantNode(e), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new VarNode("y"), 1000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(b), new ConstantNode(b), 1l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true));

            whereClause.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(g), new ConstantNode(g), 1l, true));

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            final ServiceNode serviceNode = new ServiceNode(new ConstantNode(serviceRef),
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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
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
    @SuppressWarnings("rawtypes")
    public void test_nestedOptionals01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        final IV e = makeIV(new URIImpl("http://example/e"));
        
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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
        assertSameAST(expected, actual);

    }
    
    /**
     */
    @SuppressWarnings("rawtypes")
    public void test_nestedOptionals02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

//        final IV b = makeIV(new URIImpl("http://example/b"));
//        
//        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        final IV e = makeIV(new URIImpl("http://example/e"));
        
//        final IV f = makeIV(new URIImpl("http://example/f"));
        
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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
        assertSameAST(expected, actual);

    }
    
    /**
     */
    @SuppressWarnings("rawtypes")
    public void test_nestedOptionals03() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
//        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        final IV e = makeIV(new URIImpl("http://example/e"));
        
//        final IV f = makeIV(new URIImpl("http://example/f"));
        
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
            
            // Note: both x and y are bound at this point, so the best order
            // is lowest cardinality first
            
            subgroup3.addChild(newStatementPatternNode(new VarNode("y"),
                    new ConstantNode(d), new ConstantNode(d), 1l));

            subgroup3.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 10l));
            
            subgroup2.addChild(subgroup3);
            
            whereClause.addChild(subgroup2);
            
            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
        assertSameAST(expected, actual);

    }
    

    public void test_NSI01X() {
    	new Helper() {{
    		given = select( varNodes(x,y,z), 
    				namedSubQuery("_set1",varNode(x),where(statementPatternNode(varNode(x), constantNode(a), constantNode(b),1))),
    				where (
    						namedSubQueryInclude("_set1"),
    						statementPatternNode(varNode(x), constantNode(c), varNode(y),1,OPTIONAL),
    						joinGroupNode( statementPatternNode(varNode(w), constantNode(e), varNode(z),10),
    								statementPatternNode(varNode(w), constantNode(d), varNode(x),100),
    								OPTIONAL )
    						), DISTINCT );


    		expected = select( varNodes(x,y,z), 
    				namedSubQuery("_set1",varNode(x),where(statementPatternNode(varNode(x), constantNode(a), constantNode(b),1))),
    				where (
    						namedSubQueryInclude("_set1"),
    						statementPatternNode(varNode(x), constantNode(c), varNode(y),1,OPTIONAL),
    						joinGroupNode( statementPatternNode(varNode(w), constantNode(d), varNode(x),100),
    								statementPatternNode(varNode(w), constantNode(e), varNode(z),10),
    								OPTIONAL )
    						), DISTINCT );

    	}}.test();

    }
    @SuppressWarnings("rawtypes")
    public void test_NSI01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

//        final IV search = makeIV(BDS.SEARCH);
//
//        final IV foo = makeIV(new LiteralImpl("foo"));

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));
        
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
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * ?producer bsbm:country <http://downlode.org/rdf/iso-3166/countries#RU> . # card=2081
   	 * ?product bsbm:producer ?producer . # card=280k
     * ?review bsbm:reviewFor ?product . # card=2.8m
     * ?review rev:reviewer ?reviewer . # card=2.8m
     * ?reviewer bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> . #card=61k
     * ?product a ?productType . # card=10m
     * ?productType a bsbm:ProductType . # card=2011
     *
     * <pre>
     *   select *
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(productType), ConstantNode(rdf:type), ConstantNode(#ProductType)) [2000]
     *     StatementPatternNode(VarNode(product), ConstantNode(rdf:type), VarNode(productType)) [10m]
     *     StatementPatternNode(VarNode(product), ConstantNode(#producer), VarNode(producer)) [280k]
     *     StatementPatternNode(VarNode(producer), ConstantNode(#country), ConstantNode(#RU)) [7000]
     *     StatementPatternNode(VarNode(review), ConstantNode(#reviewFor), VarNode(product)) [2.8m]
     *     StatementPatternNode(VarNode(review), ConstantNode(#reviewer), VarNode(reviewer)) [2.8m]
     *     StatementPatternNode(VarNode(reviewer), ConstantNode(#country), ConstantNode(#US)) [61k]
     *     hint:Prior hint:com.bigdata.rdf.sparql.ast.optimizers.ASTStaticJoinOptimizer.optimistic "false" .
     *   }
     * </pre>
     *
     * Reorder as
     * 
     * <pre>
     *   select *
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(producer), ConstantNode(#country), ConstantNode(#RU)) [7000]
     *     StatementPatternNode(VarNode(product), ConstantNode(#producer), VarNode(producer)) [280k]
     *     StatementPatternNode(VarNode(review), ConstantNode(#reviewFor), VarNode(product)) [2.8m]
     *     StatementPatternNode(VarNode(review), ConstantNode(#reviewer), VarNode(reviewer)) [2.8m]
     *     StatementPatternNode(VarNode(reviewer), ConstantNode(#country), ConstantNode(#US)) [61k]
     *     StatementPatternNode(VarNode(product), ConstantNode(rdf:type), VarNode(productType)) [10m]
     *     StatementPatternNode(VarNode(productType), ConstantNode(rdf:type), ConstantNode(#ProductType)) [2000]
     *   }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_pessimistic() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV country = makeIV(new URIImpl("http://example/country"));
        final IV producer = makeIV(new URIImpl("http://example/producer"));
        final IV reviewFor = makeIV(new URIImpl("http://example/reviewFor"));
        final IV reviewer = makeIV(new URIImpl("http://example/reviewer"));
        final IV RU = makeIV(new URIImpl("http://example/RU"));
        final IV US = makeIV(new URIImpl("http://example/US"));
        final IV type = makeIV(RDF.TYPE);
        final IV productType = makeIV(new URIImpl("http://example/ProductType"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("productType"),
                    new ConstantNode(type), new ConstantNode(productType), 2000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("product"),
                    new ConstantNode(type), new VarNode("productType"), 10000000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("product"),
                    new ConstantNode(producer), new VarNode("producer"), 280000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("producer"),
                    new ConstantNode(country), new ConstantNode(RU), 7000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("review"),
                    new ConstantNode(reviewFor), new VarNode("product"), 2800000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("review"),
                    new ConstantNode(reviewer), new VarNode("reviewer"), 2800000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("reviewer"),
                    new ConstantNode(country), new ConstantNode(US), 61000l));
            
            whereClause.setProperty(ASTStaticJoinOptimizer.Annotations.OPTIMISTIC, 0.67d);

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("producer"),
                    new ConstantNode(country), new ConstantNode(RU), 7000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("product"),
                    new ConstantNode(producer), new VarNode("producer"), 280000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("review"),
                    new ConstantNode(reviewFor), new VarNode("product"), 2800000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("review"),
                    new ConstantNode(reviewer), new VarNode("reviewer"), 2800000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("reviewer"),
                    new ConstantNode(country), new ConstantNode(US), 61000l));
            
            whereClause.addChild(newStatementPatternNode(new VarNode("product"),
                    new ConstantNode(type), new VarNode("productType"), 10000000l));

            whereClause.addChild(newStatementPatternNode(new VarNode("productType"),
                    new ConstantNode(type), new ConstantNode(productType), 2000l));

            whereClause.setProperty(ASTStaticJoinOptimizer.Annotations.OPTIMISTIC, 0.67d);

            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
        assertSameAST(expected, actual);

    }
    
    @SuppressWarnings("rawtypes")
    public void test_runFirstRunLast_01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        final IV f = makeIV(new URIImpl("http://example/f"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(runFirst(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 100l)));

            whereClause.addChild(runLast(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 1l)));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true));

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(runFirst(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 100l)));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(runLast(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 1l)));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true));

            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
        assertSameAST(expected, actual);

    }
    
        
    /*
     * 
     * prefix skos: <http://www.w3.org/2004/02/skos/core#>    
       prefix bds: <http://www.bigdata.com/rdf/search#>
   
select distinct ?o
where {     
   ?o bds:search "viscu*" .
    ?s skos:inScheme <http://syapse.com/vocabularies/fma/anatomical_entity#> .
    ?s skos:prefLabel|skos:altLabel ?o. 
}
     */
    
    public void test_union_trac684_A() {
    	new Helper(){{

    		given = select( varNode(z), // z is ?o
    				      
    				namedSubQuery("_bds",varNode(z),where(statementPatternNode(varNode(z), 
    						constantNode(a), // a is bds:search
    						constantNode(b), // fill in for the literal
    						1))),
    				where (
    						namedSubQueryInclude("_bds"),
    						statementPatternNode(varNode(x), constantNode(c), // inScheme
    								                         constantNode(d), // anatomical_entity
    								                         81053),
    						propertyPathUnionNode(
    						    joinGroupNode( statementPatternNode(varNode(x), constantNode(e), varNode(z),960191) ),

    						    joinGroupNode( statementPatternNode(varNode(x), constantNode(f), varNode(z),615502) ) )
    						),
    				 DISTINCT );
    		
    		
    		expected = select( varNode(z), // z is ?o
				      
				namedSubQuery("_bds",varNode(z),where(statementPatternNode(varNode(z), 
						constantNode(a), // a is bds:search
						constantNode(b), // fill in for the literal
						1))),
				where (
						namedSubQueryInclude("_bds"),
						propertyPathUnionNode(
						    joinGroupNode( statementPatternNode(varNode(x), constantNode(e), varNode(z),960191) ),

						    joinGroupNode( statementPatternNode(varNode(x), constantNode(f), varNode(z),615502) ) ),
						statementPatternNode(varNode(x), constantNode(c), // inScheme
			                         constantNode(d), // anatomical_entity
			                         81053)
						),
				 DISTINCT );
    		
    	}}.test();
    }
    /*
     prefix skos: <http://www.w3.org/2004/02/skos/core#>   
prefix bds: <http://www.bigdata.com/rdf/search#> 
   
select distinct ?o
where {     
    {
       ?s skos:prefLabel ?o .
       ?s skos:inScheme <http://syapse.com/vocabularies/fma/anatomical_entity#> .
    }
    UNION {
       ?s skos:altLabel ?o.     
        ?s skos:inScheme <http://syapse.com/vocabularies/fma/anatomical_entity#> .
    }
   ?o bds:search "viscu*"
}
     */
    
    public void test_union_trac684_B() {
    	new Helper(){{

    		given = select( varNode(z), // z is ?o
    				      
    				namedSubQuery("_bds",varNode(z),where(statementPatternNode(varNode(z), 
    						constantNode(a), // a is bds:search
    						constantNode(b), // fill in for the literal
    						1))),
    				where (
    						namedSubQueryInclude("_bds"),
    						unionNode(
    						    joinGroupNode( 
    		    						statementPatternNode(varNode(x), constantNode(c), // inScheme
						                         constantNode(d), // anatomical_entity
						                         81053),
    						    		statementPatternNode(varNode(x), constantNode(e), varNode(z),960191) 
    						    ),

    						    joinGroupNode( 
    						    		statementPatternNode(varNode(x), constantNode(f), varNode(z),615502),
    		    						statementPatternNode(varNode(x), constantNode(c), // inScheme
						                         constantNode(d), // anatomical_entity
						                         81053)
    						    ) )
    						),
    				 DISTINCT );
    		
    		
    		expected = select( varNode(z), // z is ?o
				      
				namedSubQuery("_bds",varNode(z),where(statementPatternNode(varNode(z), 
						constantNode(a), // a is bds:search
						constantNode(b), // fill in for the literal
						1))),
				where (
						namedSubQueryInclude("_bds"),
						unionNode(
						    joinGroupNode( 
						    		statementPatternNode(varNode(x), constantNode(e), varNode(z),960191),
		    						statementPatternNode(varNode(x), constantNode(c), // inScheme
					                         constantNode(d), // anatomical_entity
					                         81053)
						    ),

						    joinGroupNode( 
						    		statementPatternNode(varNode(x), constantNode(f), varNode(z),615502),
		    						statementPatternNode(varNode(x), constantNode(c), // inScheme
					                         constantNode(d), // anatomical_entity
					                         81053)
						    ) )
						),
				 DISTINCT );
    		
    	}}.test();
    }
    
    /*
prefix skos: <http://www.w3.org/2004/02/skos/core#>   
prefix bds: <http://www.bigdata.com/rdf/search#> 
   
select distinct ?o
where {     
    {
    ?s skos:prefLabel ?o .
    ?s skos:inScheme <http://syapse.com/vocabularies/fma/anatomical_entity#> .
    }
    UNION {
    ?s skos:inScheme <http://syapse.com/vocabularies/fma/anatomical_entity#> .
      ?s skos:altLabel ?o.     
    }
   ?s rdf:type skos:Concept .
   ?o bds:search "viscu*"
}
    */
   
   public void test_union_trac684_C() {
   	new Helper(){{

   		given = select( varNode(z), // z is ?o
   				      
   				namedSubQuery("_bds",varNode(z),where(statementPatternNode(varNode(z), 
   						constantNode(a), // a is bds:search
   						constantNode(b), // fill in for the literal
   						1))),
   				where (
   						namedSubQueryInclude("_bds"),
   						statementPatternNode(varNode(x), constantNode(g), // type
   								constantNode(h), // Concept
   								960191) ,
   						unionNode(
   						    joinGroupNode( 
   		    						statementPatternNode(varNode(x), constantNode(c), // inScheme
						                         constantNode(d), // anatomical_entity
						                         81053),
   						    		statementPatternNode(varNode(x), constantNode(e), varNode(z),960191) 
   						    ),

   						    joinGroupNode( 
   						    		statementPatternNode(varNode(x), constantNode(f), varNode(z),615502),
   		    						statementPatternNode(varNode(x), constantNode(c), // inScheme
						                         constantNode(d), // anatomical_entity
						                         81053)
   						    ) )
   						),
   				 DISTINCT );
   		
   		
   		expected = select( varNode(z), // z is ?o
				      
				namedSubQuery("_bds",varNode(z),where(statementPatternNode(varNode(z), 
						constantNode(a), // a is bds:search
						constantNode(b), // fill in for the literal
						1))),
				where (
						namedSubQueryInclude("_bds"),
						unionNode(
						    joinGroupNode( 
						    		statementPatternNode(varNode(x), constantNode(e), varNode(z),960191),
		    						statementPatternNode(varNode(x), constantNode(c), // inScheme
					                         constantNode(d), // anatomical_entity
					                         81053)
						    ),

						    joinGroupNode( 
						    		statementPatternNode(varNode(x), constantNode(f), varNode(z),615502),
		    						statementPatternNode(varNode(x), constantNode(c), // inScheme
					                         constantNode(d), // anatomical_entity
					                         81053)
						    ) ),

	   					statementPatternNode(varNode(x), constantNode(g), constantNode(h),960191) 
						),
				 DISTINCT );
   		
   	}}.test();
   }
    @SuppressWarnings("rawtypes")
    public void test_runFirstRunLast_02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV b = makeIV(new URIImpl("http://example/b"));
        
        final IV c = makeIV(new URIImpl("http://example/c"));
        
        final IV d = makeIV(new URIImpl("http://example/d"));
        
        final IV e = makeIV(new URIImpl("http://example/e"));
        
        final IV f = makeIV(new URIImpl("http://example/f"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(runFirst(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 100l)));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 1l, true));

            whereClause.addChild(runLast(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true)));

            given.setProjection(projection);
            given.setWhereClause(whereClause);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("x"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();

            whereClause.addChild(runFirst(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(a), new ConstantNode(a), 100l)));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(b), new ConstantNode(b), 2l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(c), new ConstantNode(c), 3l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(d), new ConstantNode(d), 4l));

            whereClause.addChild(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(e), new ConstantNode(e), 1l, true));

            whereClause.addChild(runLast(newStatementPatternNode(new VarNode("x"),
                    new ConstantNode(f), new ConstantNode(f), 1l, true)));

            expected.setProjection(projection);
            expected.setWhereClause(whereClause);
            
        }

        final IASTOptimizer rewriter = new ASTStaticJoinOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();
        
        assertSameAST(expected, actual);

    }
    
    
    private StatementPatternNode runFirst(final StatementPatternNode sp) {
    	sp.setProperty(QueryHints.RUN_FIRST, true);
    	return sp;
    }

    private StatementPatternNode runLast(final StatementPatternNode sp) {
    	sp.setProperty(QueryHints.RUN_LAST, true);
    	return sp;
    }

}
