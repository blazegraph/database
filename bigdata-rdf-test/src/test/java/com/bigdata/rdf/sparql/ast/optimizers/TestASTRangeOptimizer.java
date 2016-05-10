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

import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.RangeNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for {@link ASTRangeOptimizer}.
 */
public class TestASTRangeOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTRangeOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTRangeOptimizer(String name) {
        super(name);
    }

    @SuppressWarnings("rawtypes")
    public void test_SimpleRange() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV p = makeIV(new URIImpl("http://example/p"));
        
        final IV lower = new XSDNumericIV(25);

        final IV upper = new XSDNumericIV(35);
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            { // lower
                final FunctionNode f = 
                        new FunctionNode(FunctionRegistry.GT, null,
                                new ValueExpressionNode[] {
                                    new VarNode("p"),
                                    new ConstantNode(lower)
                        });
                
                where.addChild(new FilterNode(f));
            }
            
            { // upper
                final FunctionNode f = 
                    new FunctionNode(FunctionRegistry.LT, null,
                            new ValueExpressionNode[] {
                                new VarNode("p"),
                                new ConstantNode(upper)
                    });
            
                where.addChild(new FilterNode(f));
            }
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            where.addChild(sp);
            
            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        final AST2BOpContext ctx = new AST2BOpContext(new ASTContainer(given), store);
        
        final GlobalAnnotations globals = new GlobalAnnotations(
        		ctx.getLexiconNamespace(), ctx.getTimestamp());
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            { // lower
                final FunctionNode f = 
                        new FunctionNode(FunctionRegistry.GT, null,
                                new ValueExpressionNode[] {
                                    new VarNode("p"),
                                    new ConstantNode(lower)
                        });
                
                where.addChild(new FilterNode(f));
            }
            
            { // upper
                final FunctionNode f = 
                    new FunctionNode(FunctionRegistry.LT, null,
                            new ValueExpressionNode[] {
                                new VarNode("p"),
                                new ConstantNode(upper)
                    });
            
                where.addChild(new FilterNode(f));
            }
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            final RangeNode range = new RangeNode(
                    new VarNode("p"),
                    new ConstantNode(lower),
                    new ConstantNode(upper)
                    );
            
            final RangeBOp bop = ASTRangeOptimizer.toRangeBOp(getBOpContext(), range, globals);
            
            range.setRangeBOp(bop);
            
            sp.setRange(range);
            
            where.addChild(sp);

            expected.setProjection(projection);
            expected.setWhereClause(where);
            
        }
        
        final IASTOptimizer rewriter = new ASTRangeOptimizer();
        
        final IQueryNode actual = rewriter.optimize(
        		ctx, new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    @SuppressWarnings("rawtypes")
    public void test_SimpleRange_justLower() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV p = makeIV(new URIImpl("http://example/p"));
        
        final IV lower = new XSDNumericIV(25);

//        final IV upper = new XSDNumericIV(35);
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            { // lower
                final FunctionNode f = 
                        new FunctionNode(FunctionRegistry.GT, null,
                                new ValueExpressionNode[] {
                                    new VarNode("p"),
                                    new ConstantNode(lower)
                        });
                
                where.addChild(new FilterNode(f));
            }
            
//            { // upper
//                final FunctionNode f = 
//                    new FunctionNode(FunctionRegistry.LT, null,
//                            new ValueExpressionNode[] {
//                                new VarNode("p"),
//                                new ConstantNode(upper)
//                    });
//            
//                where.addChild(new FilterNode(f));
//            }
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            where.addChild(sp);
            
            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        final AST2BOpContext ctx = new AST2BOpContext(new ASTContainer(given), store);
        
        final GlobalAnnotations globals = new GlobalAnnotations(
        		ctx.getLexiconNamespace(), ctx.getTimestamp());
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            { // lower
                final FunctionNode f = 
                        new FunctionNode(FunctionRegistry.GT, null,
                                new ValueExpressionNode[] {
                                    new VarNode("p"),
                                    new ConstantNode(lower)
                        });
                
                where.addChild(new FilterNode(f));
            }
            
//            { // upper
//                final FunctionNode f = 
//                    new FunctionNode(FunctionRegistry.LT, null,
//                            new ValueExpressionNode[] {
//                                new VarNode("p"),
//                                new ConstantNode(upper)
//                    });
//            
//                where.addChild(new FilterNode(f));
//            }
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            final RangeNode range = new RangeNode(new VarNode("p"));
            range.setFrom(new ConstantNode(lower));
            
            range.setRangeBOp(ASTRangeOptimizer.toRangeBOp(getBOpContext(), range, globals));
            
            sp.setRange(range);
            
            where.addChild(sp);

            expected.setProjection(projection);
            expected.setWhereClause(where);
            
        }
        
        final IASTOptimizer rewriter = new ASTRangeOptimizer();
        
        final IQueryNode actual = rewriter.optimize(
        		ctx, new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    @SuppressWarnings("rawtypes")
    public void test_SimpleRange_justUpper() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV p = makeIV(new URIImpl("http://example/p"));
        
//        final IV lower = new XSDNumericIV(25);

        final IV upper = new XSDNumericIV(35);
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

//            { // lower
//                final FunctionNode f = 
//                        new FunctionNode(FunctionRegistry.GT, null,
//                                new ValueExpressionNode[] {
//                                    new VarNode("p"),
//                                    new ConstantNode(lower)
//                        });
//                
//                where.addChild(new FilterNode(f));
//            }
            
            { // upper
                final FunctionNode f = 
                    new FunctionNode(FunctionRegistry.LT, null,
                            new ValueExpressionNode[] {
                                new VarNode("p"),
                                new ConstantNode(upper)
                    });
            
                where.addChild(new FilterNode(f));
            }
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            where.addChild(sp);
            
            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        final AST2BOpContext ctx = new AST2BOpContext(new ASTContainer(given), store);
        
        final GlobalAnnotations globals = new GlobalAnnotations(
        		ctx.getLexiconNamespace(), ctx.getTimestamp());
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

//            { // lower
//                final FunctionNode f = 
//                        new FunctionNode(FunctionRegistry.GT, null,
//                                new ValueExpressionNode[] {
//                                    new VarNode("p"),
//                                    new ConstantNode(lower)
//                        });
//                
//                where.addChild(new FilterNode(f));
//            }
            
            { // upper
                final FunctionNode f = 
                    new FunctionNode(FunctionRegistry.LT, null,
                            new ValueExpressionNode[] {
                                new VarNode("p"),
                                new ConstantNode(upper)
                    });
            
                where.addChild(new FilterNode(f));
            }
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            final RangeNode range = new RangeNode(new VarNode("p"));
            range.setTo(new ConstantNode(upper));
            range.setRangeBOp(ASTRangeOptimizer.toRangeBOp(getBOpContext(), range, globals));
            
            sp.setRange(range);
            
            where.addChild(sp);

            expected.setProjection(projection);
            expected.setWhereClause(where);
            
        }
        
        final IASTOptimizer rewriter = new ASTRangeOptimizer();
        
        final IQueryNode actual = rewriter.optimize(
        		ctx, new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    @SuppressWarnings("rawtypes")
    public void test_And() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV p = makeIV(new URIImpl("http://example/p"));
        
        final IV lower = new XSDNumericIV(25);

        final IV upper = new XSDNumericIV(35);
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            final FunctionNode f1 = 
                    new FunctionNode(FunctionRegistry.GT, null,
                            new ValueExpressionNode[] {
                                new VarNode("p"),
                                new ConstantNode(lower)
                    });

            final FunctionNode f2 = 
                new FunctionNode(FunctionRegistry.LT, null,
                        new ValueExpressionNode[] {
                            new VarNode("p"),
                            new ConstantNode(upper)
                });
            
            final FunctionNode f = FunctionNode.AND(f1, f2);
                
            where.addChild(new FilterNode(f));
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            where.addChild(sp);
            
            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        final AST2BOpContext ctx = new AST2BOpContext(new ASTContainer(given), store);
        
        final GlobalAnnotations globals = new GlobalAnnotations(
        		ctx.getLexiconNamespace(), ctx.getTimestamp());
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            final FunctionNode f1 = 
                new FunctionNode(FunctionRegistry.GT, null,
                        new ValueExpressionNode[] {
                            new VarNode("p"),
                            new ConstantNode(lower)
                });
	
	        final FunctionNode f2 = 
	            new FunctionNode(FunctionRegistry.LT, null,
	                    new ValueExpressionNode[] {
	                        new VarNode("p"),
	                        new ConstantNode(upper)
	            });
	        
	        final FunctionNode f = FunctionNode.AND(f1, f2);
	            
	        where.addChild(new FilterNode(f));
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            final RangeNode range = new RangeNode(
                    new VarNode("p"),
                    new ConstantNode(lower),
                    new ConstantNode(upper)
                    );
            range.setRangeBOp(ASTRangeOptimizer.toRangeBOp(getBOpContext(), range, globals));
            
            sp.setRange(range);
            
            where.addChild(sp);

            expected.setProjection(projection);
            expected.setWhereClause(where);
            
        }
        
        final IASTOptimizer rewriter = new ASTRangeOptimizer();
        
        final IQueryNode actual = rewriter.optimize(
        		ctx, new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    @SuppressWarnings("rawtypes")
    public void test_And2() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV p = makeIV(new URIImpl("http://example/p"));
        
        final IV lower = new XSDNumericIV(25);

        final IV upper = new XSDNumericIV(35);
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            final FunctionNode f1 = 
                    new FunctionNode(FunctionRegistry.GT, null,
                            new ValueExpressionNode[] {
                                new VarNode("p"),
                                new ConstantNode(lower)
                    });

            final FunctionNode f2 = 
                new FunctionNode(FunctionRegistry.GT, null,
                        new ValueExpressionNode[] {
                            new ConstantNode(upper),
                            new VarNode("p")
                });
            
            final FunctionNode f = FunctionNode.AND(f1, f2);
                
            where.addChild(new FilterNode(f));
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            where.addChild(sp);
            
            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        final AST2BOpContext ctx = new AST2BOpContext(new ASTContainer(given), store);
        
        final GlobalAnnotations globals = new GlobalAnnotations(
        		ctx.getLexiconNamespace(), ctx.getTimestamp());
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            final FunctionNode f1 = 
                new FunctionNode(FunctionRegistry.GT, null,
                        new ValueExpressionNode[] {
                            new VarNode("p"),
                            new ConstantNode(lower)
                });

	        final FunctionNode f2 = 
	            new FunctionNode(FunctionRegistry.GT, null,
	                    new ValueExpressionNode[] {
	                        new ConstantNode(upper),
	                        new VarNode("p")
	            });
	        
	        final FunctionNode f = FunctionNode.AND(f1, f2);
	            
	        where.addChild(new FilterNode(f));
            
            final StatementPatternNode sp = new StatementPatternNode(
                    new VarNode("x"), new ConstantNode(p), new VarNode("p"));
            sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            final RangeNode range = new RangeNode(new VarNode("p"));
            range.setFrom(new ConstantNode(lower));
            range.setTo(new ConstantNode(upper));
            range.setRangeBOp(ASTRangeOptimizer.toRangeBOp(getBOpContext(), range, globals));
            
            sp.setRange(range);
            
            where.addChild(sp);

            expected.setProjection(projection);
            expected.setWhereClause(where);
            
        }
        
        final IASTOptimizer rewriter = new ASTRangeOptimizer();
        
        final IQueryNode actual = rewriter.optimize(
        		ctx, new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * select ?s2
     * where {
     *   <s1> <p> ?o1 .
     *   ?s2  <p> ?o2 .
     *   filter (?o2 > (?o1 - 10) && ?o2 < (?o1 + 10)) .
     * }
     * 
     * Since the range is not complex, it is not attached as RangeBop.
     * See https://jira.blazegraph.com/browse/BLZG-1635.
     */
    @SuppressWarnings("rawtypes")
    public void test_Complex() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV s1 = makeIV(new URIImpl("http://example/s1"));
        final IV p = makeIV(new URIImpl("http://example/p"));
        
        final IV ten = new XSDNumericIV(10);

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            {
            final FunctionNode f1 = 
                    new FunctionNode(FunctionRegistry.GT, null,
                            new ValueExpressionNode[] {
                                new VarNode("o2"),
                                FunctionNode.subtract(new VarNode("o1"), new ConstantNode(ten))
                    });

            final FunctionNode f2 = 
                new FunctionNode(FunctionRegistry.LT, null,
                        new ValueExpressionNode[] {
		                        new VarNode("o2"),
		                        FunctionNode.add(new VarNode("o1"), new ConstantNode(ten))
                });
            
            final FunctionNode f = FunctionNode.AND(f1, f2);
                
            where.addChild(new FilterNode(f));
            }
            
	        {
	        final StatementPatternNode sp = new StatementPatternNode(
	                new ConstantNode(s1), new ConstantNode(p), new VarNode("o1"));

	        where.addChild(sp);
	        }
	        
	        {
	        final StatementPatternNode sp = new StatementPatternNode(
	                new VarNode("s"), new ConstantNode(p), new VarNode("o2"));
        	sp.setQueryHint(QueryHints.RANGE_SAFE, "true");

            where.addChild(sp);
	        }
            
            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        final AST2BOpContext ctx = new AST2BOpContext(new ASTContainer(given), store);
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            expected.setProjection(projection);
            
            final JoinGroupNode where = new JoinGroupNode();
            expected.setWhereClause(where);

//            final FunctionNode sub = FunctionNode.subtract(new VarNode("o1"), new ConstantNode(ten));
//            final FunctionNode add = FunctionNode.add(new VarNode("o1"), new ConstantNode(ten));
            
            {
            final FunctionNode f1 = 
                new FunctionNode(FunctionRegistry.GT, null,
                        new ValueExpressionNode[] {
                            new VarNode("o2"),
                            FunctionNode.subtract(new VarNode("o1"), new ConstantNode(ten))
                });

	        final FunctionNode f2 = 
	            new FunctionNode(FunctionRegistry.LT, null,
	                    new ValueExpressionNode[] {
	                        new VarNode("o2"),
	                        FunctionNode.add(new VarNode("o1"), new ConstantNode(ten))
	            });
	        
	        final FunctionNode f = FunctionNode.AND(f1, f2);
	            
	        where.addChild(new FilterNode(f));
            }
            
	        {
	        final StatementPatternNode sp = new StatementPatternNode(
	                new ConstantNode(s1), new ConstantNode(p), new VarNode("o1"));

	        where.addChild(sp);
	        }
	        
	        {
	        final StatementPatternNode sp = new StatementPatternNode(
	                new VarNode("s"), new ConstantNode(p), new VarNode("o2"));
	        sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
            
            where.addChild(sp);
	        }

            
        }
        
        final IASTOptimizer rewriter = new ASTRangeOptimizer();
        
        final IQueryNode actual = rewriter.optimize(
        		ctx, new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * select ?s2
     * where {
     *   <s1> <p> ?o1 .
     *   <s2> <p> ?o2 .
     *   <s2> <p> ?o3 .
     *   ?s <p> ?o .
     *   filter (?o < ?o1 && ?o < ?o2) .
     *   filter (?o < "100") .
     * }
     * 
     * Only the constant node is attached as range.
     * See https://jira.blazegraph.com/browse/BLZG-1635.
     */
    @SuppressWarnings("rawtypes")
    public void test_Complex2() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV s1 = makeIV(new URIImpl("http://example/s1"));
        final IV s2 = makeIV(new URIImpl("http://example/s2"));
        final IV s3 = makeIV(new URIImpl("http://example/s3"));
        final IV p = makeIV(new URIImpl("http://example/p"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            
            final JoinGroupNode where = new JoinGroupNode();

            { // filters
            	
	            final FunctionNode f1 = 
	                new FunctionNode(FunctionRegistry.LT, null,
	                        new ValueExpressionNode[] {
	                            new VarNode("o"), new VarNode("o1")
	                });
	
	            final FunctionNode f2 = 
	                new FunctionNode(FunctionRegistry.LT, null,
	                        new ValueExpressionNode[] {
	                            new VarNode("o"), new VarNode("o2")
	                });
	
	            final IV upper = new XSDNumericIV(100);
	            final FunctionNode f3 = 
	                new FunctionNode(FunctionRegistry.LT, null,
	                        new ValueExpressionNode[] {
	                            new VarNode("o"), new ConstantNode(upper)
	                });
	
	            final FunctionNode and = FunctionNode.AND(f1, f2);
	                
	            where.addChild(new FilterNode(and));
	            where.addChild(new FilterNode(f3));
            
            }
            
	        { // s1, s2, s3
	        
	        	where.addChild(new StatementPatternNode(
		                new ConstantNode(s1), new ConstantNode(p), new VarNode("o1")));
	        	where.addChild(new StatementPatternNode(
		                new ConstantNode(s2), new ConstantNode(p), new VarNode("o2")));
	        	where.addChild(new StatementPatternNode(
		                new ConstantNode(s3), new ConstantNode(p), new VarNode("o3")));
	        
	        }
	        
	        { // s
	        
	        	final StatementPatternNode sp = new StatementPatternNode(
	        			new VarNode("s"), new ConstantNode(p), new VarNode("o"));
	        	sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
	        	
	        	where.addChild(sp);
	        	
	        }
            
            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        final AST2BOpContext ctx = new AST2BOpContext(new ASTContainer(given), store);
        
        final GlobalAnnotations globals = new GlobalAnnotations(
        		ctx.getLexiconNamespace(), ctx.getTimestamp());
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            expected.setProjection(projection);
            
            final JoinGroupNode where = new JoinGroupNode();
            expected.setWhereClause(where);

            { // filters
            	
	            final FunctionNode f1 = 
	                new FunctionNode(FunctionRegistry.LT, null,
	                        new ValueExpressionNode[] {
	                            new VarNode("o"), new VarNode("o1")
	                });
	
	            final FunctionNode f2 = 
	                new FunctionNode(FunctionRegistry.LT, null,
	                        new ValueExpressionNode[] {
	                            new VarNode("o"), new VarNode("o2")
	                });
	
	            final IV upper = new XSDNumericIV(100);
	            final FunctionNode f3 = 
	                new FunctionNode(FunctionRegistry.LT, null,
	                        new ValueExpressionNode[] {
	                            new VarNode("o"), new ConstantNode(upper)
	                });
	
	            final FunctionNode and = FunctionNode.AND(f1, f2);
	                
	            where.addChild(new FilterNode(and));
	            where.addChild(new FilterNode(f3));
            
            }
            
        	{ // s1

        		final StatementPatternNode sp = new StatementPatternNode(
		                new ConstantNode(s1), new ConstantNode(p), new VarNode("o1"));
        		
	        	where.addChild(sp);

        	}

        	{ // s2

        		final StatementPatternNode sp = new StatementPatternNode(
		                new ConstantNode(s2), new ConstantNode(p), new VarNode("o2"));
        		
	        	where.addChild(sp);

        	}
        	
        	{ // s3

        		final StatementPatternNode sp = new StatementPatternNode(
		                new ConstantNode(s3), new ConstantNode(p), new VarNode("o3"));
        		        		
	        	where.addChild(sp);

        	}
	        
	        { // s
	        
	        	final StatementPatternNode sp = new StatementPatternNode(
	        			new VarNode("s"), new ConstantNode(p), new VarNode("o"));
	        	sp.setQueryHint(QueryHints.RANGE_SAFE, "true");
	        	

	        	final IV upper = new XSDNumericIV(100);
	            final RangeNode range = new RangeNode(new VarNode("o"));
	            range.setTo(new ConstantNode(upper)); 
	            range.setRangeBOp(ASTRangeOptimizer.toRangeBOp(getBOpContext(), range, globals));
	            
	            sp.setRange(range);
	            
	        	where.addChild(sp);
	        	
	        }
	        
        }
        
        final IASTOptimizer rewriter = new ASTRangeOptimizer();
        
        final IQueryNode actual = rewriter.optimize(
        		ctx, new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

}
