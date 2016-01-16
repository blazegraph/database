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
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ASTUnionFiltersOptimizer}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id: TestASTEmptyGroupOptimizer.java 5302 2011-10-07 14:28:03Z
 *          thompsonbry $
 */
public class TestASTUnionFiltersOptimizer extends AbstractOptimizerTestCase {

    /**
     * 
     */
    public TestASTUnionFiltersOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTUnionFiltersOptimizer(String name) {
        super(name);
    }

    /**
     * https://sourceforge.net/apps/trac/bigdata/ticket/416
     */
    @SuppressWarnings("rawtypes")
    public void test_ticket416() throws Exception {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};
        
        final IV type = makeIV(RDF.TYPE);
        
        final IV a = makeIV(new URIImpl("http://example/a"));

        final IV t1 = makeIV(new URIImpl("http://example/t1"));

        final IV t2 = makeIV(new URIImpl("http://example/t2"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("p"));
            projection.addProjectionVar(new VarNode("r"));
            projection.addProjectionVar(new VarNode("l"));
            projection.setDistinct(true);
            
            final UnionNode union = new UnionNode(); // outer
            
            final JoinGroupNode left = new JoinGroupNode();
            left.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new ConstantNode(type),
            		new ConstantNode(a)));
            left.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new VarNode("p"),
            		new VarNode("r")));
            left.addChild(new StatementPatternNode(
            		new VarNode("r"),
            		new ConstantNode(type),
            		new VarNode("type")));
            
            final JoinGroupNode right = new JoinGroupNode();
            right.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new ConstantNode(type),
            		new ConstantNode(a)));
            right.addChild(new StatementPatternNode(
            		new VarNode("l"),
            		new VarNode("p"),
            		new VarNode("s")));
            right.addChild(new StatementPatternNode(
            		new VarNode("l"),
            		new ConstantNode(type),
            		new VarNode("type")));

            union.addChild(left);
            union.addChild(right);
            
            final JoinGroupNode where = new JoinGroupNode();
            
            where.addChild(union);
            
            where.addChild(new FilterNode(new FilterNode(new FunctionNode(
            		FunctionRegistry.OR, 
            		null/*scalarValues*/, 
            		new ValueExpressionNode[] { 
            				new FunctionNode(
            	            		FunctionRegistry.SAME_TERM, 
            	            		null/*scalarValues*/, 
            	            		new ValueExpressionNode[] { 
            	            				new VarNode("type"),
            	            				new ConstantNode(t1)
            	            		}),
    	            		new FunctionNode(
    	                    		FunctionRegistry.SAME_TERM, 
    	                    		null/*scalarValues*/, 
    	                    		new ValueExpressionNode[] { 
    	                    				new VarNode("type"),
    	                    				new ConstantNode(t2)
    	                    		})
            		}))));

            given.setProjection(projection);
            given.setWhereClause(where);
            
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("p"));
            projection.addProjectionVar(new VarNode("r"));
            projection.addProjectionVar(new VarNode("l"));
            projection.setDistinct(true);
            
            final UnionNode union = new UnionNode(); // outer
            
            final JoinGroupNode left = new JoinGroupNode();
            left.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new ConstantNode(type),
            		new ConstantNode(a)));
            left.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new VarNode("p"),
            		new VarNode("r")));
            left.addChild(new StatementPatternNode(
            		new VarNode("r"),
            		new ConstantNode(type),
            		new VarNode("type")));
            left.addChild(new FilterNode(new FunctionNode(
            		FunctionRegistry.OR, 
            		null/*scalarValues*/, 
            		new ValueExpressionNode[] { 
            				new FunctionNode(
            	            		FunctionRegistry.SAME_TERM, 
            	            		null/*scalarValues*/, 
            	            		new ValueExpressionNode[] { 
            	            				new VarNode("type"),
            	            				new ConstantNode(t1)
            	            		}),
    	            		new FunctionNode(
    	                    		FunctionRegistry.SAME_TERM, 
    	                    		null/*scalarValues*/, 
    	                    		new ValueExpressionNode[] { 
    	                    				new VarNode("type"),
    	                    				new ConstantNode(t2)
    	                    		})
            		})));
            
            final JoinGroupNode right = new JoinGroupNode();
            right.addChild(new StatementPatternNode(
            		new VarNode("s"),
            		new ConstantNode(type),
            		new ConstantNode(a)));
            right.addChild(new StatementPatternNode(
            		new VarNode("l"),
            		new VarNode("p"),
            		new VarNode("s")));
            right.addChild(new StatementPatternNode(
            		new VarNode("l"),
            		new ConstantNode(type),
            		new VarNode("type")));
            right.addChild(new FilterNode(new FunctionNode(
            		FunctionRegistry.OR, 
            		null/*scalarValues*/, 
            		new ValueExpressionNode[] { 
            				new FunctionNode(
            	            		FunctionRegistry.SAME_TERM, 
            	            		null/*scalarValues*/, 
            	            		new ValueExpressionNode[] { 
            	            				new VarNode("type"),
            	            				new ConstantNode(t1)
            	            		}),
    	            		new FunctionNode(
    	                    		FunctionRegistry.SAME_TERM, 
    	                    		null/*scalarValues*/, 
    	                    		new ValueExpressionNode[] { 
    	                    				new VarNode("type"),
    	                    				new ConstantNode(t2)
    	                    		})
            		})));

            union.addChild(left);
            union.addChild(right);
            
            expected.setProjection(projection);
            expected.setWhereClause(union);

        }

        final IASTOptimizer opt1 = new ASTUnionFiltersOptimizer();
        
        final IASTOptimizer opt2 = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = opt2.optimize(
        		null/* AST2BOpContext */,
        		new QueryNodeWithBindingSet(
        		      opt1.optimize(null/* AST2BOpContext */, new QueryNodeWithBindingSet(given, bsets)).getQueryNode(),
        		      bsets)
        		).getQueryNode();
        assertSameAST(expected, actual);

    }

	@Override
	IASTOptimizer newOptimizer() {
		return new ASTOptimizerList(new ASTUnionFiltersOptimizer(), new ASTBottomUpOptimizer());
	}
	
	public void test_ticket767_case2() {
		new Helper(){{
			given = select( varNode(w), 
					where ( joinGroupNode(
							    unionNode(
									joinGroupNode(
											statementPatternNode(constantNode(a),constantNode(b),varNode(w))
											),
											joinGroupNode()
									),
							    filter(bound(varNode(w)))
							) ) );
			
			expected = select( varNode(w), 
					where (joinGroupNode(
							unionNode(
								joinGroupNode(
											statementPatternNode(constantNode(a),constantNode(b),varNode(w)),
											filter(bound(varNode(w)))),
								joinGroupNode(filter(knownUnbound(varNode(w)))) 
								)
							) ) );
			
		}}.testWhileIgnoringExplainHints();
	}

	/**
	 * This optimizer cannot help in this case.
	 */
	public void test_ticket905() {
		new Helper(){
			QueryRoot unchanged() {
				return select( varNode(w), 
					where ( joinGroupNode(
							    statementPatternNode(constantNode(a),constantNode(b),varNode(w)),
							    filter(bound(varNode(w))),
							    statementPatternNode(varNode(x),constantNode(b),varNode(w)),
							    unionNode(
									joinGroupNode(
			    						    bind(constantNode(a), varNode(x) )
											),
									joinGroupNode(
					    					bind(constantNode(b), varNode(x) )
											)
									)
							) ) );
			}
			{
			given = unchanged();
			
			expected = unchanged();
			
		}}.test();
	}

}
