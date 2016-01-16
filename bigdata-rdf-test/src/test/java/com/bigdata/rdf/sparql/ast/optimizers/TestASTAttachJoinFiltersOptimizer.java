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

import java.util.Collections;
import java.util.List;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.BSBMQ5Setup;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase.Annotations;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for {@link ASTAttachJoinFiltersOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTAttachJoinFiltersOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTAttachJoinFiltersOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTAttachJoinFiltersOptimizer(String name) {
        super(name);
    }

    /**
     * Unit test for the attachment of the join filters to the required joins in
     * a {@link JoinGroupNode}.
     * <p>
     * Note: The core logic for deciding join filter attachment is tested
     * elsewhere. This is only testing the attachment once those decisions are
     * made.
     */
    @SuppressWarnings("unchecked")
    public void test_attachFilters() {

      /*
      * Note: DO NOT share structures in this test!!!!
      */
     final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given;
        {

            final BSBMQ5Setup s = new BSBMQ5Setup(store);
            
            given = s.queryRoot;

            /**
             * Put the joins into a known order whose correct filter attachments
             * are also known. This order is <code>[5, 3, 1, 0, 2, 4, 6]</code>.
             */
            final GraphPatternGroup<IGroupMemberNode> whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);
            
            // the required joins in a known order.
            whereClause.addChild(s.p5);
            whereClause.addChild(s.p3);
            whereClause.addChild(s.p1);
            whereClause.addChild(s.p0);
            whereClause.addChild(s.p2);
            whereClause.addChild(s.p4);
            whereClause.addChild(s.p6);
            
            // now add in the filters.
            whereClause.addChild(s.c0);
            whereClause.addChild(s.c1);
            whereClause.addChild(s.c2);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected;
        {

            final BSBMQ5Setup s = new BSBMQ5Setup(store);
            
            expected = s.queryRoot;

            /*
             * Build up the join group. The joins will appear in the same order
             * but all of the filters will have been attached to joins.
             */
            final GraphPatternGroup<IGroupMemberNode> whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            
            // the required joins in the same order.
            whereClause.addChild(s.p5);
            whereClause.addChild(s.p3);
            whereClause.addChild(s.p1);
            whereClause.addChild(s.p0);
            whereClause.addChild(s.p2);
            whereClause.addChild(s.p4);
            whereClause.addChild(s.p6);
            
            // Clear the parent references on the filters.
            s.c0.setParent(null);
            s.c1.setParent(null);
            s.c2.setParent(null);
            
            // now attach the filters to the joins.
            s.p0.setAttachedJoinFilters(Collections.singletonList(s.c0));
            s.p4.setAttachedJoinFilters(Collections.singletonList(s.c1));
            s.p6.setAttachedJoinFilters(Collections.singletonList(s.c2));
            
        }

        final IASTOptimizer rewriter = new ASTAttachJoinFiltersOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

        /*
         * Verify no change if we feed the output back into the input. This
         * tests the ability to pick up the attached join filters for
         * consideration the next time around.
         * 
         * TODO Actually, the best test would be if we changed the join order
         * as well.
         */
        final IQueryNode actual2 = rewriter.optimize(context,
              new QueryNodeWithBindingSet(BOpUtility.deepCopy(expected), bsets))
                .getQueryNode();

        assertSameAST(expected, actual2);
        
    }

    
    
    /**
     * Assert proper handling of redundant (identical) filter expressions.
     */
    @SuppressWarnings("unchecked")
	public void test_redundantFilter() {

        final String sampleInstance = "http://www.example.com/I";
        final BigdataURI someUri = valueFactory.createURI(sampleInstance);
        final BigdataValue[] terms = new BigdataValue[] { someUri };

        // resolve terms.
        store.getLexiconRelation()
                .addTerms(terms, terms.length, false/* readOnly */);

        for (BigdataValue bv : terms) {
            // Cache the Value on the IV.
            bv.getIV().setValue(bv);
        }
        
        
    	// The source AST:
        /*
         * QueryType: SELECT
         * SELECT VarNode(s)
         *   JoinGroupNode {
         *     QueryType: SELECT
         *     SELECT VarNode(s)
         *       JoinGroupNode {
         *         StatementPatternNode(VarNode(s), VarNode(p), VarNode(o)) [scope=DEFAULT_CONTEXTS]
         *     }
         *     FILTER( FunctionNode(VarNode(s),VarNode(s))[FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2005/xpath-functions#not-equal-to, valueExpr=com.bigdata.rdf.internal.constraints.CompareBOp(s,TermId(0U)[eg:b])[ CompareBOp.op=NE]] )
         *     FILTER( FunctionNode(VarNode(s),VarNode(s))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2005/xpath-functions#not-equal-to, valueExpr=com.bigdata.rdf.internal.constraints.CompareBOp(s,TermId(0U)[eg:b])[ CompareBOp.op=NE]] )
         *   }
         */
        final QueryRoot qUnoptimized = new QueryRoot(QueryType.SELECT);
        {
        	VarNode varS = new VarNode("s");
        	VarNode varP = new VarNode("p");
        	VarNode varO = new VarNode("o");
        	
        	///////////////////////////////////// STEP 1: build inner subquery  	
        	// build innermost join
        	JoinGroupNode innerJoin = new JoinGroupNode();
        	innerJoin.addArg(
        		new StatementPatternNode(varS, varP, varO));
        	
        	// build inner projection
        	ProjectionNode innerProjection = new ProjectionNode();
        	innerProjection.addProjectionVar(new VarNode("s"));
        	
        	// put it together to inner query
        	final SubqueryRoot qUnoptimizedInner = new SubqueryRoot(QueryType.SELECT);
        	qUnoptimizedInner.setProjection(innerProjection);
        	qUnoptimizedInner.setWhereClause(innerJoin);
        	
        	///////////////////////////////////// STEP 1: build outer query
        	// build filter expressions
        	FilterNode f1 = new FilterNode(FunctionNode.NE(
        		new ConstantNode(someUri.getIV()), varS));        	
        	
        	FilterNode f2 = new FilterNode(FunctionNode.NE(
            		new ConstantNode(someUri.getIV()), varS));  
        	
        	// build outer join
        	JoinGroupNode outerJoin = new JoinGroupNode();
        	outerJoin.addArg(qUnoptimizedInner);
        	outerJoin.addArg(f1);
        	outerJoin.addArg(f2);
        	
        	// build top-level projection
        	ProjectionNode projection = new ProjectionNode();
        	projection.addProjectionVar(varS);

        	// compose final query
        	qUnoptimized.setWhereClause(outerJoin);
        	qUnoptimized.setProjection(projection);
        }
        System.out.println(qUnoptimized);


        // we need to apply the value exp rewriter to calculate value exps
        final ASTSetValueExpressionsOptimizer valueExpRewriter = 
        	new ASTSetValueExpressionsOptimizer();
        final AST2BOpContext contextValueExpRewriter = 
            	new AST2BOpContext(new ASTContainer(qUnoptimized), store);
        final IBindingSet[] bsetsValueExpRewriter = new IBindingSet[]{};
        
        final IQueryNode qIntermediate = 
        	valueExpRewriter.optimize(
        		contextValueExpRewriter, 
        		new QueryNodeWithBindingSet(qUnoptimized, bsetsValueExpRewriter))
        		.getQueryNode();
        
        // the join rewriter is what we want to test
        final IASTOptimizer joinRewriter = 
        	new ASTAttachJoinFiltersOptimizer();      
        
        final AST2BOpContext contextJoinRewriter = 
            	new AST2BOpContext(new ASTContainer(
            		(QueryRoot)qIntermediate), store);
        final IBindingSet[] bsetsJoinRewriter = new IBindingSet[]{};

        
        final IQueryNode qOptimized = joinRewriter.optimize(
        	contextJoinRewriter,
        	new QueryNodeWithBindingSet(qIntermediate, bsetsJoinRewriter))
        	.getQueryNode();
        
        /** This is the output we expect
         * QueryType: SELECT
         * SELECT VarNode(s)
         *   JoinGroupNode {
         *     QueryType: SELECT
         *     SELECT VarNode(s)
         *       JoinGroupNode {
         *         StatementPatternNode(VarNode(s), VarNode(p), VarNode(o)) [scope=DEFAULT_CONTEXTS]
         *       }
         *       FILTER( FunctionNode(ConstantNode(TermId(1U)[http://www.example.com/I]),VarNode(s))[ FunctionNode.scalarVals=null, FunctionNode.functionURI=http://www.w3.org/2005/xpath-functions#not-equal-to, valueExpr=com.bigdata.rdf.internal.constraints.CompareBOp(TermId(1U)[http://www.example.com/I],s)[ CompareBOp.op=NE]] )
         *   }
         */
        
        // the optimized query contains no more filter at top-level, but one
        // filter nested inside the SELECT clause (duplicate has been removed)
        assertEquals(
        	QueryType.SELECT,
        	qOptimized.annotations().get(Annotations.QUERY_TYPE));
        JoinGroupNode topLevelJoin = 
        	(JoinGroupNode)qOptimized.annotations().get(Annotations.GRAPH_PATTERN);
        List<IGroupMemberNode> topLevelJoinChildren = topLevelJoin.getChildren();
        
        // the top level join has only one subquery, namely the select; filters
        // have been pushed inside
        assertEquals(topLevelJoinChildren.size(),1);
        SubqueryRoot innerSelect = (SubqueryRoot)topLevelJoinChildren.get(0);
        assertEquals(
        	QueryType.SELECT,
        	innerSelect.annotations().get(Annotations.QUERY_TYPE));
        
        JoinGroupNode innerJoin =
        	(JoinGroupNode)innerSelect.annotations().get(Annotations.GRAPH_PATTERN);
        assertNotNull(innerJoin);
        List<FilterNode> filters =
            	(List<FilterNode>)innerSelect.annotations().get("filters");
        assertEquals(filters.size(),1);
    }
}
