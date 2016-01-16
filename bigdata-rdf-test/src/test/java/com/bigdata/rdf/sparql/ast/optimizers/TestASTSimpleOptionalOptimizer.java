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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.eval.ASTDeferredIVResolution;

/**
 * Test suite for {@link ASTSimpleOptionalOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTSimpleOptionalOptimizer extends
        AbstractASTEvaluationTestCase {

    public TestASTSimpleOptionalOptimizer() {
        super();
    }

    public TestASTSimpleOptionalOptimizer(final String name) {
        super(name);
    }

    /**
     * Unit test for recognizing a "simple optional" and lifting it into the
     * parent join group.
     * 
     * TODO Unit test for a variation where there are FILTERS or other things in
     * the optional which mean that we can not lift out the statement pattern.
     */
    public void test_simpleOptional() throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+//
                "PREFIX dc: <http://purl.org/dc/terms/> \n"+//
                "PREFIX p1: <http://www.bigdata.com/> \n"+//
                "SELECT * \n" + //
                "WHERE { \n" + //
                "  ?_var1 rdf:type <http://suawa.org/mediadb#Album>. \n" + //
                "  ?_var1 p1:genre ?_var8.  \n" + //
                "  ?_var8 dc:title ?_var9.  \n" + //
                "  FILTER ((?_var9 in(\"Folk\", \"Hip-Hop\"))) . \n" + //
                "  OPTIONAL { \n" + //
                "    ?_var1 dc:title ?_var10 \n" + //
                "  }.  \n" + //
                "  OPTIONAL { \n" + //
                "    ?_var1 p1:mainArtist ?_var12. \n" + //
                "    ?_var12 dc:title ?_var11 \n" + //
                "  } \n" + //
                "}";

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer,store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null))
                .getQueryNode();

        queryRoot = (QueryRoot) new ASTSimpleOptionalOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null))
                .getQueryNode();

        final GraphPatternGroup<?> whereClause = queryRoot.getWhereClause();

        // Verify that we lifted out the simple optional statement pattern.
        {
            int nstmts = 0;
            for(IGroupMemberNode child : whereClause) {
                if(child instanceof StatementPatternNode) {
                    nstmts++;
                }
            }
            assertEquals("#statements", 4, nstmts);
        }
        
        /*
         * Verify that there is one optional remaining, and that it is the one
         * with two statement patterns.
         */
        {
            final Iterator<JoinGroupNode> itr = BOpUtility.visitAll(
                    whereClause, JoinGroupNode.class);
            int ngroups = 0;
            int noptionalGroups = 0;
            while (itr.hasNext()) {
                final JoinGroupNode tmp = itr.next();
                ngroups++;
                if(tmp.isOptional())
                    noptionalGroups++;
            }
            assertEquals("#ngroups", 2, ngroups);
            assertEquals("#optionalGroups", 1, noptionalGroups);
        }

    }

    /**
     * Test effective boolean value - optional.
     * 
     * <pre>
     * PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>
     * PREFIX  : <http://example.org/ns#>
     * SELECT  ?a
     * WHERE
     *     { ?a :p ?v . 
     *       OPTIONAL
     *         { ?a :q ?w } . 
     *       FILTER (?w) .
     *     }
     * </pre>
     * 
     * Note: This TCK query will fail if we do not lift the simple optional
     * correctly.
     * 
     * @see ASTSimpleOptionalOptimizer
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_sparql_bev_5() throws Exception {

        final String queryStr = "" + //
                "PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>\n" + //
                "PREFIX  : <http://example.org/ns#>" + //
                "SELECT ?a \n" + //
                "WHERE" + //
                "    { ?a :p ?v . \n" + //
                "      OPTIONAL \n" + //
                "        { ?a :q ?w } . \n" + //
                "      FILTER (?w) . \n" + //
                "    }\n" //
        ;

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI p = f.createURI("http://example.org/ns#p");
        final BigdataURI q = f.createURI("http://example.org/ns#q");
        final BigdataValue[] values = new BigdataValue[] { p, q };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        ASTDeferredIVResolution.resolveQuery(store, astContainer);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
    
        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTSimpleOptionalOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null))
                .getQueryNode();

        /*
         * Create the expected AST.
         */
        final JoinGroupNode expectedClause = new JoinGroupNode();
        {

            // :x3 :q ?w
            expectedClause.addChild(new StatementPatternNode(//
                    new VarNode("a"),// s
                    new ConstantNode(new Constant(p.getIV())),// p
                    new VarNode("v"),// o
                    null,// c
                    Scope.DEFAULT_CONTEXTS//
                    ));
        
            // :x3 :q ?w
            final StatementPatternNode liftedSp = new StatementPatternNode(//
                    new VarNode("a"),// s
                    new ConstantNode(new Constant(q.getIV())),// p
                    new VarNode("w"),// o
                    null,// c
                    Scope.DEFAULT_CONTEXTS//
                    );

            expectedClause.addChild(liftedSp);

            liftedSp.setOptional(true);

            // The filter stays in place (it is in the outer group).
            expectedClause.addChild(new FilterNode(new VarNode("w")));

        }
        
        assertSameAST(expectedClause, queryRoot.getWhereClause());        

    }
    
    /**
     * A variant of the TCK test where the filter is in the optional group and
     * uses BOUND() to wrap the variable. This filter does not have any
     * materialization requirements. Both it and the statement pattern should be
     * lifted out of the optional group. The filter should be attached to the
     * statement pattern.
     * 
     * @throws Exception
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_sparql_bev_5_withFilterInOptionalGroup() throws Exception {

        final String queryStr = "" + //
                "PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>\n" + //
                "PREFIX  : <http://example.org/ns#>" + //
                "SELECT ?a \n" + //
                "WHERE" + //
                "    { ?a :p ?v . \n" + //
                "      OPTIONAL \n" + //
                "        { ?a :q ?w ." +
                "          FILTER (BOUND(?w)) \n" +
                "        } \n" + //
                "    }\n" //
        ;

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI p = f.createURI("http://example.org/ns#p");
        final BigdataURI q = f.createURI("http://example.org/ns#q");
        final BigdataValue[] values = new BigdataValue[] { p, q };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        ASTDeferredIVResolution.resolveQuery(store, astContainer);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        queryRoot = BOpUtility.deepCopy(queryRoot);
        
        queryRoot = (QueryRoot) new ASTSimpleOptionalOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        /*
         * Create the expected AST.
         */
        final JoinGroupNode expectedClause = new JoinGroupNode();
        {

            // :x3 :q ?w
            expectedClause.addChild(new StatementPatternNode(//
                    new VarNode("a"),// s
                    new ConstantNode(new Constant(p.getIV())),// p
                    new VarNode("v"),// o
                    null,// c
                    Scope.DEFAULT_CONTEXTS//
                    ));
        
            // :x3 :q ?w
            final StatementPatternNode liftedSp = new StatementPatternNode(//
                    new VarNode("a"),// s
                    new ConstantNode(new Constant(q.getIV())),// p
                    new VarNode("w"),// o
                    null,// c
                    Scope.DEFAULT_CONTEXTS//
                    );

            expectedClause.addChild(liftedSp);

            liftedSp.setOptional(true);
            
            final List<FilterNode> filters = new LinkedList<FilterNode>();
            
            final FilterNode filterNode = new FilterNode(//
                    new FunctionNode(FunctionRegistry.BOUND, null,// scalarValues
                            new ValueExpressionNode[] {//
                            new VarNode("w")//
                            }//
                    ));
            
            final GlobalAnnotations globals = new GlobalAnnotations(
            		context.getLexiconNamespace(),
            		context.getTimestamp()
            		);
            
            AST2BOpUtility.toVE(getBOpContext(), globals,
                    filterNode.getValueExpressionNode());
            
            filters.add(filterNode);
            
            liftedSp.setAttachedJoinFilters(filters);

        }
        
        assertSameAST(expectedClause, queryRoot.getWhereClause());        

    }

    /*
     * Note: I have taken out the code path since we are not trying to lift the
     * filter in this case.
     */
//    /**
//     * A variant of the TCK test where the filter is in the optional group and
//     * uses a variable which is "incoming bound" to the optional group (hence
//     * definitely bound in the parent group).
//     * <p>
//     * This filter does not have any materialization requirements on variables
//     * bound by the optional statement pattern. Both it and the statement
//     * pattern should be lifted out of the optional group. The filter should be
//     * attached to the statement pattern.
//     * 
//     * @see TestTCK#test_opt_filter_1(), which will fail if the materialization
//     *      requirements are not properly imposed on the parent.
//     * 
//     * @throws Exception
//     */
//    @SuppressWarnings({ "rawtypes", "unchecked" })
//    public void test_sparql_bev_5_withFilterInOptionalGroup2() throws Exception {
//
//        final String queryStr = "" + //
//                "PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>\n" + //
//                "PREFIX  : <http://example.org/ns#>" + //
//                "SELECT ?a \n" + //
//                "WHERE" + //
//                "    { ?a :p ?v . \n" + //
//                "      OPTIONAL \n" + //
//                "        { ?a :q ?w ." +
//                "          FILTER (?v) \n" +
//                "        } \n" + //
//                "    }\n" //
//        ;
//
//        /*
//         * Add the Values used in the query to the lexicon. This makes it
//         * possible for us to explicitly construct the expected AST and
//         * the verify it using equals().
//         */
//        final BigdataValueFactory f = store.getValueFactory();
//        final BigdataURI p = f.createURI("http://example.org/ns#p");
//        final BigdataURI q = f.createURI("http://example.org/ns#q");
//        final BigdataValue[] values = new BigdataValue[] { p, q };
//        store.getLexiconRelation()
//                .addTerms(values, values.length, false/* readOnly */);
//
//        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
//                .parseQuery2(queryStr, baseURI);
//
//        final AST2BOpContext context = new AST2BOpContext(astContainer, store);
//
//        QueryRoot queryRoot = astContainer.getOriginalAST();
//        
//        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
//                context, queryRoot, null/* bindingSets */);
//
//        queryRoot = BOpUtility.deepCopy(queryRoot);
//        
//        queryRoot = (QueryRoot) new ASTSimpleOptionalOptimizer().optimize(
//                context, queryRoot, null/* bindingSets */);
//
//        /*
//         * Create the expected AST.
//         */
//        final JoinGroupNode expectedClause = new JoinGroupNode();
//        {
//
//            // :x3 :q ?w
//            expectedClause.addChild(new StatementPatternNode(//
//                    new VarNode("a"),// s
//                    new ConstantNode(new Constant(p.getIV())),// p
//                    new VarNode("v"),// o
//                    null,// c
//                    Scope.DEFAULT_CONTEXTS//
//                    ));
//        
//            // :x3 :q ?w
//            final StatementPatternNode liftedSp = new StatementPatternNode(//
//                    new VarNode("a"),// s
//                    new ConstantNode(new Constant(q.getIV())),// p
//                    new VarNode("w"),// o
//                    null,// c
//                    Scope.DEFAULT_CONTEXTS//
//                    );
//
//            expectedClause.addChild(liftedSp);
//
//            liftedSp.setSimpleOptional(true);
//            
//            final List<FilterNode> filters = new LinkedList<FilterNode>();
//            
//            final FilterNode filterNode = new FilterNode(new VarNode("v"));
//
////            AST2BOpUtility.toVE(context.getLexiconNamespace(),
////                    filterNode.getValueExpressionNode());
//            
//            filters.add(filterNode);
//            
//            liftedSp.setFilters(filters);
//
//            /*
//             * The mock filter is used to impose the appropriate materialization
//             * requirements onto the parent.
//             */
//            
//            expectedClause.addChild(new MockFilterNode(filterNode
//                    .getValueExpressionNode(), filterNode
//                    .getMaterializationRequirement()));
//
//        }
//
//        /*
//         * Note: This is failing because I am having difficulties getting the
//         * materialization pipeline to work correctly when a statement pattern
//         * with a filter having some materialization requirements is lifted into
//         * the parent group.  See the code in ASTSimpleOptionalOptimizer.
//         */
//        assertSameAST(expectedClause, queryRoot.getWhereClause());        
//
//    }
    
}
