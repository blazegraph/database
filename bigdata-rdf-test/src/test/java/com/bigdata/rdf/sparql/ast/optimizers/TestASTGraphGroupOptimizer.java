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
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.eval.ASTDeferredIVResolution;
import com.bigdata.rdf.sparql.ast.eval.TestNamedGraphs;

/**
 * Test suite for {@link ASTGraphGroupOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestASTGraphGroupOptimizer.java 5701 2011-11-20 00:52:36Z
 *          thompsonbry $
 * 
 * @see TestNamedGraphs
 * 
 *      FIXME Unit test where the outer graph group has a variable and the inner
 *      graph group has a constant.
 * 
 *      FIXME Unit test where the outer graph group has a constant and the inner
 *      graph group has a variable.
 */
public class TestASTGraphGroupOptimizer extends
        AbstractASTEvaluationTestCase {

    public TestASTGraphGroupOptimizer() {
        super();
    }

    public TestASTGraphGroupOptimizer(final String name) {
        super(name);
    }

    /**
     * Unit test where nested GRAPH patterns share the same variable.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT ?s
     * WHERE {
     *   GRAPH ?g {
     *     ?s :p :o .
     *     OPTIONAL { ?g :p2 ?s }
     *   }
     * }
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_graphGroupOptimizer_01() throws MalformedQueryException {
        
        final String queryStr = ""//
                + "PREFIX : <http://example.org/>\n"//
                + "SELECT ?s\n"//
                + "WHERE {\n"//
                + "  GRAPH ?g {\n"//
                + "    ?s :p :o .\n"//
                + "    OPTIONAL { ?g :p2 ?s }\n"//
                + "  }\n"//
                + "}";

        /*
         * Add the Values used in the query to the lexicon. This makes it
         * possible for us to explicitly construct the expected AST and
         * the verify it using equals().
         */
        final BigdataValueFactory f = store.getValueFactory();
        final BigdataURI p = f.createURI("http://example.org/p");
        final BigdataURI p2= f.createURI("http://example.org/p2");
        final BigdataURI o = f.createURI("http://example.org/o");
        final BigdataValue[] values = new BigdataValue[] { p, p2, o };
        store.getLexiconRelation()
                .addTerms(values, values.length, false/* readOnly */);

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        ASTDeferredIVResolution.resolveQuery(store, astContainer);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).
                getQueryNode();

        queryRoot = BOpUtility.deepCopy(queryRoot);
        
        queryRoot = (QueryRoot) new ASTGraphGroupOptimizer().optimize(
              context, new QueryNodeWithBindingSet(queryRoot, null)).
              getQueryNode();

        /*
         * Create the expected AST.
         */
        final JoinGroupNode expectedClause = new JoinGroupNode();
        {

            final VarNode g = new VarNode("g");
            final VarNode s = new VarNode("s");
            
            final JoinGroupNode graphGroup = new JoinGroupNode();
            graphGroup.setContext(g);
            expectedClause.addChild(graphGroup);

            // ?s :p :o
            graphGroup.addChild(new StatementPatternNode(//
                    s,// s
                    new ConstantNode(new Constant(p.getIV())),// p
                    new ConstantNode(new Constant(o.getIV())),// o
                    g,// c
                    Scope.NAMED_CONTEXTS//
                    ));
        
            final JoinGroupNode optionalGroup = new JoinGroupNode(true/*optional*/);
            graphGroup.addChild(optionalGroup);
            
            // ?g :p2 ?s
            optionalGroup.addChild(new StatementPatternNode(//
                    g,// s
                    new ConstantNode(new Constant(p2.getIV())),// p
                    s,// o
                    g,// c
                    Scope.NAMED_CONTEXTS//
                    ));

        }

        assertSameAST(expectedClause, queryRoot.getWhereClause());

    }

    /** 
     * Unit test where nested GRAPH patterns do not share the same variable.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT ?s
     * WHERE {
     *   GRAPH ?g {
     *     ?s :p :o .
     *     GRAPH ?g1 { ?g :p2 ?s }
     *   }
     * }
     * </pre>
    */
   @SuppressWarnings({ "unchecked", "rawtypes" })
   public void test_graphGroupOptimizer_02() throws MalformedQueryException {
       
       final String queryStr = ""//
               + "PREFIX : <http://example.org/>\n"//
               + "SELECT ?s\n"//
               + "WHERE {\n"//
               + "  GRAPH ?g {\n"//
               + "    ?s :p :o .\n"//
               + "    GRAPH ?g1 { ?g :p2 ?s }\n"//
               + "  }\n"//
               + "}";

       /*
        * Add the Values used in the query to the lexicon. This makes it
        * possible for us to explicitly construct the expected AST and
        * the verify it using equals().
        */
       final BigdataValueFactory f = store.getValueFactory();
       final BigdataURI p = f.createURI("http://example.org/p");
       final BigdataURI p2= f.createURI("http://example.org/p2");
       final BigdataURI o = f.createURI("http://example.org/o");
       final BigdataValue[] values = new BigdataValue[] { p, p2, o };
       store.getLexiconRelation()
               .addTerms(values, values.length, false/* readOnly */);

       final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
               .parseQuery2(queryStr, baseURI);

       ASTDeferredIVResolution.resolveQuery(store, astContainer);

       final AST2BOpContext context = new AST2BOpContext(astContainer, store);

       QueryRoot queryRoot = astContainer.getOriginalAST();
       
       queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
             context, new QueryNodeWithBindingSet(queryRoot, null)).
             getQueryNode();

       queryRoot = BOpUtility.deepCopy(queryRoot);
       
       queryRoot = (QueryRoot) new ASTGraphGroupOptimizer().optimize(
             context, new QueryNodeWithBindingSet(queryRoot, null)).
             getQueryNode();

       /*
        * Create the expected AST.
        */
       final JoinGroupNode expectedClause = new JoinGroupNode();
       {

           final VarNode g = new VarNode("g");
           final VarNode g1 = new VarNode("g1");
           final VarNode s = new VarNode("s");
           
           final JoinGroupNode graphGroup = new JoinGroupNode();
           expectedClause.addChild(graphGroup);
           graphGroup.setContext(g);

           // ?s :p :o
           graphGroup.addChild(new StatementPatternNode(//
                   s,// s
                   new ConstantNode(new Constant(p.getIV())),// p
                   new ConstantNode(new Constant(o.getIV())),// o
                   g,// c
                   Scope.NAMED_CONTEXTS//
                   ));
       
           final JoinGroupNode innerGraphGroup = new JoinGroupNode();
           graphGroup.addChild(innerGraphGroup);
           innerGraphGroup.setContext(g1);
           
           // ?g :p2 ?s
           innerGraphGroup.addChild(new StatementPatternNode(//
                   g,// s
                   new ConstantNode(new Constant(p2.getIV())),// p
                   s,// o
                   g1,// c
                   Scope.NAMED_CONTEXTS//
                   ));
           
            final FilterNode filterNode = new FilterNode(FunctionNode.sameTerm(
                    g, g1));

            final GlobalAnnotations globals = new GlobalAnnotations(
            		context.getLexiconNamespace(),
            		context.getTimestamp()
            		);
            
            AST2BOpUtility.toVE(getBOpContext(), globals,
                    filterNode.getValueExpressionNode());
            
            innerGraphGroup.addChild(filterNode);

        }

        assertSameAST(expectedClause, queryRoot.getWhereClause());

    }

    /**
     * A unit test where two nested graph groups have the same URI as their
     * context. This case is legal and there is nothing that needs to be changed
     * in the AST for enforce the graph context constraint.
     * 
     * @throws MalformedQueryException
     */
    public void test_graphContexts_constants_legal()
            throws MalformedQueryException {

        final String queryStr = ""//
                + "PREFIX : <http://example.org/>\n"//
                + "SELECT ?s\n"//
                + "WHERE {\n"//
                + "  GRAPH :foo {\n"//
                + "    ?s :p :o .\n"//
                + "    GRAPH :foo { ?o :p2 ?s }\n"//
                + "  }\n"//
                + "}";

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        new ASTGraphGroupOptimizer().optimize(context,
              new QueryNodeWithBindingSet(astContainer.getOriginalAST(), null));

    }

    /**
     * A unit test where two nested graph groups have the same URI as their
     * context. This case is illegal and will result in a thrown exception.
     * 
     * @throws MalformedQueryException
     */
    public void test_graphContexts_constants_illegal()
            throws MalformedQueryException {

        final String queryStr = ""//
                + "PREFIX : <http://example.org/>\n"//
                + "SELECT ?s\n"//
                + "WHERE {\n"//
                + "  GRAPH :foo {\n"//
                + "    ?s :p :o .\n"//
                + "    GRAPH :bar { ?o :p2 ?s }\n"//
                + "  }\n"//
                + "}";


        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        try {

            new ASTGraphGroupOptimizer().optimize(context,
                  new QueryNodeWithBindingSet(astContainer.getOriginalAST(), null));            
            
            fail("Expecting: " + InvalidGraphContextException.class);
            
        } catch (InvalidGraphContextException ex) {
            
            // Ignore expected exception.
            
        }

    }
   
    /*
     * All of these tests were written to the assumption that we were attempting
     * to combine non-empty join groups. However, the ASTEmptyGroupOptimizer is
     * only working with *empty* join groups so these tests are not appropriate
     * for that class (except to verify that it does not combine non-empty join
     * groups).
     * 
     * TODO These tests might be moved into TestASTGraphGroupOptimizer.  They
     * would have to be reworked, but they are relevant to the decisions made 
     * by the ASTGraphGroupOptimizer.
     */

//    /**
//     * Given
//     * 
//     * <pre>
//     *   SELECT VarNode(subj)
//     *     StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), DEFAULT_CONTEXTS)
//     *     JoinGroupNode [minus] {
//     *       StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("rdf"), VarNode(g), DEFAULT_CONTEXTS)
//     *       JoinGroupNode {
//     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), DEFAULT_CONTEXTS)
//     *       }
//     *     }
//     * </pre>
//     * 
//     * The inner most join group members are lifted into the MINUS group.
//     */
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    public void test_eliminateJoinGroup14_minus() {
//
//        /*
//         * Note: DO NOT share structures in this test!!!!
//         */
//        final IBindingSet[] bsets = new IBindingSet[]{};
//
//        final IV bdSearchIV = TermId.mockIV(VTE.URI);
//        bdSearchIV.setValue(store.getValueFactory().createURI(
//                BD.SEARCH.toString()));
//
//        final IV mikeIV = TermId.mockIV(VTE.LITERAL);
//        mikeIV.setValue(store.getValueFactory().createLiteral("mike"));
//
//        final IV rdfIV = TermId.mockIV(VTE.LITERAL);
//        rdfIV.setValue(store.getValueFactory().createLiteral("rdf"));
//
//        // The source AST.
//        final QueryRoot given = new QueryRoot(QueryType.SELECT);
//        {
//
//            final ProjectionNode projection = new ProjectionNode();
//            given.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            given.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode minusGroup = new JoinGroupNode();
//            whereClause.addChild(minusGroup);
//            minusGroup.setMinus(true);
//
//            minusGroup.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode minusGroup2 = new JoinGroupNode();
//            minusGroup.addChild(minusGroup2);
//            
//            minusGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//        }
//
//        // The expected AST after the rewrite.
//        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
//        {
//            
//            final ProjectionNode projection = new ProjectionNode();
//            expected.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            expected.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode minusGroup = new JoinGroupNode();
//            whereClause.addChild(minusGroup);
//            minusGroup.setMinus(true);
//
//            minusGroup.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//            
//            minusGroup.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//            
//        }
//
//        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
//        
//        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
//                given/* queryNode */, bsets);
//
//        assertSameAST(expected, actual);
//
//    }
//
//    /**
//     * Given
//     * 
//     * <pre>
//     *   SELECT VarNode(subj)
//     *     StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), DEFAULT_CONTEXTS)
//     *     JoinGroupNode [minus] {
//     *       StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("rdf"), VarNode(g), DEFAULT_CONTEXTS)
//     *       JoinGroupNode [context=g] {
//     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), NAMED_CONTEXTS)
//     *       }
//     *     }
//     * </pre>
//     * 
//     * The inner most join group members are NOT lifted into the MINUS group
//     * since direct children of the MINUS group are default graph BGPs while
//     * the inner most join group uses named graph BGPs.
//     */
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    public void test_eliminateJoinGroup15_minus() {
//
//        /*
//         * Note: DO NOT share structures in this test!!!!
//         */
//        final IBindingSet[] bsets = new IBindingSet[]{};
//
//        final IV bdSearchIV = TermId.mockIV(VTE.URI);
//        bdSearchIV.setValue(store.getValueFactory().createURI(
//                BD.SEARCH.toString()));
//
//        final IV mikeIV = TermId.mockIV(VTE.LITERAL);
//        mikeIV.setValue(store.getValueFactory().createLiteral("mike"));
//
//        final IV rdfIV = TermId.mockIV(VTE.LITERAL);
//        rdfIV.setValue(store.getValueFactory().createLiteral("rdf"));
//
//        // The source AST.
//        final QueryRoot given = new QueryRoot(QueryType.SELECT);
//        {
//
//            final ProjectionNode projection = new ProjectionNode();
//            given.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            given.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode minusGroup = new JoinGroupNode();
//            whereClause.addChild(minusGroup);
//            minusGroup.setMinus(true);
//
//            minusGroup.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode graphGroup = new JoinGroupNode();
//            minusGroup.addChild(graphGroup);
//            graphGroup.setContext(new VarNode("g"));
//                        
//            graphGroup.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new VarNode("g"), Scope.NAMED_CONTEXTS));
//
//        }
//
//        // The expected AST after the rewrite.
//        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
//        {
//            
//            final ProjectionNode projection = new ProjectionNode();
//            expected.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            expected.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode minusGroup = new JoinGroupNode();
//            whereClause.addChild(minusGroup);
//            minusGroup.setMinus(true);
//
//            minusGroup.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//            
//            final JoinGroupNode graphGroup = new JoinGroupNode();
//            minusGroup.addChild(graphGroup);
//            graphGroup.setContext(new VarNode("g"));
//                        
//            graphGroup.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new VarNode("g"), Scope.NAMED_CONTEXTS));
//            
//        }
//
//        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
//        
//        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
//                given/* queryNode */, bsets);
//
//        assertSameAST(expected, actual);
//
//    }
//
//    /**
//     * Given
//     * 
//     * <pre>
//     *   SELECT VarNode(subj)
//     *     StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), DEFAULT_CONTEXTS)
//     *     JoinGroupNode [context=g] {
//     *       StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("rdf"), VarNode(g), NAMED_CONTEXTS)
//     *       JoinGroupNode [context=g] {
//     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), NAMED_CONTEXTS)
//     *       }
//     *     }
//     * </pre>
//     * 
//     * Graph groups can not be combined unless they have the same graph variable
//     * or URI, or can be recognized as having the same constraint (e.g., an
//     * outer URI context and an inner variable context means that the variable
//     * is bound to the URI as a constant), in which case a FILTER needs to be
//     * added if the groups are merged which imposes that constraint. In this
//     * case we have the same context variable, so they can be collapsed without
//     * further ado.
//     */
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    public void test_eliminateJoinGroup16_graphGroups() {
//
//        /*
//         * Note: DO NOT share structures in this test!!!!
//         */
//        final IBindingSet[] bsets = new IBindingSet[]{};
//
//        final IV bdSearchIV = TermId.mockIV(VTE.URI);
//        bdSearchIV.setValue(store.getValueFactory().createURI(
//                BD.SEARCH.toString()));
//
//        final IV mikeIV = TermId.mockIV(VTE.LITERAL);
//        mikeIV.setValue(store.getValueFactory().createLiteral("mike"));
//
//        final IV rdfIV = TermId.mockIV(VTE.LITERAL);
//        rdfIV.setValue(store.getValueFactory().createLiteral("rdf"));
//
//        // The source AST.
//        final QueryRoot given = new QueryRoot(QueryType.SELECT);
//        {
//
//            final ProjectionNode projection = new ProjectionNode();
//            given.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            given.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode graphGroup1 = new JoinGroupNode();
//            whereClause.addChild(graphGroup1);
//            graphGroup1.setContext(new VarNode("g"));
//
//            graphGroup1.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new VarNode("g"), Scope.NAMED_CONTEXTS));
//
//            final JoinGroupNode graphGroup2 = new JoinGroupNode();
//            graphGroup1.addChild(graphGroup2);
//            graphGroup2.setContext(new VarNode("g"));
//
//            graphGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new VarNode("g"), Scope.NAMED_CONTEXTS));
//
//        }
//
//        // The expected AST after the rewrite.
//        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
//        {
//            
//            final ProjectionNode projection = new ProjectionNode();
//            expected.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            expected.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode graphGroup1 = new JoinGroupNode();
//            whereClause.addChild(graphGroup1);
//            graphGroup1.setContext(new VarNode("g"));
//
//            graphGroup1.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new VarNode("g"), Scope.NAMED_CONTEXTS));
//
//            final JoinGroupNode graphGroup2 = new JoinGroupNode();
//            graphGroup1.addChild(graphGroup2);
//            graphGroup2.setContext(new VarNode("g"));
//
//            graphGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new VarNode("g"), Scope.NAMED_CONTEXTS));
//            
//        }
//
//        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
//        
//        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
//                given/* queryNode */, bsets);
//
//        assertSameAST(expected, actual);
//
//    }
//
//    /**
//     * Given
//     * 
//     * <pre>
//     *   SELECT VarNode(subj)
//     *     StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), DEFAULT_CONTEXTS)
//     *     JoinGroupNode [context=g] {
//     *       StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("rdf"), VarNode(g), NAMED_CONTEXTS)
//     *       JoinGroupNode [context=c] {
//     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(c), NAMED_CONTEXTS)
//     *       }
//     *     }
//     * </pre>
//     * 
//     * In this case the graph variables are not the same, but we know that the
//     * inner graph group MUST have a scope which is no broader than the outer
//     * graph group. So, the groups MAY be collapsed, but we MUST add a
//     * <code>FILTER( ?g = ?c )</code> filter to impose the constraint that
//     * <code>?c</code> and <code>?g</code> have the same binding.
//     * 
//     * Note: The {@link ASTEmptyGroupOptimizer} does not currently collapse these
//     * groups, but it could.
//     */
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    public void test_eliminateJoinGroup17_graphGroups() {
//
//        /*
//         * Note: DO NOT share structures in this test!!!!
//         */
//        final IBindingSet[] bsets = new IBindingSet[]{};
//
//        final IV bdSearchIV = TermId.mockIV(VTE.URI);
//        bdSearchIV.setValue(store.getValueFactory().createURI(
//                BD.SEARCH.toString()));
//
//        final IV mikeIV = TermId.mockIV(VTE.LITERAL);
//        mikeIV.setValue(store.getValueFactory().createLiteral("mike"));
//
//        final IV rdfIV = TermId.mockIV(VTE.LITERAL);
//        rdfIV.setValue(store.getValueFactory().createLiteral("rdf"));
//
//        // The source AST.
//        final QueryRoot given = new QueryRoot(QueryType.SELECT);
//        {
//
//            final ProjectionNode projection = new ProjectionNode();
//            given.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            given.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode graphGroup1 = new JoinGroupNode();
//            whereClause.addChild(graphGroup1);
//            graphGroup1.setContext(new VarNode("g"));
//
//            graphGroup1.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new VarNode("g"), Scope.NAMED_CONTEXTS));
//
//            final JoinGroupNode graphGroup2 = new JoinGroupNode();
//            graphGroup1.addChild(graphGroup2);
//            graphGroup2.setContext(new VarNode("c"));
//
//            graphGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new VarNode("c"), Scope.NAMED_CONTEXTS));
//
//        }
//
//        // The expected AST after the rewrite.
//        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
//        {
//            
//            final ProjectionNode projection = new ProjectionNode();
//            expected.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            expected.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode graphGroup1 = new JoinGroupNode();
//            whereClause.addChild(graphGroup1);
//            graphGroup1.setContext(new VarNode("g"));
//
//            graphGroup1.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new VarNode("g"), Scope.NAMED_CONTEXTS));
//
//            final JoinGroupNode graphGroup2 = new JoinGroupNode();
//            graphGroup1.addChild(graphGroup2);
//            graphGroup2.setContext(new VarNode("c"));
//
//            graphGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new VarNode("c"), Scope.NAMED_CONTEXTS));
//            
//        }
//
//        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
//        
//        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
//                given/* queryNode */, bsets);
//
//        assertSameAST(expected, actual);
//
//    }
//
//    /**
//     * Given
//     * 
//     * <pre>
//     *   SELECT VarNode(subj)
//     *     StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), DEFAULT_CONTEXTS)
//     *     JoinGroupNode [context=http://example.org/foo] {
//     *       StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("rdf"), ConstantNode(http://example.org/foo), NAMED_CONTEXTS)
//     *       JoinGroupNode [context=http://example.org/foo] {
//     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), ConstantNode(http://example.org/foo), NAMED_CONTEXTS)
//     *       }
//     *     }
//     * </pre>
//     * 
//     * In this case the graph URIs are the same.
//     * 
//     * Note: The {@link ASTEmptyGroupOptimizer} does not currently collapse these
//     * groups, but it could.
//     * 
//     * Note: There are other cases which could be collapsed if the appropriate
//     * SameTerm() filter and/or Constant/2 constructed were used to link the
//     * constant and the variable.
//     */
//    @SuppressWarnings({ "unchecked", "rawtypes" })
//    public void test_eliminateJoinGroup18_graphGroups() {
//
//        /*
//         * Note: DO NOT share structures in this test!!!!
//         */
//        final IBindingSet[] bsets = new IBindingSet[]{};
//
//        final IV bdSearchIV = TermId.mockIV(VTE.URI);
//        bdSearchIV.setValue(store.getValueFactory().createURI(
//                BD.SEARCH.toString()));
//
//        final IV mikeIV = TermId.mockIV(VTE.LITERAL);
//        mikeIV.setValue(store.getValueFactory().createLiteral("mike"));
//
//        final IV rdfIV = TermId.mockIV(VTE.LITERAL);
//        rdfIV.setValue(store.getValueFactory().createLiteral("rdf"));
//
//        final IV uriIV = TermId.mockIV(VTE.URI);
//        uriIV.setValue(store.getValueFactory().createURI("http://example.org/foo"));
//
//        // The source AST.
//        final QueryRoot given = new QueryRoot(QueryType.SELECT);
//        {
//
//            final ProjectionNode projection = new ProjectionNode();
//            given.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            given.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode graphGroup1 = new JoinGroupNode();
//            whereClause.addChild(graphGroup1);
//            graphGroup1.setContext(new ConstantNode(uriIV));
//
//            graphGroup1.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new ConstantNode(uriIV), Scope.NAMED_CONTEXTS));
//
//            final JoinGroupNode graphGroup2 = new JoinGroupNode();
//            graphGroup1.addChild(graphGroup2);
//            graphGroup2.setContext(new ConstantNode(uriIV));
//
//            graphGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new ConstantNode(uriIV), Scope.NAMED_CONTEXTS));
//
//        }
//
//        // The expected AST after the rewrite.
//        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
//        {
//            
//            final ProjectionNode projection = new ProjectionNode();
//            expected.setProjection(projection);
//            
//            projection.addProjectionVar(new VarNode("subj"));
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            expected.setWhereClause(whereClause);
//
//            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
//                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
//
//            final JoinGroupNode graphGroup1 = new JoinGroupNode();
//            whereClause.addChild(graphGroup1);
//            graphGroup1.setContext(new ConstantNode(uriIV));
//
//            graphGroup1.addChild(new StatementPatternNode(new VarNode("lit"),
//                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
//                    new ConstantNode(uriIV), Scope.NAMED_CONTEXTS));
//
//            final JoinGroupNode graphGroup2 = new JoinGroupNode();
//            graphGroup1.addChild(graphGroup2);
//            graphGroup2.setContext(new ConstantNode(uriIV));
//
//            graphGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
//                    new VarNode("p"), new VarNode("lit"),
//                    new ConstantNode(uriIV), Scope.NAMED_CONTEXTS));
//            
//        }
//
//        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
//        
//        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
//                given/* queryNode */, bsets);
//
//        assertSameAST(expected, actual);
//
//    }

}
