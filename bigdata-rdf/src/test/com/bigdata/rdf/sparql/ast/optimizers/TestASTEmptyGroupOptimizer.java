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
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Test suite for {@link ASTEmptyGroupOptimizer}.
 * 
 * TODO Test for removal of empty UNIONs. Empty UNIONs can arise through pruning
 * based on unknown IVs, or filters which are provably false. However, I would
 * expect the pruning logic to eliminate the empty group in such cases.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestASTEmptyGroupOptimizer.java 5302 2011-10-07 14:28:03Z
 *          thompsonbry $
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
        mikeIV.setValue(store.getValueFactory().createLiteral("mike"));

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
     */
    public void test_eliminateJoinGroup02() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // { {} }
            final JoinGroupNode whereClause = new JoinGroupNode(new JoinGroupNode());
            given.setWhereClause(whereClause);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            // {}
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

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
     * { { {} } } => {}
     * </pre>
     */
    public void test_eliminateJoinGroup03() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // { { {} } }
            final JoinGroupNode whereClause = new JoinGroupNode(
                    new JoinGroupNode(new JoinGroupNode()));
            given.setWhereClause(whereClause);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

        }
        
        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * Unit test verifies that the rewrite of an embedded sole GRAPH pattern is
     * the lifting of that GRAPH pattern (rather than replacing it with an empty
     * non-GRAPH group).
     * 
     * <pre>
     * { GRAPH foo {} } => GRAPH foo {}
     * </pre>
     */
    public void test_eliminateJoinGroup04() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // { GRAPH ?g {} }
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final JoinGroupNode graphGroup = new JoinGroupNode(new JoinGroupNode());
            graphGroup.setContext(new VarNode("g"));
            whereClause.addChild(graphGroup);

            System.err.println("given:"+given);
        }
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            // GRAPH ?g {}
            final JoinGroupNode graphGroup = new JoinGroupNode(new JoinGroupNode());
            graphGroup.setContext(new VarNode("g"));
            expected.setWhereClause(graphGroup);

            System.err.println("expected:"+expected);
        }
        
        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * For this AST we need to replace the root {@link JoinGroupNode} with its
     * child {@link UnionNode}. This means setting the child {@link UnionNode}
     * as the whereClause on the {@link QueryBase}.
     * 
     * <pre>
     * SELECT DISTINCT VarNode(s) VarNode(o)
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
     * 
     * @throws Exception
     */
    public void test_eliminateJoinGroup05() throws Exception {
        
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
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));
            
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

            final UnionNode union = new UnionNode();
            final JoinGroupNode joinGroup1 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(p), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            final JoinGroupNode joinGroup2 = new JoinGroupNode(
                    new StatementPatternNode(new VarNode("s"),
                            new ConstantNode(q), new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(union);
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);
            
        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * For this AST we need to left the {@link UnionNode} up to replace its
     * empty parent {@link JoinGroupNode}.  The difference between this test
     * and test 05 is that the parent is not the root of the where clause.
     * 
     * <pre>
     * SELECT DISTINCT VarNode(s) VarNode(o)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(s), ConstantNode(TermId(4U)[http://example/x]), ConstantNode(TermId(5U)[http://example/y]), DEFAULT_CONTEXTS)
     *     JoinGroupNode {
     *       UnionNode {
     *         JoinGroupNode {
     *           StatementPatternNode(VarNode(s), ConstantNode(TermId(2U)[http://example/p]), VarNode(o), DEFAULT_CONTEXTS)
     *         }
     *         JoinGroupNode {
     *           StatementPatternNode(VarNode(s), ConstantNode(TermId(3U)[http://example/q]), VarNode(o), DEFAULT_CONTEXTS)
     *         }
     *       }
     *     }
     *   }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_eliminateJoinGroup06() throws Exception {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://example/p"));

        @SuppressWarnings("rawtypes")
        final IV q = makeIV(new URIImpl("http://example/q"));
        
        @SuppressWarnings("rawtypes")
        final IV x = makeIV(new URIImpl("http://example/x"));
        
        @SuppressWarnings("rawtypes")
        final IV y = makeIV(new URIImpl("http://example/y"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));
            
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
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);
            
            final JoinGroupNode emptyJoinGroup = new JoinGroupNode();
            emptyJoinGroup.addChild(union);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new ConstantNode(x), new ConstantNode(y), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(emptyJoinGroup);

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
            union.addChild(joinGroup1);
            union.addChild(joinGroup2);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new ConstantNode(x), new ConstantNode(y), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(union);
            
        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
    /**
     * Rewrite:
     * 
     * <pre>
     * SELECT ?x ?o
     *  WHERE {
     *     ?x rdfs:label ?o .
     *     {
     *       SELECT ?x WHERE {?x rdf:type foaf:Person}
     *     }
     * }
     * </pre>
     * 
     * to
     * 
     * <pre>
     * SELECT ?x ?o
     *  WHERE {
     *     ?x rdfs:label ?o .
     *     SELECT ?x WHERE {?x rdf:type foaf:Person}
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_eliminateJoinGroup07() throws Exception {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV rdfsLabel = makeIV(RDFS.LABEL);

        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // Sub-Select
            final SubqueryRoot subqueryRoot = new SubqueryRoot(QueryType.SELECT);
            {
                
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfType), new ConstantNode(foafPerson),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }

            // QueryRoot
            {

                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);
                projection.setDistinct(true);
                projection.addProjectionVar(new VarNode("x"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfsLabel), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode joinGroup = new JoinGroupNode();
                whereClause.addChild(joinGroup);

                joinGroup.addChild(subqueryRoot);
                
            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            // Sub-Select
            final SubqueryRoot subqueryRoot = new SubqueryRoot(QueryType.SELECT);
            {
                
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfType), new ConstantNode(foafPerson),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }

            // QueryRoot
            {

                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.setDistinct(true);
                projection.addProjectionVar(new VarNode("x"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfsLabel), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(subqueryRoot);
                
            }

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * Given:
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT ?v
     * { :x :p ?v . { FILTER(?v = 1) } }
     * </pre>
     * 
     * Verify that the FILTER is NOT lifted out of the join group.
     * <p>
     * Note: This is <code>Filter-nested - 2</code> (Filter on variable ?v which
     * is not in scope) from the DAWG test suite. In this case, the filter can
     * not be lifted because that change would violate the bottom up semantics
     * for this query. In general, the lifting of filters which MAY be lifted
     * should be handled by the {@link ASTLiftPreFiltersOptimizer} class. That
     * class is designed to recognize pre-filters, which are filters whose
     * variables are all known bound on entry to the group and, hence, could be
     * run against the parent group.
     */
    public void test_emptyGroupOptimizer_doNotLiftFilter() {
    
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV x = makeIV(new URIImpl("http://example/x"));

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://example/p"));

        @SuppressWarnings("rawtypes")
        final IV ONE = makeIV(new LiteralImpl("1",XSD.INTEGER));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // QueryRoot
            {

                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);
                projection.setDistinct(true);
                projection.addProjectionVar(new VarNode("v"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new ConstantNode(x),
                        new ConstantNode(p), new VarNode("v"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode joinGroup = new JoinGroupNode(true/*optional*/);
                whereClause.addChild(joinGroup);

                joinGroup.addChild(new FilterNode(new FunctionNode(
                        FunctionRegistry.EQ, null/* scalarValues */,
                        new ValueExpressionNode[] { new VarNode("v"),
                                new ConstantNode(ONE) })));
                
            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
         
            // QueryRoot
            {
                
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.setDistinct(true);
                projection.addProjectionVar(new VarNode("v"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new ConstantNode(x),
                        new ConstantNode(p), new VarNode("v"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode joinGroup = new JoinGroupNode(true/*optional*/);
                whereClause.addChild(joinGroup);

                joinGroup.addChild(new FilterNode(new FunctionNode(
                        FunctionRegistry.EQ, null/* scalarValues */,
                        new ValueExpressionNode[] { new VarNode("v"),
                                new ConstantNode(ONE) })));
                
            }

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }

    /**
     * Verify that we do not lift something out of an optional group as that
     * would destroy the optional semantics of the thing lifted.
     * <pre>
     * SELECT ?x ?o
     *  WHERE {
     *     ?x rdfs:label ?o .
     *     OPTIONAL {
     *       ?x rdf:type foaf:Person
     *     }
     * }
     * </pre>
     */
    public void test_emptyGroupOptimizer_doNotLiftFromOptionalGroup() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV rdfsLabel = makeIV(RDFS.LABEL);

        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // QueryRoot
            {

                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);
                projection.setDistinct(true);
                projection.addProjectionVar(new VarNode("x"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfsLabel), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode joinGroup = new JoinGroupNode(true/*optional*/);
                whereClause.addChild(joinGroup);

                joinGroup.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfType), new ConstantNode(foafPerson),
                        null/* c */, Scope.DEFAULT_CONTEXTS));
                
            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
         
            // QueryRoot
            {

                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.setDistinct(true);
                projection.addProjectionVar(new VarNode("x"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfsLabel), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode joinGroup = new JoinGroupNode(true/*optional*/);
                whereClause.addChild(joinGroup);

                joinGroup.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(rdfType), new ConstantNode(foafPerson),
                        null/* c */, Scope.DEFAULT_CONTEXTS));
                
                
            }

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
    
}
