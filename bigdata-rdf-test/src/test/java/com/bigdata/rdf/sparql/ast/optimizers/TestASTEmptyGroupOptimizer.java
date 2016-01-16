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

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.store.BDS;
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
                BDS.SEARCH.toString()));

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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

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
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }
    
    /**
     * <pre>
     *		WITH {
     *		  QueryType: SELECT
     *		  SELECT ( VarNode(_var1) AS VarNode(_var1) )
     *		    JoinGroupNode {
     *		      StatementPatternNode(VarNode(_var1), ConstantNode(TermId(1U)[http://example/a]), ConstantNode(TermId(2U)[http://example/b]), DEFAULT_CONTEXTS)
     *		    }
     *		} AS _set1
     *		QueryType: SELECT
     *		SELECT VarNode(_var1) VarNode(_var2) VarNode(_var4)
     *		  JoinGroupNode {
     *		    JoinGroupNode {
     *		      JoinGroupNode {
     *		        INCLUDE _set1
     *		      }
     *		      StatementPatternNode(VarNode(_var1), ConstantNode(TermId(3U)[http://example/c]), VarNode(_var2), DEFAULT_CONTEXTS) [simpleOptional]
     *		      StatementPatternNode(VarNode(_var12), ConstantNode(TermId(6U)[http://example/d]), VarNode(_var1), DEFAULT_CONTEXTS)
     *		      StatementPatternNode(VarNode(_var12), ConstantNode(TermId(7U)[http://example/e]), VarNode(_var4), DEFAULT_CONTEXTS)
     *		    }
     *		  }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_eliminateJoinGroup08() throws Exception {
        
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

        	final NamedSubqueryRoot namedSubquery = new NamedSubqueryRoot(QueryType.SELECT, "_set1");
        	{
        		
                final ProjectionNode projection = new ProjectionNode();
                namedSubquery.setProjection(projection);
                projection.addProjectionExpression(new AssignmentNode(new VarNode("_var1"), new VarNode("_var1")));
        		
                final JoinGroupNode joinGroup1 = new JoinGroupNode(
                        new StatementPatternNode(new VarNode("_var1"),
                                new ConstantNode(a), new ConstantNode(b), null/* c */,
                                Scope.DEFAULT_CONTEXTS));
                
                namedSubquery.setWhereClause(joinGroup1);
                
        	}
        	
            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("_var1"));
            projection.addProjectionVar(new VarNode("_var2"));
            projection.addProjectionVar(new VarNode("_var4"));
            
            final JoinGroupNode joinGroup1 = new JoinGroupNode();
            final JoinGroupNode joinGroup2 = new JoinGroupNode();
            final JoinGroupNode joinGroup3 = new JoinGroupNode();
            final JoinGroupNode joinGroup4 = new JoinGroupNode();
            
            joinGroup4.addChild(joinGroup3);
            
            joinGroup3.addChild(joinGroup2);
            
            joinGroup2.addChild(joinGroup1);
            joinGroup2.addChild(new StatementPatternNode(new VarNode("_var1"),
                            new ConstantNode(c), new VarNode("_var2"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            joinGroup2.addChild(new StatementPatternNode(new VarNode("_var12"),
                            new ConstantNode(d), new VarNode("_var1"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            joinGroup2.addChild(new StatementPatternNode(new VarNode("_var12"),
                            new ConstantNode(e), new VarNode("_var4"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            
            joinGroup1.addChild(new NamedSubqueryInclude("_set1"));
            
            given.setWhereClause(joinGroup4);
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
                        new StatementPatternNode(new VarNode("_var1"),
                                new ConstantNode(a), new ConstantNode(b), null/* c */,
                                Scope.DEFAULT_CONTEXTS));
                
                namedSubquery.setWhereClause(joinGroup1);
                
        	}
        	
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.setDistinct(true);
            projection.addProjectionVar(new VarNode("_var1"));
            projection.addProjectionVar(new VarNode("_var2"));
            projection.addProjectionVar(new VarNode("_var4"));
            
            final JoinGroupNode joinGroup1 = new JoinGroupNode();

//            final JoinGroupNode joinGroup2 = new JoinGroupNode();
//            joinGroup2.addChild(new NamedSubqueryInclude("_set1"));
//            joinGroup1.addChild(joinGroup2);
            joinGroup1.addChild(new NamedSubqueryInclude("_set1"));
            
            joinGroup1.addChild(new StatementPatternNode(new VarNode("_var1"),
                            new ConstantNode(c), new VarNode("_var2"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            joinGroup1.addChild(new StatementPatternNode(new VarNode("_var12"),
                            new ConstantNode(d), new VarNode("_var1"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            joinGroup1.addChild(new StatementPatternNode(new VarNode("_var12"),
                            new ConstantNode(e), new VarNode("_var4"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
            
            expected.setWhereClause(joinGroup1);
            expected.getNamedSubqueriesNotNull().add(namedSubquery);
            
        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }
    
    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(subj)
     *     JoinGroupNode {
     *       StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), NAMED_CONTEXTS)
     *       StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), NAMED_CONTEXTS)
     *       JoinGroupNode {
     *       }
     *       JoinGroupNode {
     *         JoinGroupNode {
     *         }
     *       }
     *     }
     * </pre>
     * 
     * Remove the inner {@link JoinGroupNode}s.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_eliminateJoinGroup09() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV bdSearchIV = TermId.mockIV(VTE.URI);
        bdSearchIV.setValue(store.getValueFactory().createURI(
                BDS.SEARCH.toString()));

        final IV mikeIV = TermId.mockIV(VTE.LITERAL);
        mikeIV.setValue(store.getValueFactory().createLiteral("mike"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("subj"));
            
            final JoinGroupNode graphGroup = new JoinGroupNode();

            graphGroup.addChild(new JoinGroupNode());
            graphGroup.addChild(new JoinGroupNode(new JoinGroupNode()));
            
            graphGroup.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

            graphGroup.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

            given.setWhereClause(graphGroup);
        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("subj"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            
            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }
    
    /**
     * Top-level UNION optimizations. We should look at the plan after join
     * attachment. If there is just a child union (because either there is no
     * filter or because there is a filter and it was attached to the union)
     * then we should either lift the UNION and evaluate it directly or (if we
     * do not permit that structure) make the join group evaluation a nop in
     * which it just enters the child (the union) and evaluates that.
     * 
     * <pre>
     *     SELECT DISTINCT ?subjectUri ?p ?rightValue ?leftValue
     *     WHERE {
     *     {
     *     ?subjectUri rdf:type <http://test.org/someType/a> .
     *     ?subjectUri ?p ?rightValue .
     *     ?rightValue rdf:type ?otherValueType .
     *     } UNION {
     *     ?subjectUri rdf:type <http://test.org/someType/a> .
     *     ?leftValue ?p ?subjectUri .
     *     ?leftValue rdf:type ?otherValueType .
     *     }
     *     FILTER(sameTerm(?otherValueType,
     *     'http://test.org/someOtherType/1')
     *     sameTerm(?otherValueType,
     *     'http://test.org/someOtherType/2'))
     *     }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/416
     */
    public void test_ticket416() throws Exception {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};
        
        final IV<?,?> type = makeIV(RDF.TYPE);
        
        final IV<?,?> a = makeIV(new URIImpl("http://example/a"));

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
            
            expected.setProjection(projection);
            expected.setWhereClause(union);

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(subj)
     *     StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), DEFAULT_CONTEXTS)
     *     JoinGroupNode [minus] {
     *       StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), NAMED_CONTEXTS)
     *     }
     * </pre>
     * 
     * There is no change. The top-level join group is unchanged since the MINUS
     * is not semantically empty and hence can not be lifted into it. 
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_eliminateJoinGroup10_minus() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV bdSearchIV = TermId.mockIV(VTE.URI);
        bdSearchIV.setValue(store.getValueFactory().createURI(
                BDS.SEARCH.toString()));

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

            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);

            minusGroup.setMinus(true);

            minusGroup.addChild(new StatementPatternNode(new VarNode("subj"),
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

            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);

            minusGroup.setMinus(true);

            minusGroup.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));           

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(subj)
     *     StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), DEFAULT_CONTEXTS)
     *     JoinGroupNode [minus] {
     *       JoinGroupNode [context=VarNode(g)] {
     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), NAMED_CONTEXTS)
     *       }
     *     }
     * </pre>
     * 
     * There is no change. The top-level join group is unchanged since the MINUS
     * is not semantically empty and hence can not be lifted into it. The NAMED
     * GRAPH join group is NOT lifted since MINUS is not semantically empty and
     * is does not share the same context as the inner join group.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_eliminateJoinGroup11_minus() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV bdSearchIV = TermId.mockIV(VTE.URI);
        bdSearchIV.setValue(store.getValueFactory().createURI(
                BDS.SEARCH.toString()));

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

            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);
            minusGroup.setMinus(true);

            final JoinGroupNode graphGroup = new JoinGroupNode();
            minusGroup.addChild(graphGroup);
            graphGroup.setContext(new VarNode("g"));
            
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

            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);
            minusGroup.setMinus(true);

            final JoinGroupNode graphGroup = new JoinGroupNode();
            minusGroup.addChild(graphGroup);
            graphGroup.setContext(new VarNode("g"));
            
            graphGroup.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.NAMED_CONTEXTS));

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }


    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(subj)
     *     StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), DEFAULT_CONTEXTS)
     *     JoinGroupNode [minus] {
     *       JoinGroupNode [minus] {
     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), DEFAULT_CONTEXTS)
     *       }
     *     }
     * </pre>
     * 
     * The inner MINUS group can not replace the outer MINUS. This case actually
     * corresponds to subtracting the solutions of the inner MINUS group from
     * the solutions of the outer MINUS group (an empty set).
     * <p>
     * Note: For this case, we could just wipe out both of the MINUS groups
     * since subtracting anything from an empty set yields an empty set for the
     * MINUS groups and subtracting an empty set from the parent yeilds whatever
     * solutions are in the parent. I.e., the MINUS groups reduce to a NOP. In
     * fact, this probably is done by the {@link ASTBottomUpOptimizer} since
     * there will not be any shared variables between the first level MINUS and
     * the outer parent group.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_eliminateJoinGroup12_minus() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV bdSearchIV = TermId.mockIV(VTE.URI);
        bdSearchIV.setValue(store.getValueFactory().createURI(
                BDS.SEARCH.toString()));

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

            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);
            minusGroup.setMinus(true);

            final JoinGroupNode minusGroup2 = new JoinGroupNode();
            minusGroup.addChild(minusGroup2);
            minusGroup2.setMinus(true);
            
            minusGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("subj"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);
            minusGroup.setMinus(true);

            final JoinGroupNode minusGroup2 = new JoinGroupNode();
            minusGroup.addChild(minusGroup2);
            minusGroup2.setMinus(true);
            
            minusGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * Given
     * 
     * <pre>
     *   SELECT VarNode(subj)
     *     StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("mike"), VarNode(g), DEFAULT_CONTEXTS)
     *     JoinGroupNode [minus] {
     *       StatementPatternNode(VarNode(lit), ConstantNode(bd:search), ConstantNode("rdf"), VarNode(g), DEFAULT_CONTEXTS)
     *       JoinGroupNode [minus] {
     *         StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), VarNode(g), DEFAULT_CONTEXTS)
     *       }
     *     }
     * </pre>
     * 
     * This must be evaluated as <code>G0 - (G1 - (G2))</code>. The two MINUS
     * groups may not be combined.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_eliminateJoinGroup13_minus() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        final IV bdSearchIV = TermId.mockIV(VTE.URI);
        bdSearchIV.setValue(store.getValueFactory().createURI(
                BDS.SEARCH.toString()));

        final IV mikeIV = TermId.mockIV(VTE.LITERAL);
        mikeIV.setValue(store.getValueFactory().createLiteral("mike"));

        final IV rdfIV = TermId.mockIV(VTE.LITERAL);
        rdfIV.setValue(store.getValueFactory().createLiteral("rdf"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("subj"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);
            minusGroup.setMinus(true);

            minusGroup.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup2 = new JoinGroupNode();
            minusGroup.addChild(minusGroup2);
            minusGroup2.setMinus(true);
            
            minusGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("subj"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(mikeIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);
            minusGroup.setMinus(true);

            minusGroup.addChild(new StatementPatternNode(new VarNode("lit"),
                    new ConstantNode(bdSearchIV), new ConstantNode(rdfIV),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode minusGroup2 = new JoinGroupNode();
            minusGroup.addChild(minusGroup2);
            minusGroup2.setMinus(true);
            
            minusGroup2.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"),
                    new VarNode("g"), Scope.DEFAULT_CONTEXTS));
            
        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * A test case where we can not eliminate an empty join group in a UNION.
     * 
     * <pre>
     * PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
     * PREFIX :     <http://example.org/book/> 
     * PREFIX ns:   <http://example.org/ns#> 
     * 
     * SELECT ?book
     * {
     *    {}
     *    UNION
     *    { ?book dc:title ?title }
     * }
     * </pre>
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/504"> UNION
     *      with Empty Group Pattern </a>
     */
    @SuppressWarnings("unchecked")
    public void test_union_01() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV dcTitle = TermId.mockIV(VTE.URI);
        dcTitle.setValue(store.getValueFactory().createURI(
                DC.TITLE.stringValue()));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("book"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            final UnionNode union = new UnionNode();
            whereClause.addChild(union);

            final JoinGroupNode emptyGroup = new JoinGroupNode();

            final JoinGroupNode otherGroup = new JoinGroupNode();
            otherGroup.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(dcTitle), new VarNode("title")));

            union.addChild(emptyGroup);
            union.addChild(otherGroup);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            
            projection.addProjectionVar(new VarNode("book"));
            
            final UnionNode union = new UnionNode();
            expected.setWhereClause(union);

            final JoinGroupNode emptyGroup = new JoinGroupNode();

            final JoinGroupNode otherGroup = new JoinGroupNode();
            otherGroup.addChild(new StatementPatternNode(new VarNode("book"),
                    new ConstantNode(dcTitle), new VarNode("title")));

            union.addChild(emptyGroup);
            union.addChild(otherGroup);

        }

        final IASTOptimizer rewriter = new ASTEmptyGroupOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }
    
}
