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
 * Created on Sep 13, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Collections;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for the {@link ASTQueryHintOptimizer}.
 * 
 * TODO Unit test to verify that the caller's queryId is set on the query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTQueryHintOptimizer extends
        AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTQueryHintOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTQueryHintOptimizer(String name) {
        super(name);
    }

    /**
     * Unit test for binding query hints.
     * 
     * <pre>
     * PREFIX p1: <http://www.rdfabout.com/rdf/schema/usgovt/>
     * PREFIX p2: <http://www.rdfabout.com/rdf/schema/vote/>
     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX hint: <http://www.bigdata.com/queryHints#>
     * 
     * SELECT (SAMPLE(?_var9) AS ?_var1) ?_var2 ?_var3
     * WITH {
     *     SELECT DISTINCT ?_var3
     *     WHERE {
     *         ?_var3 rdf:type <http://www.rdfabout.com/rdf/schema/politico/Politician>.
     *         ?_var3 <http://www.rdfabout.com/rdf/schema/politico/hasRole> ?_var6. 
     *         ?_var6 <http://www.rdfabout.com/rdf/schema/politico/party> "Democrat".
     *     }
     * } AS %_set1
     *         WHERE {
     *             
     *             INCLUDE %_set1 . 
     *             OPTIONAL {
     *                 ?_var3 p1:name ?_var9
     *             }. 
     *             OPTIONAL {
     *                 ?_var10 p2:votedBy ?_var3. 
     *                 ?_var10 rdfs:label ?_var2.
     *                 hint:com.bigdata.relation.accesspath.IBuffer.chunkCapacity "200".
     *             }
     *         }
     *         GROUP BY ?_var2 ?_var3
     * </pre>
     * 
     * The complex optional group in this query should have the
     * {@link BufferAnnotations#CHUNK_CAPACITY} overridden.
     */
    public void test_query_hints() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV rdfsLabel = makeIV(RDFS.LABEL);
        @SuppressWarnings("rawtypes")
        final IV politician = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/Politician"));
        @SuppressWarnings("rawtypes")
        final IV hasRole = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/hasRole"));
        @SuppressWarnings("rawtypes")
        final IV party = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/party"));
        @SuppressWarnings("rawtypes")
        final IV democrat = makeIV(new LiteralImpl("Democrat"));
        @SuppressWarnings("rawtypes")
        final IV name = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/name"));
        @SuppressWarnings("rawtypes")
        final IV votedBy = makeIV(new URIImpl(
                "http://www.rdfabout.com/rdf/schema/vote/votedBy"));

        @SuppressWarnings("rawtypes")
        final IV scopeBGP = makeIV(new URIImpl(QueryHints.NAMESPACE+QueryHintScope.BGP));
        @SuppressWarnings("rawtypes")
        final IV chunkCapacity = makeIV(new URIImpl(QueryHints.NAMESPACE+BufferAnnotations.CHUNK_CAPACITY));
        @SuppressWarnings("rawtypes")
        final IV chunkCapacityValue = makeIV(new LiteralImpl("200"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        final String namedSet1 = "_set1";
        {

            // NamedSubqueryRoot
            {

                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, namedSet1);
                given.getNamedSubqueriesNotNull().add(nsr);

                {
                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.setDistinct(true);
                    projection.addProjectionVar(new VarNode("_var3"));
                }

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var3"), new ConstantNode(a), new ConstantNode(
                        politician), null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var3"), new ConstantNode(hasRole), new VarNode(
                        "_var6"), null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var6"), new ConstantNode(party), new ConstantNode(
                        democrat), null/* c */, Scope.DEFAULT_CONTEXTS));

            }

            // Main Query
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection
                        .addProjectionExpression(new AssignmentNode(//
                                new VarNode("_var1"),// var
                                new FunctionNode(
                                        // expr
                                        FunctionRegistry.SAMPLE,
                                        Collections
                                                .singletonMap(
                                                        AggregateBase.Annotations.DISTINCT,
                                                        (Object) Boolean.FALSE)/* scalarValues */,
                                        new ValueExpressionNode[] { new VarNode(
                                                "_var9") })));
                projection.addProjectionVar(new VarNode("_var2"));
                projection.addProjectionVar(new VarNode("_var3"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new NamedSubqueryInclude(namedSet1));

                whereClause.addChild(new JoinGroupNode(true/* optional */,
                        new StatementPatternNode(new VarNode("_var3"),
                                new ConstantNode(name), new VarNode("_var9"),
                                null/* c */, Scope.DEFAULT_CONTEXTS)));

                final JoinGroupNode complexOptGroup = new JoinGroupNode(true/* optional */);
                whereClause.addChild(complexOptGroup);
                {
                    complexOptGroup.addChild(new StatementPatternNode(
                            new VarNode("_var10"), new ConstantNode(votedBy),
                            new VarNode("_var3"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                    complexOptGroup.addChild(new StatementPatternNode(
                            new VarNode("_var10"), new ConstantNode(rdfsLabel),
                            new VarNode("_var2"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
                    
                    complexOptGroup.addChild(new StatementPatternNode(
                            new ConstantNode(scopeBGP), new ConstantNode(
                                    chunkCapacity), new ConstantNode(
                                    chunkCapacityValue), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
                }

                final GroupByNode groupByNode = new GroupByNode();
                given.setGroupBy(groupByNode);
                groupByNode.addGroupByVar(new VarNode("_var2"));
                groupByNode.addGroupByVar(new VarNode("_var3"));

            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                    QueryType.SELECT, namedSet1);
            expected.getNamedSubqueriesNotNull().add(nsr);

            {
                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);

                projection.setDistinct(true);
                projection.addProjectionVar(new VarNode("_var3"));
            }

            final JoinGroupNode whereClause = new JoinGroupNode();
            nsr.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("_var3"),
                    new ConstantNode(a), new ConstantNode(politician),
                    null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("_var3"),
                    new ConstantNode(hasRole), new VarNode("_var6"),
                    null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("_var6"),
                    new ConstantNode(party), new ConstantNode(democrat),
                    null/* c */, Scope.DEFAULT_CONTEXTS));

        }

        // Main Query
        {
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);

            projection
                    .addProjectionExpression(new AssignmentNode(//
                            new VarNode("_var1"),// var
                            new FunctionNode(
                                    // expr
                                    FunctionRegistry.SAMPLE,
                                    Collections.singletonMap(
                                            AggregateBase.Annotations.DISTINCT,
                                            (Object) Boolean.FALSE)/* scalarValues */,
                                    new ValueExpressionNode[] { new VarNode(
                                            "_var9") })));
            projection.addProjectionVar(new VarNode("_var2"));
            projection.addProjectionVar(new VarNode("_var3"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new NamedSubqueryInclude(namedSet1));

            final JoinGroupNode simpleOptGroup = new JoinGroupNode(
                    true/* optional */, new StatementPatternNode(new VarNode(
                            "_var3"), new ConstantNode(name), new VarNode(
                            "_var9"), null/* c */, Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(simpleOptGroup);

            final JoinGroupNode complexOptGroup = new JoinGroupNode(true/* optional */);
            whereClause.addChild(complexOptGroup);
            {
                complexOptGroup.addChild(new StatementPatternNode(new VarNode(
                        "_var10"), new ConstantNode(votedBy), new VarNode(
                        "_var3"), null/* c */, Scope.DEFAULT_CONTEXTS));

                final StatementPatternNode sp = new StatementPatternNode(
                        new VarNode("_var10"), new ConstantNode(rdfsLabel),
                        new VarNode("_var2"), null/* c */,
                        Scope.DEFAULT_CONTEXTS);
                complexOptGroup.addChild(sp);
                sp.setQueryHint(BufferAnnotations.CHUNK_CAPACITY, "200");
              
            }

            final GroupByNode groupByNode = new GroupByNode();
            expected.setGroupBy(groupByNode);
            groupByNode.addGroupByVar(new VarNode("_var2"));
            groupByNode.addGroupByVar(new VarNode("_var3"));

        }

        final IASTOptimizer rewriter = new ASTQueryHintOptimizer();

        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);
        
    }

    /**
     * Test developed around a query which did not make progress through the
     * {@link ASTQueryHintOptimizer}.
     * 
     * <pre>
     * PREFIX hint: <http://www.bigdata.com/queryHints#>
     * SELECT (COUNT(?_var3) as ?count)
     * WHERE{
     *   hint:Query hint:com.bigdata.rdf.sparql.ast.QueryHints.optimizer "None" .
     *   GRAPH ?g {
     *     ?_var10 a <http://www.rdfabout.com/rdf/schema/vote/Option>. # 315k, 300ms for AP scan.
     *     ?_var10 <http://www.rdfabout.com/rdf/schema/vote/votedBy> ?_var3 . #2M, 17623ms for AP scan.
     *     hint:BGP hint:com.bigdata.rdf.sparql.ast.eval.hashJoin "true" . # use a hash join.
     *     hint:BGP hint:com.bigdata.bop.IPredicate.keyOrder "PCSO" . # use a specific index (default is POCS)
     *   }
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_query_hints_2() throws Exception {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV option = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/vote/Option"));
        @SuppressWarnings("rawtypes")
        final IV votedBy = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/vote/votedBy"));

        @SuppressWarnings("rawtypes")
        final IV scopeQuery = makeIV(new URIImpl(QueryHints.NAMESPACE+ QueryHintScope.Query));
        @SuppressWarnings("rawtypes")
        final IV scopeBGP = makeIV(new URIImpl(QueryHints.NAMESPACE+ QueryHintScope.BGP));
        @SuppressWarnings("rawtypes")
        final IV optimizer = makeIV(new URIImpl(QueryHints.NAMESPACE+QueryHints.OPTIMIZER));
        @SuppressWarnings("rawtypes")
        final IV hashJoin = makeIV(new URIImpl(QueryHints.NAMESPACE+AST2BOpBase.Annotations.HASH_JOIN));
        @SuppressWarnings("rawtypes")
        final IV none = makeIV(new LiteralImpl("None"));
        @SuppressWarnings("rawtypes")
        final IV t = makeIV(new LiteralImpl("true"));
        @SuppressWarnings("rawtypes")
        final IV keyOrder = makeIV(new URIImpl(QueryHints.NAMESPACE+IPredicate.Annotations.KEY_ORDER));
        @SuppressWarnings("rawtypes")
        final IV pcso = makeIV(new LiteralImpl("PCSO"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // Main Query
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection
                        .addProjectionExpression(new AssignmentNode(//
                                new VarNode("count"),// var
                                new FunctionNode(
                                        // expr
                                        FunctionRegistry.COUNT,
                                        Collections
                                                .singletonMap(
                                                        AggregateBase.Annotations.DISTINCT,
                                                        (Object) Boolean.FALSE)/* scalarValues */,
                                        new ValueExpressionNode[] { new VarNode(
                                                "_var3") })));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.setContext(new VarNode("g"));
                
                whereClause.addChild(new StatementPatternNode(new ConstantNode(
                        scopeQuery), new ConstantNode(optimizer),
                        new ConstantNode(none), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode graphGroup = new JoinGroupNode();
                whereClause.addChild(graphGroup);
                {

                    graphGroup.addChild(new StatementPatternNode(new VarNode(
                            "_var10"), new ConstantNode(a), new ConstantNode(
                            option), new VarNode("g"), Scope.NAMED_CONTEXTS));

                    graphGroup.addChild(new StatementPatternNode(new VarNode(
                            "_var10"), new ConstantNode(votedBy), new VarNode(
                            "_var3"), new VarNode("g"), Scope.NAMED_CONTEXTS));

                    graphGroup.addChild(new StatementPatternNode(
                            new ConstantNode(scopeBGP), new ConstantNode(
                                    hashJoin), new ConstantNode(t),
                            new VarNode("g"), Scope.NAMED_CONTEXTS));

                    graphGroup.addChild(new StatementPatternNode(
                            new ConstantNode(scopeBGP), new ConstantNode(
                                    keyOrder), new ConstantNode(pcso),
                            new VarNode("g"), Scope.NAMED_CONTEXTS));

                }

            }

        }
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {            // Main Query

            expected.setQueryHint(QueryHints.OPTIMIZER, "None");

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection
                        .addProjectionExpression(new AssignmentNode(//
                                new VarNode("count"),// var
                                new FunctionNode(
                                        // expr
                                        FunctionRegistry.COUNT,
                                        Collections
                                                .singletonMap(
                                                        AggregateBase.Annotations.DISTINCT,
                                                        (Object) Boolean.FALSE)/* scalarValues */,
                                        new ValueExpressionNode[] { new VarNode(
                                                "_var3") })));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);
                whereClause.setQueryHint(QueryHints.OPTIMIZER, "None");

                whereClause.setContext(new VarNode("g"));

                final JoinGroupNode graphGroup = new JoinGroupNode();
                whereClause.addChild(graphGroup);
                graphGroup.setQueryHint(QueryHints.OPTIMIZER, "None");
                {

                    final StatementPatternNode sp1 = new StatementPatternNode(
                            new VarNode("_var10"), new ConstantNode(a),
                            new ConstantNode(option), new VarNode("g"),
                            Scope.NAMED_CONTEXTS);
                    graphGroup.addChild(sp1);
                    sp1.setQueryHint(QueryHints.OPTIMIZER, "None");

                    final StatementPatternNode sp2 = new StatementPatternNode(new VarNode(
                            "_var10"), new ConstantNode(votedBy), new VarNode(
                            "_var3"), new VarNode("g"), Scope.NAMED_CONTEXTS);
                    graphGroup.addChild(sp2);
                    sp2.setQueryHint(QueryHints.OPTIMIZER, "None");
                    sp2.setQueryHint(AST2BOpBase.Annotations.HASH_JOIN, "true");
                    sp2.setQueryHint(IPredicate.Annotations.KEY_ORDER, "PCSO");

                }

            }
            
        }

        final IASTOptimizer rewriter = new ASTQueryHintOptimizer();

        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
        
    /**
     * Test verifies that a query hint with "group" scope is applied to the
     * top-level group.
     * 
     * <pre>
     * PREFIX hint: <http://www.bigdata.com/queryHints#>
     * SELECT (COUNT(?_var3) as ?count)
     * WHERE {
     *   hint:Group hint:com.bigdata.rdf.sparql.ast.QueryHints.optimizer "None" .
     *   ?_var10 a <http://www.rdfabout.com/rdf/schema/vote/Option>. # 315k, 300ms for AP scan.
     *   ?_var10 <http://www.rdfabout.com/rdf/schema/vote/votedBy> ?_var3 . #2M, 17623ms for AP scan.
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_query_hints_3() throws Exception {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV option = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/vote/Option"));
        @SuppressWarnings("rawtypes")
        final IV votedBy = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/vote/votedBy"));

        @SuppressWarnings("rawtypes")
        final IV scopeGroup = makeIV(new URIImpl(QueryHints.NAMESPACE+ QueryHintScope.Group));
        @SuppressWarnings("rawtypes")
        final IV optimizer = makeIV(new URIImpl(QueryHints.NAMESPACE+QueryHints.OPTIMIZER));
        @SuppressWarnings("rawtypes")
        final IV none = makeIV(new LiteralImpl("None"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // Main Query
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection
                        .addProjectionExpression(new AssignmentNode(//
                                new VarNode("count"),// var
                                new FunctionNode(
                                        // expr
                                        FunctionRegistry.COUNT,
                                        Collections
                                                .singletonMap(
                                                        AggregateBase.Annotations.DISTINCT,
                                                        (Object) Boolean.FALSE)/* scalarValues */,
                                        new ValueExpressionNode[] { new VarNode(
                                                "_var3") })));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new ConstantNode(
                        scopeGroup), new ConstantNode(optimizer),
                        new ConstantNode(none), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var10"), new ConstantNode(a),
                        new ConstantNode(option), new VarNode("g"),
                        Scope.NAMED_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var10"), new ConstantNode(votedBy), new VarNode(
                        "_var3"), new VarNode("g"), Scope.NAMED_CONTEXTS));

            }

        }
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {            // Main Query

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection
                        .addProjectionExpression(new AssignmentNode(//
                                new VarNode("count"),// var
                                new FunctionNode(
                                        // expr
                                        FunctionRegistry.COUNT,
                                        Collections
                                                .singletonMap(
                                                        AggregateBase.Annotations.DISTINCT,
                                                        (Object) Boolean.FALSE)/* scalarValues */,
                                        new ValueExpressionNode[] { new VarNode(
                                                "_var3") })));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);
                whereClause.setQueryHint(QueryHints.OPTIMIZER, "None");
                {

                    final StatementPatternNode sp1 = new StatementPatternNode(
                            new VarNode("_var10"), new ConstantNode(a),
                            new ConstantNode(option), new VarNode("g"),
                            Scope.NAMED_CONTEXTS);
                    whereClause.addChild(sp1);
                    sp1.setQueryHint(QueryHints.OPTIMIZER, "None");

                    final StatementPatternNode sp2 = new StatementPatternNode(new VarNode(
                            "_var10"), new ConstantNode(votedBy), new VarNode(
                            "_var3"), new VarNode("g"), Scope.NAMED_CONTEXTS);
                    whereClause.addChild(sp2);
                    sp2.setQueryHint(QueryHints.OPTIMIZER, "None");

                }

            }
            
        }

        final IASTOptimizer rewriter = new ASTQueryHintOptimizer();

        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

    }
        
    /**
     * Test verifies that we can enable native DISTINCT, native HASH JOINS, and
     * MERGE JOINs using query hints.
     * 
     * <pre>
     * PREFIX hint: <http://www.bigdata.com/queryHints#>
     * SELECT (COUNT(?_var3) as ?count)
     * WHERE {
     *   hint:Query hint:com.bigdata.rdf.sparql.ast.QueryHints.nativeDistinct "true" .
     *   hint:Query hint:com.bigdata.rdf.sparql.ast.QueryHints.nativeHashJoins "true" .
     *   hint:Query hint:com.bigdata.rdf.sparql.ast.QueryHints.mergeJoins "true" .
     *   ?_var10 a <http://www.rdfabout.com/rdf/schema/vote/Option>.
     *   ?_var10 <http://www.rdfabout.com/rdf/schema/vote/votedBy> ?_var3.
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_query_hints_4() throws Exception {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV option = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/vote/Option"));
        @SuppressWarnings("rawtypes")
        final IV votedBy = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/vote/votedBy"));

        @SuppressWarnings("rawtypes")
        final IV scopeGroup = makeIV(new URIImpl(QueryHints.NAMESPACE+ QueryHintScope.Group));
        @SuppressWarnings("rawtypes")
        final IV nativeDistinct = makeIV(new URIImpl(QueryHints.NAMESPACE+QueryHints.NATIVE_DISTINCT));
        @SuppressWarnings("rawtypes")
        final IV nativeHashJoins= makeIV(new URIImpl(QueryHints.NAMESPACE+QueryHints.NATIVE_HASH_JOINS));
        @SuppressWarnings("rawtypes")
        final IV mergeJoin = makeIV(new URIImpl(QueryHints.NAMESPACE+QueryHints.MERGE_JOIN));
        @SuppressWarnings("rawtypes")
        final IV t = makeIV(new LiteralImpl("true"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            // Main Query
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection
                        .addProjectionExpression(new AssignmentNode(//
                                new VarNode("count"),// var
                                new FunctionNode(
                                        // expr
                                        FunctionRegistry.COUNT,
                                        Collections
                                                .singletonMap(
                                                        AggregateBase.Annotations.DISTINCT,
                                                        (Object) Boolean.FALSE)/* scalarValues */,
                                        new ValueExpressionNode[] { new VarNode(
                                                "_var3") })));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new ConstantNode(
                        scopeGroup), new ConstantNode(nativeDistinct),
                        new ConstantNode(t), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new ConstantNode(
                        scopeGroup), new ConstantNode(nativeHashJoins),
                        new ConstantNode(t), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new ConstantNode(
                        scopeGroup), new ConstantNode(mergeJoin),
                        new ConstantNode(t), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var10"), new ConstantNode(a),
                        new ConstantNode(option), new VarNode("g"),
                        Scope.NAMED_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var10"), new ConstantNode(votedBy), new VarNode(
                        "_var3"), new VarNode("g"), Scope.NAMED_CONTEXTS));

            }

        }
        
        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {            // Main Query

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection
                        .addProjectionExpression(new AssignmentNode(//
                                new VarNode("count"),// var
                                new FunctionNode(
                                        // expr
                                        FunctionRegistry.COUNT,
                                        Collections
                                                .singletonMap(
                                                        AggregateBase.Annotations.DISTINCT,
                                                        (Object) Boolean.FALSE)/* scalarValues */,
                                        new ValueExpressionNode[] { new VarNode(
                                                "_var3") })));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);
                {

                    final StatementPatternNode sp1 = new StatementPatternNode(
                            new VarNode("_var10"), new ConstantNode(a),
                            new ConstantNode(option), new VarNode("g"),
                            Scope.NAMED_CONTEXTS);
                    whereClause.addChild(sp1);

                    final StatementPatternNode sp2 = new StatementPatternNode(new VarNode(
                            "_var10"), new ConstantNode(votedBy), new VarNode(
                            "_var3"), new VarNode("g"), Scope.NAMED_CONTEXTS);
                    whereClause.addChild(sp2);

                }

            }
            
        }

        final IASTOptimizer rewriter = new ASTQueryHintOptimizer();

        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);
        
        // Turn them off before hand.
        context.nativeDistinct = false;
        context.nativeHashJoins = false;
        context.mergeJoin = false;

        final IQueryNode actual = rewriter.optimize(context,
                given/* queryNode */, bsets);

        // Verify true after.
        assertTrue(context.nativeDistinct);
        assertTrue(context.nativeHashJoins);
        assertTrue(context.mergeJoin);
        
        assertSameAST(expected, actual);

    }

}
