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
import java.util.LinkedList;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.TestSubQuery;

/**
 * Test suite for {@link ASTSparql11SubqueryOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTSparql11SubqueryOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTSparql11SubqueryOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTSparql11SubqueryOptimizer(String name) {
        super(name);
    }

    /**
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT * 
     * WHERE {
     *      ?s :p ?o .
     *      { 
     *         SELECT ?s { ?s a :ty } ORDER BY ?s LIMIT 3
     *      } 
     * }
     * </pre>
     * 
     * mroycsi wrote: Based on sparql bottom up evaluation, the subquery will
     * return s1,s2,s3 as the solutions for ?s. Joined with the ?s :p ?o, you
     * should only get the statements where ?s is s1,s2,s3.
     * <p>
     * I haven't debugged bigdata so I don't know exactly what it is doing, but
     * it seems that currently with the bigdata evaluation, for each solution
     * produced from ?s :p ?o, the subquery is run, and it seems that the ?s
     * binding in the subquery is getting constrained by the ?s from the inbound
     * solution, so results of the subquery are not always s1,s2,s3, depending
     * on the inbound solution.
     * <p>
     * thompsonbry wrote: Normally bottom up evaluation only differs when you
     * are missing a shared variable such that the bindings for variables having
     * the same name are actually not correlated.
     * <P>
     * This is a bit of an odd case with an interaction between the order/limit
     * and the as-bound evaluation which leads to the "wrong" result. We
     * probably do not want to always do bottom up evaluation for a subquery
     * (e.g., by lifting it into a named subquery). Are you suggesting that
     * there is this special case which needs to be recognized where the
     * subquery MUST be evaluated first because the order by/limit combination
     * means that the results of the outer query joined with the inner query
     * could be different in this case?
     * <p>
     * mroycsi wrote: This is [a] pattern that is well known and commonly used
     * with sparql 1.1 subqueries. It is definitely a case where the subquery
     * needs to be evaluated first due to the limit clause. The order clause
     * probably doesn't matter if there isn't a limit since all the results are
     * just joined, so order doesn't matter till the solution gets to the order
     * by operations.
     * <p>
     * thompsonbry wrote: Ok. ORDER BY by itself does not matter and neither
     * does LIMIT by itself. But if you have both it matters and we need to run
     * the subquery first.
     * 
     * @see TestSubQuery#test_sparql_subquery_limiting_resource_pattern()
     */
    public void test_subSelectWithLimitAndOrderBy() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://www.example.org/p"));
        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV ty = makeIV(new URIImpl("http://www.example.org/ty"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final SubqueryRoot subqueryRoot;
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode subGroup = new JoinGroupNode();
                whereClause.addChild(subGroup);

                subqueryRoot = new SubqueryRoot(QueryType.SELECT);
                subGroup.addChild(subqueryRoot);
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
             
                subqueryRoot
                        .setSlice(new SliceNode(0L/* offset */, 3L/* limit */));
                
                final OrderByNode orderByNode = new OrderByNode();
                subqueryRoot.setOrderBy(orderByNode);
                
                orderByNode
                        .addExpr(new OrderByExpr(new VarNode("s"), true/* asc */));
                
            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            // The name of the named subquery.
            final String name = "-subSelect-1";
            
            final NamedSubqueryRoot subqueryRoot;
            {
                
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final NamedSubqueryInclude nsi = new NamedSubqueryInclude(name);
                nsi.setAttachedJoinFilters(new LinkedList<FilterNode>());
                whereClause.addChild(nsi);

                subqueryRoot = new NamedSubqueryRoot(QueryType.SELECT,name);
                final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
                expected.setNamedSubqueries(namedSubqueries);
                namedSubqueries.add(subqueryRoot);
                
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
             
                subqueryRoot
                        .setSlice(new SliceNode(0L/* offset */, 3L/* limit */));
                
                final OrderByNode orderByNode = new OrderByNode();
                subqueryRoot.setOrderBy(orderByNode);
                
                orderByNode
                        .addExpr(new OrderByExpr(new VarNode("s"), true/* asc */));
                
            }

        }

        final IASTOptimizer rewriter = new ASTSparql11SubqueryOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);
        
    }

    /**
     * Unit test verifies that a sub-select involving an aggregation is lifted
     * into a named subquery. This typically provides more efficient evaluation
     * than repeated as-bound evaluation of the sub-select. It also prevents
     * inappropriate sharing of the internal state of the {@link IAggregate}
     * functions.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT * 
     * WHERE {
     *      ?s :p ?o .
     *      { 
     *         SELECT s? (COUNT(?s) as ?x) { ?s a :ty } GROUP BY ?s
     *      } 
     * }
     * </pre>
     */
    public void test_subSelectWithAggregation() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://www.example.org/p"));
        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV ty = makeIV(new URIImpl("http://www.example.org/ty"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final SubqueryRoot subqueryRoot;
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));
                projection.addProjectionVar(new VarNode("x"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode subGroup = new JoinGroupNode();
                whereClause.addChild(subGroup);

                subqueryRoot = new SubqueryRoot(QueryType.SELECT);
                subGroup.addChild(subqueryRoot);
                
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionExpression(new AssignmentNode(
                        new VarNode("x"), new FunctionNode(
                                FunctionRegistry.COUNT,
                                Collections.singletonMap(
                                        AggregateBase.Annotations.DISTINCT,
                                        (Object) Boolean.TRUE),
                                new VarNode("s"))));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
                
                final GroupByNode groupByNode = new GroupByNode();
                subqueryRoot.setGroupBy(groupByNode);
                groupByNode.addGroupByVar(new VarNode("s"));
                
            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            // The name of the named subquery.
            final String name = "-subSelect-1";
            
            final NamedSubqueryRoot subqueryRoot;
            {
                
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));
                projection.addProjectionVar(new VarNode("x"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final NamedSubqueryInclude nsi = new NamedSubqueryInclude(name);
                nsi.setAttachedJoinFilters(new LinkedList<FilterNode>());
                whereClause.addChild(nsi);

                subqueryRoot = new NamedSubqueryRoot(QueryType.SELECT,name);
                final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
                expected.setNamedSubqueries(namedSubqueries);
                namedSubqueries.add(subqueryRoot);
                
                final GroupByNode groupByNode = new GroupByNode();
                subqueryRoot.setGroupBy(groupByNode);
                groupByNode.addGroupByVar(new VarNode("s"));
                
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionExpression(new AssignmentNode(
                        new VarNode("x"), new FunctionNode(
                                FunctionRegistry.COUNT,
                                Collections.singletonMap(
                                        AggregateBase.Annotations.DISTINCT,
                                        (Object) Boolean.TRUE),
                                new VarNode("s"))));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
             
            }

        }

        final IASTOptimizer rewriter = new ASTSparql11SubqueryOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * Unit test verifies that a sub-select is not lifted when there is no
     * reason to lift that sub-select.
     * <p>
     * The following query is used:
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT * 
     * WHERE {
     *      ?s :p ?o .
     *      { 
     *         SELECT ?s { ?s a :ty }
     *      } 
     * }
     * </pre>
     * 
     * Note: <code>?s</code> is both bound by the statement pattern and is
     * projected by the sub-select so it will be a join variable.
     * <p>
     * Note: the sub-select does not meet any of the other criteria for being
     * lifted (it does not use LIMIT + ORDER BY and it is not an aggregate).
     */
    public void test_subSelectNotLifted() {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://www.example.org/p"));
        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV ty = makeIV(new URIImpl("http://www.example.org/ty"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final SubqueryRoot subqueryRoot;
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode subGroup = new JoinGroupNode();
                whereClause.addChild(subGroup);

                subqueryRoot = new SubqueryRoot(QueryType.SELECT);
                subGroup.addChild(subqueryRoot);
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
                
            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final SubqueryRoot subqueryRoot;
            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode subGroup = new JoinGroupNode();
                whereClause.addChild(subGroup);

                subqueryRoot = new SubqueryRoot(QueryType.SELECT);
                subGroup.addChild(subqueryRoot);
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
                
            }

        }

        final IASTOptimizer rewriter = new ASTSparql11SubqueryOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }
    
    /**
     * Unit test verifies that we lift out a {@link SubqueryRoot} which is
     * marked by the {@link SubqueryRoot.Annotations#RUN_ONCE} annotation. This
     * uses the same query as the test above, but the RUN_ONCE annotation is
     * explicitly set on the {@link SubqueryRoot}.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT * 
     * WHERE {
     *      ?s :p ?o .
     *      { 
     *         SELECT ?s { ?s a :ty }
     *      } 
     * }
     * </pre>
     */
    public void test_subSelectWithRunOnceAnnotation() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://www.example.org/p"));
        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV ty = makeIV(new URIImpl("http://www.example.org/ty"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final SubqueryRoot subqueryRoot;
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode subGroup = new JoinGroupNode();
                whereClause.addChild(subGroup);

                subqueryRoot = new SubqueryRoot(QueryType.SELECT);
                subGroup.addChild(subqueryRoot);
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                // Set the query hint.
                subqueryRoot.setRunOnce(true);
                
            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            // The name of the named subquery.
            final String name = "-subSelect-1";
            
            final NamedSubqueryRoot subqueryRoot;
            {
                
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final NamedSubqueryInclude nsi = new NamedSubqueryInclude(name);
                nsi.setAttachedJoinFilters(new LinkedList<FilterNode>());
                whereClause.addChild(nsi);

                subqueryRoot = new NamedSubqueryRoot(QueryType.SELECT,name);
                final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
                expected.setNamedSubqueries(namedSubqueries);
                namedSubqueries.add(subqueryRoot);
                
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
             
            }

        }

        final IASTOptimizer rewriter = new ASTSparql11SubqueryOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }
    
    /**
     * Unit test verifies that we lift out a {@link SubqueryRoot} which does not
     * share any variables with the join group in which it appears. Since there
     * is nothing that will be projected into the sub-select, it needs to be
     * lifted out (or run before the statement pattern join) or it will run
     * as-bound for each source solution.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT * 
     * WHERE {
     *      ?s :p ?o .
     *      { 
     *         SELECT ?s1 { ?s1 a :ty }
     *      } 
     * }
     * </pre>
     */
    public void test_subSelectWithNoJoinVars() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV p = makeIV(new URIImpl("http://www.example.org/p"));
        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV ty = makeIV(new URIImpl("http://www.example.org/ty"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final SubqueryRoot subqueryRoot;
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode subGroup = new JoinGroupNode();
                whereClause.addChild(subGroup);

                subqueryRoot = new SubqueryRoot(QueryType.SELECT);
                subGroup.addChild(subqueryRoot);
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s1"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s1"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            // The name of the named subquery.
            final String name = "-subSelect-1";
            
            final NamedSubqueryRoot subqueryRoot;
            {
                
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.addProjectionVar(new VarNode("s"));
                projection.addProjectionVar(new VarNode("o"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                        new ConstantNode(p), new VarNode("o"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new NamedSubqueryInclude(name));

                subqueryRoot = new NamedSubqueryRoot(QueryType.SELECT,name);
                final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
                expected.setNamedSubqueries(namedSubqueries);
                namedSubqueries.add(subqueryRoot);
                
            }
            {
                final ProjectionNode projection = new ProjectionNode();
                subqueryRoot.setProjection(projection);
                
                projection.addProjectionVar(new VarNode("s1"));
            
                final JoinGroupNode whereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode("s1"),
                        new ConstantNode(a), new ConstantNode(ty), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
             
            }

        }

        final IASTOptimizer rewriter = new ASTSparql11SubqueryOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        /*
         * FIXME This failing because we are not computing the join variables
         * correctly (the analysis needs to depend on the actual evaluation
         * order, and that needs to be decided either by heuristics or by the
         * RTO). Since we are not computing the join variables correctly I have
         * disabled the code in the AST optimizer for SPARQL 1.1 subqueries to
         * NOT lift sub-selects for which no join variables are predicted. Since
         * it can not predict the join variables correctly, it is actually
         * lifting everything when that code is enabled.
         */
        if (!BigdataStatics.runKnownBadTests)
            return;

        assertSameAST(expected, actual);

    }

}
