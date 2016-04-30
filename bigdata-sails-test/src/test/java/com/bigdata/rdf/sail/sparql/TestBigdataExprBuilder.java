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
 * Created on Aug 20, 2011
 */
package com.bigdata.rdf.sail.sparql;

import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.impl.DatasetImpl;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Test suite for {@link BigdataExprBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBigdataExprBuilder.java 5073 2011-08-23 00:33:54Z
 *          thompsonbry $
 */
public class TestBigdataExprBuilder extends AbstractBigdataExprBuilderTestCase {

//    private static final Logger log = Logger
//            .getLogger(TestBigdataExprBuilder.class);
    
    public TestBigdataExprBuilder() {
    }

    public TestBigdataExprBuilder(String name) {
        super(name);
    }

    /**
     * Unit test for simple SELECT query
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_select_s_where_s_p_o() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SELECT DISTINCT
     * 
     * <pre>
     * SELECT DISTINCT ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_select_distinct() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select DISTINCT ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.setDistinct(true);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SELECT REDUCED
     * 
     * <pre>
     * SELECT REDUCED ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_select_reduced() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select REDUCED ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            projection.setReduced(true);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for GROUP BY in SELECT query with a bare variable in the group
     * by clause.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o} GROUP BY ?o
     * </pre>
     * 
     * MP: Pretty sure this is an illegal query?
     */
    public void test_groupBy_bareVar() throws MalformedQueryException, TokenMgrError,
            ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} GROUP BY ?o";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final GroupByNode groupBy = new GroupByNode();
            expected.setGroupBy(groupBy);
            groupBy.addExpr(new AssignmentNode(new VarNode("o"), new VarNode(
                    "o")));

        }

        /*
         * We can't ask the parser to parse this query anymore (it's an illegal
         * aggregation query) because the parser will throw an error now that
         * it checks for these sorts of things.
         */
//        final QueryRoot actual = parse(sparql, baseURI);
//
//        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for GROUP BY in SELECT query with BIND(expr AS var).
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o} GROUP BY (?o AS ?z)
     * </pre>
     * 
     * MP: Pretty sure this is an illegal query?
     */
    public void test_groupBy_bindExpr() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} GROUP BY (?o AS ?z)";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final GroupByNode groupBy = new GroupByNode();
            expected.setGroupBy(groupBy);
            groupBy.addExpr(//
                    new AssignmentNode(new VarNode("z"),
                    (IValueExpressionNode) new VarNode("o"))//
                    );

        }

        /*
         * We can't ask the parser to parse this query anymore (it's an illegal
         * aggregation query) because the parser will throw an error now that
         * it checks for these sorts of things.
         */
//        final QueryRoot actual = parse(sparql, baseURI);
//
//        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for GROUP BY in SELECT query with function call without "AS".
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o} GROUP BY str(?o)
     * </pre>
     * 
     * MP: Pretty sure this is an illegal query?
     */
    public void test_groupBy_functionCall() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} GROUP BY str(?o)";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final GroupByNode groupBy = new GroupByNode();
            expected.setGroupBy(groupBy);
            final FunctionNode funct = new FunctionNode(
                    FunctionRegistry.STR, null/* scalarValues */,
                    new ValueExpressionNode[] { new VarNode("o") });
            // Note: anonymous variable.
            final VarNode anonvar1 = new VarNode("-groupBy-1");
            anonvar1.setAnonymous(true);
            groupBy.addExpr(new AssignmentNode(anonvar1, funct));

        }

        /*
         * We can't ask the parser to parse this query anymore (it's an illegal
         * aggregation query) because the parser will throw an error now that
         * it checks for these sorts of things.
         */
//        final QueryRoot actual = parse(sparql, baseURI);
//
//        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for HAVING clause (with an implicit group consisting of all
     * solutions).
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o} HAVING (?o GT ?s)
     * </pre>
     * 
     * MP: Pretty sure this is an illegal query?
     */
    public void test_having() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} HAVING (?o > ?s)";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final HavingNode having = new HavingNode();
            expected.setHaving(having);
            having.addExpr(new FunctionNode( //
                    FunctionRegistry.GT,//
                    null, // scalarValues
                    new ValueExpressionNode[] {// args
                    new VarNode("o"), new VarNode("s") })//
            );
            
        }

        /*
         * We can't ask the parser to parse this query anymore (it's an illegal
         * aggregation query) because the parser will throw an error now that
         * it checks for these sorts of things.
         */
//        final QueryRoot actual = parse(sparql, baseURI);
//
//        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for ORDER BY in SELECT query.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o} ORDER BY DESC(?s)
     * </pre>
     */
    public void test_orderBy() throws MalformedQueryException, TokenMgrError,
            ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} ORDER BY DESC(?s)";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final OrderByNode orderBy = new OrderByNode();
            expected.setOrderBy(orderBy);
            orderBy.addExpr(new OrderByExpr(new VarNode("s"), false/* ascending */));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for ORDER BY in SELECT query using a value expression rather
     * than a bare variable (this exercises the delegation to the
     * {@link ValueExprBuilder}).
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o} ORDER BY DESC(str(?s))
     * </pre>
     */
    public void test_orderBy_expr() throws MalformedQueryException, TokenMgrError,
            ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} ORDER BY DESC(str(?s))";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final OrderByNode orderBy = new OrderByNode();
            expected.setOrderBy(orderBy);
            final FunctionNode funct = new FunctionNode(
                    FunctionRegistry.STR, null/* scalarValues */,
                    new ValueExpressionNode[] { new VarNode("s") });
            orderBy.addExpr(new OrderByExpr(funct, false/* ascending */));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for SELECT query with a wildcard (<code>*</code>).
     * 
     * <pre>
     * SELECT * where {?s ?p ?o}
     * </pre>
     */
    public void test_select_star() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select * where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for SLICE in SELECT query.
     * <pre>
     * SELECT ?s where {?s ?p ?o} LIMIT 10 OFFSET 5
     * </pre>
     * 
     * @throws MalformedQueryException
     * @throws TokenMgrError
     * @throws ParseException
     */
    public void test_slice() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o} limit 10 offset 5";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
            
            final SliceNode slice = new SliceNode();
            expected.setSlice(slice);
            slice.setLimit(10);
            slice.setOffset(5);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple ASK query. (Applications can use the ASK form to
     * test whether or not a query pattern has a solution. No information is
     * returned about the possible query solutions, just whether or not a
     * solution exists. EXISTS() is basically an ASK subquery.)
     * 
     * <pre>
     * ASK where {?s ?p ?o}
     * </pre>
     */
    public void test_ask() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "ask where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.ASK);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            /*
             * Note: No projection.
             */
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            expected.setSlice(new SliceNode(0L/* offset */, 1L/* limit */));

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple DESCRIBE query. (The main differences between
     * DESCRIBE and SELECT is that the DESCRIBE query allows only a simple list
     * of variables or IRIs in place of the select expressions and involves an
     * implicit CONSTRUCT. Both DESCRIBE and SELECT allow the same solution
     * modifiers.)
     * 
     * <pre>
     * DESCRIBE ?s where {?s ?p ?o}
     * </pre>
     * 
     * Note: The DESCRIBE projection and where class of the DESCRIBE query are
     * modeled directly, but this does not capture the semantics of the DESCRIBE
     * query. You MUST run an {@link IASTOptimizer} to rewrite the
     * {@link QueryBase} in order to capture the semantics of a DESCRIBE query.
     */
    public void test_describe() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "describe ?s where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.DESCRIBE);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);
        
    }
    
    /**
     * Unit test for <code>DESCRIBE *</code> query.
     * 
     * <pre>
     * DESCRIBE * where {?s ?p ?o}
     * </pre>
     * 
     * Note: The DESCRIBE projection and where class of the DESCRIBE query are
     * modeled directly, but this does not capture the semantics of the DESCRIBE
     * query. You MUST run an {@link IASTOptimizer} to rewrite the
     * {@link QueryBase} in order to capture the semantics of a DESCRIBE query.
     */
    public void test_describe_star() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "describe * where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.DESCRIBE);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("*"));
            expected.setProjection(projection);
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for <code>DESCRIBE</code> query for an IRI.
     * <p>
     * Note: There is no "where clause" for this query. One must be added when
     * the query semantics of describe are imposed by an AST rewrite.
     * 
     * <pre>
     * DESCRIBE <http://www.bigdata.com>
     * </pre>
     * 
     * Note: The DESCRIBE projection and where class of the DESCRIBE query are
     * modeled directly, but this does not capture the semantics of the DESCRIBE
     * query. You MUST run an {@link IASTOptimizer} to rewrite the
     * {@link QueryBase} in order to capture the semantics of a DESCRIBE query.
     */
    public void test_describe_iri() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "describe <http://www.bigdata.com>";

        final QueryRoot expected = new QueryRoot(QueryType.DESCRIBE);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            
            final VarNode anonvar = new VarNode("-iri-1");
            anonvar.setAnonymous(true);
            projection.addProjectionExpression(new AssignmentNode(anonvar,
                    new ConstantNode(makeIV(valueFactory
                            .createURI("http://www.bigdata.com")))));

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for <code>DESCRIBE</code> query where a mixture of variables
     * and IRIs are used in the projection.
     * 
     * <pre>
     * DESCRIBE ?s <http://www.bigdata.com> where {?s ?p ?o}
     * </pre>
     * 
     * Note: The DESCRIBE projection and where class of the DESCRIBE query are
     * modeled directly, but this does not capture the semantics of the DESCRIBE
     * query. You MUST run an {@link IASTOptimizer} to rewrite the
     * {@link QueryBase} in order to capture the semantics of a DESCRIBE query.
     */
    public void test_describe_vars_and_iris() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "describe ?s <http://www.bigdata.com> where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.DESCRIBE);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.addProjectionVar(new VarNode("s"));
            final VarNode anonvar = new VarNode("-iri-1");
            anonvar.setAnonymous(true);
            projection.addProjectionExpression(new AssignmentNode(anonvar,
                    new ConstantNode(makeIV(valueFactory
                            .createURI("http://www.bigdata.com")))));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);
    
    }
    
    /**
     * Unit test for simple CONSTRUCT query.
     * 
     * <pre>
     * CONSTRUCT ?s where {?s ?p ?o}
     * </pre>
     * 
     * TODO Anzo has an extension of CONSTRUCT for quads which we should also
     * support. It allows a GRAPH graph pattern to be mixed in with the triple
     * patterns.
     */
    public void test_construct() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "construct { ?s ?p ?o } where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            final ConstructNode construct = new ConstructNode();
            expected.setConstruct(construct);
            construct.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * A construct query with some constants in the template (not ground
     * triples, just RDF Values).
     * 
     * @throws MalformedQueryException
     * @throws TokenMgrError
     * @throws ParseException
     */
    public void test_construct_with_ground_terms()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "" +
        		"PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
        		"construct { ?s rdf:type ?o }\n" +
        		"where {?s rdf:type ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("rdf", RDF.NAMESPACE);
                expected.setPrefixDecls(prefixDecls);
            }

            final ConstructNode construct = new ConstructNode();
            expected.setConstruct(construct);
            construct.addChild(new StatementPatternNode(new VarNode("s"),
                    new ConstantNode(makeIV(valueFactory.createURI(RDF.TYPE
                            .toString()))), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new ConstantNode(makeIV(valueFactory.createURI(RDF.TYPE
                            .toString()))), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * The construct where shortcut form.
     * 
     * @throws MalformedQueryException
     * @throws TokenMgrError
     * @throws ParseException
     */
    public void test_construct_where_shortcut()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "" +
                "PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "construct where {?s rdf:type ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("rdf", RDF.NAMESPACE);
                expected.setPrefixDecls(prefixDecls);
            }

            final ConstructNode construct = new ConstructNode();
            expected.setConstruct(construct);
            construct.addChild(new StatementPatternNode(new VarNode("s"),
                    new ConstantNode(makeIV(valueFactory.createURI(RDF.TYPE
                            .toString()))), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new ConstantNode(makeIV(valueFactory.createURI(RDF.TYPE
                            .toString()))), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * SELECT ?who ?g ?mbox
     * FROM <http://example.org/dft.ttl>
     * FROM NAMED <http://example.org/alice>
     * FROM NAMED <http://example.org/bob>
     * WHERE
     * {
     *    ?g dc:publisher ?who .
     *    GRAPH ?g { ?x foaf:mbox ?mbox }
     * }
     * </pre>
     * 
     * @throws ParseException
     * @throws TokenMgrError
     * @throws MalformedQueryException
     */
    public void test_from_and_from_named() throws MalformedQueryException,
            TokenMgrError, ParseException {
        
        final String sparql = "" + //
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + //
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + //
                "SELECT ?who ?g ?mbox\n" + //
                "FROM <http://example.org/dft.ttl>\n" + //
                "FROM NAMED <http://example.org/alice>\n" + //
                "FROM NAMED <http://example.org/bob>\n" + //
                "WHERE {\n" + //
                "    ?g dc:publisher ?who .\n" + //
                "    GRAPH ?g { ?x foaf:mbox ?mbox } \n" + //
                "}"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("foaf", FOAFVocabularyDecl.NAMESPACE);
                prefixDecls.put("dc", DC.NAMESPACE);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.addProjectionVar(new VarNode("who"));
                projection.addProjectionVar(new VarNode("g"));
                projection.addProjectionVar(new VarNode("mbox"));
            }

            {
                final BigdataURI uri1 = valueFactory
                        .createURI("http://example.org/dft.ttl");

                final BigdataURI uri2 = valueFactory
                        .createURI("http://example.org/alice");
                
                final BigdataURI uri3 = valueFactory
                        .createURI("http://example.org/bob"); 

                final BigdataValue[] values = new BigdataValue[] { uri1, uri2,
                        uri3 };
                
                tripleStore.getLexiconRelation().addTerms(values,
                        values.length, false/* readOnly */);

                final DatasetImpl dataset = new DatasetImpl();
                dataset.addDefaultGraph(uri1);
                dataset.addNamedGraph(uri2);
                dataset.addNamedGraph(uri3);
                final DatasetNode datasetNode = new DatasetNode(dataset, false/* update */);
                expected.setDataset(datasetNode);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);
             
                whereClause.addChild(new StatementPatternNode(new VarNode("g"),
                        new ConstantNode(makeIV(valueFactory.createURI(DC.PUBLISHER
                                .toString()))), new VarNode("who"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
                
                final JoinGroupNode group = new JoinGroupNode();
                whereClause.addChild(group);
                group.setContext(new VarNode("g"));
                group.addChild(new StatementPatternNode(
                        new VarNode("x"),
                        new ConstantNode(makeIV(valueFactory
                                .createURI(FOAFVocabularyDecl.mbox.toString()))),
                        new VarNode("mbox"), new VarNode("g"),
                        Scope.NAMED_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * A variant of the above test where one of the URIs in the default / named
     * graph declarations is not a graph in the KB.
     * 
     * @throws MalformedQueryException
     * @throws TokenMgrError
     * @throws ParseException
     */
    public void test_from_and_from_named_with_unknown_graph()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + //
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + //
                "SELECT ?who ?g ?mbox\n" + //
                "FROM <http://example.org/dft.ttl>\n" + //
                "FROM NAMED <http://example.org/alice>\n" + //
                "FROM NAMED <http://example.org/bob>\n" + //
                "WHERE {\n" + //
                "    ?g dc:publisher ?who .\n" + //
                "    GRAPH ?g { ?x foaf:mbox ?mbox } \n" + //
                "}"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("foaf", FOAFVocabularyDecl.NAMESPACE);
                prefixDecls.put("dc", DC.NAMESPACE);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.addProjectionVar(new VarNode("who"));
                projection.addProjectionVar(new VarNode("g"));
                projection.addProjectionVar(new VarNode("mbox"));
            }

            {
                final BigdataURI uri1 = valueFactory
                        .createURI("http://example.org/dft.ttl");

                final BigdataURI uri2 = valueFactory
                        .createURI("http://example.org/alice");

                final BigdataURI uri3 = valueFactory
                        .createURI("http://example.org/bob");

                final BigdataValue[] values = new BigdataValue[] { //
                        uri1, //
                        uri2,//
//                        uri3 //
                };

                tripleStore.getLexiconRelation().addTerms(values,
                        values.length, false/* readOnly */);

                final DatasetImpl dataset = new DatasetImpl();
                dataset.addDefaultGraph(uri1);
                dataset.addNamedGraph(uri2);
                dataset.addNamedGraph(uri3);
                final DatasetNode datasetNode = new DatasetNode(dataset, false/* update */);
                expected.setDataset(datasetNode);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(new VarNode("g"),
                                new ConstantNode(makeIV(valueFactory
                                        .createURI(DC.PUBLISHER.toString()))),
                                new VarNode("who"), null/* c */,
                                Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode group = new JoinGroupNode();
                whereClause.addChild(group);
                group.setContext(new VarNode("g"));
                group.addChild(new StatementPatternNode(
                        new VarNode("x"),
                        new ConstantNode(makeIV(valueFactory
                                .createURI(FOAFVocabularyDecl.mbox.toString()))),
                        new VarNode("mbox"), new VarNode("g"),
                        Scope.NAMED_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for blank node "[]" syntax.
     * 
     * <pre>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT * {
     *   [ foaf:name ?name ;
     *     foaf:mbox <mailto:alice@example.org>
     *   ]
     * }
     * </pre>
     * 
     * Note that this has exactly the same interpretation as:
     * 
     * <pre>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT * {
     *   _:b18 foaf:name ?name .
     *   _:b18 foaf:mbox <mailto:alice@example.org>
     * }
     * </pre>
     * 
     * assuming that the blank node is given the identity <code>_:b18</code>.
     * <p>
     * Note: blank nodes are translated into anonymous variables in the parse
     * tree before the bigdata AST model is generated.
     */
    public void test_bnode_bracket_syntax_05() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + //
                " SELECT * {\n" + //
                "   [ foaf:name ?name ;\n" + //
                "     foaf:mbox <mailto:alice@example.org>\n" + //
                "   ]\n" + //
                " }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("foaf", FOAFVocabularyDecl.NAMESPACE);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("*"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final ConstantNode foaf_name = new ConstantNode(
                        makeIV(valueFactory.createURI(FOAFVocabularyDecl.name
                                .stringValue())));

                final ConstantNode foaf_mbox = new ConstantNode(
                        makeIV(valueFactory.createURI(FOAFVocabularyDecl.mbox
                                .stringValue())));

                final ConstantNode mailto = new ConstantNode(
                        makeIV(valueFactory
                                .createURI("mailto:alice@example.org")));

                final VarNode bnode = new VarNode("-anon-11");
                bnode.setAnonymous(true);

                whereClause.addChild(new StatementPatternNode(bnode, foaf_name,
                        new VarNode("name"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(bnode, foaf_mbox,
                        mailto, null/* c */, Scope.DEFAULT_CONTEXTS));

            }

        }

        final QueryRoot actual = parse(sparql, baseURI);
      
        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for the treatment of graph variables overshadowed
     * by sub-SELECTs.
     * 
     * <pre>
     * SELECT ?x WHERE {
     *   GRAPH ?g {
     *     {SELECT ?x WHERE {?x ?p ?g}}
     *   }
     * }
     * </pre>
     * 
     * Here SELECT ?x overshadows the variable from GRAPH ?g, 
     * so that ?g in ?x ?p ?g is actually a different variable, 
     * albeit with the same name.
     * 
     * The tested feature solves this by explicitly renaming ?g:
     * 
     * <pre>
     * SELECT ?x WHERE {
     *   GRAPH ?g {
     *     {SELECT ?x WHERE {?x ?p ?g_12345}}
     *   }
     * }
     * </pre>
     */
    public void test_subselect_under_graph_01() throws MalformedQueryException,
            TokenMgrError, ParseException {
    
        String sparql1 = "" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    {SELECT ?x WHERE {?x ?p ?g}}\n" + //
                " }\n" + //
                "}";
        
        String sparql2 = "" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    {SELECT ?x WHERE {?x ?p ?g_12345}}\n" + //
                " }\n" + //
                "}";
    
        final QueryRoot actual = parse(sparql1, baseURI);
        
        final QueryRoot expected = parse(sparql2, baseURI);
        
        assertSameASTModuloVarRenaming(sparql1, expected, actual, new Object[2]);
        
    } // test_subselect_under_graph_01()
    
    
    
    /**
     * Unit test for the treatment of graph variables overshadowed
     * by sub-SELECTs.
     * 
     * <pre>
     * PREFIX ex: <http://www.example.org/schema#>
     * SELECT ?x WHERE {
     *   GRAPH ?g {
     *     ?x ex:q ?g .
     *     {SELECT ?x WHERE {?x ex:p ?g}}
     *   }
     * }
     * </pre>
     * 
     * Here SELECT ?x overshadows the variable from GRAPH ?g, 
     * so that ?g in ?x ex:p ?g is actually a different variable, 
     * albeit with the same name.
     * 
     * The tested feature solves this by explicitly renaming ?g:
     * 
     * <pre>
     * PREFIX ex: <http://www.example.org/schema#>
     * SELECT ?x WHERE {
     *   GRAPH ?g {
     *     ?x ex:q ?g .
     *     {SELECT ?x WHERE {?x ex:p ?g_12345}}
     *   }
     * }
     * </pre>
    
     */
    public void test_subselect_under_graph_02() throws MalformedQueryException,
            TokenMgrError, ParseException {
    
        String sparql1 = "" + //
                "PREFIX ex: <http://www.example.org/schema#>\n" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    ?x ex:q ?g .\n" + //
                "    {SELECT ?x WHERE {?x ex:p ?g}}\n" + //
                "  }\n" + //
                "}";
        
        
        String sparql2 = "" + //
                "PREFIX ex: <http://www.example.org/schema#>\n" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    ?x ex:q ?g .\n" + //
                "    {SELECT ?x WHERE {?x ex:p ?g_12345}}\n" + //
                "  }\n" + //
                "}";
    
        final QueryRoot actual = parse(sparql1, baseURI);
        
        final QueryRoot expected = parse(sparql2, baseURI);
        
        assertSameASTModuloVarRenaming(sparql1, expected, actual, new Object[2]);
        
    } // test_subselect_under_graph_02()
    
      
    /**
     * Unit test for the treatment of graph variables overshadowed
     * by sub-SELECTs.
     * 
     * <pre>
     * prefix ex: <http://www.example.org/schema#>
     * prefix in: <http://www.example.org/instance#>
     * 
     * select ?x where {
     * 
     *   ?g a ex:graph_type2 .
     * 
     *   graph ?g {
     * 
     *     ?x ex:q ?z .
     * 
     *     {select ?x where {
     * 
     *       graph ?g {
     *         ?x ex:p ?y
     *       } .
     * 
     *       ?g a ex:graph_type1 .  
     *     }}
     *   }
     * }
     * </pre>
     * 
     * Here ?g from the outer graph ?g is overshadowed by the inner select ?x. 
     * The inner graph ?g is actually non-redundant because ?g there is 
     * a different variable, although it has the same name as the variable 
     * in the outer graph ?g.
     * 
     * The tested feature solves this by explicitly renaming ?g:
     * 
     * <pre>
     * prefix ex: <http://www.example.org/schema#>
     * prefix in: <http://www.example.org/instance#>
     * 
     * select ?x where {
     * 
     *   ?g a ex:graph_type2 .
     * 
     *   graph ?g {
     * 
     *     ?x ex:q ?z .
     * 
     *     {select ?x where {
     * 
     *       graph ?g_12345 {
     *         ?x ex:p ?y
     *       } .
     * 
     *       ?g_12345 a ex:graph_type1 .  
     *     }}
     *   }
     * }
     * </pre>
    
     */
    public void test_subselect_under_graph_03() throws MalformedQueryException,
            TokenMgrError, ParseException {
    
        String sparql1 = "" + //
                " prefix ex: <http://www.example.org/schema#>\n" + //
                " prefix in: <http://www.example.org/instance#>\n" + //
                " select ?x where {\n" + //
                "   ?g a ex:graph_type2 .\n" + //
                "   graph ?g {\n" + //
                "     ?x ex:q ?z .\n" + //
                "     {select ?x where {\n" + //
                "       graph ?g {\n" + //
                "         ?x ex:p ?y\n" + //
                "       } .\n" + //
                "       ?g a ex:graph_type1 .  \n" + //
                "     }}\n" + //
                "   }\n" + //
                " }";
        
        String sparql2 = "" + //
                " prefix ex: <http://www.example.org/schema#>\n" + //
                " prefix in: <http://www.example.org/instance#>\n" + //
                " select ?x where {\n" + //
                "   ?g a ex:graph_type2 .\n" + //
                "   graph ?g {\n" + //
                "     ?x ex:q ?z .\n" + //
                "     {select ?x where {\n" + //
                "       graph ?g_12345 {\n" + //
                "         ?x ex:p ?y\n" + //
                "       } .\n" + //
                "       ?g_12345 a ex:graph_type1 .  \n" + //
                "     }}\n" + //
                "   }\n" + //
                " }";
        
    
        final QueryRoot actual = parse(sparql1, baseURI);
        
        final QueryRoot expected = parse(sparql2, baseURI);
        
        assertSameASTModuloVarRenaming(sparql1, expected, actual, new Object[2]);
        
    } // test_subselect_under_graph_03()
    
    /**
     * Unit test for the treatment of graph variables overshadowed
     * by sub-SELECTs.
     * 
     * <pre>
     * prefix ex: <http://www.example.org/schema#>
     * prefix in: <http://www.example.org/instance#>
     * 
     * SELECT ?x #1 
     * {
     *   GRAPH ?g #1 
     *   {
     *     ?g a ex:graph_type2 .
     * 
     *     {SELECT ?x #2
     *      {
     *        GRAPH ?g #2
     *        {
     *          ?g a ex:graph_type1 .
     * 
     *         {SELECT ?x #3
     *          {
     *             ?x ex:p ?g          
     *           }
     *          }
     *        }
     *      }
     *     }
     *   }
     * }
     * 
     * </pre>
     * 
     * This test exercises double overshadowing of context variables
     * by sub-SELECT. The variable ?g from GRAPH ?g #1 is overshadowed by 
     * SELECT ?x #2, so that the variable in GRAPH ?g #2 is a different 
     * variable, although with the same name, and it is in its turn overshadowed
     * by SELECT ?x #3, so that ?g in ?x ?p ?g is essentially a singleton.
     * 
     * The tested feature solves this by explicitly renaming ?g two times:
     * 
     * <pre>
     * prefix ex: <http://www.example.org/schema#>
     * prefix in: <http://www.example.org/instance#>
     * 
     * SELECT ?x #1 
     * {
     *   GRAPH ?g #1 
     *   {
     *     ?g a ex:graph_type2 .
     * 
     *     {SELECT ?x #2
     *      {
     *        GRAPH ?g_12345 #2
     *        {
     *          ?g_12345 a ex:graph_type1 .
     * 
     *         {SELECT ?x #3
     *          {
     *             ?x ex:p ?g_54321          
     *           }
     *          }
     *        }
     *      }
     *     }
     *   }
     * }
     * 
     * </pre>    
     */
    public void test_subselect_under_graph_04() throws MalformedQueryException,
            TokenMgrError, ParseException {
    
        String sparql1 = "" + //
                " prefix ex: <http://www.example.org/schema#>\n" + //
                " prefix in: <http://www.example.org/instance#>\n" + //
                " SELECT ?x #1\n \n" + //
                " {\n" + //
                "   GRAPH ?g #1\n" + // 
                "   {\n" + //
                "     ?g a ex:graph_type2 .\n" + //
                "     {SELECT ?x #2\n" + //
                "      {\n" + //
                "        GRAPH ?g #2\n" + //
                "        {\n" + //
                "          ?g a ex:graph_type1 .\n" + //
                "         {SELECT ?x #3\n" + //
                "          {\n" + //
                "             ?x ex:p ?g\n" + //          
                "           }\n" + //
                "          }\n" + //
                "        }\n" + //
                "      }\n" + //
                "     }\n" + //
                "   }\n" + //
                " }";
        
        String sparql2 = "" + //
                " prefix ex: <http://www.example.org/schema#>\n" + //
                " prefix in: <http://www.example.org/instance#>\n" + //
                " SELECT ?x #1\n \n" + //
                " {\n" + //
                "   GRAPH ?g #1\n" + // 
                "   {\n" + //
                "     ?g a ex:graph_type2 .\n" + //
                "     {SELECT ?x #2\n" + //
                "      {\n" + //
                "        GRAPH ?g_12345 #2\n" + //
                "        {\n" + //
                "          ?g_12345 a ex:graph_type1 .\n" + //
                "         {SELECT ?x #3\n" + //
                "          {\n" + //
                "             ?x ex:p ?g_54321\n" + //          
                "           }\n" + //
                "          }\n" + //
                "        }\n" + //
                "      }\n" + //
                "     }\n" + //
                "   }\n" + //
                " }";
        
        
    
        final QueryRoot actual = parse(sparql1, baseURI);
        
        final QueryRoot expected = parse(sparql2, baseURI);
        
        assertSameASTModuloVarRenaming(sparql1, expected, actual, new Object[2]);
        
    } // test_subselect_under_graph_04()
    
    
    
    /**
     * Unit test for the treatment of graph variables overshadowed
     * by sub-SELECTs.
     * 
     * <pre>
     * SELECT ?x WHERE {
     *   GRAPH ?g {
     *     {SELECT ?x (SAMPLE(?y) AS ?g) WHERE {?x ?p ?y . ?x ?p ?g} GROUP BY ?x}
     *   }
     * }
     * </pre>
     * 
     * Here SELECT ?x (SAMPLE(?y) AS ?g) overshadows the variable from GRAPH ?g,
     * because ?g is introduced anew in (SAMPLE(?y) AS ?g),
     * so that ?g in ?x ?p ?g is actually a different variable, 
     * albeit with the same name.
     * 
     * The tested feature solves this by explicitly renaming ?g in ?x ?p ?g,
     * but not in SELECT ?x (SAMPLE(?y) AS ?g):
     * 
     * <pre>
     * SELECT ?x WHERE {
     *   GRAPH ?g {
     *     {SELECT ?x (SAMPLE(?y) AS ?g) WHERE {?x ?p ?y . ?x ?p ?g_12345} GROUP BY ?x}
     *   }
     * }
     * </pre>
     */
    public void test_subselect_under_graph_05() throws MalformedQueryException,
            TokenMgrError, ParseException {
           
        String sparql1 = "" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    {SELECT ?x (SAMPLE(?y) AS ?g) WHERE {?x ?p ?y . ?x ?p ?g} GROUP BY ?x}\n" + //
                " }\n" + //
                "}";
        
        String sparql2 = "" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    {SELECT ?x (SAMPLE(?y) AS ?g) WHERE {?x ?p ?y . ?x ?p ?g_12345} GROUP BY ?x}\n" + //
                " }\n" + //
                "}";
    
        final QueryRoot actual = parse(sparql1, baseURI);
        
        final QueryRoot expected = parse(sparql2, baseURI);
        
        assertSameASTModuloVarRenaming(sparql1, expected, actual, new Object[2]);
        
    } // test_subselect_under_graph_05()
    
    
    
    
    /**
     * Unit test for the treatment of graph variables overshadowed
     * by sub-SELECTs.
     * 
     * <pre>
     * SELECT ?x WHERE {
     *   GRAPH ?g {
     *     {SELECT * WHERE {?x ?p ?g}}
     *   }
     * }
     * </pre>
     * 
     * This test is to ensure that the variable renaming approach
     * to resolve variable name collision is not over-applied.
     * Here SELECT * does not overshadow the variable from GRAPH ?g, 
     * because * covers ?g, so ?g must not be renamed.
     */
    public void test_subselect_under_graph_06() throws MalformedQueryException,
            TokenMgrError, ParseException {
    
        String sparql1 = "" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    {SELECT * WHERE {?x ?p ?g}}\n" + //
                " }\n" + //
                "}";
        
        String sparql2 = "" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    {SELECT * WHERE {?x ?p ?g_12345}}\n" + //
                " }\n" + //
                "}";
    
        final QueryRoot actual = parse(sparql1, baseURI);
        
        final QueryRoot notExpected = parse(sparql2, baseURI);
        
        assertDifferentASTModuloVarRenaming(sparql1, notExpected, actual, new Object[2]);
        
    } // test_subselect_under_graph_06()
    
    
    /**
     * Unit test for the treatment of graph variables overshadowed
     * by sub-SELECTs.
     * 
     * <pre>
     * SELECT ?x WHERE {
     *   GRAPH ?g {
     *     {SELECT ?x ?g WHERE {?x ?p ?g}}
     *   }
     * }
     * </pre>
     * 
     * This test is to ensure that the variable renaming approach
     * to resolve variable name collision is not over-applied.
     * Here SELECT ?x ?g does not overshadow the variable from GRAPH ?g, 
     * because the projection includes ?g, so ?g must not be renamed.
     */
    public void test_subselect_under_graph_07() throws MalformedQueryException,
            TokenMgrError, ParseException {
    
        String sparql1 = "" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    {SELECT ?x ?g WHERE {?x ?p ?g}}\n" + //
                " }\n" + //
                "}";
        
        String sparql2 = "" + //
                "SELECT ?x WHERE {\n" + //
                "  GRAPH ?g {\n" + //
                "    {SELECT ?x ?g WHERE {?x ?p ?g_12345}}\n" + //
                " }\n" + //
                "}";
    
        final QueryRoot actual = parse(sparql1, baseURI);
        
        final QueryRoot notExpected = parse(sparql2, baseURI);
        
        assertDifferentASTModuloVarRenaming(sparql1, notExpected, actual, new Object[2]);
        
    } // test_subselect_under_graph_07()
    
}
