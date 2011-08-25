/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Aug 20, 2011
 */
package com.bigdata.rdf.sail.sparql;

import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;

import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ConstructNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IASTOptimizer;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

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
     */
    public void test_groupBy_bareVar() throws MalformedQueryException, TokenMgrError,
            ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} GROUP BY ?o";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

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

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for GROUP BY in SELECT query with BIND(expr AS var).
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o} GROUP BY (?o AS ?z)
     * </pre>
     */
    public void test_groupBy_bindExpr() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} GROUP BY (?o AS ?z)";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

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
                    new AssignmentNode(new VarNode("o"),
                    (IValueExpressionNode) new VarNode("z"))//
                    );

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for GROUP BY in SELECT query with function call without "AS".
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o} GROUP BY str(?o)
     * </pre>
     */
    public void test_groupBy_functionCall() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} GROUP BY str(?o)";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

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
            final FunctionNode funct = new FunctionNode(lex,
                    FunctionRegistry.STR, null/* scalarValues */,
                    new ValueExpressionNode[] { new VarNode("o") });
            // Note: anonymous variable.
            final VarNode anonvar1 = new VarNode("-groupBy-1");
            anonvar1.setAnonymous(true);
            groupBy.addExpr(new AssignmentNode(anonvar1, funct));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for HAVING clause (with an implicit group consisting of all
     * solutions).
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o} HAVING (?o GT ?s)
     * </pre>
     */
    public void test_having() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o} HAVING (?o > ?s)";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

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
            having.addExpr(new FunctionNode(lex, //
                    FunctionRegistry.GT,//
                    null, // scalarValues
                    new ValueExpressionNode[] {// args
                    new VarNode("o"), new VarNode("s") })//
            );
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

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
            final FunctionNode funct = new FunctionNode(lex,
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
     * FIXME CONSTRUCT has two forms which we need to test. In the first form a
     * ConstructTemplate appears before the DatasetClause. In the second form a
     * TriplesTemplate appears after the WhereClause and before the optional
     * SolutionModifier. Both forms allow the SolutionModifier. However, openrdf
     * has not yet implemented the 2nd form.  Also, Anzo has an extension of
     * CONSTRUCT for quads.
     */
    public void test_construct() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "construct { ?s ?p ?o } where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.CONSTRUCT);
        {

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
     * Unit test for WITH {subquery} AS "name" and INCLUDE. The WITH must be in
     * the top-level query. For example:
     * 
     * <pre>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * SELECT ?p2
     * WITH {
     *         SELECT DISTINCT ?p
     *         WHERE {
     *                 ?s ?p ?o
     *         }
     * } AS %namedSet1
     *  WHERE {
     *         ?p rdfs:subPropertyOf ?p2
     *         INCLUDE %namedSet1
     * }
     * </pre>
     */
    public void test_namedSubquery() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = //
                "\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" + //
                "\nSELECT ?p2" + //
                "\n WITH {" + //
                "\n         SELECT DISTINCT ?p" + //
                "\n         WHERE {" + //
                "\n                 ?s ?p ?o" + //
                "\n          }" + //
                "\n } AS %namedSet1" + //
                "\n WHERE {" + //
                "\n        ?p rdfs:subPropertyOf ?p2" + //
                "\n        INCLUDE %namedSet1" + //
                "\n}"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final String namedSet = "namedSet1";
            
            final VarNode s = new VarNode("s");
            final VarNode p = new VarNode("p");
            final VarNode o = new VarNode("o");
            final VarNode p2 = new VarNode("p2");

            final TermNode subPropertyOf = new ConstantNode(
                    makeIV(valueFactory.createURI(RDFS.SUBPROPERTYOF
                            .stringValue())));
            
            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(p2);
                expected.setProjection(projection);
            }
            
            final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
            expected.setNamedSubqueries(namedSubqueries);
            {

                final NamedSubqueryRoot namedSubqueryRoot = new NamedSubqueryRoot(
                        QueryType.SELECT, namedSet);
                
                final ProjectionNode projection = new ProjectionNode();
                namedSubqueryRoot.setProjection(projection);
                projection.addProjectionVar(p);
                projection.setDistinct(true);
                
                final JoinGroupNode whereClause = new JoinGroupNode();
                namedSubqueryRoot.setWhereClause(whereClause);
                whereClause.addChild(new StatementPatternNode(s, p, o,
                        null/* c */, Scope.DEFAULT_CONTEXTS));
                
                namedSubqueries.add(namedSubqueryRoot);
                
            }
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(p, subPropertyOf, p2,
                    null/* c */, Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(new NamedSubqueryInclude(namedSet));
            
        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
}
