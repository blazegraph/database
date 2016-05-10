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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ValueExprBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBigdataExprBuilder.java 5063 2011-08-21 16:13:03Z
 *          thompsonbry $
 */
public class TestValueExprBuilder extends AbstractBigdataExprBuilderTestCase {

//    private static final Logger log = Logger
//            .getLogger(TestValueExprBuilder.class);
    
    public TestValueExprBuilder() {
    }

    public TestValueExprBuilder(String name) {
        super(name);
    }

    /**
     * Simple variable rename in select expression.
     * 
     * <pre>
     * SELECT (?s as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_bind() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (?s as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"),//
                            new VarNode("s")//
                    ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * SELECT using math expression.
     * 
     * <pre>
     * SELECT (?s + ?o as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_math_expr() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (?s + ?o as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.ADD,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s"),
                                    new VarNode("o")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * Select using comparison expression.
     * 
     * <pre>
     * SELECT (?s < ?o as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_compare_expr() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (?s < ?o as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.LT,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s"),
                                    new VarNode("o")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);
        
        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * Simple unit test for a value expression which is a URI.
     */
    public void test_select_uri() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ( <http://xmlns.com/foaf/0.1/> as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new ConstantNode(makeIV(valueFactory
                                    .createURI("http://xmlns.com/foaf/0.1/")))));
            expected.setProjection(projection);

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * Unit test with BNode. Should be rewritten as an anonymous variable.
     * <p>
     * Note: a bare blank node may not appear in the SELECT expression with
     * (bnode as ?x) so we have to test this somewhere else in the syntax of the
     * query.
     */
    public void test_select_bnode() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p _:a1}";

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

            final VarNode blankNodeVar = new VarNode("-anon-1");
            blankNodeVar.setAnonymous(true);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), blankNodeVar, null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Simple unit test for a value expression which is a plain literal.
     */
    public void test_select_literal() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (\"abc\" as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new ConstantNode(makeIV(valueFactory
                                    .createLiteral("abc")))));
            expected.setProjection(projection);

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * Simple unit test for a value expression which is a <code>xsd:int</code>.
     */
    public void test_select_xsd_int() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (\"12\"^^<http://www.w3.org/2001/XMLSchema#int> as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new ConstantNode(makeIV(valueFactory
                                    .createLiteral(12)))));
            expected.setProjection(projection);

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * <pre>
     * SELECT (COUNT(?s) as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_count_foo() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (count(?s) as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.COUNT,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * <pre>
     * SELECT (COUNT(DISTINCT ?s) as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_count_distinct_foo() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (count(distinct ?s) as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.COUNT,//
                                    Collections.singletonMap(
                                            AggregateBase.Annotations.DISTINCT,
                                            (Object) Boolean.TRUE), // scalar
                                                                    // values.
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * <pre>
     * SELECT (COUNT(*) as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_count_star() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (count(*) as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.COUNT,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("*")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * <pre>
     * SELECT (COUNT(DISTINT *) as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_count_distinct_star() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (count(distinct *) as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.COUNT,//
                                    Collections.singletonMap(
                                            AggregateBase.Annotations.DISTINCT,
                                            (Object) Boolean.TRUE), // scalar
                                                                    // values.
                                    new ValueExpressionNode[] {// args
                                    new VarNode("*")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * <pre>
     * SELECT (FunctionCall(?s) as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_function_call() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (<" + FunctionRegistry.ADD
                + ">(?s,?o) as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.ADD,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s"),
                                    new VarNode("o")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * <pre>
     * SELECT (coalesce(?s,?p,?o) as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_coalesce() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (coalesce(?s,?p,?o) as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.COALESCE,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s"),
                                    new VarNode("p"),
                                    new VarNode("o")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * <pre>
     * SELECT (if(?s,?p,?o) as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_if_then_else() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (if(?s,?p,?o) as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.IF,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s"),
                                    new VarNode("p"),
                                    new VarNode("o")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * <pre>
     * SELECT (regex(?o, "^ali") as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_regex() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ( regex(?o,\"^ali\") as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.REGEX,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("o"),
                                    new ConstantNode(makeIV(valueFactory.createLiteral("^ali"))),
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * <pre>
     * SELECT (regex(?o, "^ali", "i") as ?x) where {?s ?p ?o}
     * </pre>
     */
    public void test_select_regex_flags() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ( regex(?o,\"^ali\", \"i\") as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.REGEX,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("o"),
                                    new ConstantNode(makeIV(valueFactory.createLiteral("^ali"))),
                                    new ConstantNode(makeIV(valueFactory.createLiteral("i"))),
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * IN with empty arg list
     * 
     * <pre>
     * SELECT (?s IN() as ?x) where {?s ?p ?o}
     * </pre>
     * 
     * @see http://www.openrdf.org/issues/browse/SES-818
     */
    public void test_select_foo_IN_none() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (?s IN() as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.IN,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s"),
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

    /**
     * IN with a non-empty arg list
     * 
     * <pre>
     * SELECT (?s IN(?p,?o) as ?x) where {?s ?p ?o}
     * </pre>
     * 
     * @see http://www.openrdf.org/issues/browse/SES-818
     */
    public void test_select_foo_IN_bar() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select (?s IN(?p,?o) as ?x) where {?s ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionExpression(//
                    new AssignmentNode(//
                            new VarNode("x"), //
                            new FunctionNode( //
                                    FunctionRegistry.IN,//
                                    null, // scalarValues
                                    new ValueExpressionNode[] {// args
                                    new VarNode("s"),
                                    new VarNode("p"),
                                    new VarNode("o")
                                    })//
                            ));
            expected.setProjection(projection);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected.getProjection(), actual.getProjection());

    }

}
