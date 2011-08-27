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
 * Created on Aug 21, 2011
 */

package com.bigdata.rdf.sail.sparql;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;

import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for building up triple patterns, including those which are covered
 * by the property paths extension in SPARQL 1.1 (a triple pattern which a
 * constant in the predicate position is treated by the sesame SPARQL grammar as
 * a degenerate case of a property path.)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestGroupGraphPatternBuilder.java 5064 2011-08-21 22:50:55Z
 *          thompsonbry $
 */
public class TestTriplePatternBuilder extends
        AbstractBigdataExprBuilderTestCase {

    /**
     * 
     */
    public TestTriplePatternBuilder() {
    }

    /**
     * @param name
     */
    public TestTriplePatternBuilder(String name) {
        super(name);
    }

    /**
     * Unit test for simple triple pattern in the default context consisting of
     * three variables.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_triple_pattern_var_var_var()
            throws MalformedQueryException, TokenMgrError, ParseException {

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
     * Unit test for simple triple pattern in the default context consisting
     * of a constant, a variable, and a variable.
     * 
     * <pre>
     * SELECT ?p where {<http://www.bigdata.com/s> ?p ?o}
     * </pre>
     */
    public void test_triple_pattern_const_var_var() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?p where {<http://www.bigdata.com/s> ?p ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ConstantNode s = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/s")));

            final VarNode p = new VarNode("p");
            
            final VarNode o = new VarNode("o");
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(p);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(s, p, o, null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple triple pattern in the default context consisting
     * of a variable, a constant, and a variable.
     * 
     * <pre>
     * SELECT ?s where {?s <http://www.bigdata.com/p> ?o}
     * </pre>
     */
    public void test_triple_pattern_var_const_var() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s <http://www.bigdata.com/p> ?o}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final VarNode s = new VarNode("s");
            
            final ConstantNode p = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/p")));

            final VarNode o = new VarNode("o");
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(s);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(s, p, o, null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple triple pattern in the default context consisting
     * of a variable, a variable, and a constant.
     * 
     * <pre>
     * SELECT ?s where {?s ?p <http://www.bigdata.com/o> }
     * </pre>
     */
    public void test_triple_pattern_var_var_const() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p <http://www.bigdata.com/o> }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final VarNode s = new VarNode("s");
            
            final VarNode p = new VarNode("p");
            
            final ConstantNode o = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/o")));

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(s);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(s, p, o, null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple triple pattern in the default context consisting
     * of a constant, a variable, and a constant.
     * 
     * <pre>
     * SELECT ?p where { <http://www.bigdata.com/s> ?p <http://www.bigdata.com/o> }
     * </pre>
     */
    public void test_triple_pattern_const_var_const() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?p where { <http://www.bigdata.com/s> ?p <http://www.bigdata.com/o> }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ConstantNode s = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/s")));
            
            final VarNode p = new VarNode("p");
            
            final ConstantNode o = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/o")));

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(p);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(s, p, o, null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple triple pattern in the default context consisting
     * of a constant, a constant, and a variable.
     * 
     * <pre>
     * SELECT ?o where { <http://www.bigdata.com/s> <http://www.bigdata.com/p> ?o }
     * </pre>
     */
    public void test_triple_pattern_const_const_var() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?o where { <http://www.bigdata.com/s> <http://www.bigdata.com/p> ?o }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ConstantNode s = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/s")));
            
            final ConstantNode p = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/p")));

            final VarNode o = new VarNode("o");
            
            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(o);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(s, p, o, null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple triple pattern in the default context consisting
     * of a constant, a variable, and a constant.
     * 
     * <pre>
     * SELECT ?x where { <http://www.bigdata.com/s> <http://www.bigdata.com/p> <http://www.bigdata.com/o> }
     * </pre>
     */
    public void test_triple_pattern_const_const_const() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?x where { <http://www.bigdata.com/s> <http://www.bigdata.com/p> <http://www.bigdata.com/o> }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ConstantNode s = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/s")));
            
            final ConstantNode p = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/p")));
            
            final ConstantNode o = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/o")));

            final VarNode x = new VarNode("x");

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(x);
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            whereClause.addChild(new StatementPatternNode(s, p, o, null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            expected.setWhereClause(whereClause);
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple join of two triple patterns.
     * 
     * <pre>
     * select ?s where {?s ?p ?o . ?o ?p2 ?s}
     * </pre>
     */
    public void test_simple_join() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o . ?o ?p2 ?s}";

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
            whereClause.addChild(new StatementPatternNode(new VarNode("o"),
                    new VarNode("p2"), new VarNode("s"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for a triples block using a predicate list where the predicate
     * is a variable. 
     * <pre>
     * select ?s where {?s ?p ?o ; ?p2 ?o2 }
     * </pre>
     */
    public void test_predicate_list() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o ; ?p2 ?o2 }";

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
            
            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p2"), new VarNode("o2"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for a triples block using a predicate list where the predicate
     * is a constant (this is interpreted as a property path).
     * 
     * <pre>
     * select ?s where {?s <http://www.bigdata.com/foo> ?o ; <http://www.bigdata.com/bar> ?o2 }
     * </pre>
     */
    public void test_predicate_list_where_predicate_is_constant()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql //
                = "select ?s \n"//
                + " where { \n"//
                + "  ?s <http://www.bigdata.com/foo> ?o "//
                + "   ; <http://www.bigdata.com/bar> ?o2"//
                + " }"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ConstantNode foo = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/foo")));

            final ConstantNode bar = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/bar")));

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause
                    .addChild(new StatementPatternNode(new VarNode("s"), foo,
                            new VarNode("o"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

            whereClause
                    .addChild(new StatementPatternNode(new VarNode("s"), bar,
                            new VarNode("o2"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for an object list with variables.
     * 
     * <pre>
     * select ?s where {?s ?p ?o , ?o2 , ?o3 . }
     * </pre>
     */
    public void test_object_list() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o , ?o2 , ?o3 . }";

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

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o2"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o3"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for an object list with constants.
     * 
     * <pre>
     * select ?s where {?s ?p <http://www.bigdata.com/foo> , <http://www.bigdata.com/bar> , <http://www.bigdata.com/goo> . }
     * </pre>
     */
    public void test_object_list_where_objects_are_constants()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p <http://www.bigdata.com/foo> , <http://www.bigdata.com/bar> , <http://www.bigdata.com/goo> . }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ConstantNode foo = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/foo")));

            final ConstantNode bar = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/bar")));

            final ConstantNode goo = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com/goo")));

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause
                    .addChild(new StatementPatternNode(new VarNode("s"),
                            new VarNode("p"), foo, null/* c */,
                            Scope.DEFAULT_CONTEXTS));

            whereClause
                    .addChild(new StatementPatternNode(new VarNode("s"),
                            new VarNode("p"), bar, null/* c */,
                            Scope.DEFAULT_CONTEXTS));

            whereClause
                    .addChild(new StatementPatternNode(new VarNode("s"),
                            new VarNode("p"), goo, null/* c */,
                            Scope.DEFAULT_CONTEXTS));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for a complex triples block using both a predicate list and an
     * object list.
     * 
     * <pre>
     * select ?s where {?s ?p ?o ; ?p2 ?o2 , ?o3}
     * </pre>
     */
    public void test_with_predicate_list_and_object_list()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o ; ?p2 ?o2 , ?o3}";

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

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p2"), new VarNode("o2"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p2"), new VarNode("o3"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
        
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
}
