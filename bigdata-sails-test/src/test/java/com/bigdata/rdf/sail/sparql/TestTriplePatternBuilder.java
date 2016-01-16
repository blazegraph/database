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
 * Created on Aug 21, 2011
 */

package com.bigdata.rdf.sail.sparql;

import info.aduna.net.ParsedURI;

import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.vocab.decls.DCElementsVocabularyDecl;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

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

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

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

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

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

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

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

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

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

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

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

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

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

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

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

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

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

    /**
     * Unit test for blank node "[]" syntax.
     * 
     * <pre>
     * PREFIX  : <http://example.org/>
     * SELECT * { [] :p "v" }
     * </pre>
     * 
     * Note that <code>[]</code> is a blank node and is represented as an
     * anonymous variable.
     * <p>
     * Note: blank nodes are translated into anonymous variables in the parse
     * tree before the bigdata AST model is generated.
     */
    public void test_bnode_bracket_syntax_01() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
                "PREFIX  : <http://example.org/>\n"+
                "SELECT * { [] :p \"v\" }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.org/");
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("*"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final ConstantNode p = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/p")));
                
                final ConstantNode v = new ConstantNode(
                        makeIV(valueFactory.createLiteral("v")));

                final VarNode s = new VarNode("-anon-1");
                s.setAnonymous(true);

                whereClause.addChild(new StatementPatternNode(s, p, v,
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for blank node "[]" syntax.
     * 
     * <pre>
     * PREFIX  : <http://example.org/>
     * SELECT * { [ :p "v" ] }
     * </pre>
     * 
     * Note that this has exactly the same interpretation as
     * 
     * <pre>
     * PREFIX  : <http://example.org/>
     * SELECT * { [] :p "v" }
     * </pre>
     * 
     * Note: blank nodes are translated into anonymous variables in the parse
     * tree before the bigdata AST model is generated.
     */
    public void test_bnode_bracket_syntax_02() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
                "PREFIX  : <http://example.org/>\n"+
                "SELECT * { [ :p \"v\" ] }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.org/");
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("*"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final ConstantNode p = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/p")));
                
                final ConstantNode v = new ConstantNode(
                        makeIV(valueFactory.createLiteral("v")));

                final VarNode s = new VarNode("-anon-11");
                s.setAnonymous(true);

                whereClause.addChild(new StatementPatternNode(s, p, v,
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for blank node "[]" syntax.
     * 
     * <pre>
     * PREFIX  : <http://example.org/>
     * SELECT * { [ :p "v" ] :q "w" }
     * </pre>
     * 
     * Note that this has exactly the same interpretation as:
     * 
     * <pre>
     * PREFIX  : <http://example.org/>
     * SELECT * {
     *   _:b57 :p "v" .
     *   _:b57 :q "w"
     * }
     * </pre>
     * 
     * assuming that the blank node is given the identity <code>_:b57</code>.
     * <p>
     * Note: blank nodes are translated into anonymous variables in the parse
     * tree before the bigdata AST model is generated.
     */
    public void test_bnode_bracket_syntax_03() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
                "PREFIX  : <http://example.org/>\n"+
                "SELECT * { [ :p \"v\" ] :q \"w\" }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.org/");
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("*"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final ConstantNode p = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/p")));

                final ConstantNode q = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/q")));

                final ConstantNode v = new ConstantNode(
                        makeIV(valueFactory.createLiteral("v")));

                final ConstantNode w = new ConstantNode(
                        makeIV(valueFactory.createLiteral("w")));

                final VarNode s = new VarNode("-anon-11");
                s.setAnonymous(true);

                whereClause.addChild(new StatementPatternNode(s, p, v,
                        null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(s, q, w,
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for blank node "[]" syntax.
     * 
     * <pre>
     * PREFIX  : <http://example.org/>
     * SELECT * { :x :q [ :p "v" ] }
     * </pre>
     * 
     * Note that this has exactly the same interpretation as:
     * 
     * <pre>
     * PREFIX  : <http://example.org/>
     * SELECT * {
     *   :x :q _:b57 .
     *   _:b57 :p "v"
     * }
     * </pre>
     * 
     * assuming that the blank node is given the identity <code>_:b57</code>.
     * <p>
     * Note: blank nodes are translated into anonymous variables in the parse
     * tree before the bigdata AST model is generated.
     */
    public void test_bnode_bracket_syntax_04() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
                "PREFIX  : <http://example.org/>\n"+
                "SELECT * { :x :q [ :p \"v\" ] }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.org/");
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("*"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final ConstantNode x = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/x")));

                final ConstantNode p = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/p")));

                final ConstantNode q = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/q")));

                final ConstantNode v = new ConstantNode(
                        makeIV(valueFactory.createLiteral("v")));

                final VarNode bnode = new VarNode("-anon-11");
                bnode.setAnonymous(true);

                whereClause.addChild(new StatementPatternNode(bnode, p, v,
                        null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(x, q, bnode,
                        null/* c */, Scope.DEFAULT_CONTEXTS));

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
    
    /*
     * Note: For the RDF Collections tests, you basically have to work out by
     * trial and error the actual names of the RDF Variables which will be
     * assigned by the visitor when it interprets the parse tree. The variable
     * names are based on a prefix and a counter. The actual names are therefore
     * dependent on the query and the path through the visitor pattern. This is
     * a bit of a PITA.
     */

    /**
     * Unit test for the RDF Collections syntax (from SPARQL 1.1 Last Call
     * Working Draft).
     * 
     * <pre>
     * PREFIX  : <http://example.org/ns#>
     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * SELECT * { (1 ?x 3 4) :p "w" . }
     * </pre>
     * 
     * has exactly the same interpretation as
     * 
     * <pre>
     *     _:b0  rdf:first  1 ;
     *           rdf:rest   _:b1 .
     *     _:b1  rdf:first  ?x ;
     *           rdf:rest   _:b2 .
     *     _:b2  rdf:first  3 ;
     *           rdf:rest   _:b3 .
     *     _:b3  rdf:first  4 ;
     *           rdf:rest   rdf:nil .
     *     _:b0  :p         "w" .
     * </pre>
     * 
     * where _:b0, etc. are blank nodes not appearing elsewhere in the query.
     * <p>
     * Note: blank nodes are translated into anonymous variables in the parse
     * tree before the bigdata AST model is generated.
     */
    public void test_rdf_collections_01() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = ""//
                + "PREFIX  : <http://example.org/ns#>\n"//
                + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"//
                + "SELECT * { (1 ?x 3 4) :p \"w\" . }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.org/ns#");
                prefixDecls.put("rdf", RDF.NAMESPACE);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("*"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final VarNode x = new VarNode("x");
                final VarNode b0 = makeAnon(new VarNode("-anon-11"));
                final VarNode b1 = makeAnon(new VarNode("-anon-1-12"));
                final VarNode b2 = makeAnon(new VarNode("-anon-1-23"));
                final VarNode b3 = makeAnon(new VarNode("-anon-1-34"));
                
                final ConstantNode rdfFirst = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.FIRST.stringValue())));
                final ConstantNode rdfRest = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.REST.stringValue())));
                final ConstantNode rdfNil= new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.NIL.stringValue())));

                final ConstantNode ONE = new ConstantNode(
                        makeIV(valueFactory.createLiteral("1", XSD.INTEGER)));
                final ConstantNode THREE = new ConstantNode(
                        makeIV(valueFactory.createLiteral("3", XSD.INTEGER)));
                final ConstantNode FOUR = new ConstantNode(
                        makeIV(valueFactory.createLiteral("4", XSD.INTEGER)));
                final ConstantNode p = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/ns#p")));
                final ConstantNode w = new ConstantNode(
                        makeIV(valueFactory.createLiteral("w")));

//                     _:b0  rdf:first    1 ;
//                           rdf:rest   _:b1 .
//                     _:b1  rdf:first    ?x ;
//                           rdf:rest   _:b2 .
//                     _:b2  rdf:first    3 ;
//                           rdf:rest   _:b3 .
//                     _:b3  rdf:first    4 ;
//                           rdf:rest   rdf:nil .
//                     _:b0  :p         "w" .

                whereClause.addChild(new StatementPatternNode(b0, rdfFirst, ONE   , null/* c */, Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(new StatementPatternNode(b0, rdfRest , b1    , null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(b1, rdfFirst, x     , null/* c */, Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(new StatementPatternNode(b1, rdfRest , b2    , null/* c */, Scope.DEFAULT_CONTEXTS));
                
                whereClause.addChild(new StatementPatternNode(b2, rdfFirst, THREE , null/* c */, Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(new StatementPatternNode(b2, rdfRest , b3    , null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(b3, rdfFirst, FOUR  , null/* c */, Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(new StatementPatternNode(b3, rdfRest,  rdfNil, null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(b0, p,        w     , null/* c */, Scope.DEFAULT_CONTEXTS));

            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for the RDF Collections syntax (from SPARQL 1.1 Last Call
     * Working Draft).
     * 
     * <pre>
     * PREFIX  : <http://example.org/ns#>
     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * SELECT * { ( (1 [:p :q] ( 2 ) ) }
     * </pre>
     * 
     * has exactly the same interpretation as
     * 
     * <pre>
     *     _:b0  rdf:first  1 ;
     *           rdf:rest   _:b1 .
     *     _:b1  rdf:first  _:b2 .
     *     _:b2  :p         :q .
     *     _:b1  rdf:rest   _:b3 .
     *     _:b3  rdf:first  _:b4 .
     *     _:b4  rdf:first  2 ;
     *           rdf:rest   rdf:nil .
     *     _:b3  rdf:rest   rdf:nil .
     * </pre>
     * 
     * where _:b0, etc. are blank nodes not appearing elsewhere in the query.
     * <p>
     * Note: blank nodes are translated into anonymous variables in the parse
     * tree before the bigdata AST model is generated.
     */
    public void test_rdf_collections_02() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = ""//
                + "PREFIX  : <http://example.org/ns#>\n"//
                + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"//
                + "SELECT * { ( 1 [:p :q] ( 2 ) ) }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.org/ns#");
                prefixDecls.put("rdf", RDF.NAMESPACE);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("*"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final VarNode b0 = makeAnon(new VarNode("-anon-11"));
                final VarNode b1 = makeAnon(new VarNode("-anon-1-12"));
                final VarNode b2 = makeAnon(new VarNode("-anon-23"));
                final VarNode b3 = makeAnon(new VarNode("-anon-1-24"));
                final VarNode b4 = makeAnon(new VarNode("-anon-35"));
                
                final ConstantNode rdfFirst = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.FIRST.stringValue())));
                final ConstantNode rdfRest = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.REST.stringValue())));
                final ConstantNode rdfNil= new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.NIL.stringValue())));

                final ConstantNode ONE = new ConstantNode(
                        makeIV(valueFactory.createLiteral("1", XSD.INTEGER)));
                final ConstantNode TWO = new ConstantNode(
                        makeIV(valueFactory.createLiteral("2", XSD.INTEGER)));
                final ConstantNode p = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/ns#p")));
                final ConstantNode q = new ConstantNode(
                        makeIV(valueFactory.createURI("http://example.org/ns#q")));

//                     _:b0  rdf:first  1 ;
//                           rdf:rest   _:b1 .
//                     _:b1  rdf:first  _:b2 .
//                     _:b2  :p         :q .
//                     _:b1  rdf:rest   _:b3 .
//                     _:b3  rdf:first  _:b4 .
//                     _:b4  rdf:first  2 ;
//                           rdf:rest   rdf:nil .
//                     _:b3  rdf:rest   rdf:nil .

                whereClause.addChild(new StatementPatternNode(b0, rdfFirst, ONE   , null/* c */, Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(new StatementPatternNode(b0, rdfRest , b1    , null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(b2, p,        q     , null/* c */, Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(new StatementPatternNode(b1, rdfFirst, b2    , null/* c */, Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(new StatementPatternNode(b1, rdfRest , b3    , null/* c */, Scope.DEFAULT_CONTEXTS));
                
                whereClause.addChild(new StatementPatternNode(b4, rdfFirst, TWO   , null/* c */, Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(new StatementPatternNode(b4, rdfRest,  rdfNil, null/* c */, Scope.DEFAULT_CONTEXTS));
                
                whereClause.addChild(new StatementPatternNode(b3, rdfFirst, b4    , null/* c */, Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(new StatementPatternNode(b3, rdfRest , rdfNil, null/* c */, Scope.DEFAULT_CONTEXTS));

            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for the RDF Collections syntax.
     * <pre>
     * PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>
     * PREFIX  : <http://example.org/ns#>
     * SELECT ?p { :x ?p (1) . }
     * ===================================
     * QueryRoot
     *    Projection
     *       ProjectionElemList
     *          ProjectionElem "p"
     *       Join
     *          Join
     *             StatementPattern
     *                Var (name=-anon-1, anonymous)
     *                Var (name=-const-2, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#first, anonymous)
     *                Var (name=-const-1, value="1"^^<http://www.w3.org/2001/XMLSchema#integer>, anonymous)
     *             StatementPattern
     *                Var (name=-anon-1, anonymous)
     *                Var (name=-const-4, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#rest, anonymous)
     *                Var (name=-const-3, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#nil, anonymous)
     *          StatementPattern
     *             Var (name=-const-5, value=http://example.org/ns#x, anonymous)
     *             Var (name=p)
     *             Var (name=-anon-1, anonymous)
     * </pre>
     */
    public void test_rdf_collections_03() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
        		"PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>\n"+
        		"PREFIX  : <http://example.org/ns#>\n"+
        		"SELECT ?p { :x ?p (1) . }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("xsd", XSD.NAMESPACE);
                prefixDecls.put("", "http://example.org/ns#");
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("p"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final VarNode p = new VarNode("p");
                
                final VarNode b0 = makeAnon(new VarNode("-anon-11"));

                final ConstantNode rdfFirst = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.FIRST.stringValue())));
                final ConstantNode rdfRest = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.REST.stringValue())));
                final ConstantNode rdfNil= new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.NIL.stringValue())));

                final ConstantNode x = new ConstantNode(
                        makeIV(valueFactory
                                .createURI("http://example.org/ns#x")));

                final ConstantNode ONE = new ConstantNode(
                        makeIV(valueFactory.createLiteral("1", XSD.INTEGER)));

//                *       Join
//                *          Join
//                *             StatementPattern
//                *                Var (name=-anon-1, anonymous)
//                *                Var (name=-const-2, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#first, anonymous)
//                *                Var (name=-const-1, value="1"^^<http://www.w3.org/2001/XMLSchema#integer>, anonymous)
//                *             StatementPattern
//                *                Var (name=-anon-1, anonymous)
//                *                Var (name=-const-4, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#rest, anonymous)
//                *                Var (name=-const-3, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#nil, anonymous)
//                *          StatementPattern
//                *             Var (name=-const-5, value=http://example.org/ns#x, anonymous)
//                *             Var (name=p)
//                *             Var (name=-anon-1, anonymous)
                whereClause.addChild(new StatementPatternNode(b0, rdfFirst, ONE   , null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(b0, rdfRest , rdfNil, null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(x,  p,        b0    , null/* c */, Scope.DEFAULT_CONTEXTS));
                
            }
        
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * PREFIX : <http://example.org/ns#>
     * 
     * SELECT ?p ?v { :x ?p (?v) . }
     * ===================================
     * QueryRoot
     *    Projection
     *       ProjectionElemList
     *          ProjectionElem "p"
     *          ProjectionElem "v"
     *       Join
     *          Join
     *             StatementPattern
     *                Var (name=-anon-1, anonymous)
     *                Var (name=-const-1, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#first, anonymous)
     *                Var (name=v)
     *             StatementPattern
     *                Var (name=-anon-1, anonymous)
     *                Var (name=-const-3, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#rest, anonymous)
     *                Var (name=-const-2, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#nil, anonymous)
     *          StatementPattern
     *             Var (name=-const-4, value=http://example.org/ns#x, anonymous)
     *             Var (name=p)
     *             Var (name=-anon-1, anonymous)
     * </pre>
     */
    public void test_rdf_collections_04() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
                "PREFIX  : <http://example.org/ns#>\n"+
                "SELECT ?p ?v { :x ?p (?v) . }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.org/ns#");
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.addProjectionVar(new VarNode("p"));
                projection.addProjectionVar(new VarNode("v"));
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final VarNode p = new VarNode("p");
                final VarNode v = new VarNode("v");
                
                final VarNode b0 = makeAnon(new VarNode("-anon-11"));
                
                final ConstantNode rdfFirst = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.FIRST.stringValue())));
                final ConstantNode rdfRest = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.REST.stringValue())));
                final ConstantNode rdfNil= new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.NIL.stringValue())));

                final ConstantNode x = new ConstantNode(
                        makeIV(valueFactory
                                .createURI("http://example.org/ns#x")));

//                *       Join
//                *          Join
//                *             StatementPattern
//                *                Var (name=-anon-1, anonymous)
//                *                Var (name=-const-1, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#first, anonymous)
//                *                Var (name=v)
//                *             StatementPattern
//                *                Var (name=-anon-1, anonymous)
//                *                Var (name=-const-3, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#rest, anonymous)
//                *                Var (name=-const-2, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#nil, anonymous)
//                *          StatementPattern
//                *             Var (name=-const-4, value=http://example.org/ns#x, anonymous)
//                *             Var (name=p)
//                *             Var (name=-anon-1, anonymous)
            
                whereClause.addChild(new StatementPatternNode(b0, rdfFirst, v     , null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(b0, rdfRest , rdfNil, null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(x,  p,        b0    , null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * PREFIX : <http://example.org/ns#>
     * 
     * SELECT ?p ?v ?w { :x ?p (?v ?w) . }
     * ===================================
     * QueryRoot
     *    Projection
     *       ProjectionElemList
     *          ProjectionElem "p"
     *          ProjectionElem "v"
     *          ProjectionElem "w"
     *       Join
     *          Join
     *             Join
     *                Join
     *                   StatementPattern
     *                      Var (name=-anon-1, anonymous)
     *                      Var (name=-const-1, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#first, anonymous)
     *                      Var (name=v)
     *                   StatementPattern
     *                      Var (name=-anon-1, anonymous)
     *                      Var (name=-const-2, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#rest, anonymous)
     *                      Var (name=-anon-1-1, anonymous)
     *                StatementPattern
     *                   Var (name=-anon-1-1, anonymous)
     *                   Var (name=-const-3, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#first, anonymous)
     *                   Var (name=w)
     *             StatementPattern
     *                Var (name=-anon-1-1, anonymous)
     *                Var (name=-const-5, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#rest, anonymous)
     *                Var (name=-const-4, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#nil, anonymous)
     *          StatementPattern
     *             Var (name=-const-6, value=http://example.org/ns#x, anonymous)
     *             Var (name=p)
     *             Var (name=-anon-1, anonymous)
     * </pre>
     */
    public void test_rdf_collections_05() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
                "PREFIX  : <http://example.org/ns#>\n"+
                "SELECT ?p ?v ?w { :x ?p (?v ?w) . }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("", "http://example.org/ns#");
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);
                projection.addProjectionVar(new VarNode("p"));
                projection.addProjectionVar(new VarNode("v"));
                projection.addProjectionVar(new VarNode("w"));
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final VarNode p = new VarNode("p");
                final VarNode v = new VarNode("v");
                final VarNode w = new VarNode("w");
                
                final VarNode b0 = makeAnon(new VarNode("-anon-11"));
                final VarNode b1 = makeAnon(new VarNode("-anon-1-12"));
                
                final ConstantNode rdfFirst = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.FIRST.stringValue())));
                final ConstantNode rdfRest = new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.REST.stringValue())));
                final ConstantNode rdfNil= new ConstantNode(
                        makeIV(valueFactory.createURI(RDF.NIL.stringValue())));

                final ConstantNode x = new ConstantNode(
                        makeIV(valueFactory
                                .createURI("http://example.org/ns#x")));

//                *       Join
//                *          Join
//                *             Join
//                *                Join
//                *                   StatementPattern
//                *                      Var (name=-anon-1, anonymous)
//                *                      Var (name=-const-1, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#first, anonymous)
//                *                      Var (name=v)
//                *                   StatementPattern
//                *                      Var (name=-anon-1, anonymous)
//                *                      Var (name=-const-2, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#rest, anonymous)
//                *                      Var (name=-anon-1-1, anonymous)
//                *                StatementPattern
//                *                   Var (name=-anon-1-1, anonymous)
//                *                   Var (name=-const-3, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#first, anonymous)
//                *                   Var (name=w)
//                *             StatementPattern
//                *                Var (name=-anon-1-1, anonymous)
//                *                Var (name=-const-5, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#rest, anonymous)
//                *                Var (name=-const-4, value=http://www.w3.org/1999/02/22-rdf-syntax-ns#nil, anonymous)
//                *          StatementPattern
//                *             Var (name=-const-6, value=http://example.org/ns#x, anonymous)
//                *             Var (name=p)
//                *             Var (name=-anon-1, anonymous)
            
                whereClause.addChild(new StatementPatternNode(b0, rdfFirst, v     , null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(b0, rdfRest , b1    , null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(b1, rdfFirst, w     , null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(b1, rdfRest , rdfNil, null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(x , p       , b0    , null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for BASE declaration.
     * 
     * <pre>
     * BASE <http://example.org/book/>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * SELECT $title
     * WHERE { <book1> dc:title ?title }
     * </pre>
     * 
     * Note: There is a trailing <code>/</code> on the BASE URI declaration
     * for this example.
     * <p>
     * Note: This also uses the alternative <code>$title</code> syntax, but
     * <code>$title</code> and <code>?title</code> are the same variable.
     */
    public void test_base_01() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" + //
                "BASE <http://example.org/book/>\n"+//
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + //
                "SELECT $title\n" +//
                "WHERE { <book1> dc:title ?title }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("dc", DCElementsVocabularyDecl.NAMESPACE);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("title"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final ConstantNode book1 = new ConstantNode(
                        makeIV(valueFactory
                                .createURI("http://example.org/book/book1")));

                final ConstantNode dc_title = new ConstantNode(
                        makeIV(valueFactory
                                .createURI(DCElementsVocabularyDecl.title
                                        .stringValue())));

                whereClause.addChild(new StatementPatternNode(book1, dc_title,
                        new VarNode("title"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Base URI resolution appears to be busted per [1]. This static flag is
     * used to disable the base URI resolution tests which fail until the
     * underlying issue is fixed in {@link ParsedURI#resolve(ParsedURI)}.
     * 
     * [1] http://www.openrdf.org/issues/browse/SES-838
     */
    private static final boolean baseURIBusted = true;
    
    /**
     * Unit test for BASE declaration.
     * 
     * <pre>
     * BASE <http://example.org/book#>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * SELECT $title
     * WHERE { <book1> dc:title ?title }
     * </pre>
     * 
     * Note: There is a trailing <code>#</code> on the BASE URI declaration for
     * this example. Per RFC3986, the fragment must be stripped from the base
     * URI before it is used for resolution.
     * <p>
     * Note: This also uses the alternative <code>$title</code> syntax, but
     * <code>$title</code> and <code>?title</code> are the same variable.
     */
    public void test_base_02() throws MalformedQueryException,
            TokenMgrError, ParseException {

        if (baseURIBusted)
            return;
        
        final String sparql = "" + //
                "BASE <http://example.org/book#>\n"+//
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + //
                "SELECT $title\n" +//
                "WHERE { <book1> dc:title ?title }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("dc", DCElementsVocabularyDecl.NAMESPACE);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("title"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                // Note: "
                final ConstantNode book1 = new ConstantNode(
                        makeIV(valueFactory
                                .createURI("http://example.org/book/book1")));

                final ConstantNode dc_title = new ConstantNode(
                        makeIV(valueFactory
                                .createURI(DCElementsVocabularyDecl.title
                                        .stringValue())));

                whereClause.addChild(new StatementPatternNode(book1, dc_title,
                        new VarNode("title"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for BASE declaration.
     * 
     * <pre>
     * BASE <http://example.org/book#abc>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * SELECT $title
     * WHERE { <book1> dc:title ?title }
     * </pre>
     * 
     * Note: There is a trailing <code>#abc</code> on the BASE URI declaration
     * for this example. Per RFC3986, the fragment must be stripped from the
     * base URI before it is used for resolution.
     * <p>
     * Note: This also uses the alternative <code>$title</code> syntax, but
     * <code>$title</code> and <code>?title</code> are the same variable.
     */
    public void test_base_03() throws MalformedQueryException,
            TokenMgrError, ParseException {

        if (baseURIBusted)
            return;

        final String sparql = "" + //
                "BASE <http://example.org/book#abc>\n"+//
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + //
                "SELECT $title\n" +//
                "WHERE { <book1> dc:title ?title }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("dc", DCElementsVocabularyDecl.NAMESPACE);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("title"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                // Note: "
                final ConstantNode book1 = new ConstantNode(
                        makeIV(valueFactory
                                .createURI("http://example.org/book/book1")));

                final ConstantNode dc_title = new ConstantNode(
                        makeIV(valueFactory
                                .createURI(DCElementsVocabularyDecl.title
                                        .stringValue())));

                whereClause.addChild(new StatementPatternNode(book1, dc_title,
                        new VarNode("title"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for BASE declaration.
     * 
     * <pre>
     * BASE <http://example.org/book>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * SELECT $title
     * WHERE { <book1> dc:title ?title }
     * </pre>
     * 
     * Note: There is nothing trailing on the BASE URI declaration for this
     * example.
     * <p>
     * Note: This also uses the alternative <code>$title</code> syntax, but
     * <code>$title</code> and <code>?title</code> are the same variable.
     */
    public void test_base_04() throws MalformedQueryException,
            TokenMgrError, ParseException {

        if (baseURIBusted)
            return;

        final String sparql = "" + //
                "BASE <http://example.org/book>\n"+//
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + //
                "SELECT $title\n" +//
                "WHERE { <book1> dc:title ?title }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
                prefixDecls.put("dc", DCElementsVocabularyDecl.NAMESPACE);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("title"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final ConstantNode book1 = new ConstantNode(
                        makeIV(valueFactory
                                .createURI("http://example.org/book/book1")));

                final ConstantNode dc_title = new ConstantNode(
                        makeIV(valueFactory
                                .createURI(DCElementsVocabularyDecl.title
                                        .stringValue())));

                whereClause.addChild(new StatementPatternNode(book1, dc_title,
                        new VarNode("title"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

}
