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

import java.util.LinkedHashMap;
import java.util.Map;

import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ExistsNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.NotExistsNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

/**
 * Test suite for translating queries which use subquery constructions,
 * including {@link SubqueryRoot}, {@link NamedSubqueryRoot} and
 * {@link NamedSubqueryInclude} and {@link ExistsNode}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestGroupGraphPatternBuilder.java 5064 2011-08-21 22:50:55Z
 *          thompsonbry $
 */
public class TestSubqueryPatterns extends
        AbstractBigdataExprBuilderTestCase {

    /**
     * 
     */
    public TestSubqueryPatterns() {
    }

    /**
     * @param name
     */
    public TestSubqueryPatterns(String name) {
        super(name);
    }
    
    /**
     * Unit test for simple subquery without anything else in the outer join
     * group.
     * 
     * <pre>
     * SELECT ?s where {{SELECT ?s where {?s ?p ?o}}}
     * </pre>
     * 
     * Note: This requires recursion back in through the
     * {@link BigdataExprBuilder}.
     */
    public void test_subSelect() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where { {select ?s where { ?s ?p ?o  } } }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect;
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                subSelect = new SubqueryRoot(QueryType.SELECT);
//                whereClause.addChild(subSelect);

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                wrapperGroup.addChild(subSelect);
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addProjectionVar(new VarNode("s"));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("s"), new VarNode("p"), new VarNode("o"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
   
    /**
     * Unit test for simple optional subquery without anything else in the outer
     * join group.
     * 
     * <pre>
     * SELECT ?s where { OPTIONAL {SELECT ?s where {?s ?p ?o}}}
     * </pre>
     * 
     * Note: This requires recursion back in through the
     * {@link BigdataExprBuilder}.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/806>
     *      Incorrect AST generated for OPTIONAL { SELECT }</a>
     */
    public void test_optional_subSelect() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where { optional {select ?s where { ?s ?p ?o  } } }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect;
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                subSelect = new SubqueryRoot(QueryType.SELECT);
//                whereClause.addChild(subSelect);

                final JoinGroupNode wrapperGroup = new JoinGroupNode(true/* optional */);
                whereClause.addChild(wrapperGroup);
                wrapperGroup.addChild(subSelect);
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addProjectionVar(new VarNode("s"));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("s"), new VarNode("p"), new VarNode("o"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
   
    /**
     * Unit test for simple subquery joined with a triple pattern in the outer
     * join group.
     * 
     * <pre>
     * SELECT ?s where { ?s ?x ?o . {SELECT ?x where {?x ?p ?x}}}
     * </pre>
     */
    public void test_triplePattern_join_subSelect() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s " //
                + "{"//
                + " ?s ?x ?o "
                + " {"//
                + "   select ?x where { ?x ?p ?x }" //
                +"  }"//
                + "}"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect;
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(
                                new VarNode("s"),
                                new VarNode("x"),
                                new VarNode("o"),
                                null/*c*/,
                                StatementPattern.Scope.DEFAULT_CONTEXTS
                                ));
                
                subSelect = new SubqueryRoot(QueryType.SELECT);
                
//                whereClause.addChild(subSelect);
                
                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                wrapperGroup.addChild(subSelect);
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addProjectionVar(new VarNode("x"));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("x"), new VarNode("p"), new VarNode("x"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * SELECT * { SELECT * { ?s ?p ?o } }
     * </pre>
     */
    public void test_select_star_select_star_s_p_o()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select * { select * { ?s ?p ?o } }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect;
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("*"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

//                whereClause.addChild(new StatementPatternNode(new VarNode("s"),
//                        new VarNode("x"), new VarNode("o"), null/* c */,
//                        StatementPattern.Scope.DEFAULT_CONTEXTS));

                subSelect = new SubqueryRoot(QueryType.SELECT);

                final JoinGroupNode wrapperGroup = new JoinGroupNode(subSelect);
                whereClause.addChild(wrapperGroup);
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addProjectionVar(new VarNode("*"));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("s"), new VarNode("p"), new VarNode("o"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for simple subquery joined with a bind.
     * 
     * <pre>
     * SELECT ?s where { bind(<http://www.bigdata.com> as ?o) { SELECT ?s where {?s ?p ?o} } }
     * </pre>
     * 
     * Note: This requires recursion back in through the
     * {@link BigdataExprBuilder}.
     */
    public void test_bind_join_subSelect() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql//
              = "select ?s" +//
        		" where {" +//
        		"   bind( <http://www.bigdata.com> as ?o )" +//
        		"   {" +//
        		"     select ?s where { ?s ?p ?o  }" +//
        		"   }" +//
        		"}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect;
        {

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            {

                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new AssignmentNode(new VarNode("o"),
                        new ConstantNode(makeIV(valueFactory
                                .createURI("http://www.bigdata.com")))));

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                
                subSelect = new SubqueryRoot(QueryType.SELECT);
                wrapperGroup.addChild(subSelect);
                
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addProjectionVar(new VarNode("s"));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("s"), new VarNode("p"), new VarNode("o"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
   
    /**
     * Unit test for sub-SubSelect. There is a top-level query. It uses a
     * subquery. The subquery uses a subquery. The purpose of this is to test
     * for the correct nesting of the generated AST.
     */
    public void test_subSubSelect() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql//
              = "select ?s \n" +//
                " where { \n" +//
                "   bind( <http://www.bigdata.com> as ?o ) \n" +//
                "   { \n" +//
                "     select ?s \n" +//
                "      where { \n" +//
                "        ?s ?p ?o . \n" +//
                "        { select ?o { bind( 12 as ?o ) } } \n" +//
                "      } \n" +//
                "   }\n" +//
                "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final SubqueryRoot subSelect1;
        final SubqueryRoot subSelect2;
        {

            final VarNode s = new VarNode("s");
            final VarNode p = new VarNode("p");
            final VarNode o = new VarNode("o");
            
            final ConstantNode const1 = new ConstantNode(
                    makeIV(valueFactory.createURI("http://www.bigdata.com")));
            
            final ConstantNode const2 = new ConstantNode(
                    makeIV(valueFactory.createLiteral("12", XSD.INTEGER)));
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            {

                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(s);
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                whereClause.addChild(new AssignmentNode(o, const1));

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                
                subSelect1 = new SubqueryRoot(QueryType.SELECT);
                wrapperGroup.addChild(subSelect1);
                
            }

            // subSelect2
            {
                
                subSelect2 = new SubqueryRoot(QueryType.SELECT);
                
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(o);
                subSelect2.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                subSelect2.setWhereClause(whereClause);

                whereClause.addChild(new AssignmentNode(o, const2));

            }
            
            // subSelect1
            {

                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(s);
                subSelect1.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                subSelect1.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(s, p, o,
                        null/* c */, Scope.DEFAULT_CONTEXTS));

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                wrapperGroup.addChild(subSelect2);
                whereClause.addChild(wrapperGroup);
                
            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for EXISTS.
     * 
     * <pre>
     * PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
     * PREFIX  foaf:   <http://xmlns.com/foaf/0.1/> 
     * 
     * SELECT ?person
     * WHERE 
     * {
     *     ?person rdf:type  foaf:Person .
     *     FILTER EXISTS { ?person foaf:name ?name }
     * }
     * </pre>
     */
    public void test_exists() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = ""//
                + "PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"//
                + "PREFIX  foaf:   <http://xmlns.com/foaf/0.1/> \n"//
                + "SELECT ?person \n"//
                + " WHERE { \n"//
                + "       ?person rdf:type  foaf:Person . \n"//
                + "       FILTER EXISTS { ?person foaf:name ?name } \n"//
                + "}"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ConstantNode rdfType = new ConstantNode(
                    makeIV(valueFactory.createURI(RDF.TYPE.stringValue())));

            final ConstantNode foafPerson = new ConstantNode(
                    makeIV(valueFactory.createURI(FOAF.PERSON.stringValue())));

            final ConstantNode foafName = new ConstantNode(
                    makeIV(valueFactory.createURI(FOAF.NAME.stringValue())));

            final VarNode person = new VarNode("person");

            final VarNode name = new VarNode("name");

            final VarNode anonvar = mockAnonVar("-exists-1");

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("rdf", RDF.NAMESPACE);
                prefixDecls.put("foaf", FOAFVocabularyDecl.NAMESPACE);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.addProjectionVar(person);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(person, rdfType,
                    foafPerson, null/* c */, Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode existsPattern = new JoinGroupNode();
            existsPattern.addChild(new StatementPatternNode(person, foafName,
                    name, null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new FilterNode(new ExistsNode(anonvar,
                    existsPattern)));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for NOT EXISTS.
     * 
     * <pre>
     * PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
     * PREFIX  foaf:   <http://xmlns.com/foaf/0.1/> 
     * 
     * SELECT ?person
     * WHERE 
     * {
     *     ?person rdf:type  foaf:Person .
     *     FILTER NOT EXISTS { ?person foaf:name ?name }
     * }
     * </pre>
     */
    public void test_not_exists() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = ""//
                + "PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"//
                + "PREFIX  foaf:   <http://xmlns.com/foaf/0.1/> \n"//
                + "SELECT ?person \n"//
                + " WHERE { \n"//
                + "       ?person rdf:type  foaf:Person . \n"//
                + "       FILTER NOT EXISTS { ?person foaf:name ?name } \n"//
                + "}"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ConstantNode rdfType = new ConstantNode(
                    makeIV(valueFactory.createURI(RDF.TYPE.stringValue())));

            final ConstantNode foafPerson = new ConstantNode(
                    makeIV(valueFactory.createURI(FOAF.PERSON.stringValue())));

            final ConstantNode foafName = new ConstantNode(
                    makeIV(valueFactory.createURI(FOAF.NAME.stringValue())));

            final VarNode person = new VarNode("person");

            final VarNode name = new VarNode("name");

            final VarNode anonvar = mockAnonVar("-exists-1");

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("rdf", RDF.NAMESPACE);
                prefixDecls.put("foaf", FOAFVocabularyDecl.NAMESPACE);
                expected.setPrefixDecls(prefixDecls);
            }

            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.addProjectionVar(person);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            whereClause.addChild(new StatementPatternNode(person, rdfType,
                    foafPerson, null/* c */, Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode existsPattern = new JoinGroupNode();
            existsPattern.addChild(new StatementPatternNode(person, foafName,
                    name, null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new FilterNode(new NotExistsNode(anonvar,
                    existsPattern)));

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
     * 
     * TODO Unit test to verify no recursion through the named subquery into a
     * named subquery (this will be rejected at the AST level so it is not
     * critical to validate it here).
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
                "\n        bind ( rdfs:subPropertyOf as ?x )"+//
                "\n        ?p ?x ?p2" + //
//                "\n        ?p rdfs:subPropertyOf ?p2" + //
                "\n        INCLUDE %namedSet1" + //
                "\n}"//
        ;

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final String namedSet = "%namedSet1";
            
            final VarNode s = new VarNode("s");
            final VarNode p = new VarNode("p");
            final VarNode o = new VarNode("o");
            final VarNode p2 = new VarNode("p2");
            final VarNode x = new VarNode("x");

            final TermNode subPropertyOf = new ConstantNode(
                    makeIV(valueFactory.createURI(RDFS.SUBPROPERTYOF
                            .stringValue())));
            
            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                prefixDecls.put("rdfs", RDFS.NAMESPACE);
                expected.setPrefixDecls(prefixDecls);
            }
            
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
            whereClause.addChild(new AssignmentNode(x,subPropertyOf));
            whereClause.addChild(new StatementPatternNode(p, x, p2,
                    null/* c */, Scope.DEFAULT_CONTEXTS));
//            whereClause.addChild(new StatementPatternNode(p, subPropertyOf, p2,
//                    null/* c */, Scope.DEFAULT_CONTEXTS));
            final NamedSubqueryInclude includeNode = new NamedSubqueryInclude(
                    namedSet);
            whereClause.addChild(includeNode);

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

//    /**
//     * Unit test for WITH {subquery} AS "name" and INCLUDE. The WITH must be in
//     * the top-level query. This example tests the use of the JOIN ON query
//     * hint. For example:
//     * 
//     * <pre>
//     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//     * SELECT ?p2
//     * WITH {
//     *         SELECT DISTINCT ?p
//     *         WHERE {
//     *                 ?s ?p ?o
//     *         }
//     * } AS %namedSet1
//     *  WHERE {
//     *         ?p rdfs:subPropertyOf ?p2
//     *         INCLUDE %namedSet1 JOIN ON (?p2)
//     * }
//     * </pre>
//     */
//    public void test_namedSubquery_joinOn() throws MalformedQueryException,
//            TokenMgrError, ParseException {
//
//        final String sparql = //
//                "\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" + //
//                "\nSELECT ?p2" + //
//                "\n WITH {" + //
//                "\n         SELECT DISTINCT ?p" + //
//                "\n         WHERE {" + //
//                "\n                 ?s ?p ?o" + //
//                "\n          }" + //
//                "\n } AS %namedSet1" + //
//                "\n WHERE {" + //
//                "\n        bind ( rdfs:subPropertyOf as ?x )"+//
//                "\n        ?p ?x ?p2" + //
////                "\n        INCLUDE %namedSet1" + //
//                "\n        INCLUDE %namedSet1 JOIN ON ( ?p2 )" + //
//                "\n}"//
//        ;
//
//        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
//        {
//
//            final String namedSet = "%namedSet1";
//            
//            final VarNode s = new VarNode("s");
//            final VarNode p = new VarNode("p");
//            final VarNode o = new VarNode("o");
//            final VarNode p2 = new VarNode("p2");
//            final VarNode x = new VarNode("x");
//
//            final TermNode subPropertyOf = new ConstantNode(
//                    makeIV(valueFactory.createURI(RDFS.SUBPROPERTYOF
//                            .stringValue())));
//            
//            {
//                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
//                prefixDecls.put("rdfs", RDFS.NAMESPACE);
//                expected.setPrefixDecls(prefixDecls);
//            }
//
//            {
//                final ProjectionNode projection = new ProjectionNode();
//                projection.addProjectionVar(p2);
//                expected.setProjection(projection);
//            }
//            
//            final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
//            expected.setNamedSubqueries(namedSubqueries);
//            {
//
//                final NamedSubqueryRoot namedSubqueryRoot = new NamedSubqueryRoot(
//                        QueryType.SELECT, namedSet);
//                
//                final ProjectionNode projection = new ProjectionNode();
//                namedSubqueryRoot.setProjection(projection);
//                projection.addProjectionVar(p);
//                projection.setDistinct(true);
//                
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                namedSubqueryRoot.setWhereClause(whereClause);
//                whereClause.addChild(new StatementPatternNode(s, p, o,
//                        null/* c */, Scope.DEFAULT_CONTEXTS));
//                
//                namedSubqueries.add(namedSubqueryRoot);
//                
//            }
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            expected.setWhereClause(whereClause);
//            whereClause.addChild(new AssignmentNode(x,subPropertyOf));
//            whereClause.addChild(new StatementPatternNode(p, x, p2,
//                    null/* c */, Scope.DEFAULT_CONTEXTS));
////            whereClause.addChild(new StatementPatternNode(p, subPropertyOf, p2,
////                    null/* c */, Scope.DEFAULT_CONTEXTS));
//            final NamedSubqueryInclude includeNode = new NamedSubqueryInclude(
//                    namedSet);
//            includeNode.setJoinVars(new VarNode[] { p2 });
//            whereClause.addChild(includeNode);
//            
//        }
//        
//        final QueryRoot actual = parse(sparql, baseURI);
//
//        assertSameAST(sparql, expected, actual);
//
//    }
//
//    /**
//     * Unit test for WITH {subquery} AS "name" and INCLUDE. The WITH must be in
//     * the top-level query. This example tests the use of the JOIN ON query
//     * hint, but explicitly specifies an empty join variable list. For example:
//     * 
//     * <pre>
//     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//     * SELECT ?p2
//     * WITH {
//     *         SELECT DISTINCT ?p
//     *         WHERE {
//     *                 ?s ?p ?o
//     *         }
//     * } AS %namedSet1
//     *  WHERE {
//     *         ?p rdfs:subPropertyOf ?p2
//     *         INCLUDE %namedSet1 JOIN ON ()
//     * }
//     * </pre>
//     */
//    public void test_namedSubquery_joinOn_noVars() throws MalformedQueryException,
//            TokenMgrError, ParseException {
//
//        final String sparql = //
//                "\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" + //
//                "\nSELECT ?p2" + //
//                "\n WITH {" + //
//                "\n         SELECT DISTINCT ?p" + //
//                "\n         WHERE {" + //
//                "\n                 ?s ?p ?o" + //
//                "\n          }" + //
//                "\n } AS %namedSet1" + //
//                "\n WHERE {" + //
//                "\n        bind ( rdfs:subPropertyOf as ?x )"+//
//                "\n        ?p ?x ?p2" + //
////                "\n        INCLUDE %namedSet1" + //
//                "\n        INCLUDE %namedSet1 JOIN ON ( )" + //
//                "\n}"//
//        ;
//
//        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
//        {
//
//            final String namedSet = "%namedSet1";
//            
//            final VarNode s = new VarNode("s");
//            final VarNode p = new VarNode("p");
//            final VarNode o = new VarNode("o");
//            final VarNode p2 = new VarNode("p2");
//            final VarNode x = new VarNode("x");
//
//            final TermNode subPropertyOf = new ConstantNode(
//                    makeIV(valueFactory.createURI(RDFS.SUBPROPERTYOF
//                            .stringValue())));
//            
//            {
//                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
//                prefixDecls.put("rdfs", RDFS.NAMESPACE);
//                expected.setPrefixDecls(prefixDecls);
//            }
//
//            {
//                final ProjectionNode projection = new ProjectionNode();
//                projection.addProjectionVar(p2);
//                expected.setProjection(projection);
//            }
//            
//            final NamedSubqueriesNode namedSubqueries = new NamedSubqueriesNode();
//            expected.setNamedSubqueries(namedSubqueries);
//            {
//
//                final NamedSubqueryRoot namedSubqueryRoot = new NamedSubqueryRoot(
//                        QueryType.SELECT, namedSet);
//                
//                final ProjectionNode projection = new ProjectionNode();
//                namedSubqueryRoot.setProjection(projection);
//                projection.addProjectionVar(p);
//                projection.setDistinct(true);
//                
//                final JoinGroupNode whereClause = new JoinGroupNode();
//                namedSubqueryRoot.setWhereClause(whereClause);
//                whereClause.addChild(new StatementPatternNode(s, p, o,
//                        null/* c */, Scope.DEFAULT_CONTEXTS));
//                
//                namedSubqueries.add(namedSubqueryRoot);
//                
//            }
//            
//            final JoinGroupNode whereClause = new JoinGroupNode();
//            expected.setWhereClause(whereClause);
//            whereClause.addChild(new AssignmentNode(x,subPropertyOf));
//            whereClause.addChild(new StatementPatternNode(p, x, p2,
//                    null/* c */, Scope.DEFAULT_CONTEXTS));
////            whereClause.addChild(new StatementPatternNode(p, subPropertyOf, p2,
////                    null/* c */, Scope.DEFAULT_CONTEXTS));
//            final NamedSubqueryInclude includeNode = new NamedSubqueryInclude(
//                    namedSet);
//            includeNode.setJoinVars(new VarNode[] { /*p2*/});
//            whereClause.addChild(includeNode);
//            
//        }
//        
//        final QueryRoot actual = parse(sparql, baseURI);
//
//        assertSameAST(sparql, expected, actual);
//
//    }

}
