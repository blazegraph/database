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

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.InlineUnsignedIntegerURIHandler;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.constraints.ComputedIN;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.MockedValueIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedLongIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.ast.ParseException;
import com.bigdata.rdf.sail.sparql.ast.TokenMgrError;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.TestInsertRate.XMLSchema;

/**
 * Test suite for translating the openrdf SPARQL AST nodes for
 * <code>GroupGraphPattern</code> into the bigdata AST (join groups, union,
 * etc).
 * 
 * @see TestSubqueryPatterns
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestGroupGraphPatternBuilder.java 5064 2011-08-21 22:50:55Z
 *          thompsonbry $
 */
public class TestGroupGraphPatternBuilder extends
        AbstractBigdataExprBuilderTestCase {

    /**
     * 
     */
    public TestGroupGraphPatternBuilder() {
    }

    /**
     * @param name
     */
    public TestGroupGraphPatternBuilder(String name) {
        super(name);
    }

    /**
     * Test empty group. The group pattern:
     * 
     * <pre>
     * {}
     * </pre>
     * 
     * matches any graph (including the empty graph) with one solution that does
     * not bind any variables. For example:
     * 
     * <pre>
     * SELECT ?x
     * WHERE {}
     * </pre>
     * 
     * matches with one solution in which variable x is not bound.
     */
    public void test_empty_group() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where { }";

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
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Unit test for named graph triple pattern where the graph is a variable.
     * 
     * <pre>
     * SELECT ?s where { GRAPH ?src { ?s ?p ?o} }
     * </pre>
     */
    public void test_named_graph_pattern() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where {GRAPH ?src {?s ?p ?o}}";

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
            
            final JoinGroupNode graphGroup = new JoinGroupNode();
            whereClause.addChild(graphGroup);
            graphGroup.setContext(new VarNode("src"));
            graphGroup.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), new VarNode("src"),
                    Scope.NAMED_CONTEXTS));
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Test for a named graph triple pattern where the graph is a constant.
     * <pre>
     * select ?s where {GRAPH <http://www.bigdata.com> {?s ?p ?o}}
     * </pre>
     */
    public void test_named_graph_pattern_graphConstant() throws MalformedQueryException,
            TokenMgrError, ParseException {
       
        final String sparql = "select ?s where {GRAPH <http://www.bigdata.com> {?s ?p ?o}}";

        final IV<BigdataValue, ?> graphConst = makeIV(valueFactory
                .createURI("http://www.bigdata.com"));

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
            final JoinGroupNode graphGroup = new JoinGroupNode();
            graphGroup.setContext(new ConstantNode(graphConst));
            graphGroup.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), new ConstantNode(
                            graphConst), Scope.NAMED_CONTEXTS));
            whereClause.addChild(graphGroup);
            expected.setWhereClause(whereClause);
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for the join of a triple pattern with a join group containing
     * only a single triple pattern.
     * 
     * <pre>
     * select ?s where { ?s ?p ?o . {?o ?p2 ?s}}
     * </pre>
     */
    public void test_triple_pattern_with_simple_join_group()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o . { ?o ?p2 ?s } }";

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
            {
            
                final JoinGroupNode joinGroup = new JoinGroupNode();
                
                joinGroup.addChild(new StatementPatternNode(new VarNode("o"),
                        new VarNode("p2"), new VarNode("s"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
                
                whereClause.addChild(joinGroup);
                
            }
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for the join of a triple pattern with a join group containing
     * only a single triple pattern.
     * 
     * <pre>
     * select ?s where { ?s ?p ?o . GRAPH ?src {?o ?p2 ?s}}
     * </pre>
     */
    public void test_triple_pattern_with_named_graph_group()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o . GRAPH ?src { ?o ?p2 ?s } }";

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
            {
            
                final JoinGroupNode joinGroup = new JoinGroupNode();
                
                joinGroup.setContext(new VarNode("src"));

                joinGroup.addChild(new StatementPatternNode(new VarNode("o"),
                        new VarNode("p2"), new VarNode("s"),
                        new VarNode("src"), Scope.NAMED_CONTEXTS));
                
                whereClause.addChild(joinGroup);
                
            }
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for two groups, each consisting of one triple pattern.
     * 
     * <pre>
     * select ?s where { {?s ?p ?o} . {?o ?p2 ?s}}
     * </pre>
     */
    public void test_two_simple_join_groups() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where { { ?s ?p ?o } .  { ?o ?p2 ?s } }";

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
            final JoinGroupNode group1 = new JoinGroupNode();
            final JoinGroupNode group2 = new JoinGroupNode();
            whereClause.addChild(group1);
            whereClause.addChild(group2);

            group1.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            group2.addChild(new StatementPatternNode(new VarNode("o"),
                    new VarNode("p2"), new VarNode("s"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Test union of two groups.
     * 
     * <pre>
     * select ?s where { { ?s ?p ?o } UNION  { ?o ?p2 ?s } }
     * </pre>
     */
    public void test_union_two_groups() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
                "select ?s" +
                " where {" +
                "   {" +
                "     ?s ?p ?o" +
                "   } UNION {" +
                "     ?o ?p2 ?s" +
                "   } " +
                "}";

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

            final UnionNode union = new UnionNode();
            whereClause.addChild(union);

            union.addChild(new JoinGroupNode(new StatementPatternNode(
                    new VarNode("s"), new VarNode("p"), new VarNode("o"),
                    null/* c */, Scope.DEFAULT_CONTEXTS)));

            union.addChild(new JoinGroupNode(new StatementPatternNode(
                    new VarNode("o"), new VarNode("p2"), new VarNode("s"),
                    null/* c */, Scope.DEFAULT_CONTEXTS)));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Test union of three groups.
     * 
     * <pre>
     * select ?s where { { ?s ?p1 ?o } UNION  { ?s ?p2 ?o } UNION  { ?s ?p3 ?o } }
     * </pre>
     */
    public void test_union_three_groups() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
                "select ?s" +
                " where {" +
                "   {" +
                "     ?s ?p1 ?o" +
                "   } UNION {" +
                "     ?s ?p2 ?o" +
                "   } UNION {" +
                "     ?s ?p3 ?o" +
                "   } " +
                "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final VarNode s = new VarNode("s");
            final VarNode p1 = new VarNode("p1");
            final VarNode p2 = new VarNode("p2");
            final VarNode p3 = new VarNode("p3");
            final VarNode o = new VarNode("o");

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);
            }

            {

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final UnionNode union1 = new UnionNode();
                whereClause.addChild(union1);

                union1.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p1, o, null/* c */, Scope.DEFAULT_CONTEXTS)));

//                final UnionNode union2 = new UnionNode();
//                union1.addChild(new JoinGroupNode(union2));
                
                union1.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p2, o, null/* c */, Scope.DEFAULT_CONTEXTS)));
                
                union1.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p3, o, null/* c */, Scope.DEFAULT_CONTEXTS)));
                
            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Test union of four groups.
     * 
     * <pre>
     * select ?s where { { ?s ?p1 ?o } UNION  { ?s ?p2 ?o } UNION  { ?s ?p3 ?o } UNION  { ?s ?p4 ?o } }
     * </pre>
     */
    public void test_union_four_groups() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "" +
                "select ?s" +
                " where {" +
                "   {" +
                "     ?s ?p1 ?o" +
                "   } UNION {" +
                "     ?s ?p2 ?o" +
                "   } UNION {" +
                "     ?s ?p3 ?o" +
                "   } UNION {" +
                "     ?s ?p4 ?o" +
                "   } " +
                "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final VarNode s = new VarNode("s");
            final VarNode p1 = new VarNode("p1");
            final VarNode p2 = new VarNode("p2");
            final VarNode p3 = new VarNode("p3");
            final VarNode p4 = new VarNode("p4");
            final VarNode o = new VarNode("o");

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);
            }

            {

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final UnionNode union1 = new UnionNode();
                whereClause.addChild(union1);

                union1.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p1, o, null/* c */, Scope.DEFAULT_CONTEXTS)));

//                final UnionNode union2 = new UnionNode();
//                union1.addChild(new JoinGroupNode(union2));
                
                union1.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p2, o, null/* c */, Scope.DEFAULT_CONTEXTS)));
                
                union1.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p3, o, null/* c */, Scope.DEFAULT_CONTEXTS)));
                
                union1.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p4, o, null/* c */, Scope.DEFAULT_CONTEXTS)));
                
            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Test union of two groups with an embedded union in the third group. 
     * 
     * <pre>
     * select ?s where { { ?s ?p1 ?o } UNION  { { ?s ?p2 ?o } UNION  { ?s ?p3 ?o } }  }
     * </pre>
     */
    public void test_union_two_groups_with_embedded_union()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "" +
                "select ?s" +
                " where {" +
                "   {" +
                "     ?s ?p1 ?o" +   // group1
                "   } UNION {" +     // union1
                "       {" +         // group4
                "       ?s ?p2 ?o" + // group2
                "       } UNION {" + // union2
                "       ?s ?p3 ?o" + // group3
                "       }" +
                "   } " +
                "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final VarNode s = new VarNode("s");
            final VarNode p1 = new VarNode("p1");
            final VarNode p2 = new VarNode("p2");
            final VarNode p3 = new VarNode("p3");
            final VarNode o = new VarNode("o");

            {
                final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
                expected.setPrefixDecls(prefixDecls);
            }

            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);
            }

            {
                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                final UnionNode union1 = new UnionNode();
                final UnionNode union2 = new UnionNode();

                whereClause.addChild(union1);

                union1.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p1, o, null/* c */, Scope.DEFAULT_CONTEXTS)));
                
                union1.addChild(new JoinGroupNode(union2));

                union2.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p2, o, null/* c */, Scope.DEFAULT_CONTEXTS)));

                union2.addChild(new JoinGroupNode(new StatementPatternNode(s,
                        p3, o, null/* c */, Scope.DEFAULT_CONTEXTS)));

            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * <pre>
     * SELECT ?s
     * WHERE {
     *    ?s ?p ?o .
     *    MINUS {
     *       ?o ?p2 ?s
     *    }
     * }
     * </pre>
     */
    public void test_minus() throws MalformedQueryException, TokenMgrError,
            ParseException {

        final String sparql = "select ?s where {?s ?p ?o MINUS { ?o ?p2 ?s }}";

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
            {
                final JoinGroupNode joinGroup = new JoinGroupNode();
                joinGroup.setMinus(true);
                joinGroup.addChild(new StatementPatternNode(new VarNode("o"),
                        new VarNode("p2"), new VarNode("s"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(joinGroup);
            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * <pre>
     * select ?s where {?s ?p ?o OPTIONAL { ?o ?p2 ?s }}
     * </pre>
     */
    public void test_join_with_optional_triple_pattern()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o OPTIONAL { ?o ?p2 ?s }}";

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
            {
                final JoinGroupNode joinGroup = new JoinGroupNode();
                joinGroup.setOptional(true);
                joinGroup.addChild(new StatementPatternNode(new VarNode("o"),
                        new VarNode("p2"), new VarNode("s"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
                whereClause.addChild(joinGroup);
            }
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple triple pattern in the default context with a FILTER.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o FILTER ?s = ?o}
     * </pre>
     */
    public void test_simple_triple_pattern_with_filter()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o . FILTER (?s = ?o) }";

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

            final ValueExpressionNode ve = new FunctionNode(
                    FunctionRegistry.EQ, null/* scalarValues */,
                    new ValueExpressionNode[] { new VarNode("s"),
                            new VarNode("o") });

            whereClause.addChild(new FilterNode(ve));
           
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for empty group in the default context with a FILTER.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o FILTER ?s = ?o}
     * </pre>
     */
    public void test_empty_group_with_filter()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select * where { "+
                "FILTER (?a >= \"1\"^^xsd:unsignedLong) "+
                "FILTER (?b >= \"1\"^^xsd:unsignedLong) "+
                "}";

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
            expected.setWhereClause(whereClause);

            ILexiconConfiguration<BigdataValue> lexiconConfiguration = tripleStore.getLexiconRelation().getLexiconConfiguration();
            
            final ValueExpressionNode ve1 = new FunctionNode(
                    FunctionRegistry.GE, null/* scalarValues */,
                    new ValueExpressionNode[] { new VarNode("a"),
                            new ConstantNode(lexiconConfiguration.createInlineIV(new LiteralImpl("1", XSD.UNSIGNED_LONG))) });

            whereClause.addChild(new FilterNode(ve1));

            
            final ValueExpressionNode ve2 = new FunctionNode(
                    FunctionRegistry.GE, null/* scalarValues */,
                    new ValueExpressionNode[] { new VarNode("b"),
                            new ConstantNode(lexiconConfiguration.createInlineIV(new LiteralImpl("1", XSD.UNSIGNED_LONG))) });

            whereClause.addChild(new FilterNode(ve2));

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple triple pattern in the default context with a BIND
     * and a FILTER.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o BIND(?o AS ?x) FILTER (?s = ?x) }
     * </pre>
     */
    public void test_simple_triple_pattern_with_bind_and_filter()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o . BIND(?o AS ?x) FILTER (?s = ?o) }";

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

            whereClause.addChild(new AssignmentNode(new VarNode("x"),
                    new VarNode("o")));
            
            final ValueExpressionNode ve = new FunctionNode(
                    FunctionRegistry.EQ, null/* scalarValues */,
                    new ValueExpressionNode[] { new VarNode("s"),
                            new VarNode("o") });

            whereClause.addChild(new FilterNode(ve));
           
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Unit test for simple triple pattern in the default context with a LET
     * and a FILTER (LET is an alternative syntax for BIND).
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o LET(?x := ?o) FILTER (?s = ?x) }
     * </pre>
     */
    public void test_simple_triple_pattern_with_let_and_filter()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o . LET(?x := ?o) FILTER (?s = ?o) }";

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

            whereClause.addChild(new AssignmentNode(new VarNode("x"),
                    new VarNode("o")));
            
            final ValueExpressionNode ve = new FunctionNode(
                    FunctionRegistry.EQ, null/* scalarValues */,
                    new ValueExpressionNode[] { new VarNode("s"),
                            new VarNode("o") });

            whereClause.addChild(new FilterNode(ve));
           
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * IN with empty arg list in a FILTER. This should be turned into a FALSE
     * constraint.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o . FILTER (?s IN()) }
     * </pre>
     */
    public void test_simple_triple_pattern_with_IN_filter() throws MalformedQueryException,
            TokenMgrError, ParseException {


        final String sparql = "SELECT ?s where {?s ?p ?o. FILTER (?s IN())}";

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

            whereClause.addChild(new FilterNode(new FunctionNode( //
                    FunctionRegistry.IN,//
                    null, // scalarValues
                    new ValueExpressionNode[] {// args
                    new VarNode("s") })));//

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * IN with an arg list in a FILTER. The arg list has a single value. This
     * should be turned into a SameTerm constraint.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o. FILTER (?s IN(?o))}
     * </pre>
     */
    public void test_simple_triple_pattern_with_IN_filter_singletonSet() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o. FILTER (?s IN(?o))}";

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

            whereClause.addChild(new FilterNode(new FunctionNode( //
                    FunctionRegistry.IN,//
                    null, // scalarValues
                    new ValueExpressionNode[] {// args
                    new VarNode("s"), // variable 
                    new VarNode("o")  // other arg
                    })));//

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * IN with a non-empty arg list in a FILTER. The arguments are expressions
     * rather than constants. This should be turned into a {@link ComputedIN}.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o. FILTER (?s IN(?p,?o))}
     * </pre>
     */
    public void test_simple_triple_pattern_with_IN_filter_variables()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o. FILTER (?s IN(?p,?o))}";

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

            whereClause.addChild(new FilterNode(new FunctionNode( //
                    FunctionRegistry.IN,//
                    null, // scalarValues
                    new ValueExpressionNode[] {// args
                    new VarNode("s"), // // variable
                    new VarNode("p"), new VarNode("o") // other args. 
                    })));//

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * IN with a non-empty arg list in a FILTER. The arguments are expressions
     * rather than constants. This should be turned into an optimized IN using a
     * hash set or binary search.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o. FILTER (?s IN(?p,?o))}
     * </pre>
     */
    public void test_simple_triple_pattern_with_IN_filter_constants()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "SELECT ?s where {?s ?p ?o. FILTER (?s IN(1,2))}";

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

            whereClause.addChild(new FilterNode(new FunctionNode( //
                    FunctionRegistry.IN,//
                    null, // scalarValues
                    new ValueExpressionNode[] {// args
                    new VarNode("s"), // var
                    // other args to IN()
                    new ConstantNode(makeIV(valueFactory.createLiteral("1",XSD.INTEGER))),
                    new ConstantNode(makeIV(valueFactory.createLiteral("2",XSD.INTEGER)))
                    })));//

        }
        
        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }
    
    /**
     * Simple SERVICE graph pattern.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o. SERVICE <http://bigdata.com/myService> {?s ?p ?o}}
     * </pre>
     */
    public void test_service_001()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String serviceExpr = "SERVICE <http://bigdata.com/myService> {?s ?p ?o}";

        final String sparql = "SELECT ?s where {?s ?p ?o. "+serviceExpr+"}";

        final IV<?, ?> serviceRefIV = makeIV(valueFactory
                .createURI("http://bigdata.com/myService"));

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            expected.setPrefixDecls(PrefixDeclProcessor.defaultDecls);

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final JoinGroupNode groupNode = new JoinGroupNode();
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                    serviceRefIV), groupNode);

            serviceNode.setExprImage(serviceExpr);
            serviceNode.setPrefixDecls(PrefixDeclProcessor.defaultDecls);

            whereClause.addArg(serviceNode);
            
        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Simple SERVICE graph pattern with SILENT keyword.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o. SERVICE SILENT <http://bigdata.com/myService> {?s ?p ?o}}
     * </pre>
     */
    public void test_service_002()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String serviceExpr = "SERVICE SILENT <http://bigdata.com/myService> {?s ?p ?o}";
        
        final String sparql = "SELECT ?s where {?s ?p ?o. " + serviceExpr + "}";

        final IV<?, ?> serviceRefIV = makeIV(valueFactory
                .createURI("http://bigdata.com/myService"));
        
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

            final JoinGroupNode groupNode = new JoinGroupNode();
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            
            final ServiceNode serviceNode = new ServiceNode(new ConstantNode(
                    serviceRefIV), groupNode);
            
            serviceNode.setSilent(true);

            serviceNode.setExprImage(serviceExpr);
            serviceNode.setPrefixDecls(PrefixDeclProcessor.defaultDecls);

            whereClause.addArg(serviceNode);

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Simple SERVICE graph pattern with variable for the URI.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o. SERVICE ?o {?s ?p ?o}}
     * </pre>
     */
    public void test_service_003()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String serviceExpr = "SERVICE ?o {?s ?p ?o}";

        final String sparql = "SELECT ?s where {?s ?p ?o. " + serviceExpr + "}";

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

            final JoinGroupNode groupNode = new JoinGroupNode();
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            
            final ServiceNode serviceNode = new ServiceNode(new VarNode("o"),
                    groupNode);

            serviceNode.setExprImage(serviceExpr);
            serviceNode.setPrefixDecls(PrefixDeclProcessor.defaultDecls);

            whereClause.addArg(serviceNode);

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * Simple SERVICE graph pattern with variable for the URI and prefix
     * declarations. This verifies that the prefix declarations are harvested
     * and attached to the {@link ServiceNode}.
     * 
     * <pre>
     * PREFIX : <http://www.bigdata.com/>
     * SELECT ?s where {?s ?p ?o. SERVICE ?o {?s ?p ?o}}
     * </pre>
     */
    public void test_service_004()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String serviceExpr = "SERVICE ?o {?s ?p ?o}";

        final String sparql = "PREFIX : <http://www.bigdata.com/>\n"
                + "SELECT ?s where {?s ?p ?o. " + serviceExpr + "}";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final Map<String, String> prefixDecls = new LinkedHashMap<String, String>(PrefixDeclProcessor.defaultDecls);
            {
                prefixDecls.put("", "http://www.bigdata.com/");
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

            final JoinGroupNode groupNode = new JoinGroupNode();
            groupNode.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            
            final ServiceNode serviceNode = new ServiceNode(new VarNode("o"),
                    groupNode);

            serviceNode.setExprImage(serviceExpr);

            serviceNode.setPrefixDecls(prefixDecls);

            whereClause.addArg(serviceNode);

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

    /**
     * A unit test for an OPTIONAL wrapping a SERVICE.
     */
    public void test_optional_SERVICE() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String serviceExpr = "service ?s { ?s ?p ?o  }";

        final String sparql = "select ?s where { optional { " + serviceExpr
                + " } }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        final ServiceNode service;
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

                final JoinGroupNode serviceGraph = new JoinGroupNode();
                serviceGraph.addChild(new StatementPatternNode(
                        new VarNode("s"), new VarNode("p"), new VarNode("o"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

                service = new ServiceNode(new VarNode("s"), serviceGraph);
                service.setExprImage(serviceExpr);
                service.setPrefixDecls(PrefixDeclProcessor.defaultDecls);

                final JoinGroupNode wrapperGroup = new JoinGroupNode(true/* optional */);
                whereClause.addChild(wrapperGroup);
                wrapperGroup.addChild(service);
            }

        }

        final QueryRoot actual = parse(sparql, baseURI);

        assertSameAST(sparql, expected, actual);

    }

}
