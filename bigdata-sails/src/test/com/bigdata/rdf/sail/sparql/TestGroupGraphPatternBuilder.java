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

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.constraints.ComputedIN;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for translating the openrdf SPARQL AST nodes for
 * <code>GroupGraphPattern</code> into the bigdata AST (join groups, subquery,
 * union, etc).
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
     * Unit test for simple triple pattern in the default context.
     * 
     * <pre>
     * SELECT ?s where {?s ?p ?o}
     * </pre>
     */
    public void test_simple_triple_pattern() throws MalformedQueryException,
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
     * Test union of two join groups.
     * 
     * <pre>
     * select ?s where { { ?s ?p ?o } UNION  { ?o ?p2 ?s } }
     * </pre>
     */
    public void test_union_two_join_groups() throws MalformedQueryException,
            TokenMgrError, ParseException {

        final String sparql = "select ?s where { { ?s ?p ?o } UNION  { ?o ?p2 ?s } }";

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            final UnionNode union = new UnionNode();
            whereClause.addChild(union);
            final JoinGroupNode group1 = new JoinGroupNode();
            final JoinGroupNode group2 = new JoinGroupNode();
            union.addChild(group1);
            union.addChild(group2);

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
     * <pre>
     * select ?s where {?s ?p ?o OPTIONAL { ?o ?p2 ?s }}
     * </pre>
     */
    public void test_join_with_optional_triple_pattern()
            throws MalformedQueryException, TokenMgrError, ParseException {

        final String sparql = "select ?s where {?s ?p ?o OPTIONAL { ?o ?p2 ?s }}";

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
     * Unit test for simple triple pattern in the default context.
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

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final ValueExpressionNode ve = new FunctionNode(lex,
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

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new FilterNode(new FunctionNode(lex, //
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

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new FilterNode(new FunctionNode(lex, //
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

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new FilterNode(new FunctionNode(lex, //
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

            final ProjectionNode projection = new ProjectionNode();
            projection.addProjectionVar(new VarNode("s"));
            expected.setProjection(projection);

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new FilterNode(new FunctionNode(lex, //
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
     * Unit test for a triples block using a predicate list. 
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
     * Unit test for an object list
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
     * Unit test for a complex triples block using both a predicate list and an
     * object list.
     * 
     * <pre>
     * select ?s where {?s ?p ?o ; ?p2 ?o2 , ?o3}
     * </pre>
     */
    public void test_complex_triples_block() throws MalformedQueryException,
            TokenMgrError, ParseException {

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
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("s"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

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
   
}
