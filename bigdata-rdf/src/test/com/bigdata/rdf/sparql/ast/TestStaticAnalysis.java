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
package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.ASTSearchOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTBottomUpOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSetValueExpressionsOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTWildcardProjectionOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * Test suite for methods supporting static analysis of the variables, including
 * whether a variable MUST be bound, MIGHT be bound, or is NOT bound. The static
 * analysis centers around {@link GraphPatternGroup}s. Methods are provided for
 * both the group itself, the group and its parent groups, and the group and its
 * child groups.
 * <p>
 * Note: The static analysis depends on the AST as generated from the parse tree
 * or as rewritten by one or more {@link IASTOptimizer}s. Most tests do not run
 * the {@link IASTOptimizer}s because we want to analyze the AST as generated
 * from the parse tree rather than the AST as optimized by the suite of
 * optimizers. In many cases, actually running the suite of optimizers will
 * cause some structures to be pruned from the AST. This is especially true of
 * the unit tests for the static analysis of the TCK queries, many of which use
 * variables in filters when the variables are not bound in that scope or use a
 * "badly designed left join" pattern.
 * 
 * TODO If we modify the methods which report on the must/may bindings to look
 * for null IVs, then we will need to add the URIs appearing in the query to the
 * database before we run the parser.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestStaticAnalysis extends AbstractASTEvaluationTestCase {

    private final static Logger log = Logger
            .getLogger(TestStaticAnalysis.class);
    
    /**
     * 
     */
    public TestStaticAnalysis() {
    }

    /**
     * @param arg0
     */
    public TestStaticAnalysis(String arg0) {
        super(arg0);
    }

    private static final Set<IVariable<?>> EMPTY_SET = Collections.emptySet();
    
    /**
     * Unit test of static analysis for variables which must be bound by a
     * query. This involves checking the WHERE clause and the projection for the
     * query.
     * 
     * @throws MalformedQueryException
     */
    public void test_static_analysis01()
            throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x where { ?x rdf:type foaf:Person }";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI).getOriginalAST();
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();
        
        expected.add(Var.var("x"));
        
        assertEquals(expected, sa.getDefinatelyProducedBindings(queryRoot));

        assertEquals(
                expected,
                sa.getDefinitelyProducedBindings(queryRoot.getWhereClause(),
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));
        
    }

    /**
     * Unit test of static analysis for variables which must be bound by a
     * query. This involves checking the WHERE clause and the projection for the
     * query. In this case, a constant is projected out of the query in addition
     * to the bindings produced by the WHERE clause.
     * 
     * @throws MalformedQueryException
     */
    public void test_static_analysis02()
            throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x (12 as ?y) where { ?x rdf:type foaf:Person }";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI).getOriginalAST();
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();
        
        expectedProjected.add(Var.var("x"));
        expectedProjected.add(Var.var("y"));
        
        assertEquals(expectedProjected, sa.getDefinatelyProducedBindings(queryRoot));

        final Set<IVariable<?>> expectedWhereClause = new LinkedHashSet<IVariable<?>>();
        
        expectedWhereClause.add(Var.var("x"));

        assertEquals(
                expectedWhereClause,
                sa.getDefinitelyProducedBindings(queryRoot.getWhereClause(),
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));
        
    }

    /**
     * Unit test of static analysis for variables with a named subquery. This
     * test verifies that we observe the variables projected by the subquery,
     * but not those which it uses internally.
     * 
     * @throws MalformedQueryException
     */
    public void test_static_analysis03()
            throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x (12 as ?y)\n" +
                "  with \n{" +
                "    select ?x {\n" +
                "      ?x rdf:type foaf:Person .\n" +
                "      ?x rdfs:label ?y .\n" +
                "    }\n" +
                "  } as %namedSet1 \n"+
                " where {\n" +
                "  include %namedSet1\n" +
                "}";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI).getOriginalAST();
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();
        
        expectedProjected.add(Var.var("x"));
        expectedProjected.add(Var.var("y"));
        
        assertEquals(expectedProjected, sa.getDefinatelyProducedBindings(queryRoot));

        final Set<IVariable<?>> expectedWhereClause = new LinkedHashSet<IVariable<?>>();
        
        expectedWhereClause.add(Var.var("x"));

        assertEquals(
                expectedWhereClause,
                sa.getDefinitelyProducedBindings(queryRoot.getWhereClause(),
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));
        
    }

    /**
     * Unit test of static analysis for variables with a SPARQL 1.1 subquery.
     * This test verifies that we observe the variables projected by the
     * subquery, but not those which it uses internally.
     * 
     * @throws MalformedQueryException
     */
    public void test_static_analysis04()
            throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x (12 as ?y)\n" +
                " where {\n" +
                "  ?q foaf:knows ?p ." +
                "  {\n" +
                "    select ?x {\n" +
                "      ?x rdf:type foaf:Person .\n" +
                "      ?x rdfs:label ?z .\n" +
                "    }\n" +
                "  }\n" +
                "}";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI).getOriginalAST();
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();
        
        expectedProjected.add(Var.var("x"));
        expectedProjected.add(Var.var("y"));
        
        assertEquals(expectedProjected, sa.getDefinatelyProducedBindings(queryRoot));

        final Set<IVariable<?>> expectedWhereClause = new LinkedHashSet<IVariable<?>>();
        
        expectedWhereClause.add(Var.var("x"));
        expectedWhereClause.add(Var.var("p"));
        expectedWhereClause.add(Var.var("q"));

        assertEquals(
                expectedWhereClause,
                sa.getDefinitelyProducedBindings(queryRoot.getWhereClause(),
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));
        
    }

    /**
     * Unit test for computing the join variables for a named subquery based on
     * the analysis of the bindings which MUST be produced by the subquery and
     * those which MUST be bound on entry into the group in which the subquery
     * solution set is included within the main query.
     * 
     * The join should be on <code>?x</code> in this example.
     * 
     * @throws MalformedQueryException 
     */
    public void test_static_analysis_join_vars() throws MalformedQueryException {
        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x ?o \n"+
                " with {"+
                "   select ?x where { ?x rdf:type foaf:Person }\n"+
                " } AS %namedSet1 \n"+
                "where { \n" +
                "   ?x rdfs:label ?o \n" +
                "   INCLUDE %namedSet1 \n"+
                "}";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI).getOriginalAST();

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        // variables which must be bound in the top-level query's projection.
        {
            
            final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();

            expectedProjected.add(Var.var("x"));
            expectedProjected.add(Var.var("o"));

            assertEquals(expectedProjected,
                    sa.getDefinatelyProducedBindings(queryRoot));
            
        }

        // variables which must be bound in the named subquery's projection.
        {

            final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();

            expectedProjected.add(Var.var("x"));

            final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) queryRoot
                    .getNamedSubqueries().get(0);

            assertEquals(expectedProjected,
                    sa.getDefinatelyProducedBindings(namedSubquery));

        }

        // variables which must be bound in the main query's where clause.
        {
            
            final Set<IVariable<?>> expectedWhereClause = new LinkedHashSet<IVariable<?>>();

            expectedWhereClause.add(Var.var("x"));
            expectedWhereClause.add(Var.var("o"));

            assertEquals(expectedWhereClause, sa.getDefinitelyProducedBindings(
                    queryRoot.getWhereClause(),
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

    }
    
    /**
     * Static analysis of TCK query:
     * 
     * <pre>
     * PREFIX : <http://example/>
     * 
     * SELECT ?v { :x :p ?v . FILTER(?v = 1)
     * </pre>
     * 
     * This query does not present a problem since <code>?v</code> is in scope
     * when the filter is evaluated.
     */
    public void test_static_analysis_filter_nested_1() throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT ?v \n" +//
                "{ :x :p ?v . FILTER(?v = 1) }";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI).getOriginalAST();

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        @SuppressWarnings("unchecked")
        final GraphPatternGroup<IGroupMemberNode> whereClause = queryRoot
                .getWhereClause();

        final Set<IVariable<?>> expected = asSet(new String[]{"v"});

        // Test "must" bound bindings for the query.
        assertEquals(expected, sa.getDefinatelyProducedBindings(queryRoot));

        // Test "must" bound bindings for the where clause.
        assertEquals(expected, sa.getDefinitelyProducedBindings(whereClause,
                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        // Test "maybe" bound bindings for the where clause.
        assertEquals(expected,
                sa.getMaybeProducedBindings(whereClause,
                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        // Test "incoming" bindings for the where clause.
        assertEquals(EMPTY_SET, sa.getIncomingBindings(whereClause,
                new LinkedHashSet<IVariable<?>>()));

    }

    /**
     * Static analysis of TCK query:
     * 
     * <pre>
     * PREFIX : <http://example/> 
     * 
     * SELECT ?v
     * { :x :p ?v . { FILTER(?v = 1) } }
     * </pre>
     * 
     * <code>?v</code> is not bound in the FILTER because it is evaluated with
     * bottom up semantics and therefore the bindings from the parent group are
     * not visible.
     */
    public void test_static_analysis_filter_nested_2()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT ?v \n" +//
                "{ :x :p ?v . { FILTER(?v = 1) } }";

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store).parseQuery2(
                queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        // Set the IValueExpressions on the AST.
        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
                context, queryRoot, null /* bindingSets */);

        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

         // Test "must" bound bindings for the query.
        assertEquals(asSet(new String[] { "v" }),
                sa.getDefinatelyProducedBindings(queryRoot));

        // WHERE clause
        {

            @SuppressWarnings("unchecked")
            final GraphPatternGroup<IGroupMemberNode> whereClause = queryRoot
                    .getWhereClause();

            final Set<IVariable<?>> expected = asSet(new String[] { "v" });

            // Test "incoming" bindings for the where clause.
            assertEquals(EMPTY_SET, sa.getIncomingBindings(whereClause,
                    new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings for the where clause.
            assertEquals(expected, sa.getDefinitelyProducedBindings(whereClause,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings for the where clause.
            assertEquals(expected, sa.getMaybeProducedBindings(whereClause,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

        // FILTER's group clause.
        {

            final JoinGroupNode filterClause = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1);

            // Test "incoming" bindings.
            assertEquals(asSet(new String[] { "v" }), sa.getIncomingBindings(
                    filterClause, new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    EMPTY_SET,
                    sa.getDefinitelyProducedBindings(filterClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(EMPTY_SET, sa.getMaybeProducedBindings(filterClause,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // The FILTER node itself.
            final FilterNode filter = BOpUtility.visitAll(queryRoot,
                    FilterNode.class).next();

            assertEquals(Collections.singletonList(filter), sa.getPreFilters(filterClause));
            assertEquals(Collections.emptyList(), sa.getJoinFilters(filterClause));
            assertEquals(Collections.emptyList(), sa.getPostFilters(filterClause));
            assertEquals(Collections.emptyList(), sa.getPruneFilters(filterClause));
            
        }
        
    }

    /**
     * Static analysis of TCK query:
     * 
     * <pre>
     * PREFIX :    <http://example/>
     * 
     * SELECT *
     * { 
     *     :x :p ?v . 
     *     { :x :q ?w 
     *       OPTIONAL {  :x :p ?v2 FILTER(?v = 1) }
     *     }
     * }
     * </pre>
     * 
     * <code>?v</code> is bound in the outer join group.
     * <p>
     * <code>?w</code> is bound in the child join group regardless of whether
     * the embedded optional succeeds.
     * <p>
     * <code>?v</code> is not bound in the FILTER because it is evaluated with
     * bottom up semantics and therefore the bindings from the outer parent
     * group are not visible.
     * <p>
     * For reference, the AST for that SPARQL query is:
     * 
     * <pre>
     * PREFIX : <http://example/>
     * QueryType: SELECT
     * SELECT * 
     *   JoinGroupNode {
     *     StatementPatternNode(ConstantNode(TermId(0U)[http://example/x]), ConstantNode(TermId(0U)[http://example/p]), VarNode(v), DEFAULT_CONTEXTS)
     *     JoinGroupNode {
     *       StatementPatternNode(ConstantNode(TermId(0U)[http://example/x]), ConstantNode(TermId(0U)[http://example/q]), VarNode(w), DEFAULT_CONTEXTS)
     *       JoinGroupNode [optional] {
     *         StatementPatternNode(ConstantNode(TermId(0U)[http://example/x]), ConstantNode(TermId(0U)[http://example/p]), VarNode(v2), DEFAULT_CONTEXTS)
     *         FILTER( com.bigdata.rdf.sparql.ast.FunctionNode(VarNode(v),ConstantNode(XSDInteger(1)))[ com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.w3.org/2005/xpath-functions#equal-to] )
     *       }
     *     }
     *   }
     * </pre>
     * 
     * This can be reduced to a filter which does not bind anything. Since the
     * FILTER can not succeed, it should logically be replaced by failing the
     * group(s) within which it appears. This needs to be done recursively up to
     * the parent, stopping at the first parent group which is optional. If all
     * parents up to the WHERE clause are eliminated, then the WHERE clause
     * itself can not succeed and the query should be replaced by a
     * "DropSolutionsBOp". The DropSolutionsOp can be substituted in directly
     * for a group which can not succeed and then we can work the pruning of the
     * parents as an efficiency.
     * <p>
     * Note: An AST optimizer needs to recognize and transform this query by
     * lifting out
     * 
     * <pre>
     *  :x :q ?w OPTIONAL {  :x :p ?v2 FILTER(?v = 1) }
     * </pre>
     * 
     * into a named subquery. Since the named subquery runs without any incoming
     * bindings, the result will be as if we had used bottom up evaluation.
     * 
     * @see ASTBottomUpOptimizer
     */
    public void test_static_analysis_filter_scope_1()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT * \n" + //
                "{ \n" + //
                "    :x :p ?v . \n" + //
                "    { :x :q ?w \n" + //
                "      OPTIONAL {  :x :p ?v2 FILTER(?v = 1) } \n" + //
                "    } \n" + //
                "}"//
        ;

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        // Set the IValueExpressions on the AST.
        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
                context, queryRoot, null /* bindingSets */);

        // Rewrite the wild card in the projection.
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null /* bindingSets */);
        
        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        // Test "must" bound bindings for the query.
        assertEquals(asSet(new String[] { "v", "w" }),
                sa.getDefinatelyProducedBindings(queryRoot));

        // Outer group clause { :x :p ?v . }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause();

            // Test "incoming" bindings.
            assertEquals(EMPTY_SET, sa.getIncomingBindings(group,
                    new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "v" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "v", "w" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "v" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "v", "w", "v2" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // Nested group clause {  :x :q ?w  }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1);

            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "v" }),
                    sa.getIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "w" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "w" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "w" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "w", "v2" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // Optional group clause { :x :p ?v2 FILTER(?v = 1)  }
        {

            final JoinGroupNode group = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1).get(1);

            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "v", "w" }),
                    sa.getIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "v2" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "v2" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "v2" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "v2" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));
            
            // The FILTER node itself.
            final FilterNode filter = BOpUtility.visitAll(queryRoot,
                    FilterNode.class).next();

            assertEquals(Collections.singletonList(filter), sa.getPreFilters(group));
            assertEquals(Collections.emptyList(), sa.getJoinFilters(group));
            assertEquals(Collections.emptyList(), sa.getPostFilters(group));
            assertEquals(Collections.emptyList(), sa.getPruneFilters(group));
            
        }

    }

    /**
     * Join-scope - 1 (aka var-scope-join-1).
     * 
     * <pre>
     * PREFIX : <http://example/>
     * 
     * SELECT *
     * { 
     *   ?X  :name "paul"
     *   {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
     * }
     * </pre>
     * <p>
     * Note: For this query, the bindings of <code>?X</code> in the outer group
     * are not visible when the inner groups are evaluated. Because of this,
     * <code>?X</code> in the optional clause binds for values which differ from
     * the values bound on <code>?X</code> in the outer group. This means that
     * the solutions from the middle group with bindings for <code>?Y</code> and
     * <code>?X</code> fail to join with the solutions in the outer group. Note
     * that there is no email address for "paul" for the data set used to run
     * this query. If there were, then the query would have a result.
     * <p>
     * These group expressions need to be evaluated independently because they
     * are not sharing a binding for <code>?X</code> until we join them together
     * on <code>?X</code>.
     * <p>
     * In order for us to run this query correctly we need to run
     * 
     * <pre>
     * {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
     * </pre>
     * 
     * BEFORE
     * 
     * <pre>
     * ?X  :name "paul"
     * </pre>
     * 
     * This can be most easily achieved by lifting the former out into a named
     * subquery.
     * 
     * @see ASTBottomUpOptimizer
     */
    public void test_static_analysis_join_scope_1()
            throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT * \n" + //
                "{ \n" + //
                "    ?X  :name \"paul\" . \n" + //
                "    {?Y :name \"george\" . OPTIONAL { ?X :email ?Z } } \n" + //
                "}"//
        ;

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        // Rewrite the wild card in the projection.
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null /* bindingSets */);

        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        // Test "must" bound bindings for the query.
        assertEquals(asSet(new String[] { "X", "Y" }),
                sa.getDefinatelyProducedBindings(queryRoot));

        // Outer group clause { ?X :name "paul" }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause();

            // Test "incoming" bindings.
            assertEquals(
                    EMPTY_SET,
                    sa.getIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "X" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "X", "Y" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "X" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "X", "Y", "Z" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // Nested group clause {  ?Y :name "george" }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1);

            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "X" }),
                    sa.getIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "Y" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "Y" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "Y" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "Y", "X", "Z" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // Optional group clause OPTIONAL { ?X :email ?Z }
        {

            final JoinGroupNode group = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1).get(1);

            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "X", "Y" }),
                    sa.getIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "X", "Z" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "X", "Z" }),
                    sa.getDefinitelyProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "X", "Z" }),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "X", "Z"}),
                    sa.getMaybeProducedBindings(group,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));
                        
        }

    }

    /**
     * Unit test for analysis of a {@link ServiceNode}. The analysis of a
     * {@link ServiceNode} is just the analysis of the graph pattern reported by
     * {@link ServiceNode#getGroupNode()}.
     * 
     * <pre>
     * PREFIX bd: <http://www.bigdata.com/rdf/search#>
     * SELECT ?subj ?score 
     * WHERE {
     *   ?lit bd:search "mike" .
     *   ?lit bd:relevance ?score .
     *   ?subj ?p ?lit .
     * }
     * </pre>
     * 
     * Note: The "be:search", "bd:relevance" and <code>?subj ?p ?lit</code>
     * statement patterns below will be transformed into a {@link ServiceNode}.
     * Since there is nothing else in the main query's WHERE clause, the
     * {@link ServiceNode} is not lifted out into a named subquery.
     * 
     * TODO This could be redone using the SPARQL SERVICE syntax once we
     * integrate with Sesame 2.5.1, which has support for that syntax in the
     * parser.
     */
    public void test_static_analysis_serviceCall() throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX bd: <http://www.bigdata.com/rdf/search#>\n" + //
                "SELECT ?subj ?score\n" + //
                "WHERE {\n" + //
                "   ?lit bd:search \"mike\" .\n" + //
                "   ?lit bd:relevance ?score .\n" + //
                "   ?subj ?p ?lit .\n" + //
                "}";

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        // rewrite the search predicates as an AST ServiceNode.
        queryRoot = (QueryRoot) new ASTSearchOptimizer().optimize(
                context, queryRoot, null /* bindingSets */);

        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        {
            // variables which must be bound in the top-level query's
            // projection.
            {

                final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();

                expectedProjected.add(Var.var("subj"));
                expectedProjected.add(Var.var("score"));

                assertEquals(expectedProjected,
                        sa.getDefinatelyProducedBindings(queryRoot));

            }

            // variables which must be bound in the main query's where clause.
            {

                final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();

                expected.add(Var.var("subj"));
                expected.add(Var.var("score"));
                expected.add(Var.var("p"));
                expected.add(Var.var("lit"));

                assertEquals(expected, sa.getDefinitelyProducedBindings(
                        queryRoot.getWhereClause(),
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            }

            // variables which may be bound in the main query's where clause.
            {

                final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();

                expected.add(Var.var("subj"));
                expected.add(Var.var("score"));
                expected.add(Var.var("p"));
                expected.add(Var.var("lit"));

                assertEquals(expected, sa.getMaybeProducedBindings(
                        queryRoot.getWhereClause(),
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            }
        }
        
    }

    /**
     * Unit test(s) for the correct identification of pre-, join-, post-, and
     * prune- filters.
     */
    public void test_static_analysis_filters() throws MalformedQueryException {
        
        final String queryStr = ""+//
        "PREFIX : <http://www.bigdata.com/>\n" +//
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+//
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+//
        "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+//
        "SELECT ?a ?b \n" +//
        " WHERE {\n" +//
        "   ?a rdf:type foaf:Person . \n" +//
        "   ?a foaf:knows ?b .\n"+//
        "   { \n"+//
        "     ?a :age ?ageA .\n"+//
        "     ?b :age ?ageB .\n"+//
        "     FILTER ( ?a != ?b ) .\n"+// pre-filter (can be lifted)
        "     FILTER ( ?ageA > ?ageB) .\n"+// join-filter
        "     FILTER ( ?x < 100 ) .\n"+// prune-filter (can be pruned)
        "   }\n"+//
        "   FILTER ( ?ageA > 20 ) .\n"+//post-filter (depends on subgroup)
        "   "+//
        "}";
        
        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        // Set the IValueExpressions on the AST.
        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
                context, queryRoot, null /* bindingSets */);

        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        /*
         * Locate the outer join group and the inner join group.
         */
        final JoinGroupNode outerGroup;
        final JoinGroupNode innerGroup;
        
        {
         
            final Iterator<JoinGroupNode> itr = BOpUtility.visitAll(
                    queryRoot.getWhereClause(), JoinGroupNode.class);

            outerGroup = itr.next();
            innerGroup = itr.next();
            
        }
        
        /*
         * Locate the different filters in the query.
         */
        
        final FilterNode ageA_GT_ageB;
        final FilterNode a_NE_b;
        final FilterNode x_LT_10;
        final FilterNode ageA_GT_20;
        
        // Filters in the outer group.
        {
            FilterNode tmp = null;
            for(IGroupMemberNode child : outerGroup) {
                if(child instanceof FilterNode) {
                    tmp = (FilterNode)child;
                    break;
                }
            }
            ageA_GT_20 = tmp;
        }
        assertNotNull(ageA_GT_20);
        
        // Filters in the inner group.
//        "     FILTER ( ?ageA > ?ageB) .\n"+// join-filter
//        "     FILTER ( ?a != ?b ) .\n"+// pre-filter
//        "     FILTER ( ?x < 100 ) .\n"+// prune-filter
        {
            FilterNode GT = null;
            FilterNode NE = null;
            FilterNode LT = null;
            for (IGroupMemberNode child : innerGroup) {
                if (child instanceof FilterNode) {
                    final FilterNode tmp = (FilterNode) child;
                    final FunctionNode expr = (FunctionNode) tmp
                            .getValueExpressionNode();
                    if (expr.getFunctionURI().equals(FunctionRegistry.GT)) {
                        GT = tmp;
                    } else if (expr.getFunctionURI()
                            .equals(FunctionRegistry.NE)) {
                        NE = tmp;
                    } else if (expr.getFunctionURI()
                            .equals(FunctionRegistry.LT)) {
                        LT = tmp;
                    } else
                        throw new AssertionError();
                }
            }
            ageA_GT_ageB = GT;
            a_NE_b = NE;
            x_LT_10 = LT;
        }
        assertNotNull(ageA_GT_ageB);
        assertNotNull(a_NE_b);
        assertNotNull(x_LT_10);

        /*
         * Now verify the static analysis of the filters.
         */
//        "     FILTER ( ?ageA > ?ageB) .\n"+// join-filter
//        "     FILTER ( ?a != ?b ) .\n"+// pre-filter
//        "     FILTER ( ?x < 100 ) .\n"+// prune-filter
//        "   }\n"+//
//        "   FILTER ( ?ageA > 20 ) .\n"+//post-filter
        
        // Inner group.
        
        assertEquals("pre-filters", Collections.singletonList(a_NE_b),
                sa.getPreFilters(innerGroup));
        
        assertEquals("join-filters", Collections.singletonList(ageA_GT_ageB),
                sa.getJoinFilters(innerGroup));
        
        assertEquals("post-filters", Collections.singletonList(x_LT_10),
                sa.getPostFilters(innerGroup));
        
        assertEquals("prune-filters", Collections.singletonList(x_LT_10),
                sa.getPruneFilters(innerGroup));

        assertEquals("definitely-and-filters", asSet(new String[] { "a", "b",
                "ageA", "ageB", "x" }),
                sa.getDefinitelyProducedBindingsAndFilterVariables(innerGroup,
                        new LinkedHashSet<IVariable<?>>()));
        
        // Outer group.

        assertEquals("pre-filters", Collections.emptyList(),
                sa.getPreFilters(outerGroup));
        
        assertEquals("join-filters", Collections.emptyList(),
                sa.getJoinFilters(outerGroup));
        
        assertEquals("post-filters", Collections.singletonList(ageA_GT_20),
                sa.getPostFilters(outerGroup));

        assertEquals("prune-filters", Collections.emptyList(),
                sa.getPruneFilters(outerGroup));

        assertEquals("definitely-and-filters", asSet(new String[] { "a", "b",
                "ageA" }),
                sa.getDefinitelyProducedBindingsAndFilterVariables(outerGroup,
                        new LinkedHashSet<IVariable<?>>()));

    }

    /**
     * FIXME Write unit tests for
     * {@link StaticAnalysis#getJoinVars(SubqueryRoot, Set)} and friends.
     */
    public void test_getJoinVars() {
        fail("write tests");
    }
    
}
