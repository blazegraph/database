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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
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
public class TestStaticAnalysisMethods extends AbstractASTEvaluationTestCase {

    private final static Logger log = Logger
            .getLogger(TestStaticAnalysisMethods.class);
    
    /**
     * 
     */
    public TestStaticAnalysisMethods() {
    }

    /**
     * @param arg0
     */
    public TestStaticAnalysisMethods(String arg0) {
        super(arg0);
    }

    private static Set<IVariable<?>> asSet(final String[] vars) {

        final Set<IVariable<?>> set = new LinkedHashSet<IVariable<?>>();

        for (String s : vars) {

            set.add(Var.var(s));

        }

        return set;

    }

    private static Set<IVariable<?>> asSet(final IVariable<?>[] vars) {

        final Set<IVariable<?>> set = new LinkedHashSet<IVariable<?>>();

        set.addAll(Arrays.asList(vars));

        return set;

    }

    private static final Set<IVariable<?>> EMPTY_SET = Collections.emptySet();
    
    /**
     * Unit test of static analysis for variables which must be bound by a
     * query. This involves checking the WHERE clause and the projection for the
     * query.
     * 
     * @throws MalformedQueryException
     */
    @SuppressWarnings("unchecked")
    public void test_static_analysis01()
            throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x where { ?x rdf:type foaf:Person }";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);
        
        final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();
        
        expected.add(Var.var("x"));
        
        assertEquals(expected, queryRoot.getDefinatelyProducedBindings());

        assertEquals(
                expected,
                queryRoot.getWhereClause().getDefinatelyProducedBindings(
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
    @SuppressWarnings("unchecked")
    public void test_static_analysis02()
            throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x (12 as ?y) where { ?x rdf:type foaf:Person }";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);
        
        final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();
        
        expectedProjected.add(Var.var("x"));
        expectedProjected.add(Var.var("y"));
        
        assertEquals(expectedProjected, queryRoot.getDefinatelyProducedBindings());

        final Set<IVariable<?>> expectedWhereClause = new LinkedHashSet<IVariable<?>>();
        
        expectedWhereClause.add(Var.var("x"));

        assertEquals(
                expectedWhereClause,
                queryRoot.getWhereClause().getDefinatelyProducedBindings(
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
                .parseQuery2(queryStr, baseURI);

        // variables which must be bound in the top-level query's projection.
        {
            
            final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();

            expectedProjected.add(Var.var("x"));
            expectedProjected.add(Var.var("o"));

            assertEquals(expectedProjected,
                    queryRoot.getDefinatelyProducedBindings());
        }

        // variables which must be bound in the named subquery's projection.
        {

            final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();

            expectedProjected.add(Var.var("x"));

            final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) queryRoot
                    .getNamedSubqueries().get(0);

            assertEquals(expectedProjected,
                    namedSubquery.getDefinatelyProducedBindings());

        }

        // variables which must be bound in the main query's where clause.
        {
            
            final Set<IVariable<?>> expectedWhereClause = new LinkedHashSet<IVariable<?>>();

            expectedWhereClause.add(Var.var("x"));
            expectedWhereClause.add(Var.var("o"));

            assertEquals(
                    expectedWhereClause,
                    queryRoot
                            .getWhereClause()
                            .getDefinatelyProducedBindings(
                                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

    }
    
    /**
     * Static analysis of TCK query:
     * <pre>
     * PREFIX : <http://example/>
     * 
     * SELECT ?v { :x :p ?v . FILTER(?v = 1) 
     * </pre>
     */
    public void test_static_analysis_filter_nested_1() throws MalformedQueryException {

        final String queryStr = "" + //
                "PREFIX : <http://example/>\n" + //
                "SELECT ?v \n" +//
                "{ :x :p ?v . FILTER(?v = 1) }";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);

        final GraphPatternGroup<IGroupMemberNode> whereClause = queryRoot
                .getWhereClause();

        final Set<IVariable<?>> expected = asSet(new String[]{"v"});

        // Test "must" bound bindings for the query.
        assertEquals(expected, queryRoot.getDefinatelyProducedBindings());

        // Test "must" bound bindings for the where clause.
        assertEquals(expected, whereClause.getDefinatelyProducedBindings(
                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        // Test "maybe" bound bindings for the where clause.
        assertEquals(expected, whereClause.getMaybeProducedBindings(
                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        // Test "incoming" bindings for the where clause.
        assertEquals(EMPTY_SET,
                whereClause
                        .getIncomingBindings(new LinkedHashSet<IVariable<?>>()));

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

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);


        // Test "must" bound bindings for the query.
        assertEquals(asSet(new String[] { "v" }),
                queryRoot.getDefinatelyProducedBindings());

        // WHERE clause
        {

            final GraphPatternGroup<IGroupMemberNode> whereClause = queryRoot
                    .getWhereClause();

            final Set<IVariable<?>> expected = asSet(new String[] { "v" });

            // Test "incoming" bindings for the where clause.
            assertEquals(
                    EMPTY_SET,
                    whereClause
                            .getIncomingBindings(new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings for the where clause.
            assertEquals(expected, whereClause.getDefinatelyProducedBindings(
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings for the where clause.
            assertEquals(expected, whereClause.getMaybeProducedBindings(
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

        // FILTER's group clause.
        {

            final JoinGroupNode filterClause = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1);

            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "v" }),
                    filterClause
                            .getIncomingBindings(new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(EMPTY_SET, filterClause.getDefinatelyProducedBindings(
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(EMPTY_SET, filterClause.getMaybeProducedBindings(
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // The FILTER node itself.
            final FilterNode filter = BOpUtility.visitAll(queryRoot,
                    FilterNode.class).next();

            assertEquals(Collections.singletonList(filter), filterClause.getPreFilters());
            assertEquals(Collections.emptyList(), filterClause.getJoinFilters());
            assertEquals(Collections.emptyList(), filterClause.getPostFilters());
            assertEquals(Collections.emptyList(), filterClause.getFiltersToPrune());
            
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
     * FIXME This can be reduced to a filter which does not bind anything. Since
     * the FILTER can not succeed, it should logically be replaced by failing
     * the group(s) within which it appears. This needs to be done recursively
     * up to the parent, stopping at the first parent group which is optional.
     * If all parents up to the WHERE clause are eliminated, then the WHERE
     * clause itself can not succeed and the query should be replaced by a
     * "DropSolutionsBOp". The DropSolutionsOp can be substituted in directly
     * for a group which can not succeed and then we can work the pruning of the
     * parents as an efficiency.
     * 
     * FIXME Note that the variable visibility rules are not currently respected
     * by getIncomingBindings(). That is why this test is failing. Once we fix
     * the variable visibility rules in getIncomingBindings() the FILTER will be
     * reported as something which can be pruned. Once we drop the filter the
     * query should succeed. However, note that there are other TCK tests where
     * we can not drop the filter to succeed. That works in this case, but the
     * underlying reason why this query is failing is that we are not using
     * bottom up evaluation and <code>?v</code> in the outer group is not
     * visible to <code>?v</code> in the inner group.
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

        QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
                .parseQuery2(queryStr, baseURI);
        
        final AST2BOpContext context = new AST2BOpContext(queryRoot, store);

        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        // Rewrite the wild card in the projection.
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null /* bindingSets */);

        // Test "must" bound bindings for the query.
        assertEquals(asSet(new String[] { "v", "w" }),
                queryRoot.getDefinatelyProducedBindings());

        // Outer group clause { :x :p ?v . }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause();

            // Test "incoming" bindings.
            assertEquals(
                    EMPTY_SET,
                    group.getIncomingBindings(new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "v" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "v", "w" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "v" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "v", "w", "v2" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // Nested group clause {  :x :q ?w  }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1);

            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "v" }),
                    group.getIncomingBindings(new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "w" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "w" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "w" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "w", "v2" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // Optional group clause { :x :p ?v2 FILTER(?v = 1)  }
        {

            final JoinGroupNode group = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1).get(1);

            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "v", "w" }),
                    group.getIncomingBindings(new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "v2" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "v2" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "v2" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "v2" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));
            
            // The FILTER node itself.
            final FilterNode filter = BOpUtility.visitAll(queryRoot,
                    FilterNode.class).next();

            assertEquals(Collections.emptyList(), group.getPreFilters());
            assertEquals(Collections.emptyList(), group.getJoinFilters());
            assertEquals(Collections.emptyList(), group.getPostFilters());
            assertEquals(Collections.singletonList(filter), group.getFiltersToPrune());
            
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
     * 
     * FIXME In order for us to run this query correctly we need to run
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
     * Or, equally, we could do a binding set join rather than a pipelined
     * subquery. It amounts to the same thing. These group expressions need to
     * be evaluated independently because they are not sharing a binding for
     * <code>?X</code> until we join them together on <code>?X</code>.
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

        QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store).parseQuery2(
                queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(queryRoot, store);

        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        // Rewrite the wild card in the projection.
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null /* bindingSets */);

        // Rewrite the wild card in the projection.
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, queryRoot, null /* bindingSets */);

        // Test "must" bound bindings for the query.
        assertEquals(asSet(new String[] { "X", "Y" }),
                queryRoot.getDefinatelyProducedBindings());

        // Outer group clause { ?X :name "paul" }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause();

            // Test "incoming" bindings.
            assertEquals(
                    EMPTY_SET,
                    group.getIncomingBindings(new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "X" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "X", "Y" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "X" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "X", "Y", "Z" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // Nested group clause {  ?Y :name "george" }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1);

            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "X" }),
                    group.getIncomingBindings(new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "Y" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "Y" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "Y" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "Y", "X", "Z" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // Optional group clause OPTIONAL { ?X :email ?Z }
        {

            final JoinGroupNode group = (JoinGroupNode) queryRoot
                    .getWhereClause().get(1).get(1);

            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "X", "Y" }),
                    group.getIncomingBindings(new LinkedHashSet<IVariable<?>>()));

            // Test "must" bound bindings.
            assertEquals(
                    asSet(new String[] { "X", "Z" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "must" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "X", "Z" }),
                    group.getDefinatelyProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            // Test "maybe" bound bindings.
            assertEquals(
                    asSet(new String[] { "X", "Z" }),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), false/* recursive */));

            // Test "maybe" bound bindings (recursive).
            assertEquals(
                    asSet(new String[] { "X", "Z"}),
                    group.getMaybeProducedBindings(
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));
                        
        }

    }

}
