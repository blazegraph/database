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
package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.ASTSearchOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTBottomUpOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSetValueExpressionsOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSubGroupJoinVarOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTWildcardProjectionOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

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

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI).getOriginalAST();
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();
        
        expected.add(Var.var("x"));
        
        assertEquals(expected, sa.getDefinitelyProducedBindings(queryRoot));

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

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI).getOriginalAST();
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();
        
        expectedProjected.add(Var.var("x"));
        expectedProjected.add(Var.var("y"));
        
        assertEquals(expectedProjected, sa.getDefinitelyProducedBindings(queryRoot));

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

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI).getOriginalAST();
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();
        
        expectedProjected.add(Var.var("x"));
        expectedProjected.add(Var.var("y"));
        
        assertEquals(expectedProjected, sa.getDefinitelyProducedBindings(queryRoot));

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

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI).getOriginalAST();
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();
        
        expectedProjected.add(Var.var("x"));
        expectedProjected.add(Var.var("y"));
        
        assertEquals(expectedProjected, sa.getDefinitelyProducedBindings(queryRoot));

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
     * Unit test of static analysis for a SERVICE call.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/816" > Wildcard projection
     *      ignores variables inside a SERVICE call </a>
     */
    public void test_static_analysis05()
            throws MalformedQueryException {

        final String queryStr = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"+
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n"+
                "select ?x (12 as ?y)\n" +
                " where {\n" +
                "    service ?uri {\n" +
                "      ?x rdf:type foaf:Person .\n" +
                "      ?x rdfs:label ?z .\n" +
                "    }\n" +
                "}";

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI).getOriginalAST();
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();
        
        expectedProjected.add(Var.var("x"));
        expectedProjected.add(Var.var("y"));
        
        assertEquals(expectedProjected, sa.getDefinitelyProducedBindings(queryRoot));

        // The spanned variables includes the SERVICE URI (if it is a variable).
        {

            final Set<IVariable<?>> expectedWhereClause = new LinkedHashSet<IVariable<?>>();

            expectedWhereClause.add(Var.var("uri"));
            expectedWhereClause.add(Var.var("x"));
            expectedWhereClause.add(Var.var("z"));

            assertEquals(expectedWhereClause, sa.getSpannedVariables(
                    queryRoot.getWhereClause(),
                    new LinkedHashSet<IVariable<?>>()));
        }

        // The definitely bound variables does NOT include the SERVICE URI. When
        // that is a variable it needs to become bound through other means.
        {

            final Set<IVariable<?>> expectedWhereClause = new LinkedHashSet<IVariable<?>>();

            expectedWhereClause.add(Var.var("x"));
            expectedWhereClause.add(Var.var("z"));

            assertEquals(expectedWhereClause, sa.getDefinitelyProducedBindings(
                    queryRoot.getWhereClause(),
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

    }

    /**
     * Unit test for computing the join variables for a named subquery based on
     * the analysis of the bindings which MUST be produced by the subquery and
     * those which MUST be bound on entry into the group in which the subquery
     * solution set is included within the main query.
     * <p>
     * Note: The join should be on <code>?x</code> in this example.
     * 
     * FIXME Write more unit tests for
     * {@link StaticAnalysis#getJoinVars(SubqueryRoot, Set)} and friends.
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

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI).getOriginalAST();

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        // variables which must be bound in the top-level query's projection.
        {
            
            final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();

            expectedProjected.add(Var.var("x"));
            expectedProjected.add(Var.var("o"));

            assertEquals(expectedProjected,
                    sa.getDefinitelyProducedBindings(queryRoot));
            
        }

        // variables which must be bound in the named subquery's projection.
        {

            final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();

            expectedProjected.add(Var.var("x"));

            final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) queryRoot
                    .getNamedSubqueries().get(0);

            assertEquals(expectedProjected,
                    sa.getDefinitelyProducedBindings(namedSubquery));

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

        // the join variables as reported by static analysis.
        {

            final Set<IVariable<?>> expectedJoinVars = new LinkedHashSet<IVariable<?>>();
            expectedJoinVars.add(Var.var("x"));

            final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) queryRoot
                    .getNamedSubqueries().get(0);

            final NamedSubqueryInclude anInclude = BOpUtility.visitAll(
                    queryRoot, NamedSubqueryInclude.class).next();

            final Set<IVariable<?>> vars = sa.getJoinVars(namedSubquery,
                    anInclude, new LinkedHashSet<IVariable<?>>());

            assertEquals(expectedJoinVars, vars);

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

        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI).getOriginalAST();

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        @SuppressWarnings("unchecked")
        final GraphPatternGroup<IGroupMemberNode> whereClause = queryRoot
                .getWhereClause();

        final Set<IVariable<?>> expected = asSet(new String[]{"v"});

        // Test "must" bound bindings for the query.
        assertEquals(expected, sa.getDefinitelyProducedBindings(queryRoot));

        // Test "must" bound bindings for the where clause.
        assertEquals(expected, sa.getDefinitelyProducedBindings(whereClause,
                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        // Test "maybe" bound bindings for the where clause.
        assertEquals(expected,
                sa.getMaybeProducedBindings(whereClause,
                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        // Test "incoming" bindings for the where clause.
        assertEquals(EMPTY_SET, sa.getDefinitelyIncomingBindings(whereClause,
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

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(
                queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        // Set the IValueExpressions on the AST.
        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

         // Test "must" bound bindings for the query.
        assertEquals(asSet(new String[] { "v" }),
                sa.getDefinitelyProducedBindings(queryRoot));

        // WHERE clause
        {

            @SuppressWarnings("unchecked")
            final GraphPatternGroup<IGroupMemberNode> whereClause = queryRoot
                    .getWhereClause();

            final Set<IVariable<?>> expected = asSet(new String[] { "v" });

            // Test "incoming" bindings for the where clause.
            assertEquals(EMPTY_SET, sa.getDefinitelyIncomingBindings(whereClause,
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
            assertEquals(asSet(new String[] { "v" }), sa.getDefinitelyIncomingBindings(
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

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();
        
        // Set the IValueExpressions on the AST.
        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        // Rewrite the wild card in the projection.
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();
        
        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        // Test "must" bound bindings for the query.
        assertEquals(asSet(new String[] { "v", "w" }),
                sa.getDefinitelyProducedBindings(queryRoot));

        // Outer group clause { :x :p ?v . }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause();

            // Test "incoming" bindings.
            assertEquals(EMPTY_SET, sa.getDefinitelyIncomingBindings(group,
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
                    sa.getDefinitelyIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

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

            sa.getDefinitelyIncomingBindings(group,new LinkedHashSet<IVariable<?>>());
            
            // Test "incoming" bindings.
            assertEquals(
                    asSet(new String[] { "v", "w" }),
                    sa.getDefinitelyIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

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

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        // Rewrite the wild card in the projection.
        queryRoot = (QueryRoot) new ASTWildcardProjectionOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        // Test "must" bound bindings for the query.
        assertEquals(asSet(new String[] { "X", "Y" }),
                sa.getDefinitelyProducedBindings(queryRoot));

        // Outer group clause { ?X :name "paul" }
        {

            final GraphPatternGroup<IGroupMemberNode> group = (JoinGroupNode) queryRoot
                    .getWhereClause();

            // Test "incoming" bindings.
            assertEquals(
                    EMPTY_SET,
                    sa.getDefinitelyIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

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
                    sa.getDefinitelyIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

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
                    sa.getDefinitelyIncomingBindings(group,new LinkedHashSet<IVariable<?>>()));

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
     * {@link ServiceNode#getGraphPattern()}.
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

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        // rewrite the search predicates as an AST ServiceNode.
        queryRoot = (QueryRoot) new ASTSearchOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

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
                        sa.getDefinitelyProducedBindings(queryRoot));
                
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
     * Test suite for predicting the join variables for a SERVICE call.
     * 
     * <pre>
     * SELECT ?s ?o1 ?o2
     * {
     *   SERVICE <http://localhost:18080/openrdf/repositories/endpoint1> {
     *   ?s ?p ?o1 . }
     *   OPTIONAL {
     *     SERVICE <http://localhost:18080/openrdf/repositories/endpoint2> {
     *     ?s ?p2 ?o2 }
     *   }
     * }
     * </pre>
     */
    public void test_static_analysis_serviceCall2() throws MalformedQueryException {

        final String queryStr = "SELECT ?s ?o1 ?o2\n"//
                + "{\n"//
                + "  SERVICE <http://localhost:18080/openrdf/repositories/endpoint1> {\n"//
                + "  ?s ?p ?o1 . }\n"//
                + "  OPTIONAL {\n"//
                + "    SERVICE <http://localhost:18080/openrdf/repositories/endpoint2> {\n"//
                + "    ?s ?p2 ?o2 }\n"//
                + "  }\n"//
                + "}";

        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        // Assign join variables to join groups.
        queryRoot = (QueryRoot) new ASTSubGroupJoinVarOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        if (log.isInfoEnabled())
            log.info("\nqueryStr=\n" + queryStr + "\nAST:\n"
                    + BOpUtility.toString(queryRoot));

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        /**
         * Locate the various pieces of the AST.
         * 
         * <pre>
         * 
         * QueryType: SELECT
         * SELECT VarNode(s) VarNode(o1) VarNode(o2)
         *   JoinGroupNode {
         *     SERVICE <ConstantNode(TermId(0U)[http://localhost:18080/openrdf/repositories/endpoint1])> {
         *       JoinGroupNode {
         *         StatementPatternNode(VarNode(s), VarNode(p), VarNode(o1), DEFAULT_CONTEXTS)
         *       }
         *     }
         *     JoinGroupNode [optional] {
         *       SERVICE <ConstantNode(TermId(0U)[http://localhost:18080/openrdf/repositories/endpoint2])> {
         *         JoinGroupNode {
         *           StatementPatternNode(VarNode(s), VarNode(p2), VarNode(o2), DEFAULT_CONTEXTS)
         *         }
         *       }
         *     }
         *   }
         * </pre>
         */
        final GraphPatternGroup<?> whereClause = queryRoot.getWhereClause();
        final ServiceNode endpoint1;
        final JoinGroupNode optionalGroup;
        final ServiceNode endpoint2;
        {
         
            endpoint1 = (ServiceNode) whereClause.get(0);
            
            assertEquals(endpoint1.getServiceRef().getValue(), new URIImpl(
                    "http://localhost:18080/openrdf/repositories/endpoint1"));

            optionalGroup = (JoinGroupNode) whereClause.get(1);
            
            assertTrue(optionalGroup.isOptional());

            endpoint2 = (ServiceNode) optionalGroup.get(0);
            
            assertEquals(endpoint2.getServiceRef().getValue(), new URIImpl(
                    "http://localhost:18080/openrdf/repositories/endpoint2"));

//            final Iterator<ServiceNode> itr = BOpUtility.visitAll(
//                    whereClause, ServiceNode.class);
//            
//            endpoint1 = itr.next();
//            
//            assertEquals(endpoint1.getServiceRef().getValue(), new URIImpl(
//                    "http://localhost:18080/openrdf/repositories/endpoint1"));
//
//            endpoint2 = itr.next();
//            
//            assertEquals(endpoint2.getServiceRef().getValue(), new URIImpl(
//                    "http://localhost:18080/openrdf/repositories/endpoint2"));
//            
//            assertFalse(itr.hasNext());

        }
        
        {
            // variables which must be bound in the top-level query's
            // projection.
            {

                final Set<IVariable<?>> expectedProjected = new LinkedHashSet<IVariable<?>>();

                expectedProjected.add(Var.var("s"));
                expectedProjected.add(Var.var("o1"));

                assertEquals(expectedProjected,
                        sa.getDefinitelyProducedBindings(queryRoot));
                
            }

            // variables which must be bound in the main query's where clause.
            {

                final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();

                expected.add(Var.var("s"));
                expected.add(Var.var("p"));
                expected.add(Var.var("o1"));

                assertEquals(expected, sa.getDefinitelyProducedBindings(
                        queryRoot.getWhereClause(),
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            }

            // variables which may be bound in the main query's where clause.
            {

                final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();

                expected.add(Var.var("s"));
                expected.add(Var.var("p"));
                expected.add(Var.var("o1"));
                expected.add(Var.var("p2"));
                expected.add(Var.var("o2"));

                assertEquals(expected, sa.getMaybeProducedBindings(
                        queryRoot.getWhereClause(),
                        new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            }

        }

        // variables which must be bound by endpoint1.
        {

            final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();

            expected.add(Var.var("s"));
            expected.add(Var.var("p"));
            expected.add(Var.var("o1"));

            assertEquals(expected, sa.getDefinitelyProducedBindings(endpoint1,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // variables which must be bound by endpoint2.
        {

            final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();

            expected.add(Var.var("s"));
            expected.add(Var.var("p2"));
            expected.add(Var.var("o2"));

            assertEquals(expected, sa.getDefinitelyProducedBindings(endpoint2,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

        // ServiceCallJoin variables for endpoint1
        {
            
            final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();

            assertEquals(expected, sa.getJoinVars(endpoint1,
                    new LinkedHashSet<IVariable<?>>()));
            
        }

        // ServiceCallJoin variables for endpoint2
        {
            
            final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();

            expected.add(Var.var("s"));

            assertEquals(expected, sa.getJoinVars(endpoint2,
                    new LinkedHashSet<IVariable<?>>()));
            
        }

        // Join with the OPTIONAL group.
        {
            
            final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();
            expected.add(Var.var("s"));

            final Set<IVariable<?>> actual = new HashSet<IVariable<?>>();
            for (IVariable<?> v : optionalGroup.getJoinVars()) {
                actual.add(v);
            }

            assertEquals(expected, actual);
            
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
        
        final ASTContainer astContainer = new Bigdata2ASTSPARQLParser()
                .parseQuery2(queryStr, baseURI);

        final AST2BOpContext context = new AST2BOpContext(astContainer, store);

        QueryRoot queryRoot = astContainer.getOriginalAST();

        // Set the IValueExpressions on the AST.
        queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer().optimize(
                context, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

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
     * Unit test focused on required and optional {@link StatementPatternNode}
     * s.
     */
    public void test_static_analysis_getMaybeProducedBindings() {

        final IV<?, ?> p = makeIV(new URIImpl("http://example/p"));
        final IV<?, ?> q = makeIV(new URIImpl("http://example/q"));

        // The source AST.
        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause;
        final StatementPatternNode sp1, sp2;
        {

            final ProjectionNode projection = new ProjectionNode();
            queryRoot.setProjection(projection);

            projection.addProjectionVar(new VarNode("a"));
            projection.addProjectionVar(new VarNode("n"));

            whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            sp1 = new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(p), new VarNode("n"));
            whereClause.addChild(sp1);

            sp2 = new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(q), new VarNode("m"));
            whereClause.addChild(sp2);
            sp2.setOptional(true);

        }
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        // Where clause.
        {

            assertEquals(Collections.emptySet(),
                    sa.getDefinitelyIncomingBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>()));

            final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("n"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            expectedVars.add(Var.var("m"));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(whereClause,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // sp1
        {

            assertEquals(Collections.emptySet(),
                    sa.getDefinitelyIncomingBindings(sp1,
                            new LinkedHashSet<IVariable<?>>()));

            final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("n"));

            assertEquals(expectedVars, sa.getDefinitelyProducedBindings(sp1,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(sp1,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
        // sp2
        {

            {
                final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

                expectedVars.add(Var.var("a"));
                expectedVars.add(Var.var("n"));

                assertEquals(expectedVars, sa.getDefinitelyIncomingBindings(
                        sp2, new LinkedHashSet<IVariable<?>>()));
            }

            {
                final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

                expectedVars.add(Var.var("a"));
                expectedVars.add(Var.var("m"));

                assertEquals(
                        expectedVars,
                        sa.getDefinitelyProducedBindings(sp2,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

                assertEquals(
                        expectedVars,
                        sa.getMaybeProducedBindings(sp2,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            }
            
        }
        
    }
    
    /**
     * Unit test of static analysis methods as they pertain to a MINUS group.
     * Unlike OPTIONAL, the variables in the left and right hand side of a MINUS
     * operator are not visible to one another. This has implications for (a)
     * MINUS operators where the right hand side does not share any variables
     * (such cases should be pruned); and (b) FILTERS in the right hand side
     * will fail in they assume visibility of variables in the left hand side.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT ?a ?n 
     * WHERE {
     *     ?a :p ?n
     *     MINUS {
     *         ?a :q ?n .
     *     }
     * }
     * </pre>
     */
    public void test_static_analysis_minus_sharedVariables() {
        
        final IV<?,?> p = makeIV(new URIImpl("http://example/p"));
        final IV<?,?> q = makeIV(new URIImpl("http://example/q"));

        // The source AST.
        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, minusGroup;
        {

            final ProjectionNode projection = new ProjectionNode();
            queryRoot.setProjection(projection);

            projection.addProjectionVar(new VarNode("a"));
            projection.addProjectionVar(new VarNode("n"));

            whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(p), new VarNode("n")));

            minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);
            minusGroup.setMinus(true);

            minusGroup.addChild(new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(q), new VarNode("n")));

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

        expectedVars.add(Var.var("a"));
        expectedVars.add(Var.var("n"));
        
        /*
         * First, the QueryRoot.
         */
        {

            assertEquals(expectedVars, sa.getDefinitelyProducedBindings(queryRoot));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(queryRoot));

        }

        /*
         * Now the whereClause.
         */
        {

            assertEquals(Collections.emptySet(),
                    sa.getDefinitelyIncomingBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>()));

            assertEquals(Collections.emptySet(),
                    sa.getMaybeIncomingBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>()));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(whereClause,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

        /*
         * Finally, the MINUS group.
         */
        {
            
            assertEquals(expectedVars, sa.getDefinitelyIncomingBindings(
                    minusGroup, new LinkedHashSet<IVariable<?>>()));

            assertEquals(expectedVars, sa.getMaybeIncomingBindings(
                    minusGroup, new LinkedHashSet<IVariable<?>>()));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(minusGroup,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(minusGroup,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
    }
    
    /**
     * Variant test for a MINUS operator without shared variables.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT ?s ?p ?o
     * WHERE {
     *     ?s ?p ?o
     *     MINUS {
     *         ?x ?y ?z .
     *     }
     * }
     * </pre>
     */
    public void test_static_analysis_minus_nothingShared() {

        // The source AST.
        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, minusGroup;
        {

            final ProjectionNode projection = new ProjectionNode();
            queryRoot.setProjection(projection);

            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("p"));
            projection.addProjectionVar(new VarNode("o"));

            whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p"), new VarNode("o")));

            minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);
            minusGroup.setMinus(true);

            minusGroup.addChild(new StatementPatternNode(new VarNode("x"),
                    new VarNode("y"), new VarNode("z")));

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

        expectedVars.add(Var.var("s"));
        expectedVars.add(Var.var("p"));
        expectedVars.add(Var.var("o"));
        
        final Set<IVariable<?>> otherVars = new LinkedHashSet<IVariable<?>>();

        otherVars.add(Var.var("x"));
        otherVars.add(Var.var("y"));
        otherVars.add(Var.var("z"));

        /*
         * First, the QueryRoot.
         */
        {

            assertEquals(expectedVars,
                    sa.getDefinitelyProducedBindings(queryRoot));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(queryRoot));

        }

        /*
         * Now the whereClause.
         */
        {

            assertEquals(Collections.emptySet(),
                    sa.getDefinitelyIncomingBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>()));

            assertEquals(Collections.emptySet(),
                    sa.getMaybeIncomingBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>()));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(whereClause,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

        /*
         * Finally, the MINUS group.
         */
        {
            
            assertEquals(expectedVars, sa.getDefinitelyIncomingBindings(
                    minusGroup, new LinkedHashSet<IVariable<?>>()));

            assertEquals(expectedVars, sa.getMaybeIncomingBindings(
                    minusGroup, new LinkedHashSet<IVariable<?>>()));

            assertEquals(
                    otherVars,
                    sa.getDefinitelyProducedBindings(minusGroup,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(otherVars, sa.getMaybeProducedBindings(minusGroup,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }
        
    }
    
    /**
     * Variant test for query mixing MINUS and OPTIONAL groups.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT ?a ?n ?b
     * WHERE {
     *     ?a :p ?n
     *     OPTIONAL {
     *        ?b :p 3.0
     *     }
     *     MINUS {
     *         ?a :q ?n .
     *         OPTIONAL {
     *            ?c :q 3.0
     *         }
     *     }
     * }
     * </pre>
     */
    public void test_static_analysis_minus_and_optional() {
        
        final IV<?,?> p = makeIV(new URIImpl("http://example/p"));
        final IV<?,?> q = makeIV(new URIImpl("http://example/q"));
        final IV<?, ?> three = makeIV(new LiteralImpl("3.0", XSD.DECIMAL));

        // The source AST.
        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, optGroup1, minusGroup, optGroup2;
        {

            final ProjectionNode projection = new ProjectionNode();
            queryRoot.setProjection(projection);

            projection.addProjectionVar(new VarNode("a"));
            projection.addProjectionVar(new VarNode("n"));
            projection.addProjectionVar(new VarNode("b"));

            whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(p), new VarNode("n")));
            
            optGroup1 = new JoinGroupNode(true/*optional*/);
            whereClause.addChild(optGroup1);
            
            optGroup1.addChild(new StatementPatternNode(new VarNode("b"),
                    new ConstantNode(p), new ConstantNode(three)));
            
            minusGroup = new JoinGroupNode();
            whereClause.addChild(minusGroup);
            minusGroup.setMinus(true);

            minusGroup.addChild(new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(q), new VarNode("n")));

            optGroup2 = new JoinGroupNode(true/*optional*/);
            minusGroup.addChild(optGroup2);
            
            optGroup2.addChild(new StatementPatternNode(new VarNode("c"),
                    new ConstantNode(q), new ConstantNode(three)));

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        /*
         * First, the QueryRoot.
         */
        {

            final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("n"));

            assertEquals(expectedVars, sa.getDefinitelyProducedBindings(queryRoot));

            expectedVars.add(Var.var("b"));
            
            assertEquals(expectedVars, sa.getMaybeProducedBindings(queryRoot));

        }

        /*
         * Now the whereClause.
         */
        {
            
            assertEquals(Collections.emptySet(),
                    sa.getDefinitelyIncomingBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>()));

            assertEquals(Collections.emptySet(),
                    sa.getMaybeIncomingBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>()));

            final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("n"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            expectedVars.add(Var.var("b"));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(whereClause,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

        /*
         * Optional group1.
         */
        {

            final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("n"));

            assertEquals(expectedVars,
                    sa.getDefinitelyIncomingBindings(optGroup1,
                            new LinkedHashSet<IVariable<?>>()));

            assertEquals(expectedVars,
                    sa.getMaybeIncomingBindings(optGroup1,
                            new LinkedHashSet<IVariable<?>>()));

            expectedVars.clear();
            expectedVars.add(Var.var("b"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(optGroup1,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(optGroup1,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

        /*
         * The MINUS group.
         */
        {

            {

                final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

                expectedVars.add(Var.var("a"));
                expectedVars.add(Var.var("n"));

                assertEquals(expectedVars, sa.getDefinitelyIncomingBindings(
                        minusGroup, new LinkedHashSet<IVariable<?>>()));

                expectedVars.add(Var.var("b"));

                assertEquals(expectedVars, sa.getMaybeIncomingBindings(
                        minusGroup, new LinkedHashSet<IVariable<?>>()));

            }

            {

                final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

                expectedVars.add(Var.var("a"));
                expectedVars.add(Var.var("n"));

                assertEquals(
                        expectedVars,
                        sa.getDefinitelyProducedBindings(minusGroup,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

                expectedVars.add(Var.var("c"));

                assertEquals(
                        expectedVars,
                        sa.getMaybeProducedBindings(minusGroup,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */));
            }

        }
        
        /*
         * Optional group 2.
         */
        {

            final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("n"));

            assertEquals(expectedVars,
                    sa.getDefinitelyIncomingBindings(optGroup2,
                            new LinkedHashSet<IVariable<?>>()));

            expectedVars.add(Var.var("b"));

            assertEquals(expectedVars,
                    sa.getMaybeIncomingBindings(optGroup2,
                            new LinkedHashSet<IVariable<?>>()));

            expectedVars.clear();
            expectedVars.add(Var.var("c"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(optGroup2,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(optGroup2,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        } 
        
    }

    /**
     * Variant test for query mixing normal child join groups and OPTIONAL
     * join groups.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT ?a ?n ?b ?c
     * WHERE {
     *     ?a :p ?n
     *     {
     *         ?a :q ?n .
     *         OPTIONAL {
     *            ?c :q 3.0
     *         }
     *     }
     *     OPTIONAL {
     *         ?b :p 3.0
     *     }
     * }
     * </pre>
     */
    public void test_static_analysis_subGroups_and_optional() {
        
        final IV<?,?> p = makeIV(new URIImpl("http://example/p"));
        final IV<?,?> q = makeIV(new URIImpl("http://example/q"));
        final IV<?, ?> three = makeIV(new LiteralImpl("3.0", XSD.DECIMAL));

        // The source AST.
        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, optGroup1, childGroup, optGroup2;
        {

            final ProjectionNode projection = new ProjectionNode();
            queryRoot.setProjection(projection);

            projection.addProjectionVar(new VarNode("a"));
            projection.addProjectionVar(new VarNode("n"));
            projection.addProjectionVar(new VarNode("b"));
            projection.addProjectionVar(new VarNode("c"));

            whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(p), new VarNode("n")));
            
            {
                childGroup = new JoinGroupNode();
                whereClause.addChild(childGroup);

                childGroup.addChild(new StatementPatternNode(new VarNode("a"),
                        new ConstantNode(q), new VarNode("n")));

                optGroup2 = new JoinGroupNode(true/* optional */);
                childGroup.addChild(optGroup2);

                optGroup2.addChild(new StatementPatternNode(new VarNode("c"),
                        new ConstantNode(q), new ConstantNode(three)));
            }

            {
                optGroup1 = new JoinGroupNode(true/* optional */);
                whereClause.addChild(optGroup1);

                optGroup1.addChild(new StatementPatternNode(new VarNode("b"),
                        new ConstantNode(p), new ConstantNode(three)));
            }
            
        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        /*
         * First, the QueryRoot.
         */
        {

            final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("n"));

            assertEquals(expectedVars, sa.getDefinitelyProducedBindings(queryRoot));

            expectedVars.add(Var.var("b"));
            expectedVars.add(Var.var("c"));
            
            assertEquals(expectedVars, sa.getMaybeProducedBindings(queryRoot));

        }

        /*
         * Now the whereClause.
         */
        {
            
            assertEquals(Collections.emptySet(),
                    sa.getDefinitelyIncomingBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>()));

            assertEquals(Collections.emptySet(),
                    sa.getMaybeIncomingBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>()));

            final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("n"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            expectedVars.add(Var.var("b"));
            expectedVars.add(Var.var("c"));

            assertEquals(expectedVars, sa.getMaybeProducedBindings(whereClause,
                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

        /*
         * The child join group.
         */
        {

            {

                final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

                expectedVars.add(Var.var("a"));
                expectedVars.add(Var.var("n"));

                assertEquals(expectedVars, sa.getDefinitelyIncomingBindings(
                        childGroup, new LinkedHashSet<IVariable<?>>()));

                assertEquals(expectedVars, sa.getMaybeIncomingBindings(
                        childGroup, new LinkedHashSet<IVariable<?>>()));

            }

            {

                final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

                expectedVars.add(Var.var("a"));
                expectedVars.add(Var.var("n"));

                assertEquals(
                        expectedVars,
                        sa.getDefinitelyProducedBindings(childGroup,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

                expectedVars.add(Var.var("c"));

                assertEquals(
                        expectedVars,
                        sa.getMaybeProducedBindings(childGroup,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */));
            }

            /*
             * Optional group 2 (embedded in the child join group).
             */
            {

                {
                    final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

                    expectedVars.add(Var.var("a"));
                    expectedVars.add(Var.var("n"));

                    assertEquals(expectedVars,
                            sa.getDefinitelyIncomingBindings(optGroup2,
                                    new LinkedHashSet<IVariable<?>>()));

                    assertEquals(expectedVars, sa.getMaybeIncomingBindings(
                            optGroup2, new LinkedHashSet<IVariable<?>>()));
                }

                {
                    final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();
                    expectedVars.add(Var.var("c"));

                    assertEquals(
                            expectedVars,
                            sa.getDefinitelyProducedBindings(optGroup2,
                                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));

                    assertEquals(
                            expectedVars,
                            sa.getMaybeProducedBindings(optGroup2,
                                    new LinkedHashSet<IVariable<?>>(), true/* recursive */));
                }

            } 

        }

        /*
         * Optional group1.
         */
        {

            {

                final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

                expectedVars.add(Var.var("a"));
                expectedVars.add(Var.var("n"));

                assertEquals(expectedVars, sa.getDefinitelyIncomingBindings(
                        optGroup1, new LinkedHashSet<IVariable<?>>()));

                expectedVars.add(Var.var("c"));

                assertEquals(expectedVars, sa.getMaybeIncomingBindings(
                        optGroup1, new LinkedHashSet<IVariable<?>>()));
            }

            {
                
                final Set<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

                expectedVars.add(Var.var("b"));

                assertEquals(
                        expectedVars,
                        sa.getDefinitelyProducedBindings(optGroup1,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

                assertEquals(
                        expectedVars,
                        sa.getMaybeProducedBindings(optGroup1,
                                new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            }

        }

    }

    /**
     * Unit test for
     * {@link StaticAnalysis#getProjectedVars(IGroupMemberNode, GraphPatternGroup, QueryBase, Set, Set)}
     * . This unit test is a based on
     * <code>bigdata-perf/CI/govtrack/queries/query10.rq</code>
     * <p>
     * Given:
     * 
     * <pre>
     * SELECT ?var1 ?var6 ?var4 ?var10
     *  WHERE {
     *         ?var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
     *         OPTIONAL {
     *                 ?var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?var6
     *         }.
     *         OPTIONAL {
     *                 ?var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?var1.
     *                 ?var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?var4
     *         }.
     *         OPTIONAL {
     *                 ?var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?var13.
     *                 ?var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family> ?var10
     *         }
     * }
     * </pre>
     * 
     * This test verifies that the correct projection is computed for each of
     * the sub-groups in the query. Those projections are as follows:
     * 
     * <pre>
     *         SELECT ?var1 ?var6
     *         WHERE {
     *                 ?var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
     *                 OPTIONAL {
     *                      ?var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?var6
     *                 }.
     *         }
     * </pre>
     * 
     * <pre>
     *         SELECT ?var1 ?var4
     *         WHERE {
     *            INCLUDE %_set1
     *            OPTIONAL {
     *                 ?var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?var1.
     *                 ?var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?var4
     *            }.
     *         }
     * </pre>
     * 
     * <pre>
     *      SELECT ?var1 ?var10
     *      WHERE {
     *         INCLUDE %_set1
     *         OPTIONAL {
     *                 ?var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?var13.
     *                 ?var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family> ?var10
     *         }
     *     }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/397
     */
    public void test_static_analysis_getProjectedVars() {

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV polititian = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/Politician"));
        @SuppressWarnings("rawtypes")
        final IV name = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/name"));
        @SuppressWarnings("rawtypes")
        final IV sponsor = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/sponsor"));
        @SuppressWarnings("rawtypes")
        final IV title = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/title"));
        @SuppressWarnings("rawtypes")
        final IV N = makeIV(new URIImpl("http://www.w3.org/2001/vcard-rdf/3.0#N"));
        @SuppressWarnings("rawtypes")
        final IV family = makeIV(new URIImpl("http://www.w3.org/2001/vcard-rdf/3.0#Family"));

        // The source AST.
        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, optionalGroup1, optionalGroup2, optionalGroup3;
        {

            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);
                projection.addProjectionVar(new VarNode("var1"));
                projection.addProjectionVar(new VarNode("var6"));
                projection.addProjectionVar(new VarNode("var4"));
                projection.addProjectionVar(new VarNode("var10"));
            }

            whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            // ?_var1 a <http://www.rdfabout.com/rdf/schema/politico/Politician>
            whereClause.addChild(new StatementPatternNode(new VarNode("var1"),
                    new ConstantNode(a), new ConstantNode(polititian),
                    null/* c */, Scope.DEFAULT_CONTEXTS));

            // ?_var1 <http://www.rdfabout.com/rdf/schema/usgovt/name> ?_var6
            {
                optionalGroup1 = new JoinGroupNode(true/* optional */);
                whereClause.addChild(optionalGroup1);
                
                optionalGroup1.addChild(new StatementPatternNode(
                        new VarNode("var1"), new ConstantNode(name),
                        new VarNode("var6"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));
            }

            {
                optionalGroup2 = new JoinGroupNode(true/* optional */);
                whereClause.addChild(optionalGroup2);

                // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/sponsor> ?_var1.
                optionalGroup2.addChild(new StatementPatternNode(new VarNode(
                        "var12"), new ConstantNode(sponsor), new VarNode(
                        "var1"), null/* c */, Scope.DEFAULT_CONTEXTS));
                
                // ?_var12 <http://www.rdfabout.com/rdf/schema/usbill/title> ?_var4
                optionalGroup2.addChild(new StatementPatternNode(new VarNode(
                        "var12"), new ConstantNode(title), new VarNode(
                        "var4"), null/* c */, Scope.DEFAULT_CONTEXTS));
                
            }

            {
                optionalGroup3 = new JoinGroupNode(true/* optional */);
                whereClause.addChild(optionalGroup3);

                // ?_var1 <http://www.w3.org/2001/vcard-rdf/3.0#N> ?_var13.
                optionalGroup3.addChild(new StatementPatternNode(new VarNode(
                        "var1"), new ConstantNode(N), new VarNode(
                        "var13"), null/* c */, Scope.DEFAULT_CONTEXTS));

                // ?_var13 <http://www.w3.org/2001/vcard-rdf/3.0#Family> ?_var10
                optionalGroup3.addChild(new StatementPatternNode(new VarNode(
                        "var13"), new ConstantNode(family), new VarNode(
                        "var10"), null/* c */, Scope.DEFAULT_CONTEXTS));

            }

        }
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        final Set<IVariable<?>> exogenousVars = new LinkedHashSet<IVariable<?>>();
        
        // Optional group 1.
        if(true){

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();
            
            expectedVars.add(Var.var("var1"));
            expectedVars.add(Var.var("var6"));
            
            assertEquals(expectedVars, sa.getProjectedVars(optionalGroup1,
                    optionalGroup1, queryRoot, exogenousVars,
                    new LinkedHashSet<IVariable<?>>()// projectedVars
                    ));
            
        }

        // Optional group 2.
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();
            
            expectedVars.add(Var.var("var1"));
            expectedVars.add(Var.var("var4"));
            
            assertEquals(expectedVars, sa.getProjectedVars(optionalGroup2,
                    optionalGroup2, queryRoot, exogenousVars,
                    new LinkedHashSet<IVariable<?>>()// projectedVars
                    ));
            
        }

        // Optional group 3.
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();
            
            expectedVars.add(Var.var("var1"));
            expectedVars.add(Var.var("var10"));
            
            assertEquals(expectedVars, sa.getProjectedVars(optionalGroup3,
                    optionalGroup3, queryRoot, exogenousVars,
                    new LinkedHashSet<IVariable<?>>()// projectedVars
                    ));
            
        }

    }

    /**
     * Unit test for static analysis of the MUST and MIGHT bound variables for a
     * subquery.
     * 
     * <pre>
     * SELECT (?a as ?b) {
     *    ?a rdf:type foaf:Person
     * }
     * </pre>
     * 
     * Should report that <code>?b</code> must be bound
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/430
     * 
     *      TODO Extend this test to cover the case where <code>b</code> is an
     *      exogenously bound variable.  This does not change the MUST/MIGHT
     *      analysis of the query.
     */
    public void test_static_analysis_projection_01() {
        
        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);

        // The source AST.
        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause;
        {

            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                projection.addProjectionExpression(new AssignmentNode(
                        new VarNode("b"), new VarNode("a")));
            }

            whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(rdfType), new ConstantNode(foafPerson)));

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        final Set<IVariable<?>> exogenousVars = new LinkedHashSet<IVariable<?>>();

        // QueryRoot
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("b"));

            assertEquals(expectedVars,
                    sa.getDefinitelyProducedBindings(queryRoot));

            assertEquals(expectedVars,
                    sa.getMaybeProducedBindings(queryRoot));

        }

        // whereClause
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(
                    expectedVars,
                    sa.getMaybeProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

    }

    /**
     * Variant with optional group binding <code>?a</code> (so <code>?b</code>
     * is MIGHT be bound).
     * 
     * <pre>
     * SELECT (?a as ?b) {
     *    OPTIONAL {
     *      ?a rdf:type foaf:Person
     *    }
     * }
     * </pre>
     * 
     * Should report that <code>?b</code> might be bound
     * 
     * TODO Extend this test to also cover the case where <code>b</code> is an
     * exogenously bound variable. In that case, it MUST be bound.
     * 
     * TODO Extend this test to also cover the case where <code>a</code> is an
     * exogenously bound variable. Since <code>a</code> is not projected, it's
     * bound value SHOULD NOT be visible inside of the query (variables with
     * exogenous variables must obey the same scoping rules as variables which
     * only become bound during query evaluation).
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/430
     */
    public void test_static_analysis_projection_02() {
        
        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
    
        // The source AST.
        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, optionalGroup;
        {
    
            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);
    
                projection.addProjectionExpression(new AssignmentNode(
                        new VarNode("b"), new VarNode("a")));
            }
    
            whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);
    
            optionalGroup = new JoinGroupNode(true/*optional*/);
            whereClause.addChild(optionalGroup);
            
            optionalGroup.addChild(new StatementPatternNode(new VarNode("a"),
                    new ConstantNode(rdfType), new ConstantNode(foafPerson)));
    
        }
    
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
    
        final Set<IVariable<?>> exogenousVars = new LinkedHashSet<IVariable<?>>();
    
        // QueryRoot
        {
    
            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();
    
            assertEquals(expectedVars,
                    sa.getDefinitelyProducedBindings(queryRoot));
    
            expectedVars.add(Var.var("b"));
            
            assertEquals(expectedVars,
                    sa.getMaybeProducedBindings(queryRoot));
    
        }
    
        // whereClause
        {
    
            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();
    
            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));
    
            expectedVars.add(Var.var("a"));
            
            assertEquals(
                    expectedVars,
                    sa.getMaybeProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));
    
        }
    
        // optionalGroup
        {
    
            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();
    
            expectedVars.add(Var.var("a"));
            
            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(optionalGroup,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));
    
            assertEquals(
                    expectedVars,
                    sa.getMaybeProducedBindings(optionalGroup,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));
    
        }
    
    }

    /**
     * Unit test where the SELECT expression includes the bind of a constant
     * onto a variable.
     */
    public void test_static_analysis_projection_03() {
        
//        @SuppressWarnings("rawtypes")
//        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);

        // The source AST.
        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause;
        {

            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                projection.addProjectionExpression(new AssignmentNode(
                        new VarNode("b"), new ConstantNode(foafPerson)));

            }

            whereClause = new JoinGroupNode();
            queryRoot.setWhereClause(whereClause);

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        final Set<IVariable<?>> exogenousVars = new LinkedHashSet<IVariable<?>>();

        // QueryRoot
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("b"));

            assertEquals(expectedVars,
                    sa.getDefinitelyProducedBindings(queryRoot));

            assertEquals(expectedVars,
                    sa.getMaybeProducedBindings(queryRoot));

        }

        // whereClause
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(
                    expectedVars,
                    sa.getMaybeProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

    }

    /**
     * Unit test for static analysis of the MUST and MIGHT bound variables for a
     * query involving a select expression which could result in an error. Note
     * that this query is NOT an aggregation query so the error will fail the
     * solution.
     * 
     * <pre>
     * SELECT ?a ?b (?a/?b as ?c) {
     *    ?x rdf:type foaf:Person .
     *    ?x :age ?a .
     *    ?x :grade ?b .
     * }
     * </pre>
     * 
     * In this query, it is presumed that <code>?b</code> is the number of years
     * of school and will be ZERO in the data for people who have not yet begun
     * formal schooling. Hence, the select expression can evaluate to an error
     * (divide by zero). However, since this is NOT an aggregation query, the
     * error will cause the solution to be dropped. Hence, <code>?c</code> is
     * definitely bound for this query.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/430
     * 
     *      TODO Extend this test to cover the case where <code>b</code> is an
     *      exogenously bound variable. This does not change the MUST/MIGHT
     *      analysis of the query.
     */
    public void test_static_analysis_projection_04() {

        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
        @SuppressWarnings("rawtypes")
        final IV age = makeIV(new URIImpl("http://example.org/age"));
        @SuppressWarnings("rawtypes")
        final IV grade = makeIV(new URIImpl("http://example.org/grade"));

        // The source AST.
        QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause;
        {

            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                projection.addProjectionVar(new VarNode("a"));
                projection.addProjectionVar(new VarNode("b"));
                projection.addProjectionExpression(new AssignmentNode(
                        new VarNode("c"), FunctionNode.binary(
                                FunctionRegistry.DIVIDE, new VarNode("a"),
                                new VarNode("b"))));
            }

            // WHERE clause.
            {
                whereClause = new JoinGroupNode();
                queryRoot.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(new VarNode("x"),
                                new ConstantNode(rdfType), new ConstantNode(
                                        foafPerson)));

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(age), new VarNode("a")));

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(grade), new VarNode("b")));
            }

        }

        /*
         * Note: Since it involves function nodes, we need to generate the value
         * expressions in order to run this test.
         */
        {

            final IBindingSet[] bindingSets = new IBindingSet[] {};

            final ASTContainer astContainer = new ASTContainer(queryRoot);

            final AST2BOpContext context = new AST2BOpContext(astContainer,
                    store);

            queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer()
                    .optimize(context, new QueryNodeWithBindingSet(queryRoot, bindingSets))
                    .getQueryNode();

        }
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        final Set<IVariable<?>> exogenousVars = new LinkedHashSet<IVariable<?>>();

        // QueryRoot
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("b"));
            expectedVars.add(Var.var("c"));

            assertEquals(expectedVars,
                    sa.getDefinitelyProducedBindings(queryRoot));

            assertEquals(expectedVars,
                    sa.getMaybeProducedBindings(queryRoot));

        }

        // whereClause
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("x"));
            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("b"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(
                    expectedVars,
                    sa.getMaybeProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

    }
    
    /**
     * Unit test for static analysis of the MUST and MIGHT bound variables for a
     * query involving a select expression which depends on a variable which is
     * not definitely bound. 
     * 
     * <pre>
     * SELECT ?a ?b (?a + ?b as ?c) {
     *    ?x rdf:type foaf:Person .
     *    ?x :age ?a .
     *    OPTIONAL {
     *       ?x :grade ?b .
     *    }
     * }
     * </pre>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/430
     * 
     *      TODO Extend this test to cover the case where <code>b</code> is an
     *      exogenously bound variable. This does not change the MUST/MIGHT
     *      analysis of the query.
     */
    public void test_static_analysis_projection_05() {

        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
        @SuppressWarnings("rawtypes")
        final IV age = makeIV(new URIImpl("http://example.org/age"));
        @SuppressWarnings("rawtypes")
        final IV grade = makeIV(new URIImpl("http://example.org/grade"));

        // The source AST.
        QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, optionalGroup;
        {

            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                projection.addProjectionVar(new VarNode("a"));
                projection.addProjectionVar(new VarNode("b"));
                projection.addProjectionExpression(new AssignmentNode(
                        new VarNode("c"), FunctionNode.binary(
                                FunctionRegistry.DIVIDE, new VarNode("a"),
                                new VarNode("b"))));
            }

            // WHERE clause.
            {
                whereClause = new JoinGroupNode();
                queryRoot.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(new VarNode("x"),
                                new ConstantNode(rdfType), new ConstantNode(
                                        foafPerson)));

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(age), new VarNode("a")));

                optionalGroup = new JoinGroupNode(true/* optional */);
                whereClause.addChild(optionalGroup);

                optionalGroup.addChild(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(grade), new VarNode(
                                "b")));
            }

        }

        /*
         * Note: Since it involves function nodes, we need to generate the value
         * expressions in order to run this test.
         */
        {

            final IBindingSet[] bindingSets = new IBindingSet[] {};
            
            final ASTContainer astContainer = new ASTContainer(queryRoot);
            
            final AST2BOpContext context = new AST2BOpContext(astContainer,
                    store);

            queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer()
                    .optimize(context, new QueryNodeWithBindingSet(queryRoot, bindingSets))
                    .getQueryNode();

        }
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        final Set<IVariable<?>> exogenousVars = new LinkedHashSet<IVariable<?>>();

        // QueryRoot
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));

            assertEquals(expectedVars,
                    sa.getDefinitelyProducedBindings(queryRoot));

            expectedVars.add(Var.var("b"));
            expectedVars.add(Var.var("c"));

            assertEquals(expectedVars,
                    sa.getMaybeProducedBindings(queryRoot));

        }

        // whereClause
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("x"));
            expectedVars.add(Var.var("a"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            expectedVars.add(Var.var("b"));

            assertEquals(
                    expectedVars,
                    sa.getMaybeProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

        // optionalGroup
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("x"));
            expectedVars.add(Var.var("b"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(optionalGroup,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(
                    expectedVars,
                    sa.getMaybeProducedBindings(optionalGroup,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

    }

    /**
     * Unit test for static analysis of the MUST and MIGHT bound variables for a
     * subquery involving aggregation and a select expression ("The result of an
     * Aggregate which returns an error, is an error, but the SELECT expression
     * result of projecting an error is unbound").
     * 
     * <pre>
     * SELECT ?a ?b (?a/?b as ?c) {
     *    ?x rdf:type foaf:Person .
     *    ?x :age ?a .
     *    ?x :grade ?b .
     * }
     * GROUP BY ?b
     * </pre>
     * 
     * In this query, it is presumed that <code>?b</code> is the number of years
     * of school and will be ZERO in the data for people who have not yet begun
     * formal schooling. Hence, the select expression can evaluate to an error
     * (divide by zero).
     * <p>
     * Since this is an aggregation query, the solution will still be reported
     * with an unbound value for <code>?c</code>. This illustrates the general
     * principle that we can not assume that a select expression based on
     * definitely bound variables will itself be definitely bound.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/430
     * 
     *      TODO Extend this test to cover the case where <code>b</code> is an
     *      exogenously bound variable. Does this change the MUST/MIGHT analysis
     *      of the query?
     */
    public void test_static_analysis_projection_06() {

        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
        @SuppressWarnings("rawtypes")
        final IV age = makeIV(new URIImpl("http://example.org/age"));
        @SuppressWarnings("rawtypes")
        final IV grade = makeIV(new URIImpl("http://example.org/grade"));

        // The source AST.
        QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause;
        {

            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                projection.addProjectionVar(new VarNode("a"));
                projection.addProjectionVar(new VarNode("b"));
                projection.addProjectionExpression(new AssignmentNode(
                        new VarNode("c"), FunctionNode.binary(
                                FunctionRegistry.DIVIDE, new VarNode("a"),
                                new VarNode("b"))));
            }

            // WHERE clause.
            {
                whereClause = new JoinGroupNode();
                queryRoot.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(new VarNode("x"),
                                new ConstantNode(rdfType), new ConstantNode(
                                        foafPerson)));

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(age), new VarNode("a")));

                whereClause.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(grade), new VarNode("b")));
            }

            // GROUP BY
            {
                final GroupByNode groupBy = new GroupByNode();
                queryRoot.setGroupBy(groupBy);
                groupBy.addGroupByVar(new VarNode("b"));
            }

        }

        /*
         * Note: Since it involves function nodes, we need to generate the value
         * expressions in order to run this test.
         */
        {

            final IBindingSet[] bindingSets = new IBindingSet[] {};
            
            final ASTContainer astContainer = new ASTContainer(queryRoot);
            
            final AST2BOpContext context = new AST2BOpContext(astContainer,
                    store);

            queryRoot = (QueryRoot) new ASTSetValueExpressionsOptimizer()
                    .optimize(context, new QueryNodeWithBindingSet(queryRoot, bindingSets))
                    .getQueryNode();

        }
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        final Set<IVariable<?>> exogenousVars = new LinkedHashSet<IVariable<?>>();

        // QueryRoot
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("b"));

            assertEquals(expectedVars,
                    sa.getDefinitelyProducedBindings(queryRoot));

            expectedVars.add(Var.var("c"));

            assertEquals(expectedVars,
                    sa.getMaybeProducedBindings(queryRoot));

        }

        // whereClause
        {

            final LinkedHashSet<IVariable<?>> expectedVars = new LinkedHashSet<IVariable<?>>();

            expectedVars.add(Var.var("x"));
            expectedVars.add(Var.var("a"));
            expectedVars.add(Var.var("b"));

            assertEquals(
                    expectedVars,
                    sa.getDefinitelyProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

            assertEquals(
                    expectedVars,
                    sa.getMaybeProducedBindings(whereClause,
                            new LinkedHashSet<IVariable<?>>(), true/* recursive */));

        }

    }

    /**
     * Unit test for locating a reference go a {@link GraphPatternGroup} which
     * appears as an annotation of another node. This covers {@link UnionNode}
     * and {@link JoinGroupNode} when appearing as children of a
     * {@link QueryRoot}'s whereClause.
     * 
     * <pre>
     * SELECT ?a ?b {
     *    ?x rdf:type foaf:Person .
     *    {
     *       ?x :age ?a
     *    } UNION {
     *       ?x :grade ?b
     *    }
     * }
     * </pre>
     */
    public void test_findParent_01() {

        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
        @SuppressWarnings("rawtypes")
        final IV age = makeIV(new URIImpl("http://example.org/age"));
        @SuppressWarnings("rawtypes")
        final IV grade = makeIV(new URIImpl("http://example.org/grade"));

        // The source AST.
        QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, joinGroup1, joinGroup2;
        final UnionNode unionNode;
        {

            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                projection.addProjectionVar(new VarNode("a"));
                projection.addProjectionVar(new VarNode("b"));
            }

            // WHERE clause.
            {
                whereClause = new JoinGroupNode();
                queryRoot.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(new VarNode("x"),
                                new ConstantNode(rdfType), new ConstantNode(
                                        foafPerson)));

                unionNode = new UnionNode();
                whereClause.addChild(unionNode);

                joinGroup1 = new JoinGroupNode();
                unionNode.addChild(joinGroup1);

                joinGroup1.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(age), new VarNode("a")));

                joinGroup2 = new JoinGroupNode();
                unionNode.addChild(joinGroup2);
                
                joinGroup2.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(grade), new VarNode("b")));
            }

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        assertTrue(unionNode == sa.findParent(joinGroup1));
        assertTrue(unionNode == sa.findParent(joinGroup2));
        assertTrue(whereClause == sa.findParent(unionNode));
        assertTrue(queryRoot == sa.findParent(whereClause));
        
    }
    
    /**
     * This verifies the ability to locate the parent of a graph group in a
     * {@link SubqueryRoot}.
     * 
     * <pre>
     * SELECT ?a ?b {
     *    ?x rdf:type foaf:Person .
     *    {
     *       SELECT ?x ?a ?b {
     *         ?x :age ?a
     *         ?x :grade ?b
     *       }
     *    }
     * }
     * </pre>
     */
    public void test_findParent_02() {

        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
        @SuppressWarnings("rawtypes")
        final IV age = makeIV(new URIImpl("http://example.org/age"));
        @SuppressWarnings("rawtypes")
        final IV grade = makeIV(new URIImpl("http://example.org/grade"));

        // The source AST.
        QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, subqueryWhereClause;
        final SubqueryRoot subqueryRoot;
        {

            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                projection.addProjectionVar(new VarNode("a"));
                projection.addProjectionVar(new VarNode("b"));
            }

            // WHERE clause.
            {
                whereClause = new JoinGroupNode();
                queryRoot.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(new VarNode("x"),
                                new ConstantNode(rdfType), new ConstantNode(
                                        foafPerson)));

                subqueryRoot = new SubqueryRoot(QueryType.SELECT);
                whereClause.addChild(subqueryRoot);

                subqueryWhereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(subqueryWhereClause);

                subqueryWhereClause.addChild(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(age), new VarNode(
                                "a")));

                subqueryWhereClause.addChild(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(grade), new VarNode(
                                "b")));
            }

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        assertTrue(subqueryRoot == sa.findParent(subqueryWhereClause));
        assertTrue(whereClause == subqueryRoot.getParent());

    }
    
    /**
     * This verifies the ability to locate the parent of a graph group in a
     * {@link NamedSubqueryRoot}.
     * 
     * <pre>
     * SELECT ?a ?b {
     *    WITH {
     *       SELECT ?x ?a ?b {
     *         ?x :age ?a
     *         ?x :grade ?b
     *       }
     *    } as %set1
     *    ?x rdf:type foaf:Person .
     *    INCLUDE %set1 .
     * }
     * </pre>
     */
    public void test_findParent_03() {

        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
        @SuppressWarnings("rawtypes")
        final IV age = makeIV(new URIImpl("http://example.org/age"));
        @SuppressWarnings("rawtypes")
        final IV grade = makeIV(new URIImpl("http://example.org/grade"));

        // The source AST.
        QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, subqueryWhereClause;
        final NamedSubqueryRoot subqueryRoot;
        final NamedSubqueryInclude include;
        final String namedSet = "set1";
        {
            
            // Named subquery.
            {
                subqueryRoot = new NamedSubqueryRoot(QueryType.SELECT, namedSet);

                subqueryWhereClause = new JoinGroupNode();
                subqueryRoot.setWhereClause(subqueryWhereClause);

                subqueryWhereClause.addChild(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(age), new VarNode(
                                "a")));

                subqueryWhereClause.addChild(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(grade), new VarNode(
                                "b")));
                
                queryRoot.getNamedSubqueriesNotNull().add(subqueryRoot);
                
            }
            
            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                projection.addProjectionVar(new VarNode("a"));
                projection.addProjectionVar(new VarNode("b"));
            }

            // WHERE clause.
            {
                whereClause = new JoinGroupNode();
                queryRoot.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(new VarNode("x"),
                                new ConstantNode(rdfType), new ConstantNode(
                                        foafPerson)));
                
                whereClause.addChild(include = new NamedSubqueryInclude(
                        namedSet));

            }

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        assertTrue(subqueryRoot == sa.findParent(subqueryWhereClause));
        assertTrue(whereClause == include.getParent());

    }
    
    /**
     * Unit test for locating a reference go a {@link GraphPatternGroup} which
     * appears as an annotation of another node. This covers {@link UnionNode}
     * and {@link JoinGroupNode} when appearing as children of a
     * {@link QueryRoot}'s whereClause.
     * 
     * <pre>
     * SELECT ?a ?b {
     *    ?x rdf:type foaf:Person .
     *    SERVICE {
     *       ?x :age ?a
     *    }
     *    FILTER {
     *       EXISTS {?x :grade ?b}
     *    }
     * }
     * </pre>
     */
    public void test_findParent_04() {

        @SuppressWarnings("rawtypes")
        final IV rdfType = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV foafPerson = makeIV(FOAFVocabularyDecl.Person);
        @SuppressWarnings("rawtypes")
        final IV age = makeIV(new URIImpl("http://example.org/age"));
        @SuppressWarnings("rawtypes")
        final IV grade = makeIV(new URIImpl("http://example.org/grade"));
        @SuppressWarnings("rawtypes")
        final IV serviceUri = makeIV(new URIImpl("http://example.org/service"));

        // The source AST.
        QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        final JoinGroupNode whereClause, serviceGroup, existsGroup;
        final ServiceNode serviceNode;
        final FilterNode filterNode;
        {

            // Top-level projection
            {
                final ProjectionNode projection = new ProjectionNode();
                queryRoot.setProjection(projection);

                projection.addProjectionVar(new VarNode("a"));
                projection.addProjectionVar(new VarNode("b"));
            }

            // WHERE clause.
            {
                whereClause = new JoinGroupNode();
                queryRoot.setWhereClause(whereClause);

                whereClause
                        .addChild(new StatementPatternNode(new VarNode("x"),
                                new ConstantNode(rdfType), new ConstantNode(
                                        foafPerson)));

                serviceGroup = new JoinGroupNode();
                serviceGroup.addChild(new StatementPatternNode(
                        new VarNode("x"), new ConstantNode(age), new VarNode(
                                "a")));
                serviceNode = new ServiceNode(new ConstantNode(serviceUri),
                        serviceGroup);
                whereClause.addChild(serviceNode);

                existsGroup = new JoinGroupNode();
                existsGroup.addChild(new StatementPatternNode(new VarNode("x"),
                        new ConstantNode(grade), new VarNode("b")));
                filterNode = new FilterNode(new ExistsNode(new VarNode(
                        "anon-var-1"), existsGroup));
                whereClause.addChild(filterNode);
                
            }

        }

        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
        
        assertTrue(serviceNode == sa.findParent(serviceGroup));
        assertTrue(filterNode == sa.findParent(existsGroup));
        assertTrue(queryRoot == sa.findParent(whereClause));
        
    }

//    /**
//     * Unit test for whether or not a variable is "in-scope" in some part of the
//     * AST.
//     * 
//     * <pre>
//     * PREFIX :    <http://example/>
//     * SELECT ?v
//     * { 
//     *     :x :p ?v . 
//     *     FILTER(?v = 1) .
//     * }
//     * </pre>
//     * 
//     * The variable <code>?v</code> is in scope in the outer group and the
//     * filter.
//     * 
//     * @see http://www.w3.org/TR/sparql11-query/#variableScope
//     */
//    public void test_static_analysis_inScope_01() {
//
//        @SuppressWarnings("rawtypes")
//        final IV x = makeIV(new URIImpl("http://example.org/x"));
//        @SuppressWarnings("rawtypes")
//        final IV p = makeIV(new URIImpl("http://example.org/p"));
//        @SuppressWarnings("rawtypes")
//        final IV one = makeIV(new LiteralImpl("1", XSD.INTEGER));
//
//        // The source AST.
//        QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
//        final JoinGroupNode whereClause;
//        final FilterNode filterNode;
//        {
//
//            // Top-level projection
//            {
//                final ProjectionNode projection = new ProjectionNode();
//                queryRoot.setProjection(projection);
//
//                projection.addProjectionVar(new VarNode("v"));
//            }
//
//            // WHERE clause.
//            {
//                whereClause = new JoinGroupNode();
//                queryRoot.setWhereClause(whereClause);
//
//                whereClause.addChild(new StatementPatternNode(new ConstantNode(
//                        x), new ConstantNode(p), new VarNode("v")));
//
//                filterNode = new FilterNode(FunctionNode.binary(
//                        FunctionRegistry.EQ, new VarNode("v"),
//                        new ConstantNode(one)));
//                whereClause.addChild(filterNode);
//                
//            }
//
//        }
//
//        final StaticAnalysis sa = new StaticAnalysis(queryRoot);
//
//        {
//
//            final Set<IVariable<?>> expected = new LinkedHashSet<IVariable<?>>();
//
//            expected.add(Var.var("v"));
//            
//            assertEquals(expected, sa.getInScopeVariables(filterNode,
//                    new LinkedHashSet<IVariable<?>>()));
//            
//        }
//        
//    }
//
//    /**
//     * <code>?v</code> is also in scope when it appears in an OPTIONAL group
//     * 
//     * <pre>
//     * PREFIX :    <http://example/>
//     * SELECT ?v
//     * { 
//     *     :x :p ?v .
//     *     OPTIONAL { 
//     *        FILTER(?v = 1) .
//     *     }
//     * }
//     * </pre>
//     */
//   public void test_static_analysis_inScope_02() {
//
//   }
//
//    /**
//     * This test examines a DAWG/TCK query based on a badly formed left join
//     * pattern:
//     * 
//     * <pre>
//     * PREFIX :    <http://example/>
//     * SELECT ?v ?w ?v2
//     * { 
//     *     :x :p ?v . 
//     *     { :x :q ?w 
//     *       OPTIONAL {  :x :p ?v2 FILTER(?v = 1) }
//     *     }
//     * }
//     * </pre>
//     * 
//     * The variable <code>?v</code> is in scope in the outer group, but it is
//     * not in scope in the filter.
//     * 
//     * TODO What about when the optional group has ?v in the BPG?
//     * 
//     * TODO What about when the optional group has ?v and the parent group 
//     * also has ?v?
//     * 
//     * TODO Examples with more variables.
//     */
//    public void test_static_analysis_inScope_03() {
//
//        fail("write tests");
//
//    }
//
//    /**
//     * 
//     * TODO The test suite should cover all of the bottom-up examples so we can
//     * work out what the right behavior of this method is in each case. It
//     * should also cover the EXISTS graph pattern, SERVICE graph pattern, and
//     * subquery graph patterns as those are not linked in the standard
//     * parent-child hierarchy (or write a separate test suite for finding those
//     * things, maybe for BOpUtility).
//     */
//    public void test_static_analysis_inScope_xxx() {
//
//        fail("write tests");
//
//    }
    
}
