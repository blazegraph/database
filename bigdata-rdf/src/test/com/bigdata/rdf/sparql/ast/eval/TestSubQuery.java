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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import org.openrdf.model.Value;

import com.bigdata.bop.BOpUtility;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSparql11SubqueryOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.TestASTSparql11SubqueryOptimizer;

/**
 * Data driven test suite.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSubQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     *
     */
    public TestSubQuery() {
    }

    /**
     * @param name
     */
    public TestSubQuery(final String name) {
        super(name);
    }

    /**
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT *
     * WHERE {
     *      ?s :p ?o .
     *      {
     *         SELECT ?s { ?s a :ty } ORDER BY ?s LIMIT 3
     *      }
     * }
     * </pre>
     *
     * mroycsi wrote: Based on sparql bottom up evaluation, the subquery will
     * return s1,s2,s3 as the solutions for ?s. Joined with the ?s :p ?o, you
     * should only get the statements where ?s is s1,s2,s3.
     * <p>
     * I haven't debugged bigdata so I don't know exactly what it is doing, but
     * it seems that currently with the bigdata evaluation, for each solution
     * produced from ?s :p ?o, the subquery is run, and it seems that the ?s
     * binding in the subquery is getting constrained by the ?s from the inbound
     * solution, so results of the subquery are not always s1,s2,s3, depending
     * on the inbound solution.
     * <p>
     * thompsonbry wrote: Normally bottom up evaluation only differs when you
     * are missing a shared variable such that the bindings for variables having
     * the same name are actually not correlated.
     * <P>
     * This is a bit of an odd case with an interaction between the order/limit
     * and the as-bound evaluation which leads to the "wrong" result. We
     * probably do not want to always do bottom up evaluation for a subquery
     * (e.g., by lifting it into a named subquery). Are you suggesting that
     * there is this special case which needs to be recognized where the
     * subquery MUST be evaluated first because the order by/limit combination
     * means that the results of the outer query joined with the inner query
     * could be different in this case?
     * <p>
     * mroycsi wrote: This is [a] pattern that is well known and commonly used
     * with sparql 1.1 subqueries. It is definitely a case where the subquery
     * needs to be evaluated first due to the limit clause. The order clause
     * probably doesn't matter if there isn't a limit since all the results are
     * just joined, so order doesn't matter till the solution gets to the order
     * by operations.
     * <p>
     * thompsonbry wrote: Ok. ORDER BY by itself does not matter and neither
     * does LIMIT by itself. But if you have both it matters and we need to run
     * the subquery first.
     * <p>
     * Note: This is handled by {@link ASTSparql11SubqueryOptimizer}.
     *
     * @see TestASTSparql11SubqueryOptimizer#test_subSelectWithLimitAndOrderBy()
     */
    public void test_sparql_subquery_limiting_resource_pattern() throws Exception {

        new TestHelper("subquery-lpr").runTest();

    }

    /**
     * Unit test of a SPARQL 1.1 subquery with a SLICE on the subquery.
     */
    public void test_sparql_subquery_slice_01() throws Exception {

        new TestHelper("subquery-slice-01").runTest();

    }

    /**
     * Simple Sub-Select unit test
     *
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT ?x ?o
     *  WHERE {
     *     ?x rdfs:label ?o .
     *     {
     *       SELECT ?x WHERE {?x rdf:type foaf:Person}
     *     }
     * }
     * </pre>
     */
    public void test_sparql_subselect() throws Exception {

        new TestHelper("sparql-subselect").runTest();

    }

    /**
     * A unit test from the Sesame 2.5 TCK.
     *
     * <pre>
     * SELECT * { SELECT * { ?s ?p ?o } }
     * </pre>
     */
    public void test_sparql11_subquery_02() throws Exception {

        new TestHelper(
                "sparql11-subquery-02", // testURI,
                "sparql11-subquery-02.rq",// queryFileURL
                "sparql11-subquery-02.ttl",// dataFileURL
                "sparql11-subquery-02.srx"// resultFileURL
                ).runTest();

    }

    /**
     * A unit test from the Sesame 2.5 TCK.
     *
     * <pre>
     * SELECT (count(*) as ?count)
     * WHERE {
     *     { SELECT ?s ?p ?o WHERE { ?s ?p ?o } }
     * }
     * </pre>
     */
    public void test_sparql11_count_subquery_01() throws Exception {

        new TestHelper(
                "sparql11-count-subquery-01", // testURI,
                "sparql11-count-subquery-01.rq",// queryFileURL
                "sparql11-count-subquery-01.ttl",// dataFileURL
                "sparql11-count-subquery-01.srx"// resultFileURL
                ).runTest();

    }

    /**
     * Test that only projected variables are included in subquery results.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT ?s ?x
     * WHERE {
     *      {
     *         SELECT ?s ?x { ?s :p ?x }
     *      }
     *      {
     *         SELECT ?s ?fake1 ?fake2 { ?x :q ?s . LET (?fake1 := 1) . LET (?fake2 := 2) . }
     *      }
     * }
     * </pre>
     */
    public void test_sparql11_subquery_scope() throws Exception {

        new TestHelper("sparql11-subquery-scope").runTest();

    }

    /**
     * In this test variant, the FILTER winds up attached to a
     * {@link NamedSubqueryRoot} (there are no shared variables projected out of
     * the sub-select) and does not require RDF {@link Value} materialization.
     * <p>
     * Note: The sub-select explicitly annotated using
     * {@link QueryHints#RUN_ONCE} to ensure that it gets lifted out as a
     * {@link NamedSubqueryRoot}, but this query does not have any shared
     * variables so the sub-select would be lifted out anyway.
     * 
     * <pre>
     * select distinct ?s 
     * where
     * {
     *         ?s ?p ?o.
     *         {
     *                 SELECT ?ps WHERE
     *                 { 
     *                         hint:SubQuery hint:runOnce true.
     *                         ?ps a <http://www.example.org/schema/Person> .
     *                 }
     *                 limit 1
     *         }
     *         filter (?s = ?ps)
     * }
     * </pre>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/796"
     *      >Filter assigned to sub-query by query generator is dropped from
     *      evaluation</a>
     */
    public void test_sparql11_subquery_filter_01()
            throws Exception {

        final TestHelper h = new TestHelper(
                "sparql11-subselect-filter-01", // testURI,
                "sparql11-subselect-filter-01.rq",// queryFileURL
                "sparql11-subselect-filter-01.nt",// dataFileURL
                "sparql11-subselect-filter-01.srx"// resultFileURL
                );

        // Run test.
        h.runTest();
        
        // Make sure that this query used a NamedSubqueryRoot.
        assertTrue(BOpUtility.visitAll(h.getASTContainer().getOptimizedAST(),
                NamedSubqueryRoot.class).hasNext());
        
    }

    /**
     * Variant where the FILTER requires RDF Value materialization and the
     * sub-select is lifted out as a named subquery.
     * 
     * <pre>
     * select distinct ?s 
     * where
     * {
     *         ?s ?p ?o.
     *         {
     *                 SELECT ?ps WHERE
     *                 { 
     *                         ?ps a <http://www.example.org/schema/Person> .
     *                 }
     *                 limit 1
     *         }
     *         filter (str(?s) = str(?ps))
     * }
     * </pre>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/796"
     *      >Filter assigned to sub-query by query generator is dropped from
     *      evaluation</a>
     */
    public void test_sparql11_subquery_filter_01b()
            throws Exception {

        final TestHelper h = new TestHelper(
                "sparql11-subselect-filter-01b", // testURI,
                "sparql11-subselect-filter-01b.rq",// queryFileURL
                "sparql11-subselect-filter-01.nt",// dataFileURL
                "sparql11-subselect-filter-01.srx"// resultFileURL
                );

        // Run test.
        h.runTest();

        // Make sure that this query used a NamedSubqueryRoot.
        assertTrue(BOpUtility.visitAll(h.getASTContainer().getOptimizedAST(),
                NamedSubqueryRoot.class).hasNext());
        
    }

}
