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
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import junit.framework.TestResult;
import junit.framework.TestSuite;

import org.apache.log4j.Logger;

import com.bigdata.BigdataStatics;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTBottomUpOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSimpleOptionalOptimizer;

/**
 * Test driver for debugging Sesame or DAWG manifest tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestTCK extends AbstractDataDrivenSPARQLTestCase {

    private static final Logger log = Logger.getLogger(TestTCK.class);
    
    /**
     * 
     */
    public TestTCK() {
    }

    /**
     * @param name
     */
    public TestTCK(String name) {
        super(name);
    }

    /**
     * This is not a DAWG test. There is no data for the aggregation. Sesame is
     * expecting a solution set consisting of a single solution with
     * 0^^xsd:integer. We are producing an empty solution set, which is the
     * correct result. The expected answer has been modified here so the test
     * will pass, but it will still fail in the Sesame test suite until they fix
     * the issue in their test manifest.
     * <p>
     * See http://www.openrdf.org/issues/browse/SES-884 (Aggregation with an
     * solution set as input should produce an empty solution as output)
     */
    public void test_sparql11_sum_02() throws Exception {

        new TestHelper(
                "sparql11-sum-02", // testURI,
                "sparql11-sum-02.rq",// queryFileURL
                "sparql11-sum-02.ttl",// dataFileURL
                "sparql11-sum-02.srx"// resultFileURL
                ).runTest();

    }

    /**
     * This is not a DAWG test. The query is simple enough. However, one of the
     * values which is bound on <code>?x</code> is a plain literal rather than a
     * numeric datatype literal.
     * 
     * <pre>
     * PREFIX : <http://example.org/>
     * SELECT (SUM(?x) AS ?total)
     * WHERE {
     *   ?a :p ?x.
     * }
     * GROUP BY ?a
     * </pre>
     * 
     * <pre>
     * @prefix : <http://example.org/> .
     * 
     * :a :p "1" .
     * :a :p 1 .
     * :b :p 3 .
     * :b :p 4 .
     * </pre>
     * 
     * The test as present in Sesame 2.5 is wrong. I've filed a bug report. The
     * correct solution should be two groups. The group for <code>:a</code>
     * should have an unbound value for <code>?x</code>. The group for
     * <code>:b</code> should have a bound value of
     * <code>"7"^^xsd:integer</code> for <code>?x</code>.
     * <p>
     * Note: This test will continue to fail in the TCK until Sesame resolve the
     * issue, which will require both updating their test suite and also
     * updating their error handling logic to conform with the spec.
     * 
     * @see http://www.openrdf.org/issues/browse/SES-862
     */
    public void test_sparql11_sum_03() throws Exception {

        new TestHelper(
                "sparql11-sum-03", // testURI,
                "sparql11-sum-03.rq",// queryFileURL
                "sparql11-sum-03.ttl",// dataFileURL
                "sparql11-sum-03.srx"// resultFileURL
                ).runTest();

    }

    /**
     * This is not a DAWG test. There is no data for the aggregation. Sesame is
     * expecting a solution set consisting of a single solution with
     * 0^^xsd:integer. We are producing an empty solution set.
     */
    public void test_sparql11_count_03() throws Exception {

        new TestHelper(
                "sparql11-count-03", // testURI,
                "sparql11-count-03.rq",// queryFileURL
                "sparql11-count-03.ttl",// dataFileURL
                "sparql11-count-03.srx"// resultFileURL
                ).runTest();

    }

    /**
     * This is not a DAWG test. We produce 10^^xsd:decimal while openrdf expects
     * 10.0^^xsd:decimal.
     */
    public void test_sparql11_sum_04() throws Exception {

        new TestHelper(
                "sparql11-sum-04", // testURI,
                "sparql11-sum-04.rq",// queryFileURL
                "sparql11-sum-04.ttl",// dataFileURL
                "sparql11-sum-04.srx"// resultFileURL
                ).runTest();

    }

    /**
     * This is not a DAWG test.
     */
    public void test_sparql11_in_02() throws Exception {
        
        new TestHelper(
                "sparql11-in-02", // testURI,
                "sparql11-in-02.rq",// queryFileURL
                "sparql11-in-02.ttl",// dataFileURL
                "sparql11-in-02.srx"// resultFileURL
                ).runTest();
        
    }

//    /**
//     * TODO I can not figure out why these "dataset" tests fail.
//     */
//    public void test_dataset_01() throws Exception {
//
//        new TestHelper(
//                "dataset-01", // testURI,
//                "dataset-01.rq",// queryFileURL
//                "data-g1.ttl",  // dataFileURL
//                "dataset-01.ttl"// resultFileURL
//                ).runTest();
//
//    }

    /**
     * Test effective boolean value - optional.
     * 
     * <pre>
     * PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>
     * PREFIX  : <http://example.org/ns#>
     * SELECT  ?a
     * WHERE
     *     { ?a :p ?v . 
     *       OPTIONAL
     *         { ?a :q ?w } . 
     *       FILTER (?w) .
     *     }
     * </pre>
     * 
     * @see ASTSimpleOptionalOptimizer
     */
    public void test_sparql_bev_5() throws Exception {

        new TestHelper(
                "bev-5", // testURI,
                "bev-5.rq",// queryFileURL
                "bev-5.ttl",// dataFileURL
                "bev-5-result.ttl"// resultFileURL
                ).runTest();

    }

    /**
     * <code>Nested Optionals - 1</code>. Badly designed left join with TWO
     * optionals.
     * 
     * <pre>
     * SELECT *
     * { 
     *     :x1 :p ?v .
     *     OPTIONAL
     *     {
     *       :x3 :q ?w .
     *       OPTIONAL { :x2 :p ?v }
     *     }
     * }
     * </pre>
     * 
     * Note: Because this is a badly designed left join, it is translated into a
     * named subquery. However, the translated query will fail if the "OPTIONAL"
     * semantics are not being attached to the INCLUDE.
     * 
     * @see ASTBottomUpOptimizer
     */
    public void test_two_nested_opt() throws Exception {

        new TestHelper(
                "two-nested-opt", // testURI,
                "two-nested-opt.rq",// queryFileURL
                "two-nested-opt.ttl",// dataFileURL
                "two-nested-opt.srx"// resultFileURL
                ).runTest();

    }
    
    /**
     * This is the same query, except we are running the rewritten version of
     * the query (that is, the version that we have to produce for the query
     * above with the badly designed left join).
     * 
     */
    public void test_two_nested_opt2() throws Exception {

        new TestHelper(
                "two-nested-opt", // testURI,
                "two-nested-opt2.rq",// queryFileURL
                "two-nested-opt.ttl",// dataFileURL
                "two-nested-opt.srx"// resultFileURL
                ).runTest();

    }

    /**
     * This case is not a problem. It provides a contrast to
     * {@link #test_filter_nested_2()}
     */
    public void test_filter_nested_1() throws Exception {

        new TestHelper(
                "filter-nested-1", // testURI,
                "filter-nested-1.rq",// queryFileURL
                "filter-nested-1.ttl",// dataFileURL 
                "filter-nested-1.srx"// resultFileURL
                ).runTest();

    }

    /**
     * Filter-nested - 2 (Filter on variable ?v which is not in scope)
     * 
     * <pre>
     * SELECT ?v
     * { :x :p ?v . { FILTER(?v = 1) } }
     * </pre>
     * 
     * @see ASTBottomUpOptimizer
     */
    public void test_filter_nested_2() throws Exception {

        new TestHelper(
                "filter-nested-2", // testURI,
                "filter-nested-2.rq",// queryFileURL
                "filter-nested-2.ttl",// dataFileURL 
                "filter-nested-2.srx"// resultFileURL
                ).runTest();

    }

    /**
     * Badly designed left join pattern plus a FILTER with a variable which is
     * not in scope.
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
     * @see ASTBottomUpOptimizer
     */
    public void test_filter_scope_1() throws Exception {

        new TestHelper(
                "filter-scope-1", // testURI,
                "filter-scope-1.rq",// queryFileURL
                "filter-scope-1.ttl",// dataFileURL 
                "filter-scope-1.srx"// resultFileURL
                ).runTest();

    }

    /**
     * Classic badly designed left join.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * 
     * SELECT *
     * { 
     *   ?X  :name "paul"
     *   {?Y :name "george" . OPTIONAL { ?X :email ?Z } }
     * }
     * 
     * </pre>
     * 
     * @see ASTBottomUpOptimizer
     */
    public void test_var_scope_join_1() throws Exception {

        new TestHelper(
                "var-scope-join-1", // testURI,
                "var-scope-join-1.rq",// queryFileURL
                "var-scope-join-1.ttl",// dataFileURL
                "var-scope-join-1.srx"// resultFileURL
                ).runTest();

    }
    
    /**
     * Complex optional semantics: 1.
     * 
     * <pre>
     * Complex optional: LeftJoin(LeftJoin(BGP(..),{..}),Join(BGP(..),Union(..,..)))
     * </pre>
     * 
     * <pre>
     * PREFIX  foaf:   <http://xmlns.com/foaf/0.1/>
     * SELECT ?person ?nick ?page ?img ?name ?firstN
     * { 
     *     ?person foaf:nick ?nick
     *     OPTIONAL { ?person foaf:isPrimaryTopicOf ?page } 
     *     OPTIONAL { 
     *         ?person foaf:name ?name 
     *         { ?person foaf:depiction ?img } UNION 
     *         { ?person foaf:firstName ?firstN } 
     *     } FILTER ( bound(?page) || bound(?img) || bound(?firstN) ) 
     * }
     * </pre>
     */
    public void test_opt_complex_1() throws Exception {

        new TestHelper(
                "opt-complex-1", // testURI,
                "opt-complex-1.rq",// queryFileURL
                "opt-complex-1.ttl",// dataFileURL
                "opt-complex-1-result.ttl"// resultFileURL
                ).runTest();

    }

    /**
     * Complex optional semantics: 2 (dawg-optional-complex-2).
     * 
     * <pre>
     * Complex optional: LeftJoin(Join(BGP(..),Graph(var,{..})),Union(..,..))
     * </pre>
     * 
     * <pre>
     * PREFIX  foaf:   <http://xmlns.com/foaf/0.1/>
     * PREFIX    ex:   <http://example.org/things#>
     * SELECT ?id ?ssn
     * WHERE 
     * { 
     *     ?person 
     *         a foaf:Person;
     *         foaf:name ?name . 
     *     GRAPH ?x { 
     *         [] foaf:name ?name;
     *            foaf:nick ?nick
     *     } 
     *     OPTIONAL { 
     *         { ?person ex:empId ?id } UNION { ?person ex:ssn ?ssn } 
     *     } 
     * }
     * </pre>
     * 
     * <pre>
     * :dawg-optional-complex-2 a mf:QueryEvaluationTest ;
     *     mf:name    "Complex optional semantics: 2" ;
     *     rdfs:comment
     *             "Complex optional: LeftJoin(Join(BGP(..),Graph(var,{..})),Union(..,..))" ;
     *     dawgt:approval dawgt:Approved ;
     *     dawgt:approvedBy <http://lists.w3.org/Archives/Public/public-rdf-dawg/2007JulSep/att-0096/21-dawg-minutes.html> ;
     *     mf:action
     *     [ qt:query  <q-opt-complex-2.rq> ;
     *       qt:graphData <complex-data-1.ttl>;
     *       qt:data   <complex-data-2.ttl> ] ;
     *     mf:result  <result-opt-complex-2.ttl> .
     * </pre>
     */
    public void test_opt_complex_2() throws Exception {

        new TestHelper(
                "opt-complex-2", // testURI,
                "opt-complex-2.rq",// queryFileURL
                new String[] { "opt-complex-2-data.ttl",
                        "opt-complex-2-graphData.ttl" },// dataFileURL
               "opt-complex-2-result.ttl"// resultFileURL
                ).runTest();

    }

    /**
     * Reification of the default graph (dawg-construct-reification-1).
     * 
     * <pre>
     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
     * PREFIX  foaf:       <http://xmlns.com/foaf/0.1/>
     * 
     * CONSTRUCT { [ rdf:subject ?s ;
     *               rdf:predicate ?p ;
     *               rdf:object ?o ] . }
     * WHERE {
     *   ?s ?p ?o .
     * }
     * </pre>
     */
    public void test_construct_reif_1() throws Exception {

        new TestHelper(
                "construct-reif-1", // testURI,
                "construct-reif-1.rq",// queryFileURL
                "construct-reif-1.ttl",// dataFileURL
                "construct-reif-1-result.ttl"// resultFileURL
                ).runTest();

    }

    /**
     * <pre>
     * SELECT
     * WHERE { ?a ?a ?b . }
     * </pre>
     * 
     * Note: This will drag in the SPORelation on the cluster if the backchain
     * access path for <code>(?foo rdf:type rdfs:Resource)</code> is enabled. I
     * have explicitly disabled the backchainers in scale-out so that people
     * will not observe this problem. This was done in the Sail, which is where
     * they are configured.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/32
     */
    public void test_dawg_triple_pattern_03() throws Exception {

        new TestHelper(
                "dawg-tp-03", // testURI,
                "dawg-tp-03.rq",// queryFileURL
                "dawg-tp-03.ttl",// dataFileURL
                "dawg-tp-03-result.ttl"// resultFileURL
                ).runTest();

    }

    /**
     * <pre>
     * PREFIX foaf:       <http://xmlns.com/foaf/0.1/>
     * SELECT ?name ?mbox
     * WHERE { ?x foaf:name ?name .
     *            OPTIONAL { ?x foaf:mbox ?mbox }
     *       }
     * ORDER BY ASC(?mbox)
     * </pre>
     * 
     * Both this test (<code>sort-3</code>) and
     * <code>sparql11-aggregate-group-01</code> (an aggregation query, which I
     * believe is a Sesame specific test rather than a DAWG compliance test) are
     * hanging in scale-out CI runs. I suspect that this has to do with failing
     * to correctly trigger the last pass evaluation required by both sort and
     * aggregation.
     */
    public void test_sort_3() throws Exception {

        new TestHelper(
                "sort-3", // testURI,
                "sort-3.rq",// queryFileURL
                "sort-3.ttl",// dataFileURL
                "sort-3-result.rdf"// resultFileURL
                ).runTest();

    }

    /**
     * DAWG SPARQL 1.0 test
     * 
     * <pre>
     * PREFIX :         <http://example/> 
     * PREFIX xsd:      <http://www.w3.org/2001/XMLSchema#> 
     * SELECT DISTINCT * 
     * WHERE { 
     *   { ?s :p ?o } UNION { ?s :q ?o }
     * }
     * </pre>
     */
    public void test_distinct_star_1() throws Exception {

        new TestHelper(
                "distinct-star-1", // testURI,
                "distinct-star-1.rq",// queryFileURL
                "distinct-star-1.ttl",// dataFileURL
                "distinct-star-1.srx"// resultFileURL
                ).runTest();

    }

    /**
     * DAWG test ("find pairs that don't value-compare").
     * 
     * <pre>
     * PREFIX     :    <http://example/>
     * PREFIX  xsd:    <http://www.w3.org/2001/XMLSchema#>
     * 
     * SELECT ?x ?v1 ?y ?v2
     * {
     *     ?x :p ?v1 .
     *     ?y :p ?v2 .
     *     OPTIONAL { ?y :p ?v3 . FILTER( ?v1 != ?v3 || ?v1 = ?v3 )}
     *     FILTER (!bound(?v3))
     * }
     * </pre>
     * 
     * <pre>
     * mf:notable mf:IllFormedLiteral ;
     * mf:requires mf:KnownTypesDefault2Neq ;
     * mf:requires mf:LangTagAwareness ;
     * </pre>
     */
    public void test_open_eq_12() throws Exception {

        new TestHelper(
                "open-eq-12", // testURI,
                "open-eq-12.rq",// queryFileURL
                "open-eq-12.ttl",// dataFileURL
                "open-eq-12.srx"// resultFileURL
                ).runTest();

    }

    /**
     * DAWG test (FILTER inside an OPTIONAL does not block an entire solution).
     * <pre>
     * PREFIX  dc: <http://purl.org/dc/elements/1.1/>
     * PREFIX  x: <http://example.org/ns#>
     * SELECT  ?title ?price
     * WHERE
     *     { ?book dc:title ?title . 
     *       OPTIONAL
     *         { ?book x:price ?price . 
     *           FILTER (?price < 15) .
     *         } .
     *     }
     * </pre>
     */
    public void test_OPTIONAL_FILTER() throws Exception {
        
        new TestHelper(
                "OPTIONAL-FILTER", // testURI,
                "OPTIONAL_FILTER.rq",// queryFileURL
                "OPTIONAL_FILTER.ttl",// dataFileURL
                "OPTIONAL_FILTER-result.ttl"// resultFileURL
                ).runTest();
        
    }

    /**
     * This is sesame TCK test <code>sparql11-subquery-04</code>. We picked it
     * up with Sesame 2.6.3. It fails in the TCK run, but it appears to be Ok
     * when run at this layer.
     * <p>
     * Ah. This is using ORDER BY on an aggregate, so this is the same as
     * {@link #test_sparql11_order_02()}.
     * 
     * Query:
     * 
     * <pre>
     * PREFIX ex: <http://example.org/>
     * SELECT ?friend
     * WHERE {
     *   ?popular ex:knows ?friend .
     *   {
     *     SELECT ?popular
     *     WHERE {
     *       ?someone ex:knows ?popular
     *     } 
     *     GROUP BY ?popular
     *     ORDER BY DESC(COUNT(?someone))
     *     LIMIT 2
     *   }
     * }
     * </pre>
     * 
     * Data:
     * 
     * <pre>
     * @prefix ex: <http://example.org/> .
     * 
     * ex:george ex:knows ex:ringo, ex:john .
     * ex:ringo ex:knows ex:george, ex:john.
     * ex:paul  ex:knows ex:george, ex:john .
     * </pre>
     */
    public void test_sparql11_subquery_04() throws Exception {
        
        assertTrue(store.isQuads());
        
        new TestHelper(
                "sparql11-subquery-04", // testURI,
                "sparql11-subquery-04.rq",// queryFileURL
                "sparql11-subquery-04.ttl",// dataFileURL
                "sparql11-subquery-04.srx"// resultFileURL
        ).runTest();

    }

    /**
     * TCK (non-DAWG) query we picked up with <code>Sesame 2.6.3</code>. I've
     * modified the test slightly to report the aggregate used in the ORDER BY
     * through the SELECT.
     * 
     * <pre>
     * SELECT ?type (count(?subj) as ?cnt)
     * WHERE { ?subj a ?type } 
     * GROUP BY ?type 
     * ORDER BY (count(?subj))
     * </pre>
     * 
     * <pre>
     * @prefix : <http://example.org/> .
     * 
     * :one a :Number .
     * :two a :Number .
     * :three a :Number .
     * :four a :Number .
     * :five a :Number .
     * :six a :Number .
     * :seven a :Number .
     * :eight a :Number .
     * :nine a :Number .
     * :ten a :Number .
     * :a a :Letter .
     * :b a :Letter .
     * :c a :Letter .
     * :Sunday a :Day .
     * :Monday a :Day .
     * :Tuesday a :Day .
     * :Wednesday a :Day .
     * :Thursday a :Day .
     * :Friday a :Day .
     * :Saturday a :Day .
     * </pre>
     * 
     * <pre>
     * <?xml version='1.0' encoding='UTF-8'?>
     * <sparql xmlns='http://www.w3.org/2005/sparql-results#'>
     *     <head>
     *         <variable name='type'/>
     *     </head>
     *     <results>
     *         <result>
     *             <binding name='type'>
     *                 <uri>http://example.org/Letter</uri>
     *             </binding>
     *         </result>
     *         <result>
     *             <binding name='type'>
     *                 <uri>http://example.org/Day</uri>
     *             </binding>
     *         </result>
     *         <result>
     *             <binding name='type'>
     *                 <uri>http://example.org/Number</uri>
     *             </binding>
     *         </result>
     *     </results>
     * </sparql>
     * </pre>
     * 
     * @see <a href="http://www.openrdf.org/issues/browse/SES-822"> ORDER by
     *      GROUP aggregate </a>
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/502"> Allow
     *      aggregates in ORDER BY clause </a>
     */
    public void test_sparql11_order_02() throws Exception {
        if(!BigdataStatics.runKnownBadTests) return;
        new TestHelper("sparql11-order-02", // testURI,
                "sparql11-order-02.rq",// queryFileURL
                "sparql11-order-02.ttl",// dataFileURL
                "sparql11-order-02.srx"// resultFileURL
                ,true// checkOrder
        ).runTest();

    }

    /**
     * TCK (non-DAWG) query we picked up with <code>Sesame 2.6.3</code>
     * 
     * <pre>
     * SELECT  ?type 
     * WHERE { ?subj a ?type } 
     * GROUP BY ?type 
     * ORDER BY DESC(count(?subj))
     * </pre>
     * 
     * @see <a href="http://www.openrdf.org/issues/browse/SES-822"> ORDER by GROUP aggregate </a>
     */
    public void test_sparql11_order_03() throws Exception {
        if(!BigdataStatics.runKnownBadTests) return;
        new TestHelper("sparql11-order-03", // testURI,
                "sparql11-order-03.rq",// queryFileURL
                "sparql11-order-03.ttl",// dataFileURL
                "sparql11-order-03.srx"// resultFileURL
                ,true// checkOrder
        ).runTest();

    }
    
    /**
     * This is based on <code>service13</code>, which is an openrdf Federated
     * Query test. The test was failing because the two SERVICE joins do not
     * share any variables and use a hash join. Thus, we had two solutions from
     * the first SERVICE and sent two <em>empty</em> solutions to the second
     * SERVICE. This caused the 2nd SERVICE to deliver twice as many solutions
     * as is expected. Because the hash join was not correlated through a rowId,
     * we wind up wind a full cross product of both (empty) source solutions and
     * all solutions coming back from the 2nd service.
     * <p>
     * It is interesting that this test passes without modification when run on
     * purely local data. This suggests that the problem is not one of bottom up
     * evaluation, but the lack of a variable to correlate the result sets from
     * the remote services.
     * <p>
     * Note: This test is a rewrite of the same query to run against purely
     * local data. All I did was comment out the SERVICE keyword and service
     * URI.
     * 
     * <pre>
     * # Test for SES 899
     * 
     * PREFIX : <http://example.org/> 
     * PREFIX owl: <http://www.w3.org/2002/07/owl#> 
     * 
     * SELECT ?a ?b
     * {
     *   #SERVICE <http://localhost:18080/openrdf/repositories/endpoint1> 
     *   {
     *      ?a a ?t1 
     *      filter(?t1 = owl:Class) 
     *   }
     *   #SERVICE <http://localhost:18080/openrdf/repositories/endpoint1> 
     *   { 
     *      ?b a ?t2 
     *      filter(?t2 = owl:Class) 
     *   } 
     * }
     * </pre>
     */
    public void test_join_with_no_shared_variables() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "join_with_no_shared_variables", // testURI,
                "join_with_no_shared_variables.rq",// queryFileURL
                "join_with_no_shared_variables.ttl",// dataFileURL
                "join_with_no_shared_variables.srx"// resultFileURL
                ,false// checkOrder
        ).runTest();

        if (log.isInfoEnabled())
            log.info(astContainer.toString());

    }

    /**
     * Execute the stress tests a couple of times.
     * 
     * @throws Exception
     */
    public void test_stressTests() throws Exception {

        for (int i = 0; i < 100; i++) {
            final TestSuite suite = new TestSuite(
                TCKStressTests.class.getSimpleName());

            suite.addTestSuite(TCKStressTests.class);
            suite.run(new TestResult());
        }
    }
    

//    /**
//     * This is BSBM BI query 05 on the PC100 data set. We picked this up with
//     * Sesame 2.6.3. It is failing with a "solution set not found" error in the
//     * TCK. The problem is reproduced by this test. However, the test is a bit
//     * long running, so take it out of this test suite once we get to the bottom
//     * of the problem.
//     * <p>
//     * I finally tracked this down to an error in the logic to decide on a merge
//     * join. The story is documented at the trac issue below. However, even
//     * after all that the predicted result for openrdf differs at the 4th
//     * decimal place. I have therefore filtered out this test from the openrdf
//     * TCK.
//     * 
//     * <pre>
//     * Missing bindings: 
//     * [product=http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product63;
//     * nrOfReviews="3"^^<http://www.w3.org/2001/XMLSchema#integer>;
//     * avgPrice="4207.426"^^<http://www.w3.org/2001/XMLSchema#float>;
//     * country=http://downlode.org/rdf/iso-3166/countries#RU]
//     * ====================================================
//     * Unexpected bindings: 
//     * [country=http://downlode.org/rdf/iso-3166/countries#RU;
//     * product=http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer2/Product63;
//     * nrOfReviews="3"^^<http://www.w3.org/2001/XMLSchema#integer>;
//     * avgPrice="4207.4263"^^<http://www.w3.org/2001/XMLSchema#float>]
//     * </pre>
//     * 
//     * @see <a
//     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/534#comment:2">
//     *      BSBM BI Q5 Error when using MERGE JOIN </a>
//     */
//    public void test_BSBM_BI_05() throws Exception {
//        
//        new TestHelper(
//                "BSBM BI 05", // testURI,
//                "bsbm-bi-q5.rq",// queryFileURL
//                "bsbm-100.ttl",// dataFileURL // per the openrdf test.
////                "bsbm/bsbm3_dataset_pc10.nt",// dataFileURL
//                "bsbm-bi-q5.srx"// resultFileURL // per the openrdf test.
//                ,false// laxCardinality
//                ,true// checkOrder
//                ).runTest();
//        
//    }
    
    /**
     * Tests to be executed in a stress test fashion, i.e. multiple times.
     * 
     * @author msc
     */
    public static class TCKStressTests extends AbstractDataDrivenSPARQLTestCase {

        /**
          * 
          */
        public TCKStressTests() {
        }

        /**
         * @param name
         */
        public TCKStressTests(String name) {
            super(name);
        }

        /**
         * Optional-filter - 1
         * 
         * <pre>
         * PREFIX :    <http://example/>
         * 
         * SELECT *
         * { 
         *   ?x :p ?v .
         *   OPTIONAL
         *   { 
         *     ?y :q ?w .
         *     FILTER(?v=2)
         *   }
         * }
         * </pre>
         * 
         * A FILTER inside an OPTIONAL can reference a variable bound in the
         * required part of the OPTIONAL
         * 
         * @see ASTBottomUpOptimizer
         * @see ASTSimpleOptionalOptimizer
         */
        public void test_opt_filter_1() throws Exception {

            new TestHelper("opt-filter-1", // testURI,
                  "opt-filter-1.rq",// queryFileURL
                  "opt-filter-1.ttl",// dataFileURL
                  "opt-filter-1.srx"// resultFileURL
            ).runTest();

        }
    }
    
}
