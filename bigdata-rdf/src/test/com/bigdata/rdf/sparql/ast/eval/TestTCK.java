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
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.rdf.sparql.ast.optimizers.ASTBottomUpOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSimpleOptionalOptimizer;

/**
 * Test driver for debugging Sesame or DAWG manifest tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTCK extends AbstractDataDrivenSPARQLTestCase {

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
     * 0^^xsd:integer. We are producing an empty solution set.
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
     * I can not figure out why these "dataset" tests fail.
     */
    public void test_dataset_01() throws Exception {

        new TestHelper(
                "dataset-01", // testURI,
                "dataset-01.rq",// queryFileURL
                "data-g1.ttl",  // dataFileURL
                "dataset-01.ttl"// resultFileURL
                ).runTest();

    }

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
     * @see ASTBottomUpOptimizer
     * 
     *      FIXME This appears to be failing because the "OPTIONAL" semantics
     *      are not being attached to the INCLUDE.
     *      <p>
     *      Modify {@link NamedSubqueryIncludOp} to support optional semantics
     *      and modify {@link ASTBottomUpOptimizer} to mark the INCLUDE as
     *      optional.
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

        new TestHelper(
                "opt-filter-1", // testURI,
                "opt-filter-1.rq",// queryFileURL
                "opt-filter-1.ttl",// dataFileURL
                "opt-filter-1.srx"// resultFileURL
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

}
