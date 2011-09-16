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
     * @see ASTBottomUpOptimizer
     * 
     *      FIXME Analyze this query. I'm not sure why it is failing. ?v is not
     *      in scope in the FILTER. Nothing should be lifted out.... Maybe it is
     *      being rewritten by mistake?
     * 
     *      I am wondering if ?v should be in scope here? I am wondering if the
     *      syntax of OPTIONAL is such that we evaluate the (?x :p ?v ) clause
     *      before the (?y :q ?w . FILTER(?v=2)) clause but that all variables
     *      in both clauses are "in scope".
     * 
     *      If so, then the change is just to
     *      handleFiltersWithVariablesNotInScope in {@link ASTBottomUpOptimizer}
     *      .
     */
    public void test_opt_filter_1() throws Exception {

        new TestHelper(
                "opt-filter-1", // testURI,
                "opt-filter-1.rq",// queryFileURL
                "opt-filter-1.ttl",// dataFileURL
                "opt-filter-1.srx"// resultFileURL
                ).runTest();

    }

}
