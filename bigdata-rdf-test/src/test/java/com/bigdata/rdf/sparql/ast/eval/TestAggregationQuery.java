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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.bop.solutions.AbstractAggregationTestCase;
import com.bigdata.bop.solutions.GroupByRewriter;
import com.bigdata.bop.solutions.GroupByState;

/**
 * Data driven test suite for aggregation queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Capture everything from {@link AbstractAggregationTestCase}
 *          here. This test suite was sculpted to exercise the
 *          {@link GroupByState} and {@link GroupByRewriter} and the differences
 *          between pipelined and at-once evaluation of aggregates.
 */
public class TestAggregationQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestAggregationQuery() {
    }

    /**
     * @param name
     */
    public TestAggregationQuery(String name) {
        super(name);
    }

    /**
     * A basic aggregation query.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT (SUM(?lprice) AS ?totalPrice)
     * WHERE {
     *   ?org :affiliates ?auth .
     *   ?auth :writesBook ?book .
     *   ?book :price ?lprice .
     * }
     * GROUP BY ?org
     * HAVING (sum(?lprice) > 10)
     * 
     * versus
     *  
     * PREFIX : <http://books.example/>
     * SELECT ?org, (SUM(?lprice) AS ?totalPrice)
     * WHERE {
     *   ?org :affiliates ?auth .
     *   ?auth :writesBook ?book .
     *   ?book :price ?lprice .
     * }
     * GROUP BY ?org
     * HAVING (SUM(?lprice) > 10)
     * </pre>
     */
    public void test_sparql11_having_01() throws Exception {
        
        new TestHelper(
                "sparql11-having-01", // testURI,
                "sparql11-having-01.rq",// queryFileURL
                "sparql11-having-01.ttl",// dataFileURL
                "sparql11-having-01.srx"// resultFileURL
                ).runTest();
        
    }

    /**
     * <pre>
     * SELECT (COUNT(?s) AS ?size)
     * FROM <http://www.bigdata.com/systap>
     * WHERE { 
     *     ?s ?p ?o . 
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_count_namedGraph01() throws Exception {
     
        new TestHelper(
                "count_namedGraph01", // testURI,
                "count_namedGraph01.rq",// queryFileURL
                "count_namedGraph01.trig",// dataFileURL
                "count_namedGraph01.srx"// resultFileURL
                ).runTest();
        
    }

    /**
     * <pre>
     * SELECT (COUNT(?s) AS ?size)
     * WHERE { 
     *     GRAPH <http://www.bigdata.com/systap> {
     *         ?s ?p ?o . 
     *     }
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_count_namedGraph02() throws Exception {

        new TestHelper("count_namedGraph02", // testURI,
                "count_namedGraph02.rq",// queryFileURL
                "count_namedGraph02.trig",// dataFileURL
                "count_namedGraph02.srx"// resultFileURL
                ).runTest();
        
    }
    
    /**
     * Query correctly returns one row having a value of ZERO (0) for the count
     * since there are no solutions in the data that match the query.
     * 
     * <pre>
     * select (count(?s) as ?count) {
     *                 ?s ?p "abcedefg" .
     * }
     * </pre>
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/868"> COUNT(DISTINCT)
     *      returns no rows rather than ZERO. </a>
     */
    public void test_count_emptyResult() throws Exception {
        
        new TestHelper("count_emptyResult", // testURI,
                "count_emptyResult.rq",// queryFileURL
                "count_emptyResult.trig",// dataFileURL
                "count_emptyResult.srx"// resultFileURL
                ).runTest();
    }

    /**
     * Variation of the query above using COUNT(DISTINCT) should also return one
     * solution having a binding of ZERO (0) for the count.
     * 
     * <pre>
     * select (count(distinct ?snippet) as ?count) {
     *                 ?snippet ?p "abcedefg" .
     * }
     * </pre>
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/868"> COUNT(DISTINCT)
     *      returns no rows rather than ZERO. </a>
     */
    public void test_count_distinct_emptyResult()throws Exception {
        
        new TestHelper("count_distinct_emptyResult", // testURI,
                "count_distinct_emptyResult.rq",// queryFileURL
                "count_distinct_emptyResult.trig",// dataFileURL
                "count_distinct_emptyResult.srx"// resultFileURL
                ).runTest();
    }

}
