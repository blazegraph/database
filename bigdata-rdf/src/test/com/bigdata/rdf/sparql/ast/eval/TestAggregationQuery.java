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
    
}
