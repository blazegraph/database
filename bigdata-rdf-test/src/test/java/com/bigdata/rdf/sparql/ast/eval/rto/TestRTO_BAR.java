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

package com.bigdata.rdf.sparql.ast.eval.rto;

import java.util.Properties;

import com.bigdata.bop.rdf.joinGraph.GenerateBarData;
import com.bigdata.rdf.sail.BigdataSail;

/**
 * Data driven test suite for the Runtime Query Optimizer (RTO) using BAR data
 * and queries.
 * 
 * @see GenerateBarData
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBasicQuery.java 6440 2012-08-14 17:57:33Z thompsonbry $
 */
public class TestRTO_BAR extends AbstractRTOTestCase {

//    private final static Logger log = Logger.getLogger(TestRTO_BAR.class);
    
    /**
     * 
     */
    public TestRTO_BAR() {
    }

    /**
     * @param name
     */
    public TestRTO_BAR(final String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        properties.setProperty(BigdataSail.Options.QUADS_MODE, "true");

        return properties;
        
    }

    /**
     * Sample query for the synthetic data set. The query is arranged in a known
     * good order.
     * <p>
     * Note: The runtime optimizer estimate of the cardinality of the edge [5 4]
     * in this query is a lower bound, which makes this an interesting test
     * case. The runtime optimizer detects this lower bound and replaces [nout]
     * with the sum of the range count of the as-bound predicates for the join,
     * which leads to an efficient query plan.
     * 
     * <pre>
     * SELECT ?employeeNum (COUNT(?type) AS ?total)
     * WHERE {
     *         ?order a <http://test/bar#Order> .
     *         ?order <http://test/bar#orderItems> ?item .
     *         ?item <http://test/bar#beverageType> "Beer" .
     *         ?item <http://test/bar#beverageType> ?type .
     *         ?order <http://test/bar#employee> ?employee .
     *         ?employee <http://test/bar#employeeNum> ?employeeNum .
     * } GROUP BY ?employeeNum
     * </pre>
     * 
     * @throws Exception
     */
    public void test_BAR_Q1() throws Exception {

        final TestHelper helper = new TestHelper(//
                "rto/BAR-Q1", // testURI,
                "rto/BAR-Q1.rq",// queryFileURL
                "src/test/resources/data/barData/barData.trig.gz",// dataFileURL
                "rto/BAR-Q1.srx"// resultFileURL
        );

        /*
         * Verify that the runtime optimizer produced the expected join path.
         */
        final int[] expected = new int[] { 5, 3, 1, 7, 9, 11 };

        assertSameJoinOrder(expected, helper);

    }

}
