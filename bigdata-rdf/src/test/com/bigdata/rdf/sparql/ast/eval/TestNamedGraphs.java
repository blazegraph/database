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
 * Created on Oct 15, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import org.openrdf.query.Dataset;


/**
 * Test suite for named and default graph stuff.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNamedGraphs extends AbstractDataDrivenSPARQLTestCase {

    public TestNamedGraphs() {
    }

    public TestNamedGraphs(final String name) {
        super(name);
    }

    /**
     * A series of quads mode unit tests for named and default graph query of a
     * data set containing a single triple.
     */
    public void test_namedGraphs_01a() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "named-graphs-01a",// testURI
                "named-graphs-01a.rq", // queryURI
                "named-graphs-01.trig", // dataURI
                "named-graphs-01.srx" // resultURI
                ).runTest();

    }

    /**
     * This version of the query specifies a FROM clause and hence causes the
     * {@link Dataset} to become non-<code>null</code> but does not specify any
     * FROM NAMED clauses. However, this query (like all the others in this
     * series) is only querying the named graphs.
     * <p>
     * Note: The correct result for this query should be NO solutions. This is
     * one of the cases covered by the DAWG/TCK graph-02 and graph-04 tests. One
     * of those tests verifies that when the data is from the default graph and
     * the query is against the named graphs then there are no solutions. The
     * other test verifies that the opposite is also true - that when the data
     * are from the named graphs and the query is against the default graph that
     * there are no solutions. This test is currently failing because it
     * contradicts that prediction. The test is probably wrong.
     */
    public void test_namedGraphs_01b() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-01b",// testURI
                "named-graphs-01b.rq", // queryURI
                "named-graphs-01.trig", // dataURI
                "named-graphs-01b.srx" // resultURI
                ).runTest();

    }
    
    public void test_namedGraphs_01c() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-01c",// testURI
                "named-graphs-01c.rq", // queryURI
                "named-graphs-01.trig", // dataURI
                "named-graphs-01.srx" // resultURI
                ).runTest();

    }

    public void test_namedGraphs_01d() throws Exception {

        if(!store.isQuads())
            return;

        new TestHelper(
                "named-graphs-01d",// testURI
                "named-graphs-01d.rq", // queryURI
                "named-graphs-01.trig", // dataURI
                "named-graphs-01.srx" // resultURI
                ).runTest();

    }

}
