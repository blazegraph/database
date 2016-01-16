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
 * Created on Oct 24, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

/**
 * Test suite for VIRTUAL GRAPHS support.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestVirtualGraphs extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestVirtualGraphs() {
    }

    /**
     * @param name
     */
    public TestVirtualGraphs(String name) {
        super(name);
    }

    /**
     * This test was developed from <code>default-graphs-01f</code>. The same
     * solutions exist as for that query. Rather than using two FROM clauses,
     * this test declares a virtual graph in the data and then queries that
     * virtual graph.
     * 
     * <pre>
     * prefix : <http://bigdata.com/> 
     * 
     * SELECT ?s ?p
     * FROM VIRTUAL GRAPH :vg
     * WHERE {
     *     ?s ?p :mary
     * }
     * </pre>
     * 
     * <pre>
     * @prefix : <http://bigdata.com/> .
     * 
     * :c1 {
     *   :john :loves :mary .
     * }
     * :c2 {
     *   :mary :loves :paul .
     * }
     * :c4 {
     *   :paul :loves :sam .
     * } 
     * :xx {
     *   :vg bd:virtualGraph :c1 .
     *   :vg bd:virtualGraph :c2 .
     * }
     * </pre>
     */
    public void test_virtualGraphs_default_graphs_01() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "virtualGraphs-default-graphs-01",// testURI
                "virtualGraphs-default-graphs-01.rq", // queryURI
                "virtualGraphs-default-graphs-01.trig", // dataURI
                "virtualGraphs-default-graphs-01.srx" // resultURI
                ).runTest();
        
    }

    /**
     * This test is a named graphs variant on the test above.
     * 
     * <pre>
     * prefix : <http://bigdata.com/> 
     * 
     * SELECT ?g ?s ?p
     * FROM NAMED VIRTUAL GRAPH :vg
     * WHERE {
     *     GRAPH ?g { ?s ?p :mary }
     * }
     * </pre>
     * 
     * <pre>
     * @prefix : <http://bigdata.com/> .
     * 
     * :c1 {
     *   :john :loves :mary .
     * }
     * :c2 {
     *   :mary :loves :paul .
     * }
     * :c4 {
     *   :paul :loves :sam .
     * } 
     * :xx {
     *   :vg bd:virtualGraph :c1 .
     *   :vg bd:virtualGraph :c2 .
     * }
     * </pre>
     */
    public void test_virtualGraphs_named_graphs_01() throws Exception {

        if(!store.isQuads())
            return;
        
        new TestHelper(
                "virtualGraphs-named-graphs-01",// testURI
                "virtualGraphs-named-graphs-01.rq", // queryURI
                "virtualGraphs-named-graphs-01.trig", // dataURI
                "virtualGraphs-named-graphs-01.srx" // resultURI
                ).runTest();
        
    }

}
