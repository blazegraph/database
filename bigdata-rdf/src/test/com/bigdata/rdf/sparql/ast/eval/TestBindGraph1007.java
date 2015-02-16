/**

Copyright (C) SYSTAP, LLC 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;

/**
 * Test suite using a bound variable to refer to a graph.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1007"> Using a bound variable to
 *      refer to a graph</a>
 * 
 * @version $Id$
 */
public class TestBindGraph1007 extends AbstractDataDrivenSPARQLTestCase {

    public TestBindGraph1007() {
    }

    public TestBindGraph1007(String name) {
        super(name);
    }

    /**
     * <pre>
     * PREFIX : <http://www.interition.net/ref/>
     * 
     * SELECT ?s ?p ?o ?literal WHERE {
     * 
     *   GRAPH <http://www.interition.net/g1> { 
     * 
     *     <http://www.interition.net/s1> :aProperty ?literal .
     * 
     *     BIND ( URI(CONCAT("http://www.interition.net/graphs/", ?literal )) AS ?graph) .
     * 
     *   }
     * 
     *   GRAPH ?graph { ?s ?p ?o . }
     * 
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_bindGraph_1007() throws Exception {
        		new TestHelper(
        				"bindGraph-1007",// testURI
        				"bindGraph-1007.rq", // queryURI
        				"bindGraph-1007.trig", // dataURI
        				"bindGraph-1007.srx" // resultURI
        				).runTest();
    }
    
}
