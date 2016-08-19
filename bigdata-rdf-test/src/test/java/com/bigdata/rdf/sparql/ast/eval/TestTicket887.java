/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;


/**
 * Test suite for a heisenbug involving BIND. Unlike the other issues this
 * sometimes happens, and is sometimes OK, so we run the test in a loop 20
 * times.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/708">
 *      Heisenbug </a>
 */
public class TestTicket887 extends AbstractDataDrivenSPARQLTestCase {

    public TestTicket887() {
    }

    public TestTicket887(String name) {
        super(name);
    }

    /**
     * <pre>
     * SELECT *
     * WHERE {
     * 
     *     GRAPH ?g {
     * 
     *         BIND( "hello" as ?hello ) .
     *         BIND( CONCAT(?hello, " world") as ?helloWorld ) .
     * 
     *     ?member a ?class .
     * 
     *     }
     * 
     * }
     * LIMIT 1
     * </pre>
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/887" > BIND is leaving a
     *      variable unbound </a>
     */
    public void test_ticket_887_bind() throws Exception {
        
        new TestHelper(
                "ticket_887_bind", // testURI,
                "ticket_887_bind.rq",// queryFileURL
                "ticket_887_bind.trig",// dataFileURL
                "ticket_887_bind.srx"// resultFileURL
                ).runTest();

    }
    
}
