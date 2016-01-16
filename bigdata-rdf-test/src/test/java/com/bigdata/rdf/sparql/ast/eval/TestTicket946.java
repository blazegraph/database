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
 * Test suite for an issue where an empty projection causes an
 * {@link IllegalArgumentException}.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/946"> Empty PROJECTION causes
 *      IllegalArgumentException</a>
 */
public class TestTicket946 extends AbstractDataDrivenSPARQLTestCase {

    public TestTicket946() {
    }

    public TestTicket946(String name) {
        super(name);
    }

    /**
	 * <pre>
	 * SELECT ?x
	 * { BIND(1 as ?x) 
	 *   { SELECT * { FILTER (true) } }
	 * }
	 * </pre>
	 */
    public void test_ticket_946_empty_projection() throws Exception {
        
        new TestHelper(
                "ticket_946", // testURI,
                "ticket_946.rq",// queryFileURL
                "ticket_946.trig",// dataFileURL
                "ticket_946.srx"// resultFileURL
                ).runTest();

    }
    
}
