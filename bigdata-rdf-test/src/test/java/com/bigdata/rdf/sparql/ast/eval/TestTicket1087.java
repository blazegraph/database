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
 * Test suite for an issue where ill designed patterns consisting of a
 * combination of UNION and nested OPTIONAL cause problems.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1087">
 * Named subquery results not referenced within query (bottom-up evaluation)</a>
 */
public class TestTicket1087 extends AbstractDataDrivenSPARQLTestCase {

    public TestTicket1087() {
    }

    public TestTicket1087(String name) {
        super(name);
    }

    /**
     * Verify correct result for query:
     * 
     * <pre>
     * SELECT * WHERE {
     *   ?s <http://example.org/p1> ?o1 .
     *   { ?s <http://example.org/p2> ?o2 } 
     *   UNION 
     *   {
     *     ?s <http://example.org/p3> ?o3 .
     *      OPTIONAL {
     *        ?s <http://example.org/p1> ?o1 .
     *      }
     *   }
     * }
     * </pre>
     */
    public void test_ticket_1087_1() throws Exception {
        
        new TestHelper(
                "ticket_1087_1", // testURI,
                "ticket_1087_1.rq",// queryFileURL
                "ticket_1087.trig",// dataFileURL
                "ticket_1087_1.srx"// resultFileURL
                ).runTest();

    }
    
    /**
     * Verify correct result for query:
     * 
     * <pre>
     * SELECT * WHERE {
     *   ?s <http://example.org/p1> ?o1 .
     *   { ?s <http://example.org/p2> ?o2 } 
     *   UNION 
     *   {
     *     ?s <http://example.org/p3> ?o3 .
     *      OPTIONAL {
     *        ?s <http://example.org/p2> ?o1 .
     *      }
     *   }
     * }
     * </pre>
     */
    public void test_ticket_1087_2() throws Exception {
       
       new TestHelper(
               "ticket_1087_2", // testURI,
               "ticket_1087_2.rq",// queryFileURL
               "ticket_1087.trig",// dataFileURL
               "ticket_1087_2.srx"// resultFileURL
               ).runTest();

   }
    
}
