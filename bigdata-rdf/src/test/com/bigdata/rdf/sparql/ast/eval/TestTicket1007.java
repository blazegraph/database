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

import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase.TestHelper;

/**
 * Various tests covering different constellations where values are constructed
 * using BIND and reused in other parts of the query, such as
 * 
 * - testing inlined vs. non-inlined configuration - using BIND prior to vs.
 * after joining/filtering the variable - BINDing to values that are in the
 * dictionary vs. BINDing to values that are not in the dictionary
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1007">Ticket 1007: Using bound
 *      variables to refer to a graph</a>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestTicket1007 extends AbstractDataDrivenSPARQLTestCase {

   public TestTicket1007() {
   }

   public TestTicket1007(String name) {
      super(name);
   }

   /**
    * Original query as defined in bug report, reusing a URI constructed in a
    * BIND clause in a join: <code>
         PREFIX : <http://www.interition.net/ref/>

         SELECT * WHERE
         {
            GRAPH <http://www.interition.net/g1>
            {
               <http://s1> :aProperty ?literal .
               BIND (URI(CONCAT("http://www.interition.net/graphs/",?literal)) 
                     AS ?graph) .
            }
            GRAPH ?graph {
               ?s ?p ?o .
            }
         }
       </code>
    * 
    * @throws Exception
    */
   public void test_ticket_1007() throws Exception {
      new TestHelper("ticket-1007",// testURI,
            "ticket-1007.rq",// queryFileURL
            "ticket-1007.trig",// dataFileURL
            "ticket-1007.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
    * Modified query with join "on top", enforced through outer VALUES clause.
    * 
    * <code>
         PREFIX : <http://www.interition.net/ref/>

         SELECT * WHERE
         {
            GRAPH <http://www.interition.net/g1>
            {
               <http://s1> :aProperty ?literal .
               BIND (URI(CONCAT("http://www.interition.net/graphs/",?literal)) 
                     AS ?graph) .
            }
         }
         VALUES ?graph { <http://www.interition.net/graphs/g2> }
       </code>
    * 
    * @throws Exception
    */
   public void test_ticket_1007b() throws Exception {
      new TestHelper("ticket-1007b",// testURI,
            "ticket-1007b.rq",// queryFileURL
            "ticket-1007.trig",// dataFileURL
            "ticket-1007b.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

}
