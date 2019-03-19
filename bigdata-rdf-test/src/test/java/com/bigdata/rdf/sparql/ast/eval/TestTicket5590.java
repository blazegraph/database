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
 * Problems with negated path expressions.
 * 
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-5590">Negated path produces exception</a>
 */
public class TestTicket5590 extends AbstractDataDrivenSPARQLTestCase {

   public TestTicket5590() {
   }

   public TestTicket5590(String name) {
      super(name);
   }

   /**
    * ORIGINAL QUERY:
    * 
    * SELECT * WHERE {
    *     ?item <http://test/p1>/<http://test/p2>/<http://test/p3> ?x .
    *     ?x (!<http://test/p4>)* <http://test/target> .
    * } LIMIT 10
    * 
    **/
  public void test_ticket_5590a() throws Exception {
     new TestHelper("ticket-5590a",// testURI,
           "ticket-5590a.rq",// queryFileURL
           "ticket-5590a.trig",// dataFileURL
           "ticket-5590a.srx",// resultFileURL
           false
     ).runTest();
  }   

}
