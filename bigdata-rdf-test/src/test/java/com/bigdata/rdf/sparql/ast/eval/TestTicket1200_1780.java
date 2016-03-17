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
 * Test case for https://jira.blazegraph.com/browse/BLZG-1200:
 * REGEX does not use SPARQL spec for conversion of literals with a language type
 * 
 * @author <a href="mailto:beebs@blazegraph.com">Brad Bebee</a>
 */
public class TestTicket1200_1780 extends AbstractDataDrivenSPARQLTestCase {

   public TestTicket1200_1780() {
   }

   public TestTicket1200_1780(String name) {
      super(name);
   }
  

   	/**
	 * Test for REGEX on a non-string literal (Inline IPv4)
	 * 
	 * {@link BLZG-1780}
	 * 
	 * <pre>
	 * prefix : <http://www.bigdata.com/> 
	 * prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
	 * prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
	 * prefix foaf: <http://xmlns.com/foaf/0.1/> 
	 * prefix xsd: <http://www.w3.org/2001/XMLSchema#>
	 * 
	 * 
	 * select ?s
	 * where {
	 *   ?s hasAddress ?address .
	 *   FILTER REGEX(?address, '^10.*', 'i')
	 * }
	 * </pre>
	 * 
	 * @throws Exception
	 */
  public void test_ticket_1200a() throws Exception {
     new TestHelper("ticket-1200a",// testURI,
           "ticket_1200a.rq",// queryFileURL
           "ticket_1200.trig",// dataFileURL
           "ticket_1200a.srx",// resultFileURL
           false /* checkOrder */
     ).runTest();
  }   
  
	/**
	 * 
	 * Test that the query without the query hint fails.
	 * 
	 * {@See BLZG-1780}
	 * 
	 * prefix : <http://www.bigdata.com/> prefix rdf:
	 * <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix rdfs:
	 * <http://www.w3.org/2000/01/rdf-schema#> prefix foaf:
	 * <http://xmlns.com/foaf/0.1/> prefix xsd:
	 * <http://www.w3.org/2001/XMLSchema#> PREFIX hint:
	 * <http://www.bigdata.com/queryHints#>
	 * 
	 * select ?s where { ?s :hasAddress ?address . FILTER(REGEX(?address,
	 * '^10.*', 'i')) }
	 * 
	 * @throws Exception
	 */
  public void test_ticket_1780a() throws Exception {
     new TestHelper("ticket-1780a",// testURI,
           "ticket_1780a.rq",// queryFileURL
           "ticket_1200.trig",// dataFileURL
           "ticket_1780a.srx",// resultFileURL
           false /* checkOrder */
     ).runTest();
  }   
  
	/**
	 * Test for REGEX on a string literal 
	 * 
	 * <pre>
	 * prefix : <http://www.bigdata.com/> 
	 * prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
	 * prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
	 * prefix foaf: <http://xmlns.com/foaf/0.1/> 
	 * prefix xsd: <http://www.w3.org/2001/XMLSchema#>
	 * 
	 * 
	 * select ?s
	 * where {
	 *   ?s rdfs:label ?address .
	 *   FILTER REGEX(?address, '^11.*', 'i')
	 * }
	 * </pre>
	 * 
	 * @throws Exception
	 */
  public void test_ticket_1200b() throws Exception {
      new TestHelper("ticket-1200b",// testURI,
            "ticket_1200b.rq",// queryFileURL
            "ticket_1200.trig",// dataFileURL
            "ticket_1200b.srx",// resultFileURL
            true /* checkOrder */
      ).runTest();
   }   

   	/**
	 * Test for REGEX on a non-string literal (Inline IPv4) with conversion
	 * 
	 * <pre>
	 * prefix : <http://www.bigdata.com/> 
	 * prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
	 * prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
	 * prefix foaf: <http://xmlns.com/foaf/0.1/> 
	 * prefix xsd: <http://www.w3.org/2001/XMLSchema#>
	 * 
	 * 
	 * select ?s
	 * where {
	 *   ?s :hasAddress ?address .
	 *   FILTER REGEX(str(?address), '^10.*', 'i')
	 * }
	 * </pre>
	 * 
	 * @throws Exception
	 */
  public void test_ticket_1200c() throws Exception {
     new TestHelper("ticket-1200c",// testURI,
           "ticket_1200c.rq",// queryFileURL
           "ticket_1200.trig",// dataFileURL
           "ticket_1200c.srx",// resultFileURL
           false /* checkOrder */
     ).runTest();
  }   

	/**
	 * Test for REGEX on a string literal with language annotation
	 * 
	 * <pre>
	 * prefix : <http://www.bigdata.com/> 
	 * prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
	 * prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
	 * prefix foaf: <http://xmlns.com/foaf/0.1/> 
	 * prefix xsd: <http://www.w3.org/2001/XMLSchema#>
	 * 
	 * 
	 * select ?s
	 * where {
	 *   ?s rdfs:label ?address .
	 *   FILTER REGEX(?address, '^12.*', 'i')
	 * }
	 * </pre>
	 * 
	 * @throws Exception
	 */
  public void test_ticket_1200d() throws Exception {
      new TestHelper("ticket-1200d",// testURI,
            "ticket_1200d.rq",// queryFileURL
            "ticket_1200.trig",// dataFileURL
            "ticket_1200d.srx",// resultFileURL
            true /* checkOrder */
      ).runTest();
   }   
  
  /**
	 * Test for REGEX on a string literal with language annotation
	 * 
	 * <pre>
	 * prefix : <http://www.bigdata.com/> 
	 * prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
	 * prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
	 * prefix foaf: <http://xmlns.com/foaf/0.1/> 
	 * prefix xsd: <http://www.w3.org/2001/XMLSchema#>
	 * 
	 * 
	 * select ?s
	 * where {
	 *   ?s rdfs:label ?address .
	 *   FILTER REGEX(str(?address), '^12.*', 'i')
	 * }
	 * </pre>
	 * 
	 * @throws Exception
	 */
public void test_ticket_1200e() throws Exception {
    new TestHelper("ticket-1200e",// testURI,
          "ticket_1200e.rq",// queryFileURL
          "ticket_1200.trig",// dataFileURL
          "ticket_1200e.srx",// resultFileURL
          true /* checkOrder */
    ).runTest();
 }   
  
}
