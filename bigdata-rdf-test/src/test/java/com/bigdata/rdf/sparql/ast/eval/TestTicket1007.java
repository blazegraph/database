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
 * Various tests covering different constellations where values are constructed
 * using BIND and reused in other parts of the query, such as
 * 
 * - testing inlined vs. non-inlined configuration - using BIND prior to vs.
 * after joining/filtering the variable - BINDing to values that are in the
 * dictionary vs. BINDing to values that are not in the dictionary
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1007">Ticket 1007: Using bound
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

   /**************************************************************************
    *********************** ORIGINAL TICKET TESTS ****************************
    **************************************************************************/

   /**
    * Original query as defined in bug report, reusing a URI constructed in a
    * BIND clause in a join: 
        <code>
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
    * Note: originally this query failed.
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
   
   /**************************************************************************
    ***************************** CUSTOM TESTS *******************************
    **************************************************************************/
   
   /*
    *  Dataset (trig) used in the tests defined in this section:
    * 
      <code>
      @prefix : <http://www.bigdata.com/> .
      @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
      @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
      @prefix foaf: <http://xmlns.com/foaf/0.1/> .
      @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

      : {
          :s :untypedString "untypedString" .
          :s :typedString "typedString"^^xsd:unsignedByte .
          :s :int "10"^^xsd:int .
          :s :integer "10"^^xsd:integer .
          :s :double "10.0"^^xsd:double .
          :s :boolean "true"^^xsd:boolean .
          :c :p5 "5"^^xsd:integer .          
      }
      </code>
    * 
    */

   /**
    <code>
      SELECT ?o
      WHERE 
      { 
         ?s ?p ?o
         BIND (10 AS ?o)
      }
    </code>
    */
   public void test_ticket_1007_number1() throws Exception {
      new TestHelper("ticket-1007-number1",// testURI,
            "ticket-1007-number1.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-number-integer.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
   <code>
      SELECT ?o
      { 
        ?s <http://www.bigdata.com/double> ?o
        BIND ("10.00"^^<http://www.w3.org/2001/XMLSchema#double> AS ?o)
      }
   </code>
   */
   public void test_ticket_1007_number2() throws Exception {
      new TestHelper("ticket-1007-number2",// testURI,
            "ticket-1007-number2.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-number-double.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
   <code>
      SELECT ?o
      { 
        ?s <http://www.bigdata.com/integer> ?o
        BIND (10 AS ?o)
      }
   </code>
   */
   public void test_ticket_1007_number3() throws Exception {
      new TestHelper("ticket-1007-number3",// testURI,
            "ticket-1007-number3.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-number-integer.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
   <code>
      SELECT ?o
      { 
        ?s <http://www.bigdata.com/integer> ?o
        BIND (2*5 AS ?o)
      }
   </code>
   */
   public void test_ticket_1007_number4() throws Exception {
      new TestHelper("ticket-1007-number4",// testURI,
            "ticket-1007-number4.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-number-integer.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
   <code>
      SELECT ?o
      { 
        ?s <http://www.bigdata.com/integer> ?o
        BIND (xsd:integer("10") AS ?o)
      }
   </code>
   *
   * Note: originally this query failed.
   */
   public void test_ticket_1007_number5() throws Exception {
      new TestHelper("ticket-1007-number5",// testURI,
            "ticket-1007-number5.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-number-integer.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }
   
   /**
   <code>
      SELECT ?o
      { 
        ?s <http://www.bigdata.com/integer> ?o .
        <http://www.bigdata.com/c> <http://www.bigdata.com/p5> ?v .
        BIND (?v*2 AS ?o)
      }
   </code>
   */   
   public void test_ticket_1007_number6() throws Exception {
      new TestHelper("ticket-1007-number6",// testURI,
            "ticket-1007-number6.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-number-integer.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
   <code>
      SELECT ?o
      WHERE 
      { 
        ?s ?p ?o
        BIND ("untypedString" AS ?o)
      }
   </code>
   */
   public void test_ticket_1007_string1() throws Exception {
      new TestHelper("ticket-1007-string1",// testURI,
            "ticket-1007-string1.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-string.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
   <code>
      SELECT ?o
      WHERE 
      { 
        ?s ?p ?o
        BIND (CONCAT("untyped","String") AS ?o)
      }
   </code>
   *
   * Note: originally this query failed.   
   */
   public void test_ticket_1007_string2() throws Exception {
      new TestHelper("ticket-1007-string2",// testURI,
            "ticket-1007-string2.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-string.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
   <code>
      SELECT ?o
      WHERE 
      { 
        ?s ?p ?o
        BIND (STRAFTER("XuntypedString","X") AS ?o)
      }
   </code>
   *
   * Note: originally this query failed.   
   */
   public void test_ticket_1007_string3() throws Exception {
      new TestHelper("ticket-1007-string3",// testURI,
            "ticket-1007-string3.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-string.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
   <code>
       SELECT ?o
       WHERE 
       { 
          ?s ?p ?o
          {
             SELECT ?o
             WHERE
             {
                BIND (CONCAT("untyped","String") AS ?o)
             }
          }
       }
   </code>
   *
   * Note: originally this query failed.
   */
   public void test_ticket_1007_string4() throws Exception {
      new TestHelper("ticket-1007-string4",// testURI,
            "ticket-1007-string4.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-string.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }   
   
   /**
   <code>
       SELECT ?o
       WHERE 
       { 
          BIND (CONCAT("untyped","String") AS ?o)
       }
   </code>
   *
   * Note: originally this query failed.
   */
   public void test_ticket_1007_string5() throws Exception {
      new TestHelper("ticket-1007-string5",// testURI,
            "ticket-1007-string5.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-string.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }   
   
   /**
   <code>
      SELECT ?o
      WHERE 
      { 
        ?s ?p ?o
        BIND (URI("http://untypedString") AS ?o)
      }
   </code>
   */
   public void test_ticket_1007_empty1() throws Exception {
      new TestHelper("ticket-1007-empty1",// testURI,
            "ticket-1007-empty1.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-empty.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }
   
   /**
   <code>
      SELECT ?o
      WHERE 
      {
        ?s ?p ?o
        BIND ("10" AS ?o)
      }
   </code>
   */
   public void test_ticket_1007_empty2() throws Exception {
      new TestHelper("ticket-1007-empty2",// testURI,
            "ticket-1007-empty2.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-empty.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   
   /**
   <code>
      SELECT DISTINCT ?z
      WHERE 
      {
        ?s ?p ?o
        BIND (URI("http://untypedUri") AS ?z)
      }
   </code>
   */
   public void test_ticket_1007_freshUri() throws Exception {
      new TestHelper("ticket-1007-freshUri",// testURI,
            "ticket-1007-freshUri.rq",// queryFileURL
            "ticket-1007-custom.trig",// dataFileURL
            "ticket-1007-freshUri.srx",// resultFileURL
            false // checkOrder (because only one solution)
      ).runTest();
   }

   /**
   <code>
      SELECT ?o
      WHERE 
      { 
        ?s <http://www.bigdata.com/boolean> ?o
        BIND (?s=?s AS ?o)
      }
   </code>
   */
  public void test_ticket_1007_boolean1() throws Exception {
     new TestHelper("ticket-1007-boolean1",// testURI,
           "ticket-1007-boolean1.rq",// queryFileURL
           "ticket-1007-custom.trig",// dataFileURL
           "ticket-1007-boolean.srx",// resultFileURL
           false // checkOrder (because only one solution)
     ).runTest();
  }   
  
  /**
  <code>
      SELECT ?o
      WHERE 
      { 
         ?s <http://www.bigdata.com/boolean> ?o
         BIND (?s=<http://www.bigdata.com/s> AS ?o)
      }
  </code>
  */
  public void test_ticket_1007_boolean2() throws Exception {
     new TestHelper("ticket-1007-boolean2",// testURI,
           "ticket-1007-boolean2.rq",// queryFileURL
           "ticket-1007-custom.trig",// dataFileURL
           "ticket-1007-boolean.srx",// resultFileURL
           false // checkOrder (because only one solution)
     ).runTest();
  }   
  
  /**
   * Test problems with BIND inside and reuse of variable outside of
   * subquery.
   * 
   * <code>
       SELECT DISTINCT *
       { 
         { 
           SELECT ?annotatedSource WHERE {
             hint:SubQuery hint:runOnce true .
             ?s ?p ?o .
             FILTER(strstarts(?o,"annotated"))
             BIND(concat(substr(?o,1,9),"Source") as ?annotatedSource)
          } 
       }
       ?ss ?pp ?annotatedSource 
       } LIMIT 20
       </code>
   * 
   * @see <a href="http://trac.bigdata.com/ticket/490#comment:5">here</a>
   *      for more details
   */
  public void test_ticket_1007_subquery() throws Exception {
     new TestHelper("ticket-1007-subquery",// testURI,
           "ticket-1007-subquery.rq",// queryFileURL
           "ticket-1007-subquery.trig",// dataFileURL
           "ticket-1007-subquery.srx",// resultFileURL
           false // checkOrder (because only one solution)
     ).runTest();
  }   
  
}
