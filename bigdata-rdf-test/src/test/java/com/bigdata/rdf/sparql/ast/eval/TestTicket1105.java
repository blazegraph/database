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

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.rdf.sparql.ast.QuadsOperationInTriplesModeException;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite asserting that queries containint quads constructs (named graphs)
 * are rejected in triples mode (at parsing phase), but go through in quads
 * mode.
 * 
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/1105">
 * SPARQL UPDATE should have nice error messages when namespace 
 * does not support named graphs</a>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestTicket1105 extends AbstractDataDrivenSPARQLTestCase {

   public TestTicket1105() {
   }

   public TestTicket1105(String name) {
      super(name);
   }
   
   public static Test suite() {

      final TestSuite suite = new TestSuite(
            AbstractDataDrivenSPARQLTestCase.class.getSimpleName());

      suite.addTestSuite(TestTriplesModeAPs.class);
      suite.addTestSuite(TestQuadsModeAPs.class);

      return suite;
   }

   
   /**
    * Triples mode test suite.
    */
   public static class TestTriplesModeAPs extends TestTicket1105 {

      @Override
      public Properties getProperties() {

         final Properties properties = new Properties(super.getProperties());

         // turn off quads.
         properties.setProperty(AbstractTripleStore.Options.QUADS, "false");

         // turn on triples
         properties.setProperty(AbstractTripleStore.Options.TRIPLES_MODE,
               "true");

         return properties;

      }
      
      /**
       * Query: 
          <code>
            INSERT DATA
            { 
               GRAPH <http://example/c> 
               { <http://example/s>  <http://example/p> <http://example/o> } 
            }
          </code>
       * 
       * throws an {@link QuadsOperationInTriplesModeException} in triples mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_triples_update1() throws Exception {

         try {
         
            new UpdateTestHelper("ticket_1105_triples_update1", // testURI,
                  "ticket_1105_update1.rq",// queryFileURL
                  "ticket_1105.trig" // dataFileURL
            );
         
         } catch (QuadsOperationInTriplesModeException e) {
            
            return; // expected

         }

         throw new RuntimeException("Exception expected, but not encountered");

      }
      
      /**
       * Query: 
          <code>
            INSERT 
            { <http://example/s>  <http://example/p> <http://example/o> }
            WHERE
            {
               GRAPH <http://example/c> 
               { <http://example/s1>  <http://example/p1> <http://example/o1> } 
            }
          </code>
       * 
       * throws an {@link QuadsOperationInTriplesModeException} in triples mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_triples_update2() throws Exception {

         try {
            
            new UpdateTestHelper("ticket_1105_triples_update2", // testURI,
                  "ticket_1105_update2.rq",// queryFileURL
                  "ticket_1105.trig"// dataFileURL
            );
            
         } catch (QuadsOperationInTriplesModeException e) {

            return; // expected

         }

         throw new RuntimeException("Exception expected, but not encountered");

      }
      
      
      
      /**
       * Query: 
          <code>
            INSERT 
            { <http://example/s>  <http://example/p> <http://example/o> }
            WHERE
            {
               GRAPH ?g 
               { <http://example/s1>  <http://example/p1> <http://example/o1> } 
            }
          </code>
       * 
       * throws an {@link QuadsOperationInTriplesModeException} in triples mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_triples_update3() throws Exception {

         try {
            
            new UpdateTestHelper("ticket_1105_triples_update3", // testURI,
                  "ticket_1105_update3.rq",// queryFileURL
                  "ticket_1105.trig"// dataFileURL
            );
            
         } catch (QuadsOperationInTriplesModeException e) {

            return; // expected

         }

         throw new RuntimeException("Exception expected, but not encountered");

      }
      
      
      /**
       * Query: 
          <code>
            DELETE 
            { <http://example/s>  <http://example/p> <http://example/o> }
            WHERE
            {
               GRAPH <http://example/c> 
               { <http://example/s1>  <http://example/p1> <http://example/o1> } 
            }
          </code>
       * 
       * throws an {@link QuadsOperationInTriplesModeException} in triples mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_triples_update4() throws Exception {

         try {
            
            new UpdateTestHelper("ticket_1105_triples_update4", // testURI,
                  "ticket_1105_update4.rq",// queryFileURL
                  "ticket_1105.trig"// dataFileURL
            );

            
         } catch (QuadsOperationInTriplesModeException e) {

            return; // expected

         }

         throw new RuntimeException("Exception expected, but not encountered");

      }  

      /**
       * Query: 
          <code>
            DELETE 
            { <http://example/s>  <http://example/p> <http://example/o> }
            WHERE
            {
               GRAPH ?g 
               { <http://example/s1>  <http://example/p1> <http://example/o1> } 
            }
          </code>
       * 
       * throws an {@link QuadsOperationInTriplesModeException} in triples mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_triples_update5() throws Exception {

         try {
            
            new UpdateTestHelper("ticket_1105_triples_update5", // testURI,
                  "ticket_1105_update5.rq",// queryFileURL
                  "ticket_1105.trig"// dataFileURL
            );
            
         } catch (QuadsOperationInTriplesModeException e) {

            return; // expected

         }

         throw new RuntimeException("Exception expected, but not encountered");

      }       
      
      
      /**
       * Query: 
          <code>
            INSERT { <http://example/s>  <http://example/p> <http://example/o> }
            USING <http://example/c> 
            WHERE { <http://example/s1>  <http://example/p1> <http://example/o1> } 
          </code>
       * 
       * throws an {@link QuadsOperationInTriplesModeException} in triples mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_triples_update6() throws Exception {

         try {

            new UpdateTestHelper("ticket_1105_triples_update6", // testURI,
                  "ticket_1105_update6.rq",// queryFileURL
                  "ticket_1105.trig"// dataFileURL
            );
            
         } catch (QuadsOperationInTriplesModeException e) {

            return; // expected

         }

         throw new RuntimeException("Exception expected, but not encountered");

            
      }   
      
      
      /**
       * Query: 
          <code>
            SELECT ?s ?p ?o
            FROM NAMED <http://example/c>
            WHERE { ?s ?p ?o }
          </code>
       * 
       * throws an {@link QuadsOperationInTriplesModeException} in triples mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_triples_select1() throws Exception {
   
         try {
         
            new TestHelper("ticket_1105_triples_select1", // testURI,
                  "ticket_1105_select1.rq",// queryFileURL
                  "ticket_1105.trig",// dataFileURL
                  "ticket_1105.srx"// resultFileURL
            ).runTest();

         } catch (QuadsOperationInTriplesModeException e) {

            return; // expected
            
         }
         
         throw new RuntimeException("Exception expected, but not encountered");
      }   
         
      /**
       * Query: 
          <code>
            SELECT ?s ?p ?o
            WHERE { GRAPH <http://www.example/c> { ?s ?p ?o } }
          </code>
       * 
       * throws an {@link QuadsOperationInTriplesModeException} in triples mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_triples_select2() throws Exception {
   
         try {
            new TestHelper("ticket_1105_triples_select2", // testURI,
                  "ticket_1105_select2.rq",// queryFileURL
                  "ticket_1105.trig",// dataFileURL
                  "ticket_1105.srx"// resultFileURL
            ).runTest();
         } catch (QuadsOperationInTriplesModeException e) {
            
            return; // expected
                  
         }
         
         throw new RuntimeException("Exception expected, but not encountered");
         
      }
   
      /**
       * Query: 
          <code>
            SELECT ?s ?p ?o
            WHERE { GRAPH ?g { ?s ?p ?o } }
          </code>
       * 
       * throws an {@link QuadsOperationInTriplesModeException} in triples mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_triples_select3() throws Exception {
   
         try {
            
            new TestHelper("ticket_1105_triples_select3", // testURI,
                  "ticket_1105_select3.rq",// queryFileURL
                  "ticket_1105.trig",// dataFileURL
                  "ticket_1105.srx"// resultFileURL
            ).runTest();

         } catch (QuadsOperationInTriplesModeException e) {
         
            return; // expected
         
         }
         
         throw new RuntimeException("Exception expected, but not encountered");
   
      }
   }
   
   /**
    * Quads mode test suite.
    */
   public static class TestQuadsModeAPs extends TestTicket1105 {
      
      /**
       * Query: 
          <code>
            INSERT DATA
            { 
               GRAPH <http://example/c> 
               { <http://example/s>  <http://example/p> <http://example/o> } 
            }
          </code>
       * 
       * is parsed successfully in quads mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_quads_update1() throws Exception {

         new UpdateTestHelper("ticket_1105_quads_update1", // testURI,
               "ticket_1105_update1.rq",// queryFileURL
               "ticket_1105.trig" // dataFileURL
         );
 
      }

      
      /**
       * Query: 
          <code>
            INSERT 
            { <http://example/s>  <http://example/p> <http://example/o> }
            WHERE
            {
               GRAPH <http://example/c> 
               { <http://example/s1>  <http://example/p1> <http://example/o1> } 
            }
          </code>
       * 
       * is parsed successfully in quads mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_quads_update2() throws Exception {

         new UpdateTestHelper("ticket_1105_quads_update2", // testURI,
               "ticket_1105_update2.rq",// queryFileURL
               "ticket_1105.trig"// dataFileURL
         );

      }
      
      
      
      /**
       * Query: 
          <code>
            INSERT 
            { <http://example/s>  <http://example/p> <http://example/o> }
            WHERE
            {
               GRAPH ?g 
               { <http://example/s1>  <http://example/p1> <http://example/o1> } 
            }
          </code>
       * 
       * is parsed successfully in quads mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_quads_update3() throws Exception {

         new UpdateTestHelper("ticket_1105_quads_update3", // testURI,
               "ticket_1105_update3.rq",// queryFileURL
               "ticket_1105.trig"// dataFileURL
         );

      }
      
      
      /**
       * Query: 
          <code>
            DELETE 
            { <http://example/s>  <http://example/p> <http://example/o> }
            WHERE
            {
               GRAPH <http://example/c> 
               { <http://example/s1>  <http://example/p1> <http://example/o1> } 
            }
          </code>
       * 
       * is parsed successfully in quads mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_quads_update4() throws Exception {

         new UpdateTestHelper("ticket_1105_quads_update4", // testURI,
               "ticket_1105_update4.rq",// queryFileURL
               "ticket_1105.trig"// dataFileURL
         );

      }  
      

      /**
       * Query: 
          <code>
            DELETE 
            { <http://example/s>  <http://example/p> <http://example/o> }
            WHERE
            {
               GRAPH ?g 
               { <http://example/s1>  <http://example/p1> <http://example/o1> } 
            }
          </code>
       * 
       * is parsed successfully in quads mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_quads_update5() throws Exception {

         new UpdateTestHelper("ticket_1105_quads_update5", // testURI,
               "ticket_1105_update5.rq",// queryFileURL
               "ticket_1105.trig"// dataFileURL
         );

      }       
      
      
      /**
       * Query: 
          <code>
            INSERT { <http://example/s>  <http://example/p> <http://example/o> }
            USING <http://example/c> 
            WHERE { <http://example/s1>  <http://example/p1> <http://example/o1> } 
          </code>
       * 
       * is parsed successfully in quads mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_quads_update6() throws Exception {

         new UpdateTestHelper("ticket_1105_quads_update6", // testURI,
               "ticket_1105_update6.rq",// queryFileURL
               "ticket_1105.trig"// dataFileURL
         );

      }   
      
      /**
       * Query: 
          <code>
            SELECT ?s ?p ?o
            FROM NAMED <http://example/c>
            WHERE { ?s ?p ?o }
          </code>
       * 
       * runs fine in quads mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_quads_select1() throws Exception {

            new TestHelper("ticket_1105_quads_select1", // testURI,
                  "ticket_1105_select1.rq",// queryFileURL
                  "ticket_1105.trig",// dataFileURL
                  "ticket_1105.srx"// resultFileURL
            ).runTest();

      }   
         
      /**
       * Query: 
          <code>
            SELECT ?s ?p ?o
            WHERE { GRAPH <http://www.example/c> { ?s ?p ?o } }
          </code>
       * 
       * runs fine in quads mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_quads_select2() throws Exception {
   
         new TestHelper("ticket_1105_quads_select2", // testURI,
               "ticket_1105_select2.rq",// queryFileURL
               "ticket_1105.trig",// dataFileURL
               "ticket_1105.srx"// resultFileURL
         ).runTest();
   
      }
   
      /**
       * Query: 
          <code>
            SELECT ?s ?p ?o
            WHERE { GRAPH ?g { ?s ?p ?o } }
          </code>
       * 
       * runs fine in quads mode.
       * 
       * @throws Exception
       */
      public void test_ticket_1105_quads_select3() throws Exception {
   
         new TestHelper("ticket_1105_quads_select3", // testURI,
               "ticket_1105_select3.rq",// queryFileURL
               "ticket_1105.trig",// dataFileURL
               "ticket_1105.srx"// resultFileURL
         ).runTest();
   
      }      
   }

}
