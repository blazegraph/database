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

import com.bigdata.rdf.store.AbstractTripleStore;


/**
 * Tests for HTree encoding problems for RDF language code literals.
 * In particular, this tests for issues occurring due to the creation
 * of RDF 1.1 language tag literals (incl. their datatype) when encoding
 * language tagged literals in the HTree.
 *  
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-2024">
 *      HTree encoding problems for RDF language code literals</a>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class TestTicket2024 extends AbstractDataDrivenSPARQLTestCase {

   public TestTicket2024() {
   }

   public TestTicket2024(String name) {
      super(name);
   }
   
   /**
    * Ticket: https://jira.blazegraph.com/browse/BLZG-2024
    * HTree encoding problems for RDF language code literals
    */
   public void test_ticket_2024a() throws Exception {
      new TestHelper(
            "ticket_bg2024a",// testURI,
            "ticket_bg2024a.rq",// queryFileURL
            "empty.trig",// dataFileURL
            "ticket_bg2024a.srx"// resultFileURL
         ).runTest();   
   }

   /**
    * Ticket: https://jira.blazegraph.com/browse/BLZG-2024
    * HTree encoding problems for RDF language code literals
    */
   public void test_ticket_2024b() throws Exception {
      new TestHelper(
            "ticket_bg2024b",// testURI,
            "ticket_bg2024b.rq",// queryFileURL
            "empty.trig",// dataFileURL
            "ticket_bg2024b.srx"// resultFileURL
         ).runTest();   
   }
   
   /**
    * Ticket: https://jira.blazegraph.com/browse/BLZG-2024
    * HTree encoding problems for RDF language code literals
    */
   public void test_ticket_2024c() throws Exception {
      new TestHelper(
            "ticket_bg2024c",// testURI,
            "ticket_bg2024c.rq",// queryFileURL
            "ticket_bg2024c.nt",// dataFileURL
            "ticket_bg2024c.srx"// resultFileURL
         ).runTest();   
   }
  
   @Override
   public Properties getProperties() {

       // Note: clone to avoid modifying!!!
       final Properties properties = (Properties) super.getProperties().clone();

       // enable inlining of literals to rule out that inlining has a negative effect
       properties.setProperty(AbstractTripleStore.Options.INLINE_TEXT_LITERALS,  "true");
       properties.setProperty(AbstractTripleStore.Options.INLINE_XSD_DATATYPE_LITERALS,  "true");
       
       return properties;

   }
}
