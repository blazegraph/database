/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval.service;

import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.service.fts.FulltextSearchException;

/**
 * Data driven test suite for external full text search. At the time being,
 * this test suite requires a local Solr index set up at 
 * http://localhost:8983/solr/solrtest/select, with
 * a collection called "blazegraph" and the file fts-solr-collection.xml 
 * loaded into this collection. There's a small mvn project for automizing 
 * the Solr index setup checked in at /src/build/solr, see the README file
 * there.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestFulltextSearch extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestFulltextSearch() {
    }

    /**
     * @param name
     */ 
    public TestFulltextSearch(String name) {
        super(name);
    }


    /**
     * Verify simple fulltext search with small configuration.
     * 
     * @throws Exception
     */
    public void testSingleFulltextSearchMin() throws Exception {
       
       new TestHelper("fts-singleMin").runTest();
       
    }

    /**
     * Verify simple fulltext search with full configuration.
     * 
     * @throws Exception
     */
    public void testSingleFulltextSearchMax() throws Exception {
       
       new TestHelper("fts-singleMax").runTest();
       
    }

    /**
     * Verify simple fulltext search with full configuration, where the
     * magic vocabulary is already encapsulated into a SERVICE node.
     * 
     * @throws Exception
     */
    public void testSingleFulltextSearchUsingService() throws Exception {
       
       new TestHelper("fts-singleAsService").runTest();
       
    }

    /**
     * Verify passing of query via variable, which leads to multiple keyword
     * search requests.
     * 
     * @throws Exception
     */
    public void testMultiFulltextSearch() throws Exception {
       
       new TestHelper("fts-multiRequest").runTest();
       
    }
    
    /**
     * Verify that a subsequent join with a keyword result returns the
     * desired results.
     * 
     * @throws Exception
     */
    public void testJoinWithFulltextSearch() throws Exception {
       
       new TestHelper("fts-join").runTest();

    }

    /**
     * Test case comprising a complex WITH query that puts everything
     * together.
     * 
     * @throws Exception
     */
    public void testComplexWithQuery() throws Exception {
       
       new TestHelper("fts-complexWithQuery").runTest();

    }
    
    /**
     * Verify that a subsequent filter applied to a keyword result 
     * returns the desired result.
     * 
     * @throws Exception
     */
    public void testFilterOfFulltextSearch() throws Exception {
       
       new TestHelper("fts-filter").runTest();

    }
    
    
    /**
     * Make sure an exception is thrown in case the query string is empty.
     * 
     * @throws Exception
     */
    public void testRejectEmptySearchString() throws Exception {
       
       try {
       
          new TestHelper("fts-rejectEmptySearchString").runTest();
          
       } catch (Exception e) {
          
          if (e.getMessage().endsWith(FulltextSearchException.NO_QUERY_SPECIFIED)) {
             return; // expected
          }
          
       }
       
       throw new Exception("Missing search string not properly rejected");
    }

    /**
     * Make sure an exception is thrown in case the query string is empty.
     * 
     * @throws Exception
     */
    public void testRejectNoSearchString() throws Exception {
       
       try {
          
          new TestHelper("fts-rejectNoSearchString").runTest();
          
       } catch (Exception e) {
          
          if (e.getMessage().endsWith(FulltextSearchException.NO_QUERY_SPECIFIED)) {
             return; // expected
          }
          
       }
       
       throw new Exception("Missing search string not properly rejected");

    }
    
    /**
     * Make sure an exception is thrown in case the endpoint is empty
     * 
     * @throws Exception
     */
    public void testRejectEmptyEndpoint() throws Exception {
       
       try {
          
          new TestHelper("fts-rejectEmptyEndpoint").runTest();
          
       } catch (Exception e) {
          
          if (e.getMessage().endsWith(FulltextSearchException.NO_ENDPOINT_SPECIFIED)) {
             return; // expected
          }
          
       }
       
       throw new Exception("Empty endpoint not properly rejected");

    }

    
    /**
     * Make sure an exception is thrown in case the endpoint is empty
     * 
     * @throws Exception
     */
    public void testRejectNoEndpoint() throws Exception {
       
       try {
          
          new TestHelper("fts-rejectNoEndpoint").runTest();
          
       } catch (Exception e) {
          
          if (e.getMessage().endsWith(FulltextSearchException.NO_ENDPOINT_SPECIFIED)) {
             return; // expected
          }
          
       }
       
       throw new Exception("Missing endpoint not properly rejected");

    }
    
    /**
     * Verify that there is a proper error message when injection variable
     * is not yet bound.
     * 
     * @throws Exception
     */
    public void testVariableInjectionFailing() throws Exception {
       
       try {

          new TestHelper("fts-variableInjectionFailing").runTest();

       } catch (Exception e) {
          
          if (e.getMessage().contains(FulltextSearchException.SERVICE_VARIABLE_UNBOUND)) {
             return; // expected
          }
       }
       
       throw new RuntimeException("Test runs through, but should not.");
    }
    
    /**
     * Casting of non-URI to URI results in proper exception.
     * 
     * @throws Exception
     */
    public void testTypeCastException() throws Exception {
       
       try {
       
          new TestHelper("fts-typeCastException").runTest();

       } catch (Exception e) {
          
          if (e.getMessage().contains(FulltextSearchException.TYPE_CAST_EXCEPTION)) {
             return; // expected
          }
       }
       
       throw new RuntimeException("Test runs through, but should not.");
    }

}
