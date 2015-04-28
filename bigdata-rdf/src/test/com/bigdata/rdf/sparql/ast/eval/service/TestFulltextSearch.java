/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Data driven test suite for external full text search.
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
     * Verify simple fulltext search with full configuration
     * 
     * @throws Exception
     */
    // TODO
    public void testSingleFulltextSearch() throws Exception {
       
       new TestHelper("fts-single").runTest();
       
    }
    
    /**
     * Verify simple fulltext search with full configuration, where the
     * magic vocabulary is already encapsulated into a SERVICE node.
     * 
     * @throws Exception
     */
    // TODO
    public void testSingleFulltextSearchUsingService() throws Exception {
       
       new TestHelper("fts-single-as-service").runTest();
       
    }

    /**
     * Verify passing of query via variable, which leads to multiple keyword
     * search requests.
     * 
     * @throws Exception
     */
    // TODO
    public void testMultiFulltextSearch() throws Exception {
       
       new TestHelper("fts-multi").runTest();
       
    }
    
    /**
     * Verify that a subsequent join with a keyword result returns the
     * desired results.
     * 
     * @throws Exception
     */
    // TODO
    public void testJoinWithFulltextSearch() throws Exception {
       
       new TestHelper("fts-join").runTest();

    }

    /**
     * Verify that a subsequent filter applied to a keyword result 
     * returns the desired result.
     * 
     * @throws Exception
     */
    // TODO
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
     * Verify that default values are used for the endpoint type,
     * the params, and the target type, in case they are left unspecified.
     * 
     * @throws Exception
     */
    // TODO
    public void testDefaultValuesUsage() throws Exception {
       
       new TestHelper("fts-defaultValues").runTest();

    }

    /**
     * Verify that default values are properly overridden for the
     * params and the target type parameter.
     * 
     * @throws Exception
     */
    // TODO
    public void testDefaultValuesOverride() throws Exception {
       
       new TestHelper("fts-defaultValuesOverride").runTest();

    }
    
}