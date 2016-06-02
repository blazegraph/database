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

package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import junit.framework.Test;


/**
 * Test to verify that the blazegraph Java client 
 * API works correctly for the correct parameter names 
 * for named graph access.
 * 
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1620" > getStatements()
 *      ignores includeInferred (REST API) </a>
 */
public class TestNamedGraphAccess extends AbstractProtocolTest {

    static public Test suite() {
        return ProxySuiteHelper.suiteWhenStandalone(TestNamedGraphAccess.class,"test.*", TestMode.quads);
    }
    public TestNamedGraphAccess(String name)  {
        super(name);
    }
    
    public void testNamedGraphAccess() throws IOException {
        
        final String namedGraph = "http://abc.com/id/graph/xyz";
        
        final String[] stmnts = {"<http://www.bigdata.com/Mike> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .", 
                                "<http://www.bigdata.com/Bryan> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .",
                                "<http://www.bigdata.com/Mike> <http://xmlns.com/foaf/0.1/knows> <http://www.bigdata.com/Bryan> .",
                                "<http://www.bigdata.com/Mike> <http://www.w3.org/2000/01/rdf-schema#label> \"Mike\" ."};

        final Header contentType = new BasicHeader("Content-Type", "text/plain");
        
        final Header[] headersUpload = {
                contentType
                };
        
        setHeaders(headersUpload);

        {
            // Upload some statements into the named graph <http://abc.com/id/graph/xyz>
            final StringBuilder data = new StringBuilder();
            
            for (int i = 0; i < stmnts.length - 1; i++) {
                
                data.append((stmnts[i]) + "\n");
                
            }
            
            setMethodisPost("text/plain", data.toString());
            
            final String[] paramValues = {"context-uri", namedGraph};
            
            serviceRequest(paramValues);
        }
        
        {
            // Upload the last statement into the default graph
            StringBuilder data = new StringBuilder();
            
            data.append(stmnts[stmnts.length - 1]);
            
            setMethodisPost("text/plain", data.toString());
            
            serviceRequest();
        }
        
        //
        resetDefaultOptions();
        
        final Header accept = new BasicHeader("Accept", "text/plain");
        
        final Header[] headersQuery = {
                accept
                };
        
        setHeaders(headersQuery);
        
        setMethodisPostUrlEncodedData();
        
        final String[] queryParamValues = {"query", "construct where {?s ?p ?o}", "default-graph-uri", namedGraph};
        
        final String response = serviceRequest(queryParamValues);
        
        // assert that the response contains all the triples expected in the named graph
        for (int i = 0; i < stmnts.length - 1; i++) {
            
            assertTrue(response.contains(stmnts[i]));
            
        }
        
        // assert that the response does not contain the triple from the default graph
        assertFalse(response.contains(stmnts[stmnts.length - 1]));
        
    }

}
