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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import junit.framework.Test;


/**
 * This test class exercises protocol issues (mimetypes, parameters etc)
 * as at release 1.2.3; prior to addressing protocol related trac items such
 * as 704, 697, 711
 * @author jeremycarroll
 *
 */
public class TestRelease123Protocol extends AbstractProtocolTest{

	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(TestRelease123Protocol.class,"test.*", TestMode.quads,TestMode.sids,TestMode.triples);
	}
	public TestRelease123Protocol(String name)  {
		super(name);
	}
	
	public void testSelectGetXML() throws IOException {
		assertTrue(serviceRequest("query",SELECT).contains("</sparql>"));
		assertEquals(BigdataRDFServlet.MIME_SPARQL_RESULTS_XML, getResponseContentType());
	}

	public void testSelectGetJSON() throws IOException {
		this.setAccept(BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON);
		assertTrue(serviceRequest("query",SELECT).contains("results"));
		assertEquals(BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON, getResponseContentType());
		
	}
	public void testAskGetXML() throws IOException {
		assertTrue(serviceRequest("query",ASK).contains("</sparql>"));
		assertEquals(BigdataRDFServlet.MIME_SPARQL_RESULTS_XML, getResponseContentType());
	}

	public void testSelectPostEncodeXML() throws IOException {
		setMethodisPostUrlEncodedData();
		testSelectGetXML();
	}

	public void testSelectPostEncodeJSON() throws IOException {
		setMethodisPostUrlEncodedData();
		testSelectGetJSON();
	}
	public void testAskPostEncodeXML() throws IOException {
		setMethodisPostUrlEncodedData();
		testAskGetXML();
	}
	

	public void testUpdateGet() throws IOException {
		// This should not cause an update - in release 1.2.3 it returns a service description
		// which seems a little strange but is not wrong; this test will also allow a 4XX response.
		checkUpdate(false);
		setAllow400s();
		serviceRequest("update",update);
		checkUpdate(false);
	}
	public void testUpdatePostEncode() throws IOException {
		checkUpdate(false);
		setMethodisPostUrlEncodedData();
		serviceRequest("update",update);
		checkUpdate(true);
	}


}
