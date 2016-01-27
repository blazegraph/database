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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import junit.framework.Test;


public class TestAskJsonTrac704 extends AbstractProtocolTest {

	public TestAskJsonTrac704(String name)  {
		super(name);
	}


	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(TestAskJsonTrac704.class,"test.*",TestMode.quads,TestMode.sids,TestMode.triples);
	}

	/**
	 * This does not work - trac 704
	 * @throws IOException
	 */
	public void testAskGetJSON() throws IOException {
		this.setAccept(BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON);
		final String response = serviceRequest("query",AbstractProtocolTest.ASK);
		assertTrue("Bad response: "+response,response.contains("boolean"));
		assertEquals(BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON, getResponseContentType());
	}

	/**
	 * This does not work - trac 704
	 * @throws IOException
	 */
	public void testAskPostEncodeJSON() throws IOException {
		setMethodisPostUrlEncodedData();
		testAskGetJSON() ;
	}
}
