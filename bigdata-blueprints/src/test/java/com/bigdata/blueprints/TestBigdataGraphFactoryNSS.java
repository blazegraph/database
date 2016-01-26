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
package com.bigdata.blueprints;


import org.junit.Test;

import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 * Class to test the BigdataGraphFactory against an embedded NSS to validate
 * client connection options and provide test suite coverage.
 * 
 * @author beebs
 *
 */
public class TestBigdataGraphFactoryNSS extends  AbstractTestNSSBlueprintsClient  {

	public TestBigdataGraphFactoryNSS() {
		super();
	}

	public void setUp() throws Exception {
		super.setUp();
	}

	@Test
	public void testBigdataGraphConnectServiceURL() {

		final String testURL = getServiceURL() + "/";

		testPrint("Connecting to Remote Repository at " + testURL);

		BigdataGraph testGraph = BigdataGraphFactory.connect(testURL);

		boolean hadException = false;

		try {
			testBigdataGraph(testGraph);
		} catch (Exception e) {
			hadException = true;
		}

		if (!hadException)
			fail("This test should not work.");
	}

	public void testBigdataGraphConnectSparqlEndpoint() {

		final String testURL = getServiceURL() + "/sparql";

		testPrint("Connecting to Remote Repository at " + testURL);

		BigdataGraph testGraph = BigdataGraphFactory.connect(testURL);

		try {
			testBigdataGraph(testGraph);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void testBigdataGraphConnectSparqlEndpointWithNamespace() {

		final String testURL = getServiceURL() + "/namespace/" + super.getNamespace() + "/sparql";

		testPrint("Connecting to Remote Repository at " + testURL);

		BigdataGraph testGraph = BigdataGraphFactory.connect(testURL);

		try {
			testBigdataGraph(testGraph);
		} catch (Exception e) {

			fail(e.toString());
		}
	}

	public void testBigdataGraphConnectHostPort() {

		BigdataGraph testGraph = BigdataGraphFactory.connect("localhost", super.getPort());

		try {
			testBigdataGraph(testGraph);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	@Override
	protected BigdataGraph getNewGraph(String file) throws Exception {
		
		final String testURL = getServiceURL() + "/sparql";

		testPrint("Connecting to Remote Repository at " + testURL);

		return new BigdataGraphClient(testURL);
	}

}
