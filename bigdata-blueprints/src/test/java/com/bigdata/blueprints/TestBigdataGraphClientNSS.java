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
package com.bigdata.blueprints;

import org.junit.Test;

import com.bigdata.rdf.sail.webapp.ProxySuiteHelper;
import com.bigdata.rdf.sail.webapp.health.TestNSSHealthCheck;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 * Test class for BigdataGraphClient against an embedded NSS provided for test
 * suite coverage of clients.
 * 
 * @author beebs
 *
 */
public class TestBigdataGraphClientNSS extends AbstractTestNSSBlueprintsClient  {

	private String serviceURL = "http://localhost:9999/bigdata";

	public TestBigdataGraphClientNSS() {
		super();
	}
	
	public void setUp() throws Exception {
		super.setUp();
		serviceURL = super.getServiceURL();
	}

	@Test
	public void testBigdataGraphConnectServiceURL() {

		final String testURL = serviceURL + "/";

		log.info("Connecting to Remote Repository at " + testURL);

		BigdataGraph testGraph = new BigdataGraphClient(testURL);

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

		final String testURL = serviceURL + "/sparql";

		log.info("Connecting to Remote Repository at " + testURL);

		BigdataGraph testGraph = new BigdataGraphClient(testURL);

		try {
			testBigdataGraph(testGraph);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void testBigdataGraphConnectSparqlEndpointWithNamespace() {

		final String testURL = serviceURL + "/namespace/" + super.getNamespace() + "/sparql";

		log.info("Connecting to Remote Repository at " + testURL);

		BigdataGraph testGraph = new BigdataGraphClient(testURL);

		try {
			testBigdataGraph(testGraph);
		} catch (Exception e) {

			fail(e.toString());
		}
	}

	protected void testBigdataGraph(BigdataGraph testGraph) throws Exception {

		final String example = "graph-example-1.xml";

		GraphMLReader.inputGraph(testGraph, this.getClass()
				.getResourceAsStream(example));

		for (Vertex v : testGraph.getVertices()) {
			log.info(v.toString());
		}
		for (Edge e : testGraph.getEdges()) {
			log.info(e.toString());
		}

		testGraph.shutdown();

	}

	@Override
	protected BigdataGraph getNewGraph(String file) throws Exception {
		
		final String testURL = serviceURL + "/sparql";

		log.info("Connecting to Remote Repository at " + testURL);

		return new BigdataGraphClient(testURL);
	}

	@Override
	protected BigdataGraph loadGraph(String file) throws Exception {
		
		return getNewGraph(file);
	}

}
