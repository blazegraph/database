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

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

public class TestBigdataGraphFactoryNSS extends  AbstractTestNSSBlueprintsClient  {

	private String serviceURL = "http://localhost:9999/bigdata";

	public TestBigdataGraphFactoryNSS() {
		super();
	}

	public void setUp() throws Exception {
		super.setUp();
		serviceURL = super.getServiceURL();
	}

	@Test
	public void testBigdataGraphConnectServiceURL() {
		// FIXME: This should start an embedded NSS and test against that.
		// For now, you must start run "ant start-blazegraph". Then
		final String testURL = serviceURL + "/";

		System.err.println("Connecting to Remote Repository at " + testURL);

		// log.info("Connecting to Remote Repository at " + testURL);

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
		// FIXME: This should start an embedded NSS and test against that.
		// For now, you must start run "ant start-blazegraph". Then
		// final String testURL = this.getServiceUrl();
		final String testURL = serviceURL + "/sparql";

		System.err.println("Connecting to Remote Repository at " + testURL);

		// log.info("Connecting to Remote Repository at " + testURL);

		BigdataGraph testGraph = BigdataGraphFactory.connect(testURL);

		try {
			testBigdataGraph(testGraph);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void testBigdataGraphConnectSparqlEndpointWithNamespace() {
		// FIXME: This should start an embedded NSS and test against that.
		// For now, you must start run "ant start-blazegraph". Then
		// final String testURL = this.getServiceUrl();
		final String testURL = serviceURL + "/namespace/" + super.getNamespace() + "/sparql";

		System.err.println("Connecting to Remote Repository at " + testURL);

		// log.info("Connecting to Remote Repository at " + testURL);

		BigdataGraph testGraph = BigdataGraphFactory.connect(testURL);

		try {
			testBigdataGraph(testGraph);
		} catch (Exception e) {

			fail(e.toString());
		}
	}

	public void testBigdataGraphConnectHostPort() {
		// FIXME: This should start an embedded NSS and test against that.
		// For now, you must start run "ant start-blazegraph". Then
		// final String testURL = this.getServiceUrl();

		// log.info("Connecting to Remote Repository at " + testURL);

		BigdataGraph testGraph = BigdataGraphFactory.connect("localhost", super.getPort());

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
			System.err.println(v);
		}
		for (Edge e : testGraph.getEdges()) {
			System.err.println(e);
		}

		testGraph.shutdown();

	}
	
	@Override
	protected BigdataGraph getNewGraph(String file) throws Exception {
		
		final String testURL = serviceURL + "/sparql";

		System.err.println("Connecting to Remote Repository at " + testURL);

		// log.info("Connecting to Remote Repository at " + testURL);

		return new BigdataGraphClient(testURL);
	}

	@Override
	protected BigdataGraph loadGraph(String file) throws Exception {
		
		return getNewGraph(file);
	}

}
