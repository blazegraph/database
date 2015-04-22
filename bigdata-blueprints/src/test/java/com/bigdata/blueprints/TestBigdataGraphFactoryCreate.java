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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.junit.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

public class TestBigdataGraphFactoryCreate extends TestCase {

	/**
	 * To pass the test the file must not exist prior to creation, it must be
	 * created, closed, and re-opened with the same graph.
	 */
	@Test
	public void testBigdataGraphFactoryCreate() {

		final String testJnl = "testCreate.jnl";

		final File testJnlFile = new File(testJnl);

		final String example = "graph-example-1.xml";

		if (testJnlFile.exists()) {
			fail("File " + testJnl + " exists prior to test.");
		}

		System.err.println("Creating embedded repoistory with journal"
				+ testJnl);
		
		int v_cnt = 0;
		int e_cnt = 0;

		// log.info("Connecting to Remote Repository at " + serviceURL);
		{
			BigdataGraph testGraph = null;
			try {
				testGraph = BigdataGraphFactory.create(testJnl);
			} catch (Exception e2) {
				fail(e2.toString());
			}

			try {
				GraphMLReader.inputGraph(testGraph, this.getClass()
						.getResourceAsStream(example));
			} catch (IOException e1) {
				fail(e1.toString());
			}

			v_cnt = vertexCount(testGraph);
			e_cnt = edgeCount(testGraph);

			testGraph.shutdown();
		}

		{
			final File postTestFile = new File(testJnl);

			if (!postTestFile.exists()) {
				fail(testJnl + " was not created during embedded graph test.");
			}

			BigdataGraph newGraph = null;

			postTestFile.deleteOnExit();

			try {
				newGraph = BigdataGraphFactory.create(testJnl);
			} catch (Exception e) {
				fail(e.toString());
			}
			
			assert(newGraph != null);

			if (v_cnt != vertexCount(newGraph)) {
				fail("Vertex count" + vertexCount(newGraph)
						+ " is not equal original vertex count of " + v_cnt);
			}

			if (e_cnt != edgeCount(newGraph)) {
				fail("Edge count" + edgeCount(newGraph)
						+ " is not equal original edge count of " + e_cnt);
			}

			assertTrue(v_cnt == vertexCount(newGraph)
					&& e_cnt == edgeCount(newGraph));
		}

	}

	protected int vertexCount(BigdataGraph g) {
		int v_cnt = 0;
		for (Vertex v : g.getVertices()) {
			System.err.println(v);
			v_cnt++;
		}

		return v_cnt;

	}

	protected int edgeCount(BigdataGraph g) {
		int e_cnt = 0;
		for (Edge e : g.getEdges()) {
			System.err.println(e);
			e_cnt++;
		}

		return e_cnt;
	}

}
