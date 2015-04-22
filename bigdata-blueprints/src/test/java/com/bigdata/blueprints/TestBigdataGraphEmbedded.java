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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

public class TestBigdataGraphEmbedded {
	
    protected static final transient Logger log = Logger.getLogger(AbstractTestNSSBlueprintsClient.class);


	private final String testJnl = "testJournal" + System.currentTimeMillis()
			+ ".jnl";
	private final String testData = "graph-example-1.xml";

	protected void loadTestGraph(BigdataGraph graph, String resource)
			throws IOException {
		GraphMLReader.inputGraph(graph, TestBigdataGraphEmbedded.class
				.getResourceAsStream(resource));

	}
	
	public void tearDown()
	{
		File f = new File(testJnl);
		f.deleteOnExit();
	}

	protected int vertexEdgeCount(BigdataGraph graph) {
		int v_e_cnt = 0;

		for (@SuppressWarnings("unused")
		Vertex v : graph.getVertices()) {

			v_e_cnt++;
		}

		for (@SuppressWarnings("unused")
		Edge e : graph.getEdges()) {

			v_e_cnt++;

		}

		return v_e_cnt;
	}

	protected boolean evaluatePreconditions(String file) {
		boolean retVal = false;

		File f = new File(file);

		if (f.exists()) {
			throw new RuntimeException("Running unit test with existing file."
					+ file);
		} else {
			retVal = true;
		}

		return retVal;

	}

	protected boolean evaluateFilePostconditions(String file) {

		File f = new File(file);

		return f.exists();
	}

	/**
	 * Create a new journal, load some data, close it, and then re-open and
	 * validate the total count of edges and vertices.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testGraphFactoryCreateNew() throws Exception {

		int loadCount = 0;

		{
			// The file should not exist.
			assert (evaluatePreconditions(testJnl));
			BigdataGraph graph = BigdataGraphFactory.create(testJnl);

			loadTestGraph(graph, testData);

			loadCount = vertexEdgeCount(graph);

			// The file should exist.
			assert (evaluateFilePostconditions(testJnl));

			graph.shutdown();
		}
		
		{

			BigdataGraph graph = BigdataGraphFactory.open(testJnl, false);

			int veCnt = vertexEdgeCount(graph); 
			
			//We should have the same total number
			assert(veCnt == loadCount);
			
			graph.shutdown();
		}

		fail("Not yet implemented");
	}

}
