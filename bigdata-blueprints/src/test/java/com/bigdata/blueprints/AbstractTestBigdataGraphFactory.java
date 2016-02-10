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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 * Abstract Test class for Blueprints client test coverage. Creates a new KB,
 * Loads a Graph, closes it, re-opens, and validates the same number of edges
 * and vertices exist. Implementations do this for the different client
 * connection and persistence methods, i.e. BigdataGraphClient,
 * BigdataGraphEmbedded, etc.
 * 
 * @author beebs
 * 
 */
public abstract class AbstractTestBigdataGraphFactory extends TestCase {

	private static final transient Logger log = Logger
			.getLogger(AbstractTestBigdataGraphFactory.class);

	public static void testPrint(Object message) {
		assert (message != null);

		if (log.isInfoEnabled())
			log.info(message.toString());
	}

	protected final String testJnl = "testJournal-" + System.currentTimeMillis()
			+ ".jnl";

	protected final String testData = "graph-example-1.xml";

	public AbstractTestBigdataGraphFactory() {
		super();
	}

	public AbstractTestBigdataGraphFactory(String name) {
		super(name);
	}

	/**
	 * Add test specific method to generate a new graph from a file.
	 * 
	 * @return
	 */
	protected abstract BigdataGraph getNewGraph(String file) throws Exception;

	/**
	 * Add test-specific method to load a graph from the file.
	 * 
	 * @param file
	 * @return
	 */
	protected abstract BigdataGraph loadGraph(String file) throws Exception;

	protected void loadTestGraph(BigdataGraph graph, String resource)
			throws IOException {
		
		GraphMLReader
				.inputGraph(graph, this.getClass().getClassLoader().
							getResourceAsStream(resource));

	}

	public void tearDown() throws Exception {
		File f = new File(testJnl);
		f.delete();
	}

	protected int vertexEdgeCount(BigdataGraph graph) {
		int v_e_cnt = 0;

		if (graph == null)
			return 0;

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

	protected void deleteFile(String file) {
		File f = new File(file);
		f.delete();
	}

	protected boolean evaluateFilePostconditions(String file) {

		File f = new File(file);
		boolean retval = false;

		if (this instanceof AbstractTestNSSBlueprintsClient) {
			// This check is not relevant for NSS test cases.
			retval = true;
		} else
			retval = f.exists();

		return retval;
	}

	/**
	 * Create a new journal, load some data, close it, and then re-open and
	 * validate the total count of edges and vertices.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testGraphFactoryCreateNew() throws Exception {

		testPrint("Testing Graph Creation.");

		int loadCount = 0;

		{
			// The file should not exist.
			assert (evaluatePreconditions(testJnl));
			BigdataGraph graph = getNewGraph(testJnl);

			loadTestGraph(graph, testData);

			try {
				loadCount = vertexEdgeCount(graph);
			} catch (Exception E) {
				loadCount = 0;
				log.warn(E.toString());
			}

			testPrint("Total Edge and Vertex Count: " + loadCount);


			// The file should exist.
			assert (evaluateFilePostconditions(testJnl));

			graph.shutdown();
		}

		openAndCheckGraphCount(testJnl, loadCount);

		deleteFile(testJnl);

	}

	/**
	 * Open an existing journal and validate the count equals the loadCount
	 * parameter. Tests that the graph is persisted to disk.
	 * 
	 * @param file
	 * @param loadCount
	 * @throws Exception
	 */
	protected void openAndCheckGraphCount(String file, int loadCount)
			throws Exception {

		BigdataGraph graph = loadGraph(file);

		int veCnt = vertexEdgeCount(graph);

		testPrint("Total Edge and Vertex Count: " + veCnt);

		// We should have the same total number
		assert (veCnt == loadCount);

		graph.shutdown();
	}
	
}
