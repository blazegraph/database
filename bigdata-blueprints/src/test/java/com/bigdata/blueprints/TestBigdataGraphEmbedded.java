package com.bigdata.blueprints;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

public class TestBigdataGraphEmbedded {

	private final String testJnl = "testJournal" + System.currentTimeMillis()
			+ ".jnl";
	private final String testData = "graph-example-1.xml";

	protected void loadTestGraph(BigdataGraph graph, String resource)
			throws IOException {
		GraphMLReader.inputGraph(graph, TestBigdataGraphEmbedded.class
				.getResourceAsStream(resource));

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
