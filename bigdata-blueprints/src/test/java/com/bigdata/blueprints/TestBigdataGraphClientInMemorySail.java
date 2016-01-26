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

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.BigdataSailNSSWrapper;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.TransactionalGraphTestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;

/**
 * Blueprints test suite for a client communicating with the server over the
 * REST API.
 */
public class TestBigdataGraphClientInMemorySail extends AbstractTestBigdataGraph {

    private static final transient Logger log = Logger.getLogger(TestBigdataGraphClientInMemorySail.class);
    
    /**
     * 
     */
    public TestBigdataGraphClientInMemorySail() {
    }

    /**
     * @param name
     */
    public TestBigdataGraphClientInMemorySail(String name) {
        super(name);
    }

    @Override
    protected GraphTest newBigdataGraphTest() {
        return new BigdataGraphTest();
    }
  /* 
   //Currently there is not transaction support in the remote client.
    public void testTransactionalGraphTestSuite() throws Exception {
        final GraphTest test = newBigdataGraphTest();
        test.stopWatch();
        test.doTestSuite(new TransactionalGraphTestSuite(test));
        GraphTest.printTestPerformance("TransactionalGraphTestSuite",
                test.stopWatch());
    }
   */
//    public void testAddVertexProperties() throws Exception {
//        final BigdataGraphTest test = new BigdataGraphTest();
//        test.stopWatch();
//        final BigdataTestSuite testSuite = new BigdataTestSuite(test);
//        try {
//            testSuite.testVertexEquality();
//        } finally {
//            test.shutdown();
//        }
//
//    }
//
//    private static class BigdataTestSuite extends TestSuite {
//        
//        public BigdataTestSuite(final BigdataGraphTest graphTest) {
//            super(graphTest);
//        }
//        
//        public void testVertexEquality() {
//            Graph graph = graphTest.generateGraph();
//
//            if (!graph.getFeatures().ignoresSuppliedIds) {
//                Vertex v = graph.addVertex(graphTest.convertId("1"));
//                Vertex u = graph.getVertex(graphTest.convertId("1"));
//                assertEquals(v, u);
//            }
//
//            this.stopWatch();
//            Vertex v = graph.addVertex(null);
//            assertNotNull(v);
//            Vertex u = graph.getVertex(v.getId());
//            assertNotNull(u);
//            assertEquals(v, u);
//            printPerformance(graph.toString(), 1, "vertex added and retrieved", this.stopWatch());
//
//            assertEquals(graph.getVertex(u.getId()), graph.getVertex(u.getId()));
//            assertEquals(graph.getVertex(v.getId()), graph.getVertex(u.getId()));
//            assertEquals(graph.getVertex(v.getId()), graph.getVertex(v.getId()));
//
//            graph.shutdown();
//        }
//    }
    
    private class BigdataGraphTest extends GraphTest {

		@Override
		public void doTestSuite(TestSuite testSuite) throws Exception {
	        for (Method method : testSuite.getClass().getDeclaredMethods()) {
	            if (method.getName().startsWith("test")) {
	                log.warn("Testing " + method.getName() + "...");
	                try {
		                method.invoke(testSuite);
	                } catch (Exception ex) {
	                	ex.getCause().printStackTrace();
	                	throw ex;
	                } finally {
		                shutdown();
	                }
	            }
	        }
		}
		
		private Map<String,BigdataSailNSSWrapper> testSails = new LinkedHashMap<String, BigdataSailNSSWrapper>();

		@Override
		public Graph generateGraph(final String key) {
			
			try {
	            if (testSails.containsKey(key) == false) {
	                final BigdataSail testSail = getSail();
	                testSail.initialize();
	                final BigdataSailNSSWrapper nss = new BigdataSailNSSWrapper(testSail);
	                nss.init();
	                testSails.put(key, nss);
	            }
	            
				final BigdataSailNSSWrapper nss = testSails.get(key);
				final BigdataGraph graph = new BigdataGraphClient(nss.m_repo.getRepositoryForDefaultNamespace());
				
				return graph;
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			
		}

		@Override
		public Graph generateGraph() {
			
			return generateGraph(null);
		}
		
		public void shutdown() {
		    for (BigdataSailNSSWrapper wrapper : testSails.values()) {
		        try {
    		        wrapper.shutdown();
    		        wrapper.getSail().__tearDownUnitTest();
		        } catch (Exception ex) {
		            throw new RuntimeException(ex);
		        }
		    }
		    testSails.clear();
		}
		
    	
    }

    public static final void main(final String[] args) throws Exception {
        
        final String url = "http://localhost:9999/bigdata/sparql";
        
        final BigdataGraph graph = BigdataGraphFactory.connect(url);
        
        for (Vertex v : graph.getVertices()) {
            
            if(log.isInfoEnabled())
            	log.info(v);
            
        }
        
        for (Edge e : graph.getEdges()) {
            
            if(log.isInfoEnabled())
            	log.info(e);
            
        }
        
    }

}
