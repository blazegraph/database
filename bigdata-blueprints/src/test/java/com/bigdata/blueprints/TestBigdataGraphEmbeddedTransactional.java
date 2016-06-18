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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.TransactionalGraphTestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 */
public class TestBigdataGraphEmbeddedTransactional extends AbstractTestBigdataGraph {

    protected static final transient Logger log = Logger.getLogger(TestBigdataGraphEmbeddedTransactional.class);
    
    /**
     * 
     */
    public TestBigdataGraphEmbeddedTransactional() {
    }

    /**
     * @param name
     */
    public TestBigdataGraphEmbeddedTransactional(String name) {
        super(name);
    }

    public void testTransactionalGraphTestSuite() throws Exception {
        final GraphTest test = newBigdataGraphTest();
        test.stopWatch();
        test.doTestSuite(new TransactionalGraphTestSuite(test));
        GraphTest.printTestPerformance("TransactionalGraphTestSuite",
                test.stopWatch());
    }
    
//  public void testGraphSuite() throws Exception {
//  final GraphTest test = newBigdataGraphTest();
//  test.stopWatch();
//    test.doTestSuite(new GraphTestSuite(test));
//    GraphTest.printTestPerformance("GraphTestSuite", test.stopWatch());
//}


//    public void testAddVertexProperties() throws Exception {
//        final BigdataGraphTest test = new BigdataGraphTest();
//        test.stopWatch();
//        final BigdataTestSuite testSuite = new BigdataTestSuite(test);
//        try {
//            testSuite.testAddVertexProperties();
//        } finally {
//            test.shutdown();
//        }
//        
//    }
    

    
    private static class BigdataTestSuite extends TestSuite {
        
        public BigdataTestSuite(final BigdataGraphTest graphTest) {
            super(graphTest);
        }
        
        public void testAddVertexProperties() throws Exception {
            BigdataGraphEmbedded graph = (BigdataGraphEmbedded) graphTest.generateGraph();
            if (graph.getFeatures().supportsVertexProperties) {
                Vertex v1 = graph.addVertex(graphTest.convertId("1"));
                Vertex v2 = graph.addVertex(graphTest.convertId("2"));
                
//                graph.commit();
                
                for (Vertex v : graph.getVertices()) {
                    if(log.isInfoEnabled())
                    	log.info(v);
                }

                if(log.isInfoEnabled())
                	log.info("\n"+graph.dumpStore());
                
                if (graph.getFeatures().supportsStringProperty) {
                    v1.setProperty("key1", "value1");
                    graph.commit();
                    if(log.isInfoEnabled())
                       log.info("\n"+graph.dumpStore());
                    assertEquals("value1", v1.getProperty("key1"));
                }

                if (graph.getFeatures().supportsIntegerProperty) {
                    v1.setProperty("key2", 10);
                    v2.setProperty("key2", 20);

                    assertEquals(10, v1.getProperty("key2"));
                    assertEquals(20, v2.getProperty("key2"));
                }

            }
            graph.shutdown();
        }

        
        private void trySetProperty(final Element element, final String key, final Object value, final boolean allowDataType) {
            boolean exceptionTossed = false;
            try {
                element.setProperty(key, value);
            } catch (Throwable t) {
                exceptionTossed = true;
                if (!allowDataType) {
                    assertTrue(t instanceof IllegalArgumentException);
                } else {
                    fail("setProperty should not have thrown an exception as this data type is accepted according to the GraphTest settings.\n\n" +
                            "Exception was " + t);
                }
            }

            if (!allowDataType && !exceptionTossed) {
                fail("setProperty threw an exception but the data type should have been accepted.");
            }
        }

        private void tryGetProperty(final Element element, final String key, final Object value, final boolean allowDataType) {

            if (allowDataType) {
                assertEquals(element.getProperty(key), value);
            }
        }



    }

    
    protected GraphTest newBigdataGraphTest() {
        return new BigdataGraphTest();
    }
    
    private class BigdataGraphTest extends GraphTest {

        private List<String> exclude = Arrays.asList(new String[] {
           // this one creates a deadlock, no way around it
           "testTransactionIsolationCommitCheck"  
        });
        
		@Override
		public void doTestSuite(TestSuite testSuite) throws Exception {
	        for (Method method : testSuite.getClass().getDeclaredMethods()) {
	            if (method.getName().startsWith("test")) {
	                if (exclude.contains(method.getName())) {
	                	if(log.isInfoEnabled())
	                		log.info("Skipping test " + method.getName() + ".");
	                } else {
	                	if(log.isInfoEnabled())
	                		log.info("Testing " + method.getName() + "...");
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
		}
		
		private Map<String,BigdataGraphEmbedded> testGraphs = new LinkedHashMap<String, BigdataGraphEmbedded>();

		@Override
		public Graph generateGraph(final String key) {
			
			try {
	            if (testGraphs.containsKey(key) == false) {
	                final Properties props = getProperties();
	                final BigdataSail testSail = getSail(props);
	                testSail.initialize();
	                final BigdataSailRepository repo = new BigdataSailRepository(testSail);
	                final BigdataGraphEmbedded graph = new BigdataGraphEmbedded(
	                        repo, BigdataRDFFactory.INSTANCE, props) {
	    
	                    /**
	                     * Test cases have weird semantics for shutdown.
	                     */
	                    @Override
	                    public void shutdown() {
	                        try {
//	                          if (cxn != null) {
//	                              cxn.commit();
//	                              cxn.close();
//	                              cxn = null;
//	                          }
//	                          commit();
//	                          super.shutdown();
	                        } catch (Exception ex) {
	                            throw new RuntimeException(ex);
	                        }
	                    }
	                    
	                };
                   testGraphs.put(key, graph);
	            }
	            
				BigdataGraph graph = testGraphs.get(key); //testSail; //getSail();
				
//				if (!graph.repo.getSail().isOpen()) {
//				    
//				    final BigdataSail sail = reopenSail(graph.repo.getSail());
//				    sail.initialize();
//                    final BigdataSailRepository repo = new BigdataSailRepository(sail);
//                    graph = new BigdataGraphEmbedded(repo);// {
//                    testGraphs.put(key, graph);
//				    
//				}
				
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
		    for (BigdataGraphEmbedded sail : testGraphs.values()) {
		        ((BigdataSail)sail.repo.getSail()).__tearDownUnitTest();
		    }
		    testGraphs.clear();
		}
		
    	
    }
    
    protected static void printVerticesEdges(BigdataGraph graph, String message)
    {
            if(log.isInfoEnabled())
            {
            	log.info(message);
            	log.info("graph:");
            }
            
            for (Vertex v : graph.getVertices()) {
               
            	if(log.isInfoEnabled())
            		log.info(v);
                
            }
            
            for (Edge e : graph.getEdges()) {
               
            	if(log.isInfoEnabled())
            		log.info(e);
                
            }
    	
    }

    public static final void main(final String[] args) throws Exception {

        { // create an in-memory instance
            
            final BigdataGraph graph = BigdataGraphFactory.create();
            
            GraphMLReader.inputGraph(graph, TestBigdataGraphEmbeddedTransactional.class.getResourceAsStream("graph-example-1.xml"));
            
            printVerticesEdges(graph,"data loaded (in-memory)");
            
            graph.shutdown();
            
        }
        
        final File jnl = File.createTempFile("bigdata", ".jnl");
        
        { // create a persistent instance
            
            final BigdataGraph graph = BigdataGraphFactory.open(jnl.getAbsolutePath(), true);
            
            GraphMLReader.inputGraph(graph, TestBigdataGraphEmbeddedTransactional.class.getResourceAsStream("graph-example-1.xml"));
            
            printVerticesEdges(graph,"data loaded (persistent)");
            
            graph.shutdown();
            
        }
        
        { // re-open the persistent instance
            
            final BigdataGraph graph = BigdataGraphFactory.open(jnl.getAbsolutePath(), true);
            
            printVerticesEdges(graph,"persistent graph re-opened");
            
            graph.shutdown();
            
        }
        
        jnl.delete();
        
    }


}
