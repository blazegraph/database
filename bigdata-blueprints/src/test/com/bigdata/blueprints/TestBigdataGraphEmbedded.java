/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraphTestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.MockSerializable;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 */
public class TestBigdataGraphEmbedded extends AbstractTestBigdataGraph {

    protected static final transient Logger log = Logger.getLogger(TestBigdataGraphEmbedded.class);
    
    /**
     * 
     */
    public TestBigdataGraphEmbedded() {
    }

    /**
     * @param name
     */
    public TestBigdataGraphEmbedded(String name) {
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


//    public void testDataTypeValidationOnProperties() throws Exception {
//        final BigdataGraphTest test = new BigdataGraphTest();
//        test.stopWatch();
//        final BigdataTestSuite testSuite = new BigdataTestSuite(test);
//        try {
//            testSuite.testDataTypeValidationOnProperties();
//        } finally {
//            test.shutdown();
//        }
//        
//    }
    
    private static class BigdataTestSuite extends TestSuite {
        
        public BigdataTestSuite(final BigdataGraphTest graphTest) {
            super(graphTest);
        }
        
        public void testDataTypeValidationOnProperties() {
            
            final Graph graph = graphTest.generateGraph();
            if (graph.getFeatures().supportsElementProperties() && !graph.getFeatures().isWrapper) {
                final Vertex vertexA = graph.addVertex(null);
                final Vertex vertexB = graph.addVertex(null);
                final Edge edge = graph.addEdge(null, vertexA, vertexB, graphTest.convertLabel("knows"));

                trySetProperty(vertexA, "keyString", "value", graph.getFeatures().supportsStringProperty);
                trySetProperty(edge, "keyString", "value", graph.getFeatures().supportsStringProperty);

                trySetProperty(vertexA, "keyInteger", 100, graph.getFeatures().supportsIntegerProperty);
                trySetProperty(edge, "keyInteger", 100, graph.getFeatures().supportsIntegerProperty);

                trySetProperty(vertexA, "keyLong", 10000L, graph.getFeatures().supportsLongProperty);
                trySetProperty(edge, "keyLong", 10000L, graph.getFeatures().supportsLongProperty);

                trySetProperty(vertexA, "keyDouble", 100.321d, graph.getFeatures().supportsDoubleProperty);
                trySetProperty(edge, "keyDouble", 100.321d, graph.getFeatures().supportsDoubleProperty);

                trySetProperty(vertexA, "keyFloat", 100.321f, graph.getFeatures().supportsFloatProperty);
                trySetProperty(edge, "keyFloat", 100.321f, graph.getFeatures().supportsFloatProperty);

                trySetProperty(vertexA, "keyBoolean", true, graph.getFeatures().supportsBooleanProperty);
                trySetProperty(edge, "keyBoolean", true, graph.getFeatures().supportsBooleanProperty);

                System.err.println("supportsSerializableObjectProperty" + graph.getFeatures().supportsSerializableObjectProperty);
                trySetProperty(vertexA, "keyDate", new Date(), graph.getFeatures().supportsSerializableObjectProperty);
                trySetProperty(edge, "keyDate", new Date(), graph.getFeatures().supportsSerializableObjectProperty);

                final ArrayList<String> listA = new ArrayList<String>();
                listA.add("try1");
                listA.add("try2");

                trySetProperty(vertexA, "keyListString", listA, graph.getFeatures().supportsUniformListProperty);
                trySetProperty(edge, "keyListString", listA, graph.getFeatures().supportsUniformListProperty);


                tryGetProperty(vertexA, "keyListString", listA, graph.getFeatures().supportsUniformListProperty);
                tryGetProperty(edge, "keyListString", listA, graph.getFeatures().supportsUniformListProperty);


                final ArrayList listB = new ArrayList();
                listB.add("try1");
                listB.add(2);

                trySetProperty(vertexA, "keyListMixed", listB, graph.getFeatures().supportsMixedListProperty);
                trySetProperty(edge, "keyListMixed", listB, graph.getFeatures().supportsMixedListProperty);

                tryGetProperty(vertexA, "keyListString", listA, graph.getFeatures().supportsMixedListProperty);
                tryGetProperty(edge, "keyListString", listA, graph.getFeatures().supportsMixedListProperty);


                trySetProperty(vertexA, "keyArrayString", new String[]{"try1", "try2"}, graph.getFeatures().supportsPrimitiveArrayProperty);
                trySetProperty(edge, "keyArrayString", new String[]{"try1", "try2"}, graph.getFeatures().supportsPrimitiveArrayProperty);

                trySetProperty(vertexA, "keyArrayInteger", new int[]{1, 2}, graph.getFeatures().supportsPrimitiveArrayProperty);
                trySetProperty(edge, "keyArrayInteger", new int[]{1, 2}, graph.getFeatures().supportsPrimitiveArrayProperty);

                trySetProperty(vertexA, "keyArrayLong", new long[]{1000l, 2000l}, graph.getFeatures().supportsPrimitiveArrayProperty);
                trySetProperty(edge, "keyArrayLong", new long[]{1000l, 2000l}, graph.getFeatures().supportsPrimitiveArrayProperty);

                trySetProperty(vertexA, "keyArrayFloat", new float[]{1000.321f, 2000.321f}, graph.getFeatures().supportsPrimitiveArrayProperty);
                trySetProperty(edge, "keyArrayFloat", new float[]{1000.321f, 2000.321f}, graph.getFeatures().supportsPrimitiveArrayProperty);

                trySetProperty(vertexA, "keyArrayDouble", new double[]{1000.321d, 2000.321d}, graph.getFeatures().supportsPrimitiveArrayProperty);
                trySetProperty(edge, "keyArrayDouble", new double[]{1000.321d, 2000.321d}, graph.getFeatures().supportsPrimitiveArrayProperty);

                trySetProperty(vertexA, "keyArrayBoolean", new boolean[]{false, true}, graph.getFeatures().supportsPrimitiveArrayProperty);
                trySetProperty(edge, "keyArrayBoolean", new boolean[]{false, true}, graph.getFeatures().supportsPrimitiveArrayProperty);

                trySetProperty(vertexA, "keyArrayEmpty", new int[0], graph.getFeatures().supportsPrimitiveArrayProperty);
                trySetProperty(edge, "keyArrayEmpty", new int[0], graph.getFeatures().supportsPrimitiveArrayProperty);

                final Map map = new HashMap();
                map.put("testString", "try");
                map.put("testInteger", "string");

                trySetProperty(vertexA, "keyMap", map, graph.getFeatures().supportsMapProperty);
                trySetProperty(edge, "keyMap", map, graph.getFeatures().supportsMapProperty);

                final MockSerializable mockSerializable = new MockSerializable();
                mockSerializable.setTestField("test");
                trySetProperty(vertexA, "keySerializable", mockSerializable, graph.getFeatures().supportsSerializableObjectProperty);
                trySetProperty(edge, "keySerializable", mockSerializable, graph.getFeatures().supportsSerializableObjectProperty);

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
	                    System.out.println("Skipping test " + method.getName() + ".");
	                } else {
    	                System.out.println("Testing " + method.getName() + "...");
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
	                final BigdataSail testSail = getSail();
	                testSail.initialize();
	                final BigdataSailRepository repo = new BigdataSailRepository(testSail);
	                final BigdataGraphEmbedded graph = new BigdataGraphEmbedded(repo) {
	    
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
	            
				BigdataGraphEmbedded graph = testGraphs.get(key); //testSail; //getSail();
				
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
		        sail.repo.getSail().__tearDownUnitTest();
		    }
		    testGraphs.clear();
		}
		
    	
    }

    public static final void main(final String[] args) throws Exception {

        { // create an in-memory instance
            
            final BigdataGraph graph = BigdataGraphFactory.create();
            
            GraphMLReader.inputGraph(graph, TestBigdataGraphEmbedded.class.getResourceAsStream("graph-example-1.xml"));
            
            System.err.println("data loaded (in-memory).");
            System.err.println("graph:");
            
            for (Vertex v : graph.getVertices()) {
                
                System.err.println(v);
                
            }
            
            for (Edge e : graph.getEdges()) {
                
                System.err.println(e);
                
            }
            
            graph.shutdown();
            
        }
        
        final File jnl = File.createTempFile("bigdata", ".jnl");
        
        { // create a persistent instance
            
            final BigdataGraph graph = BigdataGraphFactory.open(jnl.getAbsolutePath(), true);
            
            GraphMLReader.inputGraph(graph, TestBigdataGraphEmbedded.class.getResourceAsStream("graph-example-1.xml"));
            
            System.err.println("data loaded (persistent).");
            System.err.println("graph:");
            
            for (Vertex v : graph.getVertices()) {
                
                System.err.println(v);
                
            }
            
            for (Edge e : graph.getEdges()) {
                
                System.err.println(e);
                
            }
            
            graph.shutdown();
            
        }
        
        { // re-open the persistent instance
            
            final BigdataGraph graph = BigdataGraphFactory.open(jnl.getAbsolutePath(), true);
            
            System.err.println("persistent graph re-opened.");
            System.err.println("graph:");
            
            for (Vertex v : graph.getVertices()) {
                
                System.err.println(v);
                
            }
            
            for (Edge e : graph.getEdges()) {
                
                System.err.println(e);
                
            }
            
            graph.shutdown();
            
        }
        
        jnl.delete();
        
    }


}
