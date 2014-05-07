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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.ProxyBigdataSailTestCase;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQueryTestSuite;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQueryTestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 */
public class TestBigdataGraphEmbedded extends ProxyBigdataSailTestCase {

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

    public void testVertexTestSuite() throws Exception {
    	final GraphTest test = new BigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new VertexTestSuite(test));
        GraphTest.printTestPerformance("VertexTestSuite", test.stopWatch());
    }

    public void testEdgeSuite() throws Exception {
    	final GraphTest test = new BigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new EdgeTestSuite(test));
        GraphTest.printTestPerformance("EdgeTestSuite", test.stopWatch());
    }

    public void testGraphSuite() throws Exception {
    	final GraphTest test = new BigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new GraphTestSuite(test));
        GraphTest.printTestPerformance("GraphTestSuite", test.stopWatch());
    }

    public void testVertexQueryTestSuite() throws Exception {
    	final GraphTest test = new BigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new VertexQueryTestSuite(test));
        GraphTest.printTestPerformance("VertexQueryTestSuite", test.stopWatch());
    }

    public void testGraphQueryTestSuite() throws Exception {
    	final GraphTest test = new BigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new GraphQueryTestSuite(test));
        GraphTest.printTestPerformance("GraphQueryTestSuite", test.stopWatch());
    }
//
//    public void testTransactionalGraphTestSuite() throws Exception {
//    	final GraphTest test = new BigdataGraphTest();
//    	test.stopWatch();
//        test.doTestSuite(new TransactionalGraphTestSuite(test));
//        GraphTest.printTestPerformance("TransactionalGraphTestSuite", test.stopWatch());
//    }
//
//    public void testBulkTransactionsOnEdges() throws Exception {
//        final BigdataGraphTest test = new BigdataGraphTest();
//        test.stopWatch();
//        final BigdataTestSuite testSuite = new BigdataTestSuite(test);
//        try {
//            testSuite.testBulkTransactionsOnEdges();
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
//        public void testBulkTransactionsOnEdges() {
//            BigdataGraphEmbedded graph = (BigdataGraphEmbedded) graphTest.generateGraph();
//            for (int i = 0; i < 5; i++) {
//                graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), graphTest.convertLabel("test"));
//            }
//            edgeCount(graph, 5);
//            graph.rollback();
//            edgeCount(graph, 0);
//
//            for (int i = 0; i < 4; i++) {
//                graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), graphTest.convertLabel("test"));
//            }
//            edgeCount(graph, 4);
//            graph.rollback();
//            edgeCount(graph, 0);
//
//
//            for (int i = 0; i < 3; i++) {
//                graph.addEdge(null, graph.addVertex(null), graph.addVertex(null), graphTest.convertLabel("test"));
//            }
//            edgeCount(graph, 3);
//            graph.commit();
//            edgeCount(graph, 3);
//
//            graph.shutdown();
//        }
//        
//    }
//    
    
    private class BigdataGraphTest extends GraphTest {

		@Override
		public void doTestSuite(TestSuite testSuite) throws Exception {
	        for (Method method : testSuite.getClass().getDeclaredMethods()) {
	            if (method.getName().startsWith("test")) {
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
		
		private Map<String,BigdataSail> testSails = new LinkedHashMap<String, BigdataSail>();

		@Override
		public Graph generateGraph(final String key) {
			
			try {
	            if (testSails.containsKey(key) == false) {
	                final BigdataSail testSail = getSail();
	                testSail.initialize();
	                testSails.put(key, testSail);
	            }
	            
				final BigdataSail sail = testSails.get(key); //testSail; //getSail();
				final BigdataSailRepository repo = new BigdataSailRepository(sail);
				final BigdataGraph graph = new BigdataGraphEmbedded(repo) {
	
				    /**
				     * Test cases have weird semantics for shutdown.
				     */
					@Override
					public void shutdown() {
					    try {
				            if (cxn != null) {
    					        cxn.commit();
    					        cxn.close();
    					        cxn = null;
				            }
					    } catch (Exception ex) {
					        throw new RuntimeException(ex);
					    }
					}
					
				};
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
		    for (BigdataSail sail : testSails.values()) {
		        sail.__tearDownUnitTest();
		    }
		    testSails.clear();
		}
		
    	
    }

    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();

        /*
         * For example, here is a set of five properties that turns off
         * inference, truth maintenance, and the free text index.
         */
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
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
            
            final BigdataGraph graph = BigdataGraphFactory.create(jnl.getAbsolutePath());
            
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
            
            final BigdataGraph graph = BigdataGraphFactory.open(jnl.getAbsolutePath());
            
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
