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

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.GraphQueryTestSuite;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.VertexQueryTestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

import junit.framework.TestCase2;

/**
 */
public abstract class AbstractTestBigdataGraph extends TestCase2 { 
    
    /**
     * 
     */
    public AbstractTestBigdataGraph() {
    	super();
    }

    /**
     * @param name
     */
    public AbstractTestBigdataGraph(final String name) {
    	super(name);
    }
    
    protected BigdataSail getSail() {
        
        return getSail(getProperties());
        
    }
    
    @Override
    public Properties getProperties() {
        
        final Properties props = new Properties();
        
        props.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS,
                "false");

        props.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS,
                "false");

        props.setProperty(Journal.Options.HTTPD_PORT, "-1"/* none */);
        
        // transient means that there is nothing to delete after the test.
//        props.setProperty(Options.BUFFER_MODE,BufferMode.Transient.toString());
        props.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());

        /*
         * If an explicit filename is not specified...
         */
        if(props.get(Options.FILE)==null) {

            /*
             * Use a temporary file for the test. Such files are always deleted when
             * the journal is closed or the VM exits.
             */

            props.setProperty(Options.CREATE_TEMP_FILE,"true");
        
            props.setProperty(Options.DELETE_ON_EXIT,"true");
            
        }

        //no inference
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        
        // no text index
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        // triples mode
        props.setProperty(BigdataSail.Options.QUADS, "false");
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
        
        return props;
        
    }

    private Properties properties = null;

    @Override
    protected void tearDown() throws Exception {

        properties = null;
        
    }
    
    protected BigdataSail getSail(final Properties properties) {
        
        this.properties = properties;
        
        return new BigdataSail(properties);
        
    }

    protected BigdataSail reopenSail(final BigdataSail sail) {

//        final Properties properties = sail.getProperties();

        if (sail.isOpen()) {

            try {

                sail.shutDown();

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }
        
        return getSail(properties);
        
    }

    protected abstract GraphTest newBigdataGraphTest() throws Exception;
    

    public void testVertexTestSuite() throws Exception {
    	final GraphTest test = newBigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new VertexTestSuite(test));
        GraphTest.printTestPerformance("VertexTestSuite", test.stopWatch());
    }

    public void testEdgeSuite() throws Exception {
    	final GraphTest test = newBigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new EdgeTestSuite(test));
        GraphTest.printTestPerformance("EdgeTestSuite", test.stopWatch());
    }

    public void testGraphSuite() throws Exception {
    	final GraphTest test = newBigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new GraphTestSuite(test));
        GraphTest.printTestPerformance("GraphTestSuite", test.stopWatch());
    }

    public void testVertexQueryTestSuite() throws Exception {
    	final GraphTest test = newBigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new VertexQueryTestSuite(test));
        GraphTest.printTestPerformance("VertexQueryTestSuite", test.stopWatch());
    }

    public void testGraphQueryTestSuite() throws Exception {
    	final GraphTest test = newBigdataGraphTest();
    	test.stopWatch();
        test.doTestSuite(new GraphQueryTestSuite(test));
        GraphTest.printTestPerformance("GraphQueryTestSuite", test.stopWatch());
    }

    
//    public void testTransactionalGraphTestSuite() throws Exception {
//    	final GraphTest test = newBigdataGraphTest();
//    	test.stopWatch();
//        test.doTestSuite(new TransactionalGraphTestSuite(test));
//        GraphTest.printTestPerformance("TransactionalGraphTestSuite", test.stopWatch());
//    }
//
//    public void testGraphQueryForHasOR() throws Exception {
//        final BigdataGraphTest test = newBigdataGraphTest();
//        test.stopWatch();
//        final BigdataTestSuite testSuite = new BigdataTestSuite(test);
//        try {
//            testSuite.testGraphQueryForHasOR();
//        } finally {
//            test.shutdown();
//        }
//        
//    }
    
//    private static class BigdataTestSuite extends TestSuite {
//        
//        public BigdataTestSuite(final GraphTest graphTest) {
//            super(graphTest);
//        }
//
//    }
//    
//    
//    private class BigdataGraphTest extends GraphTest {
//
//		@Override
//		public void doTestSuite(TestSuite testSuite) throws Exception {
//	        for (Method method : testSuite.getClass().getDeclaredMethods()) {
//	            if (method.getName().startsWith("test")) {
//	                System.out.println("Testing " + method.getName() + "...");
//	                try {
//		                method.invoke(testSuite);
//	                } catch (Exception ex) {
//	                	ex.getCause().printStackTrace();
//	                	throw ex;
//	                } finally {
//		                shutdown();
//	                }
//	            }
//	        }
//		}
//		
//		private Map<String,BigdataSail> testSails = new LinkedHashMap<String, BigdataSail>();
//
//		@Override
//		public Graph generateGraph(final String key) {
//			
//			try {
//	            if (testSails.containsKey(key) == false) {
//	                final BigdataSail testSail = getSail();
//	                testSail.initialize();
//	                testSails.put(key, testSail);
//	            }
//	            
//				final BigdataSail sail = testSails.get(key); //testSail; //getSail();
//				final BigdataSailRepository repo = new BigdataSailRepository(sail);
//				final BigdataGraph graph = new BigdataGraphEmbedded(repo) {
//	
//				    /**
//				     * Test cases have weird semantics for shutdown.
//				     */
//					@Override
//					public void shutdown() {
//					    try {
//				            if (cxn != null) {
//    					        cxn.commit();
//    					        cxn.close();
//    					        cxn = null;
//				            }
//					    } catch (Exception ex) {
//					        throw new RuntimeException(ex);
//					    }
//					}
//					
//				};
//				return graph;
//			} catch (Exception ex) {
//				throw new RuntimeException(ex);
//			}
//			
//		}
//
//		@Override
//		public Graph generateGraph() {
//			
//			return generateGraph(null);
//		}
//		
//		public void shutdown() {
//		    for (BigdataSail sail : testSails.values()) {
//		        sail.__tearDownUnitTest();
//		    }
//		    testSails.clear();
//		}
//		
//    	
//    }

}
