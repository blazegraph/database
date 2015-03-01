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

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.AbstractBigdataSailTestCase;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.GraphQueryTestSuite;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexQueryTestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

/**
 */
public abstract class AbstractTestBigdataGraph extends AbstractBigdataSailTestCase {

    protected static final transient Logger log = Logger.getLogger(AbstractTestBigdataGraph.class);
    
    /**
     * 
     */
    public AbstractTestBigdataGraph() {
    }

    /**
     * @param name
     */
    public AbstractTestBigdataGraph(String name) {
        super(name);
    }
    
    protected BigdataSail getSail() {
        
        return getSail(getProperties());
        
    }
    
    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();

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
        
        props.setProperty(BigdataGraph.Options.READ_FROM_WRITE_CONNECTION, "true");
        
        return props;
        
    }
    
    @Override
    protected BigdataSail getSail(final Properties properties) {
        
        return new BigdataSail(properties);
        
    }

    @Override
    protected BigdataSail reopenSail(final BigdataSail sail) {

        final Properties properties = sail.getDatabase().getProperties();

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
    
    private static class BigdataTestSuite extends TestSuite {
        
        public BigdataTestSuite(final GraphTest graphTest) {
            super(graphTest);
        }
        


    }
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
