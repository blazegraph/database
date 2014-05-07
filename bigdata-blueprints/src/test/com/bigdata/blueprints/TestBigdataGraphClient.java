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

import java.lang.reflect.Method;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;

import com.bigdata.BigdataStatics;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.ProxyBigdataSailTestCase;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.util.config.NicUtil;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

/**
 */
public class TestBigdataGraphClient extends ProxyBigdataSailTestCase {

    protected static final transient Logger log = Logger.getLogger(TestBigdataGraphClient.class);
    
    /**
     * 
     */
    public TestBigdataGraphClient() {
    }

    /**
     * @param name
     */
    public TestBigdataGraphClient(String name) {
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
//
//    public void testVertexQueryTestSuite() throws Exception {
//    	final GraphTest test = new BigdataGraphTest();
//    	test.stopWatch();
//        test.doTestSuite(new VertexQueryTestSuite(test));
//        GraphTest.printTestPerformance("VertexQueryTestSuite", test.stopWatch());
//    }
//
//    public void testGraphQueryTestSuite() throws Exception {
//    	final GraphTest test = new BigdataGraphTest();
//    	test.stopWatch();
//        test.doTestSuite(new GraphQueryTestSuite(test));
//        GraphTest.printTestPerformance("GraphQueryTestSuite", test.stopWatch());
//    }
//
    
    private static class BigdataTestSuite extends TestSuite {
        
        public BigdataTestSuite(final BigdataGraphTest graphTest) {
            super(graphTest);
        }
        
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
        
    }
    
    
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
		
		private Map<String,BigdataSailNSSWrapper> testSails = new LinkedHashMap<String, BigdataSailNSSWrapper>();

		@Override
		public Graph generateGraph(final String key) {
			
			try {
	            if (testSails.containsKey(key) == false) {
	                final BigdataSail testSail = getSail();
	                testSail.initialize();
	                final BigdataSailNSSWrapper nss = new BigdataSailNSSWrapper(testSail);
	                nss.setUp();
	                testSails.put(key, nss);
	            }
	            
				final BigdataSailNSSWrapper nss = testSails.get(key);
				final BigdataGraph graph = new BigdataGraphClient(nss.m_repo);
				
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
    		        wrapper.tearDown();
    		        wrapper.sail.__tearDownUnitTest();
		        } catch (Exception ex) {
		            throw new RuntimeException(ex);
		        }
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
    
    public static class BigdataSailNSSWrapper {
        
        private final BigdataSail sail;
        
        /**
         * A jetty {@link Server} running a {@link NanoSparqlServer} instance which
         * is running against that {@link #m_indexManager}.
         */
        protected Server m_fixture;

        /**
         * The {@link ClientConnectionManager} for the {@link HttpClient} used by
         * the {@link RemoteRepository}. This is used when we tear down the
         * {@link RemoteRepository}.
         */
        private ClientConnectionManager m_cm;
        
        /**
         * Exposed to tests that do direct HTTP GET/POST operations.
         */
        protected HttpClient m_httpClient = null;

        /**
         * The client-API wrapper to the NSS.
         */
        protected RemoteRepositoryManager m_repo;

        /**
         * The effective {@link NanoSparqlServer} http end point (including the
         * ContextPath).
         */
        protected String m_serviceURL;

        /**
         * The URL of the root of the web application server. This does NOT include
         * the ContextPath for the webapp.
         * 
         * <pre>
         * http://localhost:8080 -- root URL
         * http://localhost:8080/bigdata -- webapp URL (includes "/bigdata" context path.
         * </pre>
         */
        protected String m_rootURL;
        
        public BigdataSailNSSWrapper(final BigdataSail sail) {
            this.sail = sail;
        }
        
        public void setUp() throws Exception {
            
            final Map<String, String> initParams = new LinkedHashMap<String, String>();
            {

                initParams.put(ConfigParams.NAMESPACE, sail.getDatabase().getNamespace());

                initParams.put(ConfigParams.CREATE, "false");
                
            }
            // Start server for that kb instance.
            m_fixture = NanoSparqlServer.newInstance(0/* port */,
                    sail.getDatabase().getIndexManager(), initParams);

            m_fixture.start();

            final int port = NanoSparqlServer.getLocalPort(m_fixture);

            // log.info("Getting host address");

            final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                    true/* loopbackOk */);

            if (hostAddr == null) {

                fail("Could not identify network address for this host.");

            }

            m_rootURL = new URL("http", hostAddr, port, ""/* contextPath */
            ).toExternalForm();

            m_serviceURL = new URL("http", hostAddr, port,
                    BigdataStatics.getContextPath()).toExternalForm();

            if (log.isInfoEnabled())
                log.info("Setup done: \nrootURL=" + m_rootURL + "\nserviceURL="
                        + m_serviceURL);

//            final HttpClient httpClient = new DefaultHttpClient();

//            m_cm = httpClient.getConnectionManager();
            
            m_cm = DefaultClientConnectionManagerFactory.getInstance()
                    .newInstance();

            final DefaultHttpClient httpClient = new DefaultHttpClient(m_cm);
            m_httpClient = httpClient;
            
            /*
             * Ensure that the client follows redirects using a standard policy.
             * 
             * Note: This is necessary for tests of the webapp structure since the
             * container may respond with a redirect (302) to the location of the
             * webapp when the client requests the root URL.
             */
            httpClient.setRedirectStrategy(new DefaultRedirectStrategy());

            m_repo = new RemoteRepositoryManager(m_serviceURL,
                    m_httpClient,
                    sail.getDatabase().getIndexManager().getExecutorService());

        }

        public void tearDown() throws Exception {

            if (m_fixture != null) {

                m_fixture.stop();

                m_fixture = null;

            }

            m_rootURL = null;
            m_serviceURL = null;
            
            if (m_cm != null) {
                m_cm.shutdown();
                m_cm = null;
            }

            m_httpClient = null;
            m_repo = null;
            
            log.info("tear down done");
            
        }

        
    }
    
    public static final void main(final String[] args) throws Exception {
        
        final String url = "http://localhost:9999/bigdata/";
        
        final BigdataGraph graph = BigdataGraphFactory.connect(url);
        
        for (Vertex v : graph.getVertices()) {
            
            System.err.println(v);
            
        }
        
        for (Edge e : graph.getEdges()) {
            
            System.err.println(e);
            
        }
        
    }

}
