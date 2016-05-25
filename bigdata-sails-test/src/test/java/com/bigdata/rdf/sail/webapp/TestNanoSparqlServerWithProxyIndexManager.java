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
package com.bigdata.rdf.sail.webapp;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestListener;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import junit.textui.ResultPrinter;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RWStrategy;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.AbstractScaleOutClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ScaleOutClientFactory;
import com.bigdata.util.Bytes;

/**
 * Test suite for {@link RESTServlet} (SPARQL end point and REST API for RDF
 * data).
 * 
 * TODO Add unit tests which exercise the full text index.
 * 
 * TODO Add unit tests which are specific to sids and quads modes. These tests
 * should look at the semantics of interchange of sids or quads specific data;
 * queries which exercise the context position; and the default-graph and
 * named-graph URL query parameters for quads.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestNanoSparqlServerWithProxyIndexManager<S extends IIndexManager>
        extends AbstractIndexManagerTestCase<S> {
	
	static {
		ProxySuiteHelper.proxyIndexManagerTestingHasStarted = true;
	}

	/**
	 * The {@link IIndexManager} for the backing persistence engine (may be a
	 * {@link Journal} or JiniFederation).
	 */
	private IIndexManager m_indexManager;

	/**
	 * The mode in which the test is running.
	 */
	private TestMode testMode;

	/**
	 * Run in triples mode on a temporary journal.
	 */
	public TestNanoSparqlServerWithProxyIndexManager() {

		this(null/* name */, getTemporaryJournal(BufferMode.DiskRW), TestMode.triples);

	}

	/**
	 * Run in triples mode on a temporary journal.
	 */
	public TestNanoSparqlServerWithProxyIndexManager(String name) {

		this(name, getTemporaryJournal(BufferMode.DiskRW), TestMode.triples);

	}

	static Journal getTemporaryJournal() {
	
		return getTemporaryJournal(BufferMode.Transient);
		
	}
	
	static Journal getTemporaryJournal(final BufferMode bufferMode) {

		if (bufferMode == null)
			throw new IllegalArgumentException();
		
		final Properties properties = new Properties();

		properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
				bufferMode.toString());

      // Enable GROUP_COMMIT. See BLZG-192
      properties.setProperty(com.bigdata.journal.Journal.Options.GROUP_COMMIT,
            "false");

		if (bufferMode.isStable()) {
			
			// Using something that is backed by the disk.
			properties.setProperty(
					com.bigdata.journal.Options.CREATE_TEMP_FILE, "true");
			
			properties.setProperty(com.bigdata.journal.Options.DELETE_ON_CLOSE,
					"true");

		} else if (bufferMode.isFullyBuffered()) {

			// Using something that is fully buffered in memory. Reduce the
			// initial buffer size so we do not claim too much memory for the
			// backing store.
			properties.setProperty(com.bigdata.journal.Options.INITIAL_EXTENT,
					"" + (Bytes.megabyte32 * 1));
		}

      final Journal jnl = new Journal(properties);
      
		return jnl;

	}

	/**
	 * Run test suite against an embedded {@link NanoSparqlServer} instance,
	 * which is in turn running against the caller's {@link IIndexManager}.
	 * 
	 * @param indexManager
	 *            The {@link Journal} or JiniFederation.
	 * @param testMode
	 *            Identifies what mode the kb instance will be using.
	 */
	private TestNanoSparqlServerWithProxyIndexManager(final String name,
			final IIndexManager indexManager, final TestMode testMode) {

		super(name == null ? TestNanoSparqlServerWithProxyIndexManager.class.getName()
				: name);

		this.m_indexManager = indexManager;
		
		this.testMode = testMode;
		
	}

	/**
	 * Return suite running in triples mode against a temporary journal.
	 */
	public static Test suite() {

		return suite(TestMode.triples);

	}
	
	/**
	 * Return suite running in the specified mode against a temporary journal.
	 */
	public static Test suite(final TestMode testMode) {

		return suite(getTemporaryJournal(), testMode);

	}
	
    /**
     * The {@link TestMode#triples} test suite.
     */
    public static class test_NSS_triples extends TestCase {
        public static Test suite() {
            return TestNanoSparqlServerWithProxyIndexManager.suite(
                    getTemporaryJournal(), TestMode.triples);
        }
    }

    public static class test_NSS_RWStore extends TestCase {
        public static Test suite() {
            return TestNanoSparqlServerWithProxyIndexManager.suite(
                    getTemporaryJournal(BufferMode.DiskRW), TestMode.triples);
        }
    }

    /**
     * The {@link TestMode#quads} test suite.
     */
    public static class Test_NSS_quads extends TestCase {
        public static Test suite() {
            return TestNanoSparqlServerWithProxyIndexManager.suite(
                    getTemporaryJournal(), TestMode.quads);
        }
    }
    
    /**
     * The {@link TestMode#sids} test suite.
     */
    public static class Test_NSS_sids extends TestCase {
        public static Test suite() {
            return TestNanoSparqlServerWithProxyIndexManager.suite(
                    getTemporaryJournal(), TestMode.sids);
        }
    }
    
	/**
	 * Return suite running in the given mode against the given
	 * {@link IIndexManager}.
	 */
	public static TestSuite suite(final IIndexManager indexManager,
			final TestMode testMode) {

		final boolean RWStoreMode = indexManager instanceof AbstractJournal
				&& ((AbstractJournal) indexManager).getBufferStrategy() instanceof RWStrategy;

		final ProxyTestSuite suite = createProxyTestSuite(indexManager,testMode);

        if(RWStoreMode) {
        	
			// RWSTORE SPECIFIC TEST SUITE.
			suite.addTestSuite(TestRWStoreTxBehaviors.class);
			//BLZG-1727 Needs RWStore mode
	        suite.addTestSuite(TestBackupServlet.class);

        	
        } else {


        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // tests defined by this class (if any).
//        suite.addTestSuite(TestNanoSparqlServerOnFederation.class);

        /*
         * Proxied test suites.
         */

         // Protocol
         suite.addTest(TestProtocolAll.suite());

         // RemoteRepository test (nano sparql server client-wrapper using
         // Jetty)
         suite.addTestSuite(Test_REST_Structure.class);
         suite.addTestSuite(Test_REST_ASK.class);
         suite.addTestSuite(Test_REST_DESCRIBE.class);
         suite.addTestSuite(Test_REST_ESTCARD.class);
         if(BigdataStatics.runKnownBadTests) {// FIXME Restore for BLZG-1195
             suite.addTestSuite(Test_REST_ESTCARD.ReadWriteTx.class);
         }
         suite.addTestSuite(Test_REST_HASSTMT.class);
         if(BigdataStatics.runKnownBadTests) {// FIXME Restore for BLZG-1195
             suite.addTestSuite(Test_REST_HASSTMT.ReadWriteTx.class);
         }
         if (testMode.isTruthMaintenanceSupported()) {
            suite.addTestSuite(Test_REST_HASSTMT.TruthMaintenance.class);
         }
         if(testMode == TestMode.triplesPlusTruthMaintenance) {
            suite.addTestSuite(Test_Ticket_1207.class); // BLZG-1207 (GETSTMTS with includeInferred)
            
            // BLZG-697 Manage truth maintenance in SPARQL UPDATE
            suite.addTestSuite(TestSparqlUpdateSuppressTruthMaintenance.class); 
         }
         suite.addTestSuite(Test_REST_ServiceDescription.class);
         suite.addTestSuite(Test_REST_DELETE_BY_ACCESS_PATH.class);
         suite.addTestSuite(Test_REST_DELETE_WITH_BODY.class);
         suite.addTestSuite(TestNanoSparqlClient.class);
         suite.addTestSuite(TestMultiTenancyAPI.class); // Multi-tenancy API.
         suite.addTestSuite(TestDataLoaderServlet.class); // Data Loader Servlet

         // Transaction management API.
         suite.addTestSuite(Test_REST_TX_API.class);
         suite.addTestSuite(Test_REST_TX_API.NoReadWriteTx.class); // without isolatable indices.
         suite.addTestSuite(Test_REST_TX_API.ReadWriteTx.class); // with isolatable indices.

         /*
          * BigdataSailRemoteRepository(Connection) test suite (openrdf
          * compliant client).
          */
         suite.addTestSuite(TestBigdataSailRemoteRepository.class); // without isolatable indices.
         suite.addTestSuite(TestBigdataSailRemoteRepository.ReadWriteTx.class); // with isolatable indices.

         // Insert tests from trac issues
         suite.addTestSuite(TestInsertFilterFalse727.class);
         suite.addTestSuite(TestCBD731.class);
         suite.addTestSuite(Test_Ticket_605.class);

         suite.addTestSuite(TestService794.class);

         // Tests for procedure of rebuild text index
         suite.addTestSuite(TestRebuildTextIndex.class);
         suite.addTestSuite(Test_Ticket_1893.class);
         
         if (testMode == TestMode.sids) {
            // Tests that require sids mode.
            suite.addTestSuite(TestRDROperations.class);
         }

         if (testMode == TestMode.quads) {
            /*
             * Tests that require quads mode.
             * 
             * TODO The SPARQL UPDATE test suite is quads-mode only at this
             * time.
             */
            suite.addTestSuite(TestSparqlUpdate.class);
            suite.addTestSuite(StressTestConcurrentRestApiRequests.class);
            suite.addTestSuite(NativeDistinctNamedGraphUpdateTest.class);
            suite.addTestSuite(HashDistinctNamedGraphUpdateTest.class);
         }

            // Stress tests. See code for even longer running versions.
            {
                // Multi-tenancy API (focus on add/drop namespace + LOAD)
                suite.addTestSuite(StressTest_REST_MultiTenancy.class);

                // REST API (parameterized workload).
                suite.addTestSuite(StressTestConcurrentRestApiRequests.class);

            }

         // SPARQL 1.1 Federated Query.
         suite.addTestSuite(TestFederatedQuery.class);

      }

      return suite;

    }

	static ProxyTestSuite createProxyTestSuite(final IIndexManager indexManager, final TestMode testMode) {
		final TestNanoSparqlServerWithProxyIndexManager<?> delegate = new TestNanoSparqlServerWithProxyIndexManager(
				null/* name */, indexManager, testMode); // !!!! THIS CLASS !!!!

      /*
       * Use a proxy test suite and specify the delegate.
       */

      final ProxyTestSuite suite = new ProxyTestSuite(delegate,
            "NanoSparqlServer Proxied Test Suite: indexManager="
                  + indexManager.getClass().getSimpleName()
                  + ", testMode="
                  + testMode
                  + ", bufferMode="
                  + (indexManager instanceof Journal ? ((Journal) indexManager)
                        .getBufferStrategy().getBufferMode() : ""));

      return suite;
      
   }

	@SuppressWarnings("unchecked")
    public S getIndexManager() {

		return (S) m_indexManager;

	}
    
    @Override
	public Properties getProperties() {
    
      if (testMode == null)
         throw new IllegalStateException();

      return testMode.getProperties();

    }
    
    /**
     * Open the {@link IIndexManager} identified by the property file.
     * 
     * @param propertyFile
     *            The property file (for a standalone bigdata instance) or the
     *            jini configuration file (for a bigdata federation). The file
     *            must end with either ".properties" or ".config".
     *            
     * @return The {@link IIndexManager}.
     */
    static private IIndexManager openIndexManager(final String propertyFile) {

        final File file = new File(propertyFile);

        if (!file.exists()) {

            throw new RuntimeException("Could not find file: " + file);

        }

        boolean isJini = false;
        if (propertyFile.endsWith(".config")) {
            // scale-out.
            isJini = true;
        } else if (propertyFile.endsWith(".properties")) {
            // local journal.
            isJini = false;
        } else {
            /*
             * Note: This is a hack, but we are recognizing the jini
             * configuration file with a .config extension and the journal
             * properties file with a .properties extension.
             */
            throw new RuntimeException(
                    "File must have '.config' or '.properties' extension: "
                            + file);
        }

        final IIndexManager indexManager;
        try {

            if (isJini) {

                /*
                 * A bigdata federation.
                 */

				@SuppressWarnings("rawtypes")
				final AbstractScaleOutClient<?> jiniClient = ScaleOutClientFactory
						.getJiniClient(new String[] { propertyFile });

                indexManager = jiniClient.connect();

            } else {

                /*
                 * Note: we only need to specify the FILE when re-opening a
                 * journal containing a pre-existing KB.
                 */
                final Properties properties = new Properties();
                {
                    // Read the properties from the file.
                    final InputStream is = new BufferedInputStream(
                            new FileInputStream(propertyFile));
                    try {
                        properties.load(is);
                    } finally {
                        is.close();
                    }
                    if (System.getProperty(BigdataSail.Options.FILE) != null) {
                        // Override/set from the environment.
                        properties.setProperty(BigdataSail.Options.FILE, System
                                .getProperty(BigdataSail.Options.FILE));
                    }
					if (properties
							.getProperty(com.bigdata.journal.Options.FILE) == null) {
						// Run against a transient journal if no file was
						// specified.
						properties.setProperty(
								com.bigdata.journal.Options.BUFFER_MODE,
								BufferMode.Transient.toString());
						properties.setProperty(
								com.bigdata.journal.Options.INITIAL_EXTENT, ""
										+ (Bytes.megabyte32 * 1));
					}

                }

                indexManager = new Journal(properties);

            }

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return indexManager;
        
    }

	/**
	 * Runs the test suite against an {@link IBigdataFederation} or a
	 * {@link Journal}. The federation must already be up and running. An
	 * embedded {@link NanoSparqlServer} instance will be created for each test
	 * run. Each test will run against a distinct KB instance within a unique
	 * bigdata namespace on the same backing {@link IIndexManager}.
	 * <p>
	 * When run for CI, this can be executed as:
	 * <pre>
	 * ... -Djava.security.policy=policy.all TestNanoSparqlServerWithProxyIndexManager triples /nas/bigdata/benchmark/config/bigdataStandalone.config
	 * </pre>
	 * 
	 * @param args
	 *            <code>
	 * (testMode) (propertyFile|configFile)
	 * </code>
	 * 
	 *            where propertyFile is the configuration file for a
	 *            {@link Journal}. <br/>
	 *            where configFile is the configuration file for an
	 *            {@link IBigdataFederation}.<br/>
	 *            where <i>triples</i> or <i>sids</i> or <i>quads</i> is the
	 *            database mode.</br> where <i>tm</i> indicates that truth
	 *            maintenance should be enabled (only valid with triples or
	 *            sids).
	 */
    public static void main(final String[] args) throws Exception {

		if (args.length < 2) {
			System.err
					.println("(triples|sids|quads) (propertyFile|configFile) (tm)?");
			System.exit(1);
		}

		final TestMode testMode = TestMode.valueOf(args[0]);

//		if (testMode != TestMode.triples)
//			fail("Unsupported test mode: " + testMode);
		
		final File propertyFile = new File(args[1]);

		if (!propertyFile.exists())
			fail("No such file: " + propertyFile);

    	// Setup test result.
    	final TestResult result = new TestResult();
    	
    	// Setup listener, which will write the result on System.out
    	result.addListener(new ResultPrinter(System.out));
    	
    	result.addListener(new TestListener() {
			
    		@Override
			public void startTest(Test arg0) {
				log.info(arg0);
			}
			
    		@Override
			public void endTest(Test arg0) {
				log.info(arg0);
			}
			
    		@Override
			public void addFailure(Test arg0, AssertionFailedError arg1) {
				log.error(arg0,arg1);
			}
			
    		@Override
			public void addError(Test arg0, Throwable arg1) {
				log.error(arg0,arg1);
			}
		});
    	
    	// Open Journal / Connect to the configured federation.
		final IIndexManager indexManager = openIndexManager(propertyFile
				.getAbsolutePath());

        try {

        	// Setup test suite
			final Test test = TestNanoSparqlServerWithProxyIndexManager.suite(
					indexManager, testMode);

        	// Run the test suite.
        	test.run(result);
        	
        } finally {

			if (indexManager instanceof AbstractDistributedFederation<?>) {
				// disconnect
				((AbstractDistributedFederation<?>) indexManager).shutdownNow();
			} else {
				// destroy journal.
				((Journal) indexManager).destroy();
			}

        }

		final String msg = "nerrors=" + result.errorCount() + ", nfailures="
				+ result.failureCount() + ", nrun=" + result.runCount();
        
		if (result.errorCount() > 0 || result.failureCount() > 0) {

			// At least one test failed.
			fail(msg);

		}

        // All green.
		System.out.println(msg);
        
    }
    
    @Override
	public void tearDownAfterSuite() {
		this.m_indexManager.destroy();
		this.m_indexManager = null;
	}
    
}
