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
import junit.textui.ResultPrinter;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.AbstractScaleOutClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ScaleOutClientFactory;
import com.bigdata.util.Bytes;

/**
 * A version of the test suite that is intended for local debugging and is NOT
 * run in CI. This is intended just to make it easier to run specific proxied
 * test suites.
 * 
 * @see TestNanoSparqlServerWithProxyIndexManager
 */
public class TestNanoSparqlServerWithProxyIndexManager2<S extends IIndexManager>
        extends AbstractIndexManagerTestCase<S> {
	
	static {
		ProxySuiteHelper.proxyIndexManagerTestingHasStarted = true;
	}

	/**
	 * The {@link IIndexManager} for the backing persistence engine (may be a
	 * {@link Journal} or {@link com.bigdata.service.jini.JiniFederation}).
	 */
	private IIndexManager m_indexManager;

	/**
	 * The mode in which the test is running.
	 */
	private TestMode testMode;

	/**
	 * Run in triples mode on a temporary journal.
	 */
	public TestNanoSparqlServerWithProxyIndexManager2() {

		this(null/* name */, getTemporaryJournal(), TestMode.triples);

	}

	/**
	 * Run in triples mode on a temporary journal.
	 */
	public TestNanoSparqlServerWithProxyIndexManager2(String name) {

		this(name, getTemporaryJournal(), TestMode.triples);

	}

	static private Journal getTemporaryJournal() {

		final Properties properties = new Properties();

		properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
				BufferMode.Transient.toString());

		properties.setProperty(com.bigdata.journal.Options.INITIAL_EXTENT, ""
				+ (Bytes.megabyte32 * 1));

		return new Journal(properties);

	}

	/**
	 * Run test suite against an embedded {@link NanoSparqlServer} instance,
	 * which is in turn running against the caller's {@link IIndexManager}.
	 * 
	 * @param indexManager
	 *            The {@link Journal} or {@link com.bigdata.service.jini.JiniFederation}.
	 * @param testMode
	 *            Identifies what mode the kb instance will be using.
	 */
	private TestNanoSparqlServerWithProxyIndexManager2(final String name,
			final IIndexManager indexManager, final TestMode testMode) {

		super(name == null ? TestNanoSparqlServerWithProxyIndexManager2.class.getName()
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
            return TestNanoSparqlServerWithProxyIndexManager2.suite(
                    getTemporaryJournal(), TestMode.triples);
        }
    }
    
    /**
     * The {@link TestMode#quads} test suite.
     */
    public static class Test_NSS_quads extends TestCase {
        public static Test suite() {
            return TestNanoSparqlServerWithProxyIndexManager2.suite(
                    getTemporaryJournal(), TestMode.quads);
        }
    }
    
    /**
     * The {@link TestMode#sids} test suite.
     */
    public static class Test_NSS_sids extends TestCase {
        public static Test suite() {
            return TestNanoSparqlServerWithProxyIndexManager2.suite(
                    getTemporaryJournal(), TestMode.sids);
        }
    }
    
	/**
	 * Return suite running in the given mode against the given
	 * {@link IIndexManager}.
	 */
	public static Test suite(final IIndexManager indexManager,
			final TestMode testMode) {

		final TestNanoSparqlServerWithProxyIndexManager2<?> delegate = new TestNanoSparqlServerWithProxyIndexManager2(
				null/* name */, indexManager, testMode); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "NanoSparqlServer Proxied Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
		//Protocol
		suite.addTest(TestProtocolAll.suite());

        suite.addTestSuite(TestMultiTenancyAPI.class);
        suite.addTestSuite(TestDataLoaderServlet.class); // Data Loader Servlet
        
        return suite;
    
    }

	@Override
	@SuppressWarnings("unchecked")
    public S getIndexManager() {

		return (S) m_indexManager;

	}
    
    @Override
	public Properties getProperties() {

//    	System.err.println("testMode="+testMode);
    	
	    final Properties properties = new Properties();

		switch (testMode) {
		case quads:
			properties.setProperty(AbstractTripleStore.Options.QUADS_MODE,
					"true");
			properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,
					"false");
			properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
					NoAxioms.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.VOCABULARY_CLASS,
					NoVocabulary.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
			break;
		case triples:
			properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,
					"false");
			properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
					NoAxioms.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.VOCABULARY_CLASS,
					NoVocabulary.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
			break;
		case sids:
			properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,
					"false");
			properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
					NoAxioms.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.VOCABULARY_CLASS,
					NoVocabulary.class.getName());
			properties.setProperty(
					AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "true");
			break;
		default:
			fail("Unknown mode: " + testMode);
		}
		// if (false/* triples w/ truth maintenance */) {
		// properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
		// "false");
		// }
		// if (false/* sids w/ truth maintenance */) {
		// properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
		// "true");
		// }

		return properties;
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
			final Test test = TestNanoSparqlServerWithProxyIndexManager2.suite(
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
    
}
