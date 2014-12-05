/**
Copyright (C) SYSTAP, LLC 2013.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp.health;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.jetty.server.Server;

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase2;
import junit.framework.TestListener;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import junit.textui.ResultPrinter;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepositoryManager;
import com.bigdata.rdf.sail.webapp.client.JettyResponseListener;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.config.NicUtil;

/**
 * Utility test suite provides a health check for a deployed instance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestNSSHealthCheck extends TestCase2 {

	/**
	 * A marker placed into index.html so we can recognize when that page is
	 * served.
	 */
	private static final String JUNIT_TEST_MARKER_INDEX_HTML = "junit test marker: index.html";

	/**
	 * The executor used by the http client.
	 */
	private ExecutorService executorService;

	/**
	 * The client-API wrapper to the NSS.
	 */
	protected JettyRemoteRepositoryManager m_repo;

	/**
	 * The effective {@link NanoSparqlServer} http end point (including the
	 * ContextPath).
	 * 
	 * <pre>
	 * http://localhost:8080/bigdata -- webapp URL (includes "/bigdata" context path.
	 * </pre>
	 */
	protected String m_serviceURL;

	/**
	 * The URL of the root of the web application server. This does NOT include
	 * the ContextPath for the webapp.
	 * 
	 * <pre>
	 * http://localhost:8080 -- root URL
	 * </pre>
	 */
	protected String m_rootURL;

	public TestNSSHealthCheck(final String name) {// , final String requestURI)
													// {

		super(name);

		// m_requestURI = requestURI;

	}

	/**
	 * FIXME hacked in test suite constructor.
	 */
	private static String requestURI; // No longer used!

	protected Server m_fixture;

	protected String m_namespace;

	private Journal m_indexManager;

	@Override
	protected void setUp() throws Exception {

		super.setUp();

		// m_rootURL = requestURI;

		m_namespace = getName() + UUID.randomUUID();

		m_fixture = newFixture();

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

		// m_cm = DefaultClientConnectionManagerFactory.getInstance()
		// .newInstance();

		/*
		 * Ensure that the client follows redirects using a standard policy.
		 * 
		 * Note: This is necessary for tests of the webapp structure since the
		 * container may respond with a redirect (302) to the location of the
		 * webapp when the client requests the root URL.
		 */
		executorService = Executors.newCachedThreadPool(DaemonThreadFactory
				.defaultThreadFactory());

		m_repo = new JettyRemoteRepositoryManager(m_serviceURL, executorService);

	}

	@Override
	protected void tearDown() throws Exception {

		m_rootURL = null;
		m_serviceURL = null;

		if (m_fixture != null) {

			m_fixture.stop();

			m_fixture = null;

		}

		if (m_indexManager != null && m_namespace != null) {

			dropTripleStore(m_indexManager, m_namespace);

		}

		if (m_repo != null) {
			m_repo.close();
			m_repo = null;
		}

		if (executorService != null) {
			executorService.shutdownNow();
			executorService = null;
		}

		super.tearDown();

	}

	protected void dropTripleStore(final IIndexManager indexManager,
			final String namespace) {

		if (log.isInfoEnabled())
			log.info("KB namespace=" + namespace);

		// Locate the resource declaration (aka "open"). This tells us if it
		// exists already.
		final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

		if (tripleStore != null) {

			if (log.isInfoEnabled())
				log.info("Destroying: " + namespace);

			if (!BigdataStatics.NSS_GROUP_COMMIT) {
				/*
				 * FIXME GROUP COMMIT: We need to submit a task that does this
				 * in order to stay inside of the same concurrency control
				 * mechanism as the database.
				 */
				tripleStore.destroy();
			}

		}

	}

	/*
	 * Define local NSS
	 */

	protected Server newFixture() throws Exception {
		final Properties properties = getProperties();

		m_indexManager = new Journal(properties);

		// Create the triple store instance.
		createTripleStore(m_indexManager, m_namespace, properties);

		final Map<String, String> initParams = new LinkedHashMap<String, String>();
		{

			initParams.put(ConfigParams.NAMESPACE, m_namespace);

			initParams.put(ConfigParams.CREATE, "false");

		}
		// Start server for that kb instance.
		final Server fixture = NanoSparqlServer.newInstance(0/* port */,

		m_indexManager, initParams);

		fixture.start();

		return fixture;
	}

	protected AbstractTripleStore createTripleStore(
			final IIndexManager indexManager, final String namespace,
			final Properties properties) {

		if (log.isInfoEnabled())
			log.info("KB namespace=" + namespace);

		// Locate the resource declaration (aka "open"). This tells us if it
		// exists already.
		AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

		if (tripleStore != null) {

			fail("exists: " + namespace);

		}

		/*
		 * Create the KB instance.
		 */

		if (log.isInfoEnabled()) {
			log.info("Creating KB instance: namespace=" + namespace);
			log.info("Properties=" + properties.toString());
		}

		if (indexManager instanceof Journal) {

			// Create the kb instance.
			tripleStore = new LocalTripleStore(indexManager, namespace,
					ITx.UNISOLATED, properties);

		} else {

			tripleStore = new ScaleOutTripleStore(indexManager, namespace,
					ITx.UNISOLATED, properties);
		}

		// create the triple store.
		tripleStore.create();

		if (log.isInfoEnabled())
			log.info("Created tripleStore: " + namespace);

		// New KB instance was created.
		return tripleStore;

	}

	public Properties getProperties() {
		final Properties props = super.getProperties();

		props.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());

		props.setProperty(Options.CREATE_TEMP_FILE, "true");

		return props;
	}

	static class HealthCheckTestSuite extends TestSuite {

		/**
		 * The URL of the bigdata web application.
		 */
		@SuppressWarnings("unused")
		private final String requestURI;

		/**
		 * 
		 * @param name
		 * @param requestURI
		 *            The URL of the bigdata web application.
		 */
		private HealthCheckTestSuite(final String name, final String requestURI) {

			super(name);

			this.requestURI = requestURI;

			// FIXME Hacked through static field.
			TestNSSHealthCheck.requestURI = requestURI;

		}

	}

	static HealthCheckTestSuite createTestSuite(final String name,
			final String requestURI) {

		final HealthCheckTestSuite suite = new HealthCheckTestSuite(name,
				requestURI);

		suite.addTestSuite(TestNSSHealthCheck.class);

		return suite;

	}

	/**
	 * bare URL of the server
	 * 
	 * <pre>
	 * http://localhost:8080
	 * </pre>
	 * 
	 * The response is should be <code>index.html</code> since we want the
	 * bigdata webapp to respond for the top-level context.
	 * 
	 * <p>
	 * Note: You must ensure that the client follows redirects using a standard
	 * policy. This is necessary for tests of the webapp structure since the
	 * container may respond with a redirect (302) to the location of the webapp
	 * when the client requests the root URL.
	 */
	public void test_webapp_structure_rootURL() throws Exception {

		final String content = doGET(m_rootURL);

		assertTrue(content.contains(JUNIT_TEST_MARKER_INDEX_HTML));

	}

	/**
	 * URL with correct context path
	 * 
	 * <pre>
	 * http://localhost:8080/bigdata
	 * </pre>
	 * 
	 * The response is should be <code>index.html</code>, which is specified
	 * through the welcome files list.
	 */
	public void test_webapp_structure_contextPath() throws Exception {

		final String content = doGET(m_serviceURL);

		assertTrue(content.contains(JUNIT_TEST_MARKER_INDEX_HTML));
	}

	/**
	 * URL with context path and index.html reference
	 * 
	 * <pre>
	 * http://localhost:8080/bigdata/index.html
	 * </pre>
	 * 
	 * This URL does NOT get mapped to anything (404).
	 */
	public void test_webapp_structure_contextPath_indexHtml() throws Exception {

		try {

			doGET(m_serviceURL + "/index.html");

		} catch (HttpException ex) {

			assertEquals(404, ex.getStatusCode());

		}

	}

	/**
	 * The <code>favicon.ico</code> file.
	 * 
	 * @see <a href="http://www.w3.org/2005/10/howto-favicon"> How to add a
	 *      favicon </a>
	 */
	public void test_webapp_structure_favicon() throws Exception {

		doGET(m_serviceURL + "/html/favicon.ico");

	}

	/**
	 * The <code>/status</code> servlet responds.
	 */
	public void test_webapp_structure_status() throws Exception {

		doGET(m_serviceURL + "/status");

	}

	/**
	 * The <code>/counters</code> servlet responds.
	 */
	public void test_webapp_structure_counters() throws Exception {

		doGET(m_serviceURL + "/counters");

	}

	// /**
	// * The <code>/namespace/</code> servlet responds (multi-tenancy API).
	// */
	// public void test_webapp_structure_namespace() throws Exception {
	//
	// doGET(m_serviceURL + "/namespace/");
	//
	// }

	/**
	 * The fully qualified URL for <code>index.html</code>
	 * 
	 * <pre>
	 * http://localhost:8080/bigdata/html/index.html
	 * </pre>
	 * 
	 * The response is should be <code>index.html</code>, which is specified
	 * through the welcome files list.
	 */
	public void test_webapp_structure_contextPath_html_indexHtml()
			throws Exception {

		doGET(m_serviceURL + "/html/index.html");
	}

	private String doGET(final String url) throws Exception {

		JettyResponseListener response = null;

		try {

			final ConnectOptions opts = new ConnectOptions(url);
			opts.method = "GET";

			response = m_repo.doConnect(opts);

			m_repo.checkResponseCode(response);

			return response.getResponseBody();

		} finally {

			try {
				if (response != null)
					response.consume();
			} catch (IOException ex) {
				log.warn(ex, ex);
			}

		}

	}

	/**
	 * Connect to the NSS end point and run a test suite designed to verify the
	 * health of that instance.
	 * 
	 * @param args
	 *            URL
	 * 
	 * @throws MalformedURLException
	 * 
	 *             TODO Support HA health checks as well.
	 */
	public static void main(final String[] args) throws MalformedURLException {

		if (args.length < 1) {
			System.err.println("usage: <cmd> Request-URI");
			System.exit(1);
		}

		final String requestURI = args[0];

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
				log.error(arg0, arg1);
			}

			@Override
			public void addError(Test arg0, Throwable arg1) {
				log.error(arg0, arg1);
			}
		});

		try {

			// Setup test suite
			final Test test = createTestSuite(null/* name */, requestURI);

			System.out.println("Running health check: Request-URI="
					+ requestURI);

			// Run the test suite.
			test.run(result);

		} finally {

		}

		final String msg = "nerrors=" + result.errorCount() + ", nfailures="
				+ result.failureCount() + ", nrun=" + result.runCount()
				+ " : Request-URI=" + requestURI;

		System.out.println(msg);

		if (result.errorCount() > 0 || result.failureCount() > 0) {

			// At least one test failed.
			System.exit(1);

		}

		// All green.
		System.exit(0);

	}

}
