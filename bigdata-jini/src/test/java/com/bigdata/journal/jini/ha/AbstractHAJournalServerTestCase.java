/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
/*
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpMethod;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.BytesUtil;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HADigestRequest;
import com.bigdata.ha.msg.HALogDigestRequest;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HASnapshotDigestRequest;
import com.bigdata.ha.msg.IHARootBlockRequest;
import com.bigdata.io.TestCase3;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.journal.DumpJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.jini.ha.HAJournalServer.RunStateEnum;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.rdf.sail.TestConcurrentKBCreate;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.JettyResponseListener;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for {@link HAJournalServer} test suites.
 * <p>
 * Note: jini and zookeeper MUST be running and configured appropriate for the
 * configuration files used by the {@link HAJournalServer} instances in this
 * test suite. You MUST specify a sufficiently lax security policy.
 * 
 * <pre>
 * -Djava.security.policy=policy.all
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class AbstractHAJournalServerTestCase extends TestCase3 {

    public AbstractHAJournalServerTestCase() {
    }

    public AbstractHAJournalServerTestCase(String name) {
        super(name);
    }

    /**
     * The name of the federation. This is also used for the JINI locator
     * GROUPS.
     * <p>
     * Note: If you want to change this, then you also need to change the
     * HAJournal-{ABC}.config, jiniClient.config, zkClient.config, and how
     * you run the LookupStarter class to use the new federation name.
     */
    static final protected String FEDNAME = "benchmark";
    
    /**
     * Path to the config files.
     */
    static final protected String SRC_PATH = "bigdata-jini/src/test/resources/com/bigdata/journal/jini/ha/";

    /**
     * Path to the directory in which the service directories exist. The
     * individual service directories are formed by adding the service name.
     */
    // static final protected String TGT_PATH = "/Volumes/SSDData/bigdata/"+FEDNAME+"/CI-HAJournal-1/";
    static final protected String TGT_PATH = FEDNAME + "/CI-HAJournal-1/";

    /**
     * The timeout used to await quorum meet or break.
     */
    protected final static long awaitQuorumTimeout = 5000;

    /**
     * The timeout for long running LOAD operations. This is used to prevent
     * hangs in CI in case the LOAD does not complete for some reason.
     */
    protected final static long longLoadTimeoutMillis = TimeUnit.MINUTES
            .toMillis(4);

    /**
     * A service used to run tasks for some tests.
     */
    protected ExecutorService executorService = null;

    private Level oldProcessHelperLevel = null;
    
    /**
     * The client used to connect to the remote repository interface(s). The same
     * client is reused across all concurrent requests. This keeps down the #of
     * threads that are pre-allocated by the {@link HttpClient}.
     */
    protected HttpClient httpClient;
    
    @Override
    protected void setUp() throws Exception {

       super.setUp();
      
        if (log.isInfoEnabled())
         log.info("---- TEST START " + getName() + "----");
        
        executorService = Executors
                .newCachedThreadPool(new DaemonThreadFactory(getName()));

        // Setup the http client.
        httpClient = HttpClientConfigurator.getInstance().newInstance();

        /*
         * Override the log level for the ProcessHelper to ensure that we
         * observe all output from the child process in the console of the
         * process running the unit test. This will allow us to observe any
         * failures in the test startup.
         */
        final Logger tmp = Logger.getLogger(ProcessHelper.class);
                
        oldProcessHelperLevel = tmp.getLevel();
        
        // Must be at least INFO to see all output of the child processes.
        tmp.setLevel(Level.INFO);

    }

    @Override
    protected void tearDown() throws Exception {

        if (executorService != null) {

            executorService.shutdownNow();

            executorService = null;

        }

        // Tear down the HttpClient
        if (httpClient != null) {

              try {
                 httpClient.stop();
              } catch (Throwable t) {
                 log.error(t, t);
              } finally {
                 httpClient = null;
              }

        }

        /*
         * Restore the log level for the utility class that logs the
         * output of the child process to its default.
         */
        
        if (oldProcessHelperLevel != null) {

            final Logger tmp = Logger.getLogger(ProcessHelper.class);

            tmp.setLevel(oldProcessHelperLevel);

            oldProcessHelperLevel = null;
            
        }

        if (log.isInfoEnabled())
           log.info("---- TEST END " + getName() + "----");

    }

    /**
     * Check the HAStatusEnum for the service.
     */
    protected void awaitHAStatus(final HAGlue haGlue,
            final HAStatusEnum expected) throws IOException {

        awaitHAStatus(5, TimeUnit.SECONDS, haGlue, expected);
        
    }

    /**
     * Check the HAStatusEnum for the service.
     */
    protected void awaitHAStatus(final long timeout, final TimeUnit unit,
            final HAGlue haGlue, final HAStatusEnum expected)
            throws IOException {

        assertCondition(new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals(expected, haGlue.getHAStatus());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, timeout, unit);

    }

    /**
     * Check the {@link HAStatusEnum} of each service against the expected
     * value.
     * 
     * @param expected
     * @param haGlue
     */
    protected void awaitHAStatus(final HAStatusEnum[] expected,
            final HAGlue[] haGlue) throws IOException {
        assertEquals(expected.length, haGlue.length);
        assertCondition(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < expected.length; i++) {
                        final HAStatusEnum e = expected[i];
                        final HAStatusEnum a = haGlue[i].getHAStatus();
                        if (!e.equals(a)) {
                            assertEquals("Service[" + i + "]", e, a);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 5, TimeUnit.SECONDS);

    }

    protected void awaitRunStateEnum(final RunStateEnum expected,
            final HAGlue... haGlue) throws Exception {
        assertCondition(new Runnable() {
            public void run() {
                try {
                    for (HAGlue aService : haGlue) {
                        final RunStateEnum actual = ((HAGlueTest) aService).getRunStateEnum();
                        assertEquals("Service[" + aService + "]", expected,
                                actual);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 5, TimeUnit.SECONDS);
    }

    /**
     * Check the NSS API by issuing a "GET .../status" request.
     * <p>
     * Note: The NSS server will not allow reads or writes until the
     * quorum meets.
     */
    protected void doNSSStatusRequest(final HAGlue haGlue) throws Exception {

        // Client for talking to the NSS.
//        final HttpClient httpClient = new DefaultHttpClient(ccm);

        // The NSS service URL (NOT the SPARQL end point).
        final String serviceURL = getNanoSparqlServerURL(haGlue);

        final ConnectOptions opts = new ConnectOptions(serviceURL + "/status");

        opts.method = "GET";

		JettyResponseListener response = null;
		try {
			
			final RemoteRepositoryManager rpm = new RemoteRepositoryManager(
					serviceURL, httpClient, executorService);
			try {
				RemoteRepository.checkResponseCode(response = rpm
						.doConnect(opts));

			} finally {
				if (response != null)
					response.abort();
				
				rpm.close();
			}

		} catch (IOException ex) {

            log.error(ex, ex);

            throw ex;

        }

    }

    /**
     * The zookeeper events corresponding to quorum state changes are delivered
     * in the zk event thread. In response to those events the services need to
     * queue tasks to handle the events. Thus, there can be a lag between when
     * we notice a quorum state change and when a service has actually responded
     * to that state change.
     * <p>
     * This method is designed to ensure that the service has noticed some
     * internal state changes which mean that it is ready to participate in a
     * met quorum (as a leader or follower) and further that the service's NSS
     * end point is running (the start of the NSS is also asynchronous.)
     * @throws Exception 
     */
    protected HAStatusEnum awaitNSSAndHAReady(final HAGlue haGlue)
            throws Exception {
        /*
         * Wait for the service to report that it is ready as a leader or
         * follower.
         */
        return awaitNSSAndHAReady(haGlue, awaitQuorumTimeout, TimeUnit.MILLISECONDS);
    }

    protected HAStatusEnum awaitNSSAndHAReady(final HAGlue haGlue, long timout, TimeUnit unit)
            throws Exception {
        /*
         * Wait for the service to report that it is ready as a leader or
         * follower.
         */
        haGlue.awaitHAReady(timout, unit);
        /*
         * Wait for the NSS to report the status of the service (this verifies
         * that the NSS interface is running).
         */
        int retryCount = 5;
        while (true) {
            try {
                final HAStatusEnum s = getNSSHAStatus(haGlue);
                switch (s) {
                case NotReady:
                    continue;
                }
                return s;
            } catch (HttpException httpe) {
                if (httpe.getStatusCode() == 404 && --retryCount > 0) {
                    Thread.sleep(200/* ms */);
                    continue;
                }
                throw httpe;
            }
        }
    }

    /**
     * Issue HTTP request to a service to request its HA status.
     * 
     * @param haGlue
     *            The service.
     * 
     * @throws Exception
     * @throws IOException
     *             This can include a 404 if the REST API is not yet up or has
     *             been shutdown.
     */
    protected HAStatusEnum getNSSHAStatus(final HAGlue haGlue)
            throws Exception, IOException {

        // The NSS service URL (NOT the SPARQL end point).
        final String serviceURL = getNanoSparqlServerURL(haGlue);

        final ConnectOptions opts = new ConnectOptions(serviceURL + "/status?HA");

        opts.method = "GET";

        try {
        	final RemoteRepositoryManager rpm = getRemoteRepository(haGlue, httpClient);
			try {
	            final JettyResponseListener response = rpm.doConnect(opts);
				RemoteRepository.checkResponseCode(response);

				final String s = response.getResponseBody();
				
				return HAStatusEnum.valueOf(s);
			} finally {
				rpm.close();
			}

        } catch (IOException ex) {
            log.error(ex, ex);
            throw ex;
        }

    }

    protected HAStatusEnum getNSSHAStatusAlt(final HAGlue haGlue)
			throws Exception, IOException {

		// The NSS service URL (NOT the SPARQL end point).
		final String serviceURL = getNanoSparqlServerURL(haGlue);
		final String query = serviceURL + "/status?HA";

		try {
			final org.eclipse.jetty.client.api.Request request = httpClient
					.newRequest(query).method(HttpMethod.GET);

			final JettyResponseListener response = new JettyResponseListener(
					request, TimeUnit.SECONDS.toMillis(300));

			request.send(response);
			RemoteRepository.checkResponseCode(response);
			
			final String s = response.getResponseBody();

			return HAStatusEnum.valueOf(s);

		} catch (IOException ex) {
			log.error(ex, ex);
			throw ex;
		}

	}

    /**
     * Return the {@link NanoSparqlServer} end point (NOT the SPARQL end point)
     * for the specified remote service.
     * 
     * @param haGlue
     *            The remove service.
     * 
     * @return The {@link NanoSparqlServer} end point.
     * 
     * @throws IOException
     */
    protected String getNanoSparqlServerURL(final HAGlue haGlue)
            throws IOException {

        return "http://localhost:" + haGlue.getNSSPort()
                + BigdataStatics.getContextPath();

    }

    /**
     * Return a {@link RemoteRepositoryManager} for talking to the
     * {@link NanoSparqlServer} instance associated with an {@link HAGlue}
     * interface.
     * @throws Exception 
     */
    protected RemoteRepositoryManager getRemoteRepository(final HAGlue haGlue, final HttpClient client)
            throws Exception {

        return getRemoteRepository(haGlue, false/* useLoadBalancer */, client);
        
    }

    /**
     * Return a {@link RemoteRepositoryManager} for talking to the
     * {@link NanoSparqlServer} instance associated with an {@link HAGlue}
     * interface.
     * 
     * @param haGlue
     *            The service.
     * @param useLoadBalancer
     *            when <code>true</code> the URL will be the load balancer on
     *            that service and the request MAY be redirected to another
     *            service.
     * @throws Exception 
     */
    protected RemoteRepositoryManager getRemoteRepository(final HAGlue haGlue,
            final boolean useLoadBalancer, final HttpClient client) throws Exception {

        final String serviceURL = getNanoSparqlServerURL(haGlue);
//                + (useLoadBalancer ? "/LBS" : "") 
//                + "/sparql";

        final RemoteRepositoryManager repo = new RemoteRepositoryManager(serviceURL,
                useLoadBalancer, client, executorService);

        return repo;
        
    }

    protected RemoteRepositoryManager getRemoteRepositoryManager(
            final HAGlue haGlue, final boolean useLBS) throws Exception {

        final String endpointURL = getNanoSparqlServerURL(haGlue);

        final RemoteRepositoryManager repo = new RemoteRepositoryManager(
                endpointURL, useLBS, httpClient, executorService);

        return repo;
        
    }

    /**
     * Counts the #of results in a SPARQL result set.
     * 
     * @param result
     *            The connection from which to read the results.
     * 
     * @return The #of results.
     * 
     * @throws Exception
     *             If anything goes wrong.
     */
    protected long countResults(final TupleQueryResult result) throws Exception {

        long count = 0;
        try {

            while (result.hasNext()) {

                result.next();

                count++;

            }
            
        } finally {
            
            result.close();
            
        }

        return count;

    }

    /**
     * Report COUNT(*) for the default SPARQL end point for an {@link HAGlue}
     * instance.
     * 
     * @param haGlue
     *            The service.
     * 
     * @return The value reported by COUNT(*).
     * 
     * @throws Exception
     * @throws IOException
     */
    protected long getCountStar(final HAGlue haGlue) throws IOException,
            Exception {

        return getCountStar(haGlue, false/* useLBS */);

    }

    /**
     * Report COUNT(*) for the default SPARQL end point for an {@link HAGlue}
     * instance.
     * 
     * @param haGlue
     *            The service.
     * @param useLBS
     *            <code>true</code> iff the load balancer end point should be
     *            used for the request.
     * 
     * @return The value reported by COUNT(*).
     * 
     * @throws Exception
     * @throws IOException
     */
    protected long getCountStar(final HAGlue haGlue, final boolean useLBS)
            throws IOException, Exception {

        return new CountStarTask(haGlue, useLBS).call();

    }

    /**
     * Task reports COUNT(*) for the default SPARQL end point for an
     * {@link HAGlue} instance.
     */
    protected class CountStarTask implements Callable<Long> {
        
        /**
         * The SPARQL end point for that service.
         */
        final HAGlue haGlue;
        final boolean useLBS;

        /**
         * Format for timestamps that may be used to correlate with the
         * HA log messages.
         */
        final SimpleDateFormat df = new SimpleDateFormat("hh:mm:ss,SSS");

        /**
         * @param haGlue
         *            The service to query.
         * @param useLBS
         *            <code>true</code> iff the load balanced end point should
         *            be used.
         * 
         * @throws IOException
         */
        public CountStarTask(final HAGlue haGlue, final boolean useLBS)
                throws IOException {

            /*
             * Run query against one of the services.
             */
            this.haGlue = haGlue;
            this.useLBS = useLBS;

        }

        /**
         * Return the #of triples reported by <code>COUNT(*)</code> for
         * the SPARQL end point.
         */
        @Override
        public Long call() throws Exception {
            
            final String query = "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }";

            // Run query.
            final RemoteRepositoryManager remoteRepo = getRemoteRepository(haGlue, useLBS, httpClient);
            try {
	            final TupleQueryResult result = remoteRepo.prepareTupleQuery(query)
	                    .evaluate();
	
	            final BindingSet bs = result.next();
	
	            // done.
	            final Value v = bs.getBinding("count").getValue();
	            
	            return ((org.openrdf.model.Literal) v).longValue();
            } finally {
            	remoteRepo.close();
            }

        }

    }

    /**
     * Wait until the KB exists.
     * 
     * Note: There is a data race when creating the a KB (especially the default
     * KB) and verifying that the KB exists. If we find the KB in the row store
     * cache but we do not find the axioms, then the subsequent attempts to
     * resolve the KB fail - probably due to an issue with the default resource
     * locator cache.
     * 
     * <pre>
     * INFO : 41211 2012-11-06 08:38:41,874 : WARN : 8542 2012-11-06 08:38:41,873      qtp877533177-45 org.eclipse.jetty.util.log.Slf4jLog.warn(Slf4jLog.java:50): /sparql
     * INFO : 41211 2012-11-06 08:38:41,874 : java.lang.RuntimeException: java.lang.RuntimeException: java.lang.RuntimeException: No axioms defined? : LocalTripleStore{timestamp=-1, namespace=kb, container=null, indexManager=com.bigdata.journal.jini.ha.HAJournal@4d092447}
     * INFO : 41211 2012-11-06 08:38:41,874 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doEstCard(QueryServlet.java:1120)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doGet(QueryServlet.java:178)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at com.bigdata.rdf.sail.webapp.RESTServlet.doGet(RESTServlet.java:175)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at javax.servlet.http.HttpServlet.service(HttpServlet.java:707)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at javax.servlet.http.HttpServlet.service(HttpServlet.java:820)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.servlet.ServletHolder.handle(ServletHolder.java:534)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.servlet.ServletHandler.doHandle(ServletHandler.java:475)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.ContextHandler.doHandle(ContextHandler.java:929)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.servlet.ServletHandler.doScope(ServletHandler.java:403)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.ContextHandler.doScope(ContextHandler.java:864)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:117)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.HandlerList.handle(HandlerList.java:47)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:114)
     * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.Server.handle(Server.java:352)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.server.HttpConnection.handleRequest(HttpConnection.java:596)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.server.HttpConnection$RequestHandler.headerComplete(HttpConnection.java:1051)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.http.HttpParser.parseNext(HttpParser.java:590)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.http.HttpParser.parseAvailable(HttpParser.java:212)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.server.HttpConnection.handle(HttpConnection.java:426)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.io.nio.SelectChannelEndPoint.handle(SelectChannelEndPoint.java:508)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.io.nio.SelectChannelEndPoint.access$000(SelectChannelEndPoint.java:34)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.io.nio.SelectChannelEndPoint$1.run(SelectChannelEndPoint.java:40)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.util.thread.QueuedThreadPool$2.run(QueuedThreadPool.java:451)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at java.lang.Thread.run(Thread.java:680)
     * INFO : 41212 2012-11-06 08:38:41,875 : Caused by: java.lang.RuntimeException: java.lang.RuntimeException: No axioms defined? : LocalTripleStore{timestamp=-1, namespace=kb, container=null, indexManager=com.bigdata.journal.jini.ha.HAJournal@4d092447}
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doEstCard(QueryServlet.java:1102)
     * INFO : 41212 2012-11-06 08:38:41,875 :    ... 23 more
     * INFO : 41212 2012-11-06 08:38:41,875 : Caused by: java.lang.RuntimeException: No axioms defined? : LocalTripleStore{timestamp=-1, namespace=kb, container=null, indexManager=com.bigdata.journal.jini.ha.HAJournal@4d092447}
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.store.AbstractTripleStore.getAxioms(AbstractTripleStore.java:1787)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.BigdataSail.<init>(BigdataSail.java:934)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.BigdataSail.<init>(BigdataSail.java:891)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.webapp.BigdataRDFContext.getQueryConnection(BigdataRDFContext.java:1858)
     * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doEstCard(QueryServlet.java:1074)
     * INFO : 41212 2012-11-06 08:38:41,875 :    ... 23 more
     * </pre>
     * 
     * @param haGlue
     *            The server.
     * @throws Exception 
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/617" >
     *      Concurrent KB create fails with "No axioms defined?" </a>
     * 
     * @see TestConcurrentKBCreate
     * 
     *      Note: This method sometimes deadlocked in the repo.size() call on
     *      the leader (HTTP end point). This was tracked down to an attempt by
     *      the 3rd node to setup the pipeline in handleReplicatedWrite() when
     *      it was not yet aware that a quorum existed and had not setup its
     *      local root blocks.
     * 
     * @deprecated Use {@link #awaitCommitCounter(long, HAGlue...)} instead.
     *             This method fails to ensure that the initial commit point is
     *             visible on all services the are expected to be joined with
     *             the met quorum. Instead, it relies on polling the specified
     *             service until the KB is visible. This should be modified to
     *             accept a vararg of services to be checked, it should then do
     *             an {@link #awaitCommitCounter(long, HAGlue...)} for
     *             commitCounter:=1 and finally verify that the KB is in fact
     *             visible on each of the services.
     */
    protected void awaitKBExists(final HAGlue haGlue) throws Exception {
      
    	final RemoteRepositoryManager repo = getRemoteRepository(haGlue, httpClient);
        
        try {
	        assertCondition(new Runnable() {
 	           @Override
	            public void run() {
	                try {
	                    repo.size();
	                } catch (Exception e) {
	                    // KB does not exist.
	                    fail();
	                }
	            }
	
	        }, 5, TimeUnit.SECONDS);
        } finally {
        	repo.close();
        }
        
    }
    
    /**
     * A short sleep of the current thread. If interrupted, the interrupt is
     * propagated to the current thread (that is, the interrupt is reset).
     */
    protected void shortSleep() {
        try {
            Thread.sleep(100/*ms*/);
        } catch (InterruptedException e1) {
            // Propagate the interrupt.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Verify that the digests for the backing files on the specified services
     * are equal.
     * 
     * @param services
     *            The services.
     *            
     * @throws NoSuchAlgorithmException
     * @throws DigestException
     * @throws IOException
     */
    protected void assertDigestsEquals(final HAGlue[] services)
            throws NoSuchAlgorithmException, DigestException, IOException {

        // The digest for each HALog file for each commit point.
        final byte[][] digests = new byte[services.length][];

        for (int i = 0; i < services.length; i++) {

            final HAGlue haGlue = services[i];

            if(log.isInfoEnabled()) log.info("Compute Digest for service: " + haGlue.getServiceName());
            
            digests[i] = haGlue.computeDigest(
                    new HADigestRequest(null/* storeUUID */)).getDigest();

            if (i > 0) {

                if (!BytesUtil.bytesEqual(digests[i - 1], digests[i])) {

                    fail("Services have different digest @ i="+i+": serviceA="
                            + services[i - 1] + ", serviceB=" + services[i]);

                }

            }

        }

    }

//    protected void assertReadersEquals(final HAGlue[] services)
//            throws NoSuchAlgorithmException, DigestException, IOException {
//
//        // The digest for each HALog file for each commit point.
//        final byte[][] digests = new byte[services.length][];
//
//        for (int i = 0; i < services.length; i++) {
//
//            final HAGlue haGlue = services[i];
//
//            digests[i] = haGlue.computeDigest(
//                    new HADigestRequest(null/* storeUUID */)).getDigest();
//
//            if (i > 0) {
//
//                if (!BytesUtil.bytesEqual(digests[i - 1], digests[i])) {
//
//                    fail("Services have different digest @ i="+i+": serviceA="
//                            + services[i - 1] + ", serviceB=" + services[i]);
//
//                }
//
//            }
//
//        }
//
//    }

    /**
     * Verify that HALog files were generated and are available for the
     * specified commit points.
     * <p>
     * Note: The HALog files are identified by the commit counter of the closing
     * commit point. The commit counter is ZERO (0) until the first commit.
     * Thus, the commit counter is ONE (1) for the first closing commit point.
     * 
     * @param firstCommitCounter
     *            The first commit point to be verified (inclusive lower bound).
     * @param lastCommitCounter
     *            The last commit point to be verified (inclusive upper bound).
     * @param services
     *            The set of services whose HALog files will be tested. If there
     *            is more than one service, then this method will verify that
     *            the services have the same digests for their HALog files. If
     *            there is only one service, then this will verify that the
     *            HALog file exists by computing its digest.
     * 
     * @throws IOException
     * @throws DigestException
     * @throws NoSuchAlgorithmException
     */
    protected void assertHALogDigestsEquals(final long firstCommitCounter,
            final long lastCommitCounter, final HAGlue[] services)
            throws NoSuchAlgorithmException, DigestException, IOException {

        // The digest for each HALog file for each commit point.
        final byte[][] digests = new byte[services.length][];

        for (long commitCounter = firstCommitCounter; commitCounter <= lastCommitCounter; commitCounter++) {

            for (int i = 0; i < services.length; i++) {

                final HAGlue haGlue = services[i];

                digests[i] = haGlue.computeHALogDigest(
                        new HALogDigestRequest(commitCounter)).getDigest();

                if (i > 0) {

                    if (!BytesUtil.bytesEqual(digests[i - 1], digests[i])) {

                        fail("Services have different digest: commitCounter="
                                + commitCounter + ", serviceA="
                                + services[i - 1] + ", serviceB=" + services[i]);

                    }

                }

            }

        }

    }

    /**
     * Verify that NO HALog files exist for the specified commit points.
     * <p>
     * Note: The HALog files are identified by the commit counter of the closing
     * commit point. The commit counter is ZERO (0) until the first commit.
     * Thus, the commit counter is ONE (1) for the first closing commit point.
     * 
     * @param firstCommitCounter
     *            The first commit point to be verified.
     * @param lastCommitCounter
     *            The last commit point to be verified.
     * @param services
     *            The set of services whose HALog files will be tested.
     * 
     * @throws IOException
     * @throws DigestException
     * @throws NoSuchAlgorithmException
     */
    protected void assertHALogNotFound(final long firstCommitCounter,
            final long lastCommitCounter, final HAGlue[] services)
            throws NoSuchAlgorithmException, DigestException, IOException {

        for (long commitCounter = firstCommitCounter; commitCounter <= lastCommitCounter; commitCounter++) {

            for (int i = 0; i < services.length; i++) {

                final HAGlue haGlue = services[i];

                try {
                    /*
                     * Request the digest.
                     * 
                     * Note: This will through a (wrapped) FileNotFoundException
                     * if the corresponding HALog does not exist.
                     */
                    haGlue.computeHALogDigest(
                            new HALogDigestRequest(commitCounter)).getDigest();
                    fail("HALog exists: commitCounter=" + commitCounter
                            + ", service=" + services[i]);
                } catch (IOException ex) {
                    if (InnerCause
                            .isInnerCause(ex, FileNotFoundException.class)) {
                        // Expected exception.
                        continue;
                    }

                }
                
            }

        }

    }

    /**
     * Verify the the digest of the journal is equal to the digest of the
     * indicated snapshot on the specified service.
     * <p>
     * Note: This can only succeed if the journal is at the specified commit
     * point. If there are concurrent writes on the journal, then it's digest
     * will no longer be consistent with the snapshot.
     * 
     * @param service
     *            The service.
     * @param commitCounter
     *            The commit counter for the snapshot.
     * 
     * @throws NoSuchAlgorithmException
     * @throws DigestException
     * @throws IOException
     */
    protected void assertSnapshotDigestEquals(final HAGlue service,
            final long commitCounter) throws NoSuchAlgorithmException,
            DigestException, IOException {

        final long commitCounterBefore = service
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();
        
        // Verify the journal is at the desired commit point.
        assertEquals(commitCounter, commitCounterBefore);
        
        final byte[] journalDigest = service.computeDigest(
                new HADigestRequest(null/* storeUUID */)).getDigest();

        final long commitCounterAfter = service
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // Verify the journal is still at the desired commit point.
        assertEquals(commitCounter, commitCounterAfter);

        final byte[] snapshotDigest = service.computeHASnapshotDigest(
                new HASnapshotDigestRequest(commitCounter)).getDigest();

        if (!BytesUtil.bytesEqual(journalDigest, snapshotDigest)) {

            /*
             * Note: Provides base 16 rendering as per normal md5 runs.
             */

            final String journalStr = new BigInteger(1, journalDigest)
                    .toString(16);
            
            final String snapshotStr = new BigInteger(1, snapshotDigest)
                    .toString(16);

            fail("journal=" + journalStr + ", snapshot=" + snapshotStr);

//            fail("journal=" + Arrays.toString(journalDigest) + ", snapshot="
//                    + Arrays.toString(snapshotDigest) + " for commitCounter="
//                    + commitCounter + " on service=" + service);

        }
        
    }

//    /**
//     * Assert that the remote server is at the specified commit point.
//     * <p>
//     * This method DOES NOT WAIT. It will fail unless the service is already at
//     * the specified commit point.
//     * 
//     * @param expected
//     *            The expected commit point.
//     * @param haGlue
//     *            The remote server interface(s).
//     * 
//     * @throws IOException
//     * 
//     * @deprecated by {@link #awaitCommitCounter(long, HAGlue...)}.
//     */
//    @Deprecated
//    protected void assertCommitCounter(final long expected, final HAGlue... haGlue)
//            throws IOException {
//
//        for (HAGlue server : haGlue) {
//            assertEquals(
//                    expected,
//                    server.getRootBlock(
//                            new HARootBlockRequest(null/* storeUUID */))
//                            .getRootBlock().getCommitCounter());
//        }
//
//    }

    /**
     * Await a commit point on one or more services, but only a short while as
     * these commit points should be very close together (unless there is a
     * major GC involved or when simultaneously starting several services).
     * <p>
     * Note: The 2-phase commit is not truely simultaneous. In fact, the
     * services lay down their down root block asynchronously. Therefore, the
     * ability to observe a commit point on one service does not guarantee that
     * the same commit point is simultaneously available on the other services.
     * This issue only shows up with methods such as
     * {@link #awaitKBExists(HAGlue)} since they are checking for visibility
     * directly rather than waiting on a commit.
     * <p>
     * If an application issues an mutation request, then the services that vote
     * YES in the 2-phase protocol will each have laid down their updated root
     * block before the mutation request completes.
     * 
     * @param expected
     * @param services
     * @throws IOException
     */
    protected void awaitCommitCounter(final long expected,
            final HAGlue... services) throws IOException {

        /*
         * Note: This explicitly uses a blocking form of the request in order to
         * ensure that we do not observe a root block that has been written on a
         * given service but where that service is not fully done with its share
         * of the commit protocol.
         */
        final IHARootBlockRequest req = new HARootBlockRequest(
                null/* storeUUID */, false/* isNonBlocking */);

        for (HAGlue service : services) {
            final HAGlue haGlue = service;
            assertCondition(new Runnable() {
            	@Override
                public void run() {
                    try {
                        assertEquals(expected, haGlue.getRootBlock(req)
                                .getRootBlock().getCommitCounter());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, 5000, TimeUnit.MILLISECONDS);
        }

    }

    /**
     * Return the name of the foaf data set.
     * 
     * @param string
     *            The data set.
     * @return The name that can be used with a SPARQL UPDATE "LOAD" operation.
     */
    protected String getFoafFileUrl(final String string) {

        /*
         * Note: The file path needs to be absolute since the HAJournalServer
         * instances each run in their own service directory.
         */
 
        final String s = "file:"
                + new File("bigdata-rdf/src/resources/data/foaf/", string)
                        .getAbsolutePath();

        return s;
    }
    
    /**
     * Verify the consistency of the {@link Journal} using {@link DumpJournal}.
     * 
     * @param journal
     *            the journal.
     */
    protected void dumpJournal(final Journal journal) {
		try {
			// Write to file, not String, to remove memory stress
			File file = File.createTempFile("journal", "dump");
			try {	    	
		    	final FileWriter fw = new FileWriter(file, false);
		
		        final PrintWriter w = new PrintWriter(fw);
		
		        // Verify can dump journal.
		        new DumpJournal(journal).dumpJournal(w, null/* all namespaces */,
		                true/* dumpHistory */, true/* dumpPages */,
		                true/* dumpIndices */, false/* showTuples */);
		
		        w.flush();
		
		        w.close();
		        
		        fw.close();
		        
		        if (log.isInfoEnabled()) {
		        	// read/write max 10M
		        	final int MAXWRITE = 10 * 1024 * 1024;
		        	final FileReader reader = new FileReader(file);	        	
		        	final StringWriter sw = new StringWriter();
		        	final char[] buf = new char[4096];
		        	int rdlen;
		        	int total = 0;
		        	while ((rdlen = reader.read(buf)) != -1) {
		        		sw.write(buf, 0, rdlen);
		        		total += rdlen;
		        		if (total > MAXWRITE) {
		        			
		        			break;
		        		}
		        	}
		            log.info(sw.toString());
		        }
			} finally {
				file.delete();
			}
	
		} catch (IOException e) {
			throw new RuntimeException(e);
	    }
    }

    /**
     * Recursively count any files matching the filter.
     * 
     * @param f
     *            A file or directory.
     */
    protected long recursiveCount(final File f, final FileFilter fileFilter) {
       
        return recursiveCount(f, fileFilter, 0/* initialValue */);

    }
    
    private long recursiveCount(final File f, final FileFilter fileFilter,
            long n) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles(fileFilter);

            for (int i = 0; i < children.length; i++) {

                n = recursiveCount(children[i], fileFilter, n);

            }

        } else {

            n++;

        }

        return n;

    }
    
    private void recursiveAdd(final ArrayList<File> files, final File f, final FileFilter fileFilter) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles(fileFilter);

            for (int i = 0; i < children.length; i++) {

               recursiveAdd(files, children[i], fileFilter);

            }

        } else {

            files.add(f);

        }

    }
    
    private Iterator<File> getLogs(final File f, final FileFilter fileFilter) {
    
        final ArrayList<File> files = new ArrayList<File>();

        recursiveAdd(files, f, fileFilter);

        return files.iterator();
        
    }

    protected void awaitLogCount(final File logDir, final long count) {

        assertCondition(new Runnable() {
            @Override
            public void run() {
                assertLogCount(logDir, count);
            }
        }, 10000, TimeUnit.MILLISECONDS);

    }

    /**
     * Note: There is typically some non-determinism around when an HALog file
     * is closed and a new one is opened in a 2-phase commit. Therefore you
     * should generally use {@link #awaitLogCount(File, long)} rather than this
     * method. 
     * 
     * @param logDir
     * @param count
     */
    private void assertLogCount(final File logDir, final long count) {

        final long actual = recursiveCount(logDir, IHALogReader.HALOG_FILTER);

        if (actual != count) {

            final Iterator<File> logs = getLogs(logDir,
                    IHALogReader.HALOG_FILTER);
            StringBuilder fnmes = new StringBuilder();
            while (logs.hasNext()) {
                fnmes.append("\n" + logs.next().getName());
            }

            fail("Actual log files: " + actual + ", expected: " + count
                    + ", files: " + fnmes);

        }

    }

//    /**
//     * The effective name for this test as used to name the directories in which
//     * we store things.
//     * 
//     * TODO If there are method name collisions across the different test
//     * classes then the test suite name can be added to this. Also, if there are
//     * file naming problems, then this value can be munged before it is
//     * returned.
//     */
//    private final String effectiveTestFileName = getClass().getSimpleName()
//            + "." + getName();
//
//    /**
//     * The directory that is the parent of each {@link HAJournalServer}'s
//     * individual service directory.
//     */
//    protected File getTestDir() {
//        return new File(TGT_PATH, getEffectiveTestFileName());
//    }
//
//    /**
//     * The effective name for this test as used to name the directories in which
//     * we store things.
//     */
//    protected String getEffectiveTestFileName() {
//        
//        return effectiveTestFileName;
//        
//    }

}
