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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase2;
import junit.framework.TestListener;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import junit.textui.ResultPrinter;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.util.EntityUtils;

import com.bigdata.BigdataStatics;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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
     * The {@link ClientConnectionManager} for the {@link HttpClient} used by
     * the {@link RemoteRepository}. This is used when we tear down the
     * {@link RemoteRepository}.
     */
    private ClientConnectionManager m_cm;
    
    /**
     * Exposed to tests that do direct HTTP GET/POST operations.
     */
    protected HttpClient m_httpClient;

    /**
     * The client-API wrapper to the NSS.
     */
    protected RemoteRepositoryManager m_repo;

    /**
     * The effective {@link NanoSparqlServer} http end point (including the
     * ContextPath).
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

    public TestNSSHealthCheck(final String name) {//, final String requestURI) {

        super(name);
        
//        m_requestURI = requestURI;
        
    }

    /**
     * FIXME hacked in test suite constructor.
     */
    private static String requestURI;

    @Override
    protected void setUp() throws Exception {
        
        super.setUp();

        m_rootURL = requestURI;

        m_serviceURL = m_rootURL + BigdataStatics.getContextPath();
        
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

        executorService = Executors.newCachedThreadPool(DaemonThreadFactory
                .defaultThreadFactory());

        m_repo = new RemoteRepositoryManager(m_serviceURL, m_httpClient,
                executorService);

    }
    
    @Override
    protected void tearDown() throws Exception {

        m_rootURL = null;
        m_serviceURL = null;

        if (m_cm != null) {
            m_cm.shutdown();
            m_cm = null;
        }

        m_httpClient = null;
        m_repo = null;

        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        
        super.tearDown();
        
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

//    /**
//     * The <code>/namespace/</code> servlet responds (multi-tenancy API).
//     */
//    public void test_webapp_structure_namespace() throws Exception {
//
//        doGET(m_serviceURL + "/namespace/");
//        
//    }

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
    public void test_webapp_structure_contextPath_html_indexHtml() throws Exception {

        doGET(m_serviceURL + "/html/index.html");
    }

    private String doGET(final String url) throws Exception {

        HttpResponse response = null;
        HttpEntity entity = null;

        try {
            
            final ConnectOptions opts = new ConnectOptions(url);
            opts.method = "GET";

            response = doConnect(opts);

            checkResponseCode(url, response);

            entity = response.getEntity();
            
            final String content = EntityUtils.toString(entity);
            
            return content;
            
        } finally {

            try {
                EntityUtils.consume(entity);
            } catch (IOException ex) {
                log.warn(ex, ex);
            }

        }

    }

    /**
     * Connect to a SPARQL end point (GET or POST query only).
     * 
     * @param opts
     *            The connection options.
     * 
     * @return The connection.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/619">
     *      RemoteRepository class should use application/x-www-form-urlencoded
     *      for large POST requests </a>
     */
    private HttpResponse doConnect(final ConnectOptions opts) throws Exception {

        /*
         * Generate the fully formed and encoded URL.
         */
        
        final StringBuilder urlString = new StringBuilder(opts.serviceURL);

        ConnectOptions.addQueryParams(urlString, opts.requestParams);

        final boolean isLongRequestURL = urlString.length() > 1024;

        if (isLongRequestURL && opts.method.equals("POST")
                && opts.entity == null) {

            /*
             * URL is too long. Reset the URL to just the service endpoint and
             * use application/x-www-form-urlencoded entity instead. Only in
             * cases where there is not already a request entity (SPARQL query
             * and SPARQL update).
             */

            urlString.setLength(0);
            urlString.append(opts.serviceURL);

            opts.entity = ConnectOptions.getFormEntity(opts.requestParams);

        } else if (isLongRequestURL && opts.method.equals("GET")
                && opts.entity == null) {

            /*
             * Convert automatically to a POST if the request URL is too long.
             * 
             * Note: [opts.entity == null] should always be true for a GET so
             * this bit is a paranoia check.
             */

            opts.method = "POST";

            urlString.setLength(0);
            urlString.append(opts.serviceURL);

            opts.entity = ConnectOptions.getFormEntity(opts.requestParams);
            
        }

        if (log.isDebugEnabled()) {
            log.debug("*** Request ***");
            log.debug(opts.serviceURL);
            log.debug(opts.method);
            log.debug("query=" + opts.getRequestParam("query"));
            log.debug(urlString.toString());
        }

        HttpUriRequest request = null;
        try {

            request = RemoteRepository.newRequest(urlString.toString(), opts.method);

            if (opts.requestHeaders != null) {

                for (Map.Entry<String, String> e : opts.requestHeaders
                        .entrySet()) {

                    request.addHeader(e.getKey(), e.getValue());

                    if (log.isDebugEnabled())
                        log.debug(e.getKey() + ": " + e.getValue());

                }

            }
            
//            // conn = doConnect(urlString.toString(), opts.method);
//            final URL url = new URL(urlString.toString());
//            conn = (HttpURLConnection) url.openConnection();
//            conn.setRequestMethod(opts.method);
//            conn.setDoOutput(true);
//            conn.setDoInput(true);
//            conn.setUseCaches(false);
//            conn.setReadTimeout(opts.timeout);
//            conn.setRequestProperty("Accept", opts.acceptHeader);
//            if (log.isDebugEnabled())
//                log.debug("Accept: " + opts.acceptHeader);
            
            if (opts.entity != null) {

//                if (opts.data == null)
//                    throw new AssertionError();

//                final String contentLength = Integer.toString(opts.data.length);

//                conn.setRequestProperty("Content-Type", opts.contentType);
//                conn.setRequestProperty("Content-Length", contentLength);

//                if (log.isDebugEnabled()) {
//                    log.debug("Content-Type: " + opts.contentType);
//                    log.debug("Content-Length: " + contentLength);
//                }

//                final ByteArrayEntity entity = new ByteArrayEntity(opts.data);
//                entity.setContentType(opts.contentType);

                ((HttpEntityEnclosingRequestBase) request).setEntity(opts.entity);
                
//                final OutputStream os = conn.getOutputStream();
//                try {
//                    os.write(opts.data);
//                    os.flush();
//                } finally {
//                    os.close();
//                }

            }

            final HttpResponse response = m_httpClient.execute(request);
            
            return response;
            
//            // connect.
//            conn.connect();
//
//            return conn;

        } catch (Throwable t) {
            /*
             * If something goes wrong, then close the http connection.
             * Otherwise, the connection will be closed by the caller.
             */
            try {
                
                if (request != null)
                    request.abort();
                
//                // clean up the connection resources
//                if (conn != null)
//                    conn.disconnect();
                
            } catch (Throwable t2) {
                // ignored.
            }
            throw new RuntimeException(opts.serviceURL + " : " + t, t);
        }

    }
    
    /**
     * Throw an exception if the status code does not indicate success.
     * 
     * @param response
     *            The response.
     *            
     * @return The response.
     * 
     * @throws IOException
     */
    private static HttpResponse checkResponseCode(final String url,
            final HttpResponse response) throws IOException {
        
        final int rc = response.getStatusLine().getStatusCode();
        
        if (rc < 200 || rc >= 300) {
            throw new HttpException(rc, "StatusCode=" + rc + ", StatusLine="
                    + response.getStatusLine() + ", headers="
                    + Arrays.toString(response.getAllHeaders())
                    + ", ResponseBody="
                    + EntityUtils.toString(response.getEntity()));

        }

        if (log.isDebugEnabled()) {
            /*
             * write out the status list, headers, etc.
             */
            log.debug("*** Response ***");
            log.debug("Status Line: " + response.getStatusLine());
        }

        return response;
        
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
