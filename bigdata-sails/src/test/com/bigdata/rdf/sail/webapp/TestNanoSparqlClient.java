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

package com.bigdata.rdf.sail.webapp;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import junit.framework.Test;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.IPreparedBooleanQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;
import com.bigdata.rdf.store.BD;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public class TestNanoSparqlClient<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {

    public TestNanoSparqlClient() {

    }

	public TestNanoSparqlClient(final String name) {

		super(name);

	}

	public static Test suite() {
	
        return ProxySuiteHelper.suiteWhenStandalone(TestNanoSparqlClient.class,
                "test_SELECT_ALL", TestMode.quads, TestMode.sids,
                TestMode.triples);
	    
	}

	public void test_startup() throws Exception {

	    assertTrue("open", m_fixture.isRunning());
	    
	}

    /*
     * Verify the correct structure of the webapp.
     * 
     * TODO There should be tests here to verify that we do not allow listing of
     * the directory contents in the web application. This appears to be allowed
     * by default and we do not test to ensure that this option is disabled. E.g.
     * 
     * http://172.16.0.185:8090/bigdata/html/
     * 
     * might list the directory contents.
     */

    /**
     * A marker placed into index.html so we can recognize when that page is
     * served.
     */
    private static final String JUNIT_TEST_MARKER_INDEX_HTML = "junit test marker: index.html";

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
     * Request the SPARQL SERVICE DESCRIPTION for the end point.
     */
    public void test_SERVICE_DESCRIPTION() throws Exception {

        final Graph g = RemoteRepository
                .asGraph(m_repo.getServiceDescription());

        final ValueFactory f = g.getValueFactory();

        // Verify the end point is disclosed.
        assertEquals(
                1,
                countMatches(g, null/* service */, SD.endpoint,
                        f.createURI(m_serviceURL + "/sparql")));

        // Verify description includes supported query and update languages.
        assertEquals(
                1,
                countMatches(g, null/* service */, SD.supportedLanguage,
                        SD.SPARQL10Query));
        assertEquals(
                1,
                countMatches(g, null/* service */, SD.supportedLanguage,
                        SD.SPARQL11Query));
        assertEquals(
                1,
                countMatches(g, null/* service */, SD.supportedLanguage,
                        SD.SPARQL11Update));

        // Verify support for Basic Federated Query is disclosed.
        assertEquals(
                1,
                countMatches(g, null/* service */, SD.feature,
                        SD.BasicFederatedQuery));

    }

    /**
     * "ASK" query with an empty KB and CONNEG for various known/accepted MIME
     * Types.
     */
    public void test_ASK() throws Exception {
        
        final String queryStr = "ASK where {?s ?p ?o}";
        
//        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        {
            final IPreparedBooleanQuery query = m_repo
                    .prepareBooleanQuery(queryStr);
            assertEquals(false, query.evaluate());
        }
        {
            final IPreparedBooleanQuery query = m_repo
                    .prepareBooleanQuery(queryStr);
            query.setHeader("Accept",
                    BooleanQueryResultFormat.SPARQL.getDefaultMIMEType());
            assertEquals(false, query.evaluate());
        }
        {
            final IPreparedBooleanQuery query = m_repo
                    .prepareBooleanQuery(queryStr);
            query.setHeader("Accept",
                    BooleanQueryResultFormat.TEXT.getDefaultMIMEType());
            assertEquals(false, query.evaluate());
        }
        
        /**
         * Uncommented to test CONNEG for JSON (available with openrdf 2.7).
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/588" >
         *      JSON-LD </a>
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/714" >
         *      Migrate to openrdf 2.7 </a>
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/704" >
         *      ask does not return json </a>
         */
        {
            final IPreparedBooleanQuery query = m_repo
                    .prepareBooleanQuery(queryStr);
            query.setHeader("Accept", "application/sparql-results+json");
            assertEquals(false, query.evaluate());
        }
        
    }

//    /**
//     * "ASK" query using POST with an empty KB.
//     */
//    public void test_POST_ASK() throws Exception {
//        
//        final String queryStr = "ASK where {?s ?p ?o}";
//
//        final QueryOptions opts = new QueryOptions();
//        opts.serviceURL = m_serviceURL;
//        opts.queryStr = queryStr;
//        opts.method = "POST";
//
//        opts.acceptHeader = BooleanQueryResultFormat.SPARQL.getDefaultMIMEType();
//        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
//
//        opts.acceptHeader = BooleanQueryResultFormat.TEXT.getDefaultMIMEType();
//        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
//        
//    }

    /**
     * Select everything in the kb using a GET. There will be no solutions
     * (assuming that we are using a told triple kb or quads kb w/o axioms).
     */
	public void test_SELECT_ALL() throws Exception {

		final String queryStr = "select * where {?s ?p ?o}";

        {

            final IPreparedTupleQuery query = m_repo
                    .prepareTupleQuery(queryStr);
            
            assertEquals(0, countResults(query.evaluate()));
            
        }

        {

            final IPreparedTupleQuery query = m_repo
                    .prepareTupleQuery(queryStr);
            
            query.setHeader("Accept",
                    TupleQueryResultFormat.SPARQL.getDefaultMIMEType());

            assertEquals(0, countResults(query.evaluate()));

        }

        {

            final IPreparedTupleQuery query = m_repo
                    .prepareTupleQuery(queryStr);
            
            query.setHeader("Accept",
                    TupleQueryResultFormat.BINARY.getDefaultMIMEType());

            assertEquals(0, countResults(query.evaluate()));

        }

        /**
		 * FIXME The necessary parser does not appear to be available (even with
		 * openrdf 2.7). If you enable this you will get ClassNotFoundException
		 * for <code>au/com/bytecode/opencsv/CSVReader</code>
		 */
        if (false) {

            final IPreparedTupleQuery query = m_repo
                    .prepareTupleQuery(queryStr);
            
            query.setHeader("Accept",
                    TupleQueryResultFormat.CSV.getDefaultMIMEType());

            assertEquals(0, countResults(query.evaluate()));

        }
        
        {

            final IPreparedTupleQuery query = m_repo
                    .prepareTupleQuery(queryStr);
            
            query.setHeader("Accept",
                    TupleQueryResultFormat.TSV.getDefaultMIMEType());

            assertEquals(0, countResults(query.evaluate()));

        }

        /**
		 * Enabled now that we have a JSON result format parser (openrdf 2.7).
		 * 
		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/714" >
		 *      Migrate to openrdf 2.7 </a>
		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/588" >
		 *      JSON-LD </a>
		 */
        if (true) {

            final IPreparedTupleQuery query = m_repo
                    .prepareTupleQuery(queryStr);
            
            query.setHeader("Accept",
                    TupleQueryResultFormat.JSON.getDefaultMIMEType());

            assertEquals(0, countResults(query.evaluate()));

        }
        
	}

    /**
     * A GET query which should result in an error (the query is not well
     * formed).
     */
    public void test_GET_SELECT_ERROR() throws Exception {

        final String queryStr = "select * where {?s ?p ?o} X {}";

        final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
        
        try {
		
        	assertEquals(0, countResults(query.evaluate()));
        	
        	fail("should be an error");
        	
        } catch (IOException ex) {
        	
        	// perfect
        	
        }

    }
    
    public void test_POST_INSERT_withBody_RDFXML() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.RDFXML);
        
    }
    
    public void test_POST_INSERT_withBody_NTRIPLES() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.NTRIPLES);
        
    }
    
    public void test_POST_INSERT_withBody_N3() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.N3);
        
    }
    
    public void test_POST_INSERT_withBody_TURTLE() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.TURTLE);
        
    }
    
    // Note: quads interchange
    public void test_POST_INSERT_withBody_TRIG() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.TRIG);
        
    }
    
    // Note: quads interchange
    public void test_POST_INSERT_withBody_TRIX() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.TRIX);
        
    }

//    // Note: quads interchange
    public void test_POST_INSERT_withBody_NQUADS() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.NQUADS);
        
    }

    // TODO Write test for UPDATE where we override the default context using
    // the context-uri.
    public void test_POST_INSERT_triples_with_BODY_and_defaultContext()
            throws Exception {

        if(TestMode.quads != testMode)
            return;

        final String resource = packagePath
                + "insert_triples_with_defaultContext.ttl";

        final Graph g = loadGraphFromResource(resource);

        // Load the resource into the KB.
        doInsertByBody("POST", RDFFormat.TURTLE, g, new URIImpl(
                "http://example.org"));
        
        // Verify that the data were inserted into the appropriate context.
        {
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = "GET";
//            opts.queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
//            assertEquals(7, countResults(doSparqlQuery(opts, requestPath)));
        	
        	final String queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
//            final RemoteRepository repo = new RemoteRepository(m_serviceURL);
            final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
    		assertEquals(7, countResults(query.evaluate()));

        }

    }
    
    public void test_POST_INSERT_triples_with_URI_and_defaultContext() throws Exception {

        if(TestMode.quads != testMode)
            return;
        
//        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        
        // Load the resource into the KB.
        {
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = "POST";
//            opts.requestParams = new LinkedHashMap<String, String[]>();
//            // set the resource to load.
//            opts.requestParams.put("uri", new String[] { new File(packagePath
//                    + "insert_triples_with_defaultContext.ttl").toURI()
//                    .toString() });
//            // set the default context.
//            opts.requestParams.put("context-uri",
//                    new String[] { "http://example.org" });
//            assertEquals(
//                    7,
//                    getMutationResult(doSparqlQuery(opts, requestPath)).mutationCount);
            
            final AddOp add = new AddOp(new File(packagePath
                    + "insert_triples_with_defaultContext.ttl").toURI().toString());
            add.setContext(new URIImpl("http://example.org"));
            assertEquals(7, m_repo.add(add));
            
        }

        // Verify that the data were inserted into the appropriate context.
        {
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = "GET";
//            opts.queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
//            assertEquals(7, countResults(doSparqlQuery(opts, requestPath)));
            
            final String queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
            final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
            assertEquals(7, countResults(query.evaluate()));
            
        }
        
    }

    /**
     * Test for POST of an NQuads resource by a URL.
     */
    public void test_POST_INSERT_NQuads_by_URL()
            throws Exception {

        if(TestMode.quads != testMode)
            return;

//        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        
        // Verify nothing in the KB.
        {
            final String queryStr = "ASK where {?s ?p ?o}";

//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.queryStr = queryStr;
//            opts.method = "GET";
//
//            opts.acceptHeader = BooleanQueryResultFormat.SPARQL
//                    .getDefaultMIMEType();
//            assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
            
            final IPreparedBooleanQuery query = m_repo.prepareBooleanQuery(queryStr);
            assertEquals(false, query.evaluate());
            
            
        }

        // #of statements in that RDF file.
        final long expectedStatementCount = 7;
        
        // Load the resource into the KB.
        {
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = "POST";
//            opts.requestParams = new LinkedHashMap<String, String[]>();
//            opts.requestParams
//                    .put("uri",
//                            new String[] { "file:bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/quads.nq" });
//
//            final MutationResult result = getMutationResult(doSparqlQuery(opts,
//                    requestPath));
//
//            assertEquals(expectedStatementCount, result.mutationCount);
            
            final AddOp add = new AddOp("file:bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/quads.nq");
            assertEquals(expectedStatementCount, m_repo.add(add));

        }

        /*
         * Verify KB has the loaded data.
         */
        {
            final String queryStr = "SELECT * where {?s ?p ?o}";

//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.queryStr = queryStr;
//            opts.method = "GET";
//
//            opts.acceptHeader = BooleanQueryResultFormat.SPARQL
//                    .getDefaultMIMEType();
//
//            assertEquals(expectedStatementCount, countResults(doSparqlQuery(
//                    opts, requestPath)));
            
            final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
            assertEquals(expectedStatementCount, countResults(query.evaluate()));
            
        }

    }
        
    /**
     * Test of insert and retrieval of a large literal.
     */
    public void test_INSERT_veryLargeLiteral() throws Exception {

        final Graph g = new GraphImpl();
        
        final URI s = new URIImpl("http://www.bigdata.com/");
        final URI p = RDFS.LABEL;
        final Literal o = getVeryLargeLiteral();
        final Statement stmt = new StatementImpl(s, p, o);
        g.add(stmt);
        
        // Load the resource into the KB.
        assertEquals(
                1L,
                doInsertByBody("POST", RDFFormat.RDFXML, g, null/* defaultContext */));

        // Read back the data into a graph.
        final Graph g2;
        {
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = "GET";
//            opts.queryStr = "DESCRIBE <" + s.stringValue() + ">";
//            g2 = buildGraph(doSparqlQuery(opts, requestPath));
            
//            final RemoteRepository repo = new RemoteRepository(m_serviceURL);
            final String queryStr = "DESCRIBE <" + s.stringValue() + ">";
            final IPreparedGraphQuery query = m_repo.prepareGraphQuery(queryStr);
            g2 = asGraph(query.evaluate());
            
        }
        
        assertEquals(1, g2.size());
        
        assertTrue(g2.match(s, p, o).hasNext());
        
    }
    
    /**
     * Test ability to load data from a URI.
     */
    public void test_POST_INSERT_LOAD_FROM_URIs() throws Exception {

//    	final RemoteRepository repo = new RemoteRepository(m_serviceURL);
    	
        // Verify nothing in the KB.
        {
            final String queryStr = "ASK where {?s ?p ?o}";

//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.queryStr = queryStr;
//            opts.method = "GET";
//
//            opts.acceptHeader = BooleanQueryResultFormat.SPARQL
//                    .getDefaultMIMEType();
//            assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
            
            final IPreparedBooleanQuery query = m_repo.prepareBooleanQuery(queryStr);
            assertEquals(false, query.evaluate());
            
        }

        // #of statements in that RDF file.
        final long expectedStatementCount = 4;
        
        // Load the resource into the KB.
        {
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = "POST";
//            opts.requestParams = new LinkedHashMap<String, String[]>();
//            opts.requestParams
//                    .put("uri",
//                            new String[] { "file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf" });
//
//            final MutationResult result = getMutationResult(doSparqlQuery(opts,
//                    requestPath));
//
//            assertEquals(expectedStatementCount, result.mutationCount);
            
            final AddOp add = new AddOp("file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf");
            assertEquals(expectedStatementCount, m_repo.add(add));

        }

        /*
         * Verify KB has the loaded data.
         */
        {
            final String queryStr = "SELECT * where {?s ?p ?o}";

//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.queryStr = queryStr;
//            opts.method = "GET";
//
//            opts.acceptHeader = BooleanQueryResultFormat.SPARQL
//                    .getDefaultMIMEType();
//
//            assertEquals(expectedStatementCount, countResults(doSparqlQuery(
//                    opts, requestPath)));
            
            final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
            assertEquals(expectedStatementCount, countResults(query.evaluate()));
            
        }

    }

    /**
     * Test the ESTCARD method (fast range count).
     */
    public void test_ESTCARD() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
  
        /*
         * Note: In this case, it should work out that the exact size and the
         * fast range count are the same. However, we want the FAST RANGE COUNT
         * here since that is what we are testing.
         */
        final long rangeCount = m_repo.size();
        
        assertEquals(7, rangeCount);
        
    }

    public void test_ESTCARD_s() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
        
        final long rangeCount = m_repo.rangeCount(new URIImpl(
                "http://www.bigdata.com/Mike"),// s
                null,// p
                null// o
                );

        assertEquals(3, rangeCount);
        
    }
    
    public void test_ESTCARD_p() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
        
        final long rangeCount = m_repo.rangeCount(
                null,// s
                RDF.TYPE,// p
                null// o
//                null // c
        );
        assertEquals(3, rangeCount);
        
    }

    public void test_ESTCARD_p2() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
                
        final long rangeCount = m_repo.rangeCount(
                null,// s
                RDFS.LABEL,// p
                null// o
//                null // c
        );

        assertEquals(2, rangeCount);
        
    }

    public void test_ESTCARD_o() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
  
        final long rangeCount = m_repo.rangeCount(
                null,// s
                null,// p
                new LiteralImpl("Mike")// o
                // null // c
        );
        
        assertEquals(1, rangeCount);
        
    }

    public void test_ESTCARD_so() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");

        final long rangeCount = m_repo.rangeCount(
                new URIImpl("http://www.bigdata.com/Mike"),// s,
                RDF.TYPE,// p
                null//,// o
//                null // c
        );

        assertEquals(1, rangeCount);
        
    }

    /**
     * Test the ESTCARD method (fast range count).
     */
    public void test_ESTCARD_quads_01() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", packagePath
                + "test_estcard.trig");
        
        final long rangeCount = m_repo.rangeCount(
                null,// s,
                null,// p
                null// o
//                null // c
        );
        assertEquals(7, rangeCount);
        
    }
    
    public void test_ESTCARD_quads_02() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", packagePath
                + "test_estcard.trig");
        
        final long rangeCount = m_repo.rangeCount(
                null,// s,
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/")// c
        );

        assertEquals(3, rangeCount);
        
    }
    
    public void test_ESTCARD_quads_03() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", packagePath
                + "test_estcard.trig");
        
        final long rangeCount = m_repo.rangeCount(
                null,// s,
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/c1")// c
        );
        
        assertEquals(2, rangeCount);
        
    }

    public void test_ESTCARD_quads_04() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", packagePath
                + "test_estcard.trig");

        final long rangeCount = m_repo.rangeCount(
                new URIImpl("http://www.bigdata.com/Mike"),// s,
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/c1")// c
        );
        
        assertEquals(1, rangeCount);
        
    }
    
    /**
     * Test the CONTEXTS method.
     */
    public void test_CONTEXTS() throws Exception {

    	if (testMode != TestMode.quads)
    		return;

        doInsertbyURL("POST", packagePath
                + "test_estcard.trig");
        
        final Collection<Resource> contexts = m_repo.getContexts();
        
        assertEquals(3, contexts.size());
        
    }

    /**
     * Select everything in the kb using a POST.
     */
    public void test_DELETE_withQuery() throws Exception {

//        final String queryStr = "select * where {?s ?p ?o}";

//        final QueryOptions opts = new QueryOptions();
//        opts.serviceURL = m_serviceURL;
//        opts.queryStr = queryStr;
//        opts.method = "POST";

        doInsertWithBodyTest("POST", 23, RDFFormat.NTRIPLES);

//        assertEquals(23, countResults(doSparqlQuery(opts, requestPath)));
        assertEquals(23, countAll());

        doDeleteWithQuery("construct {?s ?p ?o} where {?s ?p ?o}");

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
//        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
        assertEquals(0, countAll());
        
    }

    /**
     * Delete everything matching an access path description.
     */
    public void test_DELETE_accessPath_delete_all() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                null
        );

        assertEquals(7, mutationResult);
        
    }
    
    /**
     * Delete everything with a specific subject.
     */
    public void test_DELETE_accessPath_delete_s() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                new URIImpl("http://www.bigdata.com/Mike"),// s
                null,// p
                null
        );

        assertEquals(3, mutationResult);
        
    }

    /**
     * Delete everything with a specific predicate.
     */
    public void test_DELETE_accessPath_delete_p() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                new URIImpl("http://www.w3.org/2000/01/rdf-schema#label"),// p
                null// o
        );

        assertEquals(2, mutationResult);
        
    }
    
    /**
     * Delete everything with a specific object (a URI).
     */
    public void test_DELETE_accessPath_delete_o_URI() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                new URIImpl("http://xmlns.com/foaf/0.1/Person")// o
        );

        assertEquals(3, mutationResult);
        
    }
    
    /**
     * Delete everything with a specific object (a Literal).
     */
    public void test_DELETE_accessPath_delete_o_Literal() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                new URIImpl("http://www.bigdata.com/Bryan")// o
        );

        assertEquals(1, mutationResult);
        
    }
    
    /**
     * Delete everything with a specific predicate and object (a URI).
     */
    public void test_DELETE_accessPath_delete_p_o_URI() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                RDF.TYPE,// p
                new URIImpl("http://xmlns.com/foaf/0.1/Person")// o
        );

        assertEquals(3, mutationResult);
        
    }
    
    /**
     * Delete everything with a specific predicate and object (a Literal).
     */
    public void test_DELETE_accessPath_delete_p_o_Literal() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                RDFS.LABEL,// p
                new LiteralImpl("Bryan")// o
        );

        assertEquals(1, mutationResult);
        
    }
    
    /**
     * Delete using an access path which does not match anything.
     */
    public void test_DELETE_accessPath_delete_NothingMatched() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                new URIImpl("http://xmlns.com/foaf/0.1/XXX")// o
        );

        assertEquals(0, mutationResult);
        
    }

    /**
     * Delete everything in a named graph (context).
     */
    public void test_DELETE_accessPath_delete_c() throws Exception {

        if(TestMode.quads != testMode)
            return;
        
        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.trig");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/") // c
        );

        assertEquals(3, mutationResult);
        
    }

    /**
     * Delete everything in a different named graph (context).
     */
    public void test_DELETE_accessPath_delete_c1() throws Exception {

        if(TestMode.quads != testMode)
            return;
        
        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.trig");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/c1") // c
        );

        assertEquals(2, mutationResult);
        
    }

    /**
     * Delete using an access path with the context position bound. 
     */
    public void test_DELETE_accessPath_delete_c_nothingMatched() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.trig");

        final long mutationResult = doDeleteWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                null,// o
                new URIImpl("http://xmlns.com/foaf/0.1/XXX") // c
        );

        assertEquals(0, mutationResult);
        
    }
    
    public void test_DELETE_withPOST_RDFXML() throws Exception {
        doDeleteWithPostTest(RDFFormat.RDFXML);
    }

    public void test_DELETE_withPOST_NTRIPLES() throws Exception {
        doDeleteWithPostTest(RDFFormat.NTRIPLES);
    }

    public void test_DELETE_withPOST_N3() throws Exception {
        doDeleteWithPostTest(RDFFormat.N3);
    }

    public void test_DELETE_withPOST_TURTLE() throws Exception {
        doDeleteWithPostTest(RDFFormat.TURTLE);
    }

    public void test_DELETE_withPOST_TRIG() throws Exception {
        doDeleteWithPostTest(RDFFormat.TRIG);
    }

    public void test_DELETE_withPOST_TRIX() throws Exception {
        doDeleteWithPostTest(RDFFormat.TRIX);
    }

    public void test_GET_DESCRIBE_RDFXML() throws Exception {
        doDescribeTest("GET", RDFFormat.RDFXML);
    }

    public void test_GET_DESCRIBE_NTRIPLES() throws Exception {
        doDescribeTest("GET", RDFFormat.NTRIPLES);
    }

    public void test_GET_DESCRIBE_N3() throws Exception {
        doDescribeTest("GET", RDFFormat.N3);
    }

    public void test_GET_DESCRIBE_TURTLE() throws Exception {
        doDescribeTest("GET", RDFFormat.TURTLE);
    }

    public void test_GET_DESCRIBE_TRIG() throws Exception {
        doDescribeTest("GET", RDFFormat.TRIG);
    }

    public void test_GET_DESCRIBE_TRIX() throws Exception {
        doDescribeTest("GET", RDFFormat.TRIX);
    }

    public void test_POST_DESCRIBE_RDFXML() throws Exception {
        doDescribeTest("POST", RDFFormat.RDFXML);
    }

    public void test_POST_DESCRIBE_NTRIPLES() throws Exception {
        doDescribeTest("POST", RDFFormat.NTRIPLES);
    }

    public void test_POST_DESCRIBE_N3() throws Exception {
        doDescribeTest("POST", RDFFormat.N3);
    }

    public void test_POST_DESCRIBE_TURTLE() throws Exception {
        doDescribeTest("POST", RDFFormat.TURTLE);
    }

    public void test_POST_DESCRIBE_TRIG() throws Exception {
        doDescribeTest("POST", RDFFormat.TRIG);
    }

    public void test_POST_DESCRIBE_TRIX() throws Exception {
        doDescribeTest("POST", RDFFormat.TRIX);
    }

    public void test_GET_CONSTRUCT_RDFXML() throws Exception {
        doConstructTest("GET",RDFFormat.RDFXML);
    }
    public void test_GET_CONSTRUCT_NTRIPLES() throws Exception {
        doConstructTest("GET",RDFFormat.NTRIPLES);
    }
    public void test_GET_CONSTRUCT_N3() throws Exception {
        doConstructTest("GET",RDFFormat.N3);
    }
    public void test_GET_CONSTRUCT_TURTLE() throws Exception {
        doConstructTest("GET",RDFFormat.TURTLE);
    }
    public void test_GET_CONSTRUCT_TRIG() throws Exception {
        doConstructTest("GET",RDFFormat.TRIG);
    }
    public void test_GET_CONSTRUCT_TRIX() throws Exception {
        doConstructTest("GET",RDFFormat.TRIX);
    }
    
    public void test_POST_CONSTRUCT_RDFXML() throws Exception {
        doConstructTest("POST",RDFFormat.RDFXML);
    }
    public void test_POST_CONSTRUCT_NTRIPLES() throws Exception {
        doConstructTest("POST",RDFFormat.NTRIPLES);
    }
    public void test_POST_CONSTRUCT_N3() throws Exception {
        doConstructTest("POST",RDFFormat.N3);
    }
    public void test_POST_CONSTRUCT_TURTLE() throws Exception {
        doConstructTest("POST",RDFFormat.TURTLE);
    }
    public void test_POST_CONSTRUCT_TRIG() throws Exception {
        doConstructTest("POST",RDFFormat.TRIG);
    }
    public void test_POST_CONSTRUCT_TRIX() throws Exception {
        doConstructTest("POST",RDFFormat.TRIX);
    }
    
    /**
     * Unit test for ACID UPDATE using PUT. This test is for the operation where
     * a SPARQL selects the data to be deleted and the request body contains the
     * statements to be inserted.
     */
    public void test_PUT_UPDATE_WITH_QUERY() throws Exception {

        setupDataOnServer();

//        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
//        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");

        // The format used to PUT the data.
        final RDFFormat format = RDFFormat.NTRIPLES;
        
        /*
         * This is the query that we will use to delete some triples from the
         * database.
         */
        final String deleteQueryStr =//
            "prefix bd: <"+BD.NAMESPACE+"> " +//
            "prefix rdf: <"+RDF.NAMESPACE+"> " +//
            "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
            "CONSTRUCT { ?x bd:likes bd:RDFS }" +//
            "WHERE { " +//
//            "  ?x rdf:type bd:Person . " +//
            "  ?x bd:likes bd:RDFS " +//
            "}";

        /*
         * First, run the query that we will use the delete the triples. This
         * is a cross check on the expected behavior of the query.
         */
        {

            // The expected results.
            final Graph expected = new GraphImpl();
            {
//                expected.add(new StatementImpl(mike, RDF.TYPE, person));
                expected.add(new StatementImpl(bryan, likes, rdfs));
            }

//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.queryStr = deleteQueryStr;
//            opts.method = "GET";
//            opts.acceptHeader = TupleQueryResultFormat.SPARQL
//                    .getDefaultMIMEType();
//            
//            assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
//                    requestPath)));

            final IPreparedGraphQuery query = m_repo.prepareGraphQuery(deleteQueryStr);
            assertSameGraph(expected, asGraph(query.evaluate()));
            
        }

        /*
         * Setup the document containing the statement to be inserted by the
         * UPDATE operation.
         */
        final byte[] data;
        {
            final Graph g = new GraphImpl();
            
            // The new data.
            g.add(new StatementImpl(bryan, likes, rdf));

            final RDFWriterFactory writerFactory = RDFWriterRegistry
                    .getInstance().get(format);
            if (writerFactory == null)
                fail("RDFWriterFactory not found: format=" + format);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final RDFWriter writer = writerFactory.getWriter(baos);
            writer.startRDF();
            for (Statement stmt : g) {
                writer.handleStatement(stmt);
            }
            writer.endRDF();
            data = baos.toByteArray();
        }

        /*
         * Now, run the UPDATE operation.
         */
        {

//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.queryStr = deleteQueryStr;
//            opts.method = "PUT";
//            //opts.acceptHeader = ...;
//            opts.contentType = RDFFormat.NTRIPLES.getDefaultMIMEType();
//            opts.data = data;
//            final MutationResult ret = getMutationResult(doSparqlQuery(opts,
//                    requestPath));
//            assertEquals(2, ret.mutationCount);// FIXME 1 removed, but also 1 added.
            
            final RemoveOp remove = new RemoveOp(deleteQueryStr);
            final AddOp add = new AddOp(data, format);
            assertEquals(2, m_repo.update(remove, add));
            
        }
        
        /*
         * Now verify the post-condition state.
         */
        {

            /*
             * This query verifies that we removed the right triple (nobody is
             * left who likes 'rdfs').
             */
            {
  
                // The expected results.
                final Graph expected = new GraphImpl();

//                final QueryOptions opts = new QueryOptions();
//                opts.serviceURL = m_serviceURL;
//                opts.queryStr = deleteQueryStr;
//                opts.method = "GET";
//                opts.acceptHeader = TupleQueryResultFormat.SPARQL
//                        .getDefaultMIMEType();
//
//                assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
//                        requestPath)));
                
                assertSameGraph(expected, m_repo.prepareGraphQuery(deleteQueryStr).evaluate());

            }

            /* This query verifies that we added the right triple (two people
             * now like 'rdf').
             */
            {

                final String queryStr2 = //
                    "prefix bd: <" + BD.NAMESPACE + "> " + //
                    "prefix rdf: <" + RDF.NAMESPACE + "> " + //
                    "prefix rdfs: <" + RDFS.NAMESPACE + "> " + //
                    "CONSTRUCT { ?x bd:likes bd:RDF }" + //
                    "WHERE { " + //
//                    "  ?x rdf:type bd:Person . " + //
                    "  ?x bd:likes bd:RDF " + //
                    "}";
                
                // The expected results.
                final Graph expected = new GraphImpl();

                expected.add(new StatementImpl(mike, likes, rdf));
                expected.add(new StatementImpl(bryan, likes, rdf));

//                final QueryOptions opts = new QueryOptions();
//                opts.serviceURL = m_serviceURL;
//                opts.queryStr = queryStr2;
//                opts.method = "GET";
//                opts.acceptHeader = TupleQueryResultFormat.SPARQL
//                        .getDefaultMIMEType();
//
//                assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
//                        requestPath)));

                assertSameGraph(expected, m_repo.prepareGraphQuery(queryStr2).evaluate());

            }

        }

    }

//    /**
//     * Unit test verifies that you can have a CONSTRUCT SPARQL with an empty
//     * WHERE clause.
//     * 
//     * @throws MalformedQueryException
//     */
//    public void test_CONSTRUCT_TEMPLATE_ONLY() throws MalformedQueryException {
//
//        final String deleteQueryStr =//
//            "prefix bd: <"+BD.NAMESPACE+"> " +//
//            "CONSTRUCT { bd:Bryan bd:likes bd:RDFS }" +//
//            "{}";
//
//        new BigdataSPARQLParser().parseQuery(deleteQueryStr,
//                "http://www.bigdata.com");
//
//    }
    
    /**
     * Unit test where the "query" used to delete triples from the database
     * consists solely of a CONSTRUCT "template" without a WHERE clause (the
     * WHERE clause is basically optional as all elements of it are optional).
     * 
     * @throws Exception
     */
    public void test_PUT_UPDATE_WITH_CONSTRUCT_TEMPLATE_ONLY() throws Exception {

        setupDataOnServer();

//        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
//        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");

        // The format used to PUT the data.
        final RDFFormat format = RDFFormat.NTRIPLES;
        
        /*
         * This is the query that we will use to delete some triples from the
         * database.
         */
        final String deleteQueryStr =//
            "prefix bd: <"+BD.NAMESPACE+"> " +//
            "CONSTRUCT { bd:Bryan bd:likes bd:RDFS }" +//
            "{ }";

//        new BigdataSPARQLParser().parseQuery(deleteQueryStr,
//                "http://www.bigdata.com");
        
        /*
         * First, run the query that we will use the delete the triples. This
         * is a cross check on the expected behavior of the query.
         */
        {

            // The expected results.
            final Graph expected = new GraphImpl();
            {
//                expected.add(new StatementImpl(mike, RDF.TYPE, person));
                expected.add(new StatementImpl(bryan, likes, rdfs));
            }

//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.queryStr = deleteQueryStr;
//            opts.method = "GET";
//            opts.acceptHeader = TupleQueryResultFormat.SPARQL
//                    .getDefaultMIMEType();
//            
//            assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
//                    requestPath)));
            
            assertSameGraph(expected, m_repo.prepareGraphQuery(deleteQueryStr).evaluate());

        }

        /*
         * Setup the document containing the statement to be inserted by the
         * UPDATE operation.
         */
        final byte[] data;
        {
            final Graph g = new GraphImpl();
            
            // The new data.
            g.add(new StatementImpl(bryan, likes, rdf));

            final RDFWriterFactory writerFactory = RDFWriterRegistry
                    .getInstance().get(format);
            if (writerFactory == null)
                fail("RDFWriterFactory not found: format=" + format);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final RDFWriter writer = writerFactory.getWriter(baos);
            writer.startRDF();
            for (Statement stmt : g) {
                writer.handleStatement(stmt);
            }
            writer.endRDF();
            data = baos.toByteArray();
        }

        /*
         * Now, run the UPDATE operation.
         */
        {

//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.queryStr = deleteQueryStr;
//            opts.method = "PUT";
//            //opts.acceptHeader = ...;
//            opts.contentType = RDFFormat.NTRIPLES.getDefaultMIMEType();
//            opts.data = data;
//            final MutationResult ret = getMutationResult(doSparqlQuery(opts,
//                    requestPath));
//            assertEquals(2, ret.mutationCount);// FIXME 1 removed, but also 1 added.
            
            final RemoveOp remove = new RemoveOp(deleteQueryStr);
            final AddOp add = new AddOp(data, format);
            assertEquals(2, m_repo.update(remove, add));
            
        }
        
        /*
         * Now verify the post-condition state.
         */
        {

            /*
             * This query verifies that we removed the right triple (nobody is
             * left who likes 'rdfs').
             */
            {

                final String queryStr2 = //
                    "prefix bd: <" + BD.NAMESPACE + "> " + //
                    "prefix rdf: <" + RDF.NAMESPACE + "> " + //
                    "prefix rdfs: <" + RDFS.NAMESPACE + "> " + //
                    "CONSTRUCT { ?x bd:likes bd:RDFS }" + //
                    "WHERE { " + //
//                    "  ?x rdf:type bd:Person . " + //
                    "  ?x bd:likes bd:RDFS " + // NB: Checks the kb!
                    "}";

                // The expected results.
                final Graph expected = new GraphImpl();

//                final QueryOptions opts = new QueryOptions();
//                opts.serviceURL = m_serviceURL;
//                opts.queryStr = queryStr2;
//                opts.method = "GET";
//                opts.acceptHeader = TupleQueryResultFormat.SPARQL
//                        .getDefaultMIMEType();
//
//                assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
//                        requestPath)));

                assertSameGraph(expected, m_repo.prepareGraphQuery(queryStr2).evaluate());

            }

            /* This query verifies that we added the right triple (two people
             * now like 'rdf').
             */
            {

                final String queryStr2 = //
                    "prefix bd: <" + BD.NAMESPACE + "> " + //
                    "prefix rdf: <" + RDF.NAMESPACE + "> " + //
                    "prefix rdfs: <" + RDFS.NAMESPACE + "> " + //
                    "CONSTRUCT { ?x bd:likes bd:RDF }" + //
                    "WHERE { " + //
//                    "  ?x rdf:type bd:Person . " + //
                    "  ?x bd:likes bd:RDF " + //
                    "}";
                
                // The expected results.
                final Graph expected = new GraphImpl();

                expected.add(new StatementImpl(mike, likes, rdf));
                expected.add(new StatementImpl(bryan, likes, rdf));

//                final QueryOptions opts = new QueryOptions();
//                opts.serviceURL = m_serviceURL;
//                opts.queryStr = queryStr2;
//                opts.method = "GET";
//                opts.acceptHeader = TupleQueryResultFormat.SPARQL
//                        .getDefaultMIMEType();
//
//                assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
//                        requestPath)));

                assertSameGraph(expected, m_repo.prepareGraphQuery(queryStr2).evaluate());

            }

        }

    }

//    /**
//     * Unit test for ACID UPDATE using PUT. This test is for the operation where
//     * the request body is a multi-part MIME document conveying both the
//     * statements to be removed and the statement to be inserted.
//     */
//    public void test_PUT_UPDATE_WITH_MULTI_PART_MIME() {
//        fail("write test");
//    }

}
