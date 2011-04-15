package com.bigdata.rdf.sail.webapp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase2;

import org.eclipse.jetty.server.Server;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParser;
import org.openrdf.query.resultio.BooleanQueryResultParserFactory;
import org.openrdf.query.resultio.BooleanQueryResultParserRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.TupleQueryResultParserFactory;
import org.openrdf.query.resultio.TupleQueryResultParserRegistry;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.helpers.StatementCollector;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.util.config.NicUtil;

/**
 * Test suite for {@link RESTServlet} (SPARQL end point and REST API for RDF
 * data).
 * 
 * @todo Test default-graph-uri(s) and named-graph-uri(s).
 * 
 * @todo An NQUADS RDFWriter needs to be written. Then we can test NQUADS
 *       interchange.
 * 
 * @todo A SPARQL result sets JSON parser needs to be written (Sesame bundles a
 *       writer, but not a parser) before we can test queries which CONNEG for a
 *       JSON result set.
 * 
 * @todo Add tests for SIDS mode interchange of RDF XML.
 * 
 * @todo The methods which return a mutation count should verify the returned
 *       XML document.
 * 
 * @todo Test suite for reading from a historical commit point.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestNanoSparqlServer.java 4398 2011-04-14 13:55:29Z thompsonbry
 *          $
 */
public class TestNanoSparqlServer extends TestCase2 {

	private Journal m_jnl;
	private Server m_fixture;
	private String m_serviceURL;
	
	final private static String requestPath = "";

	protected void setUp() throws Exception {
	    
		final Properties properties = getProperties();

		final String namespace = getName();

		m_jnl = new Journal(properties);

		// Create the kb instance.
		new LocalTripleStore(m_jnl, namespace, ITx.UNISOLATED, properties).create();

//		/*
//		 * Service will not hold a read lock.
//		 * 
//		 * Queries will read from the last commit point by default and will use
//		 * a read-only tx to have snapshot isolation for that query.
//		 */
//		config.timestamp = ITx.READ_COMMITTED;

        final Map<String, String> initParams = new LinkedHashMap<String, String>();
        {
            
            initParams.put(
                    ConfigParams.NAMESPACE,
                    namespace);
            
        }
        // Start server for that kb instance.
        m_fixture = NanoSparqlServer
                .newInstance(0/* port */, m_jnl, initParams);

        m_fixture.start();

//		final int port = m_fixture.getPort();
		final int port = m_fixture.getConnectors()[0].getLocalPort();

		// log.info("Getting host address");

        final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                true/* loopbackOk */);

        if (hostAddr == null) {

            fail("Could not identify network address for this host.");

        }

        m_serviceURL = new URL("http", hostAddr, port, ""/* file */)
                .toExternalForm();

        // log.info("Setup done: "+serviceURL);

    }

    /**
     * Returns a view of the triple store using the sail interface.
     */
    protected BigdataSail getSail() {

        final String namespace = getName();

        final AbstractTripleStore tripleStore = (AbstractTripleStore) m_jnl
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        return new BigdataSail(tripleStore);

    }

    @Override
	protected void tearDown() throws Exception {

		if (m_fixture != null) {

			m_fixture.stop();

			m_fixture = null;

		}

		if (m_jnl != null) {

			m_jnl.destroy();

			m_jnl = null;

		}

		m_serviceURL = null;

		// log.info("tear down done");

	}

	public Properties getProperties() {

	    final Properties properties = new Properties();

		properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE, BufferMode.Transient.toString());

		properties.setProperty(com.bigdata.journal.Options.INITIAL_EXTENT, "" + (Bytes.megabyte32 * 1));

		// if(false/*quads*/) {
		// properties.setProperty(AbstractTripleStore.Options.QUADS_MODE,
		// "true");
		// properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,"false");
		// properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
		// NoAxioms.class.getName());
		// properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
		// NoVocabulary.class.getName());
		// properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
		// "false");
		// }
		// if (true/* told triples */) {
		properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
		properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
		properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
		properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
		// }
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

	public void test_startup() throws Exception {

	    assertTrue("open", m_fixture.isRunning());
	    
	}
	
	/**
	 * Options for the query.
	 */
	private static class QueryOptions {

		/** The default timeout (ms). */
		private static final int DEFAULT_TIMEOUT = 2000;

		/** The URL of the SPARQL endpoint. */
		public String serviceURL = null;
		/** The HTTP method (GET, POST, etc). */
		public String method = "GET";
		/** The SPARQL query. */
		public String queryStr = null;
        /** TODO DG and NG protocol params: The default graph URI (optional). */
        public String defaultGraphUri = null;
        /** The accept header. */
        public String acceptHeader = //
        BigdataRDFServlet.MIME_SPARQL_RESULTS_XML + ";q=1" + //
        "," + //
        RDFFormat.RDFXML.getDefaultMIMEType() + ";q=1"//
        ;
		
		/** The connection timeout (ms) -or- ZERO (0) for an infinite timeout. */
		public int timeout = 0;

	}

    private HttpURLConnection doConnect(final String urlString,
            final String method) throws Exception {

        final URL url = new URL(urlString);

        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod(method);
        conn.setDoOutput(true);
        conn.setUseCaches(false);

        return conn;

    }

    /**
     * Connect to a SPARQL end point (GET or POST query only).
     * 
     * @param opts
     *            The query request.
     * 
     * @return The connection.
     */
    protected HttpURLConnection doSparqlQuery(final QueryOptions opts,
            final String servlet) throws Exception {

		// Fully formed and encoded URL.
		final String urlString = opts.serviceURL
                + "/"
                + servlet
                + "?query="
                + URLEncoder.encode(opts.queryStr, "UTF-8")
                + (opts.defaultGraphUri == null ? ""
                        : ("&default-graph-uri=" + URLEncoder.encode(
                                opts.defaultGraphUri, "UTF-8")));

		HttpURLConnection conn = null;
		try {

		    conn = doConnect(urlString, opts.method);
			
			conn.setReadTimeout(opts.timeout);

            conn.setRequestProperty("Accept", opts.acceptHeader);

			// write out the request headers
			if (log.isDebugEnabled()) {
				log.debug("*** Request ***");
				log.debug(opts.serviceURL);
				log.debug(opts.queryStr);
			}

			// connect.
			conn.connect();

			final int rc = conn.getResponseCode();
			if (rc < 200 || rc >= 300) {
				throw new IOException(conn.getResponseMessage());
			}

			if (log.isDebugEnabled()) {
				/*
				 * write out the status list, headers, etc.
				 */
				log.debug("*** Response ***");
				log.debug("Status Line: " + conn.getResponseMessage());
			}
			return conn;

		} catch (Throwable t) {
			/*
			 * If something goes wrong, then close the http connection.
			 * Otherwise, the connection will be closed by the caller.
			 */
			try {
				// clean up the connection resources
				if (conn != null)
					conn.disconnect();
			} catch (Throwable t2) {
				// ignored.
			}
			throw new RuntimeException(t);
		}

	}

	/**
	 * Builds a graph from an RDF result set (statements, not binding sets).
	 * 
	 * @param conn
	 *            The connection from which to read the results.
	 * 
	 * @return The graph
	 * 
	 * @throws Exception
	 *             If anything goes wrong.
	 */
	protected Graph buildGraph(final HttpURLConnection conn) throws Exception {

		try {

			final String baseURI = "";

			final String contentType = conn.getContentType();

            if (contentType == null)
                fail("Not found: Content-Type");
            
			final RDFFormat format = RDFFormat.forMIMEType(contentType);

            if (format == null)
                fail("RDFFormat not found: Content-Type=" + contentType);

			final RDFParserFactory factory = RDFParserRegistry.getInstance().get(format);

            if (factory == null)
                fail("RDFParserFactory not found: Content-Type=" + contentType
                        + ", format=" + format);

            final Graph g = new GraphImpl();

			final RDFParser rdfParser = factory.getParser();
			
			rdfParser.setValueFactory(new ValueFactoryImpl());

			rdfParser.setVerifyData(true);

			rdfParser.setStopAtFirstError(true);

			rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

			rdfParser.setRDFHandler(new StatementCollector(g));

			rdfParser.parse(conn.getInputStream(), baseURI);

			return g;

		} finally {

			// terminate the http connection.
			conn.disconnect();

		}

	}

    /**
     * Parse a SPARQL result set for an ASK query.
     * 
     * @param conn
     *            The connection from which to read the results.
     * 
     * @return <code>true</code> or <code>false</code> depending on what was
     *         encoded in the SPARQL result set.
     * 
     * @throws Exception
     *             If anything goes wrong, including if the result set does not
     *             encode a single boolean value.
     */
    protected boolean askResults(final HttpURLConnection conn) throws Exception {

        try {

            final String contentType = conn.getContentType();

            final BooleanQueryResultFormat format = BooleanQueryResultFormat
                    .forMIMEType(contentType);

            if (format == null)
                fail("No format for Content-Type: " + contentType);

            final BooleanQueryResultParserFactory factory = BooleanQueryResultParserRegistry
                    .getInstance().get(format);

            if (factory == null)
                fail("No factory for Content-Type: " + contentType);

            final BooleanQueryResultParser parser = factory.getParser();

            final boolean result = parser.parse(conn.getInputStream());
            
            return result;

        } finally {

            // terminate the http connection.
            conn.disconnect();

        }

    }

	/**
	 * Counts the #of results in a SPARQL result set.
	 * 
	 * @param conn
	 *            The connection from which to read the results.
	 * 
	 * @return The #of results.
	 * 
	 * @throws Exception
	 *             If anything goes wrong.
	 */
	protected long countResults(final HttpURLConnection conn) throws Exception {

        try {

            final String contentType = conn.getContentType();

            final TupleQueryResultFormat format = TupleQueryResultFormat
                    .forMIMEType(contentType);

            if (format == null)
                fail("No format for Content-Type: " + contentType);

            final TupleQueryResultParserFactory factory = TupleQueryResultParserRegistry
                    .getInstance().get(format);

            if (factory == null)
                fail("No factory for Content-Type: " + contentType);

			final TupleQueryResultParser parser = factory.getParser();

	        final AtomicLong nsolutions = new AtomicLong();

			parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
				// Indicates the end of a sequence of solutions.
				public void endQueryResult() {
					// connection close is handled in finally{}
				}

				// Handles a solution.
				public void handleSolution(final BindingSet bset) {
					if (log.isDebugEnabled())
						log.debug(bset.toString());
					nsolutions.incrementAndGet();
				}

				// Indicates the start of a sequence of Solutions.
				public void startQueryResult(List<String> bindingNames) {
				}
			});

			parser.parse(conn.getInputStream());

			if (log.isInfoEnabled())
				log.info("nsolutions=" + nsolutions);

			// done.
			return nsolutions.longValue();

		} finally {

			// terminate the http connection.
			conn.disconnect();

		}

	}

	/**
	 * Issue a "status" request against the service. 
	 */
	public void test_STATUS() throws Exception {

		final HttpURLConnection conn = doConnect(m_serviceURL + "/status", "GET");

		// No solutions (assuming a told triple kb or quads kb w/o axioms).
		
		// connect.
		conn.connect();

		final int rc = conn.getResponseCode();

		if (rc < 200 || rc >= 300) {
		
		    throw new IOException(conn.getResponseMessage());
		    
		}
		
		final String txt = getStreamContents(conn.getInputStream());
		
		System.out.println(txt);

	}

    private String getStreamContents(final InputStream inputStream)
            throws IOException {

        final Reader rdr = new InputStreamReader(inputStream);
		
	    final StringBuffer sb = new StringBuffer();
		
	    final char[] buf = new char[512];
	    
		while (true) {
		
		    final int rdlen = rdr.read(buf);
			
		    if (rdlen == -1)
				break;
			
		    sb.append(buf, 0, rdlen);
		    
		}
		
		return sb.toString();

	}

    /**
     * "ASK" query using GET with an empty KB.
     */
    public void test_GET_ASK() throws Exception {
        
        final String queryStr = "ASK where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "GET";

        opts.acceptHeader = BooleanQueryResultFormat.SPARQL.getDefaultMIMEType();
        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));

        opts.acceptHeader = BooleanQueryResultFormat.TEXT.getDefaultMIMEType();
        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
        
    }

    /**
     * "ASK" query using POST with an empty KB.
     */
    public void test_POST_ASK() throws Exception {
        
        final String queryStr = "ASK where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

        opts.acceptHeader = BooleanQueryResultFormat.SPARQL.getDefaultMIMEType();
        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));

        opts.acceptHeader = BooleanQueryResultFormat.TEXT.getDefaultMIMEType();
        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
        
    }

    /**
     * Select everything in the kb using a GET. There will be no solutions
     * (assuming that we are using a told triple kb or quads kb w/o axioms).
     */
	public void test_GET_SELECT_ALL() throws Exception {

		final String queryStr = "select * where {?s ?p ?o}";

		final QueryOptions opts = new QueryOptions();
		opts.serviceURL = m_serviceURL;
		opts.queryStr = queryStr;
		opts.method = "GET";

		opts.acceptHeader = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
		assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));

		// TODO JSON parser is not bundled by openrdf.
//        opts.acceptHeader = TupleQueryResultFormat.JSON.getDefaultMIMEType();
//        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));

        opts.acceptHeader = TupleQueryResultFormat.BINARY.getDefaultMIMEType();
        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));

	}

    /**
     * Select everything in the kb using a POST. There will be no solutions
     * (assuming that we are using a told triple kb or quads kb w/o axioms).
     */
    public void test_POST_SELECT_ALL() throws Exception {

        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

        opts.acceptHeader = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));

        // TODO JSON parser is not bundled by openrdf.
//        opts.acceptHeader = TupleQueryResultFormat.JSON.getDefaultMIMEType();
//        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));

        opts.acceptHeader = TupleQueryResultFormat.BINARY.getDefaultMIMEType();
        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));

    }

    public void test_POST_UPDATE_withBody_RDFXML() throws Exception {

        do_UPDATE_withBody("POST", 23, requestPath, RDFFormat.RDFXML);
        
    }
    
    public void test_POST_UPDATE_withBody_NTRIPLES() throws Exception {

        do_UPDATE_withBody("POST", 23, requestPath, RDFFormat.NTRIPLES);
        
    }
    
    public void test_POST_UPDATE_withBody_N3() throws Exception {

        do_UPDATE_withBody("POST", 23, requestPath, RDFFormat.N3);
        
    }
    
    public void test_POST_UPDATE_withBody_TURTLE() throws Exception {

        do_UPDATE_withBody("POST", 23, requestPath, RDFFormat.TURTLE);
        
    }
    
    // Note: quads interchange
    public void test_POST_UPDATE_withBody_TRIG() throws Exception {

        do_UPDATE_withBody("POST", 23, requestPath, RDFFormat.TRIG);
        
    }
    
    // Note: quads interchange
    public void test_POST_UPDATE_withBody_TRIX() throws Exception {

        do_UPDATE_withBody("POST", 23, requestPath, RDFFormat.TRIX);
        
    }

    public void test_PUT_UPDATE_withBody_RDFXML() throws Exception {

        do_UPDATE_withBody("PUT", 23, requestPath, RDFFormat.RDFXML);

    }

    public void test_PUT_UPDATE_withBody_NTRIPLES() throws Exception {

        do_UPDATE_withBody("PUT", 23, requestPath, RDFFormat.NTRIPLES);

    }

    public void test_PUT_UPDATE_withBody_N3() throws Exception {

        do_UPDATE_withBody("PUT", 23, requestPath, RDFFormat.N3);

    }

    public void test_PUT_UPDATE_withBody_TURTLE() throws Exception {

        do_UPDATE_withBody("PUT", 23, requestPath, RDFFormat.TURTLE);

    }

    public void test_PUT_UPDATE_withBody_TRIG() throws Exception {

        do_UPDATE_withBody("PUT", 23, requestPath, RDFFormat.TRIG);

    }

    public void test_PUT_UPDATE_withBody_TRIX() throws Exception {

        do_UPDATE_withBody("PUT", 23, requestPath, RDFFormat.TRIX);

    }
	
    /**
     * Select everything in the kb using a POST.
     */
    public void test_DELETE_withQuery() throws Exception {
    	
        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

    	do_UPDATE_withBody("POST", 23, requestPath, RDFFormat.NTRIPLES);
    	
        assertEquals(23, countResults(doSparqlQuery(opts, requestPath)));
    	
    	do_DELETE_with_Query(requestPath, "construct {?s ?p ?o} where {?s ?p ?o}");

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));

    }


    /**
     * Select everything in the kb using a DELETE.
     */
    public void test_REST_DELETE_withQuery() throws Exception {
    	
        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

    	do_UPDATE_withBody("POST", 23, requestPath, RDFFormat.NTRIPLES);
    	
        assertEquals(23, countResults(doSparqlQuery(opts, requestPath)));

        do_DELETE_with_Query(requestPath, "construct {?s ?p ?o} where {?s ?p ?o}");

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
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

    /**
     * Test helps PUTs some data, verifies that it is visible, DELETEs the data,
     * and then verifies that it is gone.
     * 
     * @param format
     *            The interchange format.
     */
    private void doDeleteWithPostTest(final RDFFormat format) throws Exception {

        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

        do_UPDATE_withBody("POST", 23, requestPath, format);

        assertEquals(23, countResults(doSparqlQuery(opts, requestPath)));

        do_DELETE_withBody("", 23, format);

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
        
    }

	private void do_DELETE_with_Query(final String servlet, final String query) {
		HttpURLConnection conn = null;
		try {

			final URL url = new URL(m_serviceURL + "/" + servlet + "?query="
					+ URLEncoder.encode(query, "UTF-8"));
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("DELETE");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(0);// TODO timeout (ms)

			conn.connect();

			if (log.isInfoEnabled())
				log.info(conn.getResponseMessage());

			final int rc = conn.getResponseCode();
			
			if (rc < 200 || rc >= 300) {
				throw new IOException(conn.getResponseMessage());
			}

		} catch (Throwable t) {
			// clean up the connection resources
			if (conn != null)
				conn.disconnect();
			throw new RuntimeException(t);
		}
    }

    private void do_DELETE_withBody(final String servlet, final int ntriples,
            final RDFFormat format) {

        HttpURLConnection conn = null;
		try {

			final URL url = new URL(m_serviceURL + "/" + servlet+"?delete");
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(0);// TODO timeout (ms)

            conn
                    .setRequestProperty("Content-Type", format
                            .getDefaultMIMEType());

            final byte[] data = genNTRIPLES(ntriples, format);
			
            conn.setRequestProperty("Content-Length", ""
                    + Integer.toString(data.length));

			final OutputStream os = conn.getOutputStream();
			try {
			    os.write(data);
				os.flush();
			} finally {
				os.close();
			}

			if (log.isInfoEnabled())
				log.info(conn.getResponseMessage());

			final int rc = conn.getResponseCode();
			
			if (rc < 200 || rc >= 300) {
				throw new IOException(conn.getResponseMessage());
			}

		} catch (Throwable t) {
			// clean up the connection resources
			if (conn != null)
				conn.disconnect();
			throw new RuntimeException(t);
		}
	}

//	/**
//	 * UPDATE should not be allowed with a GET request
//	 */
//	public void test_GETUPDATE_withBody_NTRIPLES() throws Exception {
////		if (JettySparqlServer.directServletAccess) 
//		if(false) {
//			HttpURLConnection conn = null;
//			final URL url = new URL(m_serviceURL + "/update?data=stuff");
//			conn = (HttpURLConnection) url.openConnection();
//			conn.setRequestMethod("GET");
//			conn.setDoOutput(true);
//			conn.setDoInput(true);
//			conn.setUseCaches(false);
//			conn.setReadTimeout(0);// TODO timeout (ms)
//			
//			conn.connect();
//			
//			final int rc = conn.getResponseCode();
//			
//			assertTrue(rc == 405); // NOT_ALLOWED
//		}
//	}

    /**
     * Generates some statements and serializes them using the specified
     * {@link RDFFormat}.
     * 
     * @param ntriples
     *            The #of statements to generate.
     * @param format
     *            The format.
     * 
     * @return the serialized statements.
     */
    final byte[] genNTRIPLES(final int ntriples, final RDFFormat format)
            throws RDFHandlerException {

        final Graph g = new GraphImpl();

        final ValueFactory f = new ValueFactoryImpl();
        
        final URI s = f.createURI("http://www.bigdata.org/b");
        
        final URI rdfType = f
                .createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        
        for (int i = 0; i < ntriples; i++) {
        
            final URI o = f.createURI("http://www.bigdata.org/c#" + i);
            
            g.add(s, rdfType, o);
            
        }
	    
        final RDFWriterFactory writerFactory = RDFWriterRegistry.getInstance()
                .get(format);

        if (writerFactory == null)
            fail("RDFWriterFactory not found: format=" + format);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final RDFWriter writer = writerFactory.getWriter(baos);

        writer.startRDF();

        for (Statement stmt : g) {

            writer.handleStatement(stmt);

        }

        writer.endRDF();

        return baos.toByteArray();
        
	}
	
	/**
	 * FIXME Test of POST w/ BODY having data to be loaded.
	 */
    private void do_UPDATE_withBody(final String method, final int ntriples,
            final String servlet, final RDFFormat format) throws Exception {

		HttpURLConnection conn = null;
		try {

			final URL url = new URL(m_serviceURL + "/" + servlet);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(method);
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(0);// TODO timeout (ms)
			
            conn.setRequestProperty("Content-Type", format
                            .getDefaultMIMEType());

            final byte[] data = genNTRIPLES(ntriples, format);

            conn.setRequestProperty("Content-Length", Integer.toString(data
                    .length));

			final OutputStream os = conn.getOutputStream();
			try {
			    os.write(data);
				os.flush();
			} finally {
				os.close();
			}
			// conn.connect();

			final int rc = conn.getResponseCode();

            if (log.isInfoEnabled()) {
                log.info("*** RESPONSE: " + rc + " for " + method);
                log.info("*** RESPONSE: " + getResponseBody(conn));
            }

			if (rc < 200 || rc >= 300) {

			    throw new IOException(conn.getResponseMessage());
			    
			}

		} catch (Throwable t) {
			// clean up the connection resources
			if (conn != null)
				conn.disconnect();
			throw new RuntimeException(t);
		}

		{
			final String queryStr = "select * where {?s ?p ?o}";

			final QueryOptions opts = new QueryOptions();
			opts.serviceURL = m_serviceURL;
			opts.queryStr = queryStr;
			opts.method = "GET";

			assertEquals(ntriples, countResults(doSparqlQuery(opts, requestPath)));
		}

    }

    private static String getResponseBody(final HttpURLConnection conn)
            throws IOException {

        final Reader r = new InputStreamReader(conn.getInputStream());

        try {

            final StringWriter w = new StringWriter();

            int ch;
            while ((ch = r.read()) != -1) {

                w.append((char) ch);

            }

            return w.toString();
        
        } finally {
            
            r.close();
            
        }
        
    }

    // TODO Also test POST DESCRIBE
    public void test_GET_DESCRIBE_RDFXML() throws Exception {

        doDescribeTest(RDFFormat.RDFXML);

    }

    public void test_GET_DESCRIBE_NTRIPLES() throws Exception {

        doDescribeTest(RDFFormat.NTRIPLES);

    }

    public void test_GET_DESCRIBE_N3() throws Exception {

        doDescribeTest(RDFFormat.N3);

    }

    public void test_GET_DESCRIBE_TURTLE() throws Exception {

        doDescribeTest(RDFFormat.TURTLE);

    }

    public void test_GET_DESCRIBE_TRIG() throws Exception {

        doDescribeTest(RDFFormat.TRIG);

    }

    public void test_GET_DESCRIBE_TRIX() throws Exception {

        doDescribeTest(RDFFormat.TRIX);

    }

    /**
     * Inserts some data into the KB and then issues a DESCRIBE query against
     * the REST API and verifies the expected results.
     * 
     * @param format
     *            The format is used to specify the Accept header.
     *            
     * @throws Exception
     */
    private void doDescribeTest(final RDFFormat format) throws Exception {
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
        final Literal label1 = new LiteralImpl("Mike");
        final Literal label2 = new LiteralImpl("Bryan");

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        
        try {

            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
                    .getConnection();
            try {

                cxn.setAutoCommit(false);

                cxn.add(mike, RDF.TYPE, person);
                cxn.add(mike, likes, rdf);
                cxn.add(mike, RDFS.LABEL, label1);
                cxn.add(bryan, RDF.TYPE, person);
                cxn.add(bryan, likes, rdfs);
                cxn.add(bryan, RDFS.LABEL, label2);

                /*
                 * Note: The either flush() or commit() is required to flush the
                 * statement buffers to the database before executing any
                 * operations that go around the sail.
                 */
                cxn.commit();
            } finally {
                cxn.close();
            }
             
        } finally {
            sail.shutDown();
        }

        // The expected results.
        final Graph expected = new GraphImpl();
        {
            expected.add(new StatementImpl(mike, likes, rdf));
            expected.add(new StatementImpl(mike, RDF.TYPE, person));
            expected.add(new StatementImpl(mike, RDFS.LABEL, label1));
        }
        
        // Run the query and verify the results.
        {
            
            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr =//
                "prefix bd: <"+BD.NAMESPACE+"> " +//
                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
                "DESCRIBE ?x " +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
                "  ?x bd:likes bd:RDF " +//
                "}";
            opts.acceptHeader = format.getDefaultMIMEType();

            final Graph actual = buildGraph(doSparqlQuery(opts, requestPath));

            assertSameGraph(expected, actual);
            
        }

    }
    
    // TODO Test for all RDFFormats.
    // TODO Also test POST CONSTRUCT
    public void test_GET_CONSTRUCT() throws Exception {

        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
        final Literal label1 = new LiteralImpl("Mike");
        final Literal label2 = new LiteralImpl("Bryan");

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        
        try {

            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
                    .getConnection();
            try {

                cxn.setAutoCommit(false);

                cxn.add(mike, RDF.TYPE, person);
                cxn.add(mike, likes, rdf);
                cxn.add(mike, RDFS.LABEL, label1);
                cxn.add(bryan, RDF.TYPE, person);
                cxn.add(bryan, likes, rdfs);
                cxn.add(bryan, RDFS.LABEL, label2);

                /*
                 * Note: The either flush() or commit() is required to flush the
                 * statement buffers to the database before executing any
                 * operations that go around the sail.
                 */
                cxn.commit();
            } finally {
                cxn.close();
            }
             
        } finally {
            sail.shutDown();
        }

        // The expected results.
        final Graph expected = new GraphImpl();
        {
//            expected.add(new StatementImpl(mike, likes, rdf));
            expected.add(new StatementImpl(mike, RDF.TYPE, person));
            expected.add(new StatementImpl(bryan, RDF.TYPE, person));
//            expected.add(new StatementImpl(mike, RDFS.LABEL, label1));
        }
        
        // Run the query and verify the results.
        {
            
            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr =//
                "prefix bd: <"+BD.NAMESPACE+"> " +//
                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
                "CONSTRUCT { ?x rdf:type bd:Person }" +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
//                "  ?x bd:likes bd:RDF " +//
                "}";

            final Graph actual = buildGraph(doSparqlQuery(opts, requestPath));

            assertSameGraph(expected, actual);
            
        }

    }
    
    /**
     * Compare two graphs for equality.
     * <p>
     * Note: This is not very efficient if the {@link Graph} implementations are
     * not indexed.
     * <p>
     * Note: This does not handle equality testing with blank nodes (it does not
     * test for isomorphic graphs).
     * 
     * @param expected
     * @param actual
     */
    protected void assertSameGraph(final Graph expected, final Graph actual) {

        for (Statement s : expected) {

            if (!actual.contains(s))
                fail("Expecting: " + s);

        }

        assertEquals("size", expected.size(), actual.size());

    }

}
