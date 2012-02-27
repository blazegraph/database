package com.bigdata.rdf.sail.webapp;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.eclipse.jetty.server.Server;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
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
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.SailException;
import org.semanticweb.yars.nx.parser.NxParser;
import org.xml.sax.Attributes;
import org.xml.sax.ext.DefaultHandler2;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.rio.nquads.NQuadsParser;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.TestNanoSparqlServerWithProxyIndexManager.TestMode;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.util.config.NicUtil;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public class TestNanoSparqlServer2<S extends IIndexManager> extends ProxyTestCase<S> {

    /**
     * The path used to resolve resources in this package when they are being
     * uploaded to the {@link NanoSparqlServer}.
     */
    private static final String packagePath = "bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/";

	/**
	 * A jetty {@link Server} running a {@link NanoSparqlServer} instance which
	 * is running against that {@link #m_indexManager}.
	 */
	private Server m_fixture;

	/**
	 * The namespace of the {@link AbstractTripleStore} instance against which
	 * the test is running. A unique namespace is used for each test run, but
	 * the namespace is based on the test name.
	 */
	private String namespace;
	
	/**
	 * The effective {@link NanoSparqlServer} http end point.
	 */
	private String m_serviceURL;

	/**
	 * The request path for the REST API under test.
	 */
	final private static String requestPath = "/sparql";

	public TestNanoSparqlServer2() {
		
	}

	public TestNanoSparqlServer2(final String name) {

		super(name);

	}

	private AbstractTripleStore createTripleStore(
			final IIndexManager indexManager, final String namespace,
			final Properties properties) {
        
		if(log.isInfoEnabled())
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
			log.info("Creating KB instance: namespace="+namespace);
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

        if(log.isInfoEnabled())
        	log.info("Created tripleStore: " + namespace);

        // New KB instance was created.
        return tripleStore;

    }

	private void dropTripleStore(final IIndexManager indexManager,
			final String namespace) {

		if(log.isInfoEnabled())
			log.info("KB namespace=" + namespace);

		// Locate the resource declaration (aka "open"). This tells us if it
		// exists already.
		final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

		if (tripleStore != null) {

			if (log.isInfoEnabled())
				log.info("Destroying: " + namespace);

			tripleStore.destroy();
			
		}

	}
	
	TestMode testMode = null;
	
	@Override
	public void setUp() throws Exception {
	    
		super.setUp();

		log.warn("Setting up test:" + getName());
		
		final Properties properties = getProperties();

		// guaranteed distinct namespace for the KB instance.
		namespace = getName() + UUID.randomUUID();

		final IIndexManager m_indexManager = getIndexManager();
		
		// Create the triple store instance.
        final AbstractTripleStore tripleStore = createTripleStore(m_indexManager,
                namespace, properties);
        
        if (tripleStore.isStatementIdentifiers()) {
            testMode = TestMode.sids;
        } else if (tripleStore.isQuads()) {
            testMode = TestMode.quads;
        } else {
            testMode = TestMode.triples;
        }
		
        final Map<String, String> initParams = new LinkedHashMap<String, String>();
        {

            initParams.put(ConfigParams.NAMESPACE, namespace);

            initParams.put(ConfigParams.CREATE, "false");
            
        }
        // Start server for that kb instance.
        m_fixture = NanoSparqlServer.newInstance(0/* port */,
                m_indexManager, initParams);

        m_fixture.start();

		final int port = m_fixture.getConnectors()[0].getLocalPort();

		// log.info("Getting host address");

        final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                true/* loopbackOk */);

        if (hostAddr == null) {

            fail("Could not identify network address for this host.");

        }

        m_serviceURL = new URL("http", hostAddr, port, ""/* file */)
                .toExternalForm();

        if (log.isInfoEnabled())
            log.info("Setup done: name=" + getName() + ", namespace="
                    + namespace + ", serviceURL=" + m_serviceURL);

    }

    @Override
	public void tearDown() throws Exception {

//		if (log.isInfoEnabled())
			log.warn("tearing down test: " + getName());

		if (m_fixture != null) {

			m_fixture.stop();

			m_fixture = null;

		}

		final IIndexManager m_indexManager = getIndexManager();
		
		if (m_indexManager != null && namespace != null) {

			dropTripleStore(m_indexManager, namespace);

		}
		
//		m_indexManager = null;

		namespace = null;
		
		m_serviceURL = null;

		log.info("tear down done");
		
		super.tearDown();

	}

    /**
     * Returns a view of the triple store using the sail interface.
     */
    protected BigdataSail getSail() {

		final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
				.getResourceLocator().locate(namespace, ITx.UNISOLATED);

        return new BigdataSail(tripleStore);

    }

	public void test_startup() throws Exception {

	    assertTrue("open", m_fixture.isRunning());
	    
	}
	
	/**
	 * Options for the query.
	 */
	private static class QueryOptions {

		/** The URL of the SPARQL end point. */
		public String serviceURL = null;
		
		/** The HTTP method (GET, POST, etc). */
		public String method = "GET";

		/**
         * The SPARQL query (this is a short hand for setting the
         * <code>query</code> URL query parameter).
         */
		public String queryStr = null;
		
		/** Request parameters to be formatted as URL query parameters. */
		public Map<String,String[]> requestParams;
        
		/** The accept header. */
        public String acceptHeader = //
        BigdataRDFServlet.MIME_SPARQL_RESULTS_XML + ";q=1" + //
        "," + //
        RDFFormat.RDFXML.getDefaultMIMEType() + ";q=1"//
        ;
		
        /**
         * The Content-Type (iff there will be a request body).
         */
        public String contentType = null;
        
        /**
         * The data to send as the request body (optional).
         */
        public byte[] data = null;
        
		/** The connection timeout (ms) -or- ZERO (0) for an infinite timeout. */
		public int timeout = 0;

	}

    /**
     * Add any URL query parameters.
     */
    private void addQueryParams(final StringBuilder urlString,
            final Map<String, String[]> requestParams)
            throws UnsupportedEncodingException {
        boolean first = true;
        for (Map.Entry<String, String[]> e : requestParams.entrySet()) {
            urlString.append(first ? "?" : "&");
            first = false;
            final String name = e.getKey();
            final String[] vals = e.getValue();
            if (vals == null) {
                urlString.append(URLEncoder.encode(name, "UTF-8"));
            } else {
                for (String val : vals) {
                    urlString.append(URLEncoder.encode(name, "UTF-8"));
                    urlString.append("=");
                    urlString.append(URLEncoder.encode(val, "UTF-8"));
                }
            }
        } // next Map.Entry
//            + "?query="
//            + URLEncoder.encode(opts.queryStr, "UTF-8")
//            + (opts.defaultGraphUri == null ? ""
//                    : ("&default-graph-uri=" + URLEncoder.encode(
//                            opts.defaultGraphUri, "UTF-8")));

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
     * @param requestPath
     *            The request path, including the leading "/".
     * 
     * @return The connection.
     */
    protected HttpURLConnection doSparqlQuery(final QueryOptions opts,
            final String requestPath) throws Exception {

        /*
         * Generate the fully formed and encoded URL.
         */

        final StringBuilder urlString = new StringBuilder(opts.serviceURL);

        urlString.append(requestPath);

        if (opts.queryStr != null) {

            if (opts.requestParams == null) {

                opts.requestParams = new LinkedHashMap<String, String[]>();

            }
            
            opts.requestParams.put("query", new String[] { opts.queryStr });

        }
        
        if (opts.requestParams != null)
            addQueryParams(urlString,opts.requestParams);

        if (log.isDebugEnabled()) {
            log.debug("*** Request ***");
            log.debug(opts.serviceURL);
            log.debug(opts.queryStr);
        }

		HttpURLConnection conn = null;
		try {

//		    conn = doConnect(urlString.toString(), opts.method);
	        final URL url = new URL(urlString.toString());
	        conn = (HttpURLConnection) url.openConnection();
	        conn.setRequestMethod(opts.method);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setReadTimeout(opts.timeout);
            conn.setRequestProperty("Accept", opts.acceptHeader);
            
            if (opts.contentType != null) {
            
                if (opts.data == null)
                    throw new AssertionError();
                
                conn.setRequestProperty("Content-Type", opts.contentType);
            
                conn.setRequestProperty("Content-Length", Integer
                        .toString(opts.data.length));

                final OutputStream os = conn.getOutputStream();
                try {
                    os.write(opts.data);
                    os.flush();
                } finally {
                    os.close();
                }
                
            }
            
			// connect.
			conn.connect();

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

    protected HttpURLConnection checkResponseCode(final HttpURLConnection conn)
            throws IOException {
        final int rc = conn.getResponseCode();
        if (rc < 200 || rc >= 300) {
            conn.disconnect();
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

        checkResponseCode(conn);
        
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

        checkResponseCode(conn);
        
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

        checkResponseCode(conn);
        
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
     * Class representing the result of a mutation operation against the REST
     * API.
     * 
     * TODO Refactor into the non-test code base along with the XML generation
     * and XML parsing?
     */
    private static class MutationResult {

        /** The mutation count. */
        public final long mutationCount;

        /** The elapsed time for the operation. */
        public final long elapsedMillis;

        public MutationResult(final long mutationCount, final long elapsedMillis) {
            this.mutationCount = mutationCount;
            this.elapsedMillis = elapsedMillis;
        }

    }

    protected MutationResult getMutationResult(final HttpURLConnection conn) throws Exception {

        checkResponseCode(conn);
        
        try {

            final String contentType = conn.getContentType();

            if (!contentType.startsWith(BigdataRDFServlet.MIME_APPLICATION_XML)) {

                fail("Expecting Content-Type of "
                        + BigdataRDFServlet.MIME_APPLICATION_XML + ", not "
                        + contentType);

            }

            final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            
            final AtomicLong mutationCount = new AtomicLong();
            final AtomicLong elapsedMillis = new AtomicLong();

            /*
             * For example: <data modified="5" milliseconds="112"/>
             */
            parser.parse(conn.getInputStream(), new DefaultHandler2(){

                public void startElement(final String uri,
                        final String localName, final String qName,
                        final Attributes attributes) {

                    if (!"data".equals(qName))
                        fail("Expecting: 'data', but have: uri=" + uri
                                + ", localName=" + localName + ", qName="
                                + qName);

                    mutationCount.set(Long.valueOf(attributes
                            .getValue("modified")));

                    elapsedMillis.set(Long.valueOf(attributes
                            .getValue("milliseconds")));
                           
                }
                
            });
            
            // done.
            return new MutationResult(mutationCount.get(), elapsedMillis.get());

        } finally {

            // terminate the http connection.
            conn.disconnect();

        }

    }

    /**
     * Class representing the result of a mutation operation against the REST
     * API.
     * 
     * TODO Refactor into the non-test code base along with the XML generation
     * and XML parsing?
     */
    private static class RangeCountResult {

        /** The range count. */
        public final long rangeCount;

        /** The elapsed time for the operation. */
        public final long elapsedMillis;

        public RangeCountResult(final long rangeCount, final long elapsedMillis) {
            this.rangeCount = rangeCount;
            this.elapsedMillis = elapsedMillis;
        }

    }

    protected RangeCountResult getRangeCountResult(final HttpURLConnection conn) throws Exception {

        checkResponseCode(conn);
        
        try {

            final String contentType = conn.getContentType();

            if (!contentType.startsWith(BigdataRDFServlet.MIME_APPLICATION_XML)) {

                fail("Expecting Content-Type of "
                        + BigdataRDFServlet.MIME_APPLICATION_XML + ", not "
                        + contentType);

            }

            final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            
            final AtomicLong rangeCount = new AtomicLong();
            final AtomicLong elapsedMillis = new AtomicLong();

            /*
             * For example: <data rangeCount="5" milliseconds="112"/>
             */
            parser.parse(conn.getInputStream(), new DefaultHandler2(){

                public void startElement(final String uri,
                        final String localName, final String qName,
                        final Attributes attributes) {

                    if (!"data".equals(qName))
                        fail("Expecting: 'data', but have: uri=" + uri
                                + ", localName=" + localName + ", qName="
                                + qName);

                    rangeCount.set(Long.valueOf(attributes
                            .getValue("rangeCount")));

                    elapsedMillis.set(Long.valueOf(attributes
                            .getValue("milliseconds")));
                           
                }
                
            });
            
            // done.
            return new RangeCountResult(rangeCount.get(), elapsedMillis.get());

        } finally {

            // terminate the http connection.
            conn.disconnect();

        }

    }

    /**
	 * Issue a "status" request against the service. 
	 */
	public void test_STATUS() throws Exception {

        final HttpURLConnection conn = doConnect(m_serviceURL + "/status",
                "GET");

		// connect.
		conn.connect();

        try {

            final int rc = conn.getResponseCode();

            if (rc < 200 || rc >= 300) {

                throw new IOException(conn.getResponseMessage());

            }

            final String txt = getStreamContents(conn.getInputStream());

            if (log.isInfoEnabled())
                log.info(txt);

        } finally {

            conn.disconnect();

        }

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
    private byte[] genNTRIPLES(final int ntriples, final RDFFormat format)
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

    /**
     * Test helper verifies the expected status code and optionally logs the
     * response message and the response body.
     * 
     * @param conn
     * 
     * @throws IOException
     */
    private void assertErrorStatusCode(final int statusCode,
            final HttpURLConnection conn) throws IOException {

        try {

            final int rc = conn.getResponseCode();

            final String msg = conn.getResponseMessage();

            if (log.isInfoEnabled())
                log.info("statusCode=" + statusCode + ", msg=" + msg);
            
            if (rc >= 200 && rc < 300) {

                fail("Not expecting success: statusCode=" + rc + ", message="
                        + msg);

            }

            if (statusCode != rc) {

                fail("statusCode: expected=" + statusCode + ", but actual="
                        + rc + " (message=" + msg + ")");
                
            }

        } finally {

            conn.disconnect();

        }

    }

    /**
     * A GET query which should result in an error (the query is not well
     * formed).
     */
    public void test_GET_SELECT_ERROR() throws Exception {

        final String queryStr = "select * where {?s ?p ?o} X {}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "GET";

        opts.acceptHeader = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
        
        assertErrorStatusCode(HttpServletResponse.SC_BAD_REQUEST,
                doSparqlQuery(opts, requestPath));

    }
    
    public void test_POST_INSERT_withBody_RDFXML() throws Exception {

        doInsertWithBodyTest("POST", 23, requestPath, RDFFormat.RDFXML);
        
    }
    
    public void test_POST_INSERT_withBody_NTRIPLES() throws Exception {

        doInsertWithBodyTest("POST", 23, requestPath, RDFFormat.NTRIPLES);
        
    }
    
    public void test_POST_INSERT_withBody_N3() throws Exception {

        doInsertWithBodyTest("POST", 23, requestPath, RDFFormat.N3);
        
    }
    
    public void test_POST_INSERT_withBody_TURTLE() throws Exception {

        doInsertWithBodyTest("POST", 23, requestPath, RDFFormat.TURTLE);
        
    }
    
    // Note: quads interchange
    public void test_POST_INSERT_withBody_TRIG() throws Exception {

        doInsertWithBodyTest("POST", 23, requestPath, RDFFormat.TRIG);
        
    }
    
    // Note: quads interchange
    public void test_POST_INSERT_withBody_TRIX() throws Exception {

        doInsertWithBodyTest("POST", 23, requestPath, RDFFormat.TRIX);
        
    }

//    // FIXME We need an NQuadsWriter to run this test.
//    // Note: quads interchange
//    public void test_POST_INSERT_withBody_NQUADS() throws Exception {
//
//        doInsertWithBodyTest("POST", 23, requestPath, NQuadsParser.nquads);
//        
//    }

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
        doInsertByBody("POST", requestPath, RDFFormat.TURTLE, g, new URIImpl(
                "http://example.org"));
        
        // Verify that the data were inserted into the appropriate context.
        {
            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.method = "GET";
            opts.queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
            assertEquals(7, countResults(doSparqlQuery(opts, requestPath)));
        }

    }
    
    public void test_POST_INSERT_triples_with_URI_and_defaultContext() throws Exception {

        if(TestMode.quads != testMode)
            return;
        
        // Load the resource into the KB.
        {
            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.method = "POST";
            opts.requestParams = new LinkedHashMap<String, String[]>();
            // set the resource to load.
            opts.requestParams.put("uri", new String[] { new File(packagePath
                    + "insert_triples_with_defaultContext.ttl").toURI()
                    .toString() });
            // set the default context.
            opts.requestParams.put("context-uri",
                    new String[] { "http://example.org" });
            assertEquals(
                    7,
                    getMutationResult(doSparqlQuery(opts, requestPath)).mutationCount);
        }

        // Verify that the data were inserted into the appropriate context.
        {
            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.method = "GET";
            opts.queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
            assertEquals(7, countResults(doSparqlQuery(opts, requestPath)));
        }
        
    }

    /**
     * Test for POST of an NQuads resource by a URL.
     */
    public void test_POST_INSERT_NQuads_by_URL()
            throws Exception {

        if(TestMode.quads != testMode)
            return;

        // Verify nothing in the KB.
        {
            final String queryStr = "ASK where {?s ?p ?o}";

            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr = queryStr;
            opts.method = "GET";

            opts.acceptHeader = BooleanQueryResultFormat.SPARQL
                    .getDefaultMIMEType();
            assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
        }

        // #of statements in that RDF file.
        final long expectedStatementCount = 7;
        
        // Load the resource into the KB.
        {
            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.method = "POST";
            opts.requestParams = new LinkedHashMap<String, String[]>();
            opts.requestParams
                    .put("uri",
                            new String[] { "file:bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/quads.nq" });

            final MutationResult result = getMutationResult(doSparqlQuery(opts,
                    requestPath));

            assertEquals(expectedStatementCount, result.mutationCount);

        }

        /*
         * Verify KB has the loaded data.
         */
        {
            final String queryStr = "SELECT * where {?s ?p ?o}";

            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr = queryStr;
            opts.method = "GET";

            opts.acceptHeader = BooleanQueryResultFormat.SPARQL
                    .getDefaultMIMEType();

            assertEquals(expectedStatementCount, countResults(doSparqlQuery(
                    opts, requestPath)));
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
                doInsertByBody("POST", requestPath, RDFFormat.RDFXML, g, null/* defaultContext */).mutationCount);

        // Read back the data into a graph.
        final Graph g2;
        {
            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.method = "GET";
            opts.queryStr = "DESCRIBE <" + s.stringValue() + ">";
            g2 = buildGraph(doSparqlQuery(opts, requestPath));
        }
        
        assertEquals(1, g2.size());
        
        assertTrue(g2.match(s, p, o).hasNext());
        
    }
    
    /**
     * Generate and return a very large literal.
     */
    private Literal getVeryLargeLiteral() {

        final int len = 1024000;

        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {

            sb.append(Character.toChars('A' + (i % 26)));

        }
        
        return new LiteralImpl(sb.toString());
        
    }

    /**
     * Test ability to load data from a URI.
     */
    public void test_POST_INSERT_LOAD_FROM_URIs() throws Exception {

        // Verify nothing in the KB.
        {
            final String queryStr = "ASK where {?s ?p ?o}";

            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr = queryStr;
            opts.method = "GET";

            opts.acceptHeader = BooleanQueryResultFormat.SPARQL
                    .getDefaultMIMEType();
            assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
        }

        // #of statements in that RDF file.
        final long expectedStatementCount = 4;
        
        // Load the resource into the KB.
        {
            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.method = "POST";
            opts.requestParams = new LinkedHashMap<String, String[]>();
            opts.requestParams
                    .put("uri",
                            new String[] { "file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf" });

            final MutationResult result = getMutationResult(doSparqlQuery(opts,
                    requestPath));

            assertEquals(expectedStatementCount, result.mutationCount);

        }

        /*
         * Verify KB has the loaded data.
         */
        {
            final String queryStr = "SELECT * where {?s ?p ?o}";

            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr = queryStr;
            opts.method = "GET";

            opts.acceptHeader = BooleanQueryResultFormat.SPARQL
                    .getDefaultMIMEType();

            assertEquals(expectedStatementCount, countResults(doSparqlQuery(
                    opts, requestPath)));
        }

    }

    /**
     * Test the ESTCARD method (fast range count).
     */
    public void test_ESTCARD() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.ttl");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                null,// s
                null,// p
                null,// o
                null // c
        );

        assertEquals(7, rangeCountResult.rangeCount);
        
    }

    public void test_ESTCARD_s() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.ttl");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                new URIImpl("http://www.bigdata.com/Mike"),// s
                null,// p
                null,// o
                null // c
        );

        assertEquals(3, rangeCountResult.rangeCount);
        
    }
    
    public void test_ESTCARD_p() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.ttl");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                null,// s
                RDF.TYPE,// p
                null,// o
                null // c
        );

        assertEquals(3, rangeCountResult.rangeCount);
        
    }

    public void test_ESTCARD_p2() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.ttl");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                null,// s
                RDFS.LABEL,// p
                null,// o
                null // c
        );

        assertEquals(2, rangeCountResult.rangeCount);
        
    }

    public void test_ESTCARD_o() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.ttl");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                null,// s
                null,// p
                new LiteralImpl("Mike"),// o
                null // c
        );

        assertEquals(1, rangeCountResult.rangeCount);
        
    }

    public void test_ESTCARD_so() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.ttl");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                new URIImpl("http://www.bigdata.com/Mike"),// s,
                RDF.TYPE,// p
                null,// o
                null // c
        );

        assertEquals(1, rangeCountResult.rangeCount);
        
    }

    /**
     * Test the ESTCARD method (fast range count).
     */
    public void test_ESTCARD_quads_01() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.trig");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                null,// s,
                null,// p
                null,// o
                null // c
        );

        assertEquals(7, rangeCountResult.rangeCount);

    }
    
    public void test_ESTCARD_quads_02() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.trig");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                null,// s,
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/")// c
        );

        assertEquals(3, rangeCountResult.rangeCount);

    }
    
    public void test_ESTCARD_quads_03() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.trig");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                null,// s,
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/c1")// c
        );

        assertEquals(2, rangeCountResult.rangeCount);

    }

    public void test_ESTCARD_quads_04() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", requestPath, packagePath
                + "test_estcard.trig");
        
        final RangeCountResult rangeCountResult = doRangeCount(//
                requestPath,//
                new URIImpl("http://www.bigdata.com/Mike"),// s,
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/c1")// c
        );

        assertEquals(1, rangeCountResult.rangeCount);

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

        doInsertWithBodyTest("POST", 23, requestPath, RDFFormat.NTRIPLES);

        assertEquals(23, countResults(doSparqlQuery(opts, requestPath)));

        doDeleteWithQuery(requestPath, "construct {?s ?p ?o} where {?s ?p ?o}");

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));

    }

    /**
     * Delete everything matching an access path description.
     */
    public void test_DELETE_accessPath_delete_all() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.ttl");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                null,// p
                null,// o
                null // c
        );

        assertEquals(7, mutationResult.mutationCount);
        
    }
    
    /**
     * Delete everything with a specific subject.
     */
    public void test_DELETE_accessPath_delete_s() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.ttl");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                new URIImpl("http://www.bigdata.com/Mike"),// s
                null,// p
                null,// o
                null // c
        );

        assertEquals(3, mutationResult.mutationCount);
        
    }

    /**
     * Delete everything with a specific predicate.
     */
    public void test_DELETE_accessPath_delete_p() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.ttl");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                new URIImpl("http://www.w3.org/2000/01/rdf-schema#label"),// p
                null,// o
                null // c
        );

        assertEquals(2, mutationResult.mutationCount);
        
    }
    
    /**
     * Delete everything with a specific object (a URI).
     */
    public void test_DELETE_accessPath_delete_o_URI() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.ttl");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                null,// p
                new URIImpl("http://xmlns.com/foaf/0.1/Person"),// o
                null // c
        );

        assertEquals(3, mutationResult.mutationCount);
        
    }
    
    /**
     * Delete everything with a specific object (a Literal).
     */
    public void test_DELETE_accessPath_delete_o_Literal() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.ttl");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                null,// p
                new URIImpl("http://www.bigdata.com/Bryan"),// o
                null // c
        );

        assertEquals(1, mutationResult.mutationCount);
        
    }
    
    /**
     * Delete everything with a specific predicate and object (a URI).
     */
    public void test_DELETE_accessPath_delete_p_o_URI() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.ttl");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                RDF.TYPE,// p
                new URIImpl("http://xmlns.com/foaf/0.1/Person"),// o
                null // c
        );

        assertEquals(3, mutationResult.mutationCount);
        
    }
    
    /**
     * Delete everything with a specific predicate and object (a Literal).
     */
    public void test_DELETE_accessPath_delete_p_o_Literal() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.ttl");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                RDFS.LABEL,// p
                new LiteralImpl("Bryan"),// o
                null // c
        );

        assertEquals(1, mutationResult.mutationCount);
        
    }
    
    /**
     * Delete using an access path which does not match anything.
     */
    public void test_DELETE_accessPath_delete_NothingMatched() throws Exception {

        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.ttl");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                null,// p
                new URIImpl("http://xmlns.com/foaf/0.1/XXX"),// o
                null // c
        );

        assertEquals(0, mutationResult.mutationCount);
        
    }

    /**
     * Delete everything in a named graph (context).
     */
    public void test_DELETE_accessPath_delete_c() throws Exception {

        if(TestMode.quads != testMode)
            return;
        
        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.trig");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/") // c
        );

        assertEquals(3, mutationResult.mutationCount);
        
    }

    /**
     * Delete everything in a different named graph (context).
     */
    public void test_DELETE_accessPath_delete_c1() throws Exception {

        if(TestMode.quads != testMode)
            return;
        
        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.trig");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/c1") // c
        );

        assertEquals(2, mutationResult.mutationCount);
        
    }

    /**
     * Delete using an access path with the context position bound. 
     */
    public void test_DELETE_accessPath_delete_c_nothingMatched() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", requestPath, packagePath
                + "test_delete_by_access_path.trig");

        final MutationResult mutationResult = doDeleteWithAccessPath(//
                requestPath,//
                null,// s
                null,// p
                null,// o
                new URIImpl("http://xmlns.com/foaf/0.1/XXX") // c
        );

        assertEquals(0, mutationResult.mutationCount);
        
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

        doInsertWithBodyTest("POST", 23, requestPath, format);

        assertEquals(23, countResults(doSparqlQuery(opts, requestPath)));

        doDeleteWithBody(requestPath, 23, format);

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
        
    }

	private void doDeleteWithQuery(final String servlet, final String query) {
		HttpURLConnection conn = null;
		try {

			final URL url = new URL(m_serviceURL + servlet + "?query="
					+ URLEncoder.encode(query, "UTF-8"));
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("DELETE");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(0);

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

    /**
     * Submit a fast range count request.
     * 
     * @param servlet
     * @param s
     * @param p
     * @param o
     * @param c
     * @return The parsed fast range count response.
     */
    private RangeCountResult doRangeCount(//
            final String servlet,//
            final Resource s,//
            final URI p,//
            final Value o,//
            final Resource c//
            ) {
        HttpURLConnection conn = null;
        try {

            final LinkedHashMap<String, String[]> requestParams = new LinkedHashMap<String, String[]>();

            requestParams.put("ESTCARD", null);
            
            if (s != null)
                requestParams.put("s",
                        new String[] { EncodeDecodeValue.encodeValue(s) });
            
            if (p != null)
                requestParams.put("p",
                        new String[] { EncodeDecodeValue.encodeValue(p) });
            
            if (o != null)
                requestParams.put("o",
                        new String[] { EncodeDecodeValue.encodeValue(o) });
            
            if (c != null)
                requestParams.put("c",
                        new String[] { EncodeDecodeValue.encodeValue(c) });

            final StringBuilder urlString = new StringBuilder();
            urlString.append(m_serviceURL).append(servlet);
            addQueryParams(urlString, requestParams);

            final URL url = new URL(urlString.toString());
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setDoOutput(false);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setReadTimeout(0);

            conn.connect();

//            if (log.isInfoEnabled())
//                log.info(conn.getResponseMessage());

            final int rc = conn.getResponseCode();
            
            if (rc < 200 || rc >= 300) {
                throw new IOException(conn.getResponseMessage());
            }

            return getRangeCountResult(conn);
            
        } catch (Throwable t) {
            // clean up the connection resources
            if (conn != null)
                conn.disconnect();
            throw new RuntimeException(t);
        }
    }

    private MutationResult doDeleteWithAccessPath(//
            final String servlet,//
            final Resource s,//
            final URI p,//
            final Value o,//
            final Resource c//
            ) {
        HttpURLConnection conn = null;
        try {

            final LinkedHashMap<String, String[]> requestParams = new LinkedHashMap<String, String[]>();

            if (s != null)
                requestParams.put("s",
                        new String[] { EncodeDecodeValue.encodeValue(s) });
            
            if (p != null)
                requestParams.put("p",
                        new String[] { EncodeDecodeValue.encodeValue(p) });
            
            if (o != null)
                requestParams.put("o",
                        new String[] { EncodeDecodeValue.encodeValue(o) });
            
            if (c != null)
                requestParams.put("c",
                        new String[] { EncodeDecodeValue.encodeValue(c) });

            final StringBuilder urlString = new StringBuilder();
            urlString.append(m_serviceURL).append(servlet);
            addQueryParams(urlString, requestParams);

            final URL url = new URL(urlString.toString());
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            conn.setDoOutput(false);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setReadTimeout(0);

            conn.connect();

//            if (log.isInfoEnabled())
//                log.info(conn.getResponseMessage());

            final int rc = conn.getResponseCode();
            
            if (rc < 200 || rc >= 300) {
                throw new IOException(conn.getResponseMessage());
            }

            return getMutationResult(conn);
            
        } catch (Throwable t) {
            // clean up the connection resources
            if (conn != null)
                conn.disconnect();
            throw new RuntimeException(t);
        }
    }

    private void doDeleteWithBody(final String servlet, final int ntriples,
            final RDFFormat format) throws Exception {

        HttpURLConnection conn = null;
		try {

            final URL url = new URL(m_serviceURL + servlet + "?delete");
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(0);

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

        // Verify the mutation count.
        assertEquals(ntriples, getMutationResult(conn).mutationCount);

    }

	/**
	 * Test of POST w/ BODY having data to be loaded.
	 */
    private void doInsertWithBodyTest(final String method, final int ntriples,
            final String servlet, final RDFFormat format) throws Exception {

		HttpURLConnection conn = null;
		try {

			final URL url = new URL(m_serviceURL + servlet);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(method);
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(0);
			
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
//                log.info("*** RESPONSE: " + getResponseBody(conn));
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

        // Verify the mutation count.
        assertEquals(ntriples, getMutationResult(conn).mutationCount);
		
		// Verify the expected #of statements in the store.
		{
			final String queryStr = "select * where {?s ?p ?o}";

			final QueryOptions opts = new QueryOptions();
			opts.serviceURL = m_serviceURL;
			opts.queryStr = queryStr;
			opts.method = "GET";

			assertEquals(ntriples, countResults(doSparqlQuery(opts, requestPath)));
		}

    }

    /**
     * Insert a resource into the {@link NanoSparqlServer}.  This is used to
     * load resources in the test package into the server.
     */
    private MutationResult doInsertbyURL(final String method, final String servlet,
            final String resource) throws Exception {

        final String uri = new File(resource).toURI().toString();

        HttpURLConnection conn = null;
        try {

            // Load the resource into the KB.
            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.method = "POST";
            opts.requestParams = new LinkedHashMap<String, String[]>();
            opts.requestParams.put("uri", new String[] { uri });

            return getMutationResult(doSparqlQuery(opts, requestPath));

        } catch (Throwable t) {
            // clean up the connection resources
            if (conn != null)
                conn.disconnect();
            throw new RuntimeException(t);
        }

    }

    /**
     * Read the contents of a file.
     * 
     * @param file
     *            The file.
     * @return It's contents.
     */
    private static String readFromFile(final File file) throws IOException {

        final LineNumberReader r = new LineNumberReader(new FileReader(file));

        try {

            final StringBuilder sb = new StringBuilder();

            String s;
            while ((s = r.readLine()) != null) {

                if (r.getLineNumber() > 1)
                    sb.append("\n");

                sb.append(s);

            }

            return sb.toString();

        } finally {

            r.close();

        }

    }
    
    private static Graph readGraphFromFile(final File file) throws RDFParseException, RDFHandlerException, IOException {
        
        final RDFFormat format = RDFFormat.forFileName(file.getName());
        
        final RDFParserFactory rdfParserFactory = RDFParserRegistry
                .getInstance().get(format);

        if (rdfParserFactory == null) {
            throw new RuntimeException("Parser not found: file=" + file
                    + ", format=" + format);
        }

        final RDFParser rdfParser = rdfParserFactory
                .getParser();

        rdfParser.setValueFactory(new ValueFactoryImpl());

        rdfParser.setVerifyData(true);

        rdfParser.setStopAtFirstError(true);

        rdfParser
                .setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

        final StatementCollector rdfHandler = new StatementCollector();
        
        rdfParser.setRDFHandler(rdfHandler);

        /*
         * Run the parser, which will cause statements to be
         * inserted.
         */

        final FileReader r = new FileReader(file);
        try {
            rdfParser.parse(r, file.toURI().toString()/* baseURL */);
        } finally {
            r.close();
        }
        
        final Graph g = new GraphImpl();
        
        g.addAll(rdfHandler.getStatements());

        return g;

    }
    
    /**
     * Write a graph on a buffer suitable for sending as an HTTP request body.
     * 
     * @param format
     *            The RDF Format to use.
     * @param g
     *            The graph.
     *            
     * @return The serialized data.
     * 
     * @throws RDFHandlerException
     */
    static private byte[] writeOnBuffer(final RDFFormat format, final Graph g)
            throws RDFHandlerException {

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
     * Load and return a graph from a resource.
     * 
     * @param resource
     *            The resource.
     * 
     * @return The graph.
     */
    private Graph loadGraphFromResource(final String resource)
            throws RDFParseException, RDFHandlerException, IOException {

//        final RDFFormat rdfFormat = RDFFormat.forFileName(resource);

        final Graph g = readGraphFromFile(new File(resource));

        return g;

    }
    
    /**
     * Reads a resource and sends it using an INSERT with BODY request to be
     * loaded into the database.
     * 
     * @param method
     * @param servlet
     * @param resource
     * @return
     * @throws Exception
     */
    private MutationResult doInsertByBody(final String method,
            final String servlet, final RDFFormat rdfFormat, final Graph g,
            final URI defaultContext) throws Exception {

        final byte[] wireData = writeOnBuffer(rdfFormat, g);

        HttpURLConnection conn = null;
        try {

            final URL url = new URL(m_serviceURL
                    + servlet
                    + (defaultContext == null ? ""
                            : ("?context-uri=" + URLEncoder.encode(
                                    defaultContext.stringValue(), "UTF-8"))));
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(method);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setReadTimeout(0);

            conn.setRequestProperty("Content-Type",
                    rdfFormat.getDefaultMIMEType());

            final byte[] data = wireData;

            conn.setRequestProperty("Content-Length",
                    Integer.toString(data.length));

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
                // log.info("*** RESPONSE: " + getResponseBody(conn));
            }

            if (rc < 200 || rc >= 300) {

                throw new IOException(conn.getResponseMessage());

            }

            return getMutationResult(conn);

        } catch (Throwable t) {
            // clean up the connection resources
            if (conn != null)
                conn.disconnect();
            throw new RuntimeException(t);
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

    /**
     * Inserts some data into the KB and then issues a DESCRIBE query against
     * the REST API and verifies the expected results.
     * 
     * @param format
     *            The format is used to specify the Accept header.
     * 
     * @throws Exception
     */
    private void doDescribeTest(final String method, final RDFFormat format)
            throws Exception {
        
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
            opts.method = method;
            opts.acceptHeader = format.getDefaultMIMEType();
            opts.queryStr =//
                "prefix bd: <"+BD.NAMESPACE+"> " +//
                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
                "DESCRIBE ?x " +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
                "  ?x bd:likes bd:RDF " +//
                "}";

            final Graph actual = buildGraph(doSparqlQuery(opts, requestPath));

            assertSameGraph(expected, actual);
            
        }

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
     * Sets up a simple data set on the server.
     * 
     * @throws SailException
     * @throws RepositoryException
     */
    private void setupDataOnServer() throws SailException, RepositoryException {
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
        final Literal label1 = new LiteralImpl("Mike");
        final Literal label2 = new LiteralImpl("Bryan");

        final BigdataSail sail = getSail();
        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            
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
    }
    
    private void doConstructTest(final String method, final RDFFormat format)
            throws Exception {
        
        setupDataOnServer();
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
//        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
//        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
//        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
//        final Literal label1 = new LiteralImpl("Mike");
//        final Literal label2 = new LiteralImpl("Bryan");
//
//        final BigdataSail sail = getSail();
//        sail.initialize();
//        final BigdataSailRepository repo = new BigdataSailRepository(sail);
//        
//        try {
//
//            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
//                    .getConnection();
//            try {
//
//                cxn.setAutoCommit(false);
//
//                cxn.add(mike, RDF.TYPE, person);
//                cxn.add(mike, likes, rdf);
//                cxn.add(mike, RDFS.LABEL, label1);
//                cxn.add(bryan, RDF.TYPE, person);
//                cxn.add(bryan, likes, rdfs);
//                cxn.add(bryan, RDFS.LABEL, label2);
//
//                /*
//                 * Note: The either flush() or commit() is required to flush the
//                 * statement buffers to the database before executing any
//                 * operations that go around the sail.
//                 */
//                cxn.commit();
//            } finally {
//                cxn.close();
//            }
//             
//        } finally {
//            sail.shutDown();
//        }

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
            opts.method = method;
            opts.acceptHeader = format.getDefaultMIMEType();
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

    /**
     * Unit test for ACID UPDATE using PUT. This test is for the operation where
     * a SPARQL selects the data to be deleted and the request body contains the
     * statements to be inserted.
     */
    public void test_PUT_UPDATE_WITH_QUERY() throws Exception {

        setupDataOnServer();

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

            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr = deleteQueryStr;
            opts.method = "GET";
            opts.acceptHeader = TupleQueryResultFormat.SPARQL
                    .getDefaultMIMEType();
            
            assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
                    requestPath)));
            
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

            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr = deleteQueryStr;
            opts.method = "PUT";
            //opts.acceptHeader = ...;
            opts.contentType = RDFFormat.NTRIPLES.getDefaultMIMEType();
            opts.data = data;
            final MutationResult ret = getMutationResult(doSparqlQuery(opts,
                    requestPath));
            assertEquals(2, ret.mutationCount);// FIXME 1 removed, but also 1 added.
            
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

                final QueryOptions opts = new QueryOptions();
                opts.serviceURL = m_serviceURL;
                opts.queryStr = deleteQueryStr;
                opts.method = "GET";
                opts.acceptHeader = TupleQueryResultFormat.SPARQL
                        .getDefaultMIMEType();

                assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
                        requestPath)));
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

                final QueryOptions opts = new QueryOptions();
                opts.serviceURL = m_serviceURL;
                opts.queryStr = queryStr2;
                opts.method = "GET";
                opts.acceptHeader = TupleQueryResultFormat.SPARQL
                        .getDefaultMIMEType();

                assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
                        requestPath)));

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

            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr = deleteQueryStr;
            opts.method = "GET";
            opts.acceptHeader = TupleQueryResultFormat.SPARQL
                    .getDefaultMIMEType();
            
            assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
                    requestPath)));
            
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

            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = m_serviceURL;
            opts.queryStr = deleteQueryStr;
            opts.method = "PUT";
            //opts.acceptHeader = ...;
            opts.contentType = RDFFormat.NTRIPLES.getDefaultMIMEType();
            opts.data = data;
            final MutationResult ret = getMutationResult(doSparqlQuery(opts,
                    requestPath));
            assertEquals(2, ret.mutationCount);// FIXME 1 removed, but also 1 added.
            
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

                final QueryOptions opts = new QueryOptions();
                opts.serviceURL = m_serviceURL;
                opts.queryStr = queryStr2;
                opts.method = "GET";
                opts.acceptHeader = TupleQueryResultFormat.SPARQL
                        .getDefaultMIMEType();

                assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
                        requestPath)));
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

                final QueryOptions opts = new QueryOptions();
                opts.serviceURL = m_serviceURL;
                opts.queryStr = queryStr2;
                opts.method = "GET";
                opts.acceptHeader = TupleQueryResultFormat.SPARQL
                        .getDefaultMIMEType();

                assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
                        requestPath)));

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
