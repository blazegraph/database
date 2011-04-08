package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase2;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLParserFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.rdfxml.RDFXMLParser;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.BigdataContext.Config;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.util.config.NicUtil;

public class TestJettySparqlServer_StartStop extends TestCase2 {

	private Journal m_jnl;
	private JettySparqlServer m_fixture;
	private String m_serviceURL;
	
	final static String REST = "";

	protected void setUp() throws Exception {

		final Properties properties = getProperties();

		final String namespace = getName();

		// log.info("Creating journal");

		m_jnl = new Journal(properties);

		// log.info("Creating kb");

		// Create the kb instance.
		new LocalTripleStore(m_jnl, namespace, ITx.UNISOLATED, properties).create();

		final Config config = new Config();

		config.namespace = namespace;
		config.port = 0; // any open port.
		/*
		 * Service will not hold a read lock.
		 * 
		 * Queries will read from the last commit point by default and will use
		 * a read-only tx to have snapshot isolation for that query.
		 */
		config.timestamp = ITx.READ_COMMITTED;

		// log.info("Starting server");

		// Start server for that kb instance.
		m_fixture = new JettySparqlServer(config.port);
		m_fixture.startup(config, m_jnl);

		// log.info("Server running");

		assertTrue("open", m_fixture.isOpen());

		final int port = m_fixture.getPort();

		// log.info("Getting host address");

		final String hostAddr = NicUtil.getIpAddress("default.nic", "default", true/* loopbackOk */);

		if (hostAddr == null) {

			fail("Could not identify network address for this host.");

		}

		m_serviceURL = new URL("http", hostAddr, port, ""/* file */).toExternalForm();

		// log.info("Setup done: "+serviceURL);

	}

	/**
	 * Returns a view of the triple store using the sail interface.
	 */
	protected BigdataSail getSail() {

		final String namespace = getName();

		final AbstractTripleStore tripleStore = (AbstractTripleStore) m_jnl.getResourceLocator().locate(namespace,
				ITx.UNISOLATED);

		return new BigdataSail(tripleStore);

	}

	@Override
	protected void tearDown() throws Exception {

		if (m_fixture != null) {

			m_fixture.shutdownNow();

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
		assertTrue("open", m_fixture.isOpen());
	}
	
	public void testXMLBuilder() throws IOException {
		XMLBuilder xml = new XMLBuilder();
		
		XMLBuilder.Node close = xml.root("data")
			.attr("id", "TheRoot")
			.attr("name", "Test")
			.node("child", "My Child")
			.node("child")
				.attr("name", "My Child")
				.close()
			.node("child")
				.attr("name", "My Child")
				.text("Content")
				.close()
			.close();
		
		assertTrue(close == null);
		
		System.out.println(xml.toString());
	}

	/**
	 * Options for the query.
	 */
	private static class QueryOptions {

		/** The default timeout (ms). */
		private static final int DEFAULT_TIMEOUT = 2000;

		/** The URL of the SPARQL endpoint. */
		public String serviceURL = null;
		// public String username = null;
		// public String password = null;
		/** The HTTP method (GET, POST, etc). */
		public String method = "GET";
		/** The SPARQL query. */
		public String queryStr = null;
		/** The default graph URI (optional). */
		public String defaultGraphUri = null;
		
		/** The connection timeout (ms) -or- ZERO (0) for an infinate timeout. */
		// public int timeout = DEFAULT_TIMEOUT;
		public int timeout = 0;
		// public boolean showQuery = false;

	}

	protected HttpURLConnection doConnect(final String urlString, final String method) throws Exception {
		final URL url = new URL(urlString);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		
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
	protected HttpURLConnection doSparqlQuery(final QueryOptions opts, final String servlet) throws Exception {

		// Fully formed and encoded URL @todo use */* for ASK.
		final String urlString = opts.serviceURL
				+ "/" + servlet + "?query="
				+ URLEncoder.encode(opts.queryStr, "UTF-8")
				+ (opts.defaultGraphUri == null ? "" : ("&default-graph-uri=" + URLEncoder.encode(opts.defaultGraphUri,
						"UTF-8")));

		HttpURLConnection conn = null;
		try {
			conn = doConnect(urlString, opts.method);
			
			conn.setReadTimeout(opts.timeout);

			/*
			 * Set an appropriate Accept header for the query.
			 * 
			 * @todo ASK queries have boolean data, JSON format is also
			 * available.
			 */
			conn.setRequestProperty("Accept",//
					BigdataServlet.MIME_SPARQL_RESULTS_XML + ";q=1" + //
							"," + //
							BigdataServlet.MIME_RDF_XML + ";q=1"//
			);

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

		final Graph g = new GraphImpl();

		try {

			final String baseURI = "";

			final RDFXMLParser rdfParser = new RDFXMLParser(new ValueFactoryImpl());

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

		final AtomicLong nsolutions = new AtomicLong();

		try {

			final TupleQueryResultParser parser = new SPARQLResultsXMLParserFactory().getParser();

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
	 * Select everything in the kb using a GET.
	 */
	public void test_STATUS() throws Exception {

		HttpURLConnection conn = doConnect(m_serviceURL + "/status", "GET");

		// No solutions (assuming a told triple kb or quads kb w/o axioms).
		
		// connect.
		conn.connect();

		final int rc = conn.getResponseCode();
		if (rc < 200 || rc >= 300) {
			throw new IOException(conn.getResponseMessage());
		}
		
		Reader rdr = new InputStreamReader(conn.getInputStream());
		
		
		String txt = getStreamContents(conn.getInputStream());
		
		System.out.println(txt);

	}

	private String getStreamContents(InputStream inputStream) throws IOException {
		Reader rdr = new InputStreamReader(inputStream);
		
		StringBuffer sb = new StringBuffer();
		char[] buf = new char[512];
		while (true) {
			int rdlen = rdr.read(buf);
			if (rdlen == -1)
				break;
			sb.append(buf, 0, rdlen);
		}
		
		return sb.toString();
	}

	/**
	 * Select everything in the kb using a GET.
	 */
	public void test_GET_SELECT_ALL() throws Exception {

		final String queryStr = "select * where {?s ?p ?o}";

		final QueryOptions opts = new QueryOptions();
		opts.serviceURL = m_serviceURL;
		opts.queryStr = queryStr;
		opts.method = "GET";

		// No solutions (assuming a told triple kb or quads kb w/o axioms).
		assertEquals(0, countResults(doSparqlQuery(opts, REST)));

	}

    /**
     * Select everything in the kb using a POST.
     */
    public void test_POST_SELECT_ALL() throws Exception {

        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts, REST)));

    }

	public void test_POSTUPDATE_withBody_NTRIPLES() throws Exception {
		do_UPDATE_withBody_NTRIPLES("POST", 23, REST);
	}
    
	public void test_PUTUPDATE_withBody_NTRIPLES() throws Exception {
		do_UPDATE_withBody_NTRIPLES("PUT", 23, REST);
	}
	
    /**
     * Select everything in the kb using a POST.
     */
    public void test_DELETE_withqueryandnamespace() throws Exception {
    	
        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

    	do_UPDATE_withBody_NTRIPLES("POST", 23, REST);
    	
        assertEquals(23, countResults(doSparqlQuery(opts, REST)));
    	
    	do_DELETE_with_Query(REST, "construct {?s ?p ?o} where {?s ?p ?o}");

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts, REST)));

    }


    /**
     * Select everything in the kb using a DELETE.
     */
    public void test_REST_DELETE_withquery() throws Exception {
    	
        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

    	do_UPDATE_withBody_NTRIPLES("POST", 23, REST);
    	
        assertEquals(23, countResults(doSparqlQuery(opts, REST)));

        do_DELETE_with_Query(REST, "construct {?s ?p ?o} where {?s ?p ?o}");

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts, REST)));
    }

    /**
     * Select everything in the kb using a POST.
     */
    public void test_DELETE_withPOST() throws Exception {
    	
        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = m_serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

    	do_UPDATE_withBody_NTRIPLES("POST", 23, REST);
    	
        assertEquals(23, countResults(doSparqlQuery(opts, REST)));

        do_DELETE_withBody_NTRIPLES("delete", 23);

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts, REST)));

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

	private void do_DELETE_withBody_NTRIPLES(final String servlet, final int ntriples) {
		HttpURLConnection conn = null;
		try {

			final URL url = new URL(m_serviceURL + "/" + servlet);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(0);// TODO timeout (ms)

			String defmimetype = RDFFormat.NTRIPLES.getDefaultMIMEType();
			conn.setRequestProperty("Content-Type", defmimetype);

			final String data = genNTRIPLES(ntriples);
			
			conn.setRequestProperty("Content-Length", "" + Integer.toString(data.length()));

			final OutputStream os = conn.getOutputStream();
			try {
				final Writer w = new OutputStreamWriter(os);
				w.write(data);
				w.flush();
				w.close();
				os.flush();
				os.close();
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

	/**
	 * UPDATE should not be allowed with a GET request
	 */
	public void test_GETUPDATE_withBody_NTRIPLES() throws Exception {
//		if (JettySparqlServer.directServletAccess) 
		if(false) {
			HttpURLConnection conn = null;
			final URL url = new URL(m_serviceURL + "/update?data=stuff");
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(0);// TODO timeout (ms)
			
			conn.connect();
			
			final int rc = conn.getResponseCode();
			
			assertTrue(rc == 405); // NOT_ALLOWED
		}
	}
    
	String genNTRIPLES(final int ntriples) {
		StringBuffer databuf = new StringBuffer();
		databuf.append("#@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n");
		databuf.append("#@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n");
		databuf.append("#@prefix owl:  <http://www.w3.org/2002/07/owl#> .\n");
		databuf.append("#@prefix : <#> .\n");
		for (int i = 0; i < ntriples; i++) {
			databuf.append("<http://www.bigdata.org/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.bigdata.org/c#" + i + ">.\n");
		}
		
		return databuf.toString();
	}
	/**
	 * @todo Test of POST w/ BODY having data to be loaded.
	 */
	public void do_UPDATE_withBody_NTRIPLES(final String method, final int ntriples, final String servlet) throws Exception {

		HttpURLConnection conn = null;
		try {

			final URL url = new URL(m_serviceURL + "/" + servlet);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(method);
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(0);// TODO timeout (ms)
			String defmimetype = RDFFormat.NTRIPLES.getDefaultMIMEType();
			conn.setRequestProperty("Content-Type", defmimetype);

			final String data = genNTRIPLES(ntriples);
			
			conn.setRequestProperty("Content-Length", "" + Integer.toString(data.length()));

			final OutputStream os = conn.getOutputStream();
			try {
				final Writer w = new OutputStreamWriter(os);
				w.write(data);
				w.flush();
				w.close();
				os.flush();
				os.close();
			} finally {
				os.close();
			}
			// conn.connect();

			if (log.isInfoEnabled())
				log.info(conn.getResponseMessage());

			final int rc = conn.getResponseCode();
			
			System.out.println("Response: " + rc + " for " + method);
			
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

			assertEquals(ntriples, countResults(doSparqlQuery(opts, REST)));
		}

	}

	   public void test_GET_DESCRIBE() throws Exception {
		   do_construct_describe("describe");
	   }
	   
	   public void test_GET_CONSTRUCT() throws Exception {
		   fail("Fix construct test");
		   
		   do_construct_describe("construct");
	   }
    /**
     * Test GET with DESCRIBE query.
     */
    public void do_construct_describe(final String type) throws Exception {

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
                type + " ?x " +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
                "  ?x bd:likes bd:RDF " +//
                "}";

            final Graph actual = buildGraph(doSparqlQuery(opts, REST));

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
