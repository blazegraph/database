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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Server;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryResult;
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

import com.bigdata.BigdataStatics;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sail.DestroyKBTask;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.util.InnerCause;
import com.bigdata.util.config.NicUtil;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public abstract class AbstractTestNanoSparqlClient<S extends IIndexManager> extends ProxyTestCase<S> {

    /**
     * The path used to resolve resources in this package when they are being
     * uploaded to the {@link NanoSparqlServer}.
     */
    protected static final String packagePath = "src/test/java/com/bigdata/rdf/sail/webapp/";

	/**
	 * A jetty {@link Server} running a {@link NanoSparqlServer} instance.
	 */
	protected Server m_fixture;

	/**
	 * The namespace of the {@link AbstractTripleStore} instance against which
	 * the test is running. A unique namespace is used for each test run, but
	 * the namespace is based on the test name.
	 */
	protected String namespace;
	
    /**
     * The {@link ClientConnectionManager} for the {@link HttpClient} used by
     * the {@link RemoteRepository}. This is used when we tear down the
     * {@link RemoteRepository}.
     */
	// private ClientConnectionManager m_cm;
	
    /**
     * The http client.
     */
    protected HttpClient m_client;

    /**
     * The client-API wrapper for the remote service.
     */
    protected RemoteRepositoryManager m_mgr;
    
    /**
     * The client-API wrapper to the NSS for the configured default namespace.
     */
    protected RemoteRepository m_repo;

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
     * http://localhost:9999 -- root URL
     * http://localhost:9999/bigdata -- webapp URL (includes "/bigdata" context path.
     * </pre>
     */
	protected String m_rootURL;
	
//	/**
//	 * The request path for the REST API under test.
//	 */
//	final protected static String requestPath = "/sparql";

	public AbstractTestNanoSparqlClient() {
		
	}

	public AbstractTestNanoSparqlClient(final String name) {

		super(name);

	}

   private void createTripleStore(
         final IIndexManager indexManager, final String namespace,
         final Properties properties) throws InterruptedException,
         ExecutionException, SailException {

		if(log.isInfoEnabled())
			log.info("KB namespace=" + namespace);

       boolean ok = false;
       final BigdataSail sail = new BigdataSail(namespace,indexManager);
       try {
           sail.initialize();
           sail.create(properties);
        if(log.isInfoEnabled())
        	log.info("Created tripleStore: " + namespace);
           ok = true;
           return;
       } finally {
           if (!ok)
               sail.shutDown();
       }
       
//      AbstractApiTask.submitApiTask(indexManager, new CreateKBTask(namespace,
//            properties)).get();
//		
//        /**
//         * Return a view of the new KB to the caller.
//         * 
//         * Note: The caller MUST NOT attempt to modify this KB view outside of
//         * the group commit mechanisms. Therefore I am now returning a read-only
//         * view.
//         */
//      final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
//            .getResourceLocator().locate(namespace, ITx.READ_COMMITTED);
//
//      assert tripleStore != null;
//
//      return tripleStore;
      
    }

	private void dropTripleStore(final IIndexManager indexManager,
			final String namespace) throws InterruptedException, ExecutionException {

		if(log.isInfoEnabled())
			log.info("KB namespace=" + namespace);

      try {
         AbstractApiTask.submitApiTask(indexManager,
               new DestroyKBTask(namespace)).get();
      } catch (Exception ex) {
         if (InnerCause.isInnerCause(ex, DatasetNotFoundException.class)) {
            if (log.isInfoEnabled())
               log.info("namespace does not exist: " + namespace);
         }
		}

	}
	
	/**
	 * The {@link TestMode} that is in effect.
	 */
	private TestMode testMode = null;
	
	/**
	 * The {@link TestMode} that is in effect.
	 */
	protected TestMode getTestMode() {
		return testMode;
	}

	protected Server newFixture(final String lnamespace) throws Exception {

	   final IIndexManager indexManager = getIndexManager();
		
		final Properties properties = getProperties();

		// Create the triple store instance.
        createTripleStore(indexManager, lnamespace, properties);
        
        // Open an unisolated connection on that namespace and figure out what
        // mode the namespace is using.
        {
            final BigdataSail sail = new BigdataSail(lnamespace, indexManager);
            try {
                sail.initialize();
                final BigdataSailConnection con = sail.getUnisolatedConnection();
                try {
                    final AbstractTripleStore tripleStore = con.getTripleStore();
        if (tripleStore.isStatementIdentifiers()) {
			testMode = TestMode.sids;
        } else if (tripleStore.isQuads()) {
            testMode = TestMode.quads;
        } else {
            testMode = TestMode.triples;
        }
                } finally {
                    con.close();
                }
            } finally {
                sail.shutDown();
            }
        }

        final Map<String, String> initParams = new LinkedHashMap<String, String>();
        {

            initParams.put(ConfigParams.NAMESPACE, lnamespace);

            initParams.put(ConfigParams.CREATE, "false");
            
        }
        // Start server for that kb instance.
        final Server fixture = NanoSparqlServer.newInstance(0/* port */,
                indexManager, initParams);

        fixture.start();
		
        return fixture;
	}
	
	@Override
	public void setUp() throws Exception {
	    
		super.setUp();

		if (log.isTraceEnabled())
			log.trace("Setting up test:" + getName());
		
//		final Properties properties = getProperties();

		// guaranteed distinct namespace for the KB instance.
		namespace = getName() + UUID.randomUUID();
		
		m_fixture = newFixture(namespace);

//		final IIndexManager m_indexManager = getIndexManager();
//		
//		// Create the triple store instance.
//        final AbstractTripleStore tripleStore = createTripleStore(m_indexManager,
//                namespace, properties);
//        
//        if (tripleStore.isStatementIdentifiers()) {
//            testMode = TestMode.sids;
//        } else if (tripleStore.isQuads()) {
//            testMode = TestMode.quads;
//        } else {
//            testMode = TestMode.triples;
//        }
//		
//        final Map<String, String> initParams = new LinkedHashMap<String, String>();
//        {
//
//            initParams.put(ConfigParams.NAMESPACE, namespace);
//
//            initParams.put(ConfigParams.CREATE, "false");
//            
//        }
//        // Start server for that kb instance.
//        m_fixture = NanoSparqlServer.newInstance(0/* port */,
//                m_indexManager, initParams);
//
//        m_fixture.start();

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
            log.info("Setup done: \nname=" + getName() + "\nnamespace="
                    + namespace + "\nrootURL=" + m_rootURL + "\nserviceURL="
                    + m_serviceURL);

        /*
         * Ensure that the client follows redirects using a standard policy.
         * 
         * Note: This is necessary for tests of the webapp structure since the
         * container may respond with a redirect (302) to the location of the
         * webapp when the client requests the root URL.
         */

        // setup http client.
       	m_client = HttpClientConfigurator.getInstance().newInstance();
        
       	// setup manager for service.
       	m_mgr = new RemoteRepositoryManager(m_serviceURL, m_client,
               getIndexManager().getExecutorService());
       	
       	// setup client for current namespace on service.
        m_repo = m_mgr.getRepositoryForNamespace(namespace);

		if (log.isInfoEnabled())
			log.info("Setup Active Threads: " + Thread.activeCount());
	
	}

    @Override
	public void tearDown() throws Exception {

		if (log.isTraceEnabled())
			log.trace("tearing down test: " + getName());

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
        
        m_rootURL = null;
		m_serviceURL = null;
		
//        if (m_cm != null) {
//            m_cm.shutdown();
//            m_cm = null;
//        }
		
		log.info("Connection Shutdown Check");

		  m_mgr.close();
//        m_repo.close();
        m_client.stop();

        m_mgr = null;
        m_repo = null;
        m_client = null;
        
        log.info("tear down done");

        super.tearDown();
        
        final int nthreads = Thread.activeCount();

		if (log.isInfoEnabled())
			log.info("Teardown Active Threads: " + nthreads);
        
        if (nthreads > 300) {
        	log.error("High residual thread count: " + nthreads);
        }
	}

//    /**
//    * Returns a view of the triple store using the sail interface.
//    * 
//    * FIXME DO NOT CIRCUMVENT! Use the REST API throughout this test suite.
//    */
//    @Deprecated
//    protected BigdataSail getSail() {
//
//		final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
//				.getResourceLocator().locate(namespace, ITx.UNISOLATED);
//
//        return new BigdataSail(tripleStore);
//
//    }

//	protected String getStreamContents(final InputStream inputStream)
//            throws IOException {
//
//        final Reader rdr = new InputStreamReader(inputStream);
//		
//	    final StringBuffer sb = new StringBuffer();
//		
//	    final char[] buf = new char[512];
//	    
//		while (true) {
//		
//		    final int rdlen = rdr.read(buf);
//			
//		    if (rdlen == -1)
//				break;
//			
//		    sb.append(buf, 0, rdlen);
//		    
//		}
//		
//		return sb.toString();
//
//	}

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
   static protected long countResults(final TupleQueryResult result)
         throws Exception {

      try {

         long count = 0;

         while (result.hasNext()) {

            result.next();

            count++;

         }

         return count;

      } finally {

         result.close();

      }

   }

   /**
    * Counts the #of results in a SPARQL result set.
    * 
    * @param result
    *           The connection from which to read the results.
    * 
    * @return The #of results.
    * 
    * @throws Exception
    *            If anything goes wrong.
    */
   static protected long countResults(final GraphQueryResult result)
         throws Exception {

      try {

         long count = 0;

         while (result.hasNext()) {

            result.next();

            count++;

         }

         return count;
         
      } finally {

         result.close();

      }
    	
	}

   /**
    * Count matches of the triple pattern.
    */
   static protected int countMatches(final Graph g, final Resource s,
           final URI p, final Value o) {

       int n = 0;

       final Iterator<Statement> itr = g.match(s, p, o);

       while (itr.hasNext()) {

           itr.next();
           
           n++;

       }

       return n;

   }

   /**
    * Return the statements matching the triple pattern.
    */
   static protected Statement[] getMatches(final Graph g, final Resource s,
           final URI p, final Value o) {

       final List<Statement> out = new LinkedList<Statement>();

       final Iterator<Statement> itr = g.match(s, p, o);

       while (itr.hasNext()) {

           out.add(itr.next());

       }

       return out.toArray(new Statement[out.size()]);

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
    * @throws Exception
    */
  static protected void assertSameGraph(final Graph expected,
        final IPreparedGraphQuery actual) throws Exception {

     assertSameGraph(expected, asGraph(actual));

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
  static protected void assertSameGraph(final Graph expected, final Graph actual) {

     for (Statement s : expected) {

        if (!actual.contains(s))
           fail("Expecting: " + s);

     }

        assertEquals("size", expected.size(), actual.size());

    }

  /**
   * Preferred version executes the {@link IPreparedGraphQuery} and ensures
   * that the {@link GraphQueryResult} is closed.
   * 
   * @param preparedQuery
   *           The prepared query.
   * 
   * @return The resulting graph.
   * 
   * @throws Exception
   */
  static protected Graph asGraph(final IPreparedGraphQuery preparedQuery)
        throws Exception {

     final GraphQueryResult result = preparedQuery.evaluate();

     try {

        final Graph g = new LinkedHashModel();

        while (result.hasNext()) {

           g.add(result.next());

        }

        return g;

     } finally {

        result.close();

     }

  }

  /**
   * @deprecated by {@link #asGraph(IPreparedGraphQuery)} which can ensure that
   *             the {@link GraphQueryResult} is closed.
   */
  static protected Graph asGraph(final GraphQueryResult result) throws Exception {

     try {
        final Graph g = new LinkedHashModel();

        while (result.hasNext()) {

           g.add(result.next());

        }

        return g;
     } finally {
        result.close();
     }

  }

  /**
   * Return the #of solutions in a result set.
   * 
   * @param result
   *           The result set.
   * 
   * @return The #of solutions.
   */
  static protected long countResults(final RepositoryResult<Statement> result)
        throws Exception {

     try {
        long i;
        for (i = 0; result.hasNext(); i++) {
           result.next();
        }
        return i;
     } finally {
        result.close();
     }

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
    protected byte[] genNTRIPLES(final int ntriples, final RDFFormat format)
            throws RDFHandlerException {

        final Graph g = genNTRIPLES2(ntriples);
        
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
    
    protected Graph genNTRIPLES2(final int ntriples)
			throws RDFHandlerException {

		final Graph g = new LinkedHashModel();

		final ValueFactory f = new ValueFactoryImpl();

		final URI s = f.createURI("http://www.bigdata.org/b");

		final URI rdfType = f
				.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		for (int i = 0; i < ntriples; i++) {

			final URI o = f.createURI("http://www.bigdata.org/c#" + i);

			g.add(s, rdfType, o);

		}
		
		return g;

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
     * Generate and return a very large literal.
     */
    protected Literal getVeryLargeLiteral() {

        final int len = 1024000;

        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {

            sb.append(Character.toChars('A' + (i % 26)));

        }
        
        return new LiteralImpl(sb.toString());
        
    }

   /**
    * @see #getExactSize()
    */
   protected long countAll() throws Exception {
    	
//    	return getSail().getDatabase().getExplicitStatementCount(null);

       return m_repo.rangeCount(true/*exact*/, null/*s*/, null/*p*/, null/*o*/);
    	
//    	final RemoteRepository repo = new RemoteRepository(m_serviceURL);
//    	
//    	final String countQuery = "select * where {?s ?p ?o}";
//    	
//		final TupleQuery query = repo.prepareTupleQuery(countQuery);
//		
//		return countResults(query.evaluate());
    	
    }
    
   /**
    * Return the exact number of statements in the repository.
    * 
    * @see #countAll()
    */
   @Deprecated
   protected long getExactSize() {

       try {
        return countAll();
    } catch (Exception e) {
       throw new RuntimeException(e);
    }
//       return getSail().getDatabase().getStatementCount(true/* true */);

   }

    /**
     * Test helps PUTs some data, verifies that it is visible, DELETEs the data,
     * and then verifies that it is gone.
     * 
     * @param format
     *            The interchange format.
     */
    protected void doDeleteWithPostTest(final RDFFormat format) throws Exception {

        doInsertWithBodyTest("POST", 23, /*requestPath,*/ format);

        assertEquals(23, countAll());

        doDeleteWithBody(/*requestPath,*/ 23, format);

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countAll());
        
    }

	protected long doDeleteWithQuery(/*final String servlet, */final String query) throws Exception {
		
	   final RemoveOp remove = new RemoveOp(query);
		
		return m_repo.remove(remove);
		
    }

    protected long doDeleteWithAccessPath(//
//            final String servlet,//
            final URI s,//
            final URI p,//
            final Value o,//
            final URI... c//
            ) throws Exception {
    	
    	final RemoveOp remove = new RemoveOp(s, p, o, c);
    	
    	return m_repo.remove(remove);
    	
    }

    protected void doDeleteWithBody(
            /* final String servlet, */final int ntriples,
            final RDFFormat format) throws Exception {

            final byte[] data = genNTRIPLES(ntriples, format);
			
            final RemoveOp remove = new RemoveOp(data, format);
            
            assertEquals(ntriples, m_repo.remove(remove));
            
    }

	/**
	 * Test of POST w/ BODY having data to be loaded.
	 */
    protected void doInsertWithBodyTest(final String method, final int ntriples,
            /*final String servlet,*/ final RDFFormat format) throws Exception {
        
        final byte[] data = genNTRIPLES(ntriples, format);
//        final File file = File.createTempFile("bigdata-testnssclient", ".data");
        /*
         * Only for testing. Clients should use AddOp(File, RDFFormat).
         */
        final AddOp add = new AddOp(data, format);
        assertEquals(ntriples, m_repo.add(add));
		
		// Verify the expected #of statements in the store.
		{
			final String queryStr = "select * where {?s ?p ?o}";

			final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
			
			assertEquals(ntriples, countResults(query.evaluate()));
			
		}

    }

    /**
     * Insert a resource into the {@link NanoSparqlServer}.  This is used to
     * load resources in the test package into the server.
     */
    protected long doInsertbyURL(final String method, final String resource)
         throws Exception {

        final String uri = new File(resource).toURI().toString();

        final AddOp add = new AddOp(uri);

        return m_repo.add(add);

    }

    /**
     * Read the contents of a file.
     * 
     * @param file
     *            The file.
     * @return It's contents.
     */
    protected static String readFromFile(final File file) throws IOException {

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
    
    protected static Graph readGraphFromFile(final File file) throws RDFParseException, RDFHandlerException, IOException {
        
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
        
        final Graph g = new LinkedHashModel();
        
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
    static protected byte[] writeOnBuffer(final RDFFormat format, final Graph g)
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
    protected Graph loadGraphFromResource(final String resource)
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
    protected long doInsertByBody(final String method,
            /*final String servlet,*/ final RDFFormat rdfFormat, final Graph g,
            final URI defaultContext) throws Exception {

        final byte[] wireData = writeOnBuffer(rdfFormat, g);

//        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final AddOp add = new AddOp(wireData, rdfFormat);
        if (defaultContext != null)
        	add.setContext(defaultContext);
        return m_repo.add(add);

    }
    
    protected static String getResponseBody(final HttpURLConnection conn)
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

    /**
     * Inserts some data into the KB and then issues a DESCRIBE query against
     * the REST API and verifies the expected results.
     * 
     * @param format
     *            The format is used to specify the Accept header.
     * 
     * @throws Exception
     */
    protected void doDescribeTest(final String method, final RDFFormat format)
            throws Exception {
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
        final Literal label1 = new LiteralImpl("Mike");
        final Literal label2 = new LiteralImpl("Bryan");

      {
         final Graph g = new LinkedHashModel();
         g.add(mike, RDF.TYPE, person);
         g.add(mike, likes, rdf);
         g.add(mike, RDFS.LABEL, label1);
         g.add(bryan, RDF.TYPE, person);
         g.add(bryan, likes, rdfs);
         g.add(bryan, RDFS.LABEL, label2);

         m_repo.add(new AddOp(g));
      }

        // The expected results.
        final Graph expected = new LinkedHashModel();
        {
            expected.add(new StatementImpl(mike, likes, rdf));
            expected.add(new StatementImpl(mike, RDF.TYPE, person));
            expected.add(new StatementImpl(mike, RDFS.LABEL, label1));
        }
        
        // Run the query and verify the results.
        {
            
        	final String queryStr =
                "prefix bd: <"+BD.NAMESPACE+"> " +//
                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
                "DESCRIBE ?x " +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
                "  ?x bd:likes bd:RDF " +//
                "}";

        	assertSameGraph(expected, m_repo.prepareGraphQuery(queryStr));
            
        }

    }
    
    /**
    * Sets up a simple data set on the server.
    * 
    * @throws Exception
    */
    protected void setupDataOnServer() throws Exception {
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
        final Literal label1 = new LiteralImpl("Mike");
        final Literal label2 = new LiteralImpl("Bryan");

      {
         final Graph g = new LinkedHashModel();
         g.add(mike, RDF.TYPE, person);
         g.add(mike, likes, rdf);
         g.add(mike, RDFS.LABEL, label1);
         g.add(bryan, RDF.TYPE, person);
         g.add(bryan, likes, rdfs);
         g.add(bryan, RDFS.LABEL, label2);

         m_repo.add(new AddOp(g));
      }

    }
    
    /**
     * Sets up a simple data set on the server.
    * @throws Exception 
     */
    protected void setupQuadsDataOnServer() throws Exception {
        
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");
        final URI likes = new URIImpl(BD.NAMESPACE + "likes");
        final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
        final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
        final URI c1 = new URIImpl(BD.NAMESPACE + "c1");
        final URI c2 = new URIImpl(BD.NAMESPACE + "c2");
        final URI c3 = new URIImpl(BD.NAMESPACE + "c3");
        final Literal label1 = new LiteralImpl("Mike");
        final Literal label2 = new LiteralImpl("Bryan");

      {
         final Graph g = new LinkedHashModel();
         g.add(mike, RDF.TYPE, person, c1, c2, c3);
         g.add(mike, likes, rdf, c1, c2, c3);
         g.add(mike, RDFS.LABEL, label1, c1, c2, c3);
         g.add(bryan, RDF.TYPE, person, c1, c2, c3);
         g.add(bryan, likes, rdfs, c1, c2, c3);
         g.add(bryan, RDFS.LABEL, label2, c1, c2, c3);
         m_repo.add(new AddOp(g));
      }

    }
    
    protected void doConstructTest(final String method, final RDFFormat format)
            throws Exception {
        
        setupDataOnServer();
        final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
        final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
        final URI person = new URIImpl(BD.NAMESPACE + "Person");

        // The expected results.
        final Graph expected = new LinkedHashModel();
        {
//            expected.add(new StatementImpl(mike, likes, rdf));
            expected.add(new StatementImpl(mike, RDF.TYPE, person));
            expected.add(new StatementImpl(bryan, RDF.TYPE, person));
//            expected.add(new StatementImpl(mike, RDFS.LABEL, label1));
        }
        
        // Run the query and verify the results.
        {

            final String queryStr =
                "prefix bd: <"+BD.NAMESPACE+"> " +//
                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
                "CONSTRUCT { ?x rdf:type bd:Person }" +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
//                "  ?x bd:likes bd:RDF " +//
                "}";

            final IPreparedGraphQuery query = m_repo.prepareGraphQuery(queryStr);

//            final Graph actual = asGraph(query.evaluate());

            assertSameGraph(expected, query);
            
        }
    
    }
    
}
