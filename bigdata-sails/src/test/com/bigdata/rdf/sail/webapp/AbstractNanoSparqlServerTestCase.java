/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
/*
 * Created on Mar 6, 2012
 */

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

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.eclipse.jetty.server.Server;
import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
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
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.SailException;
import org.xml.sax.Attributes;
import org.xml.sax.ext.DefaultHandler2;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.util.config.NicUtil;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <S>
 */
public abstract class AbstractNanoSparqlServerTestCase<S extends IIndexManager>
        extends ProxyTestCase<S> {

    /**
     * The path used to resolve resources in this package when they are being
     * uploaded to the {@link NanoSparqlServer}.
     */
    protected static final String packagePath = "bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/";
    /**
     * A jetty {@link Server} running a {@link NanoSparqlServer} instance which
     * is running against that {@link #m_indexManager}.
     */
    protected Server m_fixture;
    /**
     * The namespace of the {@link AbstractTripleStore} instance against which
     * the test is running. A unique namespace is used for each test run, but
     * the namespace is based on the test name.
     */
    protected String namespace;
    /**
     * The effective {@link NanoSparqlServer} http end point.
     */
    protected String m_serviceURL;
    /**
     * The request path for the REST API under test.
     */
    protected static final String requestPath = "/sparql";
    protected TestMode testMode = null;

    /**
     * Options for the query.
     */
    static class QueryOptions {

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
        public Map<String, String[]> requestParams;

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
     * 
     */
    public AbstractNanoSparqlServerTestCase() {
        super();
    }
    
    /**
     * @param name
     */
    public AbstractNanoSparqlServerTestCase(String name) {
        super(name);
    }
    
    protected AbstractTripleStore createTripleStore(
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
    
    		tripleStore.destroy();
    		
    	}
    
    }

    @Override
    public void setUp() throws Exception {
        
    	super.setUp();
    
    	log.warn("Setting up test:" + getName());
    	
    	// guaranteed distinct namespace for the KB instance.
    	namespace = getName() + UUID.randomUUID();
    
        final AbstractTripleStore tripleStore = createTripleStore(
                getIndexManager(), namespace, getProperties());

    	if (tripleStore.isStatementIdentifiers()) {
            testMode = TestMode.sids;
        } else if (tripleStore.isQuads()) {
            testMode = TestMode.quads;
        } else {
            testMode = TestMode.triples;
        }
                
    	// Start the NSS with that KB instance as the default namespace.
        {
         
            final Map<String, String> initParams = new LinkedHashMap<String, String>();
            {

                initParams.put(ConfigParams.NAMESPACE, namespace);

                initParams.put(ConfigParams.CREATE, "false");

            }

            // Start server for that kb instance.
            m_fixture = NanoSparqlServer.newInstance(0/* port */,
                    getIndexManager(), initParams);

            m_fixture.start();

        }

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
     * Returns a view of the default triple store for the test using the sail
     * interface.
     * 
     * @throws SailException
     */
    protected BigdataSail getSail() throws SailException {

        return getSail(namespace);

    }

    /**
     * Returns a view of the named triple store using the sail interface.
     * 
     * @throws SailException
     */
    protected BigdataSail getSail(final String namespace) throws SailException {

        final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager()
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        if (tripleStore == null) {
         
            // Not found.
            return null;
        }

        final BigdataSail sail = new BigdataSail(tripleStore);
        
        sail.initialize();

        return sail;

    }

    /**
     * Add any URL query parameters.
     */
    protected void addQueryParams(final StringBuilder urlString,
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
        }
    }

    protected HttpURLConnection doConnect(final String urlString,
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
            addQueryParams(urlString, opts.requestParams);

        if (log.isDebugEnabled()) {
            log.debug("*** Request ***");
            log.debug(opts.serviceURL);
            log.debug(opts.queryStr);
        }

        HttpURLConnection conn = null;
        try {

            // conn = doConnect(urlString.toString(), opts.method);
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

                conn.setRequestProperty("Content-Length",
                        Integer.toString(opts.data.length));

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

    protected MutationResult getMutationResult(final HttpURLConnection conn)
            throws Exception {

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

    private static Graph readGraphFromFile(final File file)
            throws RDFParseException, RDFHandlerException, IOException {
                
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

    protected String getStreamContents(final InputStream inputStream) throws IOException {
    
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
     * Test helper verifies the expected status code and optionally logs the
     * response message and the response body.
     * 
     * @param conn
     * 
     * @throws IOException
     */
    protected void assertErrorStatusCode(final int statusCode, final HttpURLConnection conn)
            throws IOException {
            
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
    protected static byte[] writeOnBuffer(final RDFFormat format, final Graph g)
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
     * Load and return a graph from a resource.
     * 
     * @param resource
     *            The resource.
     * 
     * @return The graph.
     */
    protected Graph loadGraphFromResource(final String resource)
            throws RDFParseException, RDFHandlerException, IOException {

        // final RDFFormat rdfFormat = RDFFormat.forFileName(resource);

        final Graph g = readGraphFromFile(new File(resource));

        return g;

    }

}