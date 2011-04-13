/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Mar 9, 2011
 */

package com.bigdata.rdf.sail.bench;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
import com.bigdata.rdf.sail.bench.NanoSparqlServer.Config;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.util.config.NicUtil;

/**
 * Test suite for the {@link NanoSparqlServer}.
 * <p>
 * Note: This test suite assumes a triples only instance. This simplifying
 * assumption makes it easy to verify the expected results of various operations
 * since only the told triples should be found and queried. Support for quads
 * should be tested separately.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Test conneg for CONSTRUCT queries and DESCRIBE queries (both return
 *       statements) and SELECT queries (can be SPARQL result sets, json, or
 *       binary format supported by openrdf).
 * 
 * @todo Test POST for INSERT/DELETE for triples/sids/quads.
 * 
 *       <pre>
 * POST /
 * Content-Type: application/rdf+xml
 * Content-Length: ...
 * 
 * rdf/xml content to be inserted or deleted.
 * </pre>
 * 
 * @todo test w/ non-default namespace.
 * 
 * @todo test w/ non-default timestamp (via query parameter).
 * 
 * @todo do another test suite which uses a journal that we load once and keep
 *       around as a test file similar to the RTO test classes.
 *       
 * @deprecated along with {@link NanoSparqlServer}
 */
public class TestNanoSparqlServer extends TestCase2 {

    /**
     * 
     */
    public TestNanoSparqlServer() {
    }

    /**
     * @param name
     */
    public TestNanoSparqlServer(String name) {
        super(name);
    }

    /**
     * Setup a transient database instance for a triples only kb.
     */
    @Override
    public Properties getProperties() {

        final Properties properties = new Properties();

        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());

        properties.setProperty(com.bigdata.journal.Options.INITIAL_EXTENT, ""
                + (Bytes.megabyte32 * 1));

//        if(false/*quads*/) {
//            properties.setProperty(AbstractTripleStore.Options.QUADS_MODE, "true");
//            properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,"false");
//            properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
//            properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
//            properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
//        } 
//        if (true/* told triples */) {
            properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,"false");
            properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
            properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
            properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
//        } 
//        if (false/* triples w/ truth maintenance */) {
//            properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
//        } 
//        if (false/* sids w/ truth maintenance */) {
//            properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "true");
//        } 

        return properties;

    }
    
    protected NanoSparqlServer fixture = null;

    protected Journal jnl = null;
    
    protected String serviceURL = null;
    
    /**
     * Returns a view of the triple store using the sail interface.
     */
    protected BigdataSail getSail() {

        final String namespace = getName();

        final AbstractTripleStore tripleStore = (AbstractTripleStore) jnl
                .getResourceLocator().locate(namespace, ITx.UNISOLATED);

        return new BigdataSail(tripleStore);

    }
    
    @Override
    protected void setUp() throws Exception {

        final Properties properties = getProperties();

        final String namespace = getName();

//        log.info("Creating journal");
        
        jnl = new Journal(properties);

//        log.info("Creating kb");

        // Create the kb instance.
        new LocalTripleStore(jnl, namespace, ITx.UNISOLATED, properties)
                .create();

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

//        log.info("Starting server");

        // Start server for that kb instance.
        fixture = new NanoSparqlServer(config, jnl);

//        log.info("Server running");

        assertTrue("open", fixture.isOpen());

        final int port = fixture.getPort();

//        log.info("Getting host address");

        final String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                true/* loopbackOk */);

        if(hostAddr == null) {
       
            fail("Could not identify network address for this host.");
            
        }
        
        serviceURL = new URL("http", hostAddr, port, ""/* file */).toExternalForm();

//        log.info("Setup done: "+serviceURL);

    }

    @Override
    protected void tearDown() throws Exception {

        if (fixture != null) {

//            log.info("server shutdown");
            
            fixture.shutdownNow();
            
            fixture = null;

        }

        if(jnl != null) {

//            log.info("journal destroy");

            jnl.destroy();
        
            jnl = null;
            
        }
        
        serviceURL = null;

//        log.info("tear down done");

    }

    /**
     * Options for the query.
     */
    private static class QueryOptions {

        /** The default timeout (ms). */
        private static final int DEFAULT_TIMEOUT = 2000;
        
        /** The URL of the SPARQL endpoint. */
        public String serviceURL = null;
//        public String username = null;
//        public String password = null;
        /** The HTTP method (GET, POST, etc). */
        public String method = "GET";
        /** The SPARQL query. */
        public String queryStr = null;
        /** The default graph URI (optional). */
        public String defaultGraphUri = null;

        /** The connection timeout (ms) -or- ZERO (0) for an infinate timeout. */
        public int timeout = DEFAULT_TIMEOUT;
//        public boolean showQuery = false;

    }

    /**
     * Connect to a SPARQL end point (GET or POST query only).
     * 
     * @param opts
     *            The query request.
     * 
     * @return The connection.
     */
    protected HttpURLConnection doSparqlQuery(final QueryOptions opts)
            throws Exception {

        // Fully formed and encoded URL @todo use */* for ASK.
        final String urlString = opts.serviceURL
                + "?query="
                + URLEncoder.encode(opts.queryStr, "UTF-8")
                + (opts.defaultGraphUri == null ? ""
                        : ("&default-graph-uri=" + URLEncoder.encode(
                                opts.defaultGraphUri, "UTF-8")));

        final URL url = new URL(urlString);
        HttpURLConnection conn = null;
        try {

            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(opts.method);
            conn.setDoOutput(true);
            conn.setUseCaches(false);
            conn.setReadTimeout(opts.timeout);

            /*
             * Set an appropriate Accept header for the query.
             * 
             * @todo ASK queries have boolean data, JSON format is also
             * available.
             */
            conn.setRequestProperty("Accept",//
                    NanoSparqlServer.MIME_SPARQL_RESULTS_XML + ";q=1" + //
                            "," + //
                            NanoSparqlServer.MIME_RDF_XML + ";q=1"//
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
            
            final RDFXMLParser rdfParser = new RDFXMLParser(
                    new ValueFactoryImpl());

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

            final TupleQueryResultParser parser = new SPARQLResultsXMLParserFactory()
                    .getParser();

            parser
                    .setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
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
    public void test_GET_SELECT_ALL() throws Exception {

        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = serviceURL;
        opts.queryStr = queryStr;
        opts.method = "GET";

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts)));

    }

    /**
     * Select everything in the kb using a POST.
     */
    public void test_POST_SELECT_ALL() throws Exception {

        final String queryStr = "select * where {?s ?p ?o}";

        final QueryOptions opts = new QueryOptions();
        opts.serviceURL = serviceURL;
        opts.queryStr = queryStr;
        opts.method = "POST";

        // No solutions (assuming a told triple kb or quads kb w/o axioms).
        assertEquals(0, countResults(doSparqlQuery(opts)));

    }

    /**
     * Test GET with DESCRIBE query.
     */
    public void test_GET_DESCRIBE() throws Exception {

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
            opts.serviceURL = serviceURL;
            opts.queryStr =//
                "prefix bd: <"+BD.NAMESPACE+"> " +//
                "prefix rdf: <"+RDF.NAMESPACE+"> " +//
                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +//
                "describe ?x " +//
                "WHERE { " +//
                "  ?x rdf:type bd:Person . " +//
                "  ?x bd:likes bd:RDF " +//
                "}";

            final Graph actual = buildGraph(doSparqlQuery(opts));

            assertSameGraph(expected, actual);
            
        }

    }

    /**
     * @todo Test GET with CONSTRUCT query.
     */
    public void test_GET_CONSTRUCT() throws Exception {
        fail("write test");
    }

    /**
     * @todo Test GET with ASK query.
     */
    public void test_GET_ASK() throws Exception {
        fail("write test");
    }

    /**
     * @todo Test of POST w/ BODY having data to be loaded.
     */
    public void test_POST_withBody_NTRIPLES() throws Exception {
        
        fail("This test deadlocks - use the jetty test suite instead.");
        
        HttpURLConnection conn = null;
        try {
            final URL url = new URL(serviceURL);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setReadTimeout(0);// TODO timeout (ms)
            conn.setRequestProperty("Content-Type", RDFFormat.NTRIPLES
                    .getDefaultMIMEType());
            
            final OutputStream os = conn.getOutputStream();
            try {
                String data = //
"#@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"+//
"#@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"+//
"#@prefix owl:  <http://www.w3.org/2002/07/owl#> .\n"+//
"#@prefix : <#> .\n"+//
"<http://www.bigdata.org/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.bigdata.org/c>.\n"//
;
                final Writer w = new OutputStreamWriter(new BufferedOutputStream(os));
                w.write(data);
                w.flush();
                w.close();
                os.flush();
                os.close();
            } finally {
                os.close();
            }
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

        {
            final String queryStr = "select * where {?s ?p ?o}";

            final QueryOptions opts = new QueryOptions();
            opts.serviceURL = serviceURL;
            opts.queryStr = queryStr;
            opts.method = "GET";

            assertEquals(1, countResults(doSparqlQuery(opts)));
        }
        
        fail("do query and verify the state change.");
        
    }

    /**
     * @todo Test POST w/ URIs of data to be loaded.
     */
    public void test_POST_withURIs() throws Exception {
       fail("write test"); 
    }

    /**
     * @todo Test DELETE with query.
     */
    public void test_delete_withQuery() throws Exception {
        fail("write test");
    }

    /**
     * @todo Test DELETE with body.
     */
    public void test_delete_withBody() throws Exception {
        fail("write test");
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
