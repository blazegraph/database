package com.bigdata.rdf.sail.webapp;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.BooleanQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.GraphQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.RemoveOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.TupleQuery;
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

	public void test_startup() throws Exception {

	    assertTrue("open", m_fixture.isRunning());
	    
	}
	
    /**
     * "ASK" query with an empty KB.
     */
    public void test_ASK() throws Exception {
        
        final String queryStr = "ASK where {?s ?p ?o}";
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final BooleanQuery query = repo.prepareBooleanQuery(queryStr);
        assertEquals(false, query.evaluate());

//        final QueryOptions opts = new QueryOptions();
//        opts.serviceURL = m_serviceURL;
//        opts.queryStr = queryStr;
//        opts.method = "GET";
//
//        opts.acceptHeader = BooleanQueryResultFormat.SPARQL.getDefaultMIMEType();
//        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
//
//        opts.acceptHeader = BooleanQueryResultFormat.TEXT.getDefaultMIMEType();
//        assertEquals(false, askResults(doSparqlQuery(opts, requestPath)));
        
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

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final TupleQuery query = repo.prepareTupleQuery(queryStr);
		assertEquals(0, countResults(query.evaluate()));

        
//		final QueryOptions opts = new QueryOptions();
//		opts.serviceURL = m_serviceURL;
//		opts.queryStr = queryStr;
//		opts.method = "GET";
//
//		opts.acceptHeader = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
//		assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
//
//		// TODO JSON parser is not bundled by openrdf.
////        opts.acceptHeader = TupleQueryResultFormat.JSON.getDefaultMIMEType();
////        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
//
//        opts.acceptHeader = TupleQueryResultFormat.BINARY.getDefaultMIMEType();
//        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));

	}

//    /**
//     * Select everything in the kb using a POST. There will be no solutions
//     * (assuming that we are using a told triple kb or quads kb w/o axioms).
//     */
//    public void test_POST_SELECT_ALL() throws Exception {
//
//        final String queryStr = "select * where {?s ?p ?o}";
//
//        final QueryOptions opts = new QueryOptions();
//        opts.serviceURL = m_serviceURL;
//        opts.queryStr = queryStr;
//        opts.method = "POST";
//
//        opts.acceptHeader = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
//        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
//
//        // TODO JSON parser is not bundled by openrdf.
////        opts.acceptHeader = TupleQueryResultFormat.JSON.getDefaultMIMEType();
////        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
//
//        opts.acceptHeader = TupleQueryResultFormat.BINARY.getDefaultMIMEType();
//        assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
//
//    }

    /**
     * A GET query which should result in an error (the query is not well
     * formed).
     */
    public void test_GET_SELECT_ERROR() throws Exception {

        final String queryStr = "select * where {?s ?p ?o} X {}";

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final TupleQuery query = repo.prepareTupleQuery(queryStr);
        
        try {
		
        	assertEquals(0, countResults(query.evaluate()));
        	
        	fail("should be an error");
        	
        } catch (IOException ex) {
        	
        	// perfect
        	
        }

//		final QueryOptions opts = new QueryOptions();
//        opts.serviceURL = m_serviceURL;
//        opts.queryStr = queryStr;
//        opts.method = "GET";
//
//        opts.acceptHeader = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
//        
//        assertErrorStatusCode(HttpServletResponse.SC_BAD_REQUEST,
//                doSparqlQuery(opts, requestPath));

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

//    // FIXME We need an NQuadsWriter to run this test.
//    // Note: quads interchange
//    public void test_POST_INSERT_withBody_NQUADS() throws Exception {
//
//        doInsertWithBodyTest("POST", 23, NQuadsParser.nquads);
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
            final RemoteRepository repo = new RemoteRepository(m_serviceURL);
            final TupleQuery query = repo.prepareTupleQuery(queryStr);
    		assertEquals(7, countResults(query.evaluate()));

        }

    }
    
    public void test_POST_INSERT_triples_with_URI_and_defaultContext() throws Exception {

        if(TestMode.quads != testMode)
            return;
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        
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
            add.setContext("http://example.org");
            assertEquals(7, repo.add(add));
            
        }

        // Verify that the data were inserted into the appropriate context.
        {
//            final QueryOptions opts = new QueryOptions();
//            opts.serviceURL = m_serviceURL;
//            opts.method = "GET";
//            opts.queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
//            assertEquals(7, countResults(doSparqlQuery(opts, requestPath)));
            
            final String queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
            final TupleQuery query = repo.prepareTupleQuery(queryStr);
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

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        
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
            
            final BooleanQuery query = repo.prepareBooleanQuery(queryStr);
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
            assertEquals(expectedStatementCount, repo.add(add));

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
            
            final TupleQuery query = repo.prepareTupleQuery(queryStr);
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
            
            final RemoteRepository repo = new RemoteRepository(m_serviceURL);
            final String queryStr = "DESCRIBE <" + s.stringValue() + ">";
            final GraphQuery query = repo.prepareGraphQuery(queryStr);
            g2 = query.evaluate();
            
        }
        
        assertEquals(1, g2.size());
        
        assertTrue(g2.match(s, p, o).hasNext());
        
    }
    
    /**
     * Test ability to load data from a URI.
     */
    public void test_POST_INSERT_LOAD_FROM_URIs() throws Exception {

    	final RemoteRepository repo = new RemoteRepository(m_serviceURL);
    	
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
            
            final BooleanQuery query = repo.prepareBooleanQuery(queryStr);
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
            assertEquals(expectedStatementCount, repo.add(add));

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
            
            final TupleQuery query = repo.prepareTupleQuery(queryStr);
            assertEquals(expectedStatementCount, countResults(query.evaluate()));
            
        }

    }

    /**
     * Test the ESTCARD method (fast range count).
     */
    public void test_ESTCARD() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                null,// s
//                null,// p
//                null,// o
//                null // c
//        );
//
//        assertEquals(7, rangeCountResult.rangeCount);
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
        		null,// s
                null,// p
                null,// o
                null // c
        );
        assertEquals(7, rangeCount);
        
    }

    public void test_ESTCARD_s() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                new URIImpl("http://www.bigdata.com/Mike"),// s
//                null,// p
//                null,// o
//                null // c
//        );
//
//        assertEquals(3, rangeCountResult.rangeCount);
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
        		new URIImpl("http://www.bigdata.com/Mike"),// s
        		null,// p
                null,// o
                null // c
        );
        assertEquals(3, rangeCount);
        
    }
    
    public void test_ESTCARD_p() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                null,// s
//                RDF.TYPE,// p
//                null,// o
//                null // c
//        );
//
//        assertEquals(3, rangeCountResult.rangeCount);
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
                null,// s
                RDF.TYPE,// p
                null,// o
                null // c
        );
        assertEquals(3, rangeCount);
        
    }

    public void test_ESTCARD_p2() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                null,// s
//                RDFS.LABEL,// p
//                null,// o
//                null // c
//        );
//
//        assertEquals(2, rangeCountResult.rangeCount);
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
                null,// s
                RDFS.LABEL,// p
                null,// o
                null // c
        );
        assertEquals(2, rangeCount);
        
    }

    public void test_ESTCARD_o() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                null,// s
//                null,// p
//                new LiteralImpl("Mike"),// o
//                null // c
//        );
//
//        assertEquals(1, rangeCountResult.rangeCount);
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
                null,// s
                null,// p
                new LiteralImpl("Mike"),// o
                null // c
        );
        assertEquals(1, rangeCount);
        
    }

    public void test_ESTCARD_so() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_estcard.ttl");
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                new URIImpl("http://www.bigdata.com/Mike"),// s,
//                RDF.TYPE,// p
//                null,// o
//                null // c
//        );
//
//        assertEquals(1, rangeCountResult.rangeCount);
        
        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
                new URIImpl("http://www.bigdata.com/Mike"),// s,
                RDF.TYPE,// p
                null,// o
                null // c
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
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                null,// s,
//                null,// p
//                null,// o
//                null // c
//        );
//
//        assertEquals(7, rangeCountResult.rangeCount);

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
                null,// s,
                null,// p
                null,// o
                null // c
        );
        assertEquals(7, rangeCount);
        
    }
    
    public void test_ESTCARD_quads_02() throws Exception {

        if(TestMode.quads != testMode)
            return;

        doInsertbyURL("POST", packagePath
                + "test_estcard.trig");
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                null,// s,
//                null,// p
//                null,// o
//                new URIImpl("http://www.bigdata.com/")// c
//        );
//
//        assertEquals(3, rangeCountResult.rangeCount);

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
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
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                null,// s,
//                null,// p
//                null,// o
//                new URIImpl("http://www.bigdata.com/c1")// c
//        );
//
//        assertEquals(2, rangeCountResult.rangeCount);

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
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
        
//        final RangeCountResult rangeCountResult = doRangeCount(//
//                requestPath,//
//                new URIImpl("http://www.bigdata.com/Mike"),// s,
//                null,// p
//                null,// o
//                new URIImpl("http://www.bigdata.com/c1")// c
//        );
//
//        assertEquals(1, rangeCountResult.rangeCount);

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        final long rangeCount = repo.rangeCount(
                new URIImpl("http://www.bigdata.com/Mike"),// s,
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/c1")// c
        );
        assertEquals(1, rangeCount);
        
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
                null,// o
                null // c
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
                null,// o
                null // c
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
                null,// o
                null // c
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
                new URIImpl("http://xmlns.com/foaf/0.1/Person"),// o
                null // c
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
                new URIImpl("http://www.bigdata.com/Bryan"),// o
                null // c
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
                new URIImpl("http://xmlns.com/foaf/0.1/Person"),// o
                null // c
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
                new LiteralImpl("Bryan"),// o
                null // c
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
                new URIImpl("http://xmlns.com/foaf/0.1/XXX"),// o
                null // c
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

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        
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

            final GraphQuery query = repo.prepareGraphQuery(deleteQueryStr);
            assertSameGraph(expected, query.evaluate());
            
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
            assertEquals(2, repo.update(remove, add));
            
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
                
                assertSameGraph(expected, repo.prepareGraphQuery(deleteQueryStr).evaluate());

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

                assertSameGraph(expected, repo.prepareGraphQuery(queryStr2).evaluate());

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

        final RemoteRepository repo = new RemoteRepository(m_serviceURL);
        
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
            
            assertSameGraph(expected, repo.prepareGraphQuery(deleteQueryStr).evaluate());

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
            assertEquals(2, repo.update(remove, add));
            
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

                assertSameGraph(expected, repo.prepareGraphQuery(queryStr2).evaluate());

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

                assertSameGraph(expected, repo.prepareGraphQuery(queryStr2).evaluate());

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
