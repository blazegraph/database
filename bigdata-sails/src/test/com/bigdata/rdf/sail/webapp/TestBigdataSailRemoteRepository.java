package com.bigdata.rdf.sail.webapp;

import java.io.File;
import java.net.URL;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public class TestBigdataSailRemoteRepository<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {

    public TestBigdataSailRemoteRepository() {

    }

	public TestBigdataSailRemoteRepository(final String name) {

		super(name);

	}

	private RepositoryConnection m_cxn = null;
	
	@Override
	public void setUp() throws Exception {
		
		super.setUp();
		
        final Repository repo = new BigdataSailRemoteRepository(m_repo);
        
        m_cxn = repo.getConnection();
		
	}
	
	@Override
	public void tearDown() throws Exception {
	    
        if (m_cxn != null) {

            m_cxn.close();

            m_cxn = null;

        }

	    super.tearDown();
	    
	}
	
	@Override
    protected void doInsertWithBodyTest(final String method, final int ntriples,
            /*final String servlet,*/ final RDFFormat format) throws Exception {

        final Graph g = genNTRIPLES2(ntriples);
        m_cxn.add(g);
        assertEquals(ntriples, getExactSize());
        
		// Verify the expected #of statements in the store.
		{
			final String queryStr = "select * where {?s ?p ?o}";
			final TupleQuery query = m_cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
			assertEquals(ntriples, countResults(query.evaluate()));
		}

    }

	@Override
    protected long doInsertByBody(final String method,
            /*final String servlet,*/ final RDFFormat rdfFormat, final Graph g,
            final URI defaultContext) throws Exception {
		
		final long start = getExactSize();
		
		m_cxn.add(g, defaultContext != null ? new Resource[] { defaultContext } : new Resource[0]);
		
		return getExactSize() - start;
		
	}

	@Override
    protected long doDeleteWithAccessPath(//
//          final String servlet,//
          final URI s,//
          final URI p,//
          final Value o,//
          final URI... c//
          ) throws Exception {

		final long start = getExactSize();
		
		m_cxn.remove(s, p, o, c);
		
		return start - getExactSize();
		
	}
	
    protected RepositoryResult<Statement> doGetWithAccessPath(//
          final URI s,//
          final URI p,//
          final Value o,//
          final URI... c//
          ) throws Exception {

		return m_cxn.getStatements(s, p, o, false, c);
		
	}

    /**
     * "ASK" query with an empty KB.
     */
    public void test_ASK() throws Exception {
        
        final String queryStr = "ASK where {?s ?p ?o}";
        
        final BooleanQuery query = m_cxn.prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);
        assertEquals(false, query.evaluate());
        
    }

    /**
     * Select everything in the kb using a GET. There will be no solutions
     * (assuming that we are using a told triple kb or quads kb w/o axioms).
     */
	public void test_SELECT_ALL() throws Exception {

		final String queryStr = "select * where {?s ?p ?o}";

        final TupleQuery query = m_cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
		assertEquals(0, countResults(query.evaluate()));


	}

    /**
     * A GET query which should result in an error (the query is not well
     * formed).
     */
    public void test_GET_SELECT_ERROR() throws Exception {

        final String queryStr = "select * where {?s ?p ?o} X {}";

        final TupleQuery query = m_cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
        
        try {
		
        	assertEquals(0, countResults(query.evaluate()));
        	
        	fail("should be an error");
        	
        } catch (QueryEvaluationException ex) {
        	
        	// perfect
        	
        }

    }
    
    public void test_INSERT_withBody_RDFXML() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.RDFXML);
        
    }
    
    public void test_INSERT_withBody_NTRIPLES() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.NTRIPLES);
        
    }
    
    public void test_INSERT_withBody_N3() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.N3);
        
    }
    
    public void test_INSERT_withBody_TURTLE() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.TURTLE);
        
    }
    
    // Note: quads interchange
    public void test_INSERT_withBody_TRIG() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.TRIG);
        
    }
    
    // Note: quads interchange
    public void test_INSERT_withBody_TRIX() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.TRIX);
        
    }

    // Note: quads interchange
    public void test_POST_INSERT_withBody_NQUADS() throws Exception {

        doInsertWithBodyTest("POST", 23, RDFFormat.NQUADS);
        
    }

    // TODO Write test for UPDATE where we override the default context using
    // the context-uri.
    public void test_INSERT_triples_with_BODY_and_defaultContext()
            throws Exception {

        if(TestMode.quads != getTestMode())
            return;

        final String resource = packagePath
                + "insert_triples_with_defaultContext.ttl";

        final Graph g = loadGraphFromResource(resource);

        // Load the resource into the KB.
        doInsertByBody("POST", RDFFormat.TURTLE, g, new URIImpl(
                "http://example.org"));
        
        // Verify that the data were inserted into the appropriate context.
        {
        	final String queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
        	final TupleQuery query = m_cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
    		assertEquals(7, countResults(query.evaluate()));
        }

    }
    
    public void test_INSERT_triples_with_URI_and_defaultContext() throws Exception {

        if(TestMode.quads != getTestMode())
            return;
        
        // Load the resource into the KB.
        {
        	final File file = new File(packagePath
                    + "insert_triples_with_defaultContext.ttl");
        	m_cxn.add(file, "", RDFFormat.TURTLE, new URIImpl("http://example.org"));
            assertEquals(7, getExactSize());
        }

        // Verify that the data were inserted into the appropriate context.
        {
            final String queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
        	final TupleQuery query = m_cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
            assertEquals(7, countResults(query.evaluate()));
        }
        
    }

    /**
     * Test for POST of an NQuads resource by a URL.
     */
    public void test_INSERT_NQuads_by_URL()
            throws Exception {

        if(TestMode.quads != getTestMode())
            return;

        // Verify nothing in the KB.
        {
            final String queryStr = "ASK where {?s ?p ?o}";
        	final BooleanQuery query = m_cxn.prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);
            assertEquals(false, query.evaluate());
        }

        // #of statements in that RDF file.
        final long expectedStatementCount = 7;
        
        // Load the resource into the KB.
        {
        	final URL url = new URL("file:bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/quads.nq");
        	m_cxn.add(url, "", RDFFormat.NQUADS);
            assertEquals(7, getExactSize());
        }

        /*
         * Verify KB has the loaded data.
         */
        {
            final String queryStr = "SELECT * where {?s ?p ?o}";
        	final TupleQuery query = m_cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
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
            final String queryStr = "DESCRIBE <" + s.stringValue() + ">";
            final GraphQuery query = m_cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
            g2 = asGraph(query.evaluate());
            
        }
        
        assertEquals(1, g2.size());
        
        assertTrue(g2.match(s, p, o).hasNext());
        
    }
    
    /**
     * Test ability to load data from a URI.
     */
    public void test_INSERT_LOAD_FROM_URI() throws Exception {

        // Verify nothing in the KB.
        {
            final String queryStr = "ASK where {?s ?p ?o}";
            
            final BooleanQuery query = m_cxn.prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);
            assertEquals(false, query.evaluate());
            
        }

        // #of statements in that RDF file.
        final long expectedStatementCount = 4;
        
        // Load the resource into the KB.
        {
            m_cxn.add(new URL("file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf"), "", RDFFormat.RDFXML);
            assertEquals(expectedStatementCount, getExactSize());
        }

        /*
         * Verify KB has the loaded data.
         */
        {
            final String queryStr = "SELECT * where {?s ?p ?o}";

            final TupleQuery query = m_cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
            assertEquals(expectedStatementCount, countResults(query.evaluate()));
        }

    }

    /**
     * Test the CONTEXTS method.
     */
    public void test_CONTEXTS() throws Exception {

    	if (getTestMode() != TestMode.quads)
    		return;

        doInsertbyURL("POST", packagePath
                + "test_estcard.trig");
        
        final RepositoryResult<Resource> contexts = m_cxn.getContextIDs();
        
        int size = 0;
        while (contexts.hasNext()) {
        	contexts.next();
        	size++;
        }
        
        assertEquals(3, size);
        
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

        if(TestMode.quads != getTestMode())
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

        if(TestMode.quads != getTestMode())
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

        if(TestMode.quads != getTestMode())
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
    
    /**
     * Get everything matching an access path description.
     */
    public void test_GET_accessPath_delete_all() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                null
        ));

        assertEquals(7, result);
        
    }
    
    /**
     * Get everything with a specific subject.
     */
    public void test_GET_accessPath_delete_s() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                new URIImpl("http://www.bigdata.com/Mike"),// s
                null,// p
                null
        ));

        assertEquals(3, result);
        
    }

    /**
     * Get everything with a specific predicate.
     */
    public void test_GET_accessPath_delete_p() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                new URIImpl("http://www.w3.org/2000/01/rdf-schema#label"),// p
                null// o
        ));

        assertEquals(2, result);
        
    }
    
    /**
     * Get everything with a specific object (a URI).
     */
    public void test_GET_accessPath_delete_o_URI() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                new URIImpl("http://xmlns.com/foaf/0.1/Person")// o
        ));

        assertEquals(3, result);
        
    }
    
    /**
     * Get everything with a specific object (a Literal).
     */
    public void test_GET_accessPath_delete_o_Literal() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                new URIImpl("http://www.bigdata.com/Bryan")// o
        ));

        assertEquals(1, result);
        
    }
    
    /**
     * Get everything with a specific predicate and object (a URI).
     */
    public void test_GET_accessPath_delete_p_o_URI() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                RDF.TYPE,// p
                new URIImpl("http://xmlns.com/foaf/0.1/Person")// o
        ));

        assertEquals(3, result);
        
    }
    
    /**
     * Get everything with a specific predicate and object (a Literal).
     */
    public void test_GET_accessPath_delete_p_o_Literal() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                RDFS.LABEL,// p
                new LiteralImpl("Bryan")// o
        ));

        assertEquals(1, result);
        
    }
    
    /**
     * Get using an access path which does not match anything.
     */
    public void test_GET_accessPath_delete_NothingMatched() throws Exception {

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.ttl");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                new URIImpl("http://xmlns.com/foaf/0.1/XXX")// o
        ));

        assertEquals(0, result);
        
    }

    /**
     * Get everything in a named graph (context).
     */
    public void test_GET_accessPath_delete_c() throws Exception {

        if(TestMode.quads != getTestMode())
            return;
        
        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.trig");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/") // c
        ));

        assertEquals(3, result);
        
    }

    /**
     * Get everything in a different named graph (context).
     */
    public void test_GET_accessPath_delete_c1() throws Exception {

        if(TestMode.quads != getTestMode())
            return;
        
        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.trig");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                null,// o
                new URIImpl("http://www.bigdata.com/c1") // c
        ));

        assertEquals(2, result);
        
    }

    /**
     * Get using an access path with the context position bound. 
     */
    public void test_GET_accessPath_delete_c_nothingMatched() throws Exception {

        if(TestMode.quads != getTestMode())
            return;

        doInsertbyURL("POST", packagePath
                + "test_delete_by_access_path.trig");

        final long result = countResults(doGetWithAccessPath(//
//                requestPath,//
                null,// s
                null,// p
                null,// o
                new URIImpl("http://xmlns.com/foaf/0.1/XXX") // c
        ));

        assertEquals(0, result);
        
    }
    

}
