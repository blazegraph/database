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

import java.io.File;
import java.net.URL;
import java.util.Properties;

import junit.framework.Test;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;

/**
 * Proxied test suite for {@link BigdataSailRemoteRepository} and
 * {@link BigdataSailRemoteRepositoryConnection}.
 * <p>
 * Note: There are two versions of the test suite. One with isolatable indices
 * enabled and one without. A namespace DOES NOT need to be configured for
 * isolatable indices in order to create and manipulate transactions, but it
 * DOES need to be configured with isolatable indices in order for you to WRITE
 * on the namespace using a transaction.
 * 
 * @param <S>
 * 
 *           FIXME *** Explicit tests of web.xml tx URL rewrites. Can we hit the
 *           transaction manager if all we have is the sparqlEndpointURL?
 *           
 *           FIXME *** Can we run the same repository test suite that we use for
 *           embedded tests? (BigdataSparqlTest). This would mean having a test
 *           suite class so we can use it from another maven project.
 *           
 *           FIXME *** com.bigdata.rdf.sail.webapp.TestSparqlUpdate runs against
 *           the com.bigdata.rdf.sail.webapp.client.RemoteRepository. It could
 *           run against the {@link BigdataSailRemoteRepository}.
 * 
 *           FIXME *** Verify that we are running the full embedded repository
 *           test suite, including the tests for the extended transaction API.
 */
public class TestBigdataSailRemoteRepository<S extends IIndexManager> extends
      AbstractTestNanoSparqlClient<S> {

   public TestBigdataSailRemoteRepository() {

   }

   public TestBigdataSailRemoteRepository(final String name) {

      super(name);

   }

   public static Test suite() {

//      return ProxySuiteHelper.suiteWhenStandalone(TestBigdataSailRemoteRepository.class,
//            "test.*", TestMode.quads
////            , TestMode.sids
////            , TestMode.triples
//            );

      return ProxySuiteHelper.suiteWhenStandalone(TestBigdataSailRemoteRepository.ReadWriteTx.class,
//            "test.*", TestMode.quads
            "test_tx_begin_addStatement_commit.*", TestMode.quads
//            , TestMode.sids
//            , TestMode.triples
            );
       
   }

   /**
    * The repository under test.
    */
   protected BigdataSailRemoteRepository repo = null;

   /**
    * A connection obtained by {@link #setUp()} from {@link #repo}. This
    * connection will be closed when the fixture is torn down.
    */
   protected BigdataSailRemoteRepositoryConnection cxn = null;
	
   @Override
   public void setUp() throws Exception {

      super.setUp();

      repo = m_repo.getBigdataSailRemoteRepository();

      cxn = repo.getConnection();

   }

   @Override
   public void tearDown() throws Exception {

      if (cxn != null) {

         cxn.close();

         cxn = null;

      }

      // See #1207 (Memory leak in CI).
      repo = null;
      
      super.tearDown();

   }

   /**
    * The URI for the default prefix (":") in SPARQL QUERY and UPDATE requests.
    */
   protected static final String DEFAULT_PREFIX = "http://bigdata.com/";
   
   /**
    * A bunch of namespace declarations to be used by the tests.
    * @return
    */
   protected String getNamespaceDeclarations() {

      final StringBuilder declarations = new StringBuilder();
      declarations.append("PREFIX : <" + DEFAULT_PREFIX + "> \n");
      declarations.append("PREFIX rdf: <" + RDF.NAMESPACE + "> \n");
      declarations.append("PREFIX rdfs: <" + RDFS.NAMESPACE + "> \n");
      declarations.append("PREFIX foaf: <" + FOAF.NAMESPACE + "> \n");
      declarations.append("PREFIX xsd: <" + XMLSchema.NAMESPACE + "> \n");
      declarations.append("PREFIX dc: <" + DC.NAMESPACE + "> \n");
      declarations.append("PREFIX ex: <" + "http://example.org/" + "> \n");
      declarations.append("\n");

      return declarations.toString();
   }

   @Override
    protected void doInsertWithBodyTest(final String method, final int ntriples,
            /*final String servlet,*/ final RDFFormat format) throws Exception {

        final Graph g = genNTRIPLES2(ntriples);
        cxn.add(g);
        assertEquals(ntriples, getExactSize());
        
		// Verify the expected #of statements in the store.
		{
			final String queryStr = "select * where {?s ?p ?o}";
			final TupleQuery query = cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
			assertEquals(ntriples, countResults(query.evaluate()));
		}

    }

	@Override
    protected long doInsertByBody(final String method,
            /*final String servlet,*/ final RDFFormat rdfFormat, final Graph g,
            final URI defaultContext) throws Exception {
		
		final long start = getExactSize();
		
		cxn.add(g, defaultContext != null ? new Resource[] { defaultContext } : new Resource[0]);
		
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
		
		cxn.remove(s, p, o, c);
		
		return start - getExactSize();
		
	}
	
    protected RepositoryResult<Statement> doGetWithAccessPath(//
          final URI s,//
          final URI p,//
          final Value o,//
          final URI... c//
          ) throws Exception {

		return cxn.getStatements(s, p, o, false/*includeInferred*/, c);
		
	}

    /**
     * "ASK" query with an empty KB.
     */
    public void test_ASK() throws Exception {
        
        final String queryStr = "ASK where {?s ?p ?o}";
        
        final BooleanQuery query = cxn.prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);
        assertEquals(false, query.evaluate());
        
    }

    /**
     * Select everything in the kb using a GET. There will be no solutions
     * (assuming that we are using a told triple kb or quads kb w/o axioms).
     */
	public void test_SELECT_ALL() throws Exception {

		final String queryStr = "select * where {?s ?p ?o}";

        final TupleQuery query = cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
		assertEquals(0, countResults(query.evaluate()));


	}

    /**
     * A GET query which should result in an error (the query is not well
     * formed).
     */
    public void test_GET_SELECT_ERROR() throws Exception {

        final String queryStr = "select * where {?s ?p ?o} X {}";

        final TupleQuery query = cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
        
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
        	final TupleQuery query = cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
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
        	cxn.add(file, "", RDFFormat.TURTLE, new URIImpl("http://example.org"));
            assertEquals(7, getExactSize());
        }

        // Verify that the data were inserted into the appropriate context.
        {
            final String queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
        	final TupleQuery query = cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
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
        	final BooleanQuery query = cxn.prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);
            assertEquals(false, query.evaluate());
        }

        // #of statements in that RDF file.
        final long expectedStatementCount = 7;
        
        // Load the resource into the KB.
        {
        	final URL url = new URL("file:src/test/java/com/bigdata/rdf/sail/webapp/quads.nq");
        	cxn.add(url, "", RDFFormat.NQUADS);
            assertEquals(7, getExactSize());
        }

        /*
         * Verify KB has the loaded data.
         */
        {
            final String queryStr = "SELECT * where {?s ?p ?o}";
        	final TupleQuery query = cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
            assertEquals(expectedStatementCount, countResults(query.evaluate()));
        }

    }
        
    /**
     * Test of insert and retrieval of a large literal.
     */
    public void test_INSERT_veryLargeLiteral() throws Exception {

        final Graph g = new LinkedHashModel();
        
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
            final GraphQuery query = cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
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
            
            final BooleanQuery query = cxn.prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);
            assertEquals(false, query.evaluate());
            
        }

        // #of statements in that RDF file.
        final long expectedStatementCount = 4;
        
        // Load the resource into the KB.
        {
        	cxn.add( this.getClass().getClassLoader().getResource("com/bigdata/rdf/rio/small.rdf"), "", RDFFormat.RDFXML);
            //cxn.add(new URL("file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf"), "", RDFFormat.RDFXML);
            assertEquals(expectedStatementCount, getExactSize());
        }

        /*
         * Verify KB has the loaded data.
         */
        {
            final String queryStr = "SELECT * where {?s ?p ?o}";

            final TupleQuery query = cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
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
        
        final RepositoryResult<Resource> contexts = cxn.getContextIDs();
        
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
    
    /**
     * Basic test creates a read/write connection, issues begin(), and then
     * issues rollback() on the connection.
     */
    public void test_tx_begin_rollback() throws RepositoryException {

       assertFalse(cxn.isActive());

       cxn.begin();
       
       assertTrue(cxn.isActive());
       
       cxn.rollback();
       
       assertFalse(cxn.isActive());
       
    }

    /**
     * Basic test creates a read/write connection, issues begin(), and then
     * issues commit() on the connection.
     */
    public void test_tx_begin_commit() throws RepositoryException {

       assertFalse(cxn.isActive());

       cxn.begin();
       
       assertTrue(cxn.isActive());
       
       cxn.commit();
       
       assertFalse(cxn.isActive());
       
    }

   /**
    * An *extension* of the test suite that uses a namespace that is configured
    * to support read/write transactions.
    * <p>
    * Note: This does not change whether or not a transaction may be created,
    * just whether or not the namespace will allow an operation that is isolated
    * by a read/write transaction.
    */
   static public class ReadWriteTx<S extends IIndexManager> extends
         TestBigdataSailRemoteRepository<S> {

      public ReadWriteTx() {

      }

      public ReadWriteTx(final String name) {

         super(name);

      }

      /**
       * Enable isolatable indices for so we can have concurrent read/write
       * transactions in the {@link RepositoryConnection}.
       */
      @Override
      public Properties getProperties() {

         final Properties p = new Properties(super.getProperties());

         p.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");

         return p;

      }

      /**
       * Basic test creates a read/write connection, issues begin(), and then
       * issues commit() on the connection.
       * 
       * TODO Test where we abort the connection. Verify write set is discarded.
       * 
       * @throws RepositoryException
       * @throws MalformedQueryException
       * @throws UpdateExecutionException
       */
      public void test_tx_begin_addStatement_commit() throws RepositoryException,
            MalformedQueryException, UpdateExecutionException {
         
if(true) return; // FIXME (***) TX TEST DISABLED

         assertFalse(cxn.isActive());

         cxn.begin();

         assertTrue(cxn.isActive());

         final URI a = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "a");
         final URI b = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "b");
         final URI c = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "c");
         
         assertFalse(cxn.hasStatement(a, b, c, true/* includeInferred */));
         
         // Add to default graph.
         cxn.add(cxn.getValueFactory().createStatement(a,b,c));
         
         // visible inside of the connection.
         assertTrue(cxn.hasStatement(a, b, c, true/* includeInferred */));

         // not visible from a new connection.
         {
            final BigdataSailRemoteRepositoryConnection cxn2 = repo
                  .getConnection();
            try {
               assertTrue(cxn2 != cxn);
               // cxn2.begin();
               assertFalse(cxn2.hasStatement(a, b, c, true/* includeInferred */));
            } finally {
               cxn2.close();
            }
         }
       
         cxn.commit();

         assertFalse(cxn.isActive());

      }

      /**
       * Basic test creates a read/write connection, issues begin(), and then
       * issues commit() on the connection.
       * 
       * TODO Test where we abort the connection. Verify write set is discarded.
       * 
       * @throws RepositoryException
       * @throws MalformedQueryException
       * @throws UpdateExecutionException
       */
      public void test_tx_begin_UPDATE_commit() throws RepositoryException,
            MalformedQueryException, UpdateExecutionException {

if(true) return; // FIXME (***) TX TEST DISABLED

         assertFalse(cxn.isActive());

         cxn.begin();

         assertTrue(cxn.isActive());

         final URI a = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "a");
         final URI b = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "b");
         final URI c = cxn.getValueFactory().createURI(DEFAULT_PREFIX + "c");
         
         assertFalse(cxn.hasStatement(a, b, c, true/* includeInferred */));
         
         cxn.prepareUpdate(QueryLanguage.SPARQL,
               getNamespaceDeclarations() + "INSERT DATA { :a :b :c }").execute();

         // visible inside of the connection.
         assertTrue(cxn.hasStatement(a, b, c, true/* includeInferred */));

         // not visible from a new connection.
         {
            final BigdataSailRemoteRepositoryConnection cxn2 = repo
                  .getConnection();
            try {
               assertTrue(cxn2 != cxn);
               // cxn2.begin();
               assertFalse(cxn2.hasStatement(a, b, c, true/* includeInferred */));
            } finally {
               cxn2.close();
            }
         }
       
         cxn.commit();

         assertFalse(cxn.isActive());

      }


   }

}
