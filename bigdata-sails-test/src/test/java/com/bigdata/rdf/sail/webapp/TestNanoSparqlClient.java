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
import java.io.IOException;

import junit.framework.Test;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;
import com.bigdata.rdf.sail.webapp.client.IPreparedBooleanQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.IRemoteRepository.TupleQuery;
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
                "test.*", TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}

//	/**
//	 * Select everything in the kb using a GET. There will be no solutions
//	 * (assuming that we are using a told triple kb or quads kb w/o axioms).
//	 */
//	public void test_SELECT_ALL() throws Exception {
//
//		final String queryStr = "select * where {?s ?p ?o}";
//
//		{
//
//			final IPreparedTupleQuery query = m_repo
//					.prepareTupleQuery(queryStr);
//
//			assertEquals(0, countResults(query.evaluate()));
//
//		}
//
//		{
//
//			final IPreparedTupleQuery query = m_repo
//					.prepareTupleQuery(queryStr);
//
//			query.setHeader("Accept",
//					TupleQueryResultFormat.SPARQL.getDefaultMIMEType());
//
//			assertEquals(0, countResults(query.evaluate()));
//
//		}
//
//		{
//
//			final IPreparedTupleQuery query = m_repo
//					.prepareTupleQuery(queryStr);
//
//			query.setHeader("Accept",
//					TupleQueryResultFormat.BINARY.getDefaultMIMEType());
//
//			assertEquals(0, countResults(query.evaluate()));
//
//		}
//
//		/**
//		 * FIXME The necessary parser does not appear to be available. If you
//		 * enable this you will get ClassNotFoundException for
//		 * <code>au/com/bytecode/opencsv/CSVReader</code>
//		 * 
//		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/714" >
//		 *      Migrate to openrdf 2.7 </a>
//		 */
//		if (false) {
//
//			final IPreparedTupleQuery query = m_repo
//					.prepareTupleQuery(queryStr);
//
//			query.setHeader("Accept",
//					TupleQueryResultFormat.CSV.getDefaultMIMEType());
//
//			assertEquals(0, countResults(query.evaluate()));
//
//		}
//
//		{
//
//			final IPreparedTupleQuery query = m_repo
//					.prepareTupleQuery(queryStr);
//
//			query.setHeader("Accept",
//					TupleQueryResultFormat.TSV.getDefaultMIMEType());
//
//			assertEquals(0, countResults(query.evaluate()));
//
//		}
//
//		/**
//		 * Enabled now that we have a JSON result format parser (openrdf 2.7).
//		 * 
//		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/714" >
//		 *      Migrate to openrdf 2.7 </a>
//		 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/588" >
//		 *      JSON-LD </a>
//		 */
//		if (true) {
//
//			final IPreparedTupleQuery query = m_repo
//					.prepareTupleQuery(queryStr);
//
//			query.setHeader("Accept",
//					TupleQueryResultFormat.JSON.getDefaultMIMEType());
//
//			assertEquals(0, countResults(query.evaluate()));
//
//		}
//
//	}
//
//	// /**
//	// * Select everything in the kb using a POST. There will be no solutions
//	// * (assuming that we are using a told triple kb or quads kb w/o axioms).
//	// */
//	// public void test_POST_SELECT_ALL() throws Exception {
//	//
//	// final String queryStr = "select * where {?s ?p ?o}";
//	//
//	// final QueryOptions opts = new QueryOptions();
//	// opts.serviceURL = m_serviceURL;
//	// opts.queryStr = queryStr;
//	// opts.method = "POST";
//	//
//	// opts.acceptHeader = TupleQueryResultFormat.SPARQL.getDefaultMIMEType();
//	// assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
//	//
//	// // TODO JSON parser is not bundled by openrdf.
//	// //opts.acceptHeader = TupleQueryResultFormat.JSON.getDefaultMIMEType();
//	// //assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
//	//
//	// opts.acceptHeader = TupleQueryResultFormat.BINARY.getDefaultMIMEType();
//	// assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
//	//
//	// }
//
//	/**
//	 * A GET query which should result in an error (the query is not well
//	 * formed).
//	 */
//	public void test_GET_SELECT_ERROR() throws Exception {
//
//		final String queryStr = "select * where {?s ?p ?o} X {}";
//
//		final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
//
//		try {
//
//			assertEquals(0, countResults(query.evaluate()));
//
//			fail("should be an error");
//
//		} catch (IOException ex) {
//
//			// perfect
//
//		}
//
//	}
//
//	public void test_POST_INSERT_withBody_RDFXML() throws Exception {
//
//		doInsertWithBodyTest("POST", 23, RDFFormat.RDFXML);
//
//	}
//
//	public void test_POST_INSERT_withBody_NTRIPLES() throws Exception {
//
//		doInsertWithBodyTest("POST", 23, RDFFormat.NTRIPLES);
//
//	}
//
//	public void test_POST_INSERT_withBody_N3() throws Exception {
//
//		doInsertWithBodyTest("POST", 23, RDFFormat.N3);
//
//	}
//
//	public void test_POST_INSERT_withBody_TURTLE() throws Exception {
//
//		doInsertWithBodyTest("POST", 23, RDFFormat.TURTLE);
//
//	}
//
//	// Note: quads interchange
//	public void test_POST_INSERT_withBody_TRIG() throws Exception {
//
//		doInsertWithBodyTest("POST", 23, RDFFormat.TRIG);
//
//	}
//
//	// Note: quads interchange
//	public void test_POST_INSERT_withBody_TRIX() throws Exception {
//
//		doInsertWithBodyTest("POST", 23, RDFFormat.TRIX);
//
//	}
//
//	// Note: requires NQuadsWriter to run this test (openrdf 2.7)
//	// Note: quads interchange
//	public void test_POST_INSERT_withBody_NQUADS() throws Exception {
//
//		doInsertWithBodyTest("POST", 23, RDFFormat.NQUADS);
//
//	}
//
//	// TODO Write test for UPDATE where we override the default context using
//	// the context-uri.
//	public void test_POST_INSERT_triples_with_BODY_and_defaultContext()
//			throws Exception {
//
//		if (TestMode.quads != getTestMode())
//			return;
//
//		final String resource = packagePath
//				+ "insert_triples_with_defaultContext.ttl";
//
//		final Graph g = loadGraphFromResource(resource);
//
//		// Load the resource into the KB.
//		doInsertByBody("POST", RDFFormat.TURTLE, g, new URIImpl(
//				"http://example.org"));
//
//		// Verify that the data were inserted into the appropriate context.
//		{
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.method = "GET";
//			// opts.queryStr =
//			// "select * { GRAPH <http://example.org> {?s ?p ?p} }";
//			// assertEquals(7, countResults(doSparqlQuery(opts, requestPath)));
//
//			final String queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
//			// final RemoteRepository repo = new RemoteRepository(m_serviceURL);
//			final IPreparedTupleQuery query = m_repo
//					.prepareTupleQuery(queryStr);
//			assertEquals(7, countResults(query.evaluate()));
//
//		}
//
//	}
//
//	public void test_POST_INSERT_triples_with_URI_and_defaultContext()
//			throws Exception {
//
//		if (TestMode.quads != getTestMode())
//			return;
//
//		// final RemoteRepository repo = new RemoteRepository(m_serviceURL);
//
//		// Load the resource into the KB.
//		{
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.method = "POST";
//			// opts.requestParams = new LinkedHashMap<String, String[]>();
//			// // set the resource to load.
//			// opts.requestParams.put("uri", new String[] { new File(packagePath
//			// + "insert_triples_with_defaultContext.ttl").toURI()
//			// .toString() });
//			// // set the default context.
//			// opts.requestParams.put("context-uri",
//			// new String[] { "http://example.org" });
//			// assertEquals(
//			// 7,
//			// getMutationResult(doSparqlQuery(opts,
//			// requestPath)).mutationCount);
//
//			final AddOp add = new AddOp(new File(packagePath
//					+ "insert_triples_with_defaultContext.ttl").toURI()
//					.toString());
//			add.setContext(new URIImpl("http://example.org"));
//			assertEquals(7, m_repo.add(add));
//
//		}
//
//		// Verify that the data were inserted into the appropriate context.
//		{
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.method = "GET";
//			// opts.queryStr =
//			// "select * { GRAPH <http://example.org> {?s ?p ?p} }";
//			// assertEquals(7, countResults(doSparqlQuery(opts, requestPath)));
//
//			final String queryStr = "select * { GRAPH <http://example.org> {?s ?p ?p} }";
//			final IPreparedTupleQuery query = m_repo
//					.prepareTupleQuery(queryStr);
//			assertEquals(7, countResults(query.evaluate()));
//
//		}
//
//	}
//
//	/**
//	 * Test for POST of an NQuads resource by a URL.
//	 */
//	public void test_POST_INSERT_NQuads_by_URL() throws Exception {
//
//		if (TestMode.quads != getTestMode())
//			return;
//
//		// final RemoteRepository repo = new RemoteRepository(m_serviceURL);
//
//		// Verify nothing in the KB.
//		{
//			final String queryStr = "ASK where {?s ?p ?o}";
//
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.queryStr = queryStr;
//			// opts.method = "GET";
//			//
//			// opts.acceptHeader = BooleanQueryResultFormat.SPARQL
//			// .getDefaultMIMEType();
//			// assertEquals(false, askResults(doSparqlQuery(opts,
//			// requestPath)));
//
//			final IPreparedBooleanQuery query = m_repo
//					.prepareBooleanQuery(queryStr);
//			assertEquals(false, query.evaluate());
//
//		}
//
//		// #of statements in that RDF file.
//		final long expectedStatementCount = 7;
//
//		// Load the resource into the KB.
//		{
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.method = "POST";
//			// opts.requestParams = new LinkedHashMap<String, String[]>();
//			// opts.requestParams
//			// .put("uri",
//			// new String[] {
//			// "file:bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/quads.nq"
//			// });
//			//
//			// final MutationResult result =
//			// getMutationResult(doSparqlQuery(opts,
//			// requestPath));
//			//
//			// assertEquals(expectedStatementCount, result.mutationCount);
//
//			final AddOp add = new AddOp(
//					"file:src/test/java/com/bigdata/rdf/sail/webapp/quads.nq");
//			assertEquals(expectedStatementCount, m_repo.add(add));
//
//		}
//
//		/*
//		 * Verify KB has the loaded data.
//		 */
//		{
//			final String queryStr = "SELECT * where {?s ?p ?o}";
//
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.queryStr = queryStr;
//			// opts.method = "GET";
//			//
//			// opts.acceptHeader = BooleanQueryResultFormat.SPARQL
//			// .getDefaultMIMEType();
//			//
//			// assertEquals(expectedStatementCount, countResults(doSparqlQuery(
//			// opts, requestPath)));
//
//                        final IPreparedTupleQuery query = m_repo.prepareTupleQuery(queryStr);
//			assertEquals(expectedStatementCount, countResults(query.evaluate()));
//
//		}
//
//	}
//
//	/**
//	 * Test of insert and retrieval of a large literal.
//	 */
//	public void test_INSERT_veryLargeLiteral() throws Exception {
//
//		final Graph g = new LinkedHashModel();
//
//		final URI s = new URIImpl("http://www.bigdata.com/");
//		final URI p = RDFS.LABEL;
//		final Literal o = getVeryLargeLiteral();
//		final Statement stmt = new StatementImpl(s, p, o);
//		g.add(stmt);
//
//		// Load the resource into the KB.
//		assertEquals(
//				1L,
//				doInsertByBody("POST", RDFFormat.RDFXML, g, null/* defaultContext */));
//
//		// Read back the data into a graph.
//		final Graph g2;
//		{
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.method = "GET";
//			// opts.queryStr = "DESCRIBE <" + s.stringValue() + ">";
//			// g2 = buildGraph(doSparqlQuery(opts, requestPath));
//
//			// final RemoteRepository repo = new RemoteRepository(m_serviceURL);
//			final String queryStr = "DESCRIBE <" + s.stringValue() + ">";
//			final IPreparedGraphQuery query = m_repo.prepareGraphQuery(queryStr);
//			g2 = asGraph(query);
//
//		}
//
//		assertEquals(1, g2.size());
//
//      assertTrue(g2.match(s, p, o).hasNext());
//
//	}
//
//	/**
//	 * Test ability to load data from a URI.
//	 */
//	public void test_POST_INSERT_LOAD_FROM_URIs() throws Exception {
//
//		// final RemoteRepository repo = new RemoteRepository(m_serviceURL);
//
//		// Verify nothing in the KB.
//		{
//			final String queryStr = "ASK where {?s ?p ?o}";
//
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.queryStr = queryStr;
//			// opts.method = "GET";
//			//
//			// opts.acceptHeader = BooleanQueryResultFormat.SPARQL
//			// .getDefaultMIMEType();
//			// assertEquals(false, askResults(doSparqlQuery(opts,
//			// requestPath)));
//
//			final IPreparedBooleanQuery query = m_repo
//					.prepareBooleanQuery(queryStr);
//			assertEquals(false, query.evaluate());
//
//		}
//
//		// #of statements in that RDF file.
//		final long expectedStatementCount = 4;
//
//		// Load the resource into the KB.
//		{
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.method = "POST";
//			// opts.requestParams = new LinkedHashMap<String, String[]>();
//			// opts.requestParams
//			// .put("uri",
//			// new String[] {
//			// "file:bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf" });
//			//
//			// final MutationResult result =
//			// getMutationResult(doSparqlQuery(opts,
//			// requestPath));
//			//
//			// assertEquals(expectedStatementCount, result.mutationCount);
//
//			final AddOp add = new AddOp(
//					this.getClass().getClassLoader().getResource("com/bigdata/rdf/rio/small.rdf").toExternalForm()
//					);
//			assertEquals(expectedStatementCount, m_repo.add(add));
//
//		}
//
//		/*
//		 * Verify KB has the loaded data.
//		 */
//		{
//			final String queryStr = "SELECT * where {?s ?p ?o}";
//
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.queryStr = queryStr;
//			// opts.method = "GET";
//			//
//			// opts.acceptHeader = BooleanQueryResultFormat.SPARQL
//			// .getDefaultMIMEType();
//			//
//			// assertEquals(expectedStatementCount, countResults(doSparqlQuery(
//			// opts, requestPath)));
//
//			final IPreparedTupleQuery query = m_repo
//					.prepareTupleQuery(queryStr);
//			assertEquals(expectedStatementCount, countResults(query.evaluate()));
//
//		}
//
//	}
//
//	/**
//	 * Select everything in the kb using a POST.
//	 */
//	public void test_DELETE_withQuery() throws Exception {
//
//		doInsertWithBodyTest("POST", 23, RDFFormat.NTRIPLES);
//
//		// assertEquals(23, countResults(doSparqlQuery(opts, requestPath)));
//		assertEquals(23, countAll());
//
//		doDeleteWithQuery("construct {?s ?p ?o} where {?s ?p ?o}");
//
//		// No solutions (assuming a told triple kb or quads kb w/o axioms).
//		// assertEquals(0, countResults(doSparqlQuery(opts, requestPath)));
//		assertEquals(0, countAll());
//
//	}
//
//	public void test_GET_CONSTRUCT_RDFXML() throws Exception {
//		doConstructTest("GET", RDFFormat.RDFXML);
//	}
//
//	public void test_GET_CONSTRUCT_NTRIPLES() throws Exception {
//		doConstructTest("GET", RDFFormat.NTRIPLES);
//	}
//
//	public void test_GET_CONSTRUCT_N3() throws Exception {
//		doConstructTest("GET", RDFFormat.N3);
//	}
//
//	public void test_GET_CONSTRUCT_TURTLE() throws Exception {
//		doConstructTest("GET", RDFFormat.TURTLE);
//	}
//
//	public void test_GET_CONSTRUCT_TRIG() throws Exception {
//		doConstructTest("GET", RDFFormat.TRIG);
//	}
//
//	public void test_GET_CONSTRUCT_TRIX() throws Exception {
//		doConstructTest("GET", RDFFormat.TRIX);
//	}
//
//	public void test_POST_CONSTRUCT_RDFXML() throws Exception {
//		doConstructTest("POST", RDFFormat.RDFXML);
//	}
//
//	public void test_POST_CONSTRUCT_NTRIPLES() throws Exception {
//		doConstructTest("POST", RDFFormat.NTRIPLES);
//	}
//
//	public void test_POST_CONSTRUCT_N3() throws Exception {
//		doConstructTest("POST", RDFFormat.N3);
//	}
//
//	public void test_POST_CONSTRUCT_TURTLE() throws Exception {
//		doConstructTest("POST", RDFFormat.TURTLE);
//	}
//
//	public void test_POST_CONSTRUCT_TRIG() throws Exception {
//		doConstructTest("POST", RDFFormat.TRIG);
//	}
//
//	public void test_POST_CONSTRUCT_TRIX() throws Exception {
//		doConstructTest("POST", RDFFormat.TRIX);
//	}
//
//    /**
//     * A construct where the data has duplicate triples in different named
//     * graphs. We materialize all triples. In this version of the test the
//     * duplicate triples should be eliminated (this is verified by a count of
//     * the CONSTRUCT results).
//     * 
//     * <pre>
//     * PREFIX : <http://www.bigdata.com/>
//     * 
//     * CONSTRUCT {
//     *   ?s ?p ?o
//     * } where {
//     *   GRAPH ?g {?s ?p ?o}
//     * }
//     * </pre>
//     * 
//     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1341"> Query hint
//     *      to disable DISTINCT SPO semantics for CONSTRUCT </a>
//     */
//    public void test_construct_eliminates_duplicates_triples() throws Exception {
//       
//        if (!getTestMode().isQuads()) {
//            // This is a quads mode only test.
//            return;
//        }
//        
//        // Load data. Includes the same triple in 2 different named graphs.
//        m_repo.add(new AddOp(
//                new File(
//                        "src/test/java/com/bigdata/rdf/sail/webapp/construct-eliminates-duplicate-triples.trig"),
//                RDFFormat.TRIG));
//
//        // Should be two statements.
//        final long rangeCount = m_repo.rangeCount(null/* s */, null/* p */,
//                null/* o */, (Resource) null/* c */);
//
//        assertEquals(2, rangeCount);
//        
////        final String queryHint = "\n hint:Query hint:constructDistinctSPO true .\n";
//        final String queryHint = ""; // No query hint.
//        
//        final String queryStr = "PREFIX : <http://www.bigdata.com/> \n" + //
//                "CONSTRUCT { ?s ?p ?o }\n"//
//                + "WHERE { " + queryHint + " GRAPH ?g {?s ?p ?o} }";
//
//        final IPreparedGraphQuery query = m_repo.prepareGraphQuery(queryStr);
//
//        final long n = countResults(query.evaluate());
//
//        assertEquals(1, n);
//
//    }
//    	
//    /**
//     * A construct where the data has duplicate triples in different named
//     * graphs. We materialize all triples. In this version of the test the
//     * duplicate triples should be eliminated (this is verified by a count of
//     * the CONSTRUCT results).
//     * 
//     * <pre>
//     * PREFIX : <http://www.bigdata.com/>
//     * 
//     * CONSTRUCT {
//     *   ?s ?p ?o
//     * } where {
//     *   hint:Query hint:constructDistinctSPO false .
//     *   GRAPH ?g {?s ?p ?o}
//     * }
//     * </pre>
//     * 
//     * @see <a href="https://jira.blazegraph.com/browse/BLZG-1341"> Query hint
//     *      to disable DISTINCT SPO semantics for CONSTRUCT </a>
//     */
//    public void test_construct_does_not_eliminate_duplicates_triples() throws Exception {
//       
//        if (!getTestMode().isQuads()) {
//            // This is a quads mode only test.
//            return;
//        }
//        
//        // Load data. Includes the same triple in 2 different named graphs.
//        m_repo.add(new AddOp(
//                new File(
//                        "src/test/java/com/bigdata/rdf/sail/webapp/construct-eliminates-duplicate-triples.trig"),
//                RDFFormat.TRIG));
//
//        // Should be two statements.
//        final long rangeCount = m_repo.rangeCount(null/* s */, null/* p */,
//                null/* o */, (Resource) null/* c */);
//
//        assertEquals(2, rangeCount);
//        
//        final String queryHint = "\n hint:Query hint:constructDistinctSPO false .\n";
//        
//        final String queryStr = "PREFIX : <http://www.bigdata.com/> \n" + //
//                "CONSTRUCT { ?s ?p ?o }\n"//
//                + "WHERE { " + queryHint + " GRAPH ?g {?s ?p ?o} }";
//
//        final IPreparedGraphQuery query = m_repo.prepareGraphQuery(queryStr);
//
//        final long n = countResults(query.evaluate());
//
//        assertEquals(2, n);
//
//    }
//        
//	/**
//	 * Unit test for ACID UPDATE using PUT. This test is for the operation where
//	 * a SPARQL selects the data to be deleted and the request body contains the
//	 * statements to be inserted.
//	 */
//	public void test_PUT_UPDATE_WITH_QUERY() throws Exception {
//
//	   setupDataOnServer();
//
//		// final RemoteRepository repo = new RemoteRepository(m_serviceURL);
//
//		final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
//		final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
//		// final URI person = new URIImpl(BD.NAMESPACE + "Person");
//		final URI likes = new URIImpl(BD.NAMESPACE + "likes");
//		final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
//		final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
//
//		// The format used to PUT the data.
//		final RDFFormat format = RDFFormat.NTRIPLES;
//
//		/*
//		 * This is the query that we will use to delete some triples from the
//		 * database.
//		 */
//		final String deleteQueryStr = //
//		"prefix bd: <" + BD.NAMESPACE + "> " + //
//				"prefix rdf: <" + RDF.NAMESPACE + "> " + //
//				"prefix rdfs: <" + RDFS.NAMESPACE + "> " + //
//				"CONSTRUCT { ?x bd:likes bd:RDFS }" + //
//				"WHERE { " + //
//				// "  ?x rdf:type bd:Person . " +//
//				"  ?x bd:likes bd:RDFS " + //
//				"}";
//
//		/*
//		 * First, run the query that we will use the delete the triples. This is
//		 * a cross check on the expected behavior of the query.
//		 */
//		{
//
//			// The expected results.
//			final Graph expected = new LinkedHashModel();
//			{
//				// expected.add(new StatementImpl(mike, RDF.TYPE, person));
//				expected.add(new StatementImpl(bryan, likes, rdfs));
//			}
//
//         assertSameGraph(expected, m_repo.prepareGraphQuery(deleteQueryStr));
//
//		}
//
//		/*
//		 * Setup the document containing the statement to be inserted by the
//		 * UPDATE operation.
//		 */
//		final byte[] data;
//		{
//			final Graph g = new LinkedHashModel();
//
//			// The new data.
//			g.add(new StatementImpl(bryan, likes, rdf));
//
//			final RDFWriterFactory writerFactory = RDFWriterRegistry
//					.getInstance().get(format);
//			if (writerFactory == null)
//				fail("RDFWriterFactory not found: format=" + format);
//			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
//			final RDFWriter writer = writerFactory.getWriter(baos);
//			writer.startRDF();
//			for (Statement stmt : g) {
//				writer.handleStatement(stmt);
//			}
//			writer.endRDF();
//			data = baos.toByteArray();
//		}
//
//		/*
//		 * Now, run the UPDATE operation.
//		 */
//		{
//
//			final RemoveOp remove = new RemoveOp(deleteQueryStr);
//			final AddOp add = new AddOp(data, format);
//			// Note: 1 removed, but also 1 added.
//			assertEquals(2, m_repo.update(remove, add));
//
//		}
//
//		/*
//		 * Now verify the post-condition state.
//		 */
//		{
//
//			/*
//			 * This query verifies that we removed the right triple (nobody is
//			 * left who likes 'rdfs').
//			 */
//			{
//
//				// The expected results.
//            final Graph expected = new LinkedHashModel();
//
//            assertSameGraph(expected, m_repo.prepareGraphQuery(deleteQueryStr));
//
//			}
//
//			/*
//			 * This query verifies that we added the right triple (two people
//			 * now like 'rdf').
//			 */
//			{
//
//				final String queryStr2 = //
//				"prefix bd: <" + BD.NAMESPACE + "> " + //
//						"prefix rdf: <" + RDF.NAMESPACE + "> " + //
//						"prefix rdfs: <" + RDFS.NAMESPACE + "> " + //
//						"CONSTRUCT { ?x bd:likes bd:RDF }" + //
//						"WHERE { " + //
//						// "  ?x rdf:type bd:Person . " + //
//						"  ?x bd:likes bd:RDF " + //
//						"}";
//
//				// The expected results.
//				final Graph expected = new LinkedHashModel();
//
//				expected.add(new StatementImpl(mike, likes, rdf));
//				expected.add(new StatementImpl(bryan, likes, rdf));
//
//				// final QueryOptions opts = new QueryOptions();
//				// opts.serviceURL = m_serviceURL;
//				// opts.queryStr = queryStr2;
//				// opts.method = "GET";
//				// opts.acceptHeader = TupleQueryResultFormat.SPARQL
//				// .getDefaultMIMEType();
//				//
//				// assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
//				// requestPath)));
//
//				assertSameGraph(expected, m_repo.prepareGraphQuery(queryStr2));
//
//			}
//
//		}
//
//	}
//
//	// /**
//	// * Unit test verifies that you can have a CONSTRUCT SPARQL with an empty
//	// * WHERE clause.
//	// *
//	// * @throws MalformedQueryException
//	// */
//	// public void test_CONSTRUCT_TEMPLATE_ONLY() throws MalformedQueryException
//	// {
//	//
//	// final String deleteQueryStr =//
//	// "prefix bd: <"+BD.NAMESPACE+"> " +//
//	// "CONSTRUCT { bd:Bryan bd:likes bd:RDFS }" +//
//	// "{}";
//	//
//	// new BigdataSPARQLParser().parseQuery(deleteQueryStr,
//	// "http://www.bigdata.com");
//	//
//	// }
//
//	/**
//	 * Unit test where the "query" used to delete triples from the database
//	 * consists solely of a CONSTRUCT "template" without a WHERE clause (the
//	 * WHERE clause is basically optional as all elements of it are optional).
//	 * 
//	 * @throws Exception
//	 */
//	public void test_PUT_UPDATE_WITH_CONSTRUCT_TEMPLATE_ONLY() throws Exception {
//
//	   setupDataOnServer();
//
//		// final RemoteRepository repo = new RemoteRepository(m_serviceURL);
//
//		final URI mike = new URIImpl(BD.NAMESPACE + "Mike");
//		final URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
//		// final URI person = new URIImpl(BD.NAMESPACE + "Person");
//		final URI likes = new URIImpl(BD.NAMESPACE + "likes");
//		final URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
//		final URI rdfs = new URIImpl(BD.NAMESPACE + "RDFS");
//
//		// The format used to PUT the data.
//		final RDFFormat format = RDFFormat.NTRIPLES;
//
//		/*
//		 * This is the query that we will use to delete some triples from the
//		 * database.
//		 */
//		final String deleteQueryStr = //
//		"prefix bd: <" + BD.NAMESPACE + "> " + //
//				"CONSTRUCT { bd:Bryan bd:likes bd:RDFS }" + //
//				"{ }";
//
//		// new BigdataSPARQLParser().parseQuery(deleteQueryStr,
//		// "http://www.bigdata.com");
//
//		/*
//		 * First, run the query that we will use the delete the triples. This is
//		 * a cross check on the expected behavior of the query.
//		 */
//		{
//
//			// The expected results.
//			final Graph expected = new LinkedHashModel();
//			{
//				// expected.add(new StatementImpl(mike, RDF.TYPE, person));
//				expected.add(new StatementImpl(bryan, likes, rdfs));
//			}
//
//			// final QueryOptions opts = new QueryOptions();
//			// opts.serviceURL = m_serviceURL;
//			// opts.queryStr = deleteQueryStr;
//			// opts.method = "GET";
//			// opts.acceptHeader = TupleQueryResultFormat.SPARQL
//			// .getDefaultMIMEType();
//			//
//			// assertSameGraph(expected, buildGraph(doSparqlQuery(opts,
//			// requestPath)));
//
//         assertSameGraph(expected, m_repo.prepareGraphQuery(deleteQueryStr));
//
//		}
//
//		/*
//		 * Setup the document containing the statement to be inserted by the
//		 * UPDATE operation.
//		 */
//		final byte[] data;
//		{
//			final Graph g = new LinkedHashModel();
//
//			// The new data.
//			g.add(new StatementImpl(bryan, likes, rdf));
//
//			final RDFWriterFactory writerFactory = RDFWriterRegistry
//					.getInstance().get(format);
//			if (writerFactory == null)
//				fail("RDFWriterFactory not found: format=" + format);
//			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
//			final RDFWriter writer = writerFactory.getWriter(baos);
//			writer.startRDF();
//			for (Statement stmt : g) {
//				writer.handleStatement(stmt);
//			}
//			writer.endRDF();
//			data = baos.toByteArray();
//		}
//
//		/*
//		 * Now, run the UPDATE operation.
//		 */
//		{
//
//			final RemoveOp remove = new RemoveOp(deleteQueryStr);
//			final AddOp add = new AddOp(data, format);
//			// Note: 1 removed, but also 1 added.
//			assertEquals(2, m_repo.update(remove, add));
//
//		}
//
//		/*
//		 * Now verify the post-condition state.
//		 */
//		{
//
//			/*
//			 * This query verifies that we removed the right triple (nobody is
//			 * left who likes 'rdfs').
//			 */
//			{
//
//				final String queryStr2 = //
//				"prefix bd: <" + BD.NAMESPACE + "> " + //
//						"prefix rdf: <" + RDF.NAMESPACE + "> " + //
//						"prefix rdfs: <" + RDFS.NAMESPACE + "> " + //
//						"CONSTRUCT { ?x bd:likes bd:RDFS }" + //
//						"WHERE { " + //
//						// "  ?x rdf:type bd:Person . " + //
//						"  ?x bd:likes bd:RDFS " + // NB: Checks the kb!
//						"}";
//
//				// The expected results.
//				final Graph expected = new LinkedHashModel();
//
//				assertSameGraph(expected, m_repo.prepareGraphQuery(queryStr2));
//
//			}
//
//			/*
//			 * This query verifies that we added the right triple (two people
//			 * now like 'rdf').
//			 */
//			{
//
//				final String queryStr2 = //
//				"prefix bd: <" + BD.NAMESPACE + "> " + //
//						"prefix rdf: <" + RDF.NAMESPACE + "> " + //
//						"prefix rdfs: <" + RDFS.NAMESPACE + "> " + //
//						"CONSTRUCT { ?x bd:likes bd:RDF }" + //
//						"WHERE { " + //
//						// "  ?x rdf:type bd:Person . " + //
//						"  ?x bd:likes bd:RDF " + //
//						"}";
//
//				// The expected results.
//				final Graph expected = new LinkedHashModel();
//
//				expected.add(new StatementImpl(mike, likes, rdf));
//				expected.add(new StatementImpl(bryan, likes, rdf));
//
//				assertSameGraph(expected, m_repo.prepareGraphQuery(queryStr2));
//
//			}
//
//		}
//
//	}
//	
//	// /**
//	// * Unit test for ACID UPDATE using PUT. This test is for the operation
//	// where
//	// * the request body is a multi-part MIME document conveying both the
//	// * statements to be removed and the statement to be inserted.
//	// */
//	// public void test_PUT_UPDATE_WITH_MULTI_PART_MIME() {
//	// fail("write test");
//	// }
//	
//
    public void testServiceNodeBindings() throws Exception {
        final BigdataSailRemoteRepository repo = m_repo.getBigdataSailRemoteRepository();
        final BigdataSailRemoteRepositoryConnection cxn = 
            (BigdataSailRemoteRepositoryConnection) repo.getConnection();
        
        try {
          String queryStr = "select * where {SERVICE <http://DBpedia.org/sparql> { <http://dbpedia.org/resource/Tonga_(Nyasa)_language> rdfs:label ?langLabel. }}";
//                String queryStr = "SELECT * WHERE { BIND (<http://dbpedia.org/resource/Tonga_(Nyasa)_language> AS ?ref) . SERVICE <http://DBpedia.org/sparql> { ?ref rdfs:label ?langLabel. } }";
            final org.openrdf.query.TupleQuery tq = cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
            final TupleQueryResult tqr = tq.evaluate();
            try {
                int cnt = 0;
                while (tqr.hasNext()) {
                    tqr.next();
                    cnt++;
                }
                assertEquals(cnt, 2);
            } finally {
                tqr.close();
            }
        } finally {
            cxn.close();
        }
    }
}
