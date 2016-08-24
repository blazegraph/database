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
/*
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sail.tck;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.parser.sparql.SPARQLUpdateTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailUpdate;

/**
 * Integration with the openrdf SPARQL 1.1 update test suite.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/531" > SPARQL
 *      UPDATE for NAMED SOLUTION SETS </a>
 *      
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSPARQLUpdateTest extends SPARQLUpdateTest {

    static private final Logger logger = Logger
            .getLogger(BigdataSPARQLUpdateTest.class);

    public BigdataSPARQLUpdateTest() {
    }

//    public BigdataSparqlUpdateTest(String name) {
//        super(name);
//    }
    
    /**
     * Note: This field MUST be cleared in tearDown or a hard reference will be
     * retained to the backend until the end of CI!
     */
    private IIndexManager backend = null;

    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    public void tearDown()
        throws Exception
    {

        super.tearDown();

        if (backend != null)
            tearDownBackend(backend);

        /*
         * Note: this field MUST be cleared to null or the backing database
         * instance will be held by a hard reference throughout the execution of
         * all unit tests in this test suite!
         */

        backend = null;
    
    }

    protected void tearDownBackend(final IIndexManager backend) {
        
        backend.destroy();
        
    }

    /**
     * Note: This method may be overridden in order to run the test suite
     * against other variations of the bigdata backend.
     */
    protected Properties getProperties() {

        final Properties props = new Properties();
        
//        final File journal = BigdataStoreTest.createTempFile();
//        
//        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());

        props.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        
        // quads mode: quads=true, sids=false, axioms=NoAxioms, vocab=NoVocabulary
        props.setProperty(Options.QUADS_MODE, "true");

        // no justifications
        props.setProperty(Options.JUSTIFY, "false");
        
        // no query time inference
        props.setProperty(Options.QUERY_TIME_EXPANDER, "false");
        
//        // auto-commit only there for TCK
//        props.setProperty(Options.ALLOW_AUTO_COMMIT, "true");
        
        // exact size only the for TCK
        props.setProperty(Options.EXACT_SIZE, "true");
        
//        props.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());
        
//      Force identical unicode comparisons (assuming default COLLATOR setting).
//        props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
        
        /*
         * disable read/write transactions since this class runs against the
         * unisolated connection.
         */
        props.setProperty(Options.ISOLATABLE_INDICES, "false");
        
        // disable truth maintenance in the SAIL
        props.setProperty(Options.TRUTH_MAINTENANCE, "false");
        
        props.setProperty(Options.TEXT_INDEX, "true");
        
        // make sure we are using a unique namespace in every run to avoid 
        // possible interference of value factories floating around
        props.setProperty(Options.NAMESPACE, "BigdataSPARQLUpdate-"+UUID.randomUUID());
        
        return props;
        
    }

    @Override
    protected Repository createRepository() throws Exception {
        Repository repo = newRepository();
        repo.initialize();
        return repo;
    }

    @Override
    protected Repository newRepository() throws RepositoryException {

        final Properties props = getProperties();
        
        final BigdataSail sail = new BigdataSail(props);
        
        backend = sail.getIndexManager();

        return new BigdataSailRepository(sail);

//        if (true) {
//            final Properties props = getProperties();
//            
//            if (cannotInlineTests.contains(testURI)){
//                // The test can not be run using XSD inlining.
//                props.setProperty(Options.INLINE_XSD_DATATYPE_LITERALS, "false");
//                props.setProperty(Options.INLINE_DATE_TIMES, "false");
//            }
//            
//            if(unicodeStrengthIdentical.contains(testURI)) {
//                // Force identical Unicode comparisons.
//                props.setProperty(Options.COLLATOR, CollatorEnum.JDK.toString());
//                props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
//            }
//            
//            final BigdataSail sail = new BigdataSail(props);
//            return new DatasetRepository(new BigdataSailRepository(sail));
//        } else {
//            return new DatasetRepository(new SailRepository(new MemoryStore()));
//        }

    }

    /**
     * Note: Overridden to turn off autocommit and commit after the data are
     * loaded.
     */
    @Override
    protected void loadDataset(String datasetFile)
            throws RDFParseException, RepositoryException, IOException
        {
            logger.debug("loading dataset...");
            InputStream dataset = SPARQLUpdateTest.class.getResourceAsStream(datasetFile);
            try {
//                con.setAutoCommit(false);
                con.add(dataset, "", RDFFormat.forFileName(datasetFile));//RDFFormat.TRIG);
                con.commit();
            }
            finally {
                dataset.close();
            }
            logger.debug("dataset loaded.");
        }

    /**
	 * Unit test for isolation semantics for a sequences of updates.
	 * 
	 * @throws UpdateExecutionException
	 * @throws MalformedQueryException
	 * @throws RepositoryException
     * @throws QueryEvaluationException 
	 * 
	 * @see https://sourceforge.net/apps/trac/bigdata/ticket/558
	 */
	public void test_ticket538() throws UpdateExecutionException,
			RepositoryException, MalformedQueryException, QueryEvaluationException {

		// the [in] and [out] graphs.
        final URI gin = f.createURI("http://example/in");
        final URI gout = f.createURI("http://example/out");

//		((BigdataSailRepository) con.getRepository()).getDatabase().addTerms(
//				new BigdataValue[] { (BigdataValue) gin, (BigdataValue) gout });

//		// Make sure the target graphs are empty.
//		{
//			final String s = "# Update 1\n"
//					+ "DROP SILENT GRAPH <http://example/in>;\n"
//					+ "DROP SILENT GRAPH <http://example/out> ;\n";
//
//			con.prepareUpdate(QueryLanguage.SPARQL, s).execute();
//		}

		con.prepareUpdate(QueryLanguage.SPARQL, "DROP SILENT ALL").execute();
        // Make sure the graphs are empty.
        assertFalse(con.hasStatement(null,null,null, true, (Resource)gin));
        assertFalse(con.hasStatement(null,null,null, true, (Resource)gout));

		/*
		 * The mutation.
		 * 
		 * A statement is inserted into the [in] graph in one operation. Then
		 * all statements in the [in] graph are copied into the [out] graph.
		 */
		final String s = "# Update 2\n"//
				+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"//
				+ "INSERT DATA {\n"//
				+ "  GRAPH <http://example/in> {\n"//
				+ "        <http://example/president25> foaf:givenName \"William\" .\n"//
				+ "  }\n"//
				+ "};\n"//
				+ "INSERT {\n"//
				+ "   GRAPH <http://example/out> {\n"//
				+ "       ?s ?p ?v .\n"//
				+ "        }\n"//
				+ "    }\n"//
				+ "WHERE {\n"//
				+ "   GRAPH <http://example/in> {\n"//
				+ "       ?s ?p ?v .\n"//
				+ "        }\n"//
				+ "  }\n"//
				+ ";";

//		// The query - how many statements are in the [out] graph.
//		final String q = "# Query\n"//
//				+ "SELECT (COUNT(*) as ?cnt) {\n"//
//				+ "    GRAPH <http://example/out> {\n"//
//				+ "        ?s ?p ?v .\n"//
//				+ "  }\n"//
//				+ "} LIMIT 10";//

		// run update once.
        final BigdataSailUpdate update = (BigdataSailUpdate) con.prepareUpdate(QueryLanguage.SPARQL, s);
        update.execute();

		// Both graphs should now have some data.
        assertTrue(con.hasStatement(null,null,null, true, (Resource)gin));

		// Note: Succeeds if you do this a 2nd time.
		if (false)
			con.prepareUpdate(QueryLanguage.SPARQL, s).execute();

		assertTrue(con.hasStatement(null,null,null, true, (Resource)gout));

	}

    /**
     * Unit test for
     * 
     * <pre>
     * DROP ALL;
     * INSERT DATA {
     * GRAPH <http://example.org/one> {
     * <a> <b> <c> .
     * <d> <e> <f> .
     * }};
     * ADD SILENT GRAPH <http://example.org/one> TO GRAPH <http://example.org/two> ;
     * DROP SILENT GRAPH <http://example.org/one>  ;
     * </pre>
     * 
     * The IV cache was not not being propagated correctly with the result that
     * we were seeing mock IVs for "one" and "two". The UPDATE would work
     * correctly the 2nd time since the URIs had been entered into the
     * dictionary by then.
     * 
     * @throws RepositoryException 
     * @throws MalformedQueryException 
     * @throws UpdateExecutionException 
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/567" >
     *      Failure to set cached value on IV results in incorrect behavior for
     *      complex UPDATE operation </a>
     */
    public void testTicket567() throws RepositoryException, MalformedQueryException, UpdateExecutionException {

        // replace the standard dataset with one specific to this case.
        con.clear();
        con.commit();

        final StringBuilder update = new StringBuilder();
        update.append("DROP ALL;\n");
        update.append("INSERT DATA {\n");
        update.append(" GRAPH <http://example.org/one> {\n");
        update.append("   <http://example.org/a> <http://example.org/b> <http://example.org/c> .\n");
        update.append("   <http://example.org/d> <http://example.org/e> <http://example.org/f> .\n");
        update.append("}};\n");
        update.append("ADD SILENT GRAPH <http://example.org/one> TO GRAPH <http://example.org/two> ;\n");
        update.append("DROP SILENT GRAPH <http://example.org/one>  ;\n");
        
        final Update operation = con.prepareUpdate(QueryLanguage.SPARQL, update.toString());

        operation.execute();

        final URI one = f.createURI("http://example.org/one");
        final URI two = f.createURI("http://example.org/two");

        String msg = "Nothing in graph <one>";
        assertFalse(msg, con.hasStatement(null, null, null, true, one));

        msg = "statements are in graph <two>";
        assertTrue(msg, con.hasStatement(null, null, null, true, two));
        
    }
    
    /**
     * In the ticket, the source file contains the following triples. In this
     * test, those are triples are written into the graph using INSERT DATA
     * rather than LOAD so this test can be self-contained.
     * 
     * <pre>
     * _:bnode <http://example/p> 2 .
     * _:bnode a <http://example/Foo> .
     * <http://example/s> <http://example/p> 2 .
     * </pre>
     * 
     * Load into graphA:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * DROP SILENT GRAPH tempGraph: ;
     * DROP SILENT GRAPH graphA: ;
     * INSERT DATA {
     * GRAPH graphA: {
     *   _:bnode <http://example/p> 2 . 
     *   _:bnode a <http://example/Foo> . 
     *   <http://example/s> <http://example/p> 2 . 
     * }}
     * </pre>
     * 
     * Verify that all three triples are in graphA:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * SELECT * WHERE { GRAPH graphA: { ?s ?p ?v . } }
     * </pre>
     * 
     * Now delete some triples from graphA while inserting the deleted triples
     * into tempGraph:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * DROP SILENT GRAPH tempGraph: ;
     * DELETE { GRAPH graphA:    { ?s ?p ?v . } }
     * INSERT { GRAPH tempGraph: { ?s ?p ?v . } }
     * WHERE { GRAPH graphA: { 
     *     ?s a <http://example/Foo> .
     *     ?s ?p ?v . } }
     * </pre>
     * 
     * The result should be that the combination of tempGraph and graphA should
     * have the exact same number of triples that we started with. But now
     * notice that graphA has 0 triples:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * SELECT * WHERE { GRAPH graphA: { ?s ?p ?v . } }
     * </pre>
     * 
     * However, tempGraph has only 2 triples:
     * 
     * <pre>
     * PREFIX graphA:  <http://example/graphA>
     * PREFIX tempGraph:  <http://example/temp>
     * SELECT * WHERE { GRAPH tempGraph: { ?s ?p ?v . } }
     * </pre>
     * 
     * so one triple is missing:
     * 
     * <pre>
     * <http://example/s> <http://example/p> 2 .
     * </pre>
     * 
     * @throws QueryEvaluationException
     */
    public void testTicket571() throws RepositoryException,
            MalformedQueryException, UpdateExecutionException,
            QueryEvaluationException {
        
//        final URI graphA = f.createURI("http://example.org/graphA");
//        final URI tempGraph = f.createURI("http://example.org/tmp");
//        final URI s = f.createURI("http://example/s>");
//        final URI p = f.createURI("http://example/p>");
//        final URI x = f.createURI("http://example/x>");
//        final URI foo = f.createURI("http://example/Foo>");
//        final URI rdfType = f.createURI(RDF.TYPE.stringValue());
//        final Literal two = f.createLiteral("2", XSD.INTEGER);

        // replace the standard dataset with one specific to this case.
        con.prepareUpdate(QueryLanguage.SPARQL, "DROP ALL").execute();

//        System.err.println("##### INITIAL DATA IN DATABASE");
//        debugPrintSolutions("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }");
        
        /**
         * Load into graphA (note: file is "file:///tmp/junk.ttl" in the
         * ticket).
         * 
         * <pre>
         * PREFIX graphA:  <http://example/graphA>
//         * PREFIX tempGraph:  <http://example/temp>
//         * DROP SILENT GRAPH tempGraph: ;
//         * DROP SILENT GRAPH graphA: ;
         * LOAD <file:///tmp/junk.ttl> INTO GRAPH graphA: ;
         * </pre>
         */
        con.prepareUpdate(//
                QueryLanguage.SPARQL,//
                "PREFIX graphA:  <http://example/graphA> \n" + //
//                "PREFIX tempGraph:  <http://example/temp> \n"+//
//                "DROP SILENT GRAPH tempGraph: ;\n"+//
//                "DROP SILENT GRAPH graphA: ;\n"+//
                "INSERT DATA { \n"+//
                " GRAPH graphA: { \n" +//
                "   _:bnode <http://example/p> 2 . \n"+//
                "   _:bnode a <http://example/Foo> . \n"+//
                "   <http://example/s> <http://example/p> 2 . \n"+//
                "}}\n"//
//                "LOAD <file:bigdata-sails/src/test/org/openrdf/query/parser/sparql/ticket571.ttl> INTO GRAPH graphA: ;\n"//
        ).execute();
        
//        debugPrintSolutions("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }");

        
        /**
         * Verify that all three triples are in graphA:
         * 
         * <pre>
         * SELECT * WHERE { GRAPH <http://example/graphA> { ?s ?p ?v . } }
         * </pre>
         */
        {

            final String query = "SELECT * WHERE { GRAPH <http://example/graphA> { ?s ?p ?v . } }";

            assertEquals("graphA", 3L, countSolutions(query));
            
        }
        
        /**
         * Now delete some triples from graphA while inserting the deleted
         * triples into tempGraph:
         * 
         * <pre>
         * PREFIX graphA:  <http://example/graphA>
         * PREFIX tempGraph:  <http://example/temp>
         * DROP SILENT GRAPH tempGraph: ;  ### Note: redundant
         * DELETE { GRAPH graphA:    { ?s ?p ?v . } } 
         * INSERT { GRAPH tempGraph: { ?s ?p ?v . } }
         * WHERE { GRAPH graphA: { 
         *     ?s a <http://example/Foo> .
         *     ?s ?p ?v . } }
         * </pre>
         */
        con.prepareUpdate(//
                QueryLanguage.SPARQL, //
                "PREFIX graphA:  <http://example/graphA> \n" + //
                "PREFIX tempGraph:  <http://example/temp> \n" +//
//                "DROP SILENT GRAPH tempGraph: ;\n"+//
                "DELETE { GRAPH graphA:    { ?s ?p ?v . } } \n"+//
                "INSERT { GRAPH tempGraph: { ?s ?p ?v . } } \n"+//
                "WHERE { GRAPH graphA: { \n"+//
                "    ?s a <http://example/Foo> . \n"+//
                "    ?s ?p ?v . } }\n"//
        ).execute();

//        System.err.println("##### DATA IN DATABASE AFTER DELETE + INSERT");
//        debugPrintSolutions("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }");

        
        /**
         * graphA should have one triple remaining:
         * 
         * <pre>
         * <http://example/s> <http://example/p> 2 .
         * </pre>
         */
        {

            final String query = "SELECT * WHERE { GRAPH <http://example/graphA> { ?s ?p ?v . } }";

            assertEquals("graphA", 1L, countSolutions(query));
            
        }

        /**
         * The other 2 triples should have been moved into tempGraph.
         */
        {

            final String query = "SELECT * WHERE { GRAPH <http://example/temp> { ?s ?p ?v . } }";

            assertEquals("tempGraph", 2L, countSolutions(query));
            
        }

    }
    
    
    /**
     * Variant of test 571 without blank nodes.
     * 
     * @throws RepositoryException
     * @throws MalformedQueryException
     * @throws UpdateExecutionException
     * @throws QueryEvaluationException
     */
    public void testTicket571b() throws RepositoryException,
            MalformedQueryException, UpdateExecutionException,
            QueryEvaluationException {

//        final URI graphA = f.createURI("http://example.org/graphA");
//        final URI tempGraph = f.createURI("http://example.org/tmp");
//        final URI s = f.createURI("http://example/s>");
//        final URI p = f.createURI("http://example/p>");
//        final URI x = f.createURI("http://example/x>");
//        final URI foo = f.createURI("http://example/Foo>");
//        final URI rdfType = f.createURI(RDF.TYPE.stringValue());
//        final Literal two = f.createLiteral("2", XSD.INTEGER);

        // replace the standard dataset with one specific to this case.
        con.prepareUpdate(QueryLanguage.SPARQL, "DROP ALL").execute();

//        System.err.println("##### INITIAL DATA IN DATABASE");
//        debugPrintSolutions("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }");

        /**
         * Load into graphA (note: file is "file:///tmp/junk.ttl" in the
         * ticket).
         * 
         * <pre>
         * PREFIX graphA:  <http://example/graphA>
         * // * PREFIX tempGraph:  <http://example/temp>
         * // * DROP SILENT GRAPH tempGraph: ;
         * // * DROP SILENT GRAPH graphA: ;
         * LOAD <file:///tmp/junk.ttl> INTO GRAPH graphA: ;
         * </pre>
         */
        con.prepareUpdate(//
                QueryLanguage.SPARQL,//
                "PREFIX graphA:  <http://example/graphA> \n" + //
                        // "PREFIX tempGraph:  <http://example/temp> \n"+//
                        // "DROP SILENT GRAPH tempGraph: ;\n"+//
                        // "DROP SILENT GRAPH graphA: ;\n"+//
                        "INSERT DATA { \n" + //
                        " GRAPH graphA: { \n" + //
                        "   <http://nobnode> <http://example/p> 2 . \n" + //
                        "   <http://nobnode> a <http://example/Foo> . \n" + //
                        "   <http://example/s> <http://example/p> 2 . \n" + //
                        "}}\n"//
                        // "LOAD <file:bigdata-sails/src/test/org/openrdf/query/parser/sparql/ticket571.ttl> INTO GRAPH graphA: ;\n"//
        ).execute();

//        System.err.println("##### DATA IN DATABASE AFTER INSERT");
//        debugPrintSolutions("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }");

        /**
         * Verify that all three triples are in graphA:
         * 
         * <pre>
         * SELECT * WHERE { GRAPH <http://example/graphA> { ?s ?p ?v . } }
         * </pre>
         */
        {

            final String query = "SELECT * WHERE { GRAPH <http://example/graphA> { ?s ?p ?v . } }";

            assertEquals("graphA", 3L, countSolutions(query));

        }

        /**
         * Now delete some triples from graphA while inserting the deleted
         * triples into tempGraph:
         * 
         * <pre>
         * PREFIX graphA:  <http://example/graphA>
         * PREFIX tempGraph:  <http://example/temp>
         * DROP SILENT GRAPH tempGraph: ;  ### Note: redundant
         * DELETE { GRAPH graphA:    { ?s ?p ?v . } } 
         * INSERT { GRAPH tempGraph: { ?s ?p ?v . } }
         * WHERE { GRAPH graphA: { 
         *     ?s a <http://example/Foo> .
         *     ?s ?p ?v . } }
         * </pre>
         */
        con.prepareUpdate(//
                QueryLanguage.SPARQL, //
                "PREFIX graphA:  <http://example/graphA> \n" + //
                        "PREFIX tempGraph:  <http://example/temp> \n" + //
                        // "DROP SILENT GRAPH tempGraph: ;\n"+//
                        "DELETE { GRAPH graphA:    { ?s ?p ?v . } } \n" + //
                        "INSERT { GRAPH tempGraph: { ?s ?p ?v . } } \n" + //
                        "WHERE { GRAPH graphA: { \n" + //
                        "    ?s a <http://example/Foo> . \n" + //
                        "    ?s ?p ?v . } }\n"//
        ).execute();

//        System.err.println("##### DATA IN DATABASE AFTER DELETE + INSERT");
//        debugPrintSolutions("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }");

        /**
         * graphA should have one triple remaining:
         * 
         * <pre>
         * <http://example/s> <http://example/p> 2 .
         * </pre>
         */
        {

            final String query = "SELECT * WHERE { GRAPH <http://example/graphA> { ?s ?p ?v . } }";

            assertEquals("graphA", 1L, countSolutions(query));

        }

        /**
         * The other 2 triples should have been moved into tempGraph.
         */
        {

            final String query = "SELECT * WHERE { GRAPH <http://example/temp> { ?s ?p ?v . } }";

            assertEquals("tempGraph", 2L, countSolutions(query));

        }

    }
    
    
    
    /**
     * This test is based on a forum post. This post provided an example of an
     * issue with Unicode case-folding in the REGEX operator and a means to
     * encode the Unicode characters to avoid doubt about which characters were
     * transmitted and receieved.
     * 
     * @throws Exception 
     * 
     * @see <a href=
     *      "https://sourceforge.net/projects/bigdata/forums/forum/676946/topic/7073971"
     *      >Forum post on the REGEX Unicode case-folding issue</a>
     *      
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/655">
     *      SPARQL REGEX operator does not perform case-folding correctly for
     *      Unicode data</a>
     */
    public void testUnicodeCleanAndRegex() throws Exception {

        /*
         * If I work around this problem by switching the unencoded Unicode
         * characters into SPARQL escape sequences like this:
         */

        // Insert statement:
        final String updateStr = "PREFIX ns: <http://example.org/ns#>\n"
                + "INSERT DATA { GRAPH ns:graph { ns:auml ns:label \"\u00C4\", \"\u00E4\" } }\n";

        final BigdataSailUpdate update = (BigdataSailUpdate)
                con.prepareUpdate(QueryLanguage.SPARQL, updateStr);
        update.execute();

        // Test query:
        final String queryStr = "PREFIX ns: <http://example.org/ns#>\n"
                + "SELECT * { GRAPH ns:graph { ?s ?p ?o FILTER(regex(?o, \"\u00E4\", \"i\")) } }";

        assertEquals(2L, countSolutions(queryStr));

        /*
         * Then I still get only one result for the query, the triple with ''
         * which is \u00E4. But if I now add the 'u' flag to the regex, I get
         * both triples as result, so this seems to be a viable workaround.
         * Always setting the UNICODE_CASE flag sounds like a good idea, and in
         * fact, Jena ARQ seems to do that when given the 'i' flag:
         */
        
    }

    /**
     * Verify ability to load data from a gzip resource.
     */
    public void testLoadGZip()
            throws Exception
        {
    
    	//Depends on bigdata-rdf test resource
    	final URL url = this.getClass().getClassLoader().getResource("com/bigdata/rdf/rio/small.rdf.gz");
    	
        final String update = "LOAD <" + url.toExternalForm() + ">";
        
        final String ns = "http://bigdata.com/test/data#";
        
        con.prepareUpdate(QueryLanguage.SPARQL, update).execute();

        assertTrue(con.hasStatement(f.createURI(ns, "mike"), RDFS.LABEL,
                f.createLiteral("Michael Personick"), true));

    }
    
    /**
     * Count solutions for a TupleQuery.
     * 
     * @param query
     *            The SPARQL query.
     * @return The #of solutions.
     * @throws QueryEvaluationException
     * @throws RepositoryException
     * @throws MalformedQueryException
     */
    protected long countSolutions(final String query)
            throws QueryEvaluationException, RepositoryException,
            MalformedQueryException {
        TupleQueryResult result = con.prepareTupleQuery(QueryLanguage.SPARQL,
                query).evaluate();
        try {
            long n = 0;
            while (result.hasNext()) {
                final BindingSet bset = result.next();
                n++;
                if (logger.isInfoEnabled())
                    logger.info(bset.toString());
            }
            return n;
        } finally {
            result.close();
        }
    }
    
//    private long debugPrintSolutions(final String query)
//            throws QueryEvaluationException, RepositoryException,
//            MalformedQueryException {
//        TupleQueryResult result = con.prepareTupleQuery(QueryLanguage.SPARQL,
//                query).evaluate();
//        try {
//            long n = 0;
//            while (result.hasNext()) {
//                System.err.println("==> NEXT SOLUTION");
//                final BindingSet bset = result.next();
//                for (final String bindingName : bset.getBindingNames()) {
//                    final Binding b = bset.getBinding(bindingName);
//                    System.err.println(bindingName + " -> " + b);
//                }
//                n++;
//            }
//            return n;
//        } finally {
//            result.close();
//        }
//    }

    
    /**
     * SPARQL update test for literals with language tags, cf. ticket #1073.
     */
    public void testUpdateLiteralsWithLanguageTags() throws Exception {

        final String queryStr = "SELECT * WHERE { ?s ?p ?o }";
    	long nrTriplesBeforeUpdate = countSolutions(queryStr);
    	
        // Insert statement:
    	StringBuilder updateStrBuf = new StringBuilder();
    	updateStrBuf.append("INSERT DATA { ");
    	updateStrBuf.append("<http://rm-lod.org/object/2176/production/date> ");
    	updateStrBuf.append("<http://www.w3.org/2000/01/rdf-schema#label> ");
    	updateStrBuf.append("\"1906\"@ru . }");

    	String updateStr = updateStrBuf.toString();
    	
        final BigdataSailUpdate update = (BigdataSailUpdate)
                con.prepareUpdate(QueryLanguage.SPARQL, updateStr);
        update.execute();

        // Test query:
    	long nrTriplesAfterUpdate = countSolutions(queryStr);

        assertEquals(nrTriplesBeforeUpdate + 1, nrTriplesAfterUpdate);
    }
}
