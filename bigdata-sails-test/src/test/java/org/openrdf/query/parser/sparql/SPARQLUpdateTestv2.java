/* 
 * Licensed to Aduna under one or more contributor license agreements.  
 * See the NOTICE.txt file distributed with this work for additional 
 * information regarding copyright ownership. 
 *
 * Aduna licenses this file to you under the terms of the Aduna BSD 
 * License (the "License"); you may not use this file except in compliance 
 * with the License. See the LICENSE.txt file distributed with this work 
 * for the full License.
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
/*
 * Pulled in to extend TestCase.
 */
package org.openrdf.query.parser.sparql;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.BD;

import junit.framework.TestCase;

/**
 * Tests for SPARQL 1.1 Update functionality.
 * 
 * @author Jeen Broekstra
 */
public class SPARQLUpdateTestv2 extends TestCase {

	protected static final Logger logger = LoggerFactory.getLogger(SPARQLUpdateTestv2.class);

	private Repository rep;

	protected RepositoryConnection con;

	protected ValueFactory f;

	protected URI bob;

	protected URI alice;

	protected URI graph1;

	protected URI graph2;

	protected static final String EX_NS = "http://example.org/";
	
    /**
     * Note: This field MUST be cleared in tearDown or a hard reference will be
     * retained to the backend until the end of CI!
     */
    private IIndexManager backend = null;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp()
		throws Exception
	{
		logger.debug("setting up test");

		rep = createRepository();
		con = rep.getConnection();
		f = rep.getValueFactory();

		loadDataset("/testdata-update/dataset-update.trig");

		bob = f.createURI(EX_NS, "bob");
		alice = f.createURI(EX_NS, "alice");

		graph1 = f.createURI(EX_NS, "graph1");
		graph2 = f.createURI(EX_NS, "graph2");

		logger.debug("setup complete.");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown()
		throws Exception
	{
		logger.debug("tearing down...");
		con.close();
		con = null;

		rep.shutDown();
		rep = null;

	      
        if (backend != null)
            tearDownBackend(backend);

        /*
         * Note: this field MUST be cleared to null or the backing database
         * instance will be held by a hard reference throughout the execution of
         * all unit tests in this test suite!
         */

        backend = null;
        
		logger.debug("tearDown complete.");

	}
	
    protected void tearDownBackend(final IIndexManager backend) {
        
        backend.destroy();
        
    }
    


    protected Repository createRepository() throws Exception {
        Repository repo = newRepository();
        repo.initialize();
        return repo;
    }

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
	 * Get a set of useful namespace prefix declarations.
	 * 
	 * @return namespace prefix declarations for rdf, rdfs, dc, foaf and ex.
	 */
	protected String getNamespaceDeclarations() {
		StringBuilder declarations = new StringBuilder();
		declarations.append("PREFIX rdf: <" + RDF.NAMESPACE + "> \n");
		declarations.append("PREFIX rdfs: <" + RDFS.NAMESPACE + "> \n");
		declarations.append("PREFIX dc: <" + DC.NAMESPACE + "> \n");
		declarations.append("PREFIX foaf: <" + FOAF.NAMESPACE + "> \n");
		declarations.append("PREFIX ex: <" + EX_NS + "> \n");
		declarations.append("PREFIX xsd: <" + XMLSchema.NAMESPACE + "> \n");
        declarations.append("PREFIX bd: <" + BD.NAMESPACE + "> \n");
		declarations.append("\n");

		return declarations.toString();
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

    public void testTicket571Standalone() throws RepositoryException,
            MalformedQueryException, UpdateExecutionException,
            QueryEvaluationException {

        final URI graphA = f.createURI("http://example.org/graphA");
        final URI tempGraph = f.createURI("http://example.org/tmp");
        final URI s = f.createURI("http://example/s>");
        final URI p = f.createURI("http://example/p>");
        final URI x = f.createURI("http://example/x>");
        final URI foo = f.createURI("http://example/Foo>");
        final URI rdfType = f.createURI(RDF.TYPE.stringValue());
        final Literal two = f.createLiteral("2", XSD.INTEGER);

        // replace the standard dataset with one specific to this case.
        con.prepareUpdate(QueryLanguage.SPARQL, "DROP ALL").execute();

        System.err.println("##### INITIAL DATA IN DATABASE");
        debugPrintSolutions("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }");

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
                        "   _:bnode <http://example/p> 2 . \n" + //
                        "   _:bnode a <http://example/Foo> . \n" + //
                        "   <http://example/s> <http://example/p> 2 . \n" + //
                        "}}\n"//
                        // "LOAD <file:bigdata-sails/src/test/org/openrdf/query/parser/sparql/ticket571.ttl> INTO GRAPH graphA: ;\n"//
        ).execute();

        System.err.println("##### DATA IN DATABASE AFTER INSERT");
        debugPrintSolutions("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }");

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

        System.err.println("##### DATA IN DATABASE AFTER DELETE + INSERT");
        debugPrintSolutions("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }");

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
    
    protected long debugPrintSolutions(final String query)
            throws QueryEvaluationException, RepositoryException,
            MalformedQueryException {
        TupleQueryResult result = con.prepareTupleQuery(QueryLanguage.SPARQL,
                query).evaluate();
        try {
            long n = 0;
            while (result.hasNext()) {
                System.err.println("==> NEXT SOLUTION");
                final BindingSet bset = result.next();
                for (final String bindingName : bset.getBindingNames()) {
                    final Binding b = bset.getBinding(bindingName);
                    System.err.println(bindingName + " -> " + b);
                }
                n++;
            }
            return n;
        } finally {
            result.close();
        }
    }
}
