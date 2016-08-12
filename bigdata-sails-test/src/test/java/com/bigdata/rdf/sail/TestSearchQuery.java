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
 * Created on Apr 15, 2008
 */

package com.bigdata.rdf.sail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.helpers.StatementCollector;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.ITextIndexer;
import com.bigdata.rdf.lexicon.ITextIndexer.FullTextQuery;
import com.bigdata.rdf.lexicon.IValueCentricTextIndexer;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.BDS;
import com.bigdata.search.Hiterator;
import com.bigdata.search.IHit;
import com.bigdata.search.TestSearch;

/**
 * Test suite for high-level query against a graph containing statements about
 * statements.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestSearchQuery extends ProxyBigdataSailTestCase {

    private static final Logger log = Logger.getLogger(TestSearchQuery.class);
	
    public TestSearchQuery() {
        
    }
    
    public TestSearchQuery(String name) {
        super(name);
    }
    
    /**
     * Overridden to allow the subject-centric full text index.
     */
    @Override
    public Properties getProperties() {
        
    	final Properties properties = super.getProperties();
        
        properties.setProperty(Options.SUBJECT_CENTRIC_TEXT_INDEX, "false");
        
        return properties;
        
    }

//    /**
//     * Overriden to cause the backing store to be deleted.
//     */
//    protected void tearDown() throws Exception {
//    
//        if (sail != null) {
//
//            sail.getDatabase().closeAndDelete();
//
//        }
//        
//    }
    
//    public void test_query() throws SailException, IOException,
//            RDFHandlerException, QueryEvaluationException {
//
//        // overridden to use a disk-backed file.
//        final Properties properties = super.getProperties();
//
//        // use a disk-based mode since we will re-open the store to test restart
//        // safety.
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
//
//        properties.setProperty(Options.CREATE_TEMP_FILE, "false");
//
//        properties.setProperty(Options.FILE, file.toString());
//
//        BigdataSail sail = getSail(properties);
//
//        try {
//
//            sail.initialize();
//            
////            if (!sail.database.getStatementIdentifiers()) {
////
////                log.warn("Statement identifiers are not enabled");
////
////                return;
////
////            }
//
//            /*
//             * Load data into the sail.
//             */
//            {
//
//                final StatementBuffer<Statement> sb = new StatementBuffer<Statement>(
//                        sail.database, 100/* capacity */);
//
//                sb.add(new URIImpl("http://www.bigdata.com/A"), RDFS.LABEL,
//                        new LiteralImpl("Yellow Rose"));
//
//                sb.add(new URIImpl("http://www.bigdata.com/B"), RDFS.LABEL,
//                        new LiteralImpl("Red Rose"));
//
//                sb.add(new URIImpl("http://www.bigdata.com/C"), RDFS.LABEL,
//                        new LiteralImpl("Old Yellow House"));
//
//                sb.flush();
//
//                /*
//                 * Commit the changes to the database.
//                 */
//                sail.getDatabase().commit();
//
//            }
//
//            if (log.isInfoEnabled())
//                log
//                        .info("#statements before search: "
//                                + sail.database.getStatementCount(null/* c */,
//                                        true/* exact */));
//
//            doSearchTest(((BigdataSail) sail).getConnection());
//
//            doSearchTest(((BigdataSail) sail).getReadOnlyConnection());
//
//            if (log.isInfoEnabled())
//                log
//                        .info("#statements before restart: "
//                                + sail.database.getStatementCount(null/* c */,
//                                        true/* exact */));
//
//            // re-open the SAIL.
//            sail = reopenSail(sail);
//
//            sail.initialize();
//
//            if (log.isInfoEnabled())
//                log
//                        .info("#statements after restart: "
//                                + sail.database.getStatementCount(null/* c */,
//                                        true/* exact */));
//
//            doSearchTest(((BigdataSail) sail).getConnection());
//
//            doSearchTest(((BigdataSail) sail).getReadOnlyConnection());
//            
//        } finally {
//
//            sail.__tearDownUnitTest();
//
//        }
//        
//    }

//    /**
//     * This runs a hand-coded query corresponding to a SPARQL query using the
//     * {@link BD#SEARCH} magic predicate.
//     * 
//     * <pre>
//     * select ?evidence
//     * where
//     * { ?evidence rdf:type &lt;the type&gt; .
//     *   ?evidence ?anypredicate ?label .
//     *   ?label bigdata:search &quot;the query&quot; .
//     * }
//     * </pre>
//     */
//    protected void doSearchTest(SailConnection conn) throws SailException,
//            QueryEvaluationException {
//      
//        try {
//
//            final StatementPattern sp = new StatementPattern(//
//                    new Var("X"),//
//                    new Var("1", BDS.SEARCH),//
//                    new Var("2", new LiteralImpl("Yellow"))//
//                    );
//            final TupleExpr tupleExpr = 
//                new QueryRoot(
//                        new Projection(
//                                sp, 
//                                new ProjectionElemList(new ProjectionElem("X"))));
//
//            /*
//             * Create a data set consisting of the contexts to be queried.
//             * 
//             * Note: a [null] DataSet will cause context to be ignored when the
//             * query is processed.
//             */
//            final DatasetImpl dataSet = null; //new DatasetImpl();
//
//            final BindingSet bindingSet = new QueryBindingSet();
//
//            final CloseableIteration<? extends BindingSet, QueryEvaluationException> itr = conn
//                    .evaluate(tupleExpr, dataSet, bindingSet, true/* includeInferred */);
//
//            try {
//
//                log.info("Verifying query.");
//
//                /*
//                 * These are the expected results for the query (the bindings
//                 * for X).
//                 */
//
//                final Set<Value> expected = new HashSet<Value>();
//
//				// Note: Whether or not this solution is present depends on the
//				// default value for minCosine.
//                expected.add(new LiteralImpl("Yellow Rose"));
//
//                expected.add(new LiteralImpl("Old Yellow House"));
//
//                /*
//                 * Verify that the query results is the correct solutions.
//                 */
//
//                final int nresults = expected.size();
//
//                int i = 0;
//
//                while (itr.hasNext()) {
//
//                    final BindingSet solution = itr.next();
//
//					if (log.isInfoEnabled())
//						log.info("solution[" + i + "] : " + solution);
//
//                    final Value actual = solution.getValue("X");
//
//					if (log.isInfoEnabled())
//						log.info("X[" + i + "] = " + actual + " ("
//                            + actual.getClass().getName() + ")");
//
//                    assertTrue("Not expecting X=" + actual, expected
//                            .remove(actual));
//
//                    i++;
//
//                }
//
//                assertEquals("#results", nresults, i);
//
//            } finally {
//
//                itr.close();
//
//            }
//
//        } finally {
//
//            conn.close();
//
//        }
//
//    }

    /**
     * Unit test used to track down a commit problem.
     * 
     * @throws Exception
     */
    public void test_restart() throws Exception {

        final boolean doYouWantMeToBreak = true;
        
        final URI SYSTAP =
            new URIImpl(
                        "http://bigdata.com/elm#a479c37c-407e-4f4a-be30-5a643a54561f");
        
        final URI ORGANIZATION = new URIImpl(
                "http://bigdata.com/domain#Organization");
        
        final URI ENTITY = new URIImpl("http://bigdata.com/system#Entity");
        
        final String query = "construct {"+//
		"?s <" + RDF.TYPE + "> <" + ENTITY + "> ."+//
		" } " + "where { "+//
		" ?s <" + RDF.TYPE + "> <" + ENTITY + "> ."+//
		" ?s ?p ?lit ."+//
		" ?lit <" + BDS.SEARCH + "> \"systap\" ."+//
		" ?lit <" + BDS.MIN_RELEVANCE + "> \"0.0\"^^<http://www.w3.org/2001/XMLSchema#double> ."+//
		" }";

        // the ontology (nothing is indexed for full text search).
        final Graph test_restart_1 = new LinkedHashModel();
        {

            test_restart_1.add(new StatementImpl(ORGANIZATION, RDFS.SUBCLASSOF,
                    ENTITY));

        }

        // the entity data (the rdfs:label gets indexed for full text search)
		final Graph test_restart_2 = new LinkedHashModel();
        {
            
            test_restart_2
                    .add(new StatementImpl(SYSTAP, RDF.TYPE, ENTITY));
         
            test_restart_2.add(new StatementImpl(SYSTAP, RDFS.LABEL,
                    new LiteralImpl("SYSTAP")));
        }
        
        final File file;
        {
            try {

                file = File.createTempFile(getName(), ".tmp");

                if (log.isInfoEnabled())
                    log.info("file=" + file);

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }
        }

        // overridden to use a disk-backed file.
        final Properties properties = super.getProperties();

        // use a disk-based mode since we will re-open the store to test restart
        // safety.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "false");

        properties.setProperty(Options.FILE, file.toString());

        BigdataSail sail = getSail(properties);

        try {

            /*
             * Setup the repo over the sail.
             */

            {

                final BigdataSailRepository repo = new BigdataSailRepository(
                        sail);

                // note: initializes the SAIL.
                repo.initialize();

                { // load ontology and optionally the entity data.
                    boolean ok = false;
                    final RepositoryConnection cxn = repo.getConnection();
                    try {
                        cxn.setAutoCommit(false);
                        log.info("loading ontology");
                        cxn.add(test_restart_1);
                        if (!doYouWantMeToBreak) {
                            // optionally load the entity data here.
                            log.info("loading entity data");
                            cxn.add(test_restart_2);
                        }
                        cxn.commit();
                        ok = true;
                    } finally {
                        if(!ok)
                            cxn.rollback();
                        cxn.close();
                    }
                }

                if (doYouWantMeToBreak) {
                    // load the entity data.
                    boolean ok = false;
                    final RepositoryConnection cxn = repo.getConnection();
                    try {
                        cxn.setAutoCommit(false);
                        log.info("loading entity data");
                        cxn.add(test_restart_2);
                        cxn.commit();
                        ok = true;
                    } finally {
                        if (!ok)
                            cxn.rollback();
                        cxn.close();
                    }
                }

                { // run the query (free text search)
                    final RepositoryConnection cxn = repo.getConnection();
                    try {
                        // silly construct queries, can't guarantee distinct
                        // results
                        final Set<Statement> results = new LinkedHashSet<Statement>();
                        final GraphQuery graphQuery = cxn.prepareGraphQuery(
                                QueryLanguage.SPARQL, query);
                        graphQuery.evaluate(new StatementCollector(results));
                        for (Statement stmt : results) {
                        	if(log.isInfoEnabled())
                        		log.info(stmt);
                        }
                        assertTrue(results.contains(new StatementImpl(SYSTAP,
                                RDF.TYPE, ENTITY)));
                    } finally {
                        cxn.close();
                    }
                }

                // shutdown the KB and the backing database.
                repo.shutDown();

            }

            // re-open the backing database and the KB.
            sail = reopenSail(sail);

            // setup the repo again.
            {
                final BigdataSailRepository repo = new BigdataSailRepository(
                        sail);

                repo.initialize();

                { // run the query again
//                    final String query = "construct { ?s <" + RDF.TYPE + "> <"
//                            + ENTITY + "> . } " + "where     { ?s <" + RDF.TYPE
//                            + "> <" + ENTITY + "> . ?s ?p ?lit . ?lit <"
//                            + BDS.SEARCH + "> \"systap\" . }";
                    final RepositoryConnection cxn = repo.getConnection();
                    try {
                        // silly construct queries, can't guarantee distinct
                        // results
                        final Set<Statement> results = new LinkedHashSet<Statement>();
                        final GraphQuery graphQuery = cxn.prepareGraphQuery(
                                QueryLanguage.SPARQL, query);
                        graphQuery.evaluate(new StatementCollector(results));
                        for (Statement stmt : results) {
                            if(log.isInfoEnabled())
                            	log.info(stmt);
                        }
                        assertTrue("Lost commit?", results
                                .contains(new StatementImpl(SYSTAP, RDF.TYPE,
                                        ENTITY)));
                    } finally {
                        cxn.close();
                    }

                }

            }

        } finally {

            sail.__tearDownUnitTest();

        }

    }

//    Note: This test was migrated to com.bigdata.rdf.sparql.ast.eval.TestSearch
//    and is found in test_search_named_graphs10a()...e().
//    
//    public void testWithNamedGraphs() throws Exception {
//        
//        final BigdataSail sail = getSail();
//        try {
//            
//        if (sail.getDatabase().isQuads() == false) {
//            return;
//        }
//        
//        sail.initialize();
//        final BigdataSailRepository repo = new BigdataSailRepository(sail);
//        final BigdataSailRepositoryConnection cxn = 
//            (BigdataSailRepositoryConnection) repo.getConnection();
//        try {
//
//            cxn.setAutoCommit(false);
//            
//            final BNode a = new BNodeImpl("_:a");
//            final BNode b = new BNodeImpl("_:b");
//            final Literal alice = new LiteralImpl("Alice");
//            final Literal bob = new LiteralImpl("Bob");
//            final URI graphA = new URIImpl("http://www.bigdata.com/graphA");
//            final URI graphB = new URIImpl("http://www.bigdata.com/graphB");
//            
///**/            
//            cxn.add(
//                    a,
//                    RDFS.LABEL,
//                    alice,
//                    graphA
//                    );
//            
//            /*
//             * Graph B.
//             */
//            cxn.add(
//                    b,
//                    RDFS.LABEL,
//                    bob,
//                    graphB
//                    );
///**/
//
//            /*
//             * Note: The either flush() or commit() is required to flush the
//             * statement buffers to the database before executing any operations
//             * that go around the sail.
//             */
//            cxn.flush();//commit();
//            
///**/            
//            if (log.isInfoEnabled()) {
//                log.info("\n" + sail.getDatabase().dumpStore());
//            }
//            
//            { // run the query with no graphs specified
//                final String query = 
//                    "select ?s " + 
//                    "from <"+graphA+"> " +
//                    "where " +
//                    "{ " +
//                    "    ?s <"+BDS.SEARCH+"> \"Alice\" . " +
//                    "}";
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                answer.add(createBindingSet(new BindingImpl("s", alice)));
//                
//                compare(result, answer);
//            }
//
//            { // run the query with graphA specified as the default graph
//                final String query = 
//                    "select ?s " + 
//                    "from <"+graphA+"> " +
//                    "where " +
//                    "{ " +
//                    "    ?s <"+BDS.SEARCH+"> \"Alice\" . " +
//                    "}";
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                answer.add(createBindingSet(new BindingImpl("s", alice)));
//                
//                compare(result, answer);
//            }
//
//            { // run the query with graphB specified as the default graph
//                final String query = 
//                    "select ?s " + 
//                    "from <"+graphB+"> " +
//                    "where " +
//                    "{ " +
//                    "    ?s <"+BDS.SEARCH+"> \"Alice\" . " +
//                    "}";
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                //answer.add(createBindingSet(new BindingImpl("s", alice)));
//                
//                compare(result, answer);
//            }
//
//            { // run the query with graphB specified as the default graph
//                final String query = 
//                    "select ?s ?o " + 
//                    "from <"+graphB+"> " +
//                    "where " +
//                    "{ " +
//                    "    ?s <"+RDFS.LABEL+"> ?o . " +
//                    "    ?o <"+BDS.SEARCH+"> \"Alice\" . " +
//                    "}";
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                //answer.add(createBindingSet(new BindingImpl("s", alice)));
//                
//                compare(result, answer);
//            }
//
//            { // run the query with graphB specified as the default graph
//                final String query = 
//                    "select ?s ?o " + 
//                    "from <"+graphB+"> " +
//                    "where " +
//                    "{ " +
//                    "    ?s <"+RDFS.LABEL+"> ?o1 . " +
//                    "    ?o <"+BDS.SEARCH+"> \"Alice\" . " +
//                    "}";
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                //answer.add(createBindingSet(new BindingImpl("s", alice)));
//                
//                compare(result, answer);
//            }
//
//        } finally {
//            cxn.close();
//        }
//        } finally {
//            sail.__tearDownUnitTest();
//        }
//        
//    }

    /**
     * FIXME This should be migrated to the data-driven test suite in
     * {@link TestSearch}.
     * 
     * @see TestSearch
     */
    public void testWithMetadata() throws Exception {
        
        final BigdataSail sail = getSail();
        try {
            
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        
        try {
            
            cxn.setAutoCommit(false);

            final ValueFactory vf = sail.getValueFactory();

        	final URI s1 = vf.createURI(BDS.NAMESPACE+"s1");
        	final URI s2 = vf.createURI(BDS.NAMESPACE+"s2");
        	final URI s3 = vf.createURI(BDS.NAMESPACE+"s3");
        	final URI s4 = vf.createURI(BDS.NAMESPACE+"s4");
        	final URI s5 = vf.createURI(BDS.NAMESPACE+"s5");
        	final URI s6 = vf.createURI(BDS.NAMESPACE+"s6");
        	final URI s7 = vf.createURI(BDS.NAMESPACE+"s7");
        	final URI s8 = vf.createURI(BDS.NAMESPACE+"s8");
        	final Literal l1 = vf.createLiteral("how");
        	final Literal l2 = vf.createLiteral("now");
        	final Literal l3 = vf.createLiteral("brown");
        	final Literal l4 = vf.createLiteral("cow");
        	final Literal l5 = vf.createLiteral("how now");
        	final Literal l6 = vf.createLiteral("brown cow");
        	final Literal l7 = vf.createLiteral("how now brown cow");
        	final Literal l8 = vf.createLiteral("toilet");
        	
            cxn.add(s1, RDFS.LABEL, l1);
            cxn.add(s2, RDFS.LABEL, l2);
            cxn.add(s3, RDFS.LABEL, l3);
            cxn.add(s4, RDFS.LABEL, l4);
            cxn.add(s5, RDFS.LABEL, l5);
            cxn.add(s6, RDFS.LABEL, l6);
            cxn.add(s7, RDFS.LABEL, l7);
            cxn.add(s8, RDFS.LABEL, l8);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            final Map<IV, Literal> literals = new LinkedHashMap<IV, Literal>();
            literals.put(((BigdataValue)l1).getIV(), l1);
            literals.put(((BigdataValue)l2).getIV(), l2);
            literals.put(((BigdataValue)l3).getIV(), l3);
            literals.put(((BigdataValue)l4).getIV(), l4);
            literals.put(((BigdataValue)l5).getIV(), l5);
            literals.put(((BigdataValue)l6).getIV(), l6);
            literals.put(((BigdataValue)l7).getIV(), l7);
            literals.put(((BigdataValue)l8).getIV(), l8);
            
            final Map<IV, URI> uris = new LinkedHashMap<IV, URI>();
            uris.put(((BigdataValue)l1).getIV(), s1);
            uris.put(((BigdataValue)l2).getIV(), s2);
            uris.put(((BigdataValue)l3).getIV(), s3);
            uris.put(((BigdataValue)l4).getIV(), s4);
            uris.put(((BigdataValue)l5).getIV(), s5);
            uris.put(((BigdataValue)l6).getIV(), s6);
            uris.put(((BigdataValue)l7).getIV(), s7);
            uris.put(((BigdataValue)l8).getIV(), s8);
            
/**/            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }
            
            { 
            	final String searchQuery = "how now brown cow";
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BDS.RELEVANCE+"> ?score . " +
                    "} " +
                    "order by desc(?score)";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
                    assertEquals("wrong # of results", 7, i);
                } finally {
                    result.close();
                }
                result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
                            null, // regex
                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
                            false // matchExact
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>)hit.getDocId();
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                	if(log.isInfoEnabled())
                		log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { 
            	final String searchQuery = "how now brown cow";
            	final int maxHits = 5;
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BDS.RELEVANCE+"> ?score . " +
                    "    ?o <"+BDS.MAX_RANK+"> \""+maxHits+"\" . " +
                    "} " +
                    "order by desc(?score)";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
                    assertEquals("wrong # of results", 5, i);
                } finally {
                    result.close();
                }
                
                result = tupleQuery.evaluate();
                
                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final IValueCentricTextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
                            null, // regex
                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
                            false, // matchExact
                            BDS.DEFAULT_MIN_RELEVANCE,//0d, // minCosine
                            BDS.DEFAULT_MAX_RELEVANCE,//1.0d, // maxCosine
                            BDS.DEFAULT_MIN_RANK,//1
                            maxHits, // maxRank (=maxResults + 1)
                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>)hit.getDocId();
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                    if(log.isInfoEnabled())
                		log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { 
            	final String searchQuery = "how now brown cow";
            	final double minRelevance = 0.6d;
            	final double maxRelevance = 0.9d;
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BDS.RELEVANCE+"> ?score . " +
                    "    ?o <"+BDS.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
                    "    ?o <"+BDS.MAX_RELEVANCE+"> \""+maxRelevance+"\" . " +
//                    "    ?o <"+BDS.MAX_HITS+"> \"5\" . " +
                    "} " +
                    "order by desc(?score)";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
                    assertEquals("wrong # of results", 2, i);
                } finally {
                    result.close();
                }
                
                result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final IValueCentricTextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
                            null, // regex
                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
                            false, // matchExact
                            minRelevance, // minCosine
                            maxRelevance, // maxCosine
                            BDS.DEFAULT_MIN_RANK,//1
                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>) hit.getDocId();
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                    if(log.isInfoEnabled())
                		log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { // exact match
            	
            	final String searchQuery = "brown cow";
            	final double minRelevance = 0.0d;
            	final double maxRelevance = 1.0d;
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BDS.RELEVANCE+"> ?score . " +
//                    "    ?o <"+BDS.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
//                    "    ?o <"+BDS.MAX_HITS+"> \"5\" . " +
                    "    filter regex(?o, \""+searchQuery+"\") " +
                    "} " +
                    "order by desc(?score)";
                
                if(log.isInfoEnabled())
            		log.info("\n"+query);
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
                    assertEquals("wrong # of results: " + i, 2, i);
                } finally {
                    result.close();
                }
                
                result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final IValueCentricTextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
                            null, // regex
                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
                            false, // matchExact
                            minRelevance, // minCosine
                            maxRelevance, // maxCosine
                            BDS.DEFAULT_MIN_RANK,//1
                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>) hit.getDocId();
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                	if (!o.getLabel().contains(searchQuery))
                		continue;
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                    if(log.isInfoEnabled())
                        log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { // prefix match
            	
            	final String searchQuery = "bro*";
            	final double minRelevance = 0.0d;
            	final double maxRelevance = 1.0d;
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BDS.RELEVANCE+"> ?score . " +
//                    "    ?o <"+BDS.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
//                    "    ?o <"+BDS.MAX_HITS+"> \"5\" . " +
//                    "    filter regex(?o, \""+searchQuery+"\") " +
                    "} " +
                    "order by desc(?score)";
                
                if(log.isInfoEnabled())
            		log.info("\n"+query);
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
                    assertEquals("wrong # of results: " + i, 3, i);
                } finally {
                    result.close();
                }
                
                result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final IValueCentricTextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            true, // prefixMatch
                            null, // regex
                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
                            false, // matchExact
                            minRelevance, // minCosine
                            maxRelevance, // maxCosine
                            BDS.DEFAULT_MIN_RANK,//1
                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>) hit.getDocId();
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                    if(log.isInfoEnabled())
                        log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { // prefix match using a stopword
            	
            	final String searchQuery = "to*";
            	final double minRelevance = 0.0d;
            	final double maxRelevance = BDS.DEFAULT_MAX_RELEVANCE;//1.0d;
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BDS.RELEVANCE+"> ?score . " +
                    "    ?o <"+BDS.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
//                    "    ?o <"+BDS.MAX_HITS+"> \"5\" . " +
//                    "    filter regex(?o, \""+searchQuery+"\") " +
                    "} " +
                    "order by desc(?score)";
                
            	if(log.isInfoEnabled())
            		log.info("\n"+query);
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
                    assertEquals("wrong # of results: " + i, 1, i);
                } finally {
                    result.close();
                }
                
                result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final IValueCentricTextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            true, // prefixMatch
                            null, // regex
                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
                            false, // matchExact
                            minRelevance, // minCosine
                            maxRelevance, // maxCosine
                            BDS.DEFAULT_MIN_RANK,//1
                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>) hit.getDocId();
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                    if(log.isInfoEnabled())
                        log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { // match all terms
            	
            	final String searchQuery = "how now brown cow";
            	final double minRelevance = 0.0d;
            	final double maxRelevance = 1.0d;
            	
                final String query = 
                    "select ?s ?o " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BDS.MATCH_ALL_TERMS+"> \"true\" . " +
                    "}";
                
                if(log.isInfoEnabled())
                    log.info("\n"+query);
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
//                assertTrue("wrong # of results: " + i, i == 1);
                } finally {
                    result.close();
                }
                
                result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final IValueCentricTextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            true, // prefixMatch
                            null, // regex
                            true, // matchAllTerms
                            false, // matchExact
                            minRelevance, // minCosine
                            maxRelevance, // maxCosine
                            BDS.DEFAULT_MIN_RANK,//1
                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>) hit.getDocId();
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o));
                    if(log.isInfoEnabled())
                        log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { // minRank, maxRank
            	
            	final String searchQuery = "how now brown cow";
            	final int minRank = 2;
            	final int maxRank = 5;
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BDS.RELEVANCE+"> ?score . " +
                    "    ?o <"+BDS.MIN_RANK+"> \""+minRank+"\" . " +
                    "    ?o <"+BDS.MAX_RANK+"> \""+maxRank+"\" . " +
                    "}";
                
                if(log.isInfoEnabled())
                    log.info("\n"+query);
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
                    assertTrue("wrong # of results: " + i, i == (maxRank-minRank+1));
                } finally {
                    result.close();
                }
                
                result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final IValueCentricTextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            true, // prefixMatch
                            null, // regex
                            false, // matchAllTerms
                            false, // matchExact
                            BDS.DEFAULT_MIN_RELEVANCE, // minCosine
                            BDS.DEFAULT_MAX_RELEVANCE, // maxCosine
                            minRank,//1
                            maxRank,//10000, // maxRank (=maxResults + 1)
                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>) hit.getDocId();
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                    if(log.isInfoEnabled())
                        log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }
            
            { // countHits
            	
            	final String searchQuery = "how now brown cow";
                
                final IValueCentricTextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                
                final int i = search.count(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            true // prefixMatch
                            ));
                
                if (log.isInfoEnabled()) {
                	log.info(i + " search results.");
                }
                
                assertTrue("wrong # of results: " + i, i == 7);
                
            }
            
        } finally {
            cxn.close();
        }
        } finally {
            sail.__tearDownUnitTest();
        }
        
    }
    
//    public void testSubjectSearch() throws Exception {
//        
//        final BigdataSail sail = getSail();
//        try {
//            
//        sail.initialize();
//        final BigdataSailRepository repo = new BigdataSailRepository(sail);
//        final BigdataSailRepositoryConnection cxn = 
//            (BigdataSailRepositoryConnection) repo.getConnection();
//        
//        try {
//            
//            cxn.setAutoCommit(false);
//
//            final ValueFactory vf = sail.getValueFactory();
//
//        	final URI s1 = vf.createURI(BDS.NAMESPACE+"s1");
//        	final URI s2 = vf.createURI(BDS.NAMESPACE+"s2");
//        	final URI s3 = vf.createURI(BDS.NAMESPACE+"s3");
//        	final URI s4 = vf.createURI(BDS.NAMESPACE+"s4");
//        	final URI s5 = vf.createURI(BDS.NAMESPACE+"s5");
//        	final URI s6 = vf.createURI(BDS.NAMESPACE+"s6");
//        	final URI s7 = vf.createURI(BDS.NAMESPACE+"s7");
//        	final URI s8 = vf.createURI(BDS.NAMESPACE+"s8");
//        	final Literal l1 = vf.createLiteral("how");
//        	final Literal l2 = vf.createLiteral("now");
//        	final Literal l3 = vf.createLiteral("brown");
//        	final Literal l4 = vf.createLiteral("cow");
//        	final Literal l5 = vf.createLiteral("how now");
//        	final Literal l6 = vf.createLiteral("brown cow");
//        	final Literal l7 = vf.createLiteral("how now brown cow");
//        	final Literal l8 = vf.createLiteral("toilet");
//        	
//            cxn.add(s1, RDFS.LABEL, l1);
//            cxn.add(s2, RDFS.LABEL, l2);
//            cxn.add(s3, RDFS.LABEL, l3);
//            cxn.add(s4, RDFS.LABEL, l4);
//            cxn.add(s5, RDFS.LABEL, l5);
//            cxn.add(s6, RDFS.LABEL, l6);
//            cxn.add(s7, RDFS.LABEL, l7);
//            cxn.add(s8, RDFS.LABEL, l8);
//            
//            /*
//             * Note: The either flush() or commit() is required to flush the
//             * statement buffers to the database before executing any operations
//             * that go around the sail.
//             */
//            cxn.commit();
//            
//            sail.getDatabase().getLexiconRelation().buildSubjectCentricTextIndex();
//            
//            final Map<IV, Literal> literals = new LinkedHashMap<IV, Literal>();
//            literals.put(((BigdataURI)s1).getIV(), l1);
//            literals.put(((BigdataURI)s2).getIV(), l2);
//            literals.put(((BigdataURI)s3).getIV(), l3);
//            literals.put(((BigdataURI)s4).getIV(), l4);
//            literals.put(((BigdataURI)s5).getIV(), l5);
//            literals.put(((BigdataURI)s6).getIV(), l6);
//            literals.put(((BigdataURI)s7).getIV(), l7);
//            literals.put(((BigdataURI)s8).getIV(), l8);
//            
//            final Map<IV, URI> uris = new LinkedHashMap<IV, URI>();
//            uris.put(((BigdataURI)s1).getIV(), s1);
//            uris.put(((BigdataURI)s2).getIV(), s2);
//            uris.put(((BigdataURI)s3).getIV(), s3);
//            uris.put(((BigdataURI)s4).getIV(), s4);
//            uris.put(((BigdataURI)s5).getIV(), s5);
//            uris.put(((BigdataURI)s6).getIV(), s6);
//            uris.put(((BigdataURI)s7).getIV(), s7);
//            uris.put(((BigdataURI)s8).getIV(), s8);
//            
///**/            
//            if (log.isInfoEnabled()) {
//                log.info("\n" + sail.getDatabase().dumpStore());
//            }
//            
//            { 
//            	final String searchQuery = "how now brown cow";
//            	
//                final String query = 
//                    "select ?s ?score " + 
//                    "where " +
//                    "{ " +
//                    "    ?s <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
//                    "    ?s <"+BDS.RELEVANCE+"> ?score . " +
//                    "    ?s <"+BDS.SUBJECT_SEARCH+"> true . " +
//                    "} " +
//                    "order by desc(?score)";
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                int i = 0;
//                while (result.hasNext()) {
//                    final BindingSet tmp = result.next();
//                    if (log.isInfoEnabled())
//                        log.info(i + ": " + tmp.toString());
//                    i++;
//                }
//                assertEquals("wrong # of results", 7, i);
//                
//                result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                
//                final ITextIndexer search = 
//                	sail.getDatabase().getLexiconRelation().getSubjectCentricSearchEngine();
//                final Hiterator<IHit> hits = 
//                	search.search(new FullTextQuery(
//                			searchQuery, 
//                            null, // languageCode
//                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
//                            null, // regex
//                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
//                            false, // matchExact
//                            BDS.DEFAULT_MIN_RELEVANCE,//0d, // minCosine
//                            BDS.DEFAULT_MAX_RELEVANCE,//1.0d, // maxCosine
//                            BDS.DEFAULT_MIN_RANK,//1
//                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
//                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
//                            TimeUnit.MILLISECONDS // unit
//                            ));
//                
//                while (hits.hasNext()) {
//                	final IHit hit = hits.next();
//                	final IV id = (IV)hit.getDocId();
//                	final Literal score = vf.createLiteral(hit.getCosine());
//                	final URI s = uris.get(id);
//                    final BindingSet bs = createBindingSet(
//                    		new BindingImpl("s", s),
//                    		new BindingImpl("score", score));
//                	if(log.isInfoEnabled())
//                		log.info(bs);
//                    answer.add(bs);
//                }
//                
//                compare(result, answer);
//
//            }
//
//            { 
//            	final String searchQuery = "how now brown cow";
//            	final int maxHits = 5;
//            	
//                final String query = 
//                    "select ?s ?score " + 
//                    "where " +
//                    "{ " +
//                    "    ?s <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
//                    "    ?s <"+BDS.RELEVANCE+"> ?score . " +
//                    "    ?s <"+BDS.MAX_RANK+"> \""+maxHits+"\" . " +
//                    "    ?s <"+BDS.SUBJECT_SEARCH+"> true . " +
//                    "} " +
//                    "order by desc(?score)";
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                int i = 0;
//                while (result.hasNext()) {
//                    final BindingSet tmp = result.next();
//                    if (log.isInfoEnabled())
//                        log.info(i + ": " + tmp.toString());
//                    i++;
//                }
//                assertEquals("wrong # of results", 5, i);
//                
//                result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                
//                final ITextIndexer search = 
//                	sail.getDatabase().getLexiconRelation().getSubjectCentricSearchEngine();
//                final Hiterator<IHit> hits = 
//                	search.search(new FullTextQuery(
//                			searchQuery, 
//                            null, // languageCode
//                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
//                            null, // regex
//                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
//                            false, // matchExact
//                            BDS.DEFAULT_MIN_RELEVANCE,//0d, // minCosine
//                            BDS.DEFAULT_MAX_RELEVANCE,//1.0d, // maxCosine
//                            BDS.DEFAULT_MIN_RANK,//1
//                            maxHits, // maxRank (=maxResults + 1)
//                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
//                            TimeUnit.MILLISECONDS // unit
//                            ));
//                
//                while (hits.hasNext()) {
//                	final IHit hit = hits.next();
//                	final IV id = (IV)hit.getDocId();
//                	final Literal score = vf.createLiteral(hit.getCosine());
//                	final URI s = uris.get(id);
//                	final Literal o = literals.get(id);
//                    final BindingSet bs = createBindingSet(
//                    		new BindingImpl("s", s),
////                    		new BindingImpl("o", o),
//                    		new BindingImpl("score", score));
//                    if(log.isInfoEnabled())
//                		log.info(bs);
//                    answer.add(bs);
//                }
//                
//                compare(result, answer);
//
//            }
//
//            { 
//            	final String searchQuery = "how now brown cow";
//            	final double minRelevance = 0.6d;
//            	final double maxRelevance = 0.9d;
//            	
//                final String query = 
//                    "select ?s ?score " + 
//                    "where " +
//                    "{ " +
////                    "    ?s <"+RDFS.LABEL+"> ?o . " +
//                    "    ?s <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
//                    "    ?s <"+BDS.RELEVANCE+"> ?score . " +
//                    "    ?s <"+BDS.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
//                    "    ?s <"+BDS.MAX_RELEVANCE+"> \""+maxRelevance+"\" . " +
//                    "    ?s <"+BDS.SUBJECT_SEARCH+"> true . " +
////                    "    ?o <"+BDS.MAX_HITS+"> \"5\" . " +
//                    "} " +
//                    "order by desc(?score)";
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                int i = 0;
//                while (result.hasNext()) {
//                    final BindingSet tmp = result.next();
//                    if (log.isInfoEnabled())
//                        log.info(i + ": " + tmp.toString());
//                    i++;
//                }
//                assertEquals("wrong # of results", 2, i);
//                
//                result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                
//                final ITextIndexer search = 
//                	sail.getDatabase().getLexiconRelation().getSubjectCentricSearchEngine();
//                final Hiterator<IHit> hits = 
//                	search.search(new FullTextQuery(
//                			searchQuery, 
//                            null, // languageCode
//                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
//                            null, // regex
//                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
//                            false, // matchExact
//                            minRelevance, // minCosine
//                            maxRelevance, // maxCosine
//                            BDS.DEFAULT_MIN_RANK,//1
//                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
//                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
//                            TimeUnit.MILLISECONDS // unit
//                            ));
//                
//                while (hits.hasNext()) {
//                	final IHit hit = hits.next();
//                	final IV id = (IV) hit.getDocId();
//                	final Literal score = vf.createLiteral(hit.getCosine());
//                	final URI s = uris.get(id);
//                	final Literal o = literals.get(id);
//                    final BindingSet bs = createBindingSet(
//                    		new BindingImpl("s", s),
////                    		new BindingImpl("o", o),
//                    		new BindingImpl("score", score));
//                    if(log.isInfoEnabled())
//                		log.info(bs);
//                    answer.add(bs);
//                }
//                
//                compare(result, answer);
//
//            }
//
//            { // exact match
//            	
//            	final String searchQuery = "brown cow";
//            	final double minRelevance = 0.0d;
//            	final double maxRelevance = 1.0d;
//            	
//                final String query = 
//                    "select ?s ?score " + 
//                    "where " +
//                    "{ " +
////                    "    ?s <"+RDFS.LABEL+"> ?o . " +
//                    "    ?s <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
//                    "    ?s <"+BDS.RELEVANCE+"> ?score . " +
//                    "    ?s <"+BDS.SUBJECT_SEARCH+"> true . " +
////                    "    ?o <"+BDS.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
////                    "    ?o <"+BDS.MAX_HITS+"> \"5\" . " +
//                    "    ?s ?p ?o . " +
//                    "    filter regex(?o, \""+searchQuery+"\") " +
//                    "} " +
//                    "order by desc(?score)";
//                
//                if(log.isInfoEnabled())
//            		log.info("\n"+query);
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                int i = 0;
//                while (result.hasNext()) {
//                    final BindingSet tmp = result.next();
//                    if (log.isInfoEnabled())
//                        log.info(i + ": " + tmp.toString());
//                    i++;
//                }
//                assertEquals("wrong # of results: " + i, 2, i);
//                
//                result = tupleQuery.evaluate();
//
//                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                
//                final ITextIndexer search = 
//                	sail.getDatabase().getLexiconRelation().getSubjectCentricSearchEngine();
//                final Hiterator<IHit> hits = 
//                	search.search(new FullTextQuery(
//                			searchQuery, 
//                            null, // languageCode
//                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
//                            null, // regex
//                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
//                            false, // matchExact
//                            minRelevance, // minCosine
//                            maxRelevance, // maxCosine
//                            BDS.DEFAULT_MIN_RANK,//1
//                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
//                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
//                            TimeUnit.MILLISECONDS // unit
//                            ));
//                
//                while (hits.hasNext()) {
//                	final IHit hit = hits.next();
//                	final IV id = (IV) hit.getDocId();
//                	final Literal score = vf.createLiteral(hit.getCosine());
//                	final URI s = uris.get(id);
//                	final Literal o = literals.get(id);
//                	if (!o.getLabel().contains(searchQuery))
//                		continue;
//                    final BindingSet bs = createBindingSet(
//                    		new BindingImpl("s", s),
////                    		new BindingImpl("o", o),
//                    		new BindingImpl("score", score));
//                    if(log.isInfoEnabled())
//                        log.info(bs);
//                    answer.add(bs);
//                }
//                
//                compare(result, answer);
//
//            }
//
//            { // prefix match
//            	
//            	final String searchQuery = "bro*";
//            	final double minRelevance = 0.0d;
//            	final double maxRelevance = 1.0d;
//            	
//                final String query = 
//                    "select ?s ?score " + 
//                    "where " +
//                    "{ " +
////                    "    ?s <"+RDFS.LABEL+"> ?o . " +
//                    "    ?s <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
//                    "    ?s <"+BDS.RELEVANCE+"> ?score . " +
//                    "    ?s <"+BDS.SUBJECT_SEARCH+"> true . " +
////                    "    ?o <"+BDS.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
////                    "    ?o <"+BDS.MAX_HITS+"> \"5\" . " +
////                    "    filter regex(?o, \""+searchQuery+"\") " +
//                    "} " +
//                    "order by desc(?score)";
//                
//                if(log.isInfoEnabled())
//            		log.info("\n"+query);
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                int i = 0;
//                while (result.hasNext()) {
//                    final BindingSet tmp = result.next();
//                    if (log.isInfoEnabled())
//                        log.info(i + ": " + tmp.toString());
//                    i++;
//                }
//                assertEquals("wrong # of results: " + i, 3, i);
//                
//                result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                
//                final ITextIndexer search = 
//                	sail.getDatabase().getLexiconRelation().getSubjectCentricSearchEngine();
//                final Hiterator<IHit> hits = 
//                	search.search(new FullTextQuery(
//                			searchQuery, 
//                            null, // languageCode
//                            true, // prefixMatch
//                            null, // regex
//                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
//                            false, // matchExact
//                            minRelevance, // minCosine
//                            maxRelevance, // maxCosine
//                            BDS.DEFAULT_MIN_RANK,//1
//                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
//                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
//                            TimeUnit.MILLISECONDS // unit
//                            ));
//                
//                while (hits.hasNext()) {
//                	final IHit hit = hits.next();
//                	final IV id = (IV) hit.getDocId();
//                	final Literal score = vf.createLiteral(hit.getCosine());
//                	final URI s = uris.get(id);
//                	final Literal o = literals.get(id);
//                    final BindingSet bs = createBindingSet(
//                    		new BindingImpl("s", s),
////                    		new BindingImpl("o", o),
//                    		new BindingImpl("score", score));
//                    if(log.isInfoEnabled())
//                        log.info(bs);
//                    answer.add(bs);
//                }
//                
//                compare(result, answer);
//
//            }
//
//            { // prefix match using a stopword
//            	
//            	final String searchQuery = "to*";
//            	final double minRelevance = 0.0d;
//            	final double maxRelevance = BDS.DEFAULT_MAX_RELEVANCE;//1.0d;
//            	
//                final String query = 
//                    "select ?s ?score " + 
//                    "where " +
//                    "{ " +
////                    "    ?s <"+RDFS.LABEL+"> ?o . " +
//                    "    ?s <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
//                    "    ?s <"+BDS.RELEVANCE+"> ?score . " +
//                    "    ?s <"+BDS.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
//                    "    ?s <"+BDS.SUBJECT_SEARCH+"> true . " +
////                    "    ?o <"+BDS.MAX_HITS+"> \"5\" . " +
////                    "    filter regex(?o, \""+searchQuery+"\") " +
//                    "} " +
//                    "order by desc(?score)";
//                
//            	if(log.isInfoEnabled())
//            		log.info("\n"+query);
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                int i = 0;
//                while (result.hasNext()) {
//                    final BindingSet tmp = result.next();
//                    if (log.isInfoEnabled())
//                        log.info(i + ": " + tmp.toString());
//                    i++;
//                }
//                assertEquals("wrong # of results: " + i, 1, i);
//                
//                result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                
//                final ITextIndexer search = 
//                	sail.getDatabase().getLexiconRelation().getSubjectCentricSearchEngine();
//                final Hiterator<IHit> hits = 
//                	search.search(new FullTextQuery(
//                			searchQuery, 
//                            null, // languageCode
//                            true, // prefixMatch
//                            null, // regex
//                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
//                            false, // matchExact
//                            minRelevance, // minCosine
//                            maxRelevance, // maxCosine
//                            BDS.DEFAULT_MIN_RANK,//1
//                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
//                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
//                            TimeUnit.MILLISECONDS // unit
//                            ));
//                
//                while (hits.hasNext()) {
//                	final IHit hit = hits.next();
//                	final IV id = (IV) hit.getDocId();
//                	final Literal score = vf.createLiteral(hit.getCosine());
//                	final URI s = uris.get(id);
//                	final Literal o = literals.get(id);
//                    final BindingSet bs = createBindingSet(
//                    		new BindingImpl("s", s),
////                    		new BindingImpl("o", o),
//                    		new BindingImpl("score", score));
//                    if(log.isInfoEnabled())
//                        log.info(bs);
//                    answer.add(bs);
//                }
//                
//                compare(result, answer);
//
//            }
//
//            { // match all terms
//            	
//            	final String searchQuery = "how now brown cow";
//            	final double minRelevance = 0.0d;
//            	final double maxRelevance = 1.0d;
//            	
//                final String query = 
//                    "select ?s " + 
//                    "where " +
//                    "{ " +
//                    "    ?s <"+RDFS.LABEL+"> ?o . " +
//                    "    ?s <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
//                    "    ?s <"+BDS.MATCH_ALL_TERMS+"> \"true\" . " +
//                    "    ?s <"+BDS.SUBJECT_SEARCH+"> true . " +
//                    "}";
//                
//                if(log.isInfoEnabled())
//                    log.info("\n"+query);
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                int i = 0;
//                while (result.hasNext()) {
//                    final BindingSet tmp = result.next();
//                    if (log.isInfoEnabled())
//                        log.info(i + ": " + tmp.toString());
//                    i++;
//                }
////                assertTrue("wrong # of results: " + i, i == 1);
//                
//                result = tupleQuery.evaluate();
//
//                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                
//                final ITextIndexer search = 
//                	sail.getDatabase().getLexiconRelation().getSubjectCentricSearchEngine();
//                final Hiterator<IHit> hits = 
//                	search.search(new FullTextQuery(
//                			searchQuery, 
//                            null, // languageCode
//                            true, // prefixMatch
//                            null,
//                            true, // matchAllTerms
//                            false, // matchExact
//                            minRelevance, // minCosine
//                            maxRelevance, // maxCosine
//                            BDS.DEFAULT_MIN_RANK,//1
//                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
//                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
//                            TimeUnit.MILLISECONDS // unit
//                            ));
//                
//                while (hits.hasNext()) {
//                	final IHit hit = hits.next();
//                	final IV id = (IV) hit.getDocId();
//                	final URI s = uris.get(id);
//                	final Literal o = literals.get(id);
//                    final BindingSet bs = createBindingSet(
//                    		new BindingImpl("s", s)
////                    		new BindingImpl("o", o)
//                    		);
//                    if(log.isInfoEnabled())
//                        log.info(bs);
//                    answer.add(bs);
//                }
//                
//                compare(result, answer);
//
//            }
//
//            { // minRank, maxRank
//            	
//            	final String searchQuery = "how now brown cow";
//            	final int minRank = 2;
//            	final int maxRank = 5;
//                final String query = 
//                    "select ?s ?score " + 
//                    "where " +
//                    "{ " +
////                    "    ?s <"+RDFS.LABEL+"> ?o . " +
//                    "    ?s <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
//                    "    ?s <"+BDS.RELEVANCE+"> ?score . " +
//                    "    ?s <"+BDS.MIN_RANK+"> \""+minRank+"\" . " +
//                    "    ?s <"+BDS.MAX_RANK+"> \""+maxRank+"\" . " +
//                    "    ?s <"+BDS.SUBJECT_SEARCH+"> true . " +
//                    "}";
//                
//                if(log.isInfoEnabled())
//                    log.info("\n"+query);
//                
//                final TupleQuery tupleQuery = 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                TupleQueryResult result = tupleQuery.evaluate();
//
//                int i = 0;
//                while (result.hasNext()) {
//                    final BindingSet tmp = result.next();
//                    if (log.isInfoEnabled())
//                        log.info(i + ": " + tmp.toString());
//                    i++;
//                }
//                assertTrue("wrong # of results: " + i, i == (maxRank-minRank+1));
//                
//                result = tupleQuery.evaluate();
//
//                Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                
//                final ITextIndexer search = 
//                	sail.getDatabase().getLexiconRelation().getSubjectCentricSearchEngine();
//                final Hiterator<IHit> hits = 
//                	search.search(new FullTextQuery(
//                			searchQuery, 
//                            null, // languageCode
//                            true, // prefixMatch
//                            null,
//                            false, // matchAllTerms
//                            false, // matchExact
//                            BDS.DEFAULT_MIN_RELEVANCE, // minCosine
//                            BDS.DEFAULT_MAX_RELEVANCE, // maxCosine
//                            minRank,//1
//                            maxRank,//10000, // maxRank (=maxResults + 1)
//                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
//                            TimeUnit.MILLISECONDS
//                            ));
//                
//                while (hits.hasNext()) {
//                	final IHit hit = hits.next();
//                	final IV id = (IV) hit.getDocId();
//                	final Literal score = vf.createLiteral(hit.getCosine());
//                	final URI s = uris.get(id);
//                	final Literal o = literals.get(id);
//                    final BindingSet bs = createBindingSet(
//                    		new BindingImpl("s", s),
////                    		new BindingImpl("o", o),
//                    		new BindingImpl("score", score));
//                    if(log.isInfoEnabled())
//                        log.info(bs);
//                    answer.add(bs);
//                }
//                
//                compare(result, answer);
//
//            }
//            
//            { // countHits
//            	
//            	final String searchQuery = "how now brown cow";
//                
//                final ITextIndexer search = 
//                	sail.getDatabase().getLexiconRelation().getSubjectCentricSearchEngine();
//                
//                final int i = search.count(new FullTextQuery(
//                			searchQuery, 
//                            null, // languageCode
//                            true, // prefixMatch
//                            null, // regex
//                            false, // matchAllTerms
//                            false // matchExact
//                            ));
//                
//                if (log.isInfoEnabled()) {
//                	log.info(i + " search results.");
//                }
//                
//                assertTrue("wrong # of results: " + i, i == 7);
//                
//            }
//            
//        } finally {
//            cxn.close();
//        }
//        } finally {
//            sail.__tearDownUnitTest();
//        }
//        
//    }
    
    /**
     * FIXME This should be migrated to the data-driven test suite in
     * {@link TestSearch}.
     * 
     * @see TestSearch
     */
    public void testValidationWithIncomingBindings() throws Exception {
        
        final BigdataSail sail = getSail();
        try {
            
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        
        try {
            
            cxn.setAutoCommit(false);

            final ValueFactory vf = sail.getValueFactory();

        	final URI s1 = vf.createURI(BDS.NAMESPACE+"s1");
        	final URI s2 = vf.createURI(BDS.NAMESPACE+"s2");
        	final URI s3 = vf.createURI(BDS.NAMESPACE+"s3");
        	final URI s4 = vf.createURI(BDS.NAMESPACE+"s4");
        	final URI s5 = vf.createURI(BDS.NAMESPACE+"s5");
        	final URI s6 = vf.createURI(BDS.NAMESPACE+"s6");
        	final URI s7 = vf.createURI(BDS.NAMESPACE+"s7");
        	final URI s8 = vf.createURI(BDS.NAMESPACE+"s8");
        	final Literal l1 = vf.createLiteral("how");
        	final Literal l2 = vf.createLiteral("now");
        	final Literal l3 = vf.createLiteral("brown");
        	final Literal l4 = vf.createLiteral("cow");
        	final Literal l5 = vf.createLiteral("how now");
        	final Literal l6 = vf.createLiteral("brown cow");
        	final Literal l7 = vf.createLiteral("how now brown cow");
        	final Literal l8 = vf.createLiteral("toilet");
        	
            cxn.add(s1, RDFS.LABEL, l1);
            cxn.add(s2, RDFS.LABEL, l2);
            cxn.add(s3, RDFS.LABEL, l3);
            cxn.add(s4, RDFS.LABEL, l4);
            cxn.add(s5, RDFS.LABEL, l5);
            cxn.add(s6, RDFS.LABEL, l6);
            cxn.add(s7, RDFS.LABEL, l7);
            cxn.add(s8, RDFS.LABEL, l8);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            final Map<IV, Literal> literals = new LinkedHashMap<IV, Literal>();
            literals.put(((BigdataValue)l1).getIV(), l1);
            literals.put(((BigdataValue)l2).getIV(), l2);
            literals.put(((BigdataValue)l3).getIV(), l3);
            literals.put(((BigdataValue)l4).getIV(), l4);
            literals.put(((BigdataValue)l5).getIV(), l5);
            literals.put(((BigdataValue)l6).getIV(), l6);
            literals.put(((BigdataValue)l7).getIV(), l7);
            literals.put(((BigdataValue)l8).getIV(), l8);
            
            final Map<IV, URI> uris = new LinkedHashMap<IV, URI>();
            uris.put(((BigdataValue)l1).getIV(), s1);
            uris.put(((BigdataValue)l2).getIV(), s2);
            uris.put(((BigdataValue)l3).getIV(), s3);
            uris.put(((BigdataValue)l4).getIV(), s4);
            uris.put(((BigdataValue)l5).getIV(), s5);
            uris.put(((BigdataValue)l6).getIV(), s6);
            uris.put(((BigdataValue)l7).getIV(), s7);
            uris.put(((BigdataValue)l8).getIV(), s8);
            
/**/            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }
            
            { 
            	final String searchQuery = "how now brown cow";
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> ?searchQuery . " +
                    "    ?o <"+BDS.RELEVANCE+"> ?score . " +
                    "} " +
                    "order by desc(?score)";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                tupleQuery.setBinding("searchQuery", new LiteralImpl(searchQuery));
                
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
                    assertEquals("wrong # of results", 7, i);
                } finally {
                    result.close();
                }
                
                result = tupleQuery.evaluate();

                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
                            null,
                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
                            false, // matchExact
                            BDS.DEFAULT_MIN_RELEVANCE,//0d, // minCosine
                            BDS.DEFAULT_MAX_RELEVANCE,//1.0d, // maxCosine
                            BDS.DEFAULT_MIN_RANK,//1
                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
                            TimeUnit.MILLISECONDS
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>)hit.getDocId();
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                	if(log.isInfoEnabled())
                		log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

        } finally {
            cxn.close();
        }
        } finally {
            sail.__tearDownUnitTest();
        }
        
    }
    

    
//    private final void doQuery() throws Exception {
//        
//        final BigdataSail sail = getSail();
//        
//        try {
//            
//        sail.initialize();
//        final BigdataSailRepository repo = new BigdataSailRepository(sail);
//        final BigdataSailRepositoryConnection cxn = 
//            (BigdataSailRepositoryConnection) repo.getConnection();
//        
//        try {
//            
//            cxn.setAutoCommit(false);
//
//            final String freeTextSearch = "how now brown cow";
//            
//            final String snippetVar = "target";
//            
//            final String queryTemplate = 
//                "prefix bd: <"+BDS.NAMESPACE+"> " +
//                "prefix bds: <"+BDS.SEARCH_NAMESPACE+"> " +
//                "prefix rdf: <"+RDF.NAMESPACE+"> " +
//                "prefix rdfs: <"+RDFS.NAMESPACE+"> " +
//                "select ?target ?o ?type ?score " + 
//                "where " +
//                "{ " +
//                "    ?o bds:search \""+freeTextSearch+"\" . " +
//                "    ?o bds:minRelevance \"0.0\" . " +
//                "    ?o bds:relevance ?score . " +
//                "    ?o bds:minRank \"MINRANK\" . " +
//                "    ?o bds:maxRank \"MAXRANK\" . " +
//                "    ?target ?p ?o . " +
//                "    ?target rdf:type ?type . " +
//                "}";
//            
//            final ITextIndexer search = 
//                sail.getDatabase().getLexiconRelation().getSearchEngine();
//            
//            final int count = search.count(
//                        freeTextSearch, 
//                        null, // languageCode
//                        true, // prefixMatch
//                        0.0d, // minCosine
//                        1.0d, // maxCosine
//                        0, // minRank
//                        Integer.MAX_VALUE, // maxRank
//                        false, // matchAllTerms
//                        BDS.DEFAULT_TIMEOUT, // timeout 
//                        TimeUnit.MILLISECONDS // unit
//                        );
//
//            final Collection<BindingSet> results = new LinkedList<BindingSet>();
//            if (count < 1000) {
//                
//                // just go ahead and process the full query
//                
//                final TupleQuery tupleQuery = cxn.prepareTupleQuery(
//                        QueryLanguage.SPARQL, 
//                        queryTemplate
//                            .replace("MINRANK", "0")
//                            .replace("MAXRANK", String.valueOf(Integer.MAX_VALUE)));
//                
//                tupleQuery.setIncludeInferred(true /* includeInferred */);
//                
//                final TupleQueryResult tqr = tupleQuery.evaluate();
//                
//                while (tqr.hasNext()) {
//                    results.add(tqr.next());
//                }
//                
//            } else {
//                
//                final int numSnippets = 10;
//                
//                results.addAll(processRankChunks(
//                        cxn, queryTemplate, count, numSnippets));
//                
//            }
//            
//            // do something with the results 
//            for (BindingSet bs : results) {
//                System.err.println(bs);
//            }
//
//        } finally {
//            cxn.close();
//        }
//        } finally {
//            sail.__tearDownUnitTest();
//        }
//        
//    }
    
//    /**
//     * Process a query in min/max rank chunks, with a goal of reaching the
//     * number of snippets specified by numSnippets.
//     *  
//     * @param cxn
//     *             The sail connection.
//     * @param queryTemplate
//     *             The query template.  Uses "target" as the variable for snippets,
//     *             and the strings "MINRANK" and "MAXRANK" as the placeholders
//     *             for the rank chunk bounds.
//     * @param numFreeTextHits
//     *             The number of free text search hits this query produces if
//     *             run without min/max rank.
//     * @param numSnippets
//     *             The target number of snippets. Might produce less if all free
//     *             text search hits are processed.  Might produce more
//     *             if the rank chunk is big enough to produce excess snippets
//     *             (the entire rank chunk is always processed).
//     */
//    private Collection<BindingSet> processRankChunks(
//            final RepositoryConnection cxn, 
//            final String queryTemplate,
//            final int numFreeTextHits,
//            final int numSnippets) throws Exception {
//        
//        final Collection<BindingSet> result = new LinkedList<BindingSet>();
//        
//        // keep track of the # of snippets
//        final Set<IV> snippets = new LinkedHashSet<IV>();
//        
//        // the size of the rank chunks
//        final int chunkSize = 1000;
//        
//        int minRank = 1;
//        int maxRank = chunkSize;
//        
//        // keep doing chunks while we haven't reached our snippet goal and
//        // we haven't run out of free text search results.
//        while (snippets.size() < numSnippets && minRank < numFreeTextHits) {
//            
//            final TupleQuery tupleQuery = cxn.prepareTupleQuery(
//                    QueryLanguage.SPARQL, 
//                    queryTemplate
//                        .replace("MINRANK", String.valueOf(minRank))
//                        .replace("MAXRANK", String.valueOf(maxRank)));
//            
//            tupleQuery.setIncludeInferred(true /* includeInferred */);
//            
//            final TupleQueryResult chunk = tupleQuery.evaluate();
//            
//            while (chunk.hasNext()) {
//                
//                final BindingSet bs = chunk.next();
//                
//                final BigdataValue val = (BigdataValue) 
//                    bs.getBinding("target").getValue();
//                
//                final IV iv = val.getIV();
//                
//                // LinkedHashSet<IV> will guarantee uniqueness
//                snippets.add(iv);
//                
//                result.add(bs);
//                
//            }
//            
//            minRank = maxRank+1;
//            maxRank = maxRank+chunkSize;
//            
//        }
//        
//        return result;
//        
//    }
    
    /*

prefix BIGDATA_QUERY_HINTS: <http://www.bigdata.com/queryHints#com.bigdata.rdf.sail.QueryHints.optimizer=None>
prefix xsd: <http://www.w3.org/2001/XMLSchema#> 
select distinct ?target0  
where { 
  ?obj0 <http://www.bigdata.com/rdf/search#search> "stainless" .
  ?target0 ?p0 ?obj0 .
  ?obj1 <http://www.bigdata.com/rdf/search#search> "innovations" .
  ?target1 ?p1 ?obj1 .
  ?obj2 <http://www.bigdata.com/rdf/search#search> "cabin" .
  ?target2 ?p2 ?obj2 .
  filter(?target0 = ?target1 && ?target1 = ?target2) .
}  
		     
  FILTER (?category = <http://www.ms2w.com/ontologies/autocad/AutoCADBlock>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockAttribute>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockReference>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADFile>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADTable>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADTitleBlock>  || ?category = <http://www.ms2w.com/ontologies/file/Directory>  || ?category = <http://www.ms2w.com/ontologies/file/File>  || ?category = <http://www.ms2w.com/ontologies/pdffile/PdfAnnotation>  || ?category = <http://www.ms2w.com/ontologies/pdffile/PdfFile>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentAssembly>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeBOM>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/Component>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/Configuration>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentDrawing>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeIndented>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentPart>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypePartsOnly>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeRevision>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxCopiedPart>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxStandardPart>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeTopLevelOnly> ) . 

prefix BIGDATA_QUERY_HINTS: <http://www.bigdata.com/queryHints#com.bigdata.rdf.sail.QueryHints.optimizer=None>
prefix xsd: <http://www.w3.org/2001/XMLSchema#> 
select distinct ?target  
where { 
  ?obj0 <http://www.bigdata.com/rdf/search#search> "stainless" . 
  ?obj0 <http://www.bigdata.com/rdf/search#relevance> ?score0 . 
  ?target ?p0 ?obj0 .  
  ?target <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?category0 . 
  {	
    ?obj1 <http://www.bigdata.com/rdf/search#search> "innovations" . 
    ?obj1 <http://www.bigdata.com/rdf/search#relevance> ?score1 . 
    ?target ?p1 ?obj1 .  
    ?target <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?category1 . 
  }
  {
    ?obj2 <http://www.bigdata.com/rdf/search#search> "cabin" . 
    ?obj2 <http://www.bigdata.com/rdf/search#relevance> ?score2 . 
    ?target ?p2 ?obj2 .  
    ?target <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?category2 . 
  }
}  
ORDER BY DESC(?score2) DESC(?score1) DESC(?score0)  
LIMIT 10 OFFSET 0

  ?target <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?category0 . 
  
  ?target <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?category1 . 
  
  ?target <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?category2 . 

  FILTER (?category0 = ... ) . 
  FILTER (?category1 = ... ) . 
  FILTER (?category2 = ... ) . 

		  FILTER (?category = <http://www.ms2w.com/ontologies/autocad/AutoCADBlock>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockAttribute>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockReference>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADFile>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADTable>  || ?category = <http://www.ms2w.com/ontologies/autocad/AutoCADTitleBlock>  || ?category = <http://www.ms2w.com/ontologies/file/Directory>  || ?category = <http://www.ms2w.com/ontologies/file/File>  || ?category = <http://www.ms2w.com/ontologies/pdffile/PdfAnnotation>  || ?category = <http://www.ms2w.com/ontologies/pdffile/PdfFile>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentAssembly>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeBOM>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/Component>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/Configuration>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentDrawing>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeIndented>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentPart>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypePartsOnly>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeRevision>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxCopiedPart>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxStandardPart>  || ?category = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeTopLevelOnly> ) . 
		  FILTER (?category0 = <http://www.ms2w.com/ontologies/autocad/AutoCADBlock>  || ?category0 = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockAttribute>  || ?category0 = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockReference>  || ?category0 = <http://www.ms2w.com/ontologies/autocad/AutoCADFile>  || ?category0 = <http://www.ms2w.com/ontologies/autocad/AutoCADTable>  || ?category0 = <http://www.ms2w.com/ontologies/autocad/AutoCADTitleBlock>  || ?category0 = <http://www.ms2w.com/ontologies/file/Directory>  || ?category0 = <http://www.ms2w.com/ontologies/file/File>  || ?category0 = <http://www.ms2w.com/ontologies/pdffile/PdfAnnotation>  || ?category0 = <http://www.ms2w.com/ontologies/pdffile/PdfFile>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentAssembly>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeBOM>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/Component>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/Configuration>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentDrawing>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeIndented>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentPart>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypePartsOnly>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeRevision>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxCopiedPart>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxStandardPart>  || ?category0 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeTopLevelOnly> ) . 
		  FILTER (?category1 = <http://www.ms2w.com/ontologies/autocad/AutoCADBlock>  || ?category1 = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockAttribute>  || ?category1 = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockReference>  || ?category1 = <http://www.ms2w.com/ontologies/autocad/AutoCADFile>  || ?category1 = <http://www.ms2w.com/ontologies/autocad/AutoCADTable>  || ?category1 = <http://www.ms2w.com/ontologies/autocad/AutoCADTitleBlock>  || ?category1 = <http://www.ms2w.com/ontologies/file/Directory>  || ?category1 = <http://www.ms2w.com/ontologies/file/File>  || ?category1 = <http://www.ms2w.com/ontologies/pdffile/PdfAnnotation>  || ?category1 = <http://www.ms2w.com/ontologies/pdffile/PdfFile>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentAssembly>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeBOM>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/Component>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/Configuration>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentDrawing>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeIndented>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentPart>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypePartsOnly>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeRevision>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxCopiedPart>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxStandardPart>  || ?category1 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeTopLevelOnly> ) . 
		  FILTER (?category2 = <http://www.ms2w.com/ontologies/autocad/AutoCADBlock>  || ?category2 = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockAttribute>  || ?category2 = <http://www.ms2w.com/ontologies/autocad/AutoCADBlockReference>  || ?category2 = <http://www.ms2w.com/ontologies/autocad/AutoCADFile>  || ?category2 = <http://www.ms2w.com/ontologies/autocad/AutoCADTable>  || ?category2 = <http://www.ms2w.com/ontologies/autocad/AutoCADTitleBlock>  || ?category2 = <http://www.ms2w.com/ontologies/file/Directory>  || ?category2 = <http://www.ms2w.com/ontologies/file/File>  || ?category2 = <http://www.ms2w.com/ontologies/pdffile/PdfAnnotation>  || ?category2 = <http://www.ms2w.com/ontologies/pdffile/PdfFile>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentAssembly>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeBOM>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/Component>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/Configuration>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentDrawing>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeIndented>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmDocumentPart>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypePartsOnly>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmTableTypeRevision>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxCopiedPart>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmToolboxStandardPart>  || ?category2 = <http://www.ms2w.com/ontologies/solidworks/20091023/swDmBOMTableTypeTopLevelOnly> ) . 
	*/

    /**
     * FIXME This should be migrated to the data-driven test suite in
     * {@link TestSearch}.
     * 
     * @see TestSearch
     */
    public void testExactMatch() throws Exception {
        
        final BigdataSail sail = getSail();
        try {
            
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        
        try {
            
            cxn.setAutoCommit(false);

            final ValueFactory vf = sail.getValueFactory();

        	final URI s1 = vf.createURI(BDS.NAMESPACE+"s1");
        	final URI s2 = vf.createURI(BDS.NAMESPACE+"s2");
        	final URI s3 = vf.createURI(BDS.NAMESPACE+"s3");
        	final URI s4 = vf.createURI(BDS.NAMESPACE+"s4");
        	final URI s5 = vf.createURI(BDS.NAMESPACE+"s5");
        	final URI s6 = vf.createURI(BDS.NAMESPACE+"s6");
        	final URI s7 = vf.createURI(BDS.NAMESPACE+"s7");
        	final URI s8 = vf.createURI(BDS.NAMESPACE+"s8");
        	final Literal l1 = vf.createLiteral("0");
        	final Literal l2 = vf.createLiteral("0 0");
        	final Literal l3 = vf.createLiteral("0 1 0");
        	final Literal l4 = vf.createLiteral("0,0");
        	final Literal l5 = vf.createLiteral("1 2 0 0 3");
        	final Literal l6 = vf.createLiteral("brown cow");
        	final Literal l7 = vf.createLiteral("how now brown cow");
        	final Literal l8 = vf.createLiteral("toilet");
        	
            cxn.add(s1, RDFS.LABEL, l1);
            cxn.add(s2, RDFS.LABEL, l2);
            cxn.add(s3, RDFS.LABEL, l3);
            cxn.add(s4, RDFS.LABEL, l4);
            cxn.add(s5, RDFS.LABEL, l5);
            cxn.add(s6, RDFS.LABEL, l6);
            cxn.add(s7, RDFS.LABEL, l7);
            cxn.add(s8, RDFS.LABEL, l8);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            final Map<IV, Literal> literals = new LinkedHashMap<IV, Literal>();
            literals.put(((BigdataValue)l1).getIV(), l1);
            literals.put(((BigdataValue)l2).getIV(), l2);
            literals.put(((BigdataValue)l3).getIV(), l3);
            literals.put(((BigdataValue)l4).getIV(), l4);
            literals.put(((BigdataValue)l5).getIV(), l5);
            literals.put(((BigdataValue)l6).getIV(), l6);
            literals.put(((BigdataValue)l7).getIV(), l7);
            literals.put(((BigdataValue)l8).getIV(), l8);
            
            final Map<IV, URI> uris = new LinkedHashMap<IV, URI>();
            uris.put(((BigdataValue)l1).getIV(), s1);
            uris.put(((BigdataValue)l2).getIV(), s2);
            uris.put(((BigdataValue)l3).getIV(), s3);
            uris.put(((BigdataValue)l4).getIV(), s4);
            uris.put(((BigdataValue)l5).getIV(), s5);
            uris.put(((BigdataValue)l6).getIV(), s6);
            uris.put(((BigdataValue)l7).getIV(), s7);
            uris.put(((BigdataValue)l8).getIV(), s8);
            
/**/            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }
            
            { 
            	final String searchQuery = "0 0";
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BDS.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BDS.MATCH_EXACT+"> \"true\" . " +
                    "    ?o <"+BDS.RELEVANCE+"> ?score . " +
                    "} " +
                    "order by desc(?score)";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    int i = 0;
                    while (result.hasNext()) {
                        final BindingSet tmp = result.next();
                        if (log.isInfoEnabled())
                            log.info(i + ": " + tmp.toString());
                        i++;
                    }
                    assertEquals("wrong # of results", 2, i);
                } finally {
                    result.close();
                }
                
                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	cxn.getTripleStore().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(new FullTextQuery(
                			searchQuery, 
                            null, // languageCode
                            BDS.DEFAULT_PREFIX_MATCH,//false, // prefixMatch
                            null,
                            BDS.DEFAULT_MATCH_ALL_TERMS,//false, // matchAllTerms
                            true, // matchExact
                            BDS.DEFAULT_MIN_RELEVANCE,//0d, // minCosine
                            BDS.DEFAULT_MAX_RELEVANCE,//1.0d, // maxCosine
                            BDS.DEFAULT_MIN_RANK,//1
                            BDS.DEFAULT_MAX_RANK,//10000, // maxRank (=maxResults + 1)
                            BDS.DEFAULT_TIMEOUT,//1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            ));
                
                while (hits.hasNext()) {
                	final IHit<?> hit = hits.next();
                	final IV id = (IV<?, ?>)hit.getDocId();
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                	if(log.isInfoEnabled())
                		log.info(bs);
                    answer.add(bs);
                }
                
                result = tupleQuery.evaluate();
                compare(result, answer);

            }
            
        } finally {
            cxn.close();
        }
        } finally {
            sail.__tearDownUnitTest();
        }
        
    }
    

}
