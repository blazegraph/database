/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Apr 15, 2008
 */

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSDDoubleIV;
import com.bigdata.rdf.lexicon.ITextIndexer;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.BD;
import com.bigdata.search.Hiterator;
import com.bigdata.search.IHit;

/**
 * Test suite for high-level query against a graph containing statements about
 * statements.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSearchQuery extends ProxyBigdataSailTestCase {

	protected static final Logger log = Logger.getLogger(TestSearchQuery.class);
	
    public TestSearchQuery() {
        
    }
    
    public TestSearchQuery(String name) {
        super(name);
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

//    /**
//     * Overriden to use a persistent backing store.
//     */
//    public Properties getProperties() {
//        
//        Properties properties = super.getProperties();
//        
//        // use a disk-based mode since we will re-open the store to test restart safety.
//        properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
//
//        properties.setProperty(Options.FILE,file.toString());
//        
//        return properties;
//        
//    }

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
    
    public void test_query() throws SailException, IOException,
            RDFHandlerException, QueryEvaluationException {

        // overridden to use a disk-backed file.
        final Properties properties = super.getProperties();

        // use a disk-based mode since we will re-open the store to test restart
        // safety.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "false");

        properties.setProperty(Options.FILE, file.toString());

        BigdataSail sail = getSail(properties);

        try {

            sail.initialize();
            
//            if (!sail.database.getStatementIdentifiers()) {
//
//                log.warn("Statement identifiers are not enabled");
//
//                return;
//
//            }

            /*
             * Load data into the sail.
             */
            {

                StatementBuffer sb = new StatementBuffer(sail.database, 100/* capacity */);

                sb.add(new URIImpl("http://www.bigdata.com/A"), RDFS.LABEL,
                        new LiteralImpl("Yellow Rose"));

                sb.add(new URIImpl("http://www.bigdata.com/B"), RDFS.LABEL,
                        new LiteralImpl("Red Rose"));

                sb.add(new URIImpl("http://www.bigdata.com/C"), RDFS.LABEL,
                        new LiteralImpl("Old Yellow House"));

                sb.flush();

                /*
                 * Commit the changes to the database.
                 */
                sail.getDatabase().commit();

            }

            if (log.isInfoEnabled())
                log
                        .info("#statements before search: "
                                + sail.database.getStatementCount(null/* c */,
                                        true/* exact */));

            doSearchTest(((BigdataSail) sail).getConnection());

            doSearchTest(((BigdataSail) sail).getReadOnlyConnection());

            if (log.isInfoEnabled())
                log
                        .info("#statements before restart: "
                                + sail.database.getStatementCount(null/* c */,
                                        true/* exact */));

            // re-open the SAIL.
            sail = reopenSail(sail);

            sail.initialize();

            if (log.isInfoEnabled())
                log
                        .info("#statements after restart: "
                                + sail.database.getStatementCount(null/* c */,
                                        true/* exact */));

            doSearchTest(((BigdataSail) sail).getConnection());

            doSearchTest(((BigdataSail) sail).getReadOnlyConnection());
            
        } finally {

            sail.__tearDownUnitTest();

        }
        
    }

    /**
     * This runs a hand-coded query corresponding to a SPARQL query using the
     * {@link BD#SEARCH} magic predicate.
     * 
     * <pre>
     * select ?evidence
     * where
     * { ?evidence rdf:type &lt;the type&gt; .
     *   ?evidence ?anypredicate ?label .
     *   ?label bigdata:search &quot;the query&quot; .
     * }
     * </pre>
     */
    protected void doSearchTest(SailConnection conn) throws SailException,
            QueryEvaluationException {
      
        try {

            final StatementPattern sp = new StatementPattern(//
                    new Var("X"),//
                    new Var("1", BD.SEARCH),//
                    new Var("2", new LiteralImpl("Yellow"))//
                    );
            final TupleExpr tupleExpr = 
                new QueryRoot(
                        new Projection(
                                sp, 
                                new ProjectionElemList(new ProjectionElem("X"))));

            /*
             * Create a data set consisting of the contexts to be queried.
             * 
             * Note: a [null] DataSet will cause context to be ignored when the
             * query is processed.
             */
            final DatasetImpl dataSet = null; //new DatasetImpl();

            final BindingSet bindingSet = new QueryBindingSet();

            final CloseableIteration<? extends BindingSet, QueryEvaluationException> itr = conn
                    .evaluate(tupleExpr, dataSet, bindingSet, true/* includeInferred */);

            try {

            log.info("Verifying query.");

                /*
                 * These are the expected results for the query (the bindings
                 * for X).
                 */

                final Set<Value> expected = new HashSet<Value>();

                expected.add(new LiteralImpl("Yellow Rose"));

                expected.add(new LiteralImpl("Old Yellow House"));

                /*
                 * Verify that the query results is the correct solutions.
                 */

                final int nresults = expected.size();

                int i = 0;

                while (itr.hasNext()) {

                    final BindingSet solution = itr.next();

                    System.out.println("solution[" + i + "] : " + solution);

                    final Value actual = solution.getValue("X");

                    System.out.println("X[" + i + "] = " + actual + " ("
                            + actual.getClass().getName() + ")");

                    assertTrue("Not expecting X=" + actual, expected
                            .remove(actual));

                    i++;

                }

                assertEquals("#results", nresults, i);

            } finally {

                itr.close();

            }

        } finally {

            conn.close();

        }

    }

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
        
        // the ontology (nothing is indexed for full text search).
        final Graph test_restart_1 = new GraphImpl(); {

            test_restart_1.add(new StatementImpl(ORGANIZATION, RDFS.SUBCLASSOF, ENTITY));
            
        }

        // the entity data (the rdfs:label gets indexed for full text search)
        final Graph test_restart_2 = new GraphImpl();
        {
            
            test_restart_2
                    .add(new StatementImpl(SYSTAP, RDF.TYPE, ENTITY));
         
            test_restart_2.add(new StatementImpl(SYSTAP, RDFS.LABEL,
                    new LiteralImpl("SYSTAP")));
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
                    final RepositoryConnection cxn = repo.getConnection();
                    cxn.setAutoCommit(false);
                    try {
                        log.info("loading ontology");
                        cxn.add(test_restart_1);
                        if (!doYouWantMeToBreak) {
                            // optionally load the entity data here.
                            log.info("loading entity data");
                            cxn.add(test_restart_2);
                        }
                        cxn.commit();
                    } catch (Exception ex) {
                        cxn.rollback();
                        throw ex;
                    } finally {
                        cxn.close();
                    }
                }

                if (doYouWantMeToBreak) {
                    // load the entity data.
                    final RepositoryConnection cxn = repo.getConnection();
                    cxn.setAutoCommit(false);
                    try {
                        log.info("loading entity data");
                        cxn.add(test_restart_2);
                        cxn.commit();
                    } catch (Exception ex) {
                        cxn.rollback();
                        throw ex;
                    } finally {
                        cxn.close();
                    }
                }

                { // run the query (free text search)
                    final String query = "construct { ?s <" + RDF.TYPE + "> <"
                            + ENTITY + "> . } " + "where     { ?s <" + RDF.TYPE
                            + "> <" + ENTITY + "> . ?s ?p ?lit . ?lit <"
                            + BD.SEARCH + "> \"systap\" . }";
                    final RepositoryConnection cxn = repo.getConnection();
                    try {
                        // silly construct queries, can't guarantee distinct
                        // results
                        final Set<Statement> results = new LinkedHashSet<Statement>();
                        final GraphQuery graphQuery = cxn.prepareGraphQuery(
                                QueryLanguage.SPARQL, query);
                        graphQuery.evaluate(new StatementCollector(results));
                        for (Statement stmt : results) {
                            log.info(stmt);
                        }
                        /*
                         * @todo this test is failing : review with MikeP and
                         * figure out if it is the test or the system under
                         * test.
                         */
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
                    final String query = "construct { ?s <" + RDF.TYPE + "> <"
                            + ENTITY + "> . } " + "where     { ?s <" + RDF.TYPE
                            + "> <" + ENTITY + "> . ?s ?p ?lit . ?lit <"
                            + BD.SEARCH + "> \"systap\" . }";
                    final RepositoryConnection cxn = repo.getConnection();
                    try {
                        // silly construct queries, can't guarantee distinct
                        // results
                        final Set<Statement> results = new LinkedHashSet<Statement>();
                        final GraphQuery graphQuery = cxn.prepareGraphQuery(
                                QueryLanguage.SPARQL, query);
                        graphQuery.evaluate(new StatementCollector(results));
                        for (Statement stmt : results) {
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
    
    public void testWithNamedGraphs() throws Exception {
        
        final BigdataSail sail = getSail();
        try {
            
        if (sail.getDatabase().isQuads() == false) {
            return;
        }
        
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
            
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final Literal alice = new LiteralImpl("Alice");
            final Literal bob = new LiteralImpl("Bob");
            final URI graphA = new URIImpl("http://www.bigdata.com/graphA");
            final URI graphB = new URIImpl("http://www.bigdata.com/graphB");
            
/**/            
            cxn.add(
                    a,
                    RDFS.LABEL,
                    alice,
                    graphA
                    );
            
            /*
             * Graph B.
             */
            cxn.add(
                    b,
                    RDFS.LABEL,
                    bob,
                    graphB
                    );
/**/

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
/**/            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }
            
            { // run the query with no graphs specified
                final String query = 
                    "select ?s " + 
                    "from <"+graphA+"> " +
                    "where " +
                    "{ " +
                    "    ?s <"+BD.SEARCH+"> \"Alice\" . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                answer.add(createBindingSet(new BindingImpl("s", alice)));
                
                compare(result, answer);
            }

            { // run the query with graphA specified as the default graph
                final String query = 
                    "select ?s " + 
                    "from <"+graphA+"> " +
                    "where " +
                    "{ " +
                    "    ?s <"+BD.SEARCH+"> \"Alice\" . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                answer.add(createBindingSet(new BindingImpl("s", alice)));
                
                compare(result, answer);
            }

            { // run the query with graphB specified as the default graph
                final String query = 
                    "select ?s " + 
                    "from <"+graphB+"> " +
                    "where " +
                    "{ " +
                    "    ?s <"+BD.SEARCH+"> \"Alice\" . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                //answer.add(createBindingSet(new BindingImpl("s", alice)));
                
                compare(result, answer);
            }

            { // run the query with graphB specified as the default graph
                final String query = 
                    "select ?s ?o " + 
                    "from <"+graphB+"> " +
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BD.SEARCH+"> \"Alice\" . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                //answer.add(createBindingSet(new BindingImpl("s", alice)));
                
                compare(result, answer);
            }

            { // run the query with graphB specified as the default graph
                final String query = 
                    "select ?s ?o " + 
                    "from <"+graphB+"> " +
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o1 . " +
                    "    ?o <"+BD.SEARCH+"> \"Alice\" . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                //answer.add(createBindingSet(new BindingImpl("s", alice)));
                
                compare(result, answer);
            }

        } finally {
            cxn.close();
        }
        } finally {
            sail.__tearDownUnitTest();
        }
        
    }

    public void testWithMetadata() throws Exception {
        
        final BigdataSail sail = getSail();
        try {
            
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
            
        	final ValueFactory vf = sail.getValueFactory();

        	final URI s1 = vf.createURI(BD.NAMESPACE+"s1");
        	final URI s2 = vf.createURI(BD.NAMESPACE+"s2");
        	final URI s3 = vf.createURI(BD.NAMESPACE+"s3");
        	final URI s4 = vf.createURI(BD.NAMESPACE+"s4");
        	final URI s5 = vf.createURI(BD.NAMESPACE+"s5");
        	final URI s6 = vf.createURI(BD.NAMESPACE+"s6");
        	final URI s7 = vf.createURI(BD.NAMESPACE+"s7");
        	final URI s8 = vf.createURI(BD.NAMESPACE+"s8");
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
                log.info("\n" + sail.getDatabase().dumpStore());
            }
            
            { 
            	final String searchQuery = "how now brown cow";
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BD.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BD.RELEVANCE+"> ?score . " +
                    "} " +
                    "order by desc(?score)";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                int i = 0;
                while (result.hasNext()) {
                	System.err.println(i++ + ": " + result.next().toString());
                }
                assertTrue("wrong # of results", i == 7);
                
                result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	sail.getDatabase().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(searchQuery, 
                            null, // languageCode
                            false, // prefixMatch
                            0d, // minCosine
                            10000, // maxRank (=maxResults + 1)
                            false, // matchAllTerms
                            1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            );
                
                while (hits.hasNext()) {
                	final IHit hit = hits.next();
                	final IV id = new TermId(VTE.LITERAL, hit.getDocId());
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                	System.err.println(bs);
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
                    "    ?o <"+BD.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BD.RELEVANCE+"> ?score . " +
//                    "    ?o <"+BD.MIN_RELEVANCE+"> \"0.6\" . " +
                    "    ?o <"+BD.MAX_HITS+"> \""+maxHits+"\" . " +
                    "} " +
                    "order by desc(?score)";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                int i = 0;
                while (result.hasNext()) {
                	System.err.println(i++ + ": " + result.next().toString());
                }
                assertTrue("wrong # of results", i == 5);
                
                result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	sail.getDatabase().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(searchQuery, 
                            null, // languageCode
                            false, // prefixMatch
                            0d, // minCosine
                            maxHits+1, // maxRank (=maxResults + 1)
                            false, // matchAllTerms
                            1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            );
                
                while (hits.hasNext()) {
                	final IHit hit = hits.next();
                	final IV id = new TermId(VTE.LITERAL, hit.getDocId());
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                	System.err.println(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { 
            	final String searchQuery = "how now brown cow";
            	final double minRelevance = 0.6d;
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BD.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BD.RELEVANCE+"> ?score . " +
                    "    ?o <"+BD.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
//                    "    ?o <"+BD.MAX_HITS+"> \"5\" . " +
                    "} " +
                    "order by desc(?score)";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                int i = 0;
                while (result.hasNext()) {
                	System.err.println(i++ + ": " + result.next().toString());
                }
                assertTrue("wrong # of results", i == 3);
                
                result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	sail.getDatabase().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(searchQuery, 
                            null, // languageCode
                            false, // prefixMatch
                            minRelevance, // minCosine
                            10000, // maxRank (=maxResults + 1)
                            false, // matchAllTerms
                            1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            );
                
                while (hits.hasNext()) {
                	final IHit hit = hits.next();
                	final IV id = new TermId(VTE.LITERAL, hit.getDocId());
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                	System.err.println(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { // exact match
            	
            	final String searchQuery = "brown cow";
            	final double minRelevance = 0.0d;
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BD.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BD.RELEVANCE+"> ?score . " +
//                    "    ?o <"+BD.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
//                    "    ?o <"+BD.MAX_HITS+"> \"5\" . " +
                    "    filter regex(?o, \""+searchQuery+"\") " +
                    "} " +
                    "order by desc(?score)";
                
                log.info("\n"+query);
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                int i = 0;
                while (result.hasNext()) {
                	log.info(i++ + ": " + result.next().toString());
                }
                assertTrue("wrong # of results: " + i, i == 2);
                
                result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	sail.getDatabase().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(searchQuery, 
                            null, // languageCode
                            false, // prefixMatch
                            minRelevance, // minCosine
                            10000, // maxRank (=maxResults + 1)
                            false, // matchAllTerms
                            1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            );
                
                while (hits.hasNext()) {
                	final IHit hit = hits.next();
                	final IV id = new TermId(VTE.LITERAL, hit.getDocId());
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                	if (!o.getLabel().contains(searchQuery))
                		continue;
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                    log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { // prefix match
            	
            	final String searchQuery = "bro*";
            	final double minRelevance = 0.0d;
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BD.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BD.RELEVANCE+"> ?score . " +
//                    "    ?o <"+BD.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
//                    "    ?o <"+BD.MAX_HITS+"> \"5\" . " +
//                    "    filter regex(?o, \""+searchQuery+"\") " +
                    "} " +
                    "order by desc(?score)";
                
                log.info("\n"+query);
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                int i = 0;
                while (result.hasNext()) {
                	log.info(i++ + ": " + result.next().toString());
                }
                assertTrue("wrong # of results: " + i, i == 3);
                
                result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	sail.getDatabase().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(searchQuery, 
                            null, // languageCode
                            true, // prefixMatch
                            minRelevance, // minCosine
                            10000, // maxRank (=maxResults + 1)
                            false, // matchAllTerms
                            1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            );
                
                while (hits.hasNext()) {
                	final IHit hit = hits.next();
                	final IV id = new TermId(VTE.LITERAL, hit.getDocId());
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                    log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { // prefix match using a stopword
            	
            	final String searchQuery = "to*";
            	final double minRelevance = 0.0d;
            	
                final String query = 
                    "select ?s ?o ?score " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BD.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BD.RELEVANCE+"> ?score . " +
//                    "    ?o <"+BD.MIN_RELEVANCE+"> \""+minRelevance+"\" . " +
//                    "    ?o <"+BD.MAX_HITS+"> \"5\" . " +
//                    "    filter regex(?o, \""+searchQuery+"\") " +
                    "} " +
                    "order by desc(?score)";
                
                log.info("\n"+query);
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                int i = 0;
                while (result.hasNext()) {
                	log.info(i++ + ": " + result.next().toString());
                }
                assertTrue("wrong # of results: " + i, i == 1);
                
                result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	sail.getDatabase().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(searchQuery, 
                            null, // languageCode
                            true, // prefixMatch
                            minRelevance, // minCosine
                            10000, // maxRank (=maxResults + 1)
                            false, // matchAllTerms
                            1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            );
                
                while (hits.hasNext()) {
                	final IHit hit = hits.next();
                	final IV id = new TermId(VTE.LITERAL, hit.getDocId());
                	final Literal score = vf.createLiteral(hit.getCosine());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o),
                    		new BindingImpl("score", score));
                    log.info(bs);
                    answer.add(bs);
                }
                
                compare(result, answer);

            }

            { // match all terms
            	
            	final String searchQuery = "how now brown cow";
            	final double minRelevance = 0.0d;
            	
                final String query = 
                    "select ?s ?o " + 
                    "where " +
                    "{ " +
                    "    ?s <"+RDFS.LABEL+"> ?o . " +
                    "    ?o <"+BD.SEARCH+"> \""+searchQuery+"\" . " +
                    "    ?o <"+BD.MATCH_ALL_TERMS+"> \"true\" . " +
                    "}";
                
                log.info("\n"+query);
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();

                int i = 0;
                while (result.hasNext()) {
                	log.info(i++ + ": " + result.next().toString());
                }
//                assertTrue("wrong # of results: " + i, i == 1);
                
                result = tupleQuery.evaluate();

                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                
                final ITextIndexer search = 
                	sail.getDatabase().getLexiconRelation().getSearchEngine();
                final Hiterator<IHit> hits = 
                	search.search(searchQuery, 
                            null, // languageCode
                            true, // prefixMatch
                            minRelevance, // minCosine
                            10000, // maxRank (=maxResults + 1)
                            true, // matchAllTerms
                            1000L, // timeout 
                            TimeUnit.MILLISECONDS // unit
                            );
                
                while (hits.hasNext()) {
                	final IHit hit = hits.next();
                	final IV id = new TermId(VTE.LITERAL, hit.getDocId());
                	final URI s = uris.get(id);
                	final Literal o = literals.get(id);
                    final BindingSet bs = createBindingSet(
                    		new BindingImpl("s", s),
                    		new BindingImpl("o", o));
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
    
}
