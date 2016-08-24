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
/* Portions of this code are:
 *
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2011.
 *
 * Licensed under the Aduna BSD-style license.
 */
/*
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sail.tck;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import junit.framework.TestCase2;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for BIGDATA extension to SPARQL UPDATE for NAMED SOLUTION SETS.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/524"> SPARQL
 *      Cache </a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/531"> SPARQL
 *      UPDATE Extensions (Trac) </a>
 * @see <a
 *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=SPARQL_Update">
 *      SPARQL Update Extensions (Wiki) </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @openrdf
 * @version $Id: BigdataSPARQLUpdateTest.java 6204 2012-03-28 20:17:06Z
 *          thompsonbry $
 */
public class BigdataSPARQLUpdateTest2 extends TestCase2 {

    static private final Logger log = Logger
            .getLogger(BigdataSPARQLUpdateTest2.class);

    public BigdataSPARQLUpdateTest2() {
    }

    public BigdataSPARQLUpdateTest2(final String name) {
        super(name);
    }

    /**
     * The file path name for the package in which the data files are found.
     */
    protected final String packagePath = "file:src/test/java/com/bigdata/rdf/sail/tck/data/";
    
    /**
     * Note: This field MUST be cleared in tearDown or a hard reference will be
     * retained to the backend until the end of CI!
     */
    private IIndexManager backend = null;

    private Repository rep;

    protected RepositoryConnection con;

//    private ValueFactory f;

    /**
     * @throws java.lang.Exception
     */
    //@Before
    public void setUp()
        throws Exception
    {

        rep = createRepository();

        con = rep.getConnection();
        con.setAutoCommit(false);
        
//        f = rep.getValueFactory();

    }

    /**
     * @throws java.lang.Exception
     */
    public void tearDown()
        throws Exception
    {

        con.close();
        con = null;

        rep.shutDown();
        rep = null;

        if (backend != null) {

            backend.destroy();
            backend = null;
            
        }

    }

    protected TupleQueryResult readExpectedTupleQueryResult(
            final String resultFileURL) throws Exception {

        final TupleQueryResultFormat tqrFormat = QueryResultIO
                .getParserFormatForFileName(resultFileURL);

        if (tqrFormat == null)
            throw new RuntimeException("Format not found: resource="
                    + resultFileURL);

        final InputStream in = getResourceAsStream(resultFileURL);

        try {

            final TupleQueryResultParser parser = QueryResultIO
                    .createParser(tqrFormat);

//            parser.setValueFactory(store.getValueFactory());

            final TupleQueryResultBuilder qrBuilder = new TupleQueryResultBuilder();

            parser.setTupleQueryResultHandler(qrBuilder);

            parser.parse(in);

            return qrBuilder.getQueryResult();

        } finally {

            in.close();

        }
    
    }
    
    protected InputStream getResourceAsStream(final String resource) {

        // try the classpath
        InputStream is = getClass().getResourceAsStream(resource);

        if (is == null) {

            // Searching for the resource from the root of the class
            // returned
            // by getClass() (relative to the class' package) failed.
            // Next try searching for the desired resource from the root
            // of the jar; that is, search the jar file for an exact match
            // of the input string.
            is = getClass().getClassLoader().getResourceAsStream(resource);

        }

        if (is == null) {

            final File file = new File(resource);

            if (file.exists()) {

                try {

                    is = new FileInputStream(resource);

                } catch (FileNotFoundException e) {

                    throw new RuntimeException(e);

                }

            }

        }

        if (is == null) {

            try {

                is = new URL(resource).openStream();

            } catch (MalformedURLException e) {

                /*
                 * Ignore. we will handle the problem below if this was not
                 * a URL.
                 */

            } catch (IOException e) {

                throw new RuntimeException(e);

            }

        }

        if (is == null)
            throw new RuntimeException("Not found: " + resource);

        return is;

    }

    /**
     * Return the contents of the resource.
     * 
     * @param resource
     *            The resource.
     * 
     * @return It's contents.
     */
    protected String getResourceAsString(final String resource) {

        final StringBuilder sb = new StringBuilder();

        final InputStream is = getResourceAsStream(resource);

        if (is == null)
            throw new RuntimeException("Not found: " + resource);

        try {

            final LineNumberReader r = new LineNumberReader(
                    new InputStreamReader(is));

            String s;
            while ((s = r.readLine()) != null) {

                sb.append(s);

                sb.append("\n");

            }

            return sb.toString();

        } catch (IOException e) {

            throw new RuntimeException(e);

        } finally {

            try {

                if (is != null)
                    is.close();

            } catch (IOException e) {

                throw new RuntimeException(e);

            }

        }

    }

	protected void compareTupleQueryResults(
			final TupleQueryResult queryResult,
			final TupleQueryResult expectedResult
			)
			throws QueryEvaluationException {

		AbstractQueryEngineTestCase.compareTupleQueryResults(
				getName(),
				getName(),//testURI,
				null, // store
				null, // astContainer
				queryResult,//
				expectedResult,//
				false,// laxCardinality,
				false // checkOrder
				);

	}

	/**
     * Note: This method may be overridden in order to run the test suite
     * against other variations of the bigdata backend.
     */
    public Properties getProperties() {

        final Properties props = new Properties(super.getProperties());

        // Base version of the test uses the MemStore.
        props.setProperty(Options.BUFFER_MODE, BufferMode.MemStore.toString());
        
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
        
        return props;
        
    }

//    @Override
	protected Repository createRepository() throws Exception {

		Repository repo = newRepository();

		repo.initialize();

		return repo;

	}

//    @Override
    protected Repository newRepository() throws RepositoryException {

        final Properties props = getProperties();
        
        final BigdataSail sail = new BigdataSail(props);
        
        backend = sail.getIndexManager();

        return new BigdataSailRepository(sail);

    }

    /**
     * LOAD the data set.
     * 
     * @param datasetFile
     *            The base name of a file.
     * 
     * @throws MalformedQueryException
     * @throws UpdateExecutionException
     * @throws RepositoryException
     * 
     */
    protected void loadDataset(final String datasetFile)
            throws UpdateExecutionException, MalformedQueryException,
            RepositoryException {
    	
        final String updateStr = "LOAD <" + datasetFile + ">";

        con.prepareUpdate(QueryLanguage.SPARQL, updateStr).execute();

    }

    /**
     * Return <code>true</code> iff the SPARQL UPDATE for NAMED SOLUTION SETS
     * test should run.
     * 
     * FIXME SPARQL UPDATE for NAMED SOLUTION SETS is NOT SUPPORTED for
     * read/write transactions (against isolatable indices). This is a GIST
     * issue. See {@link AbstractTask} and the GIST ticket for blocking issues.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/531">
     *      SPARQL UPDATE Extensions (Trac) </a>
     * @see <a
     *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=SPARQL_Update">
     *      SPARQL Update Extensions (Wiki) </a>
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585"> GIST
     *      </a>
     */
    protected boolean isSolutionSetUpdateEnabled() {

        final AbstractTripleStore tripleStore = ((BigdataSailRepositoryConnection) con).getTripleStore();
        
        final boolean isolatable = Boolean.parseBoolean(tripleStore.getProperty(
                        BigdataSail.Options.ISOLATABLE_INDICES,
                        BigdataSail.Options.DEFAULT_ISOLATABLE_INDICES));

        return !isolatable;
        
    }
    
    /**
     * Unit test for <code>INSERT INTO ... SELECT</code>. This loads some data
     * into the end point, creates a named solution set, then verifies that the
     * solutions are present using a query and an INCLUDE join against the named
     * solution set.
     */
    public void test_insertIntoSolutions_01() throws Exception {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        loadDataset(packagePath + "dataset-01.trig");

        // Build the solution set.
        {

            final StringBuilder sb = new StringBuilder();

            /*
             * FIXME test variants w/ and w/o embedded sub-select and verify the
             * *order* is preserved when using the embedded subselect w/ its
             * order by. Also, verify that we translate this by lifting out the
             * sub-select since the top-level query is empty at thast point.
             * 
             * Also, document this on the wiki. The sub-select is necessary because
             * SPARQL does not allow solution modifiers on the top-level WHERE clause
             * for INSERT/DELETE+WHERE. 
             */
            /*
             * FIXME test variants w/ and w/o embedded sub-select and verify the
             * *order* is preserved when using the embedded subselect w/ its
             * order by. Also, verify that we translate this by lifting out the
             * sub-select since the top-level query is empty at thast point.
             * 
             * Also, document this on the wiki. The sub-select is necessary because
             * SPARQL does not allow solution modifiers on the top-level WHERE clause
             * for INSERT/DELETE+WHERE.
             */
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("INSERT INTO %namedSet1\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE { SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  ?x rdf:type foaf:Person .\n");
            sb.append("  ?x rdfs:label ?name .\n");
            sb.append("}\n");
            sb.append("ORDER BY ?name\n");
            sb.append("}");
            
            con.prepareUpdate(QueryLanguage.SPARQL, sb.toString()).execute();

        }
        
        // Query it.
        {
         
            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("SELECT ?x ?name\n");
//            sb.append("SELECT *\n");
            sb.append("WHERE {\n");
            sb.append("  INCLUDE %namedSet1 .\n");
            sb.append("  ?x rdfs:label \"Mike\" .\n");
            sb.append("}\n");
            
            final TupleQueryResult ret = con.prepareTupleQuery(
                    QueryLanguage.SPARQL, sb.toString()).evaluate();
            
			final TupleQueryResult expected = readExpectedTupleQueryResult(packagePath
					+ "test_insertIntoSolutions_01.srx");

            compareTupleQueryResults(ret, expected);

        }
        
    }
    
    /**
     * Unit test for <code>DELETE FROM ... SELECT</code>. This loads some data
     * into the end point, creates a named solution set, then removes some
     * solutions and verifies that they are no longer reported by query as a
     * post-condition.
     */
    public void test_deleteFromSolutions_01() throws Exception {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        loadDataset(packagePath + "dataset-01.trig");

        // Build the solution set.
        {

            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("INSERT INTO %namedSet1\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  ?x rdf:type foaf:Person .\n");
            sb.append("  ?x rdfs:label ?name .\n");
            sb.append("}\n");
            
            con.prepareUpdate(QueryLanguage.SPARQL,sb.toString()).execute();
            
        }
        
        // Remove some solutions.
        {

            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("DELETE FROM %namedSet1\n");
            sb.append("SELECT * \n");
            sb.append("WHERE { \n");
            sb.append("   BIND(<http://www.bigdata.com/Mike> as ?x)\n");
            sb.append("   BIND(\"Mike\" as ?name)\n");
            sb.append("}\n");

            con.prepareUpdate(QueryLanguage.SPARQL,sb.toString()).execute();
            
        }

        // Query it.
        {
         
            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  INCLUDE %namedSet1 .\n");
            sb.append("}\n");
            
            final TupleQueryResult ret = con.prepareTupleQuery(
                    QueryLanguage.SPARQL, sb.toString()).evaluate();
            
            final TupleQueryResult expected = readExpectedTupleQueryResult(packagePath
                    + "test_deleteFromSolutions_01.srx");

            compareTupleQueryResults(ret, expected);

        }

    }

    /**
     * Unit test for <code>DELETE FROM ... SELECT</code>. This loads some data
     * into the end point, creates a named solution set, then removes some
     * solutions and verifies that they are no longer reported by query as a
     * post-condition.
     */
    public void test_deleteFromSolutions_02() throws Exception {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        loadDataset(packagePath + "dataset-01.trig");

        // Build the solution set.
        {

            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("INSERT INTO %namedSet1\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  ?x rdf:type foaf:Person .\n");
            sb.append("  ?x rdfs:label ?name .\n");
            sb.append("}\n");
            
            con.prepareUpdate(QueryLanguage.SPARQL,sb.toString()).execute();
            
        }
        
        // Remove some solutions.
        {

            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("DELETE FROM %namedSet1\n");
            sb.append("SELECT ?x ?name\n");
			sb.append("WHERE { \n");
			sb.append("  ?x rdfs:label ?name .\n");
			sb.append("  FILTER (?x = <http://www.bigdata.com/Mike> ) .\n");
			sb.append("}\n");
            
            con.prepareUpdate(QueryLanguage.SPARQL,sb.toString()).execute();
            
        }

        // Query it.
        {
         
            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  INCLUDE %namedSet1 .\n");
            sb.append("}\n");
            
            final TupleQueryResult ret = con.prepareTupleQuery(
                    QueryLanguage.SPARQL, sb.toString()).evaluate();
            
            final TupleQueryResult expected = readExpectedTupleQueryResult(packagePath
                    + "test_deleteFromSolutions_02.srx");

            compareTupleQueryResults(ret, expected);

        }

    }

    /**
     * Unit test for <code>DELETE FROM ... SELECT</code>. This loads some data
     * into the end point, creates a named solution set, then removes some
     * solutions and verifies that they are no longer reported by query as a
     * post-condition.
     */
    public void test_deleteFromSolutions_03() throws Exception {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        loadDataset(packagePath + "dataset-01.trig");

        // Build the solution set.
        {

            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("INSERT INTO %namedSet1\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  ?x rdf:type foaf:Person .\n");
            sb.append("  ?x rdfs:label ?name .\n");
            sb.append("}\n");
            
            con.prepareUpdate(QueryLanguage.SPARQL,sb.toString()).execute();
            
        }
        
        // Remove some solutions.
        {

            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("DELETE FROM %namedSet1\n");
            sb.append("SELECT ?x ?name\n");
			sb.append("WHERE { \n");
			sb.append("  ?x rdfs:label ?name .\n");
			sb.append("  FILTER (?x = <http://www.bigdata.com/Bryan> ) .\n");
			sb.append("}\n");
            
            con.prepareUpdate(QueryLanguage.SPARQL,sb.toString()).execute();
            
        }

        // Query it.
        {
         
            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  INCLUDE %namedSet1 .\n");
            sb.append("}\n");
            
            final TupleQueryResult ret = con.prepareTupleQuery(
                    QueryLanguage.SPARQL, sb.toString()).evaluate();
            
            final TupleQueryResult expected = readExpectedTupleQueryResult(packagePath
                    + "test_deleteFromSolutions_03.srx");

            compareTupleQueryResults(ret, expected);

        }

    }

    /**
	 * Unit test where we are deleting from one solution set and inserting into
	 * another.
	 * 
	 * TODO Unit test where we are deleting some solutions from a solution set
	 * and inserting other solutions into the same solution set.
	 * 
	 * TODO Unit test where we are deleting some triples from a graph and
	 * inserting some solutions into a named solution set.
	 * 
	 * TODO Unit test where we are deleting some solutions from a named solution
	 * set and inserting some triples into a graph.
	 * 
     * @throws Exception 
	 */
	public void test_deleteInsertSolutions_01() throws Exception {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        loadDataset(packagePath + "dataset-01.trig");

        // Build the solution set.
        {

            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("INSERT INTO %namedSet1\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  ?x rdf:type foaf:Person .\n");
            sb.append("  ?x rdfs:label ?name .\n");
            sb.append("}\n");
            
            con.prepareUpdate(QueryLanguage.SPARQL,sb.toString()).execute();
            
        }
        
        // Remove some solutions, inserting them into a different solution set.
        {

            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("DELETE FROM %namedSet1\n");
            sb.append("  SELECT ?x ?name\n");
            sb.append("INSERT INTO %namedSet2\n");
            sb.append("  SELECT ?x ?name\n"); // TODO Variant with different projection.
			sb.append("WHERE { \n");
			sb.append("  ?x rdfs:label ?name .\n");
			sb.append("  FILTER (?x = <http://www.bigdata.com/Bryan> ) .\n");
			sb.append("}\n");
            
            con.prepareUpdate(QueryLanguage.SPARQL, sb.toString()).execute();
            
        }

        // Query the solution set from which the solutions were removed.
        {
         
            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  INCLUDE %namedSet1 .\n");
            sb.append("}\n");
            
            final TupleQueryResult ret = con.prepareTupleQuery(
                    QueryLanguage.SPARQL, sb.toString()).evaluate();
            
            final TupleQueryResult expected = readExpectedTupleQueryResult(packagePath
                    + "test_deleteInsertSolutions_01a.srx");

            compareTupleQueryResults(ret, expected);

        }

        // Query the solution set into which the solutions were inserted.
        {
         
            final StringBuilder sb = new StringBuilder();
            
            sb.append("PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
            sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n");
            sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
            sb.append("SELECT ?x ?name\n");
            sb.append("WHERE {\n");
            sb.append("  INCLUDE %namedSet2 .\n");
            sb.append("}\n");
            
            final TupleQueryResult ret = con.prepareTupleQuery(
                    QueryLanguage.SPARQL, sb.toString()).evaluate();
            
            final TupleQueryResult expected = readExpectedTupleQueryResult(packagePath
                    + "test_deleteInsertSolutions_01b.srx");

            compareTupleQueryResults(ret, expected);

        }

    }
    
//    /** FIXME Write test.
//     * Unit test where we INSERT some solutions into the same named solution set
//     * on which we are reading. This is done using an INCLUDE to join against
//     * the named solution set within an UPDATE operation which writes on that
//     * named solution set. The isolation semantics should provide one view for
//     * the reader and a different view for the writer.
//     * <p>
//     * Note: In order to setup this test, we will have to pre-populate the
//     * solution set. E.g., first load the data into graphs, then INSERT INTO
//     * SOLUTIONS. At that point we can do the INSERT which is also doing the
//     * "self-join" against the named solution set.
//     * 
//     * TODO DO a variant test where the operation is a DELETE.
//     */
//    public void test_isolation_insertIntoSolutionsWithIncludeFromSolutions() {
//
//        if (!isSolutionSetUpdateEnabled()) {
//            /*
//             * Test requires this feature.
//             */
//            return;
//        }
//
//        fail("write test");
//        
//    }

    /**
     * Unit test of CREATE SOLUTIONS.
     * <p>
     * Note: If named solution sets which do not exist are equivalent to empty
     * named solution sets, then the only way to verify the post-condition is to
     * issue a DROP or CLEAR on the named solution set. The DROP/CLEAR should
     * throw an exception if the named solution set is not found.
     */
    public void test_createSolutionSet_01() throws UpdateExecutionException,
            RepositoryException, MalformedQueryException {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        // Should fail since solution set does not exist.
        try {
            con.prepareUpdate(QueryLanguage.SPARQL, "drop solutions %namedSet1")
                    .execute();
            fail("Excepting: " + UpdateExecutionException.class);
        } catch(UpdateExecutionException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
        // Should succeed.
        con.prepareUpdate(QueryLanguage.SPARQL, "create solutions %namedSet1")
                .execute();

        // Should succeed (and should fail if the solution set does not exist).
        con.prepareUpdate(QueryLanguage.SPARQL, "drop solutions %namedSet1")
                .execute();

    }

    /**
     * Unit test of CREATE SILENT SOLUTIONS verifies that an error is reported
     * if SILENT is not specified and the solution set exists and that the error
     * is suppressed if the SILENT keyword is given.
     */
    public void test_createSolutionSet_02() throws UpdateExecutionException,
            RepositoryException, MalformedQueryException {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        // Should succeed.
        con.prepareUpdate(QueryLanguage.SPARQL, "create solutions %namedSet1")
                .execute();

        // Should fail.
        try {
            con.prepareUpdate(QueryLanguage.SPARQL,
                    "create solutions %namedSet1").execute();
            fail("Excepting: " + UpdateExecutionException.class);
        } catch (UpdateExecutionException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // Should succeed since SILENT.
        con.prepareUpdate(QueryLanguage.SPARQL,
                "create silent solutions %namedSet1").execute();

    }

    /**
     * A correct rejection test which verifies that an exception is thrown if
     * the named solution set does not exist.
     */
    public void test_dropSolutionSet_01() throws UpdateExecutionException,
            RepositoryException, MalformedQueryException {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        try {
            con.prepareUpdate(QueryLanguage.SPARQL, "drop solutions %namedSet1")
                    .execute();
            fail("Excepting: " + UpdateExecutionException.class);
        } catch(UpdateExecutionException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

    }
    
    /**
     * A test which verifies that an exception is NOT thrown if
     * the named solution set does not exist and SILENT is specified.
     */
    public void test_dropSolutionSet_02() throws UpdateExecutionException,
            RepositoryException, MalformedQueryException {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }
        
        con.prepareUpdate(QueryLanguage.SPARQL,
                "drop silent solutions %namedSet1").execute();

    }

    /**
     * A correct rejection test which verifies that an exception is thrown if
     * the named solution set does not exist.
     */
    public void test_clearSolutionSet_01() throws UpdateExecutionException,
            RepositoryException, MalformedQueryException {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        try {
            con.prepareUpdate(QueryLanguage.SPARQL, "clear solutions %namedSet1")
                    .execute();
            fail("Excepting: " + UpdateExecutionException.class);
        } catch(UpdateExecutionException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

    }
    
    /**
     * A test which verifies that an exception is NOT thrown if
     * the named solution set does not exist and SILENT is specified.
     */
    public void test_clearSolutionSet_02() throws UpdateExecutionException,
            RepositoryException, MalformedQueryException {

        if (!isSolutionSetUpdateEnabled()) {
            /*
             * Test requires this feature.
             */
            return;
        }

        con.prepareUpdate(QueryLanguage.SPARQL,
                "clear silent solutions %namedSet1").execute();

    }

    /*
     * TODO Write tests for DROP/CLEAR GRAPHS, DROP/CLEAR ALL, and DROP/CLEAR
     * SOLUTIONS to verify that the effect the correct resources (graphs,
     * graphs+solutions, and solutions respectively).
     */

}
