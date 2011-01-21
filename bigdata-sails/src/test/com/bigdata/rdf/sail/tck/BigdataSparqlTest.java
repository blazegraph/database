/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 19, 2008
 */
package com.bigdata.rdf.sail.tck;

import info.aduna.io.IOUtil;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.query.Dataset;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.parser.sparql.ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.dataset.DatasetRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * Test harness for running the SPARQL test suites.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSparqlTest extends SPARQLQueryTest {

    static protected final Logger log = Logger.getLogger(BigdataSparqlTest.class);
    
    
    /**
     * We cannot use inlining for these test because we do normalization on
     * numeric values and these tests test for syntatic differences, i.e.
     * 01 != 1.
     */
    static final Collection<String> cannotInlineTests = Arrays.asList(new String[] {
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-01",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-03",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-04",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-str-2",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#dawg-datatype-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-simple",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-eq",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#sameTerm-not-eq",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#eq-graph-2",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#no-distinct-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-1",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#no-distinct-9",
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#distinct-9",
    });

	/**
	 * The following tests require Unicode configuration for identical
	 * comparisons. This appears to work with {ASCII,IDENTICAL} or
	 * {JDK,IDENTICAL} but not with {ICU,IDENTICAL} for some reason.
	 */
    static final Collection<String> unicodeStrengthIdentical = Arrays.asList(new String[] {
    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/i18n/manifest#normalization-1"
    });
    
//    private static String datasetTests = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset";

    /**
     * Skip the dataset tests for now until we can figure out what is wrong with
     * them.
     * 
     * FIXME Fix the dataset tests. There is some problem in how the data to be
     * loaded into the fixture is being resolved in these tests.
     */
    public static Test suite() throws Exception {
        
        return suite(true /*hideDatasetTests*/);
        
    }
    
    public static Test suite(final boolean hideDatasetTests) throws Exception {
        
        TestSuite suite1 = suiteLTSWithPipelineJoins();

        // Only run the specified tests?
        if (!testURIs.isEmpty()) {
            final TestSuite suite = new TestSuite();
            for (String s : testURIs) {
                suite.addTest(getSingleTest(suite1, s));
            }
            return suite;
        }
        
        if(hideDatasetTests)
            suite1 = filterOutDataSetTests(suite1);
        
        return suite1;
        
    }

    /**
     * Hack filters out the "dataset" tests.
     * 
     * @param suite1
     *            The test suite.
     *            
     * @return The test suite without the data set tests.
     */
    static TestSuite filterOutDataSetTests(final TestSuite suite1) {
        
        final TestSuite suite2 = new TestSuite(suite1.getName());
        
        final Enumeration<TestSuite> e = suite1.tests();
        
        while (e.hasMoreElements()) {
            
            final TestSuite suite3 = e.nextElement();
            
            if (suite3.getName().equals("dataset") == false) {
                
                suite2.addTest(suite3);
                
            }
            
        }
        
        return suite2;
        
    }

    /**
     * An array of URIs for tests to be run. When null or empty the default test
     * suite is run. When specified, only the tests matching these test URIs are
     * run.
     */
    static final Collection<String> testURIs = Arrays.asList(new String[] {

	/*
	 * working through the new query engine failures: 0 errors, 11 failures
	 */

			/*
			 * Basically we are having a lot of problems with our compare
			 * operator, which is supposed to do fuzzy comparisons that
			 * sometimes requires materialized RDF values. These I feel I can
			 * handle on my own.
			 */

    		// "a" and "a"^^xsd:string have different term ids?  also bnodes are different
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-07",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-08",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-10",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-11",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-cmp-01",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-cmp-02",

			/*
			 * These tests have to do with that that weird "well designed"
			 * optional nesting P = A OPT (B OPT C) where A and C share
			 * variables not in B.  I think I can handle these on my own.
			 */
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-1",
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-scope-1",

    		/*
    		 * Everything below this point I need help with.
    		 */
    		
			/*
			 * This one is truly bizarre - involving a non-optional subquuery
			 * plus an optional subquery. Don't even know where to start on this
			 * guy.
			 */
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-scope-1",

			/*
			 * Sometimes, a filter is the entire join group, and it should not
			 * be able to see variables outside the group.  Frankly I do not
			 * understand this one.
			 */
//    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#filter-nested-2",

//			/*
//			 * These demonstrate the problem of where to put non-optional
//			 * filters that need to be evaluated after optional tails and
//			 * optional join groups.
//			 * 
//			 * NOTE: These are fixed.
//			 */
////    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/bound/manifest#dawg-bound-query-001",
////    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-002",
////    		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-003",
    		
//			/*
//			 * These failures have to do with nested UNIONs - we don't seem to
//			 * be handling them correctly at all.
//			 * 
//			 * NOTE: These are fixed.
//			 */
////   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-combo-1",
////   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-combo-2",
////   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-1",
////   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-2",
////   		"http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#dawg-optional-complex-4",

    });

    /**
     * Return the sole test in the suite associated with the specified testURI.
     * 
     * @param suite
     *            The test suite.
     * @param testURI
     *            The test URI (these are defined by the DAWG).
     *            
     * @return An instance of this class which will run just that one test.
     * 
     * @throws RuntimeException
     *             if there is no test in the suite which is associated with
     *             that testURI.
     */
    static SPARQLQueryTest getSingleTest(TestSuite suite,
            final String testURI) throws RuntimeException {
    
        SPARQLQueryTest test = null;
        final Enumeration e1 = suite.tests();
        while (e1.hasMoreElements()) {
            suite = (TestSuite) e1.nextElement();
            final Enumeration e2 = suite.tests();
            while (e2.hasMoreElements()) {
                 test = (SPARQLQueryTest) e2.nextElement();
                 if (testURI.equals(test.getTestURI())) {
                     return test;
                 }
            }
        }
        
        throw new RuntimeException("could not find a test with that URI");
        
    }

    /**
     * Return the test suite. 
     */
    public static TestSuite suiteLTSWithPipelineJoins() throws Exception {
       
        return ManifestTest.suite(new Factory() {

            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality) {

                return new BigdataSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality) {

                    protected Properties getProperties() {

                        final Properties p = new Properties(super
                                .getProperties());

//                        p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,
//                                "false");

                        return p;

                    }
                    
                };

            }
        });
    }

    public BigdataSparqlTest(String testURI, String name, String queryFileURL,
            String resultFileURL, Dataset dataSet, boolean laxCardinality) {

        super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality);
        
    }
    
//    public String getTestURI() {
//        return testURI;
//    }
    
    /**
     * Overridden to destroy the backend database and its files on the disk.
     */
    @Override
    public void tearDown()
        throws Exception
    {
/*
        StringBuilder message = new StringBuilder();
        message.append("data:\n");

        RepositoryConnection cxn = dataRep.getConnection();
        try {
            RepositoryResult<Statement> stmts = cxn.getStatements(null, null, null, true);
            while (stmts.hasNext()) {
                Statement stmt = stmts.next();
                message.append(stmt+"\n");
            }
        } finally {
            cxn.close();
        }
        SPARQLQueryTest.logger.error(message.toString());
*/        
        IIndexManager backend = null;
        
        Repository delegate = dataRep == null ? null : ((DatasetRepository) dataRep).getDelegate();
        
        if (delegate != null && delegate instanceof BigdataSailRepository) {
            
            backend = ((BigdataSailRepository) delegate).getDatabase()
                            .getIndexManager();
            
        }

        super.tearDown();

        if (backend != null)
            tearDownBackend(backend);

        /*
         * Note: this field MUST be cleared to null or the backing database
         * instance will be held by a hard reference throughout the execution of
         * all unit tests in this test suite!
         */

        dataRep = null;
    
    }

    /**
     * Note: This method may be overridden in order to run the test suite
     * against other variations of the bigdata backend.
     * 
     * @see #suiteLTSWithNestedSubquery()
     * @see #suiteLTSWithPipelineJoins()
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
        
        // exact size only there for TCK
        props.setProperty(Options.EXACT_SIZE, "true");
        
//        props.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());
        
//      Force identical unicode comparisons (assuming default COLLATOR setting).
//        props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
        
        // disable read/write transactions
        props.setProperty(Options.ISOLATABLE_INDICES, "false");
        
        // disable truth maintenance in the SAIL
        props.setProperty(Options.TRUTH_MAINTENANCE, "false");
        
        return props;
        
    }
    
    @Override
    protected Repository newRepository() throws RepositoryException {

        if (true) {
            final Properties props = getProperties();
            
            if (cannotInlineTests.contains(testURI)){
            	// The test can not be run using XSD inlining.
                props.setProperty(Options.INLINE_LITERALS, "false");
            }
            
            if(unicodeStrengthIdentical.contains(testURI)) {
            	// Force identical Unicode comparisons.
            	props.setProperty(Options.COLLATOR, CollatorEnum.JDK.toString());
            	props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
            }
            
            final BigdataSail sail = new BigdataSail(props);
            return new DatasetRepository(new BigdataSailRepository(sail));
        } else {
            return new DatasetRepository(new SailRepository(new MemoryStore()));
        }
    }

    protected void tearDownBackend(IIndexManager backend) {
        
        backend.destroy();
        
    }
    
    @Override
    protected Repository createRepository() throws Exception {
        Repository repo = newRepository();
        repo.initialize();
        return repo;
    }
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }
    
    public Repository getRepository() {
        return dataRep;
    }
    
    private String queryString = null;
    public String getQueryString() throws Exception {
        if (queryString == null) {
            InputStream stream = new URL(queryFileURL).openStream();
            try {
                return IOUtil.readString(new InputStreamReader(stream, "UTF-8"));
            }
            finally {
                stream.close();
            }
        }
        return queryString;
    }
    
    
    @Override
    protected void runTest()
        throws Exception
    {
        RepositoryConnection con = getQueryConnection(dataRep);
        try {

//            log.info("database dump:");
//            RepositoryResult<Statement> stmts = con.getStatements(null, null, null, false);
//            while (stmts.hasNext()) {
//                log.info(stmts.next());
//            }
            log.info("dataset:\n" + dataset);

            String queryString = readQueryString();
            log.info("query:\n" + getQueryString());
            
            Query query = con.prepareQuery(QueryLanguage.SPARQL, queryString, queryFileURL);
            if (dataset != null) {
                query.setDataset(dataset);
            }

            if (query instanceof TupleQuery) {
                TupleQueryResult queryResult = ((TupleQuery)query).evaluate();
                while (queryResult.hasNext()) {
                    log.info("query result:\n" + queryResult.next());
                }
            }

        }
        finally {
            con.close();
        }
        
        super.runTest();
    }
    
    

}
