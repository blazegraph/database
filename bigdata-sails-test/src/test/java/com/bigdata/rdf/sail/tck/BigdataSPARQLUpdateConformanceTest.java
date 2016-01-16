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
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2012.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.tck;

import info.aduna.iteration.Iterations;

import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.parser.sparql.manifest.SPARQL11ManifestTest;
import org.openrdf.query.parser.sparql.manifest.SPARQLUpdateConformanceTest;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.contextaware.ContextAwareConnection;
import org.openrdf.repository.contextaware.ContextAwareRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailUpdate;
import com.bigdata.rdf.sparql.ast.UpdateRoot;

/**
 * Harness for running the SPARQL 1.1 UPDATE compliance tests (DAWG).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataSPARQLUpdateConformanceTest extends
        SPARQLUpdateConformanceTest {

    /**
     * An array of URIs for tests to be run. When null or empty the default test
     * suite is run. When specified, only the tests matching these test URIs are
     * run.
     */
    static final Collection<String> testURIs = Arrays.asList(new String[] {
//          "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/add/manifest#add02",
//            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/basic-update/manifest#insert-where-01",
//            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/basic-update/manifest#insert-data-spo-named2",
//            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/basic-update/manifest#insert-data-spo-named3"
//            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete-data/manifest#dawg-delete-data-05"
//          "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete-where/manifest#dawg-delete-where-05"
    });
    
    /*
     * These test are known to fail.
     */
    static final Collection<String> knownBadTests = Arrays.asList(new String[] {
            /*
             * These three test fail because either we don't understand the
             * default graph semantics for SPARQL Update or the DAWG doesn't.
             * Either way, these tests are incompatible with the semantics
             * assumed by the Sesame test suite SPARQLUpdateTest.
             */
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete-data/manifest#dawg-delete-data-05",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete-where/manifest#dawg-delete-where-05",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete/manifest#dawg-delete-05",
            /*
             * These tests fail because we are not properly respecting the 
             * USING clause.
             */
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete/manifest#dawg-delete-using-01",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete/manifest#dawg-delete-using-02a",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/delete/manifest#dawg-delete-using-06a",
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
    static BigdataSPARQLUpdateConformanceTest getSingleTest(final TestSuite suite,
            final String testURI) throws RuntimeException {
    
        @SuppressWarnings("unchecked")
        final Enumeration<Test> e1 = suite.tests();
        while (e1.hasMoreElements()) {
            final Test aTest = e1.nextElement();
//            log.warn(aTest.toString());
            if (aTest instanceof TestSuite) {
                final BigdataSPARQLUpdateConformanceTest test = getSingleTest((TestSuite) aTest,
                        testURI);
                if (test != null)
                    return test;
            }
            if (aTest instanceof BigdataSPARQLUpdateConformanceTest) {
                final BigdataSPARQLUpdateConformanceTest test = (BigdataSPARQLUpdateConformanceTest) aTest;
                if (testURI.equals(test.testURI)) {
                    return test;
                }
            }
        }
        return null;
    }

    static TestSuite filterOutTests(final TestSuite suite1, final Collection<String> testURIs) {

        final TestSuite suite2 = new TestSuite(suite1.getName());
        @SuppressWarnings("unchecked")
        final Enumeration<Test> e = suite1.tests();
        while (e.hasMoreElements()) {
            final Test aTest = e.nextElement();
            if (aTest instanceof TestSuite) {
                final TestSuite aTestSuite = (TestSuite) aTest;
                suite2.addTest(filterOutTests(aTestSuite, testURIs));
            } else if (aTest instanceof BigdataSPARQLUpdateConformanceTest) {
                final BigdataSPARQLUpdateConformanceTest test = 
                        (BigdataSPARQLUpdateConformanceTest) aTest;
                if (!testURIs.contains(test.testURI)) {
                    suite2.addTest(test);
                }
            }

        }
        return suite2;
       
    }


    
    public BigdataSPARQLUpdateConformanceTest(String testURI, String name,
            String requestFile, URI defaultGraphURI,
            Map<String, URI> inputNamedGraphs, URI resultDefaultGraphURI,
            Map<String, URI> resultNamedGraphs) {

        super(testURI, name, requestFile, defaultGraphURI, inputNamedGraphs,
                resultDefaultGraphURI, resultNamedGraphs);
        
    }

    public static Test suite() throws Exception {
        
        TestSuite suite1 = SPARQL11ManifestTest.suite(new Factory() {

            @Override
            public BigdataSPARQLUpdateConformanceTest createSPARQLUpdateConformanceTest(
                    String testURI, String name, String requestFile,
                    URI defaultGraphURI, Map<String, URI> inputNamedGraphs,
                    URI resultDefaultGraphURI,
                    Map<String, URI> resultNamedGraphs) {

                return new BigdataSPARQLUpdateConformanceTest(testURI, name,
                        requestFile, defaultGraphURI, inputNamedGraphs,
                        resultDefaultGraphURI, resultNamedGraphs);
            }

        }, true, false, false);
        
        // Only run the specified tests?
        if (!testURIs.isEmpty()) {
            final TestSuite suite2 = new TestSuite();
            for (String s : testURIs) {
                final BigdataSPARQLUpdateConformanceTest test = getSingleTest(suite1, s);
                if (test == null)
                    throw new RuntimeException("Could not find test: uri="
                            + s);
                suite2.addTest(test);
            }
            return suite2;
        }
        
        if (!BigdataStatics.runKnownBadTests)
            suite1 = filterOutTests(suite1, knownBadTests);

        return suite1;
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
        
        // exact size only there for TCK
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
        
        // use a random namespace to keep the values in the actual repo
        // separate from the values in the expected repo
        props.setProperty(Options.NAMESPACE, "kb-" + UUID.randomUUID().toString());
        
        return props;
        
    }
    
    @Override
    protected BigdataSailRepository newRepository() throws RepositoryException {

//        if (true) {
            final Properties props = getProperties();
            
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
            
            final BigdataSail sail = new BigdataSail(props);
//            return new ContextAwareRepository(new BigdataSailRepository(sail));
            return new BigdataSailRepository(sail);
//        } else {
//            /*
//             * Run against openrdf.
//             */
//            SailRepository repo = new SailRepository(new MemoryStore());
//
//            return new ContextAwareRepository(repo);
//        }
    }
    
    @Override
    protected void setUp()
        throws Exception
    {
        dataRep = createRepository();

        URL graphURL = null;
        RepositoryConnection conn = dataRep.getConnection();
        try {
            conn.clear();

            if (inputDefaultGraph != null) {
                graphURL = new URL(inputDefaultGraph.stringValue());
                conn.add(graphURL, null, RDFFormat.forFileName(graphURL.toString()));
            }

            for (String ng : inputNamedGraphs.keySet()) {
                graphURL = new URL(inputNamedGraphs.get(ng).stringValue());
                conn.add(graphURL, null, RDFFormat.forFileName(graphURL.toString()),
                        dataRep.getValueFactory().createURI(ng));
            }
            
            conn.commit();
        }
        finally {
            conn.close();
        }

        expectedResultRepo = createRepository();

        conn = expectedResultRepo.getConnection();
        
        try {
            conn.clear();

            if (resultDefaultGraph != null) {
                graphURL = new URL(resultDefaultGraph.stringValue());
                conn.add(graphURL, null, RDFFormat.forFileName(graphURL.toString()));
            }

            for (String ng : resultNamedGraphs.keySet()) {
                graphURL = new URL(resultNamedGraphs.get(ng).stringValue());
                conn.add(graphURL, null, RDFFormat.forFileName(graphURL.toString()),
                        dataRep.getValueFactory().createURI(ng));
            }
            
            conn.commit();
        }
        finally {
            conn.close();
        }

    }

    @Override
    protected void runTest()
        throws Exception
    {
        BigdataSailRepositoryConnection con = (BigdataSailRepositoryConnection) dataRep.getConnection();
        BigdataSailRepositoryConnection erCon = (BigdataSailRepositoryConnection) expectedResultRepo.getConnection();
        try {
            String updateString = readUpdateString();
            
            con.begin();
//            con.setReadContexts((URI)null);
            
            Update update = con.prepareUpdate(QueryLanguage.SPARQL, updateString, requestFileURL);
//            if (this.dataset != null) {
//                update.setDataset(this.dataset);
//            }
            update.execute();

            con.commit();
            
            // check default graph
            compareGraphs(Iterations.asList(con.getStatements(null, null, null, true, (Resource)null)),
                    Iterations.asList(erCon.getStatements(null, null, null, true, (Resource)null)),
                    (BigdataSailRepositoryConnection) con);

            for (String namedGraph : inputNamedGraphs.keySet()) {
                URI contextURI = con.getValueFactory().createURI(namedGraph.replaceAll("\"", ""));
                compareGraphs(Iterations.asList(con.getStatements(null, null, null, true, contextURI)),
                        Iterations.asList(erCon.getStatements(null, null, null, true, contextURI)),
                        (BigdataSailRepositoryConnection) con);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
            if(con.isActive()) {
                con.rollback();
            }
            throw e;
        }
        finally {
            con.close();
            erCon.close();
        }
    }

    protected void compareGraphs(Iterable<? extends Statement> actual, Iterable<? extends Statement> expected,
            final BigdataSailRepositoryConnection cxn)
        throws Exception
    {
        if (!ModelUtil.equals(expected, actual)) {
            StringBuilder message = new StringBuilder(128);
            message.append("\n=========================================\n");
            message.append(getName());
            message.append("\n");
            message.append(testURI);
            message.append("\n=========================================\n");
            
            message.append("Expected results: \n");
            for (Statement bs : expected) {
                message.append(bs);
                message.append("\n");
            }
            message.append("=========================================\n");

            message.append("Bigdata results: \n");
            for (Statement bs : actual) {
                message.append(bs);
                message.append("\n");
            }
            message.append("=========================================\n");

            final String queryStr = readUpdateString();
            message.append("Query:\n"+queryStr);
            message.append("\n=========================================\n");

//                message.append("Data:\n"+readInputData(dataset));
//                message.append("\n=========================================\n");

            final BigdataSailUpdate query = (BigdataSailUpdate)
                    cxn.prepareUpdate(QueryLanguage.SPARQL, queryStr);
            
            final UpdateRoot original = query.getASTContainer().getOriginalUpdateAST();
            message.append("Original AST:\n"+original);
            message.append("\n=========================================\n");
            
            logger.error(message.toString());
            fail(message.toString());
        }
    }


}
