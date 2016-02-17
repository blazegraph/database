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

package com.bigdata.rdf.sail.sparql;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedOperation;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.query.parser.sparql.manifest.SPARQL11SyntaxTest;
import org.openrdf.query.parser.sparql.manifest.SPARQLSyntaxTest;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * Bigdata integration for the {@link SPARQLSyntaxTest}. This appears to be a
 * manifest driven test suite for both correct acceptance and correct rejection
 * tests of the SPARQL parser.  There is also an Earl report for this test suite
 * which provides a W3C markup for the test results. The Earl report is part of
 * the Sesame compliance packages.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class Bigdata2ASTSPARQL11SyntaxTest extends SPARQL11SyntaxTest {

    /**
     * When <code>true</code> use the {@link Bigdata2ASTSPARQLParser} otherwise
     * use the openrdf parser.
     */
    private static final boolean useBigdataParser = true;
    
    /**
     * @param testURI
     * @param name
     * @param queryFileURL
     * @param positiveTest
     */
    public Bigdata2ASTSPARQL11SyntaxTest(String testURI, String name,
            String queryFileURL, boolean positiveTest) {

        super(testURI, name, queryFileURL, positiveTest);
        
    }

    private AbstractTripleStore tripleStore;
    
    protected Properties getProperties() {

        final Properties properties = new Properties();

        // turn on quads.
        properties.setProperty(AbstractTripleStore.Options.QUADS, "true");

//        // override the default vocabulary.
//        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
//                NoVocabulary.class.getName());

        // turn off axioms.
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        // Note: No persistence.
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());
        
        return properties;

    }

    @Override
    protected void setUp() throws Exception {

        super.setUp();
        
        tripleStore = getStore(getProperties());

    }
    
    protected AbstractTripleStore getStore(final Properties properties) {

        final String namespace = "kb";

        // create/re-open journal.
        final Journal journal = new Journal(properties);

        final LocalTripleStore lts = new LocalTripleStore(journal, namespace,
                ITx.UNISOLATED, properties);

        lts.create();

        return lts;

    }

    @Override
    protected void tearDown() throws Exception {
        
        if (tripleStore != null) {
            
            tripleStore.__tearDownUnitTest();
            
            tripleStore = null;
            
        }

        super.tearDown();
        
    }

    /**
     * {@inheritDoc}
     * 
     * This uses the {@link Bigdata2ASTSPARQLParser}. 
     * @return 
     */
    @Override
    protected ParsedOperation parseOperation(final String query, final String queryFileURL)
            throws MalformedQueryException {

        try {

            if (useBigdataParser) {
                // bigdata parser.
                return new Bigdata2ASTSPARQLParser().parseOperation(query,
                        queryFileURL);
                
            } else {
                // openrdf parser.
                return QueryParserUtil
                        .parseOperation(QueryLanguage.SPARQL, query, queryFileURL);
            }
            
        } catch (MalformedQueryException ex) {
            
            throw new MalformedQueryException(ex + ": query=" + query
                    + ", queryFileURL=" + queryFileURL, ex);
            
        }

    }

    public static Test suite() throws Exception {

        final SPARQL11SyntaxTest.Factory factory = new SPARQL11SyntaxTest.Factory() {

            @Override
            public SPARQL11SyntaxTest createSPARQLSyntaxTest(String testURI,
                    String testName, String testAction, boolean positiveTest) {

                return new Bigdata2ASTSPARQL11SyntaxTest(testURI, testName, testAction,
                        positiveTest);
                
            }
            
        };

        /**
         * Filter out known bad tests.
         * 
         * See #1076 Negative parser tests
         */
        TestSuite suite = new TestSuite();

        suite.addTest(SPARQL11SyntaxTest.suite(factory, false));

        suite = filterOutTests(suite, knownBadTests);

        return suite;

    }

    static TestSuite filterOutTests(final TestSuite suite1, final Collection<String> testURIs) {

        final TestSuite suite2 = new TestSuite(suite1.getName());
        final Enumeration<Test> e = suite1.tests();
        while (e.hasMoreElements()) {
            final Test aTest = e.nextElement();
            if (aTest instanceof TestSuite) {
                final TestSuite aTestSuite = (TestSuite) aTest;
                suite2.addTest(filterOutTests(aTestSuite, testURIs));
            } else if (aTest instanceof Bigdata2ASTSPARQL11SyntaxTest) {
                final Bigdata2ASTSPARQL11SyntaxTest test = (Bigdata2ASTSPARQL11SyntaxTest) aTest;
                if (!testURIs.contains(test.testURI)) {
                    suite2.addTest(test);
                }
            }

        }
        return suite2;
       
    }

    /**
     * Tests that are known to fail.
     * 
     * @see See #1076 Negative parser tests
     */
    static final private Collection<String> knownBadTests = Arrays.asList(new String[] {
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-query/manifest#test_60", // syntax-BINDscope6.rq
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-query/manifest#test_61a", // syntax-BINDscope7.rq
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-query/manifest#test_62a", // syntax-BINDscope8.rq
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-query/manifest#test_65", // syntax-SELECTscope2.rq
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_25", // syntax-update-25.ru
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_31", // syntax-update-31.ru
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_54", // syntax-update-54.ru
    });
    
}
