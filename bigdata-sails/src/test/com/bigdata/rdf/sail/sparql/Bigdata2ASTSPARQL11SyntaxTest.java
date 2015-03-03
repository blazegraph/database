/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 24, 2011
 */

package com.bigdata.rdf.sail.sparql;

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
                return new Bigdata2ASTSPARQLParser(tripleStore).parseOperation(query,
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
        
        final TestSuite suite = new TestSuite();

        suite.addTest(SPARQL11SyntaxTest.suite(factory, false));

        return suite;

    }

}
