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

package com.bigdata.rdf.sail.tck;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.SPARQLSyntaxTest;

import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sail.sparql.BigdataSPARQLParser;

/**
 * Bigdata integration for the {@link SPARQLSyntaxTest}. This appears to be a
 * manifest driven test suite for both correct acceptance and correct rejection
 * tests of the SPARQL parser.  There is also an Earl report for this test suite
 * which provides a W3C markup for the test results. The Earl report is part of
 * the Sesame compliance packages.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSPARQLSyntaxTest extends SPARQLSyntaxTest {

    /**
     * @param testURI
     * @param name
     * @param queryFileURL
     * @param positiveTest
     */
    public BigdataSPARQLSyntaxTest(String testURI, String name,
            String queryFileURL, boolean positiveTest) {

        super(testURI, name, queryFileURL, positiveTest);
        
    }

    /**
     * {@inheritDoc}
     * 
     * FIXME This needs to be changed to {@link Bigdata2ASTSPARQLParser}. 
     */
    @Override
    protected void parseQuery(String query, String queryFileURL)
            throws MalformedQueryException {

        new BigdataSPARQLParser().parseQuery(query, queryFileURL);

    }

    public static Test suite() throws Exception {

        final Factory factory = new Factory() {

            @Override
            public SPARQLSyntaxTest createSPARQLSyntaxTest(String testURI,
                    String testName, String testAction, boolean positiveTest) {

                return new BigdataSPARQLSyntaxTest(testURI, testName, testAction,
                        positiveTest);
                
            }
            
        };
        
        final TestSuite suite = new TestSuite();

        suite.addTest(SPARQLSyntaxTest.suite(factory));

        return suite;

    }

}
