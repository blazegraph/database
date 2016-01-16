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
package com.bigdata.rdf.rio;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * RIO integration tests.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite() {

        final TestSuite suite = new TestSuite("RIO Integration");

        // basic tests for StatementBuffer.
        suite.addTestSuite(TestStatementBuffer.class);

        /*
         * Correctness tests for loading RDF data using DataLoader and
         * StatementBuffer. Verification is by re-parsing the RDF data and
         * checking that all statements found in the data exist in the database
         * for each access path.
         */
        suite.addTestSuite(TestLoadAndVerify.class);

//        /*
//         * Correctness tests when SIDs are enabled and for blank node handling
//         * using StatementBuffer and explicitly inserting specific triples (no
//         * parsing). The RDF/XML interchange tests serialize the hand loaded
//         * data and verify that it can be parsed and that the same graph is
//         * obtained.
//         */
//        suite.addTestSuite(TestRDFXMLInterchangeWithStatementIdentifiers.class);
        
        /*
		 * Test suite for "SIDS" support for NTRIPLES data. This test targets a
		 * hybrid capability in which the old SIDS mode is extended to consume
		 * NTRIPLES. 
		 */
        suite.addTestSuite(TestNTriplesWithSids.class);

        /*
         * Correctness tests for the asynchronous bulk data loader. This
         * requires the scale-out architecture. SIDs are not supported yet.
         */
        suite.addTestSuite(TestAsynchronousStatementBufferFactory.class);

        return suite;
        
    }

}
