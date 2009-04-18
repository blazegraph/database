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
package com.bigdata.rdf.rio;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
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

        /*
         * Correctness tests when SIDs are enabled and for blank node handling
         * using StatementBuffer and explicitly inserting specific triples (no
         * parsing). The RDF/XML interchange tests serialize the hand loaded
         * data and verify that it can be parsed and that the same graph is
         * obtained.
         */
        suite.addTestSuite(TestRDFXMLInterchangeWithStatementIdentifiers.class);

        /*
         * Correctness tests for the asynchronous bulk data loader variant
         * which does not support SIDs.
         */
        suite.addTestSuite(TestAsynchronousStatementBufferWithoutSids.class);
        
        return suite;
        
    }
    
}
