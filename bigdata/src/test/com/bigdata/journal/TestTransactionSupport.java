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
 * Created on Dec 23, 2008
 */

package com.bigdata.journal;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTransactionSupport extends ProxyTestCase {

    /**
     * Aggregates the test suites into something approximating increasing
     * dependency. This is designed to run as a <em>proxy test suite</em> in
     * which all tests are run using a common configuration and a delegatation
     * mechanism. You MUST add the returned {@link Test} into a properly
     * configured {@link ProxyTestSuite}.
     * 
     * @see ProxyTestSuite
     */
    public static Test suite()
    {
        
        final TestSuite suite = new TestSuite("Transactions");

        // test of the RunState class.
        suite.addTestSuite(TestRunState.class);
        
        /*
         * Test suite for the transaction service using a mock client.
         */
        suite.addTestSuite(TestTransactionService.class);

        /*
         * Isolation tests with a standalone database (Journal).
         */

        // tests of read-write transactions and isolation.
        suite.addTestSuite(TestTx.class);

        // tests of read-only transactions.
        suite.addTestSuite(TestReadOnlyTx.class);

        // tests of transactions starting from the last committed state.
        suite.addTestSuite(TestReadCommittedTx.class);

        return suite;
        
    }
    
    /**
     * 
     */
    public TestTransactionSupport() {
    }

    /**
     * @param name
     */
    public TestTransactionSupport(String name) {
        super(name);
    }

}
