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
 * Created on Oct 14, 2006
 */

package com.bigdata.journal.ha;


import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.journal.ProxyTestCase;

/**
 * Aggregates all tests into something approximately increasing dependency
 * order. Most of the tests that are aggregated are proxied test suites and will
 * therefore run with the configuration of the test class running that suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see AbstractHAJournalTestCase
 * @see ProxyTestCase
 */
public class TestJournalHA extends TestCase {

    public TestJournalHA() {
        super();
    }

    public TestJournalHA(String arg0) {
        super(arg0);
    }

    /**
     * Aggregates the test suites into something approximating increasing
     * dependency. This is designed to run as a <em>proxy test suite</em> in
     * which all tests are run using a common configuration and a delegation
     * mechanism. You MUST add the returned {@link Test} into a properly
     * configured {@link ProxyTestSuite}.
     * 
     * @see ProxyTestSuite
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("HA Journal");
        
        /*
         * write pipeline unit tests.
         * 
         * FIXME TESTS MUST COMPARE THE BINARY STORE FILES TO VERIFY HA !!!!
         */
        
        /*
         * 1. bootstrap 3 journals in a specified failover chain and demonstrate
         * pipelined writes and the commit protocol.
         */
        suite.addTestSuite(TestHAWritePipeline.class);

        /*
         * 2. bootstrap 3 journals in a specified failover chain, pipeline some
         * writes, attempt a commit (prepare) and have one of the journals vote
         * "no" but the other 2 vote "yes" and the commit goes through. This
         * test will not attempt to deal with the fact that the quorum has no
         * changed.
         */

        /*
         * 3. bootstrap 3 journals in a specified failover chain, pipeline some
         * writes, attempt a commit (prepare) and have 2 of the journals vote
         * "no" so the commit does not proceed and the master send out an
         * abort() message instead.
         */

        /*
         * 4. throughput test. bootstrap 3 journals in a specified failover
         * chain and write a bunch of data using raw records of a configured
         * size and committing at a configured delay interval.
         */

        /*
         * bad read unit tests.
         */

        /*
         * 1. bootstrap 3 journals in a specified failover chain, pipeline some
         * writes, prepare and commit the write set.  Now have the 2nd journal
         * send a read request to the 1st journal, simulating how we handle a
         * bad read.
         */
        
        /*
         * quorum membership unit tests.
         */

        /*
         * resynchronization unit tests.
         */

        /*
         * robust messaging unit tests.
         * 
         * unit tests for robust forwarding of client messages to peers in the
         * quorum. some of these will deal with dynamic quorum changes and
         * blocking until someone is elected the master who is in the quorum.
         */
        
        return suite;
        
    }

}
