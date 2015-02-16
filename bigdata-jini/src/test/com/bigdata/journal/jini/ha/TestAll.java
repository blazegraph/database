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

package com.bigdata.journal.jini.ha;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.journal.Journal;

/**
 * Test suite for highly available configurations of the standalone
 * {@link Journal}.
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
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("HAJournalServer");

        // commitTime => (HALog|Snapshot)Record test suites.
        suite.addTestSuite(TestHALogIndex.class);
        suite.addTestSuite(TestSnapshotIndex.class);
        
        // Test suite for direct IBufferStrategy data xfer tests.
        suite.addTestSuite(TestRawTransfers.class);

        // Basic tests for a single HAJournalServer (quorum does not meet)
        suite.addTestSuite(TestHAJournalServer.class);

        // HA1 test suite.
        suite.addTestSuite(TestHA1JournalServer.class);
        suite.addTestSuite(TestHA1SnapshotPolicy.class);
        suite.addTestSuite(TestHA1SnapshotPolicy2.class);

        // HA2 test suite (k=3, but only 2 services are running).
        suite.addTestSuite(TestHA2JournalServer.class);

        // HA3 test suite in which HALogs are purged on a fully met quorum.
        suite.addTestSuite(TestHA3JournalServer.class);

        // HA3 test suite in which normal HALog retention rules apply.
        suite.addTestSuite(TestHA3JournalServerWithHALogs.class);

        // HA3 test suite focusing on changing the leader.
        suite.addTestSuite(TestHA3ChangeLeader.class);

        // HA3 test suite focusing on sudden kills.
        suite.addTestSuite(TestHA3JustKills.class);

        // HA3 snapshot policy test suite.
        suite.addTestSuite(TestHA3SnapshotPolicy.class);
        suite.addTestSuite(TestHA3SnapshotPolicy2.class);

        // HA3 restore policy test suite.
        suite.addTestSuite(TestHA3RestorePolicy.class);

//        // Test suite for the global write lock.
//        suite.addTestSuite(TestHAJournalServerGlobalWriteLock.class);

        // Test suite for issuing a CANCEL request for Query or Update.
        suite.addTestSuite(TestHA3CancelQuery.class);
        
        // Test suite for utility to compute and compare HALog digests.
        suite.addTestSuite(TestHA3DumpLogs.class);

        // Verify ability to override the HAJournal implementation class.
        suite.addTestSuite(TestHAJournalServerOverride.class); 

        // The HA load balancer test suite.
        suite.addTest(TestAll_LBS.suite());
        
        // HA5 test suite.
        suite.addTestSuite(TestHA5JournalServer.class);
        suite.addTestSuite(TestHA5JournalServerWithHALogs.class);

        /*
         * Stress tests.
         */
        
        // Test suite of longer running stress tests for an HA3 cluster.
        suite.addTestSuite(StressTestHA3JournalServer.class);

        return suite;

    }

}
