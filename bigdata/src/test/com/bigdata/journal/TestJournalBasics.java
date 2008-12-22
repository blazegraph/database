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

package com.bigdata.journal;


import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates all tests into something approximately increasing dependency
 * order. Most of the tests that are aggregated are proxied test suites and will
 * therefore run with the configuration of the test class running that suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see AbstractJournalTestCase
 * @see ProxyTestCase
 */
public class TestJournalBasics extends TestCase {

    public TestJournalBasics() {
        super();
    }

    public TestJournalBasics(String arg0) {
        super(arg0);
    }

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

        TestSuite suite = new TestSuite("journal basics");

        // tests of creation, lookup, use, commit of named indices.
        suite.addTestSuite(TestNamedIndices.class);

        // verify that an index is restart-safe iff the journal commits.
        suite.addTestSuite(TestRestartSafe.class);

        // tests of the commit list for named indices.
        suite.addTestSuite(TestCommitList.class);

        // tests the ability to recover and find historical commit records.
        suite.addTestSuite(TestCommitHistory.class);

        // test ability to rollback a commit.
        suite.addTestSuite(TestRollbackCommit.class);

        // test compacting merge of a Journal.
        suite.addTestSuite(TestCompactJournal.class);
        
        /*
         * tests of transaction support.
         */
        
        // tests of transitions in the transaction RunState state machine.
        suite.addTest( AbstractTestTxRunState.suite() );
        // @todo update these tests of the tx-journal integration.
        suite.addTestSuite( TestTxJournalProtocol.class );
        // tests of read-write transactions and isolation.
        suite.addTestSuite( TestTx.class );
        // tests of read-only transactions.
        suite.addTestSuite( TestReadOnlyTx.class );
        // tests of read-committed transactions.
        suite.addTestSuite( TestReadCommittedTx.class );
        
        /*
         * Tests of concurrent execution of readers, writers, and transactions
         * and group commit.
         */
        
        // test basics of the concurrent task execution.
        suite.addTestSuite(TestConcurrentJournal.class);
        // test tasks to add and drop named indices.
        suite.addTestSuite(TestAddDropIndexTask.class);
        // test writing on one or more unisolated indices and verify read back after the commit.
        suite.addTestSuite(TestUnisolatedWriteTasks.class);
        // stress test of throughput when lock contention serializes unisolated writers.
        suite.addTestSuite(StressTestLockContention.class);
        // stress test of group commit.
        suite.addTestSuite(StressTestGroupCommit.class);
        // stress tests of writes on unisolated named indices.
        suite.addTestSuite(StressTestConcurrentUnisolatedIndices.class);
        /*
         * Stress test of concurrent transactions.
         * 
         * Note: transactions use unisolated operations on the live indices when
         * they commit and read against unisolated (but not live) indices so
         * stress tests written in terms of transactions cover a lot of
         * territory.
         * 
         * @todo add correctness tests here.
         * 
         * @todo we add known transaction processing benchmarks here.
         */
        suite.addTestSuite(StressTestConcurrentTx.class);

        /*
         * Test suite for low-level data replication.
         */
        suite.addTestSuite(TestReplicatedStore.class);
        
        return suite;
        
    }

}
