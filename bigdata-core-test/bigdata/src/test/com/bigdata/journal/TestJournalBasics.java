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
     * which all tests are run using a common configuration and a delegation
     * mechanism. You MUST add the returned {@link Test} into a properly
     * configured {@link ProxyTestSuite}.
     * 
     * @see ProxyTestSuite
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("journal basics");

        // tests of creation, lookup, use, commit of named indices.
        suite.addTestSuite(TestNamedIndices.class);

        // tests of Name2Addr semantics (isolation, prefix scans, etc).
        suite.addTestSuite(TestName2Addr.class);

        // verify that an index is restart-safe iff the journal commits.
        suite.addTestSuite(TestRestartSafe.class);

        // tests of the commit list for named indices.
        suite.addTestSuite(TestCommitList.class);

        // tests the ability to recover and find historical commit records.
        suite.addTestSuite(TestCommitHistory.class);

        // tests the ability to abort operations.
        suite.addTestSuite(TestAbort.class);

        // test ability to rollback a commit.
        suite.addTestSuite(TestRollbackCommit.class);

        /*
         * test behavior when journal is opened by two threads.
         * 
         * Note: This test has been disabled. First, it fails on a regular basis
         * for no reason which we can discover (the OS should forbid it).
         * Second, it appears that it is leaking file handles, perhaps when the
         * double-open is falsely allowed.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/523 (Temporary
         * Journals in CI).
         */
//        suite.addTestSuite(TestDoubleOpen.class);

        // test compacting merge of a Journal.
        suite.addTestSuite(TestCompactJournal.class);

        // test snapshot of a Journal
        suite.addTestSuite(TestSnapshotJournal.class);

        // test the DumpJournal utility.
        suite.addTestSuite(TestDumpJournal.class);

        // test ability to warmup indices in namespaces on the journal.
        suite.addTestSuite(TestWarmupJournal.class);

        /*
         * tests of transaction support.
         */
        suite.addTest(TestTransactionSupport.suite());
        
        /*
         * Tests of concurrent execution of readers, writers, and transactions
         * and group commit.
         */
        
        // test basics of the concurrent task execution.
        suite.addTestSuite(TestConcurrentJournal.class);
//        test tasks to add and drop named indices.
//        This has been commented out since the unit test has dated semantics.
//        suite.addTestSuite(TestAddDropIndexTask.class);
        // test writing on one or more unisolated indices and verify read back after the commit.
        suite.addTestSuite(TestUnisolatedWriteTasks.class);
        // test suite for hierarchical locking (namespace prefixes).
        suite.addTestSuite(TestHierarchicalLockingTasks.class);
        // test suite for GIST operations using group commit.
        //suite.addTestSuite(TestGISTTasks.class);
        // stress test of throughput when lock contention serializes unisolated writers.
        suite.addTestSuite(StressTestLockContention.class);
        // stress test of group commit.
        suite.addTestSuite(StressTestGroupCommit.class);
        // stress tests of writes on unisolated named indices using ConcurrencyManager.
        suite.addTestSuite(StressTestConcurrentUnisolatedIndices.class);
        /*
         * Stress tests of writes on unisolated named indices using
         * UnisolatedReadWriteIndex.
         */
        suite.addTestSuite(StressTestUnisolatedReadWriteIndex.class);
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
         * 
         * @todo test basic replication here 
         */
//        suite.addTestSuite(TestReplicatedStore.class);
        
        return suite;
        
    }

}
