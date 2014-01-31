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
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.journal.IRootBlockView;

/**
 * Test suites for the {@link IRestorePolicy}.
 * 
 * Note: For this test suite we do not need to verify the ability to restore the
 * journal from the various snapshots and HALogs - that is handled by the
 * {@link TestHA3SnapshotPolicy} test suite. It is sufficient for the purposes
 * of this test suite to verify that the appropriate snapshots and HALog files
 * are retained in accordance with the {@link IRestorePolicy} that is in force.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see TestHA3SnapshotPolicy
 * 
 *      FIXME Do variants on this test suite in which we test each dimension of
 *      the {@link DefaultRestorePolicy}.
 */
public class TestHA3RestorePolicy extends AbstractHA3BackupTestCase {

    public TestHA3RestorePolicy() {
    }

    public TestHA3RestorePolicy(String name) {
        super(name);
    }

    /**
     * The minimum guaranteed restore period. Snapshots must be older than this
     * before they can be purged. HALogs GT the earliest retained snapshot must
     * not be released. 
     * 
     * @see DefaultRestorePolicy
     */
    private static final long restorePolicyMinSnapshotAgeMillis = 5000;

    /**
     * @see DefaultRestorePolicy
     */
    private static final int restorePolicyMinSnapshots = 1;

    /**
     * @see DefaultRestorePolicy
     */
    private static final int restorePolicyMinRestorePoints = 0;
    
    /**
     * {@inheritDoc}
     * <p>
     * Note: This overrides some {@link Configuration} values for the
     * {@link HAJournalServer} in order to establish conditions suitable for
     * testing the {@link ISnapshotPolicy} and {@link IRestorePolicy}.
     */
    @Override
    protected String[] getOverrides() {
        
        return new String[]{
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy("+restorePolicyMinSnapshotAgeMillis+","+restorePolicyMinSnapshots+","+restorePolicyMinRestorePoints+")",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
                "com.bigdata.journal.jini.ha.HAJournalServer.HALogPurgeTimeout="+Long.MAX_VALUE+"L" // force synchronous purge.
        };
        
    }
    
    /**
     * The basic approach is to lay down a series of commit points, take a
     * snapshot, and then continue to put down commit points until the snapshot
     * should have expired and been purged (by the release policy) at some
     * commit. However, since it is the only snapshot it was retained. Then do
     * another commit and verify that the oldest snapshot (and any HALog files
     * LTE the next retained snapshot) were removed.
     * 
     * TODO Do another test in which we take snapshots every N commit points
     * (perhaps in another thread to make this asynchronous?) and verify that we
     * have retained only the necessary snapshots after each commit. (E.g., that
     * the policy remains valid under sustained writes.)
     */
    public void testAB_restorePolicy() throws Exception {

        final int N = 7; // #of transactions to run before the snapshot.
        final int M = 8; // #of transactions to run after the snapshot.
        
        /*
         * Start 3 services.
         * 
         * Note: We need to have three services running in order for the
         * snapshots 
         */
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // wait until A is ready.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));

        // Verify A is the leader.
        assertEquals(serverA, quorum.getClient().getLeader(token));

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB, serverC);
        
        /*
         * There should not be any snapshots yet since we are using the
         * NoSnapshotPolicy.  
         */
        assertEquals(0, recursiveCount(getSnapshotDirA(),SnapshotManager.SNAPSHOT_FILTER));
        assertEquals(0, recursiveCount(getSnapshotDirB(),SnapshotManager.SNAPSHOT_FILTER));
        assertEquals(0, recursiveCount(getSnapshotDirC(),SnapshotManager.SNAPSHOT_FILTER));
        
        // Now run N transactions.
        for (int i = 0; i < N; i++) {

            simpleTransaction();
            
        }

        final long commitCounterN = N + 1;

        awaitCommitCounter(commitCounterN, serverA, serverB, serverC);

        // Only the live log is retained on the services.
        assertEquals(1, recursiveCount(getHALogDirA(),IHALogReader.HALOG_FILTER));
        assertEquals(1, recursiveCount(getHALogDirB(),IHALogReader.HALOG_FILTER));
        assertEquals(1, recursiveCount(getHALogDirC(),IHALogReader.HALOG_FILTER));

        /*
         * Take a snapshot.
         * 
         * Note: The timestamp here is the lastCommitTime of the snapshot. That
         * is the timestamp that is used by the server to decide the age of the
         * snapshot.
         */
        final File snapshotFile0;
        final IRootBlockView snapshotRB0;
        final long snapshotLastCommitTime0;
        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Snapshot directory is empty.
            assertEquals(0, recursiveCount(getSnapshotDirA(),SnapshotManager.SNAPSHOT_FILTER));

            // request snapshot on A.
            final Future<IHASnapshotResponse> ft = serverA
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // Verify snapshot is being executed.
            assertNotNull(ft);
            
            // wait for the snapshot.
            try {
                ft.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                ft.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }

            snapshotRB0 = ft.get().getRootBlock();

            // Verify snapshot is for the expected commit point.
            assertEquals(commitCounterN, snapshotRB0.getCommitCounter());

            // The name of the new snapshot file.
            snapshotFile0 = SnapshotManager.getSnapshotFile(getSnapshotDirA(),
                    commitCounterN);

            // Snapshot directory contains the desired filename.
            assertExpectedSnapshots(getSnapshotDirA(),
                    new long[] { commitCounterN });

            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(serverA, commitCounterN);

            snapshotLastCommitTime0 = snapshotRB0.getLastCommitTime();
            
        }

        /*
         * Now run sets of M transactions until we have exceeded releasePolicy's
         * minimum age for the existing snapshot. However, since there is only
         * one snapshot, it SHOULD NOT be removed.
         */
        int nnewtx = 0;
        {

            final long deadline = snapshotLastCommitTime0 + restorePolicyMinSnapshotAgeMillis;
           
            while (System.currentTimeMillis() <= deadline) {
            
                for (int i = 0; i < M; i++) {
                
                    simpleTransaction();
                    
                    nnewtx++;
                    
                }

                final long commitCounterM = nnewtx + N + 1;

                awaitCommitCounter(commitCounterM, serverA, serverB, serverC);

                /*
                 * Verify that the snapshot directory contains just the expected
                 * snapshot.
                 */
                assertExpectedSnapshots(getSnapshotDirA(),
                        new long[] { snapshotRB0.getCommitCounter() });
                
                /*
                 * Check HALogs for existence on A.
                 * 
                 * Note: We can not compare the digests for equality on the
                 * difference servers because we only took a snapshot on A and
                 * therefore we have not pinned the HALogs on B or C.
                 */
                assertHALogDigestsEquals(
                        commitCounterN + 1/* firstCommitCounter */,
                        commitCounterM, new HAGlue[] { serverA });

            }

            // Verify snapshot still exists.
            assertTrue(snapshotFile0.exists());
            
            // Verify snapshot directory contains the only the one file.
            assertExpectedSnapshots(getSnapshotDirA(),
                    new long[] { snapshotRB0.getCommitCounter() });

        }

        /*
         * Request another snapshot.
         * 
         * Note: We will now have 2 snapshots. The original snapshot is NOT
         * purged. While it is older than the minimum retention time for a
         * snapshot, we do not yet have another snapshot that will allow us to
         * recover the commit points GT our oldest snapshot that are within the
         * required recovered period.
         */
        final IRootBlockView snapshotRB1;
        final File snapshotFile1;
        final long lastCommitCounter;
        {
            
            // Request snapshot on A.
            final Future<IHASnapshotResponse> ft = serverA
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // Verify snapshot is being executed.
            assertNotNull(ft);
            
            // wait for the snapshot.
            try {
                ft.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                ft.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }

            snapshotRB1 = ft.get().getRootBlock();

            // The name of the new snapshot file.
            snapshotFile1 = SnapshotManager.getSnapshotFile(getSnapshotDirA(),
                    snapshotRB1.getCommitCounter());

            // Verify new snapshot exists.
            assertTrue(snapshotFile1.exists());

            // Verify old snapshot exists.
            assertTrue(snapshotFile0.exists());

            // Verify snapshot directory contains the necessary files.
            assertExpectedSnapshots(
                    getSnapshotDirA(),
                    new long[] { snapshotRB0.getCommitCounter(),
                            snapshotRB1.getCommitCounter() });

            // The current commit counter on A.
            lastCommitCounter = serverA
                    .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                    .getRootBlock().getCommitCounter();

            // Check HALogs found on A.
            assertHALogDigestsEquals(
                    commitCounterN + 1/* firstCommitCounter */,
                    lastCommitCounter/* lastCommitCounter */,
                    new HAGlue[] { serverA });

            /*
             * Verify only the expected HALog files are retained on A (in fact,
             * all HALogs will still be retained on B since we are not taking
             * any snapshots there).
             */
            assertHALogNotFound(1L/* firstCommitCounter */,
                    commitCounterN - 1/* lastCommitCounter */,
                    new HAGlue[] { serverA });

        }

        /*
         * Sleep until the most recent snapshot is old enough to satisify our
         * recovery period.
         */
        Thread.sleep(restorePolicyMinSnapshotAgeMillis);

        /*
         * The older snapshot should still exist since we have not gone through
         * a commit.
         */
        {
            
            // Verify new snapshot exists.
            assertTrue(snapshotFile1.exists());

            // Verify old snapshot exists.
            assertTrue(snapshotFile0.exists());

            // Verify snapshot directory contains the necessary files.
            assertExpectedSnapshots(
                    getSnapshotDirA(),
                    new long[] { snapshotRB0.getCommitCounter(),
                            snapshotRB1.getCommitCounter() });

            // Check HALogs found on A.
            assertHALogDigestsEquals(
                    commitCounterN + 1/* firstCommitCounter */,
                    lastCommitCounter/* lastCommitCounter */,
                    new HAGlue[] { serverA });

        }

        // Do a simple transaction.
        simpleTransaction();

        final long lastCommitCounter2 = lastCommitCounter + 1;

        // Verify the current commit counter on A, B.
        awaitCommitCounter(lastCommitCounter2, new HAGlue[] { serverA,
                serverB, serverC });

        /*
         * Verify older snapshot and logs LT the newer snapshot are gone.
         */
        {
            
            // Verify new snapshot exists.
            assertTrue(snapshotFile1.exists());

            // Verify old snapshot is done.
            assertFalse(snapshotFile0.exists());

            // Verify snapshot directory contains the necessary files.
            assertExpectedSnapshots(getSnapshotDirA(),
                    new long[] { snapshotRB1.getCommitCounter() });

            // Check HALogs found on A.
            assertHALogDigestsEquals(
                    snapshotRB1.getCommitCounter()/* firstCommitCounter */,
                    lastCommitCounter2/* lastCommitCounter */,
                    new HAGlue[] { serverA });

            /*
             * Verify HALogs were removed from A (again, all HALogs will still
             * be on B since we have not taken a snapshot there).
             */
            assertHALogNotFound(1L/* firstCommitCounter */,
                    snapshotRB1.getCommitCounter() - 1/* lastCommitCounter */,
                    new HAGlue[] { serverA });

        }

    }

}
