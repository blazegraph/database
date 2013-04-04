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
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.rdf.sail.webapp.client.HAStatusEnum;

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
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()"
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
        
        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // wait until A is ready.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));

        // Verify A is the leader.
        assertEquals(serverA, quorum.getClient().getLeader(token));

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);
        
        assertCommitCounter(1L, serverA);
        
        // Now run N transactions.
        for (int i = 0; i < N; i++) {

            simpleTransaction();
            
        }

        final long commitCounterN = N + 1;

        assertCommitCounter(commitCounterN, serverA);

        // Check HALogs equal on A, B.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounterN,
                new HAGlue[] { serverA, serverB });

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
            assertEquals(0, getSnapshotDirA().list().length);

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
            assertEquals(new String[] { snapshotFile0.getName() },
                    getSnapshotDirA().list());

            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(serverA, commitCounterN);

            snapshotLastCommitTime0 = snapshotRB0.getLastCommitTime();
            
        }

        /*
         * Now run sets of M transactions until we have exceeded releasePolicy's
         * minimum age for the existing snapshot. Since there is only one
         * snapshot, it SHOULD NOT be removed.
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

                assertCommitCounter(commitCounterM, serverA);
                
                // Check HALogs equal on A, B.
                assertHALogDigestsEquals(1L/* firstCommitCounter */, commitCounterM,
                        new HAGlue[] { serverA, serverB });

                // Snapshot directory contains just the expected snapshot
                assertEquals(new String[] { snapshotFile0.getName() },
                        getSnapshotDirA().list());
            }

            // Verify snapshot still exists.
            assertTrue(snapshotFile0.exists());
            
            // Verify snapshot directory contains the only the one file.
            assertEquals(new String[] { snapshotFile0.getName() },
                    getSnapshotDirA().list());

        }

        /*
         * Request another snapshot.
         * 
         * Note: We will now have 2 snapshots. The original snapshot is purged
         * since it is older than the minimum retention time for a snapshot.
         */
        final IRootBlockView snapshotRB1;
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
            final File snapshotFile1 = SnapshotManager.getSnapshotFile(
                    getSnapshotDirA(), snapshotRB1.getCommitCounter());

            // Verify new snapshot exists.
            assertTrue(snapshotFile1.exists());

            // Verify old snapshot is gone.
            assertFalse(snapshotFile0.exists());

            // Verify snapshot directory contains the only the one file.
            assertEquals(new String[] { snapshotFile1.getName() },
                    getSnapshotDirA().list());

            /*
             * Verify only the expected HALog files are retained.
             */

            // The current commit counter on A.
            final long lastCommitCounter = serverA
                    .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                    .getRootBlock().getCommitCounter();

            // Check HALogs equal on A, B.
            assertHALogDigestsEquals(
                    snapshotRB1.getCommitCounter()/* firstCommitCounter */,
                    lastCommitCounter/* lastCommitCounter */, new HAGlue[] {
                            serverA, serverB });

        }

    }

}
