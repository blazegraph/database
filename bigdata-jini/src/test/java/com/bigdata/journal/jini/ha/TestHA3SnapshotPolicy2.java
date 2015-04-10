/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.msg.HARootBlockRequest;

/**
 * Test suites for the {@link DefaultSnapshotPolicy}.
 * <p>
 * Note: Since the {@link DefaultSnapshotPolicy} runs once a day, we have to
 * configure the time at which it will run so that it will not run during the
 * unit test. If it were to run during the test, then that would throw off the
 * conditions that we are trying to control from the test suite.
 * <p>
 * Note: The {@link NoSnapshotPolicy} is tested for explictly in the code in
 * order to avoid taking the initial snapshop and thereby pinning the HALogs.
 * This test suite uses the {@link DefaultSnapshotPolicy} and verifies that the
 * initial snapshot IS taken when the {@link DefaultSnapshotPolicy} is used.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see TestHA3SnapshotPolicy
 * 
 *      TODO Write test to verify that a rebuild triggered while a snapshot is
 *      running will cancel the running snapshot. This is necessary since we
 *      must overwrite the root blocks. 
 */
public class TestHA3SnapshotPolicy2 extends AbstractHA3BackupTestCase {

    public TestHA3SnapshotPolicy2() {
    }

    public TestHA3SnapshotPolicy2(String name) {
        super(name);
    }

    /** How long to wait for snapshots to appear. */
    private final long awaitSnapshotMillis = 5000;
    
    /**
     * {@inheritDoc}
     * <p>
     * Note: This overrides some {@link Configuration} values for the
     * {@link HAJournalServer} in order to establish conditions suitable for
     * testing the {@link ISnapshotPolicy} and {@link IRestorePolicy}.
     */
    @Override
    protected String[] getOverrides() {

        /*
         * We need to set the time at which the DefaultSnapshotPolicy runs to
         * some point in the Future in order to avoid test failures due to
         * violated assumptions when the policy runs up self-triggering (based
         * on the specified run time) during a CI run.
         */
        final String neverRun = getNeverRunSnapshotTime();
        
        return new String[]{
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.DefaultSnapshotPolicy("+neverRun+",0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true"
        };
        
    }

    /**
     * Verify that A+B take a snapshot of the empty journal once the quorum
     * meets (but not before). 
     */
    public void test_AB_snapshotOnQuorumMeet() throws Exception {

        // Start A. Service should NOT take a snapshot.
        final HAGlue serverA = startA();

        // Snapshot directory is empty.
        assertEquals(0, recursiveCount(getSnapshotDirA(),SnapshotManager.SNAPSHOT_FILTER));

        // Wait a bit.
        Thread.sleep(awaitSnapshotMillis/*ms*/);
        
        // Snapshot directory is still empty.
        assertEquals(0, recursiveCount(getSnapshotDirA(),SnapshotManager.SNAPSHOT_FILTER));

        // Start B.  Both A and B should take a snapshot when quorum meets.
        final HAGlue serverB = startB();

        // Await quorum meet.
        @SuppressWarnings("unused")
        final long token = awaitMetQuorum();
        
        // Wait until both services are ready.
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);
        
        // Expected commit point on A, B.
        final long commitCounter = 1;
        
        // Verify/await expected commit point.
        awaitCommitCounter(commitCounter, serverA, serverB);

        // Verify/await snapshot on A.
        awaitSnapshotExists(serverA, commitCounter);

        // Verify existence of the snapshot file.
        assertSnapshotExists(getSnapshotDirA(), commitCounter);
        
        // Verify/await snapshot on B.
        awaitSnapshotExists(serverB, commitCounter);

        // Verify existence of the snapshot file.
        assertSnapshotExists(getSnapshotDirB(), commitCounter);

        /*
         * Restart B and verify that the service is await of the snapshot
         * after a restart.
         */
        restartB();

        // Verify existence of the snapshot file after restart.
        assertSnapshotExists(getSnapshotDirB(), commitCounter);

        /*
         * Restart A and verify that the service is await of the snapshot
         * after a restart.
         */
        restartA();

        // Verify existence of the snapshot file after restart.
        assertSnapshotExists(getSnapshotDirA(), commitCounter);
        
    }

    /**
     * Verify that C snapshots the journal when it enters RunMet after
     * resynchronizing from A+B. (This can be just start A+B, await quorum meet,
     * then start C. C will resync from the leader. The snapshot should be taken
     * when resync is done and we enter RunMet.)
     */
    public void test_AB_snapshotOnQuorumMeet_C_snapshotOnResync()
            throws Exception {

        // Start A, B.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Await quorum meet.
        final long token = awaitMetQuorum();
        
        // Wait until both services are ready.
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);

        // Expected commit point on A, B.
        final long commitCounter = 1;
        
        // Verify/await expected commit point.
        awaitCommitCounter(commitCounter, serverA, serverB);

        // Verify/await snapshot on A.
        awaitSnapshotExists(serverA, commitCounter);

        // Verify/await snapshot on B.
        awaitSnapshotExists(serverB, commitCounter);

        // Start C. It will resync and join.
        final HAGlue serverC = startC();

        // Await fully quorum meet.
        final long token2 = awaitFullyMetQuorum();

        // Token is unchanged.
        assertEquals(token, token2);

        // Wait until service is ready.
        awaitNSSAndHAReady(serverC);

        // Verify that C took a snapshot.
        awaitSnapshotExists(serverC, commitCounter);

    }

    /**
     * Test is a variant on the above that verifies that the snapshot taken by C
     * is once it enters RunMet. It verifies this by issuing a update request
     * against (A+B) before starting C. This means that the snapshot on C will
     * be at commitCounter:=2 rather than commitCounter:=1.
     */
    public void test_AB_snapshotOnQuorumMeet_C_snapshotOnResync2()
            throws Exception {

        // Start A, B.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Await quorum meet.
        final long token = awaitMetQuorum();
        
        // Wait until both services are ready.
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);

        // Expected commit point on A, B.
        final long commitCounter = 1;
        
        // Verify/await expected commit point.
        awaitCommitCounter(commitCounter, serverA, serverB);

        // Verify/await snapshot on A.
        awaitSnapshotExists(serverA, commitCounter);

        // Verify/await snapshot on B.
        awaitSnapshotExists(serverB, commitCounter);

        // Take A+B through one more commit point.
        simpleTransaction();

        // Verify/await expected commit point.
        awaitCommitCounter(commitCounter + 1, serverA, serverB);

        // Start C. It will resync and join.
        final HAGlue serverC = startC();

        // Await fully quorum meet.
        final long token2 = awaitFullyMetQuorum();

        // Token is unchanged.
        assertEquals(token, token2);

        // Wait until service is ready.
        awaitNSSAndHAReady(serverC);

        // Verify/await expected commit point.
        awaitCommitCounter(commitCounter + 1, serverA, serverB, serverC);

        // Verify that C took a snapshot @ commitCounter:=2.
        awaitSnapshotExists(serverC, commitCounter + 1);

    }

    /**
     * Unit test verifies that C snapshots the journal when it is rebuilding
     * from the A+B. The leader must not have the HALogs on hand to trigger a
     * REBUILD rather than a RESYNC. E.g., delete the log files and snapshots on
     * the leader by hand. C should snapshot the journal when the REBUILD and
     * RESYNC are done and it enters RunMet.
     */
    public void test_AB_snapshotOnQuorumMeet_C_snapshotOnRebuild()
            throws Exception {


        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        
        // Current commit point.
        final long lastCommitCounter = 1;

        // Await initial commit point (KB create) on A + B.
        awaitCommitCounter(lastCommitCounter, serverA, serverB);

        /*
         * Verify that HALog files were generated and are available for commit
         * point ONE (1) on the services joined with the met quorum.
         */
        assertHALogDigestsEquals(1L/* firstCommitCounter */, lastCommitCounter,
                new HAGlue[] { serverA, serverB });

        // Verify binary equality of (A,B) journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        /*
         * Now go through a commit point with a met quorum. The HALog files
         * should be retained at that commit point.
         */
        simpleTransaction();

        // Current commit point.
        final long lastCommitCounter2 = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There are TWO (2) commit points.
        assertEquals(2L, lastCommitCounter2);

        // HALog files now exist for A & B, and original commitCounter!
        assertHALogDigestsEquals(lastCommitCounter, lastCommitCounter2,
                new HAGlue[] { serverA, serverB });
        
        // now remove the halog files ensuring a RESYNC is not possible
        // but MUST leave currently open file!!
        removeFiles(getHALogDirA(), HALogWriter.getHALogFileName(getHALogDirA(), lastCommitCounter2 + 1));
        removeFiles(getHALogDirB(), HALogWriter.getHALogFileName(getHALogDirB(), lastCommitCounter2 + 1));

        // Now Start 3rd service.
        final HAGlue serverC = startC();

        // Wait until the quorum is fully met.  After REBUILD
        assertEquals(token, awaitFullyMetQuorum());

        // HALog files now exist on ALL services @ current commit counter
        final long currentCommitCounter = lastCommitCounter2 + 1;
        assertHALogDigestsEquals(currentCommitCounter, currentCommitCounter,
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify that C took a snapshot *after* the REBUILD+RESYNC.
        awaitSnapshotExists(serverC, currentCommitCounter);

    }

}
