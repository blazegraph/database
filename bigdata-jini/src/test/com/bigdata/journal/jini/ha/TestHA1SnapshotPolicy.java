package com.bigdata.journal.jini.ha;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepositoryManager;

/**
 * Test suite for the restore of the HA1 Journal from a snapshot and transaction
 * logs.
 */
public class TestHA1SnapshotPolicy extends AbstractHA3BackupTestCase {

    public TestHA1SnapshotPolicy() {
    }

    public TestHA1SnapshotPolicy(String name) {
        super(name);
    }
 
    @Override
    protected int replicationFactor() {

        return 1;
        
    }

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
        
        /*
         * For HA1, must have onlineDisasterRecovery to ensure logs are maintained
         */
        return new String[]{
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.DefaultSnapshotPolicy("+neverRun+",0)",
                // "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
                // "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
                "com.bigdata.journal.jini.ha.HAJournalServer.replicationFactor="+replicationFactor(),
        };
        
    }

    /**
     * Start a service. The quorum meets. Take a snapshot. Verify that the
     * snapshot appears within a reasonable period of time and that it is for
     * <code>commitCounter:=1</code> (just the KB create). Verify that the
     * digest of the snapshot agrees with the digest of the journal.
     */
    public void testA_snapshot() throws Exception {

        // Start 1 service.
        final HAGlue serverA = startA();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA);

        final HAGlue leader = quorum.getClient().getLeader(token);
        assertEquals(serverA, leader); // A is the leader.
        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Verify quorum is at the expected commit point.
            assertEquals(
                    1L,
                    leader.getRootBlock(
                            new HARootBlockRequest(null/* storeUUID */))
                            .getRootBlock().getCommitCounter());

            // Snapshot directory is empty.
            assertEquals(0, recursiveCount(getSnapshotDirA(),SnapshotManager.SNAPSHOT_FILTER));

            final Future<IHASnapshotResponse> ft = leader
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // wait for the snapshot.
            try {
                ft.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                ft.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }

            final IRootBlockView snapshotRB = ft.get().getRootBlock();

            final long commitCounter = 1L;
            
            // Verify snapshot is for the expected commit point.
            assertEquals(commitCounter, snapshotRB.getCommitCounter());

            // Snapshot directory contains the desired filename.
            assertExpectedSnapshots(getSnapshotDirA(),
                    new long[] { commitCounter });
                    
            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(leader, commitCounter);

        }

    }
    /**
     * Start service. The quorum meets. Take a snapshot. Verify that the
     * snapshot appears within a resonable period of time and that it is for
     * <code>commitCounter:=1</code> (just the KB create). Request a second
     * snapshot for the same commit point and verify that a <code>null</code> is
     * returned since we already have a snapshot for that commit point.
     */
    public void testA_snapshot_await_snapshot_null() throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA);

        final HAGlue leader = quorum.getClient().getLeader(token);

        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Verify quorum is at the expected commit point.
            assertEquals(
                    1L,
                    leader.getRootBlock(
                            new HARootBlockRequest(null/* storeUUID */))
                            .getRootBlock().getCommitCounter());

            // Snapshot directory is empty.
            assertEquals(0, recursiveCount(getSnapshotDirA(),SnapshotManager.SNAPSHOT_FILTER));

            final Future<IHASnapshotResponse> ft = leader
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // wait for the snapshot.
            try {
                ft.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                ft.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }

            final IRootBlockView snapshotRB = ft.get().getRootBlock();

            final long commitCounter = 1L;
            
            // Verify snapshot is for the expected commit point.
            assertEquals(commitCounter, snapshotRB.getCommitCounter());

            // Snapshot directory contains the expected snapshot(s).
            assertExpectedSnapshots(getSnapshotDirA(),
                    new long[] { commitCounter });

            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(leader, commitCounter);

        }

        /*
         * Verify 2nd request returns null since snapshot exists for that
         * commit point.
         */
        {

            // Verify quorum is still at the expected commit point.
            assertEquals(
                    1L,
                    leader.getRootBlock(
                            new HARootBlockRequest(null/* storeUUID */))
                            .getRootBlock().getCommitCounter());

            // request another snapshot.
            final Future<IHASnapshotResponse> ft = leader
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            if (ft != null) {

                ft.cancel(true/* mayInteruptIfRunning */);

                fail("Expecting null since snapshot exists for current commit point.");

            }

        }

    }

    /**
     * Test ability to request a snapshot using an HTTP GET
     * <code>.../status?snapshot</code>.
     * 
     * TODO Variant where the percentLogSize parameter is also expressed and
     * verify that the semantics of that argument are obeyed. Use this to verify
     * that the server will not take snapshot if size on disk of HALog files
     * since the last snapshot is LT some percentage.
     */
    public void testA_snapshot_HTTP_GET() throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA);

        final HAGlue leader = quorum.getClient().getLeader(token);

        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Verify quorum is at the expected commit point.
            assertEquals(
                    1L,
                    leader.getRootBlock(
                            new HARootBlockRequest(null/* storeUUID */))
                            .getRootBlock().getCommitCounter());

            // Snapshot directory is empty.
            assertEquals(0, recursiveCount(getSnapshotDirA(),SnapshotManager.SNAPSHOT_FILTER));

            doSnapshotRequest(leader);

            /*
             * Get the Future. Should still be there, but if not then will be
             * null (it which case the snapshot is already done).
             */
            final Future<IHASnapshotResponse> ft = leader
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // wait for the snapshot.
            if (ft != null) {
                try {
                    ft.get(5, TimeUnit.SECONDS);
                } catch (TimeoutException ex) {
                    ft.cancel(true/* mayInterruptIfRunning */);
                    throw ex;
                }

                final IRootBlockView snapshotRB = ft.get().getRootBlock();

                // Verify snapshot is for the expected commit point.
                assertEquals(1L, snapshotRB.getCommitCounter());

            } else {
                
                // Snapshot completed before we got the Future.
                
            }

            final long commitCounter = 1L;

            // Snapshot directory contains the desired filename.
            assertExpectedSnapshots(getSnapshotDirA(),
                    new long[] { commitCounter });

            assertSnapshotDigestEquals(leader, commitCounter);

        }

    }

    /**
     * Verify will not take snapshot if running. Loads a bunch of data and then
     * issues (2) snapshot requests. Should both should return the same future
     * since the snapshot will take longer to create than the latency for the
     * 2nd RMI.
     */
    public void testA_snapshot_running_2nd_snapshot_same_future()
			throws Exception {

		// Start 2 services.
		final HAGlue serverA = startA();

		// Wait for a quorum meet.
		final long token = quorum.awaitQuorum(awaitQuorumTimeout,
				TimeUnit.MILLISECONDS);

		// Await initial commit point (KB create).
		awaitCommitCounter(1L, serverA);

		// The joined services, in their service join order.
		final UUID[] joined = quorum.getJoined();

		// The HAGlue interfaces for those joined services, in join order.
		final HAGlue[] services = new HAGlue[joined.length];

		final JettyRemoteRepositoryManager[] repos = new JettyRemoteRepositoryManager[joined.length];
		try {
			for (int i = 0; i < joined.length; i++) {

				services[i] = quorum.getClient().getService(joined[i]);

				repos[i] = getRemoteRepository(services[i]);

			}

			/*
			 * LOAD data on leader.
			 */
			{

				final FutureTask<Void> ft = new FutureTask<Void>(
						new LargeLoadTask(token, true/* reallyLargeLoad */));

				executorService.submit(ft);

				// impose timeout on load.
				ft.get(2 * longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

			}

			// Current commit point.
			final long lastCommitCounter2 = 2L;

			// There are now TWO (2) commit points.
			awaitCommitCounter(lastCommitCounter2, serverA);

			/*
			 * Verify that query on all nodes is allowed and now provides a
			 * non-empty result.
			 */
			for (JettyRemoteRepositoryManager r : repos) {

				// Should have data.
				assertEquals(
						100L,
						countResults(r.prepareTupleQuery(
								"SELECT * {?a ?b ?c} LIMIT 100").evaluate()));

			}

			final HAGlue leader = quorum.getClient().getLeader(token);

			{

				// Verify quorum is still valid.
				quorum.assertQuorum(token);

				// FIXME: Snapshot directory is empty.
				assertEquals(
						1,
						recursiveCount(getSnapshotDirA(),
								SnapshotManager.SNAPSHOT_FILTER));

				final Future<IHASnapshotResponse> ft = leader
						.takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

				final Future<IHASnapshotResponse> ft2 = leader
						.takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

				// Both Futures are non-null.
				assertNotNull(ft);

				assertNotNull(ft2);

				// Neither Future is done.
				assertFalse(ft.isDone());

				assertFalse(ft2.isDone());

				// wait for the snapshot.
				try {
					ft.get(20, TimeUnit.SECONDS);
				} catch (TimeoutException ex) {
					// Interrupt both futures.
					ft.cancel(true/* mayInterruptIfRunning */);
					ft2.cancel(true/* mayInterruptIfRunning */);
					throw ex;
				}

				// Verify 2nd future is also done (the should be two proxies for
				// the
				// same future).
				assertTrue(ft2.isDone());

				// Verify no error on 2nd future.
				ft2.get();

				final IRootBlockView snapshotRB = ft.get().getRootBlock();

				final long commitCounter = 2L;

				// Verify snapshot is for the expected commit point.
				assertEquals(commitCounter, snapshotRB.getCommitCounter());

				// Snapshot directory contains the desired filename.
				assertExpectedSnapshots(getSnapshotDirA(),
						new long[] { commitCounter });

				// Verify digest of snapshot agrees with digest of journal.
				assertSnapshotDigestEquals(leader, commitCounter);

			}
		} finally {
			for (JettyRemoteRepositoryManager r : repos) {

				if (r != null)
					r.close();

			}
		}

	}
    /**
     * Unit test starts A and runs N transactions. It then takes a snapshot.
     * The existance of the snapshot is verified, as is the existence of the
     * HALog files for each transaction. Finally, it runs another M
     * transactions.
     * <p>
     * The {@link HARestore} utility is then used to reconstruct a
     * {@link Journal} from the snapshot and replay the HALog files. We then
     * verify that the new journal is at the correct commit point and compare it
     * for binary equality with the original journal on A (same digests).
     */
    public void testA_snapshot_multipleTx_restore_validate() throws Exception {

        final int N1 = 7; // #of transactions to run before the snapshot.
        final int N2 = 8; // #of transactions to run after the snapshot.
        
        // Start service.
        final HAGlue serverA = startA();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Verify A is the leader.
        assertEquals(serverA, quorum.getClient().getLeader(token));

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA);
        
        // wait until A is ready.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));

        // Now run N transactions.
        for (int i = 0; i < N1; i++) {

            simpleTransaction();
            
        }

        final long commitCounterN1 = N1 + 1;

        awaitCommitCounter(commitCounterN1, serverA);

        /*
         * Take a snapshot.
         */
        {

            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            // Snapshot directory is empty.
            assertEquals(1, recursiveCount(getSnapshotDirA(),SnapshotManager.SNAPSHOT_FILTER));

            // request snapshot on A.
            final Future<IHASnapshotResponse> ft = serverA
                    .takeSnapshot(new HASnapshotRequest(0/* percentLogSize */));

            // wait for the snapshot.
            try {
                ft.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                ft.cancel(true/* mayInterruptIfRunning */);
                throw ex;
            }

            final IRootBlockView snapshotRB = ft.get().getRootBlock();

            // Verify snapshot is for the expected commit point.
            assertEquals(commitCounterN1, snapshotRB.getCommitCounter());

            // Snapshot directory contains the desired filename.
            assertExpectedSnapshots(getSnapshotDirA(), new long[]{commitCounterN1});

            // Verify digest of snapshot agrees with digest of journal.
            assertSnapshotDigestEquals(serverA, commitCounterN1);

        }

        {
            // Snapshot directory contains just the expected snapshot
            assertExpectedSnapshots(getSnapshotDirA(),
                    new long[] { commitCounterN1 });

            /*
             * Now, get the snapshot that we took above, decompress it, and then
             * roll it forward and verify it against the current committed
             * journal.
             */
            doRestoreA(serverA, commitCounterN1);
        }

        // Now run M transactions.
        for (int i = 0; i < N2; i++) {

            simpleTransaction();
            
        }

        final long commitCounterN2 = N2 + N1 + 1;

        awaitCommitCounter(commitCounterN2, serverA);

        // Snapshot directory contains just the expected snapshot
        assertExpectedSnapshots(getSnapshotDirA(), new long[]{commitCounterN1});

        /*
         * Now, get the snapshot that we took above, decompress it, and then
         * roll it forward and verify it against the current committed journal.
         */
        doRestoreA(serverA, commitCounterN1);
        
    }

}
