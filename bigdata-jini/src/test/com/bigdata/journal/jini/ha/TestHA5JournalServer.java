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
package com.bigdata.journal.jini.ha;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.rdf.sail.webapp.client.JettyHttpClient;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepositoryManager;

/**
 * HA5 test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA5JournalServer extends AbstractHA5JournalServerTestCase {

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
//        		"com.bigdata.journal.HAJournal.properties=" +TestHA3JournalServer.getTestHAJournalProperties(com.bigdata.journal.HAJournal.properties),
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
//                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\"",
                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
                "com.bigdata.journal.jini.ha.HAJournalServer.replicationFactor="+replicationFactor(),
        };
        
    }
    
    @Override
    protected int replicationFactor() {

        return 5;
        
    }

    public TestHA5JournalServer() {
    }

    public TestHA5JournalServer(String name) {
        super(name);
    }

    /**
     * Starts up a 5 member quorum and commits a transaction
     * 
     * @throws Exception
     */
    public void testStartABC_DE() throws Exception {
   
        doStartABC_DE();
        
    }
 
    protected void doStartABC_DE() throws Exception {

        // Start 3 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        
        // this should fail!!
        try {
        	quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        	
        	fail("HA5 requires quorum of 3!");
        } catch (TimeoutException te) {
        	// expected
        }
        
        
        final HAGlue serverC = startC();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        /*
         * Note: The quorum was not fully met at the last 2-phase commit.
         * Instead, 2 services participated in the 2-phase commit and the third
         * service resynchronized when it came up and then went through a local
         * commit. Therefore, the HALog files should exist on all nodes.
         */

        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        
        // Current commit point.
        final long lastCommitCounter = 1L;
        
        // Await first commit point (KB create) on A + B + C.
        awaitCommitCounter(lastCommitCounter, serverA, serverB, serverC);

        /*
         * Verify that HALog files were generated and are available for commit
         * point ONE (1) on the services joined with the met quorum.
         */
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                lastCommitCounter, new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of (A,B) journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify can not write on followers.
        assertWriteRejected(serverB);
        assertWriteRejected(serverC);
        
        // Start remaining services.
        final HAGlue serverD = startD();
        final HAGlue serverE = startE();
        
        log.warn("Awaiting full pipeline");
        awaitPipeline(new HAGlue[] {serverA, serverB, serverC, serverD, serverE});

        // Wait until the quorum is fully met. The token should not change.
        log.warn("awaitFullyMetQuorum");
        final long token2 = awaitFullyMetQuorum();
        assertEquals(token, token2);

        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        awaitHAStatus(serverD, HAStatusEnum.Follower);
        awaitHAStatus(serverE, HAStatusEnum.Follower);

        // The commit counter has not changed.
        assertEquals(
                lastCommitCounter,
                serverA.getRootBlock(
                        new HARootBlockRequest(null/* storeUUID */))
                        .getRootBlock().getCommitCounter());

        // HALog files now exist on ALL services.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, lastCommitCounter,
                new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        /*
         * Now go through a commit point with a fully met quorum. The HALog
         * files should be purged at that commit point.
         */
        simpleTransaction();

        // Current commit point.
        final long lastCommitCounter2 = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There are TWO (2) commit points.
        assertEquals(2L, lastCommitCounter2);

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify no HALog files since fully met quorum @ commit.
        assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter,
                new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Verify can not write on followers.
        assertWriteRejected(serverB);
        assertWriteRejected(serverC);
        assertWriteRejected(serverD);
        assertWriteRejected(serverE);

    }
    
    /**
     * HA5 is fully met after 5 services are started simultaneously
     */
    public void testABCDESimultaneous() throws Exception {

        final ABCDE startup = new ABCDE(false);

        awaitFullyMetQuorum();

        startup.assertDigestsEqual();
    }

    /**
     * HA5 is fully met after 5 services are started sequentially
     */
    public void testABCDESequential() throws Exception {

        final ABCDE startup = new ABCDE(true);

        awaitFullyMetQuorum();

        startup.assertDigestsEqual();
    }

    /**
     * HA5 remains met with 1 service failure
     */
    public void testABCDEShutdownC() throws Exception {

        final ABCDE startup = new ABCDE(true);

        final long token = awaitFullyMetQuorum();

        startup.assertDigestsEqual();

        shutdownC();

        awaitPipeline(new HAGlue[] { serverA, serverB, serverD, serverE });

        assertEquals(token, awaitMetQuorum());
    }

    /**
     * HA5 remains met with 2 service failures
     */
    public void testABCDEShutdownBD() throws Exception {

        final ABCDE startup = new ABCDE(true);

        final long token = awaitFullyMetQuorum();

        startup.assertDigestsEqual();

        shutdownB();
        shutdownD();

        awaitPipeline(new HAGlue[] { serverA, serverC, serverE });

        assertEquals(token, awaitMetQuorum());
    }

    /**
     * HA5 breaks with 3 service failures and re-meets when one is restarted
     */
    public void testABCDEShutdownBCD() throws Exception {

        final ABCDE startup = new ABCDE(true);

        final long token = awaitFullyMetQuorum();

        startup.assertDigestsEqual();

        shutdownB();
        shutdownC();
        shutdownD();

        // Non-deterministic pipeline order
        // awaitPipeline(new HAGlue[] {serverA, serverE});

        try {
            awaitMetQuorum();
            fail("Quorum should not be met");
        } catch (TimeoutException te) {
            // expected
        }

        startC();

        assertFalse(token == awaitMetQuorum());
    }

    /**
     * HA5 breaks when leader fails, meets on new token then fully meets on same
     * token when previous leader is restarted
     */
    public void testABCDEShutdownLeader() throws Exception {

        final ABCDE startup = new ABCDE(true);

        final long token = awaitFullyMetQuorum();

        startup.assertDigestsEqual();

        shutdownA();

        // pipeline order is non-deterministic

        final long token2 = awaitMetQuorum();

        assertFalse(token == token2);

        startA();

        assertTrue(token2 == awaitFullyMetQuorum());
    }
    
    /**
     * Similar to standard resync except that the resync occurs while a single
     * long transaction is in progress.
     * 
     * @throws Exception
     */
	public void testStartABC_DE_LiveResync() throws Exception {

		// Start 2 services.
		final HAGlue serverA = startA();
		final HAGlue serverB = startB();
		final HAGlue serverC = startC();

		// Wait for a quorum meet.
		final long token = quorum.awaitQuorum(awaitQuorumTimeout,
				TimeUnit.MILLISECONDS);

//		// Verify KB exists.
//		awaitKBExists(serverA);

		/*
		 * Note: The quorum was not fully met at the last 2-phase commit.
		 * Instead, 2 services participated in the 2-phase commit and the third
		 * service resynchronized when it came up and then went through a local
		 * commit. Therefore, the HALog files should exist on all nodes.
		 */

		// Current commit point.
		final long lastCommitCounter = 1;

        // Await initial commit point (KB create) on A + B.
        awaitCommitCounter(lastCommitCounter, serverA, serverB, serverC);

		/*
		 * Verify that HALog files were generated and are available for commit
		 * point ONE (1) on the services joined with the met quorum.
		 */
		assertHALogDigestsEquals(1L/* firstCommitCounter */,
				lastCommitCounter, new HAGlue[] { serverA, serverB, serverC });

		// Verify binary equality of (A,B) journals.
		assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

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

        /*
         * LOAD data on leader.
         */
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token, true/* reallyLargeLoad */));

        // Start LOAD.
        executorService.submit(ft);
        
		// allow load head start
		Thread.sleep(300/*ms*/);

		// Now Start D and E.
		final HAGlue serverD = startD();
		final HAGlue serverE = startE();

		// Wait until the quorum is fully met. After RESYNC
		assertEquals(token, awaitFullyMetQuorum(20));

		log.info("FULLY MET");
		
        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

		log.info("Should be safe to test digests now");
		
		// Cannot predict last commit counter or whether even logs will remain
		// assertHALogDigestsEquals(1L/* firstCommitCounter */,
		// lastCommitCounter2,
		// new HAGlue[] { serverA, serverB, serverC });

		// Verify binary equality of ALL journals.
		HAGlue[] services = new HAGlue[] { serverA, serverB, serverC, serverD, serverE };
		assertReady(services);
		// If the services are all ready then they MUST have compatible journals
		assertDigestsEquals(services);

		// Now force further commit when fully met to remove log files
		simpleTransaction();

		// Now verify no HALog files since fully met quorum @ commit.
		final long lastCommitCounter3 = serverA
				.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
				.getRootBlock().getCommitCounter();

		assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter3,
				new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // And again verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

		log.info("ALL GOOD!");
	}
	
    /**
     * Test quorum breaks and reforms when original leader fails
     * 
     * @throws Exception 
     */
	public void testQuorumBreaksABCDE_failLeader() throws Exception {

		// Start 3 services in sequence
		final ABCDE startup = new ABCDE(true/*sequential*/);

		// Wait for a quorum meet.       
		final long token = awaitMetQuorum();

//		// Verify KB exists.
//		awaitKBExists(startup.serverA);

		// Verify A is the leader.
		assertEquals(startup.serverA, quorum.getClient().getLeader(token));

        // Verify A is fully up.
        awaitNSSAndHAReady(startup.serverA);

		// Now fail leader!
		shutdownA();

		// Now check that quorum meets around the remaining 2 services.
		final long token2 = awaitNextQuorumMeet(token);

		// Verify that we have a new leader for the quorum.
		final HAGlue leader = quorum.getClient().getLeader(token2);

        assertTrue(leader.equals(startup.serverB)
                || leader.equals(startup.serverC)
                || leader.equals(startup.serverD)
                || leader.equals(startup.serverE));
        
	}
    
    /**
     * Test quorum breaks and reforms when original leader fails
     * 
     * @throws Exception 
     */
	public void testQuorumBreaksABCDE_killLeader() throws Exception {

		// Start 3 services in sequence
		final ABCDE startup = new ABCDE(true/*sequential*/);

		// Wait for a quorum meet.       
		final long token = awaitMetQuorum();

//		// Verify KB exists.
//		awaitKBExists(startup.serverA);

		// Verify A is the leader.
		assertEquals(startup.serverA, quorum.getClient().getLeader(token));

        // Verify A is fully up.
        awaitNSSAndHAReady(startup.serverA);

		// Now kill leader!
 		kill(startup.serverA);

		// Check that quorum meets around the remaining 2 services.
		final long token2 = awaitNextQuorumMeet(token);

		// Verify that we have a new leader for the quorum.
		final HAGlue leader = quorum.getClient().getLeader(token2);

        assertTrue(leader.equals(startup.serverB)
                || leader.equals(startup.serverC)
                || leader.equals(startup.serverD)
                || leader.equals(startup.serverE));
        
	}
    
    /**
     * 5 committed transactions then at 50ms delay between each
     * subsequent transaction.
     */
    public void testStartABC_DE_MultiTransactionResync_5tx_then_50ms_delay()
            throws Exception {

    	doStartABC_DE_MultiTransactionResync(50, 5);
    }

    /**
     * 10 committed transactions then at 200ms delay between each
     * subsequent transaction.
     */
    public void testStartABC_DE_MultiTransactionResync_10tx_then_200ms_delay()
            throws Exception {

    	doStartABC_DE_MultiTransactionResync(200, 10);
    }

    /**
     * Test where C starts after <i>initialTransactions</i> on A+B. A series of
     * transactions are issued with the specified delay.
     * 
     * @param transactionDelay
     *            The delay between the transactions.
     * @param initialTransactions
     *            The #of initial transactions before starting C.
     */
    private void doStartABC_DE_MultiTransactionResync(
            final long transactionDelay, final int initialTransactions)
            throws Exception {
        final long timeout = TimeUnit.MINUTES.toMillis(4);
        try {
            
			// Start 3 services.
			final HAGlue serverA = startA();
			final HAGlue serverB = startB();
			final HAGlue serverC = startC();

			// Wait for a quorum meet.
			final long token = quorum.awaitQuorum(awaitQuorumTimeout,
					TimeUnit.MILLISECONDS);

	        // Await initial commit point (KB create).
	        awaitCommitCounter(1L, serverA, serverB, serverC);
			
            final HAGlue leader = quorum.getClient().getLeader(token);

            // Verify assumption in this test.
            assertEquals(leader, serverA);

            // Wait until leader is ready.
            leader.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            
            /*
             * Note: The quorum was not fully met at the last 2-phase commit.
             * Instead, 2 services participated in the 2-phase commit and the
             * third service resynchronized when it came up and then went
             * through a local commit. Therefore, the HALog files should exist
             * on all nodes.
             */

			// Current commit point.
			final long lastCommitCounter = leader
					.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
					.getRootBlock().getCommitCounter();

			// There is ONE commit point.
			assertEquals(1L, lastCommitCounter);

            /*
             * Verify that HALog files were generated and are available for
             * commit point ONE (1) on the services joined with the met quorum.
             */
			assertHALogDigestsEquals(1L/* firstCommitCounter */,
					lastCommitCounter, new HAGlue[] { serverA, serverB, serverC });

			// Verify binary equality of (A,B) journals.
			assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

			/*
			 * Now go through a commit point with a met quorum. The HALog files
			 * should be retained at that commit point.
			 */
			simpleTransaction();

			// Current commit point.
			final long lastCommitCounter2 = leader
					.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
					.getRootBlock().getCommitCounter();

			// There are TWO (2) commit points.
			assertEquals(2L, lastCommitCounter2);

			// start concurrent task loads that continue until fully met
			final Callable<Void> task = new Callable<Void>() {
				public Void call() throws Exception {
					int count = 0;
						while (!quorum.isQuorumFullyMet(token)) {

							final StringBuilder sb = new StringBuilder();
							sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
							sb.append("INSERT DATA {\n");
							sb.append("  <http://example/book" + count++
									+ "> dc:title \"A new book\" ;\n");
							sb.append("  dc:creator \"A.N.Other\" .\n");
							sb.append("}\n");

							final String updateStr = sb.toString();

							final HAGlue leader = quorum.getClient().getLeader(
									token);

							// Verify quorum is still valid.
							quorum.assertQuorum(token);

					        final JettyHttpClient client = new JettyHttpClient();
					        client.start();
					        
							final JettyRemoteRepositoryManager repo = getRemoteRepository(leader, client);
				        	try {
				        		repo.prepareUpdate(
										updateStr).evaluate();
								log.warn("COMPLETED TRANSACTION " + count);
				        	} finally {
				        		repo.close();
				        		client.stop();
				        	}

								Thread.sleep(transactionDelay);
						}
						// done.
						return null;
				}
			};
			final FutureTask<Void> ft = new FutureTask<Void>(task);

			executorService.submit(ft);
			
            try {
                // Allow number of transactions through first
                ft.get(initialTransactions * transactionDelay,
                        TimeUnit.MILLISECONDS);
                if (ft.isDone()) {
                    fail("Not expecting task to be finished.");
                }
            } catch (TimeoutException ex) {
                // Ignore expected exception.
            }

			// Now Start D and E service.
			final HAGlue serverD = startD();
			final HAGlue serverE = startE();

			// Wait until the quorum is fully met. After RESYNC
			assertEquals(token, awaitFullyMetQuorum(10));

            awaitHAStatus(new HAStatusEnum[] { HAStatusEnum.Leader,
                    HAStatusEnum.Follower, HAStatusEnum.Follower, HAStatusEnum.Follower, HAStatusEnum.Follower },
                    new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

			log.info("FULLY MET");

			// Wait for task to end. Check Future.
			ft.get(timeout,TimeUnit.MILLISECONDS);

			log.info("Should be safe to test digests now");

		// Cannot predict last commit counter or whether even logs will remain
			// assertHALogDigestsEquals(1L/* firstCommitCounter */,
			// lastCommitCounter2,
			// new HAGlue[] { serverA, serverB, serverC });

            // But ONLY if fully met after load!
            assertTrue(quorum.isQuorumFullyMet(token));
            
            awaitHAStatus(new HAStatusEnum[] { HAStatusEnum.Leader,
                    HAStatusEnum.Follower, HAStatusEnum.Follower, HAStatusEnum.Follower, HAStatusEnum.Follower },
                    new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

            /*
             * Verify binary equality of ALL journals.
             * 
             * Note: A failure to have digest equality on the last follower
             * after a service join has been diagnosed as a problem of the last
             * follower to ensure that it's service join was visible to the
             * leader before unblocking the write replication pipeline. This
             * could result in the leader not including the newly joined
             * follower in the 2-phase commit.
             */
			assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

			// Now force further commit when fully met to remove log files
			simpleTransaction();

			// And again verify binary equality of ALL journals.
			// assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

			// Now verify no HALog files since fully met quorum @ commit.
			final long lastCommitCounter3 = leader
					.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
					.getRootBlock().getCommitCounter();
			assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter3,
					new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

		} finally {
			destroyAll();
		}
	}

    /**
     * Test Rebuild of early starting D&E services where quorum was previously
     * fully met.  This forces a pipeline re-organisation
     * 
	 * @throws Exception 
     */
    public void testStartABCDE_RebuildWithPipelineReorganisation() throws Exception {

        new ABCDE(true/* sequential */);

        awaitFullyMetQuorum();
    	 	
        // Now run several transactions
        for (int i = 0; i < 5; i++)
            simpleTransaction();

        // destroy D&E
        destroyD();
        destroyE();
        // clean shutdown ABC
        shutdownA();
        shutdownB();
        shutdownC();
        
        /*
         * Now restart all but restart D & E first.
         * 
         * Note: We start D & E first so they will be in the way when A or B or C attempts
         * to become the leader, thus forcing a pipeline reorganization.
         */
        final HAGlue serverD = startD();
        awaitPipeline(new HAGlue[] { serverD });
        final HAGlue serverE = startE();
        awaitPipeline(new HAGlue[] { serverD, serverE });

        // Now start A.
        final HAGlue serverA = startA();
        awaitPipeline(new HAGlue[] { serverD, serverE, serverA });
        
        // Now B.
        final HAGlue serverB = startB();
        awaitPipeline(new HAGlue[] { serverD, serverE, serverA, serverB });
        
        // And finally start C.
        final HAGlue serverC = startC();

        // A & B should meet
        final long token2 = awaitMetQuorum();

        // The expected pipeline.  D&E were moved to the end (in order?).
        awaitPipeline(new HAGlue[] { serverA, serverB, serverC, serverD, serverE });

        // Wait until A is fully ready.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));
        
        // Verify A is the leader.
        assertEquals(serverA, quorum.getClient().getLeader(token2));
        
        // Check journals for equality on A, B, C.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
        
        // Check HALogs equal on A, B, C.
        assertHALogDigestsEquals(7L/* firstCommitCounter */, 7L, new HAGlue[] {
                serverA, serverB, serverC });

        // D and E will have go through Rebuild before joining
        assertEquals(token2, awaitFullyMetQuorum(20/* ticks */));

//        Note: I have seen this timeout. This warrants exploring. BBT.
//        // Wait until C is fully ready.
        assertEquals(HAStatusEnum.Follower, awaitNSSAndHAReady(serverD, 4*awaitQuorumTimeout, TimeUnit.MILLISECONDS));
        assertEquals(HAStatusEnum.Follower, awaitNSSAndHAReady(serverE, 4*awaitQuorumTimeout, TimeUnit.MILLISECONDS));

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC, serverD, serverE});

        // Check HALogs equal on ALL services.
        assertHALogDigestsEquals(7L/* firstCommitCounter */, 7L, new HAGlue[] {
                serverA, serverB, serverC, serverD, serverE });

    }

}
