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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.webapp.client.HAStatusEnum;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * Test suites for an {@link HAJournalServer} quorum with a replication factor
 * of THREE (3) and a fully met {@link Quorum}.
 * 
 * TODO Do we have any guards against rolling back a service in RESYNC if the
 * other services are more than 2 commit points before it? We probably should
 * not automatically roll it back to the other services in this case, but that
 * could also reduce the ergonomics of the HA3 configuration.
 * 
 * TODO All of these live load remains met tests could also be done with BOUNCE
 * rather than SHUTDOWN/RESTART. BOUNCE exercises different code paths and
 * corresponds to a zookeeper timeout, e.g., as might occur during a full GC
 * pause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA3JournalServer extends AbstractHA3JournalServerTestCase {

    public TestHA3JournalServer() {
    }

    public TestHA3JournalServer(String name) {
        super(name);
    }

    /**
     * Start 2 services and wait for a quorum meet. Verify that the services
     * have the same data and that the HALog files exist. Start a 3rd service.
     * The 3rd service should synchronize with the met quorum, yielding a fully
     * met quorum. Verify that the same data exists on all services and that the
     * HALog files are present on all services since we have not yet gone
     * through a 2-phase commit with a fully met quorum. Finally, write some
     * data and commit. That commit will be with a fully met quorum. Therefore,
     * the data on all services should be identical and the HALog files should
     * have been purged on each service.
     */
    public void testStartAB_C() throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        /*
         * Note: The quorum was not fully met at the last 2-phase commit.
         * Instead, 2 services participated in the 2-phase commit and the third
         * service resynchronized when it came up and then went through a local
         * commit. Therefore, the HALog files should exist on all nodes.
         */

        // Current commit point.
        final long lastCommitCounter = 1L;
        
        // Await first commit point (KB create) on A + B.
        awaitCommitCounter(lastCommitCounter, serverA, serverB);

        /*
         * Verify that HALog files were generated and are available for commit
         * point ONE (1) on the services joined with the met quorum.
         */
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                lastCommitCounter, new HAGlue[] { serverA, serverB });

        // Verify binary equality of (A,B) journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        // Start 3rd service.
        final HAGlue serverC = startC();

        // Wait until the quorum is fully met. The token should not change.
        assertEquals(token, awaitFullyMetQuorum());

        // The commit counter has not changed.
        assertEquals(
                lastCommitCounter,
                serverA.getRootBlock(
                        new HARootBlockRequest(null/* storeUUID */))
                        .getRootBlock().getCommitCounter());

        // HALog files now exist on ALL services.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, lastCommitCounter,
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

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
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify no HALog files since fully met quorum @ commit.
        assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter,
                new HAGlue[] { serverA, serverB, serverC });

    }

    /**
     * Test verifies that we can concurrently start 3 services, that the quorum
     * will meet, that the KB is created, and that the journal digests are
     * equals.
     */
    public void testStartABCSimultaneous() throws Exception {
    	
        final ABC abc = new ABC(false/*sequential*/); // simultaneous start.

        final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

//        // Verify KB exists on leader.
//        final HAGlue leader = quorum.getClient().getLeader(token);

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        /*
         * Note: Since the sevices were started concurrently, it is POSSIBLE
         * (unlikely, but possible) that all services would have been joined by
         * the time the quorum went through a 2-phase commit. Therefore, this
         * test does NOT verify whether or not the HALog files exist since it
         * can not reliably predict whether or not the quorum was fully met at
         * the 2-phase commit.
         */
    }

    /**
     * Start 3 services. Verify quorum meets and is fully met and that the
     * journal digests are equals. Verify that there are no HALog files since
     * the quorum was fully met at the commit point (for the KB create). Load a
     * large data set into the quorum and commit. Verify that we can read on all
     * nodes in the qourum. Verify that the journal digests are equals. Verify
     * that there are no HALog files since the quorum was fully met at the
     * commit point.
     */
    public void testABC_LargeLoad() throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        
        // wait for a quorum met.
        final long token = awaitMetQuorum();

        // Current commit point.
        final long lastCommitCounter = 1L;

        // Verify 1st commit point is visible on A + B.
        awaitCommitCounter(lastCommitCounter, new HAGlue[] { serverA, serverB });
        
        // start C.
        final HAGlue serverC = startC();

        // wait for a fully met quorum.
        assertEquals(token, awaitFullyMetQuorum());

        // Verify 1st commit point is visible on all services.
        awaitCommitCounter(lastCommitCounter, new HAGlue[] { serverA, serverB,
                serverC });

        /*
         * Verify that HALog files were generated and are available for commit
         * point ONE (1) on the services joined with the met quorum (we have not
         * gone through a 2-phase commit with a fully met quorum).
         */
        assertHALogDigestsEquals(1L/* firstCommitCounter */, lastCommitCounter,
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // The joined services, in their service join order.
        final UUID[] joined = quorum.getJoined();

        // The HAGlue interfaces for those joined services, in join order.
        final HAGlue[] services = new HAGlue[joined.length];

        final RemoteRepository[] repos = new RemoteRepository[joined.length];
        
        for (int i = 0; i < joined.length; i++) {

            services[i] = quorum.getClient().getService(joined[i]);

            repos[i] = getRemoteRepository(services[i]);
            
        }
        
        /*
         * Verify that query on all nodes is allowed and produces nothing
         * before we write anything.
         */
        for (RemoteRepository r : repos) {

            // Should be empty.
            assertEquals(0L,
                    countResults(r.prepareTupleQuery("SELECT * {?a ?b ?c}")
                            .evaluate()));

        }

        /*
         * LOAD data on leader.
         */
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token, true/* reallyLargeLoad */));

        // Start LOAD.
        executorService.submit(ft);
        
        // Await LOAD, but with a timeout.
        ft.get(loadLoadTimeoutMillis, TimeUnit.MILLISECONDS);

        /*
         * Verify that query on all nodes is allowed and now provides a
         * non-empty result.
         */
        for (RemoteRepository r : repos) {

            // Should have data.
            assertEquals(100L,
                    countResults(r.prepareTupleQuery("SELECT * {?a ?b ?c} LIMIT 100")
                            .evaluate()));

        }

        // Current commit point.
        final long lastCommitCounter2 = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There are now TWO (2) commit points.
        assertEquals(2L,lastCommitCounter2);
        
        // Verify binary equality.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify no HALog files since fully met quorum @ commit.
        assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter2,
                new HAGlue[] { serverA, serverB, serverC });

        /*
         * Do a "DROP ALL" and reverify that no solutions are found on each
         * service.
         */
        {
            
            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            repos[0].prepareUpdate("DROP ALL").evaluate();

        }
        
        /*
         * Verify that query on all nodes is allowed and now provides an empty
         * result.
         */
        for (RemoteRepository r : repos) {

            // Should be empty.
            assertEquals(
                    0L,
                    countResults(r.prepareTupleQuery(
                            "SELECT * {?a ?b ?c} LIMIT 100").evaluate()));

        }

        // Current commit point.
        final long lastCommitCounter3 = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There are now THREE (3) commit points.
        assertEquals(3L, lastCommitCounter3);

        // Verify binary equality.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify no HALog files since fully met quorum @ commit.
        assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter2,
                new HAGlue[] { serverA, serverB, serverC });

        /*
         * TODO Continue test and verify restart? Or verify restart before we do
         * the DROP ALL?
         */
        
    }
    
    /**
     * Test Resync of late starting C service
     * @throws Exception 
     */
    public void testStartAB_C_Resync() throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

//        // Verify KB exists.
//        awaitKBExists(serverA);
        
        /*
         * Note: The quorum was not fully met at the last 2-phase commit.
         * Instead, 2 services participated in the 2-phase commit and the third
         * service resynchronized when it came up and then went through a local
         * commit. Therefore, the HALog files should exist on all nodes.
         */

        // Current commit point.
        final long lastCommitCounter = 1;

        // Await initial commit point (KB create) on A + B.
        awaitCommitCounter(lastCommitCounter, serverA, serverB);

        /*
         * Verify that HALog files were generated and are available for commit
         * point ONE (1) on the services joined with the met quorum.
         */
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                lastCommitCounter, new HAGlue[] { serverA, serverB });

        // Verify binary equality of (A,B) journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        /*
         * Now go through a commit point with a met quorum. The HALog
         * files should be retained at that commit point.
         */
        simpleTransaction();

        // Current commit point.
        final long lastCommitCounter2 = 2;

        // Await 2nd commit point on A + B.
        awaitCommitCounter(lastCommitCounter2, serverA, serverB);

        // Now Start 3rd service.
        final HAGlue serverC = startC();

        // Wait until the quorum is fully met.  After RESYNC
        assertEquals(token, awaitFullyMetQuorum());

        // HALog files now exist on ALL services, on original commitCounter!
        assertHALogDigestsEquals(1L/* firstCommitCounter */, lastCommitCounter2,
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Now force further commit when fully met to remove log files
        simpleTransaction();

        // And again verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
        
        // Now verify no HALog files since fully met quorum @ commit.
        final long lastCommitCounter3 = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();
        assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter3,
                new HAGlue[] { serverA, serverB, serverC });

    }
    
    /**
     * TWO (2) committed transactions then at 3000ms delay between each
     * subsequent transaction.
     * <P>
     * Note: C should easily catch up with plenty of quiescence
     * 
     * @throws Exception
     */
    public void testStartAB_C_MultiTransactionResync_2tx_then_3000ms_delay()
            throws Exception {
     
        doStartAB_C_MultiTransactionResync(3000, 2);
        
    }

    /**
     * FIVE (5) committed transactions then at 1500ms delay between each
     * subsequent transaction.
     */
    public void testStartAB_C_MultiTransactionResync_5tx_then_1500ms_delay()
            throws Exception {

        doStartAB_C_MultiTransactionResync(1500, 5);
        
    }

    /**
     * FIVE (5) committed transactions then at 500ms delay between each
     * subsequent transaction.
     * <p>
     * Note: C might not catch up since the transactions are spaced more closely
     * together.
     */
    public void testStartAB_C_MultiTransactionResync_5tx_then_500ms_delay()
            throws Exception {

        doStartAB_C_MultiTransactionResync(500, 5);
        
    }

    /**
     * FIVE (5) committed transactions then at 200ms delay between each
     * subsequent transaction.
     * <p>
     * Note: C might not catch up since the transactions are spaced more closely
     * together.
     */
    public void testStartAB_C_MultiTransactionResync_5tx_then_200ms_delay()
            throws Exception {

        doStartAB_C_MultiTransactionResync(200, 5);
        
    }

    /**
     * ZERO (0) committed transactions then at 500ms delay between each
     * subsequent transaction.
     * <P>
     * Note: EARLY STARTING C
     */
    public void testStartAB_C_MultiTransactionResync_0tx_then_500ms_delay()
            throws Exception {

        doStartAB_C_MultiTransactionResync(500, 0);
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
    private void doStartAB_C_MultiTransactionResync(
            final long transactionDelay, final int initialTransactions)
            throws Exception {

        try {
            
			// Start 2 services.
			final HAGlue serverA = startA();
			final HAGlue serverB = startB();

			// Wait for a quorum meet.
			final long token = quorum.awaitQuorum(awaitQuorumTimeout,
					TimeUnit.MILLISECONDS);

	        // Await initial commit point (KB create).
	        awaitCommitCounter(1L, serverA, serverB);
			
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
					lastCommitCounter, new HAGlue[] { serverA, serverB });

			// Verify binary equality of (A,B) journals.
			assertDigestsEquals(new HAGlue[] { serverA, serverB });

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

								getRemoteRepository(leader).prepareUpdate(
										updateStr).evaluate();
								log.warn("COMPLETED TRANSACTION " + count);

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

			// Now Start 3rd service.
			final HAGlue serverC = startC();

			// Wait until the quorum is fully met. After RESYNC
			assertEquals(token, awaitFullyMetQuorum(10));

			log.info("FULLY MET");

			// Wait for task to end. Check Future.
			ft.get();

			log.info("Should be safe to test digests now");

		// Cannot predict last commit counter or whether even logs will remain
			// assertHALogDigestsEquals(1L/* firstCommitCounter */,
			// lastCommitCounter2,
			// new HAGlue[] { serverA, serverB, serverC });

			// Verify binary equality of ALL journals.
			assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

			// Now force further commit when fully met to remove log files
			simpleTransaction();

			// And again verify binary equality of ALL journals.
			// assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

			// Now verify no HALog files since fully met quorum @ commit.
			final long lastCommitCounter3 = leader
					.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
					.getRootBlock().getCommitCounter();
			assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter3,
					new HAGlue[] { serverA, serverB, serverC });

		} finally {
			destroyAll();
		}
	}
    
    /**
     * Similar to standard resync except that the resync occurs while a single
     * long transaction is in progress.
     * 
     * @throws Exception
     */
	public void testStartAB_C_LiveResync() throws Exception {

		// Start 2 services.
		final HAGlue serverA = startA();
		final HAGlue serverB = startB();

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
        awaitCommitCounter(lastCommitCounter, serverA, serverB);

		/*
		 * Verify that HALog files were generated and are available for commit
		 * point ONE (1) on the services joined with the met quorum.
		 */
		assertHALogDigestsEquals(1L/* firstCommitCounter */,
				lastCommitCounter, new HAGlue[] { serverA, serverB });

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

        /*
         * LOAD data on leader.
         */
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token, true/* reallyLargeLoad */));

        // Start LOAD.
        executorService.submit(ft);
        
		// allow load head start
		Thread.sleep(300/*ms*/);

		// Now Start 3rd service.
		final HAGlue serverC = startC();

		// Wait until the quorum is fully met. After RESYNC
		assertEquals(token, awaitFullyMetQuorum(20));

		log.info("FULLY MET");
		
        // Await LOAD, but with a timeout.
        ft.get(loadLoadTimeoutMillis, TimeUnit.MILLISECONDS);

		log.info("Should be safe to test digests now");
		
		// Cannot predict last commit counter or whether even logs will remain
		// assertHALogDigestsEquals(1L/* firstCommitCounter */,
		// lastCommitCounter2,
		// new HAGlue[] { serverA, serverB, serverC });

		// Verify binary equality of ALL journals.
		assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

		// Now force further commit when fully met to remove log files
		simpleTransaction();

		// Now verify no HALog files since fully met quorum @ commit.
		final long lastCommitCounter3 = serverA
				.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
				.getRootBlock().getCommitCounter();

		assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter3,
				new HAGlue[] { serverA, serverB, serverC });

        // And again verify binary equality of ALL journals.
         assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

		log.info("ALL GOOD!");
	}
    
    /**
     * Test Rebuild of late starting C service - simulates scenario where a service is removed from a
     * fully met quorum and a new service is added.
     * 
     * The test is similar to resync except that it forces the removal of the log files for the met quorum
     * before joining the third service, therefore preventing the RESYNC and forcing a REBUILD
     * 
     * @throws Exception 
     */
    public void testStartAB_C_Rebuild() throws Exception {


        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

//        // Verify KB exists.
//        awaitKBExists(serverA);
        
        /*
         * Note: The quorum was not fully met at the last 2-phase commit.
         * Instead, 2 services participated in the 2-phase commit and the third
         * service resynchronized when it came up and then went through a local
         * commit. Therefore, the HALog files should exist on all nodes.
         */

        // Current commit point.
        final long lastCommitCounter = 1;

        // Await initial commit point (KB create) on A + B.
        awaitCommitCounter(lastCommitCounter, serverA, serverB);

        /*
         * Verify that HALog files were generated and are available for commit
         * point ONE (1) on the services joined with the met quorum.
         */
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                lastCommitCounter, new HAGlue[] { serverA, serverB });

        // Verify binary equality of (A,B) journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        /*
         * Now go through a commit point with a met quorum. The HALog
         * files should be retained at that commit point.
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
        final String openLog = HALogWriter.getHALogFileName(lastCommitCounter2 + 1);
        final File serviceDir = new File("benchmark/CI-HAJournal-1");
        removeFiles(new File(serviceDir, "A/HALog"), openLog);
        removeFiles(new File(serviceDir, "B/HALog"), openLog);

        // Now Start 3rd service.
        final HAGlue serverC = startC();

        // Wait until the quorum is fully met.  After REBUILD
        assertEquals(token, awaitFullyMetQuorum());

        // HALog files now exist on ALL services, current commit counter
        final long currentCommitCounter = lastCommitCounter2 + 1;
        assertHALogDigestsEquals(currentCommitCounter, currentCommitCounter,
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
        
        // Now force further commit when fully met to remove log files
        simpleTransaction();

        // And again verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
        
        // Now verify no HALog files since fully met quorum @ commit.
        final long lastCommitCounter3 = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();
        assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter3,
                new HAGlue[] { serverA, serverB, serverC });

        }

    private void removeFiles(final File dir, final String openFile) {
		final File[] files = dir.listFiles();
		if (files != null)
			for (File file: files) {
				if (!file.getName().equals(openFile)) {
					log.warn("removing file " + file.getName());
					
					file.delete();
				}
			}
	}

    private void copyFiles(File src, File dst) throws IOException {
		final File[] files = src.listFiles();
		log.warn("Copying " + src.getAbsolutePath() + " to " + dst.getAbsolutePath() + ", files: " + files.length);
		if (files != null) {
			for (File srcFile: files) {
				final File dstFile = new File(dst, srcFile.getName());
				log.info("Copying " + srcFile.getAbsolutePath() + " to " + dstFile.getAbsolutePath());
				final FileInputStream instr = new FileInputStream(srcFile);
				final FileOutputStream outstr = new FileOutputStream(dstFile);
				
				final byte[] buf = new byte[8192];
				while (true) {
					final int len = instr.read(buf);
					if (len == -1)
						break;
					
					outstr.write(buf, 0, len);
				}
				
				outstr.close();
				instr.close();
			}
		}
	}

    /**
     * Test Rebuild of early starting C service where quorum was previously
     * fully met.  This forces a pipeline re-organisation
     * 
	 * @throws Exception 
     */
    public void testStartABC_RebuildWithPipelineReorganisation() throws Exception {

        new ABC(true/* sequential */);

        awaitFullyMetQuorum();
    	 	
        // Now run several transactions
        for (int i = 0; i < 5; i++)
            simpleTransaction();

        // shutdown AB and destroy C
        destroyC();
        shutdownA();
        shutdownB();

        /*
         * Now restart A, B & C.
         * 
         * Note: We start C first so it will be in the way when A or B attempts
         * to become the leader, thus forcing a pipeline reorganization.
         */
        final HAGlue serverC = startC();
        awaitPipeline(new HAGlue[] { serverC });

        // Now start A.
        final HAGlue serverA = startA();
        awaitPipeline(new HAGlue[] { serverC, serverA });
        
        // And finally start B.
        final HAGlue serverB = startB();

        // A & B should meet
        final long token2 = awaitMetQuorum();

        // The expected pipeline.  C was moved to the end.
        awaitPipeline(new HAGlue[] { serverA, serverB, serverC });

        // Wait until A is fully ready.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));
        
        // Verify A is the leader.
        assertEquals(serverA, quorum.getClient().getLeader(token2));
        
        // Check journals for equality on A, B.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });
        
        // Check HALogs equal on A, B.
        assertHALogDigestsEquals(7L/* firstCommitCounter */, 7L, new HAGlue[] {
                serverA, serverB });

        // C will have go through Rebuild before joining
        assertEquals(token2, awaitFullyMetQuorum());

//        Note: I have seen this timeout. This warrants exploring. BBT.
//        // Wait until C is fully ready.
//        assertEquals(HAStatusEnum.Follower, awaitNSSAndHAReady(serverC));

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Check HALogs equal on ALL services.
        assertHALogDigestsEquals(7L/* firstCommitCounter */, 7L, new HAGlue[] {
                serverA, serverB, serverC });

    }

    /**
     * Test Rebuild of C service where quorum was previously fully met and where
     * a new quorum is met before C joins for rebuild.
     * 
     * @throws Exception
     */
    public void testStartABC_Rebuild() throws Exception {

        {

            final HAGlue serverA = startA();
            final HAGlue serverB = startB();
            final HAGlue serverC = startC();

            awaitFullyMetQuorum();

            // Await initial commit point (KB create) on all servers.
            awaitCommitCounter(1L, serverA, serverB, serverC);
            
        }
        
        // Now run several transactions
        for (int i = 0; i < 5; i++)
            simpleTransaction();

        // shutdown AB and destroy C
        destroyC();
        shutdownA();
        shutdownB();

        // Now restart A, B & C

        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // A & B should meet
        awaitMetQuorum();

        final HAGlue serverC = startC();

        // C will have go through Rebuild before joining
        awaitFullyMetQuorum();

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
    }
    
    /**
     * Test Restore by:
     * <pre>starting ABC
     * 	drop C
     *  run transaction through AB
     *  drop A
     *  start C
     *  Restore
     *  Meet on BC
     *  start A
     *  Fully Meet</pre>
     * @throws Exception 
     */
    public void testABC_Restore() throws Exception {

//        final ABC startup = 
        new ABC(true/* sequential */);

//        // Wait for a quorum meet.
//        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
//                TimeUnit.MILLISECONDS);
//
//        // Verify KB exists        
//        awaitKBExists(startup.serverA);
//
    
        /*
         * Now go through a commit point with a met quorum. The HALog
         * files should be retained at that commit point.
         */
        simpleTransaction();
        
        // now shutdown C (not destroy)
        shutdownC();
        
        log.warn("CHECK OPEN LOGS ON A B");
        
        /*
         * Now go through a commit point with a met quorum. The HALog
         * files should be retained at that commit point.
         */
        if (true/*new commit state*/) {

            simpleTransaction();
            
            // and a second one?
            // getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();
                       
        }
        log.warn("CHECK LOGS ON A B");
        
        // now shutdown A (not destroy)
        shutdownA();
        
        // copy log files from A to C
        final File serviceDir = getTestDir();
        copyFiles(new File(serviceDir, "A/HALog"), new File(serviceDir, "C/HALog"));
                      
        startC();
        
        log.warn("CHECK LOGS HAVE BEEN COPIED TO C");
        
        // new C should Restore and Meet as Follower with B as leader
        this.awaitMetQuorum();

        startA();

        // A should join the Met Quorum which will then be Fully Met
        this.awaitFullyMetQuorum();

    }    

	/**
     * Test quorum remains if C is failed when fully met
     * @throws Exception 
     */
    public void testQuorumABC_failC() throws Exception {

        // Start 2 services.
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

//        // Verify KB exists.
//        awaitKBExists(serverA);
        
        /*
         * Note: The quorum was not fully met at the last 2-phase commit.
         * Instead, 2 services participated in the 2-phase commit and the third
         * service resynchronized when it came up and then went through a local
         * commit. Therefore, the HALog files should exist on all nodes.
         */

        // Current commit point.
        final long lastCommitCounter = 1;

        // Await initial commit point (KB create).
        awaitCommitCounter(lastCommitCounter, serverA, serverB);

        /*
         * Verify that HALog files were generated and are available for commit
         * point ONE (1) on the services joined with the met quorum.
         */
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                lastCommitCounter, new HAGlue[] { serverA, serverB });

        // Verify binary equality of (A,B) journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        // Start 3rd service.
        final HAGlue serverC = startC();

        // Wait until the quorum is fully met. The token should not change.
        assertEquals(token, awaitFullyMetQuorum());

        // The commit counter has not changed.
        assertEquals(
                lastCommitCounter,
                serverA.getRootBlock(
                        new HARootBlockRequest(null/* storeUUID */))
                        .getRootBlock().getCommitCounter());

        // HALog files now exist on ALL services.
        assertHALogDigestsEquals(1L/* firstCommitCounter */, lastCommitCounter,
                new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

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
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Verify no HALog files since fully met quorum @ commit.
        assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter,
                new HAGlue[] { serverA, serverB, serverC });
        
        // Now fail C - use destroy otherwise test does not tear down cleanly
        //	leaving journal and log files
        destroyC();   
        
        // assert quorum remains met       
        assertTrue(quorum.isQuorumMet());
        // but not FULLY
        assertFalse(quorum.isQuorumFullyMet(token));

    }

    /**
     * Test quorum breaks and reforms when original leader fails
     * 
     * @throws Exception 
     */
	public void testQuorumBreaksABC_failLeader() throws Exception {

		// Start 3 services in sequence
		final ABC startup = new ABC(true/*sequential*/);

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
                || leader.equals(startup.serverC));
        
	}
    
    /**
     * Tests that halog files are generated and identical, and that
     * when a server is shutdown its logs remain
     * @throws Exception 
     */
    public void testStartAB_halog() throws Exception {

        // Start 2 services
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Await first commit point on both services (KB create).
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB });
        
        // Run through transaction
        simpleTransaction();
        
        // close both services
        shutdownA();
        shutdownB();
        
        // check that logfiles exist
        final File logsA = getHALogDirA();
        final File logsB = getHALogDirB();
        
        assertEquals(logsA.listFiles().length, 2);
        assertEquals(logsB.listFiles().length, 2);
        
    }
    
    /**
     * Tests that halog files are generated and identical and then
     * removed following resync and fullymet quorum
     * 
     * @throws Exception 
     */
    public void testStartAB_C_halog() throws Exception {

        doStartAB_C_halog();
        
	}
	
	private void doStartAB_C_halog() throws Exception {
        
	    // Start 2 services
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Await first commit point on both services (KB create).
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB });
        
        // Run through transaction
        simpleTransaction();
        
        // check that logfiles exist
        final File logsA = getHALogDirA();
        final File logsB = getHALogDirB();
        
        // There should be 3 halog files from 2 commit points and newly open
        assertEquals(logsA.listFiles().length, 3);
        assertEquals(logsB.listFiles().length, 3);
        
        // now just restart to help teardown
        startC();
        
        awaitFullyMetQuorum();
        
        // Run through another transaction
        simpleTransaction();

        // all committed logs should be removed with only open log remaining
        final File logsC = getHALogDirC();
        assertEquals(logsA.listFiles().length, 1);
        assertEquals(logsB.listFiles().length, 1);
        assertEquals(logsC.listFiles().length, 1);
	}
    
    /**
	 * Sandbox stress test, must be disabled before commit for CI runs
	 * @throws Exception
	 */
	public void _testSANDBOXStressStartAB_C_halog() throws Exception {
		for (int i = 0; i < 20; i++) {
			try {
				doStartAB_C_halog();
			} catch (Exception e) {
				throw new Exception("Failed on run: " + i, e);
			} finally {
				destroyAll();
			}
		}
	}

    /**
     * Tests that halog files are generated and removed after each commit
     * once fully met.
     * 
     * Note that committed log files are not purged on FullyMet, only after first
     * FullyMet commit.
     * 
     * @throws Exception 
     */
    public void testStartABC_halog() throws Exception {
        
        // Start 3 services
        startA();
        startB();
        startC();
        
        awaitFullyMetQuorum();
        
        // setup log directories
        final File logsA = getHALogDirA();
        final File logsB = getHALogDirB();
        final File logsC = getHALogDirC();
        
        // committed log files not purged prior to fully met commit
        assertLogCount(logsA, 2);
        assertLogCount(logsB, 2);
        assertLogCount(logsC, 2);

        // Run through transaction
        simpleTransaction();
        
        // again check that only open log files remaining
        assertLogCount(logsA, 1);
        assertLogCount(logsB, 1);
        assertLogCount(logsC, 1);

        // Now run several transactions
        for (int i = 0; i < 5; i++)
        	simpleTransaction();
        
        // again check that only open log files remaining
        assertLogCount(logsA, 1);
        assertLogCount(logsB, 1);
        assertLogCount(logsC, 1);
    }
    
    /**
     * Tests that new logs are established on restarting with
     * transitions through Met and FullyMet
     * 
     * @throws Exception
     */
    public void testStartABC_halogRestart() throws Exception {
        // Start 3 services, with delay to ensure clean starts
        startA();
        Thread.sleep(1000); // ensure A will be leader
        startB();
        startC();
        
        awaitFullyMetQuorum();
        
        // setup log directories
        final File logsA = getHALogDirA();
        final File logsB = getHALogDirB();
        final File logsC = getHALogDirC();
        
        // committed log files not purged prior to fully met commit
        assertLogCount(logsA, 2);
        assertLogCount(logsB, 2);
        assertLogCount(logsC, 2);

        // Run through transaction
        simpleTransaction();
        
        // again check that only open log files remaining
        assertLogCount(logsA, 1);
        assertLogCount(logsB, 1);
        assertLogCount(logsC, 1);
        
        // Now shutdown all servers
        shutdownB();
        shutdownC();
        shutdownA();
        
        // and check that there are no logs
        assertLogCount(logsA, 0);
        assertLogCount(logsB, 0);
        assertLogCount(logsC, 0);
        
        // startup AB
        startA();
        startB();
        
        awaitMetQuorum();
        
        // and check that there are open logs
        assertLogCount(logsA, 1);
        assertLogCount(logsB, 1);
        
        // add C
        startC();
        
        awaitFullyMetQuorum();
        
        // and check again for ABC
        assertLogCount(logsA, 1);
        assertLogCount(logsB, 1);
        assertLogCount(logsC, 1);
    }
    
    /**
     * Variant where A is shutdown first.
     */
    public void testStartABC_halogRestart2() throws Exception {
        // Start 3 services, with delay to ensure clean starts
        startA();
        Thread.sleep(1000); // ensure A will be leader
        startB();
        startC();
        
        awaitFullyMetQuorum();
        
        // setup log directories
        final File logsA = getHALogDirA();
        final File logsB = getHALogDirB();
        final File logsC = getHALogDirC();
        
        // committed log files not purged prior to fully met commit
        assertLogCount(logsA, 2);
        assertLogCount(logsB, 2);
        assertLogCount(logsC, 2);

        // Run through transaction
        simpleTransaction();
        
        // again check that only open log files remaining
        assertLogCount(logsA, 1);
        assertLogCount(logsB, 1);
        assertLogCount(logsC, 1);
        
        // Now shutdown all servers
        shutdownA();
        shutdownB();
        shutdownC();
        
        // and check that there are no logs
        assertLogCount(logsA, 0);
        assertLogCount(logsB, 0);
        assertLogCount(logsC, 0);
        
        // startup AB
        startA();
        startB();
        
        awaitMetQuorum();
        
        // and check that there are open logs
        assertLogCount(logsA, 1);
        assertLogCount(logsB, 1);
        
        // add C
        startC();
        
        awaitFullyMetQuorum();
        
        // and check again for ABC
        assertLogCount(logsA, 1);
        assertLogCount(logsB, 1);
        assertLogCount(logsC, 1);
    }

    private void assertLogCount(final File logdir, final int count) {
        final int actual = logdir.listFiles().length;
        if (actual != count) {
            fail("Actual log files: " + actual + ", expected: " + count);
        }
    }
    
    /**
     * Tests that halog files are removed after fully met on rebuild
     * face
     * @throws Exception 
     */
    public void testStartABC_rebuild_halog() throws Exception {
        // setup log directories
        final File logsA = getHALogDirA();
        final File logsB = getHALogDirB();
        final File logsC = getHALogDirC();
        
        // Start 3 services
        startA();
        startB();
        startC();
        
        awaitFullyMetQuorum();
        
        // Now run several transactions
        for (int i = 0; i < 5; i++)
        	simpleTransaction();
        
        // Now destroy service C
        destroyC();
        
        // Now run several more transactions on AB
        for (int i = 0; i < 5; i++)
        	simpleTransaction();

        // 5 committed files + 1 open == 6
        assertLogCount(logsA, 6);
        assertLogCount(logsB, 6);

        // and restart C with empty journal, forces Rebuild
        startC();
        
        awaitFullyMetQuorum();
        
        assertLogCount(logsA, 6);
        assertLogCount(logsB, 6);        
        // Log count on C is 1 after quiescent rebuild
        assertLogCount(logsC, 1);
        
        log.warn("CHECK: Committed log files not copied on Rebuild");
        
        // Now run several transactions
        for (int i = 0; i < 5; i++)
        	simpleTransaction();

        // and check that only open log files remaining
        assertLogCount(logsA, 1);
        assertLogCount(logsB, 1);
        assertLogCount(logsC, 1);
        
    }
    
    
    /**
     * Tests shutdown of met quorum, but leader first forces re-organisation concurrent 
     * with service shutdown
     * 
     * @throws Exception
     */
    public void test_fullQuorumRestartWithForcedReorganisation() throws Exception {
        // Start 3 services
        startA();
        startB();
        startC();
        
        awaitFullyMetQuorum();
        
        // Run through transaction
        simpleTransaction();
        
        // Now shutdown all servers, leader first, then others
        shutdownLeader();
        shutdownA();
        shutdownB();
        shutdownC();
        
        // startup AB
        startA();
        startB(); // need to see log of shutdown problem
        
        awaitMetQuorum();
        
        // add C
        startC();
        
        awaitFullyMetQuorum();
    }

    /**
     * Tests shutdown of met quorum, but shutting down leader last ensures no
     *  re-organisation concurrent with service shutdown
     * 
     * @throws Exception
     */
    public void test_fullQuorumRestartWithNoReorganisation() throws Exception {
        // Start 3 services
        startA();
        Thread.sleep(1000); // ensure A will be leader
        
        startB();
        startC();
        
        awaitFullyMetQuorum();
        
        // Run through transaction
        simpleTransaction();
        
        // Now shutdown all servers, leader last ensuring no reorganisation
        shutdownB();
        shutdownC();
        shutdownA();
       
        // startup AB
        startA();
        startB();
        
        awaitMetQuorum();
        
        assertEquals(quorum.getJoined().length, 2);
        assertEquals(quorum.getPipeline().length, 2);
        assertEquals(quorum.getMembers().length, 2);
        
        // add C
        startC();
        
        awaitFullyMetQuorum();
    }
    
    /**
     * We have experienced inconsistencies on test startups, this test just attempts
     * to repeatedly start an initial ABC service.
     * @throws Exception
     */
    // disable from standard test runs
    public void _testSANDBOXStressABC_Restart() throws Exception {
        for (int i = 1; i <= 20; i++) {
            try {
                new ABC(true/* sequential */);

                awaitFullyMetQuorum();

                simpleTransaction();
            } catch (Throwable e) {
                fail("Unable to meet on run " + i, e);
            }

            // destroy attempts to take services down in simplest order, leaving
            // leader until last to try to avoid reorganisation events
            destroyAll();
        }
    }

    /**
     * We have experienced inconsistencies on test startups, this test just
     * attempts to repeatedly start an initial ABC service.
     */
    // disable from std test runs
    public void _testSANDBOXStressABCStartSimultaneous() throws Exception {
        for (int i = 1; i <= 20; i++) {
            ABC tmp = null;
            try {
                tmp = new ABC(false/* sequential */);
                awaitFullyMetQuorum();
                tmp.shutdownAll();
            } catch (Throwable e) {
                fail("Unable to meet on run " + i, e);
            } finally {
                if (tmp != null) {
                    tmp.shutdownAll();
                }
            }
        }
    }

    /**
     * This sequence of transitions ensures that a quorum, once redundantly met
     * does not break when any follower leaves
     * 
     * @throws Exception
     */
    public void testABC_RemainsMet() throws Exception {
		// enforce join order
		final ABC startup = new ABC(true/*sequential*/);

		final long token = awaitFullyMetQuorum();
		
		// shutdown C, the final follower
		shutdownC();
		awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB});

		// token must remain unchanged to indicate same quorum
		assertEquals(token, awaitMetQuorum());
		
		final HAGlue serverC2 = startC();
		// return to quorum
		awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB, serverC2});
		
		// token remains unchanged		
		assertEquals(token, awaitFullyMetQuorum());
		
		// Now remove first follower
		shutdownB();
		awaitPipeline(new HAGlue[] {startup.serverA, serverC2});
		
		// token must remain unchanged to indicate same quorum
		assertEquals(token, awaitMetQuorum());
		
		final HAGlue serverB2 = startB();
		awaitPipeline(new HAGlue[] {startup.serverA, serverC2, serverB2});
		// and return to quorum
		assertEquals(token, awaitFullyMetQuorum());
     
    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, fail C (the last
     * follower). Verify that the LOAD completes successfully with the remaining
     * services (A+B).
     */
    public void testABC_LiveLoadRemainsMet_fail_C() throws Exception {

        // enforce join order
		final ABC startup = new ABC(true /*sequential*/);
		
		final long token = awaitFullyMetQuorum();
		
        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(300/* ms */);

		// Verify load is still running.
		assertFalse(ft.isDone());
		
		// shutdown C, the final follower
		shutdownC();
		awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB});

		// token must remain unchanged to indicate same quorum
		assertEquals(token, awaitMetQuorum());
		
		// Wait for the Future of the LOAD.
		ft.get();

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, shutdown and then restart
     * C (the last follower). Verify that the quorum fully meets once the LOAD
     * is complete (this test variant allows C to resync and join if it could
     * not transition to met during the LOAD).
     * 
     * @throws Exception
     */
    public void testABC_LiveLoadRemainsMet_restart_C_fullyMetAfterLOAD() throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(300/* ms */);

        // Verify load is still running.
        assertFalse(ft.isDone());
        
        // shutdown C, the final follower
        shutdownC();
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB});

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // restart C.
		final HAGlue serverC2 = startC();

		// C appears at the end of the pipeline.
		awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB, serverC2});
		
        // wait for the Future.
        ft.get();
        
        // Verify quorum becomes fully met now that LOAD is done.
        assertEquals(token, awaitFullyMetQuorum());

    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, shutdown and then restart
     * C (the last follower). Verify that the LOAD completes successfully and
     * that quorum is fully met BEFORE the LOAD is complete (that is, C is able
     * to resync and join during the LOAD).
     * 
     * @throws Exception
     */
    public void testABC_LiveLoadRemainsMet_restart_C_fullyMetDuringLOAD()
            throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(300/* ms */);

        // Verify load is still running.
        assertFalse(ft.isDone());
        
        // shutdown C, the final follower
        shutdownC();
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB});

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // restart C.
        final HAGlue serverC2 = startC();

        // C comes back at the end of the pipeline.
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB, serverC2});
        
        // Await fully met quorum *before* LOAD is done.
        assertTrue(awaitFullyMetDuringLOAD(token, ft));

        // Verify fully met.
        assertTrue(quorum.isQuorumFullyMet(token));

        // Wait for the LOAD to complete and check for errors.
        ft.get();
        
    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, shutdown B (the 1st
     * follower). Verify that the LOAD completes successfully.
     * <p>
     * Note: This test forces a pipeline reorganization.
     */
    public void testABC_LiveLoadRemainsMet_fail_B() throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(300/* ms */);

        // Verify load is still running.
        assertFalse(ft.isDone());
        
        // shutdown B, the 1st follower. This forces a pipeline reorg.
        shutdownB();
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverC});

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());
        
        // Wait for the Future of the LOAD.
        ft.get();

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());
        
    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, shutdown and then restart
     * B (the 1st follower). Verify that the quorum fully meets once the LOAD is
     * complete (this test variant allows B to resync and join if it could not
     * transition to met during the LOAD). 
     * <p>
     * Note: This test forces a pipeline reorganization.
     * 
     * @throws Exception
     */
    public void testABC_LiveLoadRemainsMet_restart_B_fullyMetAfterLOAD() throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(300/* ms */);

        // Verify load is still running.
        assertFalse(ft.isDone());
        
        // shutdown B, the 1st follower. This forces a pipeline reorg.
        shutdownB();
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverC});

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // restart B.
        final HAGlue serverB2 = startB();

        // C appears at the end of the pipeline.
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverC, serverB2});
        
        // wait for the Future.
        ft.get();
        
        // Verify quorum becomes fully met now that LOAD is done.
        assertEquals(token, awaitFullyMetQuorum());

    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, shutdown and then restart
     * B (the 1st follower). Verify that that quorum is fully met BEFORE the
     * LOAD is complete (that is, B is able to resync and join during the LOAD).
     * Verify that the LOAD completes successfully.
     * <p>
     * Note: This test forces a pipeline reorganization.
     * 
     * @throws Exception
     */
    public void testABC_LiveLoadRemainsMet_restart_B_fullyMetDuringLOAD()
            throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(300/* ms */);

        // Verify load is still running.
        assertFalse(ft.isDone());
        
        // shutdown B, the 1st follower. This forces a pipeline reorg.
        shutdownB();
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverC});

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // restart B.
        final HAGlue serverB2 = startB();

        // C comes back at the end of the pipeline.
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB, serverB2});
        
        // Await fully met quorum *before* LOAD is done.
        assertTrue(awaitFullyMetDuringLOAD(token, ft));

        // Verify fully met.
        assertTrue(quorum.isQuorumFullyMet(token));

        // Wait for the LOAD to complete and check for errors.
        ft.get();
        
    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, shutdown and then restart
     * C (the last follower). Verify that that quorum is fully met BEFORE the
     * LOAD is complete (that is, C is able to resync and join during the LOAD).
     * Then shutdown B (the first follower, so this forces a pipeline
     * reorganization). Verify that that quorum is fully met BEFORE the LOAD is
     * complete (that is, C is able to resync and join during the LOAD). Verify
     * that the LOAD completes successfully.
     */
    public void testABC_LiveLoadRemainsMet_restart_C_fullyMetDuringLOAD_restart_B_fullyMetDuringLOAD()
            throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(300/* ms */);

        // Verify load is still running.
        assertFalse(ft.isDone());
        
        // shutdown C, the final follower
        shutdownC();
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB});

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // restart C.
        final HAGlue serverC2 = startC();

        // C comes back at the end of the pipeline.
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB, serverC2});
        
        // wait for the quorum to fully meet during the LOAD.
        assertTrue(awaitFullyMetDuringLOAD(token, ft));

        // Double checked assertion. Should always be true per loop above.
        assertTrue(quorum.isQuorumFullyMet(token));

        /*
         * Now we will remove the first follower.
         */

        // Should already be true.
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverB, serverC2 });

        // Now remove first follower
        shutdownB();
        awaitPipeline(new HAGlue[] { startup.serverA, serverC2 });

        // token still valid.
        quorum.assertQuorum(token);

        // restart B.
        final HAGlue serverB2 = startB();

        // should return to the end of the pipeline.
        awaitPipeline(new HAGlue[] { startup.serverA, serverC2, serverB2 });
        
        // Await fully met quorum *before* LOAD is done.
        assertTrue(awaitFullyMetDuringLOAD(token, ft));

        // Verify fully met.
        assertTrue(quorum.isQuorumFullyMet(token));

    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, shutdown and then restart
     * B (the 1st follower, so this forces a pipeline reorganization). Verify
     * that that quorum is fully met BEFORE the LOAD is complete (that is, B is
     * able to resync and join during the LOAD). Then shutdown C (the new 1st
     * follower, so this forces another pipeline reorganization). Verify that
     * that quorum is fully met BEFORE the LOAD is complete (that is, C is able
     * to resync and join during the LOAD). Verify that the LOAD completes
     * successfully.
     * 
     * @throws Exception
     */
    public void testABC_LiveLoadRemainsMet_restart_B_fullyMetDuringLOAD_restartC_fullyMetDuringLOAD()
            throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token, true/* reallyLargeLoad */));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(300/* ms */);

        // Verify load is still running.
        assertFalse(ft.isDone());
        
        /*
         * Restart the first follower.
         */

        // Should already be true.
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverB, startup.serverC });

        // Now remove first follower
        shutdownB();
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC });

        // token still valid.
        quorum.assertQuorum(token);

        // restart B.
        final HAGlue serverB2 = startB();

        // should return to the end of the pipeline.
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC, serverB2 });
        
        // Await fully met quorum *before* LOAD is done.
        assertTrue(awaitFullyMetDuringLOAD(token, ft));

        // Verify fully met.
        assertTrue(quorum.isQuorumFullyMet(token));

        /*
         * Restart the first follower.
         * 
         * Note: This time C is the first follower.
         */
        
        // Should already be true.
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC, serverB2 });

        // Now remove first follower
        shutdownC();
        awaitPipeline(new HAGlue[] { startup.serverA, serverB2 });

        // token still valid.
        quorum.assertQuorum(token);

        // restart C.
        final HAGlue serverC2 = startC();

        // should return to the end of the pipeline.
        awaitPipeline(new HAGlue[] { startup.serverA, serverB2, serverC2 });

        // Await fully met quorum *before* LOAD is done.
        assertTrue(awaitFullyMetDuringLOAD(token, ft));

        // Verify fully met.
        assertTrue(quorum.isQuorumFullyMet(token));

    }

}
