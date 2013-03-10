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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * Test suites for an {@link HAJournalServer} quorum with a replication factor
 * of THREE (3) and a fully met {@link Quorum}.
 * 
 * FIXME (***) We need unit tests where we fail (shutdown) the service in at the head
 * of the pipeline (the leader), the 2nd position, and the last position. We
 * need to make sure that the quorum remains (when failing a follower) and that
 * it re-meets (when failing a leader). We also need to extend these tests to
 * verify that the service that was failed can be restarted. And we need to have
 * versions of these tests that bounce the service rather than shutting it down
 * and then restarting it.
 * 
 * FIXME (***) We need unit tests that focus on resynchronization (writes were missed
 * during a service shutdown or bounce and hence the service must synchronize
 * when it comes back up).
 * 
 * FIXME We need unit tests that focus on rebuild (disaster recovery).
 * 
 * TODO We can do an explicit service restart or we can tell a service to bounce
 * its zk connection. Both will force a service leave and pipeline leave. The
 * shutdown/restart tests service restart while the zk bounce tests the impact
 * of a GC causing a timeout or a cured network partition.
 * 
 * TODO Tests to make sure that bouncing services rejoin and resync.
 * 
 * FIXME ---- HA3 RESYNC TESTS ----
 * 
 * TODO We need to verify that the HALog files are generated, that they are
 * identical, that they are retained while the quorum is not fully met, and that
 * they are removed when we go through a 2-phase commit with a fully met quorum.
 * 
 * TODO 2 services start in known order and the quourm meets. Write one or more
 * write sets on the leader and commit for each write set (the KB create is one
 * write set, a LOAD would be a second write set giving us 2 commit points).
 * Start 3rd service. The third service should resynchronize each historical
 * commit point from the HALog files on the leader and then join the met quorum.
 * Note: For this test, the leader is not receiving writes during the
 * resynchronization protocol.
 * 
 * Once the quorum is fully met, verify that all journals are 100% equals on the
 * disk. Note: The HALog files will not be purged since we only do that at a
 * fully met quorum commit, and while the 3rd service joined the quourm the
 * fully met quorum has not yet gone through a commit. (It is possible to delete
 * the logs when the quorum becomes fully met, rather than only when we go
 * through a fully met commit. The current behavior is more paranoid.)
 * 
 * TODO 2 services start in known order. The quorum meets. Begin a long running
 * LOAD on the leader. While that LOAD is running, start the 3rd service. The
 * service should resynchronize the historical commit points and "catch up" to
 * the live writes on the current write set and then join the quourm. The quorum
 * should be fully met when the LOAD is done and the HALog files should be
 * purged.
 * 
 * Note: In order to guarantee that the LOAD operation does not terminate before
 * we are resynchronized, we either need to LOAD a sufficiently large file or we
 * need to stream data to the leader. I think we should go with a large file.
 * 
 * TODO Verify that resync is chosen over rebuild when the HA Logs have been
 * purged at some point in the past but all necessary HALog files remain for the
 * resync operation (possible fence post - I have observed a rebuild operation
 * initiated when a resync was possible).
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

        // Verify KB exists.
        awaitKBExists(serverA);
        
        /*
         * Note: The quorum was not fully met at the last 2-phase commit.
         * Instead, 2 services participated in the 2-phase commit and the third
         * service resynchronized when it came up and then went through a local
         * commit. Therefore, the HALog files should exist on all nodes.
         */

        // Current commit point.
        final long lastCommitCounter = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There is ONE commit point.
        assertEquals(1L, lastCommitCounter);

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
        {

            final StringBuilder sb = new StringBuilder();
            sb.append("DROP ALL;\n");
            sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
            sb.append("INSERT DATA {\n");
            sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
            sb.append("  dc:creator \"A.N.Other\" .\n");
            sb.append("}\n");
            
            final String updateStr = sb.toString();
            
            final HAGlue leader = quorum.getClient().getLeader(token);
            
            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();
            
        }

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

        final ABC abc = new ABC(); // simultaneous start.

        final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

        // Verify quorum is FULLY met.
        final long token = awaitFullyMetQuorum();

        // Verify KB exists on leader.
        final HAGlue leader = quorum.getClient().getLeader(token);

        awaitKBExists(leader);
        
        // Current commit point.
        final long lastCommitCounter = leader
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There is ONE commit point.
        assertEquals(1L, lastCommitCounter);

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

        // wait for KB create.
        awaitKBExists(serverA);
        
        // start C.
        final HAGlue serverC = startC();

        // wait for a fully met quorum.
        assertEquals(token, awaitFullyMetQuorum());

        // Current commit point.
        final long lastCommitCounter = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There is ONE commit point.
        assertEquals(1L, lastCommitCounter);

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
        {

            final StringBuilder sb = new StringBuilder();
            sb.append("DROP ALL;\n");
            sb.append("LOAD <" + getFoafFileUrl("data-0.nq.gz") + ">;\n");
            sb.append("LOAD <" + getFoafFileUrl("data-1.nq.gz") + ">;\n");
            sb.append("LOAD <" + getFoafFileUrl("data-2.nq.gz") + ">;\n");
            sb.append("LOAD <" + getFoafFileUrl("data-3.nq.gz") + ">;\n");
            sb.append("INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y };\n");
            sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
            sb.append("INSERT DATA {\n");
            sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
            sb.append("  dc:creator \"A.N.Other\" .\n");
            sb.append("}\n");
            
            final String updateStr = sb.toString();
            
            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            repos[0].prepareUpdate(updateStr).evaluate();
            
        }

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

        // Verify KB exists.
        awaitKBExists(serverA);
        
        /*
         * Note: The quorum was not fully met at the last 2-phase commit.
         * Instead, 2 services participated in the 2-phase commit and the third
         * service resynchronized when it came up and then went through a local
         * commit. Therefore, the HALog files should exist on all nodes.
         */

        // Current commit point.
        final long lastCommitCounter = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There is ONE commit point.
        assertEquals(1L, lastCommitCounter);

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
        {

            final StringBuilder sb = new StringBuilder();
            sb.append("DROP ALL;\n");
            sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
            sb.append("INSERT DATA {\n");
            sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
            sb.append("  dc:creator \"A.N.Other\" .\n");
            sb.append("}\n");
            
            final String updateStr = sb.toString();
            
            final HAGlue leader = quorum.getClient().getLeader(token);
            
            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();
            
        }

        // Current commit point.
        final long lastCommitCounter2 = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There are TWO (2) commit points.
        assertEquals(2L, lastCommitCounter2);

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
        {

            final StringBuilder sb = new StringBuilder();
            sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
            sb.append("INSERT DATA {\n");
            sb.append("  <http://example/book2> dc:title \"Another book\" ;\n");
            sb.append("  dc:creator \"A.N.Other\" .\n");
            sb.append("}\n");
            
            final String updateStr = sb.toString();
            
            final HAGlue leader = quorum.getClient().getLeader(token);
            
            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();
            
        }

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
     * Similar to standard resync except that the resync occurs while a sequence of live transactions are in progress.
     * @throws Exception 
     */
	public void testStartAB_C_MultiTransactionResync() throws Exception {

		fail("TEST FAILS");
		
		// Start 2 services.
		final HAGlue serverA = startA();
		final HAGlue serverB = startB();

		// Wait for a quorum meet.
		final long token = quorum.awaitQuorum(awaitQuorumTimeout,
				TimeUnit.MILLISECONDS);

		// Verify KB exists.
		awaitKBExists(serverA);

		/*
		 * Note: The quorum was not fully met at the last 2-phase commit.
		 * Instead, 2 services participated in the 2-phase commit and the third
		 * service resynchronized when it came up and then went through a local
		 * commit. Therefore, the HALog files should exist on all nodes.
		 */

		// Current commit point.
		final long lastCommitCounter = serverA
				.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
				.getRootBlock().getCommitCounter();

		// There is ONE commit point.
		assertEquals(1L, lastCommitCounter);

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
		{

			final StringBuilder sb = new StringBuilder();
			sb.append("DROP ALL;\n");
			sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
			sb.append("INSERT DATA {\n");
			sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
			sb.append("  dc:creator \"A.N.Other\" .\n");
			sb.append("}\n");

			final String updateStr = sb.toString();

			final HAGlue leader = quorum.getClient().getLeader(token);

			// Verify quorum is still valid.
			quorum.assertQuorum(token);

			getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();

		}

		// Current commit point.
		final long lastCommitCounter2 = serverA
				.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
				.getRootBlock().getCommitCounter();

		// There are TWO (2) commit points.
		assertEquals(2L, lastCommitCounter2);

		// start concurrent task loads that continue until fully met
		final AtomicBoolean spin = new AtomicBoolean(false);
		final Thread loadThread = new Thread() {
			public void run() {
				int count = 0;
				try {
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

						try {
							getRemoteRepository(leader)
									.prepareUpdate(updateStr).evaluate();

							Thread.sleep(1500);
						} catch (Exception e) {
							fail("Probably unexpected on run " + count, e);
						}
					}
				} finally {
					spin.set(true);
				}
			}
		};
		loadThread.start();

		// Now Start 3rd service.
		final HAGlue serverC = startC();

		// Wait until the quorum is fully met. After RESYNC
		assertEquals(token, awaitFullyMetQuorum(20));

		log.info("FULLY MET");
		
		while (!spin.get()) {
			Thread.sleep(50);
		}

		log.info("Should be safe to test digests now");
		
		// Cannot predict last commit counter or whether even logs will remain
		// assertHALogDigestsEquals(1L/* firstCommitCounter */,
		// lastCommitCounter2,
		// new HAGlue[] { serverA, serverB, serverC });

		// Verify binary equality of ALL journals.
		assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

		// Now force further commit when fully met to remove log files
		{

			final StringBuilder sb = new StringBuilder();
			sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
			sb.append("INSERT DATA {\n");
			sb.append("  <http://example/bookFinal> dc:title \"Another book\" ;\n");
			sb.append("  dc:creator \"A.N.Other\" .\n");
			sb.append("}\n");

			final String updateStr = sb.toString();

			final HAGlue leader = quorum.getClient().getLeader(token);

			// Verify quorum is still valid.
			quorum.assertQuorum(token);

			getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();

		}

		// And again verify binary equality of ALL journals.
		// assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

		// Now verify no HALog files since fully met quorum @ commit.
		final long lastCommitCounter3 = serverA
				.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
				.getRootBlock().getCommitCounter();
		assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter3,
				new HAGlue[] { serverA, serverB, serverC });

		log.info("ALL GOOD!");
	}
    
	   /**
     * Similar to standard resync except that the resync occurs while a single long transaction is in progress.
     * @throws Exception 
     */
	public void testStartAB_C_LiveResync() throws Exception {

		fail("TEST FAILS");
		
		// Start 2 services.
		final HAGlue serverA = startA();
		final HAGlue serverB = startB();

		// Wait for a quorum meet.
		final long token = quorum.awaitQuorum(awaitQuorumTimeout,
				TimeUnit.MILLISECONDS);

		// Verify KB exists.
		awaitKBExists(serverA);

		/*
		 * Note: The quorum was not fully met at the last 2-phase commit.
		 * Instead, 2 services participated in the 2-phase commit and the third
		 * service resynchronized when it came up and then went through a local
		 * commit. Therefore, the HALog files should exist on all nodes.
		 */

		// Current commit point.
		final long lastCommitCounter = serverA
				.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
				.getRootBlock().getCommitCounter();

		// There is ONE commit point.
		assertEquals(1L, lastCommitCounter);

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
		{

			final StringBuilder sb = new StringBuilder();
			sb.append("DROP ALL;\n");
			sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
			sb.append("INSERT DATA {\n");
			sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
			sb.append("  dc:creator \"A.N.Other\" .\n");
			sb.append("}\n");
			
			final String updateStr = sb.toString();

			final HAGlue leader = quorum.getClient().getLeader(token);

			// Verify quorum is still valid.
			quorum.assertQuorum(token);

			getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();

		}

		// Current commit point.
		final long lastCommitCounter2 = serverA
				.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
				.getRootBlock().getCommitCounter();

		// There are TWO (2) commit points.
		assertEquals(2L, lastCommitCounter2);

		// start concurrent task loads that continue until fully met
		final AtomicBoolean spin = new AtomicBoolean(false);
		final Thread loadThread = new Thread() {
			public void run() {
					final StringBuilder sb = new StringBuilder();
					sb.append("DROP ALL;\n");
					sb.append("LOAD <file:/bigdata/ha/data-0.nq>;\n");
					sb.append("LOAD <file:/bigdata/ha/data-1.nq>;\n");
					sb.append("LOAD <file:/bigdata/ha/data-2.nq>;\n");
					sb.append("INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y };\n");
					sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
					sb.append("INSERT DATA\n");
					sb.append("{\n");
					sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
					sb.append("    dc:creator \"A.N.Other\" .\n");
					sb.append("}\n");

					final String updateStr = sb.toString();

					final HAGlue leader = quorum.getClient().getLeader(token);

					// Verify quorum is still valid.
					quorum.assertQuorum(token);

					try {
						getRemoteRepository(leader).prepareUpdate(updateStr)
								.evaluate();
						log.info("Updated");
					} catch (Exception e) {
						e.printStackTrace();
						
						fail("Probably unexpected on run ", e);
					} finally {				
						spin.set(true);
					}
			}
		};
		loadThread.start();

		// Now Start 3rd service.
		final HAGlue serverC = startC();

		// Wait until the quorum is fully met. After RESYNC
		assertEquals(token, awaitFullyMetQuorum(20));

		log.info("FULLY MET");
		
		// Need to check if load is active, if not then test has not confirmed active load
		assertFalse(spin.get());
		
		while (!spin.get()) {
			Thread.sleep(50);
		}

		log.info("Should be safe to test digests now");
		
		// Cannot predict last commit counter or whether even logs will remain
		// assertHALogDigestsEquals(1L/* firstCommitCounter */,
		// lastCommitCounter2,
		// new HAGlue[] { serverA, serverB, serverC });

		// Verify binary equality of ALL journals.
		assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

		// Now force further commit when fully met to remove log files
		{

			final StringBuilder sb = new StringBuilder();
			sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
			sb.append("INSERT DATA {\n");
			sb.append("  <http://example/bookFinal> dc:title \"Another book\" ;\n");
			sb.append("  dc:creator \"A.N.Other\" .\n");
			sb.append("}\n");

			final String updateStr = sb.toString();

			final HAGlue leader = quorum.getClient().getLeader(token);

			// Verify quorum is still valid.
			quorum.assertQuorum(token);

			getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();

		}

		// And again verify binary equality of ALL journals.
		// assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

		// Now verify no HALog files since fully met quorum @ commit.
		final long lastCommitCounter3 = serverA
				.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
				.getRootBlock().getCommitCounter();
		assertHALogNotFound(0L/* firstCommitCounter */, lastCommitCounter3,
				new HAGlue[] { serverA, serverB, serverC });

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

        // Verify KB exists.
        awaitKBExists(serverA);
        
        /*
         * Note: The quorum was not fully met at the last 2-phase commit.
         * Instead, 2 services participated in the 2-phase commit and the third
         * service resynchronized when it came up and then went through a local
         * commit. Therefore, the HALog files should exist on all nodes.
         */

        // Current commit point.
        final long lastCommitCounter = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There is ONE commit point.
        assertEquals(1L, lastCommitCounter);

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
    	startA();
    	startB();
    	startC();
    	
    	awaitFullyMetQuorum();
    	 	
        // Now run several transactions
        for (int i = 0; i < 5; i++)
        	simpleTransaction();
        
        // shutdown AB and destroy C
        destroyC();
        shutdownA();
        shutdownB();
        
        // Now restart A, B & C
    	final HAGlue serverC = startC();
    	// Ensure that C starts first
    	Thread.sleep(100);
    	
    	final HAGlue serverA = startA();
    	final HAGlue serverB = startB();
    	
    	// A & B should meet
    	awaitMetQuorum();
    	// Check HALogs equal
//        assertHALogDigestsEquals(7L/* firstCommitCounter */,
//                7L, new HAGlue[] { serverA, serverB });
    	log.warn("CHECK AB LOGS ON MET QUORUM");       
    	
    	// C will have  go through Rebuild before joining
     	awaitFullyMetQuorum();
        
        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
    }
    
    /**
     * Test Rebuild of C service where quorum was previously
     * fully met and where a new quorum is met before C joins for rebuild.
     * 
	 * @throws Exception 
     */
    public void testStartABC_Rebuild() throws Exception {
    	startA();
    	startB();
    	startC();
    	
    	awaitFullyMetQuorum();
    	 	
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
        // Start 3 services, sleeps to ensure sequence.
        final HAGlue serverA = startA();
        //Thread.sleep(100);
        final HAGlue serverB = startB();
        //Thread.sleep(100);
    	final HAGlue serverC = startC();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Verify KB exists.es miserables oscar 2013
        
        awaitKBExists(serverA);

    
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
        final File serviceDir = new File("benchmark/CI-HAJournal-1");
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

        // Verify KB exists.
        awaitKBExists(serverA);
        
        /*
         * Note: The quorum was not fully met at the last 2-phase commit.
         * Instead, 2 services participated in the 2-phase commit and the third
         * service resynchronized when it came up and then went through a local
         * commit. Therefore, the HALog files should exist on all nodes.
         */

        // Current commit point.
        final long lastCommitCounter = serverA
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getCommitCounter();

        // There is ONE commit point.
        assertEquals(1L, lastCommitCounter);

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
        {

            final StringBuilder sb = new StringBuilder();
            sb.append("DROP ALL;\n");
            sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
            sb.append("INSERT DATA {\n");
            sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
            sb.append("  dc:creator \"A.N.Other\" .\n");
            sb.append("}\n");
            
            final String updateStr = sb.toString();
            
            final HAGlue leader = quorum.getClient().getLeader(token);
            
            // Verify quorum is still valid.
            quorum.assertQuorum(token);

            getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();
            
        }

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
    	
        // Start 3 services
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
    	final HAGlue serverC = startC();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Verify KB exists.
        awaitKBExists(serverA);
    	   	
        // Now fail leader!
        final HAGlue leader = quorum.getClient().getLeader(token);
        
        shutdown(leader);
        
        // Now check that quorum meets
        
        final long token2 = awaitMetQuorum();
        
        // with new  token
        assertTrue(token != token2);
        
        // and new leader
        assertFalse(leader.equals(quorum.getClient().getLeader(token2)));
    }
    
    /**
     * Tests that halog files are generated and identical, and that
     * when a server is shutdown its logs remain
     * @throws Exception 
     */
    public void testStartAB_halog() throws Exception {
        // Start 2 services
        startA();
        startB();
        
        // Run through transaction
        simpleTransaction();
        
        // close both services
        shutdownA();
        shutdownB();
        
        // check that logfiles exist
        final File serviceDir = new File("benchmark/CI-HAJournal-1");
        final File logsA = new File(serviceDir, "A/HALog");
        final File logsB = new File(serviceDir, "B/HALog");
        
        assertTrue(logsA.listFiles().length == 2);
        assertTrue(logsB.listFiles().length == 2);
        
        // Test ends here!!
        
        // but we'll restart to help teardown
        try {
	        startA();
	        startB();
	        
	        awaitMetQuorum();
        } catch (Throwable t) {
        	log.error("Problem on tidy up", t);
        }
    }
    
    /**
     * Tests that halog files are generated and identical and then
     * removed following resync and fullymet quorum
     * 
     * @throws Exception 
     */
    public void testStartAB_C_halog() throws Exception {
        // Start 2 services
        startA();
        startB();
        
        // Run through transaction
        simpleTransaction();
        
        // check that logfiles exist
        final File serviceDir = new File("benchmark/CI-HAJournal-1");
        final File logsA = new File(serviceDir, "A/HALog");
        final File logsB = new File(serviceDir, "B/HALog");
        
        // There should be 3 halog files from 2 commit points and newly open
        assertTrue(logsA.listFiles().length == 3);
        assertTrue(logsB.listFiles().length == 3);
        
        // now just restart to help teardown
        startC();
        
        awaitFullyMetQuorum();
        
        // Run through another transaction
        simpleTransaction();

        // all committed logs should be removed with only open log remaining
        final File logsC = new File(serviceDir, "C/HALog");
        assertTrue(logsA.listFiles().length == 1);
        assertTrue(logsB.listFiles().length == 1);
        assertTrue(logsC.listFiles().length == 1);
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
        final File serviceDir = new File("benchmark/CI-HAJournal-1");
        final File logsA = new File(serviceDir, "A/HALog");
        final File logsB = new File(serviceDir, "B/HALog");
        final File logsC = new File(serviceDir, "C/HALog");
        
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
        final File serviceDir = new File("benchmark/CI-HAJournal-1");
        final File logsA = new File(serviceDir, "A/HALog");
        final File logsB = new File(serviceDir, "B/HALog");
        final File logsC = new File(serviceDir, "C/HALog");
        
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
        // FIXME: shutting down A first triggers other problems, see test_fullQuorumRestart
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
        final File serviceDir = new File("benchmark/CI-HAJournal-1");
        final File logsA = new File(serviceDir, "A/HALog");
        final File logsB = new File(serviceDir, "B/HALog");
        final File logsC = new File(serviceDir, "C/HALog");
        
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
        
        if (false) { // DEBUG to check logs in shutdown process
	        // startup AB
	        startA();
	        startB();  // need to see log of shutdown problem
        }
        
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
        
        assertTrue(quorum.getJoined().length == 2);
        assertTrue(quorum.getPipeline().length == 2);
        assertTrue(quorum.getMembers().length == 2);
        
        // add C
        startC();
        
        awaitFullyMetQuorum();
    }
    
    /**
     * We have experienced inconsistencies on test startups, this test just attempts
     * to repeatedly start an initial ABC service.
     * @throws Exception
     */
    public void testStressABC_Restart() throws Exception {
    	for (int i = 1; i <= 20; i++) {
    		try {
        		startA();
        		startB();
        		startC();
        		
    			awaitFullyMetQuorum();
    		} catch (Throwable e) {
    			fail("Unable to meet on run " + i, e);
    		}
    		
    		
    		// FIXME: The order of the destroy IS significant since a reorganisation
    		//	on shutdown is another specific problem right now
    		destroyC();
    		destroyA();
    		destroyB();
    	}
    }

    /**
     * We have experienced inconsistencies on test startups, this test just attempts
     * to repeatedly start an initial ABC service.
     */
    public void testStressABCStartSimultaneous() throws Exception {
        for (int i = 1; i <= 20; i++) {
            ABC tmp = null;
            try {
                tmp = new ABC();
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
     * Commits update transaction after awaiting quorum
     */
    private void simpleTransaction() throws IOException, Exception {
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        /*
         * Now go through a commit point with a fully met quorum. The HALog
         * files should be purged at that commit point.
         */

        final StringBuilder sb = new StringBuilder();
        sb.append("DROP ALL;\n");
        sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
        sb.append("INSERT DATA {\n");
        sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
        sb.append("  dc:creator \"A.N.Other\" .\n");
        sb.append("}\n");
        
        final String updateStr = sb.toString();
        
        final HAGlue leader = quorum.getClient().getLeader(token);
        
        // Verify quorum is still valid.
        quorum.assertQuorum(token);

        getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();
            
     }

}
