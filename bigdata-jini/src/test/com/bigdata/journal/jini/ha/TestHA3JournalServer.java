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
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepositoryManager;

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
 * TODO Update the existing tests to verify that the quorum token is properly
 * set on C when C resyncs with A+B and that
 * {@link AbstractJournal#getHAReady()} reports the correct token. This tests
 * for a problem where we did not call setQuorumToken() again when we resync and
 * transition into the met quorum. This meant that the HAReady token is not set
 * for a service unless it is part of the initial quorum meet. One of the HA3
 * backup tests covers this, but we should be checking the HAReadyToken in this
 * test suite as well.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA3JournalServer extends AbstractHA3JournalServerTestCase {

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
        };
        
    }
    
    public TestHA3JournalServer() {
    }

    public TestHA3JournalServer(String name) {
        super(name);
    }

//    /**
//     * Complex hack to override the {@link HAJournal} properties.
//     * 
//     * @param in
//     *            The {@link NV}[] from the configuration (if you can get it).
//     *            
//     * @return The {@link NV}[] from which the {@link Properties} will be
//     *         constructed by {@link JiniClientConfig}
//     */
//    public static NV[] getTestHAJournalProperties(final NV[] in) {
//
//        return in;
//        
//    }
    
//    protected BufferMode getDiskMode() {
//    	return BufferMode.DiskRW;
//    }

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
    	doStartAB_C();
    }
    
    protected void doStartAB_C() throws Exception {

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

        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        
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

        // Verify can not write on follower.
        assertWriteRejected(serverB);
        
        // Start 3rd service.
        final HAGlue serverC = startC();

        // Wait until the quorum is fully met. The token should not change.
        assertEquals(token, awaitFullyMetQuorum());

        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);

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

        // Verify can not write on followers.
        assertWriteRejected(serverB);
        assertWriteRejected(serverC);

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

    public void _testStartABCSimultaneousLookupRestarts() throws Exception {
    	
        final ABC abc = new ABC(false/*sequential*/); // simultaneous start.

        final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

        // Verify quorum is FULLY met.
        final long t1 = awaitFullyMetQuorum();

//        // Verify KB exists on leader.
//        final HAGlue leader = quorum.getClient().getLeader(token);

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Now let's stop zookeeper
        
        ((HAJournalTest.HAGlueTest) abc.serverA).log("About to stop zookeeper");
            
        System.err.println("stop zookeeper");
        for (int i = 5; i > 0; i--) {
        	Thread.sleep(1000);
        	System.err.println("WAIT: " + i);
        }
               
        ((HAJournalTest.HAGlueTest) abc.serverA).log("About to start zookeeper");
        System.err.println("ServerA: " + abc.serverA.getHAStatus());

        System.err.println("start zookeeper");
        
        Thread.sleep(5000);
                
        ((HAJournalTest.HAGlueTest) abc.serverA).log("Waiting for quorum meet");

        // Verify quorum is FULLY met.
        final long t2 = awaitFullyMetQuorum();
        
        assertFalse(t1 == t2);
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
		assertHALogDigestsEquals(1L/* firstCommitCounter */,
				lastCommitCounter, new HAGlue[] { serverA, serverB, serverC });

		// Verify binary equality of journals.
		assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

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
			 * Verify that query on all nodes is allowed and produces nothing
			 * before we write anything.
			 */
			for (JettyRemoteRepositoryManager r : repos) {

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
			ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

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

			// Current commit point.
			final long lastCommitCounter2 = serverA
					.getRootBlock(new HARootBlockRequest(null/* storeUUID */))
					.getRootBlock().getCommitCounter();

			// There are now TWO (2) commit points.
			assertEquals(2L, lastCommitCounter2);

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
			 * Verify that query on all nodes is allowed and now provides an
			 * empty result.
			 */
			for (JettyRemoteRepositoryManager r : repos) {

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
			 * TODO Continue test and verify restart? Or verify restart before
			 * we do the DROP ALL?
			 */
		} finally {
			// close all created repos
			for (JettyRemoteRepositoryManager r : repos) {
				if (r != null)
					r.close();

			}

		}
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

        // Await C as Follower.
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        
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
     * Unit test for a situation in which A B and C start. A quorum mets and the
     * third service resyncs with the met quorum. The quorum then fully meets.
     * Once the fully met quorum is stable, C is then restarted. This test
     * exercises a code path that handles the case where C is current, but is
     * forced into RESYNC in case there are writes in progress on the leader.
     * <p>
     * Note: In this version of the test, the HALog files are purged at each
     * commit of the fully met quorum. Another version of this test exists in
     * which the HALog files are NOT purged at each commit of a fully met
     * quorum.
     */
    public void testStartABC_restartC() throws Exception {
        
        final ABC x = new ABC(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
                x.serverC });
        
        /*
         * The same number of HALog files should exist on all services.
         * 
         * Note: the restore policy is setup such that we are purging the HALog
         * files at each commit of a fully met quorum.
         */
        awaitLogCount(getHALogDirA(), 1L);
        awaitLogCount(getHALogDirB(), 1L);
        awaitLogCount(getHALogDirC(), 1L);
        
        // shutdown C.
        shutdownC();
        
        // wait for C to be gone from zookeeper.
        awaitPipeline(new HAGlue[] { x.serverA, x.serverB });
        awaitMembers(new HAGlue[] { x.serverA, x.serverB });
        awaitJoined(new HAGlue[] { x.serverA, x.serverB });
        
        // restart C.
        /*final HAGlue serverC =*/ startC();
        
        // wait until the quorum fully meets again (on the same token).
        assertEquals(token, awaitFullyMetQuorum());

        // Verify expected HALog files.
        awaitLogCount(getHALogDirA(), 1L);
        awaitLogCount(getHALogDirB(), 1L);
        awaitLogCount(getHALogDirC(), 1L);
        
    }

    /**
     * Unit test for a situation in which A B and C start. A quorum mets and the
     * third service resyncs with the met quorum. The quorum then fully meets.
     * Once the fully met quorum is stable, B is then restarted. The pipeline is
     * reorganized when B is shutdown but the quorum does not break. This test
     * exercises a code path that handles the case where B is current, but is
     * forced into RESYNC in case there are writes in progress on the leader.
     * <p>
     * Note: In this version of the test, the HALog files are NOT purged at each
     * commit of the fully met quorum.
     */
    public void testStartABC_restartB() throws Exception {
        
        final ABC x = new ABC(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
                x.serverC });
        
        /*
         * The same number of HALog files should exist on all services.
         * 
         * Note: the restore policy is setup such that we are purging the HALog
         * files at each commit of a fully met quorum.
         */
        awaitLogCount(getHALogDirA(), 1L);
        awaitLogCount(getHALogDirB(), 1L);
        awaitLogCount(getHALogDirC(), 1L);
        
        // shutdown B.
        shutdownB();
        
        // wait for B to be gone from zookeeper.
        awaitPipeline(new HAGlue[] { x.serverA, x.serverC });
        awaitMembers(new HAGlue[] { x.serverA, x.serverC });
        awaitJoined(new HAGlue[] { x.serverA, x.serverC });
        
        // restart B.
        /*final HAGlue serverB =*/ startB();
        
        // wait until the quorum fully meets again (on the same token).
        assertEquals(token, awaitFullyMetQuorum());

        // Verify expected HALog files.
        awaitLogCount(getHALogDirA(), 1L);
        awaitLogCount(getHALogDirB(), 1L);
        awaitLogCount(getHALogDirC(), 1L);
        
    }

    /**
     * Unit test for a situation in which A B and C start. A quorum mets and the
     * third service resyncs with the met quorum. The quorum then fully meets.
     * Once the fully met quorum is stable, A is then restarted. The pipeline is
     * reorganized when A is shutdown and a new leader is elected. This test
     * exercises a code path that handles the case where A is current, but is
     * forced into RESYNC in case there are writes in progress on the leader.
     * <p>
     * Note: In this version of the test, the HALog files are NOT purged at each
     * commit of the fully met quorum.
     */
    public void testStartABC_restartA() throws Exception {
        
        final ABC x = new ABC(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
                x.serverC });
        
        /*
         * The same number of HALog files should exist on all services.
         * 
         * Note: the restore policy is setup such that we are purging the HALog
         * files at each commit of a fully met quorum.
         */
        awaitLogCount(getHALogDirA(), 1L);
        awaitLogCount(getHALogDirB(), 1L);
        awaitLogCount(getHALogDirC(), 1L);
        
        // shutdown A.
        shutdownA();
        
        // wait for A to be gone from zookeeper.
//        awaitPipeline(new HAGlue[] { x.serverA, x.serverC });
//        awaitMembers(new HAGlue[] { x.serverA, x.serverC });
//        awaitJoined(new HAGlue[] { x.serverA, x.serverC });
        
        // since the leader failed over, the quorum meets on a new token.
        final long token2 = awaitNextQuorumMeet(token);
        
        // restart A.
        /*final HAGlue serverA =*/ startA();

        // wait until the quorum fully meets again (on the same token).
        assertEquals(token2, awaitFullyMetQuorum());

        // Verify expected HALog files.
        awaitLogCount(getHALogDirA(), 1L);
        awaitLogCount(getHALogDirB(), 1L);
        awaitLogCount(getHALogDirC(), 1L);
        
    }

    /**
     * Unit test of the ability to go through a simultaneous restart of all
     * services once those services are no longer at commit point 0. Two
     * services will meet on the lastCommitTime. The third will need to RESYNC
     * and then join. This test provides converage of the RESYNC and JOIN
     * transitions when the database is not empty.
     */
    public void testStartABC_RestartAllSimultaneous() throws Exception {

        // Start simultaneous.
        ABC servers = new ABC(true);

        /*
         * Now go through a commit point with a met quorum.
         */
        simpleTransaction();

        // Current commit point.
        final long lastCommitCounter2 = 2;

        // Await 2nd commit point on all services.
        awaitCommitCounter(lastCommitCounter2, servers.serverA,
                servers.serverB, servers.serverC);

        /*
         * Shutdown ALL services.
         */
        
        shutdownA();
        shutdownB();
        shutdownC();
        
        /*
         * Start simultaneous (again).
         * 
         * Note: Since these are not new service starts, we are only awaiting
         * the fully met quorum. Hence I am NOT re-verifying that the services
         * are at the same commit point (they should be of course) because I
         * would like to promote a data race for the UPDATE with the MEET +
         * RESYNC.
         */
        servers = new ABC(true, false/* newServiceStarts */);

//        // Should be at the same commit point on all services.
//        awaitCommitCounter(lastCommitCounter2, servers.serverA,
//                servers.serverB, servers.serverC);
        
        /*
         * Now go through a commit point with a met quorum.
         */
        simpleTransaction();

        // Current commit point.
        final long lastCommitCounter3 = 3;

        // Await 3rd commit point on all services.
        awaitCommitCounter(lastCommitCounter3, servers.serverA,
                servers.serverB, servers.serverC);

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
     * ZERO (0) committed transactions then at 50ms delay between each
     * subsequent transaction.
     */
    public void testStartAB_C_MultiTransactionResync_5tx_then_50ms_delay()
            throws Exception {

        doStartAB_C_MultiTransactionResync(50, 5);
    }

    public void _testStressStartAB_C_MultiTransactionResync_5tx_then_50ms_delay()
            throws Exception {

        for (int i = 0; i < 40; i++) {
            try {
                doStartAB_C_MultiTransactionResync(50, 5);
            } catch (Throwable t) {
                fail("Fail after " + (i + 1) + " trials : " + t, t);
            } finally {
                destroyAll();
            }
        }

    }
    
    /**
     * This stress test was written after seeing rare failures in
     * testStartAB_C_MultiTransactionResync_5tx_then_200ms_delay.
     *
     * Currently it offers a reliable failure.
     *
     * @throws Exception
     */
    public void _testStressStartAB_C_MultiTransactionResync_200_5()
            throws Exception {

        for (int i = 0; i < 50; i++) {
            try {
                doStartAB_C_MultiTransactionResync(200/* txDelayMillis */, 5/* initialTransactions */);
            } catch (Throwable t) {
                fail("Fail after " + (i + 1) + " trials : " + t, t);
            } finally {
                destroyAll();
            }
        }

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
        final long timeout = TimeUnit.MINUTES.toMillis(4);
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

							final JettyRemoteRepositoryManager repo = getRemoteRepository(leader);
				        	try {
				        		repo.prepareUpdate(
										updateStr).evaluate();
								log.warn("COMPLETED TRANSACTION " + count);
				        	} finally {
				        		repo.close();
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

			// Now Start 3rd service.
			final HAGlue serverC = startC();

			// Wait until the quorum is fully met. After RESYNC
			assertEquals(token, awaitFullyMetQuorum(10));

            awaitHAStatus(new HAStatusEnum[] { HAStatusEnum.Leader,
                    HAStatusEnum.Follower, HAStatusEnum.Follower },
                    new HAGlue[] { serverA, serverB, serverC });

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
                    HAStatusEnum.Follower, HAStatusEnum.Follower },
                    new HAGlue[] { serverA, serverB, serverC });

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
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

		log.info("Should be safe to test digests now");
		
		// Cannot predict last commit counter or whether even logs will remain
		// assertHALogDigestsEquals(1L/* firstCommitCounter */,
		// lastCommitCounter2,
		// new HAGlue[] { serverA, serverB, serverC });

		// Verify binary equality of ALL journals.
		HAGlue[] services = new HAGlue[] { serverA, serverB, serverC };
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
				new HAGlue[] { serverA, serverB, serverC });

        // And again verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

		log.info("ALL GOOD!");
	}
	
	// FIXME Move to StressTestHA3JournalServer
	public void _testStressTestStartAB_C_LiveResync() throws Exception {
		for (int i = 0; i < 50; i++) {
			log.warn("Starting run " + i);
			testStartAB_C_LiveResync();
			destroyAll();
		}
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
        removeFiles(getHALogDirA(), HALogWriter.getHALogFileName(getHALogDirA(), lastCommitCounter2 + 1));
        removeFiles(getHALogDirB(), HALogWriter.getHALogFileName(getHALogDirB(), lastCommitCounter2 + 1));

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
        // TODO Remove all HALogs on A/B in order to force rebuild even if a restorePolicy is being used.
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
        assertEquals(token2, awaitFullyMetQuorum(4/* ticks */));

//        Note: I have seen this timeout. This warrants exploring. BBT.
//        // Wait until C is fully ready.
//        assertEquals(HAStatusEnum.Follower, awaitNSSAndHAReady(serverC));

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        // Check HALogs equal on ALL services.
        assertHALogDigestsEquals(7L/* firstCommitCounter */, 7L, new HAGlue[] {
                serverA, serverB, serverC });

    }

    // FIXME Move to StressTestHA3JournalServer
    public void _testStress_RebuildWithPipelineReorganisation() throws Exception {
        for (int i = 0; i < 50; i++) {
            try {
                testStartABC_RebuildWithPipelineReorganisation();
            } catch (Throwable t) {
                fail("Run " + i, t);
            } finally {
                destroyAll();
            }
        }
    }

    /**
     * Test Rebuild of C service where quorum was previously fully met and where
     * a new quorum is met before C joins for rebuild.
     * 
     * @throws Exception
     */
    public void testStartABC_Rebuild() throws Exception {

        new ABC(false/*sequential*/); // simultaneous start.
//        {
//
//            final HAGlue serverA = startA();
//            final HAGlue serverB = startB();
//            final HAGlue serverC = startC();
//
//            awaitFullyMetQuorum();
//
//            // Await initial commit point (KB create) on all servers.
//            awaitCommitCounter(1L, serverA, serverB, serverC);
//            
//        }
        
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
        awaitFullyMetQuorum(4/* ticks */);

        // Verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
    }
    
    /**
     * Test automatic restore of HALog files on restart by:
     * <pre>starting ABC
     * 	drop C
     *  run transaction through AB
     *  drop A
     *  copy HALog files from A to C.
     *  start C
     *  C shoudl restore (replay any new HALog files on startup).
     *  Meet on BC
     *  start A
     *  Fully Meet</pre>
     * @throws Exception 
     */
    public void testABC_Restore() throws Exception {

        new ABC(true/* sequential */);
    
        /*
         * Now go through a commit point with a met quorum. The HALog
         * files should be retained at that commit point.
         */
        simpleTransaction();
        
        // now shutdown C (not destroy)
        shutdownC();
        
//        log.warn("CHECK OPEN LOGS ON A B");
        
        /*
         * Now go through a commit point with a met quorum. The HALog
         * files should be retained at that commit point.
         */
        if (true/*new commit state*/) {

            simpleTransaction();
            
        }

//        log.warn("CHECK LOGS ON A B");
        
        // now shutdown A (not destroy)
        shutdownA();
        
        // copy log files from A to C
        copyFiles(getHALogDirA(), getHALogDirC());

        /*
         * Restart C. C should apply the new HALog file that was copied over
         * from A.
         */
        startC();
        
//        log.warn("CHECK LOGS HAVE BEEN COPIED TO C");
        
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

    public void testQuorumABC_killC() throws Exception {

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
        
        ((HAGlueTest)serverC).log("Will kill C");
        ((HAGlueTest)serverB).log("Will kill C");
        ((HAGlueTest)serverA).log("Will kill C");
        
        // Now kill C - this will leave some file detritus
        kill(serverC);
        
        // wait around to let the kill play out by waiting for [A, B] pipeline
        awaitPipeline(getZKSessionTimeout() + 5000, TimeUnit.MILLISECONDS,
                new HAGlue[] { serverA, serverB });

        // assert quorum remains met
        assertTrue(quorum.isQuorumMet());
        
        // ...and with original token
        assertTrue(token == quorum.token());
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
     * Test quorum breaks and reforms when original leader fails
     * 
     * @throws Exception 
     */
	public void testQuorumBreaksABC_killLeader() throws Exception {

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

		// Now kill leader!
 		kill(startup.serverA);

		// Check that quorum meets around the remaining 2 services.
		final long token2 = awaitNextQuorumMeet(token);

		// Verify that we have a new leader for the quorum.
		final HAGlue leader = quorum.getClient().getLeader(token2);

        assertTrue(leader.equals(startup.serverB)
                || leader.equals(startup.serverC));
        
	}
    
    /**
     * Having observed stochastic failures, here is a stress test
     * to try and generate a more deterministic failure.
     * 
     * @throws Exception
     */
    public void _testStressQuorumBreaksABC_failLeader()
            throws Exception {

        for (int i = 0; i < 40; i++) {
        	log.warn("RUN: " + i);
        	testQuorumBreaksABC_failLeader();
            destroyAll();
        }

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
        assertEquals(2L, recursiveCount(getHALogDirA(),IHALogReader.HALOG_FILTER));
        assertEquals(2L, recursiveCount(getHALogDirB(),IHALogReader.HALOG_FILTER));
        
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
        
        // There should be 3 halog files from 2 commit points and one newly open
        assertEquals(3L, recursiveCount(getHALogDirA(),IHALogReader.HALOG_FILTER));
        assertEquals(3L, recursiveCount(getHALogDirB(),IHALogReader.HALOG_FILTER));

        // now just restart to help teardown
        startC();
        
        awaitFullyMetQuorum();
        
        // Run through another transaction
        simpleTransaction();

        /**
         * All committed logs should be removed with only open log remaining.
         * 
         * Note: This condition is only eventually true now that we allow the
         * asychronous release of the HALog files.
         * 
         * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/780"
         *      >Incremental or asynchronous purge of HALog files</a>
         */
        assertCondition(new Runnable() {

            public void run() {
                assertEquals(1L, recursiveCount(getHALogDirA(),IHALogReader.HALOG_FILTER));
                assertEquals(1L, recursiveCount(getHALogDirB(),IHALogReader.HALOG_FILTER));
                assertEquals(1L, recursiveCount(getHALogDirC(),IHALogReader.HALOG_FILTER));
            }
        });
        
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
        
        // setup log directories
        final File logsA = getHALogDirA();
        final File logsB = getHALogDirB();
        final File logsC = getHALogDirC();
        
        /*
         * Start 2 services and wait for first commit point. The HALogs will not
         * be purged because the quorum is not fully met.
         */
        HAGlue serverA = startA();
        HAGlue serverB = startB();
        final long token = awaitMetQuorum();
        // initial token value.
        assertEquals(0L, token);
        awaitCommitCounter(1L, serverA, serverB);
        awaitLogCount(logsA, 2);
        awaitLogCount(logsB, 2);

        /*
         * Start next service. The service will resync and join with the met
         * quorum. The HALog files will not be purged - they do not get purged
         * until the fully met quorum goes through a commit point.
         */
        HAGlue serverC = startC();
        final long token2 = awaitFullyMetQuorum();
        assertEquals(token2,token);
        awaitCommitCounter(1L, serverA, serverB,
                serverC);

        // committed log files not purged prior to fully met commit
        awaitLogCount(logsA, 2);
        awaitLogCount(logsB, 2);
        awaitLogCount(logsC, 2);
        
        //
        // Above here the code is shared with halogRestart()
        //

        // Run through transaction
        simpleTransaction();
        
        // again check that only open log files remaining
        awaitLogCount(logsA, 1);
        awaitLogCount(logsB, 1);
        awaitLogCount(logsC, 1);
        assertEquals(1L, recursiveCount(getHALogDirA(),IHALogReader.HALOG_FILTER));
        assertEquals(1L, recursiveCount(getHALogDirB(),IHALogReader.HALOG_FILTER));
        assertEquals(1L, recursiveCount(getHALogDirC(),IHALogReader.HALOG_FILTER));

        // Now run several transactions
        for (int i = 0; i < 5; i++) {

            simpleTransaction();

        }
        
        // again check that only open log files remaining
        awaitLogCount(logsA, 1);
        awaitLogCount(logsB, 1);
        awaitLogCount(logsC, 1);
    }
    
    /**
     * Tests that new logs are established on restarting with
     * transitions through Met and FullyMet
     * 
     * @throws Exception
     */
    public void testStartABC_halogRestart() throws Exception {
        
        // setup log directories
        final File logsA = getHALogDirA();
        final File logsB = getHALogDirB();
        final File logsC = getHALogDirC();
        
        /*
         * Start 2 services and wait for first commit point. The HALogs will not
         * be purged because the quorum is not fully met.
         */
        HAGlue serverA = startA();
        HAGlue serverB = startB();
        final long token = awaitMetQuorum();
        // initial token value.
        assertEquals(0L, token);
        awaitCommitCounter(1L, serverA, serverB);
        awaitLogCount(logsA, 2);
        awaitLogCount(logsB, 2);

        /*
         * Start next service. The service will resync and join with the met
         * quorum. The HALog files will not be purged - they do not get purged
         * until the fully met quorum goes through a commit point.
         */
        HAGlue serverC = startC();
        final long token2 = awaitFullyMetQuorum();
        assertEquals(token2,token);
        awaitCommitCounter(1L, serverA, serverB,
                serverC);

        // committed log files not purged prior to fully met commit
        awaitLogCount(logsA, 2);
        awaitLogCount(logsB, 2);
        awaitLogCount(logsC, 2);
        
        //
        // Above here the code is shared with halogRestart2()
        //

        // Run through transaction
        simpleTransaction();
        
        // again check that only open log files remaining
        awaitLogCount(logsA, 1);
        awaitLogCount(logsB, 1);
        awaitLogCount(logsC, 1);
        
        // Now shutdown all servers
        shutdownB();
        shutdownC();
        shutdownA();
        
        // and check that there are no logs
        awaitLogCount(logsA, 0);
        awaitLogCount(logsB, 0);
        awaitLogCount(logsC, 0);
        
        // startup AB
        serverA = startA();
        serverB = startB();
        
        final long token1 = awaitMetQuorum();
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        
        // Verify new quorum token.
        assertEquals(token + 1, token1);
        // and check that there are open logs
        awaitLogCount(logsA, 1);
        awaitLogCount(logsB, 1);
        
        // add C
        serverC = startC();
        
        assertEquals(token1, awaitFullyMetQuorum());
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        
        // and check again for ABC
        awaitLogCount(logsA, 1);
        awaitLogCount(logsB, 1);
        awaitLogCount(logsC, 1);
    }
    
    /**
     * Variant where A is shutdown first.
     */
    public void testStartABC_halogRestart2() throws Exception {

        // setup log directories
        final File logsA = getHALogDirA();
        final File logsB = getHALogDirB();
        final File logsC = getHALogDirC();
        
        /*
         * Start 2 services and wait for first commit point. The HALogs will not
         * be purged because the quorum is not fully met.
         */
        HAGlue serverA = startA();
        HAGlue serverB = startB();
        final long token = awaitMetQuorum();
        // initial token value.
        assertEquals(0L, token);
        awaitCommitCounter(1L, serverA, serverB);
        awaitLogCount(logsA, 2);
        awaitLogCount(logsB, 2);

        /*
         * Start next service. The service will resync and join with the met
         * quorum. The HALog files will not be purged - they do not get purged
         * until the fully met quorum goes through a commit point.
         */
        HAGlue serverC = startC();
        final long token2 = awaitFullyMetQuorum();
        assertEquals(token2,token);
        awaitCommitCounter(1L, serverA, serverB,
                serverC);

        // committed log files not purged prior to fully met commit
        awaitLogCount(logsA, 2);
        awaitLogCount(logsB, 2);
        awaitLogCount(logsC, 2);
        
        //
        // Above here the code is shared with halogRestart()
        //

        // Run through transaction
        simpleTransaction();
        
        // again check that only open log files remaining
        awaitLogCount(logsA, 1);
        awaitLogCount(logsB, 1);
        awaitLogCount(logsC, 1);

        // Verify token unchanged.
        assertEquals(token, awaitFullyMetQuorum());
        
        // Now shutdown all servers
        shutdownA();
        shutdownB();
        shutdownC();
        
        // and check that there are no logs
        awaitLogCount(logsA, 0);
        awaitLogCount(logsB, 0);
        awaitLogCount(logsC, 0);
        
        // startup AB
        serverA = startA();
        serverB = startB();
        
        final long token1 = awaitMetQuorum();
        
        /*
         * Verify new quorum token (could be a quorum meet when the leader
         * leaves so this might be ONE (1) or TWO (2).
         */
        assertTrue(token1 >= token + 1);

        // and check that there are open logs
        awaitLogCount(logsA, 1);
        awaitLogCount(logsB, 1);
        
        // add C
        serverC = startC();
        
        // Verify quorum token is unchanged.
        assertEquals(token1, awaitFullyMetQuorum());
        
        // and check again for ABC
        awaitLogCount(logsA, 1);
        awaitLogCount(logsB, 1);
        awaitLogCount(logsC, 1);
    }

    public void testABCMultiTransactionFollowerReads() throws Exception {
    	// doABCMultiTransactionFollowerReads(20000/*nTransactions*/, 20/*delay per transaction*/); // STRESS
        doABCMultiTransactionFollowerReads(200/*nTransactions*/, 20/*delay per transaction*/);
    }
    
    /**
     * Tests multiple concurrent reads on followers in presence of multiple
     * updates.
     * 
     * @param nTransactions
     *            The #of UPDATE transactions to execute.
     * @param transactionDelay
     *            The delay between the UPDATE transactions.
     *            
     * TODO Modify to run a lot of concurrent fast queries (multiple threads) on one follower and slow queries on the other (one thread).  Parameterize for stress test scenarios.
     */
    protected void doABCMultiTransactionFollowerReads(final int nTransactions,
            final long transactionDelay) throws Exception {

        // final long timeout = TimeUnit.MINUTES.toMillis(4);
        final long timeout = (transactionDelay + 1000) * nTransactions; // allow 1 second per transaction
        try {

            // Start all services.
            final ABC services = new ABC(true/* sequential */);

            // Wait for a quorum meet.
            final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                    TimeUnit.MILLISECONDS);

            assertEquals(token, awaitFullyMetQuorum());

            final HAGlue leader = quorum.getClient().getLeader(token);

            // Verify assumption in this test.
            assertEquals(leader, services.serverA);

            // Wait until all services are "HA" ready.
            leader.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            services.serverB.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            services.serverC.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);

            /*
             * Start a long running long. This gives us enough data to make the
             * query we run have a little latency.  We need that query to cross
             * a commit boundary when it is executed.
             */
            {
                
                final FutureTask<Void> ft = new FutureTask<Void>(
                        new LargeLoadTask(token, true/* reallyLargeLoad */));
                
                try {

                    // Start the LOAD task.
                    executorService.submit(ft);

                    // Wait for the LOAD task.
                    ft.get(timeout, TimeUnit.MILLISECONDS);

                } finally {
                    
                    ft.cancel(true/* mayInterruptIfRunning */);
                    
                }
                
            }
            
            /**
             * Class will issue a series of UPDATE requests.
             * <p>
             * Note: We need to actually modify the KB for each UPDATE but not
             * drop all the statemets each time.
             */
			class UpdateTask implements Callable<Void> {

				public UpdateTask() throws IOException {

				}

				public Void call() throws Exception {

					final JettyRemoteRepositoryManager remoteRepo = getRemoteRepository(leader);
					try {
						for (int n = 0; n < nTransactions; n++) {

							// Verify quorum is still valid.
							quorum.assertQuorum(token);

							// Do a simple UPDATE transaction.
							final StringBuilder sb = new StringBuilder();
							sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
							sb.append("INSERT DATA {\n");
							sb.append("  <http://example/book" + n // Note:
																	// distinct
																	// triple!
									+ "> dc:title \"A new book\" ;\n");
							sb.append("  dc:creator \"A.N.Other\" .\n");
							sb.append("}\n");

							final String updateStr = sb.toString();

							remoteRepo.prepareUpdate(updateStr).evaluate();

							if (log.isInfoEnabled())
								log.info("COMPLETED TRANSACTION " + n);

							Thread.sleep(transactionDelay);

						}

						// done.
						return null;
					} finally {
						remoteRepo.close();
					}

				}

			};
            
            /*
             * Future for the task executing a series of UPDATES.
             * 
             * Note: The task is not running yet, but we will pass the Future
             * into the QueryTask so it can monitor the UpdateTask.
             */
            final FutureTask<Void> updateTaskFuture = new FutureTask<Void>(new UpdateTask());

            /*
             * Now create a Callable for the final followes to repeatedly query
             * against the then current commit point. The task returns the #of
             * queries that were executed. The task will run until we stop
             * issuing UPDATE requests.
             */
            class QueryTask implements Callable<Long> {
                
//                /** The service to query. */
//                final HAGlue haGlue;
                
                /**
                 * The SPARQL end point for that service.
                 */
                final HAGlue haGlue;

                /**
                 * Format for timestamps that may be used to correlate with the
                 * HA log messages.
                 */
                final SimpleDateFormat df = new SimpleDateFormat("hh:mm:ss,SSS");

                /**
                 * @param haGlue
                 *            The service to query.
                 *            
                 * @throws IOException 
                 */
                public QueryTask(final HAGlue haGlue) throws IOException {
                
                    this.haGlue = haGlue;
                    
                    /*
                     * Run query against one of the followers.
                     * 
                     * 6537 queries for 2000 transactions (leader)
                     * 
                     * 10109 queries for 2000 transactions (follower)
                     */

                }

                public Long call() throws Exception {
                    
                    long queryCount = 0;
                    
                    final JettyRemoteRepositoryManager remoteRepo;
                    remoteRepo = getRemoteRepository(haGlue);
					try {
						while (!updateTaskFuture.isDone()) {

							final String query;
							if (queryCount % 2 == 0) {
								// Shorter query. Counts all tuples.
								query = "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }";
							} else {
								// Longer query. Materializies up to N tuples.
								query = "SELECT * WHERE { ?s ?p ?o } LIMIT 100000";
							}

							// Verify quorum is still valid.
							quorum.assertQuorum(token);

							// Run query.
							final long nresults = countResults(remoteRepo
									.prepareTupleQuery(query).evaluate());

							queryCount++;

							// add date time format for comparison with HA logs
							if (log.isInfoEnabled())
								log.info(df.format(new Date())
										+ " - completed query: " + queryCount
										+ ", nresults=" + nresults);

						}
					} finally {
                    	remoteRepo.close();
                    }
 
                    // done.
                    return queryCount;
                    
                }
  
            };

            final FutureTask<Long> queryTaskFuture = new FutureTask<Long>(
                    new QueryTask(services.serverC));

            /*
             * Start the tasks to run the UPDATES (against the leader) and the
             * QUERIES (against a follower).
             */

            try {

                // Start the UPDATE task.  Runs many updates.
                executorService.submit(updateTaskFuture);

                // Start the QUERY task. Runs many queries.
                executorService.submit(queryTaskFuture);

                // Wait for QueryTask : ends once UPDATE task is done.
                queryTaskFuture.get(timeout, TimeUnit.MILLISECONDS);

                // Verify update task is done.
                assertTrue(updateTaskFuture.isDone());
                
                // Check Future for errors (task is already done).
                updateTaskFuture.get();
 
            } finally {
            
                updateTaskFuture.cancel(true/* mayInterruptIfRunning */);

                queryTaskFuture.cancel(true/* mayInterruptIfRunning */);

            }

			assertDigestsEquals(new HAGlue[] { services.serverA, services.serverB, services.serverC });

        } finally {
		
            destroyAll();
            
		}
	    	
    }

    /**
     * Similar to multitransaction but rather than a number of updates following
     * a load it is simply a number of loads followed by queries on the folowers
     * that are checkd for consistency.
     */
    public void testABCMultiLoadFollowerReads() throws Exception {

        doABCMultiLoadFollowerReads2(50/* nTransactions */, false/* largeLoad */);
        
    }

    /**
     * Similar to multitransaction but rather than a number of updates following
     * a load it is simply a number of loads followed by queries on the folowers
     * that are checkd for consistency.
     */
    public void testABCMultiLoadFollowerReadsLargeLoad() throws Exception {
        
        doABCMultiLoadFollowerReads2(20/* nTransactions */, true/* largeLoad */);
        
    }
    
    /**
     * Similar to multitransaction but rather than a number of updates following
     * a load it is simply a number of loads followed by queries on the folowers
     * that are checked for consistency.
     * 
     * @param loads
     *            The number of LOAD operations to perform.
     * @param largeLoad
     *            If true, the load a large file.
     */
    protected void doABCMultiLoadFollowerReads2(final int nTransactions,
            final boolean largeLoad) throws Exception {

        // Start all services.
        final ABC services = new ABC(true/* sequential */);

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        assertEquals(token, awaitFullyMetQuorum());

        final HAGlue leader = quorum.getClient().getLeader(token);

        // Verify assumption in this test.
        assertEquals(leader, services.serverA);

        // Wait until all services are "HA" ready.
        leader.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
        services.serverB
                .awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
        services.serverC
                .awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);

        /*
         * Now create a Callable for the final followes to repeatedly query
         * against the then current commit point. The task returns the #of
         * queries that were executed. The task will run until we stop issuing
         * UPDATE requests.
         */
        class QueryTask implements Callable<Long> {

            /** The service to query. */
            private final HAGlue haGlue;

            public QueryTask(final HAGlue haGlue) throws IOException {

                this.haGlue = haGlue;

            }

            @Override
            public Long call() throws Exception {

                return getCountStar(haGlue);

            }

        } // class QueryTask

        /*
         * Sequentially run repeated loads and after each load submit queries on
         * all services, checking for consistency.
         */

        for (int t = 0; t < nTransactions; t++) {

            // Create tasks, but do not execute yet.
            final FutureTask<Void> loadTaskFuture = new FutureTask<Void>(
                    new LargeLoadTask(token, largeLoad/* reallyLargeLoad */));
            final FutureTask<Long> qAFuture = new FutureTask<Long>(
                    new QueryTask(services.serverA));
            final FutureTask<Long> qBFuture = new FutureTask<Long>(
                    new QueryTask(services.serverB));
            final FutureTask<Long> qCFuture = new FutureTask<Long>(
                    new QueryTask(services.serverC));

            try {

                // Execute LOAD.
                executorService.submit(loadTaskFuture);
                loadTaskFuture.get(); // wait on load!

                // Execute query tasks.
                executorService.submit(qAFuture);
                executorService.submit(qBFuture);
                executorService.submit(qCFuture);

                if (log.isInfoEnabled())
                    log.info("StatementsA: " + qAFuture.get()
                            + ", StatementsB: " + qBFuture.get()
                            + ", StatementsC: " + qCFuture.get());

                assertEquals(qAFuture.get(), qBFuture.get());
                assertEquals(qAFuture.get(), qCFuture.get());

            } finally {

                // Ensure all tasks are cancelled.
                loadTaskFuture.cancel(true/* mayInterruptIfRunning */);
                qAFuture.cancel(true/* mayInterruptIfRunning */);
                qBFuture.cancel(true/* mayInterruptIfRunning */);
                qCFuture.cancel(true/* mayInterruptIfRunning */);

            }

        }

        // Finally check for binary compatibility
        assertDigestsEquals(new HAGlue[] { services.serverA, services.serverB,
                services.serverC });

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
        awaitLogCount(logsA, 6);
        awaitLogCount(logsB, 6);

        // and restart C with empty journal, forces Rebuild
        startC();
        
        awaitFullyMetQuorum();
        
        awaitLogCount(logsA, 6);
        awaitLogCount(logsB, 6);        
        // Log count on C is 1 after quiescent rebuild
        awaitLogCount(logsC, 1);
        
        log.warn("CHECK: Committed log files not copied on Rebuild");
        
        // Now run several transactions
        for (int i = 0; i < 5; i++)
            simpleTransaction();

        // and check that only open log files remaining
        awaitLogCount(logsA, 1);
        awaitLogCount(logsB, 1);
        awaitLogCount(logsC, 1);
        
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
        for (int i = 1; i <= 40; i++) {
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
        for (int i = 1; i <= 200; i++) {
            ABC tmp = null;
            try {
                tmp = new ABC(false/* sequential */);
                // ABC already awaits quorum and assert commit counter!
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

    public void _testSANDBOXStressStartup() throws Exception {
        for (int i = 1; i <= 50; i++) {
        	doStartAB_C();
        	destroyAll();
        	
            ABC tmp = null;
            try {
            	
                tmp = new ABC(false/* sequential */);
                // ABC already awaits quorum and assert commit counter!
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
     * does not break when any follower leaves. For this test, we are not
     * writing on the quorum.
     * 
     * @throws Exception
     */
    public void testABC_RemainsMet() throws Exception {

        // enforce join order
		final ABC startup = new ABC(true/*sequential*/);

		final long token = awaitFullyMetQuorum();
		
		awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB, startup.serverC});

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
	
		// restart B.
		final HAGlue serverB2 = startB();
		
		// appears at the end of the pipeline.
		awaitPipeline(new HAGlue[] {startup.serverA, serverC2, serverB2});

		// quorum fully meets again.
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
		
        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

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
		
        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

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
                token, true));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(300/* ms */);

        // Verify load is still running.
        assertFalse(ft.isDone());
        
        // shutdown C, the final follower
        shutdownC();
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB});

        // And no longer joined.
        awaitJoined(new HAGlue[] {startup.serverA, startup.serverB});

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // restart C.
        final HAGlue serverC2 = startC();

        // C comes back at the end of the pipeline.
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverB, serverC2});
        
        // And Joins.
        // awaitJoined(new HAGlue[] {startup.serverA, startup.serverB, serverC2});
        
        // Await fully met quorum *before* LOAD is done.
        assertTrue(awaitFullyMetDuringLOAD2(token, ft));

        // Verify fully met.
        assertTrue(quorum.isQuorumFullyMet(token));

        /*
        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);
        // no delay needed since commit2Phase should ensure stores all synced
        
        try {
        	assertDigestsEquals(new HAGlue[] { startup.serverA, startup.serverB, serverC2 });
        } catch (final AssertionFailedError afe) {
        	shutdownA();
        	shutdownB();
        	shutdownC();
        	throw afe;
        }
        */
    }
    
    /**
     * Stress test disabled for CI
     */
	public void _test_stress() throws Exception {
		for (int i = 0; i < 20; i++) {
			try {
				testABC_LiveLoadRemainsMet_restart_C_fullyMetDuringLOAD();
			} finally {
				destroyAll();
			}
		}
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
        
        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

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
        
        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);
        
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
                token, true));

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

        // B comes back at the end of the pipeline.
        awaitPipeline(new HAGlue[] {startup.serverA, startup.serverC, serverB2});
        
        // Await fully met quorum *before* LOAD is done.
        assertTrue(awaitFullyMetDuringLOAD2(token, ft));

        // Verify fully met.
        assertTrue(quorum.isQuorumFullyMet(token));

        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);
        
//        {
//			final StringBuilder sb = new StringBuilder();
//			sb.append("SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }\n");
//	
//			final String query = sb.toString();
//	
//			// final RemoteRepository follower = getRemoteRepository(services.serverA); // try with Leader to see difference! 6537 queries (less than for follower)
//			final RemoteRepository follower = getRemoteRepository(serverB2); // 10109 queries for 2000 transact	ons
//	
//			final TupleQueryResult result = follower.prepareTupleQuery(query).evaluate();
//			
//			log.warn("Result Follower: " + result.next().getBinding("count"));
//		
//			final RemoteRepository leader = getRemoteRepository(startup.serverA); // 10109 queries for 2000 transact	ons
//			
//			final TupleQueryResult resultLeader = leader.prepareTupleQuery(query).evaluate();
//			
//			log.warn("Result Leader: " + resultLeader.next().getBinding("count"));
//        }

		// assertDigestsEquals(new HAGlue[] { startup.serverA, serverB2, startup.serverC });
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
                token, true/* reallyLargeLoad */));

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
        assertTrue(awaitFullyMetDuringLOAD2(token, ft));

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
        assertTrue(awaitFullyMetDuringLOAD2(token, ft));

        // Verify fully met.
        assertTrue(quorum.isQuorumFullyMet(token));

        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

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
        assertTrue(awaitFullyMetDuringLOAD2(token, ft));

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
        assertTrue(awaitFullyMetDuringLOAD2(token, ft));

        // Verify fully met.
        assertTrue(quorum.isQuorumFullyMet(token));

        // Await LOAD, but with a timeout.
        ft.get(2 * longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

    }

    /**
     * Variant of {@link #testABC_LargeLoad()} that issues concurrent HA Status
     * requests. This is a regression test for a deadlock observed when a status
     * request was issued during a BSBM 100M load on an HA3 cluster.
     */
    public void testABC_LargeLoad_concurrentStatusRequests() throws Exception {
        
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
        
        /*
         * LOAD data on leader.
         */
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token, true/* reallyLargeLoad */));

        final AtomicLong nsuccess = new AtomicLong();

        // All concurrent requests.
        final ScheduledExecutorService scheduledExecutor = Executors
                .newScheduledThreadPool(3/* corePoolSize */);

        ScheduledFuture<?> scheduledFuture = null;
        
        try {

            // Start LOAD.
            executorService.submit(ft);

            // Start STATUS requests.  Fixed Rate, so can be concurrent.
            scheduledFuture = scheduledExecutor.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    try {
                        doNSSStatusRequest(serverA);
                        nsuccess.incrementAndGet();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }, 50L/* initialDelay */, 20L/* period */, TimeUnit.MILLISECONDS);
            
            // Await LOAD, but with a timeout.
            ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

            if (scheduledFuture.isDone()) {
                /*
                 * Note: We are not expecting this to complete during the load.
                 */
                scheduledFuture.get();
                fail("Not expecting scheduledFuture to be done: nsuccess="
                        + nsuccess);
            }

        } finally {

            ft.cancel(true/* mayInterruptIfRunning */);

            if (scheduledFuture != null)
                scheduledFuture.cancel(true/* mayInterruptIfRunning */);
            
            scheduledExecutor.shutdownNow();
            
        }
        
    }

    /**
     * Verify that the {@link HAStatusEnum} is updated coorectly as services are
     * fail over, when their roles change, and when the quorum meets and breaks.
     */
    public void testQuorumABC_HAStatusUpdatesWithFailovers() throws Exception {

        // Start 2 services.
        HAGlue serverA = startA();
        final HAGlue serverB = startB();

        // Wait for a quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);

        // Start 3rd service.
        final HAGlue serverC = startC();

        // Wait until the quorum is fully met. The token should not change.
        assertEquals(token, awaitFullyMetQuorum());

        awaitHAStatus(serverB, HAStatusEnum.Follower);

        // Simple transaction.
        simpleTransaction();

        /*
         * Shutdown A. This causes a quorum break. B will become the new leader.
         */
        shutdownA();

//        /*
//         * TODO Why sleep here? Is it because we might otherwise still observe a
//         * valid quorum? In which case, we need to await the next token, passing
//         * in the current one rather than just tossing in a sleep().
//         */
//        Thread.sleep(50); // wait a while for A to shutdown
//
//      final long token2 = quorum.awaitQuorum(awaitQuorumTimeout * 2,
//      TimeUnit.MILLISECONDS);

        // Wait for the next quorum meet on (token+1).
        final long token2 = awaitNextQuorumMeet(token);

        // New token.
        assertEquals(token2, token + 1);

        /**
         * Figure out the new leader.
         * 
         * Note: There appears to be a problem were [leader] can not always be
         * compared with [serverB] and [serverC] using reference testing (==).
         * In the trace below, you can see that [leader] has the same data in
         * this case as [serverC] (same UUID, same TcpEndpoint). I have modified
         * the code to compare the ServiceID values using equals().
         * 
         * <pre>
         * junit.framework.AssertionFailedError: Did not elect leader consistent with expectations:
         *    leader=Proxy[HAGlue,BasicInvocationHandler[BasicObjectEndpoint[b44e72e1-dd5c-4dbc-9640-129bdab11007,TcpEndpoint[192.168.1.135:55983]]]],
         *   serverB=Proxy[HAGlue,BasicInvocationHandler[BasicObjectEndpoint[073e0614-26a6-49be-83f4-381ce6338306,TcpEndpoint[192.168.1.135:55965]]]],
         *   serverC=Proxy[HAGlue,BasicInvocationHandler[BasicObjectEndpoint[b44e72e1-dd5c-4dbc-9640-129bdab11007,TcpEndpoint[192.168.1.135:55983]]]]
         *     at junit.framework.Assert.fail(Assert.java:47)
         *     at com.bigdata.journal.jini.ha.TestHA3JournalServer.testQuorumABC_HAStatusUpdatesWithFailovers(TestHA3JournalServer.java:2946)
         * </pre>
         */
        final HAGlue leader = quorum.getClient().getLeader(token2);
        final HAGlue follower1;
        if (leader.getServiceId().equals(serverB.getServiceId())) {
            follower1 = serverC;
        } else if (leader.getServiceId().equals(serverC.getServiceId())) {
            follower1 = serverB;
        } else {
            follower1 = null; // to keep compiler happy
            fail("Did not elect leader consistent with expectations: leader="
                    + leader + ", serverB=" + serverB + ", serverC=" + serverC);
        }

        // Self-report in the correct roles.
        awaitHAStatus(leader, HAStatusEnum.Leader);
        awaitHAStatus(follower1, HAStatusEnum.Follower);

        // Restart A.
        serverA = startA();

        // Self-report in the correct roles.
        awaitHAStatus(leader, HAStatusEnum.Leader);
        awaitHAStatus(follower1, HAStatusEnum.Follower);
        awaitHAStatus(serverA, HAStatusEnum.Follower);

        /*
         * Shutdown follower1 (either B or C). Quorum remains met.
         */
        if (follower1 == serverC)
            shutdownC();
        else if (follower1 == serverB)
            shutdownB();
        else
            throw new AssertionError();
        
        // Correct roles.
        awaitHAStatus(leader, HAStatusEnum.Leader);
        awaitHAStatus(serverA, HAStatusEnum.Follower);

        /*
         * Shutdown A.  Quorum breaks. 
         */
        
        shutdownA();
        
        // Correct roles.
        awaitHAStatus(leader, HAStatusEnum.NotReady);

    }

	public void _testStressQuorumABC_HAStatusUpdatesWithFailovers()
			throws Exception {
		for (int i = 1; i <= 20; i++) {
			try {
				testQuorumABC_HAStatusUpdatesWithFailovers();
			} catch (Throwable e) {
				fail("Run " + i, e);
			} finally {
				destroyAll();
			}
		}
	}
    
    /**
     * Test verifies that we can POST a SPARQL query to a follower.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/853"> Follower does not
     *      accept POST of idempotent operations (HA) </a>
     */
	public void test_postQueryOnFollowers() throws Exception {

		final ABC abc = new ABC(false/* sequential */); // simultaneous start.

		final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

		// Verify quorum is FULLY met.
		awaitFullyMetQuorum();

		// await the KB create commit point to become visible on each service.
		awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

		// Verify binary equality of ALL journals.
		assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

		final JettyRemoteRepositoryManager[] repos = new JettyRemoteRepositoryManager[3];
		try {
			repos[0] = getRemoteRepository(serverA);
			repos[1] = getRemoteRepository(serverB);
			repos[2] = getRemoteRepository(serverC);

			/*
			 * Verify that query on all nodes is allowed.
			 */
			for (JettyRemoteRepositoryManager r : repos) {

				r.setQueryMethod("GET");

				// Should be empty.
				assertEquals(0L,
						countResults(r.prepareTupleQuery("SELECT * {?a ?b ?c}")
								.evaluate()));

			}

			// Change the maximum length of a GET for a Query.
			for (JettyRemoteRepositoryManager r : repos) {

				r.setMaxRequestURLLength(1);

			}

			// Run with the new length. All requests should be POSTs.
			for (JettyRemoteRepositoryManager r : repos) {

				r.setQueryMethod("POST");

				// Should be empty.
				assertEquals(0L,
						countResults(r.prepareTupleQuery("SELECT * {?a ?b ?c}")
								.evaluate()));

			}

		} finally {

			for (JettyRemoteRepositoryManager r : repos) {
				if (r != null) {
					r.close();
				}
			}
		}

	}

}
