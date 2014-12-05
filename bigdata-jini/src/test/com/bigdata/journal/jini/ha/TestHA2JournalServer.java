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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepositoryManager;

/**
 * Test suites for an {@link HAJournalServer} quorum with a replication factor
 * of THREE (3) but only TWO (2) services running. In this mode, the
 * {@link HAJournalServer} will retain the generated HALog files and the
 * {@link Quorum} is met, but not fully met. If a single service fails, the
 * {@link Quorum} will break. Thus, this test suite can be used to examine a
 * number of boundary conditions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA2JournalServer extends AbstractHA3JournalServerTestCase {

    protected static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

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
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
//                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\""
        };
        
    }

    public TestHA2JournalServer() {
    }

    public TestHA2JournalServer(final String name) {
        super(name);
    }
    
    /**
     * ---- HA2 TESTS ----
     * 
     * This is a series of basic tests of an HA quorum consisting of 2 services.
     * It is a degenerate case of an HA3 quorum that is slightly easier to test
     * since the 3rd service is not present.
     */

    /**
     * 2 services start, quorum meets. Test verifies that the KB is created,
     * that we can read on the KB on both the leader and the follower, that the
     * Journals have the same digest, that there is one commit point, that the
     * HALog files were created for the first commit point (the one where we
     * created the KB), and that the HALogs have the same digest.
     */
    public void testStartAB() throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        
        final long token = awaitMetQuorum();

        // Service is met in role around a quorum.
        assertEquals(token,
                serverA.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS));

        // Service is met in role around a quorum.
        assertEquals(token,
                serverB.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS));

        // Verify can access the REST API "status" page.
        //doNSSStatusRequest(serverA);
        //doNSSStatusRequest(serverB);
        
        // Check RMI API.
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);
        
        
        // So. what is the difference - at the httpClinet level - between getNSSHAStatus and hetNSSHAStatusAlt?
        assertEquals(getNSSHAStatusAlt(serverB), getNSSHAStatus(serverB));
        assertEquals(getNSSHAStatusAlt(serverA), getNSSHAStatus(serverA));

        // Verify that service self-reports role via the REST API.
        assertEquals(HAStatusEnum.Follower, getNSSHAStatusAlt(serverB));

        // Verify that service self-reports role via the REST API.
        assertEquals(HAStatusEnum.Leader, getNSSHAStatusAlt(serverA));

        /*
         * Verify we can read on the KB on both nodes.
         * 
         * Note: It is important to test the reads for the first commit on both
         * the leader and the follower.
         */
		for (HAGlue service : new HAGlue[] { serverA, serverB }) {

			final JettyRemoteRepositoryManager repo = getRemoteRepository(service);
			try {
				// Should be empty.
				assertEquals(
						0L,
						countResults(repo.prepareTupleQuery(
								"SELECT * {?a ?b ?c} LIMIT 10").evaluate()));
			} finally {
				repo.close();
			}

		}
        
        // Verify binary equality on the journal files.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        /*
         * Verify ONE (1) commit point.
         */
        final IRootBlockView rootBlock1 = serverA.getRootBlock(
                new HARootBlockRequest(null/* storeUUID */)).getRootBlock();

        final long lastCommitCounter1 = rootBlock1.getCommitCounter();
        
        assertEquals(1L, lastCommitCounter1);

        /*
         * Verify that HALog files were generated and are available for commit
         * point ONE (1).
         */
        assertHALogDigestsEquals(1L/* firstCommitCounter */,
                lastCommitCounter1, new HAGlue[] { serverA, serverB });

        // Verify can not write on follower.
        assertWriteRejected(serverB);
        
    }

    /**
     * 2 services start, quorum meets then we restart the follower and verify
     * that the quorum meets again.
     */
    public void testStartAB_RestartFollower() throws Exception {
        
        HAGlue serverA = startA();
        HAGlue serverB = startB();
        
        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        doNSSStatusRequest(serverA);
        doNSSStatusRequest(serverB);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        // Verify binary equality on the journal files.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        /*
         * Restart the follower. Verify quorum meets again and that we can read
         * on all services.
         */
        {

            final HAGlue leader = quorum.getClient().getLeader(token1);

            final UUID leaderId1 = leader.getServiceId();

            if (leaderId1.equals(serverA.getServiceId())) {

                serverB = restartB();

            } else if (leaderId1.equals(serverB.getServiceId())) {

                serverA = restartA();

            } else {

                throw new AssertionError("leader=" + leader + ", serverA="
                        + serverA + ", serverB=" + serverB);

            }

            /*
             * Wait for the quorum to break and then meet again.
             */
            final long token2 = awaitNextQuorumMeet(token1);

            /*
             * Bouncing the connection broke the quorum, so verify that the
             * quorum token was advanced.
             */
            assertEquals(token1 + 1, token2);

            /*
             * Note: The services will recast their votes. The pipeline order
             * depends on whether the leader restarts more quickly than the
             * latency required for the follower to notice that the leader is
             * dead (this depends on the negotiated zookeeper session timeout).
             */
//            // The leader should not have changed.
//            final HAGlue leader2 = quorum.getClient().getLeader(token2);
//
//            final UUID leaderId2 = leader2.getServiceId();
//
//            if (!leaderId1.equals(leaderId2)) {
//
//                fail("Expected leaderId=" + leaderId1 + ", but was "
//                        + leaderId2);
//
//            }
            
            /*
             * Verify that the votes were recast for the then current
             * lastCommitTime.
             */
            assertVotesRecast(token2, 0L/* oldConsensusVote */);

            /*
             * Verify we can read on the KB on both nodes.
             * 
             * Note: It is important to test the reads for the first commit on
             * both the leader and the follower.
             */
			for (HAGlue service : new HAGlue[] { serverA, serverB }) {

				awaitNSSAndHAReady(service);

				final JettyRemoteRepositoryManager repo = getRemoteRepository(service);
				try {
					// Should be empty.
					assertEquals(
							0L,
							countResults(repo.prepareTupleQuery(
									"SELECT * {?a ?b ?c} LIMIT 10").evaluate()));
				} finally {
					repo.close();
				}

			}

        }
        
    }
    
    /**
     * 2 services start, quorum meets then we restart the leader and verify that
     * the quorum meets again.
     */
    public void testStartAB_RestartLeader() throws Exception {

        HAGlue serverA = startA();
        HAGlue serverB = startB();

        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        doNSSStatusRequest(serverA);
        doNSSStatusRequest(serverB);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        // Verify binary equality on the journal files.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        /*
         * Restart the leader. Verify that the service that was the follower is
         * now the leader. Verify that the quorum meets.
         */
        {
            
            final HAGlue leader = quorum.getClient().getLeader(token1);
        
            final UUID leaderId1 = leader.getServiceId();

            if (leaderId1.equals(serverA.getServiceId())) {

                serverA = restartA();

            } else if (leaderId1.equals(serverB.getServiceId())) {

                serverB = restartB();

            } else {

                throw new AssertionError("leader=" + leader + ", serverA="
                        + serverA + ", serverB=" + serverB);

            }

            /*
             * Wait for the quorum to break and then meet again.
             */
            final long token2 = awaitNextQuorumMeet(token1);

            /*
             * Bouncing the connection broke the quorum, so verify that the
             * quorum token was advanced.
             */
            assertEquals(token1 + 1, token2);

            /*
             * Note: The services will recast their votes. The pipeline order
             * depends on whether the leader restarts more quickly than the
             * latency required for the follower to notice that the leader is
             * dead (this depends on the negotiated zookeeper session timeout).
             */
//            // The leader should have changed.
//            final HAGlue leader2 = quorum.getClient().getLeader(token2);
//
//            final UUID leaderId2 = leader2.getServiceId();
//
//            if (leaderId1.equals(leaderId2)) {
//                /*
//                 * This fail message is not useful.
//                 * leaderId1.equals(leaderId2). it should report what the leader
//                 * *should* have been, but reports two identical values instead.
//                 */
//                fail("Expected leaderId=" + leaderId1 + ", but was "
//                        + leaderId2);
//
//            }

            /*
             * Verify that the votes were recast for the then current
             * lastCommitTime.
             */
            assertVotesRecast(token2, 0L/* oldConsensusVote */);
   
            /*
             * Verify we can read on the KB on both nodes.
             * 
             * Note: It is important to test the reads for the first commit on
             * both the leader and the follower.
             */
			for (HAGlue service : new HAGlue[] { serverA, serverB }) {

				awaitNSSAndHAReady(service);

				final JettyRemoteRepositoryManager repo = getRemoteRepository(service);
				try {
					// Should be empty.
					assertEquals(
							0L,
							countResults(repo.prepareTupleQuery(
									"SELECT * {?a ?b ?c} LIMIT 10").evaluate()));
				} finally {
					repo.close();
				}

			}

        }
        
    }

//    /**
//     * TODO 2 services start. Quorum meets. Write enough data to force a file
//     * extension then abort(). Restart services. The quorum should meet. Write
//     * enough data to force a file extension and then commit. The services
//     * should not report a problem with the file extends. [verify that the files
//     * were extended.]
//     * 
//     * TODO We need to send enough data to extend the file and enough we need a
//     * means to interrupt the data load. I.e., open a stream and start sending
//     * data. Maybe the INSERT DATA WITH POST using a streaming post? We would
//     * have one task to send the data and another to monitor the file extent
//     * (via RMI). When the file extension was observed we would then interrupt
//     * the task sending the data, which should lead to the abort() we want to
//     * observe.
//     * 
//     * @throws Exception
//     */
//    public void testStartAB_writeExtendAbort_writeExtendCommit()
//            throws Exception {
//     
////        HAGlue serverA = startA();
////        HAGlue serverB = startB();
////
////        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
////                TimeUnit.MILLISECONDS);
////
////        doNSSStatusRequest(serverA);
////        doNSSStatusRequest(serverB);
////
////        // Wait until KB exists.
////        awaitKBExists(serverA);
////
////        // Verify binary equality on the journal files.
////        assertDigestsEquals(new HAGlue[] { serverA, serverB });
////
////        // TODO Get the file length.
////        final long extent1 = 0L;
////        
////        /*
////         * LOAD data on leader, but cancel the operation once the file has
////         * been extended. 
////         */
////        {
////            
////            final HAGlue leader = quorum.getClient().getLeader(token1);            
////
////            final StringBuilder sb = new StringBuilder();
////            sb.append("DROP ALL;\n");
////            sb.append("LOAD <file:/Users/bryan/Documents/workspace/BIGDATA_RELEASE_1_2_0/data-2.nq.gz>;\n");
////            
////            final String updateStr = sb.toString();
////            
////            // Verify quorum is still valid.
////            quorum.assertQuorum(token1);
////
////            getRemoteRepository(leader).prepareUpdate(updateStr).evaluate();
////            
////            // TODO Get the file length.
////            final long extent2 = 0L;
////
////            // Verify file was extended.
////            assertTrue(extent2 > extent1);
////
////        }
//
//        fail("write test");
//        
//    }
//
//    /**
//    * TODO 2 services start. Quorum meets. Write data onto leader. WHILE
//    * WRITING, force follower to leave the qourum (e.g., bounce the zk client
//    * on the follower) and then verify that an abort was performed. Further, on
//    * the follower this needs to be a low level abort (_abort()). Also, since
//    * there are only 2 services the quorum will break and the leader should
//    * also do a low level _abort(). Quorum should meet after both services do
//    * the _abort() since they should both vote the same lastCommitTime.
//    */
//    public void testStartAB_followerLeaveDuringWrite() {
//        
//        fail("write test");
//        
//    }
    
}
