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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;

import org.apache.zookeeper.ZooKeeper;

import com.bigdata.ha.HACommitGlue;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.msg.IHA2PhaseCommitMessage;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.ha.msg.IHANotifyReleaseTimeRequest;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.journal.jini.ha.HAJournalTest.SpuriousTestException;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.util.ClocksNotSynchronizedException;
import com.bigdata.util.InnerCause;

/**
 * Unit test of the ability to override the {@link HAJournal} implementation
 * class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHAJournalServerOverride extends AbstractHA3JournalServerTestCase {

    public TestHAJournalServerOverride() {
    }

    public TestHAJournalServerOverride(final String name) {
        super(name);
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
        
        return new String[]{
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
//                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\""
        };
        
    }
    
    /**
     * One service starts, quorum does not meet (replication factor is 3). This
     * also serves to verify the <code>HAJournal-A.config</code> file.
     */
    public void testStartA() throws Exception {
        
        final HAGlue serverA = startA();
        
        try {

            quorum.awaitQuorum(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            
            fail("Not expecting quorum meet");

        } catch (TimeoutException ex) {
        
            // ignore.
            
        }

        // Service is not met in any role around a quorum.
        try {
            serverA.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            // Ignore expected exception.
        }

        // Verify can access the REST API "status" page.
        doNSSStatusRequest(serverA);

        // Verify self-reports as NotReady.
        awaitHAStatus(serverA, HAStatusEnum.NotReady);
        
        // Verify that service self-reports as NotReady via the REST API.
        assertEquals(HAStatusEnum.NotReady, getNSSHAStatus(serverA));

        // Verify can not read on service.
        assertReadRejected(serverA);

        // Verify can not write on service.
        assertWriteRejected(serverA);

        assertTrue(getHAJournalFileA().exists());
        assertTrue(getHALogDirA().exists());
        assertTrue(getSnapshotDirA().exists());
        
        // Verify that we can invoke extension methods on the service.
        ((HAGlueTest)serverA).helloWorld();

        // Get the serviceId.
        final UUID serviceId = serverA.getServiceId();

        // Setup to fail the next invocation.
        ((HAGlueTest) serverA).failNext("getServiceId", new Class[] {},
                0/* nwait */, 1/* nfail */);

        // Verify that the next invocation fails.
        try {
            serverA.getServiceId();
            fail("Expecting: " + SpuriousTestException.class);
        } catch (RuntimeException t) {
            assertTrue(InnerCause.isInnerCause(t, SpuriousTestException.class));
        }

        // Verify 2nd try succeeds.
        assertEquals(serviceId, serverA.getServiceId());

        // Setup to fail the next invocation.
        ((HAGlueTest) serverA).failNext("getServiceId", new Class[] {},
                0/* nwait */, 1/* nfail */);

        // Clear registered method failure.
        ((HAGlueTest) serverA).clearFail("getServiceId", new Class[] {});

        // Verify RMI succeeds.
        assertEquals(serviceId, serverA.getServiceId());

//        serverA.enterErrorState().get();
//        
//        Thread.sleep(10000/*ms*/);
        
    }

    /**
     * A user level transaction abort must not cause a service leave or quorum
     * break. It should simply discard the buffered write set for that
     * transactions.
     * 
     * TODO Currently, there is a single unisolated connection commit protocol.
     * When we add concurrent unisolated writers, the user level transaction
     * abort will just discard the buffered writes for a specific
     * {@link AbstractTask}.
     * 
     * @throws Exception
     */
    public void testStartABC_userLevelAbortDoesNotCauseQuorumBreak()
            throws Exception {

        fail("write test");

    }

    /**
     * This test forces clock skew on one of the followers causing it to
     * encounter an error in its GatherTask. This models the problem that was
     * causing a deadlock in an HA3 cluster with BSBM UPDATE running on the
     * leader (EXPLORE was running on the follower, but analysis of the root
     * cause shows that this was not required to trip the deadlock). The
     * deadlock was caused by clock skew resulting in an exception and either
     * {@link IHANotifyReleaseTimeRequest} message that was <code>null</code>
     * and thus could not be processed or a failure to send that message back to
     * the leader.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/677" > HA
     *      deadlock under UPDATE + QUERY </a>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/673" > DGC
     *      in release time consensus protocol causes native thread leak in
     *      HAJournalServer at each commit </a>
     */
    public void testStartABC_releaseTimeConsensusProtocol_clockSkew()
            throws Exception {

        // Enforce the join order.
        final ABC startup = new ABC(true /*sequential*/);

        final long token = awaitFullyMetQuorum();

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);

        /*
         * Setup B with a significant clock skew to force an error during the
         * GatherTask.
         */
        ((HAGlueTest) startup.serverB).setNextTimestamp(10L);
        
        try {

            // Simple transaction.
            simpleTransaction();

        } catch (Throwable t) {
            /*
             * TODO This test is currently failing because the consensus
             * releaseTime protocol will fail if one of the joined services
             * reports an error. The protocol should be robust to an error and
             * move forward if a consensus can be formed. If a consensus can not
             * be formed (due to some curable error), then any queries running
             * on that service should break (force a service leave). Else, if
             * the remaining services were to advance the release time since
             * otherwise the service could not get through another releaseTime
             * consensus protocol exchange successfully if it is reading on a
             * commit point that has been released by the other services.
             */
            if (!t.getMessage().contains(
                    ClocksNotSynchronizedException.class.getName())) {
                /*
                 * Wrong inner cause.
                 * 
                 * Note: The stack trace of the local exception does not include
                 * the remote stack trace. The cause is formatted into the HTTP
                 * response body.
                 */
                fail("Expecting " + ClocksNotSynchronizedException.class, t);
            }

        } finally {

            // Restore the clock.
            ((HAGlueTest) startup.serverB).setNextTimestamp(-1L);

        }

        /*
         * The quorum token was not advanced. The 2-phase commit was rejected by
         * all services and a 2-phase abort was performed. All services are at
         * the same commit point.
         */

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);

        // The quorum token is unchanged.
        assertEquals(token, quorum.token());

        // The join order is unchanged.
        awaitJoined(new HAGlue[] { startup.serverA, startup.serverB,
                startup.serverC });

        /*
         * New transactions are still accepted.
         */
        ((HAGlueTest)startup.serverA).log("2nd transaction");
        ((HAGlueTest)startup.serverB).log("2nd transaction");
        ((HAGlueTest)startup.serverC).log("2nd transaction");
        simpleTransaction();

        // Should be one commit point.
        awaitCommitCounter(2L, startup.serverA, startup.serverB,
                startup.serverC);

        // The quorum token is unchanged.
        assertEquals(token, quorum.token());

    }

    /**
     * Three services are started in [A,B,C] order. B is setup for
     * {@link HACommitGlue#prepare2Phase(IHA2PhasePrepareMessage)} to throw an
     * exeption. A simple transaction is performed. We verify that the
     * transaction completes successfully, that the quorum token is unchanged,
     * and that [A,C] both participated in the commit. We also verify that B is
     * moved to the end of the pipeline (by doing a serviceLeave and then
     * re-entering the pipeline) and that it resyncs with the met quorum and
     * finally re-joins with the met quorum. The quorum should not break across
     * this test.
     * 
     * TODO Test timeout scenarios (RMI is non-responsive) where we cancel the
     * PREPARE once the timeout is elapsed.
     */
    public void testStartABC_prepare2Phase_B_votes_NO()
            throws Exception {

        // Enforce the join order.
        final ABC startup = new ABC(true /*sequential*/);

        final long token = awaitFullyMetQuorum();

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);

        // Setup B to vote "NO" on the next PREPARE request.
        ((HAGlueTest) startup.serverB).voteNo();
        ((HAGlueTest) startup.serverA).log("B will vote NO.");
        ((HAGlueTest) startup.serverB).log("B will vote NO.");
        ((HAGlueTest) startup.serverC).log("B will vote NO.");
        
        // Simple transaction.
        simpleTransaction();
        
        ((HAGlueTest) startup.serverA).log("Transaction done");
        ((HAGlueTest) startup.serverB).log("Transaction done");
        ((HAGlueTest) startup.serverC).log("Transaction done");

        // Verify quorum is unchanged.
        assertEquals(token, quorum.token());
        
        // Should be two commit points on {A,C].
        awaitCommitCounter(2L, startup.serverA, startup.serverC);
        
        ((HAGlueTest) startup.serverA).log("Commit Counter #2");
        ((HAGlueTest) startup.serverB).log("Commit Counter #2");
        ((HAGlueTest) startup.serverC).log("Commit Counter #2");
        /*
         * B should go into an ERROR state and then into SeekConsensus and from
         * there to RESYNC and finally back to RunMet. We can not reliably
         * observe the intervening states. So what we really need to do is watch
         * for B to move to the end of the pipeline and catch up to the same
         * commit point.
         */

        /*
         * The pipeline should be reordered. B will do a service leave, then
         * enter seek consensus, and then re-enter the pipeline.
         */
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC,
                startup.serverB });

        final long token2 = awaitFullyMetQuorum();
        
        assertEquals(token, token2);

        /*
         * There should be two commit points on {A,C,B} (note that this assert
         * does not pay attention to the pipeline order).
         */
        awaitCommitCounter(2L, startup.serverA, startup.serverC,
                startup.serverB);

        // B should be a follower again.
        awaitHAStatus(startup.serverB, HAStatusEnum.Follower);

        // quorum token is unchanged.
        assertEquals(token, quorum.token());

    }
    
    /**
     * Three services are started in [A,B,C] order. B is setup for
     * {@link HACommitGlue#prepare2Phase(IHA2PhasePrepareMessage)} to vote "NO".
     * A simple transaction is performed. We verify that the transaction
     * completes successfully, that the quorum token is unchanged, and that
     * [A,C] both participated in the commit. We also verify that B is moved to
     * the end of the pipeline (by doing a serviceLeave and then re-entering the
     * pipeline) and that it resyncs with the met quorum and finally re-joins
     * with the met quorum. The quorum should not break across this test.
     */
    public void testStartABC_prepare2Phase_B_throws_exception()
            throws Exception {

        // Enforce the join order.
        final ABC startup = new ABC(true /*sequential*/);

        final long token = awaitFullyMetQuorum();

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);
        
        // Setup B to fail the next PREPARE request.
        ((HAGlueTest) startup.serverB)
                .failNext("prepare2Phase",
                        new Class[] { IHA2PhasePrepareMessage.class },
                        0/* nwait */, 1/* nfail */);

        // Simple transaction.
        simpleTransaction();
        
        // Verify quorum is unchanged.
        assertEquals(token, quorum.token());
        
        // Should be two commit points on {A,C].
        awaitCommitCounter(2L, startup.serverA, startup.serverC);
        
        /*
         * Note: Unlike the test above, if there is a problem making the RMI
         * call, then B will not go through its doRejectedCommit() handler and
         * will not enter the ERROR state directly. We need to have B notice
         * that it is no longer at the same commit point, e.g., by observing a
         * LIVE write cache message with an unexpected value for the
         * commitCounter (especially, GT its current expected value). That is
         * the indication that B needs to enter an error state. Until then it
         * does not know that there was an attempt to PREPARE since it did not
         * get the prepare2Phase() message.
         * 
         * - Modified HAJournalServer to enter the error state if we observe a
         * live write cache block for a commitCounter != the expected
         * commitCounter.
         * 
         * - Modified commit2Phase() to accept the #of services that are
         * participating in the commit. If it is not a full quorum, then we can
         * not purge the HA logs in commit2Phase() regardless of what the quorum
         * state looks like.
         * 
         * - Modified this test to do another transaction. B can not notice the
         * problem until there is another write cache flushed through the
         * pipeline.
         * 
         * - Modified this test to await B to move to the end of the pipeline,
         * resync, and rejoin.
         */
        
        // Should be two commit points on {A,C}.
        awaitCommitCounter(2L, startup.serverA, startup.serverC);

        // Should be ONE commit points on {B}.
        awaitCommitCounter(1L, startup.serverB);

        // A commit is necessary for B to notice that it did not prepare.
        simpleTransaction();
        
        /*
         * The pipeline should be reordered. B will do a service leave, then
         * enter seek consensus, and then re-enter the pipeline.
         */
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC,
                startup.serverB });

        /*
         * There should be three commit points on {A,C,B} (note that this assert
         * does not pay attention to the pipeline order).
         */
        awaitCommitCounter(3L, startup.serverA, startup.serverC,
                startup.serverB);

        // B should be a follower again.
        awaitHAStatus(startup.serverB, HAStatusEnum.Follower);

        // quorum token is unchanged.
        assertEquals(token, quorum.token());

    }

    /**
     * Three services are started in [A,B,C] order. B is setup for
     * {@link HACommitGlue#prepare2Phase(IHA2PhasePrepareMessage)} to throw an
     * exeption. A simple transaction is performed. We verify that the
     * transaction completes successfully, that the quorum token is unchanged,
     * and that [A,C] both participated in the commit. We also verify that B is
     * moved to the end of the pipeline (by doing a serviceLeave and then
     * re-entering the pipeline) and that it resyncs with the met quorum and
     * finally re-joins with the met quorum. The quorum should not break across
     * this test.
     * 
     * FIXME Variant where the commit2Phase fails. Note: The COMMIT message is
     * design to do as little work as possible. In practice, this requires an
     * RMI to the followers, each follower must not encounter an error when it
     * validates the COMMIT message, and each follower must put down its new
     * root block (from the prepare message) and then sync the disk. Finally,
     * the RMI response must be returned.
     * <p>
     * Under what conditions can a COMMIT message fail where we can still
     * recover? Single node failure? Leader failure? (QuorumCommitImpl currently
     * fails the commit if there is a single failure, even though the quourm
     * might have a consensus around the new commit point.)
     * 
     * TODO Consider leader failure scenarios in this test suite, not just
     * scenarios where B fails. We MUST also cover failures of C (the 2nd
     * follower). We should also cover scenarios where the quorum is barely met
     * and a single failure causes a rejected commit (local decision) or 2-phase
     * abort (joined services in joint agreement).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/760" >
     *      Review commit2Phase semantics when a follower fails </a>
     */
    public void testStartABC_commit2Phase_B_fails()
            throws Exception {

        // Enforce the join order.
        final ABC startup = new ABC(true /*sequential*/);

        //HAJournalTest.dumpThreads();
        
        final long token = awaitFullyMetQuorum();

        // Should be one commit point.
        awaitCommitCounter(1L, startup.serverA, startup.serverB,
                startup.serverC);

        /*
         * Setup B to fail the "COMMIT" message (specifically, it will throw
         * back an exception rather than executing the commit.
         * 
         * FIXME We need to cause B to actually fail the commit such that it
         * enters the ERROR state. This is only causing the RMI to be rejected
         * so B is not being failed out of the pipeline. Thus, B will remain
         * joined with the met quorum (but at the wrong commit point) until we
         * send down another replicated write. At that point B will notice that
         * it is out of whack and enter the ERROR state.
         */
        ((HAGlueTest) startup.serverB)
                .failNext("commit2Phase",
                        new Class[] { IHA2PhaseCommitMessage.class },
                        0/* nwait */, 1/* nfail */);

        /**
         * FIXME We need to resolve the correct behavior when B fails the commit
         * after having prepared. Two code paths are outlined below. The
         * implementation currently does an abort2Phase() when the
         * commit2Phase() observe an error for B. That causes the commit point
         * to NOT advance.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/760" >
         * Review commit2Phase semantics when a follower fails </a>
         */
        
        if(true) {

            // Simple transaction.
            simpleTransaction();

            // Verify quorum is unchanged.
            assertEquals(token, quorum.token());

            // Should be two commit points on {A,C].
            awaitCommitCounter(2L, startup.serverA, startup.serverC);

            // Just one commit point on B.
            awaitCommitCounter(1L, startup.serverB);

            // B is still a follower.
            awaitHAStatus(startup.serverB, HAStatusEnum.Follower);
            
            /*
             * B should go into an ERROR state and then into SeekConsensus and
             * from there to RESYNC and finally back to RunMet. We can not
             * reliably observe the intervening states. So what we really need
             * to do is watch for B to move to the end of the pipeline and catch
             * up to the same commit point.
             * 
             * FIXME This is forcing B into an error state to simulate what
             * would happen if B had encountered an error during the 2-phase
             * commit above.
             */
            ((HAGlueTest)startup.serverB).enterErrorState();

            /*
             * The pipeline should be reordered. B will do a service leave, then
             * enter seek consensus, and then re-enter the pipeline.
             */
            awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC,
                    startup.serverB });

            awaitFullyMetQuorum();
            
            /*
             * There should be two commit points on {A,C,B} (note that this
             * assert does not pay attention to the pipeline order).
             */
            awaitCommitCounter(2L, startup.serverA, startup.serverC,
                    startup.serverB);

            // B should be a follower again.
            awaitHAStatus(startup.serverB, HAStatusEnum.Follower);

            // quorum token is unchanged.
            assertEquals(token, quorum.token());

        } else {
            
            try {

                // Simple transaction.
                simpleTransaction();
                
                fail("Expecting failed transaction");
                
            } catch (Exception t) {
                
                if (!t.getMessage().contains(
                        SpuriousTestException.class.getName())) {
                    /*
                     * Wrong inner cause.
                     * 
                     * Note: The stack trace of the local exception does not
                     * include the remote stack trace. The cause is formatted
                     * into the HTTP response body.
                     */
                    fail("Expecting " + SpuriousTestException.class, t);
                }
                
            }

            // Verify quorum is unchanged.
            assertEquals(token, quorum.token());

            // Should be ONE commit point on {A,B, C].
            awaitCommitCounter(1L, startup.serverA, startup.serverB,
                    startup.serverC);

            fail("finish test under these assumptions");
            
        }

    }
    
    /**
     * 2 services start, quorum meets then we bounce the zookeeper connection
     * for the follower and verify that the quorum meets again.
     * <p>
     * Note: Bouncing the ZK client connection causes the reflected state
     * maintained by the {@link ZKQuorumImpl} to be out of sync with the state
     * in zookeeper. Not only can some events be lost, but none of the events
     * that correspond to the elimination of the ephemeral znodes for this
     * service will be observed. Handling this correctly requires special
     * consideration.
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/718" >
     *      HAJournalServer needs to handle ZK client connection loss </a>
     */
    public void testStartAB_BounceFollower() throws Exception {
        doBounceFollower();
    }
    
    public void _testStressStartAB_BounceFollower() throws Exception {
    	for (int test = 0; test < 5; test++) {
            try {
            	doBounceFollower();
            } catch (Throwable t) {
                fail("Run " + test, t);
            } finally {
                destroyAll();
            }
    	}
    }
    
    public void doBounceFollower() throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        
        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout, TimeUnit.MILLISECONDS);

        doNSSStatusRequest(serverA);
        doNSSStatusRequest(serverB);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        // Await [A] up and running as leader.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));

        // Await [B] up and running as follower.
        assertEquals(HAStatusEnum.Follower, awaitNSSAndHAReady(serverB));

        // Verify self-reporting by RMI in their respective roles.
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        
        // Verify binary equality on the journal files.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        if (log.isInfoEnabled()) {
            log.info("Zookeeper before quorum break:\n" + dumpZoo());
        }
        
        /*
         * Bounce the follower. Verify quorum meets again and that we can read
         * on all services.
         */
        {

            final HAGlue leader = quorum.getClient().getLeader(token1);

//            final UUID leaderId1 = leader.getServiceId();
            
            if (leader.equals(serverA)) {

                ((HAGlueTest) serverB).bounceZookeeperConnection().get();

            } else {

                ((HAGlueTest) serverA).bounceZookeeperConnection().get();

            }
            // Thread.sleep(100000); // sleep to allow thread dump for analysis
            // Okay, is the problem that the quorum doesn't break?
            // assertFalse(quorum.isQuorumMet());
            
            // Right so the Quorum is not met, but the follower deosn't seem to know it's broken
            
            // Wait for the quorum to break and then meet again.
            final long token2 = awaitNextQuorumMeet(token1);

            if (log.isInfoEnabled()) {
                log.info("Zookeeper after quorum meet:\n" + dumpZoo());
            }
            
            /*
             * Bouncing the connection broke the quorun, so verify that the
             * quorum token was advanced.
             */
            assertEquals(token1 + 1, token2);
            
            // The leader MAY have changed (since the quorum broke).
            final HAGlue leader2 = quorum.getClient().getLeader(token2);

            // Verify leader self-reports in new role.
            awaitHAStatus(leader2, HAStatusEnum.Leader);

//            final UUID leaderId2 = leader2.getServiceId();
//
//            assertFalse(leaderId1.equals(leaderId2));
            
            /*
             * Verify we can read on the KB on both nodes.
             * 
             * Note: It is important to test the reads for the first commit on
             * both the leader and the follower.
             */
            for (HAGlue service : new HAGlue[] { serverA, serverB }) {

                awaitNSSAndHAReady(service);

                final RemoteRepository repo = getRemoteRepository(service);

                // Should be empty.
                assertEquals(
                        0L,
                        countResults(repo.prepareTupleQuery(
                                "SELECT * {?a ?b ?c} LIMIT 10").evaluate()));

            }

        }
        
    }
    
    /**
     * 2 services start, quorum meets then we bounce the zookeeper connection
     * for the leader and verify that the quorum meets again.
     */
    public void testStartAB_BounceLeader() throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        doNSSStatusRequest(serverA);
        doNSSStatusRequest(serverB);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB);

        // Await [A] up and running as leader.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));

        // Await [B] up and running as follower.
        assertEquals(HAStatusEnum.Follower, awaitNSSAndHAReady(serverB));

        // Verify self-reports in role.
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);

        // Verify binary equality on the journal files.
        assertDigestsEquals(new HAGlue[] { serverA, serverB });

        if (log.isInfoEnabled()) {
            log.info("Zookeeper before quorum meet:\n" + dumpZoo());
        }

        /*
         * Bounce the leader. Verify that the service that was the follower is
         * now the leader. Verify that the quorum meets.
         */
        {
            
            final HAGlue leader = quorum.getClient().getLeader(token1);

            ((HAGlueTest) leader).bounceZookeeperConnection().get();

            // Wait for the quorum to break and then meet again.
            final long token2 = awaitNextQuorumMeet(token1);

            if (log.isInfoEnabled()) {
                log.info("Zookeeper after quorum meet:\n" + dumpZoo());
            }

            /*
             * Bouncing the connection broke the quorum, so verify that the
             * quorum token was advanced.
             */
            assertEquals(token1 + 1, token2);

            // The leader MAY have changed.
            final HAGlue leader2 = quorum.getClient().getLeader(token2);

//            final UUID leaderId2 = leader2.getServiceId();
//
//            assertFalse(leaderId1.equals(leaderId2));
            
            // Verify leader self-reports in new role.
            awaitHAStatus(leader2, HAStatusEnum.Leader);

            /*
             * Verify we can read on the KB on both nodes.
             * 
             * Note: It is important to test the reads for the first commit on
             * both the leader and the follower.
             */
            for (HAGlue service : new HAGlue[] { serverA, serverB }) {

                awaitNSSAndHAReady(service);
                
                final RemoteRepository repo = getRemoteRepository(service);

                // Should be empty.
                assertEquals(
                        0L,
                        countResults(repo.prepareTupleQuery(
                                "SELECT * {?a ?b ?c} LIMIT 10").evaluate()));

            }

        }
        
    }

//    /**
//     * 2 services start, quorum meets then we bounce the zookeeper connection
//     * for the leader and verify that the quorum meets again.
//     */
//    public void testStartAB_DropReopenLeader() throws Exception {
//        
//        final HAGlue serverA = startA();
//        final HAGlue serverB = startB();
//
//        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
//                TimeUnit.MILLISECONDS);
//
//        doNSSStatusRequest(serverA);
//        doNSSStatusRequest(serverB);
//
//        // Await initial commit point (KB create).
//        awaitCommitCounter(1L, serverA, serverB);
//
//        // Await [A] up and running as leader.
//        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));
//
//        // Await [B] up and running as follower.
//        assertEquals(HAStatusEnum.Follower, awaitNSSAndHAReady(serverB));
//
//        // Verify self-reports in role.
//        awaitHAStatus(serverA, HAStatusEnum.Leader);
//        awaitHAStatus(serverB, HAStatusEnum.Follower);
//
//        // Verify binary equality on the journal files.
//        assertDigestsEquals(new HAGlue[] { serverA, serverB });
//
//        if (log.isInfoEnabled()) {
//            log.info("Zookeeper before quorum meet:\n" + dumpZoo());
//        }
//
//        /*
//         * Bounce the leader. Verify that the service that was the follower is
//         * now the leader. Verify that the quorum meets.
//         */
//        {
//            
//            final HAGlue leader = quorum.getClient().getLeader(token1);
//
////            final UUID leaderId1 = leader.getServiceId();
//            
//            ((HAGlueTest)leader).dropZookeeperConnection().get();
//
//            // without explicit reopen, will it recover?
//            // ((HAGlueTest)leader).reopenZookeeperConnection().get();
//
//            // Wait for the quorum to break and then meet again.
//            final long token2 = awaitNextQuorumMeet(token1);
//
//            if (log.isInfoEnabled()) {
//                log.info("Zookeeper after quorum meet:\n" + dumpZoo());
//            }
//
//            /*
//             * Bouncing the connection broke the quorum, so verify that the
//             * quorum token was advanced.
//             */
//            assertEquals(token1 + 1, token2);
//
//            // The leader MAY have changed.
//            final HAGlue leader2 = quorum.getClient().getLeader(token2);
//
////            final UUID leaderId2 = leader2.getServiceId();
////
////            assertFalse(leaderId1.equals(leaderId2));
//            
//            // Verify leader self-reports in new role.
//            awaitHAStatus(leader2, HAStatusEnum.Leader);
//            
//            /*
//             * Verify we can read on the KB on both nodes.
//             * 
//             * Note: It is important to test the reads for the first commit on
//             * both the leader and the follower.
//             */
//            for (HAGlue service : new HAGlue[] { serverA, serverB }) {
//
//                final RemoteRepository repo = getRemoteRepository(service);
//
//                // Should be empty.
//                assertEquals(
//                        0L,
//                        countResults(repo.prepareTupleQuery(
//                                "SELECT * {?a ?b ?c} LIMIT 10").evaluate()));
//
//            }
//
//        }
//        
//    }

    /**
     * Verify ability to stop and restart the zookeeper process under test
     * control.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void testStopStartZookeeper() throws InterruptedException,
            IOException {

        assertZookeeperRunning();
        stopZookeeper();
        try {
            Thread.sleep(3000);
            assertZookeeperNotRunning();
        } finally {
            startZookeeper();
            assertZookeeperRunning();
        }
    }

    /**
     * Verify that the {@link HAJournalServer} dies if we attempt to start it
     * and the zookeeper server process is not running.
     * 
     * @throws Exception
     */
    public void test_stopZookeeper_startA_fails()
            throws Exception {
        
        stopZookeeper();
        
        try {
            startA();
        } catch (Throwable t) {
            if (!InnerCause.isInnerCause(t, ServiceStartException.class)) {
                fail("Expecting " + ServiceStartException.class, t);
            }
        }
        
    }
    
    /**
     * Zookeeper is not required except when there are quorum state changes.
     * Futher, the services will not notice that zookeeper is dead and will
     * continue to process transactions successfully while zookeeper is dead.
     * <p>
     * Note: because this test stops and starts the zookeeper server process,
     * the {@link ZooKeeper} client objects REMAIN VALID. They will not observe
     * any state changes while the zookeeper server process is shutdown, but
     * they will re-connect when the zookeeper server process comes back up and
     * will be usable again once they reconnect.
     * 
     * TODO We should be doing NSS status requests in these zk server stop /
     * start tests to verify that we do not have any deadlocks or errors on the
     * /status page.
     */
    public void testAB_stopZookeeper_commit_startZookeeper_commit()
            throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        assertEquals(0L, token1);
        
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);
        awaitCommitCounter(1L, serverA, serverB); // wait for the initial KB create.
        
        // verify that we know which service is the leader.
        awaitHAStatus(serverA, HAStatusEnum.Leader); // must be the leader.
        awaitHAStatus(serverB, HAStatusEnum.Follower); // must be a follower.
        
        /*
         * Note: Once we stop the zookeeper service, the test harness is not
         * able to observe zookeeper change events until after zookeeper has
         * been restarted. However, the ZooKeeper client connection should
         * remain valid after zookeeper restarts.
         */
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        stopZookeeper();
        // transaction should succeed.
        simpleTransaction_noQuorumCheck(serverA);

        // restart zookeeper.
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        startZookeeper();
        simpleTransaction_noQuorumCheck(serverA);

        /*
         * The quorum has remained met. No events are propagated while zookeeper
         * is shutdown. Further, zookeepers only sense of "time" is its own
         * ticks. If the ensemble is entirely shutdown, then "time" does not
         * advance for zookeeper.
         * 
         * Since the zookeeper server has been restarted, the ZkQuorumImpl for
         * the test harness (and the HAJournalServers) will now notice quorum
         * state changes again.
         */
        final long token2 = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        assertEquals(token1, token2);

        // transaction should succeed.
        simpleTransaction();

        /*
         * Now verify that C starts, resyncs, and that the quorum fully meets.
         * These behaviors depend on zookeeper.
         */
        final HAGlue serverC = startC();

        final long token3 = awaitFullyMetQuorum();
        assertEquals(token1, token3);

        // A is still the leader.  B and C are followers.
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        
        // transaction should succeed.
        simpleTransaction();

    }
    
    /**
     * Test variant where we force a service into an error state when zookeeper
     * is shutdown. This will cause all commits to fail since the service can
     * not update the quorum state and the other services can not notice that
     * the service is not part of the quorum. Eventually the leader will be in
     * an error state, and maybe the other services as well. Once we restart
     * zookeeper, the quorum should meet again and then fully meet.
     */
    public void testABC_stopZookeeper_failC_startZookeeper_quorumMeetsAgain()
            throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        assertEquals(0L, token1);
        
        // wait for the initial KB create.
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);
        awaitNSSAndHAReady(serverC);
        awaitCommitCounter(1L, serverA, serverB, serverC);
        
        // verify that we know which service is the leader.
        awaitHAStatus(serverA, HAStatusEnum.Leader); // must be the leader.
        awaitHAStatus(serverB, HAStatusEnum.Follower); // must be a follower.
        awaitHAStatus(serverC, HAStatusEnum.Follower); // must be a follower.
        
        /*
         * Note: Once we stop the zookeeper service, the test harness is not
         * able to observe zookeeper change events until after zookeeper has
         * been restarted. However, the ZooKeeper client connection should
         * remain valid after zookeeper restarts.
         */
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        ((HAGlueTest)serverC).log("MARK");
        stopZookeeper();
        
        // transaction should succeed.
        simpleTransaction_noQuorumCheck(serverA);
        awaitCommitCounter(2L, serverA, serverB, serverC);

        /*
         * Force C into an Error state.
         * 
         * Note: C will remain in the met quorum because it is unable to act on
         * the quorum. 
         */
        ((HAGlueTest) serverC).enterErrorState();
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.NotReady);

        /*
         * Transaction will NOT fail. C will just drop the replicated writes. C
         * will vote NO on the prepare (actually, it fails on the GATHER). C
         * will not participate in the commit.  But A and B will both go through
         * the commit.
         */
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        ((HAGlueTest)serverC).log("MARK");
        simpleTransaction_noQuorumCheck(serverA);
        awaitCommitCounter(3L, serverA, serverB);
        awaitCommitCounter(2L, serverC); // C did NOT commit.

        // restart zookeeper.
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        ((HAGlueTest)serverC).log("MARK");
        startZookeeper();
        
        /*
         * C was in the Error state while zookeeper was dead. The quorum state
         * does not update until zookeeper is restarted. Once we restart
         * zookeeper, it should eventually go into Resync and then join and be
         * at the same commit point.
         */
        
        // wait until C becomes a follower again.
        awaitHAStatus(20, TimeUnit.SECONDS, serverC, HAStatusEnum.Follower);

        // quorum should become fully met on the original token.
        final long token2 = awaitFullyMetQuorum();
        assertEquals(token1, token2);
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        awaitCommitCounter(3L, serverA, serverB, serverC);

    }
    
    /**
     * Variant test where B is forced into the error state after we have
     * shutdown zookeeper. C should continue to replicate the data to C and A+C
     * should be able to go through commits.
     */
    public void testABC_stopZookeeper_failB_startZookeeper_quorumMeetsAgain()
            throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        assertEquals(0L, token1);
        
        // wait for the initial KB create.
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);
        awaitNSSAndHAReady(serverC);
        awaitCommitCounter(1L, serverA, serverB, serverC);
        awaitPipeline(new HAGlue[] { serverA, serverB, serverC });
        
        // verify that we know which service is the leader.
        awaitHAStatus(serverA, HAStatusEnum.Leader); // must be the leader.
        awaitHAStatus(serverB, HAStatusEnum.Follower); // must be a follower.
        awaitHAStatus(serverC, HAStatusEnum.Follower); // must be a follower.
        
        /*
         * Note: Once we stop the zookeeper service, the test harness is not
         * able to observe zookeeper change events until after zookeeper has
         * been restarted. However, the ZooKeeper client connection should
         * remain valid after zookeeper restarts.
         */
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        ((HAGlueTest)serverC).log("MARK");
        stopZookeeper();
        
        // transaction should succeed.
        simpleTransaction_noQuorumCheck(serverA);
        awaitCommitCounter(2L, serverA, serverB, serverC);

        /*
         * Force B into an Error state.
         * 
         * Note: B will remain in the met quorum because it is unable to act on
         * the quorum. 
         */
        ((HAGlueTest) serverB).enterErrorState();
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.NotReady);
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        
        /*
         * Transaction will NOT fail. B will just drop the replicated writes. B
         * will vote NO on the prepare (actually, it fails on the GATHER). B
         * will not participate in the commit.  But A and C will both go through
         * the commit.
         */
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        ((HAGlueTest)serverC).log("MARK");
        simpleTransaction_noQuorumCheck(serverA);
        awaitCommitCounter(3L, serverA, serverC);
        awaitCommitCounter(2L, serverB); // B did NOT commit.

        // restart zookeeper.
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        ((HAGlueTest)serverC).log("MARK");
        startZookeeper();
        
        /*
         * B was in the Error state while zookeeper was dead. The quorum state
         * does not update until zookeeper is restarted. Once we restart
         * zookeeper, it should eventually go into Resync and then join and be
         * at the same commit point.
         * 
         * Note: B will be moved to the end of the pipeline when this happens.
         */
        
        // wait until B becomes a follower again.
        awaitHAStatus(20, TimeUnit.SECONDS, serverB, HAStatusEnum.Follower);

        // pipeline is changed.
        awaitPipeline(new HAGlue[] { serverA, serverC, serverB });

        // quorum should become fully met on the original token.
        final long token2 = awaitFullyMetQuorum();
        assertEquals(token1, token2);
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        awaitCommitCounter(3L, serverA, serverB, serverC);

    }
    
    /**
     * Variant where we start A+B+C, stop zookeeper, then force A into an error
     * state. The quorum will not accept writes (leader is in an error state),
     * but B and C will still accept queries. When we restart zookeeper, a new
     * quorum will form around (A,B,C). The new leader (which could be any
     * service) will accept new transactions. There could be a race here between
     * a fully met quorum and new transactions, but eventually all services will
     * be joined with the met quorum and at the same commit point.
     * 
     * FIXME This test fails.  Figure out why.
     */
    public void testABC_stopZookeeper_failA_startZookeeper_quorumMeetsAgainOnNewToken()
            throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        assertEquals(0L, token1);
        
        // wait for the initial KB create.
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);
        awaitNSSAndHAReady(serverC);
        awaitCommitCounter(1L, serverA, serverB, serverC);
        awaitPipeline(new HAGlue[] { serverA, serverB, serverC });
        
        // verify that we know which service is the leader.
        awaitHAStatus(serverA, HAStatusEnum.Leader); // must be the leader.
        awaitHAStatus(serverB, HAStatusEnum.Follower); // must be a follower.
        awaitHAStatus(serverC, HAStatusEnum.Follower); // must be a follower.
        
        /*
         * Note: Once we stop the zookeeper service, the test harness is not
         * able to observe zookeeper change events until after zookeeper has
         * been restarted. However, the ZooKeeper client connection should
         * remain valid after zookeeper restarts.
         */
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        ((HAGlueTest)serverC).log("MARK");
        stopZookeeper();
        
        // transaction should succeed.
        simpleTransaction_noQuorumCheck(serverA);
        awaitCommitCounter(2L, serverA, serverB, serverC);

        /*
         * Force A into an Error state.
         * 
         * Note: A will remain in the met quorum because it is unable to act on
         * the quorum. However, it will no longer be a "Leader". The other
         * services will remain "Followers" since zookeeper is not running and
         * watcher events are not being triggered.
         */
        ((HAGlueTest) serverA).enterErrorState();
        awaitHAStatus(serverA, HAStatusEnum.NotReady);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        
        /*
         * Transaction will fail.
         */
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        ((HAGlueTest)serverC).log("MARK");
        try {
            simpleTransaction_noQuorumCheck(serverA);
            fail("Expecting transaction to fail");
        } catch (Exception ex) {
            log.warn("Ignoring expected exception: " + ex, ex);
        }
        
        /*
         * Can query B/C. 
         */
        for (HAGlue service : new HAGlue[] { serverB, serverC }) {

            final RemoteRepository repo = getRemoteRepository(service);

            assertEquals(
                    1L,
                    countResults(repo.prepareTupleQuery(
                            "SELECT (count(*) as ?count) {?a ?b ?c}")
                            .evaluate()));
            
        }
        
        // restart zookeeper.
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        ((HAGlueTest)serverC).log("MARK");
        startZookeeper();
        
        /*
         * A was in the Error state while zookeeper was dead. The quorum state
         * does not update until zookeeper is restarted. Once we restart
         * zookeeper, it should eventually go into Resync and then join and be
         * at the same commit point.
         * 
         * Note: Since A was the leader, the quorum will break and then reform.
         * The services could be in any order in the new quorum.
         */

        // wait for the next quorum meet.
        awaitNextQuorumMeet(token1);

        // wait until the quorum is fully met.
        final long token2 = awaitFullyMetQuorum(6);
        assertEquals(token1 + 1, token2);

        // Still 2 commit points.
        awaitCommitCounter(2L, serverA, serverB, serverC);

        // New transactions now succeed.
        simpleTransaction();

        awaitCommitCounter(3L, serverA, serverB, serverC);

    }
    
    /**
     * Variant test where we start A+B, stop zookeeper, then force B into an
     * error state. B will refuse to commit, which will cause A to fail the
     * transaction. This should push A into an error state. When we restart
     * zookeeper the quorum should break and then reform (in some arbitrary)
     * order. Services should be at the last recorded commit point. New
     * transactions should be accepted.  FIXME Test fails. Figure out why.
     */
    public void testAB_stopZookeeper_failB_startZookeeper_quorumBreaksThenMeets()
            throws Exception {
        
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();

        final long token1 = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        assertEquals(0L, token1);
        
        // wait for the initial KB create.
        awaitNSSAndHAReady(serverA);
        awaitNSSAndHAReady(serverB);
        awaitCommitCounter(1L, serverA, serverB);
        awaitPipeline(new HAGlue[] { serverA, serverB });

        // verify that we know which service is the leader.
        awaitHAStatus(serverA, HAStatusEnum.Leader); // must be the leader.
        awaitHAStatus(serverB, HAStatusEnum.Follower); // must be a follower.
        
        /*
         * Note: Once we stop the zookeeper service, the test harness is not
         * able to observe zookeeper change events until after zookeeper has
         * been restarted. However, the ZooKeeper client connection should
         * remain valid after zookeeper restarts.
         */
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        stopZookeeper();
        
        // transaction should succeed.
        simpleTransaction_noQuorumCheck(serverA);
        awaitCommitCounter(2L, serverA, serverB);

        /*
         * Force B into an Error state.
         * 
         * Note: B will remain in the met quorum because it is unable to act on
         * the quorum. 
         */
        ((HAGlueTest) serverB).enterErrorState();
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.NotReady);
        
        /*
         * Transaction will fail
         */
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        try {
            simpleTransaction_noQuorumCheck(serverA);
            fail("expecting transaction to fail");
        } catch (Exception ex) {
            log.warn("Ignoring expected exception: " + ex, ex);
        }
        awaitCommitCounter(2L, serverA, serverB); // commit counter unchanged
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.NotReady);

        // restart zookeeper.
        ((HAGlueTest)serverA).log("MARK");
        ((HAGlueTest)serverB).log("MARK");
        startZookeeper();
        
        /*
         * Quorum should break and then meet again on a new token. We do not
         * know which server will wind up as the leader. It could be either one.
         */

        awaitNextQuorumMeet(token1);
        
        // still at the same commit point.
        awaitCommitCounter(2L, serverA, serverB);
//        awaitNSSAndHAReady(serverA);
//        awaitNSSAndHAReady(serverB);

        // transactions are now accepted.
        simpleTransaction();

        // commit counter advances.
        awaitCommitCounter(3L, serverA, serverB);

    }
    
    /**
     * Attempt to start a service. Once it is running, request a thread dump and
     * then issue a sure kill - both of these operations are done using a SIGNAL
     * rather than RMI. However, the SIGNAL depends on the child PID. That is
     * obtained from {@link HAGlueTest#getPID()}.
     * 
     * @throws Exception 
     */
    public void testStartA_sureKill() throws Exception {

        final HAGlue serverA = startA();

        final int pidA = ((HAGlueTest)serverA).getPID();
        
        // Request thread dump of the child.
        trySignal(SignalEnum.QUIT, pidA);

        // Wait for the thread dump.
        Thread.sleep(2000/*ms*/);
        
        // Child should still be there.
        assertEquals(pidA, ((HAGlueTest) serverA).getPID());

        // Request sure kill of the child.
        trySignal(SignalEnum.KILL, pidA);

        // Wait just a little bit.
        Thread.sleep(100/* ms */);

        // RMI should fail (child process should be dead).
        try {
            ((HAGlueTest) serverA).getPID();
            fail("Expecting " + IOException.class);
        } catch (IOException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

}
