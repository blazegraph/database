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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;

import org.apache.zookeeper.ZooKeeper;

import com.bigdata.BigdataStatics;
import com.bigdata.ha.HACommitGlue;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.ha.msg.IHANotifyReleaseTimeRequest;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.jini.ha.HAJournalServer.RunStateEnum;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.journal.jini.ha.HAJournalTest.SpuriousTestException;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
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
     */
    public void testStartABC_userLevelAbortDoesNotCauseQuorumBreak()
            throws Exception {

        final ABC x = new ABC(true/*sequential*/);
        
        final long token = awaitFullyMetQuorum();
        
        // Now run several transactions
        final int NTX = 5;
        for (int i = 0; i < NTX; i++)
            simpleTransaction();

        // wait until the commit point is registered on all services.
        awaitCommitCounter(NTX + 1L, new HAGlue[] { x.serverA, x.serverB,
                x.serverC });

        // Verify order.
        awaitPipeline(new HAGlue[] { x.serverA, x.serverB, x.serverC });
        awaitJoined(new HAGlue[] { x.serverA, x.serverB, x.serverC });
        
        // Run a transaction that forces a 2-phase abort.
        ((HAGlueTest) x.serverA).simpleTransaction_abort();
        
        // Reverify order.
        awaitPipeline(new HAGlue[] { x.serverA, x.serverB, x.serverC });
        awaitJoined(new HAGlue[] { x.serverA, x.serverB, x.serverC });
        
        // Verify no failover of the leader.
        assertEquals(token, awaitFullyMetQuorum());

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
     * {@link HACommitGlue#prepare2Phase(IHA2PhasePrepareMessage)} to throw an
     * exception. A simple transaction is performed. We verify that the
     * transaction completes successfully, that the quorum token is unchanged,
     * and that [A,C] both participated in the commit. We also verify that B is
     * moved to the end of the pipeline (by doing a serviceLeave and then
     * re-entering the pipeline) and that it resyncs with the met quorum and
     * finally re-joins with the met quorum. The quorum should not break across
     * this test.
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
     * exception inside of the commit2Phase() method rather than at the external
     * RMI interface.
     * <p>
     * A simple transaction is performed. We verify that the transaction
     * completes successfully, that the quorum token is unchanged, and that
     * [A,C] both participated in the commit. We also verify that B is moved to
     * the end of the pipeline (by doing a serviceLeave and then re-entering the
     * pipeline). For this test, B DOES NOT resync and join. This is because A
     * and C go through their commit2Phase() methods for a fully met quorum.
     * Because we have explicitly disabled the {@link DefaultRestorePolicy},
     * this allows them to purge their HALogs. This means that B can not resync
     * with the met quorum. As a consequence, B transitions to the
     * {@link RunStateEnum#Operator} state and remains
     * {@link HAStatusEnum#NotReady}.
     * <p>
     * The quorum should not break across this test.
     * 
     * TODO Consider leader failure scenarios in this test suite (commit2Phase()
     * fails on the leader), not just scenarios where B fails. We MUST also
     * cover failures of C (the 2nd follower). We should also cover scenarios
     * where the quorum is barely met and a single failure causes a rejected
     * commit (local decision) or 2-phase abort (joined services in joint
     * agreement).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/760" >
     *      Review commit2Phase semantics when a follower fails </a>
     * 
     * @see TestHA3JournalServerWithHALogs#testStartABC_commit2Phase_B_failCommit_beforeWritingRootBlockOnJournal_HALogsNotPurgedAtCommit()
     */
    public void testStartABC_commit2Phase_B_failCommit_beforeWritingRootBlockOnJournal_HALogsPurgedAtCommit()
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
         */
        ((HAGlueTest) startup.serverB)
                .failCommit_beforeWritingRootBlockOnJournal();

        /*
         * Simple transaction.
         * 
         * Note: B will fail the commit without laying down the root block and
         * will transition into the ERROR state. From there, it will move to
         * SeekConsensus and then RESYNC. While in RESYNC it will pick up the
         * missing HALog and commit point. Finally, it will transition into
         * RunMet.
         */
        simpleTransaction();

        // Verify quorum is unchanged.
        assertEquals(token, quorum.token());

        // Should be two commit points on {A,C}.
        awaitCommitCounter(2L, startup.serverA, startup.serverC);

        /*
         * Just one commit point on B
         * 
         * TODO This is a data race. It is only transiently true.
         */
        awaitCommitCounter(1L, startup.serverB);

        /*
         * B is NotReady
         * 
         * TODO This is a data race. It is only transiently true.
         */
        awaitHAStatus(startup.serverB, HAStatusEnum.NotReady);

        /*
         * The pipeline should be reordered. B will do a service leave, then
         * enter seek consensus, and then re-enter the pipeline.
         */
        awaitPipeline(new HAGlue[] { startup.serverA, startup.serverC,
                startup.serverB });

        /*
         * IF you allow the purge of the HALog files on a fully met commit AND a
         * service fails in commit2Phase() for a fully met quorum THEN the other
         * services will have purged their HALog files and the service that
         * failed in commit2Phase() will be unable to resync and join the met
         * quorum.
         */
        awaitRunStateEnum(RunStateEnum.Operator, startup.serverB);
        awaitHAStatus(startup.serverB, HAStatusEnum.NotReady);

        // There should be two commit points on {A,C}.
        awaitCommitCounter(2L, startup.serverA, startup.serverC);

        // Just one commit point on B.
        awaitCommitCounter(1L, startup.serverB);

        // quorum token is unchanged.
        assertEquals(token, quorum.token());

    }

    /**
     * Unit test for failure to RESYNC having a root cause that the live HALog
     * file did not exist on the quorum leader after an abort2Phase() call.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/764" >
     *      RESYNC fails (HA) </a>
     */
    public void testStartABC_abort2Phase_restartC() throws Exception {
        
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
        
        /*
         * Run a transaction that forces a 2-phase abort.
         * 
         * Note: Historically, this would cause the live HALog on the leader to
         * be disabled (aka deleted) without causing that live HALog file to be
         * recreated.  This is ticket #764.
         */
        ((HAGlueTest) x.serverA).simpleTransaction_abort();
        
        /*
         * Restart C.
         * 
         * Note: C will go into RESYNC. Since all services are at the same
         * commit point, C will attempt to replicate the live HALog from the
         * leader. Once it obtains that HALog, it should figure out that it has
         * the live HALog and attempt to transition atomically to a joined
         * service.
         */
        /*final HAGlue serverC =*/ startC();
        
        // wait until the quorum fully meets again (on the same token).
        assertEquals(token, awaitFullyMetQuorum());

        // Verify expected HALog files.
        awaitLogCount(getHALogDirA(), 1L);
        awaitLogCount(getHALogDirB(), 1L);
        awaitLogCount(getHALogDirC(), 1L);

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
    
    private void doBounceFollower() throws Exception {
        
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

					final RemoteRepositoryManager repo = getRemoteRepository(service, httpClient);
					try {
						// Should be empty.
						assertEquals(
								0L,
								countResults(repo.getRepositoryForDefaultNamespace().prepareTupleQuery(
										"SELECT * {?a ?b ?c} LIMIT 10")
										.evaluate()));
					} finally {
						repo.close();
					}

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

					final RemoteRepositoryManager repo = getRemoteRepository(service, httpClient);
					try {
						// Should be empty.
						assertEquals(
								0L,
								countResults(repo.getRepositoryForDefaultNamespace().prepareTupleQuery(
										"SELECT * {?a ?b ?c} LIMIT 10")
										.evaluate()));
					} finally {
						repo.close();
					}
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
     * Test of {@link QuorumActor#forceRemoveService(UUID)}. Start A + B. Once
     * the quorum meets, we figure out which service is the leader. The leader
     * then forces the other service out of the quorum.
     */
    public void test_AB_forceRemoveService_B() throws Exception {
        
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
         * Force the follower out of the quorum. Verify quorum meets again and
         * that we can read on all services.
         */
		{

			final HAGlue leader = quorum.getClient().getLeader(token1);

			if (leader.equals(serverA)) {

				leader.submit(new ForceRemoveService(getServiceBId()), true)
						.get();

			} else {

				leader.submit(new ForceRemoveService(getServiceAId()), true)
						.get();

			}

			// Thread.sleep(100000); // sleep to allow thread dump for analysis
			// Okay, is the problem that the quorum doesn't break?
			// assertFalse(quorum.isQuorumMet());

			// Right so the Quorum is not met, but the follower deosn't seem to
			// know it's broken

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

			// final UUID leaderId2 = leader2.getServiceId();
			//
			// assertFalse(leaderId1.equals(leaderId2));

			/*
			 * Verify we can read on the KB on both nodes.
			 * 
			 * Note: It is important to test the reads for the first commit on
			 * both the leader and the follower.
			 */
				for (HAGlue service : new HAGlue[] { serverA, serverB }) {

					awaitNSSAndHAReady(service);

					final RemoteRepositoryManager repo = getRemoteRepository(
							service, httpClient);
					try {
						// Should be empty.
						assertEquals(
								0L,
								countResults(repo.getRepositoryForDefaultNamespace().prepareTupleQuery(
										"SELECT * {?a ?b ?c} LIMIT 10")
										.evaluate()));
					} finally {
						repo.close();
					}

				}

		}
        
    }

    /**
     * Test of {@link QuorumActor#forceRemoveService(UUID)}. Start A + B + C in
     * strict order. Wait until the quorum is fully met and the initial KB
     * create transaction is done. The leader then forces B out of the quorum.
     * We verify that the quorum fully meets again, that B is now the last
     * service in the pipeline order, and that the quorum did not break (same
     * token).
     */
	public void test_ABC_forceRemoveService_B() throws Exception {

		final ABC services = new ABC(true/* sequential */);
		final HAGlue serverA = services.serverA;
		final HAGlue serverB = services.serverB;
		final HAGlue serverC = services.serverC;

		final long token1 = awaitFullyMetQuorum();

		doNSSStatusRequest(serverA);
		doNSSStatusRequest(serverB);
		doNSSStatusRequest(serverC);

		// Await initial commit point (KB create).
		awaitCommitCounter(1L, serverA, serverB, serverC);

		// Await [A] up and running as leader.
		assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));

		// Await [B] up and running as follower.
		assertEquals(HAStatusEnum.Follower, awaitNSSAndHAReady(serverB));

		// Verify self-reporting by RMI in their respective roles.
		awaitHAStatus(serverA, HAStatusEnum.Leader);
		awaitHAStatus(serverB, HAStatusEnum.Follower);
		awaitHAStatus(serverC, HAStatusEnum.Follower);

		// Verify binary equality on the journal files.
		assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

		if (log.isInfoEnabled()) {
			log.info("Zookeeper before forcing service remove:\n" + dumpZoo());
		}

		/*
		 * Bounce the 1st follower out of the quorum. Verify quorum meets again
		 * and that we can read on all services.
		 */
		{

			serverA.submit(new ForceRemoveService(getServiceBId()), true).get();

			// Wait for the quorum to fully meet again.
			final long token2 = awaitFullyMetQuorum();

			if (log.isInfoEnabled()) {
				log.info("Zookeeper after quorum fully met again:\n"
						+ dumpZoo());
			}

			/*
			 * The quorum did not break. The token is unchanged.
			 */
			assertEquals(token1, token2);

			/*
			 * Service B came back in at the end of the pipeline.
			 */
			awaitPipeline(new HAGlue[] { serverA, serverC, serverB });

			/*
			 * Verify we can read on the KB on all nodes.
			 * 
			 * Note: It is important to test the reads for the first commit on
			 * both the leader and the follower.
			 */
				for (HAGlue service : new HAGlue[] { serverA, serverB, serverC }) {

					awaitNSSAndHAReady(service);

					final RemoteRepositoryManager repo = getRemoteRepository(
							service, httpClient);
					try {
						// Should be empty.
						assertEquals(
								0L,
								countResults(repo.getRepositoryForDefaultNamespace().prepareTupleQuery(
										"SELECT * {?a ?b ?c} LIMIT 10")
										.evaluate()));
					} finally {
						repo.close();
					}
				}

		}

	}

    /**
     * Test of {@link QuorumActor#forceRemoveService(UUID)}. Start A + B + C in
     * strict order. Wait until the quorum is fully met and the initial KB
     * create transaction is done. The leader then forces C out of the quorum.
     * We verify that the quorum fully meets again, that C is again the last
     * service in the pipeline order, and that the quorum did not break (same
     * token).
     */
    public void test_ABC_forceRemoveService_C() throws Exception {
        
        final ABC services = new ABC(true/*sequential*/);
        final HAGlue serverA = services.serverA;
        final HAGlue serverB = services.serverB;
        final HAGlue serverC = services.serverC;
        
        final long token1 = awaitFullyMetQuorum();

        doNSSStatusRequest(serverA);
        doNSSStatusRequest(serverB);
        doNSSStatusRequest(serverC);

        // Await initial commit point (KB create).
        awaitCommitCounter(1L, serverA, serverB, serverC);

        // Await [A] up and running as leader.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(serverA));

        // Await [B] up and running as follower.
        assertEquals(HAStatusEnum.Follower, awaitNSSAndHAReady(serverB));

        // Verify self-reporting by RMI in their respective roles.
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);
        
        // Verify binary equality on the journal files.
        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });

        if (log.isInfoEnabled()) {
            log.info("Zookeeper before forcing service remove:\n" + dumpZoo());
        }
        
        /*
         * Bounce the 1st follower out of the quorum. Verify quorum meets again
         * and that we can read on all services.
         */
        {

            serverA.submit(new ForceRemoveService(getServiceCId()), true).get();
            
            // Wait for the quorum to fully meet again.
            final long token2 = awaitFullyMetQuorum();

            if (log.isInfoEnabled()) {
                log.info("Zookeeper after quorum fully met again:\n" + dumpZoo());
            }
            
            /*
             * The quorum did not break. The token is unchanged.
             */
            assertEquals(token1, token2);
            
            /*
             * Service C came back in at the end of the pipeline (i.e., the
             * pipeline is unchanged).
             */
            awaitPipeline(new HAGlue[] { serverA, serverB, serverC });
            
            /*
             * Verify we can read on the KB on all nodes.
             * 
             * Note: It is important to test the reads for the first commit on
             * both the leader and the follower.
             */
			for (HAGlue service : new HAGlue[] { serverA, serverB, serverC }) {

				awaitNSSAndHAReady(service);

				final RemoteRepositoryManager repo = getRemoteRepository(service, httpClient);
				try {
					// Should be empty.
					assertEquals(
							0L,
							countResults(repo.getRepositoryForDefaultNamespace().prepareTupleQuery(
									"SELECT * {?a ?b ?c} LIMIT 10").evaluate()));
				} finally {
					repo.close();
				}
			}

        }
        
    }

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
     */
    public void testABC_stopZookeeper_failA_startZookeeper_quorumMeetsAgainOnNewToken()
            throws Exception {
        if (!BigdataStatics.runKnownBadTests) {
            /*
             * FIXME Test disabled for the 1.3.0 release. This test fails.
             * Figure out why.
             */
            return;
        }
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

				final RemoteRepositoryManager repo = getRemoteRepository(service, httpClient);
				try {
					assertEquals(
							1L,
							countResults(repo.getRepositoryForDefaultNamespace().prepareTupleQuery(
									"SELECT (count(*) as ?count) {?a ?b ?c}")
									.evaluate()));
				} finally {
					repo.close();
				}
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
     * transactions should be accepted.  
     */
    public void testAB_stopZookeeper_failB_startZookeeper_quorumBreaksThenMeets()
            throws Exception {
        
        if (!BigdataStatics.runKnownBadTests) {
            /*
             * FIXME Test disabled for the 1.3.0 release. This test fails.
             * Figure out why.
             */
            return;
        }

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
