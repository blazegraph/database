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
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;

import com.bigdata.ha.HACommitGlue;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.journal.jini.ha.HAJournalTest.SpuriousTestException;
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
                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\""
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
     * FIXME Variant where the GATHER failed.
     * 
     * FIXME Variant where the commit2Phase fails.
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
        
        // Simple transaction.
        simpleTransaction();
        
        // Verify quorum is unchanged.
        assertEquals(token, quorum.token());
        
        // Should be two commit points on {A,C].
        awaitCommitCounter(2L, startup.serverA, startup.serverC);
        
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
         * FIXME Unlike the test above, if there is a problem making the RMI
         * call, then B will not go through its doRejectedCommit() handler and
         * will not enter the ERROR state directly. We need to have B notice
         * that it is no longer at the same commit point, e.g., by observing a
         * LIVE write cache message with an unexpected value for the
         * commitCounter (especially, GT its current expected value). That is
         * the indication that B needs to enter an error state. Until then it
         * does not know that there was an attempt to PREPARE since it did not
         * get the prepare2Phase() message.
         * 
         * - Modify HAJournalServer to enter the error state if we observe a
         * live write cache block for a commitCounter != the expected
         * commitCounter.
         * 
         * - Modify commit2Phase() to accept the #of services that are
         * participating in the commit. If it is not a full quorum, then we can
         * not purge the HA logs in commit2Phase() regardless of what the quorum
         * state looks like.
         * 
         * - Modify this test to do another transaction. B can not notice the
         * problem until there is another write cache flushed through the
         * pipeline.
         * 
         * - Modify this test to await B to move to the end of the pipeline,
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
    
}
