/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.AssertionFailedError;

import com.bigdata.quorum.MockQuorumFixture.MockQuorumMember;

/**
 * Test the quorum semantics for a singleton quorum. This test suite allows us
 * to verify that each quorum state change is translated into the appropriate
 * methods against the public API of the quorum client or quorum member.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestSingletonQuorumSemantics.java 2984 2010-06-06 22:10:32Z
 *          thompsonbry $
 */
public class TestSingletonQuorumSemantics extends AbstractQuorumTestCase {

    /**
     * 
     */
    public TestSingletonQuorumSemantics() {
    }

    /**
     * @param name
     */
    public TestSingletonQuorumSemantics(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        k = 1;
        super.setUp();
    }

    /**
     * Unit test for quorum member add/remove.
     * 
     * @throws InterruptedException
     */
    public void test_memberAddRemove() throws InterruptedException {
        
        final Quorum<?, ?> quorum = quorums[0];
        final QuorumMember<?> client = clients[0];
        final QuorumActor<?,?> actor = actors[0];
        final UUID serviceId = client.getServiceId();
        
        // client is not a member.
        assertFalse(client.isMember());
        assertEquals(new UUID[] {}, quorum.getMembers());
        
        // instruct actor to add client as a member.
        actor.memberAdd();
        fixture.awaitDeque();

        // client is a member.
        assertTrue(client.isMember());
        assertEquals(new UUID[] {serviceId}, quorum.getMembers());

        // instruct actor to remove client as a member.
        actor.memberRemove();
        fixture.awaitDeque();
        
        // client is not a member.
        assertFalse(client.isMember());
        assertEquals(new UUID[] {}, quorum.getMembers());
        
    }

    /**
     * Unit test for quorum member add followed by the termination of the quorum
     * client. This checks for proper termination of the client, including the
     * clear down of the quorum's internal state.
     * 
     * @throws InterruptedException
     */
    public void test_memberAdd_terminateClient() throws InterruptedException {
        
        final Quorum<?, ?> quorum = quorums[0];
        final QuorumMember<?> client = clients[0];
        final QuorumActor<?,?> actor = actors[0];
        final UUID serviceId = client.getServiceId();
        
        // client is not a member.
        assertFalse(client.isMember());
        assertEquals(new UUID[] {}, quorum.getMembers());
        
        // instruct actor to add client as a member.
        actor.memberAdd();
        fixture.awaitDeque();

        // client is a member.
        assertTrue(client.isMember());
        assertEquals(new UUID[] {serviceId}, quorum.getMembers());

        /*
         * Verify termination of the quorum for that client.
         */
        
        assertEquals(client, quorum.getClient());

        quorum.terminate();
        
        try {
            quorum.getClient();
        } catch (IllegalStateException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

        // State was cleared.
        assertEquals(Quorum.NO_QUORUM, quorum.token());
        assertEquals(Quorum.NO_QUORUM, quorum.lastValidToken());
        assertEquals(new UUID[] {}, quorum.getMembers());
        assertEquals(new UUID[] {}, quorum.getJoined());
        assertEquals(new UUID[] {}, quorum.getPipeline());
        assertEquals(Collections.emptyMap(), quorum.getVotes());
        
        try {
            // Note: Quorum reference was cleared. Client throws exception.
            assertFalse(client.isMember());
        } catch (IllegalStateException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

        // Double-termination is safe.
        quorum.terminate();

    }
    
	/**
	 * Unit test for write pipeline add/remove.
	 * @throws InterruptedException 
	 */
    public void test_pipelineAddRemove() throws InterruptedException {

        final Quorum<?, ?> quorum = quorums[0];
        final MockQuorumMember<?> client = clients[0];
        final QuorumActor<?,?> actor = actors[0];
        final UUID serviceId = client.getServiceId();

        assertFalse(client.isMember());
        assertNull(client.downStreamId);
        assertFalse(client.isPipelineMember());
        assertEquals(new UUID[]{},quorum.getPipeline());

        actor.memberAdd();
        fixture.awaitDeque();

		/*
		 * add to the pipeline. since this is a singleton quorum, the downstream
		 * service will remain null.
		 */
		assertNull(client.downStreamId);
        actor.pipelineAdd();
        fixture.awaitDeque();

        assertNull(client.downStreamId);
        assertTrue(client.isPipelineMember());
        assertEquals(new UUID[]{serviceId},quorum.getPipeline());

		/*
		 * remove from the pipeline. since this is a singleton quorum, the
		 * downstream service will remain null.
		 */
		assertNull(client.downStreamId);
        actor.pipelineRemove();
        fixture.awaitDeque();

        assertNull(client.downStreamId);
        assertFalse(client.isPipelineMember());
        assertEquals(new UUID[]{},quorum.getPipeline());

        actor.memberRemove();
        fixture.awaitDeque();

        assertFalse(client.isMember());

    }

    /**
     * Runs a specific unit test a bunch of times looking for instances where
     * the test does not run within the target time period.  
     * 
     * @see #test_voting()
     */
    static public void main(final String[] args) throws Exception {
        // count anything as a "hang" which takes longer than this to run.
        final long hangTime = 1000;
        // #of times to run the test.
        final int limit = 100;
        // run the test.
        int nhang = 0;
        int nerr = 0;
        int nrun = 0;
        for (; nrun < limit; nrun++) {
            final TestSingletonQuorumSemantics t = new TestSingletonQuorumSemantics(
                    Integer.toString(nrun));
            final long begin = System.currentTimeMillis();
            t.setUp();
            try {
                t.test_voting();
            } catch (AssertionFailedError e) {
                log.error(e,e);
                nerr++;
            } finally {
                t.tearDown();
            }
            final long elapsed = System.currentTimeMillis() - begin;
            if (elapsed > hangTime) {
                nhang++;
//                System.err.print('.');
//                if (nhang % 50 == 0)
//                    System.err.println("");
            }
            System.err.println("hang=" + (elapsed > hangTime ? "Y" : "N")
                    + ", nhang=" + nhang + ", nerr=" + nerr + ", nrun=" + nrun
                    + ", elapsed=" + elapsed + "\n");
            System.err.flush();
        }
//        System.err.println("\nnhang=" + nhang + ", nerr=" + nerr + ", nrun="
//                + nrun);
        if (nerr > 0)
            fail("There were " + nerr + " failures.");
    }

    /**
     * Unit test for the voting protocol for a singleton quorum.
     * 
     * FIXME For some reason this unit test occasionally takes much longer to
     * run than would otherwise be expected (up to a few seconds versus a small
     * fraction of a second). You can see this in the timestamps of the logger.
     * <p>
     * The test terminates when the fixture tears down the AbstractQuorum's
     * internal watcher action service, which has a hung action. If the
     * WatcherActionService is also single-threaded, then this could clearly
     * lead to a deadlock since there would be no thread available to handle new
     * events.
     * <p>
     * It is awaiting the quorumBreak condition in
     * AbstractQuorumActor.clearToken(). This issue may be that we have two
     * distinct signals for quorumBreak versus quorumMeet which need to be
     * combined and then the various methods modified to also test the condition
     * variable. [I've made that change.]
     * <p>
     * It seems likely that either a concurrent watcherActionService -or- a
     * finite timeout would get the unit tests to pass. However, only the former
     * would work around a deadlock due to a stuck Condition.
     * <p>
     * Look again at what Condition is getting stuck and at the stress test in
     * {@link #main(String[])} for causes. [nhang=17, nerr=0, nrun=1000]. There
     * are several different causes, each of which clearly reflects a different
     * ordering of the events.
     * <p>
     * It maybe that we see the problem with a singleton quorum because there
     * are no other sources of events to kick the quorum into motion again once
     * it fails to join under the initial impetus.
     * <p>
     * The problem appears to stem from withdrawing the cast vote before the
     * quorum meets.
     */
    public void test_voting() throws InterruptedException,
            AsynchronousQuorumCloseException, TimeoutException {

        /*
         * When true, this test also exercises the awaitQuorum() and
         * awaitBreak() methods that accept a timeout, but only for the case in
         * which the condition should be true on entry. There is another unit
         * test in this class that verifies that the TimeoutException is
         * correctly thrown if the condition does not become true within the
         * timeout.
         */
        final boolean awaitMeetsAndBreaks = true;
	    
        final Quorum<?, ?> quorum = quorums[0];
        final MockQuorumMember<?> client = clients[0];
        final QuorumActor<?,?> actor = actors[0];
        final UUID serviceId = client.getServiceId();

        final long lastCommitTime1 = 0L;

		final long lastCommitTime2 = 2L;

		// Verify that no consensus has been achieved yet.
		assertEquals(-1L, clients[0].lastConsensusValue);

		// add as member service.
        actor.memberAdd();
        fixture.awaitDeque();

        assertTrue(clients[0].isMember());

        // join the pipeline.
        actor.pipelineAdd();
        fixture.awaitDeque();
        
        // Verify that timestamps must be non-negative.
		try {
			actor.castVote(-1L);
			fail("Expected " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		// Should not be any votes.
        assertEquals(0,quorum.getVotes().size());
        
		// Cast a vote.
		actor.castVote(lastCommitTime1);
        fixture.awaitDeque();
		
		// Should be just one vote.
		assertEquals(1,quorum.getVotes().size());

		// Verify the consensus was updated
		assertEquals(lastCommitTime1, client.lastConsensusValue);

        if (awaitMeetsAndBreaks)
            assertEquals(Quorum.NO_QUORUM + 1, quorum.awaitQuorum(100,
                    TimeUnit.MILLISECONDS));

        if(awaitMeetsAndBreaks) {
            actor.withdrawVote();
            quorum.awaitBreak();
            /*
             * FIXME I have modified castVote() to automatically add the service
             * to the pipeline. Otherwise we must do this explicitly after a
             * quorum break. Maybe the better approach to take is to have each
             * service self-report its lastCommitTime and have Quorum#start(M)
             * automatically strive towards a service join while
             * Quorum#terminate() causes leaves and then halts processing for
             * the service.  Alternatively, each client could set its target
             * state in MEMBER, PIPELINE, JOIN.  That would allow clients to
             * manage resynchronization, which they need to do.
             */
//            actor.pipelineAdd();
        }

		// Cast another vote.
		actor.castVote(lastCommitTime2);
        fixture.awaitDeque();

        // Should be just one vote since a service can only vote for one
        // lastCommitTime at a time.
        if (quorum.getVotes().size() != 1) {
            assertEquals(quorum.getVotes().toString(), 1, quorum.getVotes()
                    .size());
        }

		// Verify the consensus was updated again.
		assertEquals(lastCommitTime2, client.lastConsensusValue);

        if (awaitMeetsAndBreaks)
            assertEquals(Quorum.NO_QUORUM + 2, quorum.awaitQuorum(100,
                    TimeUnit.MILLISECONDS));

		// Remove as a member.
        actor.memberRemove();
        fixture.awaitDeque();

        assertFalse(clients[0].isMember());

        // The service vote was also removed.
        assertEquals(0,quorum.getVotes().size());
        
        if (awaitMeetsAndBreaks)
            quorum.awaitBreak(100, TimeUnit.MILLISECONDS);

    }

    /**
     * Unit test for the protocol up to a service join, which triggers a leader
     * election. Since the singleton quorum has only one member our client will
     * be elected the leader.
     * 
     * @throws InterruptedException 
     */
    public void test_serviceJoin() throws InterruptedException {

        final AbstractQuorum<?, ?> quorum = quorums[0];
        final MockQuorumMember<?> client = clients[0];
        final QuorumActor<?,?> actor = actors[0];
        final UUID serviceId = client.getServiceId();

        final long lastCommitTime = 0L;
        final long lastCommitTime2 = 2L;

        // declare the service as a quorum member.
        actor.memberAdd();
        fixture.awaitDeque();
        
        assertTrue(client.isMember());
        assertEquals(new UUID[]{serviceId},quorum.getMembers());
        
        // add to the pipeline.
        actor.pipelineAdd();
        fixture.awaitDeque();

        assertTrue(client.isPipelineMember());
        assertEquals(new UUID[]{serviceId},quorum.getPipeline());

        // cast a vote for a lastCommitTime.
        actor.castVote(lastCommitTime);
        fixture.awaitDeque();

        assertEquals(1,quorum.getVotes().size());
        assertEquals(new UUID[] { serviceId }, quorum.getVotes().get(
                lastCommitTime));

        // verify the consensus was updated.
        assertEquals(lastCommitTime, client.lastConsensusValue);

        // wait for quorum meet.
        final long token1 = quorum.awaitQuorum();
        
        // verify service was joined.
        assertTrue(client.isJoinedMember(quorum.token()));
        assertEquals(new UUID[] { serviceId }, quorum.getJoined());

        // validate the token was assigned.
        fixture.awaitDeque();
        assertEquals(Quorum.NO_QUORUM + 1, quorum.lastValidToken());
        assertEquals(Quorum.NO_QUORUM + 1, quorum.token());
        assertTrue(quorum.isQuorumMet());
        
        /*
         * Do service leave, quorum should break. 
         */

        actor.serviceLeave();
        fixture.awaitDeque();

        quorum.awaitBreak();
        
        // vote was withdrawn.
        assertEquals(0,quorum.getVotes().size());
        assertEquals(null,quorum.getVotes().get(lastCommitTime));

        // verify the consensus was updated.
        assertEquals(-1L, client.lastConsensusValue);
        
        assertFalse(quorum.isQuorumMet());
        assertEquals(Quorum.NO_QUORUM, quorum.token());
        assertEquals(token1, quorum.lastValidToken());
        assertFalse(client.isJoinedMember(quorum.token()));
        assertEquals(new UUID[]{},quorum.getJoined());
        assertFalse(client.isPipelineMember());
        assertEquals(new UUID[]{},quorum.getPipeline());

        /*
         * Cast another vote, the quorum should meet again.
         */

        actor.pipelineAdd();
        fixture.awaitDeque();

        actor.castVote(lastCommitTime2);
        fixture.awaitDeque();

        assertEquals(1,quorum.getVotes().size());
        assertEquals(null,quorum.getVotes().get(lastCommitTime));
        assertEquals(new UUID[] { serviceId }, quorum.getVotes().get(
                lastCommitTime2));

        // verify the consensus was updated.
        assertEquals(lastCommitTime2, client.lastConsensusValue);

        // await meet.
        final long token2 = quorum.awaitQuorum();

        // verify the joined services. 
        assertEquals(new UUID[] { serviceId }, quorum.getJoined());

        // validate the token was assigned by the leader.
        assertTrue(quorum.isQuorumMet());
        assertEquals(token1 + 1, token2);
        assertEquals(token1 + 1, quorum.lastValidToken());
        assertTrue(client.isJoinedMember(token2));
        assertTrue(client.isLeader(token2));
        assertFalse(client.isFollower(token2));

        /*
         * Do service leave, quorum should break again. 
         */
        
        actor.serviceLeave();
        fixture.awaitDeque();

        quorum.awaitBreak();
        
        // vote was withdrawn.
        assertEquals(0,quorum.getVotes().size());
        assertEquals(null,quorum.getVotes().get(lastCommitTime));
        assertEquals(null,quorum.getVotes().get(lastCommitTime2));

        // verify the consensus was updated.
        assertEquals(-1L, client.lastConsensusValue);
        
        assertFalse(quorum.isQuorumMet());
        assertEquals(Quorum.NO_QUORUM, quorum.token());
        assertEquals(token2, quorum.lastValidToken());
        assertFalse(client.isJoinedMember(quorum.token()));
        assertEquals(new UUID[]{},quorum.getJoined());
        assertFalse(client.isPipelineMember());
        assertEquals(new UUID[]{},quorum.getPipeline());
        
    }

    /**
     * Unit test verifying that we clear down the quorum's reflection of the
     * distributed quorum state where we first have a quorum meet and then
     * terminate the quorum client.
     * 
     * @throws InterruptedException
     */
    public void test_serviceJoin_terminateClient() throws InterruptedException {

        final AbstractQuorum<?, ?> quorum = quorums[0];
        final MockQuorumMember<?> client = clients[0];
        final QuorumActor<?,?> actor = actors[0];
        final UUID serviceId = client.getServiceId();

        final long lastCommitTime = 0L;
        final long lastCommitTime2 = 2L;

        // declare the service as a quorum member.
        actor.memberAdd();
        fixture.awaitDeque();
        
        assertTrue(client.isMember());
        assertEquals(new UUID[]{serviceId},quorum.getMembers());
        
        // add to the pipeline.
        actor.pipelineAdd();
        fixture.awaitDeque();

        assertTrue(client.isPipelineMember());
        assertEquals(new UUID[]{serviceId},quorum.getPipeline());

        // cast a vote for a lastCommitTime.
        actor.castVote(lastCommitTime);
        fixture.awaitDeque();

        assertEquals(1,quorum.getVotes().size());
        assertEquals(new UUID[] { serviceId }, quorum.getVotes().get(
                lastCommitTime));

        // verify the consensus was updated.
        assertEquals(lastCommitTime, client.lastConsensusValue);

        // wait for quorum meet.
        final long token1 = quorum.awaitQuorum();
        
        // verify service was joined.
        assertTrue(client.isJoinedMember(quorum.token()));
        assertEquals(new UUID[] { serviceId }, quorum.getJoined());

        // validate the token was assigned.
        fixture.awaitDeque();
        assertEquals(Quorum.NO_QUORUM + 1, quorum.lastValidToken());
        assertEquals(Quorum.NO_QUORUM + 1, quorum.token());
        assertTrue(quorum.isQuorumMet());
        
        /*
         * Terminate the quorum.  The state should be cleared down.
         */
        
        // Verify termination of the quorum for that client.
        assertEquals(client, quorum.getClient());

        // terminate the quorum.
        quorum.terminate();
        
        try {
            quorum.getClient();
        } catch (IllegalStateException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

        // State was cleared.
        assertEquals(Quorum.NO_QUORUM, quorum.token());
        assertEquals(Quorum.NO_QUORUM, quorum.lastValidToken());
        assertEquals(new UUID[] {}, quorum.getMembers());
        assertEquals(new UUID[] {}, quorum.getJoined());
        assertEquals(new UUID[] {}, quorum.getPipeline());
        assertEquals(Collections.emptyMap(), quorum.getVotes());
        
        try {
            // Note: Quorum reference was cleared. Client throws exception.
            assertFalse(client.isMember());
        } catch (IllegalStateException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        
        // re-terminate() is safe.
        quorum.terminate();

    }

    /**
     * Unit test of timeout in {@link Quorum#awaitQuorum(long, TimeUnit)}. and
     * {@link Quorum#awaitBreak(long, TimeUnit)}.
     * 
     * @throws AsynchronousQuorumCloseException
     * @throws InterruptedException
     */
    public void test_awaitQuorum() throws AsynchronousQuorumCloseException, InterruptedException {
        
        final AbstractQuorum<?, ?> quorum = quorums[0];
        final MockQuorumMember<?> client = clients[0];
        final QuorumActor<?,?> actor = actors[0];
        final UUID serviceId = client.getServiceId();

        final long lastCommitTime = 0L;
        final long lastCommitTime2 = 2L;

        // declare the service as a quorum member.
        actor.memberAdd();
        fixture.awaitDeque();
        
        assertTrue(client.isMember());
        assertEquals(new UUID[]{serviceId},quorum.getMembers());
        
        // add to the pipeline.
        actor.pipelineAdd();
        fixture.awaitDeque();

        assertTrue(client.isPipelineMember());
        assertEquals(new UUID[]{serviceId},quorum.getPipeline());

        final long timeout = 1500;// ms
        final long slop = 100;// margin of error.
        {
            /*
             * Verify that a we timeout when awaiting a quorum meet that does
             * not occur.
             */
            final AtomicLong didTimeout = new AtomicLong(-1L);

            final Thread t = new Thread() {
                public void run() {
                    final long begin = System.currentTimeMillis();
                    try {
                        // wait for a quorum (but will not meet).
                        log.info("Waiting for quorum meet.");
                        quorum.awaitQuorum(timeout, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        // This is what we are looking for.
                        final long elapsed = System.currentTimeMillis() - begin;
                        didTimeout.set(elapsed);
                        if (log.isInfoEnabled())
                            log.info("Timeout after " + elapsed + "ms");
                    } catch (Exception e) {
                        log.error(e, e);
                    }
                }
            };
            t.run();
            Thread.sleep(timeout + 250/* ms */);
            t.interrupt();
            final long elapsed = didTimeout.get();
            assertTrue("did not timeout", elapsed != -1);
            assertTrue("Timeout occurred too soon: elapsed=" + elapsed
                    + ",timeout=" + timeout, elapsed >= timeout);
            assertTrue("Timeout took too long: elapsed=" + elapsed
                    + ",timeout=" + timeout, elapsed < (timeout + slop));
        }

        // cast a vote for a lastCommitTime.
        actor.castVote(lastCommitTime);
        fixture.awaitDeque();

        assertEquals(1,quorum.getVotes().size());
        assertEquals(new UUID[] { serviceId }, quorum.getVotes().get(
                lastCommitTime));

        // verify the consensus was updated.
        assertEquals(lastCommitTime, client.lastConsensusValue);

        // wait for quorum meet.
        final long token1 = quorum.awaitQuorum();

        // verify service was joined.
        assertTrue(client.isJoinedMember(quorum.token()));
        assertEquals(new UUID[] { serviceId }, quorum.getJoined());

        // validate the token was assigned.
        fixture.awaitDeque();
        assertEquals(Quorum.NO_QUORUM + 1, quorum.lastValidToken());
        assertEquals(Quorum.NO_QUORUM + 1, quorum.token());
        assertTrue(quorum.isQuorumMet());
        
        {
            /*
             * Verify that we timeout when awaiting a quorum break that does not
             * occur.
             */
            final AtomicLong didTimeout = new AtomicLong(-1L);

            final Thread t = new Thread() {
                public void run() {
                    final long begin = System.currentTimeMillis();
                    try {
                        // wait for a quorum break (but will not break).
                        log.info("Waiting for quorum break.");
                        quorum.awaitBreak(timeout, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        // This is what we are looking for.
                        final long elapsed = System.currentTimeMillis() - begin;
                        didTimeout.set(elapsed);
                        if (log.isInfoEnabled())
                            log.error("Timeout after " + elapsed + "ms");
                    } catch (Exception e) {
                        log.error(e, e);
                    }
                }
            };
            t.run();
            Thread.sleep(timeout + 250/* ms */);
            t.interrupt();
            final long elapsed = didTimeout.get();
            assertTrue("did not timeout", elapsed != -1);
            assertTrue("Timeout occurred too soon: elapsed=" + elapsed
                    + ",timeout=" + timeout, elapsed >= timeout);
            assertTrue("Timeout took too long: elapsed=" + elapsed
                    + ",timeout=" + timeout, elapsed < (timeout + slop));
        }

        try {
            // Verify awaitBreak() does not return normally.
            quorum.awaitBreak(1, TimeUnit.MILLISECONDS);
            fail("Not expecting quorum break");
        } catch (TimeoutException e) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected excption: " + e);
        }

        /*
         * Do service leave, quorum should break. 
         */

        actor.serviceLeave();
        fixture.awaitDeque();

        quorum.awaitBreak();

        try {
            // Verify awaitBreak() returns normally.
            quorum.awaitBreak(1, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            fail("Not expecting " + e, e);
        }

    }
    
    /**
     * Check all valid quorum transitions with:
     * 	currentToken,
     * 	nextToken,
     * 	isJoined state
     * 	haReady token.
     * 
     * Compare tokens to check Quorum state.
     * Compare tokens with haReady and isJoined to check service state.
     * 
     * Possible outcomes are:
     * quorum didBreak
     * quorum didMeet
     * service didLeave
     * service didJoin
     * 
     * Clearly didBreak/didMeet and didLeave/didJoin are exclusive (or should be),
     * but what about didMeet and didLeave or didBreak and didJoin?
     * 
     * TODO This test only verifies the expected presence or absence of an
     * assertion error thrown by the {@link QuorumTokenTransitions} class.
     * It should do actual post-condition checks on the public fields of
     * that class.
     */
    public void testQuorumTransitions() {
    	// combinations are possible current/new/haReady - each with a true/false isJoined
    	final long tokens[][] = new long[][] {
    			// current, new, haReady
        		new long[] {-1, -1, -1}, // wasn't met, not met, wasn't joined
        		new long[] {-1, 0, -1}, // met
        		new long[] {0, 0, -1}, // remains met
        		new long[] {0, -1, -1}, // break
        		
        		new long[] {-1, -1, 0}, // notmet, wasJoined
        		new long[] {-1, 0, 0}, // met, wasJoined
        		new long[] {0, 0, 0}, // remains met, wasJoined
        		new long[] {0, -1, 0}, // break, wasJoined
 
        		// Are these scenarios plausible only as a result of locking problem
        		//	or possibly Zookeeper bounce?
//        		new long[] {0, 2, -1}, // token bumped more than one
//        		new long[] {0, 2, 0}, // token bumped more than one
//        		new long[] {0, 2, 1}, // token bumped more than one
//        		new long[] {0, 2, 2}, // token bumped more than one
    	};
    	
    	for (int i = 0; i < tokens.length; i++) {
    		final long[] tst = tokens[i];
    		
    		// token combinations with isJoined == true
    		final QuorumTokenTransitions qtjoined = new QuorumTokenTransitions(tst[0]/*current token*/, tst[1]/*new token*/, true/*isJoined*/, tst[2]/*haReady*/);
    		
    		// token combinations with isJoined == false
    		final QuorumTokenTransitions qtnotjoined = new QuorumTokenTransitions(tst[0]/*current token*/, tst[1]/*new token*/, false/*isJoined*/, tst[2]/*haReady*/);
    	}
    	
    	// test invalid scenarios to confirm AssertionErrors are thrown
    	try {
    		new QuorumTokenTransitions(1/*current token*/, 2/*new token*/, true/*isJoined*/, 1/*haReady*/);
    		fail("Expected assertion error, cannot progress quorum token without break");
    	} catch (AssertionError ae) {
    		// expected;
    	}
       	
    	try {
    		new QuorumTokenTransitions(2/*current token*/, 1/*new token*/, true/*isJoined*/, 1/*haReady*/);
    		fail("Expected assertion error, new valid < current valid");
    	} catch (AssertionError ae) {
    		// expected;
    	}
       	
    	try {
    		new QuorumTokenTransitions(1/*current token*/, 1/*new token*/, true/*isJoined*/, 2/*haReady*/);
    		fail("Expected assertion error, haReady > newToken");
    	} catch (AssertionError ae) {
    		// expected;
    	}
       	
    	try {
    		new QuorumTokenTransitions(1/*current token*/, 2/*new token*/, true/*isJoined*/, 2/*haReady*/);
    		fail("Expected assertion error, haReady > currentToken");
    	} catch (AssertionError ae) {
    		// expected;
    	}
    }
    
}
