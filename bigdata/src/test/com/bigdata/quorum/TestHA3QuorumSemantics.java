/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.util.UUID;

import com.bigdata.quorum.MockQuorumFixture.MockQuorumMember;

/**
 * Test the quorum semantics for a highly available quorum of 3 services. The
 * main points to test here are the particulars of events not observable with a
 * singleton quorum, including that all events are perceived by all clients via
 * the watcher for their quorum, that a service join which does not trigger a
 * quorum meet, that a service leave which does not trigger a quorum break, a
 * leader leave, etc.
 * <p>
 * These conditions arise because the quorum can meet with 2 out of 3 services
 * forming a consensus. Even if the third service has the same lastCommitTime,
 * if it votes after the other services, then it can be in the position of
 * joining a quorum which has already met. This leads naturally into an
 * exploration of the synchronization protocol for the persistent state of the
 * services when joining a met quorum.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestHA3QuorumSemantics.java 2965 2010-06-02 20:19:51Z
 *          thompsonbry $
 * 
 *          FIXME We should be verifying the downstreamState on the
 *          {@link MockQuorumMember}.
 * 
 *          FIXME We must also test with k:=5 in order to see some situations
 *          which do not appear with k:=3 (not sure which ones off hand, but I
 *          remember noting that there are some).
 * 
 *          FIXME There will need to be unit tests for synchronization when a
 *          service wants to join a met quorum. Persistent services need to
 *          verify their root blocks in detail against the leader. If they are
 *          not identical, then the services need to synchronize rather than
 *          join. Also, services which attempt to join an already met quorum
 *          must synchronize before they can join. All of this needs to be
 *          tested, but we need to do those tests with live journals.
 * 
 *          FIXME There will need to be unit tests for hot spares.
 */
public class TestHA3QuorumSemantics extends AbstractQuorumTestCase {

    /**
     * 
     */
    public TestHA3QuorumSemantics() {
    }

    /**
     * @param name
     */
    public TestHA3QuorumSemantics(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        k = 3;
        super.setUp();
    }

    /**
     * Unit test for quorum member add/remove.
     * 
     * @throws InterruptedException
     */
    public void test_memberAddRemove3() throws InterruptedException {
        
        final Quorum<?, ?> quorum0 = quorums[0];
        final QuorumMember<?> client0 = clients[0];
        final QuorumActor<?,?> actor0 = actors[0];
        final UUID serviceId0 = client0.getServiceId();
        
        final Quorum<?, ?> quorum1 = quorums[1];
        final QuorumMember<?> client1 = clients[1];
        final QuorumActor<?,?> actor1 = actors[1];
        final UUID serviceId1 = client1.getServiceId();

        final Quorum<?, ?> quorum2 = quorums[2];
        final QuorumMember<?> client2 = clients[2];
        final QuorumActor<?,?> actor2 = actors[2];
        final UUID serviceId2 = client2.getServiceId();
        
        // client is not a member.
        assertFalse(client0.isMember());
        assertFalse(client1.isMember());
        assertFalse(client2.isMember());
        assertEquals(new UUID[] {}, quorum0.getMembers());
        assertEquals(new UUID[] {}, quorum1.getMembers());
        assertEquals(new UUID[] {}, quorum2.getMembers());
        
        // instruct an actor to add its client as a member.
        actor0.memberAdd();
        fixture.awaitDeque();
        
        // client is a member.
        assertTrue(client0.isMember());
        assertFalse(client1.isMember());
        assertFalse(client2.isMember());
        assertEquals(new UUID[] {serviceId0}, quorum0.getMembers());
        assertEquals(new UUID[] {serviceId0}, quorum1.getMembers());
        assertEquals(new UUID[] {serviceId0}, quorum2.getMembers());

        // instruct an actor to add its client as a member.
        actor1.memberAdd();
        fixture.awaitDeque();
        
        // the client is now a member.
        assertTrue(client0.isMember());
        assertTrue(client1.isMember());
        assertFalse(client2.isMember());
        assertEquals(new UUID[] {serviceId0,serviceId1}, quorum0.getMembers());
        assertEquals(new UUID[] {serviceId0,serviceId1}, quorum1.getMembers());
        assertEquals(new UUID[] {serviceId0,serviceId1}, quorum2.getMembers());

        // instruct an actor to remove its client as a member.
        actor0.memberRemove();
        fixture.awaitDeque();
        
        // the client is no longer a member.
        assertFalse(client0.isMember());
        assertTrue(client1.isMember());
        assertFalse(client2.isMember());
        assertEquals(new UUID[] {serviceId1}, quorum0.getMembers());
        assertEquals(new UUID[] {serviceId1}, quorum1.getMembers());
        assertEquals(new UUID[] {serviceId1}, quorum2.getMembers());

        // instruct an actor to remove its client as a member.
        actor1.memberRemove();
        fixture.awaitDeque();
        
        // the client is no longer a member.
        assertFalse(client0.isMember());
        assertFalse(client1.isMember());
        assertFalse(client2.isMember());
        assertEquals(new UUID[] {}, quorum0.getMembers());
        assertEquals(new UUID[] {}, quorum1.getMembers());
        assertEquals(new UUID[] {}, quorum2.getMembers());
               
    }

    /**
     * Unit test for write pipeline add/remove.
     * 
     * @throws InterruptedException
     */
    public void test_pipelineAddRemove3() throws InterruptedException {

        final Quorum<?, ?> quorum0 = quorums[0];
        final MockQuorumMember<?> client0 = clients[0];
        final QuorumActor<?,?> actor0 = actors[0];
        final UUID serviceId0 = client0.getServiceId();
        
        final Quorum<?, ?> quorum1 = quorums[1];
        final MockQuorumMember<?> client1 = clients[1];
        final QuorumActor<?,?> actor1 = actors[1];
        final UUID serviceId1 = client1.getServiceId();

        final Quorum<?, ?> quorum2 = quorums[2];
        final MockQuorumMember<?> client2 = clients[2];
        final QuorumActor<?,?> actor2 = actors[2];
        final UUID serviceId2 = client2.getServiceId();
        
        // Verify the initial pipeline state.
        assertNull(client0.downStreamId);
        assertNull(client1.downStreamId);
        assertNull(client2.downStreamId);
        assertFalse(client0.isPipelineMember());
        assertFalse(client1.isPipelineMember());
        assertFalse(client2.isPipelineMember());
        assertEquals(new UUID[]{},quorum0.getPipeline());
        assertEquals(new UUID[]{},quorum1.getPipeline());
        assertEquals(new UUID[]{},quorum2.getPipeline());

        // Add the services to the quorum.
        actor0.memberAdd();
        actor1.memberAdd();
        actor2.memberAdd();
        fixture.awaitDeque();
        
        assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 }, quorum0
                .getMembers());

        /*
         * Add each service in turn to the pipeline, verifying the changes in
         * the pipeline state as we go.
         */
        actor0.pipelineAdd();
        fixture.awaitDeque();
        
        assertTrue(client0.isPipelineMember());
        assertFalse(client1.isPipelineMember());
        assertFalse(client2.isPipelineMember());
        assertEquals(new UUID[]{serviceId0},quorum0.getPipeline());
        assertEquals(new UUID[]{serviceId0},quorum1.getPipeline());
        assertEquals(new UUID[]{serviceId0},quorum2.getPipeline());
        assertNull(client0.downStreamId);
        assertNull(client1.downStreamId);
        assertNull(client2.downStreamId);

        actor1.pipelineAdd();
        fixture.awaitDeque();
        
        assertTrue(client0.isPipelineMember());
        assertTrue(client1.isPipelineMember());
        assertFalse(client2.isPipelineMember());
        assertEquals(new UUID[]{serviceId0,serviceId1},quorum0.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId1},quorum1.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId1},quorum2.getPipeline());
        assertEquals(serviceId1,client0.downStreamId);
        assertNull(client1.downStreamId);
        assertNull(client2.downStreamId);

        actor2.pipelineAdd();
        fixture.awaitDeque();
        
        assertTrue(client0.isPipelineMember());
        assertTrue(client1.isPipelineMember());
        assertTrue(client2.isPipelineMember());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum0.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum1.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum2.getPipeline());
        assertEquals(serviceId1,client0.downStreamId);
        assertEquals(serviceId2,client1.downStreamId);
        assertNull(client2.downStreamId);

        // test prior/next.
        assertEquals(new UUID[] { null, serviceId1 }, quorum0.getPipelinePriorAndNext(serviceId0));
        assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum0.getPipelinePriorAndNext(serviceId1));
        assertEquals(new UUID[] { serviceId1, null }, quorum0.getPipelinePriorAndNext(serviceId2));

        // remove one from the pipeline.
        actor1.pipelineRemove();
        fixture.awaitDeque();
        
        assertTrue(client0.isPipelineMember());
        assertFalse(client1.isPipelineMember());
        assertTrue(client2.isPipelineMember());
        assertEquals(new UUID[]{serviceId0,serviceId2},quorum0.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId2},quorum1.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId2},quorum2.getPipeline());
        assertEquals(serviceId2,client0.downStreamId);
        assertNull(client1.downStreamId);
        assertNull(client2.downStreamId);

        // remove another from the pipeline.
        actor0.pipelineRemove();
        fixture.awaitDeque();
        
        assertFalse(client0.isPipelineMember());
        assertFalse(client1.isPipelineMember());
        assertTrue(client2.isPipelineMember());
        assertEquals(new UUID[]{serviceId2},quorum0.getPipeline());
        assertEquals(new UUID[]{serviceId2},quorum1.getPipeline());
        assertEquals(new UUID[]{serviceId2},quorum2.getPipeline());
        assertNull(client0.downStreamId);
        assertNull(client1.downStreamId);
        assertNull(client2.downStreamId);

        // remove the last service from the pipeline.
        actor2.pipelineRemove();
        fixture.awaitDeque();
        
        assertFalse(client0.isPipelineMember());
        assertFalse(client1.isPipelineMember());
        assertFalse(client2.isPipelineMember());
        assertEquals(new UUID[]{},quorum0.getPipeline());
        assertEquals(new UUID[]{},quorum1.getPipeline());
        assertEquals(new UUID[]{},quorum2.getPipeline());
        assertNull(client0.downStreamId);
        assertNull(client1.downStreamId);
        assertNull(client2.downStreamId);

        // remove the members from the quorum.
        actor0.memberRemove();
        actor1.memberRemove();
        actor2.memberRemove();
        fixture.awaitDeque();

    }

    /**
     * Unit test for the voting protocol.
     * 
     * @throws InterruptedException
     */
    public void test_voting3() throws InterruptedException {

        final Quorum<?, ?> quorum0 = quorums[0];
        final MockQuorumMember<?> client0 = clients[0];
        final QuorumActor<?, ?> actor0 = actors[0];
        final UUID serviceId0 = client0.getServiceId();
        
        final Quorum<?, ?> quorum1 = quorums[1];
        final MockQuorumMember<?> client1 = clients[1];
        final QuorumActor<?,?> actor1 = actors[1];
        final UUID serviceId1 = client1.getServiceId();

        final Quorum<?, ?> quorum2 = quorums[2];
        final MockQuorumMember<?> client2 = clients[2];
        final QuorumActor<?,?> actor2 = actors[2];
        final UUID serviceId2 = client2.getServiceId();
        
        final long lastCommitTime1 = 0L;

        final long lastCommitTime2 = 2L;

        // Verify that no consensus has been achieved yet.
        assertEquals(-1L, client0.lastConsensusValue);
        assertEquals(-1L, client1.lastConsensusValue);
        assertEquals(-1L, client2.lastConsensusValue);

        // add as member services.
        actor0.memberAdd();
        actor1.memberAdd();
        actor2.memberAdd();
        fixture.awaitDeque();
        
        // join the pipeline.
        actor0.pipelineAdd();
        actor1.pipelineAdd();
        actor2.pipelineAdd();
        fixture.awaitDeque();
        
        // Should not be any votes.
        assertEquals(0,quorum0.getVotes().size());
        assertEquals(0,quorum1.getVotes().size());
        assertEquals(0,quorum2.getVotes().size());
        
        // Cast a vote.
        actor0.castVote(lastCommitTime1);
        fixture.awaitDeque();
        
        // The last consensus timestamp should not have been updated.
        assertEquals(-1L, client0.lastConsensusValue);
        assertEquals(-1L, client1.lastConsensusValue);
        assertEquals(-1L, client2.lastConsensusValue);

        // Should be just one timestamp for which services have voted.
        assertEquals(1,quorum0.getVotes().size());
        assertEquals(1,quorum1.getVotes().size());
        assertEquals(1,quorum2.getVotes().size());

        // Cast a vote for a different timestamp.
        actor1.castVote(lastCommitTime2);
        fixture.awaitDeque();
        
        // The last consensus timestamp should not have been updated.
        assertEquals(-1L, client0.lastConsensusValue);
        assertEquals(-1L, client1.lastConsensusValue);
        assertEquals(-1L, client2.lastConsensusValue);

        // Should be two timestamps for which services have voted.
        assertEquals(2, quorum0.getVotes().size());
        assertEquals(2, quorum1.getVotes().size());
        assertEquals(2, quorum2.getVotes().size());

        /*
         * Cast another vote for for the same timestamp. This will trigger a
         * consensus (simple majority) and the quorum will meet (but we do not
         * verify this in any detail in this unit test since it is focused on
         * the voting protocol).
         */
        actor2.castVote(lastCommitTime1);
        fixture.awaitDeque();
        
        // The last consensus timestamp should have been updated for all quorum members.
        assertEquals(lastCommitTime1, client0.lastConsensusValue);
        assertEquals(lastCommitTime1, client1.lastConsensusValue);
        assertEquals(lastCommitTime1, client2.lastConsensusValue);

        // Should be two timestamps for which services have voted.
        assertEquals(2, quorum0.getVotes().size());
        assertEquals(2, quorum1.getVotes().size());
        assertEquals(2, quorum2.getVotes().size());

        // Verify the specific services voting for each timestamp.
        assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum0.getVotes()
                .get(lastCommitTime1).toArray(new UUID[] {}));
        assertEquals(new UUID[] { serviceId1 }, quorum0.getVotes().get(
                lastCommitTime2).toArray(new UUID[] {}));

        // The quorum met.
        assertTrue(quorum0.isQuorumMet());
        assertTrue(quorum1.isQuorumMet());
        assertTrue(quorum2.isQuorumMet());
        
        // Remove as a member.  This should not affect the consensus.
        actor1.memberRemove();
        fixture.awaitDeque();
        
        // The last consensus timestamp should not have changed.
        assertEquals(lastCommitTime1, client0.lastConsensusValue);
        assertEquals(lastCommitTime1, client1.lastConsensusValue);
        assertEquals(lastCommitTime1, client2.lastConsensusValue);

        // Should be just one timestamps for which services have voted.
        assertEquals(1, quorum0.getVotes().size());
        assertEquals(1, quorum1.getVotes().size());
        assertEquals(1, quorum2.getVotes().size());

        // Verify the specific services voting for each timestamp.
        assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum0.getVotes()
                .get(lastCommitTime1).toArray(new UUID[] {}));
        assertEquals(null, quorum0.getVotes().get(lastCommitTime2));

        /*
         * Remove another service as a member. This should break the consensus
         * and (since the quorum was actually met) this will break the quorum.
         */
        actor0.memberRemove();
        fixture.awaitDeque();
        
        // The last consensus on the mock clients should have been cleared.
        assertEquals(-1L, client0.lastConsensusValue);
        assertEquals(-1L, client1.lastConsensusValue);
        assertEquals(-1L, client2.lastConsensusValue);

        // Should be just one timestamp for which services have voted.
        assertEquals(0, quorum0.getVotes().size());
        assertEquals(0, quorum1.getVotes().size());
        assertEquals(0, quorum2.getVotes().size());

        // Verify the specific services voting for each timestamp.
        assertEquals(null, quorum0.getVotes().get(lastCommitTime1));
        assertEquals(null, quorum0.getVotes().get(lastCommitTime2));

        // The quorum broke.
        assertFalse(quorum0.isQuorumMet());
        assertFalse(quorum1.isQuorumMet());
        assertFalse(quorum2.isQuorumMet());
        
        // Remove the last member service.
        actor2.memberRemove();
        fixture.awaitDeque();
        
        // No change.
        assertEquals(-1L, client0.lastConsensusValue);
        assertEquals(-1L, client1.lastConsensusValue);
        assertEquals(-1L, client2.lastConsensusValue);

        // No votes left.
        assertEquals(0, quorum0.getVotes().size());
        assertEquals(0, quorum1.getVotes().size());
        assertEquals(0, quorum2.getVotes().size());

        // Verify the specific services voting for each timestamp.
        assertEquals(null, quorum0.getVotes().get(lastCommitTime1));
        assertEquals(null, quorum0.getVotes().get(lastCommitTime2));
        
    }

    /**
     * Unit test for service join/leave where services vote in the pipeline
     * order so the leader does not need to reorganize the pipeline when the
     * number of joined services reaches (k+1)/2. The quorum meets after two
     * services cast their vote. When the third service casts its vote, it joins
     * the met quorum.
     * 
     * @throws InterruptedException
     */
    public void test_serviceJoin3_simple() throws InterruptedException {

        final Quorum<?, ?> quorum0 = quorums[0];
        final MockQuorumMember<?> client0 = clients[0];
        final QuorumActor<?, ?> actor0 = actors[0];
        final UUID serviceId0 = client0.getServiceId();

        final Quorum<?, ?> quorum1 = quorums[1];
        final MockQuorumMember<?> client1 = clients[1];
        final QuorumActor<?, ?> actor1 = actors[1];
        final UUID serviceId1 = client1.getServiceId();

        final Quorum<?, ?> quorum2 = quorums[2];
        final MockQuorumMember<?> client2 = clients[2];
        final QuorumActor<?,?> actor2 = actors[2];
        final UUID serviceId2 = client2.getServiceId();
        
//            final long lastCommitTime1 = 0L;
//
//            final long lastCommitTime2 = 2L;

        final long lastCommitTime = 0L;

        // declare the services as a quorum members.
        actor0.memberAdd();
        actor1.memberAdd();
        actor2.memberAdd();
        fixture.awaitDeque();
        
        /*
         * Have the services join the pipeline.
         */
        actor0.pipelineAdd();
        actor1.pipelineAdd();
        actor2.pipelineAdd();
        fixture.awaitDeque();
        
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum0.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum1.getPipeline());
        assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 }, quorum2
                .getPipeline());

        /*
         * Have two services cast a vote for a lastCommitTime. This will cause
         * the quorum to meet.
         */
        final long token1;
        {
        
            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
            fixture.awaitDeque();

            // services have voted for a single lastCommitTime.
            assertEquals(1, quorum0.getVotes().size());

            // verify the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId1 }, quorum0
                    .getVotes().get(lastCommitTime).toArray(new UUID[0]));

            // verify the consensus was updated.
            assertEquals(lastCommitTime, client0.lastConsensusValue);
            assertEquals(lastCommitTime, client1.lastConsensusValue);
            assertEquals(lastCommitTime, client2.lastConsensusValue);

            /*
             * Service join in the same order in which they cast their votes.
             */
            assertEquals(new UUID[] { serviceId0, serviceId1 }, quorum0
                    .getJoinedMembers());
            assertEquals(new UUID[] { serviceId0, serviceId1 }, quorum1
                    .getJoinedMembers());
            assertEquals(new UUID[] { serviceId0, serviceId1 }, quorum2
                    .getJoinedMembers());

            // validate the token was assigned.
            assertEquals(Quorum.NO_QUORUM + 1, quorum0.lastValidToken());
            assertEquals(Quorum.NO_QUORUM + 1, quorum0.token());
            assertTrue(quorum0.isQuorumMet());
            token1 = quorum0.token();
            assertEquals(token1, quorum1.lastValidToken());
            assertEquals(token1, quorum2.lastValidToken());
            assertEquals(token1, quorum1.token());
            assertEquals(token1, quorum2.token());

            // The pipeline order is the same as the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum0.getPipeline());
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum1.getPipeline());
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum2.getPipeline());
        }

        /*
         * Cast the last vote and verify that the last service joins.
         * 
         * Note: The last service should join immediately since it does not have
         * to do any validation when it joins.
         */
        {
            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();

            // services have voted for a single lastCommitTime.
            assertEquals(1, quorum0.getVotes().size());

            // verify the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum0.getVotes().get(lastCommitTime).toArray(new UUID[0]));

            // verify the consensus was NOT updated.
            assertEquals(lastCommitTime, client0.lastConsensusValue);
            assertEquals(lastCommitTime, client1.lastConsensusValue);
            assertEquals(lastCommitTime, client2.lastConsensusValue);

            /*
             * Service join in the same order in which they cast their votes.
             */
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum0.getJoinedMembers());
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum1.getJoinedMembers());
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum2.getJoinedMembers());

            // validate the token was NOT updated.
            assertEquals(token1, quorum0.lastValidToken());
            assertEquals(token1, quorum1.lastValidToken());
            assertEquals(token1, quorum2.lastValidToken());
            assertEquals(token1, quorum0.token());
            assertEquals(token1, quorum1.token());
            assertEquals(token1, quorum2.token());
            assertTrue(quorum0.isQuorumMet());
            assertTrue(quorum1.isQuorumMet());
            assertTrue(quorum2.isQuorumMet());

            // The pipeline order is the same as the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum0.getPipeline());
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum1.getPipeline());
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 },
                    quorum2.getPipeline());
        }

        /*
         * Follower leave/join test.
         */
        {

            /*
             * Fail the first follower. This will not cause a quorum break since
             * there are still (k+1)/2 services in the quorum.
             */
            actor1.serviceLeave();
            fixture.awaitDeque();

            // services have voted for a single lastCommitTime.
            assertEquals(1, quorum0.getVotes().size());

            // verify the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum0
                    .getVotes().get(lastCommitTime).toArray(new UUID[0]));

            // verify the consensus was NOT updated.
            assertEquals(lastCommitTime, client0.lastConsensusValue);
            assertEquals(lastCommitTime, client1.lastConsensusValue);
            assertEquals(lastCommitTime, client2.lastConsensusValue);

            /*
             * Service join in the same order in which they cast their votes.
             */
            assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum0
                    .getJoinedMembers());
            assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum1
                    .getJoinedMembers());
            assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum2
                    .getJoinedMembers());

            // validate the token was NOT updated.
            assertEquals(token1, quorum0.lastValidToken());
            assertEquals(token1, quorum1.lastValidToken());
            assertEquals(token1, quorum2.lastValidToken());
            assertEquals(token1, quorum0.token());
            assertEquals(token1, quorum1.token());
            assertEquals(token1, quorum2.token());
            assertTrue(quorum0.isQuorumMet());
            assertTrue(quorum1.isQuorumMet());
            assertTrue(quorum2.isQuorumMet());

            // The pipeline order is the same as the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum0
                    .getPipeline());
            assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum1
                    .getPipeline());
            assertEquals(new UUID[] { serviceId0, serviceId2 }, quorum2
                    .getPipeline());

            /*
             * Rejoin the service.
             */
            actor1.pipelineAdd();
            fixture.awaitDeque();
            actor1.castVote(lastCommitTime);
            fixture.awaitDeque();

            // services have voted for a single lastCommitTime.
            assertEquals(1, quorum0.getVotes().size());

            // verify the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
                    quorum0.getVotes().get(lastCommitTime).toArray(new UUID[0]));

            // verify the consensus was NOT updated.
            assertEquals(lastCommitTime, client0.lastConsensusValue);
            assertEquals(lastCommitTime, client1.lastConsensusValue);
            assertEquals(lastCommitTime, client2.lastConsensusValue);

            /*
             * Service join in the same order in which they cast their votes.
             */
            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
                    quorum0.getJoinedMembers());
            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
                    quorum1.getJoinedMembers());
            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
                    quorum2.getJoinedMembers());

            // validate the token was NOT updated.
            assertEquals(token1, quorum0.lastValidToken());
            assertEquals(token1, quorum1.lastValidToken());
            assertEquals(token1, quorum2.lastValidToken());
            assertEquals(token1, quorum0.token());
            assertEquals(token1, quorum1.token());
            assertEquals(token1, quorum2.token());
            assertTrue(quorum0.isQuorumMet());
            assertTrue(quorum1.isQuorumMet());
            assertTrue(quorum2.isQuorumMet());

            // The pipeline order is the same as the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
                    quorum0.getPipeline());
            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
                    quorum1.getPipeline());
            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
                    quorum2.getPipeline());

        }

        /*
         * Leader leave test.
         * 
         * This forces the quorum leader to do a serviceLeave(), which causes a
         * quorum break. All joined services should have left. Their votes were
         * withdrawn when they left and they were removed from the pipeline as
         * well.
         */
        {

            actor0.serviceLeave();
            fixture.awaitDeque();

            // the votes were withdrawn.
            assertEquals(0, quorum0.getVotes().size());

            // the consensus was cleared.
            assertEquals(-1L, client0.lastConsensusValue);
            assertEquals(-1L, client1.lastConsensusValue);
            assertEquals(-1L, client2.lastConsensusValue);

            // No one is joined.
            assertEquals(new UUID[] {}, quorum0.getJoinedMembers());
            assertEquals(new UUID[] {}, quorum1.getJoinedMembers());
            assertEquals(new UUID[] {}, quorum2.getJoinedMembers());

            // validate the token was cleared (lastValidToken is unchanged).
            assertEquals(token1, quorum0.lastValidToken());
            assertEquals(token1, quorum1.lastValidToken());
            assertEquals(token1, quorum2.lastValidToken());
            assertEquals(Quorum.NO_QUORUM, quorum0.token());
            assertEquals(Quorum.NO_QUORUM, quorum1.token());
            assertEquals(Quorum.NO_QUORUM, quorum2.token());
            assertFalse(quorum0.isQuorumMet());
            assertFalse(quorum1.isQuorumMet());
            assertFalse(quorum2.isQuorumMet());

            // No one is in the pipeline.
            assertEquals(new UUID[] {}, quorum0.getPipeline());
            assertEquals(new UUID[] {}, quorum1.getPipeline());
            assertEquals(new UUID[] {}, quorum2.getPipeline());

        }
        
        /*
         * Heal the quorum by rejoining all of the services.
         */
        final long token2;
        {

            actor0.pipelineAdd();
            actor1.pipelineAdd();
            actor2.pipelineAdd();
            fixture.awaitDeque();

            actor0.castVote(lastCommitTime);
            actor1.castVote(lastCommitTime);
            actor2.castVote(lastCommitTime);
            fixture.awaitDeque();
            
            // services have voted for a single lastCommitTime.
            assertEquals(1,quorum0.getVotes().size());
            
            // verify the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 }, quorum0
                    .getVotes().get(lastCommitTime).toArray(new UUID[0]));

            // verify the consensus was updated.
            assertEquals(lastCommitTime, client0.lastConsensusValue);
            assertEquals(lastCommitTime, client1.lastConsensusValue);
            assertEquals(lastCommitTime, client2.lastConsensusValue);

            /*
             * Service join in the same order in which they cast their votes.
             */
            assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum0.getJoinedMembers());
            assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum1.getJoinedMembers());
            assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum2.getJoinedMembers());

            // validate the token was updated.
            token2 = quorum0.token();
            assertEquals(token2, quorum0.lastValidToken());
            assertEquals(token2, quorum1.lastValidToken());
            assertEquals(token2, quorum2.lastValidToken());
            assertEquals(token2, quorum0.token());
            assertEquals(token2, quorum1.token());
            assertEquals(token2, quorum2.token());
            assertTrue(quorum0.isQuorumMet());
            assertTrue(quorum1.isQuorumMet());
            assertTrue(quorum2.isQuorumMet());

            // The pipeline order is the same as the vote order.
            assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum0.getPipeline());
            assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum1.getPipeline());
            assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum2.getPipeline());
            
        }

        /*
         * Cause the quorum to break by failing both of the followers. 
         */
        {
            
            /*
             * Fail one follower.  The quorum should not break.
             */
            actor2.serviceLeave();
            fixture.awaitDeque();
            
            // services have voted for a single lastCommitTime.
            assertEquals(1,quorum0.getVotes().size());
            
            // verify the vote order.
            assertEquals(new UUID[] { serviceId0, serviceId1 }, quorum0
                    .getVotes().get(lastCommitTime).toArray(new UUID[0]));

            // verify the consensus was NOT updated.
            assertEquals(lastCommitTime, client0.lastConsensusValue);
            assertEquals(lastCommitTime, client1.lastConsensusValue);
            assertEquals(lastCommitTime, client2.lastConsensusValue);

            /*
             * Service join in the same order in which they cast their votes.
             */
            assertEquals(new UUID[]{serviceId0,serviceId1},quorum0.getJoinedMembers());
            assertEquals(new UUID[]{serviceId0,serviceId1},quorum1.getJoinedMembers());
            assertEquals(new UUID[]{serviceId0,serviceId1},quorum2.getJoinedMembers());

            // validate the token was NOT updated.
            assertEquals(token2, quorum0.lastValidToken());
            assertEquals(token2, quorum1.lastValidToken());
            assertEquals(token2, quorum2.lastValidToken());
            assertEquals(token2, quorum0.token());
            assertEquals(token2, quorum1.token());
            assertEquals(token2, quorum2.token());
            assertTrue(quorum0.isQuorumMet());
            assertTrue(quorum1.isQuorumMet());
            assertTrue(quorum2.isQuorumMet());

            // The pipeline order is the same as the vote order.
            assertEquals(new UUID[]{serviceId0,serviceId1},quorum0.getPipeline());
            assertEquals(new UUID[]{serviceId0,serviceId1},quorum1.getPipeline());
            assertEquals(new UUID[]{serviceId0,serviceId1},quorum2.getPipeline());

            /*
             * Fail the remaining follower.  The quorum will break.
             */
            actor1.serviceLeave();
            fixture.awaitDeque();
            
            // services have voted for a single lastCommitTime.
            assertEquals(0,quorum0.getVotes().size());
            
            // verify the vote order.
            assertEquals(null, quorum0.getVotes().get(lastCommitTime));

            // verify the consensus was cleared.
            assertEquals(-1L, client0.lastConsensusValue);
            assertEquals(-1L, client1.lastConsensusValue);
            assertEquals(-1L, client2.lastConsensusValue);

            // no services are joined.
            assertEquals(new UUID[]{},quorum0.getJoinedMembers());
            assertEquals(new UUID[]{},quorum1.getJoinedMembers());
            assertEquals(new UUID[]{},quorum2.getJoinedMembers());

            // validate the token was cleared.
            assertEquals(token2, quorum0.lastValidToken());
            assertEquals(token2, quorum1.lastValidToken());
            assertEquals(token2, quorum2.lastValidToken());
            assertEquals(Quorum.NO_QUORUM, quorum0.token());
            assertEquals(Quorum.NO_QUORUM, quorum1.token());
            assertEquals(Quorum.NO_QUORUM, quorum2.token());
            assertFalse(quorum0.isQuorumMet());
            assertFalse(quorum1.isQuorumMet());
            assertFalse(quorum2.isQuorumMet());

            // Service leaves forced pipeline leaves.
            assertEquals(new UUID[]{},quorum0.getPipeline());
            assertEquals(new UUID[]{},quorum1.getPipeline());
            assertEquals(new UUID[]{},quorum2.getPipeline());

        }
        
    }

    /**
     * FIXME Write unit tests for pipeline reorganization when the leader is
     * elected. These tests need to have a pipeline order where the service
     * which will become the leader is not at the head of the pipeline. When the
     * leader is elected, the leader must cause the pipeline to be reorganized
     * such that the leader is at the head of the pipeline. In general, we can
     * not control the pipeline order imposed by the leader when it reorganizes
     * the pipeline and the leader that was aware of the network topology could
     * chose to reorganize the pipeline in order to optimize it even though the
     * leader was already at the head of the pipeline.
     */
    public void test_pipelineReorganization() {
        
        fail("write test");
        
    }
    
}
