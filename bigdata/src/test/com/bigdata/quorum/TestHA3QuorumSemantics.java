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
 *          FIXME We must also test with k:=5 in order to see some situations
 *          which do not appear with k:=3.
 * 
 *          FIXME THere will need to be unit tests for synchronization when a
 *          service wants to join a met quorum.
 * 
 *          FIXME There will need to be unit tests for hot spares.
 * 
 *          FIXME Write unit tests when services fail during a join and verify
 *          that the retraction of the service vote leads to either a failure of
 *          the quorum to meet or a met quorum once another vote is cast (that
 *          is, verify that the conformance of the vote order and the join order
 *          are preserved if the vote order is perturbed while services are
 *          joining).
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
     * @throws InterruptedException 
     */
    public void test_voting3() throws InterruptedException {

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
         * consensus (simple majority).
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

        // Remove another service as a member.  This should break the consensus.
        actor0.memberRemove();
        fixture.awaitDeque();
        
        // The last consensus on the mock clients should have been cleared.
        assertEquals(-1L, client0.lastConsensusValue);
        assertEquals(-1L, client1.lastConsensusValue);
        assertEquals(-1L, client2.lastConsensusValue);

        // Should be just one timestamps for which services have voted.
        assertEquals(1, quorum0.getVotes().size());
        assertEquals(1, quorum1.getVotes().size());
        assertEquals(1, quorum2.getVotes().size());

        // Verify the specific services voting for each timestamp.
        assertEquals(new UUID[] { serviceId2 }, quorum0.getVotes().get(
                lastCommitTime1).toArray(new UUID[] {}));
        assertEquals(null, quorum0.getVotes().get(lastCommitTime2));

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
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum2.getPipeline());

        /*
         * Have each service cast a vote for a lastCommitTime.
         */
        actor0.castVote(lastCommitTime);
//      fixture.awaitDeque(); // @todo might be necessary to await here too.
        actor1.castVote(lastCommitTime);
        fixture.awaitDeque();

        // services have voted for a single lastCommitTime.
        assertEquals(1,quorum0.getVotes().size());
        
        // verify the vote order.
        assertEquals(new UUID[] { serviceId0, serviceId1 }, quorum0.getVotes()
                .get(lastCommitTime).toArray(new UUID[0]));
        
        // verify the consensus was updated.
        assertEquals(lastCommitTime, client0.lastConsensusValue);
        assertEquals(lastCommitTime, client1.lastConsensusValue);
        assertEquals(lastCommitTime, client2.lastConsensusValue);

        /*
         * Service join in the same order in which they cast their votes.
         */
        assertEquals(new UUID[]{serviceId0,serviceId1},quorum0.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0,serviceId1},quorum1.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0,serviceId1},quorum2.getJoinedMembers());

        // validate the token was assigned.
        assertEquals(Quorum.NO_QUORUM + 1, quorum0.lastValidToken());
        assertEquals(Quorum.NO_QUORUM + 1, quorum0.token());
        assertTrue(quorum0.isQuorumMet());
        final long token1 = quorum0.token();
        assertEquals(token1,quorum1.lastValidToken());
        assertEquals(token1,quorum2.lastValidToken());
        assertEquals(token1,quorum1.token());
        assertEquals(token1,quorum2.token());        
        
        // The pipeline order is the same as the vote order.
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum0.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum1.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum2.getPipeline());

        /*
         * Cast the last vote.
         * 
         * Note: The last service should join immediately since it does not have
         * to do any validation when it joins.
         * 
         * FIXME Provide for a callback to acknowledge or reject a join so we
         * can verify the leader's root blocks in detail?
         */
        actor2.castVote(lastCommitTime);
        fixture.awaitDeque();

        // services have voted for a single lastCommitTime.
        assertEquals(1,quorum0.getVotes().size());
        
        // verify the vote order.
        assertEquals(new UUID[] { serviceId0, serviceId1, serviceId2 }, quorum0
                .getVotes().get(lastCommitTime).toArray(new UUID[0]));

        // verify the consensus was NOT updated.
        assertEquals(lastCommitTime, client0.lastConsensusValue);
        assertEquals(lastCommitTime, client1.lastConsensusValue);
        assertEquals(lastCommitTime, client2.lastConsensusValue);

        /*
         * Service join in the same order in which they cast their votes.
         */
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum0.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum1.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum2.getJoinedMembers());

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
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum0.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum1.getPipeline());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum2.getPipeline());

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
         * Leader leave/join test.
         * 
         * @todo after this, do a test where we fail 2 services to cause a
         * quorum break and then rejoin them to heal the quorum.
         */
        {

            /*
             * Fail the leader. This will cause a quorum break. All joined
             * services should have left. Their votes were withdrawn when they
             * left and they were removed from the pipeline as well.
             */
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

//            /*
//             * Heal the quorum.
//             */
//            actor1.pipelineAdd();
//            fixture.awaitDeque();
//            actor1.castVote(lastCommitTime);
//            fixture.awaitDeque();
//
//            // services have voted for a single lastCommitTime.
//            assertEquals(1, quorum0.getVotes().size());
//
//            // verify the vote order.
//            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
//                    quorum0.getVotes().get(lastCommitTime).toArray(new UUID[0]));
//
//            // verify the consensus was NOT updated.
//            assertEquals(lastCommitTime, client0.lastConsensusValue);
//            assertEquals(lastCommitTime, client1.lastConsensusValue);
//            assertEquals(lastCommitTime, client2.lastConsensusValue);
//
//            /*
//             * Service join in the same order in which they cast their votes.
//             */
//            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
//                    quorum0.getJoinedMembers());
//            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
//                    quorum1.getJoinedMembers());
//            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
//                    quorum2.getJoinedMembers());
//
//            // validate the token was NOT updated.
//            assertEquals(token1, quorum0.lastValidToken());
//            assertEquals(token1, quorum1.lastValidToken());
//            assertEquals(token1, quorum2.lastValidToken());
//            assertEquals(token1, quorum0.token());
//            assertEquals(token1, quorum1.token());
//            assertEquals(token1, quorum2.token());
//            assertTrue(quorum0.isQuorumMet());
//            assertTrue(quorum1.isQuorumMet());
//            assertTrue(quorum2.isQuorumMet());
//
//            // The pipeline order is the same as the vote order.
//            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
//                    quorum0.getPipeline());
//            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
//                    quorum1.getPipeline());
//            assertEquals(new UUID[] { serviceId0, serviceId2, serviceId1 },
//                    quorum2.getPipeline());

        }

    }
    
    /**
     * Unit test for service join/leave, including quorum meet and break.
     */
    public void test_serviceJoin3_messy() {

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
        
//        final long lastCommitTime1 = 0L;
//
//        final long lastCommitTime2 = 2L;

        final long lastCommitTime = 0L;

        // declare the services as a quorum members.
        actor0.memberAdd();
        actor1.memberAdd();
        actor2.memberAdd();

        /*
         * Have the services join the pipeline. Note that the pipeline order is
         * NOT the order in which we will have the services join the quorum.
         * This will force the AbstractQuorum to either elect the leader based
         * on the pipeline order and/or take pipeline remove/add actions. For
         * example, if there is a non-joined service at the head of the
         * pipeline, the we need to do a remove/add before the leader can be
         * elected since the leader always must be at the head of the write
         * pipeline.
         * 
         * This explicitly tests the case where we have a a non-joined service
         * in at the head of the write pipeline as well as the case where the
         * service join order is different from the pipeline order.
         */
        actor2.pipelineAdd();
        actor1.pipelineAdd();
        actor0.pipelineAdd();
        assertEquals(new UUID[]{serviceId2,serviceId1,serviceId0},quorum0.getPipeline());
        assertEquals(new UUID[]{serviceId2,serviceId1,serviceId0},quorum1.getPipeline());
        assertEquals(new UUID[]{serviceId2,serviceId1,serviceId0},quorum2.getPipeline());

        /*
         * Have each service cast a vote for a lastCommitTime.
         * 
         * Note: We could do a service join as soon as the 2nd service casts a
         * vote for the same last commit time since that would satisfy n :=
         * (k+1)/2. However, the service joins are deferred until all services
         * have cast their vote this time around. We erode and then break the
         * quorum below and then piece it back together again.
         */
        actor0.castVote(lastCommitTime);
        actor1.castVote(lastCommitTime);
        // verify the consensus was updated again.
        assertEquals(lastCommitTime, client0.lastConsensusValue);
        assertEquals(lastCommitTime, client1.lastConsensusValue);
        assertEquals(lastCommitTime, client2.lastConsensusValue);
        // cast the last vote.
        actor2.castVote(lastCommitTime);

        // validate the token is not yet assigned (no services have joined).
        assertFalse(quorum0.isQuorumMet());
        // No quorum token.
        assertEquals(Quorum.NO_QUORUM, quorum0.token());
        assertEquals(Quorum.NO_QUORUM, quorum1.token());
        assertEquals(Quorum.NO_QUORUM, quorum2.token());
        // verify no joined services.
        assertEquals(new UUID[]{},quorum0.getJoinedMembers());
        assertEquals(new UUID[]{},quorum1.getJoinedMembers());
        assertEquals(new UUID[]{},quorum2.getJoinedMembers());

        /*
         * Do service joins. The quorum should meet when the second service
         * joins.
         * 
         * FIXME We need to wait until we have (k+1)/2 services joined. At that
         * point, the leader will assign the quorum token and each
         * AbstractQourum will send out leaderElected(token,leaderId) events its
         * client.
         * 
         * We need to juggle things in order to get the pipeline and the leader
         * aligned. If the leader is not the first service in the pipeline, then
         * we need to cause the services prior to it in the pipeline to do a
         * pipeline remove/add until the leader is at the front of the pipeline.
         * This should not cause any problems because the pipeline is not doing
         * any work until the leader is assigned. [Perhaps we could defer the
         * pipeline assignments except that we need to allow services which are
         * not yet joined with the quorum into the pipeline for
         * synchronization.]
         * 
         * If the client is a follower, then it can also receive a
         * electedFollower() event. However, this event is not really critical.
         * The electedLeader() event is sufficient as long as the client knows
         * that it is joined with the quorum.
         * 
         * FIXME Ah. The way to handle this is with notification as each
         * upstream service is elected into the quorum (this is like zookeeper,
         * where everyone watches the previous znode in the queue). When a
         * service joins the quorum, it should fail any services ordered before
         * itself in the pipeline which are not yet joined. Since services only
         * enter the pipeline at the end, the first joined service will always
         * remain at the head of the pipeline - which is where we need it to be
         * when it is elected the leader.
         * 
         * When #of joined services rises to (k+1)/2 and the quorum will meet,
         * the first joined service is elected the leader. When the Quorum for
         * the next service in the joined service order see the leader election
         * event, it will issue a electedFollower() event to its client. When
         * that follower
         * 
         * FIXME Quorum#getLeaderId() is based on the join[] order. This means
         * that a leader who leaves the pipeline must also leave the joined
         * services. In fact, it is probably true that a pipeline leave must be
         * proceeded by a service leave for any join service, not just the
         * leader. Update the unit tests and AbstractQuorum for that.
         * 
         * FIXME Verify that we get followerElected() events both on meet and on
         * join after meet.
         */

        /*
         * The 1st service joins the quorum.
         * 
         * Note: This service is at the *end* of the pipeline when it joins the
         * quorum.
         */
        actor0.serviceJoin();
        // No quorum token.
        assertEquals(Quorum.NO_QUORUM, quorum0.token());
        assertEquals(Quorum.NO_QUORUM, quorum1.token());
        assertEquals(Quorum.NO_QUORUM, quorum2.token());
        // Last valid token was never set.
        assertEquals(Quorum.NO_QUORUM, quorum0.lastValidToken());
        assertEquals(Quorum.NO_QUORUM, quorum1.lastValidToken());
        assertEquals(Quorum.NO_QUORUM, quorum2.lastValidToken());
        // one joined service.
        assertEquals(new UUID[]{serviceId0},quorum0.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0},quorum1.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0},quorum2.getJoinedMembers());
        // verify no service is the leader.
        assertNull(quorum0.getLeaderId());
        assertNull(quorum1.getLeaderId());
        assertNull(quorum2.getLeaderId());
        
        // 2nd service joins the quorum.
        actor1.serviceJoin();
        // Quorum token was assigned.
        assertEquals(Quorum.NO_QUORUM+1, quorum0.token());
        assertEquals(Quorum.NO_QUORUM+1, quorum1.token());
        assertEquals(Quorum.NO_QUORUM+1, quorum2.token());
        // Last valid token was updated.
        assertEquals(Quorum.NO_QUORUM+1, quorum0.lastValidToken());
        assertEquals(Quorum.NO_QUORUM+1, quorum1.lastValidToken());
        assertEquals(Quorum.NO_QUORUM+1, quorum2.lastValidToken());
        // two joined services.
        assertEquals(new UUID[]{serviceId0,serviceId1},quorum0.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0,serviceId1},quorum1.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0,serviceId1},quorum2.getJoinedMembers());
        // verify which service is the leader.
        assertEquals(serviceId0,quorum0.getLeaderId());
        assertEquals(serviceId0,quorum1.getLeaderId());
        assertEquals(serviceId0,quorum2.getLeaderId());

        /*
         * The third service can join immediately because these mock services
         * are not accepting writes and do not know how to handle
         * synchronization.
         */
        // 3rd service joins the quorum.
        actor2.serviceJoin();
        // Quorum token is unchanged.
        assertEquals(Quorum.NO_QUORUM+1, quorum0.token());
        assertEquals(Quorum.NO_QUORUM+1, quorum1.token());
        assertEquals(Quorum.NO_QUORUM+1, quorum2.token());
        // Last valid token is unchanged.
        assertEquals(Quorum.NO_QUORUM+1, quorum0.lastValidToken());
        assertEquals(Quorum.NO_QUORUM+1, quorum1.lastValidToken());
        assertEquals(Quorum.NO_QUORUM+1, quorum2.lastValidToken());
        // three joined services.
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum0.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum1.getJoinedMembers());
        assertEquals(new UUID[]{serviceId0,serviceId1,serviceId2},quorum2.getJoinedMembers());
        // verify which service is the leader.
        assertEquals(serviceId0,quorum0.getLeaderId());
        assertEquals(serviceId0,quorum1.getLeaderId());
        assertEquals(serviceId0,quorum2.getLeaderId());

//        // validate the token was assigned by the leader.
//        final long token1 = quorum.token();
//        assertTrue(quorum.isQuorumMet());
//        assertEquals(Quorum.NO_QUORUM + 1, token1);
//        assertEquals(Quorum.NO_QUORUM + 1, quorum.lastValidToken());
//        assertTrue(client.isJoinedMember(token1));
//        assertTrue(client.isLeader(token1));
//        assertFalse(client.isFollower(token1));
//
//        /*
//         * Do service leave, quorum should break. 
//         */
//        
//        // service leave.
//        actor.serviceLeave();
//
//        // verify no joined services.
//        assertEquals(new UUID[]{},quorum.getJoinedMembers());
//
//        // verify the quorum broken.
//        assertFalse(quorum.isQuorumMet());
//        assertEquals(Quorum.NO_QUORUM, quorum.token());
//        assertEquals(token1, quorum.lastValidToken());
        
    }

}
