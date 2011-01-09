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

package com.bigdata.quorum.zk;

import java.util.UUID;

import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumMember;

/**
 * Test the quorum semantics for a singleton quorum. This test suite allows us
 * to verify that each quorum state change is translated into the appropriate
 * methods against the public API of the quorum client or quorum member.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestSingletonQuorumSemantics.java 2984 2010-06-06 22:10:32Z
 *          thompsonbry $
 */
public class TestZkSingletonQuorumSemantics extends AbstractZkQuorumTestCase {

    /**
     * 
     */
    public TestZkSingletonQuorumSemantics() {
    }

    /**
     * @param name
     */
    public TestZkSingletonQuorumSemantics(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception {
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

        // client is a member.
        assertTrue(client.isMember());
        assertEquals(new UUID[] {serviceId}, quorum.getMembers());

        // instruct actor to remove client as a member.
        actor.memberRemove();
        
        // client is not a member.
        assertFalse(client.isMember());
        assertEquals(new UUID[] {}, quorum.getMembers());
        
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

		/*
		 * add to the pipeline. since this is a singleton quorum, the downstream
		 * service will remain null.
		 */
		assertNull(client.downStreamId);
        actor.pipelineAdd();

        assertNull(client.downStreamId);
        assertTrue(client.isPipelineMember());
        assertEquals(new UUID[]{serviceId},quorum.getPipeline());

		/*
		 * remove from the pipeline. since this is a singleton quorum, the
		 * downstream service will remain null.
		 */
		assertNull(client.downStreamId);
        actor.pipelineRemove();

        assertNull(client.downStreamId);
        assertFalse(client.isPipelineMember());
        assertEquals(new UUID[]{},quorum.getPipeline());

        actor.memberRemove();

        assertFalse(client.isMember());

    }

    /**
     * Unit test for the voting protocol for a singleton quorum.
     * 
     * @throws InterruptedException
     */
	public void test_voting() throws InterruptedException {

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

        assertTrue(clients[0].isMember());

        // join the pipeline.
        actor.pipelineAdd();
        
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
		
		// Should be just one vote.
		assertEquals(1,quorum.getVotes().size());

		// Verify the consensus was updated
		assertEquals(lastCommitTime1, client.lastConsensusValue);

		// Cast another vote.
		actor.castVote(lastCommitTime2);

        // Should be just one vote since a service can only vote for one
        // lastCommitTime at a time.
        if (quorum.getVotes().size() != 1) {
            assertEquals(quorum.getVotes().toString(), 1, quorum.getVotes()
                    .size());
        }

		// Verify the consensus was updated again.
		assertEquals(lastCommitTime2, client.lastConsensusValue);

		// Remove as a member.
        actor.memberRemove();

        assertFalse(clients[0].isMember());
        // The service vote was also removed.
        assertEquals(0,quorum.getVotes().size());
        
    }

    /**
     * Unit test for the protocol up to a service join, which triggers a leader
     * election. Since the singleton quorum has only one member our client will
     * be elected the leader.
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
        
        assertTrue(client.isMember());
        assertEquals(new UUID[]{serviceId},quorum.getMembers());
        
        // add to the pipeline.
        actor.pipelineAdd();

        assertTrue(client.isPipelineMember());
        assertEquals(new UUID[]{serviceId},quorum.getPipeline());

        // cast a vote for a lastCommitTime.
        actor.castVote(lastCommitTime);

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
        assertEquals(Quorum.NO_QUORUM + 1, quorum.lastValidToken());
        assertEquals(Quorum.NO_QUORUM + 1, quorum.token());
        assertTrue(quorum.isQuorumMet());
        
        /*
         * Do service leave, quorum should break. 
         */

        actor.serviceLeave();

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

        actor.castVote(lastCommitTime2);

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

}
