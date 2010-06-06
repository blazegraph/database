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

import com.bigdata.journal.ha.QuorumException;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumMember;

/**
 * FIXME Test the quorum semantics for a singleton quorum. This test suite
 * allows us to verify that each quorum state change is translated into the
 * appropriate methods against the public API of the quorum client or quorum
 * member.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
     * Unit test verifies that the {@link QuorumMember} receives synchronous
     * state change messages when it is added to and removed from a
     * {@link Quorum}.
     */
    public void test_memberAddRemove() {
        
        assertFalse(clients[0].isMember());
        
        fixture.memberAdd(clients[0].getServiceId());
        
        assertTrue(clients[0].isMember());
        
        fixture.memberRemove(clients[0].getServiceId());
        
        assertFalse(clients[0].isMember());

    }

	/**
	 * Unit test verifies that the {@link QuorumMember} receives synchronous
	 * state change messages when it is added to and removed from a
	 * {@link Quorum}'s write pipeline.
	 * 
	 * @todo Tnis test also verifies that we need to be a member of the quorum
	 *       in order to be added to the write pipeline. If that changes, then
	 *       update this unit test.
	 * 
	 *       FIXME This should verify the pipeline change events (synchronous
	 *       messages informing the client of a change in its downstream service
	 *       in the write pipeline)!
	 */
    public void test_pipelineAddRemove() {

		final MockQuorumMember<?> client = clients[0];

//		final UUID serviceId = client.getServiceId();

		assertNull(client.downStreamId);
        assertFalse(client.isPipelineMember());

        try {
            fixture.pipelineAdd(client.getServiceId());
            fail("Expected " + QuorumException.class);
        } catch (QuorumException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        fixture.memberAdd(client.getServiceId());

		/*
		 * add to the pipeline. since this is a singleton quorum, the downstream
		 * service will remainin null.
		 */
		assertNull(client.downStreamId);
        fixture.pipelineAdd(client.getServiceId());
		assertNull(client.downStreamId);

        assertTrue(client.isPipelineMember());

		/*
		 * remove from the pipeline. since this is a singleton quorum, the
		 * downstream service will remainin null.
		 */
		assertNull(client.downStreamId);
        fixture.pipelineRemove(client.getServiceId());
		assertNull(client.downStreamId);
        
        assertFalse(client.isPipelineMember());

        fixture.memberRemove(client.getServiceId());

        assertFalse(client.isMember());

    }

	/**
	 * Unit test for the voting protocol for a singleton quorum.
	 */
	public void test_voting() {

    	final long lastCommitTime1 = 0L;

		final long lastCommitTime2 = 2L;

		// Verify that no consensus has been achieved yet.
		assertEquals(-1L, clients[0].lastConsensusValue);

		// service may not vote until it is added as a member.
		try {
			fixture.castVote(clients[0].getServiceId(), lastCommitTime1);
			fail("Expected " + QuorumException.class);
		} catch (QuorumException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

        fixture.memberAdd(clients[0].getServiceId());
        
        assertTrue(clients[0].isMember());

        // Verify that timestamps must be non-negative.
		try {
			fixture.castVote(clients[0].getServiceId(), -1L);
			fail("Expected " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		// Cast a vote.
		fixture.castVote(clients[0].getServiceId(), lastCommitTime1);

		// Verify the consensus was updated
		assertEquals(lastCommitTime1, clients[0].lastConsensusValue);

		// Cast another vote.
		fixture.castVote(clients[0].getServiceId(), lastCommitTime2);

		// Verify the consensus was updated again.
		assertEquals(lastCommitTime2, clients[0].lastConsensusValue);

        fixture.memberRemove(clients[0].getServiceId());
        
        assertFalse(clients[0].isMember());
        
    }

	/**
	 * Unit test for the protocol up to a service join, which triggers a leader
	 * election. Since the singleton quorum has only one member our client will
	 * be elected the leader.
	 * 
	 * @todo Work through when the service joins the pipeline. Normally, this is
	 *       when the service joins the quorum, right? Or if the quorum is
	 *       already met, then the service joins the pipeline as part of the
	 *       synchronization protocol before it can join the quorum.
	 */
	public void test_serviceJoin() {

		final MockQuorumMember<?> client = clients[0];
		final UUID serviceId = client.getServiceId();
		
		final long lastCommitTime = 0L;
		
		// declare the service as a quorum member.
		fixture.memberAdd(serviceId);
		
		// cast a vote for a lastCommitTime.
		fixture.castVote(serviceId, lastCommitTime);

		// verify the consensus was updated again.
		assertEquals(lastCommitTime, client.lastConsensusValue);

		// validate the token is not yet assigned.
		assertFalse(fixture.isQuorumMet());
		assertEquals(Quorum.NO_QUORUM,fixture.token());
		assertFalse(client.isJoinedMember(fixture.token()));
		
		// join the pipeline.
		fixture.pipelineAdd(serviceId);
		
		// join the quorum.
		fixture.serviceJoin(serviceId);

		/*
		 * FIXME use the QuorumActor pattern to have updateToken() when it is
		 * invoked on the client propagate that change through the fixture,
		 * which corresponds to the distributed quorum state. Verify that this
		 * is fixed by verifying the fixture state agrees with the client's
		 * quorum state.
		 */
		// validate the token was assigned by the leader.
		assertTrue(client.getQuorum().isQuorumMet());
		assertEquals(Quorum.NO_QUORUM + 1, client.getQuorum().token());
		assertTrue(client.isJoinedMember(client.getQuorum().token()));
		assertTrue(client.isLeader(client.getQuorum().token()));
		assertFalse(client.isFollower(client.getQuorum().token()));

	}

}
