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

import junit.framework.TestCase2;

import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumMember;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum.MockQuorumActor;

/**
 * Test suite for the {@link MockQuorumFixture}. This class is responsible for
 * accepting quorum state changes from a unit test, noting them in its internal
 * state, and then distributing those state change messages to the registered
 * {@link MockQuorum}s for that fixture. This simulates the effect of a shared
 * {@link Quorum} state under program control.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMockQuorumFixture extends TestCase2 {

    /**
     * 
     */
    public TestMockQuorumFixture() {
    }

    /**
     * @param name
     */
    public TestMockQuorumFixture(String name) {
        super(name);
    }

    /**
     * Tests of various illegal constructor calls.
     */
    public void test_ctor_correctRejection() {

        try {
            final int k = 2;
            final MockQuorumFixture fixture = new MockQuorumFixture();
            new MockQuorum(k, fixture);
            fail("Expected: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info(ex);
        }        

        try {
            final int k = 0;
            final MockQuorumFixture fixture = new MockQuorumFixture();
            new MockQuorum(k, fixture);
            fail("Expected: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info(ex);
        }        

        try {
            final int k = -1;
            final MockQuorumFixture fixture = new MockQuorumFixture();
            new MockQuorum(k, fixture);
            fail("Expected: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info(ex);
        }        

    }

    /**
     * Simple start()/terminate() test.
     */
    public void test_start_terminate() {

        final int k = 1;
        
        final String logicalServiceId = getName();
        
        // Create fixture providing mock of the distributed quorum state.
        final MockQuorumFixture fixture = new MockQuorumFixture();

        // Start the fixture.
        fixture.start();
        
        // Create a mock client for that fixture.
        final MockQuorum clientQuorum = new MockQuorum(k,fixture);
        
        // Run the client's quorum.
        clientQuorum.start(new MockQuorumMember(logicalServiceId));
        
        // Terminate the client's quorum.
        clientQuorum.terminate();

        // Terminate the fixture.
        fixture.terminate();
        
    }

    /**
     * A simple quorum run.
     * 
     * @throws InterruptedException
     */
    public void test_run1() throws InterruptedException {

        // The service replication factor.
        final int k = 3;
        // The logical service identifier.
        final String logicalServiceId = getName();
        // The per-client quorum objects.
        final MockQuorum[] quorums = new MockQuorum[k];
        final MockQuorumMember[] clients = new MockQuorumMember[k];
        final MockQuorumActor[] actors = new MockQuorumActor[k];
        // The mock shared quorum state object.
        final MockQuorumFixture fixture = new MockQuorumFixture();
        try {
 
            // run the fixture.
            fixture.start();

            /*
             * Setup the client quorums.
             */
            for (int i = 0; i < k; i++) {
                quorums[i] = new MockQuorum(k,fixture);
                clients[i] = new MockQuorumMember(logicalServiceId);
                quorums[i].start(clients[i]);
                actors [i] = quorums[i].getActor();
            }
            
            // The serviceId of the 1st client.
            final UUID serviceId = clients[0].getServiceId();
  
            // Verify none of the client quorums have any members.
            assertEquals(new UUID[]{},quorums[0].getMembers());
            assertEquals(new UUID[]{},quorums[1].getMembers());
            assertEquals(new UUID[]{},quorums[2].getMembers());

            // Verify the client is not a member of the quorum.
            assertFalse(clients[0].isMember());

            // tell the client's actor to add it as a quorum member.
            actors[0].memberAdd();
            fixture.awaitDeque();
            
            assertEquals(new UUID[]{serviceId},quorums[0].getMembers());
            assertEquals(new UUID[]{serviceId},quorums[1].getMembers());
            assertEquals(new UUID[]{serviceId},quorums[2].getMembers());

            // The client should now be a quorum member.
            assertTrue(clients[0].isMember());

        } finally {
            for (int i = 0; i < k; i++) {
                if (quorums[i] != null)
                    quorums[i].terminate();
            }
            fixture.terminate();
        }

    }

}
