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

import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumClient;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumFixtureClient;

import junit.framework.TestCase2;

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
     * A simple quorum run.
     */
    public void test_run1() {

        // The service replication factor.
        final int k = 3;
        // The per-client quorum objects.
        final MockQuorum[] quorums = new MockQuorum[k];
        final MockQuorumClient[] clients = new MockQuorumClient[k];
        // The mock shared quorum state object.
        final MockQuorumFixture fixture = new MockQuorumFixture(k);
        try {
 
            // run the fixture with a NOP client.
            fixture.start(new MockQuorumFixtureClient(fixture));

            /*
             * Setup the client quorums.
             */
            for (int i = 0; i < k; i++) {
                quorums[i] = new MockQuorum(k);
                clients[i] = new MockQuorumClient(quorums[i]);
                quorums[i].start(clients[i]);
                fixture.addQuorumToFixture(quorums[i]);
            }
            
            final UUID serviceId = UUID.randomUUID();
            fixture.memberAdd(serviceId);
            
        } finally {
            for(int i=0; i<k; i++) {
                if(quorums[i]!=null)
                    quorums[i].terminate();
//                if(clients[i]!=null) {
//                    clients[i].terminate();
//                }
            }
            fixture.terminate();
        }

    }

}
