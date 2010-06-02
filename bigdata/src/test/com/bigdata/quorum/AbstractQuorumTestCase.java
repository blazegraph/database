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

import junit.framework.TestCase2;

import com.bigdata.quorum.MockQuorumFixture.MockQuorum;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumClient;
import com.bigdata.quorum.MockQuorumFixture.MockQuorumFixtureClient;

/**
 * Abstract base class for testing using a {@link MockQuorumFixture}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractQuorumTestCase extends TestCase2 {

    public AbstractQuorumTestCase() {
        
    }

    public AbstractQuorumTestCase(String name) {
        super(name);
    }

    /** The service replication factor (this must be set in {@link #setupUp()}). */
    protected int k;

    /** The per-client quorum objects. */
    protected MockQuorum[] quorums;

    /** The clients. */
    protected MockQuorumClient[] clients;

    /** The mock shared quorum state object. */
    protected MockQuorumFixture fixture;

    @Override
    protected void setUp() throws Exception {

        super.setUp();
        
        if (k == 0)
            throw new AssertionError("k is not set");
        
        quorums = new MockQuorum[k];
        
        clients = new MockQuorumClient[k];
        
        fixture = new MockQuorumFixture(k);

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
    
    }

    protected void tearDown() throws Exception {
        if (quorums != null) {
            for (int i = 0; i < k; i++) {
                if (quorums[i] != null) {
                    quorums[i].terminate();
                    quorums[i] = null;
                }
            }
        }
        if (fixture != null) {
            fixture.terminate();
        }
        quorums = null;
        clients = null;
        fixture = null;
    }

}
