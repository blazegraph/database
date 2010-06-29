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
import com.bigdata.quorum.MockQuorumFixture.MockQuorumMember;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum.MockQuorumActor;

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
    protected MockQuorumMember[] clients;

    /**
     * The per-client quorum actor objects. The unit tests send events on the
     * behalf of the clients using these actor objects.
     */
    protected MockQuorumActor[] actors;

    /** The mock shared quorum state object. */
    protected MockQuorumFixture fixture;
    
    @Override
    protected void setUp() throws Exception {

        super.setUp();
        
        if(log.isInfoEnabled())
            log.info(": " + getName());

        if (k == 0)
            throw new AssertionError("k is not set");
        
        quorums = new MockQuorum[k];
        
        clients = new MockQuorumMember[k];

        actors  = new MockQuorumActor[k];

        fixture = new MockQuorumFixture();

        fixture.start(); 

        /*
         * Setup the client quorums.
         */
        final String logicalServiceId = "testLogicalService1";
        for (int i = 0; i < k; i++) {
            quorums[i] = new MockQuorum(k,fixture);
            clients[i] = new MockQuorumMember(logicalServiceId);
            quorums[i].start(clients[i]);
            actors [i] = quorums[i].getActor();
        }
    
    }

    protected void tearDown() throws Exception {
        if(log.isInfoEnabled())
            log.info(": " + getName());
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
        actors  = null;
        fixture = null;
    }

}
