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

import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The shared state for a collection of services sharing the same mock quorum.
 * This class accepts quorum state changes from a unit test, notes each state
 * change in its internal state, and then distributing that state change message
 * to the registered {@link MockQuorum}s for that fixture. This simulates the
 * effect of a shared {@link Quorum} state under program control. Any
 * {@link QuorumClient} or {@link QuorumMember} will then see the quorum state
 * change events as interpreted by the {@link AbstractQuorum} as a series of
 * calls against their public APIs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockQuorumFixture<S extends Remote, C extends QuorumClient<S>, Q extends AbstractQuorum<S, C>>
        extends AbstractQuorum<S, C> {

    /**
     * @param k
     */
    protected MockQuorumFixture(final int k) {
        super(k);
    }

    /**
     * Register than client's quorum. This class will then distribute each
     * message it receives (serviceJoin(), etc) to its base class and then to
     * all of those quorum objects. This simulates the effect of having the
     * individual client's quorums observing a shared quorum state.
     * 
     * @param clientQuorum
     */
    public void addQuorumToFixture(MockQuorum clientQuorum) {
        clients.add(clientQuorum);
    }
    private final CopyOnWriteArrayList<MockQuorum> clients = new CopyOnWriteArrayList<MockQuorum>();


    /*
     * The visibility on these methods has been expanded so you can pump events
     * into the mock quorum from a unit test.
     */
    
    public void memberAdd(final UUID serviceId) {
        super.memberAdd(serviceId);
        for(MockQuorum client : clients) {
            client.memberAdd(serviceId);
        }
    }

    public void memberRemove(final UUID serviceId) {
        super.memberRemove(serviceId);
        for(MockQuorum client : clients) {
            client.memberRemove(serviceId);
        }
    }

    public void pipelineAdd(final UUID serviceId) {
        super.pipelineAdd(serviceId);
        for(MockQuorum client : clients) {
            client.pipelineAdd(serviceId);
        }
    }

    public void pipelineRemove(final UUID serviceId) {
        super.pipelineRemove(serviceId);
        for(MockQuorum client : clients) {
            client.pipelineRemove(serviceId);
        }
    }

    public void serviceJoin(final UUID serviceId) {
        super.serviceJoin(serviceId);
        for(MockQuorum client : clients) {
            client.serviceJoin(serviceId);
        }
    }

    public void serviceLeave(final UUID serviceId) {
        super.serviceLeave(serviceId);
        for(MockQuorum client : clients) {
            client.serviceLeave(serviceId);
        }
    }

    public void updateToken(final long newToken) {
        super.updateToken(newToken);
        for(MockQuorum client : clients) {
            client.updateToken(newToken);
        }
    }

    /**
     * Mock {@link Quorum} implementation with increased visibility of some
     * methods so we can pump state changes into the {@link MockQuorum}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static class MockQuorum extends AbstractQuorum {

        /**
         * @param k
         */
        protected MockQuorum(int k) {
            super(k);
        }
        
        /*
         * The visibility on these methods has been expanded so you can pump
         * events into the mock quorum from a unit test.
         */

        public void memberAdd(final UUID serviceId) {
            super.memberAdd(serviceId);
        }

        public void memberRemove(final UUID serviceId) {
            super.memberRemove(serviceId);
        }

        public void pipelineAdd(final UUID serviceId) {
            super.pipelineAdd(serviceId);
        }

        public void pipelineRemove(final UUID serviceId) {
            super.pipelineRemove(serviceId);
        }

        public void serviceJoin(final UUID serviceId) {
            super.serviceJoin(serviceId);
        }

        public void serviceLeave(final UUID serviceId) {
            super.serviceLeave(serviceId);
        }

        public void updateToken(final long newToken) {
            super.updateToken(newToken);
        }

    }
    
    /**
     * NOP client base used with the {@link MockQuorumFixture} class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MockQuorumFixtureClient extends AbstractQuorumClient {

        /**
         * @param quorum
         */
        protected MockQuorumFixtureClient(Quorum quorum) {
            super(quorum);
        }

        public Remote getService(UUID serviceId) {
            throw new UnsupportedOperationException();
        }
        
    }

    /**
     * NOP client base class used for the individual clients for each
     * {@link MockQuorum} registered with of a shared {@link MockQuorumFixture}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static class MockQuorumMember<S extends Remote> extends
            AbstractQuorumMember<S> {

        /**
         * @param quorum
         */
        protected MockQuorumMember(Quorum quorum) {
            super(quorum, UUID.randomUUID());
        }

        public S getService(UUID serviceId) {
            throw new UnsupportedOperationException();
        }

    }

}
