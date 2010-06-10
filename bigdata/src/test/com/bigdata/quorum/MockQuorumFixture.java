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

import org.apache.log4j.Logger;

import com.bigdata.quorum.AbstractQuorum.QuorumWatcherBase;

/**
 * A mock object providing the shared quorum state for a set of
 * {@link QuorumClient}s running in the same JVM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockQuorumFixture {

    protected static final transient Logger log = Logger
            .getLogger(MockQuorumFixture.class);
    
    /**
     * The service replication factor.
     */
    private final int k;

    /**
     * The delegate which handles maintenance of the state and generation of
     * events.
     */
    private final AbstractQuorum quorumImpl;
    
    /**
     * @param k
     */
    protected MockQuorumFixture(final int k) {
        
        if (k < 1)
            throw new IllegalArgumentException();

        if ((k % 2) == 0)
            throw new IllegalArgumentException("k must be odd: " + k);

        this.k = k;

        this.quorumImpl = new AbstractQuorum(k) {
            
            /**
             * A do nothing actor used for the {@link MockQuorumFixture}'s inner quorum
             * object.
             */
            class NOPQuorumFixtureActor extends QuorumActorBase {

                public NOPQuorumFixtureActor(final UUID serviceId) {
                    super(serviceId);
                }

                @Override
                protected void doCastVote(long lastCommitTime) {
                }

                @Override
                protected void doMemberAdd() {
                    
                }

                @Override
                protected void doMemberRemove() {
                }

                @Override
                protected void doPipelineAdd() {
                }

                @Override
                protected void doPipelineRemove() {
                }

                @Override
                protected void doServiceJoin() {
                }

                @Override
                protected void doServiceLeave() {
                }

                @Override
                protected void doSetLastValidToken(long newToken) {
                }

                @Override
                protected void doSetToken() {
                }

                @Override
                protected void doClearToken() {
                }

                @Override
                protected void doWithdrawVote() {
                }

                @Override
                protected void reorganizePipeline() {
                }
                                
            };
            
            /** NOP Actor. */
            @Override
            protected QuorumActorBase newActor(final UUID serviceId) {
                return new NOPQuorumFixtureActor(serviceId);
            }

            @Override
            protected QuorumWatcherBase newWatcher() {
                return new QuorumWatcherBase() {

                    /** NOP. */
                    @Override
                    protected void setupDiscovery() {
                    }
                };
            }
        };
        
    } // constructor.

    /** Start fixture (mostly a hook). */
    void start() {

        // Start the quorum w/ a NOP client.
        quorumImpl.start(new NOPFixtureClient(quorumImpl));

    }

    
    /**
     * A NOP {@link QuorumClient} for the fixture's inner quorum object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class NOPFixtureClient extends AbstractQuorumClient {

        public NOPFixtureClient(Quorum quorum) {
            super(quorum);
        }
        
        @Override
        public Remote getService(UUID serviceId) {
            throw new UnsupportedOperationException();
        }

    }
    
    /** Terminate fixture (mostly a hook). */
    void terminate() {
        
        quorumImpl.terminate();
        
    }
    
    /*
     * State change methods are delegated to the QuorumWatcher of the inner
     * quorum object. That watcher will impose the state change on the private
     * quorum object, which will in turn send QuorumEvents to each registered
     * listener. Each MockQuorum has an inner QuorumWatch which listens for
     * those events. This simulates the distributed state change actor/watcher
     * pattern.
     */

    private void memberAdd(final UUID serviceId) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).memberAdd(serviceId);
    }

    private void memberRemove(final UUID serviceId) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).memberRemove(serviceId);
    }

    private void castVote(final UUID serviceId, final long lastCommitTime) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).castVote(serviceId,
                lastCommitTime);
    }

    private void withdrawVote(final UUID serviceId) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).withdrawVote(serviceId);
    }

    private void pipelineAdd(final UUID serviceId) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).pipelineAdd(serviceId);
    }

    private void pipelineRemove(final UUID serviceId) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).pipelineRemove(serviceId);
    }

    private void serviceJoin(final UUID serviceId) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).serviceJoin(serviceId);
    }

    private void serviceLeave(final UUID serviceId) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).serviceLeave(serviceId);
    }

    private void setLastValidToken(final long newToken) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).setLastValidToken(newToken);
    }
    
    private void setToken() {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).setToken();
    }
    
    private void clearToken() {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).clearToken();
    }
    
    /**
     * Mock {@link Quorum} implementation with increased visibility of some
     * methods so we can pump state changes into the {@link MockQuorumFixture}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id: MockQuorumFixture.java 2984 2010-06-06 22:10:32Z
     *          thompsonbry $
     */
    static class MockQuorum extends AbstractQuorum {

        private final MockQuorumFixture fixture;

        private volatile UUID _serviceId;

        protected MockQuorum(final MockQuorumFixture fixture) {

            super(fixture.k);
            
            this.fixture = fixture;
            
        }

        @Override
        protected QuorumActorBase newActor(final UUID serviceId) {
            return new MockQuorumActor(serviceId);
        }

        @Override
        protected QuorumWatcherBase newWatcher() {

            return new MockQuorumWatcher();
            
        }

        /**
         * Exposed to the unit tests which use the returned {@link QuorumActor}
         * to send state change requests to the {@link MockQuorumFixture}. From
         * there, they are noticed by the {@link MockQuorumWatcher} and become
         * visible to the client's {@link MockQuorum}.
         */
        public MockQuorumActor getActor() {
            return (MockQuorumActor)super.getActor();
        }
        
        public void start(QuorumMember client) {
            super.start(client);
            // cache the service UUID.
            _serviceId = client.getServiceId();
            // add our watcher as a listener to the fixture's inner quorum.
            fixture.quorumImpl.addListener((MockQuorumWatcher)getWatcher());
        }

        public void terminate() {
            final MockQuorumWatcher watcher = (MockQuorumWatcher) getWatcher();
            super.terminate();
            // clear the serviceId cache.
            _serviceId = null;
            // remove our watcher as a listener for the fixture's inner quorum.
            fixture.quorumImpl.removeListener(watcher);
        }

        /**
         * Actor updates the state of the {@link MockQuorumFixture}.
         */
        protected class MockQuorumActor extends QuorumActorBase {

            private final UUID serviceId;
            
            public MockQuorumActor(final UUID serviceId) {
                super(serviceId);
                this.serviceId = serviceId;
            }
           
            public void doMemberAdd() {
                fixture.memberAdd(serviceId);
            }

            public void doMemberRemove() {
                fixture.memberRemove(serviceId);
            }

            public void doCastVote(final long lastCommitTime) {
                fixture.castVote(serviceId, lastCommitTime);
            }

            public void doWithdrawVote() {
                fixture.withdrawVote(serviceId);
            }

            public void doPipelineAdd() {
                fixture.pipelineAdd(serviceId);
            }

            public void doPipelineRemove() {
                fixture.pipelineRemove(serviceId);
            }

            public void doServiceJoin() {
                fixture.serviceJoin(serviceId);
            }

            public void doServiceLeave() {
                fixture.serviceLeave(serviceId);
            }

            public void doSetLastValidToken(final long newToken) {
                fixture.setLastValidToken(newToken);
            }

            public void doSetToken() {
                fixture.setToken();
            }

            public void doClearToken() {
                fixture.clearToken();
            }

            /**
             * {@inheritDoc}
             * <p>
             * This implementation tunnels through to the fixture and makes the
             * necessary changes directly. Those changes will be noticed by the
             * {@link QuorumWatcher} implementations for the other clients in
             * the unit test.
             */
            @Override
            protected void reorganizePipeline() {
                final UUID[] pipeline = getPipeline();
                for (int i = 0; i < pipeline.length; i++) {
                    final UUID t = pipeline[i]; 
                    if (serviceId.equals(t)) {
                        // Done.
                        return;
                    }
                    fixture.pipelineRemove(t);
                    fixture.pipelineAdd(t);
                }
            }

        }

        /**
         * Watcher propagates state changes observed in the
         * {@link MockQuorumFixture} to the {@link MockQuorum}.
         * <p>
         * Note: This relies on the {@link QuorumEvent} mechanism. If there are
         * errors, they will be logged rather than propagated. This actually
         * mimics what happens if a client spams zookeeper with some bad data.
         * The errors will be observed in the QuorumWatcher of the clients
         * monitoring that quorum.
         */
        protected class MockQuorumWatcher extends QuorumWatcherBase implements
                QuorumListener {

            /** Propagate state change to our quorum. */
            public void notify(final QuorumEvent e) {

                if (log.isInfoEnabled())
                    log.info(e.toString());

                switch (e.getEventType()) {
                /**
                 * Event generated when a member service is added to a quorum.
                 */
                case MEMBER_ADDED: {
                    memberAdd(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a member service is removed form a
                     * quorum.
                     */
                case MEMBER_REMOVED: {
                    memberRemove(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a service is added to the write
                     * pipeline.
                     */
                case PIPELINE_ADDED: {
                    pipelineAdd(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a member service is removed from the
                     * write pipeline.
                     */
                case PIPELINE_REMOVED: {
                    pipelineRemove(e.getServiceId());
                    break;
                }
                    /**
                     * Vote cast by a service for some lastCommitTime.
                     */
                case VOTE_CAST: {
                    castVote(e.getServiceId(), e.lastCommitTime());
                    break;
                }
                    /**
                     * Vote for some lastCommitTime was withdrawn by a service.
                     */
                case VOTE_WITHDRAWN: {
                    withdrawVote(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a service joins a quorum.
                     */
                case SERVICE_JOINED: {
                    serviceJoin(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a service leaves a quorum.
                     */
                case SERVICE_LEFT: {
                    serviceLeave(e.getServiceId());
                    break;
                }
                case SET_LAST_VALID_TOKEN: {
                    setLastValidToken(e.lastValidToken());
                    break;
                }
                    /**
                     * Event generated when a quorum meets (used here to set the
                     * current token).
                     */
                case QUORUM_MEET: {
                    setToken();
                    break;
                }
              /**
              * Event generated when a quorum breaks (used here to clear the current token).
              */
              case QUORUM_BROKE: {
                  clearToken();
                  break;
              }
//                    /**
//                     * Event generated when a new leader is elected, including
//                     * when a quorum meets.
//                     */
//                case LEADER_ELECTED:
//                    /**
//                     * Event generated when a service joins a quorum as a
//                     * follower.
//                     */
//                case FOLLOWER_ELECTED:
//                    /**
//                     * Event generated when the leader leaves a quorum.
//                     */
//                case LEADER_LEFT:
//                    /**
//                     * A consensus has been achieved with <code>(k+1)/2</code>
//                     * services voting for some lastCommitTime. This event will
//                     * typically be associated with an invalid quorum token
//                     * since the quorum token is assigned when the leader is
//                     * elected and this event generally becomes visible before
//                     * the {@link #LEADER_ELECTED} event.
//                     */
//                case CONSENSUS:
                default:
                    if(log.isInfoEnabled())
                        log.info("Ignoring : " + e);
                }

            }

            /**
             * FIXME We really should scan the fixture's quorumImpl state using
             * getMembers(), getVotes(), getPipelineMembers(), getJoined(), and
             * token() and setup the client's quorum to mirror the state of the
             * fixture. This code could not be reused directly for zookeeper
             * because the watchers need to be setup atomically as we read the
             * distributed quorum state.
             */
            @Override
            protected void setupDiscovery() {
                if (log.isInfoEnabled())
                    log.info("");
            }

        }

    }
    
	/**
	 * NOP client base class used for the individual clients for each
	 * {@link MockQuorum} registered with of a shared {@link MockQuorumFixture}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id: MockQuorumFixture.java 2970 2010-06-03 22:21:22Z
	 *          thompsonbry $
	 */
    static class MockQuorumMember<S extends Remote> extends
            AbstractQuorumMember<S> {

        /**
         * The last lastCommitTime value around which a consensus was achieved
         * and initially -1L, but this is cleared to -1L each time the consensus
         * is lost.
         */
        protected volatile long lastConsensusValue = -1L;
        
        /**
         * The downstream service in the write pipeline.
         */
        protected volatile UUID downStreamId = null;
        
        /**
         * @param quorum
         */
        protected MockQuorumMember(MockQuorum quorum) {
            super(quorum, UUID.randomUUID());
        }

        /**
         * Strengthened return type
         */
        public MockQuorum getQuourm() {
            return (MockQuorum)super.getQuorum();
        }
        
        /**
         * Can not resolve services.
         */
        public S getService(UUID serviceId) {
            throw new UnsupportedOperationException();
        }

		/**
		 * {@inheritDoc}
		 * 
		 * Overridden to save the <i>lastCommitTime</i> on
		 * {@link #lastConsensusValue}.
		 */
        @Override
		public void consensus(long lastCommitTime) {
			super.consensus(lastCommitTime);
			this.lastConsensusValue = lastCommitTime;
		}

        @Override
        public void lostConsensus() {
            super.lostConsensus();
            this.lastConsensusValue = -1L;
        }
        
		/**
		 * {@inheritDoc}
		 * 
		 * Overridden to save the current downstream service {@link UUID} on
		 * {@link #downStreamId}
		 */
		public void pipelineChange(final UUID oldDownStreamId,
				final UUID newDownStreamId) {
			super.pipelineChange(oldDownStreamId, newDownStreamId);
			this.downStreamId = newDownStreamId;
		}

        /**
         * {@inheritDoc}
         * 
         * Overridden to clear the {@link #downStreamId}.
         */
		public void pipelineRemove() {
		    super.pipelineRemove();
		    this.downStreamId = null;
		}
		
	}

}
