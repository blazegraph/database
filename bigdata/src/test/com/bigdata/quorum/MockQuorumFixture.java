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
    
//    /**
//     * The lock used by the {@link MockQuorumActor} to make atomic state changes
//     * in this class. All fields on this class are protected by this lock.
//     */
//    private final ReentrantLock lock = new ReentrantLock();
//    
//    private final Condition memberChange = lock.newCondition();
//
//    private final Condition pipelineChange = lock.newCondition();
//
//    private final Condition votesChange = lock.newCondition();
//
//    private final Condition joinedChange = lock.newCondition();
//
//    /**
//     * The current quorum token.
//     */
//    private long token = Quorum.NO_QUORUM;
//
//    /**
//     * The last valid token assigned to this quorum. This is updated by the
//     * leader when the quorum meets.
//     */
//    private long lastValidToken = Quorum.NO_QUORUM;
//
//    /**
//     * The service {@link UUID} of each service registered as a member of this
//     * quorum.
//     */
//    private final Set<UUID> members = new LinkedHashSet<UUID>();
//
//    /**
//     * Each service votes for its lastCommitTime when it starts and after the
//     * quorum breaks.
//     */
//    private final TreeMap<Long/*lastCommitTime*/,Set<UUID>> votes = new TreeMap<Long, Set<UUID>>();
//    
//    /**
//     * The services joined with the quorum in the order in which they join.
//     */
//    private final LinkedHashSet<UUID> joined = new LinkedHashSet<UUID>();
//
//    /**
//     * The ordered set of services in the write pipeline.
//     */
//    private final LinkedHashSet<UUID> pipeline = new LinkedHashSet<UUID>();

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
            
            /** NOP Actor. */
            @Override
            protected QuorumActor newActor(final UUID serviceId) {
                return new NOPQuorumFixtureActor();
            }

            @Override
            protected QuorumWatcherBase newWatcher() {
                return new QuorumWatcherBase() {
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
     * A do nothing actor used for the {@link MockQuorumFixture}'s inner quorum
     * object.
     */
    private class NOPQuorumFixtureActor implements QuorumActor {
        public void castVote(long lastCommitTime) {
        }

        public QuorumClient getQuorumMember() {
            return null;
        }

        public Quorum getQuourm() {
            return null;
        }

        public UUID getServiceId() {
            return null;
        }

        public void memberAdd() {
        }

        public void memberRemove() {
        }

        public void pipelineAdd() {
        }

        public void pipelineRemove() {
        }

        public void serviceJoin() {
        }

        public void serviceLeave() {
        }

        public void setToken(long newToken) {
        }

        public void withdrawVote() {
        }
        
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

    private void castVote(final UUID serviceId, long lastCommitTime) {
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

    private void setToken(final long newToken) {
        ((QuorumWatcherBase) quorumImpl.getWatcher()).setToken(newToken);
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
        protected QuorumActor newActor(final UUID serviceId) {
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
         * <p>
         * Note: This relies on the {@link QuorumEvent} mechanism. If there are
         * errors, they will be logged rather than propagated. This actually
         * mimics what happens if a client spams zookeeper with some bad data.
         * The errors will be observed in the QuorumWatcher of the clients
         * monitoring that quourm.
         */
        protected class MockQuorumActor implements QuorumActor {

            private final UUID serviceId;
            
            public MockQuorumActor(final UUID serviceId) {
                this.serviceId = serviceId;
            }
            
            public QuorumMember getQuorumMember() {
                return (QuorumMember) MockQuorum.this.getClient();
            }

            public Quorum getQuourm() {
                return MockQuorum.this;
            }

            public UUID getServiceId() {
                return serviceId;
            }

            public void memberAdd() {
                fixture.memberAdd(serviceId);
            }

            public void memberRemove() {
                fixture.memberRemove(serviceId);
            }

            public void castVote(long lastCommitTime) {
                fixture.castVote(serviceId,lastCommitTime);
            }

            public void withdrawVote() {
                fixture.withdrawVote(serviceId);
            }

            public void pipelineAdd() {
                fixture.pipelineAdd(serviceId);
            }

            public void pipelineRemove() {
                fixture.pipelineRemove(serviceId);
            }

            public void serviceJoin() {
                fixture.serviceJoin(serviceId);
            }

            public void serviceLeave() {
                fixture.serviceLeave(serviceId);
            }

            public void setToken(final long newToken) {
                fixture.setToken(newToken);
            }

        }

        /**
         * Watcher propagates state changes observed in the
         * {@link MockQuorumFixture} to the {@link MockQuorum}.
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
//                     * Event generated when a quorum breaks.
//                     */
//                case QUORUM_BROKE:
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
                    log.info("Ignoring : " + e);
                }

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
	 * 
	 *          FIXME Also track whether the client is a leader (true when
	 *          leaderElecte() and until leaderLeft()), whether the client is a
	 *          follower (true when followerElect() and until serviceLeave of
	 *          the client or leaderLeft/quorumBroke).
	 */
    static class MockQuorumMember<S extends Remote> extends
            AbstractQuorumMember<S> {

		/**
		 * The last lastCommitTime value around which a consensus was achieved.
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
            this.lastConsensusValue = Quorum.NO_QUORUM;
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
