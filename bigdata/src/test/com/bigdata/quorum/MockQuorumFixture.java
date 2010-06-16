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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.quorum.MockQuorumFixture.MockQuorum.MockQuorumActor;
import com.bigdata.quorum.MockQuorumFixture.MockQuorum.MockQuorumWatcher;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A mock object providing the shared quorum state for a set of
 * {@link QuorumClient}s running in the same JVM.
 * <p>
 * This fixture dumps the events into queues drained by a per-watcher thread.
 * This approximates the zookeeper behavior and ensures that each watcher sees
 * the events in a total order. Zookeeper promises that all watchers proceed at
 * the same rate. We enforce that with a {@link #globalSynchronousLock}. Once
 * all watchers have drained the event, the next event is made available to the
 * watchers.
 * <p>
 * The fixture only generates events for actual state changes. This also mimics
 * the behavior of zookeeper.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockQuorumFixture {

    protected static final transient Logger log = Logger
            .getLogger(MockQuorumFixture.class);

    /**
     * Single threaded executor used to dispatch events to the {@link MockQuorumWatcher}s.
     */
    private ExecutorService dispatchService = null;

    /**
     * The set of registered listeners. Each listener will get each event. For
     * each event, the next listener will not get the event until it has been
     * handled by the previous listener. The {@link MockQuorumWatcher}s
     * collaborate to create this behavior.
     */
    private final CopyOnWriteArraySet<MockQuorumWatcher> listeners = new CopyOnWriteArraySet<MockQuorumWatcher>();
    
    /**
     * Lock used to force global synchronous event handling semantics.
     */
    private final Lock globalSynchronousLock = new ReentrantLock();

    /**
     * Condition used to await the watcher completing the handling of an event.
     */
    private final Condition eventDone = globalSynchronousLock.newCondition();

    /**
     * Deque of events from actors awaiting dispatch to
     * {@link MockQuorumWatcher}. The fixture and each {@link MockQuorumWatcher}
     * run a thread. The fixture's thread pumps each event into a local queue
     * for each {@link MockQuorumWatcher}. The watcher's thread takes an event
     * from the queue and interprets that event, creating a corresponding local
     * state change.
     */
    private final LinkedBlockingDeque<QuorumEvent> deque = new LinkedBlockingDeque<QuorumEvent>();
    
    /**
     * The lock protecting state changes in the remaining fields and used to
     * provide {@link Condition}s used to await various states.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition used to await an empty {@link #deque}.
     */
    private final Condition dequeEmpty = lock.newCondition();
    /**
     * Condition used to await a non-empty {@link #deque}.
     */
    private final Condition dequeNotEmpty = lock.newCondition();
    
    /**
     * The last valid quorum token.
     */
    private long lastValidToken = Quorum.NO_QUORUM;

    /**
     * The current quorum token.
     */
    private long token = Quorum.NO_QUORUM;

    /**
     * The service {@link UUID} of each service registered as a member of this
     * quorum.
     */
    private final LinkedHashSet<UUID> members = new LinkedHashSet<UUID>();

    /**
     * A map from collection of the distinct <i>lastCommitTimes</i> for which at
     * least one service has cast its vote to the set of services which have
     * cast their vote for that <i>lastCommitTime</i>, <em>in vote order</em>.
     */
    private final TreeMap<Long/* lastCommitTime */, LinkedHashSet<UUID>> votes = new TreeMap<Long, LinkedHashSet<UUID>>();

    /**
     * The services joined with the quorum in the order in which they join. This
     * MUST be a {@link LinkedHashSet} to preserve the join order.
     */
    private final LinkedHashSet<UUID> joined = new LinkedHashSet<UUID>();

    /**
     * The ordered set of services in the write pipeline. The
     * {@link LinkedHashSet} is responsible for preserving the pipeline order.
     * <p>
     * The first service in this order MUST be the leader. The remaining
     * services appear in the order in which they enter the write pipeline. When
     * a service leaves the write pipeline, the upstream service consults the
     * pipeline state to identify its new downstream service (if any) and then
     * queries that service for its {@link PipelineState} so it may begin to
     * transmit write cache blocks to the downstream service. When a service
     * joins the write pipeline, it always joins as the last service in this
     * ordered set.
     */
    private final LinkedHashSet<UUID> pipeline = new LinkedHashSet<UUID>();

    /**
     * An {@link Executor} which can be used by the unit tests.
     */
    private ExecutorService executorService;

    /**
     * A map from the serviceId to each {@link QuorumMember}.
     */
    private final ConcurrentHashMap<UUID, QuorumMember<?>> known = new ConcurrentHashMap<UUID, QuorumMember<?>>();
    
    /**
     * An {@link Executor} which can be used by the unit tests.
     * 
     * @see QuorumMember#getExecutor()
     */
    public Executor getExecutor() {
        return executorService;
    }

    /**
     * Resolve a known {@link QuorumMember} for the fixture.
     * 
     * @param serviceId
     *            The {@link UUID}for the {@link QuorumMember}'s service.
     * 
     * @return The {@link QuorumMember} -or- <code>null</code> if there is none
     *         known for that serviceId.
     */
    public QuorumMember<?> getMember(final UUID serviceId) {

        if (serviceId == null)
            throw new IllegalArgumentException();

        final QuorumMember<?> member = known.get(serviceId);

        return member;

    }

    /**
     * Resolve the service by its {@link UUID} for any service running against
     * this fixture.
     * 
     * @param serviceId
     *            The {@link UUID} for the service.
     * 
     * @return The service.
     * 
     * @throws IllegalArgumentException
     *             if there is no known {@link QuorumMember} for that serviceId.
     */
    public Object getService(final UUID serviceId) {

        final QuorumMember<?> member = getMember(serviceId);

        if (member == null)
            throw new IllegalArgumentException("Unknown: " + serviceId);

        return member.getService();

    }
    
    public MockQuorumFixture() {
    }

    /** Start fixture. */
    synchronized
    public void start() {

        token = lastValidToken = Quorum.NO_QUORUM;

        executorService = Executors
                .newCachedThreadPool(new DaemonThreadFactory("executorService"));

        dispatchService = Executors
                .newSingleThreadScheduledExecutor(new DaemonThreadFactory(
                        "dispatchService"));

        dispatchService.execute(new DispatcherTask());
        
    }
    
    /** Terminate fixture. */
    synchronized 
    public void terminate() {
        
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        
        if (dispatchService != null) {
            dispatchService.shutdownNow();
            dispatchService = null;
        }
        
    }

    private void assertRunning() {

        if (dispatchService == null)
            throw new IllegalStateException();
        
    }
    
    /**
     * Dispatches each event to each watcher in turn.  The event is not
     * dispatched to the next watcher until the current watcher is finished
     * with the event.
     */
    private class DispatcherTask implements Runnable {
        
        public void run() {
            while (true) {
                try {
                    runOnce();
                } catch (InterruptedException t) {
                    log.warn("Dispatcher exiting : " + t);
                } catch (Throwable t) {
                    log.error(t, t);
                }
            }
        }

        private void runOnce() throws Throwable {

            final QuorumEvent e;
            lock.lock();
            try {
                while(deque.isEmpty()) {
                    dequeNotEmpty.await();
                }
                /*
                 * Note: only peek for now so deque remains non-empty until we
                 * are done with this event.
                 */
                if ((e = deque.peek()) == null)
                    throw new AssertionError();
                log.warn("Next event: " + e);
            } finally {
                lock.unlock();
            }

            int i = 0;
            for (MockQuorumWatcher watcher : listeners) {
                globalSynchronousLock.lock();
                try {
                    if(log.isInfoEnabled())
                        log.info("Queuing event: " + e + " on listener#" + i);
                    // queue a single event.
                    watcher.queue.put(e);
                    // signal that an event is ready.
                    watcher.eventReady.signalAll();
                    // wait until the event has been drained.
                    while (!watcher.queue.isEmpty()) {
                        // yield until the watcher is done.
                        eventDone.await();
                    }
                } finally {
                    globalSynchronousLock.unlock();
                }
                i++;
            }

            lock.lock();
            try {
                // now take the event.
                if (e != deque.take())
                    throw new AssertionError();
                if (deque.isEmpty()) {
                    /*
                     * Signal if the deque is empty _and_ the event has been
                     * dispatched.
                     */
                    dequeEmpty.signalAll();
                }
            } finally {
                lock.unlock();
            }

        }

    }

    /**
     * Block until the event deque has been drained (that is, until all watchers
     * have handled all events which have already been generated). For example:
     * 
     * <pre>
     * actor.memberAdd();
     * actor.pipelineAdd();
     * </pre>
     * 
     * can throw an {@link QuorumException} since the actor's local quorum state
     * in all likelihood will not have been updated before we attempt to add the
     * actor to the pipeline in the distributed quorum state.
     * <p>
     * However, the following sequence will succeed.
     * 
     * <pre>
     * actor.memberAdd();
     * fixture.awaitDeque();
     * actor.pipelineAdd();
     * </pre>
     * 
     * @throws InterruptedException
     * 
     *             FIXME Make this method private and invoke it from the
     *             {@link MockQuorumActor} for each request which it issues such
     *             that the client never gets ahead of the observations of its
     *             own actions (or those of any other client).
     *             <p>
     *             The {@link #deque} must be unbounded since each action can
     *             trigger additional actions.
     *             <p>
     *             The logic in {@link MockQuorumWatcher#notify(QuorumEvent)}
     *             currently holds the {@link #globalSynchronousLock} while
     *             processing the event. In order for us to make this change it
     *             will have to locally queue the event and then process it
     *             later (or simply notify that a specific watched event has
     *             occurred and then inspect the quorum state to decide what the
     *             event was and how to handle it). If these changes are made
     *             then we can make {@link #lock} and alias for
     *             {@link #globalSynchronousLock}, make this method a NOP, and
     *             add a private variant which is invoked by the
     *             {@link MockQuorumActor} for each action which it takes. All
     *             of this would make the {@link MockQuorumFixture} more
     *             "zookeeper" like in its guarantee of global synchronous
     *             notification and the actions which clients must take to
     *             handle those notifications.
     */
    public void awaitDeque() throws InterruptedException {
        lock.lock();
        try {
            assertRunning();
            while (!deque.isEmpty()) {
                dequeEmpty.await();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Accept an event. Events are generated by the methods below which update
     * our internal state. Events are ONLY generated if the internal state is
     * changed by the request, and that state change is made atomically while
     * holding the {@link #lock}. This guarantees that we will not see duplicate
     * events arising from duplicate requests.
     * 
     * @param e
     *            The event.
     */
    private void accept(final QuorumEvent e) {
        lock.lock();
        try {
            assertRunning();
            if (false) {
                // stack trace so we can see who generated this event.
                log.warn("event=" + e, new RuntimeException("stack trace"));
            }
            deque.put(e);
            dequeNotEmpty.signalAll();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            lock.unlock();
        }
    }
    
    /*
     * Private methods implement the conditional tests on the local state an
     * invoke the protected methods which actually carry out the action to
     * effect that change in the distributed quorum state.
     * 
     * Note: These methods have pure semantics. The make the specified change,
     * and only that change, iff the current state is different and they pump
     * one event into the dispatcher if the change was made. These methods DO
     * NOT do things like withdraw a vote before casting a vote. That behavior
     * is the responsibility of the QuorumActor, which is part of what we are
     * testing here.
     */

    private void memberAdd(final UUID serviceId) {
        lock.lock();
        try {
            if (members.add(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                accept(new AbstractQuorum.E(QuorumEventEnum.MEMBER_ADD,
                        lastValidToken, token, serviceId));
            }
        } finally {
            lock.unlock();
        }
    }
    
    private void memberRemove(final UUID serviceId) {
        lock.lock();
        try {
            if (members.remove(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                accept(new AbstractQuorum.E(QuorumEventEnum.MEMBER_REMOVE,
                        lastValidToken, token, serviceId));
            }
        } finally {
            lock.unlock();
        }
    }

    private void castVote(final UUID serviceId, final long lastCommitTime) {
        lock.lock();
        try {
            LinkedHashSet<UUID> tmp = votes.get(lastCommitTime);
            if (tmp == null) {
                tmp = new LinkedHashSet<UUID>();
                votes.put(lastCommitTime, tmp);
            }
            if (tmp.add(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId + ",lastCommitTime="
                            + lastCommitTime);
                // Cast vote.
                accept(new AbstractQuorum.E(QuorumEventEnum.CAST_VOTE,
                        lastValidToken, token, serviceId, lastCommitTime));
            }
        } finally {
            lock.unlock();
        }
    }

    private void withdrawVote(final UUID serviceId) {
        lock.lock();
        try {
            // Search for and withdraw cast vote.
            final Iterator<Map.Entry<Long, LinkedHashSet<UUID>>> itr = votes
                    .entrySet().iterator();
            while (itr.hasNext()) {
                final Map.Entry<Long, LinkedHashSet<UUID>> entry = itr.next();
                final long lastCommitTime = entry.getKey();
                final Set<UUID> votes = entry.getValue();
                if (votes.remove(serviceId)) {
                    // Withdraw existing vote.
                    if (log.isDebugEnabled())
                        log.debug("serviceId=" + serviceId + ",lastCommitTime="
                                + lastCommitTime);
                    accept(new AbstractQuorum.E(QuorumEventEnum.WITHDRAW_VOTE,
                            lastValidToken, token, serviceId));
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void pipelineAdd(final UUID serviceId) {
        lock.lock();
        try {
            if (pipeline.add(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                accept(new AbstractQuorum.E(QuorumEventEnum.PIPELINE_ADD,
                        lastValidToken, token, serviceId));
            }
        } finally {
            lock.unlock();
        }
    }

    private void pipelineRemove(final UUID serviceId) {
        lock.lock();
        try {
            if (pipeline.remove(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                /*
                 * Remove the service from the pipeline.
                 */
                accept(new AbstractQuorum.E(QuorumEventEnum.PIPELINE_REMOVE,
                        lastValidToken, token, serviceId));
            }
        } finally {
            lock.unlock();
        }
    }

    private void serviceJoin(final UUID serviceId) {
        lock.lock();
        try {
            if (joined.add(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                accept(new AbstractQuorum.E(QuorumEventEnum.SERVICE_JOIN,
                        lastValidToken, token, serviceId));
            }
        } finally {
            lock.unlock();
        }
    }

    private void serviceLeave(final UUID serviceId) {
        lock.lock();
        try {
            if (joined.remove(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                accept(new AbstractQuorum.E(QuorumEventEnum.SERVICE_LEAVE,
                        lastValidToken, token, serviceId));
            }
        } finally {
            lock.unlock();
        }
    }

    private void setLastValidToken(final long newToken) {
        lock.lock();
        try {
            if (lastValidToken != newToken) {
                lastValidToken = newToken;
                if (log.isDebugEnabled())
                    log.debug("newToken=" + newToken);
                accept(new AbstractQuorum.E(
                        QuorumEventEnum.SET_LAST_VALID_TOKEN, lastValidToken,
                        token, null/* serviceId */));
            }
        } finally {
            lock.unlock();
        }
    }

    private void setToken() {
        lock.lock();
        try {
            if (token != lastValidToken) {
                token = lastValidToken;
                if (log.isDebugEnabled())
                    log.debug("newToken=" + token);
                accept(new AbstractQuorum.E(QuorumEventEnum.QUORUM_MEET,
                        lastValidToken, token, null/* serviceId */));
            }
        } finally {
            lock.unlock();
        }
    }

    private void clearToken() {
        lock.lock();
        try {
            if (token != Quorum.NO_QUORUM) {
                token = Quorum.NO_QUORUM;
                if (log.isDebugEnabled())
                    log.debug("");
                accept(new AbstractQuorum.E(QuorumEventEnum.QUORUM_BROKE,
                        lastValidToken, token, null/* serviceId */));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Mock {@link Quorum} implementation with increased visibility of some
     * methods so we can pump state changes into the {@link MockQuorumFixture2}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id: MockQuorumFixture.java 2984 2010-06-06 22:10:32Z
     *          thompsonbry $
     */
    static public class MockQuorum<S extends Remote, C extends QuorumMember<S>>
            extends AbstractQuorum<S, C> {

        private final MockQuorumFixture fixture;

        /**
         * A single threaded executor which drains the {@link #queue} and
         * submits each event to the {@link MockQuorumWatcher} to be
         * interpreted.
         * <p>
         * The task actually <em>peeks</em> at the {@link #queue} to get the
         * event and leaves the event on the {@link #queue} until it has been
         * executed, finally removing the event from the {@link #queue}. This
         * makes each of the watchers run in a strictly sequence, which mirrors
         * the zookeeper state change notification semantics.
         */
        private ExecutorService watcherService = null;

        public MockQuorum(final int k, final MockQuorumFixture fixture) {

            super(k);
            
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
         * to send state change requests to the {@link MockQuorumFixture2}. From
         * there, they are noticed by the {@link MockQuorumWatcher} and become
         * visible to the client's {@link MockQuorum}.
         */
        public MockQuorumActor getActor() {
            return (MockQuorumActor)super.getActor();
        }
        
        public void start(final C client) {
            super.start((C) client);
            // Start the service accepting events for the watcher.
            watcherService = Executors
                    .newSingleThreadScheduledExecutor(new DaemonThreadFactory(
                            "watcherService"));
            // The watcher.
            final MockQuorumWatcher watcher = (MockQuorumWatcher) getWatcher();
            // start the watcher task.
            watcherService.execute(new WatcherTask(watcher));
            // add our watcher as a listener to the fixture's inner quorum.
            fixture.listeners.add(watcher);
            // Save UUID -> QuorumMember mapping on the fixture.
            fixture.known.put(client.getServiceId(), client);
        }

        public void terminate() {
            final MockQuorumWatcher watcher = (MockQuorumWatcher) getWatcher();
            super.terminate();
            // Stop the service accepting events for the watcher.
            watcherService.shutdownNow();
            // remove our watcher as a listener for the fixture's inner quorum.
            fixture.listeners.remove(watcher);
        }

        /**
         * Accepts one event at a time and notifies the {@link DispatcherTask}
         * when we are done with it.
         */
        private class WatcherTask implements Runnable {
            
            final private MockQuorumWatcher watcher;
            
            public WatcherTask(final MockQuorumWatcher watcher) {
                
                this.watcher = watcher;
                
            }
            
            public void run() {
                while (true) {
                    try {
                        runOnce();
                    } catch (InterruptedException e) {
                        log.warn("Shutdown : " + e);
                    }
                }
            }

            private void runOnce() throws InterruptedException {
                // wait for the lock.
                fixture.globalSynchronousLock.lock();
                try {
                    // Wait for an event.
                    while(watcher.queue.isEmpty()) {
                        watcher.eventReady.await();
                    }
                    // blocking take.
                    final QuorumEvent e = watcher.queue.take();
                    if (log.isInfoEnabled())
                        log.info("Accepted event : " + e);
                    try {
                        // delegate the event.
                        watcher.notify(e);
                    } catch (Throwable t) {
                        // log an errors.
                        log.error(t, t);
                    }
                } finally {
                    // signal dispatcher that we are done.
                    fixture.eventDone.signalAll();
                    // release the lock.
                    fixture.globalSynchronousLock.unlock();
                }
            }
            
        } // class WatcherTask.
        
        /**
         * Actor updates the state of the {@link MockQuorumFixture2}.
         */
        protected class MockQuorumActor extends QuorumActorBase {

            private final UUID serviceId;
            
            public MockQuorumActor(final UUID serviceId) {
                super(serviceId);
                this.serviceId = serviceId;
            }
           
            protected void doMemberAdd() {
                fixture.memberAdd(serviceId);
            }

            protected void doMemberRemove() {
                fixture.memberRemove(serviceId);
            }

            protected void doCastVote(final long lastCommitTime) {
                fixture.castVote(serviceId, lastCommitTime);
            }

            protected void doWithdrawVote() {
                fixture.withdrawVote(serviceId);
            }

            protected void doPipelineAdd() {
                fixture.pipelineAdd(serviceId);
            }

            protected void doPipelineRemove() {
                fixture.pipelineRemove(serviceId);
            }

            protected void doServiceJoin() {
                fixture.serviceJoin(serviceId);
            }

            protected void doServiceLeave() {
                fixture.serviceLeave(serviceId);
            }

            protected void doSetLastValidToken(final long newToken) {
                fixture.setLastValidToken(newToken);
            }

            protected void doSetToken() {
                fixture.setToken();
            }

            protected void doClearToken() {
                fixture.clearToken();
            }

            /**
             * {@inheritDoc}
             * <p>
             * This implementation tunnels through to the fixture and makes the
             * necessary changes directly. Those changes will be noticed by the
             * {@link QuorumWatcher} implementations for the other clients in
             * the unit test.
             * <p>
             * Note: This operations IS NOT atomic. Each pipeline remove/add is
             * a separate atomic operation.
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
         * {@link MockQuorumFixture2} to the {@link MockQuorum}.
         * <p>
         * Note: This relies on the {@link QuorumEvent} mechanism. If there are
         * errors, they will be logged rather than propagated. This actually
         * mimics what happens if a client spams zookeeper with some bad data.
         * The errors will be observed in the QuorumWatcher of the clients
         * monitoring that quorum.
         */
        protected class MockQuorumWatcher extends QuorumWatcherBase implements
                QuorumListener {

            /**
             * The queue into which the fixture pumps events. This only needs a
             * capacity of ONE (1) because the fixture hands off the events
             * synchronously to each of the {@link MockQuorumWatcher}s.
             */
            private final BlockingQueue<QuorumEvent> queue = new LinkedBlockingQueue<QuorumEvent>();

            /**
             * Condition signaled when an event is ready for this watcher.
             */
            private final Condition eventReady = fixture.globalSynchronousLock.newCondition();
            
            /** Propagate state change to our quorum. */
            public void notify(final QuorumEvent e) {

                if (log.isInfoEnabled())
                    log.info(e.toString());

                switch (e.getEventType()) {
                /**
                 * Event generated when a member service is added to a quorum.
                 */
                case MEMBER_ADD: {
                    memberAdd(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a member service is removed form a
                     * quorum.
                     */
                case MEMBER_REMOVE: {
                    memberRemove(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a service is added to the write
                     * pipeline.
                     */
                case PIPELINE_ADD: {
                    pipelineAdd(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a member service is removed from the
                     * write pipeline.
                     */
                case PIPELINE_REMOVE: {
                    pipelineRemove(e.getServiceId());
                    break;
                }
                    /**
                     * Vote cast by a service for some lastCommitTime.
                     */
                case CAST_VOTE: {
                    castVote(e.getServiceId(), e.lastCommitTime());
                    break;
                }
                    /**
                     * Vote for some lastCommitTime was withdrawn by a service.
                     */
                case WITHDRAW_VOTE: {
                    withdrawVote(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a service joins a quorum.
                     */
                case SERVICE_JOIN: {
                    serviceJoin(e.getServiceId());
                    break;
                }
                    /**
                     * Event generated when a service leaves a quorum.
                     */
                case SERVICE_LEAVE: {
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
                    /*
                     * Note: These events do not carry any state change so we do
                     * not do anything with them here. These events will be
                     * generated by the watchers for each quorum as it accepts
                     * the state change events from the fixture.
                     */
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
     * {@link MockQuorum} registered with of a shared {@link MockQuorumFixture}
     * - you can actually use any {@link QuorumMember} implementation you like
     * with the {@link MockQuorumFixture}, not just this one. The implementation
     * you use DOES NOT have to be derived from this class. .
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
        protected MockQuorumMember() {
            
            super(UUID.randomUUID());
            
        }

//        /**
//         * Strengthened return type
//         */
//        public MockQuorum<S, QuorumMember<S>> getQuourm() {
//         
//            return (MockQuorum<S, QuorumMember<S>>) super.getQuorum();
//            
//        }
        
        /**
         * Can not resolve services (this functionality is not required for the
         * unit tests in the <code>com.bigdata.quorum</code> package.
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

        public Executor getExecutor() {
            throw new UnsupportedOperationException();
        }

        public S getService() {
            throw new UnsupportedOperationException();
        }
		
	}

    public String toString() {
        /*
         * Note: This must run w/o the lock to avoid deadlocks so there may be
         * visibility problems when accessing non-volatile member fields and the
         * data can be inconsistent if someone else is modifying it.
         */
        return super.toString() + //
        "{ lastValidToken="+lastValidToken+//
        ", token=" + token +//
        ", members="+Collections.unmodifiableCollection(members)+//
        ", pipeline="+Collections.unmodifiableCollection(pipeline)+//
        ", votes="+Collections.unmodifiableMap(votes)+//
        ", joined="+Collections.unmodifiableCollection(joined)+//
        ", listeners="+listeners+//
        ", deque="+deque+//
        "}";
    }
    
}
