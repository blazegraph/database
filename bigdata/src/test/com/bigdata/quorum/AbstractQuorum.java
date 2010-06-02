package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.TimeoutException;
import com.bigdata.journal.ha.AsynchronousQuorumCloseException;
import com.bigdata.journal.ha.QuorumException;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.zookeeper.ZooKeeperAccessor;

/**
 * Abstract base class handles much of the logic for the distribution of RMI
 * calls from the leader to the follower and for the HA write pipeline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo The zookeeper implementation will need to pass in the
 *       {@link ZooKeeperAccessor} object so we can obtain a new zoookeeper
 *       session each time an old one expires, (re-)establish various watches,
 *       etc.
 * 
 * @todo Support the synchronization protocol, including joining with the quorum
 *       at the next commit point or (if there are no pending writes) as soon as
 *       the node is caught up.
 */
public class AbstractQuorum<S extends Remote, C extends QuorumClient<S>>
        implements Quorum<S, C> {

    static protected final Logger log = Logger.getLogger(AbstractQuorum.class);

    /**
     * The replication factor.
     * 
     * @todo In order to allow changes in the target replication factor, this
     *       field will have to become mutable and state changes in the field
     *       would then be protected by the {@link #lock}. For that reason,
     *       always prefer {@link #replicationFactor()} to direct access to this
     *       field.
     */
    private final int k;

    /**
     * The current quorum token. This is volatile and will be cleared as soon as
     * the leader fails or the quorum breaks.
     * 
     * @see Quorum#NO_QUORUM
     */
    private volatile long token;

    /**
     * The lock protecting state changes in the remaining fields and used to
     * provide {@link Condition}s used to await various states.
     */
    private final ReentrantLock lock = new ReentrantLock();

//    /**
//     * Condition signaled when a service member is added to the quorum.
//     * 
//     * @todo The condition variable is?
//     */
//    private final Condition memberAdd = lock.newCondition();
//
//    /**
//     * Condition signaled when a service member is removed from the quorum.
//     * 
//     * @todo The condition variable is?
//     */
//    private final Condition memberRemove = lock.newCondition();
//
//    /**
//     * Condition signaled when a service joins the quorum.
//     * 
//     * @todo The condition variable is?
//     */
//    private final Condition serviceJoin = lock.newCondition();
//
//    /**
//     * Condition signaled when joined services leaves the quorum.
//     * 
//     * @todo The condition variable is?
//     */
//    private final Condition serviceLeave = lock.newCondition();

//    /**
//     * Condition signaled when the leader is elected. The leader will notice
//     * this event and initiate the protocol for the quorum meet.
//     * <p>
//     * @todo The condition variable is??? leader!=null && lastLeader != leader
//     */
//    private final Condition leaderElect = lock.newCondition();
//
//    /**
//     * Condition signaled when the first joined service leaves. If the leader
//     * had been elected, then that is the leader. Whether the service is
//     * currently the leader depends on whether or not the quorum is met.
//     * 
//     * @todo The condition variable is??? leader==null && lastLeader!=null.
//     */
//    private final Condition leaderLeave = lock.newCondition();

    /**
     * Condition signaled when a quorum is fully met. The preconditions for this
     * event are:
     * <ul>
     * <li>At least (k+1)/2 services agree on the same lastCommitTime.</li>
     * <li>At least (k+1)/2 services have joined the quorum.</li>
     * <li>The first service to join the quorum is the leader and it has
     * assigned itself a new quorum token.</li>
     * <li>The remaining services which join are the followers are they have
     * copied the quorum token from the leader, which signals that they are
     * prepared to follow.</li>
     * <li>The leader has marked itself as read-only.</li>
     * <li>The leader has published the new quorum token.</li>
     * </ul>
     * The condition variable is <code> token != NO_QUORUM </code>. Since the
     * {@link #token()} is cleared as soon as the leader fails or the quorum
     * breaks, this is sufficient to detect a quorum meet.
     */
    private final Condition quorumMeet = lock.newCondition();

//    /**
//     * Condition signaled when the number of joined services falls beneath
//     * (k+1)/2. The quorum token will have been cleared to
//     * {@link Quorum#NO_QUORUM} before the lock is released. The condition
//     * variable is
//     * <code>lastValidToken != NO_QUORUM && token != lastValidToken</code>.
//     */
//    private final Condition quorumBreak = lock.newCondition();

    /**
     * Condition signaled when the ordered set of services comprising the write
     * pipeline is changed. Services must notice this event, locate themselves
     * within the pipeline, and inspect their downstream service in the pipeline
     * (if any). If the downstream, service has changed, then the service must
     * reconfigure itself appropriately to relay writes to the downstream
     * service (if any).
     * <p>
     * The condition variable in this case depends on the interpreter. For a
     * service in the write pipeline, the condition variable is its downstream
     * service. If that has changed, then the condition is satisfied.
     */
    private final Condition pipelineChange = lock.newCondition();

    /**
     * The last valid token assigned to this quorum. This is updated by the
     * leader when the quorum meets.
     */
    private long lastValidToken;

    /**
     * The service {@link UUID} of the leader and <code>null</code> if the
     * quorum is not met.
     */
    private UUID leader;

    /**
     * The service {@link UUID} of each service registered as a member of this
     * quorum.
     * 
     * @todo Is this identical to the set of physical services for a logical
     *       service or can there by physical services which are not quorum
     *       members, e.g., because they have been replaced by a hot spare?
     */
    private final Set<UUID> members;

    /**
     * Each service votes for a lastCommitTime when it starts and after it
     * leaves a quorum.
     */
    private final SortedSet<Vote> votes;
    
    /**
     * The services joined with the quorum in the order in which they join.
     */
    private final LinkedHashSet<UUID> joined;

    /**
     * The ordered set of services in the write pipeline. The first service in
     * this order MUST be the leader. The remaining services appear in the order
     * in which they enter the write pipeline. When a service leaves the write
     * pipeline, the upstream service consults the pipeline state to identify
     * its new downstream service (if any) and then queries that service for its
     * {@link PipelineState} so it may begin to transmit write cache blocks to
     * the downstream service. When a service joins the write pipeline, it
     * always joins as the last service in this ordered set.
     */
    private final LinkedHashSet<UUID> pipeline;

    /**
     * The {@link QuorumClient}.
     * 
     * @see #start(QuorumClient)
     */
    private C client;
    
    /**
     * A single threaded service used to pump events to clients outside of the
     * thread in which those events arise.
     */
    private ExecutorService eventService; 
    
    /**
     * Constructor, which MUST be followed by {@link #start()} to begin
     * operations.
     */
    protected AbstractQuorum(final int k) {
        
        if (k < 1)
            throw new IllegalArgumentException();

        if ((k % 2) == 0)
            throw new IllegalArgumentException("k must be odd: " + k);

        this.k = k;
        
        this.token = NO_QUORUM;

        members = new HashSet<UUID>(k);

        votes = new TreeSet<Vote>();

        joined = new LinkedHashSet<UUID>(k);

        // There can be more than [k] services in the pipeline.
        pipeline = new LinkedHashSet<UUID>(k * 2);

    }

    protected void finalize() throws Throwable {

        terminate();

        super.finalize();

    }

    /**
     * Begin asynchronous processing.
     */
    public void start(final C client) {
        if(client == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            if(this.client != null)
                throw new IllegalStateException();
            this.client = client;
            this.eventService = Executors
                    .newSingleThreadExecutor(new DaemonThreadFactory(
                            "QuorumEventService"));
            if (log.isDebugEnabled())
                log.debug("client=" + client);
        } finally {
            lock.unlock();
        }
    }

    public void terminate() {
        boolean interrupted = false;
        lock.lock();
        try {
            if(client == null) {
                // No client is attached.
                return;
            }
            if (log.isDebugEnabled())
                log.debug("client=" + client);
            eventService.shutdown();
            try {
                eventService.awaitTermination(1000, TimeUnit.MILLISECONDS);
            } catch(TimeoutException ex) {
                // Ignore.
            } catch(InterruptedException ex) {
                // Will be propagated below.
                interrupted = true;
            } finally {
                eventService = null;
            }
            if (client instanceof QuorumMember<?>) {
                /*
                 * @todo Issue events to the client telling it to leave the
                 * quorum and remove itself as a member service of the quorum?
                 * (I think not because these actions should be generated in
                 * response to observed changes in the shared quorum state. If a
                 * client simply terminates quorum processing, then it will no
                 * longer be informed of quorum state changes, which makes it a
                 * bad citizen unless it also shuts down, e.g., by terminating
                 * its zookeeper connection).
                 */
//                final UUID clientId = ((QuorumMember<S>) client).getServiceId();
//                if (joined.contains(clientId)) {
//                    log.error("Client is joined: " + clientId);
//                    // force service leave.
//                    serviceLeave(clientId);
//                }
//                if (pipeline.contains(clientId)) {
//                    log.error("Client in pipeline: " + clientId);
//                    // force pipeline remove.
//                    pipelineRemove(clientId);
//                }
//                if (members.contains(clientId)) {
//                    log.error("Client is member: " + clientId);
//                    // force member remove.
//                    memberRemove(clientId);
//                }
            }
            this.client = null;
        } finally {
            lock.unlock();
        }
        if(interrupted) {
            // Propagate the interrupt.
            Thread.currentThread().interrupt();
        }
    }

    public int replicationFactor() {
        // Note: [k] is final.
        return k;
    }

    final public boolean isHighlyAvailable() {
        return replicationFactor() > 1;
    }

    public long lastValidToken() {
        lock.lock();
        try {
            return lastValidToken;
        } finally {
            lock.unlock();
        }
    }

    public UUID[] getMembers() {
        lock.lock();
        try {
            return members.toArray(new UUID[0]);
        } finally {
            lock.unlock();
        }
    }

    public UUID[] getJoinedMembers() {
        lock.lock();
        try {
            return joined.toArray(new UUID[0]);
        } finally {
            lock.unlock();
        }
    }

    public UUID[] getPipeline() {
        lock.lock();
        try {
            return pipeline.toArray(new UUID[0]);
        } finally {
            lock.unlock();
        }
    }

    public UUID getLeaderId() {
        lock.lock();
        try {
            if(!isQuorumMet()) {
                return null;
            }
            return leader;
        } finally {
            lock.unlock();
        }
    }

    public long token() {
        // Note: volatile read.
        return token;
    }

    final public void assertQuorum(final long token) {
        if (token != NO_QUORUM && this.token == token) {
            return;
        }
        throw new QuorumException("Expected " + token + ", but is now "
                + this.token);
    }

    public boolean isQuorumMet() {
        return token != NO_QUORUM;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This watches the current token and will return as soon as the token is
     * valid.
     */
    public long awaitQuorum() throws InterruptedException,
            AsynchronousQuorumCloseException {
        lock.lock();
        try {
            while (!isQuorumMet() && client != null) {
                quorumMeet.await();
            }
            if(client == null)
                throw new AsynchronousQuorumCloseException();
            return token;
        } finally {
            lock.unlock();
        }
    }

    /*
     * Quorum state machine.
     * 
     * Note: These methods correspond to notice of a state change in the
     * distributed quorum state. In the zookeeper integration, these methods
     * will be invoked when a Watcher notices the appropriate state change in
     * zookeeper for the quorum. In a mock quorum implementation, these methods
     * may be invoked directly to simulate various state changes.
     */

    /**
     * Method is invoked when a member service is added to the quorum and
     * updates the internal state of the quorum to reflect that state change.
     * 
     * @param serviceId
     *            The service {@link UUID}.
     */
    protected void memberAdd(final UUID serviceId) {
        if (serviceId == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            if (members.add(serviceId)) {
                // service was added as quorum member.
//                memberAdd.signalAll();
//              if (client instanceof QuorumMember<?>) {
//              ((QuorumMember<?>) client).memberAdded(serviceId);
//          }
                // queue client event.
                sendEvent(new E(QuorumEventEnum.MEMBER_ADDED, token(), serviceId));
                if (log.isInfoEnabled())
                    log.info("serviceId=" + serviceId.toString());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Method is invoked when a member service is removed from the quorum and
     * updates the internal state of the quorum to reflect that state change.
     * 
     * @param serviceId
     *            The service {@link UUID}.
     */
    protected void memberRemove(final UUID serviceId) {
        if (serviceId == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            if (members.remove(serviceId)) {
                // service is no longer a member.
//                memberRemove.signalAll();
                // remove from the pipeline @todo can non-member services exist in the pipeline?
                pipelineRemove(serviceId);
                // service leave iff joined.
                serviceLeave(serviceId);
//                if (client instanceof QuorumMember<?>) {
//                    ((QuorumMember<?>) client).memberRemoved(serviceId);
//                }
                // queue client event.
                sendEvent(new E(QuorumEventEnum.MEMBER_REMOVED, token(), serviceId));
                if (log.isInfoEnabled())
                    log.info("serviceId=" + serviceId.toString());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Method is invoked when a service is added to the write pipeline and
     * updates the internal state of the quorum to reflect that state change.
     * 
     * @param serviceId
     *            The service {@link UUID}.
     */
    protected void pipelineAdd(final UUID serviceId) {
        if (serviceId == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            if (pipeline.add(serviceId)) {
                pipelineChange.signalAll();
                if (client instanceof QuorumMember<?>) {
                    ((QuorumMember<?>) client).pipelineChanged(serviceId);
                }
                // queue client event.
                sendEvent(new E(QuorumEventEnum.PIPELINE_ADDED, token(), serviceId));
                if (log.isInfoEnabled())
                    log.info("serviceId=" + serviceId.toString());
                }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Method is invoked when a service is removed from the write pipeline and
     * updates the internal state of the quorum to reflect that state change.
     * 
     * @param serviceId
     *            The service {@link UUID}.
     */
    protected void pipelineRemove(final UUID serviceId) {
        if (serviceId == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            if (pipeline.remove(serviceId)) {
                pipelineChange.signalAll();
                if (client instanceof QuorumMember<?>) {
                    ((QuorumMember<?>) client).pipelineChanged(serviceId);
                }
                // queue client event.
                sendEvent(new E(QuorumEventEnum.PIPELINE_REMOVED, token(), serviceId));
                if (log.isInfoEnabled())
                    log.info("serviceId=" + serviceId.toString());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Method is invoked when a service joins the quorum and updates the
     * internal state of the quorum to reflect that state change.
     * 
     * @param serviceId
     *            The service {@link UUID}.
     */
    protected void serviceJoin(final UUID serviceId) {
        if (serviceId == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            if (!members.contains(serviceId))
                throw new IllegalStateException("Not a member: " + serviceId);
            if (!pipeline.contains(serviceId))
                throw new IllegalStateException("Not in the pipeline: "
                        + serviceId);
            if (!joined.add(serviceId)) {
                // Already joined.
                return;
            }
            // another service has joined the quorum.
//            serviceJoin.signalAll();
            // queue client event.
            sendEvent(new E(QuorumEventEnum.SERVICE_JOINED, token(), serviceId));
            if (log.isInfoEnabled())
                log.info("serviceId=" + serviceId.toString());
            final int njoined = joined.size();
            final int k = replicationFactor();
            final boolean willMeet = njoined == (k + 1) / 2;
            if (willMeet) {
                /*
                 * The quorum will meet.
                 * 
                 * Note: The quorum is not met until the leader election has
                 * occurred and the followers are all lined up. This state only
                 * indicates that a meet will occur once those things happen.
                 */
                if (log.isInfoEnabled())
                    log.info("Quorum will meet: k=" + k + ", njoined="
                            + njoined);
            }
            if (!(client instanceof QuorumMember<?>)) {
                /*
                 * Since our client is a quorum member, figure out whether or
                 * not it is the leader, in which case it will do the leader
                 * election.
                 */
                final QuorumMember<S> client = (QuorumMember<S>) this.client;
                // the serviceId of the leader.
                final UUID leaderId = joined.iterator().next();
                // the serviceId of our client.
                final UUID clientId = client.getServiceId();
                // true iff our client is the leader.
                final boolean isLeader = leaderId.equals(clientId);
                if (isLeader) {
                    /*
                     * Our client will become the leader.
                     * 
                     * @todo Can we get away with simply assigning a new token
                     * and marking the client as read-write? The followers
                     * should already be in the right state (post-abort() and
                     * connected with the write pipeline) so once we have a
                     * token, all should be golden.
                     */
                    updateToken(lastValidToken + 1);
                    client.electedLeader(token);
                    sendEvent(new E(QuorumEventEnum.LEADER_ELECTED, token,
                            serviceId));
                    if (log.isInfoEnabled())
                        log.info("leader=" + leaderId + ", token=" + token);
                } else {
                    /*
                     * Our client will become a follower.
                     * 
                     * Note: We need to await the leader publishing the new
                     * token here. I am not quite sure how to handle an
                     * interrupt if one does occur while we are waiting.
                     * Presumably, we should simply ignore the interrupt and
                     * continue waiting unless the quorum is asynchronously
                     * closed by terminate(), which is what this code does.
                     */
                    long token = NO_QUORUM;
                    while (true) {
                        try {
                            token = awaitQuorum();
                            break;
                        } catch (AsynchronousQuorumCloseException e) {
                            throw e;
                        } catch (InterruptedException e) {
                            // Ignore and retry.
                            continue;
                        }
                    }
                    client.electedFollower(token);
                    sendEvent(new E(QuorumEventEnum.FOLLOWER_ELECTED, token,
                            serviceId));
                    if (log.isInfoEnabled())
                        log.info("leader=" + leaderId + ", token=" + token);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Method is invoked when a joined service leaves the quorum and updates the
     * internal state of the quorum to reflect that state change.
     * 
     * @param serviceId
     *            The service {@link UUID}.
     * 
     * @todo This is currently written to recognize a difference between a
     *       leader leave and a quorum break. The token is cleared in both
     *       cases. However, a quorum break occurs only when the #of services
     *       joined with the quorum falls below (k+1)/2. When the situation is
     *       only a leader leave and not also a quorum break, then a new leader
     *       should be elected very quickly from one of the other services
     *       joined with the quorum.
     */
    protected void serviceLeave(final UUID serviceId) {
        if (serviceId == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            if (!joined.remove(serviceId)) {
                // Not joined.
                return;
            }
            // another service has left the quorum.
//            serviceLeave.signalAll();
            // queue client event.
            sendEvent(new E(QuorumEventEnum.SERVICE_LEFT, token(), serviceId));
            if (log.isInfoEnabled())
                log.info("serviceId=" + serviceId.toString());
            final int njoined = joined.size();
            final int k = replicationFactor();
            // iff the quorum was joined.
            final boolean wasJoined = njoined == ((k + 1) / 2);
            // iff the quorum will break.
            final boolean willBreak = njoined == ((k + 1) / 2) - 1;
            // iff the leader just left the quorum.
            final boolean leaderLeft;
            if (wasJoined) {
                // the serviceId of the leader.
                final UUID leaderId = joined.iterator().next();
                // true iff the service which left was the leader.
                leaderLeft = leaderId.equals(serviceId);
                if(leaderLeft) {
                    /*
                     * While the quorum is may still be satisfied, the quorum
                     * token will be invalidated since it is associated with an
                     * elected leader and we just lost the leader.
                     * 
                     * Note: If there are still (k+1)/2 services joined with the
                     * quorum, then one of them will be elected and that event
                     * will be noticed by awaitQuorum(), which watches the
                     * current token. However, the initiative for that election
                     * lies _outside_ of this class. E.g., in the behavior of
                     * the service as a zookeeper client.
                     */
                    token = NO_QUORUM;
                }
            } else {
                leaderLeft = false;
            }
            if ((client instanceof QuorumMember<?>)) {
                final QuorumMember<S> client = (QuorumMember<S>) this.client;
                client.serviceLeft();
                if (wasJoined) {
                    // the serviceId of the leader.
                    final UUID leaderId = joined.iterator().next();
                    if (leaderLeft) {
                        /*
                         * Notify all quorum members that the leader left.
                         */
                        client.leaderLeft(leaderId);
                    }
                    /*
                     * Since our client is a quorum member, we need to tell it
                     * that it is no longer in the quorum.
                     */
                    if (willBreak) {
                        client.quorumBroke();
                        if (log.isInfoEnabled())
                            log.info("leader=" + leaderId + ", token=" + token);
                    }
                }
            }
            if (leaderLeft) {
                sendEvent(new E(QuorumEventEnum.LEADER_LEFT, token(), serviceId));
            }
            if (willBreak) {
                // The quorum will break.
                sendEvent(new E(QuorumEventEnum.QUORUM_BROKE, token(),
                        serviceId));
                if (log.isInfoEnabled())
                    log.info("Quorum will break: k=" + k + ", njoined="
                            + njoined);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Method is invoked when the leader publishes out a new quorum token.
     * 
     * @param newToken
     *            The new token.
     */
    protected void updateToken(final long newToken) {
        lock.lock();
        try {
            if (newToken <= lastValidToken)
                throw new RuntimeException("lastValidToken=" + lastValidToken
                        + ", but newToken=" + newToken);
            // save off the old value.
            this.lastValidToken = this.token;
            // save the new value.
            this.token = newToken;
            // signal everyone that the quorum has met.
            quorumMeet.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Send the listener an informative event outside of the thread in which we
     * actually process these events. Quorum events are relatively infrequent
     * and this design isolates the quorum state tracking logic from the
     * behavior of {@link QuorumListener}. The inner {@link Runnable} will block
     * waiting for the {@link #lock} before it sends the event, so clients will
     * not see events propagated unless they have been handled by this class
     * first.
     * 
     * @param e
     *            The event.
     */
    private void sendEvent(final QuorumEvent e) {
        final Executor executor = eventService;
        if (executor != null) {
            try {
                // Submit task to send the event.
                executor.execute(new Runnable() {
                    public void run() {
                        lock.lock();
                        try {
                            final QuorumClient<?> client = AbstractQuorum.this.client;
                            if (client != null) {
                                client.notify(e);
                            }
                        } catch (Throwable t) {
                            log.warn(t, t);
                        } finally {
                            lock.unlock();
                        }
                    }
                });
            } catch (RejectedExecutionException ex) {
                // ignore.
            }
        }
    }

    /**
     * Simple event impl.
     */
    private static class E implements QuorumEvent {

        private final QuorumEventEnum type;

        private final long token;

        private final UUID serviceId;

        public E(final QuorumEventEnum type, final long token,
                final UUID serviceId) {
            this.type = type;
            this.token = token;
            this.serviceId = serviceId;
        }

        public QuorumEventEnum getEventType() {
            return type;
        }

        public UUID getServiceId() {
            return serviceId;
        }

        public long token() {
            return token;
        }

        public String toString() {
            return "QuorumEvent" + "{type=" + type + ",token=" + token
                    + ",serviceId=" + serviceId + "}";
        }
        
    }
    
}
