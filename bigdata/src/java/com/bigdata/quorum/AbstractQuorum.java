package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlue;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class handles much of the logic for the distribution of RMI
 * calls from the leader to the follower and for the HA write pipeline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Support the synchronization protocol, including joining with the quorum
 *       at the next commit point or (if there are no pending writes) as soon as
 *       the node is caught up.
 */
public abstract class AbstractQuorum<S extends Remote, C extends QuorumClient<S>>
        implements Quorum<S, C> {

    static protected final transient Logger log = Logger
            .getLogger(AbstractQuorum.class);

    /**
     * Text when an operation is not permitted because the service is not a
     * quorum member.
     */
    static protected final transient String ERR_NOT_MEMBER = "Not a quorum member : ";

    /**
     * Text when an operation is not permitted because the service is not part
     * of the write pipeline.
     */
    static protected final transient String ERR_NOT_PIPELINE = "Not a pipeline member : ";

    /**
     * Text when an operation is not permitted because the service has not cast
     * its vote for a lastCommitTime around which there is a consensus of
     * (k+1)/2 quorum members.
     */
    static protected final transient String ERR_NOT_IN_CONSENSUS = "Not in a quorum consensus : ";

    /**
     * Text when an operation is not permitted because the new value for the
     * lastValidToken is not strictly greater than the current value.
     */
    static protected final transient String ERR_BAD_TOKEN = "Bad token : ";

    /**
     * Text when an operation is not permitted while the quorum is met.
     */
    static protected final transient String ERR_QUORUM_MET = "Quorum is met : ";
    
    /**
     * Text when an operation is not permitted because the quorum can not meet.
     */
    static protected final transient String ERR_CAN_NOT_MEET = "Quorum can not meet : ";
    
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
     * The {@link QuorumListener}s.
     */
    private final CopyOnWriteArraySet<QuorumListener> listeners = new CopyOnWriteArraySet<QuorumListener>();

    /**
     * The lock protecting state changes in the remaining fields and used to
     * provide {@link Condition}s used to await various states. This is exposed
     * to concrete implementations of the {@link QuorumWatcherBase}.
     * 
     * @todo If we make updating the lastValidToken and the current token an
     *       atomic action when a leader is elected then we may be able to make
     *       this a private lock again.
     */
    protected final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition signaled when a quorum is fully met. The preconditions for this
     * event are:
     * <ul>
     * <li>At least (k+1)/2 services agree on the same lastCommitTime.</li>
     * <li>At least (k+1)/2 services have joined the quorum in their vote order.
     * </li>
     * <li>The first service to join the quorum is the leader and it has updated
     * the lastValidToken and the current token.</li>
     * </ul>
     * The condition variable is <code> token != NO_QUORUM </code>. Since the
     * {@link #token()} is cleared as soon as the leader fails or the quorum
     * breaks, this is sufficient to detect a quorum meet.
     */
    private final Condition quorumMeet = lock.newCondition();
    
    /**
     * Condition signaled when a quorum breaks.
     */
    private final Condition quorumBreak = lock.newCondition();

    /**
     * Condition signaled when a service UUID is added or removed from
     * {@link #members} collection by the {@link QuorumWatcherBase}.
     */
    private final Condition membersChange = lock.newCondition();
    
    /**
     * Condition signaled when a service {@link UUID} is added or removed from
     * {@link #pipeline} collection by the {@link QuorumWatcherBase}.
     */
    private final Condition pipelineChange = lock.newCondition();

    /**
     * Condition signaled when a service {@link UUID} is added or removed from
     * some entry of the {@link #votes} collection by the
     * {@link QuorumWatcherBase}.
     */
    private final Condition votesChange = lock.newCondition();

    /**
     * Condition signaled when a service {@link UUID} is added or removed from
     * {@link #joined} collection by the {@link QuorumWatcherBase}.
     */
    private final Condition joinedChange = lock.newCondition();

    /**
     * Condition signaled when the {@link #lastValidToken} is set by the
     * {@link QuorumWatcherBase}.
     * <p>
     * See {@link #quorumMeet} and {@link #quorumBreak} for the
     * {@link Condition}s pertaining to the current {@link #token}.
     */
    private final Condition lastValidTokenChange = lock.newCondition();
    
    /**
     * The last valid token assigned to this quorum. This is updated by the
     * leader when the quorum meets.
     */
    private long lastValidToken;

    /**
     * The service {@link UUID} of each service registered as a member of this
     * quorum.
     * 
     * @todo Is this identical to the set of physical services for a logical
     *       service or can there by physical services which are not quorum
     *       members, e.g., because they have been replaced by a hot spare?
     */
    private final LinkedHashSet<UUID> members;

    /**
     * A map from collection of the distinct <i>lastCommitTimes</i> for which at
     * least one service has cast its vote to the set of services which have
     * cast their vote for that <i>lastCommitTime</i>, <em>in vote order</em>.
     */
    private final TreeMap<Long/* lastCommitTime */, LinkedHashSet<UUID>> votes;

    /**
     * The services joined with the quorum in the order in which they join. This
     * MUST be a {@link LinkedHashSet} to preserve the join order.
     */
    private final LinkedHashSet<UUID> joined;

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
    private final LinkedHashSet<UUID> pipeline;

    /**
     * The {@link QuorumClient}.
     * 
     * @see #start(QuorumClient)
     */
    private C client;

    /**
     * An object which watches the distributed state of the quorum and informs
     * this quorum about distributed quorum state changes.
     */
    private QuorumWatcherBase watcher;

    /**
     * An object which causes changes in the distributed state of the quorum
     * (defined iff the client is a {@link QuorumMember} since non-members can
     * not affect the distributed quorum state).
     */
    private QuorumActorBase actor;

    /**
     * A service used by {@link QuorumWatcherBase} to execute actions outside of
     * the thread in which it handles the observed state change.
     */
    private ExecutorService watcherActionService;

    /**
     * A single threaded service used to pump events to clients outside of the
     * thread in which those events arise.
     */
    private ExecutorService eventService;

    /**
     * When true, events are send synchronously in the thread of the watcher.
     * 
     * @toso This makes it easy to write the unit tests since we do not need to
     *       "wait" for events to arrive (and they are much faster without the
     *       eventService contending for the {@link #lock} all the time). Is
     *       this Ok as a long standing policy? [I think so. These events are
     *       only going out to local objects.]
     *       <p>
     *       Note that the events are not guaranteed to arrive in the same order
     *       that the internal state changes are made.
     */
    private final boolean sendSynchronous = true;

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

        this.token = this.lastValidToken = NO_QUORUM;

        /*
         * Note: A linked hash set is used to make it easier on the unit tests
         * since the order in which the services are added to the quorum will be
         * maintained by the linked hash set. Otherwise we need to compare
         * sorted arrays.
         */
        members = new LinkedHashSet<UUID>(k);

        /*
         * Note: The TreeMap maintains data in order by ascending timestamp
         * which makes it easier to interpret.
         */
        votes = new TreeMap<Long, LinkedHashSet<UUID>>();

        joined = new LinkedHashSet<UUID>(k);

        // There can be more than [k] services in the pipeline.
        pipeline = new LinkedHashSet<UUID>(k * 2);

    }

    protected void finalize() throws Throwable {

        terminate();

        super.finalize();

    }

    /**
     * Begin asynchronous processing. The state of the quorum is reset, the
     * client is registered as a {@link QuorumListener}, the {@link QuorumActor}
     * and {@link QuorumWatcher} are created, and asynchronous discovery is
     * initialized for the {@link QuorumWatcher}.
     */
    public void start(final C client) {
        if (client == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            if (this.client != null)
                throw new IllegalStateException();
            // Clear -- must be discovered!
            this.token = this.lastValidToken = NO_QUORUM;
            // Note: Client is automatically a listener.
            this.client = client;
            // addListener(client);
            if (client instanceof QuorumMember<?>) {
                // create actor for that service.
                this.actor = newActor(client.getLogicalServiceId(),
                        ((QuorumMember<?>) client).getServiceId());
            }
            this.watcherActionService = Executors
                    .newCachedThreadPool(new DaemonThreadFactory(
                            "WatcherActionService"));
            this.watcher = newWatcher(client.getLogicalServiceId());
            this.eventService = (sendSynchronous ? null : Executors
                    .newSingleThreadExecutor(new DaemonThreadFactory(
                            "QuorumEventService")));
            if (log.isDebugEnabled())
                log.debug("client=" + client);
            /*
             * Let the service know that it is running w/ the quorum.
             * 
             * Note: We can not do this until everything is in place, but we
             * also must do this before the watcher setups up for discovery
             * since that could will sending messages to the client as it
             * discovers the distributed quorum state.
             */
            client.start(this);
            /*
             * Invoke hook for watchers. This gives them a chance to actually
             * discover the current quorum state, setup their listeners (e.g.,
             * zookeeper watchers), and pump deltas into the quorum.
             */
            watcher.start();
        } finally {
            lock.unlock();
        }
    }

    public void terminate() {
        boolean interrupted = false;
        lock.lock();
        try {
            if (client == null) {
                // No client is attached.
                return;
            }
            if (log.isDebugEnabled())
                log.debug("client=" + client);
            if (client instanceof QuorumMember<?>) {
                /*
                 * Update the distributed quorum state by removing our client
                 * from the set of member services. This will also cause a
                 * service leave, pipeline leave, and any vote to be withdrawn.
                 */
                actor.memberRemove();
            }
            /*
             * Let the service know that it is no longer running w/ the quorum.
             */
            client.terminate();
            watcher.terminate();
            if (watcherActionService != null) {
                watcherActionService.shutdown();
                try {
                    watcherActionService.awaitTermination(5000,
                            TimeUnit.MILLISECONDS);
                } catch (com.bigdata.concurrent.TimeoutException ex) {
                    // Ignore.
                } catch (InterruptedException ex) {
                    // Will be propagated below.
                    interrupted = true;
                } finally {
                    watcherActionService = null;
                }
            }
            if (!sendSynchronous) {
                eventService.shutdown();
                try {
                    eventService.awaitTermination(1000, TimeUnit.MILLISECONDS);
                } catch (com.bigdata.concurrent.TimeoutException ex) {
                    // Ignore.
                } catch (InterruptedException ex) {
                    // Will be propagated below.
                    interrupted = true;
                } finally {
                    eventService = null;
                }
            }
            this.client = null;
            // discard listeners.
            listeners.clear();
        } finally {
            lock.unlock();
        }
        if (interrupted) {
            // Propagate the interrupt.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Factory method invoked by {@link #start(QuorumClient)} iff the
     * {@link QuorumClient} is a {@link QuorumMember}.
     * 
     * @param logicalServiceId
     *            The identifier of the logical service corresponding to the
     *            highly available quorum.
     * @param serviceId
     *            The {@link UUID} of the service on whose behalf the actor will
     *            act.
     * 
     * @return The {@link QuorumActor} which will effect changes in the
     *         distributed state of the quorum.
     */
    abstract protected QuorumActorBase newActor(String logicalServiceId,
            UUID serviceId);

    /**
     * Factory method invoked by {@link #start(QuorumClient)}.
     * <p>
     * Note: Additional information can be passed to the watcher factor by
     * derived classes. For example, the {@link UUID} of the logical service to
     * 
     * @param logicalServiceId
     *            The identifier of the logical service whose quorum state
     *            will be watched.
     * 
     * @return The {@link QuorumWatcher} which will inform this
     *         {@link AbstactQuorum} of changes occurring in the distributed
     *         state of the quorum.
     */
    abstract protected QuorumWatcherBase newWatcher(String logicalServiceId);

    /**
     * Return a human readable representation of the local copy of the
     * distributed quorum state (non-blocking). The representation may be
     * inconsistent since the internal lock required for a consistent view is
     * NOT acquired.
     */
    public String toString() {
        /*
         * Note: This must run w/o the lock to avoid deadlocks so there may be
         * visibility problems when accessing non-volatile member fields and the
         * data can be inconsistent if someone else is modifying it.
         * 
         * @todo Can this result in concurrent modification exceptions even
         * though we are wrapping things as unmodifiable collections? Probably.
         * Maybe make this version private and directly access the collections
         * without wrapping them (but require the lock to be held) and expose a
         * version which acquires the lock to provide visibility for external
         * callers?
         */
        final QuorumClient<S> c = this.client;
        return super.toString() + //
        "{ k="+k+//
        ", lastValidToken="+lastValidToken+//
        ", token=" + token +//
        ", members="+Collections.unmodifiableCollection(members)+//
        ", pipeline="+Collections.unmodifiableCollection(pipeline)+//
        ", votes="+Collections.unmodifiableMap(votes)+//
        ", joined="+Collections.unmodifiableCollection(joined)+//
        ", client=" + (c == null ? "N/A" : c.getClass().getName()) + //
        ", serviceId="+(c instanceof QuorumMember<?>?((QuorumMember<?>)c).getServiceId():"N/A")+//
        ", listeners="+listeners+//
        "}";
    }

    /**
     * Return a human readable representation of an atomic snapshot of the local
     * copy of the distributed quorum state. This method acquires an internal
     * lock to provide the atomic semantics. Since acquiring lock could cause a
     * deadlock which would not otherwise arise, this method should be used
     * sparingly.
     */
    public String toStringAtomic() {
        lock.lock();
        try {
            return toString();
        } finally {
            lock.unlock();
        }
    }
    
    public C getClient() {
        lock.lock();
        try {
            if (this.client == null)
                throw new IllegalStateException();
            return client;
        } finally {
            lock.unlock();
        }
    }

    public QuorumMember<S> getMember() {
        lock.lock();
        try {
            if (this.client == null)
                throw new IllegalStateException();
            if (client instanceof QuorumMember<?>) {
                return (QuorumMember<S>) client;
            }
            throw new UnsupportedOperationException();
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Return the client cast to a {@link QuorumMember}.
     * <p>
     * Note: This is a private method designed to simply some constructs for the
     * {@link QuorumWatcherBase}. The caller MUST hold the {@link #lock} when
     * they invoke this method.
     * 
     * @return The {@link QuorumMember} -or- <code>null</code> if the client is
     *         not a {@link QuorumMember}. (If the quorum is not running, then
     *         the {@link #client} will be <code>null</code> and this method
     *         will return <code>null</code>).
     */
    private QuorumMember<S> getClientAsMember() {

        if (client instanceof QuorumMember<?>) {

            return (QuorumMember<S>) client;

        }

        return null;

    }

    /**
     * The object used to effect changes in distributed quorum state on the
     * behalf of the {@link QuorumMember}.
     * 
     * @return The {@link QuorumActor} which will effect changes in the
     *         distributed state of the quorum -or- <code>null</code> if the
     *         client is not a {@link QuorumMember} (only quorum members can
     *         take actions which effect the distributed quorum state).
     * 
     * @throws IllegalStateException
     *             if the quorum is not running.
     */
    public QuorumActor<S, C> getActor() {
        lock.lock();
        try {
            if (this.client == null)
                throw new IllegalStateException();
            return actor;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The {@link QuorumWatcher} which informs this {@link AbstactQuorum} of
     * changes occurring in the distributed state of the quorum.
     * 
     * @throws IllegalStateException
     *             if the quorum is not running.
     */
    protected QuorumWatcher<S, C> getWatcher() {
        lock.lock();
        try {
            if (this.client == null)
                throw new IllegalStateException();
            return watcher;
        } finally {
            lock.unlock();
        }
    }

    public void addListener(final QuorumListener listener) {
        if (listener == null)
            throw new IllegalArgumentException();
        if (listener == client)
            throw new IllegalArgumentException();
        listeners.add(listener);
    }

    public void removeListener(final QuorumListener listener) {
        if (listener == null)
            throw new IllegalArgumentException();
        if (listener == client)
            throw new IllegalArgumentException();
        listeners.remove(listener);
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

    public Map<Long, UUID[]> getVotes() {
        lock.lock();
        try {
            /*
             * Create a temporary map for the snapshot.
             * 
             * Note: A linked hash map will preserve the view order and is
             * faster than a TreeMap.
             */
            final Map<Long, UUID[]> tmp = new LinkedHashMap<Long, UUID[]>();
            final Iterator<Long> itr = votes.keySet().iterator();
            while (itr.hasNext()) {
                final Long lastCommitTime = itr.next();
                tmp.put(lastCommitTime, votes.get(lastCommitTime).toArray(
                        new UUID[0]));
            }
            return tmp;
        } finally {
            lock.unlock();
        }
    }

    public Long getCastVote(final UUID serviceId) {
        lock.lock();
        try {
            final Iterator<Map.Entry<Long, LinkedHashSet<UUID>>> itr = votes
                    .entrySet().iterator();
            while (itr.hasNext()) {
                final Map.Entry<Long, LinkedHashSet<UUID>> entry = itr.next();
                final Long lastCommitTime = entry.getKey();
                final Set<UUID> votes = entry.getValue();
                if (votes.contains(serviceId)) {
                    return lastCommitTime;
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /*
     * Helper methods.
     */

    /**
     * Search for the vote for the service.
     * 
     * @param serviceId
     *            The service identifier.
     * 
     * @return The lastCommitTime for which the service has cast its vote -or-
     *         <code>null</code> if the service is not participating in a
     *         consensus of at least <code>(k+1)/2</code> services.
     */
    private Long getLastCommitTimeConsensus(final UUID serviceId) {
        final Iterator<Map.Entry<Long, LinkedHashSet<UUID>>> itr = votes
                .entrySet().iterator();
        while (itr.hasNext()) {
            final Map.Entry<Long, LinkedHashSet<UUID>> entry = itr.next();
            final Set<UUID> votes = entry.getValue();
            if (votes.contains(serviceId)) {
                return entry.getKey().longValue();
            }
        }
        // Service is not part of a consensus.
        return null;
    }

//    /**
//     * Search for the vote for the service.
//     * 
//     * @param serviceId
//     *            The service identifier.
//     * 
//     * @return The lastCommitTime for which the service has cast its vote.
//     * 
//     * @throws QuorumException
//     *             unless the service has cast its vote for the consensus.
//     */
//    private long assertVotedForConsensus(final UUID serviceId) {
//        final Long lastCommitTime = getLastCommitTimeConsensus(serviceId);
//        if (lastCommitTime == null) {
//            throw new QuorumException(ERR_NOT_IN_CONSENSUS + serviceId);
//        }
//        return lastCommitTime.longValue();
//    }

    /**
     * Return the index of the service in the vote order.
     * 
     * @param voteOrder
     *            An ordered set of votes.
     * 
     * @return The index of the service in that vote order -or- <code>-1</code>
     *         if the service does not appear in the vote order.
     */
    private int getIndexInVoteOrder(final UUID serviceId, final UUID[] voteOrder) {
        for (int i = 0; i < voteOrder.length; i++) {
            if (voteOrder[i].equals(serviceId)) {
                return i;
            }
        }
        return -1;
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

    public UUID getLastInPipeline() {
        lock.lock();
        try {
            final Iterator<UUID> itr = pipeline.iterator();
            UUID lastId = null;
            while (itr.hasNext()) {
                lastId = itr.next();
            }
            return lastId;
        } finally {
            lock.unlock();
        }
    }

    public UUID[] getPipelinePriorAndNext(final UUID serviceId) {
        if (serviceId == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            final Iterator<UUID> itr = pipeline.iterator();
            UUID priorId = null;
            while (itr.hasNext()) {
                final UUID current = itr.next();
                if (serviceId.equals(current)) {
                    /*
                     * Found the caller's service in the pipeline so we return
                     * the prior service, which is its upstream, service, and
                     * the next service, which is its downstream service.
                     */
                    final UUID nextId = itr.hasNext() ? itr.next() : null;
                    return new UUID[] { priorId, nextId };
                }
                priorId = current;
            }
            // The caller's service was not in the pipeline.
            return null;
        } finally {
            lock.unlock();
        }
    }

    public UUID getLeaderId() {
        lock.lock();
        try {
            if (!isQuorumMet()) {
                return null;
            }
            // Should always exist if the quorum is met.
            return joined.iterator().next();
        } finally {
            lock.unlock();
        }
    }

    final public long token() {
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

    final public void assertLeader(final long token) {
        if (this.token == NO_QUORUM) {
            // The quorum is not met.
            throw new QuorumException();
        }
        final UUID leaderId = getLeaderId();
        final QuorumMember<S> client = getClientAsMember();
        if (client == null) {
            // Our client is not a QuorumMember (so can not join the quorum).
            throw new QuorumException();
        }
        if (!leaderId.equals(client.getServiceId())) {
            // Our client is not the leader.
            throw new QuorumException();
        }
        // Our client is the leader, now verify quorum is still valid.
        assertQuorum(token);
    }

    final public boolean isQuorumMet() {
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
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            return token;
        } finally {
            lock.unlock();
        }
    }

    public long awaitQuorum(final long timeout, final TimeUnit units)
            throws InterruptedException, TimeoutException,
            AsynchronousQuorumCloseException {
        final long begin = System.nanoTime();
        long nanos = units.toNanos(timeout);
        if(!lock.tryLock(nanos, TimeUnit.NANOSECONDS))
            throw new TimeoutException();
        try {
            // remaining -= (now - begin) [aka elapsed]
            nanos -= System.nanoTime() - begin;
            while (!isQuorumMet() && client != null) {
                if(!quorumMeet.await(nanos,TimeUnit.NANOSECONDS))
                    throw new TimeoutException();
            }
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            return token;
        } finally {
            lock.unlock();
        }
    }

    public void awaitBreak() throws InterruptedException,
            AsynchronousQuorumCloseException {
        lock.lock();
        try {
            while (isQuorumMet() && client != null) {
                quorumBreak.await();
            }
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            return;
        } finally {
            lock.unlock();
        }
    }

    public void awaitBreak(final long timeout, final TimeUnit units)
            throws InterruptedException, TimeoutException,
            AsynchronousQuorumCloseException {
        final long begin = System.nanoTime();
        long nanos = units.toNanos(timeout);
        if (!lock.tryLock(nanos, TimeUnit.NANOSECONDS))
            throw new TimeoutException();
        try {
            // remaining -= (now - begin) [aka elapsed]
            nanos -= System.nanoTime() - begin;
            while (isQuorumMet() && client != null) {
                if (!quorumBreak.await(nanos, TimeUnit.NANOSECONDS))
                    throw new TimeoutException();
            }
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            return;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Base class for {@link QuorumActor} implementations. The methods on this
     * base class are designed maintain certain invariants in the distributed,
     * both for this service and (in the case of forcing a pipeline leave on
     * another service) for other services.
     * <p>
     * This class does NOT cause changes in the local {@link AbstractQuorum}
     * state directly. Instead, it makes changes to the distributed quorum state
     * which are perceived by the {@link QuorumWatcherBase}, which makes the
     * corresponding adjustments to the {@link AbstractQuorum}'s internal state.
     * <p>
     * In order to provide the postcondition guarantee for actions that the
     * change has been made to the distributed quorum state, the
     * {@link QuorumActor} awaits an appropriate {@link Condition} on the
     * {@link AbstractQuorum} and then verifies that the desired state change
     * has occurred. This mechanism relies on the asynchronous perception of the
     * change in the distributed quorum state by the paired
     * {@link QuorumWatcherBase} and its update of the internal state maintained
     * on the {@link AbstractQuorum}.
     * <p>
     * Note: It is not possible in a distributed system to guarantee that state
     * changes made by other processes will not conflict with those which are
     * made by this actor on the behalf of its service. However, outside of rare
     * cases such as forcing another service to withdraw from the pipeline, all
     * services act solely on their own facet of the distributed quorum state
     * and all state changes are those which can be made atomic using a suitable
     * distributed state system, such as zookeeper.
     * 
     * <h3>Public API Implementation Notes</h3>
     * 
     * This base class implements the public API and delegates each public
     * method to a protected abstract method which must be implemented by the
     * concrete class. The public methods are final and encapsulate the logic to
     * maintain the invariants for the distributed quorum state. The abstract
     * protected methods must embody the logic required to effect those
     * distributed state changes. All such state changes MUST be atomic. Since
     * we will be invoking the public APIs for other operations, the public API
     * methods should only log messages if the actor will <em>do</em> something.
     * <p>
     * Everything is done while holding the {@link AbstractQuorum#lock}. Since
     * each service only acts on itself, the distributed state should remain
     * consistent. The {@link AbstractQuorum} and the {@link QuorumActorBase}
     * both <em>rely</em> on the {@link AbstractQuorum}s internal image of the
     * distributed quorum state, which is maintained by the
     * {@link QuorumWatcherBase}.
     * 
     * <h4>Public API "Add" methods</h4>
     * 
     * The "add" methods are those operations which add something to the
     * distributed quorum state, such as adding a member service, adding a
     * service to the pipeline, casting a vote, or a service join.
     * <p>
     * The public API "add" methods follow a pattern which rejects operations if
     * the preconditions for the action are not met while holding the
     * {@link AbstractQuorum#lock}. For example, a service can not "join" unless
     * it is a member, has cast a vote, there is a consensus for that vote, and
     * the service is part of the pipeline. The detailed preconditions are
     * documented on the {@link QuorumActor}.
     * 
     * <h4>Public API "Remove" methods</h4>
     * 
     * The "remove" methods are those operations which retract something from
     * the distributed quorum state, such as a service leave, a pipeline leave,
     * withdrawing a vote cast by a member service, and removing a member
     * service from the quorum.
     * <p>
     * The public API "remove" methods follow a pattern which retracts any other
     * information which must be retracted as a precondition before the
     * specified retraction may be applied. These preconditions are evaluated
     * while holding the {@link AbstractQuorum#lock}. Likewise, the actions are
     * taken while holding that lock. Since the {@link QuorumWatcherBase} must
     * obtain the lock in order to update the {@link AbstractQuorum}'s internal
     * state, the internal state of the {@link AbstractQuorum} will not (in
     * general) update until after the public API method releases the lock (this
     * might not be true for the within JVM mock objects since the watcher can
     * run in the same thread as the actor).
     * <p>
     * For example, the public API member remove operation is implemented as
     * follows. If, while holding the lock, the service is a member, then the
     * operation delegates to each of the other public API methods to remove it
     * from the joined services, the pipeline, and withdraw its votes. Finally,
     * the public {@link #memberRemove()} method invokes the protected method
     * {@link #doMemberRemove()} method to actually remove the service from the
     * set of member services in the distributed quorum state.
     * <p>
     * The public API methods all use conditional logic to cut down on remote
     * operations if the {@link AbstractQuorum}'s internal state shows that the
     * service is not a member, not in the pipeline, has not cast a vote, etc.
     * <p>
     */
    abstract protected class QuorumActorBase implements QuorumActor<S, C> {

        protected final String logicalServiceId;
        protected final UUID serviceId;

        private final QuorumMember<S> client;

        /**
         * 
         * @param logicalServiceId
         *            The identifier of the logical service.
         * @param serviceId
         *            The {@link UUID} of the physical service.
         */
        protected QuorumActorBase(final String logicalServiceId,
                final UUID serviceId) {

            if (logicalServiceId == null)
                throw new IllegalArgumentException();

            if (serviceId == null)
                throw new IllegalArgumentException();

            this.logicalServiceId = logicalServiceId;
            
            this.serviceId = serviceId;

            this.client = getClientAsMember();

        }

        final public QuorumMember<S> getQuorumMember() {
            return client;
        }

        final public Quorum<S, C> getQuourm() {
            return AbstractQuorum.this;
        }

        final public UUID getServiceId() {
            return serviceId;
        }

        /*
         * Public API methods.
         * 
         * For "add", the implementation pattern checks to see whether the
         * preconditions are satisfied (for "add") and then delegates to the
         * corresponding protected method to execute the distributed state
         * change.
         * 
         * For "remove", the implementation pattern checks to see whether each
         * post-condition is satisfied. If a given post-condition is not
         * Satisfied, then the implementation delegates to the protected method
         * to execute the necessary distributed state change.
         * 
         * Note: Each public API implementation must perform the necessary
         * pre-/post-condition tests itself in order to avoid problems with
         * mutual recursion among the public API methods.
         */
        
        final public void memberAdd() {
            lock.lock();
            try {
                conditionalMemberAddImpl();
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }

        final public void castVote(final long lastCommitTime) {
            if (lastCommitTime < 0)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                if (!members.contains(serviceId))
                    throw new QuorumException(ERR_NOT_MEMBER + serviceId);
                if (!pipeline.contains(serviceId))
                    throw new QuorumException(ERR_NOT_PIPELINE + serviceId);
                conditionalCastVoteImpl(lastCommitTime);
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }

        final public void pipelineAdd() {
            lock.lock();
            try {
                if (!members.contains(serviceId))
                    throw new QuorumException(ERR_NOT_MEMBER + serviceId);
                conditionalPipelineAddImpl();
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }

        final public void serviceJoin() {
            lock.lock();
            try {
                if (!members.contains(serviceId))
                    throw new QuorumException(ERR_NOT_MEMBER + serviceId);
                if (!pipeline.contains(serviceId))
                    throw new QuorumException(ERR_NOT_PIPELINE + serviceId);
                conditionalServiceJoin();
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }

        final public void setLastValidToken(final long newToken) {
            lock.lock();
            try {
                if (joined.size() < ((k + 1) / 2))
                    throw new QuorumException(ERR_CAN_NOT_MEET
                            + " too few services are joined: #joined="
                            + joined.size() + ", k=" + k);
                if (newToken <= lastValidToken)
                    throw new QuorumException(ERR_BAD_TOKEN + "lastValidToken="
                            + lastValidToken + ", but newToken=" + newToken);
                if (token != NO_QUORUM)
                    throw new QuorumException(ERR_QUORUM_MET);
                conditionalSetLastValidToken(newToken);
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }
        
        final public void setToken() {
            lock.lock();
            try {
                conditionalSetToken();
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }

        final public void clearToken() {
            /*
             * I have changed this to only clear the token while holding the
             * lock in order to make the leader election decidable. Otherwise it
             * is possible to have the token cleared during a service join which
             * would not have otherwise triggered an election and the election
             * would be triggered immediately.
             */
//            /*
//             * Immediately clear our local copy of the quorum token. This will
//             * cause any asserts which the service may execute currently using
//             * the old token to immediately fail.
//             */
//            token = NO_QUORUM;
            lock.lock();
            try {
                // Clear the token in the distributed quorum state.
                doClearToken();
                while (token != NO_QUORUM) {
                    quorumBreak.await();
                }
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }

        //
        // Public API "remove" methods.
        // 
        
        final public void memberRemove() {
            lock.lock();
            try {
                conditionalServiceLeaveImpl();
                conditionalPipelineRemoveImpl();
                conditionalWithdrawVoteImpl();
                conditionalMemberRemoveImpl();
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }

        final public void withdrawVote() {
            lock.lock();
            try {
                conditionalServiceLeaveImpl();
                conditionalPipelineRemoveImpl();
                conditionalWithdrawVoteImpl();
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }
        
        final public void pipelineRemove() {
            lock.lock();
            try {
                conditionalServiceLeaveImpl();
                conditionalPipelineRemoveImpl();
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }

        final public void serviceLeave() {
            lock.lock();
            try {
                conditionalWithdrawVoteImpl();
                conditionalPipelineRemoveImpl();
                conditionalServiceLeaveImpl();
            } catch(InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }
        
        /*
         * Private methods implement the conditional tests on the local state an
         * invoke the protected methods which actually carry out the action to
         * effect that change in the distributed quorum state.
         * 
         * Note: In order to avoid problems with recursion, these methods DO NOT
         * invoke methods on the QuorumActor public API.
         * 
         * Note: The caller MUST hold the lock when invoking these methods.
         */

        private void conditionalMemberAddImpl() throws InterruptedException {
            if (!members.contains(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                doMemberAdd();
                while(!members.contains(serviceId)) {
                    membersChange.await();
                }
            }
        }
        
        private void conditionalMemberRemoveImpl() throws InterruptedException {
            if (members.contains(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                doMemberRemove();
                while(members.contains(serviceId)) {
                    membersChange.await();
                }
            }
        }

        private void conditionalCastVoteImpl(final long lastCommitTime)
                throws InterruptedException {
            final Set<UUID> tmp = votes.get(lastCommitTime);
            if (tmp != null && tmp.contains(serviceId)) {
                // The service has already cast this vote.
                return;
            }
            if (log.isDebugEnabled())
                log.debug("serviceId=" + serviceId + ",lastCommitTime="
                        + lastCommitTime);
            // Withdraw any existing vote by this service.
            conditionalWithdrawVoteImpl();
            // Cast a vote.
            doCastVote(lastCommitTime);
            Long t = null;
            while ((t = getCastVote(serviceId)) == null
                    || t.longValue() != lastCommitTime) {
                votesChange.await();
            }
        }

        private void conditionalWithdrawVoteImpl() throws InterruptedException {
            final Long lastCommitTime = getCastVote(serviceId);
            if (lastCommitTime != null) {
                doWithdrawVote();
                while (getCastVote(serviceId) != null) {
                    votesChange.await();
                }
            }
        }

        private void conditionalPipelineAddImpl() throws InterruptedException {
            if (!pipeline.contains(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                doPipelineAdd();
                while(!pipeline.contains(serviceId)) {
                    pipelineChange.await();
                }
            }
        }

        private void conditionalPipelineRemoveImpl() throws InterruptedException {
            if (pipeline.contains(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                /*
                 * Remove the service from the pipeline.
                 */
                doPipelineRemove();
                while(pipeline.contains(serviceId)) {
                    pipelineChange.await();
                }
            }
        }

        private void conditionalServiceJoin() throws InterruptedException {
            /*
             * Discover the lastCommitTime of the consensus.
             */
            final Long lastCommitTime = getLastCommitTimeConsensus(serviceId);
            if (lastCommitTime == null) {
                /*
                 * Either the service did not vote for the consensus or there is
                 * no consensus.
                 */
                throw new QuorumException(ERR_NOT_IN_CONSENSUS + serviceId);
            }
            {
                // Get the vote order for the consensus.
                final UUID[] voteOrder = votes.get(lastCommitTime).toArray(
                        new UUID[0]);
                // Get the index of this service in the vote order.
                final int index = getIndexInVoteOrder(serviceId, voteOrder);
                if (index == -1) {
                    throw new AssertionError(AbstractQuorum.this.toString());
                }
                // Get the join order.
                final UUID[] joined = getJoinedMembers();
                /*
                 * No services may join out of the vote order. Verify that the
                 * predecessor(s) of this service in the vote order are joined
                 * in the vote order.
                 */
                for (int i = 0; i < index; i++) {
                    if (!joined[i].equals(voteOrder[i])) {
                        throw new QuorumException(
                                "Error in vote order: lastCommitTime="
                                        + lastCommitTime + ",serviceId="
                                        + serviceId + ",voteOrder="
                                        + Arrays.toString(voteOrder)
                                        + ",joined=" + Arrays.toString(joined)
                                        + ",errorAtIndex=" + i);
                    }
                }
                if (joined.length > index) {
                    if (joined[index].equals(serviceId)) {
                        /*
                         * Service is already joined and the joined order up to
                         * that service is the same as the vote order.
                         */
                        return;
                    }
                }
            }
            /*
             * The service is not yet joined and the vote order and the join
             * order are consistent.
             */
            if (joined.contains(serviceId)) {
                // condition should not be possible with the logic above.
                throw new AssertionError();
            }
            doServiceJoin();
            while(!joined.contains(serviceId)) {
                joinedChange.await();
            }
        }

        private void conditionalServiceLeaveImpl() throws InterruptedException {
            if (joined.contains(serviceId)) {
                doServiceLeave();
                while (joined.contains(serviceId)) {
                    joinedChange.await();
                }
            }
        }

        private void conditionalSetLastValidToken(final long newToken)
                throws InterruptedException {
            if (lastValidToken == newToken) {
                return;
            }
            doSetLastValidToken(newToken);
            while (lastValidToken != newToken) {
                lastValidTokenChange.await();
            }
        }

        private void conditionalSetToken() throws InterruptedException {
            if (token == lastValidToken) {
                return;
            }
            final long tmp = lastValidToken;
            doSetToken();
            while (tmp != token) {
                quorumMeet.await();
            }
        }
        
        /*
         * Abstract protected methods implement the atomic state change for just
         * the specific operation without any precondition maintenance.
         */

        abstract protected void doMemberAdd();

        abstract protected void doMemberRemove();

        abstract protected void doCastVote(long lastCommitTime);

        abstract protected void doWithdrawVote();

        abstract protected void doPipelineAdd();

        abstract protected void doPipelineRemove();

        /**
         * Invoked when our client will become the leader to (a) reorganize the
         * write pipeline such that our client is the first service in the write
         * pipeline (the leader MUST be the first service in the write
         * pipeline); and (b) to optionally <em>optimize</em> the write pipeline
         * for the network topology.
         * 
         * @see HAGlue#moveToEndOfPipeline()
         */
        abstract protected void reorganizePipeline();
        
        abstract protected void doServiceJoin();

        abstract protected void doServiceLeave();

        abstract protected void doSetLastValidToken(long newToken);

        abstract protected void doSetToken();
        
        abstract protected void doClearToken();

    }

    /**
     * Base class for {@link QuorumWatcher} implementations.
     * <p>
     * The protected methods on this base class are designed to be invoked by
     * the {@link QuorumWatcher} when it observes a change in the distributed
     * quorum state. Those methods know how to update the internal state of the
     * {@link AbstractQuorum} based on state changes observed by a concrete
     * {@link QuorumWatcher} implementations and will generate the appropriate
     * messages for the {@link QuorumClient} or {@link QuorumMember} and also
     * generate the appropriate {@link QuorumEvent}s.
     * <p>
     * When a state change triggers certain events on the behalf of the
     * associated {@link QuorumMember} the {@link QuorumWatcher} will issue the
     * necessary requests to the {@link QuorumActor}. For example, when the #of
     * joined services rises to <code>(k+1)/2</code>, the {@link QuorumWatcher}
     * for the first service in the vote order will issue the requests to the
     * {@link QuorumActor} which are necessary to execute the leader election
     * protocol.
     */
    abstract protected class QuorumWatcherBase implements QuorumWatcher<S, C> {

        /**
         * The identifier of the logical service whose quorum state is being
         * watched.
         */
        protected final String logicalServiceId;

        protected QuorumWatcherBase(final String logicalServiceId) {

            if (logicalServiceId == null)
                throw new IllegalArgumentException();
            
            this.logicalServiceId = logicalServiceId;
            
        }

        /**
         * Method is invoked by {@link AbstractQuorum#start(QuorumClient)} and
         * provides the {@link QuorumWatcher} with an opportunity to setup
         * discovery (such as zookeeper watchers) and read the initial state of
         * the distributed quorum, causing it to be reflected on the
         * {@link AbstractQuorum} internal state.
         */
        protected void start() {
            
        }

        /**
         * Method is invoked by {@link AbstractQuorum#terminate()} and provides
         * the {@link QuorumWatcher} with an opportunity to terminate
         * asynchronous processing and tear down any resources it may be using.
         */
        protected void terminate() {
            
        }

        /**
         * Run some action(s) on the behalf of the watcher outside of the thread
         * in which the watcher handles the state change (this is done to avoid
         * recursion through the watcher-action loop while holding the lock).
         * 
         * @param r
         *            The action(s).
         */
        protected void doAction(final Runnable r) {
            if (!lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();
            if (watcherActionService == null)
                throw new IllegalStateException();
            watcherActionService.execute(r);
//            new Thread(r).start();// asynchronous
//            r.run();// blocking
        }
        
        /**
         * Method is invoked by the {@link QuorumWatcher} when a member service
         * is added to the quorum and updates the internal state of the quorum
         * to reflect that state change.
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
                    membersChange.signalAll();
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        final UUID clientId = client.getServiceId();
                        if (serviceId.equals(clientId)) {
                            /*
                             * The service which was added is our client, so we
                             * send it a synchronous message so it can handle
                             * that add event.
                             */
                            client.memberAdd();
                        }
                    }
                    // queue client event.
                    sendEvent(new E(QuorumEventEnum.MEMBER_ADD,
                            lastValidToken, token, serviceId));
                    if (log.isInfoEnabled())
                        log.info("serviceId=" + serviceId.toString());
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when a member service
         * is removed from the quorum and updates the internal state of the
         * quorum to reflect that state change.
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
                    membersChange.signalAll();
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        final UUID clientId = client.getServiceId();
                        if (serviceId.equals(clientId)) {
                            /*
                             * The service which was removed is our client, so
                             * we send it a synchronous message so it can handle
                             * that add event.
                             */
                            client.memberRemove();
                        }
                    }
                    // queue client event.
                    sendEvent(new E(QuorumEventEnum.MEMBER_REMOVE,
                            lastValidToken, token, serviceId));
                    if (log.isInfoEnabled())
                        log.info("serviceId=" + serviceId.toString());
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when a service is
         * added to the write pipeline and updates the internal state of the
         * quorum to reflect that state change.
         * 
         * @param serviceId
         *            The service {@link UUID}.
         */
        protected void pipelineAdd(final UUID serviceId) {
            if (serviceId == null)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                // Notice the last service in the pipeline _before_ we add this
                // one.
                final UUID lastId = getLastInPipeline();
                if (pipeline.add(serviceId)) {
                    pipelineChange.signalAll();
                    if (log.isDebugEnabled()) {
                        // The serviceId will be at the end of the pipeline.
                        log.debug("pipeline=" + Arrays.toString(getPipeline()));
                    }
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        final UUID clientId = client.getServiceId();
                        if (serviceId.equals(clientId)) {
                            /*
                             * The service which was added to the write pipeline
                             * is our client, so we send it a synchronous
                             * message so it can handle that add event, e.g., by
                             * setting itself up to receive data.
                             */
                            client.pipelineAdd();
                        }
                        if (lastId != null && clientId.equals(lastId)) {
                            /*
                             * If our client was the last service in the write
                             * pipeline, then the new service is now its
                             * downstream service. The client needs to handle
                             * this event by configuring itself to send data to
                             * that service.
                             */
                            client.pipelineChange(null/* oldDownStream */,
                                    serviceId/* newDownStream */);
                        }
                    }
                    // queue client event.
                    sendEvent(new E(QuorumEventEnum.PIPELINE_ADD,
                            lastValidToken, token, serviceId));
                    if (log.isInfoEnabled())
                        log.info("serviceId=" + serviceId.toString());
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when a service is
         * removed from the write pipeline and updates the internal state of the
         * quorum to reflect that state change.
         * 
         * @param serviceId
         *            The service {@link UUID}.
         */
        protected void pipelineRemove(final UUID serviceId) {
            if (serviceId == null)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                /*
                 * Look for the service before/after the one being removed from
                 * the pipeline. If the service *before* the one being removed
                 * is our client, then we will notify it that its downstream
                 * service has changed.
                 */
                final UUID[] priorNext = getPipelinePriorAndNext(serviceId);
                if (pipeline.remove(serviceId)) {
                    pipelineChange.signalAll();
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        final UUID clientId = client.getServiceId();
                        if (serviceId.equals(clientId)) {
                            /*
                             * The service which was removed from the write
                             * pipeline is our client, so we send it a
                             * synchronous message so it can handle that add
                             * event, e.g., by tearing down its service which is
                             * receiving writes from the pipeline.
                             */
                            client.pipelineRemove();
                        }
                        if (priorNext != null && clientId.equals(priorNext[0])) {
                            /*
                             * Notify the client that its downstream service was
                             * removed from the write pipeline. The client needs
                             * to handle this event by configuring itself to
                             * send data to that service.
                             */
                            client.pipelineChange(serviceId/* oldDownStream */,
                                    priorNext[1]/* newDownStream */);
                        }
                        if (priorNext != null && priorNext[0] == null
                                && clientId.equals(priorNext[1])) {
                            /*
                             * Notify the client that its upstream service was
                             * removed from the write pipeline such that it is
                             * now the first service in the write pipeline. The
                             * client will need to handle this event by
                             * configuring itself with an HASendService rather
                             * than an HAReceiveService.
                             */
                            client.pipelineElectedLeader();
                        }
                    }
                    // queue client event.
                    sendEvent(new E(QuorumEventEnum.PIPELINE_REMOVE,
                            lastValidToken, token, serviceId));
                    if (log.isInfoEnabled())
                        log.info("serviceId=" + serviceId.toString());
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when a service is
         * votes in an attempt to join a quorum and updates the internal state
         * of the quorum to reflect that state change. Each service has a single
         * vote to cast. If the service casts a different vote, then its old
         * vote must first be withdrawn. Once (k+1)/2 services vote for the same
         * <i>lastCommitTime</i>, the client will be notified with a
         * quorumMeet() event. Once a quorum meets on a lastCommitTime, cast
         * votes should be left in place (or withdrawn?). A new vote will be
         * required if the quorum breaks. Services seeking to join a met quorum
         * must first synchronize with the quorum.
         * 
         * @param serviceId
         *            The service {@link UUID}.
         * @param lastCommitTime
         *            The lastCommitTime timestamp for which that service casts
         *            its vote.
         * 
         * @throws IllegalArgumentException
         *             if the <i>serviceId</i> is <code>null</code>.
         * @throws IllegalArgumentException
         *             if the lastCommitTime is negative.
         */
        protected void castVote(final UUID serviceId, final long lastCommitTime) {
            if (serviceId == null)
                throw new IllegalArgumentException();
            if (lastCommitTime < 0)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                // Look for a set of votes for that lastCommitTime.
                LinkedHashSet<UUID> tmp = votes.get(lastCommitTime);
                if (tmp == null) {
                    // None found, so create an empty set now.
                    tmp = new LinkedHashSet<UUID>();
                    // And add it to the map.
                    votes.put(lastCommitTime, tmp);
                }
                if (tmp.add(serviceId)) {
                    // The service cast its vote.
                    votesChange.signalAll();
                    final int nvotes = tmp.size();
                    if (log.isInfoEnabled())
                        log.info("serviceId=" + serviceId.toString()
                                + ", lastCommitTime=" + lastCommitTime
                                + ", nvotes=" + nvotes);
                    // queue event.
                    sendEvent(new E(QuorumEventEnum.CAST_VOTE, lastValidToken,
                            token, serviceId, lastCommitTime));
                    if (nvotes >= (k + 1) / 2) {
                        final QuorumMember<S> client = getClientAsMember();
                        if (nvotes == (k + 1) / 2) {
                            if (client != null) {
                                /*
                                 * Tell the client that consensus has been
                                 * reached on this last commit time.
                                 */
                                client.consensus(lastCommitTime);
                            }
                            // queue event.
                            sendEvent(new E(QuorumEventEnum.CONSENSUS,
                                    lastValidToken, token, serviceId, Long
                                            .valueOf(lastCommitTime)));
                        }
                        if (client != null) {
                            final UUID clientId = client.getServiceId();
                            final UUID[] voteOrder = tmp.toArray(new UUID[0]);
                            if (nvotes == (k + 1) / 2
                                    && clientId.equals(voteOrder[0])) {
                                /*
                                 * The client is the first service in the vote
                                 * order, so it will be the first service to
                                 * join and will become the leader when the
                                 * other services join. For now, we tell the
                                 * client's actor to join it now. The other
                                 * clients will notice the service join and join
                                 * once their predecessor in the vote order
                                 * joins.
                                 */
                                log.warn("First service will join");
                                if (!joined.isEmpty()) {
                                    throw new AssertionError(
                                            "Services already joined: "
                                                    + AbstractQuorum.this);
                                }
                                /*
                                 * Note: This will cause recursion through the
                                 * actor-watcher reflex arc unless it is run in
                                 * another thread.
                                 */
                                doAction(new Runnable() {public void run() {actor.serviceJoin();}});
                            } else if (clientId.equals(serviceId)) {
                                /*
                                 * The service which just joined is our client.
                                 * If the service before our client in the vote
                                 * order is joined, then it is time for our
                                 * client to join.
                                 */
                                final int index = getIndexInVoteOrder(clientId,
                                        voteOrder);
                                final UUID waitsFor = voteOrder[index - 1];
                                if (joined.contains(waitsFor)) {
                                    log
                                            .warn("Service will join already met quorum : njoined="
                                                    + joined.size()
                                                    + ", serviceId=" + clientId);
                                    if (joined.size() != voteOrder.length - 1)
                                        throw new AssertionError(
                                                "Expecting "
                                                        + (voteOrder.length - 1)
                                                        + " joined services, but there are "
                                                        + joined.size()
                                                        + " joined services : "
                                                        + AbstractQuorum.this);
                                    doAction(new Runnable() {public void run() {actor.serviceJoin();}});
//                                    actor.serviceJoin();
                               }
                            }
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when a service has
         * withdrawn its vote for some <i>lastCommitTime</i>.
         * <p>
         * Note: If no votes remain for a given lastCommitTime, then the
         * lastCommitTime is withdrawn from the internal pool of lastCommiTimes
         * being considered. This ensures that we do not leak memory in that
         * pool.
         * 
         * @param serviceId
         *            The service {@link UUID}.
         */
        protected void withdrawVote(final UUID serviceId) {
            if (serviceId == null)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                final Iterator<Map.Entry<Long, LinkedHashSet<UUID>>> itr = votes
                        .entrySet().iterator();
                while (itr.hasNext()) {
                    final Map.Entry<Long, LinkedHashSet<UUID>> entry = itr
                            .next();
                    final Set<UUID> votes = entry.getValue();
                    if (votes.remove(serviceId)) {
                        // The vote was withdrawn.
                        votesChange.signalAll();
                        sendEvent(new E(QuorumEventEnum.WITHDRAW_VOTE,
                                lastValidToken, token, serviceId));
                        if (votes.size() + 1 == (k + 1) / 2) {
                            final QuorumMember<S> client = getClientAsMember();
                            if (client != null) {
                                // Tell the client that the consensus was lost.
                                client.lostConsensus();
                            }
                        }
                        // found where the service had cast its vote.
                        if (log.isInfoEnabled())
                            log.info("serviceId=" + serviceId
                                    + ", lastCommitTime=" + entry.getKey());
                        if (votes.isEmpty()) {
                            // remove map entry with no votes cast.
                            itr.remove();
                        }
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when the leader
         * updates the lastValidToken. Note that the lastValidToken is updated
         * first and then the current token is set from the lastValidToken.
         * Since the quorum does not meet until the token has been updated, this
         * allows us to create an atomic protocol from piecewise atomic updates.
         * 
         * @param newToken
         *            The new token.
         */
        protected void setLastValidToken(final long newToken) {
            lock.lock();
            try {
                lastValidToken = newToken;
                lastValidTokenChange.signalAll();
                if (log.isInfoEnabled())
                    log.info("newToken=" + newToken);
                sendEvent(new E(QuorumEventEnum.SET_LAST_VALID_TOKEN, newToken,
                        token, null/* serviceId */));
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when the leader
         * publishes out a new quorum token.
         */
        protected void setToken() {
            lock.lock();
            try {
                {
                    /*
                     * Verify preconditions
                     */
                    // This must be updated before the token is set.
                    if (lastValidToken == NO_QUORUM)
                        throw new AssertionError(
                                "Last valid token never set : "
                                        + AbstractQuorum.this);
                    /*
                     * Note: This can be tripped if the watcher attempts to
                     * submit the actions in its thread (recursively while
                     * holding the lock) and the test fixture allows events to
                     * be processed in parallel. The fix was to have the watcher
                     * submit its actions for execution by a different thread.
                     */
                    if (joined.size() < (k + 1) / 2)
                        throw new AssertionError(
                                "Not enough joined services: njoined="
                                        + joined.size() + " : "
                                        + AbstractQuorum.this);
                }
                // Set the token from the lastValidToken.
                final long token = AbstractQuorum.this.token = lastValidToken;
                // signal everyone that the quorum has met.
                quorumMeet.signalAll();
                final UUID leaderId = getLeaderId();
                // must exist when quorum meets.
                assert leaderId != null : "Leader is null : "
                        + AbstractQuorum.this.toString();
                final QuorumMember<S> client = getClientAsMember();
//                if (client != null) {
//                    /*
//                     * Since our client is a quorum member, figure out whether
//                     * or not it is a follower, in which case we will send it
//                     * the electedFollower() message.
//                     */
//                    final UUID clientId = client.getServiceId();
//                    // Our client was elected the leader?
//                    if (joined.contains(clientId) && clientId.equals(leaderId)) {
//                        client.electedLeader();
//                        sendEvent(new E(QuorumEventEnum.ELECTED_LEADER,
//                                lastValidToken, token, clientId));
//                        if (log.isInfoEnabled())
//                            log.info("leader=" + clientId + ", token="
//                                    + token + ", leader=" + leaderId);
//                    }
//                    // Our client was elected a follower?
//                    if (joined.contains(clientId) && !clientId.equals(leaderId)) {
//                        client.electedFollower();
//                        sendEvent(new E(QuorumEventEnum.ELECTED_FOLLOWER,
//                                lastValidToken, token, clientId));
//                        if (log.isInfoEnabled())
//                            log.info("follower=" + clientId + ", token="
//                                    + token + ", leader=" + leaderId);
//                    }
//                }
                /*
                 * The quorum has met.
                 */
                log.warn("leader=" + leaderId + ", newToken=" + token
//                        + " : " + AbstractQuorum.this
                        );
                if (client != null) {
                    client.quorumMeet(token, leaderId);
                }
                sendEvent(new E(QuorumEventEnum.QUORUM_MEET, lastValidToken,
                        token, leaderId));
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when the current
         * quorum token is invalidated.
         */
        protected void clearToken() {
            lock.lock();
            try {
                final boolean willBreak = token != NO_QUORUM;
                token = NO_QUORUM;
                if (willBreak) {
                    quorumBreak.signalAll();
                    log.warn("Quorum break");
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        // Notify the client that the quorum broke.
                        client.quorumBreak();
                    }
                    sendEvent(new E(QuorumEventEnum.QUORUM_BROKE,
                            lastValidToken, token, null/* serviceId */));
                    if (client != null) {
                        final UUID clientId = client.getServiceId();
                        if(joined.contains(clientId)) {
                            // If our client is joined, then force serviceLeave.
//                            new Thread() {public void run() {actor.serviceLeave();}}.start();
                            doAction(new Runnable() {public void run() {actor.serviceLeave();}});
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when a service joins
         * the quorum and updates the internal state of the quorum to reflect
         * that state change.
         * 
         * @param serviceId
         *            The service {@link UUID}.
         */
        protected void serviceJoin(final UUID serviceId) {
            if (serviceId == null)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                if (!joined.add(serviceId)) {
                    // Already joined.
                    return;
                }
                joinedChange.signalAll();
                if (log.isInfoEnabled())
                    log.info("serviceId=" + serviceId.toString());
//                final int njoined = joined.size();
//                final int k = replicationFactor();
//                final boolean willMeet = njoined == (k + 1) / 2;
//                if (willMeet) {
//                    /*
//                     * The quorum will meet.
//                     * 
//                     * Note: The quorum is not met until the leader election has
//                     * occurred and the followers are all lined up. This state
//                     * only indicates that a meet will occur once those things
//                     * happen.
//                     */
//                    if (log.isInfoEnabled())
//                        log.info("Quorum will meet: k=" + k + ", njoined="
//                                + njoined);
//                }
                final QuorumMember<S> client = getClientAsMember();
                if (client != null) {
                    /*
                     * Since our client is a quorum member, figure out whether
                     * or not it is the leader, in which case it will do the
                     * leader election, or a follower, in which case we need do
                     * a service join if it is next in the vote order.
                     */
                    final UUID clientId = client.getServiceId();
                    if(serviceId.equals(clientId)) {
                        client.serviceJoin();
                    }
                }
                // queue event.
                sendEvent(new E(QuorumEventEnum.SERVICE_JOIN, lastValidToken,
                        token, serviceId));
                if(client!=null) {
                    final UUID clientId = client.getServiceId();
                    // The current consensus -or- null if our client is not in a
                    // consensus.
                    final Long lastCommitTime = getLastCommitTimeConsensus(clientId);
                    if (lastCommitTime != null) {
                        /*
                         * Our client is in the consensus.
                         */
                        // Get the vote order. This is also the target join
                        // order.
                        final UUID[] voteOrder = votes.get(lastCommitTime)
                                .toArray(new UUID[0]);
                        // the serviceId of the leader.
                        final UUID leaderId = voteOrder[0];// joined.iterator().next();
                        // true iff our client is the leader (else it is a
                        // follower since it is in the consensus).
                        final boolean isLeader = leaderId.equals(clientId);
                        if (isLeader) {
                            final int njoined = joined.size();
                            if (njoined == ((k + 1) / 2)) {
                                /*
                                 * Elect the leader.
                                 * 
                                 * Note: We are guaranteed that the #of joined
                                 * services just rose to (k+1)/2 (rather than
                                 * falling) because this is part of a
                                 * serviceJoin event which modified the joined
                                 * services set.
                                 * 
                                 * Note: The followers will get their
                                 * electedFollower() message when they notice
                                 * the token was set.
                                 */
                                log.warn("Electing leader: "
                                        + AbstractQuorum.this.toString());
                                doAction(new Runnable() {public void run() {
                                    actor.reorganizePipeline();
                                    actor.setLastValidToken(lastValidToken + 1);
                                    actor.setToken();
                                    }});
//                                client.electedLeader(); // moved to watcher.
                            }
                        } else {
                            // The index of our client in the vote order.
                            final int index = getIndexInVoteOrder(clientId,
                                    voteOrder);
                            if (index == -1) {
                                throw new AssertionError(AbstractQuorum.this
                                        .toString());
                            }

                            /*
                             * If the service which joined immediately proceeds
                             * our client in the vote order, then do a service
                             * join for our client now.
                             */
                            if (serviceId.equals(voteOrder[index - 1])) {
                                /*
                                 * Elect a follower.
                                 */
                                log.warn("Electing follower: "
                                        + AbstractQuorum.this.toString());
                                doAction(new Runnable() {public void run() {actor.serviceJoin();}});
                            }
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Method is invoked by the {@link QuorumWatcher} when a joined service
         * leaves the quorum and updates the internal state of the quorum to
         * reflect that state change.
         * 
         * @param serviceId
         *            The service {@link UUID}.
         */
        protected void serviceLeave(final UUID serviceId) {
            if (serviceId == null)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                // #of joined services _before_ the service leaves.
                final int njoinedBefore = joined.size();
                // The serviceId of the 1st joined service (and the leader iff
                // the quorum is met).
                final UUID leaderId;
                {
                    final Iterator<UUID> itr = joined.iterator();
                    leaderId = itr.hasNext() ? itr.next() : null;
                }
                if (!joined.remove(serviceId)) {
                    // This service was not joined.
                    return;
                }
                joinedChange.signalAll();
                final int k = replicationFactor();
                // iff the quorum was joined.
                final boolean wasJoined = njoinedBefore >= ((k + 1) / 2);
                // iff the leader just left the quorum.
                final boolean leaderLeft = wasJoined
                        && serviceId.equals(leaderId);
                // iff the quorum will break.
                final boolean willBreak = leaderLeft
                        || (njoinedBefore == ((k + 1) / 2));
                if (log.isInfoEnabled())
                    log.info("serviceId=" + serviceId + ", k=" + k
                            + ", njoined(before)=" + njoinedBefore + ", wasJoined="
                            + wasJoined + ", leaderLeft=" + leaderLeft
                            + ", willBreak=" + willBreak);
                final QuorumMember<S> client = getClientAsMember();
                if (client != null && willBreak) {
                    /*
                     * Invalidate the current quorum token.
                     * 
                     * Note: Only clients can take actions on the distributed
                     * quorum state. Non-clients monitoring the quorum will have
                     * to wait until their watcher notices that the quorum token
                     * in the distributed quorum state has been cleared.
                     * 
                     * Note: Services which are members of the quorum will see
                     * the quorumBreak() message. They MUST handle that message
                     * by (a) doing an abort() which will any buffered writes
                     * and reload their current root block; and (b) cast a vote
                     * for their current commit time. Once a consensus is
                     * reached on the current commit time, the services will
                     * join in the vote order, a new leader will be elected, and
                     * the quorum will meet again.
                     */
//                    new Thread() {public void run(){actor.clearToken();}}.start();
                    doAction(new Runnable() {public void run() {actor.clearToken();}});
                }
                if (client != null) {
                    // Notify all quorum members that a service left.
                    client.serviceLeave();
                }
                sendEvent(new E(QuorumEventEnum.SERVICE_LEAVE, lastValidToken,
                        token, serviceId));
//                if (client != null && leaderLeft) {
//                    // Notify all quorum members that the leader left.
//                    client.leaderLeft();
//                }
//                if (leaderLeft) {
//                    sendEvent(new E(QuorumEventEnum.LEADER_LEFT, token(),
//                            serviceId));
//                }
            } finally {
                lock.unlock();
            }
        }

    } // QuorumWatcherBase

    /*
     * 
     */

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
        if (log.isTraceEnabled())
            log.trace("" + e);
        if (sendSynchronous) {
            sendEventNow(e);
            return;
        }
        final Executor executor = eventService;
        if (executor != null) {
            try {
                // Submit task to send the event.
                executor.execute(new Runnable() {
                    public void run() {
                        /*
                         * Block until we get the lock so these events will come
                         * after the Quorum has handled the original message.
                         */
                        lock.lock();
                        try {
                            /*
                             * We acquired the lock so the events appear after
                             * they were handled by the Quourm, but we will send
                             * the events without the lock.
                             */
                        } finally {
                            lock.unlock();
                        }
                        sendEventNow(e);
                    }
                });
            } catch (RejectedExecutionException ex) {
                // ignore.
            }
        }
    }

    /**
     * Send an event to the client and all registered listeners (synchronous).
     * 
     * @param e
     *            The event.
     */
    private void sendEventNow(final QuorumEvent e) {
        // The client is always a listener.
        sendOneEvent(e, AbstractQuorum.this.client);
        // Send to any registered listeners as well.
        for (QuorumListener l : listeners) {
            sendOneEvent(e, l);
        }
    }

    /**
     * Send an event to listener (synchronous).
     * 
     * @param e
     *            The event.
     * @param l
     *            The listener.
     */
    private void sendOneEvent(final QuorumEvent e, final QuorumListener l) {
        try {
            if (l != null) {
                if (log.isTraceEnabled())
                    log.trace("event=" + e + ", listener=" + l);
                l.notify(e);
            }
        } catch (Throwable t) {
            log.warn(t, t);
        }
    }

    /**
     * Simple event impl.
     */
    protected static class E implements QuorumEvent {

        private final QuorumEventEnum type;

        private final long lastValidToken;

        private final long token;

        private final UUID serviceId;

        private final Long lastCommitTime;

        /**
         * Constructor used for most event types.
         * 
         * @param type
         * @param token
         * @param serviceId
         */
        public E(final QuorumEventEnum type, final long lastValidToken,
                final long token, final UUID serviceId) {
            this(type, lastValidToken, token, serviceId, -1L/* lastCommitTime */);
        }

        /**
         * Constructor used for vote events.
         * 
         * @param type
         * @param token
         * @param serviceId
         * @param lastCommitTime
         */
        public E(final QuorumEventEnum type, final long lastValidToken,
                final long token, final UUID serviceId,
                final long lastCommitTime) {
            this.type = type;
            this.lastValidToken = lastValidToken;
            this.token = token;
            this.serviceId = serviceId;
            this.lastCommitTime = lastCommitTime;
        }

        public QuorumEventEnum getEventType() {
            return type;
        }

        public UUID getServiceId() {
            return serviceId;
        }

        public long lastValidToken() {
            return lastValidToken;
        }
        
        public long token() {
            return token;
        }

        public long lastCommitTime() {
            if (type != QuorumEventEnum.CAST_VOTE)
                throw new UnsupportedOperationException();
            return lastCommitTime.longValue();
        }

        public String toString() {
            return "QuorumEvent"
                    + "{type="
                    + type
                    + ",lastValidToken="
                    + lastValidToken
                    + ",token="
                    + token
                    + ",serviceId="
                    + serviceId
                    + (type == QuorumEventEnum.CAST_VOTE ? ",lastCommitTime="
                            + lastCommitTime : "") + "}";
        }

    }

}
