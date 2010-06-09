package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.Arrays;
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
 *       {@link ZooKeeperAccessor} object so we can obtain a new zookeeper
 *       session each time an old one expires, (re-)establish various watches,
 *       etc.
 * 
 * @todo Support the synchronization protocol, including joining with the quorum
 *       at the next commit point or (if there are no pending writes) as soon as
 *       the node is caught up.
 */
abstract class AbstractQuorum<S extends Remote, C extends QuorumClient<S>>
        implements Quorum<S, C> {

    static protected final transient Logger log = Logger.getLogger(AbstractQuorum.class);

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

//    /**
//     * Condition signaled when the ordered set of services comprising the write
//     * pipeline is changed. Services must notice this event, locate themselves
//     * within the pipeline, and inspect their downstream service in the pipeline
//     * (if any). If the downstream, service has changed, then the service must
//     * reconfigure itself appropriately to relay writes to the downstream
//     * service (if any).
//     * <p>
//     * The condition variable in this case depends on the interpreter. For a
//     * service in the write pipeline, the condition variable is its downstream
//     * service. If that has changed, then the condition is satisfied.
//     */
//    private final Condition pipelineChange = lock.newCondition();

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
     * Each service votes for its lastCommitTime when it starts and after the
     * quorum breaks.
     */
    private final TreeMap<Long/*lastCommitTime*/,LinkedHashSet<UUID>> votes;
    
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
    private QuorumWatcher<S, C> watcher;

    /**
     * An object which causes changes in the distributed state of the quorum
     * (defined iff the client is a {@link QuorumMember} since non-members can
     * not affect the distributed quorum state).
     */
    private QuorumActor<S, C> actor;
    
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
        votes = new TreeMap<Long,LinkedHashSet<UUID>>();

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
            // Clear -- must be discovered!
            this.token = this.lastValidToken = NO_QUORUM;
            // Note: Client is automatically a listener.
            this.client = client;
//            addListener(client);
            if (client instanceof QuorumMember<?>) {
                this.actor = newActor(((QuorumMember<?>) client).getServiceId());
            }
            this.watcher = newWatcher();
            this.eventService = (sendSynchronous ? null : Executors
                    .newSingleThreadExecutor(new DaemonThreadFactory(
                            "QuorumEventService")));
            if (log.isDebugEnabled())
                log.debug("client=" + client);
            /*
             * Invoke hook for watchers. This gives them a chance to actually
             * discover the current quorum state, setup their listeners (e.g.,
             * zookeeper watchers), and pump deltas into the quorum.
             */
            setupDiscovery();
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
            if (!sendSynchronous) {
                eventService.shutdown();
                try {
                    eventService.awaitTermination(1000, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ex) {
                    // Ignore.
                } catch (InterruptedException ex) {
                    // Will be propagated below.
                    interrupted = true;
                } finally {
                    eventService = null;
                }
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
            // discard listeners.
            listeners.clear();
        } finally {
            lock.unlock();
        }
        if(interrupted) {
            // Propagate the interrupt.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Factory method invoked by {@link #start(QuorumClient)} iff the
     * {@link QuorumClient} is a {@link QuorumMember}.
     * 
     * @param serviceId
     *            The {@link UUID} of the service on whose behalf the actor will
     *            act.
     * 
     * @return The {@link QuorumActor} which will effect changes in the
     *         distributed state of the quorum.
     */
    abstract protected QuorumActor<S, C> newActor(UUID serviceId);

    /**
     * Factory method invoked by {@link #start(QuorumClient)}.
     * <p>
     * Note: Additional information can be passed to the watcher factor by
     * derived classes. For example, the {@link UUID} of the logical service to
     * be watched and its zpath.
     * 
     * @return The {@link QuorumWatcher} which will inform this
     *         {@link AbstactQuorum} of changes occurring in the distributed
     *         state of the quorum.
     */
    abstract protected QuorumWatcherBase newWatcher();

    /**
     * Return the {@link QuorumClient} iff the quorum is running.
     * 
     * @return The {@link QuorumClient}.
     * 
     * @throws IllegalStateException
     *             if the quorum is not running.
     */
    protected QuorumClient<S> getClient() {
        lock.lock();
        try {
            if (this.client != null)
                throw new IllegalStateException();
            return client;
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
     * Factory method invoked by {@link #start(QuorumClient)} iff the
     * {@link QuorumClient} is a {@link QuorumMember}.
     * 
     * @return The {@link QuorumActor} which will effect changes in the
     *         distributed state of the quorum -or- <code>null</code> if the
     *         client is not a {@link QuorumMember} (only quorum members can
     *         take actions which effect the distributed quorum state).
     * 
     * @throws IllegalStateException
     *             if the quorum is not running.
     */
    protected QuorumActor<S, C> getActor() {
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

    /**
     * @todo Setup discovery (watchers) and read the initial state of the
     *       distributed quorum, setting various internal fields appropriately.
     */
    protected void setupDiscovery() {
        
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

    public Map<Long, Set<UUID>> getVotes() {
        lock.lock();
        try {
            /*
             * Create a temporary map for the snapshot.
             * 
             * Note: A linked hash map will preserve the view order and is
             * faster than a TreeMap.
             */
            final Map<Long, Set<UUID>> tmp = new LinkedHashMap<Long, Set<UUID>>();
            final Iterator<Map.Entry<Long, LinkedHashSet<UUID>>> itr = votes.entrySet()
                    .iterator();
            while (itr.hasNext()) {
                final Map.Entry<Long, LinkedHashSet<UUID>> entry = itr.next();
                tmp.put(entry.getKey(), entry.getValue());
            }
            return tmp;
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
            if(!isQuorumMet()) {
                return null;
            }
            /*
             * FIXME Track the leader explicitly? Add a method to report the
             * followers? The followers are peers - they have no inherent order.
             */
            // Should always exist if the quorum is met.
            return joined.iterator().next();
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
     * QuorumWatcher API.
     * 
     */

    /**
     * Base class for {@link QuorumWatcher} implementations. The methods on this
     * base class are designed to be invoked by the QuorumWatcher when it
     * observes a change in the distributed quorum state. Those methods know how
     * to update the internal state of the {@link AbstractQuorum} based on state
     * changes observed by a concrete {@link QuorumWatcher} implementations.
     */
    abstract protected class QuorumWatcherBase implements QuorumWatcher<S, C> {

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
    //                memberAdd.signalAll();
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        final UUID clientId = client.getServiceId();
                        if(serviceId.equals(clientId)) {
                            /*
                             * The service which was added is our client, so we send
                             * it a synchronous message so it can handle that add
                             * event.
                             */
                            client.memberAdd();
                        }
                    }
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
    //                memberRemove.signalAll();
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        final UUID clientId = client.getServiceId();
                        if(serviceId.equals(clientId)) {
                            /*
                             * The service which was removed is our client, so we
                             * send it a synchronous message so it can handle that
                             * add event.
                             */
                            client.memberRemove();
                        }
                    }
                    // withdraw any vote cast by the service.
                    withdrawVote(serviceId);
                    // remove from the pipeline @todo can non-member services exist in the pipeline?
                    pipelineRemove(serviceId);
                    // service leave iff joined.
                    serviceLeave(serviceId);
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
                if (!members.contains(serviceId))
                    throw new QuorumException(ERR_NOT_MEMBER + serviceId);
    //            if(!members.contains(serviceId)) {
    //                /*
    //                 * Ensure that the service is a member.
    //                 * 
    //                 * Note: We do this as a general policy since the various events
    //                 * in the distributed quorum state might occur out of order and
    //                 * a pipeline add always implies a member add.
    //                 */
    //                memberAdd(serviceId);
    //            }
                // Notice the last service in the pipeline _before_ we add this one.
                final UUID lastId = getLastInPipeline();
    			if (pipeline.add(serviceId)) {
//    				pipelineChange.signalAll();
    				if (log.isDebugEnabled()) {
    					// The serviceId will be at the end of the pipeline.
    					final UUID[] a = getPipeline();
    					log.debug("pipeline: size=" + a.length + ", services="
    							+ Arrays.toString(a));
    				}
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        final UUID clientId = client.getServiceId();
                        if(serviceId.equals(clientId)) {
                            /*
                             * The service which was added to the write pipeline is
                             * our client, so we send it a synchronous message so it
                             * can handle that add event, e.g., by setting itself up
                             * to receive data.
                             */
                            client.pipelineAdd();
                        }
                        if (lastId != null && clientId.equals(lastId)) {
    						/*
    						 * If our client was the last service in the write
    						 * pipeline, then the new service is now its downstream
    						 * service. The client needs to handle this event by
    						 * configuring itself to send data to that service.
    						 */
                            client.pipelineChange(null/* oldDownStream */,
                                    serviceId/* newDownStream */);
                        }
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
                 * Look for the service before/after the one being removed
                 * from the pipeline. If the service *before* the one being
                 * removed is our client, then we will notify it that its
                 * downstream service has changed.
                 */
                final UUID[] priorNext = getPipelinePriorAndNext(serviceId);
                if (pipeline.remove(serviceId)) {
//                    pipelineChange.signalAll();
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        final UUID clientId = client.getServiceId();
                        if(serviceId.equals(clientId)) {
                            /*
                             * The service which was removed from the write pipeline is
                             * our client, so we send it a synchronous message so it
                             * can handle that add event, e.g., by tearing down its
                             * service which is receiving writes from the pipeline.
                             */
                            client.pipelineRemove();
                        }
                        if (priorNext != null && clientId.equals(priorNext[0])) {
                            /*
                             * Notify the client that its downstream service was
                             * removed from the write pipeline. The client needs to
                             * handle this event by configuring itself to send data
                             * to that service.
                             */
                            client.pipelineChange(serviceId/* oldDownStream */,
                                    priorNext[1]/* newDownStream */);
                        }
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
        protected void castVote(final UUID serviceId,final long lastCommitTime) {
            if (serviceId == null)
                throw new IllegalArgumentException();
    		if (lastCommitTime < 0)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                if (!members.contains(serviceId))
                    throw new QuorumException(ERR_NOT_MEMBER + serviceId);
                // Withdraw any existing vote for that service.
                withdrawVote(serviceId);
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
    				final int nvotes = tmp.size();
    				if (log.isInfoEnabled())
    					log.info("serviceId=" + serviceId.toString()
    							+ ", lastCommitTime=" + lastCommitTime
    							+ ", nvotes=" + nvotes);
    				// queue event.
                    sendEvent(new E(QuorumEventEnum.VOTE_CAST, token(),
                            serviceId, lastCommitTime));
                    if (nvotes == (k + 1) / 2) {
                        final QuorumMember<S> client = getClientAsMember();
                        if (client != null) {
                            /*
                             * Tell the client that consensus has been reached
                             * on this last commit time.
                             */
                            client.consensus(lastCommitTime);
                            /*
                             * @todo Consider whether this action is taken
                             * automatically by the Quorum or whether the client
                             * needs to trap the consensus() message and then
                             * join the quorum. This is currently left to the
                             * client, which makes sense for unit tests as it
                             * decouples the vote which achieves the consensus
                             * and the service join.
                             */
//                            /*
//                             * Instruct the client's actor to add it to the set
//                             * of joined services for the quorum.
//                             */
//                            getActor().serviceJoin();
    					}
    					// queue event.
    					sendEvent(new E(QuorumEventEnum.CONSENSUS, token(),
    							serviceId, Long.valueOf(lastCommitTime)));
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
//    			if (!members.contains(serviceId))
//    				throw new QuorumException(ERR_NOT_MEMBER + serviceId);
                final Iterator<Map.Entry<Long, LinkedHashSet<UUID>>> itr = votes
                        .entrySet().iterator();
    			while (itr.hasNext()) {
    				final Map.Entry<Long, LinkedHashSet<UUID>> entry = itr.next();
    				final Set<UUID> votes = entry.getValue();
                    if (votes.remove(serviceId)) {
                        if (votes.size() + 1 == (k + 1) / 2) {
                            final QuorumMember<S> client = getClientAsMember();
                            if (client != null) {
                                // Tell the client that the consensus was lost.
                                client.lostConsensus();
                            }
                        }
                        // found where the service had cast its vote.
                        if (log.isInfoEnabled())
    						log.info("serviceId=" + serviceId + ", lastCommitTime="
    								+ entry.getKey());
    					if (votes.isEmpty()) {
    						// remove map entries for which there are no votes cast.
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
         * Method is invoked by the {@link QuorumWatcher} when a service joins
         * the quorum and updates the internal state of the quorum to reflect
         * that state change.
         * 
         * @param serviceId
         *            The service {@link UUID}.
         * 
         *            FIXME A service clearly needs to be part of the pipeline
         *            before it can do a serviceJoin(). However, we still have
         *            to separate out these use cases: (1) The quorum is broken
         *            so there is nothing flowing over the write pipeline; and
         *            (2) The quorum is met, so there may be data flowing over
         *            the write pipeline and services attempting to join must do
         *            so at an atomic commit of the quorum.
         */
        protected void serviceJoin(final UUID serviceId) {
            if (serviceId == null)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                if (!members.contains(serviceId))
                    throw new QuorumException(ERR_NOT_MEMBER + serviceId);
                if (!pipeline.contains(serviceId))
                    throw new QuorumException(ERR_NOT_PIPELINE + serviceId);
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
                final QuorumMember<S> client = getClientAsMember();
                if (client != null) {
                    /*
                     * Since our client is a quorum member, figure out whether or
                     * not it is the leader, in which case it will do the leader
                     * election.
                     */
                    // the serviceId of the leader.
                    final UUID leaderId = joined.iterator().next();
                    // true iff the newly joined service is the leader.
                    final boolean isLeader = leaderId.equals(serviceId);
                    if (isLeader) {
                        /*
                         * This service is the first to join and will become the
                         * leader.
                         * 
                         * @todo Can we get away with simply assigning a new
                         * token and marking the client as read-write? The
                         * followers should already be in the right state
                         * (post-abort() and connected with the write pipeline)
                         * so once we have a token, all should be golden.
                         */
                        setToken(lastValidToken + 1);
                        client.electedLeader(token, serviceId);
                        sendEvent(new E(QuorumEventEnum.LEADER_ELECTED, token,
                                serviceId));
                        if (log.isInfoEnabled())
                            log.info("leader=" + leaderId + ", token=" + token);
                    } else {
                        // the serviceId of our client.
                        final UUID clientId = client.getServiceId();
                        if (serviceId.equals(clientId)) {
                            // Our client is being elected as a follower.
                            client.electedFollower(token);
                            sendEvent(new E(QuorumEventEnum.FOLLOWER_ELECTED,
                                    token, serviceId));
                            if (log.isInfoEnabled())
                                log.info("follower=" + serviceId + ", token="
                                        + token);
                        } else {
                            // Some other client was elected as a follower.
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
         * 
         * @todo This is currently written to recognize a difference between a
         *       leader leave and a quorum break. The token is cleared in both
         *       cases. However, a quorum break occurs only when the #of
         *       services joined with the quorum falls below (k+1)/2. When the
         *       situation is only a leader leave and not also a quorum break,
         *       then a new leader should be elected very quickly from one of
         *       the other services joined with the quorum.
         */
        protected void serviceLeave(final UUID serviceId) {
            if (serviceId == null)
                throw new IllegalArgumentException();
            lock.lock();
            try {
                // #of joined services _before_ the service leaves.
                final int njoined = joined.size();
                // The serviceId of the 1st joined service (and the leader iff
                // the quorum is met).
                final UUID leaderId;
                {
                    final Iterator<UUID> itr = joined.iterator();
                    leaderId = itr.hasNext() ? itr.next() : null;
                }
                if (!joined.remove(serviceId)) {
                    // This servic was not joined.
                    return;
                }
                // another service has left the quorum.
    //            serviceLeave.signalAll();
                // queue client event.
                sendEvent(new E(QuorumEventEnum.SERVICE_LEFT, token(), serviceId));
                if (log.isInfoEnabled())
                    log.info("serviceId=" + serviceId.toString());
                final int k = replicationFactor();
                // iff the quorum was joined.
                final boolean wasJoined = njoined == ((k + 1) / 2);
                // iff the quorum will break.
                final boolean willBreak = njoined == ((k + 1) / 2) - 1;
                // iff the leader just left the quorum.
                final boolean leaderLeft;
                if (wasJoined) {
                    // true iff the service which left was the leader.
                    leaderLeft = serviceId.equals(leaderId);
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
                final QuorumMember<S> client = getClientAsMember();
                if (client != null) {
                    client.serviceLeave();
                    if (wasJoined) {
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
         * Method is invoked by the {@link QuorumWatcher} when the leader
         * publishes out a new quorum token.
         * 
         * @param newToken
         *            The new token.
         */
        protected void setToken(final long newToken) {
            lock.lock();
            try {
                if (newToken <= lastValidToken)
                    throw new RuntimeException("lastValidToken=" + lastValidToken
                            + ", but newToken=" + newToken);
                // save the new value.
                lastValidToken = token = newToken;
                // signal everyone that the quorum has met.
    			quorumMeet.signalAll();
    			if (log.isInfoEnabled())
    				log.info("newToken=" + newToken + ",lastValidToken="
    						+ lastValidToken);
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
		if(sendSynchronous) {
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
    private static class E implements QuorumEvent {

        private final QuorumEventEnum type;

		private final long token;

		private final UUID serviceId;

		private final Long lastCommitTime;

		/**
		 * Constructor used for most event types.
		 * @param type
		 * @param token
		 * @param serviceId
		 */
		public E(final QuorumEventEnum type, final long token,
				final UUID serviceId) {
			this(type, token, serviceId, -1L/* lastCommitTime */);
		}
		
		/**
		 * Constructor used for vote events.
		 * @param type
		 * @param token
		 * @param serviceId
		 * @param lastCommitTime
		 */
		public E(final QuorumEventEnum type, final long token,
                final UUID serviceId, final long lastCommitTime) {
            this.type = type;
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

        public long token() {
            return token;
        }

        public long lastCommitTime() {
            if (type != QuorumEventEnum.VOTE_CAST)
                throw new UnsupportedOperationException();
            return lastCommitTime.longValue();
		}

		public String toString() {
			return "QuorumEvent"
					+ "{type="
					+ type
					+ ",token="
					+ token
					+ ",serviceId="
					+ serviceId
					+ (type == QuorumEventEnum.VOTE_CAST ? ",lastCommitTime="
							+ lastCommitTime : "") + "}";
		}

	}

}
