/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.quorum;

import java.io.IOException;
import java.rmi.Remote;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.util.DaemonThreadFactory;
import com.bigdata.util.InnerCause;
import com.bigdata.util.StackInfoReport;
import com.bigdata.util.concurrent.ThreadGuard;
import com.bigdata.util.concurrent.ThreadGuard.Guard;

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
     * Dedicated logger for quorum state.
     */
    static protected final transient Logger qlog = Logger
            .getLogger("com.bigdata.quorum.quorumState");

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
     * Message when a quorum breaks..
     */
    static protected final transient String ERR_QUORUM_BREAK = "Quorum break";

    /**
     * A timeout (nanoseconds) used to await some precondition to become true.
     * 
     * @see #awaitEnoughJoinedToMeet()
     * 
     *      TODO Make this configurable (settable). We might want to set it to
     *      the <code>2 x tickTime</code> for zookeeper.
     */
    private final long timeout = TimeUnit.SECONDS.toNanos(1);

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
     * The minimum #of joined services that constitutes a quorum as defined by
     * <code>(k + 1) / 2 </code>.
     * <p>
     * Note: This constant is isolated here so we can have "quorums" that
     * require ALL services to be joined. For example, a highly available system
     * that can replicate writes but can not resynchronize services that were
     * not present during a quorum commit would specify the same value for
     * {@link #k} and {@link #kmeet} in order to ensure that all services are
     * joined before the quorum "meets".
     * 
     * TODO Specify means to compute this. It is fixed by the constructor right
     * now. Maybe we should just pull out an interface for this?
     */
    protected final int kmeet;
    
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

//    /**
//     * Condition signaled when a quorum is fully met. The preconditions for this
//     * event are:
//     * <ul>
//     * <li>At least (k+1)/2 services agree on the same lastCommitTime.</li>
//     * <li>At least (k+1)/2 services have joined the quorum in their vote order.
//     * </li>
//     * <li>The first service to join the quorum is the leader and it has updated
//     * the lastValidToken and the current token.</li>
//     * </ul>
//     * The condition variable is <code> token != NO_QUORUM </code>. Since the
//     * {@link #token()} is cleared as soon as the leader fails or the quorum
//     * breaks, this is sufficient to detect a quorum meet.
//     */
//    private final Condition quorumMeet = lock.newCondition();
//    
//    /**
//     * Condition signaled when a quorum breaks.
//     */
//  private final Condition quorumBreak = lock.newCondition();

    /**
     * Condition signaled when the {@link #lastValidToken} or the {@link #token}
     * are modified. You must also inspect the state of those condition
     * variables in order to distinguish quorum meet versus quorum break and
     * related states.
     */
    private final Condition quorumChange = lock.newCondition();

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

//    /**
//     * Condition signaled when the {@link #lastValidToken} is set by the
//     * {@link QuorumWatcherBase}.
//     * <p>
//     * See {@link #quorumMeet} and {@link #quorumBreak} for the
//     * {@link Condition}s pertaining to the current {@link #token}.
//     */
//    private final Condition lastValidTokenChange = lock.newCondition();
    
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
     * <p>
     * Note: This is volatile to allow visibility without holding the
     * {@link #lock}. The field is only modified in {@link #start(QuorumClient)}
     * and {@link #terminate()}, and those methods use the {@link #lock} to
     * impose an appropriate ordering over events. The quorum is running iff
     * there is a client for which it is delivering events. When <code>null</code>,
     * the quorum is not running.
     * 
     * @see #start(QuorumClient)
     */
    private volatile C client;

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
     * <p>
     * Note: {@link ThreadPoolExecutor} is used because it exposes
     * getActiveCount().
     */
    private ThreadPoolExecutor watcherActionService;

    /*
     * @todo These fields are being used to explore a problem in test_voting
     * where there sometimes is a hang until the watcher service is terminated.
     * 
     * @todo A single thread watcher can lead to a deadlock if an action stalls
     * since a second watcher event can not be processed concurrently.
     */
    private final boolean singleThreadWatcher = false;
//    private final long watcherShutdownTimeout = Long.MAX_VALUE; // ms
    private final long watcherShutdownTimeout = 3000; // ms

    /**
     * A service used by {@link QuorumActorBase} to execute actions in a single
     * thread.
     */
    private ThreadPoolExecutor actorActionService;

    /**
     * This may be used to force a behavior where the actor is taken in the
     * caller's thread. This is not compatible with the use of timeouts for the
     * actions.
     */
    private final boolean singleThreadActor = true;
    private final long actorShutdownTimeout = watcherShutdownTimeout; // ms
    /**
     * Collector of queued or running futures for actor action tasks.
     */
    private final Set<FutureTask<Void>> knownActorTasks = new HashSet<FutureTask<Void>>();

    /**
     * A single threaded service used to pump events to clients outside of the
     * thread in which those events arise.
     */
    private ExecutorService eventService;

    /**
     * When true, events are send synchronously in the thread of the watcher.
     * <p>
     * Note: The events are not guaranteed to arrive in the same order that the
     * internal state changes are made. In order to be robust, the inner classes
     * use patterns where they wait until a {@link Condition} becomes true (or
     * false) in a loop.
     * <p>
     * Note: This makes it easy to write the unit tests since we do not need to
     * "wait" for events to arrive (and they are much faster without the
     * eventService contending for the {@link #lock} all the time). However,
     * sending the event in the caller's thread means that the caller will be
     * holding the {@link #lock} and thus the receiver MUST NOT execute any
     * blocking code. Specifically, it must not cause a different thread to
     * execute code that would content for the {@link #lock}.
     * <p>
     * Note: The receiver must not wait on any other thread to perform an action
     * that touches the {@link AbstractQuorum}. This is documented on the
     * {@link QuorumStateChangeListener}. {@link QuorumClient} implementations
     * must also be wary of blocking in the event thread.
     */
    private final boolean sendSynchronous = true;

    /**
     * Used to guard critical regions where we await a {@link Condition}.
     */
    private final ThreadGuard threadGuard = new ThreadGuard();
    
    /**
     * Execute a critical region which needs to be interrupted if we have to
     * terminate the quorum.
     * 
     * @param r
     *            The lambda.
     *            
     * @return The result (if any).
     */
    protected void guard(final Guard r) throws InterruptedException {
        threadGuard.guard(r);
    }

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
        
        this.kmeet = (k + 1) / 2;
        
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
    @Override
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
                this.actor = newActor(client.getLogicalServiceZPath(),
                        ((QuorumMember<?>) client).getServiceId());
            }
            if (singleThreadWatcher) {
                this.watcherActionService = new ThreadPoolExecutor(1, 1, 0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        new DaemonThreadFactory("WatcherActionService"));
                // this.watcherActionService =
                // Executors.newSingleThreadExecutor(new
                // DaemonThreadFactory("WatcherActionService"));
            } else {
                this.watcherActionService = (ThreadPoolExecutor) Executors
                        .newCachedThreadPool(new DaemonThreadFactory(
                                "WatcherActionService"));
            }
            if (singleThreadActor) {
                // run on a single-thread executor service.
                this.actorActionService = new ThreadPoolExecutor(1, 1, 0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        new DaemonThreadFactory("ActorActionService"));
            } else {
                // run in the caller's thread.
                this.actorActionService = null;
            }
            // Reach out to the durable quorum and get the lastValidToken
            this.lastValidToken = getLastValidTokenFromQuorumState(client);
            // Setup the watcher.
            this.watcher = newWatcher(client.getLogicalServiceZPath());
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
             * since it will be sending messages to the client as it discovers
             * the distributed quorum state.
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

    /**
     * Initialization method must return the lastValidToken from the durable
     * quorum state and {@link #NO_QUORUM} if there is no durable state.
     */
    protected long getLastValidTokenFromQuorumState(final C client) {

        return NO_QUORUM;
        
    }

    /**
     * Ensure that any guarded regions are interrupted.
     */
    public void interruptAll() {

        // Cancel any queued or running action task.
        synchronized (knownActorTasks) {
            
            for (Future<Void> ft : knownActorTasks) {
            
                ft.cancel(true/* mayInterruptIfRunning */);

            }
            /*
             * Note: we do not need to clear this collection. Tasks will be
             * removed from the collection when they are propagated through the
             * actorActionService. Cancelled tasks will immediately drop through
             * the service and have their futures removed from this collection.
             * This allows us to assert that the future is properly removed from
             * the collection in runActorTask().
             */
//            knownActorTasks.clear();

            /*
             * Interrupt any running actions.
             * 
             * TODO This is redundant with the cancel() of the knownActorTasks.
             */
            threadGuard.interruptAll();

        }

    }
    
    @Override
    public void terminate() {
        boolean interrupted = false;
        lock.lock();
        try {
            /*
             * Ensure that any guarded regions are interrupted.
             */
            interruptAll();
            if (client == null) {
                // No client?  Not running.
                return;
            }
            if (log.isDebugEnabled())
                log.debug("client=" + client);
//            if (client instanceof QuorumMember<?>) {
//                /*
//                 * Update the distributed quorum state by removing our client
//                 * from the set of member services. This will also cause a
//                 * service leave, pipeline leave, and any vote to be withdrawn.
//                 * 
//                 * We have observed Condition spins during terminate() that
//                 * result in HAJournalServer hangs. This runs another Thread
//                 * that will interrupt this Thread if the quorum member is
//                 * unable to complete the memberRemove() within a timeout.
//                 * 
//                 * Note: Since we are holding the lock in the current thread, we
//                 * MUST execute memberRemove() in this thread (it requires the
//                 * lock). Therefore, I have used a 2nd thread that will
//                 * interrupt this thread if it does not succeed in a polite
//                 * removal from the quorum within a timeout.
//                 */
//                {
//                    final long MEMBER_REMOVE_TIMEOUT = 5000;// ms.
//                    final AtomicBoolean didRemove = new AtomicBoolean(false);
//                    final Thread self = Thread.currentThread();
//                    final Thread t = new Thread() {
//                        public void run() {
//                            try {
//                                Thread.sleep(MEMBER_REMOVE_TIMEOUT);
//                            } catch (InterruptedException e) {
//                                // Expected. Ignored.
//                                return;
//                            }
//                            if (!didRemove.get()) {
//                                log.error("Timeout awaiting quorum member remove.");
//                                self.interrupt();
//                            }
//                        }
//                    };
//                    t.setDaemon(true);
//                    t.start();
//                    try {
//                        // Attempt memberRemove() (interruptably).
//                        actor.memberRemoveInterruptable();
//                        didRemove.set(true); // Success.
//                    } catch (InterruptedException e) {
//                        // Propagate the interrupt.
//                        Thread.currentThread().interrupt();
//                    } finally {
//                        t.interrupt(); // Stop execution of [t].
//                    }
//                }
//            }

            if (watcher != null) {
                try {
                    watcher.terminate();
                } catch (Throwable t) {
                    if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                        interrupted = true;
                    } else {
                        launderThrowable(t);
                    }
                }
            }
            if (watcherActionService != null) {
                watcherActionService.shutdown();
                try {
                    if (!watcherActionService.awaitTermination(
                            watcherShutdownTimeout, TimeUnit.MILLISECONDS)) {
                        log
                                .error("WatcherActionService termination timeout: activeCount="
                                        + ((ThreadPoolExecutor) watcherActionService)
                                                .getActiveCount());
                    }
                } catch (com.bigdata.concurrent.TimeoutException ex) {
                    // Ignore.
                } catch (InterruptedException ex) {
                    // Will be propagated below.
                    interrupted = true;
                } finally {
                    /*
                     * Cancel any tasks which did not terminate in a timely manner.
                     */
                    final List<Runnable> notrun = watcherActionService.shutdownNow();
                    watcherActionService = null;
                    for (Runnable r : notrun) {
                        if (r instanceof Future) {
                            ((Future<?>) r).cancel(true/* mayInterruptIfRunning */);
                        }
                    }
                }
            }
            if (actorActionService != null) {
                actorActionService.shutdown();
                try {
                    if (!actorActionService.awaitTermination(
                            actorShutdownTimeout, TimeUnit.MILLISECONDS)) {
                        log.error("ActorActionService termination timeout");
                    }
                } catch (com.bigdata.concurrent.TimeoutException ex) {
                    // Ignore.
                } catch (InterruptedException ex) {
                    // Will be propagated below.
                    interrupted = true;
                } finally {
                    /*
                     * Cancel any tasks which did not terminate in a timely manner.
                     */
                    final List<Runnable> notrun = actorActionService.shutdownNow();
                    actorActionService = null;
                    for (Runnable r : notrun) {
                        if (r instanceof Future) {
                            ((Future<?>) r).cancel(true/* mayInterruptIfRunning */);
                        }
                    }
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
                    /*
                     * Cancel any tasks which did terminate in a timely manner.
                     */
                    final List<Runnable> notrun = eventService.shutdownNow();
                    eventService = null;
                    for (Runnable r : notrun) {
                        if (r instanceof Future) {
                            ((Future<?>) r).cancel(true/* mayInterruptIfRunning */);
                        }
                    }
                }
            }
            /*
             * Let the service know that it is no longer running w/ the quorum.
             */
            try {
                client.terminate();
            } catch (Throwable t) {
                if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                    interrupted = true;
                } else {
                    launderThrowable(t);
                }
            }
            /*
             * Clear all internal state variables that mirror the distributed
             * quorum state and then signal all conditions so anyone blocked
             * will wake up.
             */
            listeners.clear(); // discard listeners.
            token = lastValidToken = NO_QUORUM;
            members.clear();
            votes.clear();
            joined.clear();
            pipeline.clear();
            quorumChange.signalAll();
            membersChange.signalAll();
            pipelineChange.signalAll();
            votesChange.signalAll();
            joinedChange.signalAll();
            // discard reference to the client.
            this.client = null;
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
    @Override
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
    
    @Override
    public C getClient() {
        lock.lock();
        try {
            final C client = this.client;
            if (client == null)
                throw new IllegalStateException();
            return client;
        } finally {
            lock.unlock();
        }
    }

//    @Override
    protected C getClientNoLock() {
//        lock.lock();
//        try {
            final C client = this.client;
            if (client == null)
                throw new IllegalStateException();
            return client;
//        } finally {
//            lock.unlock();
//        }
    }

    @Override
    public QuorumMember<S> getMember() {
        lock.lock();
        try {
            final C client = this.client;
            if (client == null)
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

        final C client = this.client;
        
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
    @Override
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

    @Override
    final public void addListener(final QuorumListener listener) {
        if (listener == null)
            throw new IllegalArgumentException();
        if (listener == client)
            throw new IllegalArgumentException();
        listeners.add(listener);
    }

    @Override
    final public void removeListener(final QuorumListener listener) {
        if (listener == null)
            throw new IllegalArgumentException();
        if (listener == client)
            throw new IllegalArgumentException();
        listeners.remove(listener);
    }

    @Override
    final public int replicationFactor() {

        // Note: [k] is final.
        return k;
        
    }

    @Override
    public final boolean isQuorum(final int njoined) {

        return njoined >= kmeet;

    }
    
    @Override
    final public boolean isHighlyAvailable() {
        
        return replicationFactor() > 1;
        
    }

    @Override
    final public long lastValidToken() {
        lock.lock();
        try {
            return lastValidToken;
        } finally {
            lock.unlock();
        }
    }

    @Override
    final public UUID[] getMembers() {
        lock.lock();
        try {
            return members.toArray(new UUID[0]);
        } finally {
            lock.unlock();
        }
    }

    @Override
    final public Map<Long, UUID[]> getVotes() {
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

    @Override
    final public Long getCastVote(final UUID serviceId) {
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

    @Override
    public Long getCastVoteIfConsensus(final UUID serviceId) {
        lock.lock();
        try {
            final Iterator<Map.Entry<Long, LinkedHashSet<UUID>>> itr = votes
                    .entrySet().iterator();
            while (itr.hasNext()) {
                final Map.Entry<Long, LinkedHashSet<UUID>> entry = itr.next();
                final Set<UUID> votes = entry.getValue();
                if (votes.contains(serviceId)) {
                    if (isQuorum(votes.size()))
                        return entry.getKey().longValue();
                    else
                        return null;
                }
            }
            // Service is not part of a consensus.
            return null;
        } finally {
            lock.unlock();
        }
    }

    /*
     * Helper methods.
     */

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

    @Override
    final public UUID[] getJoined() {
        lock.lock();
        try {
            return joined.toArray(new UUID[0]);
        } finally {
            lock.unlock();
        }
    }

    @Override
    final public UUID[] getPipeline() {
        lock.lock();
        try {
            return pipeline.toArray(new UUID[0]);
        } finally {
            lock.unlock();
        }
    }

    @Override
    final public UUID getLastInPipeline() {
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

    @Override
    final public UUID[] getPipelinePriorAndNext(final UUID serviceId) {
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

    @Override
    final public UUID getLeaderId() {
        UUID leaderId = null;
        final long tmp;
        lock.lock();
        try {
            /*
             * Note: [token] is VOLATILE, so it can change while we are holding
             * the lock. Therefore, we have to explicitly guard against this.
             */
            tmp = token; // value of token on entry.
            if (token == NO_QUORUM) {
                // No quorum, no leader.
                return null;
            }
            final UUID[] joined = getJoined();
            if (joined.length > 0) {
                // Set IFF available.
                leaderId = joined[0];
            }
        } finally {
            lock.unlock();
        }
        if (token != tmp) {
            // token concurrently changed.
            return null;
        }
        // token remained valid, return whatever we have.
        return leaderId;
    }

    @Override
    final public long token() {
        // Note: volatile read.
        return token;
    }

    final public void assertQuorum(final long token) {

        if (this.token == NO_QUORUM)
            throw new QuorumException("Quorum not met");
        
        if (token != NO_QUORUM && this.token == token) {
            return;
        }
        
        throw new QuorumException("Quorum not met on token: expected=" + token
                + ", actual=" + this.token);

    }

    @Override
    final public void assertLeader(final long token) {
        if (token == NO_QUORUM) {
            // The quorum was not met when the client obtained that token.
            throw new QuorumException("Client token is invalid.");
        }
        if (this.token == NO_QUORUM) {
            // The quorum is not met.
            throw new QuorumException("Quorum is not met.");
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

    @Override
    final public boolean isQuorumMet() {

        return token != NO_QUORUM;
        
    }
    
    @Override
    final public boolean isQuorumFullyMet(final long token) {
        
        lock.lock();

        try {

            if (joined.size() != k) {

                // Quorum is not fully met.
                return false;

            }

            // Verify token.
            assertQuorum(token);

            return true;

        } finally {

            lock.unlock();
            
        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * This watches the current token and will return as soon as the token is
     * valid.
     */
    @Override
    final public long awaitQuorum() throws InterruptedException,
            AsynchronousQuorumCloseException {
        lock.lock();
        try {
            while (token == NO_QUORUM && client != null) {
                quorumChange.await();
            }
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            return token;
        } finally {
            lock.unlock();
        }
    }

    @Override
    final public long awaitQuorum(final long timeout, final TimeUnit units)
            throws InterruptedException, TimeoutException,
            AsynchronousQuorumCloseException {
        final long begin = System.nanoTime();
        final long nanos = units.toNanos(timeout);
        long remaining = nanos;
        if(!lock.tryLock(remaining, TimeUnit.NANOSECONDS))
            throw new TimeoutException();
        try {
            // remaining = nanos - (now - begin) [aka elapsed]
            remaining = nanos - (System.nanoTime() - begin);
            while (token == NO_QUORUM && client != null && remaining > 0) {
                if (!quorumChange.await(remaining, TimeUnit.NANOSECONDS))
                    throw new TimeoutException();
                remaining = nanos - (System.nanoTime() - begin);
            }
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            if (remaining <= 0)
                throw new TimeoutException();
            return token;
        } finally {
            lock.unlock();
        }
    }

    @Override
    final public void awaitBreak() throws InterruptedException,
            AsynchronousQuorumCloseException {
        lock.lock();
        try {
            while (token != NO_QUORUM && client != null) {
                quorumChange.await();
            }
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            return;
        } finally {
            lock.unlock();
        }
    }

    @Override
    final public void awaitBreak(final long timeout, final TimeUnit units)
            throws InterruptedException, TimeoutException,
            AsynchronousQuorumCloseException {
        final long begin = System.nanoTime();
        final long nanos = units.toNanos(timeout);
        long remaining = nanos;
        if (!lock.tryLock(remaining, TimeUnit.NANOSECONDS))
            throw new TimeoutException();
        try {
            // remaining = nanos - (now - begin) [aka elapsed]
            remaining = nanos - (System.nanoTime() - begin);
            while (token != NO_QUORUM && client != null && remaining > 0) {
                if (!quorumChange.await(remaining, TimeUnit.NANOSECONDS))
                    throw new TimeoutException();
                remaining = nanos - (System.nanoTime() - begin);
            }
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            if (remaining <= 0)
                throw new TimeoutException();
            return;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enforce a precondition that at least {@link #kmeet} services have joined.
     * <p>
     * Note: This is used in certain places to work around concurrent
     * indeterminism.
     * 
     * @throws TimeoutException
     */
    private void awaitEnoughJoinedToMeet() throws QuorumException,
            TimeoutException {
        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        try {
            final long begin = System.nanoTime();
            final long nanos = timeout;
            long remaining = nanos;
            while (!isQuorum(joined.size()) && client != null && remaining > 0) {
                if (!joinedChange.await(remaining, TimeUnit.NANOSECONDS))
                    throw new QuorumException(
                            "Not enough joined services: njoined="
                                    + joined.size() + " : "
                                    + AbstractQuorum.this);
                // remaining = nanos - (now - begin) [aka elapsed]
                remaining = nanos - (System.nanoTime() - begin);
            }
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            if (remaining <= 0)
                throw new TimeoutException();
            return;
        } catch (InterruptedException e) {
            // propagate interrupt.
            Thread.currentThread().interrupt();
            return;
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

//        private final QuorumMember<S> client;

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

//            this.client = getClientAsMember();

        }

        @Override
        final public QuorumMember<S> getQuorumMember() {
            return (QuorumMember<S>) client;
        }

        @Override
        final public Quorum<S, C> getQuourm() {
            return AbstractQuorum.this;
        }

        @Override
        final public UUID getServiceId() {
            return serviceId;
        }

        /**
         * Task used to run an action.
         */
        abstract protected class ActorTask implements Callable<Void> {
            
            @Override
            public Void call() throws Exception {

                lock.lockInterruptibly();
                try {

                    doAction();

                    return null/* Void */;
                    
                } finally {
                    
                    lock.unlock();
                    
                }
                
            }

            /**
             * Execute the action - the lock is already held by the thread.
             */
            abstract protected void doAction() throws InterruptedException;
            
        }

        private Executor getActorExecutor() {
            return actorActionService;
        }
        
        private void runActorTask(final ActorTask task) {
            if (!singleThreadActor) {
                /*
                 * Run in the caller's thread.
                 */
                try {
                    task.call();
                    return;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            /*
             * Run on the single-threaded executor.
             */
            final FutureTask<Void> ft = new FutureTaskMon<Void>(task);
            synchronized(knownActorTasks) {
                if (!knownActorTasks.add(ft))
                    throw new AssertionError();
            }
            getActorExecutor().execute(ft);
            try {
                ft.get();
            } catch (InterruptedException e) {
                // Propagate interrupt
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } finally {
                ft.cancel(true/* mayInterruptIfRunning */);
                synchronized (knownActorTasks) {
                    if (!knownActorTasks.remove(ft))
                        throw new AssertionError();
                }
            }
        }

        /**
         * Variant method supporting a timeout.
         * 
         * @param task
         * @param timeout
         * @param unit
         * @throws InterruptedException
         * @throws ExecutionException
         * @throws TimeoutException
         * 
         *             TODO Add variants of memberAdd() and friends that accept
         *             a timeout. They should use this method rather than
         *             {@link #runActorTask(ActorTask)}.
         */
        private void runActorTask(final ActorTask task, final long timeout,
                final TimeUnit unit) throws InterruptedException,
                ExecutionException, TimeoutException {
            if (!singleThreadActor) {
                /*
                 * Timeout support requires an executor service to run the actor
                 * tasks.
                 */
                throw new UnsupportedOperationException();
            }
            final FutureTask<Void> ft = new FutureTaskMon<Void>(task);
            synchronized (knownActorTasks) {
                if (!knownActorTasks.add(ft))
                    throw new AssertionError();
            }
            getActorExecutor().execute(ft);
            try {
                ft.get(timeout, unit);
            } finally {
                ft.cancel(true/* mayInterruptIfRunning */);
                synchronized (knownActorTasks) {
                    if (!knownActorTasks.remove(ft))
                        throw new AssertionError();
                }
            }
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
        
        @Override
        final public void memberAdd() {
            runActorTask(new MemberAddTask());
        }

        final private class MemberAddTask extends ActorTask {
            @Override
            protected void doAction() throws InterruptedException {
                conditionalMemberAddImpl();
            }
        }
        
        @Override
        final public void castVote(final long lastCommitTime) {
            runActorTask(new CastVoteTask(lastCommitTime));
        }
        
        private final class CastVoteTask extends ActorTask {
            private final long lastCommitTime;

            public CastVoteTask(final long lastCommitTime) {
                if (lastCommitTime < 0)
                    throw new IllegalArgumentException();
                this.lastCommitTime = lastCommitTime;
            }

            @Override
            protected void doAction() throws InterruptedException {
                if (!members.contains(serviceId))
                    throw new QuorumException(ERR_NOT_MEMBER + serviceId);
                /*
                 * Note: This has been modified to automatically add the service
                 * to the pipeline and the javadoc for the pre-/post- conditions
                 * declared in QuorumActor has been updated (9/9/2012). The
                 * change is inside of conditionalCastVoteImpl().
                 */
//                if (!pipeline.contains(serviceId))
//                    throw new QuorumException(ERR_NOT_PIPELINE + serviceId);
                conditionalCastVoteImpl(lastCommitTime);
            }
        }

        @Override
        final public void pipelineAdd() {
            runActorTask(new PipelineAddTask());
        }

        private final class PipelineAddTask extends ActorTask {
            @Override
            protected void doAction() throws InterruptedException {
                if (!members.contains(serviceId))
                    throw new QuorumException(ERR_NOT_MEMBER + serviceId);
                conditionalPipelineAddImpl();
            }
        }

        @Override
        final public void serviceJoin() {
            runActorTask(new ServiceJoinTask());
        }

        private final class ServiceJoinTask extends ActorTask {
            @Override
            protected void doAction() throws InterruptedException {
                if (!members.contains(serviceId))
                    throw new QuorumException(ERR_NOT_MEMBER + serviceId);
                if (!pipeline.contains(serviceId))
                    throw new QuorumException(ERR_NOT_PIPELINE + serviceId);
                conditionalServiceJoin();
            }
        }
        
//        final public void setLastValidToken(final long newToken) {
//            lock.lock();
//            try {
//                if (!isQuorum(joined.size()))
//                    throw new QuorumException(ERR_CAN_NOT_MEET
//                            + " too few services are joined: #joined="
//                            + joined.size() + ", k=" + k);
//                if (newToken <= lastValidToken)
//                    throw new QuorumException(ERR_BAD_TOKEN + "lastValidToken="
//                            + lastValidToken + ", but newToken=" + newToken);
//                if (token != NO_QUORUM)
//                    throw new QuorumException(ERR_QUORUM_MET);
//                conditionalSetLastValidToken(newToken);
//            } catch(InterruptedException e) {
//                // propagate the interrupt.
//                Thread.currentThread().interrupt();
//                return;
//            } finally {
//                lock.unlock();
//            }
//        }
//        
//        final public void setToken() {
//            lock.lock();
//            try {
//                conditionalSetToken();
//                log.warn("Quorum meet.");
//            } catch (InterruptedException e) {
//                // propagate the interrupt.
//                Thread.currentThread().interrupt();
//                return;
//            } finally {
//                lock.unlock();
//            }
//        }

        @Override
        final public void setToken(final long newToken) {
            runActorTask(new SetTokenTask(newToken));
        }

        private final class SetTokenTask extends ActorTask {

            private final long newToken;

            public SetTokenTask(final long newToken) {
                this.newToken = newToken;
            }

            @Override
            protected void doAction() throws InterruptedException {
                if (!isQuorum(joined.size()))
                    throw new QuorumException(ERR_CAN_NOT_MEET
                            + " too few services are joined: #joined="
                            + joined.size() + ", k=" + k);
                if (newToken <= lastValidToken)
                    throw new QuorumException(ERR_BAD_TOKEN + "lastValidToken="
                            + lastValidToken + ", but newToken=" + newToken);
                if (token != NO_QUORUM)
                    throw new QuorumException(ERR_QUORUM_MET);
                conditionalSetToken(newToken);
                log.warn("Quorum meet.");
            }
        }
        
        @Override
        final public void clearToken() {
            runActorTask(new ClearTokenTask());
        }

        private final class ClearTokenTask extends ActorTask {
            @Override
            protected void doAction() throws InterruptedException {
                conditionalClearToken();
            }
        }

        //
        // Public API "remove" methods.
        // 
        
        @Override
        final public void memberRemove() {
            runActorTask(new MemberRemoveTask());
        }

        private final class MemberRemoveTask extends ActorTask {
            @Override
            protected void doAction() throws InterruptedException {
                conditionalServiceLeaveImpl();
                conditionalPipelineRemoveImpl();
                conditionalWithdrawVoteImpl();
                conditionalMemberRemoveImpl();
            }
        }

//        /**
//         * An interruptable version of {@link #memberRemove()}.
//         * <p>
//         * Note: This is used by {@link AbstractQuorum#terminate()}. That code
//         * is already holding the lock in the caller's thread. Therefore it
//         * needs to run these operations in the same thread to avoid a deadlock
//         * with itself.
//         */
//        protected void memberRemoveInterruptable() throws InterruptedException {
//            if (!lock.isHeldByCurrentThread())
//                throw new IllegalMonitorStateException();
//            conditionalServiceLeaveImpl();
//            conditionalPipelineRemoveImpl();
//            conditionalWithdrawVoteImpl();
//            conditionalMemberRemoveImpl();
//        }

        @Override
        final public void withdrawVote() {
            runActorTask(new WithdrawVoteTask());
        }

        private final class WithdrawVoteTask extends ActorTask {
            @Override
            protected void doAction() throws InterruptedException {
                conditionalServiceLeaveImpl();
                conditionalPipelineRemoveImpl();
                conditionalWithdrawVoteImpl();
            }
        }
    
        @Override
        final public void pipelineRemove() {
            runActorTask(new PipelineRemoveTask());
        }

        private final class PipelineRemoveTask extends ActorTask {
            @Override
            protected void doAction() throws InterruptedException {
                conditionalWithdrawVoteImpl();
                conditionalServiceLeaveImpl();
                conditionalPipelineRemoveImpl();
            }
        }

        @Override
        final public void serviceLeave() {
            runActorTask(new ServiceLeaveTask());
        }

        private final class ServiceLeaveTask extends ActorTask {
            @Override
            protected void doAction() throws InterruptedException {
                conditionalWithdrawVoteImpl();
                conditionalPipelineRemoveImpl();
                conditionalServiceLeaveImpl();
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
                guard(new Guard() {
                    public void run() throws InterruptedException {
                        while (!members.contains(serviceId)) {
                            membersChange.await();
                        }
                    }
                });
            }
        }

        private void conditionalMemberRemoveImpl() throws InterruptedException {
            if (members.contains(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                doMemberRemove();
                guard(new Guard() {
                    public void run() throws InterruptedException {
                        while(members.contains(serviceId)) {
                            membersChange.await();
                        }
                    }
                });
            }
        }

        private void conditionalCastVoteImpl(final long lastCommitTime)
                throws InterruptedException {
            final Set<UUID> votesForCommitTime = votes.get(lastCommitTime);
            if (votesForCommitTime != null
                    && votesForCommitTime.contains(serviceId)) {
                // The service has already cast *a* vote.
                final Long lastCommitTime2 = getCastVote(serviceId);
                if (lastCommitTime2 != null
                        && lastCommitTime2.longValue() == lastCommitTime) {
                    // The service has already cast *this* vote.
                    return;
                }
            }
            if (log.isDebugEnabled())
                log.debug("serviceId=" + serviceId + ",lastCommitTime="
                        + lastCommitTime);
            // Withdraw any existing vote by this service.
            conditionalWithdrawVoteImpl();
            // Ensure part of the pipeline.
            conditionalPipelineAddImpl();
            // Cast a vote.
            doCastVote(lastCommitTime);
            guard(new Guard() {
                public void run() throws InterruptedException {
                    Long t = null;
                    while ((t = getCastVote(serviceId)) == null
                            || t.longValue() != lastCommitTime) {
                        votesChange.await();
                    }
                }
            });
        }

        private void conditionalWithdrawVoteImpl() throws InterruptedException {
            if (log.isDebugEnabled())
                log.debug(new StackInfoReport());
            while (true) {
                // check for a cast vote.
                final Long lastCommitTime = getCastVote(serviceId);
                if (lastCommitTime == null)
                    break;
                // cast vote exists. tell actor to withdraw vote.
                if (log.isDebugEnabled())
                    log.debug("will withdraw vote: serviceId=" + serviceId
                            + ",lastCommitTime=" + lastCommitTime);
                doWithdrawVote();
                guard(new Guard() {
                    // wait until the cast vote is withdrawn.
                    public void run() throws InterruptedException {
                        Long tmp;
                        while ((tmp = getCastVote(serviceId)) != null) {
                            if (!tmp.equals(lastCommitTime)) {
                                log.warn("Concurrent vote change: old="
                                        + lastCommitTime + ", new=" + tmp);
                                return;
                            }
                            votesChange.await();
                        }
                        // a cast vote has been cleared.
                        if (log.isDebugEnabled())
                            log.debug("withdrew vote: serviceId=" + serviceId
                                    + ",lastCommitTime=" + lastCommitTime);
                    }
                });
            }
        }

        private void conditionalPipelineAddImpl() throws InterruptedException {
            if (!pipeline.contains(serviceId)) {
                if (log.isDebugEnabled())
                    log.debug("serviceId=" + serviceId);
                doPipelineAdd();
                guard(new Guard() {
                    public void run() throws InterruptedException {
                        while (!pipeline.contains(serviceId)) {
                            pipelineChange.await();
                        }
                    }
                });
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
                guard(new Guard() {
                    public void run() throws InterruptedException {
                        while (pipeline.contains(serviceId)) {
                            pipelineChange.await();
                        }
                    }
                });
            }
        }

        private void conditionalServiceJoin() throws InterruptedException {
            /*
             * Discover the lastCommitTime of the consensus.
             */
            final Long lastCommitTime = getCastVoteIfConsensus(serviceId);
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
                final UUID[] joined = getJoined();
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
            guard(new Guard() {
                public void run() throws InterruptedException {
                    while (!joined.contains(serviceId)) {
                        joinedChange.await();
                    }
                }
            });
        }

        private void conditionalServiceLeaveImpl() throws InterruptedException {
            if (joined.contains(serviceId)) {
                doServiceLeave();
                guard(new Guard() {
                    public void run() throws InterruptedException {
                        while (joined.contains(serviceId)) {
                            joinedChange.await();
                        }
                    }
                });
            }
        }

        private void conditionalClearToken() throws InterruptedException {
            final long oldValue = token;
            if (oldValue == NO_QUORUM) {
                return;
            }
            doClearToken();
            guard(new Guard() {
                public void run() throws InterruptedException {
                    while (token == oldValue && client != null) {
                        quorumChange.await();
                    }
                }
            });
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            if (token != NO_QUORUM) {
                /*
                 * The token was concurrently updated so we will not attempt
                 * to clear it.
                 */
                throw new QuorumException(
                        "Concurrent set of the token: old=" + oldValue
                                + ", new=" + token);
            }
            log.warn(ERR_QUORUM_BREAK);
        }
        
        private void conditionalSetToken(final long newValue)
                throws InterruptedException {
            if (!lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();
            if (lastValidToken == newValue) {
                return;
            }
            if (lastValidToken > newValue) {
                /*
                 * The lastValidToken must advance.
                 */
                throw new QuorumException(
                        "Last valid token must advance: lastValidToken="
                                + lastValidToken + ", newValue=" + newValue);
            }
            if (token != NO_QUORUM) {
                /*
                 * Do not change a valid token. It MUST be cleared before it may
                 * be changed.
                 */
                throw new QuorumException("Concurrent set of token: expected="
                        + NO_QUORUM + ", actual=" + token);
            }
            /*
             * This breaks out of the loop if the lastValidToken is updated.
             * Since we are not willing to change a valid token (precondition)
             * and since we update the lastValidToken and the [token]
             * atomically, all we have to do is watch the lastValidToken for a
             * change.
             */
            final long oldValue = lastValidToken;
            doSetToken(newValue);
            guard(new Guard() {
                public void run() throws InterruptedException {
//                    log.fatal("LOOP: ENTER: newValue=" + newValue
//                            + ", lastValidToken=" + lastValidToken + ", token="
//                            + token, new RuntimeException("THREAD TRACE"));
                    while (lastValidToken == oldValue && client != null) {
                        quorumChange.await();
//                        log.fatal("LOOP: AWOKE: newValue=" + newValue
//                                + ", lastValidToken=" + lastValidToken
//                                + ", token=" + token);
                    }
//                    log.fatal("LOOP: EXIT: newValue=" + newValue
//                            + ", lastValidToken=" + lastValidToken + ", token="
//                            + token);
                }
            });
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            if (lastValidToken != newValue) {
                throw new QuorumException(
                        "Concurrent set of lastValidToken: old=" + oldValue
                                + ", new=" + lastValidToken + ", expected="
                                + newValue);
            }
            if (token != newValue) {
                throw new QuorumException("Concurrent set of token: expected="
                        + newValue + ", actual=" + token);
            }
        }

//        private void conditionalSetToken(final long newValue)
//        throws InterruptedException {
//    if (lastValidToken == newValue) {
//        return;
//    }
//    if (lastValidToken > newValue) {
//        /*
//         * The lastValidToken must advance.
//         */
//        throw new QuorumException(
//                "Last valid token must advance: lastValidToken="
//                        + lastValidToken + ", newValue=" + newValue);
//    }
//    final long oldValue = lastValidToken;
//    doSetLastValidToken(newValue);
//    while (lastValidToken == oldValue && client != null) {
//        quorumChange.await();
//    }
//    if (client == null)
//        throw new AsynchronousQuorumCloseException();
//    if (lastValidToken != newValue) {
//        throw new QuorumException(
//                "Concurrent set of lastValidToken: old=" + oldValue
//                        + ", new=" + lastValidToken + ", expected="
//                        + newValue);
//    }
//}
//
//private void conditionalSetToken() throws InterruptedException {
//    if (token == lastValidToken) {
//        return;
//    }
//    if (token != NO_QUORUM) {
//        /*
//         * Do not change a valid token. It MUST be cleared before it may
//         * be changed.
//         */
//        throw new QuorumException("Concurrent set of token: expected="
//                + lastValidToken + ", actual=" + token);
//    }
//    final long expected = lastValidToken;
//    doSetToken();
//    while (token != expected && client != null) {
//        quorumChange.await();
//    }
//    if (client == null)
//        throw new AsynchronousQuorumCloseException();
//    if (token != expected) {
//        throw new QuorumException("Concurrent set of token: expected="
//                + expected + ", actual=" + token);
//    }
//}

        /*
         * Abstract protected methods implement the atomic state change for just
         * the specific operation without any precondition maintenance.
         */

        abstract protected void doMemberAdd();

        final protected void doMemberRemove() {
            doMemberRemove(serviceId);
        }

        abstract protected void doMemberRemove(UUID serviceId);

        abstract protected void doCastVote(long lastCommitTime);

        final protected void doWithdrawVote() {
            doWithdrawVote(serviceId);
        }

        abstract protected void doWithdrawVote(UUID serviceId);

        abstract protected void doPipelineAdd();

        final protected void doPipelineRemove() {
            doPipelineRemove(serviceId);
        }

        abstract protected void doPipelineRemove(UUID serviceId);

        abstract protected void doServiceJoin();

        final protected void doServiceLeave() {
            doServiceLeave(serviceId);
        }

        abstract protected void doServiceLeave(UUID serviceId);

        abstract protected void doSetToken(long newToken);

//        abstract protected void doSetLastValidToken(long newToken);
//
//        abstract protected void doSetToken();
        
        abstract protected void doClearToken();


        /**
         * {@inheritDoc}
         * <p>
         * Note: This implements an unconditional remove of the specified
         * service. It is intended to force a different service out of the
         * pipeline. This code deliberately takes this action unconditionally
         * and does NOT await the requested state change.
         * <p>
         * Note: This code could potentially cause the remote service to
         * deadlock in one of the conditionalXXX() methods if it is concurrently
         * attempting to execute quorum action on itself. If this problem is
         * observed, we should add a timeout to the conditionalXXX() methods
         * that will force them to fail rather than block forever. This will
         * then force the service into an error state if its QuorumActor can not
         * carry out the requested action within a specified timeout.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/724" >
         *      HA wire pulling and sure kill testing </a>
         */
        @Override
        final public void forceRemoveService(final UUID psid) {
            if (log.isInfoEnabled())
                log.info("Will force remove of service" + ": thisService="
                        + serviceId + ", otherServiceId=" + psid);
            runActorTask(new ForceRemoveServiceTask(psid));
        }

        private class ForceRemoveServiceTask extends ActorTask {
            private final UUID psid;
            ForceRemoveServiceTask(final UUID psid) {
                this.psid = psid;
            }
            @Override
            protected void doAction() throws InterruptedException {
                log.warn("Forcing remove of service" + ": thisService="
                        + serviceId + ", otherServiceId=" + psid);
                /**
                 * Note: The JOINED[] entry MUST be removed first in case the
                 * service is not truely dead.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/724"
                 *      > HA wire pulling and sure kill testing </a>
                 */
                doServiceLeave(psid);
                doWithdrawVote(psid);
                doPipelineRemove(psid);
                doMemberRemove(psid);
            }
        }
        
        /**
         * Invoked when our client will become the leader to (a) reorganize the
         * write pipeline such that our client is the first service in the write
         * pipeline (the leader MUST be the first service in the write
         * pipeline); and (b) to optionally <em>optimize</em> the write pipeline
         * for the network topology.
         * <p>
         * The default implementation directs each service before this service
         * in the write pipeline to move itself to the end of the write
         * pipeline.
         * 
         * @return <code>true</code> if the pipeline order was modified.
         * 
         * @see com.bigdata.ha.HAGlue#moveToEndOfPipeline()
         * 
         * @todo Provide hooks to optimize the write pipeline for the network
         *       topology.
         */
        protected boolean reorganizePipeline() {
            if(lock.isHeldByCurrentThread()) {
                /*
                 * The lock can not be held across this method because it will
                 * cause deadlocks when we invoke Future#get() on the Future
                 * returned by moveToEndOfPipeline().
                 */
                throw new IllegalMonitorStateException();
            }
            final UUID[] pipeline;
            final UUID[] joined;
            final UUID leaderId;
//            final Long lastCommitTime;
//            final UUID[] voteOrder;
            lock.lock();
            try {
                /*
                 * Extract this stuff while holding the lock so we have a
                 * snapshot of the internal state.
                 */
                pipeline = getPipeline();
                joined = getJoined();
                leaderId = joined[0];
//                // last commit time for the leader or null if no consensus.
//                lastCommitTime = getLastCommitTimeConsensus(leaderId);
//                voteOrder = getVotes().get(lastCommitTime);
            } finally {
                lock.unlock();
            }
            if (qlog.isInfoEnabled()) {
//                log.info("lastCommitTimeConsensus = "+lastCommitTime);
//                log.info("vote    =" + Arrays.toString(voteOrder));
                qlog.info("pipeline=" + Arrays.toString(pipeline));
                qlog.info("joined  =" + Arrays.toString(joined));
                qlog.info("leader  = " + leaderId);
                qlog.info("self    = " + serviceId);
            }
            boolean modified = false;
            for (int i = 0; i < pipeline.length; i++) {
                final UUID otherId = pipeline[i];
                if (leaderId.equals(otherId)) {
                    // Done.
                    return modified;
                }
                // some other service in the pipeline ahead of the leader.
                final S otherService = getQuorumMember().getService(otherId);
                if (otherService == null) {
                    throw new QuorumException(
                            "Could not discover service: serviceId="
                                    + serviceId);
                }
                try {
                    // ask it to move itself to the end of the pipeline (RMI)
                    ((HAPipelineGlue) otherService).moveToEndOfPipeline().get();
                    modified = true;
                    if(qlog.isInfoEnabled()) {
                        qlog.info("moved   ="+otherId);
                        qlog.info("pipeline="+Arrays.toString(getPipeline()));
                        qlog.info("joined  ="+Arrays.toString(getJoined()));
                    }
                } catch (IOException ex) {
                    throw new QuorumException(
                            "Could not move service to end of the pipeline: serviceId="
                                    + serviceId + ", otherId=" + otherId, ex);
                } catch (InterruptedException e) {
                    // propagate interrupt.
                    Thread.currentThread().interrupt();
                    return modified;
                } catch (ExecutionException e) {
                    throw new QuorumException(
                            "Could not move service to end of the pipeline: serviceId="
                                    + serviceId + ", otherId=" + otherId, e);
                }
            }
            return modified;
        }
        
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
     * 
     * @todo This may need to be modified per one of the following approaches to
     *       be more robust. The current implemention is predicated on seeing
     *       all state transitions, and in fact the API requires that we observe
     *       each state transition since the {@link QuorumWatcherBase} methods
     *       only allow a single change at a time to be made to the internal
     *       quorum model. However, the order in which those state changes are
     *       observed can vary substantially when there are multiple services in
     *       the quorum since each service is acting independently based on its
     *       local model of the distributed quorum state.
     *       <p>
     *       (1) Instead of having logic in each method on
     *       {@link QuorumActorBase} to decide on postcondition actions, have a
     *       single method which examines the current state and makes decisions
     *       about what action(s) need to be executed in order to bring the
     *       system from an unstable state into a stable state. For example,
     *       when a vote is cast it may be that services should join, and when
     *       services join it may be that the quorum should meet. Those are
     *       transition from unstable states into stable states.
     *       <p>
     *       (2) Another approach would be to raise a protocol for consistent
     *       state changes into the API. For example, by having a revision
     *       number associated with each datum, much like zookeeper, and making
     *       changes conditional on the revision number not being concurrently
     *       incremented.
     * 
     * @todo Some of these methods will await preconditions become true, but no
     *       longer than a timeout, e.g., using
     *       {@link AbstractQuorum#awaitEnoughJoinedToMeet()}. This is used to
     *       compensate for uncertainty in the ordering of events in a
     *       distributed system such that the watcher might see the request to
     *       set the current token before it has seen the request to set the
     *       lastValidToken. Those requests can not be processed out of order
     *       since that would cause the token to regress. However, this approach
     *       can introduce occasional pauses while the timeout expires which are
     *       somewhat suprising. Option (1) above would address this concern
     *       using a single postcondition path for all state transitions, in
     *       which case we should be able to make due without the timeout /
     *       condition await.
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
            watcherActionService.execute(new Runnable() {
                public void run() {
                    /*
                     * Note: DO NOT acquire the lock here. It will cause
                     * reorganizePipeline() to deadlock if it runs with the lock
                     * held.
                     */
                    try {
                        r.run();
                    } catch (Throwable t) {
                        log.error(t, t);
                    }
                }
            });
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
                            try {
                                client.memberAdd();
                            } catch (Throwable t) {
                                launderThrowable(t);
                            }
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
                            try {
                                client.memberRemove();
                            } catch (Throwable t) {
                                launderThrowable(t);
                            }
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
                            try {
                                client.pipelineAdd();
                            } catch (Throwable t) {
                                launderThrowable(t);
                            }
                        }
                        if (lastId != null && clientId.equals(lastId)) {
                            /*
                             * If our client was the last service in the write
                             * pipeline, then the new service is now its
                             * downstream service. The client needs to handle
                             * this event by configuring itself to send data to
                             * that service.
                             */
                            try {
                                client.pipelineChange(null/* oldDownStream */,
                                        serviceId/* newDownStream */);
                            } catch (Throwable t) {
                                launderThrowable(t);
                            }
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
                 * the pipeline.
                 * 
                 * If the service that was remove is out client, then we notify
                 * it that it was removed from the pipeline.
                 * 
                 * If the service *before* the one being removed is our client,
                 * then we will notify it that its downstream service has
                 * changed.
                 * 
                 * If the service *after* the one being removed is our client,
                 * then we will notify it that its upstream service has changed.
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
                            try {
                                client.pipelineRemove();
                            } catch (Throwable t) {
                                launderThrowable(t);
                            }
                        }
                        if (priorNext != null && clientId.equals(priorNext[0])) {
                            /*
                             * Notify the client that its downstream service was
                             * removed from the write pipeline. The client needs
                             * to handle this event by configuring itself to
                             * send data to that service.
                             */
                            try {
                                client.pipelineChange(
                                        serviceId/* oldDownStream */,
                                        priorNext[1]/* newDownStream */);
                            } catch (Throwable t) {
                                launderThrowable(t);
                            }
                        }
                        if (priorNext != null && priorNext[0] != null
                                && clientId.equals(priorNext[1])) {
                            /*
                             * Notify the client that its upstream service was
                             * removed from the write pipeline but it is NOT the
                             * first service in the write pipeline. The client
                             * will need to handle this event by closing the
                             * client socket connection to the old upstream
                             * service.
                             */
                            try {
                                client.pipelineUpstreamChange();
                            } catch (Throwable t) {
                                launderThrowable(t);
                            }
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
                            try {
                                client.pipelineElectedLeader();
                            } catch (Throwable t) {
                                launderThrowable(t);
                            }
                        }
                    }
                    // queue client event.
                    sendEvent(new E(QuorumEventEnum.PIPELINE_REMOVE,
                            lastValidToken, token, serviceId));
                    /*
                     * If the service that left the pipeline was blocking the
                     * service that is trying to become the leader and we are
                     * that service trying to become the leader, then we need to
                     * elect ourselves as the leader.
                     * 
                     * Note: In fact, we only need to check this for the case
                     * when the service that was removed from the pipeline was
                     * at the head of the pipeline. That is only only time that
                     * a pipelineRemove() could unblock a leader election. We
                     * test that condition by checking whether prior is null.
                     * 
                     * @see TestHA3JournalServer#testStartABC_RebuildWithPipelineReorganization.
                     */
                    if (client != null && priorNext != null && priorNext[0] == null) {
                        final UUID clientId = client.getServiceId();
                        // The current consensus -or- null if our client is not in a
                        // consensus.
                        final Long lastCommitTime = getCastVoteIfConsensus(clientId);
                        if (lastCommitTime != null) {
                            /*
                             * Our client is in the consensus.
                             */
                            if (log.isInfoEnabled())
                                log.info("This client is in consensus on commitTime: "
                                        + lastCommitTime);
                            // Get the vote order. This is also the target join order.
                            final UUID[] voteOrder = votes.get(lastCommitTime)
                                    .toArray(new UUID[0]);
                            // the serviceId of the leader.
                            final UUID leaderId = voteOrder[0];
                            // true iff our client is the leader (else it is a
                            // follower since it is in the consensus).
                            final boolean isLeader = leaderId.equals(clientId);
                            if (isLeader) {
                                final int njoined = joined.size();
                                if (njoined >= kmeet && token == NO_QUORUM) {
                                    /*
                                     * Elect the leader.
                                     */
                                    final UUID[] pipeline = getPipeline();
                                    if (pipeline.length > 0
                                            && pipeline[0].equals(clientId)) {
                                        /*
                                         * The pipeline is well organized, so
                                         * elect ourselves as the leader now.
                                         */
                                        if (log.isInfoEnabled())
                                            log.info("Electing leader: "
                                                    + AbstractQuorum.this
                                                            .toString());
                                        doAction(new Runnable() {
                                            /*
                                             * Note: This needs to be submitted
                                             * as an action since we will
                                             * otherwise be in the zk event
                                             * thread and be unable to observe
                                             * the effect of the action that we
                                             * take.
                                             * 
                                             * This was observed for the
                                             * following HA3 test. A was unable
                                             * to observe the new token value
                                             * when it invoked setToken() from
                                             * the current thread rather than by
                                             * submitting the setToken() method
                                             * in a runnable as we do here.
                                             * 
                                             * See TestHA3JournalServer.
                                             * testStartABC_RebuildWithPipelineReorganisation
                                             */
                                            public void run() {
                                                actor.setToken(lastValidToken + 1);
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }
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
                LinkedHashSet<UUID> votesForCommitTime = votes
                        .get(lastCommitTime);
                if (votesForCommitTime == null) {
                    // None found, so create an empty set now.
                    votesForCommitTime = new LinkedHashSet<UUID>();
                    // And add it to the map.
                    votes.put(lastCommitTime, votesForCommitTime);
                }
                if (votesForCommitTime.add(serviceId)) {
                    // The service cast its vote.
                    votesChange.signalAll();
                    final int nvotes = votesForCommitTime.size();
                    if (log.isInfoEnabled())
                        log.info("serviceId=" + serviceId.toString()
                                + ", lastCommitTime=" + lastCommitTime
                                + ", nvotes=" + nvotes);
                    // queue event.
                    sendEvent(new E(QuorumEventEnum.CAST_VOTE, lastValidToken,
                            token, serviceId, lastCommitTime));
                    if (isQuorum(nvotes)) {
                        final QuorumMember<S> client = getClientAsMember();
                        if (nvotes == kmeet) {
                            if (client != null) {
                                /*
                                 * Tell the client that consensus has been
                                 * reached on this last commit time.
                                 */
                                try {
                                    client.consensus(lastCommitTime);
                                } catch (Throwable t) {
                                    launderThrowable(t);
                                }
                            }
                            // queue event.
                            sendEvent(new E(QuorumEventEnum.CONSENSUS,
                                    lastValidToken, token, serviceId, Long
                                            .valueOf(lastCommitTime)));
                        }
                        if (client != null) {
                            final UUID clientId = client.getServiceId();
                            final UUID[] voteOrder = votesForCommitTime.toArray(new UUID[0]);
                            if (nvotes == kmeet
                                    && clientId.equals(voteOrder[0])) {
                                /*
                                 * The client is the first service in the vote
                                 * order, so it will be the first service to
                                 * join and will become the leader when the
                                 * other services join.
                                 * 
                                 * Instruct the client's actor to join now. The
                                 * other clients will notice the service join
                                 * and join once their predecessor in the vote
                                 * order joins.
                                 * 
                                 * Note: This will cause recursion through the
                                 * actor-watcher reflex arc unless it is run in
                                 * another thread.
                                 */
                                if (log.isInfoEnabled())
                                    log.info("First service will join");
                                doAction(new Runnable() {public void run() {actor.serviceJoin();}});
                            } else {
                                if (clientId.equals(serviceId)) {
                                    /*
                                     * The service which just cast its vote is
                                     * our client. If the service before our
                                     * client in the vote order is already
                                     * joined, then it is time for our client to
                                     * join.
                                     */
                                    final int index = getIndexInVoteOrder(
                                            clientId, voteOrder);
                                    if (index == -1) {
                                        throw new AssertionError(
                                                AbstractQuorum.this.toString());
                                    }
                                    // the service our client waits for to join.
                                    final UUID waitsFor = voteOrder[index - 1];
                                    if (joined.contains(waitsFor)) {
                                        // our client can join immediately.
                                        if (log.isInfoEnabled())
                                            log.info("Follower will join: "
                                                    + AbstractQuorum.this
                                                            .toString());
                                        doAction(new Runnable() {
                                            public void run() {
                                                actor.serviceJoin();
                                                if(qlog.isInfoEnabled())
                                                    qlog.info("After join: "
                                                                + AbstractQuorum.this);
                                            }
                                        });
                                    }
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
                    final Set<UUID> votesForCommitTime = entry.getValue();
                    if (votesForCommitTime.remove(serviceId)) {
                        // The vote was withdrawn.
                        votesChange.signalAll();
                        sendEvent(new E(QuorumEventEnum.WITHDRAW_VOTE,
                                lastValidToken, token, serviceId));
                        if (votesForCommitTime.size() + 1 == kmeet) {
                            final QuorumMember<S> client = getClientAsMember();
                            if (client != null) {
                                // Tell the client that the consensus was lost.
                                try {
                                    client.lostConsensus();
                                } catch (Throwable t) {
                                    launderThrowable(t);
                                }
                            }
                        }
                        // found where the service had cast its vote.
                        if (log.isInfoEnabled())
                            log.info("serviceId=" + serviceId
                                    + ", lastCommitTime=" + entry.getKey());
                        if (votesForCommitTime.isEmpty()) {
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
         * updates the (lastValidToken,token) pair atomically. 
         * 
         * @param newToken
         *            The new token.
         */
        protected void setToken(final long newToken) {
            lock.lock();
            try {
                /*
                 * @todo think through this again. if the watcher can see events
                 * out of their proper order then it may need to do things like
                 * wait for enough joined services before a quorum can meet.
                 * However, out of order events could also result in overwriting
                 * a newer token with an older one, which is a Bad Thing.
                 */
                if (lastValidToken == newToken && token == newToken) {
                    // Already met.
                    return;
                }
                if (lastValidToken > newToken) {
                    log.error("Concurrent update: lastValidToken="
                            + lastValidToken + ", newToken=" + newToken);
                    return;
                }
                try {
                    awaitEnoughJoinedToMeet();
                } catch (TimeoutException e1) {
                    throw new QuorumException(e1);
                }
                token = lastValidToken = newToken;
                quorumChange.signalAll();
                if (log.isInfoEnabled())
                    log.info("newToken=" + newToken);
                final UUID leaderId = getLeaderId();
                // must exist when quorum meets.
                assert leaderId != null : "Leader is null : "
                        + AbstractQuorum.this.toString();
                /*
                 * The quorum has met.
                 */
                if (log.isInfoEnabled())
                    log.info("leader=" + leaderId + ", newToken=" + token
                // + " : " + AbstractQuorum.this
                        );
                final QuorumMember<S> client = getClientAsMember();
                if (client != null) {
                    try {
                        client.quorumMeet(token, leaderId);
                    } catch (Throwable t) {
                        launderThrowable(t);
                    }
                }
                /*
                 * Note: If we send out an event here then any code path that
                 * reenters the AbstractQuorum from another thread seeking a
                 * lock will deadlock. Either the event must be dispatched after
                 * we release the lock (which might be held at multiple points
                 * in the call stack) or the dispatch of the event must not
                 * cause this thread to block. That could either be accomplished
                 * by handing off the event to a dispatcher thread or by
                 * requiring the receiver to perform a non-blocking operation
                 * when they get the event.
                 */
                final E e = new E(QuorumEventEnum.QUORUM_MEET, lastValidToken,
                        token, leaderId);
                sendEvent(e);
            } finally {
                lock.unlock();
            }
        }

//        /**
//         * Method is invoked by the {@link QuorumWatcher} when the leader
//         * updates the lastValidToken. Note that the lastValidToken is updated
//         * first and then the current token is set from the lastValidToken.
//         * Since the quorum does not meet until the token has been updated, this
//         * allows us to create an atomic protocol from piecewise atomic updates.
//         * 
//         * @param newToken
//         *            The new token.
//         */
//        protected void setLastValidToken(final long newToken) {
//            lock.lock();
//            try {
//                awaitEnoughJoinedToMeet();
//                lastValidToken = newToken;
//                quorumChange.signalAll();
//                if (log.isInfoEnabled())
//                    log.info("newToken=" + newToken);
//                sendEvent(new E(QuorumEventEnum.SET_LAST_VALID_TOKEN, newToken,
//                        token, null/* serviceId */));
//            } finally {
//                lock.unlock();
//            }
//        }
//
//        /**
//         * Method is invoked by the {@link QuorumWatcher} when the leader
//         * publishes out a new quorum token.
//         */
//        protected void setToken() {
//            lock.lock();
//            try {
//                {
//                    /*
//                     * Verify preconditions
//                     */
//                    // This must be updated before the token is set.
//                    if (lastValidToken == NO_QUORUM)
//                        throw new AssertionError(
//                                "Last valid token never set : "
//                                        + AbstractQuorum.this);
//                }
//                awaitEnoughJoinedToMeet();
//                // Set the token from the lastValidToken.
//                final long token = AbstractQuorum.this.token = lastValidToken;
//                // signal everyone that the quorum has met.
//                quorumChange.signalAll();
//                final UUID leaderId = getLeaderId();
//                // must exist when quorum meets.
//                assert leaderId != null : "Leader is null : "
//                        + AbstractQuorum.this.toString();
//                final QuorumMember<S> client = getClientAsMember();
////                if (client != null) {
////                    /*
////                     * Since our client is a quorum member, figure out whether
////                     * or not it is a follower, in which case we will send it
////                     * the electedFollower() message.
////                     */
////                    final UUID clientId = client.getServiceId();
////                    // Our client was elected the leader?
////                    if (joined.contains(clientId) && clientId.equals(leaderId)) {
////                        client.electedLeader();
////                        sendEvent(new E(QuorumEventEnum.ELECTED_LEADER,
////                                lastValidToken, token, clientId));
////                        if (log.isInfoEnabled())
////                            log.info("leader=" + clientId + ", token="
////                                    + token + ", leader=" + leaderId);
////                    }
////                    // Our client was elected a follower?
////                    if (joined.contains(clientId) && !clientId.equals(leaderId)) {
////                        client.electedFollower();
////                        sendEvent(new E(QuorumEventEnum.ELECTED_FOLLOWER,
////                                lastValidToken, token, clientId));
////                        if (log.isInfoEnabled())
////                            log.info("follower=" + clientId + ", token="
////                                    + token + ", leader=" + leaderId);
////                    }
////                }
//                /*
//                 * The quorum has met.
//                 */
//                log.warn("leader=" + leaderId + ", newToken=" + token
////                        + " : " + AbstractQuorum.this
//                        );
//                if (client != null) {
//                    client.quorumMeet(token, leaderId);
//                }
//                sendEvent(new E(QuorumEventEnum.QUORUM_MEET, lastValidToken,
//                        token, leaderId));
//            } finally {
//                lock.unlock();
//            }
//        }

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
                    quorumChange.signalAll();
                    log.warn(ERR_QUORUM_BREAK);
                    final QuorumMember<S> client = getClientAsMember();
                    if (client != null) {
                        // Notify the client that the quorum broke.
                        try {
                            client.quorumBreak();
                        } catch(Exception t) {
                            launderThrowable(t);
                        }
                    }
                    sendEvent(new E(QuorumEventEnum.QUORUM_BROKE,
                            lastValidToken, token, null/* serviceId */));
/*
 * Note: Replacing this code with the logic below fixes a problem where a leader
 * was failing to update its lastCommitTime after a quorum break caused by
 * a follower that was halted.  The quorum could not meet after the follower
 * was restarted because the leader had not voted for a lastCommitTime. The
 * code below addresses that explicitly as long as the QuorumMember is a 
 * QuorumService. 
 */
//                    if (client != null) {
//                        final UUID clientId = client.getServiceId();
//                        if(joined.contains(clientId)) {
//                            // If our client is joined, then force serviceLeave.
////                            new Thread() {public void run() {actor.serviceLeave();}}.start();
//                            doAction(new Runnable() {public void run() {actor.serviceLeave();}});
//                        }
//                    }
                  if (client != null) {
                    final UUID clientId = client.getServiceId();
                    if (joined.contains(clientId)) {
                      final QuorumMember<S> member = getMember();
                      if (false && member instanceof QuorumService) {
                        /*
                         * Set the last commit time.
                         * 
                         * Note: After a quorum break, a service MUST
                         * recast its vote for it's then-current
                         * lastCommitTime. If it fails to do this, then
                         * it will be impossible for a consensus to form
                         * around the then current lastCommitTimes for
                         * the services. It appears to be quite
                         * difficult for the service to handle this
                         * itself since it can not easily recognize when
                         * it's old vote has been widthdrawn. Therefore,
                         * the logic to do this has been moved into the
                         * QuorumWatcherBase.
                         */
                        final long lastCommitTime = ((QuorumService<?>) member)
                                .getLastCommitTime();
                        doAction(new Runnable() {
                            public void run() {
                                // recast our vote.
                                actor.castVote(lastCommitTime);
                            }
                        });
                    } else {
                        // just withdraw the vote.
                        doAction(new Runnable() {
                            public void run() {
                                actor.withdrawVote();
                            }
                        });
                    }
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
//                final boolean willMeet = njoined == kmeet;
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
                     * or not it just joined, in which case we send it a
                     * synchronous message.
                     */
                    final UUID clientId = client.getServiceId();
                    if(serviceId.equals(clientId)) {
                        try {
                            client.serviceJoin();
                        } catch (Throwable t) {
                            launderThrowable(t);
                        }
                    }
                }
                // queue event.
                sendEvent(new E(QuorumEventEnum.SERVICE_JOIN, lastValidToken,
                        token, serviceId));
                if (client != null) {
                    final UUID clientId = client.getServiceId();
                    // The current consensus -or- null if our client is not in a
                    // consensus.
                    final Long lastCommitTime = getCastVoteIfConsensus(clientId);
                    if (lastCommitTime != null) {
                        /*
                         * Our client is in the consensus.
                         */
                        if (log.isInfoEnabled())
                            log.info("This client is in consensus on commitTime: " + lastCommitTime);

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
                            if (njoined >= kmeet && token == NO_QUORUM) {
                                /*
                                 * Elect the leader.
                                 */
                                if (log.isInfoEnabled())
                                    log.info("Ready to elect leader or reorganize pipeline: "
                                            + AbstractQuorum.this.toString());
                                doAction(new Runnable() {
                                    public void run() {
                                        if (actor.reorganizePipeline()) {
                                            /*
                                             * Reorganizing the pipeline can
                                             * cause service leaves for services
                                             * before the leader in the pipeline
                                             * order. This means that we need to
                                             * wait until the pipeline is
                                             * properly organized and then try
                                             * again.
                                             */
                                            if (log.isInfoEnabled())
                                                log.info("Reorganized the pipeline: "
                                                        + AbstractQuorum.this
                                                                .toString());
                                        } else {
                                            /*
                                             * The pipeline is well organized,
                                             * so elect the leader now.
                                             */
                                            if (log.isInfoEnabled())
                                                log.info("Electing leader: "
                                                    + AbstractQuorum.this
                                                            .toString());
//                                            actor
//                                                    .setLastValidToken(lastValidToken + 1);
                                            actor.setToken(lastValidToken + 1);
                                        }
                                    }
                                });
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
                            final UUID waitsFor = voteOrder[index - 1];
                            /*
                             * If the service which joined immediately proceeds
                             * our client in the vote order, then do a service
                             * join for our client now.
                             */
                            if (serviceId.equals(waitsFor)) {
                                /*
                                 * Elect a follower.
                                 */
                                if (log.isInfoEnabled())
                                    log.info("Follower will join: "
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
                final boolean wasJoined = njoinedBefore >= kmeet;
                // iff the leader just left the quorum.
                final boolean leaderLeft = wasJoined
                        && serviceId.equals(leaderId);
                // iff the quorum will break.
                final boolean willBreak = leaderLeft
                        || (njoinedBefore == kmeet);
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
                     * by (a) doing an abort() which will discard any buffered
                     * writes and reload their current root block; and (b) cast
                     * a vote for their current commit time. Once a consensus is
                     * reached on the current commit time, the services will
                     * join in the vote order, a new leader will be elected, and
                     * the quorum will meet again.
                     */
//                    new Thread() {public void run(){actor.clearToken();}}.start();
                    doAction(new Runnable() {public void run() {actor.clearToken();}});
                }
                if (client != null) {
                    // Notify all quorum members that a service left.
                    try {
                        /**
                         * PREVIOUSLY called client.serviceLeave()
                         * unconditionally
                         * 
                         * @see <a
                         *      href="https://sourceforge.net/apps/trac/bigdata/ticket/695">
                         *      HAJournalServer reports "follower" but is in
                         *      SeekConsensus and is not participating in
                         *      commits </a>
                         */
                        final UUID clientId = client.getServiceId();
                    	if (serviceId.equals(clientId))
                    			client.serviceLeave();
                    	
                    } catch (Throwable t) {
                        launderThrowable(t);
                    }
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
    protected void sendEvent(final QuorumEvent e) {
        if (log.isTraceEnabled())
            log.trace("" + e);
        if (sendSynchronous) {
            sendEventNow(e);
            return;
        }
        @SuppressWarnings("unused")
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
                    + ",currentToken="
                    + token
                    + ",serviceId="
                    + serviceId
                    + (type == QuorumEventEnum.CAST_VOTE ? ",lastCommitTime="
                            + lastCommitTime : "") + "}";
        }

    }

    /**
     * Launder something thrown by the {@link QuorumClient}.
     * 
     * @param t
     *            The throwable.
     */
    protected void launderThrowable(final Throwable t) {

        if (InnerCause.isInnerCause(t, InterruptedException.class)) {

            // Propagate the interrupt.
            Thread.currentThread().interrupt();

            return;
        }

        /*
         * Log and ignore. We do not want bugs in the QuorumClient to interfere
         * with the Quorum protocol.
         */
        
        log.error(t, t);

    }

}
