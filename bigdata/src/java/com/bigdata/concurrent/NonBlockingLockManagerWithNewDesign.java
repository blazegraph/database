/**

 Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 3, 2007
 */

package com.bigdata.concurrent;

import static com.bigdata.concurrent.NonBlockingLockManagerWithNewDesign.ServiceRunState.Halted;
import static com.bigdata.concurrent.NonBlockingLockManagerWithNewDesign.ServiceRunState.Running;
import static com.bigdata.concurrent.NonBlockingLockManagerWithNewDesign.ServiceRunState.Shutdown;
import static com.bigdata.concurrent.NonBlockingLockManagerWithNewDesign.ServiceRunState.ShutdownNow;
import static com.bigdata.concurrent.NonBlockingLockManagerWithNewDesign.ServiceRunState.Starting;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.AbstractTask;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.QueueStatisticsTask;

/**
 * This class coordinates a schedule among concurrent operations requiring
 * exclusive access to shared resources. Whenever possible, the result is a
 * concurrent schedule - that is, operations having non-overlapping lock
 * requirements run concurrently while operations that have lock contentions are
 * queued behind operations that currently have locks on the relevant resources.
 * A {@link ResourceQueue} is created for each resource and used to block
 * operations that are awaiting a lock. When locks are not being pre-declared
 * (and hence deadlocks are possible), a {@link TxDag WAITS_FOR} graph is used
 * to detect deadlocks.
 * <p>
 * This implementation uses a single {@link AcceptTask} thread to accept tasks,
 * update the requests in the {@link ResourceQueue}s, and in general perform
 * housekeeping for the internal state. Tasks submitted to this class ARE NOT
 * bound to a worker thread until they are executed by the delegate
 * {@link Executor}.
 * <p>
 * When a task is removed from the {@link #acceptedTasks} queue it is passed to
 * {@link #ready(Runnable)} (if no locks were requested or if all locks could be
 * granted immediately) or placed into one or more {@link ResourceQueue}s to
 * await its locks. When those locks become available, {@link #ready(Runnable)}
 * will be invoked with the task. Whenever a task releases its locks, we note
 * the {@link ResourceQueue}s in which it held the lock.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param R
 *            The type of the object that identifies a resource for the purposes
 *            of the locking system. This is typically the name of an index.
 * 
 * @see #ready(Runnable)
 * 
 * @todo a fair option? What constraints or freedoms would it provide?
 * 
 * @todo a {@link SynchronousQueue} for the {@link #acceptedTasks}?
 * 
 * FIXME In order to support 2PL we need to decouple the {@link LockFutureTask}
 * from the transaction with which it is associated and use the latter in the
 * {@link TxDag}. Otherwise each {@link #submit(Comparable[], Callable)} will
 * always look like a new transaction (2PL is impossible unless you can execute
 * multiple tasks for the same transaction).
 * 
 * @todo Support escalation of operation priority based on time and scheduling
 *       of higher priority operations. the latter is done by queueing lock
 *       requests in front of pending requests for each resource on which an
 *       operation attempt to gain a lock. The former is just a dynamic
 *       adjustment of the position of the operation in the resource queue where
 *       it is awaiting a lock (an operation never awaits more than one lock at
 *       a time). This facility could be used to give priority to distributed
 *       transactions over local unisolated operations and to priviledge certain
 *       operations that have low latency requirements. This is not quite a
 *       "real-time" guarentee since the VM is not (normally) providing
 *       real-time guarentees and since we are not otherwise attempting to
 *       ensure anything except lower latency when compared to other operations
 *       awaiting their own locks.
 */
abstract class NonBlockingLockManagerWithNewDesign</* T, */R extends Comparable<R>> {

    final protected static Logger log = Logger
            .getLogger(NonBlockingLockManagerWithNewDesign.class);

    final protected static boolean INFO = log.isInfoEnabled();
    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * Each resource that can be locked has an associated {@link ResourceQueue}.
     * <p>
     * Note: This is a concurrent collection since new resources may be added
     * while concurrent operations resolve resources to their queues. Stale
     * {@link ResourceQueue}s are purged after they become only weakly
     * reachable.
     * 
     * @todo reconsider the timeout. it is set to one LBS period right now. this
     *       is just so you can see which resource queues had activity in the
     *       last reporting period.
     */
    final private ConcurrentWeakValueCacheWithTimeout<R, ResourceQueue<LockFutureTask<? extends Object>>> resourceQueues = new ConcurrentWeakValueCacheWithTimeout<R, ResourceQueue<LockFutureTask<? extends Object>>>(
            1000/* nresources */, TimeUnit.SECONDS.toNanos(60));

    /**
     * True iff locks MUST be predeclared by the operation - this is a special
     * case of 2PL (two-phrase locking) that allows significant optimizations
     * and avoids the possibility of deadlock altogether.
     */
    final private boolean predeclareLocks;

    /**
     * When true, the resources in a lock request are sorted before the lock
     * requests are made to the various resource queues. This option is ONLY
     * turned off for testing purposes as it ALWAYS reduces the chance of
     * deadlocks and eliminates it entirely when locks are also predeclared.
     */
    final private boolean sortLockRequests;

    /**
     * The maximum #of times that a task whose lock requests would produce a
     * deadlock will be retried. Deadlock is typically transient but the
     * potential for deadlock can vary depending on the application. Note that
     * deadlock CAN NOT arise if you are predeclaring and sorting the lock
     * requests.
     */
    final private int maxLockTries;

    /**
     * Used to track dependencies among transactions.
     */
    final private TxDag waitsFor;

    /**
     * Used to run the {@link AcceptTask} and the {@link MonitorTask}.
     * 
     * FIXME Monitor this service using a {@link QueueStatisticsTask} to convert
     * {@link Counters#nrunning} and {@link Counters#nwaiting} into moving
     * averages.
     */
    private final ExecutorService service = Executors
            .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                    .getName()));

    /**
     * The service run state.
     */
    private volatile ServiceRunState serviceRunState = ServiceRunState.Starting;

    /**
     * Lock used to protect changes various state changes, including change to
     * the {@link #serviceRunState}.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition is signaled whenever the {@link AcceptTask} needs to wake up.
     */
    private final Condition stateChanged = lock.newCondition();

    /**
     * Tasks accepted for eventual execution but not yet waiting on their locks.
     * Tasks are removed from this queue once they have posted their lock
     * requests. The queue is cleared if the lock service is halted.
     * 
     * @todo review the use of this queue carefully before trying to replace it
     *       with a {@link SynchronousQueue} as things like isEmpty() will
     *       always report <code>true</code>.
     * 
     * @todo this could be renamed the deadlockQueue and
     *       {@link TaskRunState#Accepted} could be renamed "Deadlock".
     */
    final private BlockingQueue<LockFutureTask<? extends Object>> acceptedTasks = new LinkedBlockingQueue<LockFutureTask<? extends Object>>();

    /**
     * Run states for the {@link NonBlockingLockManagerWithNewDesign}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static enum ServiceRunState {

        /**
         * During startup. Tasks are NOT accepted. (Since no tasks are accepted
         * and nothing can be queued [tasksCancelled] is never considered in
         * this run state.)
         */
        Starting(0, false/* tasksAccepted */, true/* tasksCancelled */),

        /**
         * While running (aka open). Tasks are accepted and submitted for
         * execution once they hold their locks.
         */
        Running(1, true/* tasksAccepted */, false/* tasksCancelled */),

        /**
         * When shutting down normally. New tasks are not accepted but
         * {@link Future}s are still monitored for completion and waiting tasks
         * will eventually be granted their locks and execute on the delegate.
         */
        Shutdown(2, false/* tasksAccepted */, false/* tasksCancelled */),

        /**
         * When shutting down immediately. New tasks are not accepted, tasks
         * waiting for their locks are cancelled (they will not execute) and
         * {@link Future}s for running tasks are cancelled (they are
         * interrupted).
         */
        ShutdownNow(3, false/* tasksAccepted */, true/* tasksCancelled */),

        /**
         * When halted. New tasks are not accepted. No tasks are waiting. Any
         * {@link Future}s were cancelled.
         */
        Halted(4, false/* tasksAccepted */, true/* tasksCancelled */);

        /**
         * @param val
         *            The integer value for this run state.
         * @param tasksAccepted
         *            <code>true</code> iff new tasks are accepted and will be
         *            eventually executed while in this run state.
         * @param tasksCancelled
         *            <code>true</code> iff tasks (whether accepted, waiting
         *            for their locks, or executed) are cancelled while in this
         *            run state.
         */
        private ServiceRunState(final int val, final boolean tasksAccepted,
                final boolean tasksCancelled) {

            this.val = val;
            
            this.tasksAccepted = tasksAccepted;
            
            this.tasksCancelled = tasksCancelled;

        }

        final private int val;

        final private boolean tasksAccepted;

        final private boolean tasksCancelled;
        
        public int value() {

            return val;

        }

        /**
         * Return <code>true</code> iff new tasks are accepted and will be
         * eventually executed while in this run state.
         */
        public boolean tasksAccepted() {
            
            return tasksAccepted;
            
        }

        /**
         * Return <code>true</code> iff tasks (whether accepted, waiting for
         * their locks, or executed) are cancelled while in this run state.
         */
        public boolean tasksCancelled() {

            return tasksCancelled;
            
        }

        /**
         * Return <code>true</code> iff the transition the specified run state
         * is legal.
         * 
         * @param newval
         *            The new run state.
         *            
         * @return <code>true</code> if that is a legal state transition.
         */
        public boolean isTransitionLegal(final ServiceRunState newval) {

            if (this == Starting) {

                if (newval == Running)
                    return true;

                if (newval == Halted)
                    return true;

            } else if (this == Running) {

                if (newval == Shutdown)
                    return true;

                if (newval == ShutdownNow)
                    return true;

            } else if (this == Shutdown) {

                if (newval == ShutdownNow)
                    return true;

                if (newval == Halted)
                    return true;

            } else if (this == ShutdownNow) {

                if (newval == Halted)
                    return true;

            }

            return false;

        }

    }

    /*
     * counters
     */

    synchronized public CounterSet getCounters() {

        if (root == null) {

            root = new CounterSet();

            root.addCounter("nrejected", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.nrejected);
                }
            });

            root.addCounter("naccepted", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.naccepted);
                }
            });

            root.addCounter("ncancel", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.ncancel);
                }
            });

            root.addCounter("nerror", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.nerror);
                }
            });

            // #of deadlocks (can be incremented more than once for the same task).
            root.addCounter("ndeadlock", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.ndeadlock);
                }
            });

            // #of tasks waiting on locks.
            root.addCounter("nwaiting", new Instrument<Integer>() {
                public void sample() {
                    setValue(counters.nwaiting);
                }
            });

            // #of tasks ready to be executed (on the readyQueue).
            root.addCounter("nready", new Instrument<Integer>() {
                public void sample() {
                    setValue(counters.nready);
                }
            });

            // #of tasks that are actually executing (in run()).
            root.addCounter("nrunning", new Instrument<Integer>() {
                public void sample() {
                    setValue(counters.nrunning);
                }
            });

            // the maximum observed value for [nrunning].
            root.addCounter("maxRunning", new Instrument<Integer>() {
                public void sample() {
                    setValue(counters.maxRunning);
                }
            });

            // #of resource queues
            root.addCounter("nresourceQueues", new Instrument<Integer>() {
                public void sample() {
                    setValue(resourceQueues.size());
                }
            });
            
            root.addCounter("runState", new Instrument<String>() {
                public void sample() {
                    setValue(serviceRunState.toString());
                }
            });

            /*
             * Displays the #of tasks waiting on each resource queue for the
             * lock (does not count the task at the head of the queue).
             * 
             * @todo this could also be handled by dynamic reattachment of the
             * counters.
             */
            root.addCounter("queues", new Instrument<String>() {
                public void sample() {
                    final Iterator<Map.Entry<R, WeakReference<ResourceQueue<LockFutureTask<? extends Object>>>>> itr = resourceQueues
                            .entryIterator();
                    final LinkedList<ResourceQueueSize> list = new LinkedList<ResourceQueueSize>();
                    while (itr.hasNext()) {
                        final Map.Entry<R, WeakReference<ResourceQueue<LockFutureTask<? extends Object>>>> entry = itr
                                .next();
                        final WeakReference<ResourceQueue<LockFutureTask<? extends Object>>> queueRef = entry
                                .getValue();
                        final ResourceQueue<LockFutureTask<? extends Object>> queue = queueRef
                                .get();
                        if (queue == null)
                            continue;
                        list.add(new ResourceQueueSize(queue));
                    }
                    final Object[] a = list.toArray();
                    Arrays.sort(a);
                    final StringBuilder sb = new StringBuilder();
                    for (Object t : a) {
                        sb.append(t.toString());
                        sb.append(" ");
                    }
                    setValue(sb.toString());
                }
            });
            
        }

        return root;

    }

    private CounterSet root;

    /**
     * Helper class pairs up the resource with its sampled queue size and allows
     * ordering of the samples.  E.g., by size or by resource.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ResourceQueueSize implements Comparable<ResourceQueueSize> {
        final R resource;
        int size;
        public ResourceQueueSize(ResourceQueue<LockFutureTask<? extends Object>> queue){
            resource = queue.getResource();
            size = queue.getQueueSize();
        }
        public int compareTo(ResourceQueueSize arg0) {
            // resource name order.
//            return resource.compareTo(arg0.resource);
            // descending queue size order.
            return arg0.size - size;
        }
        public String toString() {
            return "(" + resource + "," + size + ")";
        }
    }
    
    /**
     * Counters for the {@link NonBlockingLockManagerWithNewDesign}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class Counters {

        /**
         * The #of tasks that were rejected by the service (running total).
         */
        public long nrejected;

        /**
         * The #of tasks that were accepted by the service (running total).
         */
        public long naccepted;

        /**
         * The #of tasks that were cancelled (running total).
         */
        public long ncancel;

        /**
         * The #of tasks whose exception was set (running total).
         */
        public long nerror;

        /**
         * The #of tasks that deadlocked when they attempted to acquire their
         * locks (running total). Note that a task MAY retry lock acquisition
         * and this counter will be incremented each time it does so and then
         * deadlocks.
         */
        public long ndeadlock;

        /**
         * #of tasks that are waiting on one or more locks.
         */
        public int nwaiting;

        /**
         * The #of tasks that have acquired their locks but not yet begun to
         * execute.
         */
        public int nready;
        
        /**
         * #of tasks are currently executing (in {@link LockFutureTask#run()}).
         */
        public int nrunning;

        /**
         * The maximum observed value of {@link #nrunning}.
         */
        public int maxRunning;
        
    }

    /**
     * Counters for various things.
     */
    final Counters counters = new Counters();

    /**
     * Create a lock manager. No concurrency limit imposed when
     * <i>predeclareLocks</i> is <code>true</code> as deadlocks are
     * impossible and we do not maintain a WAITS_FOR graph.
     * 
     * @param maxConcurrency
     *            The maximum multi-programming level (ignored if
     *            <i>predeclareLocks</i> is <code>true</code>).
     * @param maxLockTries
     *            The maximum #of times that a task whose lock requests would
     *            produce a deadlock will be retried. Deadlock is typically
     *            transient but the potential for deadlock can vary depending on
     *            the application. Note that deadlock CAN NOT arise if you are
     *            predeclaring and sorting the lock requests.
     * @param predeclareLocks
     *            When <code>true</code>, operations MUST declare all locks
     *            before they begin to execute. This makes possible several
     *            efficiencies and by sorting the resources in each lock request
     *            into a common order we are able to avoid deadlocks entirely.
     */
    public NonBlockingLockManagerWithNewDesign(final int maxConcurrency,
            final int maxLockTries, final boolean predeclareLocks) {

        this(maxConcurrency, maxLockTries, predeclareLocks,
                true/* sortLockRequests */);

    }

    /**
     * Create a lock manager. No concurrency limit imposed when
     * <i>predeclareLocks</i> is <code>true</code> as deadlocks are
     * impossible and we do not maintain a WAITS_FOR graph.
     * 
     * @param maxConcurrency
     *            The maximum multi-programming level (ignored if
     *            <i>predeclareLocks</i> is <code>true</code>).
     * @param maxLockTries
     *            The maximum #of times that a task whose lock requests would
     *            produce a deadlock will be retried. Deadlock is typically
     *            transient but the potential for deadlock can vary depending on
     *            the application. Note that deadlock CAN NOT arise if you are
     *            predeclaring and sorting the lock requests.
     * @param predeclareLocks
     *            When <code>true</code>, operations MUST declare all locks
     *            before they begin to execute. This makes possible several
     *            efficiencies and by sorting the resources in each lock request
     *            into a common order we are able to avoid deadlocks entirely.
     * @param sortLockRequests
     *            This option indicates whether or not the resources in a lock
     *            request will be sorted before attempting to acquire the locks
     *            for those resources. Normally <code>true</code> this option
     *            MAY be disabled for testing purposes. It is an error to
     *            disable this option if <i>predeclareLocks</i> is
     *            <code>false</code>.
     */
    NonBlockingLockManagerWithNewDesign(final int maxConcurrency,
            final int maxLockTries, final boolean predeclareLocks,
            final boolean sortLockRequests) {

        if (maxConcurrency < 2 && !predeclareLocks) {

            throw new IllegalArgumentException(
                    "maxConcurrency: must be 2+ unless you are predeclaring locks, not "
                            + maxConcurrency);

        }

        if (maxLockTries < 1)
            throw new IllegalArgumentException("maxTries: must be GTE 1, not "
                    + maxLockTries);
        
        if (predeclareLocks && !sortLockRequests) {

            /*
             * This is required since we do not maintain TxDag when locks are
             * predeclare and therefore can not detect deadlocks. Sorting with
             * predeclared locks avoids the possibility of deadlocks so we do
             * not need the TxDag (effectively, it means that all locks that can
             * be requested by an operation are sorted since they are
             * predeclared and acquired in one go).
             */

            throw new IllegalArgumentException(
                    "Sorting of lock requests MUST be enabled when locks are being predeclared.");

        }

        this.maxLockTries = maxLockTries;
        
        this.predeclareLocks = predeclareLocks;

        this.sortLockRequests = sortLockRequests;

        if (predeclareLocks) {

            /*
             * Note: waitsFor is NOT required if we will acquire all locks at
             * once for a given operation since we can simply sort the lock
             * requests for each operation into a common order, thereby making
             * deadlock impossible!
             * 
             * Note: waitsFor is also NOT required if we are using only a single
             * threaded system.
             * 
             * Note: if you allocate waitsFor here anyway then you can measure
             * the cost of deadlock detection. As far as I can tell it is
             * essentially zero when locks are predeclared.
             */

            waitsFor = null;

            // waitsFor = new TxDag(maxConcurrency);

        } else {

            /*
             * Construct the directed graph used to detect deadlock cycles.
             */

            waitsFor = new TxDag(maxConcurrency);

        }
        
        // start service.
        service.submit(new AcceptTask());

        // change the run state.
        lock.lock();
        try {

            setServiceRunState(ServiceRunState.Running);

        } finally {

            lock.unlock();

        }

    }

    /**
     * Method invoked when a task is ready to execute holding any locks which it
     * declared to {@link #submit(Comparable[], Callable)} or
     * {@link #submit(Comparable[], Runnable, Object)}. The implementation will
     * normally submit the {@link Runnable} to an {@link Executor}. The
     * {@link Runnable} wraps the original task and the task will automatically
     * release its locks when it is done executing.
     * <p>
     * Note: Implementations SHOULD NOT cause the {@link Runnable} to execute in
     * the caller's thread. That will cause this service to block while the task
     * is executing. The implementation can safely submit the task to a
     * {@link ThreadPoolExecutor} whose work queue is a {@link SynchronousQueue}
     * as long as the the {@link ThreadPoolExecutor} has an unbounded pool size.
     * Another option is to submit the task to a {@link ThreadPoolExecutor}
     * whose work queue is unbounded queue, such as {@link LinkedBlockingQueue}
     * when no queue capacity was specified. The {@link SynchronousQueue} may be
     * the better choice since the {@link ResourceQueue}s already provide an
     * unbounded queue and the actual concurrency of the delegate will be
     * bounded by the #of distinct resources for which tasks are actively
     * contending for locks. See the discussion on queues at
     * {@link ThreadPoolExecutor}.
     * 
     * @param task
     *            The {@link Callable} or {@link Runnable} wrapped up as a
     *            {@link LockFutureTask}.
     */
    abstract protected void ready(Runnable task);
    
    public boolean isOpen() {

        return serviceRunState == ServiceRunState.Running;

    }

    public boolean isShutdown() {

        switch (serviceRunState) {
        case Shutdown:
        case ShutdownNow:
        case Halted:
            return true;
        }

        return false;

    }

    public boolean isTerminated() {

        return serviceRunState == ServiceRunState.Halted;

    }

    public void shutdown() {

        lock.lock();

        try {

            if (serviceRunState.val < ServiceRunState.Shutdown.val) {

                setServiceRunState(ServiceRunState.Shutdown);

            }

        } finally {

            lock.unlock();

        }

    }

    public void shutdownNow() {

        lock.lock();

        try {

            if (serviceRunState.val < ServiceRunState.ShutdownNow.val) {

                setServiceRunState(ServiceRunState.ShutdownNow);

            }

        } finally {

            lock.unlock();

        }

    }

    /**
     * Change the {@link #serviceRunState}.
     * 
     * @param newval
     *            The new value.
     * 
     * @throws IllegalStateException
     *             if the state transition is illegal.
     * @throws IllegalMonitorStateException
     *             if the current thread does not hold the {@link #lock}.
     */
    private final void setServiceRunState(final ServiceRunState newval) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (!serviceRunState.isTransitionLegal(newval)) {

            throw new IllegalStateException("runState=" + serviceRunState
                    + ", but newValue=" + newval);

        }

        if (serviceRunState != newval) {

            if (INFO)
                log.info("Set runState=" + newval);

            serviceRunState = newval;

            stateChanged.signal();

        }

    }

    /**
     * Submit a task for execution. The task will wait until it holds the
     * declared locks. It will then execute. This method is non-blocking. The
     * caller must use {@link FutureTask#get()} to await the outcome.
     * 
     * @param resource
     *            An array of resources whose locks are required to execute the
     *            <i>task</i>.
     * @param task
     *            The task to be executed.
     * 
     * @throws IllegalArgumentException
     *             if <i>resource</i> is <code>null</code> or if any element
     *             of that array is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>task</i> is <code>null</code>.
     * 
     * @throws RejectedExecutionException
     *             if the task can not be queued for execution (including if the
     *             service is not running or if a blocking queue was used and
     *             the queue is at capacity).
     */
    public <T> Future<T> submit(final R[] resource, final Callable<T> task) {

        if (serviceRunState != ServiceRunState.Running)
            throw new RejectedExecutionException();
        
        if (resource == null)
            throw new IllegalArgumentException();

        for (R r : resource) {

            if (r == null)
                throw new IllegalArgumentException();

        }

        if (task == null)
            throw new IllegalArgumentException();

        if (maxLockTries <= 0)
            throw new IllegalArgumentException();

        /*
         * Note: We clone the resources to avoid side-effects on the caller if
         * the resources are sorted (below) and also to prevent the caller from
         * changing the declared locks after they submit the task.
         */
        final R[] a = resource.clone();

        if (sortLockRequests) {

            /*
             * Sort the resources in the lock request.
             * 
             * Note: Sorting the resources reduces the chance of a deadlock and
             * excludes it entirely when predeclaration of locks is also used.
             * 
             * Note: This will throw an exception if the "resource" does not
             * implement Comparable.
             */

            Arrays.sort(a);

        }

        lock.lock();
        try {

            if (!serviceRunState.tasksAccepted()) {

                counters.nrejected++;

                throw new RejectedExecutionException();

            }

            return new LockFutureTask<T>(a, task).acceptTask();

        } finally {

            lock.unlock();

        }
        
    }

    /**
     * Variant for a {@link Runnable} target.
     * 
     * @param resource
     *            The declared locks.
     * @param task
     *            The {@link Runnable} target.
     * @param val
     *            The value to be returned by the {@link Future}.
     * 
     * @return The {@link Future} for that task.
     * 
     * @param <T>
     *            The generic type of the value which will be returned by the
     *            {@link Future}.
     * 
     * @throws IllegalArgumentException
     *             if <i>resource</i> is <code>null</code> or if any element
     *             of that array is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>task</i> is <code>null</code>.
     * 
     * @throws RejectedExecutionException
     *             if the task can not be queued for execution (including if the
     *             service is not running or if a blocking queue was used and
     *             the queue is at capacity).
     */
    public <T> Future<T> submit(final R[] resource, final Runnable task,
            final T val) {

        if (serviceRunState != ServiceRunState.Running)
            throw new RejectedExecutionException();
        
        if (resource == null)
            throw new IllegalArgumentException();

        for (R r : resource) {

            if (r == null)
                throw new IllegalArgumentException();

        }

        if (task == null)
            throw new IllegalArgumentException();

        if (maxLockTries <= 0)
            throw new IllegalArgumentException();

        /*
         * Note: We clone the resources to avoid side-effects on the caller if
         * the resources are sorted (below) and also to prevent the caller from
         * changing the declared locks after they submit the task.
         */
        final R[] a = resource.clone();

        if (sortLockRequests) {

            /*
             * Sort the resources in the lock request.
             * 
             * Note: Sorting the resources reduces the chance of a deadlock and
             * excludes it entirely when predeclaration of locks is also used.
             * 
             * Note: This will throw an exception if the "resource" does not
             * implement Comparable.
             */

            Arrays.sort(a);

        }

        lock.lock();
        try {

            if (!serviceRunState.tasksAccepted()) {

                counters.nrejected++;

                throw new RejectedExecutionException();

            }

            return new LockFutureTask<T>(a, task, val).acceptTask();

        } finally {

            lock.unlock();

        }

    }

    /**
     * If there is a task holding ALL of the specified locks then its locks are
     * released. This is intended to support workflows in which the task can
     * release its locks before {@link Runnable#run()} is finished.
     * 
     * @param resource[]
     *            The declared locks for the task.
     * 
     * @throws IllegalStateException
     *             if there is no task which holds all the declared locks.
     */
    public final void releaseLocksForTask(final R[] resource) {

        if (resource == null)
            throw new IllegalArgumentException();
        
        if(resource.length == 0) {
            
            // No declared locks.
            return;
            
        }
        
        lock.lock();
        try {
            
            LockFutureTask<? extends Object> task = null;

            for (R r : resource) {

                final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = resourceQueues
                        .get(r);

                if (task == null) {

                    /*
                     * find the task by checking the resource queue for any of
                     * its declared locks.
                     */
                    task = resourceQueue.queue.peek();

                    if (task == null)
                        throw new IllegalStateException(
                                "Task does not hold declared lock: " + r);

                } else {

                    /*
                     * verify that the task holds the rest of its declared
                     * locks.
                     */
                    if (task != resourceQueue.queue.peek()) {

                        throw new IllegalStateException(
                                "Task does not hold declared lock: " + r);

                    }
                    
                }

            }

            if(task == null) {
                
                throw new AssertionError();
                
            }

            if (task.taskRunState != TaskRunState.RunningWithLocks) {

                throw new IllegalStateException("taskRunState="
                        + task.taskRunState);

            }
            
            /*
             * At this point we have verified that there is a task which holds
             * all of the locks specified by the caller. We now change its run
             * state so that it will release its locks and free other tasks to
             * run.
             */

            // update the run state.
            task.setTaskRunState(TaskRunState.RunningReleasedLocks);

        } finally {
            
            lock.unlock();
            
        }
        
    }
    
    /**
     * Typesafe enum for the run state of a {@link LockFutureTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static enum TaskRunState {
        /**
         * A newly created {@link LockFutureTask}.
         */
        New(0, false/* lockRequestsPosted */, false/* running */),
        /**
         * Task has been accepted but has not yet successfully issued its lock
         * requests. {@link LockFutureTask#ntries} gives the #of times the locks
         * have been requested. If the lock requests can not be issued after
         * {@link LockFutureTask#maxLockTries} then the task will be cancelled.
         */
        Accepted(0, false/* lockRequestsPosted */, false/* running */),
        /**
         * Task has issued its lock requests and is awaiting its locks.
         */
        LocksRequested(1, true/* lockRequestsPosted */, false/* running */),
        /**
         * Task has issued its lock requests and now holds all locks but is not
         * yet running.
         */
        LocksReady(1, true/* lockRequestsPosted */, false/* running */),
        /**
         * The task is running with its locks.
         */
        RunningWithLocks(4, true/* lockRequestsPosted */, true/* running */),
        /**
         * The task is running but has released its locks.
         */
        RunningReleasedLocks(5, false/* lockRequestsPosted */, true/* running */),
        /**
         * The task is halted (absorbing state). A task in this run state: (1)
         * IS NOT holding any locks; (2) DOES NOT have any lock requests posted
         * in the {@link ResourceQueue}s; and (3) DOES NOT have any edges or
         * vertices in the {@link TxDag}.
         * <p>
         * A transition to this state is triggered by
         * {@link Future#cancel(boolean)}, by
         * {@link LockFutureTask#setException(Throwable)}, or when
         * {@link LockTaskFuture#run()} completes.
         */
        Halted(9, false/* lockRequestsPosted */, false/* running */);

        /**
         * Return <code>true</code> iff the state transition is legal.
         */
        public boolean isTransitionLegal(final TaskRunState newval) {

            if (this == New) {

                // transition when the lock requests would deadlock.
                if (newval == Accepted)
                    return true;

                // allows task to directly request locks.
                if (newval == LocksRequested)
                    return true;

                // allows task to directly obtain locks.
                if (newval == LocksReady)
                    return true;

                if (newval == Halted)
                    return true;

            } else if (this == Accepted) {

                if (newval == LocksRequested)
                    return true;

                if (newval == LocksReady)
                    return true;

                if (newval == Halted)
                    return true;

            } else if (this == LocksRequested) {

                if (newval == LocksReady)
                    return true;

                if (newval == Halted)
                    return true;

            } else if (this == LocksReady) {

                if (newval == RunningWithLocks)
                    return true;

                if (newval == Halted)
                    return true;

            } else if (this == RunningWithLocks) {

                if (newval == RunningReleasedLocks)
                    return true;

                if (newval == Halted)
                    return true;

            } else if (this == RunningReleasedLocks) {

                if (newval == Halted)
                    return true;

            } else {

                throw new AssertionError("Unknown runState=" + this);
                
            }

            // anything not permitted is forbidden :-)
            return false;

        }

        private final int val;
        private final boolean lockRequestPosted;
        private final boolean running;
        
        /**
         * 
         * @param val
         *            The integer value corresponding to the run state.
         * @param lockRequestsPosted
         *            <code>true</code> iff a task has posted lock requests in
         *            each {@link ResourceQueue} corresponding to a lock
         *            declared by that task.
         * @param running
         *            <code>true</code> iff a task is currently executing.
         */
        private TaskRunState(final int val, final boolean lockRequestsPosted,
                final boolean running) {
            
            this.val = val;
            
            this.lockRequestPosted = lockRequestsPosted;
            
            this.running = running;
            
        }
        
        /**
         * Return the integer value corresponding to this run state.
         */
        public int get() {

            return val;
            
        }

        /**
         * Return <code>true</code> iff a task has posted lock requests in
         * each {@link ResourceQueue} corresponding to a lock declared by that
         * task.
         */
        public boolean isLockRequestsPosted() {
            
            return lockRequestPosted;
            
        }

        /**
         * Return <code>true</code> iff a task is currently executing.
         */
        public boolean isRunning() {
            
            return running;
            
        }

    }
    
    /**
     * {@link FutureTask} which executes once it holds its locks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @param <T>
     *            The generic type of the outcome for the {@link Future}.
     */
    protected class LockFutureTask<T> extends FutureTask<T> {

        private final R[] resource;

        /**
         * Incremented each time a deadlock is detected. We will not retry if
         * {@link #maxLockTries} is exceeded.
         */
        private int ntries = 0;
        
        /**
         * The timestamp in nanoseconds when this task was accepted for eventual
         * execution. This is used to determine the {@link #lockWaitingTime}.
         */
        final private long acceptTime;

        /**
         * The set of {@link ResourceQueue}s for which this task owns a lock
         * (is a member of the granted group) (NOT THREAD SAFE).
         * <p>
         * Note: This collection is required in order for the
         * {@link ResourceQueue}s for which the task has asserted a lock
         * request to remain strongly reachable. Without such hard references
         * the {@link ResourceQueue}s would be asynchronously cleared from the
         * {@link NonBlockingLockManagerWithNewDesign#resourceQueues} collection by the
         * garbage collector.
         */
        private final LinkedHashSet<ResourceQueue<LockFutureTask<? extends Object>>> lockedResources = new LinkedHashSet<ResourceQueue<LockFutureTask<? extends Object>>>();

        /**
         * Either a {@link Callable} or a {@link Runnable}.
         */
        private final Object callersTask;

        /**
         * The run state for the task.
         */
        private volatile TaskRunState taskRunState = TaskRunState.New;

        /**
         * The run state for the task.
         */
        final TaskRunState getTaskRunState() {
            
            return taskRunState;
            
        }
    
        /**
         * Change the run state for the task.
         * 
         * @param newval
         *            The new run state.
         *            
         * @throws IllegalStateException
         *             if the state transition is not legal.
         */
        private void setTaskRunState(final TaskRunState newval) {

            if(!lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();

            if (newval == null)
                throw new IllegalArgumentException();
            
            if (!taskRunState.isTransitionLegal(newval)) {
                log.error("Illegal state change: current=" + taskRunState
                        + ", newval=" + newval);
                throw new IllegalStateException("current=" + taskRunState
                        + ", newval=" + newval);
            }

            try {

                final TaskRunState oldval = this.taskRunState;

                if (oldval.isLockRequestsPosted()
                        && !newval.isLockRequestsPosted()) {

                    /*
                     * The task has posted lock requests but the new run state
                     * does not have posted lock requests so we clear the posted
                     * lock requests now.
                     * 
                     * This covers the following state transitions:
                     * 
                     * LocksRequested -> Halted
                     * 
                     * LocksReady -> Halted
                     * 
                     * RunningWithLocks -> (RunningWithoutLocks | Halted)
                     */

                    final boolean waiting = !oldval.isRunning();

                    releaseLocksForTask(this, waiting);

                }

            } finally {

                this.taskRunState = newval;

                /*
                 * @todo There may not be any reason do to this here. The
                 * AcceptTask only needs to handle the acceptQueue and notice
                 * when the runState of the service changes. Since each task
                 * which releases its locks is itself responsible for seeing if
                 * another task is ready to run, the acceptTask does not need to
                 * be woken up for that purpose any more.
                 */
                
                stateChanged.signal();

            }
            
        }

        /**
         * The resource(s) that are pre-declared by the task. {@link #call()}
         * will ensure that the task as a lock on these resources before it
         * invokes {@link #run()} to execution the task.
         */
        public R[] getResource() {

            return resource;

        }

        /**
         * The elapsed nanoseconds the task waited to acquire its locks.
         */
        public long getLockLatency() {

            return lockWaitingTime;

        }
        private long lockWaitingTime;

        public String toString() {

            return super.toString() + //
                    "{resources=" + Arrays.toString(resource) + //
                    ", runState=" + taskRunState + //
                    ", done=" + isDone() + //
                    ", cancelled=" + isCancelled() + //
                    ", ntries=" + ntries + "}";

        }

        public LockFutureTask(final R[] resource, final Callable<T> task) {

            super(task);

            this.resource = resource;

            this.callersTask = task;

            this.acceptTime = System.nanoTime();

        }

        public LockFutureTask(final R[] resources, final Runnable task,
                final T val) {

            super(task, val);

            this.resource = resources;
            
            this.callersTask = task;
        
            this.acceptTime = System.nanoTime();

        }
        
        /**
         * Accept the task for eventual execution.
         */
        private LockFutureTask<T> acceptTask() {

            if (!lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();

            switch (serviceRunState) {
            case Shutdown:
                /*
                 * Any task that has been accepted will be eventually run.
                 */
                // fall through.
            case Running:
                /*
                 * Note: it is assumed that this code block will succeed. This
                 * is not an outrageous assumption.
                 * 
                 * Note: Since this thread holds the [lock] the acceptedTasks
                 * queue MUST NOT block (it would deadlock the service).
                 * 
                 * As an optimization, we immediately request the locks. If they
                 * can be granted then the new run state was already set for the
                 * task. Otherwise we verify that the task was not cancelled and
                 * leave the task in the accept queue.
                 */
                if (requestLocks()) {
                    /*
                     * The task is either waiting on its locks or holding its
                     * locks.
                     * 
                     * It should not be possible for it to have been cancelled
                     * asynchronous since the caller does not have the Future
                     * yet and we are still holding the [lock].
                     * 
                     * If there is an internal error, the task may have had its
                     * exception set.
                     * 
                     * Regardless, we are done with it and do not need to place
                     * it onto the accepted queue.
                     */
                    return this;
                }
                /*
                 * Put the task onto the accepted queue.
                 * 
                 * Note: this case should only arise if there is a deadlock
                 * since the lock requests had to be backed out.
                 */
                setTaskRunState(TaskRunState.Accepted);
                acceptedTasks.add(this); // Note: MUST NOT block!
                counters.naccepted++;
                return this;
            default:
                    throw new IllegalStateException(serviceRunState.toString());
            }
        }

        /**
         * Request any locks declared by the task. If the locks are available
         * then submit the task for execution immediately.
         * 
         * @return <code>true</code> if the task should be removed from
         *         {@link NonBlockingLockManagerWithNewDesign#acceptedTasks} by
         *         the caller.
         */
        private boolean requestLocks() {

            if (!lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();
            
            if (isCancelled()) {

                // already cancelled, e.g., by the caller.
                return true;

            }

            if (serviceRunState.tasksCancelled()) {

                // tasks are cancelled in this run state.

                cancel(false/* taskIsNotRunning */);

                return true;

            }

            int nvertices = -1;
            try {

                // #of vertices before we request the locks.
                if (waitsFor != null)
                    nvertices = waitsFor.size();
                
                if (waitsFor != null && waitsFor.isFull()) {
                
                    /*
                     * Note: When TxDag is used we MUST NOT add the lock
                     * requests if it would exceed the configured
                     * multi-programming capacity (an exception would be
                     * thrown by TxDag). Therefore we stop processing
                     * accepted tasks and will wait until a running task
                     * completes so we can start another one.
                     * 
                     * Note: when tasks != transactions we need to wait
                     * until a transaction completes. This could require
                     * multiple tasks to complete if they are tasks for the
                     * same transaction.
                     */
                    
                    if (INFO)
                        log.info("Maximum multi-programming capacity.");
                    
                    return false;
                
                }
                
                /*
                 * Modify the state of the ResourceQueue(s) and the optional
                 * TxDag to reflect the lock requests. The method will
                 * release the lock requests if a deadlock would arise.
                 * 
                 * Note: Can thrown DeadlockException.
                 */

                ntries++;
                
                if (waitsFor != null) {

                    /*
                     * Declare the vertex.
                     * 
                     * Note: The problem here is twofold.
                     * 
                     * On the one hand vertices are not being released
                     * automatically when there are no more edges for a
                     * transaction. Even with removeEdges(tx,waiting) the
                     * vertex for the _other_ transaction is not being
                     * removed automatically.
                     * 
                     * On the other hand, one vertex can be added per lock
                     * declared by the transaction and one more vertex for
                     * the transaction itself.
                     * 
                     * The way to solve this is to declare the vertex for
                     * the transaction before we request the lock
                     * _regardless_ of whether an edge needs to be added and
                     * to remove it when the lock is released (which we
                     * already do). That way adding an edge will never cause
                     * a new vertex to be defined automagically.
                     */
                    waitsFor.lookup(this, true/* insert */);
                    
                }

                /*
                 * Post the lock requests.
                 * 
                 * Note: This method has no side-effects if it fails.
                 */
                if(postLockRequests(this)) {
                    // run now.
                    if (INFO)
                        log.info("Task is ready to run: " + this);
                    setTaskRunState(TaskRunState.LocksReady);
                    counters.nready++;
                    ready(this);
                } else {
                    // must wait for locks.
                    setTaskRunState(TaskRunState.LocksRequested);
                    counters.nwaiting++;
                }
                
                // either way, the task was handled.
                return true;

            } catch (Throwable t2) {

                if ((t2 instanceof DeadlockException)) {

                    /*
                     * Note: DeadlockException is the ONLY expected exception
                     * when we issue the lock requests, and then IFF TxDag is in
                     * use. Anything else is an internal error.
                     */
                    
                    log.warn("Deadlock: " + this + ", task=" + this /* , ex */);

                    counters.ndeadlock++;

                    if ((ntries < maxLockTries)) {

                        log.warn("Will retry task: " + this);

                        // leave on queue to permit retry.
                        return false;

                    }
                    
                } else {
                    
                    log.error("Internal error: " + this, t2);
                    
                }

                // set exception on the task (clears locks)
                setException(t2);

                if (waitsFor != null) {
                    /*
                     * Paranoia check to make sure that we did not leave
                     * anything in the WAITS_FOR graph.
                     */
                    final int nafter = waitsFor.size();
                    if (nvertices != nafter) {
                        throw new AssertionError("#vertices: before="
                                + nvertices + ", after=" + nafter);
                    }
                }
                
                // caller should remove from the queue.
                return true;
                               
            }

        }

        /**
         * Note: We do not need to remove the task from the readyQueue when it
         * an exception is set on it. When the task is pulled from the queue by
         * an executor service, the task will be marked as cancelled and its
         * run() method will be a NOP.
         * 
         * @todo verify with unit test.
         */
        @Override
        protected void setException(final Throwable t) {
            lock.lock();
            try {
                super.setException(t);
                if (taskRunState != TaskRunState.Halted) {
                    if (DEBUG)
                        log.debug("Exception: " + this + ", cause=" + t, t);
                    counters.nerror++;
                    setTaskRunState(TaskRunState.Halted);
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Note: We do not need to remove the task from the readyQueue when it
         * is cancelled. When the task is pulled from the queue by an executor
         * service, the task will be marked as cancelled and its run() method
         * will be a NOP.
         */
        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            lock.lock();
            try {
                final boolean ret = super.cancel(mayInterruptIfRunning);
                if (taskRunState != TaskRunState.Halted) {
                    if (DEBUG)
                        log.debug("Cancelled: " + this);
                    counters.ncancel++;
                    setTaskRunState(TaskRunState.Halted);
                }
                return ret;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void run() {

            /*
             * Increment by the amount of time that the task was waiting to
             * acquire its lock(s).
             * 
             * Note: This is being measured from the time when the task was
             * accepted by submit() on the outer class and counts all time until
             * the task begins to execute with its locks held.
             */
            if (callersTask instanceof AbstractTask) {
                
                final long lockWaitingTime = System.nanoTime() - acceptTime;

                ((AbstractTask) callersTask).getTaskCounters().lockWaitingTime
                        .addAndGet(lockWaitingTime);

            }

            lock.lock();
            try {
                if (taskRunState == TaskRunState.LocksReady) {
                    /*
                     * Note: run() can be invoked on tasks which have been
                     * cancelled so we don't want to do this for those tasks.
                     */
                    setTaskRunState(TaskRunState.RunningWithLocks);
                    counters.nready--;
                    counters.nrunning++;
                    if (counters.nrunning > counters.maxRunning) {
                        counters.maxRunning = counters.nrunning;
                    }
                }
            } finally {
                lock.unlock();
            }

            if (DEBUG)
                log.debug("Running: " + this);
            
            try {
                super.run();
            } finally {
                /*
                 * Note: FutureTask asynchronously reports the result back to
                 * get() when super.run() completes.
                 * 
                 * @todo I have tried overriding both set(T) and done(), but
                 * neither appears to be invoked synchronously on this class
                 * during super.run(). Therefore the locks and the run state
                 * MIGHT NOT have been updated before the Future#get() returns
                 * to the caller!
                 */
                lock.lock();
                try {
                    if (taskRunState.isRunning()) {
                        if (DEBUG)
                            log.debug("Did run: " + this);
                        counters.nrunning--;
                        setTaskRunState(TaskRunState.Halted);
                    }
                } finally {
                    lock.unlock();
                }
            }

        }

//        /**
//         * Releases the locks and changes to {@link TaskRunState#Halted}.
//         */
//        @Override
//        protected void done() {
//            super.done();
//        }
        
    }

    /**
     * {@link Runnable} drains the {@link #acceptedTasks} queue and manages
     * state changes in the {@link ResourceQueue}s. Once a task is holding all
     * necessary locks, the task is submitted to the delegate {@link Executor}
     * for execution. This thread is also responsible for monitoring the
     * {@link Future}s and releasing locks for a {@link Future} it is complete.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class AcceptTask implements Runnable {

        public void run() {
            while (true) {
                switch (serviceRunState) {
                case Starting: {
                    awaitStateChange(Starting);
                    continue;
                }
                case Running: {
                    lock.lock();
                    try {
                        while (processAcceptedTasks()) {
                            // do work
                        }
                        awaitStateChange(Running);
                    } finally {
                        lock.unlock();
                    }
                    continue;
                }
                case Shutdown: {
                    lock.lock();
                    try {
                        while (processAcceptedTasks()) {
                            /*
                             * Do work.
                             * 
                             * Note: will run anything already accepted. That is
                             * intentional. Once the lock manager is shutdown it will no
                             * longer accept tasks, but it will process those tasks
                             * which it has already accepted.
                             */
                        }
                        if (acceptedTasks.isEmpty()) {
                            /*
                             * There is no more work to be performed so we can
                             * change the runState.
                             */
                            if(INFO)
                                log.info("No more work.");
                            if (serviceRunState.val < ServiceRunState.ShutdownNow.val) {
                                setServiceRunState(ServiceRunState.ShutdownNow);
                                break;
                            }
                        }
                        awaitStateChange(Shutdown);
                    } finally {
                        lock.unlock();
                    }
                    continue;
                }
                case ShutdownNow: {
                    /*
                     * Cancel all tasks, clearing all queues. Note that only
                     * tasks which are on [runningTasks] need to be interrupted
                     * as tasks on the other queues are NOT running.
                     */
                    lock.lock();
                    try {
                        if (INFO)
                            log.info(serviceRunState);
                        cancelTasks(acceptedTasks.iterator(), false/* mayInterruptIfRunning */);
                        if (serviceRunState.val < ServiceRunState.Halted.val) {
                            setServiceRunState(ServiceRunState.Halted);
                            if (INFO)
                                log.info(serviceRunState);
                        }
                        // Done.
                        return;
                    } finally {
                        lock.unlock();
                    }
                }
                case Halted: {
                    if (INFO)
                        log.info(serviceRunState);
                    // Done.
                    return;
                }
                default:
                    throw new AssertionError();
                } // switch(runState)
            } // while(true)
        } // run

        /**
         * IFF there is no work that could be performed and we are in the
         * expected {@link ServiceRunState} then this blocks until someone
         * signals {@link NonBlockingLockManagerWithNewDesign#stateChanged}.
         * 
         * @see LockFutureTask
         * @see NonBlockingLockManagerWithNewDesign#submit(Comparable[],
         *      Callable)
         */
        private void awaitStateChange(final ServiceRunState expected) {
            lock.lock();
            try {
                /*
                 * While we hold the lock we verify that there really is no work
                 * to be done and that we are in the expected run state. Then
                 * and only then do we wait on [stateChanged].
                 */
                if (serviceRunState != expected) {
                    // In a different run state.
                    return;
                }
                if (!acceptedTasks.isEmpty()) {
                    // Some work can be done.
                    return;
                }
                if (INFO)
                    log.info("Waiting...");
                stateChanged.await();
                if (INFO)
                    log.info("Woke up...");
            } catch (InterruptedException ex) {
                // someone woke us up.
            } finally {
                lock.unlock();
            }
        }
        
        /**
         * Cancel all tasks and remove them from the queue.
         * 
         * @param tasks
         *            The tasks.
         */
        private void cancelTasks(
                final Iterator<LockFutureTask<? extends Object>> itr,
                final boolean mayInterruptIfRunning) {

            while (itr.hasNext()) {

                final LockFutureTask<? extends Object> t = itr.next();

                t.cancel(mayInterruptIfRunning);

                itr.remove();

            }

        }

        /**
         * Processes accepted tasks.
         * 
         * @return <code>true</code> iff any tasks were removed from this
         *         queue.
         */
        private boolean processAcceptedTasks() {

            int nchanged = 0;

            final Iterator<LockFutureTask<? extends Object>> itr = acceptedTasks
                    .iterator();

            while (itr.hasNext()) {

                final LockFutureTask<? extends Object> t = itr.next();

                if (t.requestLocks()) {

                    /*
                     * Note: a [true] return means that we will remove the task
                     * from the [acceptedTasks] queue. It does NOT mean that the
                     * task was granted its locks.
                     */
                    
                    itr.remove();

                    nchanged++;
                    
                }

            }

            if (INFO && nchanged > 0)
                log.info("#nchanged=" + nchanged);
            
            return nchanged > 0;
            
        }

//        /**
//         * For each task waiting to run:
//         * <ul>
//         * <li>if the task has been cancelled, then remove it from the waiting
//         * tasks queue</li>
//         * <li>if the lock timeout has expired, then set an exception on the
//         * task and remove it from the waiting task queue and remove its lock
//         * requests from the various {@link ResourceQueue}s</li>
//         * <li>if the lock requests for that task have been granted then submit
//         * the task to execute on the delegate and move it to the running tasks
//         * queue</li>
//         * <li>if the delegate rejects the task, then it is NOT removed from
//         * the waiting tasks queue and this method returns immediately</li>
//         * </ul>
//         * 
//         * @return <code>true</code> if any tasks were moved to the running
//         *         tasks list.
//         */
//        private boolean processWaitingTasks() {
//
//            final Iterator<LockFutureTask<? extends Object>> itr = waitingTasks
//                    .iterator();
//
//            int nstarted = 0;
//            
//            while (itr.hasNext()) {
//
//                final LockFutureTask<? extends Object> t = itr.next();
//
//                if (t.isCancelled()) {
//
//                    // cancelled while awaiting locks.
//                    itr.remove();
//
//                    continue;
//
//                }
//                
//                final boolean holdsLocks;
//                lock.lock();
//                try {
//                    holdsLocks = holdsAllLocks(t);
//                } finally {
//                    lock.unlock();
//                }
//                
//                if (holdsLocks) {
//
//                    // holding locks, so execute the task.
//
//                    if (INFO)
//                        log.info("Executing: " + t);
//
//                    try {
//
//                        /*
//                         * Note: FutureTask will take can of updating its state
//                         * before/after the runnable target.
//                         * 
//                         * Note: If delegate.execute() blocks then the acceptor
//                         * thread will also block and this class will be
//                         * non-responsive until the delegate had accepted each
//                         * [waitingTask] for execution.  Some executors can cause
//                         * the task to be run in the caller's thread, which would
//                         * be the AcceptTask itself.
//                         */
//
//                        assert !lock.isHeldByCurrentThread();
//                        
//                        delegate.execute(t);
//
//                    } catch (RejectedExecutionException t2) {
//
//                        /*
//                         * We can't queue this task now so we stop processing
//                         * the waiting tasks. We will pick up on those tasks
//                         * again the next time this method is invoked.
//                         */
//                        if(INFO)
//                            log.info("Delegate is busy.");
//
//                        return nstarted > 0;
//                        
//                    }
//
//                    itr.remove();
//
//                    nstarted++;
//                    
//                    continue;
//
//                }
//
//            }
//
//            if (INFO && nstarted > 0)
//                log.info("#started=" + nstarted);
//
//            return nstarted > 0;
//
//        }

    } // AcceptTask

    /**
     * Add if absent and return a {@link ResourceQueue} for the named resource.
     * 
     * @param resource
     *            The resource.
     * 
     * @return The {@link ResourceQueue}.
     */
    private ResourceQueue<LockFutureTask<? extends Object>> declareResource(final R resource) {

        // test 1st to avoid creating a new ResourceQueue if it already exists.
        ResourceQueue<LockFutureTask<?extends Object>> resourceQueue = resourceQueues
                .get(resource);

        // not found, so create a new ResourceQueue for that resource.
        resourceQueue = new ResourceQueue<LockFutureTask<?extends Object>>(resource);

        // put if absent.
        final ResourceQueue<LockFutureTask<?extends Object>> oldval = resourceQueues
                .putIfAbsent(resource, resourceQueue);

        if (oldval != null) {

            // concurrent insert, so use the winner's resource queue.
            return oldval;

        }

        // we were the winner, so return the our new resource queue.
        return resourceQueue;

    }

    /**
     * Update the {@link ResourceQueue}s and the optional {@link TxDag} to
     * reflect the lock requests for the task. The operation is atomic and
     * succeeds iff the lock requests could be issued without deadlock. If an
     * error occurs then the lock requests will not have been registered on the
     * {@link TxDag}.
     * 
     * @param task
     *            The task.
     * 
     * @return <code>true</code> if the task is at the head of the queue for
     *         each lock declared for that task (e.g., if the task can execute
     *         immediately).
     * 
     * @throws DeadlockException
     *             If the lock request(s) would cause a deadlock.
     * @throws IllegalStateException
     *             If locks are being predeclared and there are already locks
     *             held by the operation.
     */
    private <T> boolean postLockRequests(final LockFutureTask<T> task)
            throws DeadlockException {

        if (task == null)
            throw new IllegalArgumentException();

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        switch(serviceRunState) {
        case ShutdownNow:
        case Halted:
            /*
             * No new lock requests are permitted once we reach ShutdownNow.
             */
            throw new IllegalStateException("runState=" + serviceRunState);
        }
        
        if (task.resource.length == 0) {
        
            // The task can execute immediately.
            return true;
            
        }
        
        if (predeclareLocks) {

            // verify that no locks are held for this operation.
            if (!task.lockedResources.isEmpty()) {

                /*
                 * The operation has already declared some locks. Since
                 * [predeclareLocks] is true it is not permitted to grow the set
                 * of declared locks, so we throw an exception.
                 */

                throw new IllegalStateException(
                        "Operation already has lock(s): " + task);

            }

        }

        if (DEBUG)
            log.debug("Acquiring lock(s): " + Arrays.toString(task.resource));

        if (waitsFor != null) {

            /*
             * Detect deadlocks using TxDag.
             * 
             * We take this in three stages.
             * 
             * 1. Collect the set of distinct tasks which are already in the
             * resource queues for the lock requests declared by this task.
             * 
             * 2. If that set is NOT empty, then add edges to TxDag for each
             * element of that set. If a DeadlockException is thrown then we can
             * not issue those lock requests at this time.
             * 
             * 3. Add the task to each of the resource queues.
             */
            
            /*
             * Collect the set of tasks on which this task must wait.
             */
            final LinkedHashSet<LockFutureTask<? extends Object>> predecessors = new LinkedHashSet<LockFutureTask<? extends Object>>();
            for (R r : (R[]) task.resource) {
            
                // make sure queue exists for this resource.
                final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = declareResource(r);

                if (!resourceQueue.queue.isEmpty()) {

                    predecessors.addAll(resourceQueue.queue);

                }
                
            }

            if(!predecessors.isEmpty()) {
                
                /*
                 * Add edges to the WAITS_FOR graph for each task on which this
                 * task must wait.
                 * 
                 * Note: throws DeadlockException if the lock requests would
                 * cause a deadlock.
                 * 
                 * Note: If an exception is thrown then the state of the TxDag
                 * is unchanged (the operation either succeeds or fails
                 * atomically).
                 * 
                 * FIXME In fact, predeclaring locks appears sufficient to avoid
                 * deadlocks as long as we make issue the lock requests
                 * atomically for each task. This means that TxDag would only be
                 * useful for 2PL, and this class (NonBlockingLockManager)
                 * currently does not support 2PL (because it couples the
                 * concepts of the task and the transaction together).
                 */
                
                waitsFor.addEdges(task, predecessors.toArray());
                
            }
            
            /*
             * Now that we have updated TxDag and know that the lock requests do
             * not cause a deadlock, we register those requests on the
             * ResourceQueues.
             */
            
            for (R r : (R[]) task.resource) {

                // make sure queue exists for this resource.
                final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = declareResource(r);

                /*
                 * Add a lock request for this resource.
                 */
                resourceQueue.queue.add(task);

                /*
                 * Add the resource queue to the set of queues whose locks are
                 * held by this task.
                 */
                task.lockedResources.add(resourceQueue);

            }

            /*
             * If there are no processessors then no WAITS_FOR edges were
             * asserted and this task can execute immediately.
             */
            return predecessors.isEmpty();
            
        } else {

            /*
             * When we are not using the WAITS_FOR graph all we have to do is
             * add the task to each of the resource queues. It will run once it
             * is at the head of each resource queue into which it is placed by
             * its lock requests.
             */

            // #of locks on which this task must wait.
            int waitingLockCount = 0;
            
            for (R r : (R[]) task.resource) {

                // make sure queue exists for this resource.
                final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = declareResource(r);
                
                /*
                 * Add a lock request for this resource.
                 */
                resourceQueue.queue.add(task);

                if(resourceQueue.queue.peek() != task) {
                    
                    waitingLockCount++;
                    
                }
                
                /*
                 * Add the resource queue to the set of queues whose locks are held
                 * by this task.
                 */
                task.lockedResources.add(resourceQueue);

            }
            
            /*
             * If the task is not waiting on any locks then it can run
             * immediately.
             */
            return waitingLockCount == 0;

        }
    
    }

    /**
     * Release all locks for the task.
     * 
     * @param task
     *            The task.
     * @param waiting
     *            When false, caller asserts that this transaction it is NOT
     *            waiting on any other transaction. This assertion is used to
     *            optimize the update of the path count matrix by simply
     *            removing the row and column associated with this transaction.
     *            When [waiting == true], a less efficient procedure is used to
     *            update the path count matrix.
     *            <p>
     *            Do NOT specify [waiting == false] unless you <em>know</em>
     *            that the transaction is NOT waiting. In general, this
     *            knowledge is available to the 2PL locking package.
     */
    private <T> void releaseLocksForTask(final LockFutureTask<T> t,
            final boolean waiting) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (DEBUG)
            log.debug("Releasing locks: " + t);

        /*
         * The set of resource queues for which this task was holding a lock (at
         * the head of the queue).
         */
        final List<ResourceQueue<LockFutureTask<? extends Object>>> resourceQueues = new LinkedList<ResourceQueue<LockFutureTask<? extends Object>>>();

        try {

            final Iterator<ResourceQueue<LockFutureTask<? extends Object>>> itr = t.lockedResources
                    .iterator();

            while (itr.hasNext()) {

                final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = itr
                        .next();

                if (resourceQueue.queue.peek() == t) {

                    // task was holding the lock in this queue.
                    resourceQueues.add(resourceQueue);

                }
                
                /*
                 * Remove lock request from resource queue
                 */
                if (!resourceQueue.queue.remove(t)) {

                    log.error("Lock request not found: resource="
                            + resourceQueue.getResource() + ", task=" + t);
                    
                }

                // remove lock from collection since no longer held by task.
                itr.remove();

            }

        } catch(Throwable t2) {
         
            /*
             * Log an internal error but do not throw the error back since we
             * need to clean up the WAITS_FOR graph and also start any tasks we
             * now hold all of their locks.
             */
            
            log.error(this, t2);

        }

        /*
         * At this point there are edges in the WAITS_FOR graph and the task
         * is no longer on any of the resource queues.
         */
        if (waitsFor != null) {

            synchronized (waitsFor) {

                try {

                    waitsFor.removeEdges(t, waiting);

                    /*
                     * Release the vertex (if any) in the WAITS_FOR graph.
                     * 
                     * Note: Since we always declare a vertex before we request
                     * the locks for a task this method SHOULD NOT return
                     * [false].
                     */

                    if (waitsFor.releaseVertex(t)) {

                        log.error("No vertex? " + t);

                    }

                } catch (Throwable t2) {

                    log.error(this, t2);

                }

            }

        }
        
        /*
         * Consider all the resource queues in which this task was holding
         * the lock.  If any task at the each of those queues now holds all
         * of its locks, then put that task on the [readyQueue].
         */
        {
         
            final Iterator<ResourceQueue<LockFutureTask<? extends Object>>> itr = resourceQueues
                    .iterator();

            while (itr.hasNext()) {

                final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = itr
                        .next();

                final LockFutureTask<? extends Object> task = resourceQueue.queue
                        .peek();

                if (task != null && holdsAllLocks(task)) {

                    if (INFO)
                        log.info("Task is ready to run: " + task);
                    
                    // add to queue of tasks ready to execute.
                    task.setTaskRunState(TaskRunState.LocksReady);
                    counters.nready++;
                    ready(task);

                }
                
            }
            
        }
        
    } 

    /**
     * Return <code>true</code> if the lock is held by the task at the moment
     * when it is inspected.
     * 
     * @param lock
     *            The lock.
     * @param task
     *            The task.
     * @return <code>true</code> if the lock was held by that task.
     */
    public boolean isLockHeldByTask(final R lock, final Runnable task) {

        final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = resourceQueues
                .get(lock);

        if (resourceQueues != null && resourceQueue.queue.peek() == task) {

            return true;

        }

        return false;

    }

    /**
     * Return <code>true</code> iff the task holds all of its declared locks.
     * 
     * @param task
     *            The task.
     * 
     * @return <code>true</code> iff it holds its locks.
     */
    private boolean holdsAllLocks(final LockFutureTask<? extends Object> task) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        for (R r : (R[]) task.resource) {

            final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = resourceQueues
                    .get(r);

            assert resourceQueue != null : "resource=" + r;

            if (!resourceQueue.isGranted(task)) {

                return false;

            }

        }

        return true;

    }

    public String toString() {

        // return getCounters().toString();

        return getClass().getName() + //
                "{ #rejected=" + counters.nrejected + //
                ", #accepted=" + counters.naccepted + //
                ", #cancel=" + counters.ncancel + //
                ", #error=" + counters.nerror + //
                ", #deadlock=" + counters.ndeadlock + //
                ", #waiting=" + counters.nwaiting+ //
                ", #ready=" + counters.nready + //
                ", #nrunning=" + counters.nrunning+ //
                ", #maxrunning=" + counters.maxRunning + //
                (waitsFor != null ? ", vertices=" + waitsFor.size() : "") + //
                "}";

    }

    /**
     * Unbounded queue of operations waiting to gain an exclusive lock on a
     * resource.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @param <T>
     *            The generic type of the elements in the queue.
     * 
     * @see LockManager
     * @see TxDag
     */
    protected class ResourceQueue<T extends LockFutureTask<? extends Object>> {

        /**
         * The resource whose access is controlled by this object.
         */
        final private R resource;

        /**
         * The queue of transactions seeking access to the {@link #resource}.
         * The object at the head of the queue is the transaction with the lock
         * on the resource.
         */
        final BlockingQueue<T/* tx */> queue = new LinkedBlockingQueue<T>(/* unbounded */);

        /**
         * The resource whose locks are administeded by this object.
         */
        public R getResource() {

            return resource;

        }

        /**
         * True iff there is a granted group.
         */
        public boolean isLocked() {

            return !queue.isEmpty();

        }

        /**
         * The #of pending requests for a lock on the resource.
         */
        public int getQueueSize() {

            return Math.max(0, queue.size() - 1);

        }

        /**
         * Return true if the transaction currently holds the lock.
         * 
         * @param tx
         *            The transaction.
         */
        public boolean isGranted(final T tx) {

            if (tx == null) {

                throw new IllegalArgumentException();

            }

            return queue.peek() == tx;

        }

        /**
         * Note: This uses {@link LinkedBlockingQueue#toString()} to serialize
         * the state of the resource queue so the result will be consistent per
         * the contract of that method and
         * {@link LinkedBlockingQueue#iterator()}.
         */
        public String toString() {

            return getClass().getSimpleName() + "{resource=" + resource
                    + ", queue=" + queue.toString() + "}";

        }

        /**
         * Create a queue of lock requests for a resource.
         * 
         * @param resource
         *            The resource.
         */
        public ResourceQueue(final R resource) {

            if (resource == null)
                throw new IllegalArgumentException();

            this.resource = resource;

        }

    } // ResourceQueue

}
