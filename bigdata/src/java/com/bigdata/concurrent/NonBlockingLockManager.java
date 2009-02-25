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

import static com.bigdata.concurrent.NonBlockingLockManager.RunState.Halted;
import static com.bigdata.concurrent.NonBlockingLockManager.RunState.Running;
import static com.bigdata.concurrent.NonBlockingLockManager.RunState.Shutdown;
import static com.bigdata.concurrent.NonBlockingLockManager.RunState.ShutdownNow;
import static com.bigdata.concurrent.NonBlockingLockManager.RunState.Starting;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * This class coordinates a schedule among concurrent operations requiring
 * exclusive access to shared resources. Whenever possible, the result is a
 * concurrent schedule - that is, operations having non-overlapping lock
 * requirements run concurrently while operations that have lock contentions are
 * queued behind operations that currently have locks on the relevant resources.
 * A {@link ResourceQueue} is created for each resource and used to block
 * operations that are awaiting a lock. When locks are not being pre-declared, a
 * {@link TxDag WAITS_FOR} graph is additionally used to detect deadlocks.
 * <p>
 * This implementation uses a single {@link AcceptTask} thread to accept tasks,
 * update the requests in the {@link ResourceQueue}s, and in general perform
 * housekeeping for the internal state. Tasks submitted to this class ARE NOT
 * bound to a worker thread until they are executed by the delegate
 * {@link Executor}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param R
 *            The type of the object that identifies a resource for the purposes
 *            of the locking system. This is typically the name of an index.
 * 
 * FIXME The {@link ConcurrencyManager} will need to explicitly use this class
 * to buffer unisolated operations such that they are only submitted to the
 * {@link WriteExecutorService} once they already have their locks.
 * <p>
 * {@link AbstractTask} needs to be refactored as well since I believe it
 * currently takes responsibility for acquiring the locks and it should not.
 * 
 * @todo a fair option? What constraints or freedoms would it provide?
 * 
 * @todo a {@link SynchronousQueue} for the {@link #acceptedTasks}?
 * 
 * FIXME In order to support 2PL we need to decouple the {@link LockFutureTask}
 * from the transaction with which it is associated. Otherwise each
 * {@link #submit(Comparable[], Callable)} will look like a new transaction (2PL
 * is impossible unless you can execute multiple tasks for the same
 * transaction).
 * <p>
 * Perhaps this would be easier if we did not delegate the task to an
 * {@link Executor} since that does not really support the 2PL pattern.
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
public class NonBlockingLockManager</* T, */R extends Comparable<R>> {

    final protected static Logger log = Logger
            .getLogger(NonBlockingLockManager.class);

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
     * @todo could also use timeout to purge stale resource queues, but it
     *       should not matter since the {@link ResourceQueue} does not have a
     *       reference to the resource itself - just to its name.
     */
    final private ConcurrentWeakValueCache<R, ResourceQueue<LockFutureTask<? extends Object>>> resourceQueues = new ConcurrentWeakValueCache<R, ResourceQueue<LockFutureTask<? extends Object>>>(
            1000/* nresources */);

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
     * Used to track dependencies among transactions.
     */
    final protected TxDag waitsFor;

    /**
     * Tasks holding their locks are submitted to this service for execution.
     */
    final protected Executor delegate;

    /**
     * Run states for the {@link NonBlockingLockManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public enum RunState {

        /**
         * During startup. Tasks are NOT accepted.
         */
        Starting(0),

        /**
         * While running (aka open). Tasks are accepted and submitted for
         * execution once they hold their locks.
         */
        Running(1),

        /**
         * When shutting down normally. New tasks are not accepted but
         * {@link Future}s are still monitored for completion and waiting tasks
         * will eventually be granted their locks and execute on the delegate.
         */
        Shutdown(2),

        /**
         * When shutting down immediately. New tasks are not accepted, tasks
         * waiting for their locks are cancelled (they will not execute) and
         * {@link Future}s for running tasks are cancelled (they are
         * interrupted).
         */
        ShutdownNow(3),

        /**
         * When halted. New tasks are not accepted. No tasks are waiting. Any
         * {@link Future}s were cancelled.
         */
        Halted(4);

        private RunState(int val) {

            this.val = val;

        }

        final private int val;

        public int value() {

            return val;

        }

        public boolean isTransitionLegal(final RunState newval) {

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

            root.addCounter("naccepted", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.naccepted);
                }
            });

            root.addCounter("nrejected", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.nrejected);
                }
            });

            root.addCounter("nstarted", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.nstarted);
                }
            });

            root.addCounter("nended", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.nended);
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

            root.addCounter("ndeadlock", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.ndeadlock);
                }
            });

            root.addCounter("ntimeout", new Instrument<Long>() {
                public void sample() {
                    setValue(counters.ntimeout);
                }
            });

            // Note: #that are waiting for their locks.
            root.addCounter("nwaiting", new Instrument<Integer>() {
                public void sample() {
                    setValue(counters.nwaiting);
                }
            });

            // Note: #that have acquired locks are executing concurrently.
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
                    setValue(runState.toString());
                }
            });

        }

        return root;

    }

    private CounterSet root;

    /**
     * Counters for the {@link NonBlockingLockManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class Counters {

        /**
         * The #of tasks that were accepted by the service (running total).
         */
        public long naccepted;

        /**
         * The #of tasks that were rejected by the service (running total).
         */
        public long nrejected;

        /**
         * The #of tasks that have been started on the delegate {@link Executor}
         * (running total).
         */
        public long nstarted;

        /**
         * The #of tasks that whose execution on the delegate {@link Executor}
         * is complete (either by normal completion or by error, but only for
         * tasks which were executed on the delegate) (running total).
         */
        public long nended;

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
         * The #of tasks that timed out when they attempted to acquire their
         * locks (running total). Note that a task MAY retry lock acquisition
         * and this counter will be incremented each time it does so and then
         * times out.
         */
        public long ntimeout;

        /**
         * #of tasks that are currently waiting on locks. This is the effective
         * queue length of the {@link NonBlockingLockManager}. To get the
         * actual queue length you need to add this to the length of the queue
         * for the delegate {@link Executor}.
         */
        public int nwaiting;

        /**
         * #of tasks that have acquired their locks and are concurrently
         * executing. This is the true measure of concurrency.
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
     * @param predeclareLocks
     *            When <code>true</code>, operations MUST declare all locks
     *            before they begin to execute. This makes possible several
     *            efficiencies and by sorting the resources in each lock request
     *            into a common order we are able to avoid deadlocks entirely.
     * @param delegate
     *            The service on which the tasks will be executed.
     *            <p>
     *            Note: The <i>delegate</i> MUST NOT use a bounded queue or
     *            cause tasks to be run in the caller's thread. The use of a
     *            {@link SynchronousQueue} or an unbounded
     *            {@link LinkedBlockingQueue} for the <i>delegate</i>'s
     *            workQueue are both acceptable.
     *            <p>
     *            Note: If {@link Executor#execute(Runnable)} blocks for the
     *            <i>delegate</i> then the {@link AcceptTask} will also block
     *            and this class will be non-responsive until the <i>delegate</i>
     *            has accepted each [waitingTask] for execution. Some
     *            {@link Executor}s can cause the task to be run in the
     *            caller's thread, which would be the {@link AcceptTask} itself
     *            and which also has the effect of causing this class to be
     *            non-responsive until the task is complete.
     */
    public NonBlockingLockManager(final int maxConcurrency,
            final boolean predeclareLocks, final Executor delegate) {

        this(maxConcurrency, predeclareLocks, true/* sortLockRequests */,
                delegate);

    }

    /**
     * Create a lock manager. No concurrency limit imposed when
     * <i>predeclareLocks</i> is <code>true</code> as deadlocks are
     * impossible and we do not maintain a WAITS_FOR graph.
     * 
     * @param maxConcurrency
     *            The maximum multi-programming level (ignored if
     *            <i>predeclareLocks</i> is <code>true</code>).
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
     * @param delegate
     *            The service on which the tasks will be executed.
     *            <p>
     *            Note: The <i>delegate</i> MUST NOT use a bounded queue or
     *            cause tasks to be run in the caller's thread. The use of a
     *            {@link SynchronousQueue} or an unbounded
     *            {@link LinkedBlockingQueue} for the <i>delegate</i>'s
     *            workQueue are both acceptable.
     *            <p>
     *            Note: If {@link Executor#execute(Runnable)} blocks for the
     *            <i>delegate</i> then the {@link AcceptTask} will also block
     *            and this class will be non-responsive until the <i>delegate</i>
     *            has accepted each [waitingTask] for execution. Some
     *            {@link Executor}s can cause the task to be run in the
     *            caller's thread, which would be the {@link AcceptTask} itself
     *            and which also has the effect of causing this class to be
     *            non-responsive until the task is complete.
     */
    NonBlockingLockManager(final int maxConcurrency,
            final boolean predeclareLocks, final boolean sortLockRequests,
            final Executor delegate) {

        if (maxConcurrency < 2 && !predeclareLocks) {

            throw new IllegalArgumentException(
                    "maxConcurrency: must be 2+ unless you are predeclaring locks, not "
                            + maxConcurrency);

        }

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

        if (delegate == null)
            throw new IllegalArgumentException();

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

        this.delegate = delegate;

        // start service.
        service.submit(new AcceptTask());

        // change the run state.
        lock.lock();
        try {

            setRunState(RunState.Running);

        } finally {

            lock.unlock();

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

        private final long lockTimeout;

        private final int maxLockTries;

        /**
         * Incremented each time a deadlock is detected. We will not retry if
         * {@link #maxLockTries} is exceeded.
         */
        private int ntries = 0;
        
        /**
         * The timestamp in nanoseconds when this task was accepted. This is
         * used to decide whether the {@link #lockTimeout} has expired.
         */
        final private long acceptTime = System.nanoTime();

        /**
         * The set of {@link ResourceQueue}s for which this task owns a lock
         * (is a member of the granted group) (NOT THREAD SAFE).
         * <p>
         * Note: This collection is required in order for the
         * {@link ResourceQueue}s for which the task has asserted a lock
         * request to remain strongly reachable. Without such hard references
         * the {@link ResourceQueue}s would be asynchronously cleared from the
         * {@link NonBlockingLockManager#resourceQueues} collection by the
         * garbage collector.
         */
        private final LinkedHashSet<ResourceQueue<LockFutureTask<? extends Object>>> lockedResources = new LinkedHashSet<ResourceQueue<LockFutureTask<? extends Object>>>();

        /**
         * True if the {@link #lockTimeout} has expired when measured against
         * <i>now</i>.
         */
        protected boolean isTimeout() {

            return (System.nanoTime() - acceptTime) >= lockTimeout;

        }

        public String toString() {

            return super.toString() + //
                    "{resources=" + Arrays.toString(resource) + //
                    ", done=" + isDone() + //
                    ", cancelled=" + isCancelled() + //
                    ", ntries=" + ntries +
                    "}";

        }

        public LockFutureTask(final R[] resource, final Callable<T> task,
                final long timeout, final int maxLockTries) {

            super(task);

            this.resource = resource;

            this.lockTimeout = timeout;

            this.maxLockTries = maxLockTries;

        }

        public LockFutureTask(final R[] resources, final Runnable task,
                final T val, final long timeout, final int maxLockTries) {

            super(task, val);

            this.resource = resources;

            this.lockTimeout = timeout;

            this.maxLockTries = maxLockTries;

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

            return nanoTime_lockLatency;

        }

        private long nanoTime_lockLatency;

        /**
         * The maximum #of times that the task will attempt to acquire its locks
         * (positive integer).
         */
        public int getMaxLockTries() {

            return maxLockTries;

        }

        /**
         * The timeout (milliseconds) or ZERO (0L) for an infinite timeout.
         */
        public long getLockTimeout() {

            return lockTimeout;

        }

        /**
         * Extended signal {@link NonBlockingLockManager#stateChanged} when the
         * task completes, to track counters, and also exposed to the outer
         * class.
         */
        @Override
        protected void setException(final Throwable t) {

            super.setException(t);

            lock.lock();
            try {
                if (DEBUG)
                    log.debug("Exception: " + this + ", cause=" + t, t);
                counters.nerror++;
                /*
                 * Note: Not known to be running, hence assume waiting (this
                 * method is sometimes called before the task begins to
                 * execute).
                 */
                final boolean waiting = true;
                releaseLocks(this,waiting);
                stateChanged.signal();
            } finally {
                lock.unlock();
            }

        }

        /**
         * Extended signal {@link NonBlockingLockManager#stateChanged} when the
         * task completes and to track counters.
         */
        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            
            final boolean ret = super.cancel(mayInterruptIfRunning);
            
            lock.lock();
            try {
                if (DEBUG)
                    log.debug("Cancelled: " + this);
                counters.ncancel++;
                /*
                 * Note: Not known to be running, hence assume waiting (this
                 * method is sometimes called before the task begins to
                 * execute).
                 */
                final boolean waiting = true;
                releaseLocks(this, waiting);
                stateChanged.signal();
            } finally {
                lock.unlock();
            }

            return ret;

        }

        /**
         * Extended signal {@link NonBlockingLockManager#stateChanged} when the
         * task completes and to track counters.
         */
        @Override
        public void run() {

            synchronized (counters) {

                counters.nstarted++;

                counters.nrunning++;

                if (counters.nrunning > counters.maxRunning) {

                    counters.maxRunning = counters.nrunning;

                }

            }

            try {

                if(DEBUG)
                    log.debug("Running: "+this);
                
                super.run();

            } finally {

                lock.lock();
                try {
                    if(DEBUG)
                        log.debug("Did run: "+this);
                    synchronized (counters) {
                        counters.nended++;
                        counters.nrunning--;
                    }
                    /*
                     * The task is KNOWN to not be waiting since it was running.
                     */
                    final boolean waiting = false;
                    releaseLocks(this, waiting);
                    stateChanged.signal();
                } finally {
                    lock.unlock();
                }
                
            }

        }

    }

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
     * 
     * @todo add variant for Runnable target.
     * 
     * @todo get rid [lockTimeout] since you can do get(timeout) on the Future
     *       and the task will be cancelled unless it is complete by the
     *       timeout.
     * 
     * @todo get rid of [maxLockTries] since you can specify a timeout and that
     *       will determine how much effort will be put into attempting to work
     *       around a deadlock?
     */
    public <T> Future<T> submit(final R[] resource, final Callable<T> task) {

        return submit(resource, task, TimeUnit.SECONDS,
                Long.MAX_VALUE/* timeout */, 1/* maxLockTries */);

    }

    public <T> Future<T> submit(final LockCallable<R, T> task) {

        return submit(task.getResource(), task);

    }

    public <T> Future<T> submit(final R[] resource, final Callable<T> task,
            final TimeUnit unit, final long lockTimeout, final int maxLockTries) {

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

            switch (runState) {

            case Running: {

                final LockFutureTask<T> future = new LockFutureTask<T>(a, task,
                        lockTimeout, maxLockTries);

                try {

                    acceptedTasks.add(future);

                    counters.naccepted++;

                } catch (IllegalStateException ex) {

                    counters.nrejected++;
                    
                    throw new RejectedExecutionException(ex);

                }

                stateChanged.signal();

                return future;

            }

            default:

                counters.nrejected++;

                throw new RejectedExecutionException("runState=" + runState);

            }

        } finally {

            lock.unlock();

        }

    }

    /**
     * Used to run the {@link AcceptTask} and the {@link MonitorTask}.
     * 
     * @todo Monitor this service. Convert {@link Counters#nrunning} and
     *       {@link Counters#nwaiting} into moving averages.
     */
    private final ExecutorService service = Executors
            .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                    .getName()));

    /**
     * The service run state.
     */
    private volatile RunState runState = RunState.Starting;

    /**
     * Lock used to protect changes various state changes, including change to
     * the {@link #runState}.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition is signaled whenever the {@link AcceptTask} needs to wake up.
     */
    private final Condition stateChanged = lock.newCondition();

    /**
     * Tasks accepted but not yet waiting on their locks. Tasks are moved from
     * here to the {@link #waitingTasks} asynchronously in order to de-couple
     * the caller from {@link DeadlockException}s or periods when the system is
     * at the maximum multi-programming level and can not accept another lock
     * request.
     */
    final private BlockingQueue<LockFutureTask<? extends Object>> acceptedTasks = new LinkedBlockingQueue<LockFutureTask<? extends Object>>();

    /**
     * Tasks whose lock requests are in the appropriate {@link ResourceQueue}s
     * but which are not yet executing.
     */
    private final BlockingQueue<LockFutureTask<? extends Object>> waitingTasks = new LinkedBlockingQueue<LockFutureTask<? extends Object>>();

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
                switch (runState) {
                case Starting: {
                    awaitStateChange(Starting);
                    continue;
                }
                case Running: {
                    while (processAcceptedTasks() || processWaitingTasks()) {
                        // do work
                    }
                    awaitStateChange(Running);
                    continue;
                }
                case Shutdown: {
                    while (processAcceptedTasks() || processWaitingTasks()) {
                        /*
                         * Do work.
                         * 
                         * Note: will run anything already accepted. That is
                         * intentional. Once the lock manager is shutdown it will no
                         * longer accept tasks, but it will process those tasks
                         * which it has already accepted.
                         */
                    }
                    lock.lock();
                    try {
                        if (acceptedTasks.isEmpty() && waitingTasks.isEmpty()) {
                            /*
                             * There is no more work to be performed so we can
                             * change the runState.
                             */
                            log.warn("No more work.");
                            if (runState.val < RunState.ShutdownNow.val) {
                                setRunState(RunState.ShutdownNow);
                                break;
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                    awaitStateChange(Shutdown);
                    continue;
                }
                case ShutdownNow: {
                    /*
                     * Cancel all tasks, clearing all queues. Note that only
                     * tasks which are on [runningTasks] need to be interrupted
                     * as tasks on the other queues are NOT running.
                     */
                    log.warn(runState);
                    cancelTasks(acceptedTasks.iterator(), false/* mayInterruptIfRunning */);
                    cancelTasks(waitingTasks.iterator(), false/* mayInterruptIfRunning */);
                    lock.lock();
                    try {
                        if (runState.val < RunState.Halted.val) {
                            setRunState(RunState.Halted);
                        }
                    } finally {
                        lock.unlock();
                    }
                    // fall through.
                }
                case Halted: {
                    log.warn(runState);
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
         * expected {@link RunState} then this blocks until someone signals
         * {@link NonBlockingLockManager#stateChanged}. That signal can come
         * either from submitting a new task, from a running task that
         * completes, from a task being cancelled, or from a task whose
         * exception was set.
         * 
         * @see LockFutureTask
         * @see NonBlockingLockManager#submit(Comparable[], Callable, TimeUnit, long, int)
         */
        private void awaitStateChange(final RunState expected) {
            lock.lock();
            try {
                /*
                 * While we hold the lock we verify that there really is no work
                 * to be done and that we are in the expected run state. Then
                 * and only then do we wait on [stateChanged].
                 */
                if (runState != expected) {
                    // In a different run state.
                    return;
                }
                if (!acceptedTasks.isEmpty() || !waitingTasks.isEmpty()) {
                    // Some work can be done.
                    return;
                }
                log.warn("Waiting...");
                stateChanged.await();
                log.warn("Woke up...");
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
         * Processes accepted tasks, adding lock requests for each in turn:
         * <ul>
         * <li> If requesting locks for a task would exceed the configured
         * multi-programming level then we do not issue the request and return
         * immediately</li>
         * <li>If the lock requests for a task would cause a deadlock then set
         * the {@link DeadlockException} on the {@link Future} and drop the task</li>
         * <li>If the timeout for the lock requests has already expired, then
         * set set {@link TimeoutException} on the {@link Future} and drop the
         * task</li>
         * </ul>
         * 
         * @return true iff any tasks were moved to the waiting queue.
         */
        private boolean processAcceptedTasks() {

            int nmoved = 0;

            final Iterator<LockFutureTask<? extends Object>> itr = acceptedTasks
                    .iterator();

            while (itr.hasNext()) {

                final LockFutureTask<? extends Object> t = itr.next();

                if (t.isCancelled()) {

                    // already cancelled, e.g., by the caller.
                    itr.remove();
                    
                    continue;

                }

                if (t.isTimeout()) {

                    // set exception on the task.
                    t.setException(new java.util.concurrent.TimeoutException());

                    counters.ntimeout++;
                    
                    itr.remove();
                    
                    continue;

                }

                int nvertices = -1;
                lock.lock();
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
                        
                        return nmoved > 0;
                    
                    }
                    
                    /*
                     * Modify the state of the ResourceQueue(s) and the optional
                     * TxDag to reflect the lock requests. The method will
                     * release the lock requests if a deadlock would arise.
                     * 
                     * Note: Can thrown DeadlockException.
                     */

                    t.ntries++;
                    
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
                        waitsFor.lookup(t, true/* insert */);
                        
                    }

//                    log.warn("Requesting locks: "+t);
                    requestLocks(t);

                } catch (Throwable t2) {

//                    /*
//                     * Note: Since this method does not support 2PL we know that the
//                     * task was not waiting on any transactions (and hence that none are
//                     * waiting on it). All of its lock requests will be at the end of
//                     * any resource queues.
//                     */
//                    final boolean waiting = true;
//
//                    releaseLocks(t, waiting);

                    // set exception on the task (clears locks)
                    t.setException(t2);

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
                    
                    if ((t2 instanceof DeadlockException)) {

                        /*
                         * This is the ONLY expected exception when we issue the
                         * lock requests.
                         */
                        
                        log.warn("Deadlock: " + this + ", task=" + t /* , ex */);

                        counters.ndeadlock++;

                        if ((t.ntries < t.maxLockTries)) {

                            log.warn("Will retry task: " + t);

                            // leave on queue to permit retry.
                            continue;

                        } else {

                            log.error("Deadlock not resolved: " + this
                                    + ", task=" + t);

                        }

                    } else {

                        /*
                         * Anything else is an internal error.
                         */

                        log.error("Internal error: task=" + t, t2);
                        
                    }
                    
                    /*
                     * Remove task from the accepted queue since we will not
                     * re-run it.
                     * 
                     * Note: We DO retry tasks which result in a deadlock so
                     * they stay in the queue.
                     */
                    itr.remove();
                    
                } finally {

                    lock.unlock();
                   
                }

                waitingTasks.add(t);

                counters.nwaiting++;

                nmoved++;

                itr.remove();
                
                if (DEBUG) // moved to the waiting queue.
                    log.debug("Waiting: " + t);

            }

            if (INFO && nmoved > 0)
                log.info("#moved=" + nmoved);
            
            return nmoved > 0;
            
        }

        /**
         * For each task waiting to run:
         * <ul>
         * <li>if the task has been cancelled, then remove it from the waiting
         * tasks queue</li>
         * <li>if the lock timeout has expired, then set an exception on the
         * task and remove it from the waiting task queue and remove its lock
         * requests from the various {@link ResourceQueue}s</li>
         * <li>if the lock requests for that task have been granted then submit
         * the task to execute on the delegate and move it to the running tasks
         * queue</li>
         * <li>if the delegate rejects the task, then it is NOT removed from
         * the waiting tasks queue and this method returns immediately</li>
         * </ul>
         * 
         * @return <code>true</code> if any tasks were moved to the running
         *         tasks list.
         */
        private boolean processWaitingTasks() {

            final Iterator<LockFutureTask<? extends Object>> itr = waitingTasks
                    .iterator();

            int nstarted = 0;
            
            while (itr.hasNext()) {

                final LockFutureTask<? extends Object> t = itr.next();

                if (t.isCancelled()) {

                    // cancelled while awaiting locks.
                    itr.remove();

                    continue;

                }
                
                if (t.isTimeout()) {

                    // timeout while awaiting locks.
                    t.setException(new java.util.concurrent.TimeoutException());

                    itr.remove();

                    counters.ntimeout++;
                    
                    continue;

                }

                final boolean holdsLocks;
                lock.lock();
                try {
                    holdsLocks = holdsAllLocks(t);
                } finally {
                    lock.unlock();
                }
                
                if (holdsLocks) {

                    // holding locks, so execute the task.

                    if (INFO)
                        log.info("Executing: " + t);

                    try {

                        /*
                         * Note: FutureTask will take can of updating its state
                         * before/after the runnable target.
                         * 
                         * Note: If delegate.execute() blocks then the acceptor
                         * thread will also block and this class will be
                         * non-responsive until the delegate had accepted each
                         * [waitingTask] for execution.  Some executors can cause
                         * the task to be run in the caller's thread, which would
                         * be the AcceptTask itself.
                         */

                        assert !lock.isHeldByCurrentThread();
                        
                        delegate.execute(t);

                    } catch (RejectedExecutionException t2) {

                        /*
                         * We can't queue this task now so we stop processing
                         * the waiting tasks. We will pick up on those tasks
                         * again the next time this method is invoked.
                         */
                        if(INFO)
                            log.info("Delegate is busy.");

                        return nstarted > 0;
                        
                    }

                    itr.remove();

                    nstarted++;
                    
                    continue;

                }

            }

            if (INFO && nstarted > 0)
                log.info("#started=" + nstarted);

            return nstarted > 0;

        }

    } // AcceptTask

    public boolean isOpen() {

        return runState == RunState.Running;

    }

    public boolean isShutdown() {

        switch (runState) {
        case Shutdown:
        case ShutdownNow:
        case Halted:
            return true;
        }

        return false;

    }

    public boolean isTerminated() {

        return runState == RunState.Halted;

    }

    public void shutdown() {

        lock.lock();

        try {

            if (runState.val < RunState.Shutdown.val) {

                setRunState(RunState.Shutdown);

            }

        } finally {

            lock.unlock();

        }

    }

    public void shutdownNow() {

        lock.lock();

        try {

            if (runState.val < RunState.ShutdownNow.val) {

                setRunState(RunState.ShutdownNow);

            }

        } finally {

            lock.unlock();

        }

    }

    /**
     * Change the {@link #runState}.
     * 
     * @param newval
     *            The new value.
     * 
     * @throws IllegalStateException
     *             if the state transition is illegal.
     * @throws IllegalMonitorStateException
     *             if the current thread does not hold the {@link #lock}.
     */
    private final void setRunState(final RunState newval) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (!runState.isTransitionLegal(newval)) {

            throw new IllegalStateException("runState=" + runState
                    + ", but newValue=" + newval);

        }

        if (runState != newval) {

            if (INFO)
                log.info("Set runState=" + newval);

            runState = newval;

            stateChanged.signal();

        }

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
     * @throws DeadlockException
     *             If the lock request(s) would cause a deadlock.
     * @throws IllegalStateException
     *             If locks are being predeclared and there are already locks
     *             held by the operation.
     */
    private <T> void requestLocks(final LockFutureTask<T> task)
            throws DeadlockException {

        if (task == null)
            throw new IllegalArgumentException();

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        switch(runState) {
        case ShutdownNow:
        case Halted:
            /*
             * No new lock requests are permitted once we reach ShutdownNow.
             */
            throw new IllegalStateException("runState=" + runState);
        }
        
        if (task.resource.length == 0)
            return; // NOP.

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

        } else {

            /*
             * When we are not using the WAITS_FOR graph all we have to do is
             * add the task to each of the resource queues. It will run once it
             * is at the head of each resource queue into which it is placed by
             * its lock requests.
             */

            for (R r : (R[]) task.resource) {

                // make sure queue exists for this resource.
                final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = declareResource(r);

                /*
                 * Add a lock request for this resource.
                 */
                resourceQueue.queue.add(task);

                /*
                 * Add the resource queue to the set of queues whose locks are held
                 * by this task.
                 */
                task.lockedResources.add(resourceQueue);

            }

        }
    
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
    private <T> void releaseLocks(final LockFutureTask<T> t,
            final boolean waiting) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (DEBUG)
            log.debug("Releasing locks: " + t);

        try {

            final Iterator<ResourceQueue<LockFutureTask<? extends Object>>> itr = t.lockedResources
                    .iterator();

            while (itr.hasNext()) {

                final ResourceQueue<LockFutureTask<? extends Object>> resourceQueue = itr
                        .next();

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

        } finally {

            /*
             * At this point there are edges in the WAITS_FOR graph and the no
             * longer on any of the resource queues. Since we know that it is
             * not waiting we can just clear its edges the easy way.
             * 
             * @todo The [waiting] flag is not being used to optimize the
             * removal of edges from the WAITS_FOR graph. Fixing this will
             * require us to remove the operation from each
             * {@link ResourceQueue} without updating the {@link TxDag} and then
             * update the {@link TxDag} using
             * {@link TxDag#removeEdges(Object, boolean)} and specifying "false"
             * for "waiting". Since this operation cuts across multiple queues
             * at once additional synchronization MAY be required.
             */
            if (waitsFor != null) {

                synchronized (waitsFor) {

                    try {
                        
                        waitsFor.removeEdges(t, waiting);
                        
                    } catch (Throwable t2) {
                        
                        log.warn(t2);
                        
                    }

                }

            }

            /*
             * Release the vertex (if any) in the WAITS_FOR graph.
             * 
             * Note: A vertex is created iff a dependency chain is established.
             * Therefore it is possible for a transaction to obtain a lock
             * without a vertex begin created for that tranasaction. Hence it is
             * Ok if this method returns [false].
             */

            if (waitsFor != null) {

                waitsFor.releaseVertex(t);

            }
            
        } 

    }

    public String toString() {

        // return getCounters().toString();

        return getClass().getName() + //
                "{ accepted=" + acceptedTasks.size() + //
                ", waiting=" + waitingTasks.size() + //
//                ", running=" + runningTasks.size() + //
                ", #started="+counters.nstarted+//
                ", #ended="+counters.nended+//
                ", #cancel="+counters.ncancel+//
                ", #timeout="+counters.ntimeout+//
                ", #error="+counters.nerror+//
                ", #deadlock="+counters.ndeadlock+//
                (waitsFor!=null?", vertices="+waitsFor.size():"")+//
                "}";

    }

    /**
     * Unbounded queue of operations waiting to gain an exclusive lock on a
     * resource. By default, the queue imposes a "fair" schedule for access to
     * the resource. Deadlocks among resources are detected using a
     * <code>WAITS_FOR</code> graph that is shared by all resources and
     * transactions for a given database instance.
     * <p>
     * Note: deadlock detection MAY be disabled when all lock requests are (a)
     * pre-declared; and (b) sorted. When disabled the <code>WAITS_FOR</code>
     * graph is NOT maintained.
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

//        /**
//         * Return iff the tx currently holds the lock on the resource.
//         * 
//         * @throws IllegalStateException
//         *             if the tx is not in the granted group.
//         */
//        private final void assertOwnsLock(final T tx) {
//
//            if (queue.peek() != tx) {
//
//                throw new IllegalStateException("Does not hold lock: " + tx);
//
//            }
//
//        }

//        /**
//         * Request a lock on the resource. If the queue is empty or if the task
//         * already owns the lock then return immediately. Otherwise, update the
//         * optional {@link TxDag} to determine if the a deadlock would result.
//         * If no deadlock would result, then add the task to the queue.
//         * 
//         * @param tx
//         *            The transaction.
//         * 
//         * @return <code>true</code> if the lock is granted for that
//         *         transaction (either it already owns the lock or the resource
//         *         queue is empty so it is immediately granted the lock).
//         * 
//         * @throws DeadlockException
//         *             if the request would cause a deadlock among the running
//         *             transactions.
//         */
//        public void add(final T tx) throws DeadlockException {
//
//            if (tx == null)
//                throw new IllegalArgumentException();
//
//            if(!lock.isHeldByCurrentThread())
//                throw new IllegalMonitorStateException();
//
//            if (DEBUG)
//                log.debug("enter: tx=" + tx + ", queue=" + this);
//
//            // already locked.
//            if (queue.peek() == tx) {
//
//                /*
//                 * Note: This is being disallowed since we are not supporting
//                 * 2PL here and it should not be possible for the tx to already
//                 * own the lock. If you want to support 2PL then you would allow
//                 * this case.
//                 */
//                throw new IllegalStateException("Already owns lock: tx=" + tx
//                        + ", queue=" + this);
//
//            }
//
//            if (queue.isEmpty()) {
//
//                // the queue is empty so immediately grant the lock.
//                queue.add(tx);
//
//                if (INFO)
//                    log.info("Granted lock with empty queue: tx=" + tx
//                            + ", queue=" + this);
//
//                // lock can be granted immediately.
//                return;
//
//            }
//
//            /*
//             * Update the WAITS_FOR graph since we are now going to wait on the
//             * tx that currently holds this lock.
//             * 
//             * We need to add an edge from this transaction to the transaction
//             * that currently holds the lock for this resource. This indicates
//             * that [tx] WAITS_FOR the operation that holds the lock.
//             * 
//             * We need to do this for each predecessor in the queue so that the
//             * correct WAITS_FOR edges remain when a predecessor is granted the
//             * lock.
//             * 
//             * FIXME The problem here is when there are two transactions share
//             * two or more locks requests. If one transaction is already running
//             * then the other transaction will wind up asserting one WAITS_FOR
//             * edge for each of the shared lock requests. TxDag does not allow
//             * more than a single WAITS_FOR edge count for the same source and
//             * target and throws an IllegalStateException.
//             * 
//             * This was not a problem in the old LockManager because we
//             * incrementally grew the lock requests as they were granted. It is
//             * a problem now because we issue all requests at once.
//             * 
//             * One way to solve this is to add to each of the queues, receiving
//             * back a Set of the predecessors. Merge those sets. That gives the
//             * set of transactions on which the current task must wait. Then
//             * create those edges. If any edge would cause a deadlock rollback
//             * the changes to each of the resource queues (the TxDag request is
//             * atomic so it will not have been modified).
//             */
//            if (waitsFor != null) {
//
//                final Object[] predecessors = queue.toArray();
//
//                /*
//                 * Note: this operation is atomic. If it fails, then none of
//                 * the edges were added.
//                 * 
//                 * Note: throws DeadlockException.
//                 */
//                try {
//                waitsFor.addEdges(tx/* src */, predecessors);
//                } catch(IllegalStateException ex) {
//                    System.err.println("task: "+tx);
//                    System.err.println("predecessors: "+Arrays.toString(predecessors));
//                    System.err.println("queue:"+toString());
//                    System.err.println(waitsFor.toString());
//                    waitsFor.addEdges(tx/* src */, predecessors);
//                    System.exit(1);
//                }
//
//            }
//
//            /*
//             * Now that we know that the request does not directly cause a
//             * deadlock we add the request to the queue. The task will not
//             * execute until it maeks it to the head of the queue.
//             */
//            queue.add(tx);
//
//        }
//
//        /**
//         * Remove the tx from the {@link ResourceQueue}.
//         * 
//         * @param tx
//         *            The transaction.
//         * 
//         * @deprecated This should be optimized out.
//         */
//        public void remove(final T tx) {
//
//            if (tx == null)
//                throw new IllegalArgumentException();
//            
//            if(!lock.isHeldByCurrentThread())
//                throw new IllegalMonitorStateException();
//            
//            if (queue.peek() != tx) {
//
//                /*
//                 * Removing some tx which does not own the lock.
//                 */
//
//                if (!queue.remove(tx)) {
//
//                    throw new AssertionError("Not in queue? tx=" + tx
//                            + ", queue=" + Arrays.toString(queue.toArray()));
//                
//                }
//                
//                return;
//
//            }
//
//            /*
//             * Removing the tx that owns the lock.
//             */
//            if (tx != queue.remove()) {
//
//                throw new AssertionError("Removed wrong tx?");
//                
//            }
//
//            /*
//             * We just removed the granted lock. Now we have to update the
//             * WAITS_FOR graph to remove all edges whose source is a pending
//             * transaction (for this resource) since those transactions are
//             * waiting on the transaction that just released the lock.
//             */
//
//            if (DEBUG)
//                log.debug("removed lock owner from queue: " + tx);
//
//            if (waitsFor != null) {
//
//                final Iterator<T> itr = queue.iterator();
//
//                synchronized (waitsFor) {
//
//                    while (itr.hasNext()) {
//
//                        final T pendingTx = itr.next();
//
//                        if (DEBUG)
//                            log.debug("Removing edge: pendingTx=" + pendingTx);
//
//                        waitsFor.removeEdge(pendingTx, tx);
//
//                    }
//
//                }
//
//            }
//
//        }

    } // ResourceQueue

}
