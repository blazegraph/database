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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.concurrent.NonBlockingLockManagerWithNewDesign.LockFutureTask;
import com.bigdata.concurrent.NonBlockingLockManagerWithNewDesign.ResourceQueue;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.AbstractTask;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.MovingAverageTask;
import com.bigdata.util.concurrent.QueueSizeMovingAverageTask;
import com.bigdata.util.concurrent.WriteTaskCounters;

/**
 * This class coordinates a schedule among concurrent operations requiring
 * exclusive access to shared resources. Whenever possible, the result is a
 * concurrent schedule - that is, operations having non-overlapping lock
 * requirements run concurrently while operations that have lock contentions are
 * queued behind operations that currently have locks on the relevant resources.
 * A {@link ResourceQueue} is created for each resource and used to block
 * operations that are awaiting a lock. When those locks become available,
 * {@link #ready(Runnable)} will be invoked with the task.
 * <p>
 * The class will use an optional {@link TxDag WAITS_FOR} graph to detect
 * deadlocks if locks are not being pre-declared (and hence deadlocks are
 * possible).
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
 * @todo 2PL
 *       <p>
 *       Note: It is not really possible to have deadlocks without 2PL. Instead,
 *       what happens is that the maximum multi-programming capacity of the
 *       {@link TxDag} is temporarily exceeded. Review the unit tests again when
 *       supporting 2PL and verify that the deadlock handling logic works.
 *       <p>
 *       In order to support 2PL we need to decouple the {@link LockFutureTask}
 *       from the transaction with which it is associated and use the latter in
 *       the {@link TxDag}. Otherwise each
 *       {@link #submit(Comparable[], Callable)} will always look like a new
 *       transaction (2PL is impossible unless you can execute multiple tasks
 *       for the same transaction).
 *       <p>
 *       To introduce 2PL I need to create interfaces which extend
 *       {@link Callable} which are understood by this class. One such interface
 *       should provide a method by which a task can release its locks. For 2PL,
 *       either that interface or an extension of the interface would need to
 *       have a method by which a task could post new lock requests. Such a
 *       callable could be submitted directly. It would wait if it declared any
 *       precondition locks and otherwise pass through to
 *       {@link #ready(Runnable)} immediately. We can't really just pass the
 *       task through again when it requests additional locks, so maybe the Tx
 *       would submit a task to the lock service each time it wanted to gain
 *       more or more locks and {@link #ready(Runnable)} would change its run
 *       state...
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
 *       "real-time" guarantee since the VM is not (normally) providing
 *       real-time guarantees and since we are not otherwise attempting to
 *       ensure anything except lower latency when compared to other operations
 *       awaiting their own locks.
 */
public abstract class NonBlockingLockManagerWithNewDesign</* T, */R extends Comparable<R>> {

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
     * <p>
     * Note: The timeout is set to one LBS reporting period so you can see which
     * resource queues had activity in the last reporting period.
     */
    final private ConcurrentWeakValueCacheWithTimeout<R, ResourceQueue<R, LockFutureTask<R, ? extends Object>>> resourceQueues = new ConcurrentWeakValueCacheWithTimeout<R, ResourceQueue<R, LockFutureTask<R, ? extends Object>>>(
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
     * Used to run the {@link AcceptTask}.
     */
    private final ExecutorService service = Executors
            .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                    .getName()));

    /**
     * This {@link Runnable} should be submitted to a
     * {@link ScheduledExecutorService} in order to track the average queue size
     * for each active {@link ResourceQueue} and various moving averages
     * pertaining to the lock service as a whole.
     */
    public final StatisticsTask statisticsTask = new StatisticsTask();
    
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
     * A queue of tasks (a) whose lock requests would have resulted in a
     * deadlock when they were first submitted; -or- (b) which would have
     * exceeded the maximum multi-programming capacity of the {@link TxDag}.
     * These tasks are NOT waiting on their locks.
     * <p>
     * For deadlock, up {@link #maxLockTries} will be made to post the lock
     * requests for a task, including the initial try which is made when the
     * task is accepted by this service. Tasks which are enqueued because the
     * {@link TxDag} was at its multi-programming capacity are retried until
     * they can post their lock requests.
     * <p>
     * Tasks are removed from this queue if their lock requests can be posted
     * without creating a deadlock or if {@link #maxLockTries} would be exceeded
     * for that task. The queue is cleared if the lock service is halted.
     */
    final private BlockingQueue<LockFutureTask<R, ? extends Object>> retryQueue = new LinkedBlockingQueue<LockFutureTask<R, ? extends Object>>();

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

    /**
     * Note: You MUST submit {@link #statisticsTask} to a
     * {@link ScheduledExecutorService} in order counter values which report
     * moving averages to be maintained.
     * <p>
     * Note: A new instance is returned every time. This makes the pattern where
     * the counters are "attached" to a hierarchy work since that has the
     * side-effect of "detaching" them from the returned object.
     */
//    synchronized 
    public CounterSet getCounters() {

//        if (root == null) {

        final CounterSet root = new CounterSet();

        root.addCounter("nrejected", new Instrument<Long>() {
            public void sample() {
                setValue(counters.nrejected);
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

        /*
         * #of tasks waiting to retry after a deadlock was detected -or- when
         * the maximum multi-programming capacity of the TxDag would have been
         * exceeded (the size of the retry queue, moving average).
         */
        root.addCounter("averageRetryCount", new Instrument<Double>() {
            public void sample() {
                setValue(statisticsTask.nretryAverageTask.getMovingAverage());
            }
        });

        // #of tasks waiting on locks (moving average).
        root.addCounter("averageWaitingCount", new Instrument<Double>() {
            public void sample() {
                setValue(statisticsTask.nwaitingAverageTask.getMovingAverage());
            }
        });

        // #of tasks ready to be executed (on the readyQueue, moving average).
        root.addCounter("averageReadyCount", new Instrument<Double>() {
            public void sample() {
                setValue(statisticsTask.nreadyAverageTask.getMovingAverage());
            }
        });

        /*
         * #of queues that have at least one task (moving average). Each queue
         * corresponds to a resource for which at least one task had declared
         * and posted a lock during the last sample.
         */
        root.addCounter("averageQueueBusyCount", new Instrument<Double>() {
            public void sample() {
                setValue(statisticsTask.nqueueBusyAverageTask.getMovingAverage());
            }
        });

        // #of tasks that are executing (in run(), moving average).
        root.addCounter("averageRunningCount", new Instrument<Double>() {
            public void sample() {
                setValue(statisticsTask.nrunningAverageTask.getMovingAverage());
            }
        });

        /*
         * #of tasks that are executing and are holding their locks (in run()
         * with locks not yet released, moving average).
         */
        root.addCounter("averageRunningWithLocksHeldCount",
                new Instrument<Double>() {
                    public void sample() {
                        setValue(statisticsTask.nrunningWithLocksHeldAverageTask
                                .getMovingAverage());
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

        // the current run state.
        root.addCounter("runState", new Instrument<String>() {
            public void sample() {
                setValue(serviceRunState.toString());
            }
        });

        /*
         * Adds an instrument reporting the moving average of each resourceQueue
         * defined at the moment that the caller requested these counters.
         */
        {
            
            final CounterSet tmp = root.makePath("queues");

            final Iterator<Map.Entry<R, WeakReference<ResourceQueue<R,LockFutureTask<R,? extends Object>>>>> itr = resourceQueues
                    .entryIterator();

            while (itr.hasNext()) {

                final Map.Entry<R, WeakReference<ResourceQueue<R,LockFutureTask<R,? extends Object>>>> entry = itr
                        .next();

                final WeakReference<ResourceQueue<R,LockFutureTask<R,? extends Object>>> queueRef = entry
                        .getValue();

                final ResourceQueue<R,LockFutureTask<R,? extends Object>> queue = queueRef
                        .get();

                if (queue == null)
                    continue;

                tmp.addCounter(queue.resource.toString(),
                        new WeakRefResourceQueueInstrument(queue.resource));
                
            }

        }

        // }

        return root;

    }

// private CounterSet root;
    
    /**
     * A class that dynamically resolves the {@link ResourceQueue} by its
     * resource from {@link NonBlockingLockManagerWithNewDesign#resourceQueues}.
     * If there is a {@link ResourceQueue} for that resource then the moving
     * average of its queue size is reported. Otherwise, ZERO (0d) is reported
     * as the moving average of its queue size.
     */
    private class WeakRefResourceQueueInstrument extends Instrument<Double> {

        private final R resource;
    
        public WeakRefResourceQueueInstrument(final R resource) {

            if (resource == null)
                throw new IllegalArgumentException();

            this.resource = resource;

        }

        public void sample() {

            final ResourceQueue<R, LockFutureTask<R, ? extends Object>> queue = resourceQueues
                    .get(resource);
            
            if (queue == null) {
            
                setValue(0d);
                
            } else {
                
                final double averageQueueSize = queue.statisticsTask
                        .getAverageQueueSize();

                setValue(averageQueueSize);
           
            }
            
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
         * The #of tasks that were cancelled (running total).
         */
        public long ncancel;

        /**
         * The #of tasks whose exception was set (running total).
         */
        public long nerror;

        /**
         * The #of tasks that are currently on the retry queue awaiting an
         * opportunity to post their lock requests that does not result in a
         * deadlock or exceed the multi-programming capacity of the optional
         * {@link TxDag}.
         */
        public int nretry;

        /**
         * #of tasks that are currently waiting on one or more locks.
         */
        public int nwaiting;

        /**
         * The #of tasks that currently hold their locks but are not executing.
         */
        public int nready;
        
        /**
         * #of tasks are currently executing (in {@link LockFutureTask#run()}).
         */
        public int nrunning;

        /**
         * #of tasks are currently executing (in {@link LockFutureTask#run()})
         * AND are holding their locks.
         */
        public int nrunningWithLocksHeld;

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
        service.submit(new AcceptTask<R>(this));

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

            return new LockFutureTask<R,T>(this, a, task).acceptTask();

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

            return new LockFutureTask<R,T>(this, a, task, val).acceptTask();

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

            final LockFutureTask task = (LockFutureTask) getTaskWithLocks(resource);
            
            if(task == null) {
                
                /*
                 * There is no task holding all of these locks.
                 */
                
                throw new IllegalStateException();
                
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
     * Return the task holding all of the specified locks.
     * 
     * @param resource
     *            The locks.
     * @return The task -or- <code>null</code> iff there is no such task.
     */
    public Runnable getTaskWithLocks(final R[] resource) {
        
        lock.lock();

        try {

            LockFutureTask<R, ? extends Object> task = null;

            for (R r : resource) {

                final ResourceQueue<R, LockFutureTask<R, ? extends Object>> resourceQueue = resourceQueues
                        .get(r);

                if (resourceQueue == null) {

                    /*
                     * There is no ResourceQueue for this resource so there can
                     * not be any task holding the lock for that resource.
                     */

                    if (DEBUG)
                        log.debug("No task holds this lock: " + r);

                    return null;

                }

                if (task == null) {

                    /*
                     * Find the task by checking the resource queue for any of
                     * its declared locks.
                     */

                    task = resourceQueue.queue.peek();

                    if (task == null) {

                        if (DEBUG)
                            log.debug("No task holds this lock: " + r);

                        return null;

                    }

                } else {

                    /*
                     * verify that the task holds the rest of its declared
                     * locks.
                     */

                    if (task != resourceQueue.queue.peek()) {

                        if (DEBUG)
                            log
                                    .debug("Task holding the other locks does not hold this lock: "
                                            + r);

                        return null;
                        
                    }

                }

            }

            if (task == null) {

                throw new AssertionError();

            }

            if (task.taskRunState != TaskRunState.RunningWithLocks) {

                throw new IllegalStateException("taskRunState="
                        + task.taskRunState);

            }

            return task;

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
         * requests.
         */
        Retry(0, false/* lockRequestsPosted */, false/* running */),
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

                // transition when the lock requests would deadlock or
                // exceed the multi-programming capacity of the TxDag.
                if (newval == Retry)
                    return true;

                // allows task to directly request locks.
                if (newval == LocksRequested)
                    return true;

                // allows task to directly obtain locks.
                if (newval == LocksReady)
                    return true;

                if (newval == Halted)
                    return true;

            } else if (this == Retry) {

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
    static public class LockFutureTask<R extends Comparable<R>, T> extends FutureTask<T> {

        /**
         * The instance of the outer class.
         * <p>
         * Note: I ran into compilation errors on 64-bit systems when making
         * references to non-static inner classes so this is now an explicit
         * ctor parameter.
         */
        final NonBlockingLockManagerWithNewDesign<R> lockService;
        
        /**
         * The locks which the task needs to run.
         */
        private final R[] resource;

        /**
         * Incremented each time a deadlock is detected. We will not retry if
         * {@link #maxLockTries} would be exceeded.
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
         * {@link NonBlockingLockManagerWithNewDesign#resourceQueues} collection
         * by the garbage collector.
         */
        private final LinkedHashSet<ResourceQueue<R, LockFutureTask<R, ? extends Object>>> lockedResources = new LinkedHashSet<ResourceQueue<R, LockFutureTask<R, ? extends Object>>>();

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

            if(!lockService.lock.isHeldByCurrentThread())
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

                    if(oldval.isRunning()) {
                        
                        lockService.counters.nrunningWithLocksHeld--;
                        
                    }
                    
                    lockService.releaseLocksForTask(this, waiting);

                }

            } finally {

                this.taskRunState = newval;

                if (!lockService.retryQueue.isEmpty()) {

                    /*
                     * The purpose of the retryQueue is to automatically retry
                     * tasks which would have created a deadlock -or- which
                     * would have exceeded the maximum multi-programming
                     * capacity of the TxDag when the task was first submitted.
                     * In order to do that we need to wake up the AcceptTask if
                     * there are tasks on the retryQueue.
                     */

                    lockService.stateChanged.signal();
                    
                }

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

        private LockFutureTask(
                final NonBlockingLockManagerWithNewDesign<R> lockService,
                final R[] resource, final Callable<T> task) {

            super(task);

            if (lockService == null)
                throw new IllegalArgumentException();

            this.lockService = lockService;

            this.resource = resource;

            this.callersTask = task;

            this.acceptTime = System.nanoTime();

        }

        private LockFutureTask(
                final NonBlockingLockManagerWithNewDesign<R> lockService,
                final R[] resources, final Runnable task, final T val) {

            super(task, val);

            if (lockService == null)
                throw new IllegalArgumentException();
            
            this.lockService = lockService;

            this.resource = resources;
            
            this.callersTask = task;
        
            this.acceptTime = System.nanoTime();

        }
        
        /**
         * Accept the task for eventual execution.
         */
        protected LockFutureTask<R, T> acceptTask() {

            if (!lockService.lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();

            switch (lockService.serviceRunState) {
            case Shutdown:
                /*
                 * Any task that has been accepted will be eventually run.
                 */
                // fall through.
            case Running:
                /*
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
                 * Note: this case can arise if there is a deadlock (in which
                 * case the lock requests were already backed out) -or- if the
                 * maximum multi-programming capacity of the TxDag would be
                 * exceeded.
                 */
                setTaskRunState(TaskRunState.Retry);
                lockService.retryQueue.add(this); // Note: MUST NOT block!
                lockService.counters.nretry++; // #on the retry queue.
                return this;
            default:
                    throw new IllegalStateException(lockService.serviceRunState.toString());
            }
        }

        /**
         * Return <code>true</code> iff the task holds all its locks.
         */
        public boolean isLocksHeld() {
            
            lockService.lock.lock();
            
            try {
                
                return lockService.holdsAllLocks(this);
                
            } finally {
                
                lockService.lock.unlock();
                
            }
            
        }
        
        /**
         * Request any locks declared by the task. If the locks are available
         * then submit the task for execution immediately.
         * 
         * @return <code>true</code> if the task should be removed from
         *         {@link NonBlockingLockManagerWithNewDesign#retryQueue} by
         *         the caller.
         */
        private boolean requestLocks() {

            if (!lockService.lock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();
            
            if (isCancelled()) {

                // already cancelled, e.g., by the caller.
                return true;

            }

            if (lockService.serviceRunState.tasksCancelled()) {

                /*
                 * Tasks are cancelled in this run state. mayInterruptIfRunning
                 * is true so the interrupt can be propagated if necessary even
                 * though the task is not running.
                 */

                cancel(true/* mayInterruptIfRunning */);

                return true;

            }

            int nvertices = -1;
            try {

                // #of vertices before we request the locks.
                if (lockService.waitsFor != null)
                    nvertices = lockService.waitsFor.size();
                
                if (lockService.waitsFor != null && lockService.waitsFor.isFull()) {
                
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

                if (lockService.waitsFor != null) {

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
                    lockService.waitsFor.lookup(this, true/* insert */);
                    
                }

                /*
                 * Post the lock requests.
                 * 
                 * Note: This method has no side-effects if it fails.
                 */
                if(lockService.postLockRequests(this)) {
                    // run now.
                    if (INFO)
                        log.info("Task is ready to run: " + this);
                    setTaskRunState(TaskRunState.LocksReady);
                    lockService.counters.nready++;
                    lockService.ready(this);
                } else {
                    // must wait for locks.
                    setTaskRunState(TaskRunState.LocksRequested);
                    lockService.counters.nwaiting++;
                }
                
                // either way, the task was handled.
                return true;

            } catch (Throwable t2) {

                if (t2 instanceof DeadlockException) {

                    /*
                     * Note: DeadlockException is the ONLY expected exception
                     * when we issue the lock requests, and then IFF TxDag is in
                     * use. Anything else is an internal error.
                     */
                    
                    if (INFO)
                        log
                                .info("Deadlock: " + this + ", task=" + this /* , ex */);

                    if (++ntries < lockService.maxLockTries) {

                        // leave on queue to permit retry.

                        if(INFO)
                            log.info("Will retry task: " + this);

                        return false;

                    }
                    
                } else {
                    
                    // any other exception is an internal error.
                    log.error("Internal error: " + this, t2);
                    
                }

                // set exception on the task (clears locks)
                setException(t2);

                if (lockService.waitsFor != null) {
                    /*
                     * Paranoia check to make sure that we did not leave
                     * anything in the WAITS_FOR graph.
                     */
                    final int nafter = lockService.waitsFor.size();
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
         */
        @Override
        protected void setException(final Throwable t) {
            lockService.lock.lock();
            try {
                super.setException(t);
                if (taskRunState != TaskRunState.Halted) {
                    if (DEBUG)
                        log.debug("Exception: " + this + ", cause=" + t, t);
                    lockService.counters.nerror++;
                    if (taskRunState.isRunning()) {
                        lockService.counters.nrunning--;
                    }
                    setTaskRunState(TaskRunState.Halted);
                }
            } finally {
                lockService.lock.unlock();
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
            lockService.lock.lock();
            try {
                final boolean ret = super.cancel(mayInterruptIfRunning);
                if (taskRunState != TaskRunState.Halted) {
                    if (DEBUG)
                        log.debug("Cancelled: " + this);
                    lockService.counters.ncancel++;
                    if (taskRunState.isRunning()) {
                        lockService.counters.nrunning--;
                    }
                    setTaskRunState(TaskRunState.Halted);
                }
                return ret;
            } finally {
                lockService.lock.unlock();
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
            if (callersTask instanceof AbstractTask
                    && ((AbstractTask) callersTask).getTaskCounters() instanceof WriteTaskCounters) {

                final long lockWaitingTime = System.nanoTime() - acceptTime;

                ((WriteTaskCounters) ((AbstractTask) callersTask)
                        .getTaskCounters()).lockWaitingNanoTime
                        .addAndGet(lockWaitingTime);

            }

            lockService.lock.lock();
            try {
                if (taskRunState == TaskRunState.LocksReady) {
                    /*
                     * Note: run() can be invoked on tasks which have been
                     * cancelled so we don't want to do this for those tasks.
                     */
                    setTaskRunState(TaskRunState.RunningWithLocks);
                    lockService.counters.nready--;
                    lockService.counters.nrunning++;
                    lockService.counters.nrunningWithLocksHeld++;
                    if (lockService.counters.nrunning > lockService.counters.maxRunning) {
                        lockService.counters.maxRunning = lockService.counters.nrunning;
                    }
                }
            } finally {
                lockService.lock.unlock();
            }

            if (DEBUG)
                log.debug("Running: " + this);
            
            try {
                super.run();
            } finally {
                /*
                 * Note: FutureTask asynchronously reports the result back to
                 * get() when super.run() completes. Therefore the locks and the
                 * run state MIGHT NOT have been updated before the Future#get()
                 * returns to the caller (in fact, this is quite common).
                 * 
                 * @todo I have tried overriding both set(T) and done(), but
                 * neither appears to be invoked synchronously on this class
                 * during super.run(). This is especially difficult to
                 * understand for set(T). I think that it delegates to an inner
                 * class and when the inner run() exits the outer set(T) does
                 * not get invoked.  Perhaps post this as an issue?
                 */
                lockService.lock.lock();
                try {
                    if (taskRunState.isRunning()) {
                        if (DEBUG)
                            log.debug("Did run: " + this);
                        lockService.counters.nrunning--;
                        setTaskRunState(TaskRunState.Halted);
                    }
                } finally {
                    lockService.lock.unlock();
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
     * {@link Runnable} drains the {@link #retryQueue} queue and manages
     * service state changes.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class AcceptTask<R extends Comparable<R>> implements
            Runnable {

        private final NonBlockingLockManagerWithNewDesign<R> lockManager;
        
        public AcceptTask(final NonBlockingLockManagerWithNewDesign<R> lockManager) {

            this.lockManager = lockManager;
            
        }
        
        public void run() {
            while (true) {
                switch (lockManager.serviceRunState) {
                case Starting: {
                    awaitStateChange(Starting);
                    continue;
                }
                case Running: {
                    lockManager.lock.lock();
                    try {
                        while (processRetryQueue()) {
                            // do work
                        }
                        awaitStateChange(Running);
                    } finally {
                        lockManager.lock.unlock();
                    }
                    continue;
                }
                case Shutdown: {
                    lockManager.lock.lock();
                    try {
                        while (processRetryQueue()) {
                            /*
                             * Do work.
                             * 
                             * Note: will run anything already accepted. That is
                             * intentional. Once the lock manager is shutdown it will no
                             * longer accept tasks, but it will process those tasks
                             * which it has already accepted.
                             */
                        }
                        if (lockManager.retryQueue.isEmpty()) {
                            /*
                             * There is no more work to be performed so we can
                             * change the runState.
                             */
                            if(INFO)
                                log.info("No more work.");
                            if (lockManager.serviceRunState.val < ServiceRunState.ShutdownNow.val) {
                                lockManager.setServiceRunState(ServiceRunState.ShutdownNow);
                                break;
                            }
                        }
                        awaitStateChange(Shutdown);
                    } finally {
                        lockManager.lock.unlock();
                    }
                    continue;
                }
                case ShutdownNow: {
                    /*
                     * Cancel all tasks, clearing all queues.
                     * 
                     * Note that even tasks on the retryQueue need to be
                     * interrupted so the interrupt can be propagated out of the
                     * LockFutureTask.
                     */
                    lockManager.lock.lock();
                    try {
                        if (INFO)
                            log.info(lockManager.serviceRunState);
                        // clear retry queue (tasks will not be run).
                        cancelTasks(lockManager.retryQueue.iterator(), true/* mayInterruptIfRunning */);
                        // nothing on the retry queue.
                        lockManager.counters.nretry = 0;
                        // change the run state?
                        if (lockManager.serviceRunState.val < ServiceRunState.Halted.val) {
                            lockManager.setServiceRunState(ServiceRunState.Halted);
                            if (INFO)
                                log.info(lockManager.serviceRunState);
                        }
                        // Done.
                        awaitStateChange(ShutdownNow);
                    } finally {
                        lockManager.lock.unlock();
                    }
                }
                case Halted: {
                    if (INFO)
                        log.info(lockManager.serviceRunState);
                    // stop the service running for this task.
                    lockManager.service.shutdown () ;
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
            lockManager.lock.lock();
            try {
                /*
                 * While we hold the lock we verify that there really is no work
                 * to be done and that we are in the expected run state. Then
                 * and only then do we wait on [stateChanged].
                 */
                if (lockManager.serviceRunState != expected) {
                    // In a different run state.
                    return;
                }
                if (!lockManager.retryQueue.isEmpty()) {
                    // Some work can be done.
                    return;
                }
                if (INFO)
                    log.info("Waiting...");
                lockManager.stateChanged.await();
                if (INFO)
                    log.info("Woke up...");
            } catch (InterruptedException ex) {
                // someone woke us up.
            } finally {
                lockManager.lock.unlock();
            }
        }
        
        /**
         * Cancel all tasks and remove them from the queue.
         * 
         * @param tasks
         *            The tasks.
         */
        private void cancelTasks(
                final Iterator<LockFutureTask<R, ? extends Object>> itr,
                final boolean mayInterruptIfRunning) {

            while (itr.hasNext()) {

                final LockFutureTask<R, ? extends Object> t = itr.next();

                t.cancel(mayInterruptIfRunning);

                itr.remove();

            }

        }

        /**
         * Processes tasks on the retry queue.
         * 
         * @return <code>true</code> iff any tasks were removed from this
         *         queue.
         */
        private boolean processRetryQueue() {

            if (lockManager.waitsFor != null && lockManager.waitsFor.isFull()) {

                // Nothing to do until some tasks releases its locks.
                return false;
                
            }
            
            int nchanged = 0;

            final Iterator<LockFutureTask<R, ? extends Object>> itr = lockManager.retryQueue
                    .iterator();

            while (itr.hasNext()) {

                final LockFutureTask<R, ? extends Object> t = itr.next();

                if (t.requestLocks()) {
                    /*
                     * Note: a [true] return means that we will remove the task
                     * from the [retryQueue]. It does NOT mean that the task was
                     * granted its locks.
                     */
                    itr.remove();
                    lockManager.counters.nretry--; // was removed from retryQueue.
                    nchanged++;
                    continue;
                }
                /*
                 * Note: Merely stopping as soon as we find a task that can not
                 * run provides a HUGE (order of magnitude) improvement in
                 * throughput when retries are common.
                 * 
                 * @todo review this choice when real deadlocks are occurring
                 * rather than just running into the multi-programming capacity
                 * limit on TxDag.
                 */
                break;

            }

            if (INFO && nchanged > 0)
                log.info("#nchanged=" + nchanged);
            
            return nchanged > 0;
            
        }

    } // AcceptTask

    /**
     * Add if absent and return a {@link ResourceQueue} for the named resource.
     * 
     * @param resource
     *            The resource.
     * 
     * @return The {@link ResourceQueue}.
     */
    private ResourceQueue<R, LockFutureTask<R, ? extends Object>> declareResource(
            final R resource) {

        // test 1st to avoid creating a new ResourceQueue if it already exists.
        ResourceQueue<R, LockFutureTask<R, ? extends Object>> resourceQueue = resourceQueues
                .get(resource);

        if (resourceQueue != null) {

            // already exists.
            return resourceQueue;

        }

        // not found, so create a new ResourceQueue for that resource.
        resourceQueue = new ResourceQueue<R, LockFutureTask<R, ? extends Object>>(
                this, resource);

        // put if absent.
        final ResourceQueue<R, LockFutureTask<R, ? extends Object>> oldval = resourceQueues
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
    private <T> boolean postLockRequests(final LockFutureTask<R,T> task)
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
            final LinkedHashSet<LockFutureTask<R,? extends Object>> predecessors = new LinkedHashSet<LockFutureTask<R,? extends Object>>();
            for (R r : (R[]) task.resource) {
            
                // make sure queue exists for this resource.
                final ResourceQueue<R,LockFutureTask<R,? extends Object>> resourceQueue = declareResource(r);

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
                final ResourceQueue<R, LockFutureTask<R, ? extends Object>> resourceQueue = declareResource(r);

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
                final ResourceQueue<R, LockFutureTask<R, ? extends Object>> resourceQueue = declareResource(r);
                
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
    private <T> void releaseLocksForTask(final LockFutureTask<R,T> t,
            final boolean waiting) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if (DEBUG)
            log.debug("Releasing locks: " + t);

        /*
         * The set of resource queues for which this task was holding a lock (at
         * the head of the queue).
         */
        final List<ResourceQueue<R, LockFutureTask<R, ? extends Object>>> resourceQueues = new LinkedList<ResourceQueue<R, LockFutureTask<R, ? extends Object>>>();

        try {

            final Iterator<ResourceQueue<R, LockFutureTask<R, ? extends Object>>> itr = t.lockedResources
                    .iterator();

            while (itr.hasNext()) {

                final ResourceQueue<R, LockFutureTask<R, ? extends Object>> resourceQueue = itr
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
         
            final Iterator<ResourceQueue<R, LockFutureTask<R, ? extends Object>>> itr = resourceQueues
                    .iterator();

            while (itr.hasNext()) {

                final ResourceQueue<R, LockFutureTask<R, ? extends Object>> resourceQueue = itr
                        .next();

                final LockFutureTask<R, ? extends Object> task = resourceQueue.queue
                        .peek();

                if (task != null
                        && task.taskRunState == TaskRunState.LocksRequested
                        && holdsAllLocks(task)) {

                    if (INFO)
                        log.info("Task is ready to run: " + task);
                    
                    // add to queue of tasks ready to execute.
                    task.setTaskRunState(TaskRunState.LocksReady);
                    counters.nwaiting--;
                    counters.nready++;
                    try {
                        ready(task);
                    } catch (Throwable t2) {
                        /*
                         * Note: If the implementation of ready(task) submits
                         * the task to an Executor, that Executor can throw a
                         * RejectedExecutionException here (e.g., because it was
                         * shutdown). We set the exception (whatever it is) on
                         * the task, which will cause its run state to shift to
                         * Halted.
                         */
                        task.setException(t2);
                    }

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
     *            
     * @return <code>true</code> if the lock was held by that task.
     */
    public boolean isLockHeldByTask(final R lock, final Runnable task) {

        final ResourceQueue<R, LockFutureTask<R, ? extends Object>> resourceQueue = resourceQueues
                .get(lock);

        if (resourceQueue.queue.peek() == task) {

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
    private boolean holdsAllLocks(final LockFutureTask<R, ? extends Object> task) {

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        for (R r : (R[]) task.resource) {

            final ResourceQueue<R, LockFutureTask<R, ? extends Object>> resourceQueue = resourceQueues
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
                ", #error=" + counters.nerror + //
                ", #cancel=" + counters.ncancel + //
                ", averageDeadlock=" + ((int)(10*statisticsTask.nretryAverageTask.getMovingAverage())/10d)+ //
                ", averageWaiting=" + ((int)(10*statisticsTask.nwaitingAverageTask.getMovingAverage())/10d)+ //
                ", averageReady=" + ((int)(10*statisticsTask.nreadyAverageTask.getMovingAverage())/10d)+ //
                ", averageRunning=" + ((int)(10*statisticsTask.nrunningAverageTask.getMovingAverage())/10d)+ //
                ", averageRunningWithLocksHeld=" + ((int)(10*statisticsTask.nrunningWithLocksHeldAverageTask.getMovingAverage())/10d)+ //
                ", #maxrunning=" + counters.maxRunning + //
//                (waitsFor != null ? ", vertices=" + waitsFor.size() : "") + //
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
    static protected class ResourceQueue<R extends Comparable<R>, T extends LockFutureTask<R, ? extends Object>> {

        /**
         * The outer class.
         * <p>
         * Note: I ran into compilation errors on 64-bit systems when making
         * references to non-static inner classes so this is now an explicit
         * ctor parameter.
         */
        final private NonBlockingLockManagerWithNewDesign<R> lockService;
        
        /**
         * The resource whose access is controlled by this object.
         */
        final private R resource;

        /**
         * The queue of transactions seeking access to the {@link #resource}.
         * The object at the head of the queue is the transaction with the lock
         * on the resource.
         */
        final private BlockingQueue<T/* tx */> queue;

        /**
         * Used to track statistics for this queue while it exists.
         */
        final QueueSizeMovingAverageTask statisticsTask;
        
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
        public ResourceQueue(
                final NonBlockingLockManagerWithNewDesign<R> lockService,
                final R resource) {

            if (lockService == null)
                throw new IllegalArgumentException();

            if (resource == null)
                throw new IllegalArgumentException();

            this.lockService = lockService;

            this.resource = resource;

            this.queue = new LinkedBlockingQueue<T>(/* unbounded */);
            
            this.statisticsTask = new QueueSizeMovingAverageTask(resource.toString(), queue);
            
        }

    } // ResourceQueue
    
    /**
     * Class for tracking the average queue size of each {@link ResourceQueue}
     * and various other moving averages for the service as a whole.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see NonBlockingLockManagerWithNewDesign#statisticsTask
     */
    protected class StatisticsTask implements Runnable {
        
        final MovingAverageTask nretryAverageTask = new MovingAverageTask(
                "nretry", new Callable<Integer>() {
                    public Integer call() {
                        return counters.nretry;
                    }
                });
        
        final MovingAverageTask nwaitingAverageTask = new MovingAverageTask(
                "nwaiting", new Callable<Integer>() {
                    public Integer call() {
                        return counters.nwaiting;
                    }
                });
        
        final MovingAverageTask nreadyAverageTask = new MovingAverageTask(
                "nready", new Callable<Integer>() {
                    public Integer call() {
                        return counters.nready;
                    }
                });
        
        final MovingAverageTask nrunningAverageTask = new MovingAverageTask(
                "nrunning", new Callable<Integer>() {
                    public Integer call() {
                        return counters.nrunning;
                    }
                });

        final MovingAverageTask nrunningWithLocksHeldAverageTask = new MovingAverageTask(
                "nrunningWithLocksHeld", new Callable<Integer>() {
                    public Integer call() {
                        return counters.nrunningWithLocksHeld;
                    }
                });

        /**
         * Used to stuff the datum to be incorporated into the average into
         * {@link #nqueueBusyAverageTask}.  This is set by {@link #run()}
         */
        final private AtomicInteger queueCountWithNonZeroTasks = new AtomicInteger();

        /**
         * #of queues with at least one task.
         */
        final MovingAverageTask nqueueBusyAverageTask = new MovingAverageTask(
                "nbusy", new Callable<Integer>() {
                    public Integer call() {
                        return queueCountWithNonZeroTasks.get();
                    }
                });

        private StatisticsTask() {

        }

        /**
         * This method updates the {@link QueueSizeMovingAverageTask} for each
         * {@link ResourceQueue} each time it is run.
         * <p>
         * Note: This is written so as to not cause hard references to be
         * retained to the {@link ResourceQueue}s. The
         * {@link QueueSizeMovingAverageTask} is a member field for the
         * {@link ResourceQueue} for the same reason. This way the statistics
         * for active {@link ResourceQueue}s are tracked and may be reported
         * but {@link ResourceQueue}s will remain strongly reachable only if
         * there are tasks holding their locks.
         */
        public void run() {

            nretryAverageTask.run();
            
            nwaitingAverageTask.run();
            
            nreadyAverageTask.run();
            
            nrunningAverageTask.run();
            
            nrunningWithLocksHeldAverageTask.run();
            
            int nbusy = 0;
            
            final Iterator<Map.Entry<R, WeakReference<ResourceQueue<R, LockFutureTask<R, ? extends Object>>>>> itr = resourceQueues
                    .entryIterator();

            while (itr.hasNext()) {

                final Map.Entry<R, WeakReference<ResourceQueue<R, LockFutureTask<R, ? extends Object>>>> entry = itr
                        .next();

                final WeakReference<ResourceQueue<R, LockFutureTask<R, ? extends Object>>> queueRef = entry
                        .getValue();

                final ResourceQueue<R, LockFutureTask<R, ? extends Object>> queue = queueRef
                        .get();

                if (queue == null)
                    continue;
                
                queue.statisticsTask.run();
                
                final int size = queue.queue.size();

                if (size == 1) {
                
                    nbusy++;
                    
                }
                
            }
            
            this.queueCountWithNonZeroTasks.set(nbusy);

            nqueueBusyAverageTask.run();

        }

    }

}
