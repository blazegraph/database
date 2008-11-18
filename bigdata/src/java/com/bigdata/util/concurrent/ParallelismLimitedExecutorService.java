/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Oct 29, 2008
 */

package com.bigdata.util.concurrent;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.journal.CompactTask;
import com.bigdata.relation.rule.eval.pipeline.JoinMasterTask;
import com.bigdata.service.DataService;

/**
 * A lightweight class that may be used to impose a parallelism limit on tasks
 * submitted to a delegate {@link Executor}. Typically, the delegate will be an
 * {@link ExecutorService} that is shared among multiple facets of the
 * application and which does not impose an inherent parallelism limit. This
 * class may be useful when you want to limit the #of concurrent tasks submitted
 * by some aspect of the application to a common {@link Executor}. For example,
 * this class can be used to realize parallelism limits designs such as
 * map/reduce, concurrent data load, JOIN algorithms, etc.
 * <p>
 * Note: Some effort has been made to define methods parallel to those declared
 * by {@link ThreadPoolExecutor}, including {@link #getQueue()},
 * {@link #getActiveCount()}, {@link #getSuccessCount()}, etc. In addition,
 * {@link #getMaxActiveCount()} reports the maximum observed parallelism for
 * this service.
 * <p>
 * Note: This class is designed to delegate the actual execute of tasks to an
 * {@link Executor}, which is often an {@link ExecutorService}. As such the
 * {@link #shutdown()} and related methods pertain only to an instance of
 * <em>this</em> class and the tasks submitted by that instance for execution
 * by the delegate. In particular, {@link #shutdown()} and
 * {@link #shutdownNow()} DO NOT shutdown the delegate {@link Executor}.
 * <p>
 * Note: The advantage of this class over a {@link ThreadPoolExecutor} with a
 * maximum pool size are two fold. First, you can have many instances of this
 * class which each target the same {@link ThreadPoolExecutor}. Second, while
 * you can layer one {@link Executor} over another, this design would require
 * twice as many threads to accomplish the same limit. In contrast, this class
 * requires a single {@link Thread} per instance (for its inner
 * {@link SubmitTask}) and all worker {@link Thread}s are hosted by the
 * delegate.
 * <p>
 * Note: Since {@link AbstractExecutorService} does not provide a factory for
 * the {@link FutureTask}s which it creates we have to override most of the
 * methods on that class, which is a bit of a pain.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME There are incompatable API changes for {@link ExecutorService} as of
 * java 6. In order to work for both Java 5 and java 6 we need to write
 * {@link #invokeAll(Collection)} and friends using the raw type (no generics).
 * However, changes to {@link AbstractExecutorService} (factory methods that can
 * be override in the subclass) and {@link RunnableFuture} (nice for proxying)
 * mean that we will probably lock in on Java 6 in any case. This means that
 * code which relies on the {@link ParallelismLimitedExecutorService} will not
 * compile/execute under java 5 going foward. Currently, this is restricted to
 * the {@link CompactTask} and the {@link JoinMasterTask} so there is really no
 * hinderance for people stuck in Java 5 -- for now.
 * 
 * @see http://osdir.com/ml/java.jsr.166-concurrency/2007-06/msg00030.html
 * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6267833
 * 
 * @todo Refactor or write a variant that is capable of single-threaded
 *       execution when the executor is a remote service, e.g., a
 *       {@link DataService}. For this context, something that plays the same
 *       role as {@link FutureTask} needs to be proxied and sent to the
 *       {@link DataService} (or we need to unbundle the task and its future and
 *       send both, which is essentially what {@link FutureTask} is doing
 *       anyway). This means that the client exports the {@link Future} and that
 *       the {@link DataService} will notify the client when the {@link Future}
 *       is complete and use the proxy to set the result on the future as well.
 *       This means that the Future on the client can signal the
 *       {@link SubmitTask} when its future is ready, perhaps placing its
 *       {@link Future} onto a queue of futures that have been completed. This
 *       design will allow the {@link ParallelismLimitedExecutorService} to be
 *       used for map/reduce style processing. A client should be able to manage
 *       1000s of concurrent tasks without trouble.
 * 
 * FIXME The ParallelismLimitedExecutorService is broken (11/10/08). This can be
 * demonstrated if it is enabled for the pipeline join. Therefore it has been
 * taken out of service until it can be fixed. Users include the
 * {@link CompactTask} and the {@link JoinMasterTask}. Prospective users
 * include the ConcurrentDataLoader (RDF DB) and map/reduce processing.
 */
public class ParallelismLimitedExecutorService extends AbstractExecutorService {

    protected static final Logger log = Logger.getLogger(ParallelismLimitedExecutorService.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * Used to assign JVM local unique {@link #id}s. The only purpose of the
     * #id is to show up in logging messages.
     */
    private static final AtomicLong idfactory = new AtomicLong(0);
    
    /**
     * Set from the {@link #idfactory}.
     */
    private final long id;
    
    /**
     * The service that is used to execute the submitted tasks.
     */
    private final Executor executor;
    
    /**
     * The maximum task parallelism that may be imposed on the {@link #executor}
     * by this instance.
     */
    private final int maxParallel;
    
    /**
     * A {@link BlockingQueue} of tasks awaiting executing by the
     * {@link #executor}.
     */
    private final BlockingQueue<Runnable> workQueue;

    /**
     * The queue of tasks submitted to this service that are pending submission
     * to the delegate {@link Executor}.
     */
    public BlockingQueue<Runnable> getQueue() {
        
        return workQueue;
        
    }
    
    /**
     * The tasks that are currently running.
     */
    private final QueueingFuture[] running;

    /**
     * add() overwrites the first <code>null</code> entry in the
     * {@link #running} table.
     * 
     * @param f
     *            The future.
     */
    private final void addRunning(final QueueingFuture f) {
        
        if (f == null)
            throw new IllegalArgumentException();

        synchronized (running) {

            for (int i = 0; i < running.length; i++) {

                if (running[i] == null) {

                    running[i] = f;

                    // #of tasks executing concurrently.
                    activeCount++;
                    
                    if (activeCount > maxActiveCount) {
                       
                        // update the maximum observed parallelism.
                        maxActiveCount = activeCount;
                        
                    }
                    
                    if (DEBUG)
                        log.debug("add   : " + dumpRunning().toString() + ", taskCount="
                                + activeCount);
                    
                    return;

                }

            }

            log.error(dumpRunning().toString());
            
            throw new AssertionError("No room in running table?");

        }

    }

    /**
     * Scans the {@link #running} table and clears the first slot having the
     * same reference.
     * 
     * @param f
     *            The future.
     */
    private final void removeRunning(final QueueingFuture f) {

        if (f == null)
            throw new IllegalArgumentException();

        synchronized (running) {

            for (int i = 0; i < running.length; i++) {

                if (running[i] == f) {
                
                    running[i] = null;
                    
                    /*
                     * Decrement the counter tracking the #of tasks executing
                     * concurrently.
                     */

                    activeCount--;

                    if (DEBUG)
                        log.debug("remove: " + dumpRunning().toString()
                                + ", taskCount=" + activeCount);

                    return;
                    
                }
                
            }
            
            log.error(dumpRunning().toString());
            
            throw new AssertionError("Not found in running table?");
            
        }
        
    }
    
    /**
     * Formats the table as a string with spaces for empty slots and markers for
     * filled slots.
     * 
     * @return
     */
    private final StringBuilder dumpRunning() {

        final StringBuilder sb = new StringBuilder(running.length + 20);

        sb.append("table=[");
        
        for (int i = 0; i < running.length; i++) {
        
            sb.append(running[i] == null ? ' ' : '|');
            
        }
        
        sb.append("]");
        
        return sb;

    }
    
    /**
     * The maximum parallelism with which this object will drive the delegate
     * {@link Executor} (this is the value specified to the ctor).
     */
    final public int getMaxParallel() {
        
        return maxParallel;
        
    }

    /**
     * {@link Lock} used to coordinate various things.
     */
    private final ReentrantLock lock;
    
    /**
     * Used by {@link SubmitTask} to wait until the #of executing tasks falls
     * below the parallelism limit. Signalled each time a task submitted to the
     * delegate {@link #executor} completes.
     */
    private final Condition taskDone; 
    
    /**
     * Used by {@link #awaitTermination(long, TimeUnit)}. Signalled by
     * {@link SubmitTask#run()} once the {@link #runState} is such that no more
     * tasks will be executed and there are no more tasks running.
     */
    private final Condition terminated;
    
    /**
     * Run state.
     */
    private volatile int runState = RUNNING;

    /**
     * Paranoid gateway for state change.
     * 
     * @param newState
     *            The new state.
     */
    private final void setRunState(int newState) {
        
        final int oldState = runState;
     
        assert oldState >= RUNNING && oldState <= TERMINATED : "oldState="
            + oldState;
        
        assert newState >= RUNNING && newState <= TERMINATED : "newState="
                + newState;

        assert oldState < newState;
        
//        assert lock.isHeldByCurrentThread();
        
        runState = newState;
        
        if (INFO)
            log.info("id="+id+", oldState=" + oldState + ", newState=" + runState);
        
    }
    
    /**
     * The initial mode on startup. In this mode the service will accept new
     * tasks and will execute tasks that are already in its {@link #workQueue}.
     */
    static private final int RUNNING = 0;

    /**
     * The mode for normal shutdown. In this mode the service WILL NOT accept
     * new tasks but it will continue to execute tasks that are already in its
     * {@link #workQueue}. This contraint is imposed by
     * {@link #execute(Runnable)}. Once all tasks in the {@link #workQueue}
     * have been submitted the {@link #runState} is changed to
     * {@link #TERMINATED} by {@link SubmitTask#run()}.
     */
    static private final int SHUTDOWN = 1;

    /**
     * The mode for immediate shutdown. In this mode the service WILL NOT accept
     * new tasks and it WILL NOT submit any tasks that are already its
     * {@link #workQueue} for execution by the delegate {@link #executor}. The
     * {@link #runState} will be changed to {@link #TERMINATED} by
     * {@link SubmitTask#run()} as soon as it notices the {@link #STOPPED}
     * runState.
     */
    static private final int STOPPED = 2;

    /**
     * Terminal state. In this mode the service WILL NOT accept new tasks and
     * all tasks which it has submitted have either completed successfully or
     * been cancelled.
     */
    static private final int TERMINATED = 3;

    public boolean isShutdown() {

        return runState != RUNNING;
        
    }

    public boolean isTerminating() {

        return runState == STOPPED;
        
    }

    public boolean isTerminated() {

        return runState == TERMINATED;
        
    }
    
    /**
     * Variant using a workQueue with a default capacity.
     * 
     * @param executor
     *            The {@link Executor} that will run the tasks.
     * @param maxParallel
     *            The maximum #of tasks submitted by this instance that may
     *            execute concurrently on that {@link Executor}.
     */
    public ParallelismLimitedExecutorService(final Executor executor,
            final int maxParallel) {
        
        this(executor, maxParallel, 16/* default */);
        
    }

    /**
     * Variant using a {@link ArrayBlockingQueue} with a caller specified
     * capacity.
     * 
     * @param executor
     *            The {@link Executor} that will run the tasks.
     * @param maxParallel
     *            The maximum #of tasks submitted by this instance that may
     *            execute concurrently on that {@link Executor}.
     * @param capacity
     *            The maximum #of tasks that may be queued by this instance
     *            awaiting execution by that {@link Executor}.
     */
    public ParallelismLimitedExecutorService(final Executor executor,
            final int maxParallel, final int capacity) {
        
        this(executor, maxParallel, new ArrayBlockingQueue<Runnable>(capacity));
        
    }
    
    /**
     * Core ctor.
     * 
     * @param executor
     *            The {@link Executor} that will run the tasks.
     * @param maxParallel
     *            The maximum #of tasks submitted by this instance that may
     *            execute concurrently on that {@link Executor}.
     * @param workQueue
     *            A queue that will be used to hold tasks which have been
     *            submitted to this instance but have not yet been submitted to
     *            that {@link Executor}.
     */
    public ParallelismLimitedExecutorService(final Executor executor,
            final int maxParallel, final BlockingQueue<Runnable> workQueue) {

        if (executor == null)
            throw new IllegalArgumentException();

        if (maxParallel <= 0)
            throw new IllegalArgumentException();
        
        if (workQueue == null)
            throw new IllegalArgumentException();
        
        this.id = idfactory.incrementAndGet();
        
        this.executor = executor;

        this.maxParallel = maxParallel;
        
        this.workQueue = workQueue;

        this.lock = new ReentrantLock();
        
        this.taskDone = lock.newCondition();

        this.terminated = lock.newCondition();
        
        this.running = new QueueingFuture[maxParallel];

        if (INFO)
            log.info("Starting: id=" + id);
        
        /*
         * Now that everything is initialized, start the task to drain the
         * workQueue.
         */
        
        executor.execute(new SubmitTask());
        
    }

    /**
     * Task this imposes the parallelism constraint. It waits until the #of
     * tasks which it has submitted and which are still executing falls below
     * the parallelism limit and then submits the next task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class SubmitTask implements Runnable {

        /**
         * Timeout limiting how long we will block on the workQueue or
         * underCapacity condition. This essentially controls how long it will
         * take this task to notice that the runState has changed when it would
         * otherwise be blocked.
         */
        final long timeout = TimeUnit.NANOSECONDS.convert(1L,
                TimeUnit.MILLISECONDS);
        
        public void run() {

            if (INFO)
                log.info("Started: id=" + id);
            
            try {

                /*
                 * Submit new tasks while until STOP or TERMINATED
                 */
                while (runState < STOPPED) {

                    if(DEBUG) 
                        log.debug("runState=" + runState + ", taskCount="
                                + activeCount + ", workQueue is empty");
                    
                    /*
                     * Poll w/ timeout if nothing available so that we will
                     * notice runState changes in a timely manner.
                     */

                    final Runnable task = workQueue.poll(timeout,
                            TimeUnit.NANOSECONDS);

                    if (task == null) {

                        // There is nothing available right now.
                        
                        if (runState == SHUTDOWN) {

                            /*
                             * There is nothing available and we have been
                             * instructed to shutdown the service. Therefore we
                             * change the runState to STOPPED in order to
                             * prevent any new tasks from executing (this breaks
                             * us out of the loop).
                             */
                            
                            if (INFO)
                                log.info("Service is shutdown and workQueue is empty.");
                            
                            setRunState(STOPPED);
                            
                        }

                        continue; // while(runState<STOPPED)

                    }

                    if(DEBUG)
                        log.debug("Awaiting lock");
                    
                    assert task != null;

                    lock.lock();

                    try {

                        while (runState < STOPPED && activeCount >= maxParallel) {

                            /*
                             * Await condition indicating that we are within the
                             * parallelism limit and may submit another task.
                             */

                            if (INFO)
                                log.info("Waiting to execute: runState="
                                        + runState + ", taskCount="
                                        + activeCount + ", task=" + task);

                            taskDone.await(timeout, TimeUnit.NANOSECONDS);

                        }

                        if (runState < STOPPED) {

                            /*
                             * We have a task and we are under capacity so
                             * submit that task to the delegate Executor.
                             */

                            if (INFO)
                                log.info("Executing: id=" + id + ", task="
                                        + task);

                            addRunning((QueueingFuture) task);

                            executor.execute(task);

                        } else {

                            /*
                             * Note: The task will not be submitted because of
                             * the runState.
                             */

                            if (INFO)
                                log.info("Will not execute: id=" + id
                                        + ", runState=" + runState
                                        + ", taskCount=" + activeCount
                                        + ", task=" + task);

                        }

                    } finally {

                        lock.unlock();

                    }

                } // while(runState<STOPPED)

            } catch (InterruptedException t) {

                // terminated by interrupt
                return;

            } catch (Throwable t) {

                log.error(t, t);

            } finally {

                if (DEBUG)
                    log.debug("Exit logic.");
                
                if (runState != TERMINATED) {

                    // wait for running tasks to complete.
                    terminate();
                    
                }

            }

        }
        
        /**
         * Service termination logic.
         * 
         * @throws InterruptedException
         */
        private void terminate() {

            if (INFO)
                log.info("Will terminate: runState=" + runState);

            lock.lock();

            try {

                // wait until no more tasks are running.
                while (activeCount > 0) {

                    if (INFO)
                        log.info("runState=" + runState + ", taskCount="
                                + activeCount);

                    try {

                        // wait until another task completes.
                        taskDone.await();

                    } catch (InterruptedException ex) {

                        log.warn("Interrupted awaiting termination: taskCount="
                                + activeCount);

                        /*
                         * Note: This continues to await the termination of the
                         * running tasks. Without this, awaitTermination() we
                         * would never terminated.signal() and
                         * awaitTermination() would not complete.
                         */

                        continue;

                    }

                }

                if (INFO)
                    log.info("No more tasks are running: "+this);

                setRunState(TERMINATED);

                /*
                 * Note: Only awaitTermination() uses this signal so we do not
                 * use signalAll().
                 */
                terminated.signal();

                if (INFO)
                    log.info("Did terminate: "+this);

            } finally {

                lock.unlock();

            }

        }
        
        /**
         * Delegates to the outer class.
         */
        public String toString() {
            
            return ParallelismLimitedExecutorService.this.toString();
            
        }

    }
    
    /**
     * #of tasks submitted by this instance that are executing concurrently
     * (fluctuates with the current actual parallelism).
     * <p>
     * This is updated by {@link #addRunning(QueueingFuture)} and decremented by
     * {@link #removeRunning(QueueingFuture)}. Anyone is allowed to peek, but
     * you can only trust the value if hold the {@link #lock} since that governs
     * access (indirectly) to those methods.
     */
    private volatile int activeCount = 0;
    
    /**
     * The maximum #of tasks submitted to this service that were executing
     * concurrently on the delegate {@link Executor} (strictly increasing).
     */
    private volatile int maxActiveCount = 0;

    /**
     * #of tasks which have been submitted for execution (strictly increasing).
     */
    private volatile long taskCount = 0L;

    /**
     * #of tasks which have completed execution regardless of their outcome
     * (strictly increasing).
     */
    private volatile long completedTaskCount = 0L;

    /**
     * #of submitted tasks which were cancelled (strictly increasing).
     */
    private volatile long cancelCount = 0L;

    /**
     * #of submitted tasks which resulted in an error (strictly increasing).
     */
    private volatile long errorCount = 0L;

    /**
     * #of submitted tasks which were completed successfully (strictly
     * increasing).
     */
    private volatile long successCount = 0L;
    
    /**
     * The approximate #of tasks submitting by this service that are
     * concurrently executing. This method is very fast and returns current
     * value of an internal counter.
     */
    public int getActiveCount() {
        
        return activeCount;
        
    }

    /**
     * The approximate maximum #of tasks submitted to this service that were
     * executing concurrently on the delegate {@link Executor} (that is, the
     * maximum observed parallelism of this service). This method is very fast
     * and returns current value of an internal counter.
     */
    public int getMaxActiveCount() {

        return maxActiveCount;
        
    }
    
    /**
     * The approximate #of tasks which have been submitted for execution by this
     * service. Such tasks may be the queue for this service, on the queue for
     * the delegate {@link Executor}, currently executing, or completed. This
     * method is very fast and returns current value of an monotonically
     * increasing internal counter.
     */
    public long getTaskCount() {
        
        return taskCount;
        
    }
    
    /**
     * The approximate #of tasks that have completed execution (regardless or
     * whether they were cancelled, resulted in an error, or executed
     * successfully). This method is very fast and returns current value of an
     * monotonically increasing internal counter.
     */
    public long getCompletedTaskCount() {
        
        return completedTaskCount;
        
    }
    
    /**
     * The approximate #of submitted tasks which were cancelled. This method is
     * very fast and returns current value of an monotonically increasing
     * internal counter.
     */
    public long getCancelCount() {

        return cancelCount;
        
    }

    /**
     * The approximate #of submitted tasks which resulted in an error (does not
     * include any tasks which have been cancelled). This method is very fast
     * and returns current value of an monotonically increasing internal
     * counter.
     */
    public long getErrorCount() {
        
        return errorCount;
        
    }

    /**
     * The approximate #of submitted tasks which were completed successfully.
     * This method is very fast and returns current value of an monotonically
     * increasing internal counter.
     */
    public long getSuccessCount() {
        
        return successCount;
        
    }

    /**
     * Returns an instantenous snapshot of the service counters. No lock is
     * obtained so the counters are NOT guarenteed to be mutually consistent. In
     * particular, errorCount+successCount+cancelCount is NOT guarenteed to be
     * equal to the completedTaskCount.
     */
    public String toString() {
        
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        
        sb.append("{ id=" + id);

        sb.append(", maxParallel=" + maxParallel);
        
        sb.append(", taskCount=" + taskCount);
        
        sb.append(", activeCount="+activeCount);
        
        sb.append(", maxActiveCount="+maxActiveCount);
        
        sb.append(", completedTaskCount="+completedTaskCount);
        
        sb.append(", cancelCount="+cancelCount);
        
        sb.append(", errorCount="+errorCount);
        
        sb.append(", successCount="+successCount);
        
        sb.append(", runState="+runState);
        
        sb.append("}");

        return sb.toString();
        
    }
    
    /**
     * Extends {@link FutureTask} to track the #of concurrently executing tasks
     * and also track the references for those tasks so that they may be
     * cancelled if the outer service is shutdown.
     */
    private class QueueingFuture<V> extends FutureTask<V> {

        private final Object task;
        
        QueueingFuture(Callable<V> c) {
            super(c);
            task = c;
        }

        QueueingFuture(Runnable t, V r) {
            super(t, r);
            task = r;
        }
        
//        public void run() {
//  
//            /*
//             * Note: We can not wait until the task is bound to a worker task
//             * and executing since that allows more than [maxParallel] tasks to
//             * exist on the delegate [executor] between its queue and its worker
//             * tasks.
//             */
//            
////            addRunning(this);
//            
//            super.run();
//            
//        }
        
        /**
         * Extended to decrement the #of concurrently running tasks. 
         */
        protected void done() {

            /*
             * Note: the entry must be removed from the table before
             * taskDone.signal() since otherwise it is possible for SubmitTask
             * to submit another task for execution before the entry has been
             * cleared from the table.
             */
            removeRunning(this);

            super.done();
            
            lock.lock();
            try {

                /*
                 * Track #of completed tasks, #of cancelled, errored, and
                 * successful tasks, etc.
                 */

                completedTaskCount++;
                
                if(isCancelled()) {

                    cancelCount++;

                    if(INFO)
                        log.info("cancelled: "+task);
                    
                } else {
                    
                    try {
                        get();
                        successCount++;
                        if(INFO)
                            log.info("succeeded: "+task);
                    } catch (InterruptedException e) {
                        throw new AssertionError(
                                "Should not wait for completed task");
                    } catch (ExecutionException e) {
                        if(INFO)
                            log.info("failed: "+task, e);
                        errorCount++;
                    }

                }

                /*
                 * Signal now that this task has completed.
                 * 
                 * Note: Only the SubmitTask await()s this signal so we do not
                 * use signalAll().
                 */
                    
                taskDone.signal();

            } finally {
                
                lock.unlock();
                
            }
            
        }

        public String toString() {
            
            if (INFO)
                return super.toString() + "{ task=" + task + "}";

            return super.toString();
            
        }
        
    }

    /**
     * Invoked if a command is rejected for execution.
     * 
     * @param command
     *            The command.
     */
    private void reject(Runnable command) {

        throw new RejectedExecutionException();
        
    }

    /**
     * Initiates an orderly shutdown in which previously submitted tasks are
     * executed, but no new tasks will be accepted. Invocation has no additional
     * effect if already invoked.
     */
    public void shutdown() {

        lock.lock();

        try {

            if (runState == RUNNING) {

                setRunState( SHUTDOWN );

            }

        } finally {

            lock.unlock();

        }

    }

    public List<Runnable> shutdownNow() {

        lock.lock();
        
        try {

            if (runState < STOPPED) {

                /*
                 * Prevent any tasks in the workQueue from executing.
                 */

                setRunState( STOPPED );
                
            }
            
            /*
             * Collect the pending tasks from the workQueue.
             */
            
            final LinkedList<Runnable> pending = new LinkedList<Runnable>();
            
            workQueue.drainTo(pending);

            /*
             * Interrupt any executing tasks that we submitted to the delegate
             * Executor.
             */
            {
             
                final QueueingFuture[] a = running.clone();

                for (QueueingFuture f : a) {

                    if (f != null && !f.isDone()) {

                        f.cancel(true/* mayInterruptIfRunning */);
                        
                    }

                }
                
            }

            // return the pending tasks.
            return pending;
            
        } finally {

            lock.unlock();
            
        }
                
    }

    /**
     * Makes the factory pattern easier.
     * 
     * @param futureTask
     *            A task to be executed.
     *            
     * @return The task.
     */
    final private <T> FutureTask<T> execute2(final FutureTask<T> futureTask) {
        
        /*
         * Note: lock gives us an atomic view of the runState.
         */
        lock.lock();

        try {

            if (runState != RUNNING) {

                reject(futureTask);

                return futureTask;

            }

            try {

                // put to the workQueue (may block).
                workQueue.put(futureTask);

                // #of tasks submitted for execution. 
                taskCount++;

            } catch (InterruptedException ex) {

                if (INFO)
                    log.info("Interrupted");

                reject(futureTask);
                
                return futureTask;

            }

        } finally {

            lock.unlock();
            
        }
        
        return futureTask;
        
    }

//    /**
//     * Returns a RunnableFuture for the given callable task.
//     * 
//     * @since 1.6
//     */
//    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
//
//        return super.newTaskFor(callable);
//        
//    }
//
//    /**
//     * Returns a RunnableFuture for the given runnable and default value.
//     * 
//     * @since 1.6
//     */
//    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
//
//        return super.newTaskFor(runnable, value);
//        
//    }

    /**
     * {@link FutureTask} factory.
     * 
     * @param task
     *            A task.
     * 
     * @return A {@link FutureTask} for that task.
     * 
     * 
     * FIXME replace in 1.6 with {@link #newTaskFor(Callable)}. Also includes
     * the {@link RunnableFuture} abstraction which will make life much easier.
     */
    protected <T> FutureTask<T> newFuture(final Callable<T> task) {
        
        if (task == null)
            throw new NullPointerException();
        
        return new QueueingFuture<T>(task);
        
    }
    
    /**
     * {@link FutureTask} factory.
     * 
     * @param task
     *            A task.
     * @param result
     *            The result for that tas.
     * 
     * @return A {@link FutureTask} for that task.
     * 
     * FIXME replace in 1.6 with {@link #newTaskFor(Runnable, Object)}.
     */
    protected <T> FutureTask<T> newFuture(Runnable task, T result) {

        if (task == null)
            throw new NullPointerException();
        
        if(INFO)
            log.info("task="+task);
        
        return new QueueingFuture<T>(task, result);
        
    }

    public void execute(final Runnable command) {
        
        if (command == null)
            throw new IllegalArgumentException();

        if(DEBUG)
            log.debug("task: "+command);
        
        execute2(newFuture(command, null));
        
    }

    @Override
    public Future<?> submit(final Runnable task) {

        if(DEBUG)
            log.debug("task: "+task);
        
        return execute2(newFuture(task, null));
        
    }
    
    @Override
    public <T> Future<T> submit(final Runnable task, final T result) {

        if(DEBUG)
            log.debug("task: "+task);
        
        return execute2(newFuture(task, result));
        
    }

    @Override
    public <T> Future<T> submit(final Callable<T> task) {
        
        if(DEBUG)
            log.debug("task: "+task);
        
        return execute2(newFuture(task));
        
    }

    /**
     * Await termination of this service (not the delegate service).
     */
    public boolean awaitTermination(final long timeout, final TimeUnit unit)
            throws InterruptedException {

        if (INFO)
            log.info("id="+id+", timeout=" + timeout + ", unit=" + unit);
        
        long remaining = unit.toNanos(timeout);

        lock.lock();

        try {

            while (runState < TERMINATED && remaining > 0L) {

                remaining = terminated.awaitNanos(remaining);

                if (DEBUG)
                    log.debug("id="+id+", runState=" + runState + ", taskCount="
                            + activeCount + ", remaining=" + remaining);
                
            }

            final boolean terminated = runState == TERMINATED;

            if (INFO)
                log.info("id="+id+", terminated="+terminated);
            
            return terminated;

        } finally {

            lock.unlock();

        }
        
    }
    
    /**
     * FIXME As of Java 1.6 the method signature is changed and you need to
     * modify the method signature here in the source code to use
     * 
     * <pre>
     * Collection&lt;? extends Callable&lt;T&gt;&gt; tasks
     * </pre>
     * 
     * rather than
     * 
     * <pre>
     * Collection &lt; Callable &lt; T &gt;&gt; tasks
     * </pre>
     * 
     * I love generics.
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6267833
     */
    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        
        final List<Future<T>> futures = new LinkedList<Future<T>>();

        boolean done = false;

        try {

            // submit all.
            
            for (Callable<T> task : tasks) {

                futures.add(submit(task));

            }

            // await all futures.
            
            for (Future<T> f : futures) {

                if (!f.isDone()) {

                    try {

                        f.get();

                    } catch (ExecutionException ex) {

                        // ignore.
                        
                    } catch (CancellationException ex) {

                        // ignore.

                    }

                }
                
            }

            done = true;
            
            return futures;
            
        } finally {
            
            if (!done) {

                // At least one future did not complete.
                
                for (Future<T> f : futures) {

                    if(!f.isDone()) {

                        f.cancel(true/* mayInterruptIfRunning */);
                        
                    }
                    
                }
                
            }
        
        }

    }

    /**
     * This is just a variant of the method above but we also have to count down
     * the nanos remaining and then cancel any tasks which are not yet complete
     * if there is a timeout.
     * 
     * FIXME As of Java 1.6 the method signature is changed and you need to
     * modify the method signature here in the source code to use
     * 
     * <pre>
     * Collection&lt;? extends Callable&lt;T&gt;&gt; tasks
     * </pre>
     * 
     * rather than
     * 
     * <pre>
     * Collection &lt; Callable &lt; T &gt;&gt; tasks
     * </pre>
     * 
     * I love generics.
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6267833
     */
    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks,
            final long timeout, final TimeUnit unit)
            throws InterruptedException {

        final long begin = System.nanoTime();
        
        long remaining = unit.toNanos(timeout);
        
        final List<Future<T>> futures = new LinkedList<Future<T>>();

        boolean done = false;

        try {

            // submit all, even before we consider the timeout.
            for (Callable<T> task : tasks) {

                futures.add(submit(task));
                
            }

            // await all futures, but no longer than the timeout.
            for (Future<T> f : futures) {

                remaining = System.nanoTime() - begin;
                
                if (remaining < 0) {

                    return cancelAll(futures);
                    
                }
                
                if (!f.isDone()) {

                    try {

                        f.get(remaining, TimeUnit.NANOSECONDS);

                    } catch (ExecutionException ex) {

                        // ignore.
                        
                    } catch (CancellationException ex) {

                        // ignore.

                    } catch(TimeoutException ex) {

                        return cancelAll(futures);
                        
                    }

                }
                
            }

            done = true;
            
            return futures;
            
        } finally {
            
            if (!done) {

                // At least one future did not complete.
                return cancelAll(futures);

            }

        }

    }

    private <T> List<Future<T>> cancelAll(final List<Future<T>> futures) {

        for (Future<T> f : futures) {

            if (!f.isDone()) {

                f.cancel(true/* mayInterruptIfRunning */);

            }

        }

        return futures;

    }

}
