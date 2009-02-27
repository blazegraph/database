package com.bigdata.util.concurrent;

import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.IDataService;

/**
 * Class captures various data about the execution of {@link AbstractTask}s.
 * These data are collected by the {@link ConcurrencyManager} in groups the
 * different services on which the tasks are run.
 * <p>
 * Note: The field names here are consistent with those in the
 * {@link ThreadPoolExecutorStatisticsTask}.
 * <p>
 * Note: The various counters are {@link AtomicLong}s since we need them to be
 * thread-safe. (<code>counter++</code> and <code>counter+=foo</code> are
 * NOT guarenteed to be thread-safe for simple fields or even
 * <code>volatile</code> fields).
 * 
 * @todo add counters for commit waiting time, task checkpoint time (as part of
 *       task service time), and commit service time and report out those data
 *       as moving averages.
 * 
 * @todo for the client add counters for retry's as follow ups to a
 *       {@link StaleLocatorException}.
 * 
 * @see ThreadPoolExecutorStatisticsTask
 */
public class TaskCounters {

    public String toString() {
        return getClass().getSimpleName()+//
        "{#submit="+taskSubmitCount+//
        ",#complete="+taskCompleteCount+//
        ",#fail="+taskFailCount+//
        ",#success="+taskSuccessCount+//
        ",#queueWaitingTime="+queueWaitingNanoTime+//
        ",#lockWaitingTime="+lockWaitingNanoTime+//
        ",#serviceTime="+serviceNanoTime+//
        ",#commitWaitingTime="+commitWaitingNanoTime+//
        ",#commitServiceTime="+commitServiceNanoTime+//
        ",#queuingTime="+queuingNanoTime+//
        "}"
        ;
    }
    
    /** #of tasks that have been submitted. */
    final public AtomicLong taskSubmitCount = new AtomicLong();
    
    /** #of tasks that have been completed. */
    final public AtomicLong taskCompleteCount = new AtomicLong();

    /** #of tasks that failed. */
    final public AtomicLong taskFailCount = new AtomicLong();

    /** #of tasks that succeeded. */
    final public AtomicLong taskSuccessCount = new AtomicLong();

    /**
     * Cumulative elapsed time in nanoseconds waiting on the queue pending
     * service.
     */
    final public AtomicLong queueWaitingNanoTime = new AtomicLong();
    
    /**
     * Cumulative elapsed time in nanoseconds consumed by tasks while waiting
     * for an resource lock.
     * <p>
     * Note: this value will only be non-zero for {@link ITx#UNISOLATED} tasks
     * since they are the only tasks that wait for locks and then only when
     * measuring the times on the {@link IDataService} rather than the client's
     * thread pool.
     */
    final public AtomicLong lockWaitingNanoTime = new AtomicLong();

    /**
     * Cumulative elapsed time in nanoseconds consumed by tasks while assigned
     * to a worker thread.
     * <p>
     * Note: Since this is aggregated over concurrent tasks the reported elapsed
     * time MAY exceed the actual elapsed time during which those tasks were
     * executed.
     */
    final public AtomicLong serviceNanoTime = new AtomicLong();

    /**
     * Cumulative elapsed time in nanoseconds consumed by tasks from when they
     * are submitted until they are complete.
     */
    final public AtomicLong queuingNanoTime = new AtomicLong();
    
    /**
     * Cumulative elapsed time in nanoseconds consumed by tasks awaiting group
     * commit (iff the task is run on the {@link WriteExecutorService}).
     */
    final public AtomicLong commitWaitingNanoTime = new AtomicLong();
    
    /**
     * Cumulative elapsed time in nanoseconds servicing group commit (iff the
     * task is run on the {@link WriteExecutorService}).
     */
    final public AtomicLong commitServiceNanoTime = new AtomicLong();
    
    /** Ctor */
    public TaskCounters() {

    }

    /**
     * Adds counters to this set.
     */
    public void add(TaskCounters c) {

        taskSubmitCount.addAndGet( c.taskSubmitCount.get() );

        taskCompleteCount.addAndGet( c.taskCompleteCount.get() );

        taskFailCount.addAndGet( c.taskFailCount.get() );

        taskSuccessCount.addAndGet( c.taskSuccessCount.get() );

        queueWaitingNanoTime.addAndGet( c.queueWaitingNanoTime.get() );

        lockWaitingNanoTime.addAndGet( c.lockWaitingNanoTime.get() );
        
        serviceNanoTime.addAndGet( c.serviceNanoTime.get() );

        queuingNanoTime.addAndGet( c.queuingNanoTime.get() );

    }

}
