package com.bigdata.journal;

import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.IDataService;

/**
 * Class captures various data about the execution of {@link AbstractTask}s.
 * These data are collected by the {@link ConcurrencyManager} in groups the
 * different services on which the tasks are run.
 * <p>
 * Note: The field names here are consistent with those in the
 * {@link QueueStatisticsTask}.
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
 * @see QueueStatisticsTask
 */
public class TaskCounters {

    /** #of tasks that have been submitted. */
    public AtomicLong taskSubmitCount = new AtomicLong();
    
    /** #of tasks that have been completed. */
    public AtomicLong taskCompleteCount = new AtomicLong();

    /** #of tasks that failed. */
    public AtomicLong taskFailCount = new AtomicLong();

    /** #of tasks that succeeded. */
    public AtomicLong taskSuccessCount = new AtomicLong();

    /**
     * Cumulative elapsed time waiting on the queue pending service.
     */
    public AtomicLong queueWaitingTime = new AtomicLong();
    
    /**
     * Cumulative elapsed time consumed by tasks while waiting for an resource
     * lock.
     * <p>
     * Note: this value will only be non-zero for {@link ITx#UNISOLATED} tasks
     * since they are the only tasks that wait for locks and then only when
     * measuring the times on the {@link IDataService} rather than the client's
     * thread pool.
     */
    public AtomicLong lockWaitingTime = new AtomicLong();

    /**
     * Cumulative elapsed time consumed by tasks while assigned to a worker
     * thread.
     * <p>
     * Note: Since this is aggregated over concurrent tasks the reported elapsed
     * time MAY exceed the actual elapsed time during which those tasks were
     * executed.
     */
    public AtomicLong serviceNanoTime = new AtomicLong();

    /**
     * Cumulative elapsed time consumed by tasks from when they are submitted
     * until they are complete.
     */
    public AtomicLong queuingNanoTime = new AtomicLong();
    
    /**
     * Cumulative elapsed time consumed by tasks awaiting group commit (iff the
     * task is run on the {@link WriteExecutorService}).
     */
    public AtomicLong commitWaitingTime = new AtomicLong();
    
    /**
     * Cumulative elapsed time servicing group commit (iff the task is run on
     * the {@link WriteExecutorService}).
     */
    public AtomicLong commitServiceTime = new AtomicLong();
    
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

        queueWaitingTime.addAndGet( c.queueWaitingTime.get() );

        lockWaitingTime.addAndGet( c.lockWaitingTime.get() );
        
        serviceNanoTime.addAndGet( c.serviceNanoTime.get() );

        queuingNanoTime.addAndGet( c.queuingNanoTime.get() );

    }

}
