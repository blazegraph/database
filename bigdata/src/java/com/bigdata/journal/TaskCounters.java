package com.bigdata.journal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Class captures various data about the execution of {@link AbstractTask}s.
 * These data are collected by the {@link ConcurrencyManager} in groups the
 * different services on which the tasks are run.
 * <p>
 * Note: The field names here are consistent with those in the
 * {@link QueueStatisticsTask}.
 * 
 * FIXME The various counters should be {@link AtomicLong}s if we want them to
 * be thread-safe. As it is <code>counter++</code> and
 * <code>counter+=foo</code> are NOT guarenteed to be thread-safe.
 * 
 * @see QueueStatisticsTask
 */
public class TaskCounters {

    /** #of tasks that have been submitted. */
    public long taskSubmitCount;
    
    /** #of tasks that have been completed. */
    public long taskCompleteCount;

    /** #of tasks that failed. */
    public long tailFailCount;

    /** #of tasks that succeeded. */
    public long taskSuccessCount;

    /**
     * Cumulative elapsed time waiting on the queue pending service.
     */
    public long queueWaitingTime;
    
    /**
     * Cumulative elapsed time consumed by tasks while waiting for an
     * resource lock.
     * <p>
     * Note: this value will only be non-zero for {@link ITx#UNISOLATED}
     * tasks since they are the only tasks that wait for locks.
     */
    public long lockWaitingTime;

    /**
     * Cumulative elapsed time consumed by tasks while assigned to a worker
     * thread.
     * <p>
     * Note: Since this is aggregated over concurrent tasks the reported
     * elapsed time MAY exceed the actual elapsed time during which those
     * tasks were executed.
     */
    public long serviceNanoTime;

    /**
     * Cumulative elapsed time consumed by tasks from when they are submitted
     * until they are complete.
     */
    public long queuingNanoTime;
    
    /** Ctor */
    public TaskCounters() {

    }

    /**
     * Adds counters to this set.
     */
    public void add(TaskCounters c) {

        taskSubmitCount += c.taskSubmitCount;

        taskCompleteCount += c.taskCompleteCount;

        tailFailCount += c.tailFailCount;

        taskSuccessCount += c.taskSuccessCount;

        queueWaitingTime += c.queueWaitingTime;

        lockWaitingTime += c.lockWaitingTime;
        
        serviceNanoTime += c.serviceNanoTime;

        queuingNanoTime += c.queuingNanoTime;

    }

}
