package com.bigdata.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.util.concurrent.IQueueCounters.ITaskCounters;

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
 * @see ThreadPoolExecutorStatisticsTask
 */
public class TaskCounters {

    public String toString() {
        return getClass().getSimpleName() + //
                "{#submit=" + taskSubmitCount + //
                ",#complete=" + taskCompleteCount + //
                ",#fail=" + taskFailCount + //
                ",#success=" + taskSuccessCount + //
                ",#queueWaitingTime=" + queueWaitingNanoTime + //
//                ",#lockWaitingTime=" + lockWaitingNanoTime + //
                ",#serviceTime=" + serviceNanoTime + //
//                ",#commitWaitingTime=" + commitWaitingNanoTime + //
//                ",#commitServiceTime=" + commitServiceNanoTime + //
                ",#queuingTime=" + queuingNanoTime + //
                "}";
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
     * Cumulative elapsed time in nanoseconds consumed by tasks while assigned
     * to a worker thread.
     * <p>
     * Note: Since this is aggregated over concurrent tasks the reported elapsed
     * time MAY exceed the actual elapsed time during which those tasks were
     * executed.
     * <p>
     * Note: Service time on the client includes queuing time on the service and
     * the checkpoint time (for write tasks).
     */
    final public AtomicLong serviceNanoTime = new AtomicLong();

    /**
     * Cumulative elapsed time in nanoseconds consumed by write tasks while
     * checkpointing their indices.
     * <p>
     * Note: Since this is aggregated over concurrent tasks the reported elapsed
     * time MAY exceed the actual elapsed time during which those tasks were
     * executed.
     * <p>
     * Note: This time is already reported by the {@link #serviceNanoTime} but
     * is broken out here for additional detail. Checkpoint time can be most of
     * the service time for a task since indices buffer writes and but are
     * required to flush those writes to the backing during during a checkpoint.
     */
    final public AtomicLong checkpointNanoTime = new AtomicLong();

    /**
     * Cumulative elapsed time in nanoseconds consumed by tasks from when they
     * are submitted until they are complete.
     * <p>
     * Note: Queueing time on the client includes queueing time on the service.
     */
    final public AtomicLong queuingNanoTime = new AtomicLong();

    /** Ctor */
    public TaskCounters() {

    }

    /**
     * Note: The elapsed time counters ({@link #queueWaitingNanoTime},
     * {@link #serviceNanoTime}, and {@link #queuingNanoTime}) are reported as
     * cumulative <i>milliseconds</i> by this method. These data are turned
     * into moving averages by the {@link ThreadPoolExecutorStatisticsTask}.
     */
    public CounterSet getCounters() {
        
        final CounterSet counterSet = new CounterSet();

        // count of all tasks submitted to the service.
        counterSet.addCounter(ITaskCounters.TaskSubmitCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(taskSubmitCount.get());
                    }
                });

        // count of all tasks completed by the service (failed + success).
        counterSet.addCounter(ITaskCounters.TaskCompleteCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(taskCompleteCount.get());
                    }
                });

        // count of all tasks which failed during execution.
        counterSet.addCounter(ITaskCounters.TaskFailCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(taskFailCount.get());
                    }
                });

        // count of all tasks which were successfully executed.
        counterSet.addCounter(ITaskCounters.TaskSuccessCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(taskSuccessCount.get());
                    }
                });

        counterSet.addCounter(ITaskCounters.QueueWaitingTime,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(TimeUnit.NANOSECONDS
                                .toMillis(queueWaitingNanoTime.get()));
                    }
                });

        counterSet.addCounter(ITaskCounters.ServiceTime,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(TimeUnit.NANOSECONDS.toMillis(serviceNanoTime
                                .get()));
                    }
                });

        counterSet.addCounter(ITaskCounters.CheckpointTime,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(TimeUnit.NANOSECONDS.toMillis(checkpointNanoTime
                                .get()));
                    }
                });

        counterSet.addCounter(ITaskCounters.QueuingTime,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(TimeUnit.NANOSECONDS.toMillis(queuingNanoTime
                                .get()));
                    }
                });

        return counterSet;

    }

}