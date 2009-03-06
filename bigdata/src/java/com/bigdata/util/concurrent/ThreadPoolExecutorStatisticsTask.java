package com.bigdata.util.concurrent;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.util.concurrent.IQueueCounters.IThreadPoolExecutorCounters;
import com.bigdata.util.concurrent.IQueueCounters.IThreadPoolExecutorTaskCounters;
import com.bigdata.util.concurrent.IQueueCounters.IWriteServiceExecutorCounters;

/**
 * Class tracks a variety of information about a {@link ThreadPoolExecutor}
 * including the moving average of its queue length, queuing times, etc.
 * 
 * @todo {@link ThreadPoolExecutor#getActiveCount()},
 *       {@link ThreadPoolExecutor#getTaskCount()}, and
 *       {@link ThreadPoolExecutor#getCompletedTaskCount()} all obtain a lock
 *       and then iterate over the workers. This makes them heavier operations
 *       than you might otherwise expect!
 *       <p>
 *       Consider subclassing {@link ThreadPoolExecutor} to maintain these
 *       counters more cheaply.
 * 
 * @todo refactor to layer {@link QueueStatisticsTask} then
 *       {@link ThreadPoolExecutorStatisticsTask} then for the
 *       {@link WriteServiceExecutor}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThreadPoolExecutorStatisticsTask implements Runnable {

    protected static final Logger log = Logger.getLogger(ThreadPoolExecutorStatisticsTask.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * The service that is being monitored.
     */
    private final ThreadPoolExecutor service;
    
    /**
     * The label for the service (used in log messages).
     */
    private final String name;
    
    /**
     * The weight used to compute the moving average.
     */
    private final double w;

    /**
     * #of samples taken so far.
     */
    private long nsamples = 0;
    
    /*
     * There are several different moving averages which are computed.
     */
    
    private double averageQueueSize   = 0d;
    private double averageActiveCount = 0d;
    private double averageQueueLength = 0d;
    private double averageActiveCountWithLocksHeld = 0d;
    
    /**
     * The #of tasks not yet assigned to any thread which are waiting to run
     * (moving average).
     * 
     * @see IQueueCounters#AverageQueueSize
     */
    public final Instrument<Double> averageQueueSizeInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageQueueSize );
            
        }
        
    };

    /**
     * The #of tasks that are currently running (moving average).
     * 
     * @see IThreadPoolExecutorCounters#AverageActiveCount
     */
    public final Instrument<Double> averageActiveCountInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageActiveCount );
            
        }
        
    };
    
    /**
     * The #of tasks that are currently running <strong>with locks held</strong>
     * (moving average) (this is only reported for the
     * {@link WriteExecutorService}).
     * 
     * @see IWriteServiceExecutorCounters#AverageActiveCountWithLocksHeld
     */
    public final Instrument<Double> averageActiveCountWithLocksHeldInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageActiveCountWithLocksHeld );
            
        }
        
    };
    
    /**
     * The queue length (moving average).
     * <p>
     * Note: this is the primary average of interest - it includes both the
     * tasks waiting to be run and those that are currently running in the
     * definition of the "queue length".
     * 
     * @see IThreadPoolExecutorCounters#AverageQueueLength
     */
    public final Instrument<Double> averageQueueLengthInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue(averageQueueLength);

        }

    };

    /**
     * The #of tasks waiting to execute on the internal lock on the
     * {@link WriteExecutorService}.
     */
    public final Instrument<Double> averageReadyCountInst = new Instrument<Double>() {

        @Override
        protected void sample() {

            setValue(averageReadyCount);
            
        }
        
    };

    /**
     * Data collected about {@link AbstractTask}s run on a service (optional).
     */
    private final TaskCounters taskCounters;
    
    /*
     * These are moving averages based on the optional TaskCounters.
     * 
     * Note: We also store the old value for these since they are cumulative
     * counters but we need only the delta from the last value for the counter
     * to compute the moving averages.
     * 
     * @todo there are other data that could be computed and reported out.
     */

    // time waiting on the queue until the task begins to execute.
    private double averageQueueWaitingTime = 0d;
    // time waiting for resource locks.
    private double averageLockWaitingTime = 0d;
    // time doing work (with any resources locks, excludes commit).
    private double averageServiceTime = 0d;
    // total time from submit to completion.
    private double averageQueuingTime = 0d;

    private double averageCommitWaitingTime = 0d;
    private double averageCommitServiceTime = 0d;
    private double averageCommitGroupSize = 0d;
    private double averageByteCountPerCommit = 0d;
    
    private long queueWaitingTime = 0L;
    private long lockWaitingTime = 0L;
    private long serviceTime = 0L;
    private long queuingTime = 0L;

    private long commitWaitingTime = 0L;
    private long commitServiceTime = 0L;
    
    private double averageReadyCount;

    /**
     * Moving average in milliseconds of the time a task waits on a queue
     * pending execution.
     * 
     * @see IThreadPoolExecutorTaskCounters#AverageQueueWaitingTime
     */
    public final Instrument<Double> averageQueueWaitingTimeInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageQueueWaitingTime );
            
        }

    };

    /**
     * Moving average in milliseconds of the time that a task is waiting for
     * resource locks (zero unless the task is unisolated).
     * 
     * @see IWriteServiceExecutorCounters#AverageLockWaitingTime
     */
    public final Instrument<Double> averageLockWaitingTimeInst = new Instrument<Double>() {

        @Override
        protected void sample() {

            setValue(averageLockWaitingTime);

        }

    };
    
    /**
     * Moving average in milliseconds of the time that a task is being serviced
     * by a worker thread (elapsed clock time from when the task was assigned to
     * the thread until the task completes its work).
     * <p>
     * Note: For tasks which acquire resource lock(s), this does NOT include the
     * time waiting to acquire the resource lock(s).
     * 
     * @see IThreadPoolExecutorTaskCounters#AverageServiceTime
     */
    public final Instrument<Double> averageServiceTimeInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageServiceTime );
            
        }
        
    };

    /**
     * Moving average in milliseconds of the time between the submission of a
     * task and its completion including any time spent waiting for resource
     * locks, commit processing and any time spent servicing that task.
     * 
     * @see IThreadPoolExecutorTaskCounters#AverageQueuingTime
     */
    public final Instrument<Double> averageQueuingTimeInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageQueuingTime );
            
        }
        
    };

    /**
     * Moving average in milliseconds of the time that the task that initiates
     * the group commit waits for other tasks to join the commit group (zero
     * unless the service is unisolated).
     * 
     * @see IWriteServiceExecutorCounters#AverageCommitWaitingTime
     */
    public final Instrument<Double> averageCommitWaitingTimeInst = new Instrument<Double>() {

        @Override
        protected void sample() {

            setValue(averageCommitWaitingTime);

        }

    };
    
    /**
     * Moving average in milliseconds of the time servicing the group commit
     * (zero unless the service is unisolated).
     * 
     * @see IWriteServiceExecutorCounters#AverageCommitServiceTime
     */
    public final Instrument<Double> averageCommitServiceTimeInst = new Instrument<Double>() {

        @Override
        protected void sample() {

            setValue(averageCommitServiceTime);

        }

    };

    /**
     * Moving average of the size of commits (zero unless the service is
     * unisolated).
     * 
     * @see IWriteServiceExecutorCounters#AverageCommitGroupSize
     */
    public final Instrument<Double> averageCommitGroupSizeInst = new Instrument<Double>() {

        @Override
        protected void sample() {

            setValue(averageCommitGroupSize);

        }

    };

    /**
     * Moving average of the #of bytes written since the previous commit (zero
     * unless the service is unisolated).
     * 
     * @see IWriteServiceExecutorCounters#AverageByteCountPerCommit
     */
    public final Instrument<Double> averageByteCountPerCommitInst = new Instrument<Double>() {

        @Override
        protected void sample() {

            setValue(averageByteCountPerCommit);

        }

    };
    
    /**
     * Scaling factor converts nanoseconds to milliseconds.
     */
    static final double scalingFactor = 1d / TimeUnit.NANOSECONDS.convert(1,
            TimeUnit.MILLISECONDS);
    
    
    /**
     * The weight used to compute the moving average.
     */
    public double getWeight() {
        
        return w;
        
    }
    
    /**
     * #of samples taken so far.
     */
    public long getSampleCount() {
        
        return nsamples;
        
    }
    
    /**
     * The recommended default weight.
     */
    public static final double DEFAULT_WEIGHT = .2d;
    
    /**
     * Ctor variant when the {@link ThreadPoolExecutor} does not have hooks for
     * an {@link AbstractTask} and therefore does not update
     * {@link TaskCounters}s.
     * 
     * @param name
     *            The label for the service.
     * 
     * @param service
     *            The service to be monitored.
     */
    public ThreadPoolExecutorStatisticsTask(String name, ThreadPoolExecutor service) {

        this(name, service, null/* taskCounters */, DEFAULT_WEIGHT);

    }

    /**
     * Ctor variant when the {@link ThreadPoolExecutor} has hooks for an
     * {@link AbstractTask} and updates the given {@link TaskCounters}s.
     * 
     * @param name
     *            The label for the service.
     * @param service
     *            The service to be monitored.
     * @param taskCounters
     *            The per-task counters used to compute the latency data for
     *            tasks run on that service.
     */
    public ThreadPoolExecutorStatisticsTask(String name, ThreadPoolExecutor service,
            TaskCounters taskCounters) {

        this(name, service, taskCounters, DEFAULT_WEIGHT);

    }
    
    /**
     * Core impl.
     * 
     * @param name
     *            The label for the service.
     * @param service
     *            The service to be monitored.
     * @param taskCounters
     *            The per-task counters used to compute the latency data for
     *            tasks run on that service.
     * @param w
     *            The weight to be used by
     *            {@link #getMovingAverage(double, double, double)}
     */
    public ThreadPoolExecutorStatisticsTask(String name, ThreadPoolExecutor service,
            TaskCounters taskCounters, double w) {
    
        if (name == null)
            throw new IllegalArgumentException();

        if (service == null)
            throw new IllegalArgumentException();

        // Note: MAY be null (useful for generic executor services).
//        if (taskCounters == null)
//            throw new IllegalArgumentException();

        if (w <= 0d || w >= 1d)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.service = service;
        
        this.taskCounters = taskCounters;
        
        this.w = w;
        
    }
    
    /**
     * Compute a moving average: <code>(1 - w) * avg + w * q</code>
     * 
     * @param avg
     *            The previous average and initially zero (0.0).
     * @param q
     *            The current value (e.g., the instantaneous measurement of the
     *            #of active tasks plus the length of the queue).
     * @param w
     *            The weight for the moving average in (0:1). Values around .2
     *            seem appropriate.
     * 
     * @return The updated moving average.
     */
    public double getMovingAverage(double avg, double q, double w) {
        
        return (1 - w) * avg + (w * q);
        
    }

    /**
     * Note: don't throw anything from here or it will cause the task to no
     * longer be run!
     */
    public void run() {

        try {

            // queueSize := #of tasks in the queue.
            final int queueSize = service.getQueue().size();

            {
                
                // activeCount := #of tasks assigned a worker thread
                final int activeCount = service.getActiveCount();

                // This is just the tasks that are currently waiting to run (not
                // assigned to any thread).
                averageQueueSize = getMovingAverage(averageQueueSize,
                        queueSize, w);

                // This is just the tasks that are currently running (assigned
                // to a worker thread).
                averageActiveCount = getMovingAverage(averageActiveCount,
                        activeCount, w);

                /*
                 * Note: this is the primary average of interest - it includes
                 * both the tasks waiting to be run and those that are currently
                 * running in the definition of the "queue length".
                 */
                averageQueueLength = getMovingAverage(averageQueueLength,
                        (activeCount + queueSize), w);

            }

            if (service instanceof WriteExecutorService) {

                /*
                 * Note: For the WriteExecutorService we also compute the
                 * averageActiveCount using a definition of [activeCount] that
                 * only counts tasks that are currently holding their exclusive
                 * resource lock(s). This is the real concurrency of the write
                 * service since tasks without locks are waiting on other tasks
                 * so that they can obtain their lock(s) and "run".
                 */
                
                final int activeCountWithLocksHeld = ((WriteExecutorService) service)
                        .getActiveTaskCountWithLocksHeld();

                averageActiveCountWithLocksHeld = getMovingAverage(
                        averageActiveCountWithLocksHeld, activeCountWithLocksHeld, w);

            }

            if (taskCounters != null) {
                
                /*
                 * Compute some latency data that relies on the task counters.
                 */

                // #of tasks that have been submitted so far.
                final long taskCount = taskCounters.taskCompleteCount.get();
                
                if (taskCount > 0) {

                    /*
                     * Time waiting on the queue to begin execution.
                     */
                    {

                        final long newValue = taskCounters.queueWaitingNanoTime.get();

                        final long delta = newValue - queueWaitingTime;

                        assert delta >= 0 : "" + delta;

                        queueWaitingTime = newValue;

                        averageQueueWaitingTime = getMovingAverage(
                                averageQueueWaitingTime,
                                (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                                w);

                    }

                    /*
                     * Time waiting on resource lock(s).
                     */
                    if(service instanceof WriteExecutorService) {
                        
                        final long newValue = taskCounters.lockWaitingNanoTime.get();

                        final long delta = newValue - lockWaitingTime;

                        assert delta >= 0 : "" + delta;

                        lockWaitingTime = newValue;

                        averageLockWaitingTime = getMovingAverage(
                                averageLockWaitingTime,
                                (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                                w);

                    }
                    
                    /*
                     * Time that the task is being serviced (after its obtained
                     * any locks).
                     */
                    {

                        final long newValue = taskCounters.serviceNanoTime.get();

                        final long delta = newValue - serviceTime;

                        assert delta >= 0 : "" + delta;

                        serviceTime = newValue;

                        averageServiceTime = getMovingAverage(
                                averageServiceTime,
                                (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                                w);

                    }

                    /*
                     * Queuing time (elapsed time from submit until completion).
                     */
                    {

                        final long newValue = taskCounters.queuingNanoTime.get();

                        final long delta = newValue - queuingTime;

                        assert delta >= 0 : "" + delta;

                        queuingTime = newValue;

                        averageQueuingTime = getMovingAverage(
                                averageQueuingTime,
                                (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                                w);

                    }

                }

                if (service instanceof WriteExecutorService) {

                    final WriteExecutorService tmp = (WriteExecutorService) service;

                    final long groupCommitCount = tmp.getGroupCommitCount();

                    if (groupCommitCount > 0) {

                        // Time waiting for the commit.
                        {

                            final long newValue = taskCounters.commitWaitingNanoTime
                                    .get();

                            final long delta = newValue - commitWaitingTime;

                            assert delta >= 0 : "" + delta;

                            commitWaitingTime = newValue;

                            averageCommitWaitingTime = getMovingAverage(
                                    averageCommitWaitingTime,
                                    (delta * scalingFactor / groupCommitCount),
                                    w);

                        }

                        // Time servicing the commit.
                        {

                            final long newValue = taskCounters.commitServiceNanoTime
                                    .get();

                            final long delta = newValue - commitServiceTime;

                            assert delta >= 0 : "" + delta;

                            commitServiceTime = newValue;

                            averageCommitServiceTime = getMovingAverage(
                                    averageCommitServiceTime,
                                    (delta * scalingFactor / groupCommitCount),
                                    w);

                        }

                    }
                    
                    // moving average of the size of the commit groups. 
                    averageCommitGroupSize = getMovingAverage(
                            averageCommitGroupSize, tmp.getCommitGroupSize(), w);

                    // moving average of the #of bytes written since the
                    // previous commit.
                    averageByteCountPerCommit = getMovingAverage(
                            averageByteCountPerCommit, tmp
                                    .getByteCountPerCommit(), w);

                } // end (if service instanceof WriteExecutorService )

            }
            
            nsamples++;

            // @todo config reporting period.
            final int period = 10;
            if (nsamples % period == 0) {

                // todo log all counter values using counterSet.asXML()?
                if(INFO)
                log.info(name + ":\naverageQueueLength=" + averageQueueLength
                        + " (activeCountAverage=" + averageActiveCount
                        + ",queueSizeAverage=" + averageQueueSize
                        + "), nsamples=" + nsamples+"\n"+service+"\n"
                        + "#active="+service.getActiveCount()
                        +", poolSize="+service.getPoolSize()
                        +", maxPoolSize="+service.getMaximumPoolSize()
                        +", largestPoolSize="+service.getLargestPoolSize()
                        +", queueSize="+service.getQueue().size()
                        );

            }
            
        } catch (Exception ex) {

            log.warn(ex.getMessage(),ex);

        }
        
    }

    /**
     * Adds counters for all innate variables defined for a
     * {@link ThreadPoolExecutor} and for each of the variables computed by this
     * class. Note that some variables (e.g., the lock waiting time) are only
     * available when the <i>service</i> specified to the ctor is a
     * {@link WriteExecutorService}.
     * 
     * @param counterSet
     *            The counters will be added to this {@link CounterSet}.
     * 
     * @return The caller's <i>counterSet</i>
     */
    public CounterSet addCounters(final CounterSet counterSet) {

        if (counterSet == null)
            throw new IllegalArgumentException();
        
        synchronized(counterSet) {
        
        /*
         * Defined for ThreadPoolExecutor.
         */

//        Note: reported as moving average instead.
//        counterSet.addCounter("#active",
//                new Instrument<Integer>() {
//                    public void sample() {
//                        setValue(service.getActiveCount());
//                    }
//                });
//        
//      Note: reported as moving average instead.
//        counterSet.addCounter("#queued",
//                new Instrument<Integer>() {
//                    public void sample() {
//                        setValue(service.getQueue().size());
//                    }
//                });
//

            /*
             * Computed variables based on just the Queue.
             */
            {

                counterSet.addCounter(IQueueCounters.AverageQueueSize,
                        averageQueueSizeInst);

            }

            /*
             * Computed variables based on the service itself.
             */
            {

                if (taskCounters == null) {

                    /*
                     * Report iff not being reported via the TaskCounters.
                     */

                    counterSet.addCounter(
                            IThreadPoolExecutorCounters.TaskCompleteCount,
                            new Instrument<Long>() {
                                public void sample() {
                                    setValue(service.getCompletedTaskCount());
                                }
                            });

                }

                counterSet.addCounter(IThreadPoolExecutorCounters.PoolSize,
                        new Instrument<Integer>() {
                            public void sample() {
                                setValue(service.getPoolSize());
                            }
                        });

                counterSet.addCounter(IThreadPoolExecutorCounters.LargestPoolSize,
                        new Instrument<Integer>() {
                            public void sample() {
                                setValue(service.getLargestPoolSize());
                            }
                        });

                counterSet.addCounter(
                        IThreadPoolExecutorCounters.AverageActiveCount,
                        averageActiveCountInst);

                counterSet.addCounter(
                        IThreadPoolExecutorCounters.AverageQueueLength,
                        averageQueueLengthInst);

            }

            /*
             * Computed variables based on the per-task counters.
             */
            if (taskCounters != null) {

                /*
                 * Simple counters.
                 */

                // count of all tasks submitted to the service.
                counterSet.addCounter(
                        IThreadPoolExecutorTaskCounters.TaskSubmitCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(taskCounters.taskSubmitCount.get());
                            }
                        });

                // count of all tasks completed by the service (failed +
                // success).
                counterSet.addCounter(
                        IThreadPoolExecutorCounters.TaskCompleteCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(taskCounters.taskCompleteCount.get());
                            }
                        });

                // count of all tasks which failed during execution.
                counterSet.addCounter(
                        IThreadPoolExecutorTaskCounters.TaskFailCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(taskCounters.taskFailCount.get());
                            }
                        });

                // count of all tasks which were successfully executed.
                counterSet.addCounter(
                        IThreadPoolExecutorTaskCounters.TaskSuccessCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(taskCounters.taskSuccessCount.get());
                            }
                        });

                /*
                 * Moving averages.
                 */

                counterSet.addCounter(IThreadPoolExecutorTaskCounters.AverageQueueWaitingTime,
                        averageQueueWaitingTimeInst);

                counterSet.addCounter(IThreadPoolExecutorTaskCounters.AverageServiceTime,
                        averageServiceTimeInst);

                counterSet.addCounter(IThreadPoolExecutorTaskCounters.AverageQueuingTime,
                        averageQueuingTimeInst);

            }

            /*
             * These data are available only for the write service.
             */
            if (service instanceof WriteExecutorService) {

                final WriteExecutorService writeService = (WriteExecutorService) service;

                /*
                 * Simple counters.
                 */

                counterSet.addCounter(IWriteServiceExecutorCounters.CommitCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(writeService.getGroupCommitCount());
                            }
                        });

                counterSet.addCounter(IWriteServiceExecutorCounters.AbortCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(writeService.getAbortCount());
                            }
                        });

                counterSet.addCounter(IWriteServiceExecutorCounters.OverflowCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(writeService.getOverflowCount());
                            }
                        });

                counterSet.addCounter(IWriteServiceExecutorCounters.RejectedExecutionCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(writeService
                                        .getRejectedExecutionCount());
                            }
                        });

                /*
                 * Maximum observed values.
                 */

                counterSet.addCounter(IWriteServiceExecutorCounters.MaxCommitWaitingTime,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(writeService.getMaxCommitWaitingTime());
                            }
                        });

                counterSet.addCounter(IWriteServiceExecutorCounters.MaxCommitServiceTime,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(writeService.getMaxCommitServiceTime());
                            }
                        });

                counterSet.addCounter(IWriteServiceExecutorCounters.MaxCommitGroupSize,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue((long) writeService
                                        .getMaxCommitGroupSize());
                            }
                        });

                counterSet.addCounter(IWriteServiceExecutorCounters.MaxRunning,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(writeService.getMaxRunning());
                            }
                        });

                /*
                 * Moving averages available only for the write executor service.
                 */

                counterSet.addCounter(
                        IWriteServiceExecutorCounters.AverageActiveCountWithLocksHeld,
                        averageActiveCountWithLocksHeldInst);

                counterSet.addCounter(
                        IWriteServiceExecutorCounters.AverageReadyCount,
                        averageReadyCountInst);

                counterSet.addCounter(IWriteServiceExecutorCounters.AverageCommitGroupSize,
                        averageCommitGroupSizeInst);

                counterSet.addCounter(IWriteServiceExecutorCounters.AverageLockWaitingTime,
                        averageLockWaitingTimeInst);

                counterSet.addCounter(IWriteServiceExecutorCounters.AverageCommitWaitingTime,
                        averageCommitWaitingTimeInst);

                counterSet.addCounter(IWriteServiceExecutorCounters.AverageCommitServiceTime,
                        averageCommitServiceTimeInst);

                counterSet.addCounter(IWriteServiceExecutorCounters.AverageByteCountPerCommit,
                        averageByteCountPerCommitInst);

            }

            return counterSet;

        }

    }

}
