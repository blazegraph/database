package com.bigdata.journal;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;

/**
 * Helper class maintains the moving average of the length of a queue.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueueStatisticsTask implements Runnable {

    protected static Logger log = Logger.getLogger(QueueStatisticsTask.class);
    
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
     * @see #getMovingAverage(double, double, double)
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
     * @see #getMovingAverage(double, double, double)
     */
    public final Instrument<Double> averageQueueActiveCountInst = new Instrument<Double>() {
        
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
     * @see #getMovingAverage(double, double, double)
     */
    public final Instrument<Double> averageQueueActiveCountWithLocksHeldInst = new Instrument<Double>() {
        
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
     * @see #getMovingAverage(double, double, double)
     */
    public final Instrument<Double> averageQueueLengthInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageQueueLength );
            
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
    private double averageTaskQueueWaitingTime = 0d;
    // time waiting for resource locks.
    private double averageTaskLockWaitingTime = 0d;
    // time doing work (with any resources locks, excludes commit).
    private double averageTaskServiceTime = 0d;
    // total time from submit to completion.
    private double averageTaskQueuingTime = 0d;

    private double averageCommitWaitingTime = 0d;
    private double averageCommitServiceTime = 0d;
    
    private long taskWaitingTime = 0L;
    private long taskLockWaitingTime = 0L;
    private long taskServiceTime = 0L;
    private long taskQueuingTime = 0L;

    private long commitWaitingTime = 0L;
    private long commitServiceTime = 0L;

    // FIXME aka throughput (1000d/queueingTime).
    private double averageTasksPerSecond = 0d;
    
    /**
     * Moving average in milliseconds of the time a task waits on a queue
     * pending execution.
     */
    public final Instrument<Double> averageTaskQueueWaitingTimeInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageTaskQueueWaitingTime );
            
        }

    };

    /**
     * Moving average in milliseconds of the time that a task is waiting for
     * resource locks (zero unless the task is unisolated).
     */
    public final Instrument<Double> averageTaskLockWaitingTimeInst = new Instrument<Double>() {

        @Override
        protected void sample() {

            setValue(averageTaskLockWaitingTime);

        }

    };
    
    /**
     * Moving average in milliseconds of the time that a task is being serviced
     * by a worker thread (elapsed clock time from when the task was assigned to
     * the thread until the task completes its work).
     */
    public final Instrument<Double> averageTaskServiceTimeInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageTaskServiceTime );
            
        }
        
    };

    /**
     * Moving average in milliseconds of the time between the submission of a
     * task and its completion including any time spent waiting for resource
     * locks, commit processing and any time spent servicing that task.
     */
    public final Instrument<Double> averageTaskQueuingTimeInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageTaskQueuingTime );
            
        }
        
    };

    /**
     * Moving average in milliseconds of the time that the task that initiates
     * the group commit waits for other tasks to join the commit group (zero
     * unless the service is unisolated).
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
     */
    public final Instrument<Double> averageCommitServiceTimeInst = new Instrument<Double>() {

        @Override
        protected void sample() {

            setValue(averageCommitServiceTime);

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
     * 
     * 
     * @param name The label for the service.
     * 
     * @param service
     *            The service to be monitored.
     */
    public QueueStatisticsTask(String name, ThreadPoolExecutor service) {
    
        this(name, service, null/*taskCounters*/, DEFAULT_WEIGHT);
        
    }
    
    /**
     * 
     * @param name
     *            The label for the service.
     * @param service
     *            The service to be monitored.
     * @param taskCounters
     *            The per-task counters used to compute the latency data for
     *            tasks run on that service.
     */
    public QueueStatisticsTask(String name, ThreadPoolExecutor service, TaskCounters taskCounters) {
        
        this(name, service, taskCounters, DEFAULT_WEIGHT);
        
    }
    
    /**
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
    public QueueStatisticsTask(String name, ThreadPoolExecutor service, TaskCounters taskCounters, double w) {
    
        assert name != null;
        
        assert service != null;
        
        assert w > 0d && w < 1d;
        
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
                
                final int activeCount = ((WriteExecutorService) service)
                        .getConcurrentTaskCount();

                averageActiveCountWithLocksHeld = getMovingAverage(
                        averageActiveCountWithLocksHeld, activeCount, w);

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

                        final long newValue = taskCounters.queueWaitingTime.get();

                        final long delta = newValue - taskWaitingTime;

                        assert delta >= 0 : "" + delta;

                        taskWaitingTime = newValue;

                        averageTaskQueueWaitingTime = getMovingAverage(
                                averageTaskQueueWaitingTime,
                                (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                                w);

                    }

                    /*
                     * Time waiting on resource lock(s).
                     */
                    if(service instanceof WriteExecutorService) {
                        
                        final long newValue = taskCounters.lockWaitingTime.get();

                        final long delta = newValue - taskLockWaitingTime;

                        assert delta >= 0 : "" + delta;

                        taskLockWaitingTime = newValue;

                        averageTaskLockWaitingTime = getMovingAverage(
                                averageTaskLockWaitingTime,
                                (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                                w);

                    }
                    
                    /*
                     * Time that the task is being serviced (after its obtained
                     * any locks).
                     */
                    {

                        final long newValue = taskCounters.serviceNanoTime.get();

                        final long delta = newValue - taskServiceTime;

                        assert delta >= 0 : "" + delta;

                        taskServiceTime = newValue;

                        averageTaskServiceTime = getMovingAverage(
                                averageTaskServiceTime,
                                (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                                w);

                    }

                    /*
                     * Queuing time (elapsed time from submit until completion).
                     */
                    {

                        final long newValue = taskCounters.queuingNanoTime.get();

                        final long delta = newValue - taskQueuingTime;

                        assert delta >= 0 : "" + delta;

                        taskQueuingTime = newValue;

                        averageTaskQueuingTime = getMovingAverage(
                                averageTaskQueuingTime,
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

                            final long newValue = taskCounters.commitWaitingTime
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

                            final long newValue = taskCounters.commitServiceTime
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
                    
                }

            }
            
            nsamples++;

            // @todo config reporting period.
            final int period = 10;
            if (nsamples % period == 0) {

                // todo log all counter values using counterSet.asXML()?
                if(log.isInfoEnabled())
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
     * @param service
     *            The service for which the counters will be reported.
     * 
     * @return The caller's <i>counterSet</i>
     */
    public CounterSet addCounters(CounterSet counterSet) {
        
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

        if (taskCounters == null) {

            /*
             * Report iff not being reported via the TaskCounters.
             */
            
            counterSet.addCounter("taskCompletedCount", new Instrument<Long>() {
                public void sample() {
                    setValue(service.getCompletedTaskCount());
                }
            });
        
        }
        
        counterSet.addCounter("poolSize",
                new Instrument<Integer>() {
                    public void sample() {
                        setValue(service.getPoolSize());
                    }
                });

        counterSet.addCounter("largestPoolSize",
                new Instrument<Integer>() {
                    public void sample() {
                        setValue(service.getLargestPoolSize());
                    }
                });

        /*
         * Computed variables based on the service itself.
         */

        counterSet.addCounter("averageQueueSize", averageQueueSizeInst);

        counterSet.addCounter("averageQueueActiveCount",
                averageQueueActiveCountInst);

        counterSet.addCounter("averageQueueLength", averageQueueLengthInst);

        /*
         * Computed variables based on the per-task counters.
         */
        if (taskCounters != null) {

            /*
             * Simple counters.
             */

            // count of all tasks submitted to the service. 
            counterSet.addCounter("taskSubmittedCount", new Instrument<Long>() {
                public void sample() {
                    setValue(taskCounters.taskSubmitCount.get());
                }
            });

            // count of all tasks completed by the service (failed + success).
            counterSet.addCounter("taskCompleteCount", new Instrument<Long>() {
                public void sample() {
                    setValue(taskCounters.taskCompleteCount.get());
                }
            });

            // count of all tasks which failed during execution.
            counterSet.addCounter("taskFailedCount", new Instrument<Long>() {
                public void sample() {
                    setValue(taskCounters.taskFailCount.get());
                }
            });

            // count of all tasks which were successfully executed.
            counterSet.addCounter("taskSuccessCount", new Instrument<Long>() {
                public void sample() {
                    setValue(taskCounters.taskSuccessCount.get());
                }
            });

            /*
             * Moving averages.
             */
            
            counterSet.addCounter("averageTaskQueueWaitingTime",
                    averageTaskQueueWaitingTimeInst);

            counterSet.addCounter("averageTaskServiceTime",
                    averageTaskServiceTimeInst);

            counterSet.addCounter("averageTaskQueuingTime",
                    averageTaskQueuingTimeInst);
            
        }
    
        /*
         * These data are available only for the write service.
         */
        if(service instanceof WriteExecutorService) {
            
            final WriteExecutorService writeService = (WriteExecutorService)service;
           
            /*
             * Simple counters.
             */
            
            counterSet.addCounter("commits", new Instrument<Long>() {
                public void sample() {
                    setValue(writeService.getGroupCommitCount());
                }
            });

            counterSet.addCounter("#aborts", new Instrument<Long>() {
                public void sample() {
                    setValue(writeService.getAbortCount());
                }
            });

            counterSet.addCounter("overflowCount", new Instrument<Long>() {
                public void sample() {
                    setValue(writeService.getOverflowCount());
                }
            });

            /*
             * Maximum observed values.
             */

            counterSet.addCounter("maxLatencyUntilCommit",
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(writeService.getMaxLatencyUntilCommit());
                        }
                    });

            counterSet.addCounter("maxCommitLatency", new Instrument<Long>() {
                public void sample() {
                    setValue(writeService.getMaxCommitLatency());
                }
            });

            counterSet.addCounter("maxRunning", new Instrument<Long>() {
                public void sample() {
                    setValue(writeService.getMaxRunning());
                }
            });

            /*
             * Moving averages available only for the write executor service.
             */

            counterSet.addCounter("averageQueueActiveCountWithLocksHeld",
                    averageQueueActiveCountWithLocksHeldInst);

            counterSet.addCounter("averageTaskLockWaitingTime",
                    averageTaskLockWaitingTimeInst);
                
            counterSet.addCounter("averageCommitWaitingTime",
                        averageCommitServiceTimeInst);
                    
            counterSet.addCounter("averageCommitServiceTime",
                        averageCommitServiceTimeInst);
                    
            
        }

        return counterSet;

    }

}
