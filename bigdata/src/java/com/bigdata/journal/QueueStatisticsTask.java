package com.bigdata.journal;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.LockManager;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;

/**
 * Helper class maintains the moving average of the length of a queue.
 * 
 * @todo the instruments counters declared on this class could be collected and
 *       reported as a {@link CounterSet}.  this would make it somewhat easier
 *       to use.  this could also make toString() a bit easier to write.
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
     * 
     * @todo reconcile the variable and instrument names with those commonly
     * used for performance analysis (this has been done for the task-specific
     * ones).
     */
    
    private double averageQueueLength = 0d;
    private double averageActiveCount = 0d;
    private double averageQueueSize   = 0d;
    
    /**
     * The queue length (moving average).
     * <p>
     * Note: this is the primary average of interest - it includes both the
     * tasks waiting to be run and those that are currently running in the
     * definition of the "queue length".
     * <p>
     * Note: for the {@link WriteExecutorService} this is reporting the task
     * execution concurrency <strong>with locks held</strong>. This is the real
     * concurrency of the tasks on the {@link WriteExecutorService} since tasks
     * need to wait (in a queue on the {@link LockManager}) until they have
     * their resource lock(s) before they can begin to do useful work.
     * 
     * @see #averageQueueLength(double, double, double)
     */
    public final Instrument<Double> averageQueueLengthInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageQueueLength );
            
        }
        
    };
    
    /**
     * The #of tasks that are currently running (moving average).
     * <p>
     * Note: for the {@link WriteExecutorService} this is reporting the task
     * execution concurrency <strong>with locks held</strong>.
     * 
     * @see #averageQueueLength(double, double, double)
     */
    public final Instrument<Double> averageQueueActiveCountInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageActiveCount );
            
        }
        
    };
    
    /**
     * The #of tasks not yet assigned to any thread which are waiting to run
     * (moving average).
     * 
     * @see #averageQueueLength(double, double, double)
     */
    public final Instrument<Double> averageQueueSizeInst = new Instrument<Double>() {
        
        @Override
        protected void sample() {

            setValue( averageQueueSize );
            
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

    private long taskWaitingTime = 0L;
    private long taskLockWaitingTime = 0L;
    private long taskServiceTime = 0L;
    private long taskQueuingTime = 0L;
    
    // FIXME aka throughput (1/queueingTime).
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
     * @param w
     *            The weight to be used by
     *            {@link #averageQueueLength(double, double, double)}
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
     * average queue length <code>(1 - w) * avg + w * q</code>
     * 
     * @param avg
     *            The previous average and initially zero (0.0).
     * @param q
     *            The current queue length (instantaneous measurement of the
     *            #of active tasks plus the length of the queue).
     * @param w
     *            The weight for the moving average in (0:1). Values around
     *            .2 seem appropriate.
     *            
     * @return The updated moving average.
     */
    public double averageQueueLength(double avg, double q, double w) {
        
        return (1 - w) * avg + (w * q);
        
    }

    /**
     * Note: don't throw anything from here or it will cause the task to no
     * longer be run!
     */
    public void run() {

        try {

            final int activeCount;
            if (service instanceof WriteExecutorService) {

                /*
                 * Note: This is reporting the task execution concurrency
                 * _with_locks_held_ for the write service
                 */

                activeCount = ((WriteExecutorService) service)
                        .getConcurrentTaskCount();
                
            } else {
                
                /*
                 * This reports all tasks since there are no locks.
                 */
                
                activeCount = service.getActiveCount();
                
            }
            
            final int queueSize =  service.getQueue().size();

            /*
             * Note: this is the primary average of interest - it includes both
             * the tasks waiting to be run and those that are currently running
             * in the definition of the "queue length".
             */ 
            averageQueueLength = averageQueueLength(averageQueueLength, (activeCount + queueSize), w);

            // This is just the tasks that are currently running.
            averageActiveCount = averageQueueLength(averageActiveCount, activeCount, w);
            
            // This is just the tasks that are currently waiting to run (not assigned to any thread).
            averageQueueSize = averageQueueLength(averageQueueSize, queueSize, w);

            if (taskCounters != null) {
                
                /*
                 * Compute some latency data that relies on the task counters.
                 */

                // #of tasks that have been submitted so far.
                final long taskCount = taskCounters.completedCount;
                
                if (taskCount > 0) {

                    /*
                     * Time waiting on the queue to begin execution.
                     */
                    {

                        final long newValue = taskCounters.queueWaitingTime;

                        final long delta = newValue - taskWaitingTime;

                        assert delta >= 0 : "" + delta;

                        taskWaitingTime = newValue;

                        averageTaskQueueWaitingTime = averageQueueLength(
                                averageTaskQueueWaitingTime,
                                (delta * scalingFactor / taskCounters.completedCount),
                                w);

                    }

                    /*
                     * Time waiting on resource lock(s).
                     */
                    {

                        final long newValue = taskCounters.lockWaitingTime;

                        final long delta = newValue - taskLockWaitingTime;

                        assert delta >= 0 : "" + delta;

                        taskLockWaitingTime = newValue;

                        averageTaskLockWaitingTime = averageQueueLength(
                                averageTaskLockWaitingTime,
                                (delta * scalingFactor / taskCounters.completedCount),
                                w);

                    }
                    
                    /*
                     * Time that the task is being serviced (after its obtained
                     * any locks).
                     */
                    {

                        final long newValue = taskCounters.serviceNanoTime;

                        final long delta = newValue - taskServiceTime;

                        assert delta >= 0 : "" + delta;

                        taskServiceTime = newValue;

                        averageTaskServiceTime = averageQueueLength(
                                averageTaskServiceTime,
                                (delta * scalingFactor / taskCounters.completedCount),
                                w);

                    }

                    /*
                     * Queuing time (elapsed time from submit until completion).
                     */
                    {

                        final long newValue = taskCounters.queuingNanoTime;

                        final long delta = newValue - taskQueuingTime;

                        assert delta >= 0 : "" + delta;

                        taskQueuingTime = newValue;

                        averageTaskQueuingTime = averageQueueLength(
                                averageTaskQueuingTime,
                                (delta * scalingFactor / taskCounters.completedCount),
                                w);

                    }

                }
                
            }
            
            nsamples++;

            // @todo config reporting period.
            final int period = 10;
            if (
//                    name.equals("writeService") && 
                    nsamples % period == 0) {

                // FIXME log all counter values.
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

}
