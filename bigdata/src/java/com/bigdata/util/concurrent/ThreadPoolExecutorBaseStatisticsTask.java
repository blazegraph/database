package com.bigdata.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.util.concurrent.IQueueCounters.ITaskCounters;
import com.bigdata.util.concurrent.IQueueCounters.IThreadPoolExecutorCounters;
import com.bigdata.util.concurrent.IQueueCounters.IThreadPoolExecutorTaskCounters;
import com.bigdata.util.concurrent.IQueueCounters.IWriteServiceExecutorCounters;

/**
 * Class tracks a variety of information about a {@link ThreadPoolExecutor}
 * including the moving average of its queue length, queue size, average active
 * count, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThreadPoolExecutorBaseStatisticsTask implements Runnable {

    protected static final Logger log = Logger
            .getLogger(ThreadPoolExecutorBaseStatisticsTask.class);

    /**
     * The executor service that is being monitored.
     */
    protected final ThreadPoolExecutor service;

    /**
     * The time when we started to collect data about the {@link #service} (set
     * by the ctor).
     */
    protected final long startNanos;
    
    /**
     * The weight used to compute the moving average.
     */
    protected final double w;
    
    /*
     * There are several different moving averages which are computed.
     */
    
    /**
     * Scaling factor converts nanoseconds to milliseconds.
     */
    static final double scalingFactor = 1d / TimeUnit.NANOSECONDS.convert(1,
            TimeUnit.MILLISECONDS);
    
    /**
     * 
     * @param service
     *            The service to be monitored.
     */
    public ThreadPoolExecutorBaseStatisticsTask(ThreadPoolExecutor service) {

        this(service, MovingAverageTask.DEFAULT_WEIGHT);

    }

    /**
     * Core impl.
     * 
     * @param service
     *            The service to be monitored.
     * @param w
     *            The weight to be used by the {@link MovingAverageTask}s.
     */
    public ThreadPoolExecutorBaseStatisticsTask(final ThreadPoolExecutor service,
            final double w) {
    
        if (service == null)
            throw new IllegalArgumentException();

        if (w <= 0d || w >= 1d)
            throw new IllegalArgumentException();
        
        this.service = service;
        
        this.startNanos = System.nanoTime();

        this.w = w;

        queueSizeTask = new MovingAverageTask("queueSize",
                new Callable<Integer>() {
                    public Integer call() {
                        return service.getQueue().size();
                    }
                }, w);

        activeCountTask = new MovingAverageTask("activeCount",
                new Callable<Integer>() {
                    public Integer call() {
                        return service.getActiveCount();
                    }
                }, w);

        queueLengthTask = new MovingAverageTask("queueLength",
                new Callable<Integer>() {
                    public Integer call() {
                        return service.getQueue().size()
                                + service.getActiveCount();
                    }
                }, w);

        poolSizeTask = new MovingAverageTask("poolSize",
                new Callable<Integer>() {
                    public Integer call() {
                        return service.getPoolSize();
                    }
                }, w);

    }

    /**
     * The moving average of {@link Queue#size()} for the
     * {@link ThreadPoolExecutor}'s work queue.
     */
    private final MovingAverageTask queueSizeTask;
    
    /**
     * The moving average of {@link ThreadPoolExecutor#getActiveCount().
     */
    private final MovingAverageTask activeCountTask;
    
    /**
     * The moving average of (queueSize + activeCount).
     */
    private final MovingAverageTask queueLengthTask;

    /**
     * The moving average of {@link ThreadPoolExecutor#getPoolSize()}.
     */
    private final MovingAverageTask poolSizeTask;

    /**
     * This should be invoked once per second to sample various counters in
     * order to turn their values into moving averages.
     * <p>
     * Note: don't throw anything from here or it will cause the scheduled task
     * executing this to no longer be run!
     */
    public void run() {

        try {

            queueSizeTask.run();
            
            activeCountTask.run();
            
            queueLengthTask.run();
            
            poolSizeTask.run();
            
        } catch (Exception ex) {

            log.error(this,ex);

        }
        
    }

    /**
     * Return the moving averages and various other counters sampled and
     * reported by this class.
     */
    public CounterSet getCounters() {

        final CounterSet counterSet = new CounterSet();

        counterSet.addCounter(IThreadPoolExecutorCounters.AverageQueueSize,
                new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        setValue(queueSizeTask.getMovingAverage());
                    }
                });
        
        counterSet.addCounter(IThreadPoolExecutorCounters.AverageActiveCount,
                new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        setValue(activeCountTask.getMovingAverage());
                    }
                });

        counterSet.addCounter(IThreadPoolExecutorCounters.AverageQueueLength,
                new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        setValue(queueLengthTask.getMovingAverage());
                    }
                });
        
        // @todo Report iff not being reported via the TaskCounters.
        counterSet.addCounter(IThreadPoolExecutorCounters.TaskCompleteCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(service.getCompletedTaskCount());
                    }
                });

        counterSet.addCounter(IThreadPoolExecutorCounters.PoolSize,
                new Instrument<Double>() {
                    public void sample() {
                        setValue(poolSizeTask.getMovingAverage());
                    }
                });

        counterSet.addCounter(IThreadPoolExecutorCounters.LargestPoolSize,
                new Instrument<Integer>() {
                    public void sample() {
                        setValue(service.getLargestPoolSize());
                    }
                });

        return counterSet;

    }

}
