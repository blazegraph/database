package com.bigdata.util.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * Class tracks the moving average of the queue size.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueueSizeMovingAverageTask extends MovingAverageTask implements Runnable {

    /**
     * The size of the queue (moving average).
     * 
     * @see IQueueCounters#AverageQueueSize
     */
    public double getAverageQueueSize() {
       
        return getMovingAverage();
        
    }

    /**
     * 
     * @param name
     *            The label for the service.
     * @param queue
     *            The queue to be monitored.
     */
    public QueueSizeMovingAverageTask(String name, BlockingQueue<?> queue) {

        this(name, queue, DEFAULT_WEIGHT);

    }

    /**
     * Core impl.
     * 
     * @param name
     *            The label for the service.
     * @param queue
     *            The queue to be monitored.
     * @param w
     *            The weight to be used by
     *            {@link #getMovingAverage(double, double, double)}
     */
    public QueueSizeMovingAverageTask(final String name, final BlockingQueue<?> queue,
            final double w) {

        super(name, new Callable<Integer>() {

            public Integer call() {
                
                return queue.size();
                
            }
            
        }, w);

    }

}
