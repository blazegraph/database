package com.bigdata.journal;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.counters.Instrument;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Helper class maintains the moving average of the length of a queue.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueueLengthTask extends Instrument<Double> implements Runnable {

    protected static Logger log = Logger.getLogger(QueueLengthTask.class);
    
    /**
     * The service that is being monitored.
     */
    protected final ThreadPoolExecutor service;
    
    /**
     * The label for the service (used in log messages).
     */
    public final String name;
    
    /**
     * The weight used to compute the moving average.
     */
    public final double w;
    
    /**
     * The current value of the moving average.
     */
    private double avg = 0.0d;
    private double averageActiveCount = 0.0d;
    private double averageQueueSize = 0.0d;
    
    /**
     * The current value of the moving average.
     */
    public double getAverage() {
    
        return avg;
        
    }

    private long nsamples = 0;
    
    /**
     * #of samples taken so far.
     */
    public long getSampleCount() {
        
        return nsamples;
        
    }
    
    /**
     * Uses a default weight of <code>.2</code>.
     * 
     * @param name The label for the service.
     * 
     * @param service
     *            The service to be monitored.
     */
    public QueueLengthTask(String name, ThreadPoolExecutor service) {
    
        this(name,service,.2d);
        
    }
    
    /**
     * 
     * @param name The label for the service.
     * 
     * @param service
     *            The service to be monitored.
     * @param w
     *            The weight to be used by
     *            {@link #averageQueueLength(double, double, double)}
     */
    public QueueLengthTask(String name, ThreadPoolExecutor service, double w) {
    
        assert name != null;
        
        assert service != null;
        
        assert w > 0d && w < 1d;
        
        this.name = name;
        
        this.service = service;
        
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
            
            final int activeCount = service.getActiveCount();
            
            final int queueSize =  service.getQueue().size();

            /*
             * Note: this is the primary average of interest - it includes both
             * the tasks waiting to be run and those that are currently running
             * in the definition of the "queue length".
             */ 
            avg = averageQueueLength(avg, (activeCount + queueSize), w);

            // This is just the tasks that are currently running.
            averageActiveCount = averageQueueLength(averageActiveCount, activeCount, w);
            
            // This is just the tasks that are currently waiting to run.
            averageQueueSize = averageQueueLength(averageQueueSize, queueSize, w);

            nsamples++;

            // @todo config reporting period.
            final int period = 10;
            if (
//                    name.equals("writeService") && 
                    nsamples % period == 0) {

                log.info(name + ":\naverageQueueLength=" + avg
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

    @Override
    protected void sample() {

        setValue( getAverage() );
        
    }
    
}