package com.bigdata.util.concurrent;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

/**
 * Class tracks a the moving average of some sampled datum.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MovingAverageTask implements Runnable {

    protected static final Logger log = Logger
            .getLogger(MovingAverageTask.class);

    /**
     * The label used in log messages.
     */
    protected final String name;

    /**
     * Used to sample whatever is being measured.
     */
    protected final Callable<?extends Number> sampleTask;
    
    /**
     * The weight used to compute the moving average.
     */
    protected final double w;

    /**
     * #of samples taken so far.
     */
    protected long nsamples = 0;

    /**
     * The moving average.
     */
    protected double average = 0d;

    /**
     * The current value of the moving average.
     */
    public double getMovingAverage() {
       
        return average;
        
    }

//    /**
//     * Scaling factor converts nanoseconds to milliseconds.
//     */
//    static protected final double scalingFactor = 1d / TimeUnit.NANOSECONDS
//            .convert(1, TimeUnit.MILLISECONDS);

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
     * @param name
     *            The label for log messages.
     * @param sampleTask
     *            Task that returns a sampled value.
     */
    public MovingAverageTask(final String name,
            final Callable<? extends Number> sampleTask) {

        this(name, sampleTask, DEFAULT_WEIGHT);

    }

    /**
     * Core impl.
     * 
     * @param name
     *            The label for log messages.
     * @param queue
     *            Task that returns a sampled value.
     * @param w
     *            The weight to be used by
     *            {@link #getMovingAverage(double, double, double)}
     */
    public MovingAverageTask(final String name,
            final Callable<? extends Number> sampleTask, final double w) {

        if (name == null)
            throw new IllegalArgumentException();

        if (sampleTask == null)
            throw new IllegalArgumentException();

        if (w <= 0d || w >= 1d)
            throw new IllegalArgumentException();

        this.name = name;

        this.sampleTask = sampleTask;

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
    final static protected double getMovingAverage(final double avg,
            final double q, final double w) {

        return (1 - w) * avg + (w * q);

    }

    /**
     * Note: don't throw anything from here or it will cause the task to no
     * longer be run!
     */
    public void run() {

        try {

            final double sample = sampleTask.call().doubleValue();

            average = getMovingAverage(average, sample, w);

            nsamples++;

        } catch (Exception ex) {

            log.warn(name, ex);

        }

    }

}
