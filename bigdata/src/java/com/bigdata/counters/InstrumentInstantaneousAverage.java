package com.bigdata.counters;

import java.util.concurrent.TimeUnit;

/**
 * An {@link IInstrument} that reports the instantaneous average during a
 * reporting period where the average is based on a counter of events and the
 * cumulative elapsed time for those events. For this implementation we need to
 * "sample" both the counter and the cumulative elapsed time. Then we report the
 * (deltaCounter / deltaCumulativeElapsedTime) and multiply through by the
 * specified scaling factor.
 * <p>
 * Note: This {@link IInstrument} does NOT monitor the value. Instead it decides
 * each time {@link #getValue()} is invoked whether sufficient time has elapsed
 * to take another {@link #sample()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class InstrumentInstantaneousAverage implements
        IInstrument<Double> {

    /** The minimum duration between samples. */
    final private long duration;

    /** The scaling factor to be applied. */
    final private double scalingFactor;

    /** #of samples taken. */
    private long nsamples = 0;

    /** Timestamp of the last sample taken. */
    private long lastSampleTime;

    /** Value of the last counter sample taken. */
    private long lastEventCounterSample = 0;

    /** Value of the last cumulative time sample taken. */
    private long lastCumulativeEventTimeSample = 0;

    /** The normalized value reported by the instrument. */
    private double normalizedValue = 0.0d;

    /**
     * The instrument will report the change in the sample per second and
     * uses a scaling factor of <code>1.0</code>.
     */
    public InstrumentInstantaneousAverage() {

        this(1, TimeUnit.SECONDS, 1.0);

    }

    /**
     * Once an initial sample is taken, another sample will not be taken
     * until the specified <i>duration</i> <i>sampleUnit</i>s have
     * elapsed.
     * 
     * @param duration
     *            The minimum duration between samples.
     * @param sampleUnit
     *            The units in which that sample duration is expressed.
     * @param scalingFactor
     *            The result is multipled by the scaling factor before being
     *            reported by {@link #getValue()}.
     */
    public InstrumentInstantaneousAverage(long duration, TimeUnit sampleUnit,
            double scalingFactor) {

        if (duration <= 0L)
            throw new IllegalArgumentException();

        if (sampleUnit == null)
            throw new IllegalArgumentException();

        if (scalingFactor == 0d)
            throw new IllegalArgumentException();

        // convert the minimum sample duration to nanoseconds.
        this.duration = TimeUnit.NANOSECONDS.convert(duration, sampleUnit);

        this.scalingFactor = scalingFactor;

    }

    /**
     * The value is reported as the change in the sample as normalized to a
     * reporting period of duration-units. Both the sample itself and the
     * reported value are updated every no more frequently than every
     * duration-units.
     * 
     * @return The change in the sample value.
     */
    public Double getValue() {

        if (nsamples == 0) {

            /*
             * Take an initial sample.
             * 
             * Note: the reported value will remain [0.0] until we take
             * another sample.
             */

            lastEventCounterSample = sampleEventCounter();

            lastCumulativeEventTimeSample = sampleCumulativeEventTime();

            lastSampleTime = System.nanoTime();

            nsamples++;

        }

        final long now = System.nanoTime();

        final long deltaT = now - lastSampleTime;

        if (deltaT >= duration) {

            final long newEventCounterSample = sampleEventCounter();

            final long newCumulativeEventTimeSample = sampleCumulativeEventTime();

            nsamples++;

            // delta in the sampled value over the sample period.
            final long delta = newEventCounterSample - lastEventCounterSample;

            // the change in the cumulative event time over the sample period.
            final long deltaCumulativeEventTime = newCumulativeEventTimeSample
                    - lastCumulativeEventTimeSample;

            if (deltaCumulativeEventTime == 0) {

                //                    assert delta == 0; // sanity check on the source data.

                // no events, no cumulative event time.
                normalizedValue = 0.0d;

            } else {

                // instantenous average.
                normalizedValue = delta * deltaCumulativeEventTime;

            }

            // update the last sample and last sample time.

            lastEventCounterSample = newEventCounterSample;

            lastCumulativeEventTimeSample = newCumulativeEventTimeSample;

            lastSampleTime = now;

        }

        return normalizedValue * scalingFactor;

    }

    /**
     * Return the current value of the event counter.
     */
    abstract protected long sampleEventCounter();

    /**
     * Return the current value of the cumulative time for the events.
     */
    abstract protected long sampleCumulativeEventTime();

    public long lastModified() {
        
        return lastSampleTime;
        
    }
    
    /**
     * @throws UnsupportedOperationException
     *             always
     * 
     * @todo could propably be implemented but watch datatype for the value and
     *       the time units (nanos vs milliseconds).
     */
    public void setValue(Double value, long timestamp) {

        throw new UnsupportedOperationException(); 
        
    }

}
