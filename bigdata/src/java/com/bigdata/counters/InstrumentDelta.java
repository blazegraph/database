package com.bigdata.counters;

import java.util.concurrent.TimeUnit;

/**
 * An {@link IInstrument} that reports the change in a sample. The reported
 * change in the sampled value is based on exactly two samples - the
 * previous sample and the most recent sample. Samples are taken no more
 * frequently than the specified <i>duration</i> <i>unit</i>s. The
 * measured change in between the previous and the most recent samples is
 * normalized and reported as the change per <i>duration</i> <i>unit</i>.
 * An initial sample value of <code>0.0</code> is presumed. The reported
 * value will reflect this value until at least <i>duration</i> <i>unit</i>s
 * have elapsed.
 * <p>
 * Note: This {@link IInstrument} does NOT monitor {@link #sample()}.
 * Instead it decides each time {@link #getValue()} is invoked whether
 * sufficient time has elapsed to take another {@link #sample()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class InstrumentDelta implements IInstrument<Double> {

    /** The minimum duration between samples. */
    final private long duration;
    
    /** The reporting period expressed in nanoseconds.*/
    final private long reportTime;

    /** #of samples taken. */
    private long nsamples = 0;

    /** Timestamp of the last sample taken. */
    private long lastSampleTime;

    /** Value of the last sample taken. */
    private long lastSample = 0;

    /** The normalized value reported by the instrument. */
    private double normalizedValue = 0.0d;

    /**
     * The instrument will take samples no less than one second apart and
     * will report the change in the sample per second.
     */
    public InstrumentDelta() {
     
        this(1, TimeUnit.SECONDS);
        
    }

    /**
     * Once an initial sample is taken, another sample will not be taken
     * until the specified <i>duration</i> <i>sampleUnit</i>s have
     * elapsed and will report the change in the sample per second.
     * 
     * @param duration
     *            The minimum duration between samples.
     * @param sampleUnit
     *            The units in which that sample duration is expressed.
     */
    public InstrumentDelta(long duration, TimeUnit sampleUnit) {

        this(1, TimeUnit.SECONDS, TimeUnit.SECONDS);

    }
    
    /**
     * Once an initial sample is taken, another sample will not be taken
     * until the specified <i>duration</i> <i>sampleUnit</i>s have
     * elapsed.
     * <p>
     * For example, you are taking samples that are 60 seconds apart but
     * reporting in change per second, then <code>duration := 60</code>,
     * <code>sampleUnit := {@link TimeUnit#SECONDS}</code>, and
     * <code>reportingUnit := {@link TimeUnit#SECONDS}</code>.
     * 
     * @param duration
     *            The minimum duration between samples.
     * @param sampleUnit
     *            The units in which that sample duration is expressed.
     * @param reportingUnit
     *            The value will be normalized as the change per
     *            <i>reportingUnit</i>.
     */
    public InstrumentDelta(long duration, TimeUnit sampleUnit, TimeUnit reportingUnit) {
        
        if (duration <= 0L)
            throw new IllegalArgumentException();

        if (sampleUnit == null)
            throw new IllegalArgumentException();

        if (reportingUnit == null)
            throw new IllegalArgumentException();

        // convert the minimum sample duration to nanoseconds.
        this.duration = TimeUnit.NANOSECONDS.convert(duration, sampleUnit);

        // convert reporting units to nanoseconds.
        this.reportTime = TimeUnit.NANOSECONDS.convert(1, reportingUnit);            
        
        if (reportTime < duration) {

            throw new IllegalArgumentException(
                    "Can not report at rates faster than the sample period");
            
        }
        
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
            lastSample = sample();
            
            lastSampleTime = System.nanoTime();
            
            nsamples++;
            
        }

        final long now = System.nanoTime();
        
        final long deltaT = now - lastSampleTime;
        
        if (deltaT >= duration) {

            final long newSample = sample();
        
            nsamples++;
        
            // delta in the sampled value over the sample period.
            final long delta = newSample - lastSample;

            // normalize the delta to the reporting period.
            normalizedValue = delta * ( reportTime / deltaT );
            
            // update the last sample and last sample time.
            lastSample = newSample;
            
            lastSampleTime = now;
            
        }
        
        return normalizedValue;
    
    }

    /**
     * Take a sample.
     */
    abstract protected long sample();
    
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
