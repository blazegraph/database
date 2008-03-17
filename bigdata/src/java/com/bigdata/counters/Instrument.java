/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Mar 14, 2008
 */

package com.bigdata.counters;

import java.util.concurrent.TimeUnit;

/**
 * Abstract class for reporting instrumented values supporting some useful
 * conversions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class Instrument<T> implements IInstrument<T> {

    /** <code>N/A</code> */
    protected static final transient String NA = "N/A";
    
    /**
     * Converts an event count whose durations were measured in elapsed
     * nanoseconds to an event rate per second.
     * 
     * @param counter
     *            The event count.
     * @param nanos
     *            The elapsed nanoseconds for the events.
     * 
     * @return Either {@value #NA} if <i>nanos</i> is ZERO (0) or the rate per
     *         second for the event.
     */
    protected final String nanosToPerSec(long counter, long nanos) {

        long secs = TimeUnit.SECONDS.convert(nanos, TimeUnit.NANOSECONDS);

        if (secs == 0L)
            return NA;

        return "" + counter / secs;

    }

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
    abstract static public class InstrumentDelta extends Instrument<Double> {

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
        
    }
    
    /**
     * An {@link IInstrument} that reports the instantaneous average during a
     * reporting period where the average is based on a counter of events and
     * the cumulative elapsed time for those events. For this implementation we
     * need to "sample" both the counter and the cumulative elapsed time. Then
     * we report the (deltaCounter / deltaCumulativeElapsedTime) and multiply
     * through by the specified scaling factor.
     * <p>
     * Note: This {@link IInstrument} does NOT monitor {@link #sample()}.
     * Instead it decides each time {@link #getValue()} is invoked whether
     * sufficient time has elapsed to take another {@link #sample()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract static public class InstrumentInstantaneousAverage extends Instrument<Double> {

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
        public InstrumentInstantaneousAverage(long duration, TimeUnit sampleUnit, double scalingFactor) {

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
                final long deltaCumulativeEventTime = newCumulativeEventTimeSample - lastCumulativeEventTimeSample;

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
        
    }
    
}
