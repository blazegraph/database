package com.bigdata.counters;

import org.apache.log4j.Logger;

/**
 * A history instrument which aggregates samples into a circular buffer with a
 * specified sample period. Old samples are overwritten (or cleared in the case
 * of missed samples) only as new samples arrive. The value reported for a given
 * moment is the average of the samples which were aggregated into the same slot
 * in the underlying {@link History}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <T>
 *            Which must be Double, Long, or String.
 */
public class HistoryInstrument<T> implements IInstrument<T> {

    protected static Logger log = Logger.getLogger(HistoryInstrument.class);
    
    public final History<T> minutes;

    public final History<T> hours;

    public final History<T> days;

    /**
     * Return the history data.
     */
    public History<T> getHistory() {

        return minutes;
        
    }

    /**
     * Return the {@link History} for the specified base period.
     * 
     * @param basePeriod
     *            The base period.
     * 
     * @return The history having samples with that base period.
     */
    public History<T> getHistory(PeriodEnum basePeriod) {

        switch (basePeriod) {
        case Minutes:
            return minutes;
        case Hours:
            return hours;
        case Days:
            return days;
        default:
            throw new AssertionError();
        }

    }

    public HistoryInstrument(final History<T> minutes) {

        if (minutes == null)
            throw new IllegalArgumentException();

        this.minutes = minutes;

        if (minutes.getSink() != null) {

            hours = minutes.getSink();

            if (hours.getSink() != null) {

                days = hours.getSink();

            } else {

                // no overflow.
                days = null;

            }

        } else {

            // no overflow.
            hours = days = null;
            
        }

    }

//    /**
//     * 
//     * @param minutes
//     *            An array of the desired data type. If the array does not have
//     *            60 elements then a new array of the same type will be created.
//     *            Regardless, the array will be used to store one sample per
//     *            minute.
//     */
//    @SuppressWarnings("unchecked")
//    public HistoryInstrument(T[] minutes) {
//
//        if (minutes == null)
//            throw new IllegalArgumentException();
//
//        if (minutes.length != 60) {
//
//            minutes = (T[]) java.lang.reflect.Array.newInstance(minutes
//                    .getClass().getComponentType(), 60);
//
//        }
//
//        /*
//         * Note: The base period is one minute in milliseconds
//         */
//        this.minutes = new History<T>(minutes, PeriodEnum.Minutes
//                .getPeriodMillis());
//
//        // 24 hours in a day
//        this.hours = new History<T>(24, this.minutes);
//
//        // 30 days of history
//        this.days = new History<T>(30, this.hours);
//
//    }

    /**
     * Return the last value.
     */
    public T getValue() {

        final IHistoryEntry<T> sample = minutes.getSample();

        if (sample == null)
            return null;

        return sample.getValue();

    }

    /**
     * Return the timestamp associated with the last value.
     */
    public long lastModified() {

        final IHistoryEntry<T> sample = minutes.getSample();

        if (sample == null)
            return -1L;

        return sample.lastModified();

    };

    /**
     * Adds the sample to the history. Samples in the same slot are averaged.
     * 
     * @param timestamp
     *            The timestamp.
     * @param value
     *            The value of the counter as of that timestamp.
     */
    public void add(final long timestamp, final T value) {

        if (log.isInfoEnabled())
            log.info("timestamp=" + timestamp + ", value=" + value);

        minutes.add(timestamp, value);

    }

    /**
     * Adds the sample to the history. Samples in the same slot are averaged.
     */
    public void setValue(final T value, final long timestamp) {

        add(timestamp, value);
        
    }
    
    public String toString() {

        return minutes.toString();
        
//        StringBuilder sb = new StringBuilder();
//        
//        sb.append("history{");
//        
//        sb.append("\nminutes="+minutes);
//
//        sb.append("\nhours="+hours);
//        
//        sb.append("\ndays="+days);
//        
//        sb.append("\n}");
//        
//        return sb.toString();
        
    }

}
