package com.bigdata.counters;

import org.apache.log4j.Logger;

/**
 * A history instrument that retains one sample per minute for an hour, 24
 * once-per-hour averages since for a day, and 30 once-per-day averages for a
 * "month". Samples are overwritten (or cleared in the case of missed samples)
 * only as new samples arrive. Averages are computed from the start of the last
 * hour, day, or "month".
 * 
 * @todo always promote int to long and float to double since we want the
 *       additional bits when aggregating data over time.
 * 
 * @todo do we need to know if a counter is an average or not?
 * 
 * @todo it would be nice to know the units.
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
     * 
     * @param minutes
     *            An array of the desired data type. If the array does not have
     *            60 elements then a new array of the same type will be created.
     *            Regardless, the array will be used to store one sample per
     *            minute.
     */
    @SuppressWarnings("unchecked")
    public HistoryInstrument(T[] minutes) {

        if (minutes == null)
            throw new IllegalArgumentException();
        
        if (minutes.length != 60) {
            
            minutes = (T[]) java.lang.reflect.Array.newInstance(minutes
                    .getClass().getComponentType(), 60);
        
        }
        
        /*
         * Note: The base period is one minute in milliseconds
         */
        final long basePeriod = 60 * 1000;
        
        this.minutes = new History<T>(minutes, basePeriod);

        this.hours = new History<T>(24, this.minutes);

        this.days = new History<T>(30, this.hours);

    }

    public interface IEntry<T> {
        
        public long lastModified();
        
        public T getValue();
        
    };

    /**
     * Return the last value.
     */
    public T getValue() {

        final IEntry<T> sample = minutes.getSample();

        if (sample == null)
            return null;

        return sample.getValue();

    }

    /**
     * Return the timestamp associated with the last value.
     */
    public long lastModified() {

        final IEntry<T> sample = minutes.getSample();

        if (sample == null)
            return -1L;

        return sample.lastModified();

    };

    /**
     * Adds a sample to the history.
     * 
     * @param timestamp
     * @param value
     */
    public void add(long timestamp,T value) {
        
        minutes.add(timestamp, value);
        
    }

    public void setValue(T value, long timestamp) {

        add(timestamp, value);
        
    }
    
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("history{");
        
        sb.append("\nminutes="+minutes);

        sb.append("\nhours="+hours);
        
        sb.append("\ndays="+days);
        
        sb.append("\n}");
        
        return sb.toString();
        
    }

}
