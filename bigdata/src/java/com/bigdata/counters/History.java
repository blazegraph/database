package com.bigdata.counters;

import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.counters.HistoryInstrument.IEntry;

/**
 * Retains history for N periods, where the period is expressed in
 * milliseconds. When the history would overflow, the average for the last N
 * periods is optionally added to another {@link History} which aggregates
 * this one.
 * <p>
 * This class is thread-safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            Which must be Double, Long, or String.
 */
public class History<T> {

    protected static Logger log = Logger.getLogger(History.class);
    
    /**
     * The #of samples that can be stored in the buffer.
     */
    public int capacity() {

        return capacity;

    }

    /**
     * The #of non-missing samples that are stored in the buffer.
     */
    public int size() {

        return size;

    }

    public boolean isNumeric() {

        return _numeric;

    }

    public boolean isLong() {

        return _long;

    }

    public boolean isDouble() {

        return _double;

    }

    /**
     * Takes a snapshot of the samples in the {@link History} and then visits those
     * samples.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class EntryIterator implements Iterator<IEntry<T>> {

        private final int n;

        private int current = -1; // until you call next().

        private final long[] _timestamps;

        private final T[] _data;

        private final Entry entry = new Entry();

        private class Entry implements IEntry<T> {

            public long lastModified() {
                return _timestamps[current];
            }

            public T getValue() {
                return _data[current];
            }

            public String toString() {

                return "(" + getValue() + "," + new Date(lastModified()) + ")";

            }

        }

        @SuppressWarnings("unchecked")
        protected EntryIterator() {

            if (lastLogicalSlot == -1) {

                n = 0;

                _timestamps = null;

                _data = null;

                return;

            }

            /*
             * Find the earliest sample in the buffer and convert to a
             * [logicalSlot].
             */
            final long firstSampleTime;
            final long firstLogicalSlot;
            {

                long t = Long.MAX_VALUE;

                for (int i = 0; i < capacity; i++) {

                    if (timestamps[i] != 0 & timestamps[i] < t) {

                        t = timestamps[i];

                    }

                }

                firstSampleTime = t;

                firstLogicalSlot = firstSampleTime / period;

            }

            /*
             * Count [capacity * period] samples from that [logicalSlot],
             * skipping ones without data - this is the #of samples that we
             * will visit [n].
             */
            final long lastLogicalSlot = firstLogicalSlot + capacity;
            {

                int count = 0;

                for (long ls = firstLogicalSlot; ls < lastLogicalSlot; ls++) {

                    final int physicalSlot = (int) (ls % capacity);

                    if (data[physicalSlot] != null) {

                        count++;

                    }

                }

                n = count;

            }

            /*
             * Allocate internal buffers and produce a dense copy of the
             * source samples.
             * 
             * Note: allocate based on the type of the history.
             */

            _timestamps = new long[n];

            _data = (T[]) java.lang.reflect.Array.newInstance(data.getClass()
                    .getComponentType(), n);

            {

                int count = 0;

                for (long ls = firstLogicalSlot; ls < lastLogicalSlot; ls++) {

                    final int physicalSlot = (int) (ls % capacity);

                    if (data[physicalSlot] != null) {

                        _timestamps[count] = timestamps[physicalSlot];

                        _data[count] = data[physicalSlot];

                        count++;

                    }

                }

            }

        }

        public boolean hasNext() {

            return (current + 1) < n;

        }

        public IEntry<T> next() {

            if (!hasNext())
                throw new NoSuchElementException();

            current++;

            return entry;

        }

        public void remove() {

            throw new UnsupportedOperationException();

        }

    }

    /**
     * Return a snapshot of the most recent value in the buffer -or-
     * <code>null</code> if there are no samples in the buffer.
     */
    synchronized public IEntry<T> getSample() {

        if (lastLogicalSlot == -1) {

            return null;

        }

        final int physicalSlot = (int) (lastLogicalSlot % capacity);

        final long lastModified = timestamps[physicalSlot];

        final T value = data[physicalSlot];

        return new IEntry<T>() {

            public long lastModified() {
                return lastModified;
            }

            public T getValue() {
                return value;
            }

            public String toString() {

                return "(" + value + "," + new Date(lastModified) + ")";

            }

        };

    }

    /**
     * Visits a snapshot of the samples in the buffer in timestamp order.
     * This includes all non-missing samples over the last N periods, where
     * N is the capacity of the buffer.
     */
    synchronized public Iterator<IEntry<T>> iterator() {

        return new EntryIterator();

    }

    /**
     * Return a representation of a snapshot of the samples in buffer.
     */
    synchronized public String toString() {

        final T average = getAverage(); //@todo move to end

        final StringBuilder sb = new StringBuilder();

        sb.append("{");

        final Iterator<IEntry<T>> itr = iterator();

        int n = 0;

        while (itr.hasNext()) {

            final IEntry<T> entry = itr.next();

            final long lastModified = entry.lastModified();

            final T value = entry.getValue();

            assert value != null;

            sb.append("(" + value + "," + new Date(lastModified) + ")");

            if (itr.hasNext())
                sb.append(",");

            n++;

        }

        sb.append("},average=" + average + ",n=" + n);

        return sb.toString();

    }

    /**
     * Computes the average of the samples.
     * 
     * @return The average -or- <code>null</code> if the samples are not
     *         numbers (no average is reported for dates, strings, etc).
     * 
     * @todo could report the most frequent value for non-numeric data or a list
     *       of the distinct values.
     */
    public T getAverage() {

        return getAverage(capacity);
        
    }
    
    /**
     * Compute the average of the samples over the last N reporting periods.
     * 
     * @param nperiods
     *            The #of reporting periods over which the average is to be
     *            computed. E.g., last 10 minutes. The reporting periods have to
     *            be read from the logicalSlot for 10 minutes ago up through the
     *            current logicalSlot.
     * 
     * @return The average over the last N reporting periods.
     * 
     * @throws IllegalArgumentException
     *             If you request data that is older (in reporting periods) that
     *             is stored within the history. E.g., you can not ask for more
     *             than a 60 minute average if the reporting period is minutes
     *             and the capacity is 60.
     */
    synchronized public T getAverage(int nperiods) {

        if (!isNumeric()) {

            /*
             * Not numeric.
             */

            return null;

        }

        if (nperiods < 1 || nperiods > capacity) {
            
            throw new IllegalArgumentException("Must be in [0:" + capacity
                    + "], not " + nperiods);
            
        }
        
        if(this.lastLogicalSlot == -1) {
            
            // No data.
            
            return valueOf(0d);
            
        }
        
        // total of the non-null values.
        double total = 0d;

        // #of non-null values.
        int n = 0;

        final long currentLogicalSlot = timestamps[(int)(lastLogicalSlot % capacity)]
                / period;

        final long firstLogicalSlot = lastLogicalSlot - nperiods + 1;

        // tally non-null samples within the reporting period.
        for (long ls = firstLogicalSlot; ls <= currentLogicalSlot; ls++) {

            final int physicalSlot = (int) (ls % capacity);

            if (data[physicalSlot] == null)
                continue;

            total += ((Number) data[physicalSlot]).doubleValue();

            n++;
            
            assert n <= capacity;

        }

// for (int i = 0; i < capacity; i++) {
//
//            if (data[i] == null)
//                continue;
//
//            total += ((Number) data[i]).doubleValue();
//
//            n++;
//
//        }

        /*
         * Note: assertion could be violated if concurrent modifications were
         * allowed.
         * 
         * FIXME i've seen this assertion but things appear to be synchronized
         * so look into this further for possible fenceposts!
         */
//        assert n == size : "size=" + size + ", but n=" + n;

        if (n == 0) {

            // No samples found.

            return valueOf(0d);

        }

        return valueOf(total / n);

    }

    /**
     * Convert a double to an instance of the generic type parameter for
     * this class.
     * 
     * @param d
     *            The double value.
     *            
     * @return The corresponding instance of the generic type parameter.
     */
    @SuppressWarnings("unchecked")
    protected T valueOf(double d) {

        if (!isNumeric())
            throw new UnsupportedOperationException();

        if (isLong())
            return (T) Long.valueOf((long) d);

        if (isDouble())
            return (T) Double.valueOf(d);

        throw new AssertionError();

    }

    /**
     * Adds a sample to the history. The sample is placed into a slot in
     * this history that reflects its <i>timestamp</i>.
     * <p>
     * If the history wraps around into the next period and there is another
     * history that aggregates this one, then the average for the last
     * period will be added to the aggregating history.
     * <p>
     * Note: If we already have a sample for the same period then the new
     * sample is ignored.
     * 
     * @param timestamp
     *            The timestamp associated with the sample.
     * @param value
     *            The sampled value.
     */
    synchronized public void add(final long timestamp, final T value) {

        log.info("timestamp=" + timestamp + ", value="
                + value);

        if (timestamp <= 0) {

            /*
             * Historical timestamps are not allowed. 
             */

            throw new IllegalArgumentException("timestamp=" + timestamp
                    + ", value=" + value);

        }

        /*
         * The [logicalSlot] is a strictly increasing index corresponding to
         * the #of elapsed periods since the epoch (when timestamp was 0).
         * 
         * The [physicalSlot] is the index at which the new sample will be
         * placed in the buffer. This is always interpreted as logically
         * greater than the last sample (we have already asserted that the
         * timestamp is greater than lastModified), even if the actual index
         * is less than or equal to the current index.
         * 
         * All samples lying between the current sample (exclusive) and the
         * new sample (inclusive and modulo the capacity of the buffer) are
         * cleared. The new sample is then written at the computed position.
         */

        final long logicalSlot = timestamp / period;

        final int physicalSlot = (int) (logicalSlot % capacity);

        if (lastLogicalSlot == -1) {

            /*
             * Special case when this is the first sample.
             */

            assert lastLogicalSlot == -1;
            assert size == 0;

            timestamps[physicalSlot] = timestamp;

            data[physicalSlot] = value;

            size = 1;

        } else {

            final int lastPhysicalSlot = (int) (lastLogicalSlot % capacity);

            final long lastModified = timestamps[lastPhysicalSlot];

            assert lastModified > 0 : "lastModified=" + lastModified;

            if (timestamp / period == lastModified / period) {

                /*
                 * This would cause us to overwrite the last value since the
                 * sample is for the same time period (same logicalSlot).
                 * 
                 * Note: This is checked _before_ we test for time going
                 * backwards since we want to allow updates of host-wide
                 * counters for multiple services running on the same host,
                 * in which case there will be more than one report for the
                 * same time period and those reports will rarely be in
                 * strict timestamp order.
                 * 
                 * @todo probably better off replacing the existing value in
                 * the same logicalSlot.
                 */

                log.warn("overwrite ignored: t=" + timestamp
                        + ", value=" + value);

                return;

            }

            if (timestamp < lastModified) {

                throw new IllegalStateException(
                        "Time goes backwards: lastModified=" + lastModified
                                + ", but timestamp=" + timestamp);

            }

            for (long ls = lastLogicalSlot + 1; ls <= logicalSlot; ls++) {

                final int ps = (int) (ls % capacity);

                if (ps == 0 && sink != null) {

                    /*
                     * Overflow.
                     * 
                     * Note: The overflow point is designed to be on an even
                     * period boundary for the next level of aggregation.
                     */

                    final long t = ls * period/*timestamp*/;

                    final T avg = getAverage();

                    log.info("overflow: t=" + t + ", avg="
                            + avg);

                    sink.add(t, avg);

                }

                if (data[ps] != null) {

                    size--;

                    assert size >= 0 : "size=" + size;

                    data[ps] = null;

                    timestamps[ps] = 0L;

                }

            }

            data[physicalSlot] = value;

            timestamps[physicalSlot] = timestamp;

            size++;

            assert size <= capacity : "size=" + size;

        }

        lastLogicalSlot = logicalSlot;

    }

    /**
     * Constructor used at the base collection period.
     * 
     * @param data
     *            An array whose size is the capacity of the history buffer.
     *            The contents of the array will be used to store the data. (This
     *            API requirement arises since generics are fixed at compile time
     *            rather than runtime.)
     * @param period
     *            The period covered by each slot in milliseconds.
     */
    @SuppressWarnings("unchecked")
    protected History(final T[] data, final long period) {

        if (data == null)
            throw new IllegalArgumentException();

        if (data.length == 0)
            throw new IllegalArgumentException();

        if (period <= 0)
            throw new IllegalArgumentException();

        this.capacity = data.length;

        this.period = period;

        //            this.source = null;

        this.timestamps = new long[capacity];

        this.data = data;

        final Class<T> ctype = (Class<T>) data.getClass().getComponentType();

        this._long = ctype == Long.class;

        this._double = ctype == Double.class;

        _numeric = (_long || _double);

    }

    /**
     * Constructor used when aggregating from another collection period.
     * 
     * @param capacity
     *            The #of slots in the history.
     * @param source
     *            The source whose values are aggregated each time its
     *            history overflows.
     */
    @SuppressWarnings("unchecked")
    protected History(final int capacity, final History<T> source) {

        if (capacity <= 0)
            throw new IllegalArgumentException();

        if (source == null)
            throw new IllegalArgumentException();

        this.capacity = capacity;

        this.period = source.period * capacity;

        //            this.source = source;

        this.timestamps = new long[capacity];

        // Note: allocate based on the type of the source history.
        this.data = (T[]) java.lang.reflect.Array.newInstance(source.data
                .getClass().getComponentType(), capacity);

        // reverse link.
        source.sink = this;

        final Class ctype = data.getClass().getComponentType();

        this._long = ctype == Long.class;

        this._double = ctype == Double.class;

        _numeric = (_long || _double);

    }

    private final int capacity;

    private final long period;

    private final boolean _numeric;

    private final boolean _long;

    private final boolean _double;

    private History<T> sink;

    final private long[] timestamps;

    final private T[] data;

    /**
     * Number of valid samples in the buffer.
     */
    private int size = 0;

    /**
     * The last logical slot in the buffer in which a sample was written and
     * <code>-1</code> until the first sample has been written.
     * <p>
     * The [logicalSlot] is a strictly increasing index corresponding to the #of
     * elapsed periods since the epoch (when timestamp was 0).
     * <p>
     * The [physicalSlot] is the index at which the new sample will be placed in
     * the buffer. This is always interpreted as logically greater than the last
     * sample (we have already asserted that the timestamp is greater than
     * lastModified), even if the actual index is less than or equal to the
     * current index.
     */
    private long lastLogicalSlot = -1;

}
