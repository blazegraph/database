package com.bigdata.counters.query;

import java.util.Arrays;

import org.apache.log4j.Logger;

import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.IHistoryEntry;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.History.SampleIterator;

/**
 * A class representing one or more performance counter histories where those
 * histories have been aligned so that the individual timestamps for the
 * performance counter values in each row are aligned.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME provide aggregation either here or in how the counters are selected.
 * The capturing groups should specify the dimensions of aggregation.
 */
public class HistoryTable {

    protected static final Logger log = Logger.getLogger(HistoryTable.class);
    
    /**
     * The selected counters.
     */
    public final ICounter[] a;

    /**
     * Identifies the history to be written for each of the selected
     * counters by its based reporting period.
     */
    public final PeriodEnum basePeriod;

    /**
     * 
     * @param a
     *            The selected counters.
     * @param basePeriod
     *            Identifies the history to be written for each of the
     *            selected counters by its based reporting period.
     */
    public HistoryTable(final ICounter[] a, final PeriodEnum basePeriod) {
        
        if (a == null)
            throw new IllegalArgumentException();

        if (basePeriod == null)
            throw new IllegalArgumentException();

        this.a = a;

        this.ncols = a.length;
        
        this.basePeriod = basePeriod;
        
        // The units of the history (first column).
        units = basePeriod.name();

        // The period for those units.
        period = basePeriod.getPeriodMillis();

        if (log.isInfoEnabled())
            log.info("#counters=" + a.length + ", units=" + units
                    + ", period=" + period);
        
        /*
         * Create a snapshot iterator for each counter and find the first and
         * last timestamp across the samples for all histories that will be
         * written into the table.
         */
        long firstTimestamp = Long.MAX_VALUE;
        long lastTimestamp = Long.MIN_VALUE;
        int maxSamplesIndex = -1;
        int firstSampleTimeIndex = -1;
        int lastSampleTimeIndex = -1;
        this.firstLogicalSlot = new long[a.length];
        
        /*
         * An array of snapshot history iterators for the selected units of
         * the specified counters.
         */
        final SampleIterator[] hitrs = new SampleIterator[ncols];

        for (int col = 0; col < ncols; col++) {

            if (a[col] == null)
                throw new IllegalArgumentException();

            if (!(a[col].getInstrument() instanceof HistoryInstrument)) {

                throw new IllegalArgumentException();

            }

            // snapshot iterator for the history for that counter.
            final SampleIterator itr = ((HistoryInstrument) a[col]
                    .getInstrument()).getHistory(/*basePeriod*/).iterator();

            hitrs[col] = itr;

            final int sampleCount = itr.getSampleCount();

            if (sampleCount == 0) {

                // No samples for that counter and this period.
                continue;
                
            }
            
            // the logical slot into which the first sample falls for
            // that history.
            firstLogicalSlot[col] = itr.getFirstSampleTime() / period;

            if (itr.getFirstSampleTime() < firstTimestamp) {

                // update the earliest timestamp for the histories.
                firstTimestamp = itr.getFirstSampleTime();

                // update the index of the history with the earliest sample.
                firstSampleTimeIndex = col;

            }

            if (itr.getLastSampleTime() > lastTimestamp) {

                // update the latest timestamp for the histories.
                lastTimestamp = itr.getLastSampleTime();

                // update the index of the history with the latest sample.
                lastSampleTimeIndex = col;

            }

            if (maxSamplesIndex == -1
                    || sampleCount > hitrs[maxSamplesIndex]
                            .getSampleCount()) {

                // update the index of the history with the most samples.
                maxSamplesIndex = col;

            }

        }

        if (maxSamplesIndex != -1) {

            /*
             * There is some data for the table.
             */
            
            assert firstSampleTimeIndex != -1;
            assert lastSampleTimeIndex != -1;

            // the maximum #of samples.
            this.maxSamples = hitrs[maxSamplesIndex].getSampleCount();
            this.maxSamplesIndex = maxSamplesIndex;

            this.firstTimestamp = firstTimestamp;
            this.lastTimestamp = lastTimestamp;
            this.firstSampleTimeIndex = firstSampleTimeIndex;
            this.lastSampleTimeIndex = lastSampleTimeIndex;
            this.logicalSlotOffset = firstLogicalSlot[firstSampleTimeIndex];

            /*
             * Figure out how many rows we need to display. This can be more
             * than the #of samples. It is in fact the max( sampleCount +
             * the offset of the first row) for each counters.
             */
            int nrows = -1;
            for (int col = 0; col < ncols; col++) {

                final int x = getFirstRowIndex(col)
                        + hitrs[col].getSampleCount();

                if (x > nrows) {

                    nrows = x;

                }

            }
            this.nrows = nrows;

            if (log.isInfoEnabled()) {

                log.info("nrows=" + nrows + ", ncols=" + ncols);
                
                log.info("maxSamples=" + maxSamples + " @ index="
                        + maxSamplesIndex);

                log.info("firstTimestamp=" + firstTimestamp + " @ index="
                        + firstSampleTimeIndex);

                log.info("lastTimestamp=" + lastTimestamp + " @ index="
                        + lastSampleTimeIndex);

                log.info("logicalSlotOffset=" + logicalSlotOffset
                        + " : firstLogicalSlot="
                        + Arrays.toString(firstLogicalSlot));

                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (int i = 0; i < a.length; i++) {

                    if (i > 0)
                        sb.append(", ");

                    sb.append(getFirstRowIndex(i));
                    
                }
                sb.append("]");
                
                log.info("adjustedLogicalSlots: "+sb);
                
            }
            
        } else {

            this.nrows = 0;
            this.maxSamples = 0;
            this.maxSamplesIndex = 0;
            this.firstTimestamp = 0L;
            this.firstSampleTimeIndex = 0;
            this.lastTimestamp = 0L;
            this.lastSampleTimeIndex = 0;
            this.logicalSlotOffset = 0L;

        }

        /*
         * Align the counters and populate the rows with the counter values
         * from the selected history unit.
         */
        
        data = new IHistoryEntry[nrows][];

        // pre-populate each row of the table with an array.
        for (int row = 0; row < nrows; row++) {

            data[row] = new IHistoryEntry[ncols];

        }

        /*
         * Now fill in each cell of the table. Since we know the row at
         * which each counter starts in the table, it is easier to proceed
         * in column order for each counter in turn.
         */
        for (int col = 0; col < ncols; col++) {

            final int firstRow = getFirstRowIndex(col);
            
            final SampleIterator itr = hitrs[col];

            int i = 0;

            if (log.isDebugEnabled() && col == 0)
                log.debug(a[col].getPath());

            while (itr.hasNext()) {

                final IHistoryEntry e = itr.next();

                final int row = i + firstRow;

                if (log.isDebugEnabled() && col == 0)
                    log.debug("data[" + row + "," + col + "] = " + e);

                data[row][col] = e;

                i++;
                
            }
            
        }

    }

    /**
     * The logical slot into which the first sample falls for each of the
     * specified counters. This is just <code>timestamp/period</code> for
     * the sample.
     */
    final long[] firstLogicalSlot;

    /**
     * The logical slots are adjusted to a common base (origin zero) by
     * subtracting out the logical slot of the counter with the earliest
     * timestamp for any of the specified counters - this is the value of
     * {@link #firstLogicalSlot} at the {@link #firstSampleTimeIndex}.
     */
    final long logicalSlotOffset;

    /**
     * The index of the row into which the first sample for a counter falls
     * is given by
     * 
     * <pre>
     * (int) (firstLogicalSlot[counterIndex] - logicalSlotOffset)
     * </pre>
     * 
     * @param counterIndex
     *            The index of the counter in the array specified to the
     *            ctor.
     */
    public int getFirstRowIndex(final int counterIndex) {

        return (int) (firstLogicalSlot[counterIndex] - logicalSlotOffset);

    }
    
    /**
     * The earliest timestamp in the selected history units for any of the
     * specified counters.
     */
    public final long firstTimestamp;

    /**
     * The most recent timestamp in the selected history units for any of
     * the specified counters.
     */
    public final long lastTimestamp;

    /**
     * The index of the counter in the specified array having the greatest
     * number of samples for the selected history units.
     */
    final int maxSamplesIndex;

    /**
     * The index of the counter in the specified array whose first sample
     * timestamp was selected as the {@link #firstTimestamp} for the table.
     */
    final int firstSampleTimeIndex;

    /**
     * The index of the counter in the specified array whose last sample
     * timestamp was selected as the {@link #lastTimestamp} for the table.
     */
    final int lastSampleTimeIndex;

    /**
     * The index of the counter in the specified array with the greatest #of
     * samples for the selected history units.
     */
    final int maxSamples;
    
    /**
     * The #of rows in the table. This can be more than the #of samples. It
     * is in fact the max( sampleCount + rowOffset) for each counters.
     */
    public final int nrows;

    /**
     * The #of columns in the table.  This is the same as the #of counters
     * specified to the ctor.
     */
    public final int ncols;
    
    /**
     * The label for the units of the history.
     */
    public final String units;
    
    /**
     * The #of milliseconds in each unit for {@link #units}.
     */
    public final long period;

    /**
     * An array of the performance counter values. The first index is the
     * row. The second index is the column and is correlated with the array
     * specified to the ctor. The rows of the performance counters in the
     * caller's array are aligned by first deciding which counter has the
     * earliest timestamp to be reported ({@link #firstSampleTimeIndex})
     * and then examining the other counters and deciding if they have a
     * value for the same reporting period. If a counter has a value for the
     * same reporting period then the value is incorporated into that row
     * and the row index for that counter is advanced. Otherwise the row
     * index for that counter IS NOT advanced. If there is no data for a
     * given counter in a given row then that cell will be <code>null</code>.
     * It is necessary to align the samples in this manner as counters are
     * created and destroyed over the life of the system and thus some
     * counters may not have data for some reporting periods.
     */
    public final IHistoryEntry[][] data;

    /**
     * Return the timestamp for the row, which is the timestamp of first
     * sample which would be allowed into the logical slot for that row.
     * 
     * @param row
     *            The row.
     * 
     * @return The timestamp of the first sample which would be allowed into
     *         that row.
     */
    public long getTimestamp(final int row) {

        return (row + logicalSlotOffset) * period;
        
    }
    
}