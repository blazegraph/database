package com.bigdata.counters.render;

import java.io.IOException;
import java.io.Writer;
import java.util.regex.Pattern;

import com.bigdata.counters.ICounter;
import com.bigdata.counters.IHistoryEntry;
import com.bigdata.counters.query.HistoryTable;
import com.bigdata.counters.query.QueryUtil;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TabDelimitedHistoryTableRenderer extends
        HistoryTableRenderer {

    /**
     * 
     * @param tbl
     * @param pattern
     *            Used to identify capturing groups and extract column
     *            labels.
     * @param formatter
     */
    public TabDelimitedHistoryTableRenderer(HistoryTable tbl,
            Pattern pattern, ValueFormatter formatter) {

        super(tbl, pattern, formatter);
        
    }
    
    /**
     * Generate the table.
     * <p>
     * The table has one column for each counter having history data. The
     * 1st header row of the table has the counter path. The seconds header
     * row has just the counter name. The data rows are the samples from
     * each period of the history for each counter.
     */
    public void render(final Writer w) throws IOException {

        // the header rows.
        {

            // header row.
            w.write("From: " + formatter.date(t.firstTimestamp));
            w.write("\tTo: " + formatter.date(t.lastTimestamp));
            w.write("\n");

            // header row.
            w.write("\t");
            for (ICounter counter : t.a) {
                formatter.writeFullPath(w, counter.getPath());
                w.write("\t");
            }
            w.write("\n");

            // header row.
            w.write(t.units+"\t");
            for (ICounter counter : t.a) {
                /*
                 * If the pattern included capturing groups then use the matched
                 * groups as the label for the column.
                 */
                final String[] groups = QueryUtil.getCapturedGroups(pattern, counter);
                final String label;
                if (groups != null) {
                    final StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < groups.length; i++) {
                        final String s = groups[i];
                        if (i > 0)
                            sb.append(":");
                        sb.append(s);
                    }
                    label = sb.toString();
                } else {
                    // default label is the counter name.
                    label = counter.getName();
                }
                w.write(label);
                w.write("\t");
            }
            w.write("Timestamp\n");

        }

        // for each row.
        for (int row = 0; row < t.nrows; row++) {

            /*
             * Populate array with the formatted values from each counter's
             * history for the current row.
             */

            final String[] valStr = new String[t.a.length];

            // for each counter (in a fixed order).
            for (int col = 0; col < t.ncols; col++) {

                final ICounter c = t.a[col];

                final IHistoryEntry e = t.data[row][col];

                valStr[col] = formatter.value(c, e == null ? "" : e.getValue());

            }

            /*
             * The time will be zero for the first row and a delta (expressed in
             * the units of the history) for the remaining rows.
             * 
             * Note: The time units are computed using floating point math and
             * then converted to a display form using formatting in order to be
             * able to accurately convey where a sample falls within the
             * granularity of the unit (e.g., early or late in the day).
             */

            final long timestamp = t.getTimestamp(row);

            final String timeStr = formatter.unitsFormat
                    .format(((double) timestamp - t
                            .getTimestamp(0/* row */))
                            / t.period);

            w.write(timeStr + "\t");

            for (String s : valStr) {

                w.write(s + "\t");

            }

            w.write(formatter.date(timestamp) + "\n");

        } // next row.

    }
    
}