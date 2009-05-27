package com.bigdata.counters.render;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;

import com.bigdata.counters.ICounter;
import com.bigdata.counters.IHistoryEntry;
import com.bigdata.counters.query.CSet;
import com.bigdata.counters.query.HistoryTable;
import com.bigdata.counters.query.PivotTable;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TabDelimitedPivotTableRenderer extends
        PivotTableRenderer {

    public TabDelimitedPivotTableRenderer(PivotTable pt,
            ValueFormatter formatter) {

        super(pt, formatter);
        
    }

    /**
     * Generate the table.
     */
    public void render(final Writer w) throws IOException {

        final HistoryTable t = pt.src;
        
        // the header rows.
        {

            // timestamp column headers.
            w.write(t.units + "\t");
            w.write("timestamp\t");
            for (String s : pt.cnames) {
                // category column headers.
                w.write(s + "\t");
            }
            for (String s : pt.vcols) {
                // performance counter column headers.
                w.write(s + "\t");
            }
            w.write("\n");

        }

        /*
         * FIXME Refactor to use PivotTable and an iterator construct that
         * visits rows.
         */
        
        // for each row in the HistoryTable.
        for (int row = 0; row < t.nrows; row++) {

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

            final String unitStr = XHTMLRenderer.cdata(formatter.unitsFormat
                    .format(((double) timestamp - t
                            .getTimestamp(0/* row */))
                            / t.period));

            final String timeStr = XHTMLRenderer.cdata(formatter.date(timestamp));
            
            /*
             * The set of distinct ordered matched category values in the
             * current row of the history table.
             */
            for(CSet cset : pt.csets) {

                assert cset.cats.length == pt.cnames.length : "cset categories="
                        + Arrays.toString(cset.cats) + " vs "
                        + "category names: " + Arrays.toString(pt.cnames);
                
                /*
                 * Aggregate values for counters in this cset having a value for
                 * each value column in turn.
                 * 
                 * If none of the counters in the cset have a value for the row
                 * in the data table then we will not display a row in the
                 * output table for this cset. However, there can still be other
                 * csets which do select counters in the data table for which
                 * there are samples and that would be displayed under the
                 * output for for their cset.
                 */
                
                final Double[] vals = new Double[pt.vcols.size()];
                
                // #of value columns having a value.
                int ndefined = 0;
                
                // index of the current value column.
                int valueColumnIndex = 0;
                
                // for each value column.
                for (String vcol : pt.vcols) {

                    // #of values aggregated for this value column.
                    int valueCountForColumn = 0;
                    
                    // The aggregated value for this value column.
                    double val = 0d;

                    // consider each counter in the cset for this output row.
                    for (ICounter c : cset.counters) {

                        if (!c.getName().equals(vcol)) {

                            // not for this value column (skip over).
                            continue;

                        }

                        // find the index for that counter in the data table.
                        for (int col = 0; col < t.a.length; col++) {

                            if (c != t.a[col])
                                continue;

                            // get the sample from the data table.
                            final IHistoryEntry e = t.data[row][col];

                            if (e == null) {

                                // no sampled value.
                                continue;
                                
                            }

                            // @todo catch class cast problems and ignore
                            // val.  @todo protected against overflow of
                            // double.
                            val += ((Number) e.getValue()).doubleValue();

                            valueCountForColumn++;
                            
                            /*
                             * The counter appears just once in the data table
                             * so we can stop once we find its index.
                             */
                            break;

                        }

                    } // next counter in CSet.

                    if (valueCountForColumn > 0) {

                        /*
                         * There was at least one sample for the current value
                         * column.
                         */
                        
                        // save the value.
                        vals[valueColumnIndex] = val;

                        // #of non-empty values in this row.
                        ndefined++;

                    }

                    if (log.isDebugEnabled() && valueCountForColumn > 0)
                        log.debug("vcol=" + vcol + ", vcol#="
                                + valueColumnIndex + ", #values="
                                + valueCountForColumn + ", val=" + val);
                    
                    valueColumnIndex++;

                } // next value column.

                if (ndefined == 0) {
                 
                    // no data for this row.
                    continue;
                    
                }

                w.write(unitStr+"\t");

                w.write(timeStr + "\t");

                for (int j = 0; j < pt.cnames.length; j++) {

                    w.write(cset.cats[j] + "\t");

                }

                for (int j = 0; j < vals.length; j++) {

                    final String s = vals[j] == null ? "" : Double
                            .toString(vals[j]);

                    w.write(s + "\t");

                }
                
                w.write("\n");
                
            }

        } // next row.
       
    }
    
}