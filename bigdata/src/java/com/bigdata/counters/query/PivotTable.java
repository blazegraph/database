package com.bigdata.counters.query;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterNode;
import com.bigdata.counters.IHistoryEntry;
import com.bigdata.counters.httpd.XHTMLRenderer.Model;

/**
 * Aggregates data from a table by grouping the cells in the table into sets ({@link CSet}s)
 * of category columns. The values for cells belonging to the same
 * {@link CSet} are aggregated for each distinct
 * {@link ICounterNode#getName()}. 
 */
public class PivotTable {

    protected static final Logger log = Logger.getLogger(PivotTable.class);

    /**
     * The HistoryTable (converts counter heirarchy into regular table).
     */
    public final HistoryTable src;

    /**
     * The selected counters (redundent reference to {@link HistoryTable#a}.
     */
    public final ICounter[] a;

    /**
     * The ordered set of distinct counter names. The order of the selected
     * counters is preserved here (minus duplicate counter names) due to the
     * virtues of the linked hash set.
     */
    public final LinkedHashSet<String> vcols;

    /**
     * Aggregation of the selected counters ({@link #a}) into sets sharing
     * the same category values.
     */
    public final List<CSet> csets;

    /**
     * An array of category column names. The names can be specified using
     * URL query parameters. When they are not specified or when there are
     * not enough specified parameters then we use some generated names.
     * 
     * @see Model#CATEGORY
     */
    public final String[] cnames;

    /**
     * 
     * @param pattern
     *            The pattern used to specify the counters of interest and the
     *            capturing groups which determined how the counters will be
     *            aggregated.
     *            <p>
     *            If a capturing group is used for the counter name then that
     *            capturing group will be ignored. This makes it easier switch
     *            back and forth between a {@link PivotTable} and a
     *            {@link HistoryTable}. If the counter name was in fact treated
     *            as a capturing group for the purposes of determining the
     *            category columns, then that would give you a distinct row for
     *            every sample and blanks in the other value columns.
     * @param category
     *            The ordered labels to be assigned to the category columns
     *            (optional). When given, the order of the category names
     *            parameters MUST correspond with the order of the capturing
     *            groups in the <i>pattern</i>.
     * @param t
     *            The source data.
     */
    public PivotTable(final Pattern pattern, final String[] category,
            final HistoryTable t) {

        if (t == null)
            throw new IllegalArgumentException();

        // the HistoryTable (converts counter heirarchy into regular table).
        this.src = t;

        // the selected counters (used to derived the HistoryTable).
        this.a = t.a;

        /*
         * The ordered set of distinct counter names. The order of the selected
         * counters is preserved here (minus duplicate counter names) due to the
         * virtues of the linked hash set.
         */
        vcols = new LinkedHashSet<String>();
        for (int i = 0; i < a.length; i++) {

            vcols.add(a[i].getName());

        }

        if (log.isInfoEnabled())
            log.info("vnames: " + vcols);

        // #of capturing groups in the pattern (via side-effect).
        final int ngroups;
        
        // aggregate counters into sets sharing the same category values.
        {
        
            final AtomicInteger tmp = new AtomicInteger();
            
            csets = getCategoryValueSets(pattern, a, tmp);
            
            ngroups = tmp.get();
            
            if (log.isInfoEnabled())
                log.info("csets: " + csets);

//            // #of capturing groups in the pattern.
//            final int ngroups = pattern.matcher("").groupCount();

            if (log.isInfoEnabled())
                log.info("ngroups=" + ngroups);

        }

        /*
         * An array of category column names. The names can be specified using
         * URL query parameters. When they are not specified or when there are
         * not enough specified parameters then we use some generated names.
         */
        cnames = new String[ngroups];

        for (int i = 0; i < ngroups; i++) {

            if (category != null && category.length > i) {

                cnames[i] = category[i];

            } else {

                cnames[i] = "group#" + i;

            }

        }

        if (log.isInfoEnabled())
            log.info("category names=" + Arrays.toString(cnames));

        // for each row in the HistoryTable.
        for (int row = 0; row < t.nrows; row++) {

            // The timestamp for the row.
            final long timestamp = t.getTimestamp(row);

            /*
             * The set of distinct ordered matched category values in the
             * current row of the history table.
             */
            for (CSet cset : csets) {

                assert cset.cats.length == cnames.length : "cset categories="
                        + Arrays.toString(cset.cats) + " vs "
                        + "category names: " + Arrays.toString(cnames);

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

                final Double[] vals = new Double[vcols.size()];

                // #of value columns having a value.
                int ndefined = 0;

                // index of the current value column.
                int valueColumnIndex = 0;

                // for each value column.
                for (String vcol : vcols) {

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
                        for (int col = 0; col < a.length; col++) {

                            if (c != a[col])
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

                // @todo else output a PivotRow.
                //                    
                //                    new PivotRow(row, timestamp,cset,vals);

            }

        } // next row.

    }

    /**
     * A row in a {@link PivotTable}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * FIXME Not yet in use. Either finish or remove this abstraction.
     */
    class PivotRow {

        /**
         * The row of the source {@link HistoryTable} whose aggregated
         * values are captured by the row of the pivot table.
         */
        final int row;

        /**
         * The timestamp associated with the data in the row.
         */
        final long timestamp;

        /**
         * The category set for this row. The values for the category
         * columns in the row are {@link CSet#cats}.
         */
        final CSet cset;

        /**
         * The value columns for the row. There is one element in the array
         * for each element in {@link PivotTable#vcols}. The element MAY be
         * <code>null</code> in which case there was no data for that
         * counter for this row.
         */
        final Double[] values;

        /**
         * 
         * @param row
         *            The row of the source {@link HistoryTable} whose
         *            aggregated values are captured by the row of the pivot
         *            table.
         * @param timestamp
         *            The timestamp associated with the data in the row.
         * @param cset
         *            The category set for this row. The values for the
         *            category columns in the row are {@link CSet#cats}.
         * @param values
         *            The value columns for the row. There is one element in
         *            the array for each element in {@link PivotTable#vcols}.
         *            The element MAY be <code>null</code> in which case
         *            there was no data for that counter for this row.
         */
        PivotRow(final int row, final long timestamp, final CSet cset,
                final Double[] values) {

            if (cset == null)
                throw new IllegalArgumentException();

            if (cset.cats.length != cnames.length)
                throw new IllegalArgumentException();

            if (values == null)
                throw new IllegalArgumentException();

            if (values.length != vcols.size())
                throw new IllegalArgumentException();

            this.row = row;

            this.timestamp = timestamp;

            this.cset = cset;

            this.values = values;

        }

    }

    /**
     * The set of distinct ordered matched sets of category values in the
     * current row of the history table paired with the {@link ICounter}s
     * matched up on those category values.
     * <p>
     * Note: This automatically detects if the last capturing group captures the
     * counter name and then drop that from the set of category columns. This
     * makes it much easier to switch between a correlated view and a pivot view
     * since you often want the counter name to be a capturing group for the
     * correlated view.
     * 
     * @param ngroups
     *            The #of capturing groups which were actually used (by
     *            side-effect). This is either the #of capturing groups which
     *            were specified in the <i>pattern</i> -or- one less than that
     *            value iff the last capturing group captures the counter name.
     */
    static protected List<CSet> getCategoryValueSets(final Pattern pattern,
            final ICounter[] a, final AtomicInteger ngroups) {

        if (a == null)
            throw new IllegalArgumentException();

        // maximum result is one set per counter.
        final String[][] sets = new String[a.length][];

        // #of capturing groups that were actually used.
        int usedGroupCount = 0;
        
        for (int i = 0; i < a.length; i++) {

            final ICounter c = a[i];

            if (a[i] == null)
                throw new IllegalArgumentException();

            final String[] groups = QueryUtil.getCapturedGroups(pattern, c);
            
            final int n = groups.length;

            if (n > 0 && c.getName().equals(groups[n - 1])) {

                /*
                 * We drop the last capturing group since it captures the
                 * counter name. This is a common query design when building a
                 * normal table view, but using a capturing group for the
                 * counter name for a pivot table will result in a single
                 * counter value per row. By dropping the capturing group
                 * corresponding to the counter name we make it easier to reuse
                 * the same query for both normal table and pivot table views.
                 */
                
                sets[i] = new String[n - 1];

                System.arraycopy(groups, 0, sets[i], 0, n - 1);

                usedGroupCount = Math.max(usedGroupCount, n - 1);
                
            } else {
                
                sets[i] = groups;
                
                usedGroupCount = Math.max(usedGroupCount, n);

            }

        }

        // return value via side-effect.
        ngroups.set(usedGroupCount);
        
        /*
         * Now figure out which of those sets are distinct. Each time we find a
         * set that duplicates the current set we clear its reference. After
         * each set has been checked for duplicates in the set of sets we move
         * on to the next set whose reference has not been cleared. We are done
         * when all references in [sets] have been cleared.
         */
        final List<CSet> csets = new LinkedList<CSet>();

        for (int i = 0; i < sets.length; i++) {

            final String[] set = sets[i];

            if (set == null) // already cleared.
                continue;

            final CSet cset = new CSet(set, a[i]);

            // add to the collection that we will return.
            csets.add(cset);

            // and clear any duplicates in [sets].
            for (int j = i + 1; j < sets.length; j++) {

                final String[] oset = sets[j];

                if (oset == null) // already cleared.
                    continue;

                // all sets must be the same size.
                assert oset.length == set.length;

                // assume same set until proven otherwise.
                boolean same = true;
                for (int k = 0; k < set.length && same; k++) {

                    if (!set[k].equals(oset[k])) {

                        // not the same set : will terminate loop.
                        same = false;

                    }

                }

                if (same) {

                    // clear oset reference since it is a duplicate.
                    sets[j] = null;

                    // add another counter to that set.
                    cset.add(a[j]);

                }

            }

        }

        return csets;

    }

}
