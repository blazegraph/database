package com.bigdata.counters.render;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterNode;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.query.HistoryTable;
import com.bigdata.counters.query.PivotTable;
import com.bigdata.counters.query.URLQueryModel;

/**
 * Writes out a pivot table containing the histories for the selected
 * counters. Sample output is:
 * 
 * <pre>
 * Minutes  Timestamp    hostname indexName   indexPartitionName  readSecs    writeSecs   leafSplits  tailSplits
 * 0    1:41PM  blade8  spo spo#12  0.12    0.3 10  4
 * 0    1:41PM  blade8  spo spo#14  0.01    0.2 20  1
 * </pre>
 * 
 * There are two time columns. One gives the #of elapsed units and the
 * column header is the specified <i>basePeriod</i> and will be zero for
 * the first row and is incremented by one each time we move into another
 * sampling period for the given {@link PeriodEnum}. The other time column
 * is the timestamp associated with the row. The format of that timestamp is
 * specified by {@link URLQueryModel#timestampFormat}.
 * <p>
 * In this example, there are three category columns (hostname, indexName,
 * and indexPartitionName). The category columns were selected by the
 * capturing groups in {@link URLQueryModel#pattern} and take on different values
 * for each row in the table. In order to get nice column names you MUST
 * specify the column names using the {@link URLQueryModel#CATEGORY} URL query
 * parameter. The given category column names are used in order and
 * synthetic column names are generated if none (or not enough) were
 * specified in the {@link URLQueryModel}.
 * <p>
 * In this example, there are four value columns. Each vlaue column
 * corresponds to an {@link ICounter} whose path was matched by the
 * {@link URLQueryModel#pattern}.
 * <p>
 * Note: The orientation of the data in the pivot table view is different
 * from the data in the correlated view. The pivot table puts each set of
 * cells having the same values for the category columns onto a single line.
 * Those cells are choosen by a read cross the columns in a given row of the
 * {@link HistoryTable}. The timestamp columns correspond to the current
 * row of the history table and are therefore repeated if there is more than
 * one set of distinct ordered bindings for the category column values in
 * the selected counters.
 * <p>
 * Note: The counter sets are designed as regular hierarchies: we often find
 * the "same" counter name under different paths. For example:
 * 
 * <pre>
 *   indices/TERM2ID/TERM2ID#0/IO/writeSecs
 *   indices/TERM2ID/TERM2ID#2/IO/writeSecs
 *   indices/SPO/SPO#7/IO/writeSecs
 *   Live Journal/IO/writeSecs
 * </pre>
 * 
 * All of these have a "writeSecs" counter. However, the first 3 are in
 * essence the same counter, just for a different resource index, while the
 * last one is a counter for a journal resource.
 * <p>
 * The {@link Pattern}
 * 
 * <pre>
 * 
 * .*\Qindices/\E(.*)\Q/IO/writeSecs\E
 * 
 * </pre>
 * 
 * would match only the first three rows and would capture
 * 
 * <pre>
 * [TERM2ID/TERM2ID#0]
 * [TERM2ID/TERM2ID#2]
 * [SPO/SPO#7]
 * </pre>
 * 
 * You can also capture additional groups, such as the name of the host on
 * which the data service was running within which the index partition is
 * living. Those capture groups might look like:
 * 
 * <pre>
 * [blade2, TERM2ID/TERM2ID#0]
 * [blade3, TERM2ID/TERM2ID#2]
 * [blade2, SPO/SPO#7]
 * </pre>
 * 
 * There will be only one "writeSecs" value column in the generated pivot
 * table. The performance {@link ICounter} for a given resource will be
 * reported under that "writeSecs" column in a row whose category column
 * values are identical to the captured groups for that
 * {@link ICounterNode#getPath()}.
 * <p>
 * Note: When switching from a pivot table view to a correlated view be sure
 * that you DO NOT use a capturing group for the counter name (the last
 * component of its path). That will give you a distinct row for every
 * sample and blanks in the other value columns.
 * 
 * @param a
 *            The selected counters.
 * @param basePeriod
 *            Identifies the history to be written for each of the selected
 *            counters by its based reporting period.
 * @param timestampFormat
 *            The format in which to report the timestamp associated with
 *            the row.
 * 
 * @throws IllegalArgumentException
 *             if <i>w</i> is <code>null</code>.
 * @throws IllegalArgumentException
 *             if <i>a</i> is <code>null</code>.
 * @throws IllegalArgumentException
 *             if any element of <i>a</i> <code>null</code>.
 * @throws IllegalArgumentException
 *             if any element of <i>a</i> does not use a
 *             {@link HistoryInstrument}.
 * 
 * @todo automatically detect if the last capturing group captures the
 *       counter name and then drop that from the set of category columns.
 *       this will make it much easier to switch between a correlated view
 *       and a pivot view since you often want the counter name to be a
 *       capturing group for the pivot view.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class PivotTableRenderer implements IRenderer {
    
    protected static final Logger log = Logger.getLogger(PivotTableRenderer.class);
    
    protected final PivotTable pt;
    
    protected final ValueFormatter formatter;
    
    protected PivotTableRenderer(PivotTable pt, ValueFormatter formatter) {
        
        this.pt = pt;
        
        this.formatter = formatter;
        
    }
    
}