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
 * Created on Apr 6, 2009
 */

package com.bigdata.counters.query;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.counters.ICounter;
import com.bigdata.counters.IRequiredHostCounters;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.httpd.XHTMLRenderer;
import com.bigdata.counters.render.IRenderer;
import com.bigdata.counters.render.TabDelimitedHistoryTableRenderer;
import com.bigdata.counters.render.TabDelimitedPivotTableRenderer;
import com.bigdata.counters.render.TextValueFormatter;
import com.bigdata.counters.store.CounterSetBTree;
import com.bigdata.journal.Journal;

/**
 * Utility to query counter sets on a {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo How will navigational views be supported against a counter set on a
 *       store?
 * 
 * @todo Another useful query is to scan for time periods when a counter exceeds
 *       a threshold and report those counter values.
 * 
 * @todo Could report weighted averages, not just the raw values.
 * 
 * @todo flot output for graphs, including flot of averages, multi-line flot,
 *       etc.
 * 
 * @todo there really ought to be unit tests of the {@link HistoryTable} and the
 *       {@link PivotTable} against known data, e.g., as loaded from XML files
 *       or from a test journal resource.
 */
abstract public class CounterSetQuery {

    static protected final Logger log = Logger.getLogger(CounterSetQuery.class);

    public final long fromTime;
    public final long toTime;
    public final TimeUnit unit;
    public final Pattern pattern;
    public final TimeUnit granularity;

    /**
     * 
     * @param fromTime
     *            The first time whose counters will be visited.
     * @param toTime
     *            The first time whose counters WILL NOT be visited.
     * @param unit
     *            The unit in which <i>fromTime</i> and <i>toTime</i> are
     *            expressed.
     * @param pattern
     *            Only paths matched by the {@link Pattern} will be accepted
     *            (optional).
     * @param granularity
     *            The unit of aggregation.
     */
    protected CounterSetQuery(long fromTime, long toTime, TimeUnit unit,
            Pattern pattern, TimeUnit granularity) {

        this.fromTime = fromTime;
        this.toTime = toTime;
        this.unit = unit;
        this.pattern = pattern;
        this.granularity = granularity;
        
    }

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class HistoryTableQuery extends CounterSetQuery {

        public HistoryTableQuery(long fromTime, long toTime, TimeUnit unit,
                Pattern pattern, TimeUnit granularity) {

            super(fromTime, toTime, unit, pattern, granularity);
            
        }
        
    }

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class PivotTableQuery extends CounterSetQuery {
    
        public final String[] category;

        /**
         * 
         * 
         * @param pattern
         *            The pattern used to specify the counters of interest and
         *            the capturing groups which determined how the counters
         *            will be aggregated.
         * @param category
         *            The ordered labels to be assigned to the category columns
         *            (optional). When given, the order of the category names
         *            parameters MUST correspond with the order of the capturing
         *            groups in the <i>pattern</i>.
         */
        public PivotTableQuery(long fromTime, long toTime, TimeUnit unit,
                Pattern pattern, TimeUnit granularity, String[] category) {

            super(fromTime, toTime, unit, pattern, granularity);

            this.category = category;

        }
        
    }
    
    /**
     * 
     * @param args
     * @throws IOException
     * 
     * @todo arg for the journal file.
     * 
     * @todo args for the query parameters, but this is really more suited to
     *       testing than anything else as you can issue all of these queries
     *       using the {@link XHTMLRenderer}.
     */
    public static void main(final String[] args) throws IOException {

        final Properties properties = new Properties();

        final File file = new File("counters.jnl");

        if (!file.exists()) {

            System.err.println("No journal: " + file);

            System.exit(1);

        }

        // @todo config
        properties.setProperty(Journal.Options.FILE, file.toString());

        // (re-)open the store.
        final Journal store = new Journal(properties);

        // (re-)open the counter set B+Tree on the store.
        final CounterSetBTree btree = (CounterSetBTree) store.getIndex(
                "counters");//, store.getLastCommitTime());

        if (btree == null) {

            System.err.println("No counters: " + file);

            System.exit(1);

        }

        System.out.println("There are " + btree.rangeCount()
                + " counter values covering "
                + new Date(btree.getFirstTimestamp()) + " to "
                + new Date(btree.getLastTimestamp()));
        
        final long firstTimestamp = btree.getFirstTimestamp();
        
        final long lastTimestamp = btree.getLastTimestamp();

        /*
         * Setup the query parameters.
         */
        
        // all data, aggregated by the minute.
//        final long fromTime = 0L;
//        final long toTime = 0L;
//        final TimeUnit unit = TimeUnit.MINUTES;

        // N minutes of data, starting from the fromTime. @todo drop +1 here and below.
        final long fromTime = firstTimestamp + TimeUnit.MINUTES.toMillis(0);
        final long toTime = fromTime + TimeUnit.MINUTES.toMillis(60
                /* N */+ 1);
        final TimeUnit unit = TimeUnit.MINUTES;
        
        // N minutes of data, starting from the last reported value.
//        final long fromTime = lastTimestamp - TimeUnit.MINUTES.toMillis(10);
//        final long toTime = lastTimestamp + TimeUnit.MINUTES.toMillis(1);
//        final TimeUnit unit = TimeUnit.MINUTES;
    
        // all data, aggregated for each hour.
//        final long fromTime = 0L;
//        final long toTime = 0L;
        // first N hours.
//        final long fromTime = firstTimestamp;
//        final long toTime = fromTime + TimeUnit.HOURS.toMillis(5/*N*/ + 1);
//        final TimeUnit unit = TimeUnit.HOURS;
        
        final Pattern pattern;

        // CPU Time by host
//      pattern = Pattern.compile("/([^/]*)/CPU/.*Processor Time");
      
        // major page faults by host
//        pattern = Pattern.compile("/([^/]*)/"
//                + IRequiredHostCounters.Memory_majorFaultsPerSecond);
      
        // swapping
//        pattern = Pattern
//                .compile("/([^/]*)/.*/([^/]*DataService)/Memory/Major Page Faults Per Second");

        /*
         * Pattern extracts the index read and write time counters and
         * aggregates them by index partition and host (if you use a
         * pivot table; otherwise it has too many columns to be useful
         * as there are 4 columns per index partition).
         *
         * @todo excellent opportunity to add aggregation to the history table.
         * 
/blade8.dpp2.org/service/com.bigdata.service.IDataService/91ba0bfd-3285-452a-8a05-90965b07376e/Resource Manager/Index Manager/indices/U8000.spo.SPO/U8000.spo.SPO#23/IO/deserializeSecs 
         */
        pattern = Pattern.compile("/([^/]*)/.*IDataService.*/([^/]*)/([^/]*)/IO/(readSecs|writeSecs|serializeSecs|deserializeSecs)");

        /*
         * Live journal (readSecs|writeSecs) for data services only, grouped by host.
         * 
         * @todo group by data service to see which one is hot on a host.
         * 
         * /blade10.dpp2.org/service/com.bigdata.service.IDataService/63dcfde8-cd48-4faf-b156-2b2487d72e3a/Resource Manager/Live Journal/disk
         */
//        pattern = Pattern.compile("/([^/]*)/.*IDataService.*/.*/Live Journal/disk/(readSecs|writeSecs)");

        // optional filter by depth.
        final int depth = 0;

        /*
         * Read the relevant performance counter data from the store.
         */
        final ICounter[] counters = new CounterSetBTreeSelector(btree)
                .selectCounters(depth, pattern, fromTime, toTime, PeriodEnum
                        .getValue(unit));

        /*
         * Figure out how we will format the timestamp (From:, To:, and the last
         * column).
         * 
         * @todo handle epoch here (for flot) and minutes, hours, days.
         */
        final DateFormat dateFormat = DateFormat
                .getTimeInstance(DateFormat.SHORT);

        /*
         * FIXME Figure out aggregation for the history table or use the pivot
         * table for that purpose but then transform back to table for plotting.
         */
        final HistoryTable historyTable = new HistoryTable(counters, PeriodEnum
                .getValue(unit));
        
        /*
         * @todo support HTML as well as tab-delimited here. This will require
         * either a refactor of the HTMLValueFormatter to decouple it from
         * XHTMLRenderer.Model or just parse a URL into the appropriate format.
         */
        final IRenderer renderer;

        if (false) {

            /*
             * normal (aka history) table.
             */
            
            renderer = new TabDelimitedHistoryTableRenderer(historyTable,
                    pattern, new TextValueFormatter(dateFormat));

            //                      new HTMLHistoryTableRenderer(tbl, pattern,
            //          new HTMLValueFormatter(dateFormat));

        } else {

            /*
             * pivot table.
             */
            
            // generated labels will be used for the category columns.
//            final String[] category = null;

            // use the specified labels for the category columns.
            final String[] category = new String[]{"host","indexName","indexPartition"};
            
            final PivotTable pivotTable = new PivotTable(pattern, category,
                    historyTable);

            renderer = new TabDelimitedPivotTableRenderer(pivotTable,
                    new TextValueFormatter(dateFormat));

        }

        final StringWriter w = new StringWriter();

        //        new PrintWriter(
        //                System.out, true/* autoFlush */);

        renderer.render(w);

        System.out.print(w);

    }

}
