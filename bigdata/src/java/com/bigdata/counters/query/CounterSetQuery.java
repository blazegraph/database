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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.bigdata.Banner;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.IHostCounters;
import com.bigdata.counters.IRequiredHostCounters;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.httpd.DummyEventReportingService;
import com.bigdata.counters.httpd.XHTMLRenderer.Model.ReportEnum;
import com.bigdata.counters.render.IRenderer;
import com.bigdata.counters.render.TabDelimitedHistoryTableRenderer;
import com.bigdata.counters.render.TabDelimitedPivotTableRenderer;
import com.bigdata.counters.render.TextValueFormatter;
import com.bigdata.counters.store.CounterSetBTree;
import com.bigdata.journal.Journal;

/**
 * Utility to query counter sets.
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
 *       
 *       @see IHostCounters
 *       @see IRequiredHostCounters
 */
public class CounterSetQuery {

    static protected final Logger log = Logger.getLogger(CounterSetQuery.class);

//    public final long fromTime;
//    public final long toTime;
//    public final TimeUnit unit;
//    public final Pattern pattern;
//    public final TimeUnit granularity;
//
//    /**
//     * 
//     * @param fromTime
//     *            The first time whose counters will be visited.
//     * @param toTime
//     *            The first time whose counters WILL NOT be visited.
//     * @param unit
//     *            The unit in which <i>fromTime</i> and <i>toTime</i> are
//     *            expressed.
//     * @param pattern
//     *            Only paths matched by the {@link Pattern} will be accepted
//     *            (optional).
//     * @param granularity
//     *            The unit of aggregation.
//     */
//    protected CounterSetQuery(long fromTime, long toTime, TimeUnit unit,
//            Pattern pattern, TimeUnit granularity) {
//
//        this.fromTime = fromTime;
//        this.toTime = toTime;
//        this.unit = unit;
//        this.pattern = pattern;
//        this.granularity = granularity;
//        
//    }
//
//    /**
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static class HistoryTableQuery extends CounterSetQuery {
//
//        public HistoryTableQuery(long fromTime, long toTime, TimeUnit unit,
//                Pattern pattern, TimeUnit granularity) {
//
//            super(fromTime, toTime, unit, pattern, granularity);
//            
//        }
//        
//    }
//
//    /**
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static class PivotTableQuery extends CounterSetQuery {
//    
//        public final String[] category;
//
//        /**
//         * 
//         * 
//         * @param pattern
//         *            The pattern used to specify the counters of interest and
//         *            the capturing groups which determined how the counters
//         *            will be aggregated.
//         * @param category
//         *            The ordered labels to be assigned to the category columns
//         *            (optional). When given, the order of the category names
//         *            parameters MUST correspond with the order of the capturing
//         *            groups in the <i>pattern</i>.
//         */
//        public PivotTableQuery(long fromTime, long toTime, TimeUnit unit,
//                Pattern pattern, TimeUnit granularity, String[] category) {
//
//            super(fromTime, toTime, unit, pattern, granularity);
//
//            this.category = category;
//
//        }
//        
//    }

    /**
     * The supported output formats.
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public enum FormatEnum {
     
        /**
         * XHTML.
         */
        html,
     
        /**
         * tab-delimited text.
         */
        text;
        
    }

    /**
     * Utility class for running extracting data from performance counter dumps
     * and running various kinds of reports on those data.
     * 
     * @param args
     *            Command line arguments.
     * 
     * @throws IOException
     * @throws ParserConfigurationException
     * @throws SAXException
     * 
     * @todo arg for the journal file.
     */
    public static void main(final String[] args) throws IOException,
            SAXException, ParserConfigurationException {

        Banner.banner();
        
        if (true) {

            /*
             * The performance counters read from the file(s).
             */
            final CounterSet counterSet = new CounterSet();

            /*
             * The events read from the file(s).
             */
            final DummyEventReportingService service = new DummyEventReportingService();

            /*
             * Optional filter by depth.  This is normally zero for reports. Non-zero
             * values are generally used for the navigational view to restrict the
             * #of descendant nodes that are visible at one time.
             */
            int depth = 0;

            // any -filter arguments.
            final Collection<String> filter = new LinkedList<String>();

            // any -regex arguments.
            final Collection<String> regex = new LinkedList<String>();

            /*
             * The ordered labels to be assigned to the category columns in a
             * pivot table report. The order of the names MUST correspond with
             * the order of the capturing groups in the regex.
             * 
             * @todo I will have to separate the use of the regex to filter the
             * counters which are read from the file from its use in a query if
             * you can run multiple queries in a single program run against a
             * filtered subset of the counter set XML data.
             */
            final List<String> category = new LinkedList<String>();

            /*
             * Note: fromTime=0, toTime=0, period=Minutes means all data
             * aggregated by minutes.
             * 
             * @todo args and apply as filter when reading the data from files,
             * when returning ICounter[]s from a CounterSet, and when querying
             * the data from a store.
             */
            final long fromTime = 0L;
            final long toTime = 0L;

            // The units in which the data will be reported.
            PeriodEnum period = PeriodEnum.Minutes;

            // The default report type.
            ReportEnum report = ReportEnum.correlated;

            // The output format (text, html, etc.)
            FormatEnum format = FormatEnum.text;

            /*
             * Figure out how we will format the timestamp (From:, To:, and the
             * last column).
             * 
             * @todo handle epoch here (for flot) and minutes, hours, days.
             * 
             * @todo use an arg for this
             */
            final DateFormat dateFormat = DateFormat
                    .getTimeInstance(DateFormat.SHORT);

            for (int i = 0; i < args.length; i++) {

                final String arg = args[i];

                if (arg.startsWith("-")) {

                    if (arg.equals("-depth")) {

                        depth = Integer.parseInt(args[++i]);

                    } else if (arg.equals("-report")) {

                        report = ReportEnum.valueOf(args[++i]);

                    } else if (arg.equals("-format")) {

                        format = FormatEnum.valueOf(args[++i]);

                    } else if (arg.equals("-filter")) {

                        filter.add(args[++i]);

                    } else if (arg.equals("-regex")) {

                        regex.add(args[++i]);

                    } else if (arg.equals("-category")) {

                        category.add(args[++i]);

                    } else if (arg.equals("-period")) {

                        period = PeriodEnum.valueOf(args[++i]);

                    } else if (arg.equals("-events")) {

                        QueryUtil.readEvents(service, new File(args[++i]));

                    } else {

                        System.err.println("Unknown option: " + arg);

                        System.exit(1);

                    }

                } else {

                    /*
                     * Compute the optional filter to be applied when reading
                     * this file and then read counters accepted by the optional
                     * filter into the counter set to be served.
                     */
                    final int nslots = 1000; // @todo arg
                    
                    final File file = new File(arg);
                    
                    if(file.isDirectory()) {
                    
                        System.err.println("Reading directory: "+file);
                        
                        final File[] files = file
                                .listFiles(new FilenameFilter() {

                                    public boolean accept(File dir, String name) {
                                        return name.endsWith(".xml");
                                    }
                                });

                        for (File f : files) {

                            System.err.println("Reading file: "+f);

                            QueryUtil.readCountersFromFile(f, counterSet,
                                    QueryUtil.getPattern(filter, regex),
                                    nslots, period);
                            
                        }
                        
                    } else {

                        System.err.println("Reading file: "+file);

                        QueryUtil
                            .readCountersFromFile( file, counterSet,
                                    QueryUtil.getPattern(filter, regex),
                                    nslots, period);
                    }

                }

            }

            /*
             * The regular expression formed from the mixture of filters and/or
             * regular expressions specified on the command line.
             */
            final Pattern pattern = QueryUtil.getPattern(filter, regex);
            
            // Select the relevant performance counters.
            final ICounter[] counters = new CounterSetSelector(counterSet)
                    .selectCounters(depth, pattern, fromTime, toTime, period);

            /*
             * FIXME Figure out aggregation for the history table or use the
             * pivot table for that purpose but then transform back to table for
             * plotting.
             */
            final HistoryTable historyTable = new HistoryTable(counters, period);

            final IRenderer renderer;

            switch (report) {
            case correlated: {
                /*
                 * normal (aka history) table.
                 */
                switch (format) {
                case text: {
                    renderer = new TabDelimitedHistoryTableRenderer(
                            historyTable, pattern, new TextValueFormatter(
                                    dateFormat));
                    break;
                }
                case html: {
                    /*
                     * @todo support HTML as well as tab-delimited here. This
                     * will require either a refactor of the HTMLValueFormatter
                     * to decouple it from XHTMLRenderer.Model or just parse a
                     * URL into the appropriate format.
                     */
                    // renderer = new HTMLHistoryTableRenderer(
                    // historyTable, pattern,
                    // new HTMLValueFormatter(dateFormat,model));
                }
                default:
                    throw new UnsupportedOperationException(format.toString());
                }
                break;
            }
            case pivot: {
                /*
                 * pivot table.
                 */
                final PivotTable pivotTable = new PivotTable(pattern, category
                        .toArray(new String[] {}), historyTable);
                switch (format) {
                case text: {
                    renderer = new TabDelimitedPivotTableRenderer(pivotTable,
                            new TextValueFormatter(dateFormat));
                    break;
                }
                case html: {
                    /*
                     * @todo support HTML as well as tab-delimited here. This
                     * will require either a refactor of the HTMLValueFormatter
                     * to decouple it from XHTMLRenderer.Model or just parse a
                     * URL into the appropriate format.
                     */
                }
                default:
                    throw new UnsupportedOperationException(format.toString());
                }
            }
            case events: {
                /*
                 * @todo support flot rendering of events
                 * 
                 * @todo support aggregation of events?
                 */
                throw new UnsupportedOperationException(report.toString());
            }
            case hierarchy: {
                /*
                 * @todo Is there any reason why would we want to render this
                 * view for reporting purposes?
                 */
                throw new UnsupportedOperationException(report.toString());
            }
            default:
                /*
                 * An unrecognized value.
                 */
                throw new UnsupportedOperationException(report.toString());
            }

            final StringWriter w = new StringWriter();

            // new PrintWriter(
            // System.out, true/* autoFlush */);

            renderer.render(w);

            System.out.print(w);

        } else {

            /*
             * @todo This is an alternative approach based on a journal on which
             * the desired performance counter data has been stored. However,
             * the current representation of performance counters on a journal
             * is not efficient for query or space. There are various notes on
             * this issue in this package and the com.bigdata.counters.store
             * package and on a possible refactor to make this space and time
             * efficient. Until then, this code is out of line.
             */

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
            final CounterSetBTree btree = (CounterSetBTree) store
                    .getIndex("counters");// , store.getLastCommitTime());

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

            // optional depth constraint.
            final int depth = 0;

            // sample pattern.
            final Pattern pattern = Pattern
                    .compile("/([^/]*)/.*IDataService.*/([^/]*)/([^/]*)/IO/(readSecs|writeSecs|serializeSecs|deserializeSecs)");

            // all data, aggregated by the minute.
            // final long fromTime = 0L;
            // final long toTime = 0L;
            // final TimeUnit unit = TimeUnit.MINUTES;

            // N minutes of data, starting from the fromTime. @todo drop +1 here
            // and below.
            final long fromTime = firstTimestamp + TimeUnit.MINUTES.toMillis(0);
            final long toTime = fromTime + TimeUnit.MINUTES.toMillis(60
            /* N */+ 1);
            final TimeUnit unit = TimeUnit.MINUTES;

            // N minutes of data, starting from the last reported value.
            // final long fromTime = lastTimestamp -
            // TimeUnit.MINUTES.toMillis(10);
            // final long toTime = lastTimestamp + TimeUnit.MINUTES.toMillis(1);
            // final TimeUnit unit = TimeUnit.MINUTES;

            // all data, aggregated for each hour.
            // final long fromTime = 0L;
            // final long toTime = 0L;
            // first N hours.
            // final long fromTime = firstTimestamp;
            // final long toTime = fromTime + TimeUnit.HOURS.toMillis(5/*N*/ +
            // 1);
            // final TimeUnit unit = TimeUnit.HOURS;

            /*
             * Read the relevant performance counter data from the store.
             */
            final ICounter[] counters = new CounterSetBTreeSelector(btree)
                    .selectCounters(depth, pattern, fromTime, toTime,
                            PeriodEnum.getValue(unit));

        }

    }

}
