package com.bigdata.counters.httpd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.History;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterNode;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IHistoryEntry;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.httpd.XHTMLRenderer.Model.ReportEnum;
import com.bigdata.counters.httpd.XHTMLRenderer.Model.TimestampFormatEnum;
import com.bigdata.counters.query.CSet;
import com.bigdata.counters.query.CounterSetSelector;
import com.bigdata.counters.query.HistoryTable;
import com.bigdata.counters.query.PivotTable;
import com.bigdata.counters.query.QueryUtil;
import com.bigdata.counters.render.HTMLHistoryTableRenderer;
import com.bigdata.counters.render.PivotTableRenderer;
import com.bigdata.counters.render.ValueFormatter;
import com.bigdata.service.Event;
import com.bigdata.service.IEventReportingService;
import com.bigdata.service.IService;
import com.bigdata.util.HTMLUtility;

/**
 * (X)HTML rendering of a {@link CounterSet}.
 * 
 * @todo UI widgets for regex filters, depth, correlated.
 * 
 * @todo make documentation available on the counters via click through on their
 *       name.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class XHTMLRenderer {
    
    final static protected Logger log = Logger.getLogger(XHTMLRenderer.class);
    
    final public static String ps = ICounterSet.pathSeparator;

    public static class Model {
        
        /**
         * Name of the URL query parameter specifying the starting path for the page
         * view.
         */
        static final String PATH = "path";

        /**
         * Depth to be displayed from the given path -or- zero (0) to display
         * all levels.
         */
        static final String DEPTH = "depth";
        
        /**
         * URL query parameter whose value is the type of report to generate.
         * The default is {@link ReportEnum#hierarchy}.
         * 
         * @see ReportEnum
         */
        static final String REPORT = "report";
        
        /**
         * The different kinds of reports that we can generate.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static enum ReportEnum {
           
            /**
             * This is the navigational view of the performance counter
             * hierarchy.
             */
            hierarchy,
            
            /**
             * A correlated view will be generated for the spanned counters
             * showing all samples for each counter in a column where the rows
             * are all for the same sample period. Counters without history are
             * elided in this view. This is useful for plotting timeseries.
             */
            correlated,
            
            /**
             * This is a pivot table ready view, which is useful for aggregating
             * the performance counter data in a variety of ways.
             */
            pivot,
            
            /**
             * Plot a timeline of events.
             */
            events;
            
        }
        
        /**
         * The ordered labels to be assigned to the category columns in a
         * {@link ReportEnum#pivot} report. The order of the names in the URL
         * query parameters MUST correspond with the order of the capturing
         * groups in the {@link #REGEX}.
         */
        static final String CATEGORY = "category";
        
        /**
         * Name of the URL query parameter specifying whether the optional
         * correlated view for counter histories will be displayed.
         * <p>
         * Note: This is a shorthand for specifying {@link #REPORT} as
         * {@value ReportEnum#correlated}.
         */
        static final String CORRELATED = "correlated";
        
        /**
         * Name of the URL query parameter specifying one or more strings for
         * the filter to be applied to the counter paths.
         */
        static final String FILTER = "filter";

        /**
         * Name of the URL query parameter specifying one or more regular
         * expression for the filter to be applied to the counter paths. Any
         * capturing groups in this regular expression will be used to generate
         * the column title when examining correlated counters in a table view.
         * If there are no capturing groups then the counter name is used as the
         * default title.
         */
        static final String REGEX = "regex";

        /**
         * Name of the URL query parameter specifying that the format for the
         * first column of the history counter table view. This column is the
         * timestamp associated with the counter but it can be reported in a
         * variety of ways. The possible values for this option are specified by
         * {@link TimestampFormatEnum}.
         * 
         * @see TimestampFormatEnum
         */
        static final String TIMESTAMP_FORMAT = "timestampFormat";

        /**
         * The reporting period to be displayed. When not specified, all periods
         * will be reported. The value may be any {@link PeriodEnum}.
         */
        static final String PERIOD = "period";
        
        /**
         * A collection of event filters. Each filter is a regular expression.
         * The key is the {@link Event} {@link Field} to which the filter will
         * be applied. The events filters are specified using URL query
         * parameters having the general form: <code>events.column=regex</code>.
         * For example,
         * 
         * <pre>
         * events.majorEventType = AsynchronousOverflow
         * </pre>
         * 
         * would select just the asynchronous overflow events and
         * 
         * <pre>
         * events.hostname=blade12.*
         * </pre>
         * 
         * would select events reported for blade12.
         */
        public final HashMap<Field,Pattern> eventFilters = new HashMap<Field, Pattern>();

        /**
         * The <code>eventOrderBy=fld</code> URL query parameters specifies
         * the sequence in which events should be grouped. The value of the
         * query parameter is an ordered list of the names of {@link Event}
         * {@link Field}s. For example:
         * 
         * <pre>
         * eventOrderBy=majorEventType &amp; eventOrderOrderBy=hostname
         * </pre>
         * 
         * would group the events first by the major event type and then by the
         * hostname. All events for the same {@link Event#majorEventType} and
         * the same {@link Event#hostname} would appear on the same Y value.
         * <p>
         * If no value is specified for this URL query parameter then the
         * default is as if {@link Event#hostname} was specified.
         */
        static final String EVENT_ORDER_BY = "eventOrderBy";
        
        /**
         * The order in which the events will be grouped.
         * 
         * @see #EVENT_ORDER_BY
         */
        public final Field[] eventOrderBy;
        
        /**
         * Type-safe enum for the options used to render the timestamp of the
         * row in a history or correlated history.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        enum TimestampFormatEnum {

            /**
             * 
             */
            dateTime,
            
            /**
             * Report the timestamp of the counter value in milliseconds since
             * the epoch (localtime).
             */
            epoch;

        };

//        /**
//         * The service reference IFF one was specified when
//         * {@link CounterSetHTTPD} was started.
//         */
//        final public IService service;
        
        /**
         * Object used to select the performance counters of interest.
         */
        final public ICounterSelector counterSelector;
        
        /**
         * The URI from the request.
         */
        final public String uri;
        
        /**
         * The parameters from the request (eg, as parsed from URL query
         * parameters).
         */
        final public LinkedHashMap<String,Vector<String>> params;
        
        /**
         * The request headers.
         */
        final public Properties headers;
        
        /**
         * The value of the {@link #PATH} query parameter. 
         */
        final public String path;
        
        /**
         * The value of the {@link #DEPTH} query parameter.
         */
        final public int depth;
        
        /**
         * The kind of report to generate.
         * 
         * @see #REPORT
         * @see ReportEnum
         */
        final public ReportEnum reportType;
        
        /**
         * @see #timestampFormat
         * @see TimestampFormatEnum
         */
        final public TimestampFormatEnum timestampFormat;

        /**
         * The ordered labels to be assigned to the category columns in a
         * {@link ReportEnum#pivot} report (optional). The order of the names in
         * the URL query parameters MUST correspond with the order of the
         * capturing groups in the {@link #REGEX}.
         * 
         * @see #CATEGORY
         */
        final public String[] category;
        
        /**
         * The inclusive lower bound in milliseconds of the timestamp for the
         * counters or events to be selected.
         */
        final long fromTime;

        /**
         * The exclusive upper bound in milliseconds of the timestamp for the
         * counters or events to be selected.
         */
        final long toTime;
        
        /**
         * The reporting period to be used. When <code>null</code> all periods
         * will be reported. When specified, only that period is reported.
         */
        final public PeriodEnum period;
        
        /**
         * The {@link Pattern} compiled from the {@link #FILTER} query
         * parameters and <code>null</code> iff there are no {@link #FILTER}
         * query parameters.
         */
        final public Pattern pattern;

        /**
         * The events iff they are available from the service.
         * 
         * @see IEventReportingService
         */
//        final LinkedHashMap<UUID, Event> events;
        final IEventReportingService eventReportingService;
        
        /**
         * <code>true</code> iff we need to output the scripts to support
         * flot.
         */
        final boolean flot;
        
        /**
         * Used to format double and float counter values.
         */
        final DecimalFormat decimalFormat;
        
        /**
         * Used to format counter values that can be inferred to be a percentage.
         */
        final NumberFormat percentFormat;
        
        /**
         * Used to format integer and long counter values.
         */
        final NumberFormat integerFormat;
        
        /**
         * Used to format the units of time when expressed as elapsed units since
         * the first sample of a {@link History}.
         */
        final DecimalFormat unitsFormat;
//        final DateFormat dateFormat;
        
        /**
         * @param fed
         *            The service object IFF one was specified when
         *            {@link CounterSetHTTPD} was started.
         * @param counterSelector
         *            Object used to select the performance counters of
         *            interest.
         * @param uri
         *            Percent-decoded URI without parameters, for example
         *            "/index.cgi"
         * @param parms
         *            Parsed, percent decoded parameters from URI and, in case
         *            of POST, data. The keys are the parameter names. Each
         *            value is a {@link Collection} of {@link String}s
         *            containing the bindings for the named parameter. The order
         *            of the URL parameters is preserved.
         * @param header
         *            Header entries, percent decoded
         */
        public Model(final IService service, final ICounterSelector counterSelector,
                final String uri,
                final LinkedHashMap<String, Vector<String>> params,
                final Properties headers) {

//            this.service = service;
            
            this.counterSelector = counterSelector;

            this.uri = uri;

            this.params = params;
            
            this.headers = headers;

            this.path = getProperty(params, PATH, ps);

            if (log.isInfoEnabled())
                log.info(PATH + "=" + path);

            this.depth = Integer.parseInt(getProperty(params, DEPTH, "2"));
            
            if (log.isInfoEnabled())
                log.info(DEPTH + "=" + depth);
            
            if (depth <= 0)
                throw new IllegalArgumentException("depth must be GT ZERO(0)");

            /*
             * FIXME fromTime and toTime are not yet being parsed. They should
             * be interpreted so as to allow somewhat flexible specification and
             * should be applied to both performance counter views and event
             * views.
             */
            fromTime = 0L;
            toTime = Long.MAX_VALUE;

            // assemble the optional filter.
            this.pattern = QueryUtil.getPattern(//
                    params.get(FILTER),//
                    params.get(REGEX)//
                    );

            if (service != null && service instanceof IEventReportingService) {

                // events are available.
                eventReportingService = ((IEventReportingService) service);

            } else {

                // events are not available.
                eventReportingService = null;
                
            }
            
            if (params.containsKey(REPORT) && params.containsKey(CORRELATED)) {

                throw new IllegalArgumentException("Please use either '"
                        + CORRELATED + "' or '" + REPORT + "'");
                
            }

            if(params.containsKey(REPORT)) {
            
                this.reportType = ReportEnum.valueOf(getProperty(
                    params, REPORT, ReportEnum.hierarchy.toString()));

                if (log.isInfoEnabled())
                    log.info(REPORT + "=" + reportType);
                
            } else {
            
                final boolean correlated = Boolean.parseBoolean(getProperty(
                        params, CORRELATED, "false"));
            
                if (log.isInfoEnabled())
                    log.info(CORRELATED + "=" + correlated);

                this.reportType = correlated ? ReportEnum.correlated
                        : ReportEnum.hierarchy;

            }

            if (eventReportingService != null) {
                
                final Iterator<Map.Entry<String, Vector<String>>> itr = params
                        .entrySet().iterator();
                
                while(itr.hasNext()) {
                    
                    final Map.Entry<String, Vector<String>> entry = itr.next();
                    
                    final String name = entry.getKey();
                    
                    if (!name.startsWith("events."))
                        continue;
                    
                    final int pos = name.indexOf('.');

                    if (pos == -1) {

                        throw new IllegalArgumentException(
                                "Missing event column name: " + name);
                        
                    }
                    
                    // the name of the event column.
                    final String col = name.substring(pos + 1, name.length());
                    
                    final Field fld;
                    try {
                        
                        fld = Event.class.getField(col);
                        
                    } catch(NoSuchFieldException ex) {

                        throw new IllegalArgumentException("Unknown event field: "+col);
                        
                    }

                    final Vector<String> patterns = entry.getValue();
                    
                    if (patterns.size() == 0)
                        continue;

                    if (patterns.size() > 1)
                        throw new IllegalArgumentException(
                                "Only one pattern per field: " + name);
                    
                    /*
                     * compile the pattern
                     * 
                     * Note: Throws PatternSyntaxException if the pattern can
                     * not be compiled.
                     */
                    final Pattern pattern = Pattern.compile(patterns.firstElement());
                    
                    eventFilters.put(fld, pattern);
                    
                }

                if (log.isInfoEnabled()) {
                    final StringBuilder sb = new StringBuilder();
                    for (Field f : eventFilters.keySet()) {
                        sb.append(f.getName() + "=" + eventFilters.get(f));
                    }
                    log.info("eventFilters={" + sb + "}");
                }
                
            }
            
            // eventOrderBy
            {
                
                final Vector<String> v = params.get(EVENT_ORDER_BY);

                if (v == null) {

                    /*
                     * Use a default for eventOrderBy.
                     */

                    try {

                        eventOrderBy = new Field[] { Event.class
                                .getField("hostname") };

                    } catch (Throwable t) {

                        throw new RuntimeException(t);

                    }

                } else {

                    final Vector<Field> fields = new Vector<Field>();

                    for (String s : v) {

                        try {

                            fields.add(Event.class.getField(s));

                        } catch (Throwable t) {

                            throw new RuntimeException(t);

                        }

                    }

                    eventOrderBy = fields.toArray(new Field[0]);

                }
               
                if (log.isInfoEnabled())
                    log.info(EVENT_ORDER_BY + "="
                            + Arrays.toString(eventOrderBy));

            }
            
            switch (reportType) {
            case events:
                if (eventReportingService == null) {

                    /*
                     * Throw exception since the report type requires events but
                     * they are not available.
                     */

                    throw new IllegalStateException("Events are not available.");

                }
                flot = true;
                break;
            default:
                flot = false;
                break;
            }
            
            this.category = params.containsKey(CATEGORY) ? params.get(CATEGORY)
                    .toArray(new String[0]) : null;

            if (log.isInfoEnabled() && category != null)
                log.info(CATEGORY + "=" + Arrays.toString(category));

            this.timestampFormat = TimestampFormatEnum.valueOf(getProperty(
                    params, TIMESTAMP_FORMAT, TimestampFormatEnum.dateTime.toString()));
            
            if (log.isInfoEnabled())
                log.info(TIMESTAMP_FORMAT + "=" + timestampFormat);

            this.period = PeriodEnum.valueOf(getProperty(params, PERIOD,
                    PeriodEnum.Minutes.toString()/* defaultValue */));

            if (log.isInfoEnabled())
                log.info(PERIOD + "=" + period);

            /*
             * @todo this should be parameter whose default is set on the server and
             * which can be overriden by a URL query parameter (.
             */
//            this.decimalFormat = new DecimalFormat("0.###E0");
            this.decimalFormat = new DecimalFormat("##0.#####E0");
            
//            decimalFormat.setGroupingUsed(true);
    //
//            decimalFormat.setMinimumFractionDigits(3);
//            
//            decimalFormat.setMaximumFractionDigits(6);
//            
//            decimalFormat.setDecimalSeparatorAlwaysShown(true);
            
            this.percentFormat = NumberFormat.getPercentInstance();
            
            this.integerFormat = NumberFormat.getIntegerInstance();
            
            integerFormat.setGroupingUsed(true);
            
            this.unitsFormat = new DecimalFormat("0.#");
            
        }

        /**
         * Return the first value for the named property.
         * 
         * @param params
         *            The request parameters.
         * @param property
         *            The name of the property
         * @param defaultValue
         *            The default value (optional).
         * 
         * @return The first value for the named property and the defaultValue
         *         if there named property was not present in the request.
         * 
         * @todo move to a request object?
         */
        public String getProperty(final Map<String, Vector<String>> params,
                final String property, final String defaultValue) {
            
            if (params == null)
                throw new IllegalArgumentException();

            if (property == null)
                throw new IllegalArgumentException();

            final Vector<String> vals = params.get(property);

            if (vals == null)
                return defaultValue;

            return vals.get(0);
            
        }

        /**
         * Re-create the request URL, including the protocol, host, port, and
         * path but not any query parameters.
         */
        public StringBuilder getRequestURL() {
            
            final StringBuilder sb = new StringBuilder();

            // protocol
            sb.append("http://");
            
            // host and port
            sb.append(headers.get("host"));
            
            // path (including the leading '/')
            sb.append(uri);

            return sb;

        }

        /**
         * Re-create the request URL.
         * 
         * @param override
         *            Overriden query parameters (optional).
         *            
         * @todo move to request object?
         */
        public String getRequestURL(final NV[] override) {
        
            // Note: Used throughput to preserve the parameter order.
            final LinkedHashMap<String,Vector<String>> p;
            
            if(override == null) {
                
                p = params;
                
            } else {
                
                p = new LinkedHashMap<String,Vector<String>>(params);
                
                for(NV x : override) {
                                            
                    p.put(x.name, x.values);
                    
                }
                
            }

            final StringBuilder sb = getRequestURL();
            
            sb.append("?path=" + encodeURL(getProperty(p, PATH, ps)));
            
            final Iterator<Map.Entry<String, Vector<String>>> itr = p
                    .entrySet().iterator();
            
            while(itr.hasNext()) {
                
                final Map.Entry<String, Vector<String>> entry = itr.next();

                final String name = entry.getKey();

                if (name.equals(PATH)) {

                    // already handled.
                    continue;

                }

                final Collection<String> vals = entry.getValue();

                for (String s : vals) {

                    sb.append("&" + encodeURL(name) + "=" + encodeURL(s));

                }
                
            }
            
            return sb.toString();

        }

        static public String encodeURL(final String url) {

            final String charset = "UTF-8";

            try {

                return URLEncoder.encode(url, charset);

            } catch (UnsupportedEncodingException e) {

                log.error("Could not encode: charset=" + charset + ", url="
                        + url);

                return url;

            }
            
        }
        
    } // class Model
    
    static final private String encoding = "UTF-8";
    
    /*
     * Note: the page is valid for any of these doctypes.
     */
//    final private DoctypeEnum doctype = DoctypeEnum.html_4_01_strict
//    final private DoctypeEnum doctype = DoctypeEnum.html_4_01_transitional
    static final private DoctypeEnum doctype = DoctypeEnum.xhtml_1_0_strict;
    
    /**
     * Allows override of the binding(s) for a named property.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class NV {
        
        public final String name;
        public final Vector<String> values;
        
        public NV(String name, String value) {

            if (name == null)
                throw new IllegalArgumentException();

            this.name = name;

            if (value == null) {

                this.values = null;

            } else {

                Vector<String> values = new Vector<String>();

                values.add(value);

                this.values = values;

            }
            
        }
        
        public NV(String name, String[] values) {
            
            if (name == null)
                throw new IllegalArgumentException();
            
            if (values == null)
                throw new IllegalArgumentException();
            
            this.name = name;
            
            Vector<String> tmp = new Vector<String>();
            
            for(String s : values) {

                tmp.add(s);
                
            }
            
            this.values = tmp;
            
        }
        
    }
    
    /**
     * Describes the state of the controller.
     */
    protected final Model model;
    
    /**
     * 
     */
    public XHTMLRenderer(final Model model) {

        if (model == null)
            throw new IllegalArgumentException();

        this.model = model;

    }

    public void write(final Writer w) throws IOException {

        writeXmlDecl(w);
        
        writeDocType(w);
        
        writeHtml(w);
        
        writeHead(w);
        
        writeBody(w);
                
        w.write("</html\n>");
        
    }
    
    protected void writeXmlDecl(Writer w) throws IOException {
        
        w.write("<?xml version=\"1.0\" encoding=\"" + encoding + "\"?>\n");
        
    }
    
    /**
     * 
     * @param w
     * @throws IOException
     */
    protected void writeDocType(Writer w) throws IOException {
        
//        if(true) return;
        
        w.write("<!DOCTYPE html PUBLIC");
        
        w.write(" \""+doctype.publicId()+"\"");
        
        w.write(" \""+doctype.systemId()+"\"");
        
        w.write(">\n");
        
    }

    /** The start <code>html</code> tag. */
    protected void writeHtml(Writer w) throws IOException {
        
        w.write("<html ");
        
        if(doctype.isXML()) {

            w.write(" xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\"");
            
        }
        
        w.write(" lang=\"en\"");
        
        w.write("\n>");
        
    }
    
    protected void writeHead(Writer w) throws IOException {

        w.write("<head\n>");
        
        writeTitle(w);
        
        writeScripts(w);
        
        w.write("</head\n>");
    }
    
    protected void writeTitle(Writer w)  throws IOException {
        
        w.write("<title>bigdata(tm) telemetry : "+cdata(model.path)+"</title\n>");
        
    }

    protected void writeScripts(Writer w)  throws IOException {
        
        if (model.flot) {

            final String s = model.getRequestURL().toString();
            
            w.write("<script\n type=\"text/javascript\" src=\""+s+"jquery.js\"></script\n>");

            w.write("<script\n type=\"text/javascript\" src=\""+s+"jquery.flot.js\"></script\n>");

            w.write("<!--[if IE]><script type=\"text/javascript\" src=\""+s+"excanvas.pack.js\"></script><![endif]-->");

        }
        
    }
    
    protected void writeBody(Writer w) throws IOException  {
        
        w.write("<body\n>");

        /*
         * @todo alternative ICounterSelect given just a Path.
         */
        final ICounterNode node = ((CounterSetSelector)model.counterSelector).getRoot().getPath(model.path);
        
        if(node == null) {

            /*
             * Used when the path does not evaluate to anything in the
             * hierarchy. The generated markup at least lets you choose a parent
             * from the path.
             */
            
            w.write("<p>");
            
            w.write("No such counter or counter set: ");
            
            writeFullPath(w, model.path);
            
            w.write("</p>");

            return;
            
        }
        
        if(node instanceof ICounter) {

            writeCounter(w, (ICounter) node);

        } else {

            switch (model.reportType) {
            
            case hierarchy:
            
                /*
                 * @todo rewrite to use node.getDepth() + model.depth so that
                 * the relative depth is maintained during navigation.
                 */ 
                writeCounterSet(w, (CounterSet) node, model.depth);
                
                break;
                
            case correlated:
                
                writeHistoryTable(w, model.counterSelector.selectCounters(
                        model.depth, model.pattern, model.fromTime,
                        model.toTime, model.period), model.period,
                        model.timestampFormat);
                
                break;
                
            case pivot:
                
                writePivotTable(w, model.counterSelector.selectCounters(
                        model.depth, model.pattern, model.fromTime,
                        model.toTime, model.period));
                
                break;
                
            case events:
                
                // render the time-series chart : FIXME respect fromTime/toTime.
                writeFlot(w, model.eventReportingService);
                
                break;
                
            }

        }
        
        /*
         * This is valid XHTML, but having the image here this makes copy and
         * paste a little more difficult.
         */
//        doctype.writeValid(w);

        w.write("</body\n>");
        
    }

    /**
     * A clickable trail of the path from the root.
     * 
     * @deprecated by refactor inside of a rendering object.
     */
    protected void writeFullPath(Writer w, String path)
            throws IOException {

        writePath(w, path, 0/* root */);

    }
    
    /**
     * A clickable trail of the path.
     * 
     * @param rootDepth
     *            The path components will be shown beginning at this depth -
     *            ZERO (0) is the root.
     * 
     * @deprecated by refactor inside of a rendering object.
     */
    protected void writePath(Writer w, String path, int rootDepth)
            throws IOException {

        final String[] a = path.split(ps);

        if (rootDepth == 0) {
            
            // click through to the root of the counter hierarchy
            w.write("<a href=\""
                    + model.getRequestURL(new NV[] { new NV(Model.PATH, ps) })
                    + "\">");
            w.write(ps);
            w.write("</a>");
            
        }
        
        // builds up the path query parameter for each split.
        final StringBuilder sb = new StringBuilder(ps);

        for (int n = 1; n < a.length; n++) {

            final String name = a[n];
            
            if (n > 1) {

                if ((n+1) > rootDepth) {

                    w.write("&nbsp;");

                    w.write(ps);
                    
                }

                sb.append(ps);

            }

            final String prefix = sb.toString();
            
            sb.append(name);

            if ((n+1) > rootDepth) {

                if(rootDepth!=0 && n==rootDepth) {
                    
                    w.write("<a href=\""
                            + model.getRequestURL(new NV[] { new NV(Model.PATH, prefix) }) + "\">");

                    w.write("...");

                    w.write("</a>");
                    
                    w.write("&nbsp;"+ps);
                    
                }
                
                w.write("&nbsp;");

                w.write("<a href=\""
                        + model.getRequestURL(new NV[] { new NV(Model.PATH, sb
                                .toString()) }) + "\">");

                // current path component.
                w.write(cdata(name));

                w.write("</a>");

            }

        }
        
    }
    
// protected void writeCounterNode(Writer w, ICounterNode node) throws
// IOException {
//        
//        if(node instanceof ICounterSet) {
//            
//            writeCounterSet(w, (CounterSet)node);
//            
//        } else {
//
//            /*
//             * How to render a single counter? 
//             */
//            
//            throw new UnsupportedOperationException();
//            
//        }
//        
//    }

    /**
     * Writes all counters in the hierarchy starting with the specified
     * {@link CounterSet} in a single table (this is the navigational view of
     * the counter set hierarchy).
     */
    protected void writeCounterSet(Writer w, final CounterSet counterSet,
            final int depth) throws IOException {
        
        // depth of the hierarchy at the point where we are starting.
        final int ourDepth = counterSet.getDepth();

        if (log.isInfoEnabled())
            log.info("path=" + counterSet.getPath() + ", depth=" + depth
                    + ", ourDepth=" + ourDepth);

        final String summary = "Showing counters for path="
                + counterSet.getPath();
        
        w.write("<table border=\"1\" summary=\""+attrib(summary)+"\"\n>");

        // @todo use css to left justify the path.
        w.write(" <caption>");
        writeFullPath(w,counterSet.getPath());
        w.write("</caption\n>");
        
        w.write(" <tr\n>");
        w.write("  <th rowspan=\"2\" >Name</th\n>");
        w.write("  <th colspan=\"3\">Averages</th\n>");
        w.write("  <th rowspan=\"2\">Current</th\n>");
        w.write(" </tr\n>");
        
        w.write(" <tr\n>");
        w.write("  <th>Minutes</th\n>");
        w.write("  <th>Hours</th\n>");
        w.write("  <th>Days</th\n>");
        w.write(" </tr\n>");

        final Iterator<ICounterNode> itr = counterSet.getNodes(model.pattern);
        
//        final Iterator<ICounter> itr = counterSet.directChildIterator(
//                true/* sorted */, ICounter.class/* type */);
        
        while(itr.hasNext()) {

            final ICounterNode node = itr.next();

            if (log.isDebugEnabled())
                log.debug("considering: " + node.getPath());
            
            if(depth != 0) { 
                
                final int counterDepth = node.getDepth();
                
//                log.info("counterDepth("+counterDepth+") - rootDepth("+rootDepth+") = "+(counterDepth-rootDepth));
                
                if((counterDepth - ourDepth) > depth) {
                
                    // prune rendering
                    if (log.isDebugEnabled())
                        log.debug("skipping: " + node.getPath());
                    
                    continue;
                    
                }
                
            }
            
            final String path = node.getPath();
            
//            if (filter != null) {
//
//                if (!filter.matcher(path).matches()) {
//
//                    // skip counter not matching filter.
//                    
//                    continue;
//                    
//                }
//                
//            }

            w.write(" <tr\n>");

            if(node instanceof ICounterSet) {
            
                w.write("  <th align=\"left\">");// colspan=\"5\">");
                writePath(w, path, ourDepth);
                w.write("  </th\n>");
                w.write("  <td colspan=\"4\">&nbsp;...</td>");
                
            } else {
                
                final ICounter counter = (ICounter) node;

                /*
                 * write out values for the counter.
                 */

                w.write("  <th align=\"left\">");
                writePath(w, path, ourDepth);
                w.write("  </th\n>");

                if (counter.getInstrument() instanceof HistoryInstrument) {

                    /*
                     * Report the average over the last hour, day, and month.
                     * 
                     * @todo could report the current value, the weighted
                     * average for the last 5 units, and the weighted average
                     * for the last 10 units. Need a method to compute the
                     * weighted average for a History. Reuse that method in the
                     * LBS.
                     */

                    HistoryInstrument inst = (HistoryInstrument) counter
                            .getInstrument();

                    w.write("  <td>" + value(counter,inst.minutes.getAverage())
                            + " (" + value(counter,inst.minutes.size()) + ")"
                            + "</td\n>");

                    w.write("  <td>" + value(counter,inst.hours.getAverage())
                            + " (" + value(counter,inst.hours.size()) + ")"
                            + "</td\n>");

                    w.write("  <td>" + value(counter,inst.days.getAverage())
                            + " (" + value(counter,inst.days.size()) + ")"
                            + "</td\n>");

                    // the most recent value.
                    w.write("  <td>" + value(counter,counter.getValue())
                            + "</td\n>");

                } else {

                    /*
                     * Report only the most recent value.
                     */

                    // w.write(" <th>N/A</th\n>");
                    // w.write(" <th>N/A</th\n>");
                    // w.write(" <th>N/A</th\n>");
                    w.write("  <td colspan=\"4\">"
                            + value(counter,counter.getValue()) + "</td\n>");

                }

            }
            
            w.write(" </tr\n>");
            
        }

        w.write("</table\n>");

    }
    
    /**
     * Writes details on a single counter using a {@link HistoryTable} view.
     * 
     * @param counter
     *            The counter.
     * 
     * @throws IOException
     */
    protected void writeCounter(final Writer w, final ICounter counter)
            throws IOException {

        if (counter.getInstrument() instanceof HistoryInstrument) {

            writeHistoryTable(w, new ICounter[] { counter }, model.period,
                    model.timestampFormat);

        }

    }
    
//    /**
//     * Writes details on a single counter whose {@link IInstrument} provides a
//     * history. The goal is to be able to easily copy and paste the data into a
//     * program for plotting, e.g., as an X-Y graph (values against time).
//     * 
//     * @param counter
//     *            The counter.
//     * 
//     * @see HistoryInstrument
//     */
//    protected void writeHistoryCounter(Writer w, ICounter counter)
//            throws IOException {
//
//        final HistoryInstrument inst = (HistoryInstrument) counter.getInstrument();
//        
//        if (inst.minutes.size() > 0) {
//            w.write("<p>");
//            w.write("</p>");
//            writeSamples(w, counter, inst.minutes);
//        }
//
//        if (inst.hours.size() > 0) {
//            w.write("<p>");
//            w.write("</p>");
//            writeSamples(w, counter, inst.hours);
//        }
//
//        if (inst.days.size() > 0) {
//            w.write("<p>");
//            w.write("</p>");
//            writeSamples(w, counter, inst.days);
//        }
//        
//    }

//    /**
//     * Writes a table containing the samples for a {@link History} for some
//     * {@link ICounter}.
//     * 
//     * @param w
//     * @param counter
//     * @param h
//     * 
//     * @throws IOException
//     * 
//     * @deprecated replace with
//     *             {@link #writeHistoryTable(Writer, ICounter[], PeriodEnum, TimestampFormatEnum)}
//     *             for the specified counter and units.
//     */
//    protected void writeSamples(Writer w, ICounter counter, History h) throws IOException {
//        
//        /*
//         * Figure out the label for the units of the history.
//         */
//        final String units;
//        final DateFormat dateFormat;
//        final long period;
//        if (h.getPeriod() == 1000 * 60L) {
//            units = "Minutes";
//            period = 1000*60;// 60 seconds (in ms).
//            dateFormat = DateFormat.getTimeInstance(DateFormat.SHORT);
//        } else if (h.getPeriod() == 1000 * 60 * 24L) {
//            units = "Hours";
//            period = 1000*60*60;// 60 minutes (in ms).
//            dateFormat = DateFormat.getTimeInstance(DateFormat.MEDIUM);
//        } else if (h.getSource() != null
//                && h.getSource().getPeriod() == 1000 * 60 * 24L) {
//            units = "Days";
//            period = 1000*60*60*24;// 24 hours (in ms).
//            dateFormat = DateFormat.getDateInstance(DateFormat.MEDIUM);
//        } else {
//            throw new AssertionError("period="+h.getPeriod());
////            units = "period=" + h.getPeriod() + "ms";
////            dateFormat = DateFormat.getDateTimeInstance();
//        }
//
//        /*
//         * Iterator will visit the timestamped samples in the history.
//         * 
//         * Note: the iterator is a snapshot of the history at the time that the
//         * iterator is requested.
//         * 
//         * @todo This scans the history first, building up the table rows in a
//         * buffer. Originally that was required to get the first/last timestamps
//         * from the history before we format the start of the table but now the
//         * iterator will report those timestamps directly and this could be
//         * moved inline.
//         */
//        final SampleIterator itr = h.iterator();
//
//        final StringBuilder sb = new StringBuilder();
//
//        final long firstTimestamp = itr.getFirstSampleTime();
//
//        final long lastTimestamp = itr.getLastSampleTime();
//
//        while (itr.hasNext()) {
//
//            final IHistoryEntry sample = itr.next();
//
//            sb.append(" <tr\n>");
//
//            final long lastModified = sample.lastModified();
//
//            /*
//             * The time will be zero for the first row and a delta (expressed in
//             * the units of the history) for the remaining rows.
//             * 
//             * Note: The time units are computed using floating point math and
//             * then converted to a display form using formatting in order to be
//             * able to accurately convey where a sample falls within the
//             * granularity of the unit (e.g., early or late in the day).
//             */
//            final String timeStr = model.unitsFormat
//                    .format(((double) lastModified - firstTimestamp) / period);
//
//            sb.append("  <td>" + cdata(timeStr) + "</td\n>");
//
//            sb.append("  <td>" + value(counter, sample.getValue())
//                    + "</td\n>");
//
//            sb.append("  <td>"
//                    + cdata(dateFormat.format(new Date(lastModified)))
//                    + "</td\n>");
//
//            sb.append(" </tr\n>");
//
//        }
//
//        /*
//         * Summary for the table.
//         * 
//         * @todo add some more substance to the summary?
//         */
//        final String summary = "Showing samples: period=" + units + ", path="
//                + counter.getPath();
//
//        /*
//         * Format the entire table now that we have all the data on hand.
//         */
//
//        w.write("<table border=\"1\" summary=\"" + attrib(summary) + "\"\n>");
//
//        // // caption : @todo use css to left justify the path.
//        // w.write(" <caption>");
//        // writePath(w, counter.getPath());
//        // w.write(" </caption\n>");
//
//        // header row.
//        w.write(" <tr\n>");
//        w.write("  <th colspan=\"3\">");
//        writeFullPath(w, counter.getPath());
//        w.write("  </th\n>");
//        w.write(" </tr\n>");
//
//        // header row.
//        w.write(" <tr\n>");
//        w.write("  <th>" + "From: "
//                + dateFormat.format(new Date(firstTimestamp)) + "</th\n>");
//        w.write("  <th>" + "To: " + dateFormat.format(new Date(lastTimestamp))
//                + "</th\n>");
//        // w.write(" <th></th>");
//        w.write(" </tr\n>");
//
//        // header row.
//        w.write(" <tr\n>");
//        w.write("  <th>" + cdata(units) + "</th\n>");
//        w.write("  <th>" + cdata(counter.getName()) + "</th\n>");
//        w.write("  <th>Timestamp</th>\n");
//        w.write(" </tr\n>");
//
//        // data rows.
//        w.write(sb.toString());
//
//        w.write("</table\n>");
//
//    }

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class HTMLValueFormatter extends ValueFormatter {

        protected final Model model;
        
        /**
         * 
         * @param dateFormat
         * @param model
         * 
         * FIXME Do not require {@link Model} -- it makes it to difficult to run
         * outside of the {@link CounterSetHTTPD}? (Not really, we could just
         * parse a URL into the required form.) The other problem is that this
         * is a non-static inner class of {@link XHTMLRenderer} (for
         * {@link #value(ICounter, Object)}, which could be handled easily
         * enough by copying the code, making it reference the
         * {@link HTMLValueFormatter}, etc.).
         */
        public HTMLValueFormatter(DateFormat dateFormat, Model model) {
            
            super(dateFormat);
            
            this.model = model;
            
        }
        
        /**
         * Formats a counter value as a String AND performs any escaping necessary
         * for inclusion in a CDATA section (we do both operations together so that
         * we can format {@link IServiceCounters#LOCAL_HTTPD} as a link anchor.
         */
        public String value(final ICounter counter, final Object val) {

            return XHTMLRenderer.this.value(counter,val);
            
        }

        /**
         * A clickable trail of the path from the root.
         */
        public void writeFullPath(Writer w, String path)
                throws IOException {

            writePath(w, path, 0/* root */);

        }
        
        /**
         * A clickable trail of the path.
         * 
         * @param rootDepth
         *            The path components will be shown beginning at this depth -
         *            ZERO (0) is the root.
         */
        public void writePath(Writer w, String path, int rootDepth)
                throws IOException {

            final String[] a = path.split(ps);

            if (rootDepth == 0) {
                
                // click through to the root of the counter hierarchy
                w.write("<a href=\""
                        + model.getRequestURL(new NV[] { new NV(Model.PATH, ps) })
                        + "\">");
                w.write(ps);
                w.write("</a>");
                
            }
            
            // builds up the path query parameter for each split.
            final StringBuilder sb = new StringBuilder(ps);

            for (int n = 1; n < a.length; n++) {

                final String name = a[n];
                
                if (n > 1) {

                    if ((n+1) > rootDepth) {

                        w.write("&nbsp;");

                        w.write(ps);
                        
                    }

                    sb.append(ps);

                }

                final String prefix = sb.toString();
                
                sb.append(name);

                if ((n+1) > rootDepth) {

                    if(rootDepth!=0 && n==rootDepth) {
                        
                        w.write("<a href=\""
                                + model.getRequestURL(new NV[] { new NV(Model.PATH, prefix) }) + "\">");

                        w.write("...");

                        w.write("</a>");
                        
                        w.write("&nbsp;"+ps);
                        
                    }
                    
                    w.write("&nbsp;");

                    w.write("<a href=\""
                            + model.getRequestURL(new NV[] { new NV(Model.PATH, sb
                                    .toString()) }) + "\">");

                    // current path component.
                    w.write(cdata(name));

                    w.write("</a>");

                }

            }
            
        }
        
    }
    
    /**
     * Writes out a table containing the histories for the selected counters.
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
     * @todo review use of basePeriod - this is {@link Model#period}, right?
     */
    protected void writeHistoryTable(final Writer w, final ICounter[] a,
            final PeriodEnum basePeriod,
            final TimestampFormatEnum timestampFormat) throws IOException {

        if (w == null)
            throw new IllegalArgumentException();

        if (a == null)
            throw new IllegalArgumentException();

        if (a.length == 0) {

            // No data.
            
            return;
            
        }
        
        if (basePeriod == null)
            throw new IllegalArgumentException();

        if (timestampFormat == null)
            throw new IllegalArgumentException();

        final HistoryTable t = new HistoryTable(a, basePeriod);
        
        /*
         * Figure out how we will format the timestamp (From:, To:, and the last
         * column).
         */
        final DateFormat dateFormat;
        switch(timestampFormat) {
        case dateTime:
            switch (basePeriod) {
            case Minutes:
                dateFormat = DateFormat.getTimeInstance(DateFormat.SHORT);
                break;
            case Hours:
                dateFormat = DateFormat.getTimeInstance(DateFormat.MEDIUM);
                break;
            case Days:
                dateFormat = DateFormat.getDateInstance(DateFormat.MEDIUM);
                break;
            default:
                throw new AssertionError();
            }
            break;
        case epoch:
            dateFormat = null;
            break;
        default:
            throw new AssertionError(timestampFormat.toString());
        }

        new HTMLHistoryTableRenderer(t, model.pattern, new HTMLValueFormatter(
                dateFormat, model)).render(w);
        
    }

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class HTMLPivotTableRenderer extends PivotTableRenderer {

        public HTMLPivotTableRenderer(PivotTable pt,
                ValueFormatter formatter) {

            super(pt, formatter);
            
        }

        /**
         * Generate the table.
         */
        public void render(final Writer w) throws IOException {

            final HistoryTable t = pt.src;
            
            // the table start tag.
            {

                // Summary for the table : @todo more substance in summary.
                final String summary = "Showing samples: period=" + t.units;

                w.write("<table border=\"1\" summary=\"" + attrib(summary)
                        + "\"\n>");

            }

            // the header rows.
            {

                // header row.
                w.write(" <tr\n>");
                // timestamp column headers.
                w.write("  <th>" + cdata(t.units) + "</th\n>");
                w.write("  <th>" + cdata("timestamp") + "</th\n>");
                for (String s : pt.cnames) {
                    // category column headers.
                    w.write("  <th>" + cdata(s) + "</th\n>");
                }
                for (String s : pt.vcols) {
                    // performance counter column headers.
                    w.write("  <th>" + cdata(s) + "</th\n>");
                }
                w.write(" </tr\n>");

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

                final String unitStr = cdata(formatter.unitsFormat
                        .format(((double) timestamp - t
                                .getTimestamp(0/* row */))
                                / t.period));

                final String timeStr = cdata(formatter.date(timestamp));
                
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

                    w.write(" <tr\n>");

                    w.write("  <td>" + unitStr + "</td\n>");

                    w.write("  <td>" + timeStr + "</td\n>");

                    for (int j = 0; j < pt.cnames.length; j++) {

                        w.write("  <td>" + cset.cats[j] + "</td\n>");

                    }

                    for (int j = 0; j < vals.length; j++) {

                        final String s = vals[j] == null ? "" : Double
                                .toString(vals[j]);

                        w.write("  <td>" + s + "</td\n>");

                    }
                    
                    w.write(" </tr\n>");
                    
                }

            } // next row.
           
            // the table end tag.
            w.write("</table\n>");

        }
        
    }
    
    /**
     * Writes out a pivot table view.
     * 
     * @param w
     *            Where to write the data.
     * @param a
     *            The selected counters.
     * @param basePeriod
     * @param timestampFormat
     * 
     * @throws IOException
     * 
     * @todo review use of basePeriod. is this {@link Model#period}?
     */
    protected void writePivotTable(final Writer w, final ICounter[] a,
            final PeriodEnum basePeriod,
            final TimestampFormatEnum timestampFormat) throws IOException {

        if (w == null)
            throw new IllegalArgumentException();

        if (a == null)
            throw new IllegalArgumentException();

        if (a.length == 0) {

            // No data.
            
            return;
            
        }
        
        if (basePeriod == null)
            throw new IllegalArgumentException();

        if (timestampFormat == null)
            throw new IllegalArgumentException();

        final HistoryTable t = new HistoryTable(a, basePeriod);

        final PivotTable pt = new PivotTable(model.pattern, model.category, t);
        
        /*
         * Figure out how we will format the timestamp (From:, To:, and the last
         * column).
         */
        final DateFormat dateFormat;
        switch(timestampFormat) {
        case dateTime:
            switch (basePeriod) {
            case Minutes:
                dateFormat = DateFormat.getTimeInstance(DateFormat.SHORT);
                break;
            case Hours:
                dateFormat = DateFormat.getTimeInstance(DateFormat.MEDIUM);
                break;
            case Days:
                dateFormat = DateFormat.getDateInstance(DateFormat.MEDIUM);
                break;
            default:
                throw new AssertionError();
            }
            break;
        case epoch:
            dateFormat = null;
            break;
        default:
            throw new AssertionError(timestampFormat.toString());
        }

        new HTMLPivotTableRenderer(pt,
                new HTMLValueFormatter(dateFormat, model)).render(w);
        
    }
    
    /**
     * Writes data in a format suitable for use in a pivot table.
     * <p>
     * The pivot table data are selected in the same manner as the correlated
     * view and are used to generate a {@link HistoryTable}. There will be one
     * data row per row in the history table. There will be one category column
     * for each capturing group in the {@link Model#pattern}, one column for
     * the timestamp associated with the row, and one for the value of each
     * performance counter selected by the {@link Model#pattern}.
     * <p>
     * Since the pivot table and the correlated view are both based on the
     * {@link HistoryTable} you can switch between these views simply by
     * changing the {@link Model#reportType} using the {@value Model#REPORT} URL
     * query parameter.
     * 
     * @see ReportEnum#pivot
     */
    protected void writePivotTable(final Writer w, final ICounter[] a)
            throws IOException {
        
        if (model.period == null) {

            /*
             * Report for all periods.
             */
            
            writePivotTable(w, a, PeriodEnum.Minutes, model.timestampFormat);

            writePivotTable(w, a, PeriodEnum.Hours, model.timestampFormat);

            writePivotTable(w, a, PeriodEnum.Days, model.timestampFormat);

        } else {

            /*
             * Report only the specified period.
             */

            switch (model.period) {
            case Minutes:
                writePivotTable(w, a, PeriodEnum.Minutes, model.timestampFormat);
                break;
            case Hours:
                writePivotTable(w, a, PeriodEnum.Hours, model.timestampFormat);
                break;
            case Days:
                writePivotTable(w, a, PeriodEnum.Days, model.timestampFormat);
                break;
            default:
                throw new AssertionError(model.period.toString());
            }

        }
        
    }
    
    /**
     * Write the html to render the Flot-based time-series chart, plotting the
     * supplied events.
     */
    protected void writeFlot(final Writer w,
            final IEventReportingService eventReportingService)
            throws IOException {
        
        writeResource(w, "flot-start.txt");
        
        writeEvents(w, eventReportingService);
        
        writeResource(w, "flot-end.txt");
        
    }
    
    /**
     * Applies the {@link Model#eventFilters} to the event.
     * 
     * @param e
     *            The event.
     * @return <code>true</code> if the filters accept the event.
     */
    protected boolean acceptEvent(final Event e) {
        
        final Iterator<Map.Entry<Field, Pattern>> itr = model.eventFilters
                .entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<Field, Pattern> filterEntry = itr.next();

            final Field fld = filterEntry.getKey();

            final Pattern pattern = filterEntry.getValue();

            final String val;
            try {
             
                val = "" + fld.get(e);
                
            } catch (Throwable t) {
                
                throw new RuntimeException("Could not access field: " + fld);
                
            }

            if (!pattern.matcher(val).matches()) {

                if (log.isDebugEnabled())
                    log.debug("Rejected event: fld=" + fld.getName()
                            + " : val=" + val);
                
                return false;

            }

        }

        return true;

    }

    /**
     * The key for an event group is formed by combining the String value of the
     * fields of the event identified by the orderEventBy[] in order and using a
     * ":" when combining two or more event fields together.
     */
    protected String getEventKey(final Event e) {

        final StringBuilder sb = new StringBuilder();

        int n = 0;

        for (Field f : model.eventOrderBy) {

            if (n > 0)
                sb.append(":");

            try {

                sb.append("" + f.get(e));
                
            } catch (Exception ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            n++;
            
        }
        
        return sb.toString();
        
    }
    
    // FIXME format the event as an HTML table and link into the code.
    protected String getEventTable(final Event e) {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append(e.toString());
        
        return sb.toString();
        
    }
    
    /**
     * Plots events using <code>flot</code>.
     * 
     * @see ReportEnum#events
     * @see Model#eventFilters
     * @see Model#eventOrderBy
     * 
     * FIXME Modify to allow visualization of a performance counter timeseries
     * data.
     * 
     * FIXME Modify to allow linked viz of performance counter timeseries w/ an
     * event timeseries and an overview that controls what is seen in both.
     * 
     * @todo allow a data label for the start point to be specified as a query
     *       parameter. E.g., the indexName+partitionId or the hostname.
     * 
     * @todo clicking on the overview should reset to the total view.
     * 
     * @todo allow aggregation of elapsed time for events and have click through
     *       to the individual events broken out using eventOrderBy=uuid.
     * 
     * @todo a form to set the various query parameters.
     * 
     * @todo a dashboard view of the federation as a jsp page incorporating
     *       output from a variety of sources. basically a replacement for the
     *       use of a worksheet to look at views of the data which we know to be
     *       interesting. since I don't want to force the bundling of a servlet
     *       engine with each service, this could be a standoff module.
     */
    protected void writeEvents(final Writer w,
            final IEventReportingService eventReportingService)
            throws IOException {

        /*
         * Map from the key for the event group to the basename of the variables
         * for that event group.
         */
        final Map<String,String> seriesByGroup = new HashMap<String, String>();

        /*
         * Map from the key for the event group to the StringBuilder whose
         * contents are rendered into the page and provide the visualization of
         * the timeseries for that event group. The events are grouped together
         * in this manner so that they will be assigned the same color by the
         * legend.
         */
        final Map<String,StringBuilder> eventsByHost = new HashMap<String,StringBuilder>();

        /*
         * Map from the key for the event group to the StringBuilder whose
         * contents are rendered into the page and provide the tooltip for each
         * event in that event group.
         */
        final Map<String,StringBuilder> tooltipsByHost = new HashMap<String,StringBuilder>();

        int naccepted = 0;
        int nvisited = 0;
        
        final Iterator<Event> itr = eventReportingService.rangeIterator(
                model.fromTime, model.toTime);
        
        while (itr.hasNext()) {

            final Event e = itr.next();

            nvisited++;
            
            // apply the event filters.
            if (!e.isComplete() || !acceptEvent(e)) {

                continue;
                
            }

            naccepted++;

            final String key = getEventKey(e);
            
            // basename for the variables for this data series.
            final String series;

            // per event-group buffer
            StringBuilder eventsSB = eventsByHost.get(key);
            
            if (eventsSB == null) {
                
                eventsSB = new StringBuilder();
                
                series = "series_" + seriesByGroup.size();

                seriesByGroup.put(key, series);

                eventsByHost.put(key, eventsSB);
                
                eventsSB.append("var ");
                
                eventsSB.append(series);

                eventsSB.append(" = [\n");
                
                final StringBuilder tooltipsSB = new StringBuilder();
                
                final String tooltipvar = series + "tooltips";

                tooltipsByHost.put(key, tooltipsSB);
                
                tooltipsSB.append("var ");
                
                tooltipsSB.append(tooltipvar);
                
                tooltipsSB.append(" = [\n");
                
            } else {
                
                series = seriesByGroup.get(key);
                
            }
            
            eventsSB.append("[ ");
            
            eventsSB.append(e.getStartTime());
            
            eventsSB.append(", ");

//            final double offset = Math.sin(e
//                    .getEndTime()/100d) / 4d;
            final double offset = (Math.random()*.85d)+.05;// - .5);// / 1d;
            
            final String hostyvar = series + "y";
            
            eventsSB.append(hostyvar);

            eventsSB.append((offset < 0 ? "" : "+") + offset+" ], [ ");

            eventsSB.append(e.getEndTime());

            eventsSB.append(", ");

            eventsSB.append(hostyvar);

            eventsSB.append((offset < 0 ? "" : "+") +offset+" ], null,\n");
            
            final StringBuilder tooltipsSB = tooltipsByHost.get(key);

            final String tooltip;
            if (false) {
                /*
                 * FIXME Finish the event flyover formatting support. I need to
                 * validate the HTML table and then validate how it is embedded
                 * inside of the flot data. Since it occurs inline, it probably
                 * needs to be escaped. It may also be impossible to do this in
                 * a manner which validates, but still possible to do it in a
                 * manner which is accepted by at least some browsers.
                 */
                StringWriter sw = new StringWriter();
                writeEventFlyover(sw, e);
                tooltip = sw.toString();
            } else {
                /*
                 * use the tab-delimited format, but remove the trailing
                 * newline.
                 */
                tooltip = e.toString().replace("\n", "");
            }
            
            // @todo does this need to escape embedded quotes for javascript?
            if (tooltip != null && !tooltip.startsWith("\"")) {

                tooltipsSB.append("\"");

            }
            
            tooltipsSB.append(tooltip);
            
            if (tooltip != null && !tooltip.endsWith("\"")) {
                
                tooltipsSB.append("\"");
                
            }
            
//            tooltipsSB.append(", null,\n");
            tooltipsSB.append(", null, null,\n");
            
        }

        log.warn("accepted: " + naccepted + " out of "
                + nvisited
                + " events");
        
        // all series.
        final String[] keys = eventsByHost.keySet().toArray(new String[0]);
        
        // put into lexical order.
        Arrays.sort(keys);
 
        for (int i = 0; i < keys.length; i++) {

            final int hosty = i;
            
            final String key = keys[i];
            
            final StringBuilder eventsSB = new StringBuilder();

            final String series = seriesByGroup.get(key);
            
            final String hostyvar = series + "y";
            
            eventsSB.append("var ");

            eventsSB.append(hostyvar);

            eventsSB.append(" = ");

            eventsSB.append(hosty);

            eventsSB.append(";\n");

            final StringBuilder sb = eventsSB.append(eventsByHost.get(key));
            
            sb.setLength(sb.length() - ", null,\n".length());
            
            sb.append("\n];");
            
            w.write(sb.toString());
            
            w.write("\n");
            
        }

        /*
         * Output the variables containing the tooltips for each series.
         */
        for (int i = 0; i < keys.length; i++) {

            final StringBuilder sb = tooltipsByHost.get(keys[i]);

            sb.setLength(sb.length() - 2);

            sb.append("\n];");

            w.write(sb.toString());

            w.write("\n");

        }

        /*
         * data[].
         * 
         * Note: The data[] and the tooltips[] MUST be correlated. They are
         * taken in reverse of the generated series order to put the legend
         * in the same order (its just a little trick).
         */
        
        final StringBuilder data = new StringBuilder();

        data.append("var data = [\n");

        for (int i = keys.length - 1; i >= 0; i--) {

            final String key = keys[i];

            final String series = seriesByGroup.get(key);
            
            data.append("{ label: \"");

            data.append(key);

            data.append("\", data: ");

            data.append(series);

            data.append(" },\n");

        }

        if (data.charAt(data.length() - 2) == ',') {

            data.setLength(data.length() - 2);

        }

        data.append("\n];\n");

        w.write(data.toString());

        /*
         * tooltips[]
         */
        final StringBuilder tooltips = new StringBuilder();

        tooltips.append("var tooltips = [\n");

        for (int i = keys.length - 1; i >= 0; i--) {

            final String key = keys[i];

            final String series = seriesByGroup.get(key);

            final String tooltipvar = series + "tooltips";

            tooltips.append(tooltipvar);

            tooltips.append(",\n");
        }

        if (tooltips.charAt(tooltips.length() - 2) == ',') {

            tooltips.setLength(tooltips.length() - 2);

        }

        tooltips.append("\n];\n");

        w.write(tooltips.toString());

    }

    /**
     * Pretty up an event by rendering onto the {@link Writer} as an (X)HTML
     * table.
     * 
     * @param w
     *            The writer.
     * @param e
     *            The event.
     * 
     * @throws IOException
     */
    protected void writeEventFlyover(final Writer w, final Event e)
            throws IOException {
        
        final DateFormat dateFormat = DateFormat.getDateTimeInstance();

        final String summary = e.majorEventType + " from "
                + dateFormat.format(new Date(e.getStartTime())) + " to "
                + dateFormat.format(new Date(e.getEndTime())) + ", uuid="
                + e.eventUUID.toString();
        
        w.write("<table border=\"1\" summary=\"" + attrib(summary) + "\"\n>");

        w.write(" <caption>");
        w.write(cdata(e.majorEventType + " from "
                + cdata(dateFormat.format(new Date(e.getStartTime()))) + " to "
                + e.getEndTime()));
        w.write("</caption\n>");
        
        // header row.
        w.write(" <tr\n>");
        w.write("  <th>" + "From: "
                + cdata(dateFormat.format(new Date(e.getStartTime())))
                + "</th\n>");
        w.write("  <th>" + "To: "
                + cdata(dateFormat.format(new Date(e.getEndTime())))
                + "</th\n>");
        w.write("  <th>"
                + "Duration: " + (e.getEndTime() - e.getStartTime())
                + "s</th\n>");
        w.write(" </tr\n>");

        // attributes.
        w.write(" <tr\n>");
        w.write("  <th align=\"left\">hostname</th>");
        w.write("  <td colspan=\"2\">"+cdata(e.hostname.toString())+"</td>");
        w.write(" </tr\n>");

        w.write(" <tr\n>");
        w.write("  <th align=\"left\">serviceIface</th>");
        w.write("  <td colspan=\"2\">"+cdata(e.serviceIface.toString())+"</td>");
        w.write(" </tr\n>");

        w.write(" <tr\n>");
        w.write("  <th align=\"left\">serviceName</th>");
        w.write("  <td colspan=\"2\">"+cdata(e.serviceName.toString())+"</td>");
        w.write(" </tr\n>");

        w.write(" <tr\n>");
        w.write("  <th align=\"left\">serviceUUID</th>");
        w.write("  <td colspan=\"2\">"+cdata(e.serviceUUID.toString())+"</td>");
        w.write(" </tr\n>");

        w.write(" <tr\n>");
        w.write("  <th align=\"left\">resource</th>");
        w.write("  <td colspan=\"2\">"+cdata(e.resource.toString())+"</td>");
        w.write(" </tr\n>");
        
        w.write(" <tr\n>");
        w.write("  <th align=\"left\">minorEventType</th>");
        w.write("  <td colspan=\"2\">"+cdata(e.minorEventType.toString())+"</td>");
        w.write(" </tr\n>");

        w.write(" <tr\n>");
        w.write("  <th align=\"left\">majorEventType</th>");
        w.write("  <td colspan=\"2\">"+cdata(e.majorEventType.toString())+"</td>");
        w.write(" </tr\n>");

        if (true && e.getDetails() != null && e.getDetails().size() > 0) {
            for (Map.Entry<String,Object> entry: e.getDetails().entrySet()) {
                w.write(" <tr\n>");
                w.write("  <th align=\"left\">" + cdata(entry.getKey()) + "</th>");
                w.write("  <td colspan=\"4\">" + cdata(""+entry.getValue()) + "</td>");
                w.write(" </tr\n>");
            }
        }
        
        w.write("</table\n>");

    }
    
    /**
     * Write a text file into the html. The supplied resource will be relative
     * to this class.
     */
    protected void writeResource(final Writer w, final String resource)
            throws IOException {

        final InputStream is = getClass().getResourceAsStream(resource);

        if (is == null)
            throw new IOException("Resource not on classpath: " + resource);
        
        try {
            
            final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            
            String s = null;
            
            boolean first = true;
            
            // read each line (note: chops off newline).
            while ((s = reader.readLine()) != null) {
                
                if(!first) {

                    // write out the chopped off newline.
                    w.write("\n");
                    
                }
                
                w.write(s);
                
                first = false;
                
            }
            
        } finally {

            is.close();
            
        }
        
    }
    
    /**
     * Encode a string for including in a CDATA section.
     * 
     * @param s
     *            The string.
     * 
     * @return The encoded string.
     */
    static public String cdata(final String s) {

        if (s == null)
            throw new IllegalArgumentException();
        
        return HTMLUtility.escapeForXHTML(s);
        
    }
    
    /**
     * Encoding a string for including in an (X)HTML attribute value.
     * 
     * @param s
     *            The string.
     *            
     * @return
     */
    static public String attrib(final String s) {
        
        return HTMLUtility.escapeForXHTML(s);
        
    }
    
    /**
     * Formats a counter value as a String AND performs any escaping necessary
     * for inclusion in a CDATA section (we do both operations together so that
     * we can format {@link IServiceCounters#LOCAL_HTTPD} as a link anchor.
     * 
     * @param counter
     *            The counter.
     * @param value
     *            The counter value (MAY be <code>null</code>).
     * @return
     * 
     * @deprecated Move into formatter objects.
     */
    protected String value(final ICounter counter, final Object val) {
        
        if (counter == null)
            throw new IllegalArgumentException();
        
        if (val == null)
            return cdata("N/A");
        
        if(val instanceof Double || val instanceof Float) {
            
            Format fmt = model.decimalFormat;
            
            if (counter.getName().contains("%")
                    || percent_pattern.matcher(counter.getName()).matches()) {
                
                fmt = model.percentFormat;
                
            }
            
            return cdata( fmt.format(((Number)val).doubleValue()) );
            
        } else if(val instanceof Long || val instanceof Integer) {
            
            final Format fmt = model.integerFormat;
            
            return cdata(fmt.format(((Number) val).longValue()));
            
        }

        if (counter.getName().equals(IServiceCounters.LOCAL_HTTPD)) {

            return "<a href=" + val + ">" + cdata(val.toString()) + "</a>";

        }

        return cdata(val.toString());

    }

    /**
     * A pattern matching the occurrence of the word "percent" in a counter
     * name. Leading and trailing wildcards are used and the match is
     * case-insensitive.
     */
    static private final Pattern percent_pattern = Pattern.compile(
            ".*percent.*", Pattern.CASE_INSENSITIVE);

}