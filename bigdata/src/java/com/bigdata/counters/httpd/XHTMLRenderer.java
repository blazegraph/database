package com.bigdata.counters.httpd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.History;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterNode;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IHistoryEntry;
import com.bigdata.counters.IInstrument;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.History.SampleIterator;
import com.bigdata.counters.httpd.XHTMLRenderer.Model.ReportEnum;
import com.bigdata.counters.httpd.XHTMLRenderer.Model.TimestampFormatEnum;
import com.bigdata.service.Event;
import com.bigdata.service.IEventReportingService;
import com.bigdata.service.IService;
import com.bigdata.util.HTMLUtility;

/**
 * (X)HTML rendering of a {@link CounterSet}.
 * 
 * @todo UI widgets for regex filters, depth, correlated.
 * 
 * @todo parameterize for expand/collapse of paths, perhaps in session (need
 *       more support on the server to do that) or else just in the URL query
 *       parameters.
 * 
 * @todo make documentation available on the counters via click through on their
 *       name.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class XHTMLRenderer {
    
    final static protected Logger log = Logger.getLogger(XHTMLRenderer.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

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
         * events.majorType = AsynchronousOverflow
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

        final private String ps = ICounterSet.pathSeparator;

        /**
         * The service reference IFF one was specified when
         * {@link CounterSetHTTPD} was started.
         */
        final public IService service;
        
        /**
         * The {@link CounterSet} provided by the caller.
         */
        final public CounterSet root;
        
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
         * The ordered category column names to be used for a
         * {@link ReportEnum#pivot} report.
         * 
         * @see #CATEGORY
         */
        final public String[] category;
        
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
        final LinkedHashMap<UUID, Event> events;
        
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
         * @param root
         *            The root of the counter set.
         * @parm uri Percent-decoded URI without parameters, for example
         *       "/index.cgi"
         * @parm parms Parsed, percent decoded parameters from URI and, in case
         *       of POST, data. The keys are the parameter names. Each value is
         *       a {@link Collection} of {@link String}s containing the
         *       bindings for the named parameter. The order of the URL
         *       parameters is preserved.
         */
        public Model(final IService service, final CounterSet root,
                final String uri,
                final LinkedHashMap<String, Vector<String>> params) {

            this.service = service;
            
            this.root = root;

            this.uri = uri;

            this.params = params;

            this.path = getProperty(params, PATH, ps);

            if (INFO)
                log.info(PATH + "=" + path);

            this.depth = Integer.parseInt(getProperty(params, DEPTH, "2"));
            
            if (INFO)
                log.info(DEPTH + "=" + depth);
            
            if (depth <= 0)
                throw new IllegalArgumentException("depth must be GT ZERO(0)");

            {
            
                // the regex that we build up (if any).
                final StringBuilder sb = new StringBuilder();

                if ((Collection<String>) params.get(FILTER) != null) {

                    /*
                     * Joins multiple values for ?filter together in OR of
                     * quoted patterns.
                     */
                    final Collection<String> filter = (Collection<String>) params
                            .get(FILTER);

                    for (String val : filter) {

                        if (INFO)
                            log.info(FILTER + "=" + val);

                        if (sb.length() > 0) {

                            // OR of previous pattern and this pattern.
                            sb.append("|");

                        }

                        // non-capturing group.
                        sb.append("(?:.*" + Pattern.quote(val) + ".*)");

                    }

                }

                if ((Collection<String>) params.get(REGEX) != null) {

                    /*
                     * Joins multiple values for ?regex together in OR of
                     * patterns.
                     */
                    final Collection<String> filter = (Collection<String>) params
                            .get(REGEX);

                    for (String val : filter) {

                        if (INFO)
                            log.info(REGEX + "=" + val);

                        if (sb.length() > 0) {

                            // OR of previous pattern and this pattern.
                            sb.append("|");

                        }

                        // Non-capturing group.
                        sb.append("(?:" + val + ")");

                    }

                }

                if (sb.length() > 0) {

                    final String regex = sb.toString();

                    if (INFO)
                        log.info("effective regex filter=" + regex);

                    this.pattern = Pattern.compile(regex);

                } else {

                    this.pattern = null;

                }

            }

            if (service != null && service instanceof IEventReportingService) {

                // events are available.
                events = ((IEventReportingService) service).getEvents();

            } else {

                // events are not available.
                events = null;
                
            }
            
            if (params.containsKey(REPORT) && params.containsKey(CORRELATED)) {

                throw new IllegalArgumentException("Please use either '"
                        + CORRELATED + "' or '" + REPORT + "'");
                
            }

            if(params.containsKey(REPORT)) {
            
                this.reportType = ReportEnum.valueOf(getProperty(
                    params, REPORT, ReportEnum.hierarchy.toString()));

                if (INFO)
                    log.info(REPORT + "=" + reportType);
                
            } else {
            
                final boolean correlated = Boolean.parseBoolean(getProperty(
                        params, CORRELATED, "false"));
            
                if (INFO)
                    log.info(CORRELATED + "=" + correlated);

                this.reportType = correlated ? ReportEnum.correlated
                        : ReportEnum.hierarchy;

            }

            if (events != null) {
                
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

                if (INFO) {
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
               
                if (INFO)
                    log.info(EVENT_ORDER_BY + "="
                            + Arrays.toString(eventOrderBy));

            }
            
            switch (reportType) {
            case events:
                if (events == null) {

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

            if (INFO && category != null)
                log.info(CATEGORY + "=" + Arrays.toString(category));

            this.timestampFormat = TimestampFormatEnum.valueOf(getProperty(
                    params, TIMESTAMP_FORMAT, TimestampFormatEnum.dateTime.toString()));
            
            if (INFO)
                log.info(TIMESTAMP_FORMAT + "=" + timestampFormat);

            this.period = getProperty(params, PERIOD, null) == null ? null
                    : PeriodEnum.valueOf(getProperty(params, PERIOD, null));

            if (INFO)
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

        // @todo parameterize, perhaps by passing in as an interface.
        protected boolean isExpanded(String path) {
            
            return true;
            
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
         * Re-create the request URL.
         * 
         * @todo move to request object?
         */
        public String getRequestURL() {
            
            return getRequestURL(null);
            
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
            
            final StringBuilder sb = new StringBuilder();
            
            sb.append(uri);
            
            sb.append("?path=" + encodeURL(getProperty(p,PATH, ps)));
            
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
                
                log.error("Could not encode: charset="+charset+", url="+url);
                
                return url;
                
            }
            
        }
        
    } // class Model
    
    final private String encoding = "UTF-8";
    
    final private String ps = ICounterSet.pathSeparator;
    
    /*
     * Note: the page is valid for any of these doctypes.
     */
//    final private DoctypeEnum doctype = DoctypeEnum.html_4_01_strict
//    final private DoctypeEnum doctype = DoctypeEnum.html_4_01_transitional
    final private DoctypeEnum doctype = DoctypeEnum.xhtml_1_0_strict;
    
    /**
     * Allows override of the binding(s) for a named property.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class NV {
        
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

            w.write("<script\n type=\"text/javascript\" src=\"jquery.js\"></script\n>");

            w.write("<script\n type=\"text/javascript\" src=\"jquery.flot.js\"></script\n>");

            w.write("<!--[if IE]><script type=\"text/javascript\" src=\"excanvas.pack.js\"></script><![endif]-->");

        }
        
    }
    
    protected void writeBody(Writer w) throws IOException  {
        
        w.write("<body\n>");

        final ICounterNode node = model.root.getPath(model.path);
        
        if(node == null) {

            /*
             * Used when the path does not evaluate to anything in the
             * hierarchy. The generate markup at least lets you choose a parent
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
            
                writeCounterSet(w, (CounterSet) node, model.depth);
                
                break;
                
            case correlated:
                
                writeCorrelatedCounters(w, selectCounters((CounterSet) node,
                        model.depth));
                
                break;
                
            case pivot:
                
                writePivotTable(w, selectCounters((CounterSet) node,
                        model.depth));
                
                break;
                
            case events:
                
                // render the time-series chart
                synchronized(model.events) {
                    
                    writeFlot(w, model.events);
                    
                }

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
     */
    protected void writeFullPath(Writer w, String path) throws IOException {
        
        writePath(w, path, 0/*root*/);
        
    }
    
    /**
     * A clickable trail of the path.
     * 
     * @param rootDepth
     *            The path components will be shown beginning at this depth -
     *            ZERO (0) is the root.
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
     * {@link CounterSet} in a single table.
     */
    protected void writeCounterSet(Writer w, final CounterSet counterSet,
            final int depth) throws IOException {
        
        // depth of the hierarchy at the point where we are starting.
        final int ourDepth = counterSet.getDepth();

        if (INFO)
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

            if (DEBUG)
                log.debug("considering: " + node.getPath());
            
            if(depth != 0) { 
                
                final int counterDepth = node.getDepth();
                
//                log.info("counterDepth("+counterDepth+") - rootDepth("+rootDepth+") = "+(counterDepth-rootDepth));
                
                if((counterDepth - ourDepth) > depth) {
                
                    // prune rendering
                    if (DEBUG)
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
     * Writes details on a single counter.
     * 
     * @param counter
     *            The counter.
     * @throws IOException 
     */
    protected void writeCounter(Writer w, ICounter counter) throws IOException {
        
//        w.write("<p>path: ");
//        
//        writePath(w, path);
//
//        w.write("</p>");
//
//        w.write("<p>value: ");
//        
//        // the most recent value.
//        w.write(cdata(value(counter.getValue())));
//        
//        w.write("</p>");
//
//        w.write("<p>time: ");
//
//        w.write(cdata(new Date(counter.lastModified()).toString()));
//        
//        w.write("</p>");

        if(counter.getInstrument() instanceof HistoryInstrument) {
         
            writeHistoryCounter(w, counter);
            
        }
        
    }
    
    /**
     * Writes details on a single counter whose {@link IInstrument} provides a
     * history. The goal is to be able to easily copy and paste the data into a
     * program for plotting, e.g., as an X-Y graph (values against time).
     * 
     * @param counter
     *            The counter.
     * 
     * @see HistoryInstrument
     */
    protected void writeHistoryCounter(Writer w, ICounter counter)
            throws IOException {

        HistoryInstrument inst = (HistoryInstrument) counter.getInstrument();

        if (inst.minutes.size() > 0) {
            w.write("<p>");
            w.write("</p>");
            writeSamples(w, counter, inst.minutes);
        }

        if (inst.hours.size() > 0) {
            w.write("<p>");
            w.write("</p>");
            writeSamples(w, counter, inst.hours);
        }

        if (inst.days.size() > 0) {
            w.write("<p>");
            w.write("</p>");
            writeSamples(w, counter, inst.days);
        }
        
    }

    /**
     * Writes a table containing the samples for a {@link History} for some
     * {@link ICounter}.
     * 
     * @param w
     * @param counter
     * @param h
     * 
     * @throws IOException
     */
    protected void writeSamples(Writer w, ICounter counter, History h) throws IOException {
        
        /*
         * Figure out the label for the units of the history.
         * 
         * FIXME The history really needs to be separately informed of the
         * period (in ms) of its buckets (that is, how many seconds, minutes,
         * hours, etc), and the #of buckets that it will collect. The two are
         * not of necessity aligned: the source MUST include at least enough
         * samples to make up one sample at the higher level, but the source MAY
         * collect more samples and the higher level MAY collect an arbitrary
         * number of samples. Change this in History, allow configuration of the
         * HistoryInstrument, make sure that interchange reflects this, and then
         * replace the explicitly specified period here with the appropriate
         * method call on the History object. And add a label to the history
         * object or a typesafe enum for minutes, hours, days, etc.
         */
        final String units;
        final DateFormat dateFormat;
        final long period;
        if (h.getPeriod() == 1000 * 60L) {
            units = "Minutes";
            period = 1000*60;// 60 seconds (in ms).
            dateFormat = DateFormat.getTimeInstance(DateFormat.SHORT);
        } else if (h.getPeriod() == 1000 * 60 * 24L) {
            units = "Hours";
            period = 1000*60*60;// 60 minutes (in ms).
            dateFormat = DateFormat.getTimeInstance(DateFormat.MEDIUM);
        } else if (h.getSource() != null
                && h.getSource().getPeriod() == 1000 * 60 * 24L) {
            units = "Days";
            period = 1000*60*60*24;// 24 hours (in ms).
            dateFormat = DateFormat.getDateInstance(DateFormat.MEDIUM);
        } else {
            throw new AssertionError("period="+h.getPeriod());
//            units = "period=" + h.getPeriod() + "ms";
//            dateFormat = DateFormat.getDateTimeInstance();
        }

        /*
         * Iterator will visit the timestamped samples in the history.
         * 
         * Note: the iterator is a snapshot of the history at the time that the
         * iterator is requested.
         * 
         * @todo This scans the history first, building up the table rows in a
         * buffer. Originally that was required to get the first/last timestamps
         * from the history before we format the start of the table but now the
         * iterator will report those timestamps directly and this could be
         * moved inline.
         */
        final SampleIterator itr = h.iterator();

        final StringBuilder sb = new StringBuilder();

        final long firstTimestamp = itr.getFirstSampleTime();

        final long lastTimestamp = itr.getLastSampleTime();

        while (itr.hasNext()) {

            final IHistoryEntry sample = itr.next();

            sb.append(" <tr\n>");

            final long lastModified = sample.lastModified();

            /*
             * The time will be zero for the first row and a delta (expressed in
             * the units of the history) for the remaining rows.
             * 
             * Note: The time units are computed using floating point math and
             * then converted to a display form using formatting in order to be
             * able to accurately convey where a sample falls within the
             * granularity of the unit (e.g., early or late in the day).
             */
            final String timeStr = model.unitsFormat
                    .format(((double) lastModified - firstTimestamp) / period);

            sb.append("  <td>" + cdata(timeStr) + "</td\n>");

            sb.append("  <td>" + value(counter, sample.getValue())
                    + "</td\n>");

            sb.append("  <td>"
                    + cdata(dateFormat.format(new Date(lastModified)))
                    + "</td\n>");

            sb.append(" </tr\n>");

        }

        /*
         * Summary for the table.
         * 
         * @todo add some more substance to the summary?
         */
        final String summary = "Showing samples: period=" + units + ", path="
                + counter.getPath();

        /*
         * Format the entire table now that we have all the data on hand.
         */

        w.write("<table border=\"1\" summary=\"" + attrib(summary) + "\"\n>");

        // // caption : @todo use css to left justify the path.
        // w.write(" <caption>");
        // writePath(w, counter.getPath());
        // w.write(" </caption\n>");

        // header row.
        w.write(" <tr\n>");
        w.write("  <th colspan=\"3\">");
        writeFullPath(w, counter.getPath());
        w.write("  </th\n>");
        w.write(" </tr\n>");

        // header row.
        w.write(" <tr\n>");
        w.write("  <th>" + "From: "
                + dateFormat.format(new Date(firstTimestamp)) + "</th\n>");
        w.write("  <th>" + "To: " + dateFormat.format(new Date(lastTimestamp))
                + "</th\n>");
        // w.write(" <th></th>");
        w.write(" </tr\n>");

        // header row.
        w.write(" <tr\n>");
        w.write("  <th>" + cdata(units) + "</th\n>");
        w.write("  <th>" + cdata(counter.getName()) + "</th\n>");
        w.write("  <th>Timestamp</th>\n");
        w.write(" </tr\n>");

        // data rows.
        w.write(sb.toString());

        w.write("</table\n>");

    }

    /**
     * A class representing one or more performance counter histories where
     * those histories have been aligned so that the individual timestamps for
     * the performance counter values in each row are aligned.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class HistoryTable {

        /**
         * The selected counters.
         */
        final ICounter[] a;

        /**
         * Identifies the history to be written for each of the selected
         * counters by its based reporting period.
         */
        final PeriodEnum basePeriod;

        /**
         * 
         * @param a
         *            The selected counters.
         * @param basePeriod
         *            Identifies the history to be written for each of the
         *            selected counters by its based reporting period.
         */
        public HistoryTable(final ICounter[] a,
                final PeriodEnum basePeriod) {
            
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
            period = basePeriod.getBasePeriodMillis();

            if (INFO)
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
                        .getInstrument()).getHistory(basePeriod).iterator();

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

                if (INFO) {

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

                if (DEBUG && col == 0)
                    log.debug(a[col].getPath());

                while (itr.hasNext()) {

                    final IHistoryEntry e = itr.next();

                    final int row = i + firstRow;

                    if (DEBUG && col == 0)
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
        final long firstTimestamp;

        /**
         * The most recent timestamp in the selected history units for any of
         * the specified counters.
         */
        final long lastTimestamp;

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
        final int nrows;

        /**
         * The #of columns in the table.  This is the same as the #of counters
         * specified to the ctor.
         */
        final int ncols;
        
        /**
         * The label for the units of the history.
         */
        final String units;
        
        /**
         * The #of milliseconds in each unit for {@link #units}.
         */
        final long period;

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
        final IHistoryEntry[][] data;

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
    
    /**
     * Writes out a table containing the histories for the selected counters.
     * <p>
     * The <code>From:</code> and <code>To:</code> values will report the
     * first timestamp in any history and the last timestamp in history
     * respectively. 
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

        /*
         * Generate the table.
         * 
         * The table has one column for each counter having history data. The
         * 1st header row of the table has the counter path. The seconds header
         * row has just the counter name. The data rows are the samples from
         * each period of the history for each counter.
         */

        // the table start tag.
        {
            /*
             * Summary for the table.
             * 
             * @todo add some more substance to the summary?
             */
            final String summary = "Showing samples: period=" + t.units;

            /*
             * Format the entire table now that we have all the data on hand.
             */

            w.write("<table border=\"1\" summary=\"" + attrib(summary)
                    + "\"\n>");

            // // caption : @todo use css to left justify the path.
            // w.write(" <caption>");
            // writePath(w, counter.getPath());
            // w.write(" </caption\n>");
        }

        // the header rows.
        {

            // header row.
            w.write(" <tr\n>");
            w.write("  <th></th\n>");
            w.write("  <th>"
                    + "From: "
                    + cdata((dateFormat != null ? dateFormat.format(new Date(
                            t.firstTimestamp)) : Long.toString(t.firstTimestamp)))
                    + "</th\n>");
            w.write("  <th colspan=\"*\">"
                    + "To: "
                    + cdata((dateFormat != null ? dateFormat.format(new Date(
                            t.lastTimestamp)) : Long.toString(t.lastTimestamp)))
                    + "</th\n>");
            w.write(" </tr\n>");

            // header row.
            w.write(" <tr\n>");
            w.write("  <th></th\n>");
            for (ICounter counter : a) {
                w.write("  <th colspan=\"*\">");
                writeFullPath(w, counter.getPath());
                w.write("  </th\n>");
            }
            w.write(" </tr\n>");

            // header row.
            w.write(" <tr\n>");
            w.write("  <th>" + cdata(t.units) + "</th\n>");
            for (ICounter counter : a) {
                /*
                 * If the pattern included capturing groups then use the matched
                 * groups as the label for the column.
                 */
                final String[] groups = getCapturedGroups(counter);
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
                w.write("  <th>" + cdata(label) + "</th\n>");
            }
            w.write("  <th>Timestamp</th>\n");
            w.write(" </tr\n>");

        }

        // for each row.
        for (int row = 0; row < t.nrows; row++) {

            /*
             * Populate array with the formatted values from each counter's
             * history for the current row.
             */

            final String[] valStr = new String[a.length];

            // for each counter (in a fixed order).
            for (int col = 0; col < t.ncols; col++) {

                final ICounter c = a[col];

                final IHistoryEntry e = t.data[row][col];

                valStr[col] = value(c, e == null ? "" : e.getValue());

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

            final String timeStr = model.unitsFormat
                    .format(((double) timestamp - t.getTimestamp(0/* row */))
                            / t.period);

            w.write(" <tr\n>");

            w.write("  <td>" + cdata(timeStr) + "</td\n>");
            
            for(String s : valStr) {

                w.write("  <td>" + s + "</td\n>");
                
            }

            w.write("  <td>"
                    + cdata((dateFormat != null ? dateFormat.format(new Date(
                            timestamp)) : Long.toString(timestamp)))
                    + "</td\n>");

            w.write(" </tr\n>");

        } // next row.
       
        // the table end tag.
        w.write("</table\n>");

    }

    /**
     * Return the data captured by {@link Model#pattern} from the path of the
     * specified <i>counter</i>.
     * <p>
     * Note: This is used to extract the parts of the {@link Model#pattern}
     * which are of interest - presuming that you mark some parts with capturing
     * groups and other parts that you do not care about with non-capturing
     * groups.
     * <p>
     * Note: There is a presumption that {@link Model#pattern} was used to
     * {@link #selectCounters(CounterSet, int)} and that <i>counter</i> is one
     * of the selected counters, in which case {@link Model#pattern} is KNOWN to
     * match {@link ICounterNode#getPath()}.
     * 
     * @return The captured groups -or- <code>null</code> if there are no
     *         capturing groups.
     */
    protected String[] getCapturedGroups(final ICounter counter) {

        if (counter == null)
            throw new IllegalArgumentException();
        
        final Matcher m = model.pattern.matcher(counter.getPath());
        
        // #of capturing groups in the pattern.
        final int groupCount = m.groupCount();

        if(groupCount == 0) {
            
            // No capturing groups.
            return null;

        }

        if (!m.matches()) {

            throw new IllegalArgumentException("No match? counter=" + counter
                    + ", regex=" + model.pattern);

        }

        /*
         * Pattern is matched w/ at least one capturing group so assemble a
         * label from the matched capturing groups.
         */

        if (DEBUG) {
            log.debug("input  : " + counter.getPath());
            log.debug("pattern: " + model.pattern);
            log.debug("matcher: " + m);
            log.debug("result : " + m.toMatchResult());
        }

        final String[] groups = new String[groupCount];

        for (int i = 1; i <= groupCount; i++) {

            final String s = m.group(i);

            if (DEBUG)
                log.debug("group[" + i + "]: " + m.group(i));

            groups[i - 1] = s;

        }

        return groups;

    }

    /**
     * Pairs together an ordered set of category values for a pivot table with
     * the set of counters which share that set of category values.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class CSet {
        
        /**
         * The set of ordered category values.
         */
        final String[] cats;
        
        /**
         * The set of counters sharing the same set of ordered category values.
         */
        final List<ICounter> counters;
        
        public String toString() {

//            final StringBuilder sb = new StringBuilder();
//
//            int i = 0;
//
//            for (ICounter c : counters) {
//
//                if (i > 0)
//                    sb.append(",");
//
//                // @todo use c.getPath() if you want to see WHICH counter was included.
//                sb.append(c.getName());
//
//                i++;
//                
//            }
            
            return "CSet{cats=" + Arrays.toString(cats) + //
                    ", #counters="+counters.size()+//
                    // Note: This is just too much detail.
                    //",counters=[" + sb + "]" + //
                    "}";
            
        }

        /**
         * Create a set based on the specified category values and initially
         * containing the specified {@link ICounter}.
         * 
         * @param cats
         *            An ordered set of category values.
         * @param counter
         *            A counter from whose {@link ICounterNode#getPath()} the
         *            category values were extracted as capturing groups.
         */
        public CSet(final String[] cats, final ICounter counter) {
            
            if(cats == null)
                throw new IllegalArgumentException();

            if(counter == null)
                throw new IllegalArgumentException();

            this.cats = cats;
            
            this.counters = new LinkedList<ICounter>();
            
            add( counter );
            
        }

        /**
         * Add another counter to that set.
         * 
         * @param counter
         *            The counter.
         */
        public void add(final ICounter counter) {

            if(counter == null)
                throw new IllegalArgumentException();

            counters.add( counter );
            
        }
        
    }
    
    /**
     * The set of distinct ordered matched sets of category values in the
     * current row of the history table paired with the {@link ICounter}s
     * matched up on those category values.
     */
    protected List<CSet> getCategoryValueSets(final ICounter[] a) {

        if (a == null)
            throw new IllegalArgumentException();
        
        // maximum result is one set per counter.
        final String[][] sets = new String[a.length][];

        for (int i = 0; i < a.length; i++) {

            final ICounter c = a[i];

            if (a[i] == null)
                throw new IllegalArgumentException();
            
            sets[i] = getCapturedGroups(c);

        }

        /*
         * Now figure out which of those sets are distinct. Each time we find a
         * set that duplicates the current set we clear its reference. After
         * each set has been checked for duplicates in the set of sets we move
         * on to the next set whose reference has not been cleared. We are done
         * when all references in [sets] have been cleared.
         */
        final List<CSet> csets = new LinkedList<CSet>();

        for(int i=0; i<sets.length; i++) {
            
            final String[] set = sets[i];
            
            if(set == null) // already cleared.
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

                if(same) {
                    
                    // clear oset reference since it is a duplicate.
                    sets[j] = null;
            
                    // add another counter to that set.
                    cset.add(a[j]);
                    
                }
                
            }
            
        }
        
        return csets;

    }
        
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
     * is timestamp associated with the row. The format of that timestamp is
     * specified by {@link Model#timestampFormat}.
     * <p>
     * There are three category columns (hostname, indexName, and
     * indexPartitionName). The category were selected by the capturing groups
     * in {@link Model#pattern} and take on different values for each row in the
     * table. In order to get nice column names you MUST specify the column
     * names using the {@link Model#CATEGORY} URL query parameter. The given
     * category column names are used in order and synthetic column names are
     * generated if none (or not enough) were specified in the {@link Model}.
     * <p>
     * There are four counter columns. Each counter column corresponds to an
     * {@link ICounter} whose path was matched by the {@link Model#pattern}.
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
     * Would match only the first three rows and would capture
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

        final PivotTable pt = new PivotTable(t);
        
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

        /*
         * Generate the table.
         */

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

            final String unitStr = cdata(model.unitsFormat
                    .format(((double) timestamp - t.getTimestamp(0/* row */))
                            / t.period));

            final String timeStr = cdata((dateFormat != null ? dateFormat
                    .format(new Date(timestamp)) : Long.toString(timestamp)));
            
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

                    if (DEBUG && valueCountForColumn > 0)
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

            }
            
            w.write(" </tr\n>");

        } // next row.
       
        // the table end tag.
        w.write("</table\n>");

    }

    /**
     * Aggregates data from a table by grouping the cells in the table into sets ({@link CSet}s)
     * of category columns. The values for cells belonging to the same
     * {@link CSet} are aggregated for each distinct
     * {@link ICounterNode#getName()}. 
     */
    class PivotTable {
       
        /**
         * The HistoryTable (converts counter heirarchy into regular table).
         */
        public final HistoryTable src;
        
        /**
         * The selected counters (redundent reference to {@link HistoryTable#a}.
         */
        final ICounter[] a;

        /**
         * The ordered set of distinct counter names. The order of the selected
         * counters is preserved here (minus duplicate counter names) due to the
         * virtues of the linked hash set.
         */
        final LinkedHashSet<String> vcols;
        
        /**
         * Aggregation of the selected counters ({@link #a}) into sets sharing
         * the same category values.
         */
        final List<CSet> csets;
        
        /**
         * An array of category column names. The names can be specified using
         * URL query parameters. When they are not specified or when there are
         * not enough specified parameters then we use some generated names.
         * 
         * @see Model#CATEGORY
         */
        final String[] cnames;
        
        /**
         * 
         * @param t
         *            The source data.
         */
        public PivotTable(final HistoryTable t) {

            if(t == null)
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

            if (INFO)
                log.info("vnames: " + vcols);

            // aggregate counters into sets sharing the same category values.
            csets = getCategoryValueSets(a);

            if (INFO)
                log.info("csets: " + csets);

            // #of capturing groups in the pattern.
            final int ngroups = model.pattern.matcher("").groupCount();

            if (INFO)
                log.info("ngroups=" + ngroups);

            /*
             * An array of category column names. The names can be specified using
             * URL query parameters. When they are not specified or when there are
             * not enough specified parameters then we use some generated names.
             */
            cnames = new String[ngroups];

            for (int i = 0; i < ngroups; i++) {

                if (model.category != null && model.category.length > i) {

                    cnames[i] = model.category[i];

                } else {

                    cnames[i] = "group#" + i;

                }

            }

            if (INFO)
                log.info("category names=" + Arrays.toString(cnames));
            
            // for each row in the HistoryTable.
            for (int row = 0; row < t.nrows; row++) {

                // The timestamp for the row.
                final long timestamp = t.getTimestamp(row);
                
                /*
                 * The set of distinct ordered matched category values in the
                 * current row of the history table.
                 */
                for(CSet cset : csets) {

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

                        if (DEBUG && valueCountForColumn > 0)
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
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
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

                if (cset.cats.length != PivotTable.this.cnames.length)
                    throw new IllegalArgumentException();
                
                if (values == null)
                    throw new IllegalArgumentException();

                if (values.length != PivotTable.this.vcols.size())
                    throw new IllegalArgumentException();

                this.row = row;
                
                this.timestamp = timestamp;
                
                this.cset = cset;
                
                this.values = values;

            }

        }

    }

    /**
     * Selects and returns all counters with history in the hierarchy starting
     * with the specified {@link CounterSet} up to the specified depth.
     */
    protected ICounter[] selectCounters(final CounterSet counterSet,
            final int depth) {

        // depth of the hierarchy at the point where we are starting.
        final int ourDepth = counterSet.getDepth();

        if (INFO)
            log.info("path=" + counterSet.getPath() + ", depth=" + depth
                    + ", ourDepth=" + ourDepth);

        final Iterator<ICounterNode> itr = counterSet.getNodes(model.pattern);

        final Vector<ICounter> counters = new Vector<ICounter>();
        
        while(itr.hasNext()) {

            final ICounterNode node = itr.next();

            if (DEBUG)
                log.debug("considering: " + node.getPath());
            
            if(depth != 0) { 
                
                final int counterDepth = node.getDepth();
                
                if((counterDepth - ourDepth) > depth) {
                
                    // prune rendering
                    if (DEBUG)
                        log.debug("skipping: " + node.getPath());
                    
                    continue;
                    
                }
                
            }
            
            if(node instanceof ICounter) {

                final ICounter c = (ICounter) node;
                
                if(c.getInstrument() instanceof HistoryInstrument) {

                    counters.add( c );
                    
                }
                
            }
            
        }
        
        final ICounter[] a = counters.toArray(new ICounter[counters.size()]);

        return a;
        
    }
    
    /**
     * Writes all counters with history in the hierarchy starting with the
     * specified {@link CounterSet} in a single table using a correlated view.
     */
    protected void writeCorrelatedCounters(final Writer w, final ICounter[] a)
            throws IOException {
        
        if (model.period == null) {

            /*
             * Report for all periods.
             */
            
            writeHistoryTable(w, a, PeriodEnum.Minutes, model.timestampFormat);

            writeHistoryTable(w, a, PeriodEnum.Hours, model.timestampFormat);

            writeHistoryTable(w, a, PeriodEnum.Days, model.timestampFormat);

        } else {

            /*
             * Report only the specified period.
             */

            switch (model.period) {
            case Minutes:
                writeHistoryTable(w, a, PeriodEnum.Minutes, model.timestampFormat);
                break;
            case Hours:
                writeHistoryTable(w, a, PeriodEnum.Hours, model.timestampFormat);
                break;
            case Days:
                writeHistoryTable(w, a, PeriodEnum.Days, model.timestampFormat);
                break;
            default:
                throw new AssertionError(model.period.toString());
            }

        }
        
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
            final LinkedHashMap<UUID, Event> events) throws IOException {
        
        writeResource(w, "flot-start.txt");
        
        writeEvents(w, events);
        
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

                if (DEBUG)
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
            final LinkedHashMap<UUID, Event> events) throws IOException {

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
        
//        int y = 1;

        int naccepted = 0;
        
        for (Map.Entry<UUID, Event> entry : events.entrySet()) {
        
            final Event e = entry.getValue();
            
            // apply the event filters.
            if (!e.isComplete() || !acceptEvent(e)) {
                
                continue;
                
            }

            naccepted++;

            final String key = getEventKey(e);
            
//            {
//
//                final int i = e.hostname.indexOf('.');
//
//                if (i >= 0) {
//
//                    hostname = e.hostname.substring(0, i);
//
//                } else {
//
//                    hostname = e.hostname;
//
//                }
//                
//            }

            // basename for the variables for this data series.
            final String series;

            // per event-group buffer
            StringBuilder eventsSB = eventsByHost.get(key);
            
            if (eventsSB == null) {
                
                eventsSB = new StringBuilder();
                
                series = "series_" + seriesByGroup.size();

                seriesByGroup.put(key, series);

                eventsByHost.put(key, eventsSB);
                
//                final int hosty = y++; //Integer.valueOf(e.hostname.substring(5, i));
                
//                final String hostvar = series;
                
//                eventsSB.append("var " + series + "y;\n"); // declare hostyvar
                
//                eventsSB.append(" = ");
//                
//                eventsSB.append(hosty);
                
//                eventsSB.append(";\n");
                
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
            
//            eventsSB.append(hostyvar);
//            
//            eventsSB.append(" ], [ ");
//            
//            eventsSB.append((e.getEndTime() + e.getStartTime()) / 2);
//
            // eventsSB.append(", ");

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
            
//            tooltipsSB.append("null, ");
            
            /*
             * use the tab-delimited format, but remove the trailing newline.
             */
            final String tooltip = e.toString().replace("\n", "");
            
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

        log.warn("accepted: " + naccepted + " out of " + events.size()
                + " events");
        
        // all series.
        final String[] keys = eventsByHost.keySet().toArray(new String[0]);
        
        // put into lexical order.
        Arrays.sort(keys);
        
//        for (StringBuilder sb : eventsByHost.values()) {
 
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
     * @return
     */
    protected String cdata(final String s) {

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
    protected String attrib(final String s) {
        
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