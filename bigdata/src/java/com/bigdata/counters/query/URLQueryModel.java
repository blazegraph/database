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
 * Created on May 26, 2009
 */

package com.bigdata.counters.query;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.counters.History;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.service.Event;
import com.bigdata.service.IEventReportingService;
import com.bigdata.service.IService;

/**
 * The model for a URL used to query an {@link ICounterSelector}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class URLQueryModel {

    protected static transient final Logger log = Logger.getLogger(URLQueryModel.class);
    
    /**
     * Name of the URL query parameter specifying the starting path for the page
     * view.
     */
    public static final String PATH = "path";

    /**
     * Depth to be displayed from the given path -or- zero (0) to display
     * all levels.
     */
    public static final String DEPTH = "depth";
    
    /**
     * URL query parameter whose value is the type of report to generate.
     * The default is {@link ReportEnum#hierarchy}.
     * 
     * @see ReportEnum
     */
    public static final String REPORT = "report";
    
    /**
     * The ordered labels to be assigned to the category columns in a
     * {@link ReportEnum#pivot} report. The order of the names in the URL
     * query parameters MUST correspond with the order of the capturing
     * groups in the {@link #REGEX}.
     */
    public static final String CATEGORY = "category";
    
    /**
     * Name of the URL query parameter specifying whether the optional
     * correlated view for counter histories will be displayed.
     * <p>
     * Note: This is a shorthand for specifying {@link #REPORT} as
     * {@value ReportEnum#correlated}.
     */
    public static final String CORRELATED = "correlated";
    
    /**
     * Name of the URL query parameter specifying one or more strings for
     * the filter to be applied to the counter paths.
     */
    public static final String FILTER = "filter";

    /**
     * Name of the URL query parameter specifying one or more regular
     * expression for the filter to be applied to the counter paths. Any
     * capturing groups in this regular expression will be used to generate
     * the column title when examining correlated counters in a table view.
     * If there are no capturing groups then the counter name is used as the
     * default title.
     */
    public static final String REGEX = "regex";

    /**
     * Name of the URL query parameter specifying that the format for the first
     * column of the history counter table view. This column is the timestamp
     * associated with the counter but it can be reported in a variety of ways.
     * The possible values for this option are specified by
     * {@link TimestampFormatEnum}.
     * 
     * @see TimestampFormatEnum
     * 
     * @todo add support for elapsed period units since the fromTime, since a
     *       specified time, or since the federation up time.
     */
    public static final String TIMESTAMP_FORMAT = "timestampFormat";

    /**
     * The reporting period to be displayed. When not specified, all periods
     * will be reported. The value may be any {@link PeriodEnum}.
     */
    public static final String PERIOD = "period";

    /**
     * Optional override of the MIME type from a URL query parameter.
     */
    public static final String MIMETYPE = "mimeType";

    /**
     * Parameter recognized as the name of the local file on which to render the
     * counters (this option is supported only by utility classes run from a
     * command line, not by the httpd interface).
     */
    public static final String FILE = "file";
    
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
     * The URI from the request.
     */
    final public String uri;

    /**
     * The parameters from the request (as parsed from URL query parameters).
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
     * @see #TIMESTAMP_FORMAT
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
    final public long fromTime;

    /**
     * The exclusive upper bound in milliseconds of the timestamp for the
     * counters or events to be selected.
     */
    final public long toTime;
    
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
    final public IEventReportingService eventReportingService;
    
    /**
     * <code>true</code> iff we need to output the scripts to support
     * <code>flot</code>.
     */
    final public boolean flot;
    
    /**
     * Used to format double and float counter values.
     */
    public final DecimalFormat decimalFormat;
    
    /**
     * Used to format counter values that can be inferred to be a percentage.
     */
    public final NumberFormat percentFormat;
    
    /**
     * Used to format integer and long counter values.
     */
    public final NumberFormat integerFormat;
    
    /**
     * Used to format the units of time when expressed as elapsed units since
     * the first sample of a {@link History}.
     */
    public final DecimalFormat unitsFormat;

    /**
     * Used to format the timestamp fields (From:, To:, and the last column) and
     * the epoch for <code>flot</code>. This is set dynamically based on the
     * {@link #TIMESTAMP_FORMAT} and the {@link #PERIOD}. Flot always requires
     * epoch numbering, so it does not use this field.
     */
    public final Format dateFormat;

    /**
     * Optional override of the MIME type from a URL query parameter.
     * 
     * @see MIMETYPE
     */
    public final String mimeType;
    
    /**
     * The name of a local file on which to write the data (this option is
     * supported only by local utility classes, not by the httpd interface).
     * 
     * @see #FILE
     */
    final public File file;
    
    /**
     * @param fed
     *            The service object IFF one was specified when
     *            {@link CounterSetHTTPD} was started.
     * @param uri
     *            Percent-decoded URI without parameters, for example
     *            "/index.cgi"
     * @param parms
     *            Parsed, percent decoded parameters from URI and, in case of
     *            POST, data. The keys are the parameter names. Each value is a
     *            {@link Vector} of {@link String}s containing the bindings for
     *            the named parameter. The order of the URL parameters is
     *            preserved by the insertion order of the {@link LinkedHashMap}
     *            and the elements of the {@link Vector} values.
     * @param header
     *            Header entries, percent decoded
     */
    public URLQueryModel(final IService service, final String uri,
            final LinkedHashMap<String, Vector<String>> params,
            final Properties headers) {
    
        this.uri = uri;

        this.params = params;
        
        this.headers = headers;

        this.path = getProperty(params, PATH, ICounterSet.pathSeparator);

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
         * @todo this should be specified by a URL query parameter and
         * passed into the IRenderer instances.
         */
//        this.decimalFormat = new DecimalFormat("0.###E0");
        this.decimalFormat = new DecimalFormat("##0.#####E0");
        
//        decimalFormat.setGroupingUsed(true);
//
//        decimalFormat.setMinimumFractionDigits(3);
//        
//        decimalFormat.setMaximumFractionDigits(6);
//        
//        decimalFormat.setDecimalSeparatorAlwaysShown(true);
        
        this.percentFormat = NumberFormat.getPercentInstance();
        
        this.integerFormat = NumberFormat.getIntegerInstance();
        
        integerFormat.setGroupingUsed(true);
        
        this.unitsFormat = new DecimalFormat("0.#");
        
        /*
         * Figure out how we will format the timestamp (From:, To:, and the last
         * column).
         */
        switch(timestampFormat) {
        case dateTime:
            /*
             * Note: I have decided to go with the long format (date + time)
             * since runs often span days and the time along is not enough
             * information.
             */
          dateFormat = DateFormat.getDateTimeInstance(
                    DateFormat.MEDIUM/* date */, DateFormat.MEDIUM/* time */);
//            switch (period) {
//            case Minutes:
//                dateFormat = DateFormat.getTimeInstance(DateFormat.SHORT);
//                break;
//            case Hours:
//                dateFormat = DateFormat.getTimeInstance(DateFormat.MEDIUM);
//                break;
//            case Days:
//                dateFormat = DateFormat.getDateInstance(DateFormat.MEDIUM);
//                break;
//            default:
//                throw new UnsupportedOperationException(period.toString());
//            }
            break;
        case epoch: {
            // milliseconds since the epoch
            final NumberFormat f = NumberFormat.getIntegerInstance();
            f.setGroupingUsed(false);
            f.setMinimumFractionDigits(0);
            dateFormat = f;
            break;
        }
        default:
            throw new UnsupportedOperationException(timestampFormat.toString());
        }

        this.mimeType = (params.containsKey(MIMETYPE) ? getProperty(params,
                MIMETYPE, null) : null);

        this.file = (params.containsKey(FILE) ? new File(getProperty(params,
                FILE, null)) : null);

        if (log.isInfoEnabled())
            log.info(FILE + "=" + file);

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
    static protected String getProperty(
            final Map<String, Vector<String>> params, final String property,
            final String defaultValue) {
        
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

        // protocol (known from the container).
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
     *            Overridden query parameters (optional).
     *            
     * @todo move to request object?
     */
    public String getRequestURL(final URLQueryParam[] override) {
    
        // Note: Used throughput to preserve the parameter order.
        final LinkedHashMap<String,Vector<String>> p;
        
        if(override == null) {
            
            p = params;
            
        } else {
            
            p = new LinkedHashMap<String,Vector<String>>(params);
            
            for(URLQueryParam x : override) {
                                        
                p.put(x.name, x.values);
                
            }
            
        }

        final StringBuilder sb = getRequestURL();
        
        sb.append("?path="
                + encodeURL(getProperty(p, PATH, ICounterSet.pathSeparator)));
        
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

    static protected String encodeURL(final String url) {

        final String charset = "UTF-8";

        try {

            return URLEncoder.encode(url, charset);

        } catch (UnsupportedEncodingException e) {

            log.error("Could not encode: charset=" + charset + ", url="
                    + url);

            return url;

        }
        
    }
    
}

