package com.bigdata.counters.httpd;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import com.bigdata.counters.IInstrument;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.History.SampleIterator;
import com.bigdata.util.HTMLUtility;

/**
 * (X)HTML rendering of a {@link CounterSet}.
 * 
 * @todo UI widgets for regex filters and depth
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
         * Name of the URL query parameter specifying whether the optional
         * correlated view for counter histories will be displayed.
         */
        static final String CORRELATED = "correlated";
        
        /**
         * Name of the URL query parameter specifying a regular expression for the
         * filter to be applied to the counter paths.
         */
        static final String FILTER = "filter";
        
        final private String ps = ICounterSet.pathSeparator;

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
        final public Map<String,Vector<String>> params;
        
        /**
         * The value of the {@link #PATH} query parameter. 
         */
        final public String path;
        
        /**
         * The value of the {@link #DEPTH} query parameter.
         */
        final public int depth;
        
        /**
         * The value(s) of the {@link #FILTER} query parameter.
         */
        final public Collection<String> filter;
        
        /**
         * When <code>true</code> a correlated view will be generated for the
         * spanned counters showing all samples for each counter in a column
         * where the rows are all for the same sample period. Counters without
         * history are elided in this view.
         */
        final public boolean correlated;
        
        /**
         * The {@link Pattern} compiled from the {@link #FILTER} query
         * parameters and <code>null</code> iff there are no {@link #FILTER}
         * query parameters.
         */
        final public Pattern pattern;
        
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
         * 
         */
        public Model(CounterSet root, String uri, Map<String,Vector<String>> params) {

            this.root = root;

            this.uri = uri;

            this.params = params;

            this.path = getProperty(params, PATH, ps);
            log.info(PATH+"="+path);

            // @todo must be non-negative.
            this.depth = Integer.parseInt(getProperty(params, DEPTH, "2"));
            log.info(DEPTH+"="+depth);

            this.filter = (Collection<String>)params.get(FILTER);

            if(filter != null) {

                /*
                 * Joins multiple values for ?filter together in OR of quoted
                 * patterns.
                 * 
                 * @todo make this more flexible in terms of allowing actual
                 * regex from the user agent?
                 */

                final StringBuilder sb = new StringBuilder();
                
                for(String val : filter) {
                
                    log.info("filter="+val);
                
                    if(sb.length()>0) {
                        
                        sb.append("|");
                        
                    }
                    
                    sb.append("(.*"+Pattern.quote(val)+".*)");
                    
                }

                final String regex = sb.toString();
                
                log.info(FILTER+"=" + regex);
                
                this.pattern = Pattern.compile(regex);
                
            } else {
                
                this.pattern = null;
                
            }

            this.correlated = Boolean.parseBoolean(getProperty(params,
                    CORRELATED, "false"));
            log.info(CORRELATED + "=" + correlated);
            
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
        public String getProperty(Map<String,Vector<String>> params, String property, String defaultValue) {
            
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
        public String getRequestURL(NV[] override) {
        
            final Map<String,Vector<String>> p;
            
            if(override == null) {
                
                p = params;
                
            } else {
                
                p = new HashMap<String,Vector<String>>(params);
                
                for(NV x : override) {
                                            
                    p.put(x.name, x.values);
                    
                }
                
            }
            
            StringBuilder sb = new StringBuilder();
            
            sb.append(uri);
            
            sb.append("?path=" + encodeURL(getProperty(p,PATH, ps)));
            
            Iterator itr = p.entrySet().iterator();
            
            while(itr.hasNext()) {
                
                final Map.Entry entry = (Map.Entry)itr.next();
                
                String name = (String)entry.getKey();

                if(name.equals(PATH)) {
                    
                    // already handled.
                    continue;
                    
                }
                
                Collection<String> vals = (Collection<String>)entry.getValue();
                
                for(String s : vals ) {

                    sb.append("&"+encodeURL(name)+"="+encodeURL(s));
                    
                }
                
            }
            
            return sb.toString();
            
        }
        
        static public String encodeURL(String url) {
            
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
    public XHTMLRenderer(Model model) {

        if(model==null) throw new IllegalArgumentException();
        
        this.model = model;
        
    }

    public void write(Writer w) throws IOException {

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
        
        w.write("</head\n>");
    }
    
    protected void writeTitle(Writer w)  throws IOException {
        
        w.write("<title>bigdata(tm) telemetry : "+cdata(model.path)+"</title\n>");
        
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

            if(model.correlated) {

                writeCorrelatedCounters(w, (CounterSet)node, model.depth);
                
            } else {
                
                writeCounterSet(w, (CounterSet)node, model.depth);
                
            }
            
//            Iterator<CounterSet> itr = ((CounterSet) node).preOrderIterator();
//
//            while (itr.hasNext()) {
//
//                final CounterSet cset = itr.next();
//
//                final String path = cset.getPath();
//
//                if (isExpanded(path)) {
//
//                    // write only the expanded children.
//                    writeCounterSet(w, cset);
//
//                }
//
//            }

        }
        
        /*
         * It is valid, but having the image here this makes copy and paste a
         * little more difficult.
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

        if(log.isInfoEnabled())
        log.info("path="+counterSet.getPath()+", depth="+depth+", ourDepth="+ourDepth);

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

            if(log.isDebugEnabled())
            log.debug("considering: "+node.getPath());
            
            if(depth != 0) { 
                
                final int counterDepth = node.getDepth();
                
//                log.info("counterDepth("+counterDepth+") - rootDepth("+rootDepth+") = "+(counterDepth-rootDepth));
                
                if((counterDepth - ourDepth) > depth) {
                
                    // prune rendering
                    if(log.isDebugEnabled())
                    log.debug("skipping: "+node.getPath());
                    
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
         * iterator will report those timestamps directory and this could be
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
     * Writes out a table containing the histories for the selected counters.
     * 
     * @param a
     *            The selected counters.
     * @param basePeriod
     *            Identifies the history to be written for each of the selected
     *            counters by its based reporting period.
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
    protected void writeHistoryTable(Writer w, ICounter[] a,
            PeriodEnum basePeriod) throws IOException {

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

        /*
         * Create a snapshot iterator for each counter and find the first and
         * last timestamp across the samples for all histories that will be
         * written into the table.
         */
        long firstTimestamp = Long.MAX_VALUE;
        long lastTimestamp = Long.MIN_VALUE;
        int maxSamplesIndex = -1;
        final SampleIterator[] hitrs = new SampleIterator[a.length];
        {

            for (int i = 0; i < a.length; i++) {

                if (a[i] == null)
                    throw new IllegalArgumentException();

                if (!(a[i].getInstrument() instanceof HistoryInstrument)) {

                    throw new IllegalArgumentException();

                }

                // snapshot iterator for the history for that counter.
                final SampleIterator itr = ((HistoryInstrument) a[i]
                        .getInstrument()).getHistory(basePeriod).iterator();

                hitrs[i] = itr;

                if (itr.getFirstSampleTime() < firstTimestamp) {

                    // update the earliest timestamp for the histories.
                    firstTimestamp = itr.getFirstSampleTime();

                }

                if (itr.getLastSampleTime() > lastTimestamp) {

                    // update the latest timestamp for the histories.
                    lastTimestamp = itr.getLastSampleTime();

                }

                if (maxSamplesIndex == -1
                        || itr.getSampleCount() > hitrs[maxSamplesIndex]
                                .getSampleCount()) {

                    // update the index of the history with the most samples.
                    maxSamplesIndex = i;
                    
                }

            }

        }
        // the maximum #of samples.
        final int maxSamples = hitrs[maxSamplesIndex].getSampleCount();

        if(maxSamples == 0) {
            
            // No data.
            
            return;
            
        }
        
        /*
         * Figure out the label for the units of the history.
         */
        final String units = basePeriod.name();
        final long period = basePeriod.getBasePeriodMillis();
        final DateFormat dateFormat;
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
            final String summary = "Showing samples: period=" + units;

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
            w.write("  <th>" + "From: "
                    + dateFormat.format(new Date(firstTimestamp)) + "</th\n>");
            w.write("  <th colspan=\"*\">" + "To: "
                    + dateFormat.format(new Date(lastTimestamp)) + "</th\n>");
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
            w.write("  <th>" + cdata(units) + "</th\n>");
            for (ICounter counter : a) {
                w.write("  <th>" + cdata(counter.getName()) + "</th\n>");
            }
            w.write("  <th>Timestamp</th>\n");
            w.write(" </tr\n>");

        }

        // for each row.
        for (int row = 0; row < maxSamples; row++) {

            /*
             * Populate array with the values from each counter's history for
             * the current row. We also figure out the timestamp that we will
             * report as the "timestamp" for the row (in reality, each cell in
             * the row has its own timestamp).
             */
            final String[] valStr = new String[a.length];
            long lastModified = -1;

            // for each counter (in a fixed order).
            for (int col = 0; col<a.length; col++) {

                final ICounter c  = a[col];

                final SampleIterator itr = hitrs[col];

                if (itr.hasNext()) {

                    final IHistoryEntry sample = itr.next();

                    valStr[col] = value(c, sample.getValue());

                    if (sample.lastModified() > lastModified) {

                        /*
                         * Note: this is the greatest value of [lastModified]
                         * for the row of samples. We could report the earlest
                         * value or the average of the values instead.
                         */
                        
                        lastModified = sample.lastModified();

                    }
                    
                } else {
                    
                    valStr[col] = "";
                    
                }

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

            final String timeStr = model.unitsFormat
                    .format(((double) lastModified - firstTimestamp) / period);

            w.write(" <tr\n>");

            w.write("  <td>" + cdata(timeStr) + "</td\n>");
            
            for(String s : valStr) {

                w.write("  <td>" + s + "</td\n>");
                
            }

            w.write("  <td>" + cdata(dateFormat.format(new Date(lastModified)))
                    + "</td\n>");

            w.write(" </tr\n>");

        } // next row.
       
        // the table end tag.
        w.write("</table\n>");

    }
    
    /**
     * Writes all counters with history in the hierarchy starting with the
     * specified {@link CounterSet} in a single table using a correlated view.
     */
    protected void writeCorrelatedCounters(Writer w, final CounterSet counterSet,
            final int depth) throws IOException {
        
        // depth of the hierarchy at the point where we are starting.
        final int ourDepth = counterSet.getDepth();

        if(log.isInfoEnabled())
        log.info("path="+counterSet.getPath()+", depth="+depth+", ourDepth="+ourDepth);

        final Iterator<ICounterNode> itr = counterSet.getNodes(model.pattern);

        Vector<ICounter> counters = new Vector<ICounter>();
        
        while(itr.hasNext()) {

            final ICounterNode node = itr.next();

            if(log.isDebugEnabled())
            log.debug("considering: "+node.getPath());
            
            if(depth != 0) { 
                
                final int counterDepth = node.getDepth();
                
                if((counterDepth - ourDepth) > depth) {
                
                    // prune rendering
                    if(log.isDebugEnabled())
                    log.debug("skipping: "+node.getPath());
                    
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
        
        writeHistoryTable(w, a, PeriodEnum.Minutes);

        writeHistoryTable(w, a, PeriodEnum.Hours);
        
        writeHistoryTable(w, a, PeriodEnum.Days);
        
    }
    
    /**
     * Encode a string for including in a CDATA section.
     * 
     * @param s
     *            The string.
     * 
     * @return
     */
    protected String cdata(String s) {

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
    protected String attrib(String s) {
        
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
    protected String value(ICounter counter,Object val) {
        
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

        if(counter.getName().equals(IServiceCounters.LOCAL_HTTPD)) {
            
            return "<a href="+val+">"+cdata(val.toString())+"</a>";
            
        }
            
        return cdata(val.toString());
        
    }
    
    /**
     * A pattern matching the occurrence of the word "percent" in a counter
     * name. Leading and trailing wildcards are used and the match is
     * case-insensitive.
     */
    static private final Pattern percent_pattern = Pattern.compile(".*percent.*",Pattern.CASE_INSENSITIVE);
    
}
