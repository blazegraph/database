package com.bigdata.counters.httpd;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
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
import com.bigdata.util.HTMLUtility;

/**
 * (X)HTML rendering of a {@link CounterSet}. *
 * 
 * @todo URI options for filters, depth of view in the counter hierarchy, etc.
 * 
 * @todo Nice view touches would include graphs of counter history, but I would
 *       like to keep that all client-side using XSL, e.g., by generating SVG
 *       dynamically.
 * 
 * @todo parameterize the temporal aggregation (either summary or detail for
 *       minutes, hours, or days).
 * 
 * @todo show the timestamp for the values, at least in flyover.
 * 
 * @todo provide view that makes easy copy and paste into excel for graphing
 *       selected counter(s).
 * 
 * @todo parameterize for expanded or collapsed nodes.
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

    final private String encoding = "UTF-8";
    final private String ps = ICounterSet.pathSeparator;
    
    final private CounterSet root;
    final private String uri;
    final private Properties params;
    
    final private String path;
    final private Pattern filter;
    /*
     * Note: the page is valid for any of these doctypes.
     */
//    final private DoctypeEnum doctype = DoctypeEnum.html_4_01_strict
//    final private DoctypeEnum doctype = DoctypeEnum.html_4_01_transitional
    final private DoctypeEnum doctype = DoctypeEnum.xhtml_1_0_strict;
    
    private class NV {
        
        public final String name;
        public final String value;
        
        public NV(String name, String value) {
            
            if (name == null)
                throw new IllegalArgumentException();
            
            this.name = name;
            
            this.value = value;
            
        }
        
    }
    
    /**
     * 
     * @param root
     * @param uri
     * @param params
     */
    public XHTMLRenderer(CounterSet root, String uri, Properties params) {

        this.root = root;

        this.uri = uri;

        this.params = params;
        
        this.path = params.getProperty(PATH, ps);

        {
            String val = params.getProperty(FILTER);

            if (val == null) {

                this.filter = null;

            } else {

                /*
                 * @todo join multiple values for ?filter together in OR of
                 * quoted patterns.
                 */

                this.filter = Pattern.compile(Pattern.quote(val));
                
            }
            
        }

        // @todo parameter?
        this.decimalFormat = new DecimalFormat();
        
        decimalFormat.setGroupingUsed(true);

        decimalFormat.setMinimumFractionDigits(3);
        
        decimalFormat.setMaximumFractionDigits(6);
        
        decimalFormat.setDecimalSeparatorAlwaysShown(true);
        
    }

    /**
     * Name of the URL query parameter specifying the starting path for the page
     * view.
     */
    final String PATH = "path";

    /**
     * Name of the URL query parameter specifying a regular expression for the
     * filter to be applied to the counter paths.
     */
    final String FILTER = "filter";

    /**
     * Re-create the request URL.
     */
    public String getRequestURL() {
        
        return getRequestURL(null);
        
    }

    /**
     * Re-create the request URL.
     * 
     * @param override
     *            Overriden query parameters (optional).
     */
    public String getRequestURL(NV[] override) {
    
        final Properties p;
        
        if(override == null) {
            
            p = params;
            
        } else {
            
            p = new Properties(params);
            
            for(NV x : override) {
                                        
                p.setProperty(x.name,x.value);
                
            }
            
        }
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(uri);
        
        sb.append("?path="+p.getProperty(PATH,ps));
        
        Iterator itr = p.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            final Map.Entry entry = (Map.Entry)itr.next();
            
            String name = (String)entry.getKey();

            if(name.equals(PATH)) continue;
            
            String value = (String)entry.getValue();
            
            sb.append("&"+encodeURL(name)+"="+encodeURL(value));
            
        }
        
        return sb.toString();
        
    }
    
    public String encodeURL(String url) {
        
        final String charset = "UTF-8";
        try {
            return URLEncoder.encode(url, charset);
        } catch (UnsupportedEncodingException e) {
            log.error("Could not encode: charset="+charset+", url="+url);
            return url;
        }
        
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
        
        w.write("<title>bigdata(tm) telemetry : "+cdata(path)+"</title\n>");
        
    }
    
    protected void writeBody(Writer w) throws IOException  {
        
        w.write("<body\n>");

        final ICounterNode node = root.getPath(path);
        
        if(node == null) {

            /*
             * Used when the path does not evaluate to anything in the
             * hierarchy. The generate markup at least lets you choose a parent
             * from the path.
             */
            
            w.write("<p>");
            
            w.write("No such counter or counter set: ");
            
            writePath(w, path);
            
            w.write("</p>");

            return;
            
        }
        
        if(node instanceof ICounter) {

            writeCounter(w, (ICounter) node);

        } else {

            writeCounterSet(w, (CounterSet)node);
            
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
        
        doctype.writeValid(w);

        w.write("</body\n>");
        
    }

    /**
     * A clickable trail of the current path.
     */
    protected void writePath(Writer w, String path) throws IOException {

        final String[] a = path.split(ps);

        // click through to the root of the counter hierarchy
        w.write("<a href=\""
                + getRequestURL(new NV[] { new NV(PATH,
                        ps) }) + "\">");
        w.write(ps);
        w.write("</a>");

        // builds up the path query parameter for each split.
        final StringBuilder sb = new StringBuilder(ps);

        for (int n = 1; n < a.length; n++) {

            final String name = a[n];
            
            if (n > 1) {

                w.write("&nbsp;");

                w.write(ps);

                sb.append(ps);
                
            }
            
            sb.append(name);
            
            w.write("&nbsp;");

            w.write("<a href=\""
                    + getRequestURL(new NV[] { new NV(PATH, sb.toString()) })
                    + "\">");

            // current path component.
            w.write(cdata(name));
            
            w.write("</a>");

        }
        
    }
    
//    protected void writeCounterNode(Writer w, ICounterNode node) throws IOException {
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
    protected void writeCounterSet(Writer w, CounterSet counterSet) throws IOException {

        final String summary = "Showing counters for path="
                + counterSet.getPath();
        
        w.write("<table border=\"1\" summary=\""+attrib(summary)+"\"\n>");

        // @todo use css to left justify the path.
        w.write(" <caption>");
        writePath(w,counterSet.getPath());
        w.write("</caption\n>");
        
        w.write(" <tr\n>");
        w.write("  <th rowspan=\"2\" >Name</th\n>");
        w.write("  <th colspan=\"3\">Unit Average</th\n>");
        w.write("  <th rowspan=\"2\">Current</th\n>");
        w.write(" </tr\n>");
        
        w.write(" <tr\n>");
        w.write("  <th>Minutes</th\n>");
        w.write("  <th>Hours</th\n>");
        w.write("  <th>Days</th\n>");
        w.write(" </tr\n>");

        final Iterator<ICounter> itr = counterSet.getCounters(filter);
        
//        final Iterator<ICounter> itr = counterSet.directChildIterator(
//                true/* sorted */, ICounter.class/* type */);
        
        while(itr.hasNext()) {

            final ICounter counter = itr.next();

            final String path = counter.getPath();
            
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

            /*
             * write out values for the counter.
             */

            w.write("  <th align=\"left\">" );
//            w.write( cdata(path) );
            writePath(w, path);
            w.write( "</th\n>");

            if (counter.getInstrument() instanceof HistoryInstrument) {

                /*
                 * Report the average over the last hour, day, and month.
                 */

                HistoryInstrument inst = (HistoryInstrument) counter
                        .getInstrument();

                w.write("  <td>" + cdata(value(inst.minutes.getAverage()))
                        + " (" + cdata(value(inst.minutes.size())) + ")"
                        + "</td\n>");

                w.write("  <td>" + cdata(value(inst.hours.getAverage())) + " ("
                        + cdata(value(inst.hours.size())) + ")" + "</td\n>");

                w.write("  <td>" + cdata(value(inst.days.getAverage())) + " ("
                        + cdata(value(inst.days.size())) + ")" + "</td\n>");

                // the most recent value.
                w.write("  <td>" + cdata(value(counter.getValue()))
                                + "</td\n>");

            } else {

                /*
                 * Report only the most recent value.
                 */

                // w.write(" <th>N/A</th\n>");
                // w.write(" <th>N/A</th\n>");
                // w.write(" <th>N/A</th\n>");
                w.write("  <td colspan=\"4\">"
                        + cdata(value(counter.getValue())) + "</td\n>");

            }
            
            w.write(" </tr\n>");
            
        }

        w.write("</table\n>");

    }

    // @todo parameterize, perhaps by passing in as an interface.
    protected boolean isExpanded(String path) {
        
        return true;
        
    }
    
    /**
     * Writes details on a single counter.
     * 
     * @param counter
     *            The counter.
     * @throws IOException 
     */
    protected void writeCounter(Writer w, ICounter counter) throws IOException {
        
        w.write("<p>path: ");
        
        writePath(w, path);

        w.write("</p>");

        w.write("<p>value: ");
        
        // the most recent value.
        w.write(cdata(value(counter.getValue())));
        
        w.write("</p>");

        w.write("<p>time: ");

        w.write(cdata(new Date(counter.lastModified()).toString()));
        
        w.write("</p>");

        if(counter.getInstrument() instanceof HistoryInstrument) {
         
            writeHistoryCounter(w, counter);
            
        }
        
    }
    
    /**
     * Writes details on a single counter whose {@link IInstrument} provides a
     * history.
     * 
     * @param counter
     *            The counter.
     * 
     * @see HistoryInstrument
     * 
     * @todo write three tables, perhaps aligned side-by-side. Each table has a
     *       label (minutes, hours, days), the average, the last value, and the
     *       timestamped samples for that level of aggregation. The point is to
     *       be able to copy and past into a worksheet for plotting.
     */
    protected void writeHistoryCounter(Writer w, ICounter counter) throws IOException {

        HistoryInstrument inst = (HistoryInstrument) counter.getInstrument();
        
        w.write("<p>");
        w.write("</p>");
        writeSamples(w, counter, inst.minutes);

        w.write("<p>");
        w.write("</p>");
        writeSamples(w, counter, inst.hours);
        
        w.write("<p>");
        w.write("</p>");
        writeSamples(w, counter, inst.days);
        
    }

    protected void writeSamples(Writer w, ICounter counter, History h) throws IOException {
        
        final long period = h.getPeriod();
        
        final String units;
        if(period == 1000*60L) units="Minutes";
        else if(period == 1000*60*60L) units="Hours";
        else if(period == 1000*60*60*24L) units="Days";
        else units="period="+period+"ms";
        
        final String summary = "Showing samples: period="+units+", path=" + counter.getPath();

        w.write("<table border=\"1\" summary=\"" + attrib(summary) + "\"\n>");

        // @todo use css to left justify the path.
        w.write(" <caption>");
        writePath(w, counter.getPath());
        w.write("</caption\n>");

        w.write(" <tr\n>");
        w.write("  <th>"+cdata(units)+"</th\n>");
        w.write("  <th>Value</th\n>");
        w.write(" </tr\n>");

        /*
         * samples.
         */
        final Iterator<IHistoryEntry> itr = h.iterator();
        
        // zero initially.
        long firstTimestamp = 0;
        
        while (itr.hasNext()) {

            final IHistoryEntry sample = itr.next();
                        
            w.write(" <tr\n>");

            final long lastModified = sample.lastModified();

            if (firstTimestamp == 0) {
                // zero base for first row.
                firstTimestamp = lastModified;
            }
            
            // all other rows a delta in units of measure.
            final String timeStr = ""+(lastModified-firstTimestamp)/period;
            
//            final Date date = new Date(lastModified);
            
//            final String timeStr = date.toString();
            
            w.write("  <td>" + cdata(timeStr) + "</td\n>");

            w.write("  <td>" + cdata(value(sample.getValue())) + "</td\n>");

            w.write(" </tr\n>");

        }

        w.write("</table\n>");

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
        
        if(s == null) throw new IllegalArgumentException();
        
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
     * Formats a counter value as a String.
     * 
     * @param value
     *            The counter value (MAY be <code>null</code>).
     * @return
     */
    protected String value(Object val) {
        
        if(val == null) return "N/A";
        
        if(val instanceof Double || val instanceof Float) {
            
            return decimalFormat.format(((Number)val).doubleValue());
            
        }

        return val.toString();
        
    }
    
    final DecimalFormat decimalFormat;
    
}
