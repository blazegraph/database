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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.bigdata.Banner;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.IHostCounters;
import com.bigdata.counters.IRequiredHostCounters;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.httpd.DummyEventReportingService;
import com.bigdata.counters.query.CounterSetSelector;
import com.bigdata.counters.query.QueryUtil;
import com.bigdata.counters.query.URLQueryModel;
import com.bigdata.counters.render.IRenderer;
import com.bigdata.counters.render.RendererFactory;
import com.bigdata.service.Event;
import com.bigdata.util.httpd.NanoHTTPD;

/**
 * Utility to extract a batch of performance counters from a collection of
 * logged XML counter set files. This utility accepts file(s) giving the URLs
 * which would be used to demand the corresponding performance counters against
 * the live bigdata federation. The URLs listed in that file are parsed. The
 * host and port information are ignored, but the URL query parameters are
 * extracted and used to configured a set of {@link URLQueryModel}s.
 * <p>
 * A single pass is made through the specified XML counter set files. Each file
 * is read into memory by itself, and each query implied by a listed URL is run
 * against the in-memory {@link CounterSet} hierarchy. The results are collected
 * in independent {@link CounterSet} provisioned for the specified reporting
 * units, etc. Once the last XML counter set file has been processed, the
 * various reports requested by the listed URLs are generated.
 * <p>
 * For each generated report, the name of the file on which the report will be
 * written is taken from the name of the counter whose value was extracted for
 * that report. This filename may be overridden by including the URL query
 * parameter {@value URLQueryModel#FILE}, which specifies the file on which to
 * write the report for that query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IHostCounters
 * @see IRequiredHostCounters
 * 
 * @todo When rendering HTML output using flot, the flot resources need to be
 *       available in order to view the graphs. They should be written once into
 *       the output directory and the links in the (X)HTML output should resolve
 *       them there.
 */
public class CounterSetQuery {

    static protected final Logger log = Logger.getLogger(CounterSetQuery.class);

    /**
     * Create a {@link URLQueryModel} from a URL.
     * 
     * @param url
     *            The URL.
     * 
     * @return The {@link URLQueryModel}
     * 
     * @throws UnsupportedEncodingException
     */
    static private URLQueryModel newQueryModel(final URL url)
            throws UnsupportedEncodingException {

        // Extract the URL query parameters.
        final LinkedHashMap<String, Vector<String>> params = NanoHTTPD
                .decodeParms(url.getQuery());

        // add any relevant headers
        final Properties headers = new Properties();

        headers.setProperty("host", url.getHost() + ":" + url.getPort());

        return new URLQueryModel(null/* service */, url.toString(), params,
                headers);

    }

    /**
     * Reads a list of {@link URL}s from a file. Blank lines and comment lines
     * are ignored.
     * 
     * @param file
     *            A file containing URLs, blank lines, and comment lines (which
     *            start with '#').
     * 
     * @return A list of the URLs read from the file.
     * 
     * @throws IOException
     */
    private static Collection<URL> readURLsFromFile(final File file) throws IOException {

        if(log.isInfoEnabled())
            log.info("Reading queries: "+file);
        
        final List<URL> tmp = new LinkedList<URL>();

        final BufferedReader r = new BufferedReader(new FileReader(file));

        String s;
        while ((s = r.readLine()) != null) {

            s = s.trim();

            if (s.isEmpty())
                continue;

            if (s.startsWith("#"))
                continue;

            tmp.add(new URL(s));

        }

        return tmp;

    }

    /**
     * Reads URLs from a file or all files (recursively) in a directory.
     * 
     * @param file
     *            The file or directory.
     *            
     * @return The URLs read from the file(s).
     * 
     * @throws IOException 
     */
    static private Collection<URL> readURLs(final File file) throws IOException {

        /*
         * note: duplicates are not filtered out but this preserves the
         * evaluation order.
         */
        final Collection<URL> urls = new LinkedList<URL>();
        
        if (file.isDirectory()) {

            final File[] files = file.listFiles();
            
            for(File f : files) {
               
                urls.addAll(readURLsFromFile(f));
                               
            }
            
        } else {

            urls.addAll(readURLsFromFile(file));

        }

        return urls;
        
    }
    
    /**
     * Utility class for running extracting data from performance counter dumps
     * and running various kinds of reports on those data.
     * <p>
     * Usage:
     * <dl>
     * <dt>-outputDir</dt>
     * <dd>The output directory (default is the current working directory).</dd>
     * <dt>-mimeType</dt>
     * <dd>The default MIME type for the rendered reports. The default is
     * <code>text/plain</code>, but can be overridden on a query by query basis
     * using {@link URLQueryModel#MIMETYPE}.</dd>
     * <dt>-nsamples</dt>
     * <dd>Override for the default #of history samples to be retained. It is an
     * error if there are more distinct samples in the processed XML counter set
     * files (that is, if the #of time periods sampled exceeds this many
     * samples). If there are fewer, then some internal arrays will be
     * dimensioned larger than is otherwise necessary.</dd>
     * <dt>-events &lt;file&gt;</dt>
     * <dd>A file containing tab-delimited {@link Event}s. The {@link Event}s
     * are not required for simple performance counter views.</dd>
     * <dt>-queries &lt;file&gt;</dt>
     * <dd>A file, or directory of files, containing a list of URLs, each of
     * which is interpreted as a {@link URLQueryModel}.</dd>
     * <dt>&lt;file&gt;(s)</dt>
     * <dd>One or more XML counter set files or directories containing such
     * files. All such files will be processed before the reports are generated.
     * </dd>
     * </dl>
     * 
     * @param args
     *            Command line arguments.
     * 
     * @throws IOException
     * @throws ParserConfigurationException
     * @throws SAXException
     */
    public static void main(final String[] args) throws IOException,
            SAXException, ParserConfigurationException {

        Banner.banner();

        if (args.length == 0) {

            System.err.println("See javadoc for usage.");

            System.exit(1);
            
        }
        
        /*
         * The events read from the file(s).
         */
        final DummyEventReportingService service = new DummyEventReportingService();

        // The default output format (text, html, etc.)
        String defaultMimeType = NanoHTTPD.MIME_TEXT_PLAIN;

        /*
         * The #of slots to allocate (one slot per period of data to be read).
         * 
         * Note: The default is 7 days of data if period is minutes.
         */
        int nsamples = 60 * 24 * 7;

        // The output directory defaults to the current working directory.
        File outputDir = new File(".");
        
        // the set of queries to be processed.
        final List<URLQueryModel> queries = new LinkedList<URLQueryModel>();
        
        // the set of counter set XML files to be processed.
        final List<File> counterSetFiles = new LinkedList<File>();
        
        for (int i = 0; i < args.length; i++) {

            final String arg = args[i];

            if (arg.startsWith("-")) {

                if (arg.equals("-outputDir")) {

                    outputDir = new File(args[++i]);

                    if (log.isInfoEnabled()) {

                        log.info("outputDir: " + outputDir);

                    }
                    
                    if(!outputDir.exists()) {
                        
                        outputDir.mkdirs();
                        
                    }
                    
                } else if (arg.equals("-mimeType")) {

                    defaultMimeType = args[++i];

                } else if (arg.equals("-nsamples")) {

                    nsamples = Integer.valueOf(args[++i]);

                    if (nsamples <= 0)
                        throw new IllegalArgumentException(
                                "nslots must be positive.");

                } else if (arg.equals("-events")) {

                    // @todo read list of event files once all args are parsed.
                    QueryUtil.readEvents(service, new File(args[++i]));

                } else if (arg.equals("-queries")) {

                    final File file = new File(args[++i]);

                    final Collection<URL> urls = readURLs(file);
                    
                    for (URL url : urls) {
                        
                        queries.add(newQueryModel(url));

                    }

                } else {

                    System.err.println("Unknown option: " + arg);

                    System.exit(1);

                }

            } else {

                final File file = new File(arg);

                if (!file.exists())
                    throw new FileNotFoundException(file.toString());

                counterSetFiles.add(file);

            }

        }

        if (queries.isEmpty()) {

            throw new RuntimeException("No queries were specified.");

        }

        if (counterSetFiles.isEmpty()) {

            throw new RuntimeException("No counter set files were specified.");

        }

        /*
         * Compute a regular expression which will match anything which would
         * have been matched by the individual URLs. E.g., the OR of the
         * individual regular expressions entailed by each URL when interpreted
         * as a query.
         */
        final Pattern regex;
        {
            final List<Pattern> tmp = new LinkedList<Pattern>();

            for (URLQueryModel model : queries) {

                if (model.pattern != null) {

                    tmp.add(model.pattern);

                }

            }

            regex = QueryUtil.getPattern(tmp);

        }

        /*
         * Read counters accepted by the optional filter into the counter set to
         * be served.
         */

        // The performance counters read from the file(s).
        final CounterSet counterSet = new CounterSet();

        if(log.isInfoEnabled())
            log.info("Reading performance counters from "
                    + counterSetFiles.size() + " sources.");
        
        for (File file : counterSetFiles) {

            /*
             * @todo this does not support reading at different periods for each
             * query.
             */
            final PeriodEnum period = PeriodEnum.Minutes;

            if (file.isDirectory()) {

                // @todo does not process subdirectories recursively.
                if(log.isInfoEnabled())
                    log.info("Reading directory: " + file);

                final File[] files = file.listFiles(new FilenameFilter() {

                    public boolean accept(File dir, String name) {
                        return name.endsWith(".xml");
                    }
                });

                for (File f : files) {

                    if(log.isInfoEnabled())
                        log.info("Reading file: " + f);

                    QueryUtil.readCountersFromFile(f, counterSet, regex,
                            nsamples, period);

                }

            } else {

                if(log.isInfoEnabled())
                    log.info("Reading file: " + file);

                QueryUtil.readCountersFromFile(file, counterSet, regex, nsamples,
                        period);

            }

        }

        /*
         * Run each query in turn against the filtered pre-loaded counter set.
         */
        if (log.isInfoEnabled())
            log.info("Evaluating " + queries.size() + " queries.");
        
        for (URLQueryModel model : queries) {

            final IRenderer renderer = RendererFactory.get(model,
                    new CounterSetSelector(counterSet), defaultMimeType);

            /*
             * Render on a file. The file can be specified by a URL query
             * parameter.
             * 
             * FIXME Use the munged counter name (when one can be identified) as
             * the default filename.
             */
            File file;

            if (model.file == null) {

                file = File.createTempFile("query", ".out", outputDir);

            } else {

                if (!model.file.isAbsolute()) {

                    file = new File(outputDir, model.file.toString());

                } else {

                    file = model.file;

                }

            }

            if (file.getParentFile() != null && !file.getParentFile().exists()) {

                if (log.isInfoEnabled()) {
                 
                    log.info("Creating directory: " + file.getParentFile());
                    
                }
                
                // make sure the parent directory exists.
                file.getParentFile().mkdirs();
                
            }

            if (log.isInfoEnabled()) {

                log.info("Writing file: " + file + " for query: " + model.uri);

            }
            
            final Writer w = new BufferedWriter(
                    new FileWriter(file, false/* append */));

            try {

                renderer.render(w);

                w.flush();

            } finally {

                w.close();

            }

        }

    }

}
