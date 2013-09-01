/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import cutthecrap.utils.striterators.EmptyIterator;

/**
 * Utility class for operations on the public interfaces.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASUtil {

    private static final Logger log = Logger.getLogger(GASUtil.class);

    /**
     * The average fan out of the frontier.
     * 
     * @param frontierSize
     *            The size of the frontier.
     * @param nedges
     *            The number of edges visited when mapping the operation across
     *            that frontier.
     * 
     * @return The average fan out.
     */
    public static double fanOut(final int frontierSize, final long nedges) {
    
        return ((int) (nedges * 10d / frontierSize)) / 10d;
    
    }

    /**
     * The traversed edges per second.
     * 
     * @param nedges
     *            The number of traversed edges.
     * @param elapsedNanos
     *            The elapsed time (nanoseconds).
     *            
     * @return The traversed edges per second.
     */
    public static long getTEPS(final long nedges, long elapsedNanos) {

        // avoid division by zero.
        if (elapsedNanos == 0)
            elapsedNanos = 1;

        // edges/nanosecond.
        final double tepns = ((double) nedges) / elapsedNanos;

        // scale units to edges/second.
        final double teps = tepns * TimeUnit.SECONDS.toNanos(1);

        // Round off to get rid of those nasty factions.
        final long r = Math.round(teps);

        return r;

    }

    /**
     * An empty vertex iterator.
     */
    @SuppressWarnings({ "unchecked" })
    public static final Iterator<Value> EMPTY_VERTICES_ITERATOR = EmptyIterator.DEFAULT;

    /**
     * An empty edge iterator.
     */
    @SuppressWarnings({ "unchecked" })
    public static final Iterator<Statement> EMPTY_EDGES_ITERATOR = EmptyIterator.DEFAULT;

    /**
     * Return the best guess at the {@link RDFFormat} for a resource.
     * <p>
     * Note: This handles the .gz and .zip extensions.
     * 
     * @param n
     *            The name of the resource.
     * @param rdfFormat
     *            The fallback format (optional).
     *            
     * @return The best guess format.
     */
    private RDFFormat guessRDFFormat(final String n, final RDFFormat rdfFormat) {

        RDFFormat fmt = RDFFormat.forFileName(n);

        if (fmt == null && n.endsWith(".zip")) {
            fmt = RDFFormat.forFileName(n.substring(0, n.length() - 4));
        }

        if (fmt == null && n.endsWith(".gz")) {
            fmt = RDFFormat.forFileName(n.substring(0, n.length() - 3));
        }

        if (fmt == null) // fallback
            fmt = rdfFormat;

        return fmt;

    }
    
    public void loadGraph(final SailConnection cxn, final RDFFormat fallback,
            final String... resources) throws Exception {

        if (resources != null) {

            for (String resource : resources) {

                loadGraph(cxn, fallback, resource);

            }

        }

    }

    /**
     * Load a resource from the classpath, the file system, or a URI. GZ
     * compressed files are decompressed. Directories are processed recursively.
     * The entries in a ZIP archive are processed. Resources that are not
     * recognized as some {@link RDFFormat} will be ignored unless the
     * <i>fallback</i> argument is given, in which case that format will be
     * <em>assumed</em> for such resources.
     * 
     * @param cxn
     *            The connection used to load the data.
     * @param resource
     *            A resource on the class path, a file or a directory, or a URI.
     * @param fallback
     *            The default {@link RDFFormat} to be assumed (optional).
     * 
     * @throws IOException
     * @throws URISyntaxException
     * @throws RDFHandlerException
     * @throws RDFParseException
     */
    public void loadGraph(final SailConnection cxn, final RDFFormat fallback,
            final String resource) throws IOException, URISyntaxException,
            RDFParseException, RDFHandlerException {

        if (log.isInfoEnabled())
            log.info("Loading: " + resource);

        String baseURI = null;

        InputStream is = null;
        try {

            // try the classpath
            is = getClass().getResourceAsStream(resource);

            if (is != null) {
                
                // set for resource on classpath.
                baseURI = getClass().getResource(resource).toURI().toString();
                
            } else {

                // try file system.
                final File file = new File(resource);

                if (file.exists()) {

                    if (file.isDirectory()) {

                        /*
                         * Recursion.
                         */

                        final File[] a = file.listFiles();

                        for (File f : a) {

                            loadGraph(cxn, fallback, f.toString());

                        }

                    }
                    
                    is = new FileInputStream(file);

                    // set for file as URI.
                    baseURI = file.toURI().toString();
                    
                } else {
                    
                    throw new IOException("Could not locate resource: "
                            + resource);

                }
                
            }

            if (resource.endsWith(".gz")) {

                is = new GZIPInputStream(is);

            } else if (resource.endsWith(".zip")) {

                final ZipInputStream zis = new ZipInputStream(is);

                try {
                    
                    ZipEntry e;

                    while ((e = zis.getNextEntry()) != null) {

                        if (e.isDirectory()) {
                            
                            // skip directories.
                            continue;
                        }

                        baseURI = resource + "/" + e.getName();
                        
                        loadFromStream(cxn, zis, resource, baseURI, fallback);

                        final RDFFormat format = guessRDFFormat(e.getName(),
                                fallback);

                        if (format == null) {

                            if (log.isInfoEnabled())
                                log.info("Skipping non-RDF entry: resource="
                                        + resource + ", entry=" + e.getName());

                            continue;

                        }

                    }
                    
                    return;
                    
                } finally {
                    
                    zis.close();
                    
                }
                
            }

            loadFromStream(cxn, is, resource, baseURI, fallback);

        } finally {

            if (is != null) {

                try {
                    is.close();
                } catch (Throwable t) {
                    log.error(t);
                }

            }

        }

    }

    private void loadFromStream(final SailConnection cxn, final InputStream is,
            final String resource, final String baseURI,
            final RDFFormat fallback) throws RDFParseException,
            RDFHandlerException, IOException {

        // guess at the RDF Format
        final RDFFormat rdfFormat = guessRDFFormat(resource, fallback);
        
        if (rdfFormat == null) {
         
            if (log.isInfoEnabled())
                log.info("Skipping non-RDF resource: " + resource);
            
            return;
            
        }
        
        /*
         * Obtain a buffered reader on the input stream.
         */
        final Reader reader = new BufferedReader(new InputStreamReader(
                is));

        try {

            final RDFParserFactory rdfParserFactory = RDFParserRegistry
                    .getInstance().get(rdfFormat);

            final RDFParser rdfParser = rdfParserFactory.getParser();

            rdfParser.setStopAtFirstError(false);

            final AddStatementHandler h = new AddStatementHandler(cxn);

            rdfParser.setRDFHandler(h);

            /*
             * Run the parser, which will cause statements to be
             * inserted.
             */

            rdfParser.parse(reader, baseURI);

            if (log.isInfoEnabled())
                log.info("Done: " + resource + ", nread=" + h.ntriples);

        } finally {

            try {
                reader.close();
            } catch (Throwable t) {
                log.error(t);
            }

        }

    }
    
    /**
     * Helper class adds statements to the sail as they are visited by a parser.
     */
    private static class AddStatementHandler extends RDFHandlerBase {

        private final SailConnection conn;
        /**
         * Only used if the statements themselves do not have a context.
         */
        private final Resource[] defaultContext;
        
        private long ntriples = 0;

        /**
         * 
         * @param conn
         */
        public AddStatementHandler(final SailConnection conn) {
            this.conn = conn;
            this.defaultContext = new Resource[0];
        }

        public void handleStatement(final Statement stmt)
                throws RDFHandlerException {

            final Resource[] c = (Resource[]) 
                    (stmt.getContext() == null 
                    ?  defaultContext
                    : new Resource[] { stmt.getContext() }); 
            
            try {

                conn.addStatement(//
                        stmt.getSubject(), //
                        stmt.getPredicate(), //
                        stmt.getObject(), //
                        c
                        );

                if (c == null || c.length == 0)
                    ntriples++;
                else
                    ntriples += c.length;
                
            } catch (SailException e) {

                throw new RDFHandlerException(e);

            }

        }

    }
    
}
