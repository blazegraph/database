/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jun 25, 2010
 */

package com.bigdata.rdf.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.channels.AsynchronousCloseException;
import java.text.NumberFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.rio.Rio;
import org.openrdf.rio.RDFParser.DatatypeHandling;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.AsynchronousStatementBufferFactory;
import com.bigdata.rdf.rio.BasicRioLoader;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.NQuadsParser;
import com.bigdata.rdf.rio.RDFParserOptions;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;

/**
 * A utility class for splitting RDF files in various formats into smaller files
 * in some format. The format can be changed when the files are split. The
 * output files are written into a hierarchical directory structure with no more
 * than 1000 files per level. The output or input files may be compressed using
 * .zip or .gz. The utility efficiently processing multiple files in parallel.
 * 
 * <pre>
 * /nas/data/U32000/University0/University0/University0_0.owl.gz
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Splitter {

    protected static final Logger log = Logger.getLogger(Splitter.class);

    /**
     * The component name for this class (for use with the
     * {@link ConfigurationOptions}).
     */
    public static final String COMPONENT = Splitter.class.getName();

    /**
     * Enumeration of supported file compressions.
     */
    static public enum CompressEnum {
        None(""), GZip(".gz"),
        /**
         * Supports a single zip file entry.
         */
        Zip(".zip");
        private CompressEnum(final String ext) {
            this.ext = ext;
        }
        private final String ext;
        public String getExt() {
            return ext;
        }
    }

    /**
     * Configuration options for the {@link Splitter}.
     */
    public static interface ConfigurationOptions {

        /*
         * Source options.
         */
        
        /**
         * The source file or directory.
         */
        String SRC_DIR = "srcDir";

        /**
         * An optional {@link FilenameFilter} to be applied to the source
         * directory.
         */
        String SRC_FILTER = "srcFilter";

        /**
         * The name of the {@link RDFFormat} to use when the format can not be
         * determined from the examination of the visited files. The default is
         * {@value #DEFAULT_SRC_FORMAT}. The known values are: "RDF/XML",
         * "N-Triples", "Turtle", "N3", "TriX", "TriG", "nquads".
         */
        String SRC_FORMAT = "srcFormat";
        
        String DEFAULT_SRC_FORMAT = RDFFormat.RDFXML.getName();

        /**
         * Option controlling the behavior of the RDF parser (optionally; the
         * defaults have been tuned for splitting and generally work better than
         * messing with this value).
         */
        String PARSER_OPTIONS = "parserOptions";
        
        /*
         * Output options.
         */
        
        String OUT_DIR = "outDir";

        /**
         * The name of the {@link RDFFormat} to use when the format can not be
         * determined from the examination of the visited files. The default is
         * whatever the source format was for that file. The known values are:
         * "RDF/XML", "N-Triples", "Turtle", "N3", "TriX", "TriG", "nquads".
         */
        String OUT_FORMAT = "outFormat";

        /**
         * The number of RDF statements per output file (10,000 is a good number).
         */
        String OUT_CHUNK_SIZE = "outChunkSize";

        int DEFAULT_OUT_CHUNK_SIZE = 10000;

        /**
         * The compression style for the output files.
         */
        String OUT_COMPRESS = "outCompress";
        
        String DEFAULT_OUT_COMPRESS = CompressEnum.None.toString();
        
        /*
         * Thread pool.
         */
        
        String THREAD_POOL_SIZE = "threadPoolSize";

        int DEFAULT_THREAD_POOL_SIZE = 10;

        /*
         * Output directory structure.
         */

        String SUBDIRS = "subdirs";

        boolean DEFAULT_SUBDIRS = true;

        String MAX_PER_SUB_DIR = "maxPerSubDir";

        int DEFAULT_MAX_PER_SUBDIR = 1000;

    }
    
    /**
     * Settings per the {@link ConfigurationOptions}.
     */
    private static class Settings {

        /*
         * Source.
         */
        final File srcDir;
        final FilenameFilter srcFilter;
        final RDFFormat srcFormat;

        /*
         * Output.
         */
        final File outDir;
        final RDFFormat outFormat;
        final CompressEnum outCompress;
        final int outChunkSize;
        final RDFParserOptions parserOptions;
        final boolean subdirs;
        final int maxPerSubdir;

        /*
         * Thread pool.
         */
        final int threadPoolSize;
        
        public Settings(Configuration c) throws ConfigurationException {

            /*
             * Source
             */

            srcDir = (File) c.getEntry(COMPONENT, ConfigurationOptions.SRC_DIR,
                    File.class);

            srcFilter = (FilenameFilter) c
                    .getEntry(COMPONENT, ConfigurationOptions.SRC_FILTER,
                            FilenameFilter.class, null/* default */);

            srcFormat = RDFFormat.valueOf((String) c.getEntry(COMPONENT,
                    ConfigurationOptions.SRC_FORMAT, String.class,
                    ConfigurationOptions.DEFAULT_SRC_FORMAT));

            // verify the source format is supported (if given).
            if (srcFormat != null
                    && RDFParserRegistry.getInstance().get(srcFormat) == null) {
                throw new ConfigurationException(
                        ConfigurationOptions.SRC_FORMAT + "=" + srcFormat);
            }

            /*
             * Output
             */

            outDir = (File) c.getEntry(COMPONENT, ConfigurationOptions.OUT_DIR,
                    File.class);

            // Note: default decided on per-file basis at runtime.
            outFormat = RDFFormat.valueOf((String) c.getEntry(COMPONENT,
                    ConfigurationOptions.OUT_FORMAT, String.class));

            // verify the output format is supported (if given).
            if (outFormat != null
                    && RDFWriterRegistry.getInstance().get(outFormat) == null) {
                throw new ConfigurationException(
                        ConfigurationOptions.OUT_FORMAT + "=" + outFormat);
            }

            outCompress = (CompressEnum) c.getEntry(COMPONENT,
                    ConfigurationOptions.OUT_COMPRESS, CompressEnum.class,
                    CompressEnum.None/* default */);

			outChunkSize = (Integer) c.getEntry(COMPONENT,
					ConfigurationOptions.OUT_CHUNK_SIZE, Integer.TYPE,
					ConfigurationOptions.DEFAULT_OUT_CHUNK_SIZE);

            {
                /*
                 * The defaults here are intended to facilitate splitting.
                 */
                final RDFParserOptions defaultParserOptions = new RDFParserOptions();
                
                // Blank node IDs should be preserved.
                defaultParserOptions.setPreserveBNodeIDs(true);

                // Log errors, but do not stop.
                defaultParserOptions.setStopAtFirstError(false);

                // Turn off verification.
                defaultParserOptions.setVerifyData(false);

                // Do not validate datatypes.
                defaultParserOptions.setDatatypeHandling(DatatypeHandling.IGNORE);

                parserOptions = (RDFParserOptions) c.getEntry(COMPONENT,
                        ConfigurationOptions.PARSER_OPTIONS,
                        RDFParserOptions.class, defaultParserOptions);
            }

            subdirs = (Boolean) c.getEntry(COMPONENT,
                    ConfigurationOptions.SUBDIRS, Boolean.TYPE,
                    ConfigurationOptions.DEFAULT_SUBDIRS);

            maxPerSubdir = (Integer) c.getEntry(COMPONENT,
                    ConfigurationOptions.MAX_PER_SUB_DIR, Integer.TYPE,
                    ConfigurationOptions.DEFAULT_MAX_PER_SUBDIR);

            /*
             * thread pool.
             */
            
            threadPoolSize = (Integer) c.getEntry(COMPONENT,
                    ConfigurationOptions.THREAD_POOL_SIZE, Integer.TYPE,
                    ConfigurationOptions.DEFAULT_THREAD_POOL_SIZE);

        }
        
    }

    /** The configured {@link Settings}. */
    private final Settings s;

    private volatile ExecutorService service;
    
    /**
     * A unique integer for each output file to be written.
     */
    private final AtomicLong nextId = new AtomicLong();

    protected Splitter(final Settings settings) {
        
        this.s = settings;
        
    }

    /**
     * Start the service.
     * 
     * @throws InterruptedException
     * @throws Exception
     */
    synchronized public void start() throws InterruptedException, Exception {

        if (service != null) {
            // Already running.
            throw new IllegalStateException();
        }
        
        service = Executors.newFixedThreadPool(s.threadPoolSize);

    }

    /**
     * Immediate termination (running tasks are cancelled).
     */
    synchronized public void terminate() {

        if (service == null) {
            // Not running.
            return;
        }

        service.shutdownNow();
        
        service = null;

    }

    /**
     * Submit all files in a directory for processing via
     * {@link #submitOne(String)}, returning when all files have been processed.
     * 
     * @param fileOrDir
     *            The file or directory.
     * @param filter
     *            An optional filter. Only the files selected by the filter will
     *            be processed.
     * @param rdfFormat
     *            The default {@link RDFFormat} for the files.
     * 
     * @throws Exception
     * @throws InterruptedException
     */
    public void submitAll(final File fileOrDir, final FilenameFilter filter,
            final RDFFormat rdfFormat) throws InterruptedException, Exception {

        final List<Callable<Void>> tasks = acceptAll(s.srcDir, s.srcFilter,
                s.srcFormat);

        if (log.isInfoEnabled())
            log.info("Running: " + tasks.size() + " tasks");
        
        service.invokeAll(tasks);

    }
    
    /**
     * Submit all files in a directory for processing via
     * {@link #submitOne(String)}.
     * 
     * @param fileOrDir
     *            The file or directory.
     * @param filter
     *            An optional filter. Only the files selected by the filter will
     *            be processed.
     * @param rdfFormat
     *            The default {@link RDFFormat} for the files.
     * 
     * @return A list of tasks which may be submitted for execution.
     */
    private List<Callable<Void>> acceptAll(final File fileOrDir,
            final FilenameFilter filter, final RDFFormat rdfFormat)
            throws Exception {

        return new RunnableFileSystemLoader(fileOrDir, filter, rdfFormat)
                .call();

    }
    
    /**
     * Return a list of tasks which may be used submitted to an {@link Executor}
     * to transform the source files.
     */
    private class RunnableFileSystemLoader implements
            Callable<List<Callable<Void>>> {

        final File fileOrDir;

        final FilenameFilter filter;
        
        final RDFFormat rdfFormat;
        
        final List<Callable<Void>> futures = new LinkedList<Callable<Void>>();
        
        /**
         * 
         * @param fileOrDir
         *            The file or directory to be loaded.
         * @param filter
         *            An optional filter on files that will be accepted when
         *            processing a directory.
         */
        public RunnableFileSystemLoader(final File fileOrDir,
                final FilenameFilter filter, final RDFFormat rdfFormat) {

            if (fileOrDir == null)
                throw new IllegalArgumentException();

            this.fileOrDir = fileOrDir;

            this.filter = filter; // MAY be null.

            this.rdfFormat = rdfFormat;

        }

        /**
         * Creates a task using the {@link #taskFactory}, submits it to the
         * {@link #loader} and and waits for the task to complete. Errors are
         * logged, but not thrown.
         * 
         * @throws RuntimeException
         *             if interrupted.
         */
        public List<Callable<Void>> call() throws Exception {

            process2(fileOrDir);

            return futures;

        }

        /**
         * Scans file(s) recursively starting with the named file, and, for each
         * file that passes the filter, submits the task.
         * 
         * @param file
         *            A plain file or directory containing files to be
         *            processed.
         * 
         * @throws InterruptedException
         *             if the thread is interrupted while queuing tasks.
         */
        private void process2(final File file) throws InterruptedException {

            if (file.isHidden()) {

                // ignore hidden files.
                return;

            }

            if (file.isDirectory()) {

                if (log.isInfoEnabled())
                    log.info("Scanning directory: " + file);

                // filter is optional.
                final File[] files = filter == null ? file.listFiles() : file
                        .listFiles(filter);

                for (final File f : files) {

                    process2(f);

                }

            } else {

                /*
                 * Processing a standard file.
                 */

                try {

                    if (log.isInfoEnabled())
                        log.info("Accepting file: " + file);

                    futures.add(new ParserTask(file, rdfFormat));

                    return;

                } catch (Exception ex) {

                    log.error(file, ex);

                }

            }

        }

    }

    public Future<Void> submitOne(final File file, final RDFFormat rdfFormat)
            throws Exception {

        if (log.isInfoEnabled())
            log.info("file=" + file + ", rdfFormat=" + rdfFormat);

        final FutureTask<Void> ft = new FutureTask<Void>(new ParserTask(file,
                rdfFormat));

        service.execute(ft);

        return ft;

    }

    /**
     * Note: for very large data sets the ext3 file system only allows ~32000
     * subdirectories in a directory. Therefore we have to break up the file
     * system hierarchy.
     * 
     * @return The directory into which the next file should be written.
     */
    protected File getOutDir() {

        if (!s.subdirs) {
        
            // All files in the same directory.
            return s.outDir;
            
        }

        /*
         * Ensure subdirectory exists.
         */
        final int n = s.maxPerSubdir;

        final long k = nextId.incrementAndGet();

        final File p = new File(s.outDir, Long.toString(k / n));

        // ensure directory structure exists.
        if (p.mkdirs()) {
            if (log.isInfoEnabled())
                log.info("new subdirectory: " + p);
        }

        return p;
        
    }
    
    /**
     * Tasks parses an RDF document, writing a new file every N statements.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class ParserTask implements Callable<Void> {

        /**
         * The resource to be loaded.
         */
        private final File file;
        
        /**
         * The base URL for that resource.
         */
        private final String baseURL;
        
        /**
         * The RDF interchange syntax that the file uses.
         */
        private final RDFFormat defaultRDFFormat;

        /**
         * 
         * @param file
         *            The file to be parsed.
         * @param defaultRDFFormat
         *            The default {@link RDFFormat} that will be assumed for the
         *            file.
         */
        public ParserTask(final File file, final RDFFormat defaultRDFFormat) {

            if (file == null)
                throw new IllegalArgumentException();

//            if (baseURL == null)
//                throw new IllegalArgumentException();

            this.file = file;

            // Convert the resource identifier to a URL.
            this.baseURL = file.toURI().toString();

            this.defaultRDFFormat = defaultRDFFormat;
            
        }

        public Void call() throws Exception {
            if (log.isInfoEnabled())
                log.info("file=" + file);
            final IStatementBuffer<?> buffer = new MyStatementBuffer(file);
            try {
                // open input stream on the file.
                InputStream is = new FileInputStream(file);
                try {
                    /*
                     * Figure out the source format based on examination, stripping
                     * off the compression file extension (if recognized) and the
                     * source RDF format file extension (if recognized). This gives
                     * us a basename for the source file.
                     */
                    String basename = file.getName();
                    RDFFormat srcfmt = null;
                    {
                        if (basename.endsWith(".zip")) {
                            // strip compression suffix.
                            basename = basename.substring(0,
                                    basename.length() - 4);
                            // wrap to decompress.
                            is = new ZipInputStream(is);
                        } else if (basename.endsWith(".gz")) {
                            // strip compression suffix.
                            basename = basename.substring(0,
                                    basename.length() - 3);
                            // wrap to decompress.
                            is = new GZIPInputStream(is);
                        }

                        if ((srcfmt = RDFFormat.forFileName(basename)) != null) {
                            for (String ext : srcfmt.getFileExtensions()) {
                                if (basename.endsWith(ext)) {
                                    // strip off known RDF format file extensions.
                                    basename = basename.substring(0, basename
                                            .length()
                                            - ext.length());
                                    if(basename.endsWith(".")) {
                                        /*
                                         * The "." is not part of the RDFFormat
                                         * file extension declarations.
                                         */
                                        basename = basename.substring(0,
                                                basename.length() - 1);
                                    }
                                    break;
                                }
                            }
                        }
                        if (srcfmt == null) {
                            srcfmt = defaultRDFFormat;
                            if (srcfmt == null) {
                                throw new UnsupportedOperationException(
                                        "Could not identify format: " + file);
                            }
                        }
                    }
                    // Obtain a buffered reader on the input stream.
                    final Reader reader = new BufferedReader(
                            new InputStreamReader(is));
                    try {
                        // run the parser.
                        new MyLoader(buffer).loadRdf(reader, baseURL,
                                defaultRDFFormat, s.parserOptions);
                    } finally {
                        reader.close();
                    }
                } finally {
                    is.close();
                }
            } catch (Exception ex) {
                log.error("file=" + file + " : " + ex, ex);
                throw ex;
            }
            // done : flush anything left in the buffer.
            buffer.flush();
            return null;
        }

    } // ParserTask

    /**
     * Statement handler for the RIO RDF Parser that writes on a
     * {@link StatementBuffer}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private 
    static class MyLoader extends BasicRioLoader implements RDFHandler
    {

        /**
         * Used to buffer RDF {@link Value}s and {@link Statement}s emitted by
         * the RDF parser (the value is supplied by the ctor). 
         */
        final protected IStatementBuffer<?> buffer;

        /**
         * Sets up parser to load RDF.
         * 
         * @param buffer
         *            The statement buffer.
         */
        public MyLoader(final IStatementBuffer<?> buffer) {

            super(new ValueFactoryImpl());

            this.buffer = buffer;

        }
            
        /**
         * bulk insert the buffered data into the store.
         */
        protected void success() {

            if (buffer != null) {
                
                buffer.flush();
                
            }

        }

        protected void error(final Exception ex) {
            
            if(buffer != null) {
                
                // discard all buffered data.
                buffer.reset();
                
            }

            super.error( ex );
            
        }
        
        public RDFHandler newRDFHandler() {

            return this;

        }

        public void handleStatement(final Statement stmt) {

            if (log.isDebugEnabled()) {

                log.debug(stmt);

            }

            // buffer the write (handles overflow).
            buffer.add(stmt.getSubject(), stmt.getPredicate(),
                    stmt.getObject(), stmt.getContext());

            stmtsAdded++;

            if (stmtsAdded % 100000 == 0) {

                notifyListeners();

            }

        }

        public void endRDF() throws RDFHandlerException {

        }

        public void handleComment(String arg0) throws RDFHandlerException {

        }

        public void handleNamespace(String arg0, String arg1)
                throws RDFHandlerException {

        }

        public void startRDF() throws RDFHandlerException {

        }

    }

    
    /**
     * Buffers the parser statements in memory, flushing chunks onto new output
     * files each time the internal buffer fills up and when flush() is called.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <S>
     */
    private class MyStatementBuffer implements
            IStatementBuffer<Statement> {

        private final File srcFile;
        private final Statement[] stmts = new Statement[s.outChunkSize];
        private int numStmts = 0;
        private long nwritten = 0;

        private int nchunks = 0;

        public MyStatementBuffer(final File srcFile) {
        
            this.srcFile = srcFile;
            
        }

        public AbstractTripleStore getDatabase() {
            return null;
        }

        public AbstractTripleStore getStatementStore() {
            return null;
        }

        public void setBNodeMap(Map<String, BigdataBNode> bnodes) {
            throw new UnsupportedOperationException();
        }

        public boolean isEmpty() {
            return numStmts == 0;
        }

        public int size() {
            return numStmts;
        }
        
        public void add(Resource s, URI p, Value o) {
            add(new StatementImpl(s, p, o));
        }

        public void add(Resource s, URI p, Value o, Resource c) {
            add(new ContextStatementImpl(s, p, o,c));
        }

        public void add(Resource s, URI p, Value o, Resource c,
                StatementEnum type) {
            add(new ContextStatementImpl(s, p, o,c));
        }

        public void add(final Statement stmt) {
            if (numStmts == stmts.length) {
                flush();
            }
            stmts[numStmts++] = stmt;
        }

        public void reset() {
            for(int i=0; i<stmts.length; i++) {
                // clear references
                stmts[i] = null;
            }
            nwritten = numStmts = 0;
        }

        /**
         * Writes a chunk of statements on a uniquely named output file.
         * <p>
         * {@inheritDoc}
         */
        public long flush() {
            {

                // The output directory for the next file.
                final File outDir = getOutDir();

                /*
                 * Figure out the source format based on examination, stripping
                 * off the compression file extension (if recognized) and the
                 * source RDF format file extension (if recognized). This gives
                 * us a basename for the source file.
                 */
                String basename = srcFile.getName();
                RDFFormat srcfmt = null;
                {
                    if (basename.endsWith(".zip")) {
                        // strip compression suffix.
                        basename = basename.substring(0, basename.length() - 4);
                    } else if (basename.endsWith(".gz")) {
                        // strip compression suffix.
                        basename = basename.substring(0, basename.length() - 3);
                    }

                    if ((srcfmt = RDFFormat.forFileName(basename)) != null) {
                        for (String ext : srcfmt.getFileExtensions()) {
                            if (basename.endsWith(ext)) {
                                // strip off known RDF format file extensions.
                                basename = basename.substring(0, basename
                                        .length()
                                        - ext.length());
                                if(basename.endsWith(".")) {
                                    /*
                                     * The "." is not part of the RDFFormat
                                     * file extension declarations.
                                     */
                                    basename = basename.substring(0,
                                            basename.length() - 1);
                                }
                                break;
                            }
                        }
                    }
                    if (srcfmt == null) {
                        srcfmt = s.srcFormat;
                        if (srcfmt == null) {
                            throw new UnsupportedOperationException(
                                    "Could not identify format: " + srcFile);
                        }
                    }
                }

                /*
                 * The actual output format will be the one specified in the
                 * configuration, unless the output format was not specified, in
                 * which case it will be whatever we figured out above.
                 */
                RDFFormat outfmt = s.outFormat;
                if (outfmt == null) {
                    outfmt = srcfmt;
                }

                // Format the chunk number as a string.
                final String chunkStr;
                {
                    final NumberFormat d = NumberFormat.getIntegerInstance();
                    // use leading zeros for natural sort order.
                    d.setMinimumIntegerDigits(6);
                    // no commas.
                    d.setGroupingUsed(false);
                    chunkStr = d.format(nchunks);
                }

                // Form a unique output file name.
                final File outFile = new File(outDir, basename + "_" + chunkStr
                        + "." + outfmt.getDefaultFileExtension()
                        + s.outCompress.getExt());

                try {
                    // write an output file.
                    writeFile(outFile, numStmts, stmts);
                } catch (IOException ex) {
                    // log.error(outFile + " : " + ex);
                    throw new RuntimeException(ex);
                }
            }
            // clear references
            for (int i = 0; i < numStmts; i++) {
                stmts[i] = null;
            }
            // update counters.
            nwritten += numStmts;
            nchunks++;
            numStmts = 0;
            return nwritten;
        }

        /**
         * Write a bunch of statements onto a uniquely named file.
         * 
         * @param outFile
         *            The name of the output file.
         * @param n
         *            The #of statements to write.
         * @param a
         *            The statements.
         */
        protected void writeFile(final File outFile, final int n,
                final Statement[] a) throws IOException {

            if (log.isInfoEnabled())
                log.info("Writing " + n + " statements on " + outFile);

            OutputStream os = new FileOutputStream(outFile);

            try {

                // buffer.
                os = new BufferedOutputStream(os);

                // setup compression.
                switch (s.outCompress) {
                case None:
                    break;
                case GZip:
                    os = new GZIPOutputStream(os);
                    break;
                case Zip:
                    os = new ZipOutputStream(os);
                    break;
                default:
                    throw new AssertionError("Unknown value: outCompress="
                            + s.outCompress);
                }

                // write statements on the file.
                final RDFWriter w = Rio.createWriter(s.outFormat, os);
                try {
                    w.startRDF();
                    for (int i = 0; i < n; i++) {
                        w.handleStatement(a[i]);
                    }
                    w.endRDF();
                } catch (RDFHandlerException ex) {
                    throw new IOException(ex);
                }

            } finally {

                // close the file.
                os.close();

            }

        }

    }

    /**
     * Run the split utility.
     * 
     * @param args
     *            The name of the configuration file.
     * 
     * @throws Exception
     * 
     * @see {@link ConfigurationOptions}
     * 
     * @todo Implement and register an RDFWriter for NQuads. Until then, use
     *       Trig as the output format.
     * 
     * @todo Add options to preserve bnodes when splitting either by them to
     *       URIs using a stable conversion
     * 
     * @todo The file extensions specified by RDFFormat DO NOT include the "."
     *       character. This is messing with the logic in this class (fixed), in
     *       the {@link DataLoader}, and in the
     *       {@link AsynchronousStatementBufferFactory} which attempts to
     *       recognize and strip off the trailing file extension.
     */
    public static void main(final String[] args) throws Exception {

//        for (RDFFormat t : RDFFormat.values()) {
//            System.err.println(t.getName() + " : " + t);
//        }
//        System.exit(0);
        
        NQuadsParser.forceLoad();
//        System.err.println(NQuadsParser.nquads.toString());
        
        final Configuration c = ConfigurationProvider.getInstance(args);

        final Settings settings = new Settings(c);
        
        settings.outDir.mkdirs();
        
        final Splitter splitter = new Splitter(settings);

        /*
         * Install a shutdown hook (normal kill will trigger this hook).
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {

                splitter.terminate();

            }

        });

        // start
        splitter.start();
        try {

            // submit and await split of all files matching the optional filter.
            splitter.submitAll(settings.srcDir, settings.srcFilter,
                    settings.srcFormat);

            System.exit(0);

        } finally {
            // done.
            splitter.terminate();

        }

    }

}
