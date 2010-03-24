/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Nov 1, 2007
 */

package com.bigdata.rdf.store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.spo.SPO;

/**
 * A utility class to load RDF data into an {@link AbstractTripleStore} without
 * using Sesame API. This class does not parallelize the RDF parsing and writing
 * on the database. This class is not efficient for scale-out.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo It should be easier to configure the underlying parsers in order to
 *       enable or disable various features which they support, e.g., preserving
 *       BNode IDs, validation, etc.
 */
public class DataLoader {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(DataLoader.class);

//    protected static final boolean INFO = log.isInfoEnabled();

    private final boolean verifyData;

    /**
     * The {@link StatementBuffer} capacity.
     */
    private final int bufferCapacity;
    
    /**
     * The target database.
     */
    private final AbstractTripleStore database;
    
    /**
     * The target database.
     */
    public AbstractTripleStore getDatabase() {
        
        return database;
        
    }
    
    /**
     * The object used to compute entailments for the database.
     */
    private final InferenceEngine inferenceEngine;
    
    /**
     * The object used to maintain the closure for the database iff incremental
     * truth maintenance is enabled.
     */
    private final TruthMaintenance tm;
    
    /**
     * The object used to compute entailments for the database.
     */
    public InferenceEngine getInferenceEngine() {
        
        return inferenceEngine;
        
    }
    
    /**
     * Used to buffer writes.
     * 
     * @see #getAssertionBuffer()
     */
    private StatementBuffer buffer;
    
    /**
     * Return the assertion buffer.
     * <p>
     * The assertion buffer is used to buffer statements that are being asserted
     * so as to maximize the opportunity for batch writes. Truth maintenance (if
     * enabled) will be performed no later than the commit of the transaction.
     * <p>
     * Note: The same {@link #buffer} is reused by each loader so that we can on
     * the one hand minimize heap churn and on the other hand disable auto-flush
     * when loading a series of small documents. However, we obtain a new buffer
     * each time we perform incremental truth maintenance.
     * <p>
     * Note: When non-<code>null</code> and non-empty, the buffer MUST be
     * flushed (a) if a transaction completes (otherwise writes will not be
     * stored on the database); or (b) if there is a read against the database
     * during a transaction (otherwise reads will not see the unflushed
     * statements).
     * <p>
     * Note: if {@link #truthMaintenance} is enabled then this buffer is backed
     * by a temporary store which accumulates the {@link SPO}s to be asserted.
     * Otherwise it will write directly on the database each time it is flushed,
     * including when it overflows.
     * 
     * @todo this should be refactored as an {@link IStatementBufferFactory}
     *       where the appropriate factory is required for TM vs non-TM
     *       scenarios (or where the factory is parameterize for tm vs non-TM).
     */
    synchronized protected StatementBuffer getAssertionBuffer() {

        if (buffer == null) {

            if (tm != null) {

                buffer = new StatementBuffer(tm.newTempTripleStore(),
                        database, bufferCapacity);

            } else {

                buffer = new StatementBuffer(database, bufferCapacity);

            }

        }
        
        return buffer;
        
    }

    private final CommitEnum commitEnum;
    
    private final ClosureEnum closureEnum;
    
    private final boolean flush;
    
//    public boolean setFlush(boolean newValue) {
//        
//        boolean ret = this.flush;
//        
//        this.flush = newValue;
//        
//        return ret;
//        
//    }
    
    /**
     * When <code>true</code> (the default) the {@link StatementBuffer} is
     * flushed by each {@link #loadData(String, String, RDFFormat)} or
     * {@link #loadData(String[], String[], RDFFormat[])} operation and when
     * {@link #doClosure()} is requested. When <code>false</code> the caller
     * is responsible for flushing the {@link #buffer}.
     * <p>
     * This behavior MAY be disabled if you want to chain load a bunch of small
     * documents without flushing to the backing store after each document and
     * {@link #loadData(String[], String[], RDFFormat[])} is not well-suited to
     * your purposes. This can be much more efficient, approximating the
     * throughput for large document loads. However, the caller MUST invoke
     * {@link #endSource()} once all documents are loaded successfully. If an error
     * occurs during the processing of one or more documents then the entire
     * data load should be discarded.
     * 
     * @return The current value.
     * 
     * @see Options#FLUSH
     */
    public boolean getFlush() {
        
        return flush;
        
    }
    
    /**
     * Flush the {@link StatementBuffer} to the backing store.
     * <p>
     * Note: If you disable auto-flush AND you are not using truth maintenance
     * then you MUST explicitly invoke this method once you are done loading
     * data sets in order to flush the last chunk of data to the store. In all
     * other conditions you do NOT need to call this method. However it is
     * always safe to invoke this method - if the buffer is empty the method
     * will be a NOP.
     */
    public void endSource() {

        if (buffer != null) {

            if(log.isInfoEnabled())
                log.info("Flushing the buffer.");
            
            buffer.flush();
            
        }
        
    }
    
    /**
     * How the {@link DataLoader} will maintain closure on the database.
     */
    public ClosureEnum getClosureEnum() {
        
        return closureEnum;
        
    }

    /**
     * Whether and when the {@link DataLoader} will invoke
     * {@link ITripleStore#commit()}
     */
    public CommitEnum getCommitEnum() {
        
        return commitEnum;
        
    }

    /**
     * A type-safe enumeration of options effecting whether and when the database
     * will be committed.
     * 
     * @see ITripleStore#commit()
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum CommitEnum {
        
        /**
         * Commit as each document is loaded into the database.
         */
        Incremental,
        
        /**
         * Commit after each set of documents has been loaded into the database.
         */
        Batch,

        /**
         * The {@link DataLoader} will NOT commit the database - this is left to
         * the caller.
         */
        None;
        
    }
    
    /**
     * A type-safe enumeration of options effecting whether and when entailments
     * are computed as documents are loaded into the database using the
     * {@link DataLoader}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum ClosureEnum {
        
        /**
         * Document-at-a-time closure.
         * <p>
         * Each documents is loaded separately into a temporary store, the
         * temporary store is closed against the database, and the results of
         * the closure are transferred to the database.
         */
        Incremental,
        
        /**
         * Set-of-documents-at-a-time closure.
         * <p>
         * A set of documents are loaded into a temporary store, the temporary
         * store is closed against the database, and the results of the closure
         * are transferred to the database. maintaining closure.
         */
        Batch,

        /**
         * Closure is not maintained as documents are loaded.
         * <p>
         * You can always use the {@link InferenceEngine} to (re-)close a
         * database. If explicit statements MAY have been deleted, then you
         * SHOULD first delete all inferences before re-computing the closure.
         */
        None;
        
    }
    
    /**
     * Options for the {@link DataLoader}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {
        
        /**
         * Optional boolean property may be used to turn on data verification in
         * the RIO parser (default is <code>false</code>).
         */
        public static final String VERIFY_DATA = DataLoader.class.getName()+".verifyData";
        
        public static final String DEFAULT_VERIFY_DATA = "false";
        
        /**
         * Optional property specifying whether and when the {@link DataLoader}
         * will {@link ITripleStore#commit()} the database (default
         * {@link CommitEnum#Batch}).
         * <p>
         * Note: commit semantics vary depending on the specific backing store.
         * See {@link ITripleStore#commit()}.
         */
        public static final String COMMIT = DataLoader.class.getName()+".commit";
        
        public static final String DEFAULT_COMMIT = CommitEnum.Batch.toString();

        /**
         * Optional property specifying the capacity of the
         * {@link StatementBuffer} (default is 100k statements).
         */
        public static final String BUFFER_CAPACITY = DataLoader.class.getName()+".bufferCapacity";
        
        public static final String DEFAULT_BUFFER_CAPACITY = "100000";

        /**
         * Optional property controls whether and when the RDFS(+) closure is
         * maintained on the database as documents are loaded (default
         * {@link ClosureEnum#Batch).
         * <p>
         * Note: The {@link InferenceEngine} supports a variety of options. When
         * closure is enabled, the caller's {@link Properties} will be used to
         * configure an {@link InferenceEngine} object to compute the
         * entailments. It is VITAL that the {@link InferenceEngine} is always
         * configured in the same manner for a given database with regard to
         * options that control which entailments are computed using forward
         * chaining and which entailments are computed using backward chaining.
         * <p>
         * Note: When closure is being maintained the caller's
         * {@link Properties} will also be used to provision the
         * {@link TempTripleStore}.
         * 
         * @see InferenceEngine
         * @see InferenceEngine.Options
         */
        public static final String CLOSURE = DataLoader.class.getName()+".closure";
        
        public static final String DEFAULT_CLOSURE = ClosureEnum.Batch.toString();
        
        /**
         * 
         * When <code>true</code> (the default) the {@link StatementBuffer} is
         * flushed by each
         * {@link DataLoader#loadData(String, String, RDFFormat)} or
         * {@link DataLoader#loadData(String[], String[], RDFFormat[])}
         * operation and when {@link DataLoader#doClosure()} is requested. When
         * <code>false</code> the caller is responsible for flushing the
         * {@link #buffer}.
         * <p>
         * This behavior MAY be disabled if you want to chain load a bunch of
         * small documents without flushing to the backing store after each
         * document and
         * {@link DataLoader#loadData(String[], String[], RDFFormat[])} is not
         * well-suited to your purposes. This can be much more efficient,
         * approximating the throughput for large document loads. However, the
         * caller MUST invoke {@link DataLoader#endSource()} (or
         * {@link DataLoader#doClosure()} if appropriate) once all documents are
         * loaded successfully. If an error occurs during the processing of one
         * or more documents then the entire data load should be discarded (this
         * is always true).
         * <p>
         * <strong>This feature is most useful when blank nodes are not in use,
         * but it causes memory to grow when blank nodes are in use and forces
         * statements using blank nodes to be deferred until the application
         * flushes the {@link DataLoader} when statement identifiers are enabled.
         * </strong>
         */
        public static final String FLUSH = DataLoader.class.getName()+".flush";
        
        /**
         * The default value (<code>true</code>) for {@link #FLUSH}.
         */
        public static final String DEFAULT_FLUSH = "true";
        
    }

    /**
     * Configure {@link DataLoader} using properties used to configure the
     * database.
     * 
     * @param database
     *            The database.
     */
    public DataLoader(final AbstractTripleStore database) {
        
        this(database.getProperties(), database );
        
    }

    /**
     * Configure a data loader with overridden properties.
     * 
     * @param properties
     *            Configuration properties - see {@link Options}.
     * 
     * @param database
     *            The database.
     */
    public DataLoader(final Properties properties,
            final AbstractTripleStore database) {

        if (properties == null)
            throw new IllegalArgumentException();

        if (database == null)
            throw new IllegalArgumentException();

        verifyData = Boolean.parseBoolean(properties.getProperty(
                Options.VERIFY_DATA, Options.DEFAULT_VERIFY_DATA));

        if (log.isInfoEnabled())
            log.info(Options.VERIFY_DATA + "=" + verifyData);

        commitEnum = CommitEnum.valueOf(properties.getProperty(Options.COMMIT,
                Options.DEFAULT_COMMIT));

        if (log.isInfoEnabled())
            log.info(Options.COMMIT + "=" + commitEnum);

        closureEnum = database.getAxioms().isNone() ? ClosureEnum.None
                : (ClosureEnum.valueOf(properties.getProperty(Options.CLOSURE,
                        Options.DEFAULT_CLOSURE)));

        if (log.isInfoEnabled())
            log.info(Options.CLOSURE + "=" + closureEnum);

        bufferCapacity = Integer.parseInt(properties.getProperty(
                Options.BUFFER_CAPACITY, Options.DEFAULT_BUFFER_CAPACITY));

        this.database = database;

        inferenceEngine = database.getInferenceEngine();

        if (closureEnum != ClosureEnum.None) {

            /*
             * Truth maintenance: buffer will write on a tempStore.
             */

            tm = new TruthMaintenance(inferenceEngine);

        } else {

            /*
             * No truth maintenance: buffer will write on the database.
             */

            tm = null;

        }

        flush = Boolean.parseBoolean(properties.getProperty(Options.FLUSH,
                Options.DEFAULT_FLUSH));

        if (log.isInfoEnabled())
            log.info(Options.FLUSH + "=" + flush);

    }

    /**
     * Load a resource into the database.
     * 
     * @param resource
     * @param baseURL
     * @param rdfFormat
     * 
     * @return
     * 
     * @throws IOException
     */
    final public LoadStats loadData(final String resource, final String baseURL,
            final RDFFormat rdfFormat) throws IOException {

        if (resource == null)
            throw new IllegalArgumentException();

        if (baseURL == null)
            throw new IllegalArgumentException();

        if (rdfFormat == null)
            throw new IllegalArgumentException();

        return loadData(//
                new String[] { resource }, //
                new String[] { baseURL },//
                new RDFFormat[] { rdfFormat }//
                );

    }
    
    /**
     * Load a set of RDF resources into the database.
     * 
     * @param resource
     * @param baseURL
     * @param rdfFormat
     * @return
     * 
     * @throws IOException
     */
    final public LoadStats loadData(final String[] resource,
            final String[] baseURL, final RDFFormat[] rdfFormat)
            throws IOException {

        if (resource.length != baseURL.length)
            throw new IllegalArgumentException();

        if (resource.length != rdfFormat.length)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("commit=" + commitEnum + ", closure=" + closureEnum
                    + ", resource=" + Arrays.toString(resource));

        final LoadStats totals = new LoadStats();

        for (int i = 0; i < resource.length; i++) {

            final boolean endOfBatch = i + 1 == resource.length;

            loadData2(//
                    totals,//
                    resource[i],//
                    baseURL[i],//
                    rdfFormat[i],//
                    endOfBatch//
                    );
            
        }

        if (flush && buffer != null) {

            // Flush the buffer after the document(s) have been loaded.
            
            buffer.flush();
            
        }
        
        if (commitEnum == CommitEnum.Batch) {

            if (log.isInfoEnabled())
                log.info("Commit after batch of "+resource.length+" resources");

            long beginCommit = System.currentTimeMillis();
            
            database.commit();

            totals.commitTime += System.currentTimeMillis() - beginCommit;

            if (log.isInfoEnabled())
                log.info("commit: latency="+totals.commitTime+"ms");

        }

        if (log.isInfoEnabled())
            log.info("Loaded " + resource.length+" resources: "+totals);
        
        return totals;
        
    }

    /**
     * Load from a reader.
     * 
     * @param reader
     * @param baseURL
     * @param rdfFormat
     * @return
     * @throws IOException
     */
    public LoadStats loadData(final Reader reader, final String baseURL,
            final RDFFormat rdfFormat) throws IOException {

        try {

            final LoadStats totals = new LoadStats();

            loadData3(totals, reader, baseURL, rdfFormat, true/*endOfBatch*/);
            
            return totals;
        
        } finally {
            
            reader.close();
            
        }
        
    }

    /**
     * Load from an input stream.
     * 
     * @param is
     * @param baseURL
     * @param rdfFormat
     * @return
     * @throws IOException
     */
    public LoadStats loadData(final InputStream is, final String baseURL,
            final RDFFormat rdfFormat) throws IOException {

        try {

            final LoadStats totals = new LoadStats();
            
            loadData3(totals, is, baseURL, rdfFormat, true/* endOfBatch */);
            
            return totals;
            
        } finally {
            
            is.close();
            
        }

    }

    /**
     * Load from a {@link URL}.
     * 
     * @param url
     * @param baseURL
     * @param rdfFormat
     * @return
     * @throws IOException
     */
    public LoadStats loadData(final URL url, final String baseURL,
            final RDFFormat rdfFormat) throws IOException {

        if (url == null)
            throw new IllegalArgumentException();
        
        if(log.isInfoEnabled())
            log.info("loading: " + url);

        final InputStream is = url.openStream();
        
        try {
        
            final LoadStats totals = new LoadStats();
            
            loadData3(totals, is, baseURL, rdfFormat, true/*endOfBatch*/);
            
            return totals;
        
        } finally {
            
            is.close();
            
        }
        
    }

    /**
     * Load an RDF resource into the database.
     * 
     * @param resource
     *            Either the name of a resource which can be resolved using the
     *            CLASSPATH, or the name of a resource in the local file system,
     *            or a URL.
     * @param baseURL
     * @param rdfFormat
     * @param endOfBatch
     * @return
     * 
     * @throws IOException
     *             if the <i>resource</i> can not be resolved or loaded.
     */
    protected void loadData2(final LoadStats totals, final String resource,
            final String baseURL, final RDFFormat rdfFormat,
            final boolean endOfBatch) throws IOException {

        if (log.isInfoEnabled())
            log.info("loading: " + resource);

        // try the classpath
        InputStream rdfStream = getClass().getResourceAsStream(resource);

        if (rdfStream == null) {

            /*
             * If we do not find as a Resource then try the file system.
             */
            
            final File file = new File(resource);
            
            if(file.exists()) {
                
                loadFiles(totals, 0/* depth */, file, baseURL,
                        rdfFormat, filter, endOfBatch);

                return;
                
            }
            
        }
        
        /* 
         * Obtain a buffered reader on the input stream.
         */
        
        if (rdfStream == null) {

            throw new IOException("Could not locate resource: " + resource);
            
        }

        // @todo reuse the backing buffer to minimize heap churn. 
        final Reader reader = new BufferedReader(
                new InputStreamReader(rdfStream)
//               , 20*Bytes.kilobyte32 // use a large buffer (default is 8k)
                );

        try {

            loadData3(totals, reader, baseURL, rdfFormat, endOfBatch);

        } catch (Exception ex) {

            throw new RuntimeException("While loading: " + resource, ex);

        } finally {

            reader.close();

            rdfStream.close();

        }
        
    }

    /**
     * 
     * @param file
     *            The file or directory (required).
     * @param baseURI
     *            The baseURI (optional, when not specified the name of the each
     *            file load is converted to a URL and used as the baseURI for
     *            that file).
     * @param rdfFormat
     *            The format of the file (optional, when not specified the
     *            format is deduced for each file in turn using the
     *            {@link RDFFormat} static methods).
     * @param filter
     *            A filter selecting the file names that will be loaded
     *            (optional). When specified, the filter MUST accept directories
     *            if directories are to be recursively processed.
     * 
     * @return The aggregated load statistics.
     * 
     * @throws IOException
     */
    public LoadStats loadFiles(final File file, final String baseURI,
            final RDFFormat rdfFormat, final FilenameFilter filter)
            throws IOException {

        if (file == null)
            throw new IllegalArgumentException();
        
        final LoadStats totals = new LoadStats();

        loadFiles(totals, 0/* depth */, file, baseURI, rdfFormat, filter, true/* endOfBatch */
        );

        return totals;

    }

    protected void loadFiles(final LoadStats totals, final int depth,
            final File file, final String baseURI, final RDFFormat rdfFormat,
            final FilenameFilter filter, final boolean endOfBatch)
            throws IOException {

        if (file.isDirectory()) {

            if (log.isInfoEnabled())
                log.info("loading directory: " + file);

//            final LoadStats loadStats = new LoadStats();

            final File[] files = (filter != null ? file.listFiles(filter)
                    : file.listFiles());

            for (int i = 0; i < files.length; i++) {

                final File f = files[i];

//                final RDFFormat fmt = RDFFormat.forFileName(f.toString(),
//                        rdfFormat);

                loadFiles(totals, depth + 1, f, baseURI, rdfFormat, filter,
                        (depth == 0 && i < files.length ? false : endOfBatch));
                
            }
            
            return;
            
        }
        
        final String n = file.getName();
        
        RDFFormat fmt = RDFFormat.forFileName(n);

        if (fmt == null && n.endsWith(".zip")) {
            fmt = RDFFormat.forFileName(n.substring(0, n.length() - 4));
        }

        if (fmt == null && n.endsWith(".gz")) {
            fmt = RDFFormat.forFileName(n.substring(0, n.length() - 3));
        }

        if (fmt == null) // fallback
            fmt = rdfFormat;

        InputStream is = null;

        try {

            is = new FileInputStream(file);

            if (n.endsWith(".gz")) {

                is = new GZIPInputStream(is);

            } else if (n.endsWith(".zip")) {

                is = new ZipInputStream(is);

            }

            /*
             * Obtain a buffered reader on the input stream.
             */

            // @todo reuse the backing buffer to minimize heap churn.
            final Reader reader = new BufferedReader(new InputStreamReader(is)
            // , 20*Bytes.kilobyte32 // use a large buffer (default is 8k)
            );

            try {

                // baseURI for this file.
                final String s = baseURI != null ? baseURI : file.toURI()
                        .toString();

                loadData3(totals, reader, s, fmt, endOfBatch);
                
                return;

            } catch (Exception ex) {

                throw new RuntimeException("While loading: " + file, ex);

            } finally {

                reader.close();

            }

        } finally {
            
            if (is != null)
                is.close();

        }

    }
    
    /**
     * Loads data from the <i>source</i>. The caller is responsible for closing
     * the <i>source</i> if there is an error.
     * 
     * @param source
     *            A {@link Reader} or {@link InputStream}.
     * @param baseURL
     * @param rdfFormat
     * @param endOfBatch
     * @return
     */
    protected void loadData3(final LoadStats totals, final Object source,
            final String baseURL, final RDFFormat rdfFormat,
            final boolean endOfBatch) throws IOException {

        final long begin = System.currentTimeMillis();
        
        final LoadStats stats = new LoadStats();
        
        // Note: allocates a new buffer iff the [buffer] is null.
        getAssertionBuffer();
        
        if(!buffer.isEmpty()) {
            
            /*
             * Note: this is just paranoia. If the buffer is not empty when we
             * are starting to process a new document then either the buffer was
             * not properly cleared in the error handling for a previous source
             * or the DataLoader instance is being used by concurrent threads.
             */
            
            buffer.reset();
            
        }
        
        // Setup the loader.
        final PresortRioLoader loader = new PresortRioLoader(buffer);

        // @todo review: disable auto-flush - caller will handle flush of the buffer.
//        loader.setFlush(false);

        // add listener to log progress.
        loader.addRioLoaderListener( new RioLoaderListener() {
            
            public void processingNotification( final RioLoaderEvent e ) {
                
                if (log.isInfoEnabled()) {
                    log.info
                    ( e.getStatementsProcessed() + 
                      " stmts added in " + 
                      (e.getTimeElapsed() / 1000d) +
                      " secs, rate= " + 
                      e.getInsertRate()
                      );
                }
                
            }
            
        });
        
        try {
            
            if(source instanceof Reader) {
                
                loader.loadRdf((Reader) source, baseURL, rdfFormat, verifyData);

            } else if (source instanceof InputStream) {

                loader.loadRdf((InputStream) source, baseURL, rdfFormat,
                        verifyData);

            } else
                throw new AssertionError();

            final long nstmts = loader.getStatementsAdded();

            stats.toldTriples = nstmts;

            stats.loadTime = System.currentTimeMillis() - begin;

            if (closureEnum == ClosureEnum.Incremental
                    || (endOfBatch && closureEnum == ClosureEnum.Batch)) {

                /*
                 * compute the closure.
                 * 
                 * @todo batch closure logically belongs in the outer method.
                 */

                if (log.isInfoEnabled())
                    log.info("Computing closure.");

                stats.closureStats.add(doClosure());

            }

            // commit the data.
            if (commitEnum == CommitEnum.Incremental) {

                if(log.isInfoEnabled())
                    log.info("Commit after each resource");

                final long beginCommit = System.currentTimeMillis();

                database.commit();

                stats.commitTime = System.currentTimeMillis() - beginCommit;

                if (log.isInfoEnabled())
                    log.info("commit: latency=" + stats.commitTime + "ms");

            }

            stats.totalTime = System.currentTimeMillis() - begin;

            if (log.isInfoEnabled()) {
                log.info(stats.toString());
                if (buffer != null
                        && buffer.getDatabase() instanceof AbstractLocalTripleStore) {
                    log.info(((AbstractLocalTripleStore) buffer.getDatabase())
                            .getLocalBTreeBytesWritten(new StringBuilder())
                            .toString());
                }
            }
            
            return;
            
        } catch ( Exception ex ) {

            /*
             * Note: discard anything in the buffer in case auto-flush is
             * disabled. This prevents the buffer from retaining data after a
             * failed load operation. The caller must still handle the thrown
             * exception by discarding the writes already on the backing store
             * (that is, by calling abort()).
             */

            if(buffer != null) {
            
                // clear any buffer statements.
                buffer.reset();

                if (tm != null) {
                    
                    // delete the tempStore if truth maintenance is enabled.
                    buffer.getStatementStore().close();
                    
                }

                buffer = null;
                
            }
            
            if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;

            if (ex instanceof IOException)
                throw (IOException) ex;
            
            final IOException ex2 = new IOException("Problem loading data?");
            
            ex2.initCause(ex);
            
            throw ex2;
            
        } finally {
            
            // aggregate regardless of the outcome.
            totals.add(stats);
            
        }

    }
    
    /**
     * Compute closure as configured. If {@link ClosureEnum#None} was selected
     * then this MAY be used to (re-)compute the full closure of the database.
     * 
     * @see #removeEntailments()
     * 
     * @throws IllegalStateException
     *             if assertion buffer is <code>null</code>
     */
    public ClosureStats doClosure() {
        
        if (buffer == null)
            throw new IllegalStateException();
        
        // flush anything in the buffer.
        buffer.flush();
        
        final ClosureStats stats;
        
        switch (closureEnum) {

        case Incremental:
        case Batch: {

            /*
             * Incremental truth maintenance.
             */
            
            stats = new TruthMaintenance(inferenceEngine)
                    .assertAll((TempTripleStore) buffer.getStatementStore());
            
            /*
             * Discard the buffer since the backing tempStore was closed when
             * we performed truth maintenance.
             */
            
            buffer = null;

            break;

        }

        case None: {

            /*
             * Close the database against itself.
             * 
             * Note: if there are already computed entailments in the database
             * ANY any explicit statements have been deleted then the caller
             * needs to first delete all entailments from the database.
             */

            stats = inferenceEngine.computeClosure(null/* focusStore */);
            
            break;
            
        }

        default:
            throw new AssertionError();

        }

        return stats;
        
    }

    /**
     * Utility method may be used to create and/or load RDF data into a local
     * database instance. Directories will be recursively processed. The data
     * files may be compressed using zip or gzip, but the loader does not
     * support multiple data files within a single archive.
     * 
     * @param args
     *            [-namespace <i>namespace</i>] propertyFile (fileOrDir)+
     * 
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {

        // default namespace.
        String namespace = "kb";
        
        int i = 0;

        while (i < args.length) {
            
            final String arg = args[i];
            
            if(arg.startsWith("-")) {
                
                if(arg.equals("-namespace")) {
        
                    namespace = args[++i];
                    
                } else {
                    
                    System.err.println("Unknown argument: " + arg);
                    
                    usage();
                    
                }
                
            } else {
                
                break;

            }
            
            i++;
            
        }
        
        final int remaining = args.length - i;

        if (remaining < 2) {

            System.err.println("Not enough arguments.");

            usage();

        }

        final File propertyFile = new File(args[i++]);

        if (!propertyFile.exists()) {

            throw new FileNotFoundException(propertyFile.toString());

        }

        final Properties properties = new Properties();
        {
            System.out.println("Reading properties: "+propertyFile);
            final InputStream is = new FileInputStream(propertyFile);
            try {
                properties.load(is);
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }

        final List<File> files = new LinkedList<File>();
        while(i<args.length) {
            
            final File fileOrDir = new File(args[i++]);
            
            if(!fileOrDir.exists()) {
                
                throw new FileNotFoundException(fileOrDir.toString());
                
            }
            
            files.add(fileOrDir);
            
            System.out.println("Will load from: " + fileOrDir);

        }
            
        Journal jnl = null;
        try {

            jnl = new Journal(properties);
            
            final long firstOffset = jnl.getRootBlockView().getNextOffset();
            
            System.out.println("Journal file: "+jnl.getFile());

            AbstractTripleStore kb = (AbstractTripleStore) jnl
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (kb == null) {

                kb = new LocalTripleStore(jnl, namespace, Long
                        .valueOf(ITx.UNISOLATED), properties);

                kb.create();
                
            }

            final DataLoader dataLoader = kb.getDataLoader();
            
            for (File fileOrDir : files) {

                dataLoader.loadFiles(fileOrDir, null/* baseURI */,
                        null/* rdfFormat */, filter);

            }
            
            dataLoader.endSource();
            
            jnl.commit();
            
            final long lastOffset = jnl.getRootBlockView().getNextOffset();

            System.out.println("Wrote: " + (lastOffset - firstOffset)
                    + " bytes.");
            
        } finally {

            if (jnl != null) {

                jnl.close();

            }
            
        }

    }

    private static void usage() {
        
        System.err.println("usage: [-namespace namespace] propertyFile (fileOrDir)+");

        System.exit(1);
        
    }

    /**
     * Note: The filter is chosen to select RDF data files and to allow the data
     * files to use owl, ntriples, etc as their file extension.  gzip and zip
     * extensions are also supported.
     */
    final private static FilenameFilter filter = new FilenameFilter() {

        public boolean accept(final File dir, final String name) {

            if (new File(dir, name).isDirectory()) {

                if(dir.isHidden()) {
                    
                    // Skip hidden files.
                    return false;
                    
                }
                
//                if(dir.getName().equals(".svn")) {
//                    
//                    // Skip .svn files.
//                    return false;
//                    
//                }
                
                // visit subdirectories.
                return true;
                
            }

            // if recognizable as RDF.
            boolean isRDF = RDFFormat.forFileName(name) != null
                    || (name.endsWith(".zip") && RDFFormat.forFileName(name
                            .substring(0, name.length() - 4)) != null)
                    || (name.endsWith(".gz") && RDFFormat.forFileName(name
                            .substring(0, name.length() - 3)) != null);

            System.err.println("dir=" + dir + ", name=" + name + " : isRDF="
                    + isRDF);

            return isRDF;

        }

    };

}
