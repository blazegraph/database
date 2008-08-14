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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.spo.SPO;

/**
 * A utility class to efficiently load RDF data into an
 * {@link AbstractTripleStore} without using Sesame API.
 * 
 * FIXME it should be easier to configure the underlying parsers in order to
 * enable or disable various features which they support, e.g., preserving BNode
 * IDs, validation, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataLoader {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger(DataLoader.class);

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

            log.info("");
            
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
        public static final String VERIFY_DATA = "dataLoader.verifyData";
        
        public static final String DEFAULT_VERIFY_DATA = "false";
        
        /**
         * Optional property specifying whether and when the {@link DataLoader}
         * will {@link ITripleStore#commit()} the database (default
         * {@link CommitEnum#Batch}).
         * <p>
         * Note: commit semantics vary depending on the specific backing store.
         * See {@link ITripleStore#commit()}.
         */
        public static final String COMMIT = "dataLoader.commit";
        
        public static final String DEFAULT_COMMIT = CommitEnum.Batch.toString();

        /**
         * Optional property specifying the capacity of the
         * {@link StatementBuffer} (default is 100k statements).
         */
        public static final String BUFFER_CAPACITY = "dataLoader.bufferCapacity";
        
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
        public static final String CLOSURE = "dataLoader.closure";
        
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
         * flushes the {@link DataLoader} when statement identifers are enabled.
         * </strong>
         */
        public static final String FLUSH = "dataLoader.flush";
        
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
    public DataLoader(AbstractTripleStore database) {
        
        this(database.getProperties(), database );
        
    }

    /**
     * Configure a data loader with overriden properties.
     * 
     * @param properties
     *            Configuration properties - see {@link Options}.
     * 
     * @param database
     *            The database.
     */
    public DataLoader(Properties properties, AbstractTripleStore database) {
        
        if (properties == null)
            throw new IllegalArgumentException();

        if (database == null)
            throw new IllegalArgumentException();
        
        verifyData = Boolean.parseBoolean(properties.getProperty(
                Options.VERIFY_DATA, Options.DEFAULT_VERIFY_DATA));
        
        log.info(Options.VERIFY_DATA+"="+verifyData);
        
        commitEnum = CommitEnum.valueOf(properties.getProperty(
                Options.COMMIT, Options.DEFAULT_COMMIT));
        
        log.info(Options.COMMIT+"="+commitEnum);

        closureEnum = ClosureEnum.valueOf(properties.getProperty(Options.CLOSURE,
                Options.DEFAULT_CLOSURE));

        log.info(Options.CLOSURE+"="+closureEnum);

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
        
        flush = Boolean.parseBoolean(properties.getProperty(
                Options.FLUSH, Options.DEFAULT_FLUSH));
        
        log.info(Options.FLUSH+"="+flush);
        
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
    final public LoadStats loadData(String resource, String baseURL,
            RDFFormat rdfFormat) throws IOException {

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
    final public LoadStats loadData(String[] resource, String[] baseURL,
            RDFFormat[] rdfFormat) throws IOException {

        if (resource.length != baseURL.length)
            throw new IllegalArgumentException();

        if (resource.length != rdfFormat.length)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("commit=" + commitEnum + ", closure=" + closureEnum
                    + ", resource=" + Arrays.toString(resource));

        LoadStats totals = new LoadStats();
        
        LoadStats[] loadStats = new LoadStats[resource.length];

        for(int i=0; i<resource.length; i++) {
            
            final boolean endOfBatch = i + 1 == resource.length;
            
            loadStats[i] = loadData2(//
                    resource[i],//
                    baseURL[i],//
                    rdfFormat[i],//
                    endOfBatch
                    );
            
            totals.add(loadStats[i]);
            
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
    public LoadStats loadData(Reader reader, String baseURL, RDFFormat rdfFormat) throws IOException  {

        try {

            return loadData3(reader, baseURL, rdfFormat, true/*endOfBatch*/);
        
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
    public LoadStats loadData(InputStream is, String baseURL,
            RDFFormat rdfFormat) throws IOException {

        try {

            return loadData3(is, baseURL, rdfFormat, true/* endOfBatch */);
            
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
    public LoadStats loadData(URL url, String baseURL, RDFFormat rdfFormat)
            throws IOException {

        if (url == null)
            throw new IllegalArgumentException();
        
        log.info("loading: " + url);

        final InputStream is = url.openStream();
        
        try {
        
            return loadData3(is, baseURL, rdfFormat, true/*endOfBatch*/);
        
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
    protected LoadStats loadData2(String resource, String baseURL,
            RDFFormat rdfFormat, boolean endOfBatch) throws IOException {

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
                
                return loadFiles(0/* depth */, file, baseURL, rdfFormat,
                        null/* filter */, endOfBatch);
                
            }
            
        }
        
        /* 
         * Obtain a buffered reader on the input stream.
         */

        // @todo reuse the backing buffer to minimize heap churn. 
        final Reader reader = new BufferedReader(
                new InputStreamReader(rdfStream)
//               , 20*Bytes.kilobyte32 // use a large buffer (default is 8k)
                );

        try {

            return loadData3(reader, baseURL, rdfFormat, endOfBatch);

        } catch (Exception ex) {

            throw new RuntimeException("While loading: " + resource, ex);

        } finally {

            reader.close();

            rdfStream.close();

        }
        
    }

    public LoadStats loadFiles(File file, String baseURL, RDFFormat rdfFormat,
            FilenameFilter filter) throws IOException {

        return loadFiles(0/* depth */, file, baseURL, rdfFormat, filter, true/* endOfBatch */
        );

    }

    protected LoadStats loadFiles(int depth, File file, String baseURL,
            RDFFormat rdfFormat, FilenameFilter filter, boolean endOfBatch)
            throws IOException {

        if (file.isDirectory()) {

            final LoadStats loadStats = new LoadStats();

            final File[] files = (filter != null ? file.listFiles(filter)
                    : file.listFiles());

            for (int i = 0; i < files.length; i++) {

                final File f = files[i];

                final RDFFormat fmt = RDFFormat.forFileName(f.toString(),
                        rdfFormat);

                loadStats.add(loadFiles(depth + 1, f, baseURL, fmt, filter,
                        (depth == 0 && i < files.length ? false : endOfBatch)));
                
            }
            
            return loadStats;
            
        }
        
        final InputStream rdfStream = new FileInputStream(file);

        /* 
         * Obtain a buffered reader on the input stream.
         */

        // @todo reuse the backing buffer to minimize heap churn. 
        final Reader reader = new BufferedReader(
                new InputStreamReader(rdfStream)
//               , 20*Bytes.kilobyte32 // use a large buffer (default is 8k)
                );
        
        try {

            return loadData3(reader, baseURL, rdfFormat, endOfBatch);

        } catch (Exception ex) {

            throw new RuntimeException("While loading: " + file, ex);

        } finally {

            reader.close();

            rdfStream.close();

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
    protected LoadStats loadData3(Object source, String baseURL,
            RDFFormat rdfFormat, boolean endOfBatch) throws IOException {

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
            
            public void processingNotification( RioLoaderEvent e ) {
                
                log.info
                    ( e.getStatementsProcessed() + 
                      " stmts added in " + 
                      (e.getTimeElapsed() / 1000d) +
                      " secs, rate= " + 
                      e.getInsertRate() 
                      );
                
            }
            
        });
        
        try {
            
            if(source instanceof Reader) {
                
                loader.loadRdf((Reader)source, baseURL, rdfFormat, verifyData);
                
            } else if(source instanceof InputStream) {
                
                loader.loadRdf((InputStream)source, baseURL, rdfFormat, verifyData);
                
            } else throw new AssertionError();
            
            long nstmts = loader.getStatementsAdded();
            
            stats.toldTriples = nstmts;
            
            stats.loadTime = System.currentTimeMillis() - begin;

            if (closureEnum == ClosureEnum.Incremental
                    || (endOfBatch && closureEnum == ClosureEnum.Batch)) {
                
                /*
                 * compute the closure.
                 * 
                 * FIXME closure stats are not being reported out, e.g., to the DataLoader.
                 * 
                 * Also, batch closure logically belongs in the outer method.
                 */
                
                if (log.isInfoEnabled())
                    log.info("Computing closure.");
                
                stats.closureStats.add(doClosure());
                
            }

            // commit the data.
            if (commitEnum == CommitEnum.Incremental) {

                log.info("Commit after each resource");

                long beginCommit = System.currentTimeMillis();

                database.commit();

                stats.commitTime = System.currentTimeMillis() - beginCommit;

                log.info("commit: latency="+stats.commitTime+"ms");
                
            }
            
            stats.totalTime = System.currentTimeMillis() - begin;
            
            log.info( stats.toString());

            return stats;
            
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
                    buffer.getStatementStore().closeAndDelete();
                    
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
    
}
