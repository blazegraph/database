/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 1, 2007
 */

package com.bigdata.rdf.store;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.sesame.constants.RDFFormat;

import com.bigdata.journal.TemporaryStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.rio.IRioLoader;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * A utility class to facility loading RDF data into an
 * {@link AbstractTripleStore}.
 * 
 * @todo for concurrent data writers, this class should probably allocate an
 *       {@link RdfKeyBuilder} provisioned according to the target database and
 *       attach it to the {@link StatementBuffer}. Alternatively, have the
 *       {@link StatementBuffer} do that. In either case, the batch API on the
 *       {@link AbstractTripleStore} should then use the {@link RdfKeyBuilder}
 *       attached to the {@link StatementBuffer}.
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

    private final int bufferCapacity;
    
    private final CommitEnum commitEnum;
    
    private final ClosureEnum closureEnum;

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
     * The target database.
     */
    private final AbstractTripleStore database;
    
    /**
     * The object used to compute the closure of the {@link #tempStoreRef}
     * against the {@link #database} (IFF entailments are to be computed).
     */
    // Note: public since exposed to a test case.
    public final InferenceEngine inferenceEngine;

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
    public static class Options {
        
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
        
    }
    
    /**
     * Used to retain the {@link StatementBuffer} between runs of the parser
     * while allowing it to be garbage collected if necessary.
     */
    private SoftReference<StatementBuffer> bufferRef;

    /**
     * Returns the {@link StatementBuffer} instance that is used by the data
     * loader.
     */
    protected StatementBuffer getStatementBuffer() {

        StatementBuffer b = null;

        if (bufferRef != null) {

            b = bufferRef.get();

        }

        if (b == null) {

            /*
             * Note: will write on the tempStore iff it is defined.
             */
            
            b = new StatementBuffer(getTempStore(), database, bufferCapacity);

            bufferRef = new SoftReference<StatementBuffer>(b);

            log.info("Allocated statement buffer: capacity=" + bufferCapacity);

        }

        return b;

    }

    /**
     * Used to retain the {@link TempTripleStore} between runs of the parser
     * while allowing it to be garbage collected if necessary.
     */
    private SoftReference<TempTripleStore> tempStoreRef;

    /**
     * Return the {@link TempTripleStore} for the {@link DataLoader} IFF it the
     * {@link ClosureEnum} will maintain closure. The {@link TempTripleStore} is
     * lazily allocated since it may have been released by {@link #close()}.
     */
    protected TempTripleStore getTempStore() {

        TempTripleStore t = null;
        
        if(tempStoreRef!=null) {
            
            t = tempStoreRef.get();
            
        }
        
        if(t==null && closureEnum!=ClosureEnum.None) {

            t = new TempTripleStore(database.getProperties());
            
            tempStoreRef = new SoftReference<TempTripleStore>( t );
            
        }
        
        return t;
        
    }
    
    /**
     * 
     */
    public DataLoader(Properties properties, AbstractTripleStore database) {
        
        this.database = database;
        
        verifyData = Boolean.parseBoolean(properties.getProperty(
                Options.VERIFY_DATA, Options.DEFAULT_VERIFY_DATA));
        
        log.info(Options.VERIFY_DATA+"="+verifyData);
        
        commitEnum = CommitEnum.valueOf(properties.getProperty(
                Options.COMMIT, Options.DEFAULT_COMMIT));
        
        log.info(Options.COMMIT+"="+commitEnum);
        
        bufferCapacity = Integer.parseInt(properties.getProperty(
                Options.BUFFER_CAPACITY, Options.DEFAULT_BUFFER_CAPACITY));
        
        log.info(Options.BUFFER_CAPACITY+"="+bufferCapacity);

        closureEnum = ClosureEnum.valueOf(properties.getProperty(Options.CLOSURE,
                Options.DEFAULT_CLOSURE));

        log.info(Options.CLOSURE+"="+closureEnum);

        // allocated regardless - useful for recomputing the full closure.
        this.inferenceEngine = new InferenceEngine(properties, database);

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

        log.info("commit="+commitEnum+", closure="+closureEnum+", resource="+Arrays.toString(resource));
        
        /*
         * Hold a hard reference (will be null if we are not using the temporary
         * store). Holding a hard reference prevents GC from sweeping the temp
         * store while we are loading data! Otherwise it might get swept between
         * data set loads!!!
         */

        final TempTripleStore hardRef = getTempStore();
        
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

        if (commitEnum==CommitEnum.Batch) {

            log.info("Commit after batch of "+resource.length+" resources");

            long beginCommit = System.currentTimeMillis();
            
            database.commit();

            totals.commitTime += System.currentTimeMillis() - beginCommit;

            log.info("commit: latency="+totals.commitTime+"ms");

        }
        
        return totals;
        
    }

    /**
     * Load an RDF resource into the database.
     * 
     * @todo change to use correct Parser method depending on Reader vs
     *       InputStream (SAX Source)
     * 
     * @todo support reading from a URL.
     */
    protected LoadStats loadData2(String resource, String baseURL,
            RDFFormat rdfFormat, boolean endOfBatch) throws IOException {

        final long begin = System.currentTimeMillis();
        
        LoadStats stats = new LoadStats();
        
        log.info( "loading: " + resource );
        
        IRioLoader loader = new PresortRioLoader(getStatementBuffer());

        loader.addRioLoaderListener( new RioLoaderListener() {
            
            public void processingNotification( RioLoaderEvent e ) {
                
                log.info
                    ( e.getStatementsProcessed() + 
                      " stmts added in " + 
                      ((double)e.getTimeElapsed()) / 1000d +
                      " secs, rate= " + 
                      e.getInsertRate() 
                      );
                
            }
            
        });
        
        InputStream rdfStream = getClass().getResourceAsStream(resource);

        if (rdfStream == null) {

            // If we do not find as a Resource then try the file system.
            rdfStream = new BufferedInputStream(new FileInputStream(resource));

        }

        Reader reader = new BufferedReader(new InputStreamReader(rdfStream));
        
        try {
            
            loader.loadRdf(reader, baseURL, rdfFormat, verifyData);
            
            long nstmts = loader.getStatementsAdded();
            
            stats.toldTriples = nstmts;
            
            stats.loadTime = System.currentTimeMillis() - begin;

            if (closureEnum == ClosureEnum.Incremental
                    || (endOfBatch && closureEnum == ClosureEnum.Batch)) {
                
                /*
                 * compute the closure.
                 */
                
                log.info("Computing closure.");
                
                stats.closureStats = doClosure();
                
            }
            
            // commit the data.
            if(commitEnum==CommitEnum.Incremental) {
                
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
            
            throw new RuntimeException("While loading: "+resource, ex);
            
        } finally {
            
            reader.close();
            
        }

    }

    /**
     * Wipes out the entailments from the database (this requires a full scan on
     * all statement indices).
     * 
     * @todo implement and move to {@link AbstractTripleStore}.
     */
    public void removeEntailments() {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Compute closure as configured. If {@link ClosureEnum#None} was selected
     * then this MAY be used to (re-)compute the full closure of the database.
     * 
     * @see #removeEntailments()
     */
    public ClosureStats doClosure() {
        
        final ClosureStats stats;
        
        switch (closureEnum) {

        case Incremental:
        case Batch: {

            /*
             * closes the temporary store against the database, writing
             * entailments into the temporary store.
             */
            
            TempTripleStore tempStore = this.tempStoreRef.get();
            
            if(tempStore==null) {
                
                throw new RuntimeException("No temporary store?");
                
            }
            
            final int nbeforeClosure = tempStore.getStatementCount();

            log.info("Computing closure of the temporary store with "
                    + nbeforeClosure + " statements");

            stats = inferenceEngine.computeClosure(tempStore);

            final int nafterClosure = tempStore.getStatementCount();

            log.info("There are " + nafterClosure
                    + " statements in the temporary store after closure");
            
            // measure time for these other operations as well.

            final long begin = System.currentTimeMillis();
            
            /*
             * copy statements from the temporary store to the database.
             */
            
            log.info("Copying statements from the temporary store to the database");
            
            int ncopied = tempStore.copyStatements(database, null/*filter*/);
            
            // note: this is the number that are _new_ to the database.
            log.info("Copied "+ncopied+" statements that were new to the database.");
            
            /*
             * clear the temporary store (drops the indices).
             */

            log.info("Clearing the temporary store");
            
            tempStore.clear();
            
            /*
             * If the temporary store has grown "too large" then delete it and
             * create a new one.
             * 
             * Note: The backing store is a WORM (wrote once, read many).
             * Therefore it never shrinks in size. By periodically deleting the
             * backing store we avoid having it consume too much space on the
             * disk.
             */
            
            TemporaryStore backingStore = tempStore.getBackingStore();
            
            if(backingStore.getBufferStrategy().getNextOffset() > 200 * Bytes.megabyte) {
                
                log.info("Closing the temporary store");
                
                // delete the backing file.
                tempStore.close();

                // clear the soft reference.
                tempStoreRef = null;
                
            }
            
            stats.elapsed += System.currentTimeMillis() - begin;
            
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
            
            stats = inferenceEngine.computeClosure(null);
            
            break;
            
        }

        default:
            throw new AssertionError();

        }

        return stats;
        
    }

    /**
     * Closes the optional {@link TempTripleStore} and releases the
     * {@link StatementBuffer} for GC. They will be re-created as necessary.
     * <p>
     * Note: Do NOT invoke this if you are concurrently loading data...
     */
    public void close() {

        TempTripleStore t = (tempStoreRef==null?null:tempStoreRef.get());

        if (t != null) {
            
            t.close();
            
        }
        
        tempStoreRef = null;
        
        bufferRef = null;
        
    }
    
}
