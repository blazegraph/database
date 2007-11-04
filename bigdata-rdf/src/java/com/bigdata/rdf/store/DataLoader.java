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
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.sesame.constants.RDFFormat;

import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.rio.IRioLoader;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.BigdataRdfRepository;

/**
 * A utility class to facility loading RDF data into an
 * {@link AbstractTripleStore} without using {@link BigdataRdfRepository}..
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
     * The object used to compute entailments for the database.
     */
    public InferenceEngine getInferenceEngine() {
        
        return inferenceEngine;
        
    }
    
    /**
     * Used to buffer writes. This will be a {@link TMStatementBuffer} iff we are
     * using {@link ClosureEnum#Incremental} and a {@link StatementBuffer}
     * otherwise.
     */
    private final IStatementBuffer buffer;
    
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
     * Create a data loader.
     * 
     * @param properties
     *            Configuration properties - see {@link Options}.
     * 
     * @param database
     *            The database.
     *            
     * @todo have the caller pass in the InferenceEngine?
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
        
        inferenceEngine = new InferenceEngine(properties, database);

        if (closureEnum != ClosureEnum.None) {
            
            buffer = new TMStatementBuffer(inferenceEngine, bufferCapacity,
                    true/* buffer contains assertions */);
            
        } else {
            
            buffer = new StatementBuffer(null, database, bufferCapacity);
            
        }
        
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

        log.info("Loaded "+resource.length+" resources: "+totals);
        
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
        
        IRioLoader loader = new PresortRioLoader(buffer);

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
                 * 
                 * FIXME closure stats are not being reported out, e.g., to the DataLoader.
                 * 
                 * Also, batch closure logically belongs in the outer method.
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

            assert buffer != null;
            
            stats = ((TMStatementBuffer)buffer).assertAll();
            
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
    
}
