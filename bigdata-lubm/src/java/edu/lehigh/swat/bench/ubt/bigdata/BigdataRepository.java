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
 * Created on Apr 17, 2007
 */

package edu.lehigh.swat.bench.ubt.bigdata;

import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailException;
import org.openrdf.sail.config.SailConfigException;

import com.bigdata.LRUNexus;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.load.ConcurrentDataLoader;
import com.bigdata.rdf.load.FileSystemLoader;
import com.bigdata.rdf.load.RDFLoadTaskFactory;
import com.bigdata.rdf.load.RDFVerifyTaskFactory;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.IBigdataFederation;

import edu.lehigh.swat.bench.ubt.api.Query;
import edu.lehigh.swat.bench.ubt.api.QueryResult;
import edu.lehigh.swat.bench.ubt.api.Repository;
import edu.lehigh.swat.bench.ubt.bigdata.BigdataRepositoryFactory.IRepositoryLifeCycle;
import edu.lehigh.swat.bench.ubt.bigdata.BigdataRepositoryFactory.Options;

/**
 * Exposes the {@link BigdataSailConnection} for testing using Serql queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRepository implements Repository {

    final protected static Logger log = Logger.getLogger(BigdataRepository.class);
    
//    final protected static boolean log.isInfoEnabled() = log.isInfoEnabled();
//
//    final protected static boolean DEBUG = log.isDebugEnabled();

    private final IRepositoryLifeCycle lifeCycle;
    
    BigdataSail sail;

    private QueryParser engine;

    /**
     * The URL of the ontology.
     * 
     * @see #setOntology(String)
     */
    private String ontology;

    /**
     * When <code>true</code>, the specified ontology and data files will
     * be loaded. This can be disabled if you just want to compute the
     * closure of the database.
     */
    final boolean loadData;

    /**
     * When <code>true</code>, verify that all statements loaded from the
     * ontology and data files exist as explicit statements in the knowledge
     * base.
     */
    final boolean verifyData;

    /**
     * When <code>true</code>, the closure of the data set will be computed.
     * Note that the closure will be computed ANYWAY if the {@link BigdataSail}
     * is configured for incremental truth maintenance.
     * 
     * @see BigdataSail.Options#TRUTH_MAINTENANCE
     */
    final boolean computeClosure;

    /**
     * When true, as the RDF parser to verify the source file.
     */
    final boolean verifyRDFSourceData;

    /**
     * When <code>true</code> the source files are deleted after they have
     * been processed.
     */
    final boolean deleteAfter = false;

    /** When true, will force overflow of data services after load(+closure). */
    final boolean forceOverflowAfterLoad;    
    
    /*
     * Properties that are used iff we use the ConcurrentDataLoader.
     */
    final int nthreads;
    final int nclients;
    final int clientNum;
    final int bufferCapacity;

    /**
     * Saves the properties but does not open the repository.
     * 
     * @see Options
     */
    public BigdataRepository(final IRepositoryLifeCycle lifeCycle) {

        this.lifeCycle = lifeCycle;
    
        /*
         * Note: These are the properties from the factory NOT from the
         * persistent triple store. We use the factory properties for things
         * that can be overriden using -D on the command line.
         * 
         * Note: DO NOT initialize things here that need to be set from the
         * properties stored in the triple store instance!
         */

        final Properties properties = lifeCycle.getProperties();
        
        /*
         * Initialize some things based on properties specified in the
         * factory or using -D on the command line.
         */
        
        loadData = Boolean.parseBoolean(properties.getProperty(
                Options.LOAD_DATA, Options.DEFAULT_LOAD_DATA));

        if (log.isInfoEnabled())
            log.info(Options.LOAD_DATA + "=" + loadData);

        verifyData = Boolean.parseBoolean(properties.getProperty(
                Options.VERIFY_DATA, Options.DEFAULT_VERIFY_DATA));

        if (log.isInfoEnabled())
            log.info(Options.VERIFY_DATA + "=" + verifyData);
        
        computeClosure = Boolean.parseBoolean(properties
                .getProperty(Options.COMPUTE_CLOSURE,
                        Options.DEFAULT_COMPUTE_CLOSURE));

        if (log.isInfoEnabled())
            log.info(Options.COMPUTE_CLOSURE + "=" + computeClosure);
        
        /*
         * When true, as the RDF parser to verify the source file.
         * 
         * @todo configure via Properties.
         */
        verifyRDFSourceData = false;

        // when true, will force overflow of data services after load(+closure).
        forceOverflowAfterLoad = Boolean
                .parseBoolean(properties
                        .getProperty(
                                BigdataRepositoryFactory.Options.FORCE_OVERFLOW_AFTER_LOAD,
                                BigdataRepositoryFactory.Options.DEFAULT_FORCE_OVERFLOW_AFTER_LOAD));

        if (log.isInfoEnabled())
            log.info(Options.FORCE_OVERFLOW_AFTER_LOAD + "="
                    + forceOverflowAfterLoad);

        /*
         * Properties that are used iff we use the ConcurrentDataLoader.
         */
        nthreads = Integer
                .parseInt(properties
                        .getProperty(
                                BigdataRepositoryFactory.Options.CONCURRENT_DATA_LOADER_NTHREADS,
                                BigdataRepositoryFactory.Options.DEFAULT_CONCURRENT_DATA_LOADER_NTHREADS));

        if (log.isInfoEnabled())
            log.info(Options.CONCURRENT_DATA_LOADER_NTHREADS + "=" + nthreads);

        nclients = Integer
                .parseInt(properties
                        .getProperty(
                                BigdataRepositoryFactory.Options.CONCURRENT_DATA_LOADER_NCLIENTS,
                                BigdataRepositoryFactory.Options.DEFAULT_CONCURRENT_DATA_LOADER_NCLIENTS));

        if (log.isInfoEnabled())
            log.info(Options.CONCURRENT_DATA_LOADER_NCLIENTS + "=" + nclients);

        clientNum = Integer
                .parseInt(properties
                        .getProperty(
                                BigdataRepositoryFactory.Options.CONCURRENT_DATA_LOADER_CLIENTNUM,
                                BigdataRepositoryFactory.Options.DEFAULT_CONCURRENT_DATA_LOADER_CLIENTNUM));

        if (log.isInfoEnabled())
            log
                    .info(Options.CONCURRENT_DATA_LOADER_CLIENTNUM + "="
                            + clientNum);

        bufferCapacity = Integer
                .parseInt(properties
                        .getProperty(
                                BigdataRepositoryFactory.Options.CONCURRENT_DATA_LOADER_BUFFER_CAPACITY,
                                BigdataRepositoryFactory.Options.DEFAULT_CONCURRENT_DATA_LOADER_BUFFER_CAPACITY));

        if (log.isInfoEnabled())
            log.info(Options.CONCURRENT_DATA_LOADER_BUFFER_CAPACITY + "="
                    + bufferCapacity);

    }

    /**
     * Return various interesting metadata about the KB state.
     */
    protected StringBuilder getKBInfo() {
        
        final StringBuilder sb = new StringBuilder();

        try {
        
        if (sail.getDatabase() instanceof LocalTripleStore) {

            final LocalTripleStore lts = (LocalTripleStore)sail.getDatabase();
            
            sb.append("nextOffset\t"
                    + ((Journal) lts.getIndexManager()).getRootBlockView()
                            .getNextOffset()+"\n");
            
//            if (false) {
//
//                // show the disk access details.
//
//                sb.append(((Journal) sail.getDatabase().getIndexManager())
//                        .getBufferStrategy().getCounters().toString());
//
//            }
            
        }
        
//        if(true) return sb;// comment out to get detailed info.
        
        final AbstractTripleStore tripleStore = sail.getDatabase();
        
        sb.append("class\t"+tripleStore.getClass().getName()+"\n");

        sb.append("indexManager\t"+tripleStore.getIndexManager().getClass().getName()+"\n");
        
        /*
         * Note: This can take long enough for a large KB that it causes
         * problems with the historical read point being released while the task
         * is running!
         */
//        sb.append("exactStatementCount\t" + tripleStore.getExactStatementCount()+"\n");

        sb.append("statementCount\t" + tripleStore.getStatementCount()+"\n");
        
        sb.append("termCount\t" + tripleStore.getTermCount()+"\n");
        
        sb.append("uriCount\t" + tripleStore.getURICount()+"\n");
        
        sb.append("literalCount\t" + tripleStore.getLiteralCount()+"\n");
        
        sb.append("bnodeCount\t" + tripleStore.getBNodeCount()+"\n");

        sb.append("branchingFactor\t"
                + tripleStore.getSPORelation().getPrimaryIndex()
                        .getIndexMetadata().getBranchingFactor() + "\n");

        sb.append("writeRetentionCapacity\t"
                + tripleStore.getSPORelation().getPrimaryIndex()
                        .getIndexMetadata()
                        .getWriteRetentionQueueCapacity() + "\n");

        if (LRUNexus.INSTANCE != null)
            sb.append(LRUNexus.INSTANCE.toString() + "\n");

//        sb.append(tripleStore.predicateUsage());
        
        } catch(Throwable t) {
        
            log.warn(t.getMessage(), t);
            
        }
        
        return sb;
        
    }
    
    /**
     * Opens the database configured by the constructor.
     * 
     * @param database
     *            The value of the <code>database</code> property from the kb
     *            config file.
     * 
     * @see BigdataRepositoryFactory
     */
    public void open(final String database) {

        final String namespace = BigdataRepositoryFactory.getNamespace();
        
        if (log.isInfoEnabled())
            log.info("Opening tripleStore: database=" + database
                    + ", namespace=" + namespace);

        try {

            final IIndexManager indexManager = lifeCycle.open();
            
            // locate a mutable view of the tripleStore resource - must exist.
            final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);
            
            // setup a SAIL over that tripleStore.
            sail = new BigdataSail(tripleStore);

            // initialize the SAIL.
            sail.initialize();

            // show some info on the KB state.
            System.out.println(getKBInfo());
            
            // used to parse qeries.
            engine = new SPARQLParserFactory().getParser();

        } catch (SailException ex) {

            log.error("Problem with open?", ex);
            
            throw new RuntimeException(ex);

        }
        
    }

    public void close() {

        if(log.isInfoEnabled()) {
            
            log.info("Closing repository");
            
        }

        try {
            
            // show some info on the KB state.
            System.out.println(getKBInfo());
            
            sail.shutDown();
            
            lifeCycle.close(sail.getDatabase().getIndexManager());
            
        } catch (SailException e) {

            log.error("Problem with shutdown?", e);
            
            throw new RuntimeException(e);
            
        }
        
        sail = null;

        engine = null;

    }

    public void clear() {

        try {

            final BigdataSailConnection conn = (BigdataSailConnection) sail.getConnection();

            try {
                
                conn.clear();

                conn.commit();

            } finally {

                conn.close();
                
            }

        } catch (SailException ex) {
            
            throw new RuntimeException(ex);

        }

    }

    /**
     * Sets the ontology to be loaded.
     */
    public void setOntology(String ontology) {

        this.ontology = ontology;
        
    }

    /**
     * Note: The filter is choosen to select the data files but not the ontology
     * and to allow the data files to use owl, ntriples, etc as their file
     * extension.
     */
    final private FilenameFilter filter = new FilenameFilter() {

        public boolean accept(File dir, String name) {

            if(new File(dir,name).isDirectory()) {
                
                // visit subdirectories.
                return true;
                
            }

            // if recognizable as RDF.
            return RDFFormat.forFileName(name) != null
                    || (name.endsWith(".zip") && RDFFormat.forFileName(name
                            .substring(0, name.length() - 4)) != null)
                    || (name.endsWith(".gz") && RDFFormat.forFileName(name
                            .substring(0, name.length() - 3)) != null);
            // return ( // as generated by ubt.
//            name.endsWith("owl") //
//            || ( // allegro graphs' U50 data set
//            name.startsWith("university") && name.endsWith("ntriples")//
//            )
//            || name.endsWith("nt")
//            );

        }

    };
    
    /**
     * Loads the ontology and all ".owl" files in the named directory. Uses the
     * {@link ConcurrentDataLoader} for the {@link IBigdataFederation} based
     * triple stores and a single-threaded loader otherwise.
     * <p>
     * This sets the <i>baseURI</i> to the URI form of the name of each file to
     * be loaded. The LUBM data makes two assertions made about the baseURI in
     * each file.
     * 
     * <pre>
     * 
     * baseURI rdf:type     http://www.w3.org/2002/07/owl#Ontology
     * 
     * baseURI owl:imports   http://www.lehigh.edu/&tilde;zhp2/2004/0401/univ-bench.owl
     * 
     * </pre>
     * 
     * If you instead set the baseURI to a constant that you will not generate
     * these assertions on a per-file basis and you will undergenerate
     * assertions for the dataset as a whole. (This is more annoying than
     * practical as the queries do not rely on these two assertions.)
     * 
     * @todo add option to run validation after the concurrent data load (and
     *       before closure) for EDS and JDS verification of dynamic index
     *       partitioning.
     */
    public boolean load(final String dataDir) {

        // adds "ntriples" as an N-Triples filename extension.
        RDFFormat NTRIPLES = new RDFFormat("N-Triples", "text/plain", Charset
                .forName("US-ASCII"), Arrays.asList("nt", "ntriples"), false,
                false);

        RDFFormat.register(NTRIPLES);
        
        final File dir = new File(dataDir);

        if (!dir.exists() || !dir.isDirectory()) {

            throw new RuntimeException("Could not read directory: " + dataDir);

        }

        /*
         * true iff truth maintenance was performed incrementally during load.
         */
        final boolean truthMaintenance;

        /**
         * Note: This is the properties that are currently configured for the
         * tripleStore.
         * 
         * Note: If you want to use the properties that can be overriden on the
         * command line then do that in the ctor!!!
         */
        final Properties properties = sail.getDatabase().getProperties();

        final long begin = System.currentTimeMillis();
        
        if (loadData) {

            // show the current time when we start.
            System.out.println("Loading data: now="+new Date().toString());
            
            try {

                if (sail.getDatabase().getIndexManager() instanceof IBigdataFederation) {

                    /*
                     * database at once closure is required with concurrent bulk
                     * load (it does not do incremental truth maintenance).
                     */
                    truthMaintenance = false;
            
                    loadConcurrent(nthreads, nclients, clientNum,
                            bufferCapacity, dir, verifyRDFSourceData, deleteAfter);

                    // @todo property for post-load and pre-closure validation
                    // of the concurrent data load.

                } else {

                    truthMaintenance = sail.getTruthMaintenance();

                    loadSingleThreaded(dir);

                }

                /*
                 * Make the changes restart-safe.
                 * 
                 * Note: We already have a commit point if we used the concurent
                 * data loader, but this also writes some info on System.out.
                 * 
                 * Note: skipping the commit if you are going to compute the
                 * closure can be a big win since it does not checkpoint the
                 * indices.
                 */
                if(!computeClosure) 
                    commit();
                
            } catch (Throwable t) {

                log.error("Error loading files: dir=" + dir, t);

                return false;

            }

        } else {

            // Note: [false] here will let us trigger closure below.
            truthMaintenance = false;
            
        }

        if(verifyData) {

            try {
            
                verifyConcurrent(nthreads, nclients, clientNum, bufferCapacity,
                        dir, verifyRDFSourceData, deleteAfter);
            
            } catch (Throwable t) {

                log.error("Error verifying files: dir=" + dir, t);

                return false;

            }

        }
        
        /*
         * If incremental truth maintenance was not performed and closure was
         * requested then compute the database-at-once closure.
         */
        if (!truthMaintenance && computeClosure) {

            computeClosure();
            
        }

        if (forceOverflowAfterLoad
                && (loadData || computeClosure)) {
            
            forceOverflow();
   
        }
        
        if (loadData && computeClosure) {

            /*
             * Report total tps throughput for load+closure. While closure
             * generates solutions very quickly, it produces many non-distinct
             * solutions which reduces the overall throughput.
             */
            
            final long elapsed = System.currentTimeMillis() - begin;
            
            final long ntriples = sail.getDatabase().getStatementCount(false/*exact*/);
            
            final long tps = ((long) (((double) ntriples) / ((double) elapsed) * 1000d));
            
            System.err.println("Net: tps=" + tps + ", ntriples=" + ntriples
                    + ", elapsed=" + elapsed + "ms");
            
        }
        
        // show the current time when we start.
        System.out.println("Done: now="+new Date().toString());
        
        return true;
        
    }

    /**
     * FIXME Must await all clients at a barrier (really, that should be part of
     * the CDL behavior). CDL should discover N data services (one per client),
     * send each a task, and have them await on a double barrier (enter, leave).
     * <p>
     * The data must be either read or generated using a hash partitioned
     * approach. In both cases, we want to update a checkpoint stored in a znode
     * marked with the client number (so persistent and not sequential). That
     * checkpoint should be updated every some many files, directories, or
     * universities (for lubm) processed so that a client (or system) failure
     * does not force us to restart the entire load. We have a set of
     * checkpoints (one per client) that provide a last known completion state.
     * <p>
     * To handle client death, we would have to store the queue of failures in a
     * persistent znode for the client. Otherwise retries would not succeed.
     * <p>
     * Once the data load is complete, the clients must contend for a lock and
     * only the winner should run the closure operation. Given the long running
     * nature of closure over a large data set, clients could write the set of
     * rules that have reached fixed point (stages in the program) onto the lock
     * node. That way if the client computing closure fails, the operation can
     * failover to the next client which gains the lock.
     */
    protected void computeClosure() {
        
        // show the current time when we start.
        System.out.println("Computing closure: now="+new Date().toString());
        
//        // show some info on the KB state (before computing closure).
//        System.out.println(getKBInfo());
        
        final InferenceEngine inf = sail.getDatabase().getInferenceEngine();

        // database at once closure.
        final ClosureStats closureStats = inf.computeClosure(null/* focusStore */);
        
        System.out.println("closure: "+closureStats);

        // make the changes restart-safe
        commit();

    }
    
    /**
     * Force overflow of each data service in the scale-out federation (only
     * scale-out federations support overflow processing).
     */
    protected void forceOverflow() {

        final IIndexManager indexManager = sail.getDatabase().getIndexManager();

        if (!(indexManager instanceof IBigdataFederation))
            return;

        if (!((IBigdataFederation) indexManager).isScaleOut())
            return;

        final AbstractScaleOutFederation fed = (AbstractScaleOutFederation) sail
                .getDatabase().getIndexManager();

        fed.forceOverflow(true/* truncateJournal */);

    }
    
    /**
     * Commits the store and writes some basic information onto {@link System#out}.
     */
    protected void commit() {
        
        // make the changes restart-safe
        sail.getDatabase().commit();

        if (sail.getDatabase().getIndexManager() instanceof Journal) {

            /*
             * This reports the #of bytes used on the Journal.
             */
            
            System.out.println("nextOffset: "
                    + ((Journal) sail.getDatabase().getIndexManager()).getRootBlockView()
                            .getNextOffset());
            
        }

        // show some info on the KB state (#of triples, #of terms).
        System.out.println(getKBInfo());
        
    }
    
    /**
     * Loads files concurrently into the configured {@link AbstractTripleStore}.
     * 
     * @param nthreads
     *            The #of threads in which files will be loaded.
     * @param bufferCapacity
     *            The capacity of the {@link StatementBuffer} used by each
     *            thread.
     * @param dataDir
     *            The directory from which the filters will be loaded.
     * 
     * @throws InterruptedException
     *             if interrupted.
     */
    private void loadConcurrent(final int nthreads, final int nclients,
            final int clientNum, final int bufferCapacity, final File dataDir,
            final boolean verifyRDFSourceData, final boolean deleteAfter)
            throws InterruptedException {
        
        System.out
                .println("Will load files (concurrent): #threads=" + nthreads
                        + ", #clients=" + nclients + ", client#=" + clientNum
                        + ", bufferCapacity=" + bufferCapacity + ", dataDir="
                        + dataDir);
        
        final IBigdataFederation fed = (IBigdataFederation) sail.getDatabase()
                .getIndexManager();

        final RDFFormat fallback = RDFFormat.RDFXML;

        final AbstractTripleStore db = sail.getDatabase();

//        if (clientNum == 0) {
//            
//            /*
//             * Load the ontology from only client#0.
//             * 
//             * Note: this is a work around for an apparent problem with simply
//             * adding the ontology into the set of files to be loaded by the
//             * CDL.
//             */
//        
//            loadOntology();
//            
        // }

        final RDFLoadTaskFactory loadTaskFactory = //
        new RDFLoadTaskFactory(db, bufferCapacity, verifyRDFSourceData,
                deleteAfter, fallback);

        final ConcurrentDataLoader service = new ConcurrentDataLoader(fed,
                nthreads);
        
        final FileSystemLoader scanner = new FileSystemLoader(service,
                nclients, clientNum);
        
        try {

            /*
             * Note: Add the counters to be reported to the client's counter
             * set. The added counters will be reported when the client reports its
             * own counters.
             */
            final CounterSet serviceRoot = fed.getServiceCounterSet();

            final String relPath = "Concurrent Data Loader";

            synchronized (serviceRoot) {

                if (serviceRoot.getPath(relPath) == null) {

                    // Create path to CDL counter set.
                    final CounterSet tmp = serviceRoot.makePath(relPath);

                    // Attach CDL counters.
                    tmp.attach(service.getCounters());

                    // Attach task factory counters.
                    tmp.attach(loadTaskFactory.getCounters());

                }
                
            }

            // notify will run tasks.
            loadTaskFactory.notifyStart();

//            Note: apparent problem with ontology not loaded when nclients GT ONE(1).
            
            // load ontology : baseURI will be the filename.
            scanner.process(new File(ontology), filter, loadTaskFactory);

            // read files and run tasks : baseURI will be the filename.
            scanner.process(dataDir, filter, loadTaskFactory);

            // await completion of all tasks.
            service.awaitCompletion(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            System.out.println("client#=" + clientNum + ", #scanned="
                    + scanner.getScanCount() + ", ntasked="
                    + service.getTaskedCount());

        } finally {
            
            // shutdown the load service (a memory leak otherwise!)
            service.shutdown();
  
        }

        // notify did run tasks.
        loadTaskFactory.notifyEnd();
        
        System.out.println(loadTaskFactory.reportTotals());
        
    }
    

    /**
     * Verifies that all statements in the visited files are also found in the
     * {@link AbstractTripleStore}.
     * 
     * @param nthreads
     *            The #of threads in which files will be loaded.
     * @param bufferCapacity
     *            The capacity of the {@link StatementBuffer} used by each
     *            thread.
     * @param dataDir
     *            The directory from which the filters will be loaded.
     * 
     * @throws InterruptedException
     *             if interrupted.
     */
    private void verifyConcurrent(int nthreads, int nclients, int clientNum,
            int bufferCapacity, File dataDir, boolean verifyRDFSourceData,
            boolean deleteAfter)
            throws InterruptedException {
        
        System.out.println("Will verify files: dataDir="+dataDir);
        
        final IBigdataFederation fed = (IBigdataFederation) sail.getDatabase()
                .getIndexManager();

        final RDFFormat fallback = RDFFormat.RDFXML;

        final AbstractTripleStore db = sail.getDatabase();
        
        final RDFVerifyTaskFactory verifyTaskFactory = new RDFVerifyTaskFactory(
                db, bufferCapacity, verifyRDFSourceData, deleteAfter, fallback);

        final ConcurrentDataLoader service = new ConcurrentDataLoader(fed,
                nthreads);
        
        final FileSystemLoader scanner = new FileSystemLoader(service,
                nclients, clientNum);

        try {

            // Setup counters.
//            verifyTaskFactory.setupCounters(service.getCounters(fed));

            // notify will run tasks.
            verifyTaskFactory.notifyStart();

            // load ontology : baseURI will be the filename.
            scanner.process(new File(ontology), filter, verifyTaskFactory);

            // read files and run tasks : baseURI will be the filename.
            scanner.process(dataDir, filter, verifyTaskFactory);

            // await completion of all tasks.
            service.awaitCompletion(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        } finally {
            
            // shutdown the load service (a memory leak otherwise!)
            service.shutdown();
            
        }

        // notify did run tasks.
        verifyTaskFactory.notifyEnd();
        
        System.out.println(verifyTaskFactory.reportTotals());
        
    }

    /**
     * Loads the files using the {@link DataLoader}.
     * <p>
     * Note: the parsing of files does NOT proceed in parallel with index writes
     * which limits the throughput of this loader. The scale-out bulk loader
     * does not have that limitation.
     * 
     * @param dataDir
     *            The directory containing the files.
     */
    private void loadSingleThreaded(final File dataDir) {

        System.out.println("Will load files (singleThread): dataDir=" + dataDir);

        final long statementCountBefore = sail.getDatabase().getStatementCount();

        final long termCountBefore = sail.getDatabase().getTermCount();

        final long begin = System.currentTimeMillis();

        final DataLoader dataLoader = sail.getDatabase().getDataLoader();

        loadOntology();
        
        // load data files (loads subdirs too).
        try {

            final LoadStats loadStats = dataLoader.loadFiles(dataDir,
                    null/* baseURI */, null/* rdfFormat */, filter);
            
//            System.out.println(loadStats);

        } catch (IOException ex) {
            
            throw new RuntimeException("Problem loading file(s): " + ex, ex);
            
        }

        final long elapsed1 = System.currentTimeMillis() - begin;
        
        final long statementCountAfter = sail.getDatabase().getStatementCount();
        
        final long termCountAfter = sail.getDatabase().getTermCount();

        final long statementsLoaded = statementCountAfter
                - statementCountBefore;

        final long termsLoaded = termCountAfter - termCountBefore;
        
        final long statementsPerSecond = (long) (statementsLoaded * 1000d / elapsed1);

        System.out.println("Loaded data files: loadTime(ms)=" + elapsed1
                + ", loadRate(tps)=" + statementsPerSecond + ", toldTriples="
                + statementsLoaded+", #terms="+termsLoaded);

    }

    /**
     * Loads just the ontology file.
     */
    protected void loadOntology() {

        final long begin = System.currentTimeMillis();
        
        final DataLoader dataLoader = sail.getDatabase().getDataLoader();
        
        // load the ontology.
        try {

            final String filename = ontology;

            System.out.print("Loading ontology: " + ontology + "...");

            final String baseURI = new File(filename).toURI().toString();
            
            final RDFFormat rdfFormat = RDFFormat.forFileName(filename);

            if (rdfFormat == null) {

                throw new RuntimeException("Could not identify RDF format: "
                        + filename);
                
            }
            
            dataLoader.loadData(filename, baseURI, rdfFormat);

            System.out.println("done.");
            
        } catch (Throwable ex) {

            final long elapsed = System.currentTimeMillis() - begin;

            throw new RuntimeException("Exception loading ontology: " + ex
                    + " after " + elapsed + "ms", ex);
            
        }

    }
    
    /*
     * Query stuff.
     */

    /**
     * Return a read-historical connection for query.
     */
    protected BigdataSailConnection getQueryConnection() {

        final long timestamp = sail.getDatabase().getIndexManager()
                .getLastCommitTime();
        
        return sail.getReadHistoricalView(timestamp);
        
    }
    
    /**
     * Accepts SPARQL queries.
     */
    public QueryResult issueQuery(final Query theQuery) {

        /*
         * Note: These are the properties that are currently configured for the
         * tripleStore.
         */
        final Properties properties = sail.getDatabase().getProperties();
        
        /*
         * When <code>true</code> inferences will be included in the results
         * for high-level query.
         */
        final boolean includeInferred = Boolean.parseBoolean(properties
                .getProperty(Options.INCLUDE_INFERRED,
                        Options.DEFAULT_INCLUDE_INFERRED));

        if (log.isInfoEnabled())
            log.info(Options.INCLUDE_INFERRED + "=" + includeInferred);
        
        try {
            
            final String queryString = theQuery.getString();
            
            if (log.isDebugEnabled())
                log.debug("query: " + queryString);

            final String baseURI = null;// @todo what default?
            
            final ParsedQuery query = engine.parseQuery(queryString, baseURI);

            logQuery(queryString, query);

            /*
             * Create a data set consisting of the contexts to be queried.
             * 
             * Note: a [null] DataSet will cause context to be ignored when the
             * query is processed.
             */
            final DatasetImpl dataSet = null; //new DatasetImpl();
            
            final BindingSet bindingSet = new QueryBindingSet();

            final BigdataSailConnection conn = getQueryConnection();

            // Note: Will close the [conn] for all outcomes.
            return new MyQueryResult(conn, conn.evaluate(query.getTupleExpr(),
                    dataSet, bindingSet, includeInferred ));
        
        } catch (Throwable e) {
         
            /*
             * Log an error and return an empty result since LUBM does not
             * handle exceptions.
             */
            
            log.error("Query failed: " + e + ", query=" + theQuery, e);

            /*
             * An empty result (the result is a large negative value so that we
             * will notice errors right away in the summary).
             */
            return new QueryResult() {

                public long getNum() {
                    return -100000L;
                }

                public boolean next() {
                    return false;
                }
                
            };
            
//            throw new RuntimeException(e);
            
        }
        
    }

    /**
     * Set of distinct queries issued. This is used to reduce the detailed
     * logging of the query being run.
     */
    private HashSet<String> queries = new HashSet<String>();
    
    private void logQuery(String queryString, ParsedQuery query) {

        if (queries.contains(queryString))
            return;

        queries.add(queryString);

//        System.err.println("parsed: " + query);

    }
    
    /**
     * Counts the #of results from the query and closes the
     * {@link SailConfigException}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MyQueryResult implements QueryResult {

        private BigdataSailConnection conn;

        private CloseableIteration<? extends BindingSet, QueryEvaluationException> itr;

        private long nsolutions = 0;

        public MyQueryResult(
                BigdataSailConnection conn,
                CloseableIteration<? extends BindingSet, QueryEvaluationException> itr) {

            this.conn = conn;

            this.itr = itr;

        }

        protected void finalized() throws Throwable {

            close();

        }

        private void close() {

            if (itr != null) {

                try {
                    
                    itr.close();
                    
                    itr = null;
                    
                } catch (QueryEvaluationException e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }

            if (false) {
                /*
                 * @todo disable - this shows the effective bloom filter error
                 * rate to date (or at least since the index object was last
                 * read from the store).
                 * 
                 * Note: The repository is re-opened before each set of
                 * presentations of a given query, so the false positive error
                 * rate climbs only during a given set of presentations of the
                 * same query.
                 * 
                 * Note: This does not report anything for the scale-out
                 * federations since the joins are performed using the index
                 * partitions not the scale-out indices.
                 */
                final IIndex ndx = conn.getTripleStore().getSPORelation()
                        .getPrimaryIndex();

                if (ndx instanceof ILocalBTreeView) {

                    /*
                     * Note: For a fused view, the false positives are reported
                     * to the mutable btree.
                     */
                    final BTree btree = ((ILocalBTreeView) ndx)
                            .getMutableBTree();

                    final BloomFilter filter = btree.getBloomFilter();
                    
                    if (filter != null)
                        System.err.println(filter.counters
                                .getBloomFilterPerformance());

                }

            }
            
            /*
             * FIXME test on isReadOnly() is a workaround for the sail which
             * does not hand us a distinct BigdataSailConnection for each
             * read-historical or each read-committed view.
             */
            if (conn != null && !conn.getTripleStore().isReadOnly()) {

                try {

                    conn.close();

                } catch (SailException e) {
                    
                    throw new RuntimeException(e);
                    
                } finally {
                    
                    conn = null;
                    
                }

            }
            
        }
        
        public long getNum() {

            return nsolutions;

        }

        public boolean next() {

            if (itr == null)
                throw new IllegalStateException();
            
            if (conn == null)
                throw new IllegalStateException();
            
            try {

                if (!itr.hasNext()) {

                    close();

                    return false;

                }

                final BindingSet bindingSet = itr.next();
                
                // dumps the binding set - uncomment to verify the solutions.
//                System.err.println(bindingSet.toString());

                nsolutions++;

                return true;

            } catch (QueryEvaluationException e) {

                close();

                throw new RuntimeException(e);

            }

        }

    }

}
