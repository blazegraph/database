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
package com.bigdata.rdf.load;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.ITx;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.TaskMaster;

/**
 * Distributed bulk loader for RDF data. Creates/(re-)opens the
 * {@link AbstractTripleStore}, loads the optional ontology, and starts the
 * clients. The clients will run until the master is canceled loading any data
 * found in the {@link JobState#dataDir}. Files are optionally deleted after
 * they have been successfully loaded. Closure may be optionally computed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Support loading files from URLs, BFS, etc. This can be achieved via
 *       subclassing and overriding {@link #newClientTask(int)} and
 *       {@link #newJobState(String, Configuration)} as necessary.
 */
public class RDFDataLoadMaster<S extends RDFDataLoadMaster.JobState, T extends Callable<U>, U>
        extends TaskMaster<S, T, U> {

    final protected static Logger log = Logger
            .getLogger(RDFDataLoadMaster.class);

    /**
     * {@link Configuration} options for the {@link RDFDataLoadMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions extends TaskMaster.ConfigurationOptions {

        /**
         * The KB namespace.
         */
        String NAMESPACE = "namespace";

        /**
         * The directory from which the data will be read.
         */
        String DATA_DIR = "dataDir";

        /**
         * Only files matched by the filter will be loaded from the
         * {@link #DATA_DIR} (optional, but must be {@link Serializable} if
         * given).
         */
        String DATA_DIR_FILTER = "dataDirFilter";
        
        /**
         * A file or directory whose data will be loaded into the KB when it is
         * created. If it is a directory, then all data in that directory will
         * be loaded. Unlike the distributed bulk load, the file or directory
         * MUST be readable by the master and the data in this file and/or
         * directory are NOT deleted after they have been loaded.
         * <p>
         * Note: This is intended for the one-time load of ontologies pertaining
         * to the data to be loaded. If you need to do additional non-bulk data
         * loads you can always use the {@link com.bigdata.rdf.sail.BigdataSail}.
         */
        String ONTOLOGY = "ontology";

        /**
         * The core pool size for the thread pool running the parser tasks.
         */
        String PARSER_POOL_SIZE = "parserPoolSize";

        /**
         * The capacity of the work queue for the thread pool running the parser
         * tasks.
         */
        String PARSER_QUEUE_CAPACITY = "parserQueueCapacity";

        /**
         * The delay in milliseconds between resubmits of a task when the queue
         * of tasks awaiting execution is at capacity.
         */
        String REJECTED_EXECUTION_DELAY = "rejectedExecutionDelay";

        /** {@value #DEFAULT_REJECTED_EXECUTION_DELAY}ms */
        long DEFAULT_REJECTED_EXECUTION_DELAY = 250;
        
        /**
         * The #of threads used to buffer asynchronous writes for the TERM2ID
         * index.
         */
        String TERM2ID_WRITER_POOL_SIZE = "term2IdWriterPoolSize";

        int DEFAULT_TERM2ID_WRITER_POOL_SIZE = 5;

        /**
         * The #of threads used to buffer asynchronous writes for the other
         * indices.
         */
        String OTHER_WRITER_POOL_SIZE = "otherWriterPoolSize";

        int DEFAULT_OTHER_WRITER_POOL_SIZE = 5;

        /**
         * The #of threads used to delete files once they have been successfully
         * processed.
         */
        String NOTIFY_POOL_SIZE = "notifyPoolSize";

        int DEFAULT_NOTIFY_POOL_SIZE = 5;

        /**
         * The maximum #of statements which can be parsed but not yet buffered
         * on for asynchronous index writes before new parser tasks will be
         * paused. This is used to control the RAM demand of the parser tasks.
         * The RAM demand of the buffered index writes in controlled by the
         * capacity and chunk size for the asynchronous index write buffers.
         */
        String UNBUFFERED_STATEMENT_THRESHOLD = "unbufferedStatementThreshold";

        long DEFAULT_UNBUFFERED_STATEMENT_THRESHOLD = Bytes.megabyte * 1;

//        /**
//         * The buffer capacity for parsed RDF statements (not used when
//         * {@link #ASYNCHRONOUS_WRITES} are enabled). 3x this gives the initial
//         * capacity of the RDF {@link Value}s hash map.
//         */
//        String STATEMENT_BUFFER_CAPACITY = "statementBufferCapacity";

//        /**
//         * When <code>true</code> the asynchronous index write API will be
//         * used.
//         */
//        String ASYNCHRONOUS_WRITES = "asynchronousWrites";
        
        /**
         * When terms and values are parsed from a document then are aggregated
         * into chunks of this size before they are written onto the master for
         * the asynchronous write API (10k to 20k should be fine).
         */
        String PRODUCER_CHUNK_SIZE = "producerChunkSize";

//        /**
//         * When <code>true</code> synchronous RPC is used for writes on the
//         * TERM2ID index. When <code>false</code> asynchronous writes are used
//         * on that index.  Asynchronous writes have much better throughput.
//         */
//        String SYNC_RPC_FOR_TERM2ID = "syncRPCForTERM2ID";
        
        /**
         * The initial capacity of the hash map used to store RDF {@link Value}s
         * when processing a document (asynchronous writes only).
         */
        String VALUES_INITIAL_CAPACITY = "valuesInitialCapacity";
        
        /**
         * The initial capacity of the hash map used to store RDF {@link Value}s
         * when processing a document (asynchronous writes only).
         */
        String BNODES_INITIAL_CAPACITY = "bnodesInitialCapacity";
        
        /**
         * When <code>true</code>, the master will create the
         * {@link ITripleStore} identified by {@link #NAMESPACE } if it does not
         * exist.
         */
        String CREATE = "create";

        /**
         * When <code>true</code>, the data files will be loaded. This can be
         * disabled if you just want to compute the closure of the database.
         */
        String LOAD_DATA = "loadData";

        /**
         * When <code>true</code>, the closure of the data set will be
         * computed.
         * 
         * @see com.bigdata.rdf.sail.BigdataSail.Options#TRUTH_MAINTENANCE
         * 
         * @todo Note that the closure will be computed ANYWAY if the
         *       {@link com.bigdata.rdf.sail.BigdataSail} is configured for incremental truth
         *       maintenance. (Create w/o incremental TM).
         * 
         * @todo Change to an enum type with support for justification chains,
         *       recomputing closure without magic sets or justification chains,
         *       computing the incremental closure, etc. Make sure that the bulk
         *       delete of inferences for recomputing closure is efficient
         *       (range iterator with filter for inferences and delete flag).
         */
        String COMPUTE_CLOSURE = "computeClosure";

        /**
         * When <code>true</code>, the data files will be deleted as they are
         * consumed. (They will only be deleted once the data from the file are
         * known to be restart safe in the {@link ITripleStore}.)
         */
        String DELETE_AFTER = "deleteAfter";

        /**
         * When <code>true</code>, an overflow with a compacting merge will
         * be requested for each data service before we compute the database at
         * once closure. This can save effort because we will need to scan large
         * key-ranges in the database for some rules in order to compute the
         * closure, and where the rule is embedded in a fixed point program, we
         * will need to scan those key-ranges more than once. Also, the overflow
         * operation is full distributed so it does not add all that much
         * latency while the closure operation has less concurrency.
         */
        String FORCE_OVERFLOW_BEFORE_CLOSURE = "forceOverflowBeforeClosure";
        
        /**
         * When <code>true</code> a validating parsed will be used.
         */
        String PARSER_VALIDATES = "parserValidates";
        
        boolean DEFAULT_PARSER_VALIDATES = false;

//        /**
//         * The maximum #of times an attempt will be made to load any given file.
//         */
//        String MAX_TRIES = "maxTries";
//
//        /** {@value #DEFAULT_MAX_TRIES} */
//        int DEFAULT_MAX_TRIES = 3;
        
    }

    /**
     * The job description for an {@link RDFDataLoadMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class JobState extends TaskMaster.JobState {

        /**
         * 
         */
        private static final long serialVersionUID = -7097810235721797668L;

        /**
         * The namespace of the {@link ITripleStore} into which the data will be
         * loaded (must exist).
         */
        public final String namespace;

        /**
         * The directory from which files will be read.
         * 
         * @see ConfigurationOptions#DATA_DIR
         */
        public final File dataDir;

        /**
         * Only files matched by the filter in the {@link #dataDir} will be
         * processed (optional, but must be {@link Serializable}).
         * 
         * @see ConfigurationOptions#DATA_DIR_FILTER
         */
        public final FilenameFilter dataDirFilter;
        
        /**
         * The file or directory from which files will be loaded when the
         * {@link ITripleStore} is first created.
         * 
         * @see ConfigurationOptions#ONTOLOGY
         */
        public final File ontology;

        /**
         * @see ConfigurationOptions#PARSER_POOL_SIZE
         */
        public final int parserPoolSize;

        /**
         * @see ConfigurationOptions#PARSER_QUEUE_CAPACITY
         */
        final public int parserQueueCapacity;

        /**
         * @see ConfigurationOptions#REJECTED_EXECUTION_DELAY
         */
        final public long rejectedExecutionDelay;
        
        /**
         * @see ConfigurationOptions#TERM2ID_WRITER_POOL_SIZE
         */
        public final int term2IdWriterPoolSize;

        /**
         * @see ConfigurationOptions#OTHER_WRITER_POOL_SIZE
         */
        public final int otherWriterPoolSize;

        /**
         * @see ConfigurationOptions#NOTIFY_POOL_SIZE
         */
        public final int notifyPoolSize;

        /**
         * @see ConfigurationOptions#UNBUFFERED_STATEMENT_THRESHOLD
         */
        public final long unbufferedStatementThreshold;
        
        /**
         * @see ConfigurationOptions#PRODUCER_CHUNK_SIZE
         */
        public final int producerChunkSize;

        /**
         * @see ConfigurationOptions#VALUES_INITIAL_CAPACITY
         */
        public final int valuesInitialCapacity;

        /**
         * @see ConfigurationOptions#BNODES_INITIAL_CAPACITY
         */
        public final int bnodesInitialCapacity;
        
        /**
         * When <code>true</code>, the master will create the
         * {@link ITripleStore} identified by {@link #namespace} if it does not
         * exist.
         */
        public final boolean create;

        /**
         * When <code>true</code>, the clients will load data.
         */
        public final boolean loadData;

        /**
         * When <code>true</code>, the closure of the data set will be
         * computed once all data have been loaded.
         * 
         * @see ConfigurationOptions#COMPUTE_CLOSURE
         */
        public final boolean computeClosure;

        /**
         * @see ConfigurationOptions#FORCE_OVERFLOW_BEFORE_CLOSURE
         */
        final boolean forceOverflowBeforeClosure;
        
        /**
         * When <code>true</code>, the each data file will be deleted once
         * its data has been loaded into the {@link ITripleStore}.
         */
        public final boolean deleteAfter;
                
        /**
         * When <code>true</code> a validating parsed will be used.
         * 
         * @see ConfigurationOptions#PARSER_VALIDATES
         */
        final public boolean parserValidates;
        
        /**
         * Default format assumed when file ext is unknown.
         * 
         * @todo configure the filter. Since {@link RDFFormat} is not
         *       serializable we will have to specify the filter class and
         *       create an instance on the target machine.
         */
        final static transient public RDFFormat fallback = RDFFormat.RDFXML;

        @Override
        protected void toString(StringBuilder sb) {
        
            super.toString(sb);
            
            sb.append(", " + ConfigurationOptions.NAMESPACE + "="
                    + namespace);
            
            sb.append(", " + ConfigurationOptions.DATA_DIR+ "="
                    + dataDir);
        
            sb.append(", " + ConfigurationOptions.DATA_DIR_FILTER + "="
                    + dataDirFilter);
        
            sb.append(", " + ConfigurationOptions.PARSER_POOL_SIZE + "="
                    + parserPoolSize);
            
            sb.append(", " + ConfigurationOptions.PARSER_QUEUE_CAPACITY + "="
                    + parserQueueCapacity);

            sb.append(", " + ConfigurationOptions.REJECTED_EXECUTION_DELAY + "="
                        + rejectedExecutionDelay);

            // @todo term2IdWriterPoolSize, etc.
            
            sb.append(", " + ConfigurationOptions.PRODUCER_CHUNK_SIZE+ "="
                    + producerChunkSize);

            sb.append(", " + ConfigurationOptions.VALUES_INITIAL_CAPACITY + "="
                    + valuesInitialCapacity);

            sb.append(", " + ConfigurationOptions.BNODES_INITIAL_CAPACITY + "="
                    + bnodesInitialCapacity);

            sb.append(", " + ConfigurationOptions.CREATE + "=" + create);
            
            sb.append(", " + ConfigurationOptions.LOAD_DATA + "=" + loadData);
            
            sb.append(", " + ConfigurationOptions.COMPUTE_CLOSURE + "="
                    + computeClosure);
          
            sb.append(", " + ConfigurationOptions.PARSER_VALIDATES + "="
                    + parserValidates);
            
            sb.append(", " + ConfigurationOptions.DELETE_AFTER + "="
                    + deleteAfter);

            sb.append(", " + ConfigurationOptions.FORCE_OVERFLOW_BEFORE_CLOSURE + "="
                    + forceOverflowBeforeClosure);

            // @todo more fields in the job state.

        }

        /**
         * {@inheritDoc}
         */
        public JobState(final String component, final Configuration config)
                throws ConfigurationException {

            super(component, config);
            
            namespace = (String) config.getEntry(component,
                    ConfigurationOptions.NAMESPACE, String.class);

            dataDir = (File) config.getEntry(component,
                    ConfigurationOptions.DATA_DIR, File.class);

            dataDirFilter = (FilenameFilter) config.getEntry(component,
                    ConfigurationOptions.DATA_DIR_FILTER, FilenameFilter.class,
                    null/* default */);

            ontology = (File) config
                    .getEntry(component, ConfigurationOptions.ONTOLOGY,
                            File.class, null/* defaultValue */);

            parserPoolSize = (Integer) config.getEntry(component,
                    ConfigurationOptions.PARSER_POOL_SIZE, Integer.TYPE);

            parserQueueCapacity = (Integer) config.getEntry(component,
                    ConfigurationOptions.PARSER_QUEUE_CAPACITY, Integer.TYPE);

            term2IdWriterPoolSize = (Integer) config.getEntry(component,
                    ConfigurationOptions.TERM2ID_WRITER_POOL_SIZE,
                    Integer.TYPE,
                    ConfigurationOptions.DEFAULT_TERM2ID_WRITER_POOL_SIZE);

            otherWriterPoolSize = (Integer) config.getEntry(component,
                    ConfigurationOptions.OTHER_WRITER_POOL_SIZE, Integer.TYPE,
                    ConfigurationOptions.DEFAULT_OTHER_WRITER_POOL_SIZE);

            notifyPoolSize = (Integer) config.getEntry(component,
                    ConfigurationOptions.NOTIFY_POOL_SIZE, Integer.TYPE,
                    ConfigurationOptions.DEFAULT_NOTIFY_POOL_SIZE);

            unbufferedStatementThreshold = (Long) config.getEntry(component,
                    ConfigurationOptions.UNBUFFERED_STATEMENT_THRESHOLD,
                    Long.TYPE,
                    ConfigurationOptions.DEFAULT_UNBUFFERED_STATEMENT_THRESHOLD);

            producerChunkSize = (Integer) config
                    .getEntry(
                            component,
                            ConfigurationOptions.PRODUCER_CHUNK_SIZE,
                            Integer.TYPE);

            valuesInitialCapacity = (Integer) config.getEntry(component,
                    ConfigurationOptions.VALUES_INITIAL_CAPACITY, Integer.TYPE);

            bnodesInitialCapacity = (Integer) config.getEntry(component,
                    ConfigurationOptions.BNODES_INITIAL_CAPACITY, Integer.TYPE);

            create = (Boolean) config.getEntry(component,
                    ConfigurationOptions.CREATE, Boolean.TYPE);

            loadData = (Boolean) config.getEntry(
                    component,
                    ConfigurationOptions.LOAD_DATA, Boolean.TYPE);

            computeClosure = (Boolean) config.getEntry(
                    component,
                    ConfigurationOptions.COMPUTE_CLOSURE, Boolean.TYPE);

            deleteAfter = (Boolean) config.getEntry(
                    component,
                    ConfigurationOptions.DELETE_AFTER, Boolean.TYPE);

            forceOverflowBeforeClosure = (Boolean) config.getEntry(component,
                    ConfigurationOptions.FORCE_OVERFLOW_BEFORE_CLOSURE,
                    Boolean.TYPE);

            parserValidates = (Boolean) config.getEntry(
                    component,
                    ConfigurationOptions.FORCE_OVERFLOW, Boolean.TYPE,
                    ConfigurationOptions.DEFAULT_PARSER_VALIDATES);

            rejectedExecutionDelay = (Long) config.getEntry(
                    component,
                    ConfigurationOptions.REJECTED_EXECUTION_DELAY, Long.TYPE,
                    ConfigurationOptions.DEFAULT_REJECTED_EXECUTION_DELAY);
            
//            maxTries = (Integer) config.getEntry(
//                    component,
//                    ConfigurationOptions.MAX_TRIES, Integer.TYPE,
//                    ConfigurationOptions.DEFAULT_MAX_TRIES);
            
        }

    }

    /**
     * Runs the master. SIGTERM (normal kill or ^C) will cancel the job,
     * including any running clients. Use <code>-Dbigdata.component</code> to
     * override the configuration component name.
     * 
     * @param args
     *            The {@link Configuration} and any overrides.
     * 
     * @throws ConfigurationException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws KeeperException
     */
    static public void main(final String[] args) throws ConfigurationException,
            ExecutionException, InterruptedException, KeeperException {

        final JiniFederation<?> fed = new JiniClient(args).connect();

        try {
        
            final TaskMaster task = new RDFDataLoadMaster(fed);

            // execute master wait for it to finish.
            task.execute();

        } finally {

            fed.shutdown();
            
        }
        
    }
    
    public RDFDataLoadMaster(final JiniFederation<?> fed)
            throws ConfigurationException {

        super(fed);
    
    }

    /**
     * Extended to support optional load, closure, and reporting.
     */
    protected void runJob() throws Exception {

        final S jobState = getJobState();

        final long begin = System.currentTimeMillis();

        final AbstractTripleStore tripleStore = openTripleStore();

        // @todo this will include any pre-loaded ontology and axioms
        // (non-zero). @todo exact?
        final long statementCount0 = tripleStore.getStatementCount(); 

        if (jobState.loadData) {

            /*
             * Start the producer and consumer tasks.
             * 
             * Note: The producer and consumer tasks are paired. The tasks in
             * each pair run on the SAME data service.
             */

            super.runJob();

            /*
             * The data generator aspect of the job is finished.
             */

            final long elapsed = System.currentTimeMillis() - begin;

            /*
             * Report tps for distributed data load.
             */

            final long statementCount = tripleStore.getStatementCount(); // @todo exact?

            final long statementsAdded = statementCount - statementCount0;

            final long tps = ((long) (((double) statementsAdded)
                    / ((double) elapsed) * 1000d));

            System.out.println("Load: tps=" + tps + ", ntriples="
                    + statementCount + ", nnew=" + statementsAdded
                    + ", elapsed=" + elapsed + "ms");

            /*
             * @todo consider commenting this out but SHOULD be safe w/
             * read-committed or read-historical reads.
             */ 
            System.out.println(getKBInfo(tripleStore));

        }
        
        if (jobState.computeClosure) {

            /*
             * Compute database-at-once closure.
             * 
             * @todo Given the long running nature of closure over a large data
             * set, clients could write the set of rules that have reached fixed
             * point (stages in the program) onto the lock node. That way if the
             * client computing closure fails, the operation can failover to the
             * next client which gains the lock.
             * 
             * @todo Also, without some persistent record of the closure
             * operation state if there are two masters running then the 2nd
             * master will begin the closure operation _again_ as soon as the
             * first one completes!
             * 
             * @todo We should probably put the zlock for this into
             * inf.computeClosure() itself together with the state updates for
             * the closure job. That is going to be the only way to put a lock
             * on things that will prevent anyone else from running closure.
             * 
             * @todo We also want to prevent concurrent writes on the triple
             * store while computing the closure or the wrong fixed point could
             * be computed. This means that other writes need to be buffered
             * elsewhere until the closure is complete, which is really the
             * workflow level for bigdata triple store updates.
             */
            final IResourceLock lock = fed.getResourceLockService()
                    .acquireLock(tripleStore.getNamespace());

            if (jobState.forceOverflowBeforeClosure) {

                /*
                 * Force overflow before computing the closure since we will
                 * perform full range scans on several predicates, some range
                 * scans on all statements, and some of these things we will do
                 * more than once if the triple pattern occurs within a fixed
                 * point enclosure.
                 */

                forceOverflow();

                System.out.println(getKBInfo(tripleStore));

            }

            try {

                final long statementCount1 = tripleStore.getStatementCount(); // @todo exact?
                
                final long beginClosure = System.currentTimeMillis();
                
                // show the current time when we start.
                System.out.println("Computing closure: now="
                        + new Date().toString());

                final InferenceEngine inf = tripleStore.getInferenceEngine();

                // database at once closure.
                final ClosureStats closureStats = inf
                        .computeClosure(null/* focusStore */);

                System.out.println("closure: " + closureStats);

                final long elapsed = System.currentTimeMillis() - beginClosure;

                final long statementCount = tripleStore.getStatementCount(); // @todo exact?
                
                final long statementsAdded = statementCount - statementCount1;

                final long tps = ((long) (((double) statementsAdded)
                        / ((double) elapsed) * 1000d));

                System.out.println("Closure: tps=" + tps + ", ntriples="
                        + statementCount + ", nnew=" + statementsAdded
                        + ", elapsed=" + elapsed + "ms");

                System.out.println(getKBInfo(tripleStore));
                
            } finally {

                lock.unlock();
                
            }

        }

        if (jobState.loadData
                && jobState.computeClosure) {

            /*
             * Report total tps throughput for load+closure.
             */

            final long elapsed = System.currentTimeMillis() - begin;
            
            final long statementCount = tripleStore.getStatementCount(); // @todo exact?
            
            final long statementsAdded = statementCount - statementCount0;

            final long tps = ((long) (((double) statementsAdded)
                    / ((double) elapsed) * 1000d));

            System.out.println("Net: tps=" + tps + ", ntriples="
                    + statementCount + ", nnew=" + statementsAdded
                    + ", elapsed=" + elapsed + "ms");

        }
        
        if (jobState.forceOverflow) {

            System.out.println("Forcing overflow: now=" + new Date());

            fed.forceOverflow(true/* truncateJournal */);

            System.out.println("Forced overflow: now=" + new Date());

            System.out.println(getKBInfo(tripleStore));

        }

    }

    /**
     * Return various interesting metadata about the KB state.
     */
    protected StringBuilder getKBInfo(final AbstractTripleStore tripleStore) {
        
        final StringBuilder sb = new StringBuilder();

//      if(true) return sb;// comment out to get detailed info.

        try {

            sb.append("namespace\t" + tripleStore.getNamespace() + "\n");

            sb.append("class\t" + tripleStore.getClass().getName() + "\n");

            sb
                    .append("indexManager\t"
                            + tripleStore.getIndexManager().getClass()
                                    .getName() + "\n");
            
            sb.append("statementCount\t" + tripleStore.getStatementCount()
                    + "\n");

            sb.append("termCount\t" + tripleStore.getTermCount() + "\n");

            sb.append("uriCount\t" + tripleStore.getURICount() + "\n");

            sb.append("literalCount\t" + tripleStore.getLiteralCount() + "\n");

            sb.append("bnodeCount\t" + tripleStore.getBNodeCount() + "\n");

            // sb.append(tripleStore.predicateUsage());

        } catch (Throwable t) {

            log.warn(t.getMessage(), t);

        }
        
        return sb;
        
    }

    /**
     * Extended to open/create the KB.
     */
    protected void beginJob(final S jobState) throws Exception {

        super.beginJob(jobState);

        // create/re-open the kb.
        openTripleStore();
        
    }
    
    /**
     * Create/re-open the repository.
     */
    public AbstractTripleStore openTripleStore() throws ConfigurationException {

        AbstractTripleStore tripleStore;

        final JobState jobState = getJobState();
        
        // locate the resource declaration (aka "open").
        tripleStore = (AbstractTripleStore) fed.getResourceLocator().locate(
                jobState.namespace, ITx.UNISOLATED);

        if (tripleStore == null) {

            /*
             * Does not exist.
             */

            if (!jobState.create) {

                throw new RuntimeException("Does not exist: "
                        + jobState.namespace);

            }

            // create kb.
            tripleStore = createTripleStore();

            showProperties(tripleStore);

            // load any one time files.
            try {

                loadOntology(tripleStore);

            } catch (Exception ex) {

                throw new RuntimeException("Could not load: "
                        + jobState.ontology, ex);

            }

        } else {

            if (log.isInfoEnabled())
                log.info("Re-opened tripleStore: " + jobState.namespace);

            showProperties(tripleStore);
            
        }

        return tripleStore;

    }

    /**
     * Create the {@link AbstractTripleStore} specified by
     * {@link ConfigurationOptions#NAMESPACE} using the <code>properties</code>
     * associated with the {@link TaskMaster.JobState#component}.
     * 
     * @return The {@link AbstractTripleStore}
     */
    protected AbstractTripleStore createTripleStore() throws ConfigurationException {

        final JobState jobState = getJobState();
        
        if (log.isInfoEnabled())
            log.info("Creating tripleStore: " + jobState.namespace);

        /*
         * Pick up properties configured for the client as defaults.
         * 
         * You can specify those properties using NV[] for the component that is
         * executing the master.
         */
        final Properties properties = fed.getClient().getProperties(
                jobState.component);
        
        final AbstractTripleStore tripleStore = new ScaleOutTripleStore(fed,
                jobState.namespace, ITx.UNISOLATED, properties);

        // create the triple store.
        tripleStore.create();

        // show #of axioms.
        System.out.println("axiomCount=" + tripleStore.getStatementCount());
        
        if (log.isInfoEnabled())
            log.info("Created tripleStore: " + jobState.namespace);

        return tripleStore;

    }

    /**
     * Loads the file or directory specified by
     * {@link ConfigurationOptions#ONTOLOGY} into the {@link ITripleStore}
     * 
     * @throws IOException
     */
    protected void loadOntology(final AbstractTripleStore tripleStore)
            throws IOException {

        final JobState jobState = getJobState();
        
        if (log.isInfoEnabled())
            log.info("Loading ontology: " + jobState.ontology);

        tripleStore.getDataLoader().loadFiles(jobState.ontology/* file */,
                jobState.ontology.getPath()/* baseURI */,
                JobState.fallback/* rdfFormat */, new RDFFilenameFilter());

        System.out.println("axiomAndOntologyCount="
                + tripleStore.getStatementCount());
        
        if (log.isInfoEnabled())
            log.info("Loaded ontology: " + jobState.ontology);

    }

    /**
     * Dump some properties of interest.
     */
    public void showProperties(final AbstractTripleStore tripleStore) {

        if (!log.isInfoEnabled()) return;

        log.info("tripleStore: namespace=" + tripleStore.getNamespace());
        
        final Properties p = tripleStore.getProperties();

        log.info(Options.TERMID_BITS_TO_REVERSE + "="
                + p.getProperty(Options.TERMID_BITS_TO_REVERSE));

        // log.info(Options.INCLUDE_INFERRED + "="
        // + p.getProperty(Options.INCLUDE_INFERRED));
        //                        
        // log.info(Options.QUERY_TIME_EXPANDER + "="
        // + p.getProperty(Options.QUERY_TIME_EXPANDER));

        log.info(Options.NESTED_SUBQUERY + "="
                + p.getProperty(Options.NESTED_SUBQUERY));

//        log.info(IndexMetadata.Options.BTREE_READ_RETENTION_QUEUE_CAPACITY + "=" + p .getProperty(IndexMetadata.Options.BTREE_READ_RETENTION_QUEUE_CAPACITY));

        log.info(Options.CHUNK_CAPACITY + "="
                + p.getProperty(Options.CHUNK_CAPACITY));

        log.info(Options.CHUNK_TIMEOUT
                + "="
                + p.getProperty(Options.CHUNK_TIMEOUT,
                        Options.DEFAULT_CHUNK_TIMEOUT));

        log.info(IBigdataClient.Options.CLIENT_RANGE_QUERY_CAPACITY
                        + "="
                        + p
                                .getProperty(
                                        IBigdataClient.Options.CLIENT_RANGE_QUERY_CAPACITY,
                                        IBigdataClient.Options.DEFAULT_CLIENT_RANGE_QUERY_CAPACITY));

        log.info(Options.FULLY_BUFFERED_READ_THRESHOLD
                + "="
                + p.getProperty(Options.FULLY_BUFFERED_READ_THRESHOLD,
                        Options.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD));

        log.info(Options.MAX_PARALLEL_SUBQUERIES + "="
                + p.getProperty(Options.MAX_PARALLEL_SUBQUERIES));

        // log.info(com.bigdata.rdf.sail.BigdataSail.Options.QUERY_TIME_EXPANDER + "="
        // + p.getProperty(com.bigdata.rdf.sail.BigdataSail.Options.QUERY_TIME_EXPANDER));

//        log.info("bloomFilterFactory="
//                + tripleStore.getSPORelation().getSPOIndex().getIndexMetadata()
//                        .getBloomFilterFactory());

    }

    /**
     * The default creates {@link RDFFileLoadTask} instances.
     */
    @Override
    protected T newClientTask(final int clientNum) {

        return (T)new RDFFileLoadTask(getJobState(), clientNum);
        
    }

    @Override
    protected S newJobState(final String component, final Configuration config)
            throws ConfigurationException {

        return (S) new JobState(component, config);
        
    }

}
