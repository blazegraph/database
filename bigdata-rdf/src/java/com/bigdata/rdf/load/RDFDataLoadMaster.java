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
 * Created on Jan 17, 2009
 */

package com.bigdata.rdf.load;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.openrdf.model.BNode;
import org.openrdf.rio.RDFFormat;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZooHelper;
import com.bigdata.zookeeper.ZooQueue;

/**
 * Distributed bulk loader for RDF data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo pull a lot of stuff into an abstract base class for masters.
 */
public class RDFDataLoadMaster implements Callable<Void> {

    final protected static Logger log = Logger
            .getLogger(RDFDataLoadMaster.class);

    final protected static boolean INFO = log.isInfoEnabled();

    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * {@link Configuration} options for the {@link RDFDataLoadMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions {

        /**
         * The component (namespace for the configuration options).
         */
        String COMPONENT = RDFDataLoadMaster.class.getName();

        /**
         * The job name is used to identify the job within zookeeper. A znode
         * with this name will be created as follows:
         * 
         * <pre>
         * zroot (of the federation)
         *    / jobs
         *      / RDFDataLoadMaster (fully qualified name of that class).
         *        / jobName
         * </pre>
         * 
         * Under that znode are the following.
         * 
         * <pre>
         *          / client# (where # is the client#).
         *            / locknode (used to elect the client that is running if there is contention).
         *            ...
         * </pre>
         */
        String JOB_NAME = "jobName";

        /**
         * The KB namespace.
         */
        String NAMESPACE = "namespace";

        /**
         * The directory from which the data will be read.
         */
        String DATA_DIR = "dataDir";

        /**
         * A file or directory whose data will be loaded into the KB when it is
         * created. If it is a directory, then all data in that directory will
         * be loaded. Unlike the distributed bulk load, the file or directory
         * MUST be readable by the master and the data in this file and/or
         * directory are NOT deleted after they have been loaded.
         * <p>
         * Note: This is intended for the one-time load of ontologies pertaining
         * to the data to be loaded. If you need to do additional non-bulk data
         * loads you can always use the {@link BigdataSail}.
         */
        String ONTOLOGY = "ontology";

        /**
         * #of clients to use.
         */
        String NCLIENTS = "nclients";

        /**
         * #of threads to use on each client.
         */
        String NTHREADS = "nthreads";

        /**
         * The buffer capacity for parsed RDF statements.
         */
        String BUFFER_CAPACITY = "bufferCapacity";

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
         * @todo Note that the closure will be computed ANYWAY if the
         *       {@link BigdataSail} is configured for incremental truth
         *       maintenance. (Create w/o incremental TM).
         * 
         * @see BigdataSail.Options#TRUTH_MAINTENANCE
         */
        String COMPUTE_CLOSURE = "computeClosure";

        /**
         * When <code>true</code>, the data files will be deleted as they are
         * consumed. (They will only be deleted once the data from the file are
         * known to be restart safe in the {@link ITripleStore}.)
         */
        String DELETE_AFTER = "deleteAfter";

        /**
         * When <code>true</code> as an after action on the job, the
         * {@link DataService}s in the federation will be made to undergo
         * asynchronous overflow processing and the live journals will be
         * truncated so that the total size on disk of the federation is at its
         * minimum footprint for the given history retention policy. The master
         * will block during this operation so you can readily tell when it is
         * finished. Note that this option only makes sense in benchmark
         * environments where you can contol the total system otherwise
         * asynchronous writes may continue.
         * 
         * @see AbstractScaleOutFederation#forceOverflow(boolean)
         */
        String FORCE_OVERFLOW = "forceOverflow";

        /**
         * When <code>true</code> the {@link StatementBuffer} is flushed after
         * each document and cleared on error. When <code>false</code> the
         * buffer is only flushed when it would overflow and the caller has
         * responsibility to make buffer is flushed before the total job
         * completes.
         * <p>
         * Note: The use of this option will blur the boundaries between one
         * document and the next and therefore can cause {@link BNode}
         * resolution to span document boundaries. However, the use of blank
         * nodes is already not "safe" (meaning idempotent) since each time a
         * given document is loaded the blank nodes in that document will be
         * recognized as distinct resources.
         * 
         * FIXME This option MAY NOT be turned off yet. There are two main
         * problems.
         * <p>
         * (1) A thread-local {@link IStatementBuffer} is used to bind the
         * buffer to the thread. This keeps down the #of buffers and makes it
         * possible to reuse the same buffer for each task run by a given
         * thread. However, to turn off {@link #AUTO_FLUSH} we would have to
         * identify and flush those thread-local buffers in
         * {@link ConcurrentDataLoader#awaitCompletion(long, java.util.concurrent.TimeUnit)}
         * or perhaps by signaling {@link ReaderTask} that it must flush the
         * buffer(s). Raising the buffer into the caller is a bit difficult as
         * we queue up tasks before they are executed so we can't really assign
         * the buffer to the task until it starts to execute.
         * <p>
         * (2) If {@link #AUTO_FLUSH} is turned off then {@link #DELETE_AFTER}
         * can delete a file before the {@link IStatementBuffer} containing its
         * assertions has been flushed. This weakens the "eventual success"
         * guarentee since the file will not be re-processed once it has been
         * deleted and a database error could occur after the file has been
         * parsed and deleted but before the statement buffer containing the
         * last of its assertions has been flushed to the database. To regain
         * that guarentee we can not deletes a file until the statement buffer
         * on which it wrote has been successfully flushed to the database.
         * <p>
         * (3) Also, the {@link PresortRioLoader}, the {@link DataLoader} and
         * various other things need to be sure that they do not reset, clear,
         * etc. the buffer on error or on success when autoFlush is turned off.
         * 
         * @see ConcurrentDataLoader#submitTask(String, ITaskFactory)
         * @see AbstractRDFTaskFactory#newTask(String)
         * @see ReaderTask#readData()
         */
        String AUTO_FLUSH = "autoFlush";

        boolean DEFAULT_AUTO_FLUSH = true;

        /**
         * When <code>true</code> a validating parsed will be used.
         */
        String PARSER_VALIDATES = "parserValidates";
        
        boolean DEFAULT_PARSER_VALIDATES = false;
        
        /**
         * The minimum #of {@link IDataService}s to discover before the master
         * will assign the clients to those services.
         */
        String MIN_DATA_SERVICES = "minDataServices";

        /**
         * The timeout in milliseconds to await the discovery of
         * {@link #MIN_DATA_SERVICES}.
         */
        String AWAIT_DATA_SERVICES_TIMEOUT = "awaitDataServicesTimeout";

        /**
         * The capacity of the queue of jobs awaiting execution.
         */
        String QUEUE_CAPACITY = "queueCapacity";
        
        /**
         * The delay in milliseconds between resubmits of a task when the queue
         * of tasks awaiting execution is at capacity.
         */
        String REJECTED_EXECUTION_DELAY = "rejectedExecutionDelay";

        /** {@value #DEFAULT_REJECTED_EXECUTION_DELAY}ms */
        long DEFAULT_REJECTED_EXECUTION_DELAY = 250;
        
        /**
         * The maximum #of times an attempt will be made to load any given file.
         */
        String MAX_TRIES = "maxTries";

        /** {@value #DEFAULT_MAX_TRIES} */
        int DEFAULT_MAX_TRIES = 3;
        
    }

    /**
     * The job description for an {@link RDFDataLoadMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class JobState implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -7097810235721797668L;

        /**
         * The job name.
         */
        final String jobName;

        /**
         * The namespace of the {@link ITripleStore} into which the data will be
         * loaded (must exist).
         */
        public final String namespace;

        /**
         * The directory from which files will be read.
         * 
         * @todo is this needed if we use a {@link ZooQueue} integration?
         */
        public final File dataDir;

        /**
         * The file or directory from which files will be loaded when the
         * {@link ITripleStore} is first created.
         * 
         * @see ConfigurationOptions#ONTOLOGY
         */
        public final File ontology;

        /**
         * The #of clients. Each client will be assigned to run on some
         * {@link IDataService}. Each client may run multiple threads.
         * <p>
         * Note: When loading data from the local file system a client MUST be
         * created on an {@link IDataService} on each host in order to read data
         * from its local file system. This is not an issue when reading from a
         * shared volume.
         */
        public final int nclients;

        /**
         * The #of concurrent threads to use to load the data. A single thread
         * will be used to scan the file system, but this many threads will be
         * used to read, parse, and write the data onto the {@link ITripleStore}.
         */
        public final int nthreads;

        /**
         * The capacity of the buffers used to hold the parsed RDF data.
         */
        public final int bufferCapacity;

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
         */
        public final boolean computeClosure;

        /**
         * When <code>true</code>, the each data file will be deleted once
         * its data has been loaded into the {@link ITripleStore}.
         */
        public final boolean deleteAfter;

        /**
         * When <code>true</code> the {@link DataService}s in the federation
         * will be made to undergo asynchronous overflow processing and the
         * live journals will be truncated so that the total size on disk of
         * the federation is at its minimum footprint for the given history
         * retention policy.
         * 
         * @see ConfigurationOptions#FORCE_OVERFLOW
         */
        public final boolean forceOverflow;
                
        /**
         * When <code>true</code> a validating parsed will be used.
         * 
         * @see ConfigurationOptions#PARSER_VALIDATES
         */
        final public boolean parserValidates;

        /**
         * @see ConfigurationOptions#QUEUE_CAPACITY
         */
        final public int queueCapacity;
        
        /**
         * @see ConfigurationOptions#REJECTED_EXECUTION_DELAY
         */
        final public long rejectedExecutionDelay;
        
        /**
         * @see ConfigurationOptions#MAXTRIES
         */
        final public int maxTries;
        
        /**
         * Default format assumed when file ext is unknown.
         * 
         * @todo configure the filter. Since {@link RDFFormat} is not
         *       serializable we will have to specify the filter class and
         *       create an instance on the target machine.
         */
        final static transient public RDFFormat fallback = RDFFormat.RDFXML;

        public String toString() {

            return getClass().getName()
                    + //
                    "{ " + ConfigurationOptions.JOB_NAME
                    + "="
                    + jobName
                    + //
                    ", " + ConfigurationOptions.NAMESPACE
                    + "="
                    + namespace
                    + //
                    ", " + ConfigurationOptions.DATA_DIR
                    + "="
                    + dataDir
                    + //
                    ", " + ConfigurationOptions.NCLIENTS
                    + "="
                    + nclients
                    + //
                    ", " + ConfigurationOptions.NTHREADS
                    + "="
                    + nthreads
                    + //
                    ", " + ConfigurationOptions.BUFFER_CAPACITY
                    + "="
                    + bufferCapacity
                    + //
                    ", " + ConfigurationOptions.CREATE + "="
                    + create
                    + //
                    ", " + ConfigurationOptions.LOAD_DATA + "="
                    + loadData
                    + //
                    ", " + ConfigurationOptions.COMPUTE_CLOSURE + "="
                    + computeClosure
                    + //
                    ", " + ConfigurationOptions.DELETE_AFTER + "="
                    + deleteAfter
                    + //
                    ", " + ConfigurationOptions.FORCE_OVERFLOW + "="
                    + forceOverflow + //
                    "}";

        }

        /**
         * @param configuration
         * @throws ConfigurationException
         */
        public JobState(final Configuration config)
                throws ConfigurationException {

            jobName = (String) config.getEntry(ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.JOB_NAME, String.class);

            namespace = (String) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.NAMESPACE, String.class);

            dataDir = (File) config.getEntry(ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.DATA_DIR, File.class);

            ontology = (File) config
                    .getEntry(ConfigurationOptions.COMPONENT,
                            ConfigurationOptions.ONTOLOGY, File.class, null/* defaultValue */);

            nclients = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.NCLIENTS, Integer.TYPE);

            nthreads = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.NTHREADS, Integer.TYPE);

            bufferCapacity = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.BUFFER_CAPACITY, Integer.TYPE);

            create = (Boolean) config.getEntry(ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.CREATE, Boolean.TYPE);

            loadData = (Boolean) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.LOAD_DATA, Boolean.TYPE);

            computeClosure = (Boolean) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.COMPUTE_CLOSURE, Boolean.TYPE);

            deleteAfter = (Boolean) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.DELETE_AFTER, Boolean.TYPE);

            forceOverflow = (Boolean) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.FORCE_OVERFLOW, Boolean.TYPE);

            parserValidates = (Boolean) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.FORCE_OVERFLOW, Boolean.TYPE,
                    ConfigurationOptions.DEFAULT_PARSER_VALIDATES);

            queueCapacity = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.QUEUE_CAPACITY, Integer.TYPE);
            
            rejectedExecutionDelay = (Long) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.REJECTED_EXECUTION_DELAY, Long.TYPE,
                    ConfigurationOptions.DEFAULT_REJECTED_EXECUTION_DELAY);
            
            maxTries = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.MAX_TRIES, Integer.TYPE,
                    ConfigurationOptions.DEFAULT_MAX_TRIES);
            
            client2DataService = new UUID[nclients];

        }

        /**
         * The mapping of clients onto the {@link DataService}s. The index is
         * the client#. The value is the {@link DataService} {@link UUID}.
         * 
         * @todo a stable mapping is only required if the files are being loaded
         *       from local directories vs NAS or some other "file" source.
         */
        final public UUID[] client2DataService;

        /**
         * Return the zpath of the node for all jobs which are instances of this class.
         */
        public String getJobClassZPath(final JiniFederation fed) {
            
            return fed.getZooConfig().zroot + "/" + BigdataZooDefs.JOBS + "/"
                    + RDFDataLoadMaster.class.getName();
            
        }
        
        /**
         * Return the zpath to the node which corresponds to the
         * {@link JobState}.
         */
        public String getJobZPath(final JiniFederation fed) {

            return getJobClassZPath(fed) + "/" + jobName;

        }

        /**
         * Return the zpath to the node which corresponds to the specified
         * client task.
         * 
         * @param clientNum
         *            The client number.
         */
        public String getClientZPath(final JiniFederation fed,
                final int clientNum) {

            return getJobZPath(fed) + "/" + "client" + clientNum;

        }

        /**
         * Return the zpath of the locknode for the specified client task. Any
         * tasks running with that clientNum MUST contend for a {@link ZLock}
         * which permits it to run the task. This prevents concurrent execution
         * of the task for the specified client in the event that more than one
         * master is running for the same {@link JobState}.
         * 
         * @param clientNum
         *            The client number.
         */
        public String getLockNodeZPath(final JiniFederation fed,
                final int clientNum) {

            return getClientZPath(fed, clientNum) + "/" + "locknode";

        }

    }

    protected final JiniFederation fed;

    protected final ZooKeeper zookeeper;

    public JobState jobState;

    /**
     * @see ConfigurationOptions#MIN_DATA_SERVICES
     */
    final int minDataServices;

    /**
     * @see ConfigurationOptions#AWAIT_DATA_SERVICES_TIMEOUT}
     */
    final long awaitDataServicesTimeout;

    /**
     * The zpath to the znode which corresponds to the job which is being
     * executed.
     */
    final String jobZPath;

    /**
     * Runs a bulk data load job. SIGTERM (normal kill or ^C) will cancel the
     * job, including any running clients.
     * 
     * @param args
     *            The {@link Configuration} and any overrides.
     * 
     * @throws ConfigurationException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void main(final String[] args) throws ConfigurationException,
            InterruptedException, ExecutionException {

        final JiniFederation fed = new JiniClient(args).connect();

        final Callable<Void> task = new RDFDataLoadMaster(fed);

        final Future<Void> future = fed.getExecutorService().submit(task);

        /*
         * Install a shutdown hook so that the master will cancel any running
         * clients if it is interrupted (normal kill will trigger this hook).
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {

                future.cancel(true/* mayInterruptIfRunning */);

            }

        });

        // wait for the master to finish.
        future.get();

    }

    public RDFDataLoadMaster(final JiniFederation fed)
            throws ConfigurationException {

        if (fed == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.zookeeper = fed.getZookeeper();

        final Configuration config = fed.getClient().getConfiguration();

        jobState = new JobState(config);

        minDataServices = (Integer) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.MIN_DATA_SERVICES, Integer.TYPE);

        awaitDataServicesTimeout = (Long) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.AWAIT_DATA_SERVICES_TIMEOUT, Long.TYPE);

        jobZPath = jobState.getJobZPath(fed);

    }

    /**
     * Creates/(re-)opens the {@link AbstractTripleStore}, loads the optional
     * ontology, and starts the clients. The clients will run until the master
     * is cancelled loading any data found in the {@link JobState#dataDir}.
     * Files are optionally deleted after they have been succesfully loaded.
     * 
     * Wait a bit to discover some minimum #of data services. Then allocate the
     * clients to the data services. There can be more than one per data
     * service.
     */
    public Void call() throws Exception {

        try {
            // ensure znode exists.
            fed.getZookeeper().create(
                    fed.getZooConfig().zroot + "/" + BigdataZooDefs.JOBS,
                    new byte[0], fed.getZooConfig().acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }
        try {
            // ensure znode exists.
            fed.getZookeeper().create(jobState.getJobClassZPath(fed),
                    new byte[0], fed.getZooConfig().acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }

        boolean restart = false;
        try {

            // create znode that is the root for the job.
            fed.getZookeeper().create(jobZPath,
                    SerializerUtil.serialize(jobState), fed.getZooConfig().acl,
                    CreateMode.PERSISTENT);

        } catch (NodeExistsException ex) {

            /*
             * Resuming a job already in progress and/or providing backup
             * clients for a job that is currently running.
             * 
             * Note: We use the client to data service UUID assignments read
             * from the znode data!
             */

            jobState = (JobState) SerializerUtil.deserialize(zookeeper.getData(
                    jobZPath, false, new Stat()));

            restart = true;

            log.warn("Running an existing job: " + jobZPath);

        }

        final UUID[] serviceUUIDs = fed.awaitServices(minDataServices,
                awaitDataServicesTimeout);

        final long begin = System.currentTimeMillis();

        /*
         * The #of services that will be tasked to run the clients. If there are
         * more clients than services, then some services will be tasked with
         * more than one client.
         */
        final int nservices = serviceUUIDs.length;

        if (INFO)
            log.info("Will run " + jobState.nclients + " on " + nservices
                    + " services");

        final Map<Integer/* client# */, Future> producerFutures = new LinkedHashMap<Integer, Future>(
                jobState.nclients/* initialCapacity */);

        if (!restart) {

            /*
             * Make stable assignments of each client to a specific data
             * service. The same assignments MUST be used by the producer and
             * the consumer or the consumer will not be able to find the data if
             * it was written onto a local file system.
             */

            for (int clientNum = 0; clientNum < jobState.nclients; clientNum++) {

                final int i = clientNum % serviceUUIDs.length;

                final UUID serviceUUID = serviceUUIDs[i];

                final String clientZPath = jobState.getClientZPath(fed,
                        clientNum);

                // ClientState clientState;
                try {

                    // clientState = new ClientState(jobState.startIndex);

                    zookeeper.create(clientZPath, /*
                                                     * SerializerUtil
                                                     * .serialize(clientState)
                                                     */new byte[0],
                            fed.getZooConfig().acl, CreateMode.PERSISTENT);

                } catch (NodeExistsException ex) {

                    // clientState = (ClientState) SerializerUtil
                    // .deserialize(zookeeper.getData(clientZPath, false,
                    // new Stat()));

                    if (INFO)
                        log.info("Client will restart: ");// + clientState);

                }

                jobState.client2DataService[clientNum] = serviceUUID;

            }

            // write those assignments into zookeeper.
            zookeeper
                    .setData(jobZPath, SerializerUtil.serialize(jobState), -1/* version */);

            if (INFO)
                log.info("Wrote client assignments into zookeeper.");

        }

        // open/create the kb.
        openTripleStore();

        /*
         * Start the producer and consumer tasks.
         * 
         * Note: The producer and consumer tasks are paired. The tasks in each
         * pair run on the SAME data service.
         */
        for (int clientNum = 0; clientNum < jobState.nclients; clientNum++) {

            // use the stable assignment made above or read from zookeeper.
            final UUID serviceUUID = jobState.client2DataService[clientNum];

            // lookup the data service.
            final IDataService dataService = fed.getDataService(serviceUUID);

            if (INFO)
                log.info("Running client#=" + clientNum + " on "
                        + dataService.getHostname());

            final RDFFileLoadTask task = new RDFFileLoadTask(jobState,
                    clientNum);

            producerFutures.put(clientNum, dataService.submit(task));

        }

        try {

            while (!allDone(producerFutures)) {

                final int nremaining = producerFutures.size();

                if (DEBUG)
                    log.debug("#remaining futures=" + nremaining);

                if (nremaining < 10)
                    // sleep a bit before rechecking the futures.
                    Thread.sleep(1000/* ms */);
                else
                    // sleep longer if there are more clients.
                    Thread.sleep(10000/* ms */);

            }

        } catch (Throwable t) {

            /*
             * Cancel all futures on error.
             */

            try {
                cancelAll(producerFutures, true/* mayInterruptIfRunning */);
            } catch (Throwable t2) {
                log.error(t2);
            }

            throw new Exception(t);

        }

        /*
         * Delete all znode children and the jobZPath when the job completes
         * successfully.
         */

        if (INFO)
            log.info("Success - cleaning up job state: elapsed="
                    + (System.currentTimeMillis() - begin));

        ZooHelper.destroyZNodes(fed.getZookeeper(), jobZPath, 0/* depth */);

        if (INFO)
            log.info("All done: elapsed="
                    + (System.currentTimeMillis() - begin));

        return null;

    }

    /**
     * Check the futures.
     * <p>
     * Note: This polls the futures of the spawned clients. Those tasks are
     * running with the {@link ZooKeeper} client of the {@link DataService}'s
     * {@link JiniFederation}, so they will only appear to be "disconnected"
     * and their ephemeral znodes will only disappear if the {@link DataService}
     * itself becomes disconnected from the zookeeper ensemble.
     * 
     * @todo This master can not handle {@link DataService} death. It assumes
     *       that the files are being written onto the local file system for the
     *       host on which the DataService is running. If the
     *       {@link DataService} were to die the files would be inaccessible to
     *       another {@link DataService} unless it was running on the same host.
     *       However, {@link DataService} failover is not supported yet either,
     *       so if the {@link DataService} dies you are toast.
     */
    protected boolean allDone(final Map<Integer/* client */, Future> futures) {

        // Note: used to avoid concurrent modification of [futures].
        final List<Integer> finished = new LinkedList<Integer>();

        for (Map.Entry<Integer, Future> entry : futures.entrySet()) {

            final int clientNum = entry.getKey();

            final Future future = entry.getValue();

            if (future.isDone()) {

                try {

                    future.get();

                } catch (Throwable t) {

                    /*
                     * @todo To be robust we would have to restart the client (a
                     * producer or consumer) if one throws an exception.
                     * 
                     * This could be done automatically.
                     * 
                     * However, this also can be done manually by running
                     * another instance of the master with the same
                     * configuration. It will automatically create backup
                     * clients. Since the original client is dead, the backup
                     * client will gain the zlock and pickup where the other
                     * client left off.
                     */

                    log.error("client#=" + clientNum + ": " + t, t);

                }

                finished.add(clientNum);

            }

        }

        for (int clientNum : finished) {

            futures.remove(clientNum);

        }

        // finished iff no more futures.
        return futures.isEmpty();

    }

    protected void cancelAll(final Map<Integer, Future> futures,
            final boolean mayInterruptIfRunning) {

        final Iterator<Future> itr = futures.values().iterator();

        while (itr.hasNext()) {

            final Future f = itr.next();

            if (!f.isDone()) {

                f.cancel(mayInterruptIfRunning);

            }

            itr.remove();

        }

    }

    /**
     * Create/re-open the repository.
     * <p>
     * If the backing database does not exist, then create it and create the
     * {@link AbstractTripleStore} on that database. When the backing database
     * is an {@link IBigdataFederation}, then you can either re-open an
     * existing federation or create one for the purposes of the test ({@link LDS},
     * {@link EDS}) or connect to an existing federation ({@link JDS} for
     * scale-out configurations).
     */
    public AbstractTripleStore openTripleStore() {

        /*
         * Create/re-open the triple store.
         */
        AbstractTripleStore tripleStore;

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

            if (INFO)
                log.info("Re-opened tripleStore: " + jobState.namespace);

            showProperties(tripleStore);
            
        }

        return tripleStore;

    }

    /**
     * Create the {@link ITripleStore} specified by
     * {@link ConfigurationOptions#NAMESPACE}.
     * <p>
     * Note: The properties used to configure the {@link ITripleStore} are taken
     * from {@link IBigdataClient#getProperties()}. For the jini deployment,
     * you specify those properties by declaring
     * 
     * <pre>
     * properties = NV[]{...};
     * </pre>
     * 
     * for the component which is the master for the job, e.g., the
     * {@link RDFDataLoadMaster}.
     * 
     * @return The {@link ITripleStore}
     */
    protected AbstractTripleStore createTripleStore() {

        if (INFO)
            log.info("Creating tripleStore: " + jobState.namespace);

        /*
         * Pick up properties configured for the client as defaults.
         * 
         * You can specify those properties using NV[] for the component that is
         * executing the master.
         */
        final Properties properties = fed.getClient().getProperties();

        final AbstractTripleStore tripleStore = new ScaleOutTripleStore(fed,
                jobState.namespace, ITx.UNISOLATED, properties);

        // create the triple store.
        tripleStore.create();

        // show #of axioms.
        System.out.println("axiomCount=" + tripleStore.getStatementCount());
        
        if (INFO)
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

        if (INFO)
            log.info("Loading ontology: " + jobState.ontology);

        tripleStore.getDataLoader().loadFiles(jobState.ontology/* file */,
                jobState.ontology.getPath()/* baseURI */,
                JobState.fallback/* rdfFormat */, new RDFFilenameFilter());

        System.out.println("axiomAndOntologyCount="
                + tripleStore.getStatementCount());
        
        if (INFO)
            log.info("Loaded ontology: " + jobState.ontology);

    }

    /**
     * Dump some properties of interest.
     */
    public void showProperties(final AbstractTripleStore tripleStore) {

        if (!INFO) return;

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

        log.info(IndexMetadata.Options.BTREE_READ_RETENTION_QUEUE_CAPACITY
                        + "="
                        + p
                                .getProperty(IndexMetadata.Options.DEFAULT_BTREE_READ_RETENTION_QUEUE_CAPACITY));

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

        // log.info(BigdataSail.Options.QUERY_TIME_EXPANDER + "="
        // + p.getProperty(BigdataSail.Options.QUERY_TIME_EXPANDER));

//        log.info("bloomFilterFactory="
//                + tripleStore.getSPORelation().getSPOIndex().getIndexMetadata()
//                        .getBloomFilterFactory());

    }

}
