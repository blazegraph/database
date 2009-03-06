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
 * Created on Jan 16, 2009
 */

package com.bigdata.service.jini;

import java.io.File;
import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.jini.DumpFederation.ScheduledDumpTask;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZLockImpl;
import com.bigdata.zookeeper.ZooHelper;

/**
 * Utility class that can be used to execute a distributed job. The master
 * creates a set of tasks, submits each task to an {@link IDataService} for
 * execution, and awaits their {@link Future}s. There are a variety of
 * {@link ConfigurationOptions}. You specify a concrete instance of this class
 * using {@link ConfigurationOptions#MASTER_CLASS}. You specify the client task
 * using {@link TaskMaster#newClientTask(int)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <S> The generic type of the master's state.
 * @param <T> The generic type of the client task.
 */
abstract public class TaskMaster<S extends TaskMaster.JobState, T extends Callable<? extends Object>>
        implements Callable<Void> {

    final protected static Logger log = Logger.getLogger(TaskMaster.class);

    final protected static boolean INFO = log.isInfoEnabled();

    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * {@link Configuration} options for the {@link TaskMaster} and derived
     * classes. The "component" for these options is the name of the concrete
     * master class to be executed.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions {
        
        /**
         * When <code>true</code> as an after action on the job, the
         * {@link DataService}s in the federation will be made to undergo
         * asynchronous overflow processing and the live journals will be
         * truncated so that the total size on disk of the federation is at its
         * minimum footprint for the given history retention policy (default
         * <code>false</code>). The master will block during this operation
         * so you can readily tell when it is finished. Note that this option
         * only makes sense in benchmark environments where you can contol the
         * total system otherwise asynchronous writes may continue.
         * 
         * @see AbstractScaleOutFederation#forceOverflow(boolean)
         */
        String FORCE_OVERFLOW = "forceOverflow";

        /**
         * #of clients to use.
         */
        String NCLIENTS = "nclients";

        /**
         * The path to the directory in where {@link ScheduledDumpTask}s will
         * write metadata about the state, size, and other aspects of the index
         * partitions throughout the run (optional).
         * 
         * @see #INDEX_DUMP_NAMESPACE
         */
        String INDEX_DUMP_DIR = "indexDumpDir";

        /**
         * The namespace to be used for the {@link ScheduledDumpTask}s
         * (optional).
         * 
         * @see #INDEX_DUMP_DIR
         */
        String INDEX_DUMP_NAMESPACE = "indexDumpNamespace";

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
         * The job name is used to identify the job within zookeeper. A znode
         * with this name will be created as follows:
         * 
         * <pre>
         * zroot (of the federation)
         *    / jobs
         *      / TaskMaster (fully qualified name of the concrete master class).
         *        / jobName
         * </pre>
         * 
         * Under that znode are the following.
         * 
         * <pre>
         *          / client# (where # is the client#; the data of this znode is typically the client's state).
         *            / locknode (used to elect the client that is running if there is contention).
         *            / ...
         * </pre>
         */
        String JOB_NAME = "jobName";
        
    }

    /**
     * State describing the job to be executed. The various properties are all
     * defined by {@link ConfigurationOptions}.
     */
    public static class JobState implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -340273551639560974L;

        /*
         * Public options and configuration information. 
         */
        
        /**
         * The name of the class that is the master (aka the component for the
         * jini {@link Configuration}).
         */
        public final String component;

        /**
         * The #of client tasks
         * 
         * @see ConfigurationOptions#NCLIENTS
         */
        public final int nclients;

        /**
         * The job name.
         * 
         * @see ConfigurationOptions#JOB_NAME
         */
        public final String jobName;

        /**
         * @see ConfigurationOptions#MIN_DATA_SERVICES
         */
        final int minDataServices;

        /**
         * @see ConfigurationOptions#AWAIT_DATA_SERVICES_TIMEOUT}
         */
        final long awaitDataServicesTimeout;

        /*
         * Debugging and benchmarking options.
         */
        
        /**
         * <code>true</code> iff overflow will be forced on the data services
         * after the client tasks are done.
         * 
         * @see ConfigurationOptions#FORCE_OVERFLOW
         */
        final boolean forceOverflow;

        /**
         * The directory into which scheduled dumps of the index partition
         * metadata will be written by a {@link ScheduledDumpTask} (optional).
         * 
         * @see ConfigurationOptions#INDEX_DUMP_DIR
         */
        final File indexDumpDir;

        /**
         * The namespace to be used for the scheduled dumps of the index
         * partition metadata (optional).
         * 
         * @see ConfigurationOptions#INDEX_DUMP_NAMESPACE
         */
        final String indexDumpNamespace;
        
        /**
         * Allows extension of {@link #toString()}
         * 
         * @param sb
         */
        protected void toString(StringBuilder sb) {
            
        }
        
        public String toString() {

            final StringBuilder sb = new StringBuilder();
            
            sb.append(getClass().getName());

            /*
             * General options.
             */
            
            sb.append(", component=" + component);

            sb.append(", " + ConfigurationOptions.NCLIENTS + "=" + nclients);

            sb.append(", " + ConfigurationOptions.JOB_NAME + "=" + jobName);

            /*
             * Debugging and benchmarking options.
             */
            
            sb.append(", " + ConfigurationOptions.FORCE_OVERFLOW + "="
                    + forceOverflow);

            /*
             * Run state stuff. 
             */
            
//            sb.append(", client2DataService="
//                    + Arrays.toString(client2DataService));

            /*
             * Subclass's options.
             */
            toString(new StringBuilder());
            
            sb.append("}");
            
            return sb.toString();

        }

        protected JobState(final String component, final Configuration config)
                throws ConfigurationException {

            if (component == null)
                throw new IllegalArgumentException();

            if (config == null)
                throw new IllegalArgumentException();

            this.component = component;

            /*
             * general options.
             */
            
            nclients = (Integer) config.getEntry(component,
                    ConfigurationOptions.NCLIENTS, Integer.TYPE);

            jobName = (String) config.getEntry(component,
                    ConfigurationOptions.JOB_NAME, String.class);

            minDataServices = (Integer) config.getEntry(component,
                    ConfigurationOptions.MIN_DATA_SERVICES, Integer.TYPE);

            awaitDataServicesTimeout = (Long) config
                    .getEntry(component,
                            ConfigurationOptions.AWAIT_DATA_SERVICES_TIMEOUT,
                            Long.TYPE);

            client2DataService = new UUID[nclients];

            /*
             * Benchmarking and debugging options.
             */
            
            forceOverflow = (Boolean) config.getEntry(component,
                    ConfigurationOptions.FORCE_OVERFLOW, Boolean.TYPE,
                    Boolean.FALSE);

            indexDumpDir = (File) config.getEntry(component,
                    ConfigurationOptions.INDEX_DUMP_DIR, File.class, null);

            indexDumpNamespace = (String) config.getEntry(component,
                    ConfigurationOptions.INDEX_DUMP_NAMESPACE, String.class,
                    null);

        }

        /**
         * The mapping of clients onto the {@link DataService}s. The index is
         * the client#. The value is the {@link DataService} {@link UUID}.
         */
        final public UUID[] client2DataService;

        /**
         * Return the zpath of the node for all jobs which are instances of the
         * configured master's class.
         * 
         * @see #component
         */
        public String getJobClassZPath(final JiniFederation fed) {
            
            return fed.getZooConfig().zroot + "/" + BigdataZooDefs.JOBS + "/"
                    + component;
            
        }
        
        /**
         * Return the zpath to the znode which corresponds to the job which is
         * being executed. The data for this znode is this {@link JobState}.
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

    /**
     * The federation (from the ctor).
     */
    protected final JiniFederation fed;

    /**
     * Either set from the {@link Configuration} (new job) or read from
     * zookeeper (existing job) and thereafter unchanging.
     */
    public S getJobState() {
        
        return jobState;
        
    }
    private S jobState;

    /**
     * Runs the master. SIGTERM (normal kill or ^C) will cancel the job,
     * including any running clients.
     * 
     * @return The {@link Future} for the master. Use {@link Future#get()} to
     *         await the outcome of the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    final protected Future<Void> innerMain() {

        final Future<Void> future = fed.getExecutorService().submit(this);

        /*
         * Install a shutdown hook so that the master will cancel any running
         * clients if it is interrupted (normal kill will trigger this hook).
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {

                future.cancel(true/* mayInterruptIfRunning */);

            }

        });

        return future;

    }
    
    /**
     * 
     * @param fed
     * 
     * @throws ConfigurationException
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected TaskMaster(final JiniFederation fed)
            throws ConfigurationException, KeeperException,
            InterruptedException {

        if (fed == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        final Configuration config = fed.getClient().getConfiguration();

        jobState = newJobState(getClass().getName(), config);

    }

    /**
     * Wait a bit to discover some minimum #of data services. Then allocate the
     * clients to the data services. There can be more than one per data
     * service.
     * 
     * @return <code>null</code>
     */
    public Void call() throws Exception {

        /*
         * Setup the jobState.
         * 
         * Note: [jobState] will be replaced as a side-effect if there is an
         * existing instance of the job in zookeeper (same component and
         * jobName).
         */
        setupJobState();

        // note: take timestamp after discovering services!
        final long begin = System.currentTimeMillis();

        // callback for overrides.
        beginJob();
        
        /*
         * Setup scheduled metadata dumps of the index partitions in the KB.
         */
        if (jobState.indexDumpDir != null) {
           
            // runs @t0, 1m, 2m, ... 9m.
            fed.addScheduledTask(new ScheduledDumpTask(fed,
                    jobState.indexDumpNamespace, 10, jobState.indexDumpDir,
                    "indexDump", TimeUnit.MINUTES), 0/* initialDelay */,
                    1/* delay */, TimeUnit.MINUTES);

            // runs @t10m, 20m, 30m, ... 50m.
            fed.addScheduledTask(new ScheduledDumpTask(fed,
                    jobState.indexDumpNamespace, 5, jobState.indexDumpDir,
                    "indexDump", TimeUnit.MINUTES), 10/* initialDelay */,
                    10/* delay */, TimeUnit.MINUTES);

            // runs @t1h, 2h, ... until cancelled.
            fed.addScheduledTask(new ScheduledDumpTask(fed,
                    jobState.indexDumpNamespace, 0, jobState.indexDumpDir,
                    "indexDump", TimeUnit.MINUTES), 1/* initialDelay */,
                    1/* delay */, TimeUnit.HOURS);

        }

        {

            /*
             * Start the client tasks.
             */

            // set if some task fails.
            boolean failure = false;

            /*
             * The #of services that will be tasked to run the clients. If there
             * are more clients than services, then some services will be tasked
             * with more than one client.
             */
            final int nservices = jobState.client2DataService.length;

            if (INFO)
                log.info("Will run " + jobState.nclients + " on " + nservices
                        + " services");

            final Map<Integer/* client# */, Future> clientFutures = new LinkedHashMap<Integer, Future>(
                    jobState.nclients/* initialCapacity */);

            for (int clientNum = 0; clientNum < jobState.nclients; clientNum++) {

                // use the stable assignment made above or read from zookeeper.
                final UUID serviceUUID = jobState.client2DataService[clientNum];

                // lookup the data service.
                final IDataService dataService = fed
                        .getDataService(serviceUUID);

                if (INFO)
                    log.info("Running client#=" + clientNum + " on "
                            + dataService.getHostname());

                final Callable<? extends Object> clientTask = newClientTask(clientNum);

                clientFutures.put(clientNum, dataService.submit(clientTask));

            }

            try {

                while (!allDone(clientFutures)) {

                    final int nremaining = clientFutures.size();

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

                failure = true;
                
                try {

                    cancelAll(clientFutures, true/* mayInterruptIfRunning */);

                } catch (Throwable t2) {

                    log.error(t2);

                }

                throw new Exception(t);

            }

            /*
             * Done.
             */

            final long elapsed = System.currentTimeMillis() - begin;

            if (INFO)
                log.info("Done: " + (failure ? "failure" : "success")
                        + ", elapsed=" + elapsed);

            if (failure) {

                // halt processing on failure.
                return null;
                
            }
            
        }

        if (jobState.forceOverflow) {

            /*
             * @todo This is an operation that we would like to run once by the
             * master which actually executes the clients even if there are
             * multiple masters (multiple master support is not really all there
             * yet and there are interactions with how the client tasks handle
             * multiple instances of themselves so this is all forward looking).
             */

            System.out.println("Forcing overflow: now=" + new Date());

            fed.forceOverflow(true/* truncateJournal */);

            System.out.println("Forced overflow: now=" + new Date());

        }
        
        /*
         * Delete zookeeper state when the job completes successfully.
         */
        ZooHelper.destroyZNodes(fed.getZookeeperAccessor().getZookeeper(),
                jobState.getJobZPath(fed), 0/* depth */);

        if (INFO)
            log.info("All done: elapsed="
                    + (System.currentTimeMillis() - begin));

        return null;

    }
    
    /**
     * Return a {@link JobState}.
     * 
     * @param component
     *            The component.
     * @param config
     *            The configuration.
     *            
     * @return The {@link JobState}.
     */
    abstract protected S newJobState(String component, Configuration config)
            throws ConfigurationException;
    
    /**
     * Return a client to be executed on a remote data service. The client can
     * obtain access to the {@link IBigdataFederation} when it executes on the
     * remote data service if it implements {@link IDataServiceAwareProcedure}.
     * You can use {@link AbstractClientTask} as a starting point.
     * 
     * @param clientNum
     *            The client number.
     * 
     * @return The client task.
     * 
     * @see AbstractClientTask
     */
    abstract protected T newClientTask(final int clientNum);
    
    /**
     * Callable invoked when the job is ready to execute. The default
     * implementation is a NOP. This may be used to register indices, etc.
     * 
     * @throws Exception
     */
    protected void beginJob() throws Exception {
        
    }
    
    /**
     * Sets up the {@link JobState} in zookeeper, including the assignment of
     * service {@link UUID}s to each client. {@link #jobState} will be replaced
     * with the {@link JobState} read from zookeeper if a pre-existing job is
     * found in zookeeper.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    protected void setupJobState() throws KeeperException,
            InterruptedException, TimeoutException {

        final ZooKeeper zookeeper = fed.getZookeeperAccessor().getZookeeper();
        
        try {
            // ensure znode exists.
            zookeeper.create(
                    fed.getZooConfig().zroot + "/" + BigdataZooDefs.JOBS,
                    new byte[0], fed.getZooConfig().acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }
        
        final String jobClassZPath = jobState.getJobClassZPath(fed);

        try {
            // ensure znode exists.
            zookeeper.create(jobClassZPath, new byte[0],
                    fed.getZooConfig().acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }

        /*
         * Use a global lock to protect the job state during setup.
         * 
         * Note: We just created parent of this lock node (or at any rate,
         * ensured that it exists).
         */
        final ZLock zlock = ZLockImpl.getLock(zookeeper, jobClassZPath + "/"
                + "locknode_" + jobState.jobName, fed.getZooConfig().acl);

        zlock.lock();
        try {

            final String jobZPath = jobState.getJobZPath(fed);
            
            try {

                // create znode that is the root for the job.
                zookeeper.create(jobZPath, SerializerUtil.serialize(jobState),
                        fed.getZooConfig().acl, CreateMode.PERSISTENT);

                try {

                    assignClientsToServices(jobState);

                    // write those assignments into zookeeper.
                    zookeeper.setData(jobZPath, SerializerUtil
                            .serialize(jobState), -1/* version */);

                    if (INFO)
                        log.info("Wrote client assignments into zookeeper.");

                } catch (Throwable t) {

                    /*
                     * Since we created the jobState znode, delete the jobState
                     * while we are still holding the zlock.
                     */
                    try {
                        zookeeper.delete(jobZPath, -1/* version */);
                    } catch (Throwable t2) {
                        log.error(t2);
                    }

                    throw new RuntimeException(t);

                }

            } catch (NodeExistsException ex) {

                /*
                 * Resuming a job already in progress and/or providing backup
                 * clients for a job that is currently running.
                 * 
                 * Note: We use the client to data service UUID assignments read
                 * from the znode data!
                 */

                jobState = (S) SerializerUtil.deserialize(zookeeper.getData(
                        jobZPath, false, new Stat()));

                log.warn("Running an existing job: " + jobZPath);

            }

        } finally {

            zlock.unlock();

        }

    }

    /**
     * Make stable assignments of each client to a specific data service.
     * 
     * @todo stable assignments are only required if the client will be reading
     *       or writing data local to the data service so this should be a
     *       configuration option.
     *       <p>
     *       we also need to support pipelines of master tasks.
     */
    protected void assignClientsToServices(final JobState jobState)
            throws KeeperException, InterruptedException, TimeoutException {

        final UUID[] serviceUUIDs = fed.awaitServices(jobState.minDataServices,
                jobState.awaitDataServicesTimeout);

        for (int clientNum = 0; clientNum < jobState.nclients; clientNum++) {

            final int i = clientNum % serviceUUIDs.length;

            final UUID serviceUUID = serviceUUIDs[i];

            jobState.client2DataService[clientNum] = serviceUUID;

        }

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
     * @param futures
     *            The futures of the client tasks that are being executed.
     * 
     * @return <code>true</code> when no more tasks are running.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected boolean allDone(final Map<Integer/* client */, Future> futures)
            throws InterruptedException, ExecutionException {

        if (futures == null)
            throw new IllegalArgumentException();
        
        // Note: used to avoid concurrent modification of [futures].
        final List<Integer> finished = new LinkedList<Integer>();

        for (Map.Entry<Integer, Future> entry : futures.entrySet()) {

            final int clientNum = entry.getKey();

            final Future future = entry.getValue();

            if (future.isDone()) {

                // note: test the client's future and halt if the client fails.
                future.get();
                
                finished.add(clientNum);

            }

        }

        for (int clientNum : finished) {

            futures.remove(clientNum);

        }

        // finished iff no more futures.
        return futures.isEmpty();

    }

    /**
     * Cancel the futures.
     * 
     * @param futures
     *            The futures.
     * @param mayInterruptIfRunning
     *            If the tasks for the futures may be interrupted.
     */
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
     * Base class for client tasks run by the master on one or more data
     * services. This class contends for a {@link ZLock} based on the assigned
     * {@link #clientNum} and then invoked {@link #runWithZLock()} if the
     * {@link ZLock} if granted. If the lock is lost, it will continue to
     * contend for the lock and then run until finished.
     * <p>
     * Note: This implementation presumes that {@link #runWithZLock()} has some
     * means of understanding when it is done and can restart its work from
     * where it left off. One way to handle that is to write the state of the
     * client into the client's znode and to update that from time to time as
     * the client makes progress on its task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <S>
     *            The generic for the {@link JobState}.
     * @param <T>
     *            The generic for the {@link Callable}'s return type.
     * @param <U>
     *            The generic type of the client state (stored in zookeeper).
     */
    abstract public static class AbstractClientTask<S extends JobState, T, U extends Serializable>
            implements Callable<T>, Serializable, IDataServiceAwareProcedure {

        protected final S jobState;

        protected final int clientNum;

        /**
         * The {@link DataService} on which this task is execution.
         */
        protected transient DataService dataService;

        /**
         * The federation object for the {@link DataService} on which the client
         * is executing.
         */
        protected transient JiniFederation fed;

        /**
         * The zpath for this client (set once the client starts executing on
         * the target {@link DataService}). The data of this znode is the
         * client's state (if it saves its state in zookeeper).
         */
        protected transient String clientZPath;

        protected transient List<ACL> acl;

        /**
         * The zpath for the {@link ZLock} node. Only the instance of this task
         * holding the {@link ZLock} is allowed to run. This makes it safe to
         * run multiple instances of this task for the same {@link #clientNum}.
         * If one instance dies, the instance that gains the {@link ZLock} will
         * read the client's state from zookeeper and continue processing.
         * <p>
         * This is <code>null</code> until {@link #call()}.
         */
        protected transient ZLockImpl zlock;

        public void setDataService(final DataService dataService) {

            if (dataService == null)
                throw new IllegalArgumentException();

            if (INFO)
                log.info("Got data service.");

            this.dataService = dataService;

            this.fed = (JiniFederation) dataService.getFederation();

            this.clientZPath = jobState.getClientZPath(fed, clientNum);

            this.acl = fed.getZooConfig().acl;

        }

        /**
         * The {@link JiniFederation} for the {@link DataService} on which this
         * task is executed.
         * 
         * @throws IllegalStateException
         *             unless the task is running within the {@link DataService}.
         */
        protected JiniFederation getFederation() {

            if (fed == null)
                throw new IllegalStateException();

            return fed;

        }

        public String toString() {

            return getClass().getName() + "{clientNum=" + clientNum + "}";

        }

        protected AbstractClientTask(final S jobState, final int clientNum) {

            this.jobState = jobState;

            this.clientNum = clientNum;

        }

        /**
         * Runs the generator.
         */
        public T call() throws Exception {

            if (INFO)
                log.info("Running: client#=" + clientNum + ", " + jobState);

            while (true) {

                zlock = ZLockImpl.getLock(fed.getZookeeper(), jobState
                        .getLockNodeZPath(fed, clientNum), acl);

                zlock.lock();
                try {

                    final T ret = runWithZLock();

                    if (INFO)
                        log.info("Finished: client#=" + clientNum + ", "
                                + jobState);

                    return ret;

                } catch (SessionExpiredException ex) {

                    /*
                     * Log warning and then try to re-obtain the zlock so we can
                     * finish the job.
                     */

                    log.warn(this + " : will seek zlock again", ex);

                    continue;

                } finally {

                    zlock.unlock();

                }

            }

        }

        /**
         * Do work while holding the {@link ZLock}. The implementation SHOULD
         * verify from time to time that it in fact holds the {@link ZLock}
         * using {@link ZLock#isLockHeld()}.
         * 
         * @return The result.
         * 
         * @throws Exception
         * @throws KeeperException
         * @throws InterruptedException
         */
        abstract protected T runWithZLock() throws Exception, KeeperException,
                InterruptedException;

        /**
         * Method should be invoked from within {@link #runWithZLock()} if the
         * client wishes to store state in zookeeper. If there is no state in
         * zookeeper, then {@link #newClientState()} is invoked and the result
         * will be stored in zookeeper. You can update the client's state from
         * time to time using #writeClientState(). If the client looses the
         * {@link ZLock}, it can read the client state from zookeeper using
         * this method and pick up processing more or less where it left off
         * (depending on when you last updated the client state in zookeeper).
         * 
         * @return
         * 
         * @throws InterruptedException
         * @throws KeeperException
         */
        protected U setupClientState() throws InterruptedException, KeeperException {

            if (!zlock.isLockHeld())
                throw new InterruptedException("Lost ZLock");

            final ZooKeeper zookeeper = zlock.getZooKeeper();

            final String clientZPath = jobState.getClientZPath(fed, clientNum);

            U clientState;
            try {

                clientState = newClientState();

                zookeeper.create(clientZPath, SerializerUtil
                        .serialize(clientState), fed.getZooConfig().acl,
                        CreateMode.PERSISTENT);

                if (INFO)
                    log.info("Running: " + clientState);

            } catch (NodeExistsException ex) {

                clientState = (U) SerializerUtil.deserialize(zookeeper.getData(
                        clientZPath, false, new Stat()));

                if (INFO)
                    log.info("Client will restart: " + clientState);

            }

            return clientState;
            
        }

        /**
         * Method updates the client state in zookeeper. The caller MUST be
         * holding the {@link ZLock} (this is verified).
         * 
         * @param clientState
         *            The state to be written into the znode identified by
         *            {@link #clientZPath}.
         * 
         * @throws InterruptedException
         * @throws KeeperException
         */
        protected void writeClientState(U clientState) throws KeeperException,
                InterruptedException {

            if (clientState == null)
                throw new IllegalArgumentException();
            
            if (!zlock.isLockHeld())
                throw new InterruptedException("Lost ZLock");

            final ZooKeeper zookeeper = zlock.getZooKeeper();

            try {
                /*
                 * Update the client state.
                 */
                zookeeper.setData(clientZPath, SerializerUtil
                        .serialize(clientState), -1/* version */);
            } catch (ConnectionLossException ex) {
                /*
                 * Note: There are a variety of transient errors which can
                 * occur. Next time we are connected and and this method is
                 * invoked we will update the client state, and that should be
                 * good enough.
                 */
                log.warn(ex);
            }

        }
        
        /**
         * Return a new instance of the client's state.
         */
        abstract protected U newClientState();

    }

}
