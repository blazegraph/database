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
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.ACL;
import org.eclipse.jetty.server.Server;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAGlueDelegate;
import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.QuorumServiceBase;
import com.bigdata.ha.RunState;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HALogRequest;
import com.bigdata.ha.msg.HALogRootBlocksRequest;
import com.bigdata.ha.msg.HARebuildRequest;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HAWriteSetStateRequest;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHASendStoreResponse;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteSetStateResponse;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.SerializerUtil;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumEvent;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.quorum.zk.QuorumBackupState;
import com.bigdata.quorum.zk.ZKQuorum;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.RemoteAdministrable;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.bigdata.util.concurrent.MonitoredFutureTask;
import com.bigdata.util.config.NicUtil;
import com.bigdata.zookeeper.ZooKeeperAccessor;
import com.sun.jini.start.LifeCycle;

/**
 * An administratable server for an {@link HAJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/530"> Journal
 *      HA </a>
 */
public class HAJournalServer extends AbstractServer {

    private static final Logger log = Logger.getLogger(HAJournalServer.class);

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /**
     * Configuration options for the {@link HAJournalServer}.
     */
    public interface ConfigurationOptions extends
            AbstractServer.ConfigurationOptions {

        String COMPONENT = HAJournalServer.class.getName();
        
        /**
         * The target replication factor (k).
         */
        String REPLICATION_FACTOR = "replicationFactor";
        
        /**
         * The {@link InetSocketAddress} at which the managed {@link HAJournal}
         * exposes its write pipeline interface (required).
         */
        String WRITE_PIPELINE_ADDR = "writePipelineAddr";

        /**
         * The logical service identifier for this highly available journal.
         * There may be multiple logical highly available journals, each
         * comprised of <em>k</em> physical services. The logical service
         * identifier is used to differentiate these different logical HA
         * journals. The service {@link UUID} is used to differentiate the
         * physical service instances. By assigning a logical service identifier
         * to an {@link HAJournalServer} you associate that server instance with
         * the specified logical highly available journal.
         * <p>
         * The identifier may be any legal znode node.
         * 
         * TODO This needs to be reconciled with the federation. The federation
         * uses ephemeral sequential to create the logical service identifiers.
         * Here they are being assigned manually. This is basically the "flex"
         * versus "static" issue.
         */
        String LOGICAL_SERVICE_ID = "logicalServiceId";
        
    }
    
    /**
     * Configuration options for the {@link NanoSparqlServer}.
     */
    public interface NSSConfigurationOptions extends ConfigParams {
        
        String COMPONENT = NanoSparqlServer.class.getName();
        
        /**
         * The port at which the embedded {@link NanoSparqlServer} will respond
         * to HTTP requests (default {@value #DEFAULT_PORT}). This MAY be ZERO
         * (0) to use a random open port.
         */
        String PORT = "port";

        int DEFAULT_PORT = 8080;
        
    }

    /**
     * Caching discovery client for the {@link HAGlue} services.
     */
    private HAJournalDiscoveryClient discoveryClient;

    /**
     * The journal.
     */
    private HAJournal journal;
    
    private UUID serviceUUID;

    private ZookeeperClientConfig zkClientConfig;
    
    private ZooKeeperAccessor zka;
    
    /**
     * An executor used to handle events that were received in the zk watcher
     * event thread. We can not take actions that could block in the watcher
     * event thread. Therefore, a task for the event is dropped onto this
     * service where it will execute asynchronously with respect to the watcher
     * thread.
     * <p>
     * Note: This executor will be torn down when the backing
     * {@link AbstractJournal#getExecutorService()} is torn down. Tasks
     * remaining on the backing queue for the {@link LatchedExecutor} will be
     * unable to execute successfuly and the queue will be drained as attempts
     * to run those tasks result in {@link RejectedExecutionException}s.
     */
    private LatchedExecutor singleThreadExecutor;
    
    private HAGlue haGlueService;
    
    /**
     * The znode name for the logical service.
     * 
     * @see ConfigurationOptions#LOGICAL_SERVICE_ID
     */
    private String logicalServiceId;
    
    /**
     * The zpath for the logical service.
     * 
     * @see ConfigurationOptions#LOGICAL_SERVICE_ID
     */
    private String logicalServiceZPath;
    
    /**
     * The {@link HAQuorumService}.
     */
    private HAQuorumService<HAGlue, HAJournal> quorumService;
    
    /**
     * An embedded jetty server exposing the {@link NanoSparqlServer} webapp.
     * The {@link NanoSparqlServer} webapp exposes a SPARQL endpoint for the
     * Journal, which is how you read/write on the journal (the {@link HAGlue}
     * interface does not expose any {@link Remote} methods to write on the
     * {@link HAJournal}.
     */
    private volatile Server jettyServer;

    /**
     * Caching discovery client for the {@link HAGlue} services.
     */
    public HAJournalDiscoveryClient getDiscoveryClient() {

        return discoveryClient;
        
    }

    public HAJournalServer(final String[] args, final LifeCycle lifeCycle) {

        super(args, lifeCycle);

    }
    
    @Override
    protected void terminate() {

        if (discoveryClient != null) {
        
            discoveryClient.terminate();
            
            discoveryClient = null;
            
        }

        super.terminate();
    
    }

    @Override
    protected HAGlue newService(final Configuration config)
            throws Exception {

        /*
         * Verify discovery of at least one ServiceRegistrar.
         */
        {
            final long begin = System.currentTimeMillis();

            ServiceRegistrar[] registrars = null;

            long elapsed = 0;

            while ((registrars == null || registrars.length == 0)
                    && elapsed < TimeUnit.SECONDS.toMillis(10)) {

                registrars = getDiscoveryManagement().getRegistrars();

                Thread.sleep(100/* ms */);

                elapsed = System.currentTimeMillis() - begin;

            }

            if (registrars == null || registrars.length == 0) {

                throw new RuntimeException(
                        "Could not discover ServiceRegistrar(s)");

            }

            if (log.isInfoEnabled()) {
                log.info("Found " + registrars.length + " service registrars");
            }

        }

        // Setup discovery for HAGlue clients.
        discoveryClient = new HAJournalDiscoveryClient(
                getServiceDiscoveryManager(),
                null/* serviceDiscoveryListener */, cacheMissTimeout);

        // Jini/River ServiceID.
        final ServiceID serviceID = getServiceID();

        if (serviceID == null)
            throw new AssertionError("ServiceID not assigned?");
        
        // UUID variant of that ServiceID.
        serviceUUID = JiniUtil.serviceID2UUID(serviceID);
        
        /*
         * Setup the Quorum / HAJournal.
         */

        zkClientConfig = new ZookeeperClientConfig(config);

        // znode name for the logical service.
        logicalServiceId = (String) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.LOGICAL_SERVICE_ID, String.class); 

        final String logicalServiceZPathPrefix = zkClientConfig.zroot + "/"
                + HAJournalServer.class.getName();
        
        // zpath for the logical service.
        logicalServiceZPath = logicalServiceZPathPrefix + "/"
                + logicalServiceId;

        final int replicationFactor = (Integer) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.REPLICATION_FACTOR, Integer.TYPE);        

        {

            // The address at which this service exposes its write pipeline.
            final InetSocketAddress writePipelineAddr = (InetSocketAddress) config
                    .getEntry(ConfigurationOptions.COMPONENT,
                            ConfigurationOptions.WRITE_PIPELINE_ADDR,
                            InetSocketAddress.class);

            /*
             * Configuration properties for this HAJournal.
             */
            final Properties properties = JiniClient.getProperties(
                    HAJournal.class.getName(), config);

            // Force the writePipelineAddr into the Properties.
            properties.put(HAJournal.Options.WRITE_PIPELINE_ADDR,
                    writePipelineAddr);

            /*
             * Zookeeper quorum.
             */
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum;
            {
                final List<ACL> acl = zkClientConfig.acl;
                final String zoohosts = zkClientConfig.servers;
                final int sessionTimeout = zkClientConfig.sessionTimeout;

                zka = new ZooKeeperAccessor(zoohosts, sessionTimeout);

                if (!zka.awaitZookeeperConnected(10, TimeUnit.SECONDS)) {

                    throw new RuntimeException("Could not connect to zk");

                }

                if (log.isInfoEnabled()) {
                    log.info("Connected to zookeeper");
                }

                /*
                 * Ensure key znodes exist.
                 */
                try {
                    zka.getZookeeper()
                            .create(zkClientConfig.zroot,
                                    new byte[] {/* data */}, acl,
                                    CreateMode.PERSISTENT);
                } catch (NodeExistsException ex) {
                    // ignore.
                }
                try {
                    zka.getZookeeper()
                            .create(logicalServiceZPathPrefix,
                                    new byte[] {/* data */}, acl,
                                    CreateMode.PERSISTENT);
                } catch (NodeExistsException ex) {
                    // ignore.
                }
                try {
                    zka.getZookeeper()
                            .create(logicalServiceZPath,
                                    new byte[] {/* data */}, acl,
                                    CreateMode.PERSISTENT);
                } catch (NodeExistsException ex) {
                    // ignore.
                }

                quorum = new ZKQuorumImpl<HAGlue, QuorumService<HAGlue>>(
                        replicationFactor, zka, acl);
            }

            // The HAJournal.
            this.journal = new HAJournal(properties, quorum);
            
        }

        // executor for events received in the watcher thread.
        singleThreadExecutor = new LatchedExecutor(
                journal.getExecutorService(), 1/* nparallel */);
        
        // our external interface.
        haGlueService = journal.newHAGlue(serviceUUID);

        // wrap the external interface, exposing administrative functions.
        final AdministrableHAGlueService administrableService = new AdministrableHAGlueService(
                this, haGlueService);

        // return that wrapped interface.
        return administrableService;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to handle initialization that must wait until the
     * {@link ServiceItem} is registered.
     */
    @Override
    protected void startUpHook() {

        if (log.isInfoEnabled())
            log.info("Starting server.");

        // Setup listener that logs quorum events @ TRACE.
        journal.getQuorum().addListener(new QuorumListener() {
            @Override
            public void notify(final QuorumEvent e) {
                if (log.isTraceEnabled())
                    log.trace(e);
            }
        });

        // Setup the quorum client (aka quorum service).
        quorumService = newQuorumService(logicalServiceZPath, serviceUUID,
                haGlueService, journal);

        // Start the quorum.
        journal.getQuorum().start(quorumService);

        // Enter a run state for the HAJournalServer.
        quorumService.enterRunState(quorumService.new RestoreTask());
        
        /*
         * The NSS will start on each service in the quorum. However,
         * only the leader will create the default KB (if that option is
         * configured).
         */
        try {

            startNSS();

        } catch (Exception e1) {

            log.error("Could not start NanoSparqlServer: " + e1, e1);

        }

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to tear down the {@link NanoSparqlServer}, the {@link Quorum},
     * and the {@link HAJournal}.
     */
    @Override
    protected void beforeShutdownHook(final boolean destroy) {

        if (log.isInfoEnabled())
            log.info("destroy=" + destroy);

        if (jettyServer != null) {

            try {

                // Shutdown the embedded NSS.
                jettyServer.stop();

                // Wait for it to terminate.
                jettyServer.join();

                // Clear reference.
                jettyServer = null;

            } catch (Exception e) {

                log.error(e, e);

            }

        }

        if (quorumService != null) {

            /*
             * FIXME SHUTDOWN: What if we are already running a ShutdownTask? We
             * should just submit a ShutdownTask here and let it work this out.
             */

            /*
             * Ensure that the HAQuorumService will not attempt to cure any
             * serviceLeave or related actions.
             * 
             * TODO SHUTDOWN: If we properly enter a ShutdownTask run state then
             * we would not have to do this since it will already be in the
             * Shutdown runstate.
             */
            quorumService.runStateRef
                    .set(HAQuorumService.RunStateEnum.Shutdown);

            /*
             * Terminate any running task.
             */
            final FutureTask<?> ft = quorumService.runStateFutureRef.get();

            if (ft != null) {

                ft.cancel(true/* mayInterruptIfRunning */);

            }
            
        }

        final HAJournal tjournal = journal;

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = tjournal == null ? null
                : tjournal.getQuorum();

        if (quorum != null) {
            
            try {

                /*
                 * Terminate the watchers and threads that are monitoring and
                 * maintaining our reflection of the global quorum state.
                 * 
                 * FIXME SHUTDOWN: This can deadlock if there is a concurrent
                 * attempt to addMember(), addPipeline(), castVote(),
                 * serviceJoin(), etc. The deadlock arises because we are
                 * undoing all of those things in this thread and then waiting
                 * until the Condition is satisified. With a concurrent action
                 * to do the opposite thing, we can wind up with the end state
                 * that we are not expecting and just waiting forever inside of
                 * AbstractQuorum.doConditionXXXX() for our condition variable
                 * to reach the desired state.
                 * 
                 * One possibility is to simply kill the zk connection first.
                 * 
                 * Otherwise, we need to make sure that we do not tell the
                 * quorum actor to do things that are contradictory.
                 */
                quorum.terminate();

                /*
                 * Close our zookeeper connection, invalidating all ephemeral
                 * znodes for this service.
                 * 
                 * Note: This provides a decisive mechanism for removing this
                 * service from the joined services, the pipeline, withdrawing
                 * its vote, and removing it as a quorum member.
                 */
                if (haLog.isInfoEnabled())
                    haLog.warn("FORCING UNCURABLE ZOOKEEPER DISCONNECT");
                
                if (zka != null) {

                    zka.close();
                    
                }

            } catch (Throwable t) {

                log.error(t, t);

            }
            
        }
        
        if (tjournal != null) {

            if (destroy) {

                tjournal.destroy();

            } else {

                tjournal.close();

            }

        }

    }

    /**
     * Factory for the {@link QuorumService} implementation.
     * 
     * @param logicalServiceZPath
     * @param serviceId
     * @param remoteServiceImpl
     *            The object that implements the {@link Remote} interfaces
     *            supporting HA operations.
     * @param store
     *            The {@link HAJournal}.
     */
    private HAQuorumService<HAGlue, HAJournal> newQuorumService(
            final String logicalServiceZPath,
            final UUID serviceId, final HAGlue remoteServiceImpl,
            final HAJournal store) {

        return new HAQuorumService<HAGlue, HAJournal>(logicalServiceZPath,
                serviceId, remoteServiceImpl, store, this);

    }

    /**
     * Concrete {@link QuorumServiceBase} implementation for the
     * {@link HAJournal}.
     */
    static private class HAQuorumService<S extends HAGlue, L extends HAJournal>
            extends QuorumServiceBase<S, L> {

        private final L journal;
        private final HAJournalServer server;

        /**
         * Lock to guard the HALogWriter.
         */
        private final Lock logLock = new ReentrantLock();
        
        /**
         * Future for task responsible for resynchronizing a node with
         * a met quorum.
         */
        private final AtomicReference<FutureTask<Void>> runStateFutureRef = new AtomicReference<FutureTask<Void>>(/*null*/);

        /**
         * Enum of the run states. The states are labeled by the goal of the run
         * state.
         */
        private enum RunStateEnum {
            Restore,
            SeekConsensus,
            RunMet,
            Resync,
            Rebuild, 
            Shutdown; // TODO SHUTDOWN: We are not using this systematically (no ShutdownTask for this run state).
        }
        
        private final AtomicReference<RunStateEnum> runStateRef = new AtomicReference<RunStateEnum>(
                null/* none */);

        protected void setRunState(final RunStateEnum runState) {

            if (runStateRef.get() == RunStateEnum.Shutdown) {

                final String msg = "Shutting down: can not enter runState="
                        + runState;

                haLog.warn(msg);

                throw new IllegalStateException(msg);

            }
            
            final RunStateEnum oldRunState = runStateRef.getAndSet(runState);

            if (oldRunState == runState) {

                /*
                 * Note: This can be a real problem depending on what the run
                 * state was doing.
                 */
                
                haLog.warn("Rentering same state? runState=" + runState);
                
            }

            haLog.warn("runState=" + runState + ", oldRunState=" + oldRunState
                    + ", serviceName=" + server.getServiceName());

        }
        
        private abstract class RunStateCallable<T> implements Callable<T> {
            
            /**
             * The {@link RunStateEnum} for this task.
             */
            protected final RunStateEnum runState;

            protected RunStateCallable(final RunStateEnum runState) {

                if (runState == null)
                    throw new IllegalArgumentException();
                
                this.runState = runState;
                
            }

            final public T call() throws Exception {

                setRunState(runState);

                try {

                    return doRun();

                } catch (Throwable t) {

                    if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                        // Note: This is a normal exit condition.
                        log.info("Interrupted.");

                    } else {

                        log.error(t, t);

                        /*
                         * Unhandled error. Transition to SeekConsensus!
                         */

                        enterRunState(new SeekConsensusTask());

                    }

                    if (t instanceof Exception)
                        throw (Exception) t;

                    throw new RuntimeException(t);

                } finally {

                    haLog.warn(runState + ": exit.");

                    /*
                     * Note: Do NOT clear the run state since it could have been
                     * already changed by enterRunState() for the new runState.
                     */
                    
                }

            }

            /**
             * Core method.
             * <p>
             * Note: The service will AUTOMATICALLY transition to
             * {@link RunStateEnum#SeekConsensus} if there is an abnormal exit
             * from this method UNLESS it has entered
             * {@link RunStateEnum#Shutdown}.
             * 
             * @return <T> if this is a normal exit.
             * 
             * @throws InterruptedException
             *             if this is a normal exit.
             * @throws Exception
             *             if this is an abnormal exit.
             */
            abstract protected T doRun() throws Exception;
            
            /**
             * Block the thread in an interruptable manner. This is used when a
             * task must wait until it is interrupted.
             * 
             * @throws InterruptedException
             */
            protected void blockInterruptably() throws InterruptedException {

                if (haLog.isInfoEnabled())
                    haLog.info(this.toString());

                while (true) {

                    Thread.sleep(Long.MAX_VALUE);

                }

            }

            public String toString() {

                return getClass().getName() + "{runState=" + runState + "}";
                
            }
            
        } // RunStateCallable
        
        /**
         * Change the run state.
         * 
         * @param runStateTask
         *            The task for the new run state.
         */
        private void enterRunState(final RunStateCallable<Void> runStateTask) {

            if (runStateTask == null)
                throw new IllegalArgumentException();

            synchronized (runStateRef) {

                final FutureTask<Void> ft = new FutureTaskMon<Void>(
                        runStateTask);

                final Future<?> oldFuture = runStateFutureRef.get();

                boolean success = false;

                try {

                    runStateFutureRef.set(ft);

                    // submit future task.
                    journal.getExecutorService().submit(ft);

                    success = true;

//                    if (haLog.isInfoEnabled())
//                        haLog.info("Entering runState="
//                                + runStateTask.getClass().getSimpleName());

                } finally {

                    if (oldFuture != null) {

                        oldFuture.cancel(true/* interruptIfRunning */);

                    }

                    if (!success) {

                        ft.cancel(true/* interruptIfRunning */);

                        runStateFutureRef.set(null);

                    }

                }

            }

        }
        
//        /**
//         * Used to submit {@link RunStateCallable} tasks for execution from
//         * within the zk watcher thread.
//         * 
//         * @param task
//         *            The task to be run.
//         */
//        private void enterRunStateFromWatcherThread(
//                final RunStateCallable<Void> task) {
//
//            // Submit task to handle this event.
//            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
//                    new SubmitRunStateTask(task)));
//
//        }
//        
//        /**
//         * Class is used to force a run state transition based on an event
//         * received in the zk watcher thread. 
//         */
//        private class SubmitRunStateTask implements Callable<Void> {
//            private final RunStateCallable<Void> task;
//            public SubmitRunStateTask(final RunStateCallable<Void> task) {
//                this.task = task;
//            }
//            public Void call() throws Exception {
//                enterRunState(task);
//                return null;
//            }
//        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Cleans up the return type.
         */
        @SuppressWarnings("unchecked")
        @Override
        public Quorum<HAGlue,QuorumService<HAGlue>> getQuorum() {

            return (Quorum<HAGlue, QuorumService<HAGlue>>) super.getQuorum();
            
        }

        /**
         * @param logicalServiceZPath
         * @param serviceId
         * @param remoteServiceImpl
         *            The object that implements the {@link Remote} interfaces
         *            supporting HA operations.
         * @param store
         *            The {@link HAJournal}.
         */
        public HAQuorumService(final String logicalServiceZPath,
                final UUID serviceId, final S remoteServiceImpl, final L store,
                final HAJournalServer server) {

            super(logicalServiceZPath, serviceId, remoteServiceImpl, store);

            this.journal = store;
            
            this.server = server;

        }

        /**
         * Resolve an {@link HAGlue} object from its Service UUID.
         */
        @Override
        public S getService(final UUID serviceId) {
            
            final HAJournalDiscoveryClient discoveryClient = server
                    .getDiscoveryClient();

            final ServiceItem serviceItem = discoveryClient
                    .getServiceItem(serviceId);
            
            if (serviceItem == null) {

                // Not found (per the API).
                throw new QuorumException("Service not found: uuid="
                        + serviceId);

            }

            @SuppressWarnings("unchecked")
            final S service = (S) serviceItem.service;

            return service;
            
        }

        @SuppressWarnings("unchecked")
        protected void doLocalCommit(final IRootBlockView rootBlock) {

            journal.doLocalCommit((QuorumService<HAGlue>) HAQuorumService.this,
                    rootBlock);
            
        }

//        @Override
//        public void start(final Quorum<?,?> quorum) {
//            
//            if (haLog.isTraceEnabled())
//                log.trace("START");
//            
//            super.start(quorum);
//
//            // Note: It appears to be a problem to do this here. Maybe because
//            // the watcher is not running yet? Could submit a task that could
//            // await an appropriate condition to start....
////            final QuorumActor<?, ?> actor = quorum.getActor();
////            actor.memberAdd();
////            actor.pipelineAdd();
////            actor.castVote(journal.getLastCommitTime());
//
////            // Inform the Journal about the current token (if any).
////            journal.setQuorumToken(quorum.token());
//            
//        }
        
        @Override
        public void quorumMeet(final long token, final UUID leaderId) {

            super.quorumMeet(token, leaderId);

            // Submit task to handle this event.
            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
                    new QuorumMeetTask(token, leaderId)));

        }

        private class QuorumMeetTask implements Callable<Void> {
            private final long token;
//            private final UUID leaderId;
            public QuorumMeetTask(final long token, final UUID leaderId) {
                this.token = token;
//                this.leaderId = leaderId;
            }
            public Void call() throws Exception {
                journal.setQuorumToken(token);
                return null;
            }
        }
        
        @Override
        public void quorumBreak() {

            super.quorumBreak();

            // Submit task to handle this event.
            server.singleThreadExecutor.execute(new MonitoredFutureTask<Void>(
                    new QuorumBreakTask()));

        }

        private class QuorumBreakTask implements Callable<Void> {
            public Void call() throws Exception {
                journal.setQuorumToken(Quorum.NO_QUORUM);
                try {
                    journal.getHALogWriter().disable();
                } catch (IOException e) {
                    haLog.error(e, e);
                }
                enterRunState(new SeekConsensusTask());
                return null;
            }
        }
        
        /**
         * If there is a fully met quorum, then we can purge all HA logs
         * <em>EXCEPT</em> the current one.
         */
        @Override
        public void serviceJoin() {

            super.serviceJoin();

            if (false) {
                /*
                 * TODO BACKUP: Purge of HALog files on a fully met quorum is
                 * current disabled. Enable, but review implications in more
                 * depth first.
                 */
                final long token = getQuorum().token();

                if (getQuorum().isQuorumFullyMet(token)) {

                    purgeHALogs(false/* includeCurrent */);

                }
            }

        }

        /**
         * Task to handle a quorum break event.
         */
        private class SeekConsensusTask extends RunStateCallable<Void> {

            protected SeekConsensusTask() {

                super(RunStateEnum.SeekConsensus);
                
            }

            @Override
            public Void doRun() throws Exception {

                /*
                 * Pre-conditions.
                 */
                {

                    final long token = getQuorum().token();

                    if (isJoinedMember(token)) // not joined.
                        throw new IllegalStateException();

                    if (getQuorum().getCastVote(getServiceId()) != null) {
                        // no vote cast.
                        throw new IllegalStateException();
                    }

                    if (journal.getHALogWriter().isOpen())
                        throw new IllegalStateException();

                }

                /*
                 * Attempt to add the service as a member, add the service to
                 * the pipeline, and conditionally cast a vote for the last
                 * commit time IFF the quorum has NOT met.
                 * 
                 * If the service is already a quorum member or already in the
                 * pipeline, then those are NOPs.
                 * 
                 * If the quorum is already met, then the service DOES NOT cast
                 * a vote for its lastCommitTime - it will need to resynchronize
                 * instead.
                 */

                // ensure member.
                getActor().memberAdd();

                // ensure in pipeline.
                getActor().pipelineAdd();

                {

                    final long token = getQuorum().token();

                    if (token != Quorum.NO_QUORUM) {

                        /*
                         * Quorum is already met.
                         */
                        
                        // Resync since quorum met before we cast a vote.
                        enterRunState(new ResyncTask(token));

                        // Done.
                        return null;
                        
                    }
                    
                }

                /*
                 * Cast a vote for our lastCommitTime.
                 * 
                 * Note: If the quorum is already met, then we MUST NOT cast a
                 * vote for our lastCommitTime since we can not join the met
                 * quorum without attempting to synchronize.
                 */

                final long lastCommitTime = journal.getLastCommitTime();

                getActor().castVote(lastCommitTime);

                // await the quorum meet.
                final long token = getQuorum().awaitQuorum();

                if (!isJoinedMember(token)) {

                    /*
                     * The quorum met on a different vote.
                     */

                    // Resync with the quorum.
                    enterRunState(new ResyncTask(token));

                }

                final UUID leaderId = getQuorum().getLeaderId();

                // Transition to RunMet.
                enterRunState(new RunMetTask(token, leaderId));

                // Done.
                return null;
                
            }

        }

        /**
         * While the quorum is met, accept replicated writes, laying them down
         * on the HALog and the backing store, and participate in the 2-phase
         * commit protocol.
         */
        private class RunMetTask extends RunStateCallable<Void> {

            private final long token;
            private final UUID leaderId;

            public RunMetTask(final long token, final UUID leaderId) {
                
                super(RunStateEnum.RunMet);

                this.token = token;
                this.leaderId = leaderId;
                
            }
            
            @Override
            public Void doRun() throws Exception {

                /*
                 * Guards on entering this run state.
                 */
                {

                    // Verify leader is still the same.
                    if (!leaderId.equals(getQuorum().getLeaderId()))
                        throw new InterruptedException();

                    // Verify we are talking about the same token.
                    getQuorum().assertQuorum(token);

                    if (!isJoinedMember(token)) {
                        /*
                         * The quorum met, but we are not in the met quorum.
                         * 
                         * Note: We need to synchronize in order to join an
                         * already met quorum. We can not just vote our
                         * lastCommitTime. We need to go through the
                         * synchronization protocol in order to make sure that
                         * we actually have the same durable state as the met
                         * quorum.
                         */
                        throw new InterruptedException();
                    }

                } // validation of pre-conditions.

                // Block until this run state gets interrupted.
                blockInterruptably();
                
                // Done.
                return null;
                
            } // call()
            
        } // class RunMetTask

        /**
         * Run state responsible for replaying local HALog files during service
         * startup. This allows an offline restore of the backing journal file
         * (a full backup) and zero or more HALog files (each an incremental
         * backup corresponding to a single commit point). When the service
         * starts, it will replay each HALog file for the successor of its then
         * current commit counter.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        private class RestoreTask extends RunStateCallable<Void> {
         
            protected RestoreTask() {

                super(RunStateEnum.Restore);

            }

            @Override
            protected Void doRun() throws Exception {
                /*
                 * FIXME RESTORE: Enable restore and test. There is a problem
                 * with replication of the WORM HALog files and backup/restore.
                 * The WORM HALog files currently do not have the actual data on
                 * the leader. This makes some of the code messier and also
                 * means that the HALog files can not be binary equals on the
                 * leader and follower and could cause problems if people
                 * harvest them from the file system directly rather than
                 * through sendHALogFile() since they will be missing the
                 * necessary state in the file system if they were put there by
                 * the leader.
                 */
                if(false)
                while (true) {

                    long commitCounter = journal.getRootBlockView()
                            .getCommitCounter();

                    try {

                        final IHALogReader r = journal.getHALogWriter()
                                .getReader(commitCounter + 1);

                        applyHALog(r);

                        doLocalCommit(r.getClosingRootBlock());

                    } catch (FileNotFoundException ex) {

                        /*
                         * No such HALog file. Ignore and exit this loop.
                         */
                        break;

                    } catch (IOException ex) {

                        log.error("Problem reading HALog file: commitCounter="
                                + commitCounter + ": " + ex, ex);
                        
                        break;

                    }

                }

                // Submit task to seek consensus.
                enterRunState(new SeekConsensusTask());

                // Done.
                return null;

            }

            /**
             * Apply the write set to the local journal.
             * 
             * @param r
             *            The {@link IHALogReader} for the HALog file containing
             *            the write set.
             *            
             * @throws IOException
             * @throws InterruptedException
             */
            private void applyHALog(final IHALogReader r) throws IOException,
                    InterruptedException {

                final IBufferAccess buf = DirectBufferPool.INSTANCE.acquire();

                try {

                    while (r.hasMoreBuffers()) {

//                        // IHABufferStrategy
//                        final IHABufferStrategy strategy = journal
//                                .getBufferStrategy();

                        // get message and fill write cache buffer (unless
                        // WORM).
                        final IHAWriteMessage msg = r.processNextBuffer(buf
                                .buffer());

                        writeWriteCacheBlock(msg, buf.buffer());
                        
                    }

                    haLog.warn("Applied HALog: closingCommitCounter="
                            + r.getClosingRootBlock().getCommitCounter());

                } finally {

                    buf.release();

                }
            }

        }
        
        /**
         * Rebuild the backing store from scratch.
         * <p>
         * If we can not replicate ALL log files for the commit points that we
         * need to catch up on this service, then we can not incrementally
         * resynchronize this service and we will have to do a full rebuild of
         * the service instead.
         * <p>
         * A rebuild begins by pinning the history on the quorum by asserting a
         * read lock (a read-only tx against then current last commit time).
         * This prevents the history from being recycled, but does not prevent
         * concurrent writes on the existing backing store extent, or extension
         * of the backing store. This read lock is asserted by the quourm leader
         * when we ask it to send its backing store over the pipeline.
         * <p>
         * The copy backing file will be consistent with the root blocks on the
         * leader at the point where we have taken the read lock. Once we have
         * copied the backing file, we then put down the root blocks reported
         * back from the {@link HAPipelineGlue#sendHAStore(IHARebuildRequest)}
         * method.
         * <p>
         * At this point, we are still not up to date. We transition to
         * {@link ResyncTask} to finish catching up with the quorum.
         * <p>
         * Note: Rebuild only guarantees consistent data, but not binary
         * identity of the backing files since there can be ongoing writes on
         * the leader.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        private class RebuildTask extends RunStateCallable<Void> {

            /**
             * The quorum token in effect when we began the rebuild.
             */
            private final long token;

            /**
             * The quorum leader. This is fixed until the quorum breaks.
             */
            private final S leader;
            
            public RebuildTask(final long token) {

                super(RunStateEnum.Rebuild);
                
                // run while quorum is met.
                this.token = token;

                // The leader for that met quorum (RMI interface).
                leader = getLeader(token);

            }

            protected Void doRun() throws Exception {

                /*
                 * DO NOT start a rebuild if the quorum is broken. Instead, we
                 * will try again after SeekConsensus.
                 */
                getQuorum().assertQuorum(token);
                
                /*
                 * Rebuild needs to throw away anything that is buffered on the
                 * local backing file to prevent any attempts to interpret the
                 * data on the backing file in light of its current root blocks.
                 * To do this, we overwrite the root blocks.
                 */
                {

                    // The current root block.
                    final IRootBlockView rb = journal.getRootBlockView();

                    // Use timestamp.
                    final long createTime = System.currentTimeMillis();

                    // New root blocks for a (logically) empty Journal.
                    final RootBlockUtility rbu = new RootBlockUtility(journal
                            .getBufferStrategy().getBufferMode(),
                            rb.getOffsetBits(), createTime, token, rb.getUUID());

                    /*
                     * Install both root blocks.
                     * 
                     * Note: This will take us through a local abort. That is
                     * important. We need to discard any writes that might have
                     * been buffered before we start the resynchronization of
                     * the local store.
                     */
                    installRootBlocks(rbu.rootBlock0, rbu.rootBlock1);

                }

                /*
                 * Replicate the backing store of the leader.
                 * 
                 * Note: This remoteFuture MUST be cancelled if the RebuildTask
                 * is interrupted.
                 */
                final Future<IHASendStoreResponse> remoteFuture = leader
                        .sendHAStore(new HARebuildRequest(getServiceId()));

                final IHASendStoreResponse resp;
                
                try {

                    // Wait for the raw store to be replicated.
                    resp = remoteFuture.get();
                    
                } finally {

                    // Ensure remoteFuture is cancelled.
                    remoteFuture.cancel(true/* mayInterruptIfRunning */);
                    
                }
                
                // Caught up on the backing store as of that copy.
                installRootBlocks(resp.getRootBlock0(), resp.getRootBlock1());

                // Resync.
                enterRunState(new ResyncTask(token));

                // Done.
                return null;
                
            }
            
        } // class RebuildTask
        
        /**
         * This class handles the resynchronization of a node that is not at the
         * same commit point as the met quorum. The task will replicate write
         * sets (HA Log files) from the services in the met quorum, and apply
         * those write sets (in pure commit sequence) in order to advance its
         * own committed state to the same last commit time as the quorum
         * leader. Once it catches up with the quorum, it still needs to
         * replicate logged writes from a met services in the quorum until it
         * atomically can log from the write pipeline rather than replicating a
         * logged write. At that point, the service can vote its lastCommitTime
         * and will join the met quorum.
         * <p>
         * Note: In fact, the service can always begin logging from the write
         * pipeline as soon as it observes (or acquires) the root block and
         * observes the seq=0 write cache block (or otherwise catches up with
         * the write pipeline). However, it can not write those data onto the
         * local journal until it is fully caught up. This optimization might
         * allow the service to catch up slightly faster, but the code would be
         * a bit more complex.
         */
        private class ResyncTask extends RunStateCallable<Void> {

            /**
             * The quorum token in effect when we began the resync.
             */
            private final long token;

            /**
             * The quorum leader. This is fixed until the quorum breaks.
             */
            private final S leader;
            
            public ResyncTask(final long token) {
                
                super(RunStateEnum.Resync);

                // run while quorum is met.
                this.token = token;

                // The leader for that met quorum (RMI interface).
                leader = getLeader(token);

            }

            /**
             * Replicate each write set for commit points GT the current commit
             * point on this service. As each write set is replicated, is also
             * applied and we advance to another commit point. This method loops
             * until we have all write sets locally replicated and the service
             * is able to begin accepting write cache blocks from the write
             * pipeline rather than through the resynchronization protocol.
             * <p>
             * Note: This task will be interrupted when the service catches up
             * and is able to log and write the write cache block from the write
             * pipeline. At that point, we no longer need to replicate the write
             * sets from the leader.
             * 
             * @throws Exception
             */
            protected Void doRun() throws Exception {

//                // Wait for the token to be set, root blocks to be valid.
//                awaitJournalToken(token, true/* awaitRootBlocks */);
                pipelineSetup();

                /*
                 * Note: We need to discard any writes that might have been
                 * buffered before we start the resynchronization of the local
                 * store. Otherwise they could wind up flushed through by the
                 * RWStore (for example, when handling a file extension).
                 * 
                 * Note: This IS necessary. We do a low-level abort when we
                 * install the root blocks from the quorum leader before we sync
                 * the first commit point, but we do not do the low-level abort
                 * if we already have the correct root blocks in place.
                 */

                journal.doLocalAbort();

                /*
                 * We will do a local commit with each HALog (aka write set)
                 * that is replicated. This let's us catch up incrementally with
                 * the quorum.
                 */

                // Until joined with the met quorum.
                while (!getQuorum().getMember().isJoinedMember(token)) {

                    // The current commit point on the local store.
                    final long commitCounter = journal.getRootBlockView()
                            .getCommitCounter();

                    // Replicate and apply the next write set
                    replicateAndApplyWriteSet(leader, token, commitCounter + 1);

                }

                // Done
                return null;

            }

        } // class ResyncTask

        /**
         * Replicate the write set having the specified commit counter, applying
         * each {@link WriteCache} block as it is received and eventually going
         * through a local commit when we receiving the closing
         * {@link IRootBlockView} for that write set.
         * 
         * @param leader
         *            The quorum leader.
         * @param token
         *            The quorum token that must remain valid throughout this
         *            operation.
         * @param closingCommitCounter
         *            The commit counter for the <em>closing</em> root block of
         *            the write set to be replicated.
         * 
         * @throws IOException
         * @throws FileNotFoundException
         * @throws ExecutionException
         * @throws InterruptedException
         */
        private void replicateAndApplyWriteSet(final S leader,
                final long token, final long closingCommitCounter)
                throws FileNotFoundException, IOException,
                InterruptedException, ExecutionException {

            if (leader == null)
                throw new IllegalArgumentException();

            if (closingCommitCounter <= 0)
                throw new IllegalArgumentException();

            // Abort if the quorum breaks.
            getQuorum().assertQuorum(token);

            if (haLog.isInfoEnabled())
                haLog.info("RESYNC: commitCounter=" + closingCommitCounter);

            final IHALogRootBlocksResponse resp;
            try {
                
                // Request the root blocks for the write set.
                resp = leader
                        .getHALogRootBlocksForWriteSet(new HALogRootBlocksRequest(
                                closingCommitCounter));

            } catch (FileNotFoundException ex) {

                /*
                 * Oops. The leader does not have that log file.
                 * 
                 * We will have to rebuild the service from scratch since we do
                 * not have the necessary HA Log files to synchronize with the
                 * existing quorum.
                 * 
                 * Note: If we are already doing a REBUILD (incremental=false),
                 * then we will restart the REBUILD. Rationale: if someone
                 * restarts the service after the shutdown, it is going to do
                 * another REBUILD anyway. Likely causes for the missing HALog
                 * on the leader are file system errors or someone deleting the
                 * HALog files that we need. However, REBUILD is still
                 * semantically correct so long as we restart the procedure.
                 * 
                 * TODO RESYNC: It is possible to go to another service in the
                 * met quorum for the same log file, but it needs to be a
                 * service that is UPSTREAM of this service.
                 */

                final String msg = "HALog not available: commitCounter="
                        + closingCommitCounter;

                log.error(msg);

                enterRunState(new RebuildTask(token));

                // Force immediate exit of the resync protocol.
                throw new InterruptedException(msg);
                
            }

            // root block when the quorum started that write set.
            final IRootBlockView openRootBlock = resp.getOpenRootBlock();
            final IRootBlockView tmpCloseRootBlock = resp.getCloseRootBlock();

            if (openRootBlock.getCommitCounter() != closingCommitCounter - 1) {
                
                /*
                 * The opening root block for the write set must have a
                 * commit counter that is ONE less than the requested commit
                 * point.
                 */
                
                throw new AssertionError(
                        "Should start at the previous commit point: requested commitCounter="
                                + closingCommitCounter + ", openRootBlock="
                                + openRootBlock);
            }
            
            /*
             * If the local journal is empty, then we need to replace both of
             * it's root blocks with the opening root block.
             */
            if (closingCommitCounter == 1) {

                // Install the initial root blocks.
                installRootBlocks(
                        openRootBlock.asRootBlock(true/* rootBlock0 */),
                        openRootBlock.asRootBlock(false/* rootBlock0 */));

            }

            // Make sure we have the correct HALogWriter open.
            // TODO Replace with pipelineSetup()?
            logLock.lock();
            try {
                final HALogWriter logWriter = journal.getHALogWriter();
                logWriter.disable();
                logWriter.createLog(openRootBlock);
            } finally {
                logLock.unlock();
            }

            /*
             * Atomic decision whether HALog *was* for live write set when rbs
             * were obtained from leader.
             * 
             * Note: Even if true, it is possible that the write set could have
             * been committed since then. However, if false then guaranteed that
             * this write set is historical.
             */
            final boolean liveHALog = openRootBlock.getCommitCounter() == tmpCloseRootBlock
                    .getCommitCounter();

            if (liveHALog) {

                /*
                 * If this was the live write set at the moment when we
                 * requested the root blocks from the leader, then we want to
                 * decide whether or not there have been writes on the leader
                 * for that write set. If there have NOT been any replicated
                 * writes observed on the pipeline for that commitCounter, then
                 * the leader might be quiescent. In this case we want to join
                 * immediately since who knows when the next write cache block
                 * will come through (maybe never if the deployment is
                 * effectively read-only).
                 */

                if (conditionalJoinWithMetQuorum(leader, token,
                        closingCommitCounter - 1)) {

                    /*
                     * We are caught up and have joined the met quorum.
                     * 
                     * Note: Future will be canceled in finally clause.
                     */

                    throw new InterruptedException("Joined with met quorum.");

                }

            }

            /*
             * We need to transfer and apply the write cache blocks from the HA
             * Log file on some service in the met quorum. This code works with
             * the leader, which is known to be upstream from all other services
             * in the write pipeline. Replicating from the leader is simpler
             * conceptually and makes for simpler code.
             */
            final IRootBlockView closeRootBlock = replicateAndApplyHALog(
                    leader, closingCommitCounter, resp);

            // Local commit.
            doLocalCommit(closeRootBlock);

            // Close out the current HALog writer.
            logLock.lock();
            try {
                final HALogWriter logWriter = journal.getHALogWriter();
                logWriter.closeLog(closeRootBlock);
            } finally {
                logLock.unlock();
            }

            if (haLog.isInfoEnabled())
                haLog.info("Replicated write set: commitCounter="
                        + closingCommitCounter);

        }

        private IRootBlockView replicateAndApplyHALog(final S leader,
                final long closingCommitCounter,
                final IHALogRootBlocksResponse resp) throws IOException,
                InterruptedException, ExecutionException {

            /*
             * Request the HALog from the leader.
             */
            {

                Future<Void> ft = null;
                boolean success = false;
                try {

                    if (haLog.isDebugEnabled())
                        haLog.debug("HALOG REPLICATION START: closingCommitCounter="
                                + closingCommitCounter);

                    ft = leader.sendHALogForWriteSet(new HALogRequest(
                            server.serviceUUID, closingCommitCounter
                    // , true/* incremental */
                            ));

                    // Wait until all write cache blocks are received.
                    ft.get();

                    success = true;

                    /*
                     * Note: Uncaught exception will result in SeekConsensus.
                     */

                } finally {

                    if (ft != null) {
                        // ensure terminated.
                        ft.cancel(true/* mayInterruptIfRunning */);
                    }

                    if (haLog.isDebugEnabled())
                        haLog.debug("HALOG REPLICATION DONE : closingCommitCounter="
                                + closingCommitCounter + ", success=" + success);

                }

            }

            /*
             * Figure out the closing root block. If this HALog file was active
             * when we started reading from it, then the open and close root
             * blocks would have been identical in the [resp] and we will need
             * to grab the root blocks again now that it has been closed.
             */
            final IRootBlockView closeRootBlock;
            {

                final IRootBlockView openRootBlock = resp.getOpenRootBlock();

                // root block when the quorum committed that write set.
                IRootBlockView tmp = resp.getCloseRootBlock();

                if (openRootBlock.getCommitCounter() == tmp.getCommitCounter()) {

                    /*
                     * The open and close commit counters were the same when we
                     * first requested them, so we need to re-request the close
                     * commit counter now that we are done reading on the file.
                     */

                    // Re-request the root blocks for the write set.
                    final IHALogRootBlocksResponse resp2 = leader
                            .getHALogRootBlocksForWriteSet(new HALogRootBlocksRequest(
                                    closingCommitCounter));

                    tmp = resp2.getCloseRootBlock();

                }

                closeRootBlock = tmp;

                if (closeRootBlock.getCommitCounter() != closingCommitCounter) {

                    throw new AssertionError(
                            "Wrong commitCounter for closing root block: expected commitCounter="
                                    + closingCommitCounter
                                    + ", but closeRootBlock=" + closeRootBlock);

                }

            }

            return closeRootBlock;

        }
        
        /**
         * Conditional join of a service attempting to synchronize with the met
         * quorum. If the current commit point that is being replicated (as
         * indicated by the <i>commitCounter</i>) is thought to be the most
         * current root block on the leader AND we have not received any writes
         * on the HALog, then we assume that the leader is quiescent (no write
         * activity) and we attempt to join the qourum.
         * 
         * @param leader
         *            The quorum leader.
         * @param openingCommitCounter
         *            The commit counter for the <em>opening</em> root block of
         *            the write set that is currently being replicated.
         * 
         * @return <code>true</code> iff we were able to join the met quorum.
         * 
         * @throws InterruptedException
         */
        private boolean conditionalJoinWithMetQuorum(final S leader,
                final long token, final long openingCommitCounter)
                throws IOException, InterruptedException {

//            // Get the current root block from the quorum leader.
//            final IRootBlockView currentRootBlockOnLeader = leader
//                    .getRootBlock(new HARootBlockRequest(null/* storeId */))
//                    .getRootBlock();

            final IHAWriteSetStateResponse currentWriteSetStateOnLeader = leader
                    .getHAWriteSetState(new HAWriteSetStateRequest());

            final boolean sameCommitCounter = currentWriteSetStateOnLeader
                    .getCommitCounter() == openingCommitCounter;

            if (haLog.isDebugEnabled())
                haLog.debug("sameCommitCounter=" + sameCommitCounter
                        + ", openingCommitCounter=" + openingCommitCounter
                        + ", currentWriteSetStateOnLeader="
                        + currentWriteSetStateOnLeader);

            if (!sameCommitCounter
                    || currentWriteSetStateOnLeader.getSequence() > 0L) {

                /*
                 * We can not join immediately. Either we are not at the same
                 * commit point as the quorum leader or the block sequence on
                 * the quorum leader is non-zero.
                 */

                return false;

            }
            
            /*
             * This is the same commit point that we are trying to replicate
             * right now. Check the last *live* HAWriteMessage. If we have not
             * observed any write cache blocks, then we can attempt to join the
             * met quorum.
             * 
             * Note: We can not accept replicated writes while we are holding
             * the logLock (the lock is required to accept replicated writes).
             * Therefore, by taking this lock we can make an atomic decision
             * about whether to join the met quorum.
             * 
             * Note: We did all RMIs before grabbing this lock to minimize
             * latency and the possibility for distributed deadlocks.
             */
            logLock.lock();

            try {

                final IHAWriteMessage lastLiveMsg = journal.lastLiveHAWriteMessage;

                if (lastLiveMsg != null) {

                    /*
                     * Can not join. Some write has been received. Leader has
                     * moved on.
                     * 
                     * Note: [lastLiveMsg] was cleared to [null] when we did a
                     * local abort at the top of resync() or rebuild().
                     */

                    return false;

                }
                
                final HALogWriter logWriter = journal.getHALogWriter();

                if (haLog.isDebugEnabled())
                    haLog.debug("HALog.commitCounter="
                            + logWriter.getCommitCounter()
                            + ", HALog.getSequence="
                            + logWriter.getSequence());

                if (logWriter.getCommitCounter() != openingCommitCounter
                        || logWriter.getSequence() != 0L) {

                    /*
                     * We verified the last live message above (i.e., none)
                     * while holding the [logLock]. The HALogWriter must be
                     * consistent with the leader at this point since we are
                     * holding the [logLock] and that lock is blocking pipeline
                     * replication.
                     */

                    throw new AssertionError("openingCommitCount="
                            + openingCommitCounter
                            + ", logWriter.commitCounter="
                            + logWriter.getCommitCounter()
                            + ", logWriter.sequence=" + logWriter.getSequence());

                }

                if (haLog.isInfoEnabled())
                    haLog.info("Will attempt to join met quorum.");

                final UUID leaderId = getQuorum().getLeaderId();
                
                // The vote cast by the leader.
                final long lastCommitTimeOfQuorumLeader = getQuorum()
                        .getCastVote(leaderId);

                // Verify that the quorum is valid.
                getQuorum().assertQuorum(token);

                // Cast that vote.
                getActor().castVote(lastCommitTimeOfQuorumLeader);

                // Verify that the quorum is valid.
                getQuorum().assertQuorum(token);

                // Attempt to join the met quorum.
                getActor().serviceJoin();

                // Verify that the quorum is valid.
                getQuorum().assertQuorum(token);

                haLog.warn("Joined met quorum: runState=" + runStateRef
                        + ", commitCounter=" + openingCommitCounter
                        + ", lastCommitTimeOfLeader="
                        + lastCommitTimeOfQuorumLeader + ", nextBlockSeq="
                        + logWriter.getSequence());

                // Verify that the quorum is valid.
                getQuorum().assertQuorum(token);

                enterRunState(new RunMetTask(token, leaderId));
                
                // Done with resync.
                return true;

            } finally {

                logLock.unlock();

            }
            
        }

        
        /**
         * Make sure the pipeline is setup properly.
         * <p>
         * We need to block until the quorum meet has caused the quorumToken to
         * be set on the Journal and the HALogWriter to be configured so we can
         * begin to accept replicated live writes.
         * <p>
         * Also, if this is the first quorum meet (commit counter is ZERO (0)),
         * then we need to make sure that the root blocks have been replicated
         * from the leader before proceeding.
         * 
         * @throws IOException
         * @throws FileNotFoundException
         * @throws InterruptedException
         */
        private void pipelineSetup() throws FileNotFoundException, IOException,
                InterruptedException {

            // The current token (if any).
            final long token = getQuorum().token();

            // Verify that the quorum is met.
            getQuorum().assertQuorum(token);

            // Verify that we have valid root blocks
            awaitJournalToken(token);

            logLock.lock();
            
            try {

                final HALogWriter logWriter = journal.getHALogWriter();

                if (!logWriter.isOpen()) {
             
                    /*
                     * Open the HALogWriter for our current root blocks.
                     * 
                     * Note: We always use the current root block when receiving
                     * an HALog file, even for historical writes. This is
                     * because the historical log writes occur when we ask the
                     * leader to send us a prior commit point in RESYNC.
                     */
                    
                    journal.getHALogWriter().createLog(
                            journal.getRootBlockView());
                    
                }

            } finally {
            
                logLock.unlock();
                
            }

        }
        
        @Override
        protected void handleReplicatedWrite(final IHASyncRequest req,
                final IHAWriteMessage msg, final ByteBuffer data)
                throws Exception {

            pipelineSetup();
            
            logLock.lock();
            try {

                if (haLog.isDebugEnabled())
                    haLog.debug("msg=" + msg + ", buf=" + data);

                if (req == null) {

                    // Save off reference to most recent *live* message.
                    journal.lastLiveHAWriteMessage = msg;
                    
                }
                
                if (req != null && req instanceof IHARebuildRequest) {

                    /*
                     * This message and payload are part of a ground up service
                     * rebuild (disaster recovery from the quorum) rather than
                     * an incremental resynchronization.
                     * 
                     * Note: HALog blocks during rebuild are written onto the
                     * appropriate HALog file using the same rules that apply to
                     * resynchronization. We also capture the root blocks for
                     * those replicated HALog files. However, during a REBUILD,
                     * we DO NOT go through a local commit for the replicated
                     * HALog files until the service is fully synchronized. This
                     * prevents the service from having root blocks that are
                     * "non-empty" when the file state is not fully consistent
                     * with the leader.
                     * 
                     * Note: Replicated backing store write blocks MUST be
                     * written directly onto the backing FileChannel after
                     * correcting the offset (which is relative to the root
                     * block). Even for the RWStore, the replicated backing
                     * store blocks represent a contiguous extent on the file
                     * NOT scattered writes.
                     * 
                     * REBUILD : Replicated HALog write blocks are handled just
                     * like resync (see below).
                     */

                    journal.getBufferStrategy().writeRawBuffer(
                            (HARebuildRequest) req, msg, data);
                    
                    return;

                }

                final HALogWriter logWriter = journal.getHALogWriter();

                assert logWriter.isOpen();
                
                if (msg.getCommitCounter() == logWriter.getCommitCounter()
                        && msg.getSequence() == (logWriter.getSequence() - 1)) {

                    /*
                     * Duplicate message. This can occur due retrySend() in
                     * QuorumPipelineImpl#replicate(). retrySend() is used to
                     * make the pipeline robust if a service (other than the
                     * leader) drops out and we need to change the connections
                     * between the services in the write pipeline in order to
                     * get the message through.
                     */

                    if (log.isInfoEnabled())
                        log.info("Ignoring message (dup): " + msg);

                    return;

                }

                final RunStateEnum runState = runStateRef.get();

                if (RunStateEnum.Resync.equals(runState)) {

                    /*
                     * If we are resynchronizing, then pass ALL messages (both
                     * live and historical) into handleResyncMessage().
                     * 
                     * Note: This method handles the transition into the met
                     * quorum when we observe a LIVE message that is the
                     * successor of the last received HISTORICAL message. This
                     * is the signal that we are caught up on the writes on the
                     * met quorum and may join.
                     */
                    
                    handleResyncMessage((IHALogRequest) req, msg, data);

                } else if (journal.getRootBlockView().getCommitCounter() == msg.getCommitCounter()
                        && isJoinedMember(msg.getQuorumToken())) {

                    /*
                     * We are not resynchronizing this service. This is a
                     * message for the current write set. The service is joined
                     * with the quorum.
                     */

                    // write on the log and the local store.
                    acceptHAWriteMessage(msg, data);

                } else {
                    
                    if (log.isInfoEnabled())
                        log.info("Ignoring message: " + msg);
                    
                    /*
                     * Drop the pipeline message.
                     * 
                     * Note: There are two cases here.
                     * 
                     * (A) It is a historical message that is being ignored on
                     * this node;
                     * 
                     * (B) It is a live message, but this node is not caught up
                     * and therefore can not log the message yet.
                     */
                    
                }

            } finally {

                logLock.unlock();

            }

        }
        
        /**
         * Adjust the size on the disk of the local store to that given in the
         * message.
         * <p>
         * Note: When historical messages are being replayed, the caller needs
         * to decide whether the message should applied to the local store. If
         * so, then the extent needs to be updated. If not, then the message
         * should be ignored (it will already have been replicated to the next
         * follower).
         */
        private void setExtent(final IHAWriteMessage msg) throws IOException {

            try {

                ((IHABufferStrategy) journal.getBufferStrategy())
                        .setExtentForLocalStore(msg.getFileExtent());

            } catch (InterruptedException e) {

                throw new RuntimeException(e);

            } catch (RuntimeException t) {

                // Wrap with the HA message.
                throw new RuntimeException("msg=" + msg + ": " + t, t);
                
            }

        }

        /**
         * Handle a replicated write requested to resynchronize this service
         * with the quorum. The {@link WriteCache} messages for HA Logs are
         * delivered over the write pipeline, along with messages for the
         * current write set. This method handles those that are for historical
         * write sets (replayed from HA Log files) as well as those that are
         * historical writes for the current write set (that is, messages that
         * this service missed because it joined the write pipeline after the
         * first write message for the current write set and was thus not able
         * to log and/or apply the write message even if it did observe it).
         * <p>
         * Note: The quorum token associated with historical message needs to be
         * ignored. The quorum could have broken and met again since, in which
         * case any attempt to use that old token will cause a QuorumException.
         * 
         * @throws InterruptedException
         * @throws IOException
         */
        private void handleResyncMessage(final IHALogRequest req,
                final IHAWriteMessage msg, final ByteBuffer data)
                throws IOException, InterruptedException {

            logLock.lock();

            try {

                final HALogWriter logWriter = journal.getHALogWriter();

                if (req == null) {
                    
                    /*
                     * Live message.
                     */

                    if ((msg.getCommitCounter() == journal.getRootBlockView().getCommitCounter())
                    && ((msg.getSequence() + 1) == logWriter.getSequence())) {

                        /*
                         * We just received a live message that is the successor
                         * of the last resync message. We are caught up. We need
                         * to log and apply this live message, cancel the resync
                         * task, and enter RunMet.
                         */

                        resyncTransitionToMetQuorum(msg, data);

                        return;

                    } else {

                        /*
                         * Drop live messages since we are not caught up.
                         */

                        if (haLog.isDebugEnabled())
                            log.debug("Ignoring write cache block: msg=" + msg);

                        return;

                    }

                } else {

                    /*
                     * A historical message (replay of an HALog file).
                     * 
                     * Note: We will see ALL messages. We can only log the
                     * message if it is for our commit point.
                     */

                    if (!server.serviceUUID.equals(req.getServiceId())) {

                        /*
                         * Not our request. Drop the message.
                         */
                        
                        if (haLog.isDebugEnabled())
                            log.debug("Ignoring write cache block: msg=" + msg);

                        return;

                    }

                    // log and write cache block.
                    acceptHAWriteMessage(msg, data);

                }
                
            } finally {

                logLock.unlock();

            }

        }

        /**
         * Atomic transition to the met quorum, invoked when we receive the same
         * sequence number for some {@link WriteCache} block in the current
         * write set twice. This happens when we get it once from the explicit
         * resynchronization task and once from the normal write pipeline
         * writes. In both cases, the block is transmitted over the write
         * pipeline. The double-presentation of the block is our signal that we
         * are caught up with the normal write pipeline writes.
         */
        private void resyncTransitionToMetQuorum(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            final HALogWriter logWriter = journal.getHALogWriter();
            
            final IRootBlockView rootBlock = journal.getRootBlockView();

            if (logWriter.getCommitCounter() != rootBlock.getCommitCounter()) {

                throw new AssertionError("HALogWriter.commitCounter="
                        + logWriter.getCommitCounter() + ", but rootBlock="
                        + rootBlock);

            }

            if (msg.getCommitCounter() != rootBlock.getCommitCounter()
                    || msg.getLastCommitTime() != rootBlock.getLastCommitTime()) {

                throw new AssertionError("msg=" + msg + ", but rootBlock="
                        + journal.getRootBlockView());

            }

            /*
             * Service is not joined but is caught up with the write
             * pipeline and is ready to join.
             */

            // Accept the message - log and apply.
            acceptHAWriteMessage(msg, data);

            // Vote the consensus for the met quorum.
            final Quorum<?, ?> quorum = getQuorum();
            final UUID leaderId = quorum.getLeaderId();
            final long token = msg.getQuorumToken();
            quorum.assertQuorum(token);
            // Note: Concurrent quorum break will cause NPE here.
            final long leadersVote = quorum.getCastVote(leaderId);
            getActor().castVote(leadersVote);
            // getActor().castVote(rootBlock.getLastCommitTime());

            log.warn("Service voted for lastCommitTime of quorum, is receiving pipeline writes, and should join the met quorum");

            /*
             * Attempt to join.
             * 
             * Note: This will throw an exception if this services is not in the
             * consensus.
             * 
             * Note: We are BLOCKING the pipeline while we wait here (since we
             * are handling a replicated write).
             * 
             * TODO What happens if we are blocked here?
             */
            getActor().serviceJoin();

            // Transition to RunMet.
            enterRunState(new RunMetTask(token, leaderId));

        }
        
        /**
         * Verify commitCounter in the current log file and the message are
         * consistent, then log and apply the {@link WriteCache} block.
         */
        private void acceptHAWriteMessage(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            final HALogWriter logWriter = journal.getHALogWriter();

            if (msg.getCommitCounter() != logWriter.getCommitCounter()) {

                throw new AssertionError();

            }

            /*
             * Log the message and write cache block.
             */
            logWriteCacheBlock(msg, data);

            writeWriteCacheBlock(msg, data);

        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Writes the {@link IHAWriteMessage} and the data onto the
         * {@link HALogWriter}
         */
        @Override
        public void logWriteCacheBlock(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException {

            try {

                // Make sure the pipeline is setup properly.
                pipelineSetup();
                
            } catch (InterruptedException e) {
                
                // Propagate the interrupt.
                Thread.currentThread().interrupt();
                
                return;
                
            }
            
            logLock.lock();

            try {

                journal.getHALogWriter().write(msg, data);
                
            } finally {
                
                logLock.unlock();
                
            }
            
        }

        /**
         * Write the raw {@link WriteCache} block onto the backing store.
         */
        private void writeWriteCacheBlock(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            setExtent(msg);
            
            /*
             * Note: the ByteBuffer is owned by the HAReceiveService. This just
             * wraps up the reference to the ByteBuffer with an interface that
             * is also used by the WriteCache to control access to ByteBuffers
             * allocated from the DirectBufferPool. However, release() is a NOP
             * on this implementation since the ByteBuffer is owner by the
             * HAReceiveService.
             */
            
            final IBufferAccess b = new IBufferAccess() {

                @Override
                public void release(long timeout, TimeUnit unit)
                        throws InterruptedException {
                    // NOP
                }

                @Override
                public void release() throws InterruptedException {
                    // NOP
                }

                @Override
                public ByteBuffer buffer() {
                    return data;
                }
            };

            ((IHABufferStrategy) journal.getBufferStrategy())
                    .writeRawBuffer(msg, b);
            
        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Writes the root block onto the {@link HALogWriter} and closes the log
         * file. A new log is then opened, using the given root block as the
         * starting point for that log file.
         */
        @Override
        public void logRootBlock(final IRootBlockView rootBlock) throws IOException {

            logLock.lock();

            try {

                // Close off the old log file with the root block.
                journal.getHALogWriter().closeLog(rootBlock);
                
                // Open up a new log file with this root block.
                journal.getHALogWriter().createLog(rootBlock);
                
            } finally {
                
                logLock.unlock();
                
            }
            
        }

        /**
         * {@inheritDoc}
         * <p>
         * Destroys the HA log files in the HA log directory.
         * 
         * TODO BACKUP: This already parses the closing commit counter out of
         * the HALog filename. To support backup, we MUST NOT delete an HALog
         * file that is being retained by the backup retention policy. This will
         * be coordinated through zookeeper.
         * 
         * <pre>
         * logicalServiceZPath/backedUp {inc=CC;full=CC}
         * </pre>
         * 
         * where
         * <dl>
         * <dt>inc</dt>
         * <dd>The commitCounter of the last incremental backup that was
         * captured</dd>
         * <dt>full</dt>
         * <dd>The commitCounter of the last full backup that was captured.</dd>
         * </dl>
         * 
         * IF this znode exists, then backups are being captured. When backups
         * are being captured, then HALog files may only be removed once they
         * have been captured by a backup. The znode is automatically created by
         * the backup utility. If you destroy the znode, it will no longer
         * assume that backups are being captured until the next time you run
         * the backup utility.
         * 
         * @see HABackupManager
         */
        @Override
        public void purgeHALogs(final boolean includeCurrent) {

            logLock.lock();

            try {

                final QuorumBackupState backupState;
                {
                    QuorumBackupState tmp = null;
                    try {
                        final String zpath = server.logicalServiceZPath + "/"
                                + ZKQuorum.QUORUM + "/"
                                + ZKQuorum.QUORUM_BACKUP;
                        tmp = (QuorumBackupState) SerializerUtil
                                .deserialize(server.zka
                                        .getZookeeper()
                                        .getData(zpath, false/* watch */, null/* stat */));
                    } catch (KeeperException.NoNodeException ex) {
                        // ignore.
                    } catch (KeeperException e) {
                        // log @ error and ignore.
                        log.error(e);
                    } catch (InterruptedException e) {
                        // Propagate interrupt.
                        Thread.currentThread().interrupt();
                        return;
                    }
                    backupState = tmp;
                }
                
                /*
                 * List the HALog files for this service.
                 */
                final File[] logFiles;
                {

                    final File currentLogFile = journal.getHALogWriter()
                            .getFile();

                    final String currentLogFileName = currentLogFile == null ? null
                            : currentLogFile.getName();

                    final File logDir = journal.getHALogDir();

                    logFiles = logDir.listFiles(new FilenameFilter() {

                        /**
                         * Return <code>true</code> iff the file is an HALog
                         * file that should be deleted.
                         * 
                         * @param name
                         *            The name of that HALog file (encodes the
                         *            commitCounter).
                         */
                        @Override
                        public boolean accept(final File dir, final String name) {

                            if (!name.endsWith(IHALogReader.HA_LOG_EXT)) {
                                // Not an HALog file.
                                return false;
                            }

                            if (!includeCurrent && currentLogFile != null
                                    && name.equals(currentLogFileName)) {
                                /*
                                 * The caller requested that we NOT purge the
                                 * current HALog, and this is it.
                                 */
                                return false;
                            }

                            // Strip off the filename extension.
                            final String logFileBaseName = name.substring(0,
                                    IHALogReader.HA_LOG_EXT.length());

                            // Closing commitCounter for HALog file.
                            final long logCommitCounter = Long
                                    .parseLong(logFileBaseName);

                            if (backupState != null) {
                                /*
                                 * Backups are being made.
                                 * 
                                 * When backups are being made, we DO NOT delete
                                 * HALog files for commit points GT this
                                 * commitCounter.
                                 */
                                if (logCommitCounter > backupState.inc()) {
                                    /*
                                     * Backups are being made and this HALog
                                     * file has not been backed up yet.
                                     */
                                    return false;
                                }
                            }

                            // This HALog file MAY be deleted.
                            return true;
                            
                        }
                    });
                    
                }

                int ndeleted = 0;
                long totalBytes = 0L;

                for (File logFile : logFiles) {

                    // #of bytes in that HALog file.
                    final long len = logFile.length();

                    if (!logFile.delete()) {

                        haLog.warn("COULD NOT DELETE FILE: " + logFile);

                        continue;

                    }

                    ndeleted++;

                    totalBytes += len;

                }

                haLog.info("PURGED LOGS: ndeleted=" + ndeleted
                        + ", totalBytes=" + totalBytes);

            } finally {

                logLock.unlock();

            }

        }

        @Override
        public void installRootBlocks(final IRootBlockView rootBlock0,
                final IRootBlockView rootBlock1) {

            journal.installRootBlocks(rootBlock0, rootBlock1);

        }

        @Override
        public File getServiceDir() {
            return server.getServiceDir();
        }
        
        /**
         * Spin until the journal.quorumToken is set (by the event handler).
         * <p>
         * Note: The {@link #quorumMeet(long, UUID)} arrives in the zookeeper
         * event thread. We can not take actions that could block in that
         * thread, so it is pumped into a single threaded executor. When that
         * executor handles the event, it sets the token on the journal. This
         * process is of necessity asynchronous with respect to the run state
         * transitions for the {@link HAJournalServer}. Futher, when the local
         * journal is empty (and this service is joined with the met quorum as a
         * follower), setting the quorum token will also cause the root blocks
         * from the leader to be installed on this service. This method is used
         * to ensure that the local journal state is consistent (token is set,
         * root blocks are have been copied if the local journal is empty)
         * before allowing certain operations to proceed.
         * 
         * @see #quorumMeet(long, UUID)
         * @see HAJournal#setQuorumToken(long)
         */
        private void awaitJournalToken(final long token) throws IOException,
                InterruptedException {
            /*
             * Note: This is called for each HA write message received. DO NOT
             * use any high latency or RMI calls unless we need to wait for the
             * root blocks. That condition is detected below without any high
             * latency operations.
             */
            S leader = null;
            IRootBlockView rbLeader = null;
            final long sleepMillis = 10;
            while (true) {
                // while token is valid.
                getQuorum().assertQuorum(token);
                final long journalToken = journal.getQuorumToken();
                if (journalToken != token) {
                    Thread.sleep(sleepMillis/* ms */);
                    continue;
                }
                if (isFollower(token)) {// if (awaitRootBlocks) {
                    final IRootBlockView rbSelf = journal.getRootBlockView();
                    if (rbSelf.getCommitCounter() == 0) {
                        /*
                         * Only wait if this is an empty Journal.
                         */
                        if (leader == null) {
                            /*
                             * RMI to the leader for its current root block
                             * (once).
                             */
                            leader = getLeader(token);
                            rbLeader = leader
                                    .getRootBlock(
                                            new HARootBlockRequest(null/* storeUUID */))
                                    .getRootBlock();
                        }
                        if (!rbSelf.getUUID().equals(rbLeader.getUUID())) {
                            Thread.sleep(sleepMillis/* ms */);
                            continue;
                        }
                    }
                }
                /*
                 * Good to go.
                 */
                if (haLog.isInfoEnabled())
                    haLog.info("Journal quorumToken is set.");
                break;
            }
        }
        
    } // class HAQuorumService

    /**
     * Setup and start the {@link NanoSparqlServer}.
     * <p>
     * Note: We need to wait for a quorum meet since this will create the KB
     * instance if it does not exist and we can not write on the
     * {@link HAJournal} until we have a quorum meet.
     */
    private void startNSS() throws Exception {

        final String COMPONENT = NSSConfigurationOptions.COMPONENT;

        final String namespace = (String) config.getEntry(COMPONENT,
                NSSConfigurationOptions.NAMESPACE, String.class,
                NSSConfigurationOptions.DEFAULT_NAMESPACE);

        final Integer queryPoolThreadSize = (Integer) config.getEntry(
                COMPONENT, NSSConfigurationOptions.QUERY_THREAD_POOL_SIZE,
                Integer.TYPE,
                NSSConfigurationOptions.DEFAULT_QUERY_THREAD_POOL_SIZE);

        final boolean create = (Boolean) config.getEntry(COMPONENT,
                NSSConfigurationOptions.CREATE, Boolean.TYPE,
                NSSConfigurationOptions.DEFAULT_CREATE);

        final Integer port = (Integer) config.getEntry(COMPONENT,
                NSSConfigurationOptions.PORT, Integer.TYPE,
                NSSConfigurationOptions.DEFAULT_PORT);

        log.warn("Starting NSS: port=" + port);

        final Map<String, String> initParams = new LinkedHashMap<String, String>();
        {

            initParams.put(ConfigParams.NAMESPACE, namespace);

            initParams.put(ConfigParams.QUERY_THREAD_POOL_SIZE,
                    queryPoolThreadSize.toString());

            // Note: Create will be handled by the QuorumListener (above).
            initParams.put(ConfigParams.CREATE, Boolean.toString(create));

        }
        
        if (jettyServer != null && jettyServer.isRunning()) {
        
            throw new RuntimeException("Already running");
            
        }

        // Setup the embedded jetty server for NSS webapp.
        jettyServer = NanoSparqlServer.newInstance(port, journal, initParams);

        // Start the server.
        jettyServer.start();

        /*
         * Report *an* effective URL of this service.
         * 
         * Note: This is an effective local URL (and only one of them, and even
         * then only one for the first connector). It does not reflect any
         * knowledge about the desired external deployment URL for the service
         * end point.
         */
        final String serviceURL;
        {

            final int actualPort = jettyServer.getConnectors()[0].getLocalPort();

            String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                    true/* loopbackOk */);

            if (hostAddr == null) {

                hostAddr = "localhost";

            }

            serviceURL = new URL("http", hostAddr, actualPort, ""/* file */)
                    .toExternalForm();

            System.out.println("logicalServiceZPath: " + logicalServiceZPath);
            System.out.println("serviceURL: " + serviceURL);

        }

    }
    
    /**
     * Start an {@link HAJournal}.
     * <p>
     * <strong>Jini MUST be running</strong>
     * <p>
     * <strong>You MUST specify a sufficiently lax security policy</strong>,
     * e.g., using <code>-Djava.security.policy=policy.all</code>, where
     * <code>policy.all</code> is the name of a policy file.
     * 
     * @param args
     *            The name of the configuration file.
     */
    public static void main(final String[] args) {

        if (args.length == 0) {

            System.err.println("usage: <config-file> [config-overrides]");

            System.exit(1);

        }

        final HAJournalServer server = new HAJournalServer(args,
                new FakeLifeCycle());

        // Wait for the HAJournalServer to terminate.
        server.run();
        
//        try {
//            final Server tmp = server.jettyServer;
//            if (tmp != null) {
//                // Wait for the jetty server to terminate.
//                tmp.join();
//            }
//        } catch (InterruptedException e) {
//            log.warn(e);
//        }

        System.exit(0);

    }
    
    /**
     * Adds jini administration interfaces to the basic {@link HAGlue} interface
     * exposed by the {@link HAJournal}.
     * 
     * @see HAJournal.HAGlueService
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class AdministrableHAGlueService extends HAGlueDelegate
            implements RemoteAdministrable, RemoteDestroyAdmin {

        final protected HAJournalServer server;

        public AdministrableHAGlueService(final HAJournalServer server,
                final HAGlue service) {

            super(service);

            this.server = server;
            
        }
        
        public Object getAdmin() throws RemoteException {

            if (log.isInfoEnabled())
                log.info("serviceID=" + server.getServiceID());

            return server.proxy;
            
        }
        
//        /**
//         * Sets up the {@link MDC} logging context. You should do this on every
//         * client facing point of entry and then call
//         * {@link #clearLoggingContext()} in a <code>finally</code> clause. You
//         * can extend this method to add additional context.
//         * <p>
//         * This implementation adds the following parameters to the {@link MDC}.
//         * <dl>
//         * <dt>serviceName</dt>
//         * <dd>The serviceName is typically a configuration property for the
//         * service. This datum can be injected into log messages using
//         * <em>%X{serviceName}</em> in your log4j pattern layout.</dd>
//         * <dt>serviceUUID</dt>
//         * <dd>The serviceUUID is, in general, assigned asynchronously by the
//         * service registrar. Once the serviceUUID becomes available it will be
//         * added to the {@link MDC}. This datum can be injected into log
//         * messages using <em>%X{serviceUUID}</em> in your log4j pattern layout.
//         * </dd>
//         * <dt>hostname</dt>
//         * <dd>The hostname statically determined. This datum can be injected
//         * into log messages using <em>%X{hostname}</em> in your log4j pattern
//         * layout.</dd>
//         * <dt>clientname
//         * <dt>
//         * <dd>The hostname or IP address of the client making the request.</dd>
//         * </dl>
//         * Note: {@link InetAddress#getHostName()} is used. This method makes a
//         * one-time best effort attempt to resolve the host name from the
//         * {@link InetAddress}.
//         */
//        private void setupLoggingContext() {
//
//            try {
//
//                // Note: This _is_ a local method call.
//                final ServiceID serviceUUID = server.getServiceID();
//
//                // Will be null until assigned by the service registrar.
//
//                if (serviceUUID != null) {
//
//                    MDC.put("serviceUUID", serviceUUID);
//
//                }
//
//                MDC.put("serviceName", server.getServiceName());
//
//                MDC.put("hostname", server.getHostName());
//
//                try {
//
//                    final InetAddress clientAddr = ((ClientHost) ServerContext
//                            .getServerContextElement(ClientHost.class))
//                            .getClientHost();
//
//                    MDC.put("clientname", clientAddr.getHostName());
//
//                } catch (ServerNotActiveException e) {
//
//                    /*
//                     * This exception gets thrown if the client has made a
//                     * direct (vs RMI) call so we just ignore it.
//                     */
//
//                }
//
//            } catch (Throwable t) {
//
//                /*
//                 * Ignore.
//                 */
//
//            }
//
//        }

        /**
         * Clear the logging context.
         */
        protected void clearLoggingContext() {
            
            MDC.remove("serviceName");

            MDC.remove("serviceUUID");

            MDC.remove("hostname");
            
            MDC.remove("clientname");

        }

        /*
         * DestroyAdmin
         */

        @Override
        public void destroy() {

            server.runShutdown(true/* destroy */);

        }

        @Override
        public void shutdown() {

            server.runShutdown(false/* destroy */);

        }

        @Override
        public void shutdownNow() {

            server.runShutdown(false/* destroy */);

        }

//        /**
//         * Extends the base behavior to return a {@link Name} of the service
//         * from the {@link Configuration}. If no name was specified in the
//         * {@link Configuration} then the value returned by the base class is
//         * returned instead.
//         */
//        @Override
//        public String getServiceName() {
//
//            String s = server.getServiceName();
//
//            if (s == null)
//                s = super.getServiceName();
//
//            return s;
//
//        }

        @Override
        public int getNSSPort() {

            final String COMPONENT = NSSConfigurationOptions.COMPONENT;

            try {

                final Integer port = (Integer) server.config.getEntry(
                        COMPONENT, NSSConfigurationOptions.PORT, Integer.TYPE,
                        NSSConfigurationOptions.DEFAULT_PORT);

                return port;

            } catch (ConfigurationException e) {

                throw new RuntimeException(e);
                
            }

        }

        @Override
        public RunState getRunState() {
            
            return server.getRunState();
            
        }
        
    }

}
