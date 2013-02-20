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
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.ACL;

import com.bigdata.Banner;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.ha.QuorumPipelineImpl;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.msg.HARebuildRequest;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHASendStoreResponse;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteSetStateRequest;
import com.bigdata.ha.msg.IHAWriteSetStateResponse;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.quorum.AbstractQuorumMember;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumEvent;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.zookeeper.ZooKeeperAccessor;

/**
 * Service for making full and incremental backups.
 * 
 * FIXME This is completely non-functional code.  I was just experimenting
 * with creating a standalone utility.  A functional version should be 
 * derived by refactoring AbstractServer, HAJournalServer, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HABackupManager {

    private static final Logger log = Logger.getLogger(HAJournalServer.class);

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /**
     * Configuration options for the {@link HAJournalServer}.
     */
    public interface ConfigurationOptions 
//    extends AbstractServer.ConfigurationOptions 
    {

        String COMPONENT = HABackupManager.class.getName();

        /**
         * The target replication factor (k).
         * 
         * @see HAJournalServer.ConfigurationOptions#REPLICATION_FACTOR
         */
        String REPLICATION_FACTOR = HAJournalServer.ConfigurationOptions.REPLICATION_FACTOR;
        
        /**
         * The {@link InetSocketAddress} at which the {@link HABackupManager}
         * receives replicated writes.
         * 
         * @see HAJournalServer.ConfigurationOptions#WRITE_PIPELINE_ADDR
         */
        String WRITE_PIPELINE_ADDR = HAJournalServer.ConfigurationOptions.WRITE_PIPELINE_ADDR;

        /**
         * The logical service identifier for the {@link HAJournalServer}
         * replication cluster that that will be backed up by this service.
         * 
         * @see HAJournalServer.ConfigurationOptions#LOGICAL_SERVICE_ID
         */
        String LOGICAL_SERVICE_ID = HAJournalServer.ConfigurationOptions.LOGICAL_SERVICE_ID;
        
    }

    private LookupDiscoveryManager lookupDiscoveryManager;

    private ServiceDiscoveryManager serviceDiscoveryManager;

    /**
     * The {@link Configuration} read based on the args[] provided when the
     * server is started.
     */
    protected Configuration config;

    /**
     * The timeout in milliseconds to await the discovery of a service if there
     * is a cache miss (default {@value #DEFAULT_CACHE_MISS_TIMEOUT}).
     */
    final protected long cacheMissTimeout;

    /**
     * A randomly generated {@link UUID} that this utility uses to identify
     * itself.
     */
    private final UUID serviceId = UUID.randomUUID();

    /**
     * The directory for the service. This is the directory within which the
     * {@link #serviceIdFile} exists. A service MAY have its own concept of a
     * data directory, log directory, etc. which can be somewhere else.
     */
    private File serviceDir;
    
    /**
     * Caching discovery client for the {@link HAGlue} services.
     */
    private HAJournalDiscoveryClient discoveryClient;

    private ZookeeperClientConfig zkClientConfig;
    
    private ZooKeeperAccessor zka;

    private Quorum<HAGlue, QuorumService<HAGlue>> quorum;
    
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
     * The {@link MyQuorumMember}.
     */
    private MyQuorumMember quorumMember;

    /**
     * An object used to manage jini service registrar discovery.
     */
    public LookupDiscoveryManager getDiscoveryManagement() {
        
        return lookupDiscoveryManager;
        
    }

    /**
     * An object used to lookup services using the discovered service registars.
     */
    public ServiceDiscoveryManager getServiceDiscoveryManager() {
        
        return serviceDiscoveryManager;
        
    }
    
    /**
     * Runs {@link AbstractServer#shutdownNow()} and terminates all asynchronous
     * processing, including discovery. This is used for the shutdown hook (^C).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class ShutdownThread extends Thread {

        public ShutdownThread() {

            super("shutdownThread");
            
            setDaemon(true);
            
        }

        public void run() {

            /*
             * FIXME Interrupt the backup (if any). It is logically empty until
             * we write the root blocks. Those need to BOTH be written
             * atomically (tricky) -or- just write the same root block twice
             * (sneaky).
             */

        }

    }

    private HABackupManager(final String[] args) throws ConfigurationException {
        
        /*
         * The runtime shutdown hook appears to be a robust way to handle ^C by
         * providing a clean service termination.
         * 
         * Note: This is setup before we start any async threads, including
         * service discovery.
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        // Show the copyright banner during startup.
        Banner.banner();

        AbstractServer.setSecurityManager();

        /*
         * Display the banner.
         * 
         * Note: This also installs the UncaughtExceptionHandler.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/601
         */
        Banner.banner();

        /*
         * Read jini configuration & service properties 
         */

        List<Entry> entries = null;
        
        final String COMPONENT = getClass().getName();
        final JiniClientConfig jiniClientConfig;
        {

            config = ConfigurationProvider.getInstance(args);

            cacheMissTimeout = (Long) config.getEntry(COMPONENT,
                    AbstractServer.ConfigurationOptions.CACHE_MISS_TIMEOUT, Long.TYPE,
                    AbstractServer.ConfigurationOptions.DEFAULT_CACHE_MISS_TIMEOUT);

            jiniClientConfig = new JiniClientConfig(
                    JiniClientConfig.Options.NAMESPACE, config);

            // convert Entry[] to a mutable list.
            entries = new LinkedList<Entry>(
                    Arrays.asList((Entry[]) jiniClientConfig.entries));
            
            if (log.isInfoEnabled())
                log.info(jiniClientConfig.toString());
        
        }
        
        /*
         * Make sure that the parent directory exists.
         * 
         * Note: the parentDir will be null if the serviceIdFile is in the
         * root directory or if it is specified as a filename without any
         * parents in the path expression. Note that the file names a file
         * in the current working directory in the latter case and the root
         * always exists in the former - and in both of those cases we do
         * not have to create the parent directory.
         */
        serviceDir = (File) config.getEntry(COMPONENT,
                AbstractServer.ConfigurationOptions.SERVICE_DIR, File.class);

        if (serviceDir != null && !serviceDir.exists()) {

            log.warn("Creating: " + serviceDir);

            serviceDir.mkdirs();

        }

        try {

            /*
             * Note: This class will perform multicast discovery if ALL_GROUPS
             * is specified and otherwise requires you to specify one or more
             * unicast locators (URIs of hosts running discovery services). As
             * an alternative, you can use LookupDiscovery, which always does
             * multicast discovery.
             */
            lookupDiscoveryManager = new LookupDiscoveryManager(
                    jiniClientConfig.groups, jiniClientConfig.locators,
                    null /* DiscoveryListener */, config);

            /*
             * Setup a helper class that will be notified as services join or
             * leave the various registrars to which the data server is
             * listening.
             */
            try {

                serviceDiscoveryManager = new ServiceDiscoveryManager(
                        lookupDiscoveryManager, new LeaseRenewalManager(),
                        config);

            } catch (IOException ex) {

                throw new RuntimeException(
                        "Could not initiate service discovery manager", ex);

            }

        } catch (IOException ex) {

            fatal("Could not setup discovery", ex);
            throw new AssertionError();// keep the compiler happy.

        } catch (ConfigurationException ex) {

            fatal("Could not setup discovery", ex);
            throw new AssertionError();// keep the compiler happy.

        }

        /*
         * Create the service object.
         */
        try {
            
            /*
             * Note: By creating the service object here rather than outside of
             * the constructor we potentially create problems for subclasses of
             * AbstractServer since their own constructor will not have been
             * executed yet.
             * 
             * Some of those problems are worked around using a JiniClient to
             * handle all aspects of service discovery (how this service locates
             * the other services in the federation).
             * 
             * Note: If you explicitly assign values to those clients when the
             * fields are declared, e.g., [timestampServiceClient=null] then the
             * ctor will overwrite the values set by [newService] since it is
             * running before those initializations are performed. This is
             * really crufty, may be JVM dependent, and needs to be refactored
             * to avoid this subclass ctor init problem.
             */

            if (log.isInfoEnabled())
                log.info("Creating service impl...");

            // init.
//            impl = 
            newService(config);
            
//            if (log.isInfoEnabled())
//                log.info("Service impl is " + impl);
            
        } catch(Exception ex) {
        
            fatal("Could not start service: "+this, ex);
            throw new AssertionError();// keeps compiler happy.
        }

    }
    
    protected void fatal(String msg, Throwable t) {

        log.fatal(msg, t);
        
        terminate();
        
        System.exit(1);
        
    }

    /**
     * Terminates service management threads.
     * <p>
     * Subclasses which start additional service management threads SHOULD
     * extend this method to terminate those threads. The implementation should
     * be <strong>synchronized</strong>, should conditionally terminate each
     * thread, and should trap, log, and ignore all errors.
     */
    private void terminate() {

        if (log.isInfoEnabled())
            log.info("Terminating service management threads.");

//        if (joinManager != null) {
//            
//            try {
//
//                joinManager.terminate();
//
//            } catch (Throwable ex) {
//
//                log.error("Could not terminate the join manager: " + this, ex);
//
//            } finally {
//                
//                joinManager = null;
//
//            }
//
//        }
        
        if (serviceDiscoveryManager != null) {

            serviceDiscoveryManager.terminate();

            serviceDiscoveryManager = null;

        }

        if (lookupDiscoveryManager != null) {

            lookupDiscoveryManager.terminate();

            lookupDiscoveryManager = null;

        }
        
    }
    
    protected void newService(final Configuration config)
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

//        // Setup discovery for HAGlue clients.
//        final HAJournalDiscoveryClient discoveryClient = new HAJournalDiscoveryClient(
//                getServiceDiscoveryManager(),
//                null/* serviceDiscoveryListener */, cacheMissTimeout);

        /*
         * Setup the Quorum.
         */

        zkClientConfig = new ZookeeperClientConfig(config);

        // znode name for the logical service.
        final String logicalServiceId = (String) config.getEntry(
                ConfigurationOptions.COMPONENT,
                ConfigurationOptions.LOGICAL_SERVICE_ID, String.class); 

        final String logicalServiceZPathPrefix = zkClientConfig.zroot + "/"
                + HAJournalServer.class.getName();
        
        // zpath for the logical service.
        final String logicalServiceZPath = logicalServiceZPathPrefix + "/"
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

//            // The HAJournal.
//            this.journal = new HAJournal(properties, quorum);
            
        }

//        TODO executor for events received in the watcher thread.
//        singleThreadExecutor = new LatchedExecutor(
//                journal.getExecutorService(), 1/* nparallel */);
        
//        // our external interface.
//        haGlueService = journal.newHAGlue(serviceUUID);
//
//        // wrap the external interface, exposing administrative functions.
//        final AdministrableHAGlueService administrableService = new AdministrableHAGlueService(
//                this, haGlueService);
//
//        // return that wrapped interface.
//        return administrableService;

    }

    protected void startUp() throws IOException {

        if (log.isInfoEnabled())
            log.info("Starting server.");

        getQuorum().addListener(new QuorumListener() {

            @Override
            public void notify(final QuorumEvent e) {
                if (log.isTraceEnabled())
                    log.trace(e); // TODO LOG @ TRACE
            }
        });

        // Setup the quorum client (aka quorum service).
        quorumMember = new MyQuorumMember(logicalServiceZPath, serviceId);

    }
    
    protected void backup() throws Exception {

        final long token = quorumMember.getQuorum().token();
        
        quorumMember.getQuorum().assertQuorum(token);
        
        quorumMember.new BackupTask(token).call();
        
    }
    
    /**
     * 
     * A randomly generated {@link UUID} that this utility uses to identify
     * itself.
     */
    protected UUID getServiceId() {
        return serviceId;
    }
    
    protected Quorum<?,?> getQuorum() {
        return quorum;
    }

    private class MyQuorumMember extends AbstractQuorumMember<HAPipelineGlue> {

        private final QuorumPipelineImpl<HAPipelineGlue> pipelineImpl = new QuorumPipelineImpl<HAPipelineGlue>(this) {

            @Override
            protected void handleReplicatedWrite(final IHASyncRequest req,
                    final IHAWriteMessage msg, final ByteBuffer data)
                    throws Exception {

                MyQuorumMember.this.handleReplicatedWrite(req, msg, data);

            }

            @Override
            public long getLastCommitTime() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public long getLastCommitCounter() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public void logWriteCacheBlock(IHAWriteMessage msg, ByteBuffer data)
                    throws IOException {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void logRootBlock(IRootBlockView rootBlock)
                    throws IOException {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void purgeHALogs(boolean includeCurrent) {
                // TODO Auto-generated method stub
                
            }
            
//            @Override
//            public long getLastCommitTime() {
//
//                return MyQuorumMember.this.getLastCommitTime();
//                
//            }
//        
//            @Override
//            public long getLastCommitCounter() {
//
//                return MyQuorumMember.this.getLastCommitCounter();
//                
//            }
//
//            @Override
//            public void logWriteCacheBlock(final IHAWriteMessage msg,
//                    final ByteBuffer data) throws IOException {
//
//                MyQuorumMember.this.logWriteCacheBlock(msg, data);
//                
//            }
//            
//            @Override
//            public void logRootBlock(final IRootBlockView rootBlock)
//                    throws IOException {
//
//                MyQuorumMember.this.logRootBlock(rootBlock);
//
//            }
//
//            @Override
//            public void purgeHALogs(final boolean includeCurrent) {
//
//                MyQuorumMember.this.purgeHALogs(includeCurrent);
//
//            }

        };

        /**
         * The local implementation of the {@link Remote} interface.
         */
        private final HAPipelineGlue service;
        
//        /**
//         * Simple service registrar.
//         */
//        private final MockServiceRegistrar<S> registrar;
        
        /**
         * The last lastCommitTime value around which a consensus was achieved
         * and initially -1L, but this is cleared to -1L each time the consensus
         * is lost.
         */
        protected volatile long lastConsensusValue = -1L;

        /**
         * The downstream service in the write pipeline.
         */
        protected volatile UUID downStreamId = null;

        private volatile ExecutorService executorService = null;
        
        protected MyQuorumMember(final String logicalServiceId,
                final UUID serviceId) throws IOException {

            super(logicalServiceId, serviceId);

            service = new MyQuorumService();
            
            /*
             * Delegates.
             */

            addListener(this.pipelineImpl);

        }

        @Override
        public void start(final Quorum<?, ?> quorum) {
            if (executorService == null)
                executorService = Executors
                        .newSingleThreadExecutor(DaemonThreadFactory
                                .defaultThreadFactory());
            super.start(quorum);
        }
        
        @Override
        public void terminate() {
            super.terminate();
            if(executorService!=null) {
                executorService.shutdownNow();
                executorService = null;
            }
        }
        
        public Executor getExecutor() {
            return executorService;
        }

//        /**
//         * Factory for the local service implementation object.
//         */
//        abstract S newService();
        
        public HAPipelineGlue getService() {
            return service;
        }

        /**
         * Resolve an {@link HAGlue} object from its Service UUID.
         */
        @Override
        public HAGlue getService(final UUID serviceId) {
            
//            final HAJournalDiscoveryClient discoveryClient = 
//                    .getDiscoveryClient();

            final ServiceItem serviceItem = discoveryClient
                    .getServiceItem(serviceId);
            
            if (serviceItem == null) {

                // Not found (per the API).
                throw new QuorumException("Service not found: uuid="
                        + serviceId);

            }

            @SuppressWarnings("unchecked")
            final HAGlue service = (HAGlue) serviceItem.service;

            return service;
            
        }

        /**
         * {@inheritDoc}
         * 
         * Overridden to save the <i>lastCommitTime</i> on
         * {@link #lastConsensusValue}.
         */
        @Override
        public void consensus(long lastCommitTime) {
            super.consensus(lastCommitTime);
            this.lastConsensusValue = lastCommitTime;
        }

        @Override
        public void lostConsensus() {
            super.lostConsensus();
            this.lastConsensusValue = -1L;
        }

        /**
         * {@inheritDoc}
         * 
         * Overridden to save the current downstream service {@link UUID} on
         * {@link #downStreamId}
         */
        public void pipelineChange(final UUID oldDownStreamId,
                final UUID newDownStreamId) {
            super.pipelineChange(oldDownStreamId, newDownStreamId);
            this.downStreamId = newDownStreamId;
        }

        /**
         * {@inheritDoc}
         * 
         * Overridden to clear the {@link #downStreamId}.
         */
        public void pipelineRemove() {
            super.pipelineRemove();
            this.downStreamId = null;
        }

        /**
         * @see HAPipelineGlue
         */
        protected void handleReplicatedWrite(IHASyncRequest req,
                IHAWriteMessage msg, ByteBuffer data) throws Exception {
            
            // FIXME handle replicated writes!
            
        }
     
        /**
         * Mock service class.
         */
        class MyQuorumService implements HAPipelineGlue {

            private final InetSocketAddress addrSelf;
            
            public MyQuorumService() throws IOException {
                this.addrSelf = new InetSocketAddress(getPort(0));
            }
            
            public InetSocketAddress getWritePipelineAddr() {
                return addrSelf;
            }

            /**
             * @todo This is not fully general purpose since it is not strictly
             *       forbidden that the service's lastCommitTime could change, e.g.,
             *       due to explicit intervention, and hence be updated across this
             *       operation. The real implemention should be a little more
             *       sophisticated.
             */
            public Future<Void> moveToEndOfPipeline() throws IOException {
                final FutureTask<Void> ft = new FutureTask<Void>(new Runnable() {
                    public void run() {

                        // note the current vote (if any).
                        final Long lastCommitTime = getQuorum().getCastVote(
                                getServiceId());
                        
                        if (isPipelineMember()) {
                        
//                            System.err
//                                    .println("Will remove self from the pipeline: "
//                                            + getServiceId());
                            
                            getActor().pipelineRemove();
                            
//                            System.err
//                                    .println("Will add self back into the pipeline: "
//                                            + getServiceId());
                            
                            getActor().pipelineAdd();
                            
                            if (lastCommitTime != null) {
                            
//                                System.err
//                                        .println("Will cast our vote again: lastCommitTime="
//                                                + +lastCommitTime
//                                                + ", "
//                                                + getServiceId());
                                
                                getActor().castVote(lastCommitTime);
                                
                            }

                        }
                    }
                }, null/* result */);
                getExecutor().execute(ft);
                return ft;
            }

            @Override
            public Future<Void> receiveAndReplicate(final IHASyncRequest req,
                    IHAWriteMessage msg) throws IOException {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public IHALogRootBlocksResponse getHALogRootBlocksForWriteSet(
                    IHALogRootBlocksRequest msg) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<Void> sendHALogForWriteSet(IHALogRequest msg)
                    throws IOException {
                throw new UnsupportedOperationException();
            }
        
            @Override
            public Future<IHASendStoreResponse> sendHAStore(IHARebuildRequest msg)
                    throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public IHAWriteSetStateResponse getHAWriteSetState(
                    IHAWriteSetStateRequest req) {
                throw new UnsupportedOperationException();
            }

        }

        /**
         * 
         * Create a full backup.
         */
        private class BackupTask implements Callable<Void> {

            /**
             * The quorum token in effect when we began the resync.
             */
            private final long token;

            /**
             * The quorum leader. This is fixed until the quorum breaks.
             */
            private final HAGlue leader;
            
            public BackupTask(final long token) {

                // run while quorum is met.
                this.token = token;

                // The leader for that met quorum (RMI interface).
                leader = (HAGlue) getLeader(token);

            }

            public Void call() throws Exception {

                /*
                 * DO NOT start a rebuild if the quorum is broken. Instead, we
                 * will try again after SeekConsensus.
                 */
                getQuorum().assertQuorum(token);

                /*
                 * Replicate the backing store of the leader.
                 * 
                 * Note: This remoteFuture MUST be cancelled if the RebuildTask
                 * is interrupted.
                 * 
                 * FIXME This needs to write onto a configured file.
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
                // FIXME Install the root blocks (atomically or as dups of the current root block).
//                installRootBlocks(resp.getRootBlock0(), resp.getRootBlock1());

                // Done
                return null;

            }

        } // class BackupTask

    }// class MyQuorumMember
    
    /**
     * Return an open port on current machine. Try the suggested port first. If
     * suggestedPort is zero, just select a random port
     */
    private static int getPort(final int suggestedPort) throws IOException {
        
        ServerSocket openSocket;
        
        try {
        
            openSocket = new ServerSocket(suggestedPort);
            
        } catch (BindException ex) {
            
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        
        }

        final int port = openSocket.getLocalPort();
        
        openSocket.close();

        return port;
        
    }

//    /**
//     * Lock controlling access to the {@link #discoveryEvent} {@link Condition}.
//     */
//    protected final ReentrantLock discoveryEventLock = new ReentrantLock();
//
//    /**
//     * Condition signaled any time there is a {@link DiscoveryEvent} delivered to
//     * our {@link DiscoveryListener}.
//     */
//    protected final Condition discoveryEvent = discoveryEventLock
//            .newCondition();
//
//    /**
//     * Signals anyone waiting on {@link #discoveryEvent}.
//     */
//    public void discarded(final DiscoveryEvent e) {
//
//        try {
//            
//            discoveryEventLock.lockInterruptibly();
//            
//            try {
//                
//                discoveryEvent.signalAll();
//            
//            } finally {
//                
//                discoveryEventLock.unlock();
//                
//            }
//            
//        } catch (InterruptedException ex) {
//            
//            return;
//            
//        }
//        
//    }
//
//    /**
//     * Signals anyone waiting on {@link #discoveryEvent}.
//     */
//    public void discovered(final DiscoveryEvent e) {
//
//        try {
//
//            discoveryEventLock.lockInterruptibly();
//
//            try {
//
//                discoveryEvent.signalAll();
//
//            } finally {
//
//                discoveryEventLock.unlock();
//
//            }
//
//        } catch (InterruptedException ex) {
//
//            return;
//
//        }
//        
//    }
    
    /**
     * Start the {@link HABackupManager}.
     * <p>
     * <strong>Jini MUST be running</strong>
     * <p>
     * <strong>You MUST specify a sufficiently lax security policy</strong>,
     * e.g., using <code>-Djava.security.policy=policy.all</code>, where
     * <code>policy.all</code> is the name of a policy file.
     * 
     * @param args
     *            The name of the configuration file.
     * 
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {

        if (args.length == 0) {

            System.err.println("usage: <config-file> [config-overrides]");

            System.exit(1);

        }

        final HABackupManager server = new HABackupManager(args);

        server.startUp();

        server.backup();

        System.exit(0);

    }

}
