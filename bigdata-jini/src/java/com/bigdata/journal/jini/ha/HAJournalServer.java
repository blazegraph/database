package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.jini.config.Configuration;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.export.ServerContext;
import net.jini.io.context.ClientHost;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.ACL;
import org.eclipse.jetty.server.Server;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAGlueDelegate;
import com.bigdata.ha.HALogWriter;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.QuorumServiceBase;
import com.bigdata.ha.msg.HALogRequest;
import com.bigdata.ha.msg.HALogRootBlocksRequest;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumEvent;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.service.DataService;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.RemoteAdministrable;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.util.InnerCause;
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
    protected static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /**
     * Configuration options for the {@link HAJournalServer}.
     */
    public interface ConfigurationOptions {
        
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
     * FIXME RESYNC : FLAG CONDITIONALLY ENABLES THE HA LOG AND RESYNC PROTOCOL. Disabled
     * in committed code until we have this running properly.
     */
    private static final boolean HA_LOG_ENABLED = false;
    
    /**
     * Caching discovery client for the {@link HAGlue} services.
     */
    private HAJournalDiscoveryClient discoveryClient;

    /**
     * The journal.
     */
    private HAJournal journal;
    
    private UUID serviceUUID;
    private HAGlue haGlueService;
    private ZookeeperClientConfig zkClientConfig;
    
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
     * The {@link Quorum} for the {@link HAJournal}.
     */
    private Quorum<HAGlue, QuorumService<HAGlue>> quorum;

    /**
     * An embedded jetty server exposing the {@link NanoSparqlServer} webapp.
     * The {@link NanoSparqlServer} webapp exposes a SPARQL endpoint for the
     * Journal, which is how you read/write on the journal (the {@link HAGlue}
     * interface does not expose any {@link Remote} methods to write on the
     * {@link HAJournal}.
     */
    private Server jettyServer;

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

        try {
            
            if (jettyServer != null) {

                // Shut down the NanoSparqlServer.
                jettyServer.stop();
                
                jettyServer = null;
                
            }

        } catch (Exception e) {
        
            log.error(e);
            
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
            {
                final List<ACL> acl = zkClientConfig.acl;
                final String zoohosts = zkClientConfig.servers;
                final int sessionTimeout = zkClientConfig.sessionTimeout;

                final ZooKeeperAccessor zka = new ZooKeeperAccessor(zoohosts,
                        sessionTimeout);

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

            this.journal = new HAJournal(properties, quorum);
            
        }

        haGlueService = journal.newHAGlue(serviceUUID);

        final AdministrableHAGlueService administrableService = new AdministrableHAGlueService(
                this, haGlueService);

        return administrableService;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to handle initialization that must wait until the
     * {@link ServiceItem} is registered.
     */
    @Override
    public void run() {

        if (log.isInfoEnabled())
            log.info("Started server.");

        /*
         * Name the thread for the class of server that it is running.
         * 
         * Note: This is generally the thread that ran main(). The thread does
         * not really do any work - it just waits until the server is terminated
         * and then returns to the caller where main() will exit.
         */
        try {

            Thread.currentThread().setName(getClass().getName());
            
        } catch(SecurityException ex) {
            
            // ignore.
            log.warn("Could not set thread name: " + ex);
            
        }
        
        quorum.addListener(new QuorumListener() {

            @Override
            public void notify(final QuorumEvent e) {
                if (log.isTraceEnabled())
                    System.err.println("QuorumEvent: " + e); // TODO LOG @ TRACE
//                switch(e.getEventType()) {
//                case CAST_VOTE:
//                    break;
//                case CONSENSUS:
//                    break;
//                case MEMBER_ADD:
//                    break;
//                case MEMBER_REMOVE:
//                    break;
//                case PIPELINE_ADD:
//                    break;
//                case PIPELINE_REMOVE:
//                    break;
//                case QUORUM_BROKE:
//                    break;
//                case QUORUM_MEET:
//                    break;
//                case SERVICE_JOIN:
//                    break;
//                case SERVICE_LEAVE:
//                    break;
//                case WITHDRAW_VOTE: 
//                }
            }
        });

        // Set the quorum client aka quorum service and start the quorum.
        quorum.start(newQuorumService(logicalServiceZPath, serviceUUID,
                haGlueService, journal));

        /*
         * Note: It appears that these methods CAN NOT moved into
         * QuorumServiceImpl.start(Quorum). I suspect that this is because the
         * quorum watcher would not be running at the moment that we start doing
         * things with the actor, but I have not verified that rationale in
         * depth.
         */
        final QuorumActor<?,?> actor = quorum.getActor();
        actor.memberAdd();
        actor.pipelineAdd();
        actor.castVote(journal.getLastCommitTime());

        /*
         * Wait until the server is terminated.
         */

        synchronized (keepAlive) {

            try {

                while (keepAlive.get()) {
                
                    try {
                    
                        keepAlive.wait();

                    } catch (InterruptedException ex) {

                        if (log.isInfoEnabled())
                            log.info(ex.getLocalizedMessage());

                    }
                }
            } finally {

                // terminate.

                shutdownNow(false/* destroy */);

            }

        }
        
        System.out.println("Service is down: class=" + getClass().getName()
                + ", name=" + getServiceName());
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to set {@link #keepAlive} to <code>false</code> and notify the
     * {@link #keepAlive} monitor so {@link #run()} will terminate.
     */
    @Override
    synchronized public void shutdownNow(final boolean destroy) {

        if (keepAlive.compareAndSet(true/* expect */, false/* update */)) {

            synchronized (keepAlive) {

                keepAlive.notifyAll();
                
            }

            try {

                // Notify the quorum that we are leaving.
                quorum.getActor().serviceLeave();

                quorum.terminate();

            } catch (Throwable t) {
                
                log.error(t,t);
                
            } finally {
                
                super.shutdownNow(destroy);

            }

        }

    }

    /**
     * {@link AtomicBoolean} serves as both a condition variable and the monitor
     * on which we wait.
     */
    final private AtomicBoolean keepAlive = new AtomicBoolean(true);

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
    private QuorumServiceBase<HAGlue, HAJournal> newQuorumService(
            final String logicalServiceZPath,
            final UUID serviceId, final HAGlue remoteServiceImpl,
            final HAJournal store) {

        return new HAQuorumService<HAGlue, HAJournal>(logicalServiceZPath,
                serviceId, remoteServiceImpl, store, this);

    }

    /**
     * Concrete {@link QuorumServiceBase} implementation for the
     * {@link HAJournal}.
     * 
     * @param logicalServiceZPath
     * @param serviceId
     * @param remoteServiceImpl
     *            The object that implements the {@link Remote} interfaces
     *            supporting HA operations.
     * @param store
     *            The {@link HAJournal}.
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
        private FutureTask<Void> resyncFuture = null;
        
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

        @Override
        public void start(final Quorum<?,?> quorum) {
            
            if (haLog.isTraceEnabled())
                log.trace("START");
            
            super.start(quorum);

            // TODO It appears to be a problem to do this here. Maybe because
            // the watcher is not running yet?
//            final QuorumActor<?, ?> actor = quorum.getActor();
//            actor.memberAdd();
//            actor.pipelineAdd();
//            actor.castVote(journal.getLastCommitTime());

            // Inform the Journal about the current token (if any).
            journal.setQuorumToken(quorum.token());
            
        }
        
        @Override
        public void quorumBreak() {

            super.quorumBreak();
            
            // Inform the Journal that the quorum token is invalid.
            journal.setQuorumToken(Quorum.NO_QUORUM);

            if(HA_LOG_ENABLED) {
                
                try {

                    journal.getHALogWriter().disable();

                } catch (IOException e) {

                    haLog.error(e, e);

                }
                
            }
            
        }

        @Override
        public void quorumMeet(final long token, final UUID leaderId) {

            super.quorumMeet(token, leaderId);

            // Inform the journal that there is a new quorum token.
            journal.setQuorumToken(token);

            if (HA_LOG_ENABLED) {

                if (isJoinedMember(token)) {

                    try {

                        journal.getHALogWriter().createLog(
                                journal.getRootBlockView());

                    } catch (IOException e) {

                        /*
                         * We can not remain in the quorum if we can not write
                         * the HA Log file.
                         */
                        haLog.error("CAN NOT OPEN LOG: " + e, e);

                        getActor().serviceLeave();

                    }

                } else {

                    conditionalStartResync(token);

                }
                
            }

            if (isJoinedMember(token) && server.jettyServer == null) {
                /*
                 * The NSS will start on each service in the quorum. However,
                 * only the leader will create the default KB (if that option is
                 * configured).
                 * 
                 * Submit task since we can not do this in the event thread.
                 */
                journal.getExecutorService().submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if (server.jettyServer == null) {
                            try {
                                server.startNSS();
                            } catch (Exception e1) {
                                log.error("Could not start NanoSparqlServer: "
                                        + e1, e1);
                            }
                        }
                        return null;
                    }
                });

            }

        }

        @Override
        public void pipelineAdd() {

            super.pipelineAdd();

            final Quorum<?, ?> quorum = getQuorum();

            final long token = quorum.token();

//            if (quorum.isQuorumMet()) {
            
            conditionalStartResync(token);
                
//            }

        }

        /**
         * Conditionally start resynchronization of this service with the met
         * quorum.
         * 
         * @param token
         */
        private void conditionalStartResync(final long token) {
            
            if (isPipelineMember() && !isJoinedMember(token)
                    && getQuorum().isQuorumMet()
                    && (resyncFuture == null || resyncFuture.isDone())) {

                /*
                 * Start the resynchronization protocol.
                 */

                if (resyncFuture != null) {

                    // Cancel future if already running (paranoia).
                    resyncFuture.cancel(true/* mayInterruptIfRunning */);

                }

                resyncFuture = new FutureTaskMon<Void>(new ResyncTask());

                journal.getExecutorService().submit(resyncFuture);
                

            }

        }

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
         * 
         * TODO RESYNC : In fact, the service can always begin logging from the write
         * pipeline as soon as it observes (or acquires) the root block and
         * observes the seq=0 write cache block (or otherwise catches up with
         * the write pipeline). However, it can not write those data onto the
         * local journal until it is fully caught up. This optimization might
         * allow the service to catch up slightly faster, but the code would be
         * a bit more complex.
         * 
         * FIXME RESYNC : Examine how the service joins during the 2-phase
         * commit and how it participates during the 2-phase prepare (and
         * whether it needs to participate - if not, then rollback some of the
         * changes to support non-joined services in the 2-phase commit to
         * simplify the code).
         */
        private class ResyncTask implements Callable<Void> {

            private final long token;
            private final S leader;
            
            public ResyncTask() {

                // run while quorum is met.
                token = getQuorum().token();

                // The leader for that met quorum (RMI interface).
                leader = getLeader(token);

            }

            @Override
            public Void call() throws Exception {

                try {

                    doRun();

                } catch (Throwable t) {

                    if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                        log.info("Interrupted.");

                    }

                    log.error(t, t);

                }

                return null;

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
            private void doRun() throws Exception {

                haLog.warn("RESYNCH: " + server.getServiceName());

                while (true) {

                    // The current commit point on the local store.
                    final long commitCounter = journal.getRootBlockView()
                            .getCommitCounter();

                    // Replicate and apply the next write set
                    replicateAndApplyWriteSet(commitCounter + 1);

                }

            }

            /**
             * Replicate the write set having the specified commit counter,
             * applying each {@link WriteCache} block as it is received and
             * eventually going through a local commit when we receiving the
             * closing {@link IRootBlockView} for that write set.
             * 
             * @param commitCounter
             *            The commit counter for the desired write set.
             * 
             * @throws IOException
             * @throws FileNotFoundException
             * @throws ExecutionException
             * @throws InterruptedException
             */
            private void replicateAndApplyWriteSet(final long commitCounter)
                    throws FileNotFoundException, IOException,
                    InterruptedException, ExecutionException {

                if (haLog.isInfoEnabled())
                    haLog.info("RESYNC: commitCounter=" + commitCounter);

                final IHALogRootBlocksResponse resp;
                try {
                    
                    // Request the root blocks for the write set.
                    resp = leader
                            .getHALogRootBlocksForWriteSet(new HALogRootBlocksRequest(
                                    commitCounter));

                } catch (FileNotFoundException ex) {

                    /*
                     * Oops. The leader does not have that log file.
                     * 
                     * TODO REBUILD : If we can not replicate ALL log files for
                     * the commit points that we need to make up on this
                     * service, then we can not incrementally resynchronize this
                     * service and we will have to do a full rebuild of the
                     * service instead.
                     * 
                     * TODO RESYNC : It is possible to go to another service in
                     * the met quorum for the same log file, but it needs to be
                     * a service that is UPSTREAM of this service.
                     */

                    // Abort the resynchronization effort.
                    throw new RuntimeException(
                            "HA Log not available: commitCounter="
                                    + commitCounter, ex);

                }

                // root block when the quorum started that write set.
                final IRootBlockView openRootBlock = resp.getOpenRootBlock();

                // root block when the quorum committed that write set.
                final IRootBlockView closeRootBlock = resp.getCloseRootBlock();

                if (openRootBlock.getCommitCounter() != commitCounter - 1) {
                    
                    /*
                     * The opening root block for the write set must have a
                     * commit counter that is ONE less than the requested commit
                     * point.
                     */
                    
                    throw new AssertionError(
                            "Should start at the previous commit point: requested commitCounter="
                                    + commitCounter + ", openRootBlock="
                                    + openRootBlock);
                }
                
                if (openRootBlock.getCommitCounter() == closeRootBlock
                        .getCommitCounter()) {
                    
                    /*
                     * FIXME RESYNC : This is not an error condition. The quorum
                     * is still writing on the HA Log file for the current write
                     * set. However, we do not yet have code that will let us
                     * read on a log file that is currently being written.
                     */
                    
                    throw new AssertionError(
                            "Write set is not closed: requested commitCounter="
                                    + commitCounter);
                
                }
                
                /*
                 * If the local journal is empty, then we need to replace both
                 * of it's root blocks with the opening root block.
                 */
                if (journal.getRootBlockView().getCommitCounter() == 0) {

                    // Install the initial root blocks.
                    installRootBlocksFromQuorum(openRootBlock);

                }
                
                // Make sure we have the correct HALogWriter open.
                logLock.lock();
                try {
                    final HALogWriter logWriter = journal.getHALogWriter();
                    logWriter.disable();
                    logWriter.createLog(openRootBlock);
                } finally {
                    logLock.unlock();
                }

                /*
                 * We need to transfer and apply the write cache blocks from the
                 * HA Log file on some service in the met quorum. This code
                 * works with the leader because it is known to be upstream from
                 * all other services in the write pipeline.
                 * 
                 * Note: Because we are multiplexing the write cache blocks on
                 * the write pipeline, there is no advantage to gathering
                 * different write sets from different nodes. A slight advantage
                 * could be had by reading off of the immediate upstream node,
                 * but understandability of the code is improved by the
                 * assumption that we are always replicating these data from the
                 * quorum leader.
                 */

                Future<Void> ft = null;
                try {

                    ft = leader.sendHALogForWriteSet(new HALogRequest(
                            commitCounter));

                    // Wait until all write cache blocks are received.
                    ft.get();

                } finally {

                    if (ft != null) {

                        ft.cancel(true/* mayInterruptIfRunning */);

                        ft = null;

                    }

                }

                // Local commit.
                journal.doLocalCommit(
                        (QuorumService<HAGlue>) HAQuorumService.this,
                        closeRootBlock);

                // Close out the current HALog writer.
                logLock.lock();
                try {
                    final HALogWriter logWriter = journal.getHALogWriter();
                    logWriter.closeLog(closeRootBlock);
                } finally {
                    logLock.unlock();
                }

            }

        }
        
        @Override
        protected void handleReplicatedWrite(final IHALogRequest req,
                final IHAWriteMessage msg, final ByteBuffer data)
                throws Exception {

            logLock.lock();
            try {

                if (haLog.isDebugEnabled())
                    haLog.debug("msg=" + msg + ", buf=" + data);

                // The current root block on this service.
                final long commitCounter = journal.getRootBlockView()
                        .getCommitCounter();

                if (resyncFuture != null && !resyncFuture.isDone()) {

                    setExtent(msg);
                    handleResyncMessage(msg, data);

                } else if (commitCounter == msg.getCommitCounter()
                        && isJoinedMember(msg.getQuorumToken())) {

                    /*
                     * We are not resynchronizing this service. This is a
                     * message for the current write set. The service is joined
                     * with the quorum.
                     */

                    // write on the log and the local store.
                    setExtent(msg);
                    acceptHAWriteMessage(msg, data);

                } else {
                    
                    if (log.isInfoEnabled())
                        log.info("Ignoring message: " + msg);
                    
                    /*
                     * Drop the pipeline message. We can't log it yet.
                     */
                    
                }

            } finally {

                logLock.unlock();

            }

        }
        
        /**
         * Adjust the size on the disk of the local store to that given in the
         * message.
         * 
         * Note: DO NOT do this for historical messages!
         * 
         * @throws IOException
         * 
         * @todo Trap truncation vs extend?
         */
        private void setExtent(final IHAWriteMessage msg) throws IOException {

            try {

                ((IHABufferStrategy) journal.getBufferStrategy())
                        .setExtentForLocalStore(msg.getFileExtent());

            } catch (InterruptedException e) {

                throw new RuntimeException(e);

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
         * 
         *             FIXME RESYNC : There is only one {@link HALogWriter}. It
         *             can only have one file open. We need to explicitly
         *             coordinate which log file is open when so we never
         *             attempt to write a cache block on the write log file (or
         *             one that is out of sequence). [Consider making those
         *             things errors in the {@link HALogWriter} - it quietly
         *             ignores this right now.]
         */
        private void handleResyncMessage(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            logLock.lock();

            try {

                /*
                 * FIXME RESYNC : Review the transition conditions. [msg.seq+q ==
                 * log.nextSeq] implies that we have observed and logged this
                 * write block already. That is the duplicate write cache block
                 * that let's us know that we are fully synchronized with the
                 * quorum.
                 * 
                 * FIXME RESYNC : Review when (and where) we open and close log files.
                 */

                final HALogWriter logWriter = journal.getHALogWriter();

                final long journalCommitCounter = journal.getRootBlockView()
                        .getCommitCounter();

                if (msg.getCommitCounter() == journalCommitCounter
                        && msg.getSequence() + 1 == logWriter.getSequence()) {

                    /*
                     * We just received the last resync message that we need to
                     * join the met quorum.
                     */
                    
                    resyncTransitionToMetQuorum(msg,data);
                    
                    return;
                    
                }

                /*
                 * Log the message and write cache block.
                 */

                if (logWriter.getCommitCounter() != msg.getCommitCounter()) {

                    if (haLog.isDebugEnabled())
                        log.debug("Ignoring write cache block: msg=" + msg);

                    return;

                }
                
                // log and write cache block.
                acceptHAWriteMessage(msg, data);
                
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

            if (resyncFuture != null) {

                // Caught up.
                resyncFuture.cancel(true/* mayInterruptIfRunning */);

            }

            // Accept the message - log and apply.
            acceptHAWriteMessage(msg, data);

            // Vote our lastCommitTime.
            getActor().castVote(rootBlock.getLastCommitTime());

            log.warn("Service voted for lastCommitTime of quorum, is receiving pipeline writes, and should join the met quorum");

            // /*
            // * TODO RESYNC : If there is a fully met quorum, then we can purge
            // * all HA logs *EXCEPT* the current one. However, in order
            // * to have the same state on each node, we really need to
            // * make this decision when a service observes the
            // * SERVICE_JOIN event that results in a fully met quorum.
            // * That state change will be globally visible. If we do
            // this
            // * here, then only the service that was resynchronizing
            // will
            // * wind up purging its logs.
            // */
            // if (getQuorum().isQuorumFullyMet()) {
            //
            // purgeHALogs(false/* includeCurrent */);
            //
            // }

        }
        
        /**
         * Verify commitCounter in the current log file and the message are
         * consistent, then log and apply the {@link WriteCache} block.
         */
        private void acceptHAWriteMessage(final IHAWriteMessage msg,
                final ByteBuffer data) throws IOException, InterruptedException {

            if (HA_LOG_ENABLED) {

                final HALogWriter logWriter = journal.getHALogWriter();

                if (msg.getCommitCounter() != logWriter.getCommitCounter()) {

                    throw new AssertionError();

                }

                /*
                 * Log the message and write cache block.
                 */
                logWriteCacheBlock(msg, data);
                
            }

            writeWriteCacheBlock(msg,data);

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

            if (!HA_LOG_ENABLED)
                return;

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

            if (!HA_LOG_ENABLED)
                return;

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
         */
        @Override
        public void purgeHALogs(final boolean includeCurrent) {

            if (!HA_LOG_ENABLED)
                return;

            logLock.lock();

            try {

                final File logDir = journal.getHALogDir();

                final File[] files = logDir.listFiles(new FilenameFilter() {

                    @Override
                    public boolean accept(final File dir, final String name) {

                        return name.endsWith(HALogWriter.HA_LOG_EXT);

                    }
                });

                int ndeleted = 0;
                long totalBytes = 0L;

                final File currentFile = journal.getHALogWriter().getFile();

                for (File file : files) {

                    final long len = file.length();

                    final boolean delete = includeCurrent
                            || currentFile != null
                            && file.getName().equals(currentFile.getName());

                    if (delete && !file.delete()) {

                        haLog.warn("COULD NOT DELETE FILE: " + file);

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

        @SuppressWarnings("unchecked")
        @Override
        public void installRootBlocksFromQuorum(final IRootBlockView rootBlock) {

            journal.installRootBlocksFromQuorum((QuorumService<HAGlue>) this,
                    rootBlock);

        }
        
    }
    
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

            initParams.put(ConfigParams.CREATE, Boolean.toString(create));

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
        
        try {
            final Server tmp = server.jettyServer;
            if (tmp != null) {
                // Wait for the jetty server to terminate.
                tmp.join();
            }
        } catch (InterruptedException e) {
            log.warn(e);
        }

        System.exit(0);

    }
    
    /**
     * Adds jini administration interfaces to the basic {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
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
        
        /**
         * Sets up the {@link MDC} logging context. You should do this on every
         * client facing point of entry and then call
         * {@link #clearLoggingContext()} in a <code>finally</code> clause. You
         * can extend this method to add additional context.
         * <p>
         * This implementation adds the following parameters to the {@link MDC}.
         * <dl>
         * <dt>serviceName</dt>
         * <dd>The serviceName is typically a configuration property for the
         * service. This datum can be injected into log messages using
         * <em>%X{serviceName}</em> in your log4j pattern layout.</dd>
         * <dt>serviceUUID</dt>
         * <dd>The serviceUUID is, in general, assigned asynchronously by the
         * service registrar. Once the serviceUUID becomes available it will be
         * added to the {@link MDC}. This datum can be injected into log
         * messages using <em>%X{serviceUUID}</em> in your log4j pattern layout.
         * </dd>
         * <dt>hostname</dt>
         * <dd>The hostname statically determined. This datum can be injected
         * into log messages using <em>%X{hostname}</em> in your log4j pattern
         * layout.</dd>
         * <dt>clientname
         * <dt>
         * <dd>The hostname or IP address of the client making the request.</dd>
         * </dl>
         * Note: {@link InetAddress#getHostName()} is used. This method makes a
         * one-time best effort attempt to resolve the host name from the
         * {@link InetAddress}.
         */
        protected void setupLoggingContext() {

            try {

                // Note: This _is_ a local method call.
                final ServiceID serviceUUID = server.getServiceID();

                // Will be null until assigned by the service registrar.

                if (serviceUUID != null) {

                    MDC.put("serviceUUID", serviceUUID);

                }

                MDC.put("serviceName", server.getServiceName());

                MDC.put("hostname", server.getHostName());

                try {

                    final InetAddress clientAddr = ((ClientHost) ServerContext
                            .getServerContextElement(ClientHost.class))
                            .getClientHost();

                    MDC.put("clientname", clientAddr.getHostName());

                } catch (ServerNotActiveException e) {

                    /*
                     * This exception gets thrown if the client has made a
                     * direct (vs RMI) call so we just ignore it.
                     */

                }

            } catch (Throwable t) {

                /*
                 * Ignore.
                 */

            }

        }

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
        synchronized public void destroy() {

            if (!server.isShuttingDown()) {

                /*
                 * Run thread which will destroy the service (asynchronous).
                 * 
                 * Note: By running this is a thread, we avoid closing the
                 * service end point during the method call.
                 * 
                 * TODO We also need to destroy the persistent state (dataDir,
                 * serviceDir), the zk state for this service, and ensure that
                 * the proxy is unexported from any discovered registrars (that
                 * is probably being done). [unit tests for the admin methods.]
                 */

                server.runDestroy();

//            } else if (isOpen()) {
//
//                /*
//                 * The server is already shutting down, so invoke our super
//                 * class behavior to destroy the persistent state.
//                 */
//
//                super.destroy();

            }

        }

        @Override
        synchronized public void shutdown() {

//            // normal service shutdown (blocks).
//            super.shutdown();

            // jini service and server shutdown.
            server.shutdownNow(false/* destroy */);

        }

        @Override
        synchronized public void shutdownNow() {
            
//            // immediate service shutdown (blocks).
//            super.shutdownNow();

            // jini service and server shutdown.
            server.shutdownNow(false/* destroy */);
            
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

    }

}
