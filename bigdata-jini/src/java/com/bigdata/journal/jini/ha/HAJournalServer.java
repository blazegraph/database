package com.bigdata.journal.jini.ha;

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
import java.util.concurrent.TimeUnit;

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

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAGlueDelegate;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.QuorumServiceBase;
import com.bigdata.io.IBufferAccess;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.ha.HAWriteMessage;
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
import com.bigdata.util.config.NicUtil;
import com.bigdata.zookeeper.ZooKeeperAccessor;
import com.sun.jini.start.LifeCycle;

/**
 * An administratable server for an {@link HAJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/530"> Journal
 *      HA </a>
 * 
 *      TODO Make sure that ganglia reporting can be enabled.
 */
public class HAJournalServer extends AbstractServer {

    private static final Logger log = Logger.getLogger(HAJournal.class);

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
                    System.err.print("QuorumEvent: " + e);
                switch(e.getEventType()) {
                case CAST_VOTE:
                    break;
                case CONSENSUS:
                    break;
                case MEMBER_ADD:
                    break;
                case MEMBER_REMOVE:
                    break;
                case PIPELINE_ADD:
                    break;
                case PIPELINE_REMOVE:
                    break;
                case QUORUM_BROKE:
                    break;
                case QUORUM_MEET:
                    if (jettyServer == null) {
                        /*
                         * The NSS will start on each service in the quorum.
                         * However, only the leader will create the default KB
                         * (if that option is configured).
                         * 
                         * Submit task since we can not do this in the event
                         * thread.
                         */
                        journal.getExecutorService().submit(
                                new Callable<Void>() {
                                    @Override
                                    public Void call() throws Exception {
                                        if (jettyServer == null) {
                                            try {
                                                startNSS();
                                            } catch (Exception e1) {
                                                log.error(
                                                        "Could not start NanoSparqlServer: "
                                                                + e1, e1);
                                            }
                                        }
                                        return null;
                                    }
                                });
                    }
                    break;
                case SERVICE_JOIN:
                    break;
                case SERVICE_LEAVE:
                    break;
                case WITHDRAW_VOTE: 
                }
            }
        });

        quorum.start(newQuorumService(logicalServiceZPath, serviceUUID,
                haGlueService, journal));

        // TODO These methods could be moved into QuorumServiceImpl.start(Quorum)
        final QuorumActor<?,?> actor = quorum.getActor();
        actor.memberAdd();
        actor.pipelineAdd();
        actor.castVote(journal.getLastCommitTime());

        /*
         * Wait until the server is terminated.
         * 
         * TODO Since spurious wakeups are possible, this should be used in a loop
         * with a condition variable.
         */
        
        synchronized (keepAlive) {
            
            try {
                
                keepAlive.wait();
                
            } catch (InterruptedException ex) {
                
                if (log.isInfoEnabled())
                    log.info(ex.getLocalizedMessage());

            } finally {
                
                // terminate.
                
                shutdownNow(false/* destroy */);
                
            }
            
        }
        
        System.out.println("Service is down: class=" + getClass().getName()
                + ", name=" + getServiceName());
        
    }

    final private Object keepAlive = new Object();

    /**
     * Factory for the {@link QuorumService} implementation.
     * 
     * @param remoteServiceImpl
     *            The object that implements the {@link Remote} interfaces
     *            supporting HA operations.
     */
    private QuorumServiceBase<HAGlue, AbstractJournal> newQuorumService(
            final String logicalServiceZPath,
            final UUID serviceId, final HAGlue remoteServiceImpl,
            final AbstractJournal store) {

        return new QuorumServiceBase<HAGlue, AbstractJournal>(
                logicalServiceZPath, serviceId, remoteServiceImpl, store) {

            @Override
            public void start(final Quorum<?,?> quorum) {
                
                super.start(quorum);

                // Inform the Journal about the current token (if any).
                journal.setQuorumToken(quorum.token());
                
            }
            
            @Override
            public void quorumBreak() {

                super.quorumBreak();
                
                // Inform the Journal that the quorum token is invalid.
                journal.setQuorumToken(Quorum.NO_QUORUM);
                
            }

            @Override
            public void quorumMeet(final long token, final UUID leaderId) {

                super.quorumMeet(token, leaderId);
                
                // Inform the journal that there is a new quorum token.
                journal.setQuorumToken(token);

            }
            
            /**
             * Resolve an {@link HAGlue} object from its Service UUID.
             */
            @Override
            public HAGlue getService(final UUID serviceId) {
                
                final ServiceItem serviceItem = discoveryClient
                        .getServiceItem(serviceId);

                if (serviceItem == null) {

                    // Not found (per the API).
                    throw new QuorumException("Service not found: uuid="
                            + serviceId);

                }

                return (HAGlue) serviceItem.service;
                
            }

            @Override
            protected void handleReplicatedWrite(final HAWriteMessage msg,
                    final ByteBuffer data) throws Exception {

                if (haLog.isInfoEnabled())
                    haLog.info("msg=" + msg + ", buf=" + data);

                /*
                 * Note: the ByteBuffer is owned by the HAReceiveService. This
                 * just wraps up the reference to the ByteBuffer with an
                 * interface that is also used by the WriteCache to control
                 * access to ByteBuffers allocated from the DirectBufferPool.
                 * However, release() is a NOP on this implementation since the
                 * ByteBuffer is owner by the HAReceiveService.
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

        };

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
