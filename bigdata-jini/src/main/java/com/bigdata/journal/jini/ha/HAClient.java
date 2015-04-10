/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Mar 24, 2007
 */

package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.bigdata.ha.HAGlue;
import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.quorum.AbstractQuorumClient;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumClient;
import com.bigdata.quorum.QuorumEvent;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.quorum.zk.QuorumTokenState;
import com.bigdata.quorum.zk.ZKQuorum;
import com.bigdata.quorum.zk.ZKQuorumClient;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.service.IDataService;
import com.bigdata.service.IService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.util.StackInfoReport;
import com.sun.jini.start.ServiceDescriptor;

/**
 * A client capable of connecting to a bigdata highly available replication
 * cluster.
 * <p>
 * Clients are configured using a Jini service configuration file. The name of
 * that file is passed to {@link #newInstance(String[])}. The configuration must
 * be consistent with the configuration of the federation to which you wish to
 * connect.
 * <p>
 * Each HA replication cluster has a logical service identifier. You can use
 * this to obtain the {@link Quorum} for that cluster. See
 * {@link HAConnection#getHAGlueQuorum(String)}. Once you have the quorum, you
 * can get the {@link QuorumClient} and obtain the {@link UUID}s of the leader
 * and the followers using {@link Quorum#token()},
 * {@link QuorumClient#getLeader(long)} and {@link Quorum#getJoined()} (which
 * reports all joined services, including the leader and the followers). You can
 * then use {@link HAConnection#getHAGlueService(UUID)} to obtain the RMI proxy
 * for a given service using its service identifier (the UUID).
 * <p>
 * Once you have the RMI proxy for the service, you can use the {@link HAGlue}
 * interface to talk directly to that service. Some methods of interest include
 * {@link HAGlue#getHAStatus()}, {@link HAGlue#getHostname()}, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/728" > Refactor
 *      to create HAClient</a>
 * 
 *      TODO Refactor the HA3 test suite to use the HAClient class.
 */
public class HAClient {

    private static final Logger log = Logger.getLogger(HAClient.class);

    /**
     * Options understood by the {@link HAClient}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static interface ConfigurationOptions {

        /**
         * The namespace for the configuration options declared by this
         * interface.
         */
        String COMPONENT = HAClient.class.getName();

        /**
         * The timeout in milliseconds to await the discovery of a service if
         * there is a cache miss (default {@value #DEFAULT_CACHE_MISS_TIMEOUT}).
         */
        String CACHE_MISS_TIMEOUT = "cacheMissTimeout";

        long DEFAULT_CACHE_MISS_TIMEOUT = 2000L;

    }

    /**
     * The value is the {@link HAConnection} and <code>null</code> iff not
     * connected.
     */
    private final AtomicReference<HAConnection> fed = new AtomicReference<HAConnection>();

    /**
     * The lock used to guard {@link #connect()} and
     * {@link #disconnect(boolean)}.
     * <p>
     * Note: In order to avoid some deadlocks during the shutdown protocol, I
     * refactored several methods which were using synchronized(this) to either
     * use an {@link AtomicReference} (for {@link #fed} or to use a hidden lock.
     */
    private final Lock connectLock = new ReentrantLock(false/* fair */);

    public boolean isConnected() {

        return fed.get() != null;

    }

    /**
     * Get the current {@link HAConnection}.
     * 
     * @return The {@link HAConnection}.
     * 
     * @throws IllegalStateException
     *             if the {@link HAClient} is not connected.
     */
    public HAConnection getConnection() {

        final HAConnection fed = this.fed.get();

        if (fed == null) {

            throw new IllegalStateException();

        }

        return fed;

    }

    /**
     * Terminate the connection if one exists.
     * <p>
     * Note: Immediate shutdown can cause odd exceptions to be logged. Normal
     * shutdown is recommended unless there is a reason to force immediate
     * shutdown.
     * 
     * <pre>
     * java.rmi.MarshalException: error marshalling arguments; nested exception is: 
     *     java.io.IOException: request I/O interrupted
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethodOnce(BasicInvocationHandler.java:785)
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethod(BasicInvocationHandler.java:659)
     *     at net.jini.jeri.BasicInvocationHandler.invoke(BasicInvocationHandler.java:528)
     *     at $Proxy5.notify(Ljava.lang.String;Ljava.util.UUID;Ljava.lang.String;[B)V(Unknown Source)
     * </pre>
     * 
     * These messages may be safely ignored if they occur during immediate
     * shutdown.
     * 
     * @param immediateShutdown
     *            When <code>true</code> the shutdown is <em>abrupt</em>. You
     *            can expect to see messages about interrupted IO such as
     */
    public void disconnect(final boolean immediateShutdown) {

        connectLock.lock();

        try {

            final HAConnection cxn = this.fed.get();

            if (cxn != null) {

                if (immediateShutdown) {

                    cxn.shutdownNow();

                } else {

                    cxn.shutdown();

                }

            }

            this.fed.set(null);

        } finally {

            connectLock.unlock();

        }

    }

    /**
     * Return a valid connection. If the client is not connected, then it is
     * connected. If the client is connected, the existing connection is
     * returned.
     */
    public HAConnection connect() {

        connectLock.lock();

        try {

            HAConnection cxn = this.fed.get();

            if (cxn == null) {

                cxn = new HAConnection(jiniConfig, zooConfig);

                this.fed.set(cxn);

                cxn.start(this);

            }

            return cxn;

        } finally {

            connectLock.unlock();

        }

    }

    /**
     * The {@link JiniClientConfig}.
     */
    public final JiniClientConfig jiniConfig;

    /**
     * The {@link ZooKeeper} client configuration.
     */
    public final ZookeeperClientConfig zooConfig;

    /**
     * The {@link Configuration} object used to initialize this class.
     */
    private final Configuration config;

    /**
     * The {@link JiniClientConfig}.
     */
    public JiniClientConfig getJiniClientConfig() {

        return jiniConfig;

    }

    /**
     * The {@link ZooKeeper} client configuration.
     */
    public final ZookeeperClientConfig getZookeeperClientConfig() {

        return zooConfig;

    }

    /**
     * The {@link Configuration} object used to initialize this class.
     */
    public final Configuration getConfiguration() {

        return config;

    }

    /**
     * Installs a {@link SecurityManager} and returns a new client.
     * 
     * @param args
     *            Either the command line arguments or the arguments from the
     *            {@link ServiceDescriptor}. Either way they identify the jini
     *            {@link Configuration} (you may specify either a file or URL)
     *            and optional overrides for that {@link Configuration}.
     * 
     * @return The new client.
     * 
     * @throws RuntimeException
     *             if there is a problem reading the jini configuration for the
     *             client, reading the properties for the client, etc.
     */
    public static HAClient newInstance(final String[] args) {

        // set the security manager.
        setSecurityManager();

        try {

            return new HAClient(args);

        } catch (ConfigurationException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * Configures a client.
     * 
     * @param args
     *            The jini {@link Configuration} (you may specify either a file
     *            or URL) and optional overrides for that {@link Configuration}.
     * 
     * @throws ConfigurationException
     */
    public HAClient(final String[] args) throws ConfigurationException {

        this(HAClient.class, ConfigurationProvider.getInstance(args));

    }

    /**
     * Configures a client.
     * 
     * @param cls
     *            The class of the client (optional) determines the component
     *            whose configuration will be read in addition to that for the
     *            {@link JiniClient} itself. Component specific values will
     *            override those specified for the {@link JiniClient} in the
     *            {@link Configuration}.
     * @param config
     *            The configuration object.
     * 
     * @throws ConfigurationException
     */
    public HAClient(final Class<?> cls, final Configuration config)
            throws ConfigurationException {

        if (config == null)
            throw new IllegalArgumentException();

        // this.properties = JiniClient.getProperties(cls.getName(), config);

        this.jiniConfig = new JiniClientConfig(cls.getName(), config);

        this.zooConfig = new ZookeeperClientConfig(config);

        this.config = config;

    }

    /**
     * Conditionally install a suitable security manager if there is none in
     * place. This is required before the client can download code. The code
     * will be downloaded from the HTTP server identified by the
     * <code>java.rmi.server.codebase</code> property specified for the VM
     * running the service.
     */
    static protected void setSecurityManager() {

        final SecurityManager sm = System.getSecurityManager();

        if (sm == null) {

            System.setSecurityManager(new SecurityManager());

            if (log.isInfoEnabled())
                log.info("Set security manager");

        } else {

            if (log.isInfoEnabled())
                log.info("Security manager already in place: " + sm.getClass());

        }

    }

    /**
     * Invoked when a service join is noticed.
     * 
     * @param service
     *            The RMI interface for the service.
     * @param serviceUUID
     *            The service identifier.
     * 
     *            TODO It is pointless having this method and
     *            {@link #serviceLeave(UUID)} without a facility to delegate
     *            these methods to override them. Right now they just log.
     */
    protected void serviceJoin(final IService service, final UUID serviceUUID) {

        if (log.isInfoEnabled())
            log.info("service=" + service + ", serviceUUID" + serviceUUID);

    }

    /**
     * Invoked when a service leave is noticed.
     * 
     * @param serviceUUID
     *            The service identifier.
     */
    protected void serviceLeave(final UUID serviceUUID) {

        if (log.isInfoEnabled())
            log.info("serviceUUID=" + serviceUUID);

    }

    /**
     * A connection to discovered {@link HAGlue} services.
     */
    static public class HAConnection implements DiscoveryListener,
            ServiceDiscoveryListener, IServiceShutdown {

        private final JiniClientConfig jiniConfig;

        private final ZookeeperClientConfig zooConfig;

        /**
         * The {@link HAClient} reference. When non-<code>null</code> the client
         * is connected. When <code>null</code> it is disconnected.
         */
        private final AtomicReference<HAClient> clientRef = new AtomicReference<HAClient>();

        private ZooKeeper zk;

        private LookupDiscoveryManager lookupDiscoveryManager;

        private ServiceDiscoveryManager serviceDiscoveryManager;

        /**
         * Caching discovery client for the {@link HAGlue} services.
         */
        private HAGlueServicesClient discoveryClient;

        /**
         * The set of quorums that were accessed through the
         * {@link HAConnection} class.
         * <p>
         * Note: Changes to this map are synchronized on {@link #quorums}. This
         * is done solely to guard against current creates of a {@link Quorum}
         * for the same logical service id. The map itself is thread-safe to
         * avoid contentions for a lock in
         * {@link #terminateDiscoveryProcesses()}.
         */
        private final Map<String, Quorum<HAGlue, ZKQuorumClient<HAGlue>>> quorums = Collections
                .synchronizedMap(new LinkedHashMap<String, Quorum<HAGlue, ZKQuorumClient<HAGlue>>>());

        private HAConnection(final JiniClientConfig jiniConfig,
                final ZookeeperClientConfig zooConfig) {

            if (jiniConfig == null)
                throw new IllegalArgumentException();

            if (zooConfig == null)
                throw new IllegalArgumentException();

            this.jiniConfig = jiniConfig;

            this.zooConfig = zooConfig;

        }

        /**
         * Return the client object that was used to obtain the connection.
         * 
         * @return The {@link HAClient} reference. When non-<code>null</code>
         *         the client is connected. When <code>null</code> it is
         *         disconnected.
         */
        public HAClient getClient() {

            return clientRef.get();

        }

        /**
         * Return the client object that was used to obtain the connection.
         * 
         * @return The {@link HAClient} reference and never <code>null</code>.
         * 
         * @throws IllegalStateException
         *             if the client disconnected.
         */
        public HAClient getClientIfOpen() {

            final HAClient client = clientRef.get();

            if (client == null)
                throw new IllegalStateException();

            return client;

        }

        @Override
        public boolean isOpen() {

            return getClient() != null;

        }

        private void assertOpen() {

            if (!isOpen()) {

                throw new IllegalStateException();

            }

        }

        /**
         * Return the zookeeper client configuration.
         */
        public ZookeeperClientConfig getZooConfig() {

            return zooConfig;

        }

        /**
         * Return the {@link ZooKeeper} client connection.
         * 
         * @throws IllegalStateException
         *             if the {@link HAClient} is not connected.
         */
        public ZooKeeper getZookeeper() {

            assertOpen();

            final ZooKeeper zk = this.zk;

            if (zk == null)
                throw new IllegalStateException();

            return zk;

        }

        private synchronized void start(final HAClient client) {

            if (client == null)
                throw new IllegalArgumentException();

            if (isOpen())
                throw new IllegalStateException();

            if (log.isInfoEnabled())
                log.info(jiniConfig.toString(), new StackInfoReport());

            final String[] groups = jiniConfig.groups;

            final LookupLocator[] lookupLocators = jiniConfig.locators;

            try {

                /*
                 * Note: This class will perform multicast discovery if
                 * ALL_GROUPS is specified and otherwise requires you to specify
                 * one or more unicast locators (URIs of hosts running discovery
                 * services). As an alternative, you can use LookupDiscovery,
                 * which always does multicast discovery.
                 */
                lookupDiscoveryManager = new LookupDiscoveryManager(groups,
                        lookupLocators, this /* DiscoveryListener */,
                        client.getConfiguration());

                /*
                 * Setup a helper class that will be notified as services join
                 * or leave the various registrars to which the data server is
                 * listening.
                 */
                try {

                    serviceDiscoveryManager = new ServiceDiscoveryManager(
                            lookupDiscoveryManager, new LeaseRenewalManager(),
                            client.getConfiguration());

                } catch (IOException ex) {

                    throw new RuntimeException(
                            "Could not initiate service discovery manager", ex);

                }

                /**
                 * The timeout in milliseconds to await the discovery of a
                 * service if there is a cache miss (default
                 * {@value #DEFAULT_CACHE_MISS_TIMEOUT}).
                 */
                final long cacheMissTimeout = (Long) client
                        .getConfiguration()
                        .getEntry(HAClient.ConfigurationOptions.COMPONENT,
                                ConfigurationOptions.CACHE_MISS_TIMEOUT,
                                Long.TYPE,
                                ConfigurationOptions.DEFAULT_CACHE_MISS_TIMEOUT);

                // Setup discovery for HAGlue clients.
                discoveryClient = new HAGlueServicesClient(
                        serviceDiscoveryManager,
                        this/* serviceDiscoveryListener */, cacheMissTimeout);

                /*
                 * Connect to a zookeeper service in the declare ensemble of
                 * zookeeper servers.
                 */
                log.info("Creating ZooKeeper connection.");
                
                zk = new ZooKeeper(zooConfig.servers, zooConfig.sessionTimeout,
                        new Watcher() {
                            @Override
                            public void process(final WatchedEvent event) {
                                if (log.isInfoEnabled())
                                    log.info(event);
                            }
                        });

                /**
                 * Wait until zookeeper is connected. Figure out how long that
                 * took. If reverse DNS is not setup, then the following two
                 * tickets will prevent the service from starting up in a timely
                 * manner. We detect this with a delay of 4+ seconds before
                 * zookeeper becomes connected. This issue does not appear in
                 * zookeeper 3.3.4.
                 * 
                 * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1652
                 * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1666
                 */
                {
                    boolean reverseDSNError = false;
                    final long begin = System.nanoTime();
                    while (zk.getState().isAlive()) {
                        if (zk.getState() == States.CONNECTED) {
                            // connected.
                            break;
                        }
                        final long elapsed = System.nanoTime() - begin;
                        if (!reverseDSNError
                                && TimeUnit.NANOSECONDS.toSeconds(elapsed) > 4) {
                            reverseDSNError = true; // just one warning.
                            log.error("Reverse DNS is not configured. The ZooKeeper client is taking too long to resolve server(s): "
                                    + zooConfig.servers
                                    + ", took="
                                    + TimeUnit.NANOSECONDS.toMillis(elapsed)
                                    + "ms");
                        }
                        if (TimeUnit.NANOSECONDS.toSeconds(elapsed) > 10) {
                            // Fail if we can not reach zookeeper.
                            throw new RuntimeException(
                                    "Could not connect to zookeeper: state="
                                            + zk.getState()
                                            + ", config"
                                            + zooConfig
                                            + ", elapsed="
                                            + TimeUnit.NANOSECONDS
                                                    .toMillis(elapsed) + "ms");
                        }
                        // wait and then retry.
                        Thread.sleep(100/* ms */);
                    }
                }

                // And set the reference. The client is now "connected".
                this.clientRef.set(client);

                log.info("Done.");
                
            } catch (Throwable ex) {

                log.fatal(
                        "Could not connect: "
                                + ex.getMessage(), ex);

                try {

                    shutdownNow();

                } catch (Throwable t) {

                    log.error(t.getMessage(), t);

                }

                throw new RuntimeException(ex);

            }

        }

        /**
         * {@inheritDoc}
         * <p>
         * Extended to terminate discovery.
         */
        @Override
        synchronized public void shutdown() {

            if (!isOpen())
                return;

            if (log.isInfoEnabled())
                log.info("begin");

            // Disconnect.
            clientRef.set(null);

            final long begin = System.currentTimeMillis();

            // super.shutdown();

            terminateDiscoveryProcesses();

            final long elapsed = System.currentTimeMillis() - begin;

            if (log.isInfoEnabled())
                log.info("Done: elapsed=" + elapsed + "ms");

        }

        /**
         * {@inheritDoc}
         * <p>
         * Extended to terminate discovery.
         */
        @Override
        synchronized public void shutdownNow() {

            if (!isOpen())
                return;

            if (log.isInfoEnabled())
                log.info("begin");

            // Disconnect.
            clientRef.set(null);

            final long begin = System.currentTimeMillis();

            // super.shutdownNow();

            terminateDiscoveryProcesses();

            final long elapsed = System.currentTimeMillis() - begin;

            if (log.isInfoEnabled())
                log.info("Done: elapsed=" + elapsed + "ms");

        }

        /**
         * Stop various discovery processes.
         */
        private void terminateDiscoveryProcesses() {

            /*
             * bigdata specific service discovery.
             */

            if (discoveryClient != null) {

                if (log.isInfoEnabled())
                    log.info("Terminating " + discoveryClient);
                
                discoveryClient.terminate();

                discoveryClient = null;

            }

            /*
             * and the lower level jini processes.
             */

            if (serviceDiscoveryManager != null) {

                if (log.isInfoEnabled())
                    log.info("Terminating " + serviceDiscoveryManager);

                serviceDiscoveryManager.terminate();

                serviceDiscoveryManager = null;

            }

            if (lookupDiscoveryManager != null) {

                if (log.isInfoEnabled())
                    log.info("Terminating " + lookupDiscoveryManager);

                lookupDiscoveryManager.terminate();

                lookupDiscoveryManager = null;

            }

            // Terminate any quorums opened by the HAConnection.
            for (Quorum<HAGlue, ZKQuorumClient<HAGlue>> quorum : quorums.values()) {

                quorum.terminate();

            }

            /*
             * Close our zookeeper connection, invalidating all ephemeral znodes
             * for this service.
             * 
             * Note: This provides a decisive mechanism for removing this
             * service from the joined services, the pipeline, withdrawing its
             * vote, and removing it as a quorum member.
             */
            log.warn("FORCING UNCURABLE ZOOKEEPER DISCONNECT");

            if (zk != null) {

                try {
                    zk.close();
                } catch (InterruptedException e) {
                    // propagate the interrupt.
                    Thread.currentThread().interrupt();
                }
                zk = null;

            }

        }

        /**
         * An object used to manage jini service registrar discovery.
         */
        public LookupDiscoveryManager getDiscoveryManagement() {

            return lookupDiscoveryManager;

        }

        /**
         * An object used to lookup services using the discovered service
         * registars.
         */
        public ServiceDiscoveryManager getServiceDiscoveryManager() {

            return serviceDiscoveryManager;

        }

        /**
         * Caching discovery client for the {@link HAGlue} services.
         */
        public HAGlueServicesClient getHAGlueServicesClient() {

            return discoveryClient;

        }

        /**
         * Resolve the service identifier to an {@link IDataService}.
         * <p>
         * Note: Whether the returned object is a proxy or the service
         * implementation depends on whether the federation is embedded (in
         * process) or distributed (networked).
         * 
         * @param serviceUUID
         *            The identifier for a {@link IDataService}.
         * 
         * @return The RMI proxy for the specified {@link HAGlue} or
         *         <code>null</code> iff the {@link HAGlue} could not be
         *         discovered from its identifier.
         */
        public HAGlue getHAGlueService(final UUID serviceUUID) {

            return discoveryClient.getService(serviceUUID);

        }

        /**
         * Resolve the array of service {@link UUID}s to their RMI proxies.
         * 
         * @param serviceUUIDs
         *            The service {@link UUID}s.
         * 
         * @return The correlated array of RMI proxies.
         */
        public HAGlue[] getHAGlueService(final UUID[] serviceUUIDs) {

            final HAGlue[] a = new HAGlue[serviceUUIDs.length];

            for (int i = 0; i < a.length; i++) {

                a[i] = discoveryClient.getService(serviceUUIDs[i]);

            }

            return a;

        }

        /**
         * Return an array UUIDs for discovered {@link HAGlue} services.
         * 
         * @param maxCount
         *            The maximum #of data services whose UUIDs will be
         *            returned. When zero (0) the UUID for all known data
         *            services will be returned.
         * 
         * @return An array of {@link UUID}s for the discovered services.
         */
        public UUID[] getHAGlueServiceUUIDs(final int maxCount) {

            assertOpen();

            return discoveryClient.getServiceUUIDs(maxCount, null/* filter */);

        }

        /*
         * HAGlue Quorum
         */

        /**
         * Return the set of known logical service identifiers for HA
         * replication clusters. These are extracted from zookeeper.
         * 
         * @return The known logical service identifiers (just the last
         *         component of the zpath).
         * 
         * @throws InterruptedException
         * @throws KeeperException
         */
        public String[] getHALogicalServiceIds() throws KeeperException,
                InterruptedException {

            final ZookeeperClientConfig zkClientConfig = getZooConfig();

            // zpath dominating the HA replication clusters.
            final String logicalServiceZPathPrefix = zkClientConfig.zroot + "/"
                    + HAJournalServer.class.getName();

            final String[] children = getZookeeper()
                    .getChildren(logicalServiceZPathPrefix, false/* watch */)
                    .toArray(new String[0]);

            return children;

        }

        /**
         * This is a convenience method that provides access to a Quorum for
         * some logical service corresponding to an HA replication cluster. The
         * returned quorum object will monitor the and reflect the state of the
         * identified quorum. A simple {@link QuorumClient} is started and will
         * run for that quorum until any of: (a) the quorum terminated; (b) the
         * {@link HAConnection} is closed; (c) it is noticed that the zookeeper
         * session is expired.
         * <p>
         * Note: Each quorum that you start has asynchronous threads and MUST be
         * terminated. Termination is automatically performed by
         * {@link HAClient#disconnect(boolean)}.
         * 
         * @param logicalServiceId
         *            The logical service identifier.
         * 
         * @return A {@link Quorum} that will monitor and reflect the state of
         *         that HA replication cluster -or- <code>null</code> iff there
         *         is no such logical service.
         * 
         * @throws InterruptedException
         * @throws KeeperException
         */
        public Quorum<HAGlue, ZKQuorumClient<HAGlue>> getHAGlueQuorum(
                final String logicalServiceId) throws KeeperException,
                InterruptedException {

            /*
             * Fast path. Check for an existing instance. 
             */
            Quorum<HAGlue, ZKQuorumClient<HAGlue>> quorum;
            synchronized (quorums) {

                quorum = quorums.get(logicalServiceId);

                if (quorum != null) {

                    /*
                     * Note: There is no guarantee that the client is running
                     * for the returned quorum. If there is no such quorum, then
                     * a client is created for that quorum. If a quorum is
                     * found, then it is simply returned. The argument is that a
                     * quorum will continue to run for a client unless the
                     * HAClient is disconnected, the zookeeper session is
                     * expired, or quorum.terminate() is explicitly invoked. The
                     * former two cases can only be cured by a disconnect()
                     * followed by a connect(). The latter has to be done
                     * explicitly by the application, so they can deal with it
                     * and start a new client if desired.
                     */
                    return quorum;

                }

            }

            /*
             * Setup a new instance.
             */

            final ZookeeperClientConfig zkClientConfig = getZooConfig();

            // zpath dominating the HA replication clusters.
            final String logicalServiceZPathPrefix = zkClientConfig.zroot + "/"
                    + HAJournalServer.class.getName();

            // zpath for the logical service (child is "quorum" znode).
            final String logicalServiceZPath = logicalServiceZPathPrefix + "/"
                    + logicalServiceId;

            // zpath for the QUORUM znode for the specified logical service.
            final String quorumZPath = logicalServiceZPath + "/"
                    + ZKQuorum.QUORUM;

            final List<ACL> acl = zkClientConfig.acl;

            /*
             * Ensure key znodes exist.
             */
            try {
                getZookeeper().create(zkClientConfig.zroot,
                        new byte[] {/* data */}, acl, CreateMode.PERSISTENT);
            } catch (NodeExistsException ex) {
                // ignore.
            }
            try {
                getZookeeper().create(logicalServiceZPathPrefix,
                        new byte[] {/* data */}, acl, CreateMode.PERSISTENT);
            } catch (NodeExistsException ex) {
                // ignore.
            }
            try {
                getZookeeper().create(logicalServiceZPath,
                        new byte[] {/* data */}, acl, CreateMode.PERSISTENT);
            } catch (NodeExistsException ex) {
                // ignore.
            }

            /*
             * Extract replicationFactor from the quorum znode.
             */
            final int replicationFactor;
            {
                final byte[] data;
                try {

                    final Stat stat = new Stat();

                    data = getZookeeper().getData(quorumZPath,
                            false/* watch */, stat);

                    final QuorumTokenState tokenState = (QuorumTokenState) SerializerUtil
                            .deserialize(data);

                    if (log.isInfoEnabled())
                        log.info("Starting with quorum that has already met in the past: "
                                + tokenState);

                    replicationFactor = tokenState.replicationFactor();

                    if (replicationFactor == 0) {

                        /*
                         * The replication factor was not originally part of the
                         * QuorumTokenState. It was introduced before the first
                         * HA release along with an automated migration
                         * mechanism. If the replicationFactor is not set, then
                         * you need to restart one of the HAJournalServer
                         * processes. It has the correct replication factor in
                         * its Configuration file. ZKQuorumImpl will
                         * automatically impose the replicationFactor when it is
                         * started by the HAJournalServer. We can not do that
                         * here because the HAClient does not know the
                         * replication factor (that information is not part of
                         * its configuration and can vary from one HA
                         * replication cluster to another, which is why it needs
                         * to be stored in zookeeper).
                         */

                        throw new UnsupportedOperationException(
                                "The replicationFactor will be set when an HAJournalProcess is restarted: logicalServiceId="
                                        + logicalServiceId);

                    }

                } catch (NoNodeException e) {
                    // This is Ok. The node just does not exist yet.
                    return null;
                } catch (KeeperException e) {
                    // Anything else is a problem.
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            synchronized (quorums) {

                quorum = quorums.get(logicalServiceId);

                if (quorum == null) {

                    quorums.put(
                            logicalServiceId,
                            quorum = new ZKQuorumImpl<HAGlue, ZKQuorumClient<HAGlue>>(
                                    replicationFactor));//, zka, acl));

                    quorum.start(new MyQuorumClient(logicalServiceZPath));

                }

            }

            return quorum;

        }

        private class MyQuorumClient extends AbstractQuorumClient<HAGlue>
                implements ZKQuorumClient<HAGlue> {

            protected MyQuorumClient(final String logicalServiceZPath) {

                super(logicalServiceZPath);

            }

            @Override
            public HAGlue getService(final UUID serviceId) {

                return getHAGlueService(serviceId);

            }

            @Override
            public ZooKeeper getZooKeeper() {

                return HAConnection.this.getZookeeper();
                
            }

            @Override
            public List<ACL> getACL() {

                return zooConfig.acl;
                
            }

        }

        /*
         * ServiceDiscoveryListener
         */

        /**
         * Invokes {@link HAClient#serviceJoin(IService, UUID)} if the newly
         * discovered service implements {@link IService}.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public void serviceAdded(final ServiceDiscoveryEvent e) {

            final ServiceItem serviceItem = e.getPostEventServiceItem();

            if (serviceItem.service instanceof IService) {

                final UUID serviceUUID = JiniUtil
                        .serviceID2UUID(serviceItem.serviceID);

                final HAClient client = getClient();

                if (client != null) {

                    client.serviceJoin((IService) serviceItem.service,
                            serviceUUID);

                }

            } else {

                if (log.isInfoEnabled())
                    log.info("Not an " + IService.class.getName() + " : " + e);

            }

        }

        /**
         * NOP.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public void serviceChanged(final ServiceDiscoveryEvent e) {

            // Ignored.

        }

        /**
         * Invokes {@link HAClient#serviceLeave(UUID)}.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public void serviceRemoved(final ServiceDiscoveryEvent e) {

            final ServiceItem serviceItem = e.getPreEventServiceItem();

            final UUID serviceUUID = JiniUtil
                    .serviceID2UUID(serviceItem.serviceID);

            final HAClient client = getClient();

            if (client != null) {

                client.serviceLeave(serviceUUID);

            }

        }

        /*
         * DiscoveryListener
         */

        /**
         * Lock controlling access to the {@link #discoveryEvent}
         * {@link Condition}.
         */
        private final ReentrantLock discoveryEventLock = new ReentrantLock();

        /**
         * Condition signaled any time there is a {@link DiscoveryEvent}
         * delivered to our {@link DiscoveryListener}.
         */
        private final Condition discoveryEvent = discoveryEventLock
                .newCondition();

        /**
         * Signals anyone waiting on {@link #discoveryEvent}.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public void discarded(final DiscoveryEvent e) {

            try {

                discoveryEventLock.lockInterruptibly();

                try {

                    discoveryEvent.signalAll();

                } finally {

                    discoveryEventLock.unlock();

                }

            } catch (InterruptedException ex) {

                // Propagate interrupt.
                Thread.currentThread().interrupt();
                return;

            }

        }

        /**
         * Signals anyone waiting on {@link #discoveryEvent}.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public void discovered(final DiscoveryEvent e) {

            try {

                discoveryEventLock.lockInterruptibly();

                try {

                    discoveryEvent.signalAll();

                } finally {

                    discoveryEventLock.unlock();

                }

            } catch (InterruptedException ex) {

                // Propagate interrupt.
                Thread.currentThread().interrupt();
                return;

            }

        }

        /*
         * Misc methods.
         */

        /**
         * Await discovery of at least one {@link ServiceRegistrar}.
         * 
         * @param timeout
         *            The timeout.
         * @param unit
         *            The units for that timeout.
         * 
         * @throws IllegalArgumentException
         *             if minCount is non-positive.
         */
        public ServiceRegistrar[] awaitServiceRegistrars(final long timeout,
                final TimeUnit unit) throws TimeoutException,
                InterruptedException {

            final long begin = System.nanoTime();
            final long nanos = unit.toNanos(timeout);
            long remaining = nanos;

            ServiceRegistrar[] registrars = null;

            while ((registrars == null || registrars.length == 0)
                    && remaining > 0) {

                registrars = getDiscoveryManagement().getRegistrars();

                Thread.sleep(100/* ms */);

                final long elapsed = System.nanoTime() - begin;

                remaining = nanos - elapsed;

            }

            if (registrars == null || registrars.length == 0) {

                throw new RuntimeException(
                        "Could not discover ServiceRegistrar(s)");

            }

            if (log.isInfoEnabled()) {

                log.info("Found " + registrars.length + " service registrars");

            }

            return registrars;

        }

    } // class HAConnection

    /**
     * Simple main just connects and then disconnects after a few seconds. It
     * prints out all discovered {@link HAGlue} services before it shuts down.
     * 
     * @param args
     * 
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public static void main(final String[] args) throws ConfigurationException,
            InterruptedException, KeeperException {

        final HAClient client = new HAClient(args);

        final HAConnection ctx = client.connect();

        /*
         * Note: If non-null, will connect to quorum for this logical service.
         * 
         * TODO Command line arg?
         */
        final String logicalServiceId = null;
//        final String logicalServiceId = "test-1";

        try {

            /*
             * Show the HA replication cluster instances (this data is in
             * zookeeper).
             */
            {

                final String[] a = ctx.getHALogicalServiceIds();

                for (int i = 0; i < a.length; i++) {

                    System.out.println("logicalServiceId: " + a[i]);

                }

            }

            if (logicalServiceId != null) {
                /*
                 * This shows up to lookup a known replication cluster.
                 */
                final Quorum<HAGlue, ZKQuorumClient<HAGlue>> quorum = ctx
                        .getHAGlueQuorum(logicalServiceId);

                // Setup listener that logs quorum events @ TRACE.
                quorum.addListener(new QuorumListener() {
                    @Override
                    public void notify(final QuorumEvent e) {
                        // if (log.isInfoEnabled())
                        // log.info(e);
                        System.err.println("EVENT: " + e);
                    }
                });

            }

            /*
             * Show the discoverable HAGlue services.
             */
            {
                System.out
                        .println("Connected - waiting for service discovery.");

                Thread.sleep(1000/* ms */);

                // Get UUIDs for all discovered services.
                final UUID[] serviceIds = ctx
                        .getHAGlueServiceUUIDs(0/* maxCount */);

                System.out.println("Found " + serviceIds.length + " services.");

                for (UUID x : serviceIds) {

                    System.out.println("service: " + x + ", proxy: "
                            + ctx.getHAGlueService(x));

                }

            }

            if (logicalServiceId != null) {

                /*
                 * Sleep a bit to allow quorum messages to be written to the
                 * console.
                 */
                
                Thread.sleep(10000/* ms */);

            }

        } finally {

            ctx.shutdown();

        }

        System.out.println("Bye");

    }

}
