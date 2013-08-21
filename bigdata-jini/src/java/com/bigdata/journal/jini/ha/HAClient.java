/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Mar 24, 2007
 */

package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceItem;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.ha.HAGlue;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.service.IDataService;
import com.bigdata.service.IService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.zookeeper.ZooKeeperAccessor;
import com.sun.jini.start.ServiceDescriptor;

/**
 * A client capable of connecting to a bigdata highly available replication
 * cluster.
 * <p>
 * Clients are configured using a Jini service configuration file. The name of
 * that file is passed to {@link #newInstance(String[])}. The configuration must
 * be consistent with the configuration of the federation to which you wish to
 * connect.
 * 
 * FIXME Review how we connect to a specific logical HA replication cluster and
 * verify that we can connect to multiple such clusters in order to support RMI
 * operations across those clusters (as long as they are in the same space). It
 * might be that this is achieved through a HAConnection to each logical cluster
 * instance, or not. The quorums are certainly specific to a logical instance
 * and that is determined by the zkroot. Investigate!
 * 
 * FIXME Retro fit into HAJournalServer (discoveryClient) and AbstractServer.
 * 
 * FIXME This does not start up a quorum and set a quorum client. It should so
 * we can see the quorum events. That could be a useful thing to do in main().
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HAClient {

    private static final Logger log = Logger.getLogger(HAClient.class);
    
    /**
     * Options understood by the {@link HAClient}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static interface Options { 
        
        /**
         * The timeout in milliseconds that the client will await the discovery
         * of a service if there is a cache miss (default
         * {@value #DEFAULT_CACHE_MISS_TIMEOUT}).
         * 
         * @see HAJournalDiscoveryClient
         */
        String CACHE_MISS_TIMEOUT = "cacheMissTimeout";

        String DEFAULT_CACHE_MISS_TIMEOUT = "" + (2 * 1000);
     
    }
    
    /**
     * The value is the {@link HAConnection} and <code>null</code> iff not connected.
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
    
    public HAConnection getConnection() {

        final HAConnection fed = this.fed.get();
        
        if (fed == null) {

            throw new IllegalStateException();

        }

        return fed;

    }

    /**
     * {@inheritDoc}
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

            final HAConnection fed = this.fed.get();

            if (fed != null) {

                if (immediateShutdown) {

                    fed.shutdownNow();

                } else {

                    fed.shutdown();

                }

            }

            this.fed.set(null);

        } finally {

            connectLock.unlock();

        }

    }

    public HAConnection connect() {

        connectLock.lock();

        try {

            HAConnection fed = this.fed.get();

            if (fed == null) {

                fed = new HAConnection(jiniConfig, zooConfig);

                this.fed.set(fed);
                
                fed.start(this);

            }

            return fed;

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

//    /**
//     * {@inheritDoc}
//     * 
//     * @see #getProperties(String component)
//     */
//    public Properties getProperties() {
//        
//        return properties;
//        
//    }
    
//    /**
//     * Return the {@link Properties} for the {@link JiniClient} merged with
//     * those for the named component in the {@link Configuration}. Any
//     * properties found for the {@link JiniClient} "component" will be read
//     * first. Properties for the named component are next, and therefore will
//     * override those given for the {@link JiniClient}. You can specify
//     * properties for either the {@link JiniClient} or the <i>component</i>
//     * using:
//     * 
//     * <pre>
//     * properties = NV[]{...};
//     * </pre>
//     * 
//     * in the appropriate section of the {@link Configuration}. For example:
//     * 
//     * <pre>
//     * 
//     * // Jini client configuration
//     * com.bigdata.service.jini.JiniClient {
//     * 
//     *     // ...
//     * 
//     *     // optional JiniClient properties.
//     *     properties = new NV[] {
//     *        
//     *        new NV(&quot;foo&quot;,&quot;bar&quot;)
//     *        
//     *     };
//     * 
//     * }
//     * </pre>
//     * 
//     * And overrides for a named component as:
//     * 
//     * <pre>
//     * 
//     * com.bigdata.service.FooBaz {
//     * 
//     *    properties = new NV[] {
//     *    
//     *        new NV(&quot;foo&quot;,&quot;baz&quot;),
//     *        new NV(&quot;goo&quot;,&quot;12&quot;),
//     *    
//     *    };
//     * 
//     * }
//     * </pre>
//     * 
//     * @param component
//     *            The name of the component.
//     * 
//     * @return The properties specified for that component.
//     * 
//     * @see #getConfiguration()
//     */
//    public Properties getProperties(final String component)
//            throws ConfigurationException {
//
//        return JiniClient.getProperties(component, getConfiguration());
//
//    }
    
//    /**
//     * Read {@value JiniClientConfig.Options#PROPERTIES} for the optional
//     * application or server class identified by [cls].
//     * <p>
//     * Note: Anything read for the specific class will overwrite any value for
//     * the same properties specified for {@link JiniClient}.
//     * 
//     * @param className
//     *            The class name of the client or service (optional). When
//     *            specified, properties defined for that class in the
//     *            configuration will be used and will override those specified
//     *            for the {@value Options#NAMESPACE}.
//     * @param config
//     *            The {@link Configuration}.
//     * 
//     * @todo this could be replaced by explicit use of the java identifier
//     *       corresponding to the Option and simply collecting all such
//     *       properties into a Properties object using their native type (as
//     *       reported by the ConfigurationFile).
//     */
//    static public Properties getProperties(final String className,
//            final Configuration config) throws ConfigurationException {
//    
//        final NV[] a = (NV[]) config
//                .getEntry(JiniClient.class.getName(),
//                        JiniClientConfig.Options.PROPERTIES, NV[].class,
//                        new NV[] {}/* defaultValue */);
//
//        final NV[] b;
//        if (className != null) {
//
//            b = (NV[]) config.getEntry(className,
//                    JiniClientConfig.Options.PROPERTIES, NV[].class,
//                    new NV[] {}/* defaultValue */);
//    
//        } else
//            b = null;
//    
//        final NV[] tmp = ConfigMath.concat(a, b);
//    
//        final Properties properties = new Properties();
//
//        for (NV nv : tmp) {
//    
//            properties.setProperty(nv.getName(), nv.getValue());
//    
//        }
//
//        if (log.isInfoEnabled() || BigdataStatics.debug) {
//
//            final String msg = "className=" + className + " : properties="
//                    + properties.toString();
//
//            if (BigdataStatics.debug)
//                System.err.println(msg);
//
//            if (log.isInfoEnabled())
//                log.info(msg);
//
//        }
//        
//        return properties;
//    
//    }

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
    public HAClient(final Class cls, final Configuration config)
            throws ConfigurationException {

        if (config == null)
            throw new IllegalArgumentException();
        
//        this.properties = JiniClient.getProperties(cls.getName(), config);
        
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
    
//    /**
//     * Read and return the content of the properties file.
//     * 
//     * @param propertyFile
//     *            The properties file.
//     * 
//     * @throws IOException
//     */
//    static protected Properties getProperties(final File propertyFile)
//            throws IOException {
//
//        if(log.isInfoEnabled()) {
//            
//            log.info("Reading properties: file="+propertyFile);
//            
//        }
//        
//        final Properties properties = new Properties();
//
//        InputStream is = null;
//
//        try {
//
//            is = new BufferedInputStream(new FileInputStream(propertyFile));
//
//            properties.load(is);
//
//            if(log.isInfoEnabled()) {
//                
//                log.info("Read properties: " + properties);
//                
//            }
//            
//            return properties;
//
//        } finally {
//
//            if (is != null)
//                is.close();
//
//        }
//
//    }

    /**
     * Invoked when a service join is noticed.
     * 
     * @param service
     *            The RMI interface for the service.
     * @param serviceUUID
     *            The service identifier.
     */
    public void serviceJoin(final IService service, final UUID serviceUUID) {

        if (log.isInfoEnabled())
            log.info("service=" + service + ", serviceUUID" + serviceUUID);

    }

    /**
     * Invoked when a service leave is noticed.
     * 
     * @param serviceUUID The service identifier.
     */
    public void serviceLeave(final UUID serviceUUID) {

        if (log.isInfoEnabled())
            log.info("serviceUUID=" + serviceUUID);

    }

    /**
     * 
     * TODO Pattern after JiniFederation. Take the DiscoveryListener, etc. from
     * AbstractServer but compare to JiniFederation.
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
        
        private ZooKeeperAccessor zooKeeperAccessor;

        private LookupDiscoveryManager lookupDiscoveryManager;

        private ServiceDiscoveryManager serviceDiscoveryManager;

        /**
         * Caching discovery client for the {@link HAGlue} services.
         */// TODO Rename as haGlueDiscoveryService
        private HAJournalDiscoveryClient discoveryClient;

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
         * 
         * @throws IllegalStateException
         *             if the client disconnected and this object is no longer
         *             valid.
         * 
         *             TODO add getOpenClient() and use to verify that the
         *             client is non-null for various internal methods. or use
         *             lambda to make this safe for things that need to be done
         *             conditionally.
         */
        public HAClient getClient() {

            return clientRef.get();
            
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

        public synchronized void start(final HAClient client) {

            if(client == null)
                throw new IllegalArgumentException();
            
            if (isOpen())
                throw new IllegalStateException();

            if (log.isInfoEnabled())
                log.info(jiniConfig.toString());
            
            final String[] groups = jiniConfig.groups;
            
            final LookupLocator[] lookupLocators = jiniConfig.locators;

            try {

                /*
                 * Connect to a zookeeper service in the declare ensemble of
                 * zookeeper servers.
                 */

                zooKeeperAccessor = new ZooKeeperAccessor(zooConfig.servers,
                        zooConfig.sessionTimeout);
                
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

                } catch(IOException ex) {
                    
                    throw new RuntimeException(
                            "Could not initiate service discovery manager", ex);
                    
                }

                // FIXME Configuration [properties].
//                final long cacheMissTimeout = Long.valueOf(client.getProperties()
//                        .getProperty(JiniClient.Options.CACHE_MISS_TIMEOUT,
//                                JiniClient.Options.DEFAULT_CACHE_MISS_TIMEOUT));
                final long cacheMissTimeout = Long
                        .valueOf(JiniClient.Options.DEFAULT_CACHE_MISS_TIMEOUT);

                // Setup discovery for HAGlue clients.
                // TODO Refactor out of HAJournalServer.
                discoveryClient = new HAJournalDiscoveryClient(
                        serviceDiscoveryManager,
                        null/* serviceDiscoveryListener */, cacheMissTimeout);

                // And set the reference. The client is now "connected".
                this.clientRef.set(client);
                
            } catch (Exception ex) {

                log.fatal("Problem initiating service discovery: "
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
            
            if(!isOpen()) return;

            if (log.isInfoEnabled())
                log.info("begin");
            
            // Disconnect.
            clientRef.set(null);

            final long begin = System.currentTimeMillis();

//            super.shutdown();

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

                discoveryClient.terminate();

                discoveryClient = null;
                
            }

            /*
             * and the lower level jini processes.
             */
            
            if (serviceDiscoveryManager != null) {

                serviceDiscoveryManager.terminate();

                serviceDiscoveryManager = null;

            }

            if (lookupDiscoveryManager != null) {

                lookupDiscoveryManager.terminate();

                lookupDiscoveryManager = null;

            }

            try {

                // close the zookeeper client.
                zooKeeperAccessor.close();

            } catch (InterruptedException e) {

                throw new RuntimeException(e);

            }
            
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

            return discoveryClient.getService();
                    
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

        /**
         * Return ANY {@link HAGlue} service which has been (or could be)
         * discovered and which is part of the connected federation.
         * 
         * @return <code>null</code> if there are NO known {@link HAGlue}
         *         services.
         */
        public HAGlue getAnyHAGlueService() {

            assertOpen();

            return discoveryClient.getService();
            
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
         * Lock controlling access to the {@link #discoveryEvent} {@link Condition}.
         */
        private final ReentrantLock discoveryEventLock = new ReentrantLock();

        /**
         * Condition signaled any time there is a {@link DiscoveryEvent} delivered to
         * our {@link DiscoveryListener}.
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

                return;

            }
            
        }
        
    }

    /**
     * Simple main just connects and then disconnects after a few seconds. It
     * prints out all discovered {@link HAGlue} services before it shutsdown.
     * 
     * @param args
     * @throws ConfigurationException
     * @throws InterruptedException
     */
    public static void main(final String[] args) throws ConfigurationException,
            InterruptedException {

        final HAClient client = new HAClient(args);

        final HAConnection ctx = client.connect();

        try {

            System.out.println("Connected - waiting for service discovery.");
            
            Thread.sleep(5000/* ms */);

            // Get UUIDs for all discovered services.
            final UUID[] serviceIds = ctx.getHAGlueServiceUUIDs(0/* maxCount */);

            System.out.println("Found " + serviceIds.length + " services.");

            for(UUID x : serviceIds) {
                
                System.out.println("service: " + x + " has proxy "
                        + ctx.getHAGlueService(x));
                
            }
            
        } finally {

            ctx.shutdown();
            
        }
        
        System.out.println("Bye");
        
    }
    
}
