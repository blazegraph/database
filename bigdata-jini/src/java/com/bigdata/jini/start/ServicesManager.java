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
 * Created on Jan 2, 2009
 */

package com.bigdata.jini.start;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationFile;
import net.jini.config.ConfigurationProvider;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.bigdata.Banner;
import com.bigdata.io.SerializerUtil;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.TransactionServer;
import com.bigdata.service.jini.AbstractServer.RemoteAdministrable;
import com.bigdata.service.jini.AbstractServer.RemoteDestroyAdmin;
import com.bigdata.zookeeper.ZookeeperClientConfig;

/**
 * A class for bootstrapping a {@link JiniFederation} across a cluster based on
 * a {@link Configuration} file describing some basic configuration parameters
 * and dynamically managing service discovery using <code>jini</code> and
 * master election and other distributed decision making using
 * <code>zookeeper</code>.
 * <p>
 * Each host runs one or more instances of this class. The class will fork other
 * processes as necessary in order to start jini, start zookeeper, start an
 * httpd service for the RMI CODEBASE, and start an instance of the various
 * services required by an {@link IBigdataFederation} if instances of those
 * services either can not be discovered or the configuration requirements in
 * zookeeper are not satisfied.
 * <p>
 * The initial configuration for the federation is specified in a
 * {@link Configuration} file. That file is best located on a shared volume or
 * volume image replicated on the hosts in the cluster, along with the various
 * dependencies required to start and run the federation (java, jini, bigdata,
 * log4j configuration, related JARs, etc). The {@link Configuration} is used to
 * discover and/or start jini and zookeeper instance(s). Once zookeeper can be
 * discovered (using its own protocol, not jini), {@link ServicesManager}
 * instances will contend for a lock on a node corresponding to the
 * {@link IBigdataFederation} described in the {@link Configuration}. If the
 * node is empty, then it will be populated with the initial configuration by
 * whichever process holds the lock, which is why it is important that all hosts
 * running this class are use an identical {@link Configuration}. Thereafter,
 * zookeeper will be used to manage the services in the federation.
 * <p>
 * Once running, the {@link ServicesManager} will watch the zookeeper nodes for
 * the various kinds of services and will start or stop services as the state of
 * those nodes changes.
 * 
 * <pre>
 * 
 * zroot-for-federation
 *     / config 
 *              / jini {ServiceConfiguration}
 *                  ...
 *              / ClassServer {JavaConfiguration}
 *                  ...
 *              / TransactionServer {BigdataServiceConfiguration}
 *                  ...
 *              / MetadataServer
 *                  ...
 *              / DataServer {ServiceConfiguration}
 *                      / logicalService1 {logicalServiceUUID, params}
 *                                The order of the children determines
 *                                replication chain and new primary if the
 *                                master fails.
 *                          / physicalService1 {serviceUUID, host}
 *                          / physicalService2 {serviceUUID, host}
 *                          ...
 *                      / logicalService2 {logicalServiceUUID, params}
 *                  ...
 *              / LoadBalancerServer
 *                  ...
 *              / ResourceLockServer
 *                  ...
 * </pre>
 * 
 * Each configuration node defines the service type, the target #of service
 * instances, the replication count, etc for a service. The children of the
 * configuration node are the logical service instances and use
 * {@link CreateMode#PERSISTENT_SEQUENTIAL}.
 * 
 * The children of a logical service are the actual service instances. Those
 * nodes use {@link CreateMode#EPHEMERAL_SEQUENTIAL} so that zookeeper will
 * remove them if the client dies. The ordered set of instances for a logical
 * service is in effect an election, and the winner is the current primary for
 * that service (for services which have a primary rather than just peers).
 * 
 * Each {@link ServicesManager} sets a {@link Watcher} for the each service
 * configuration node. This allows it to observe changes in the target
 * serviceCount for a given service type and the target replicationCount for a
 * logical service of that type. If the #of logical services is under the target
 * count, then we need to create a new logical service instance (just a znode)
 * and set a watch on it (@todo anyone can create the new logical service since
 * it only involves assigning a UUID, but we need an election or a lock to
 * decide who actually does it so that we don't get a flood of logical services
 * created. All watchers need to set a watch on the new logical service once it
 * is created.) [note: we need the logical / physical distinction for services
 * such as jini which are peers even before we introduce replication for bigdata
 * services.]
 * 
 * The {@link ServicesManager} also sets a {@link Watcher} for each logical
 * service of any service type. This allows it to observe the join and leave of
 * physical service instances. If it observes that a logical service is under
 * the target replication count (which either be read from the configuration
 * node which is the parent of that logical service or must be copied onto the
 * logical service when it is created) AND the host satisifies the
 * {@link IServiceConstraint}s, then it joins a priority election of ephemeral
 * nodes (@todo where) and waits for up to a timeout for N peers to join. The
 * winner of the election is the {@link ServicesManager} on the host best suited
 * to start the new service instance and it starts an instance of the service on
 * the host where it is running. (@todo after the new service starts, the
 * logical service node will gain a new child (in the last position). that will
 * trigger the watch. if the logical service is still under the target, then the
 * process will repeat.)
 * 
 * @todo Replicated bigdata services are created under a parent having an
 *       instance number assigned by zookeeper. The parent corresponds to a
 *       logical service. It is assigned a UUID for compatibility with the
 *       existing APIs (which do not really support replication). The children
 *       of the logical service node are the znodes for the physical instances
 *       of that logical service.
 * 
 * Each physical service instance has a service UUID, which is assigned by jini
 * sometime after it starts and then recorded in the zookeeper znode for that
 * physical service instance.
 * 
 * The physical service instances use an election to determine which of them is
 * the primary, which are the secondaries, and the order for replicating data to
 * the secondaries.
 * 
 * <strong>Destroying a service instance is dangerous - if it is not replicated
 * then you can loose all your data!</strong>. In order to destroy a specific
 * service instance you have to use the {@link RemoteAdministrable} API or zap
 * it from the command line (that will only take the service down, but not
 * destroy its data). However, if the federation is now undercapacity for that
 * service type (or under the replication count for a service instance), then a
 * new instance will simply be created.
 * 
 * The services create an EPHERMERAL znode when they (re)start. That znode
 * contains the service UUID and is deleted automatically by zookeeper when the
 * service's {@link ZooKeeper} client is closed. The services with persistent
 * state DO NOT use the SEQUENTIAL flag since the same znode MUST be re-created
 * if the service is restarted. Also, since the znodes are EPHEMERAL, no
 * children are allowed. Therefore all behavior performed by the services occurs
 * in queues, elections and other high-level data control structures using
 * znodes elsewhere in zookeeper.
 * 
 * @todo management semantics for deleting znodes are not yet defined. Probably
 *       a physical service should watch its logicalService's znode and compete
 *       to re-create that znode if it is deleted. Likewise, the physical
 *       service should ensure that its own znode is not removed (that can be
 *       done via ACLs as well).
 *       <p>
 *       In particular, deleting a znode SHOULD NOT cause the corresponding
 *       logical or physical service to be terminated. Use the
 *       {@link RemoteDestroyAdmin} API to terminate physical services.
 * 
 * @todo The federation can be destroyed using {@link JiniFederation#destroy()}.
 *       (Modify this to to delete the zroot, which gives us an ACL for
 *       destroying the federation).
 * 
 * @todo document dependencies for performance counter reporting and supported
 *       platforms.
 * 
 * @todo make this a discoverable service (using jini) and support destroy of
 *       that service.
 * 
 * FIXME Add a federation identifier and use it as a filter for service
 * discovery in order to make it impossible for one federation to "pickup"
 * services from another. That identifier should also be used as the [zroot] of
 * the federation in zookeeper (and the parameter for that removed from the
 * zookeeper client config since we have it on the federation client).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see ServiceConfigurationWatcher, manages the #of logical service instances
 *      for a {@link ServiceConfiguration} znode.
 * 
 * @see LogicalServiceWatcher, manages the #of physical service instances for a
 *      logical service (the physical service manages its own znode and uses an
 *      ACL to prevent others from messing with it).
 */
public class ServicesManager implements IServiceListener {

    protected static final Logger log = Logger.getLogger(ServicesManager.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    protected static final transient String ZSLASH = "/";
    
    /**
     * {@link Configuration} options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {
       
        /**
         * Namespace for these options.
         */
        String NAMESPACE = "com.bigdata.jini.start";
        
        /**
         * {@link ACL} that will be used if we create the root znode for the
         * federation.
         * 
         * @todo move to {@link ZookeeperClientConfig} and refactor to be a
         *       {@link ServiceConfiguration} instance (even though we must
         *       bootstap zookeeper if it is not already running).
         */
        String ACL = "acl";
        
        /**
         * {@link File} that will be executed to start jini.
         * 
         * @todo not used yet. probably move into the
         *       {@link JiniRegistrarServiceConfiguration} (an subclass of
         *       {@link ServiceConfiguration} that is specialized for starting
         *       jini).
         */
        String JINI = "jini";
        
    }
    
    /**
     * @todo verify java version used to run this class.
     * 
     * @todo should we automatically use the same java version or one specified
     *       in the configuration?
     * 
     * @todo verify jar(s)?
     * 
     * @todo verify jini installer?
     * 
     * @todo verify platform specific dependencies (vmstat, typeperf, sysstat,
     *       etc).
     */
    protected void checkDependencies() {
        
    }
    
    /**
     * The {@link Configuration} object obtained by processing the command line
     * arguments provided to the ctor.
     */
    protected final ConfigurationFile config;
    
//    /**
//     * The local host.
//     */
//    protected final InetAddress localhost;
    
    /**
     * The federation.
     */
    private JiniFederation fed;
    
    public JiniFederation getFederation() {
        
        if (fed == null)
            throw new IllegalStateException();

        return fed;
        
    }
    
    /**
     * The set of currently running {@link Process}es. A {@link Process} is
     * automatically added to this collection by the {@link ProcessHelper} and
     * will remove itself from this collection once the
     * {@link Process#getInputStream()} is closed (e.g., when the process is
     * terminated). If you {@link Process#destroy()} a {@link Process}
     * registered by the {@link ProcessHelper} in this collection, then it will
     * automatically become unregistered.
     */
    final private ConcurrentLinkedQueue<ProcessHelper> runningProcesses = new ConcurrentLinkedQueue<ProcessHelper>();
    
    public void add(ProcessHelper service) {
        
        runningProcesses.add(service);
        
    }

    public void remove(ProcessHelper service) {
        
        runningProcesses.remove(service);
        
    }
    
    /**
     * Invoked by {@link #main(String[])}.
     * 
     * @see #main(String[])
     */
    protected ServicesManager(final String[] args) throws Exception {

        // Obtain the configuration object.
        config = (ConfigurationFile) ConfigurationProvider.getInstance(args);

//        localhost = InetAddress.getLocalHost();
        
        /*
         * The runtime shutdown hook appears to be a robust way to handle ^C by
         * providing a clean service termination.
         * 
         * Note: This is setup before we start any async threads, including
         * service discovery.
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        
        checkDependencies();

        // if necessary, start zookeeper (a server instance).
        ZookeeperProcessHelper.startZookeeper(config, this/*listener*/);

        /*
         * Connect to jini and zookeeper.
         * 
         * Note: if jini is not running, the federation will listen for jini
         * registars. Once one is running, it will be used to discover the other
         * services. If we are supposed to be running jini, then it will be
         * started by the watcher.
         */
        fed = JiniClient.newInstance(args).connect();
        
//        // add watcher to manage our services.
//        fed.addWatcher(new ServiceManager());

        /*
         * Establish watchers for all nodes of interest and populate the
         * federation configuration if the [zroot] does not exist.
         */
        {
            
            final ZooKeeper zoo = fed.getZookeeper();

            // root znode for the federation.
            final String zroot = fed.getZooConfig().zroot;

            // znode for configuration metadata.
            final String zconfig = zroot + ZSLASH + "config";

            // true iff the zroot is known to exist right now.
            final boolean exists = zoo.exists(zroot, false/* watch */) != null;
            
            // watch [zroot] itself.
            // zoo.exists(zroot, true/* watch */);

            /*
             * Watch the config znode. If any children are added then this
             * watcher will set up a watcher on the service configuration node.
             */
            zoo.exists(zconfig, new ConfigWatcher(fed,this));
            
            /*
             * Set watch for service configuration changes.
             * 
             * Note: zookeeper only allows a "." to appears as a filename
             * extension, so it is not directly compatible with fully qualified
             * java class names.
             * 
             * @todo start httpd for downloadable code. (contend for lock on
             * node, start instance if insufficient instances are running). The
             * codebase URI should be the concatenation of the URIs for each
             * httpd instance that has been configured. Unlike some other
             * configuration properties, I am not sure that the codebase URI can
             * be changed once a service has been started. We will have to
             * unpack all of the classes into the file system, and then possibly
             * create a single JAR from them, and expose that use the
             * ClassServer. This should be done BEFORE starting jini since jini
             * can then recognize our services in the service browser (the
             * codebase URI needs to be set for that to work).
             * 
             * @see https://deployutil.dev.java.net/
             * 
             * @todo start jini using configured RMI codebase URO (contend for
             * lock on node, start instance if insufficient instances are
             * running). jini should be started iff one of the locators
             * corresponds to an address for this host and jini is not found to
             * be running. We will need some sort of lock to resolve contention
             * concerning which instance of this class should start jini. That
             * can be done using zookeeper.
             * 
             * @todo Do the txService. NO dependencies (well, jini must be
             * running for it to be discoverable).
             * 
             * @todo Do the resource lock server. NO dependencies (jini must be
             * running for it to be discoverable). For the short term, uses
             * zookeeper to provide non-hierarchical locks w/o deadlock
             * detection. Will be replaced by full transactions. A client
             * library can provide queues and barriers using zookeeper.
             * 
             * @todo Do the LBS.
             * 
             * @todo Do the MDS.
             * 
             * @todo Do the data services.
             * 
             */
//            zoo.exists(zconfig + ZSLASH + "jini", true/* watch */);
//            zoo.exists(zconfig + ZSLASH + ClassServer.class.getSimpleName(), true/* watch */);
//            zoo.exists(zconfig + ZSLASH + TransactionServer.class.getSimpleName());
            
            new ServiceConfigurationWatcher(fed, this/* listener */, zconfig
                    + ZSLASH + TransactionServer.class.getSimpleName());
            
//            zoo.exists(zconfig + ZSLASH + ResourceLockServer.class.getSimpleName(), true/* watch */);
//            zoo.exists(zconfig + ZSLASH + MetadataServer.class.getSimpleName(), true/* watch */);
//            zoo.exists(zconfig + ZSLASH + DataServer.class.getSimpleName(), true/* watch */);
//            zoo.exists(zconfig + ZSLASH + LoadBalancerServer.class.getSimpleName(), true/* watch */);
            
            if (!exists) {
                
                /*
                 * Create the configuration root.
                 * 
                 * If successful, we will populate the configuration.
                 * 
                 * Otherwise an exception is thrown and someone else has already
                 * done this concurrently.
                 * 
                 * Note: Since zookeeper does not support transaction across
                 * more than one request we validate all of the configuration
                 * entries before we create the zroot node.
                 */

                // ACL for the zroot.
                final List<ACL> acl = Arrays.asList((ACL[]) config.getEntry(
                        Options.NAMESPACE, "acl", ACL[].class));
                
//                final ServiceConfiguration jiniConfig = new JiniRegistrarServiceConfiguration(
//                        config);
                
//                final ServiceConfiguration classServerConfig = new JavaServiceConfiguration(
//                        ClassServer.class, config);

                final ServiceConfiguration transactionServerConfig = new TransactionServiceConfiguration(
                        config);

//                final ServiceConfiguration metadataServerConfig = new BigdataServiceConfiguration(
//                        MetadataServer.class, config);
//
//                final ServiceConfiguration dataServerConfig = new BigdataServiceConfiguration(
//                        DataServer.class, config);
//
//                final ServiceConfiguration resourceLockServerConfig = new BigdataServiceConfiguration(
//                        ResourceLockServer.class, config);
//
//                final ServiceConfiguration loadBalancerServerConfig = new BigdataServiceConfiguration(
//                        LoadBalancerServer.class, config);

                try {

                    zoo.create(zroot, new byte[] {}/* data */, acl,
                            CreateMode.PERSISTENT);

                    zoo.create(zconfig, new byte[] {}/* data */, acl,
                            CreateMode.PERSISTENT);

//                    // jini registrar(s).
//                    zoo.create(zconfig + ZSLASH + "jini", SerializerUtil
//                            .serialize(jiniConfig), acl, CreateMode.PERSISTENT);
//
//                    // class server(s).
//                    zoo.create(zconfig + ZSLASH
//                            + ClassServer.class.getSimpleName(), SerializerUtil
//                            .serialize(classServerConfig), acl,
//                            CreateMode.PERSISTENT);

                    // transaction server(s).
                    zoo.create(zconfig + ZSLASH
                            + TransactionServer.class.getSimpleName(),
                            SerializerUtil.serialize(transactionServerConfig),
                            acl, CreateMode.PERSISTENT);
                    
//                    // metadata server(s).
//                    zoo.create(zconfig + ZSLASH
//                            + MetadataServer.class.getSimpleName(),
//                            SerializerUtil.serialize(metadataServerConfig),
//                            acl, CreateMode.PERSISTENT);
//
//                    // data server(s).
//                    zoo.create(zconfig + ZSLASH
//                            + DataServer.class.getSimpleName(), SerializerUtil
//                            .serialize(dataServerConfig), acl,
//                            CreateMode.PERSISTENT);
//
//                    // resource lock server(s).
//                    zoo.create(zconfig + ZSLASH
//                            + ResourceLockServer.class.getSimpleName(),
//                            SerializerUtil.serialize(resourceLockServerConfig),
//                            acl, CreateMode.PERSISTENT);
//
//                    // load balancer server(s).
//                    zoo.create(zconfig + ZSLASH
//                            + LoadBalancerServer.class.getSimpleName(),
//                            SerializerUtil.serialize(loadBalancerServerConfig),
//                            acl, CreateMode.PERSISTENT);
                    
                } catch (KeeperException ex) {

                    if (ex.getCode() == KeeperException.Code.NodeExists) {

                        // that's fine - the configuration already exists.
                        if (INFO)
                            log.info("Node exists: zroot=" + zroot);

                    }

                    // anything else is a problem.
                    throw ex;

                }

            }
            
        }

    }

    /**
     * Run the server (this should be invoked from <code>main</code>).
     */
    public void run() {

        if (INFO)
            log.info("Running...");

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
        
        /*
         * Note: I have found the Runtime shutdown hook to be much more robust
         * than attempting to install a signal handler.  It is installed by
         * the server constructor rather than here so that it will be used 
         * when the server is run by the ServiceStarter as well as from main().
         */
        
        /*
         * Wait until the server is terminated.
         */
        
        Object keepAlive = new Object();
        
        synchronized (keepAlive) {
            
            try {
                
                keepAlive.wait();
                
            } catch (InterruptedException ex) {
                
                if (INFO)
                    log.info(ex.getLocalizedMessage());

            } finally {
                
                // terminate.
                
                shutdownNow();
                
            }
            
        }
        
    }

    /**
     * Runs {@link AbstractServer#shutdownNow()} and terminates all asynchronous
     * processing, including discovery.
     * 
     * FIXME must notify the object that holds this service alive.
     * 
     * FIXME make this class an {@link AbstractServer} and reuse its shutdown
     * logic.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class ShutdownThread extends Thread {
        
        protected ShutdownThread() {
            
            super("shutdownThread");
            
        }
        
        public void run() {
            
            try {

                if (INFO)
                    log.info("Running shutdown.");

                /*
                 * Note: This is the "server" shutdown. It will delegate to the
                 * service shutdown protocol as well as handle unexport of the
                 * service and termination of jini processing.
                 */
                
                shutdownNow();
                
            } catch (Exception ex) {

                log.error("While shutting down service: " + ex, ex);

            }

        }
        
    }

    /**
     * Overriden to use {@link System#exit()} since this is the command
     * line interface.
     */
    protected void fatal(String msg, Throwable t) {

        log.fatal(msg, t);

        try {

            shutdownNow();
            
        } catch (Throwable t2) {
            
            log.error(t2.getMessage(), t2);
            
        }
        
        System.exit(1);

    }

    /**
     * Destroys any managed services (those started by this process and
     * represented in {@link #runningProcesses}).
     */
    protected void shutdownNow() {
        
        if(INFO)
            log.info("");

        // disconnect from the federation (jini and zookeeper clients).
        if (fed != null) {

            fed.shutdownNow();
            
            fed = null;
            
        }
        
        // destroy any running processes.
        for(ProcessHelper helper : runningProcesses) {
            
            helper.destroy();
            
        }

    }
    
    /**
     * Conditionally install a suitable security manager if there is none in
     * place.
     */
    static protected void setSecurityManager() {

        final SecurityManager sm = System.getSecurityManager();

        if (sm == null) {

            System.setSecurityManager(new SecurityManager());

            if (INFO)
                log.info("Set security manager");

        } else {

            if (INFO)
                log.info("Security manager already in place: " + sm.getClass());

        }

    }

    /**
     * Starts and maintains services based on the specified configuration file
     * and/or an existing zookeeper ensemble.
     * 
     * <pre>
     * java -Djava.security.policy=policy.all com.bigdata.jini.service.BigdataStarter src/resources/config/bigdata.config
     * </pre>
     * 
     * @param args
     *            The command line arguments.
     * 
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {

        // Show the copyright banner during statup.
        Banner.banner();

        setSecurityManager();

        // start the server.
        new ServicesManager(args).run();
        
    }
    
    /**
     * Watcher for the <code>config</code> znode. The children are service
     * configurations. The watcher registers a
     * {@link ServiceConfigurationWatcher} for each child to monitor changes in
     * its {@link ServiceConfiguration}.
     * 
     * @todo run the {@link ServiceConfigurationWatcher} for each pre-existing
     *       child in case there is an action which could be triggered based on
     *       its current state?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class ConfigWatcher implements Watcher {

        protected final JiniFederation fed;
        protected final IServiceListener listener;

        /** The path to the "config" znode. */
        private final String zconfig;

        public ConfigWatcher(final JiniFederation fed,
                final IServiceListener listener)
                throws KeeperException, InterruptedException {

            if (fed == null)
                throw new IllegalArgumentException();

            if (listener == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;
            
            this.listener = listener;
            
            this.zconfig = fed.getZooConfig().zroot + "/config";

//            final ServiceConfigurationWatcher serviceConfWatcher;
//            
//            this.serviceConfWatcher = serviceConfWatcher;

            updateWatchers();
            
        }
        
        public void process(final WatchedEvent event) {
    
            try {

                updateWatchers();
                
            } catch (Exception ex) {
                
                log.error(ex.getLocalizedMessage(), ex);
                
            }
            
        }
        
        /**
         * (Re)sets a watch on the {@link #zconfig} node, on its children (if
         * the node exists), and on each child that exists at this time. This
         * class watches the {@link #zconfig} node and its list of children
         * while the {@link #serviceConfWatcher} watches the child nodes
         * themselves.
         */
        protected void updateWatchers() throws KeeperException, InterruptedException {
           
            // set watch on the zconfig node.
            fed.getZookeeper().exists(zconfig, this);
            
            final List<String> children;
            try {

                /*
                 * Get list of children (and set watch on the list of children).
                 */
               
                children = fed.getZookeeper().getChildren(zconfig, this);
                
            } catch (KeeperException.NoNodeException ex) {

                return;
                
            }

            for (String child : children) {

                /*
                 * Watcher for service configuration nodes.
                 * 
                 * @todo Create one watcher per child.
                 */
//                final ServiceConfigurationWatcher serviceConfigWatcher = new ServiceConfigurationWatcher(
//                        fed, listener, zconfig + ZSLASH + child);
//
//                // set watcher on child.
//                fed.getZookeeper().exists(zconfig + ZSLASH + child,
//                        serviceConfigWatcher);
                
            }
            
        }
        
    }

}
