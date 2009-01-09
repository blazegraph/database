package com.bigdata.jini.start;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.ACL;

import com.bigdata.Banner;
import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.ServicesManagerServer.Options;
import com.bigdata.service.AbstractService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.TransactionServer;

/**
 * Core impl.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractServicesManagerService extends AbstractService
        implements IServicesManagerService, IServiceListener {

    private final Properties properties;

    /**
     * An object wrapping the properties specified to the ctor.
     */
    public Properties getProperties() {

        return new Properties(properties);

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

    protected AbstractServicesManagerService(Properties properties) {

        // show the copyright banner during statup.
        Banner.banner();

        this.properties = (Properties) properties.clone();

    }

    /**
     * Destroys any managed services (those started by this process and
     * represented in {@link #runningProcesses}).
     */
    synchronized public void shutdown() {
        // destroy any running processes.
        for (ProcessHelper helper : runningProcesses) {

            helper.destroy();

        }

    }

    @Override
    public Class getServiceIface() {

        return IServicesManagerService.class;

    }

    /**
     * Return the parsed {@link Configuration} used to start the service. 
     */
    abstract protected Configuration getConfiguration();

    /**
     * Strengthen the return type.
     */
    abstract public JiniFederation getFederation();

    @Override
    public AbstractServicesManagerService start() {

        try {

            setup();

        } catch (Exception e) {

            throw new RuntimeException(e);

        }

        return this;

    }

    /**
     * Establish watchers for all nodes of interest and populate the federation
     * configuration if the [zroot] does not exist.
     */
    protected void setup() throws Exception {

        final JiniFederation fed = getFederation();

        final ZooKeeper zookeeper = fed.getZookeeper();

        final Configuration config = getConfiguration();

        // root znode for the federation.
        final String zroot = fed.getZooConfig().zroot;

        // znode for configuration metadata.
        final String zconfig = zroot + BigdataZooDefs.ZSLASH
                + BigdataZooDefs.CONFIG;

        /*
         * Create and start task that will monitor the config znode. If any
         * children are added then this task will set up a watcher on the
         * service configuration node. From there everything will happen
         * automatically.
         * 
         * FIXME MUST monitor future on this task and make sure that it is still
         * running, but it is really only used when the config znode children
         * are created.
         */
        final MonitorConfigZNodeTask task = new MonitorConfigZNodeTask(fed,
                this/* listener */);

        final Future f1 = fed.getExecutorService().submit(task);

        // ACL for the zroot.
        final List<ACL> acl = Arrays.asList((ACL[]) config.getEntry(
                Options.NAMESPACE, "acl", ACL[].class));

        try {

            /*
             * Create the configuration root.
             * 
             * If successful, we will populate the configuration.
             * 
             * Otherwise an exception is thrown and someone else has already
             * done this concurrently.
             * 
             * Note: Since zookeeper does not support transaction across more
             * than one request we validate all of the configuration entries
             * before we create the zroot node.
             */

            zookeeper.create(zroot, new byte[] {}/* data */, acl,
                    CreateMode.PERSISTENT);
            
        } catch (NodeExistsException ex) {

            // that's fine - the configuration already exists.
            if (INFO)
                log.info("zroot exists: " + zroot);

            return;

        }

        // create critical nodes used by the federation.
        createKeyZNodes(zookeeper, zroot, acl);

        // load configuration from file into zookeeper.
        loadConfiguration(zookeeper, zconfig, acl, config);
        
    }

    /**
     * Create key znodes used by the federation.
     * 
     * @param zookeeper
     * @param zroot
     * @param acl
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected void createKeyZNodes(final ZooKeeper zookeeper,
            final String zroot, final List<ACL> acl) throws KeeperException,
            InterruptedException {

        // znode for configuration metadata.
        zookeeper.create(zroot + BigdataZooDefs.ZSLASH + BigdataZooDefs.CONFIG,
                new byte[0], acl, CreateMode.PERSISTENT);

        // znode for most locks.
        zookeeper.create(zroot + BigdataZooDefs.ZSLASH + BigdataZooDefs.LOCKS,
                new byte[0], acl, CreateMode.PERSISTENT);

        zookeeper.create(zroot + "/"
                + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE, new byte[0],
                acl, CreateMode.PERSISTENT);

    }
    
    /**
     * Load the initial configuration for the federation into zookeeper.
     * 
     * @throws ConfigurationException 
     * @throws InterruptedException 
     * @throws KeeperException 
     * 
     * @todo start httpd for downloadable code. (contend for lock on node, start
     *       instance if insufficient instances are running). The codebase URI
     *       should be the concatenation of the URIs for each httpd instance
     *       that has been configured. Unlike some other configuration
     *       properties, I am not sure that the codebase URI can be changed once
     *       a service has been started. We will have to unpack all of the
     *       classes into the file system, and then possibly create a single JAR
     *       from them, and expose that use the ClassServer. This should be done
     *       BEFORE starting jini since jini can then recognize our services in
     *       the service browser (the codebase URI needs to be set for that to
     *       work).
     * 
     * @see https://deployutil.dev.java.net/
     * 
     * @todo start jini using configured RMI codebase URO (contend for lock on
     *       node, start instance if insufficient instances are running). jini
     *       should be started iff one of the locators corresponds to an address
     *       for this host and jini is not found to be running. We will need
     *       some sort of lock to resolve contention concerning which instance
     *       of this class should start jini. That can be done using zookeeper.
     * 
     * @todo Do the resource lock server. NO dependencies (jini must be running
     *       for it to be discoverable). For the short term, uses zookeeper to
     *       provide non-hierarchical locks w/o deadlock detection. Will be
     *       replaced by full transactions. A client library can provide queues
     *       and barriers using zookeeper.
     * 
     * @todo Do the LBS.
     * 
     * @todo Do the MDS.
     * 
     * @todo Do the data services.
     */
    protected void loadConfiguration(final ZooKeeper zookeeper,
            final String zconfig, final List<ACL> acl,
            final Configuration config) throws KeeperException,
            InterruptedException, ConfigurationException {

        // // jini registrar(s).
        // zoo.create(zconfig + ZSLASH + "jini", SerializerUtil
        // .serialize(jiniConfig), acl, CreateMode.PERSISTENT);
        //
        // // class server(s).
        // zoo.create(zconfig + ZSLASH
        // + ClassServer.class.getSimpleName(), SerializerUtil
        // .serialize(classServerConfig), acl,
        // CreateMode.PERSISTENT);

        // transaction server(s).
        zookeeper.create(zconfig + BigdataZooDefs.ZSLASH
                + TransactionServer.class.getName(), SerializerUtil
                .serialize(new TransactionServiceConfiguration(config)), acl,
                CreateMode.PERSISTENT);

        // // metadata server(s).
        // zoo.create(zconfig + ZSLASH
        // + MetadataServer.class.getSimpleName(),
        // SerializerUtil.serialize(metadataServerConfig),
        // acl, CreateMode.PERSISTENT);
        //
        // // data server(s).
        // zoo.create(zconfig + ZSLASH
        // + DataServer.class.getSimpleName(), SerializerUtil
        // .serialize(dataServerConfig), acl,
        // CreateMode.PERSISTENT);
        //
        // // resource lock server(s).
        // zoo.create(zconfig + ZSLASH
        // + ResourceLockServer.class.getSimpleName(),
        // SerializerUtil.serialize(resourceLockServerConfig),
        // acl, CreateMode.PERSISTENT);
        //
        // // load balancer server(s).
        // zoo.create(zconfig + ZSLASH
        // + LoadBalancerServer.class.getSimpleName(),
        // SerializerUtil.serialize(loadBalancerServerConfig),
        // acl, CreateMode.PERSISTENT);

        
    }
    
}
