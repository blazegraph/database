package com.bigdata.jini.start;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.ACL;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.DataServiceConfiguration;
import com.bigdata.jini.start.config.LoadBalancerServiceConfiguration;
import com.bigdata.jini.start.config.MetadataServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.TransactionServiceConfiguration;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.service.AbstractService;
import com.bigdata.service.jini.JiniFederation;

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
     * 
     * @todo we don't really need this since we are not destroying the children
     *       with the parent.
     */
    final private ConcurrentLinkedQueue<ProcessHelper> runningProcesses = new ConcurrentLinkedQueue<ProcessHelper>();

    public void add(ProcessHelper service) {

        runningProcesses.add(service);

    }

    public void remove(ProcessHelper service) {

        runningProcesses.remove(service);

    }

    protected AbstractServicesManagerService(final Properties properties) {

        super();
        
        this.properties = (Properties) properties.clone();

    }

    /**
     * Destroys any managed services (those started by this process and
     * represented in {@link #runningProcesses}), but leaves the zookeeper and
     * jini services for last.
     * 
     * @todo Do not permit new processes to start during shutdown? I am not sure
     *       how much this matters. If the child was started, then it will just
     *       run and succeed or die as it likes.
     */
    synchronized public void shutdown() {

//        if(true) return;
//        
//        final ConcurrentLinkedQueue<ProcessHelper> problems = new ConcurrentLinkedQueue<ProcessHelper>();
//        
//        // destroy any running processes
//        for (ProcessHelper helper : runningProcesses) {
//
//            if (helper instanceof JiniProcessHelper)
//                continue;
//            
//            if (helper instanceof ZookeeperProcessHelper)
//                continue;
//
//            try {
//                helper.kill();
//            } catch (Throwable t) {
//                log.warn("Could not kill process: "+helper);
//                // add to list of problem processes.
//                problems.add(helper);
//                // remove from list of running processes.
//                runningProcesses.remove(helper);
//            }
//
//        }
//
//        // try again for the problem processes, raising the logging level.
//        for (ProcessHelper helper : problems) {
//
//            try {
//                helper.kill();
//            } catch (Throwable t) {
//                log.error("Could not kill process: " + helper);
//                problems.add(helper);
//            }
//
//        }
//
//        /*
//         * This time we take down zookeeper and jini.
//         */ 
//        for (ProcessHelper helper : runningProcesses) {
//
//            try {
//                helper.kill();
//            } catch (Throwable t) {
//                log.warn("Could not kill process: " + helper);
//            }
//
//        }

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
         * We monitor future on this task and make sure that it is still
         * running, but it is really only used when the config znode children
         * are created.
         */
        fed.submitMonitoredTask(new MonitorConfigZNodeTask(fed, this/* listener */));

        /*
         * Create and start task that will compete for locks to start physical
         * service instances.
         */
        fed.submitMonitoredTask(new MonitorCreatePhysicalServiceLocksTask(fed,
                        this/* listener */));

        /*
         * Generate service configurations based on the configuration file.
         * 
         * Note: This lets us validate things before we try to load them into
         * zookeeper.
         * 
         * @todo if we declare the set of configurations in the Configuration
         * file then we can add one more metalevel here.  The jini service
         * starter basically does that.
         */
        final ServiceConfiguration[] serviceConfigurations = getServiceConfigurations(config);

        // ACL for the zroot, etc.
        final List<ACL> acl = fed.getClient().zooConfig.acl;

        // create critical nodes used by the federation.
        createKeyZNodes(zookeeper, zroot, acl);

        // push the service configurations into zookeeper (create/update).
        pushConfiguration(zookeeper, zconfig, acl, serviceConfigurations);
        
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

        final String[] a = new String[] {
          
                // znode for the federation root.
                zroot,
                
                // znode for configuration metadata.
                zroot + "/" + BigdataZooDefs.CONFIG,

                // znode dominating most locks.
                zroot + "/" + BigdataZooDefs.LOCKS,

                zroot + "/" + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE,

                zroot + "/" + BigdataZooDefs.LOCKS_SERVICE_CONFIG_MONITOR,

        };
        
        for (String zpath : a) {

            try {

                zookeeper.create(zpath, new byte[] {}/* data */, acl,
                        CreateMode.PERSISTENT);

            } catch (NodeExistsException ex) {

                // that's fine - the configuration already exists.
                if (INFO)
                    log.info("exists: " + zpath);

                return;

            }

        }

    }

    /**
     * Generates {@link ServiceConfiguration}s from the {@link Configuration}
     * file.
     * 
     * @param config
     *            The {@link Configuration} file.
     * 
     * @return An array of {@link ServiceConfiguration}s populated from the
     *         {@link Configuration} file.
     * 
     * @throws ConfigurationException
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
     * @todo Use class server URL(s) when starting services for their RMI
     *       codebase.
     */
    public ServiceConfiguration[] getServiceConfigurations(Configuration config)
            throws ConfigurationException {

        final List<ServiceConfiguration> v = new LinkedList<ServiceConfiguration>();

            // // class server(s).
            // zoo.create(zconfig + ZSLASH
            // + ClassServer.class.getSimpleName(), SerializerUtil
            // .serialize(classServerConfig), acl,
            // CreateMode.PERSISTENT);

            // transaction server
            v.add(new TransactionServiceConfiguration(config));

            // metadata server
            v.add(new MetadataServiceConfiguration(config));

            // data server(s) (lots!)
            v.add(new DataServiceConfiguration(config));

            // load balancer server.
            v.add(new LoadBalancerServiceConfiguration(config));
            
            return v.toArray(new ServiceConfiguration[0]);
            
    }
    
    /**
     * Pushs the {@link ServiceConfiguration}s for the federation into
     * zookeeper. A new znode is created if none exists. Otherwise this
     * overwrites the existing data for those znodes.
     * 
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws KeeperException
     * 
     * @todo We should either watch the configuration file and push the
     *       configuration if it is updated or install a SIGHUP handled and push
     *       the configuration if we receive that signal.
     */
    protected void pushConfiguration(final ZooKeeper zookeeper,
            final String zconfig, final List<ACL> acl,
            final ServiceConfiguration[] config) throws KeeperException,
            InterruptedException, ConfigurationException {

        for (ServiceConfiguration x : config) {

            final String zpath = zconfig + BigdataZooDefs.ZSLASH + x.className;

            final byte[] data = SerializerUtil.serialize(x);

            try {

                zookeeper.create(zpath, data, acl, CreateMode.PERSISTENT);

                if (INFO)
                    log.info("Created: " + zpath + " : " + x);

            } catch (NodeExistsException ex) {

                try {
                    zookeeper.setData(zpath, data, -1/* version */);

                    if (INFO)
                        log.info("Updated: " + zpath + " : " + x);

                } catch (KeeperException ex2) {

                    log.error("Could not update: zpath=" + zpath);

                }
                
            }

        }

    }

}
