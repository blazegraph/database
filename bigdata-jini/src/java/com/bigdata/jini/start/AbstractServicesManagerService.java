package com.bigdata.jini.start;

import java.io.IOException;
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
import com.bigdata.jini.start.config.JiniCoreServicesConfiguration;
import com.bigdata.jini.start.config.ManagedServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.ServicesManagerConfiguration;
import com.bigdata.jini.start.process.JiniCoreServicesProcessHelper;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.start.process.ZookeeperProcessHelper;
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
     * NOP
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

        final Configuration config = getConfiguration();

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
        fed
                .submitMonitoredTask(new MonitorConfigZNodeTask(fed, this/* listener */));

        /*
         * Create and start task that will compete for locks to start physical
         * service instances.
         */
        fed.submitMonitoredTask(new MonitorCreatePhysicalServiceLocksTask(fed,
                this/* listener */));

        // the service manager's own configuration.
        final ServicesManagerConfiguration selfConfig = new ServicesManagerConfiguration(config);

        if (!selfConfig.canStartService(fed)) {

            // refuse to start.
            throw new RuntimeException("Constraints do not permit start: "
                    + selfConfig);
            
        }
        
        /*
         * These are the services that we will start and/or manage.
         * 
         * @todo review how we decide whether or not to start zookeeper and jini
         * 
         * @todo we should be able to start the necessary jini services just
         * from the bundled JARs without actually running the installer, right?
         */
        final ServiceConfiguration[] serviceConfigurations = selfConfig
                .getServiceConfigurations(config);

        // start if configured to run on host and not running.
        startZookeeperService(config);

        for (ServiceConfiguration serviceConfig : serviceConfigurations) {

            if (serviceConfig instanceof JiniCoreServicesConfiguration) {

                /*
                 * Start jini if configured to run on this host and not running.
                 */

                startJiniCoreServices(config);

            }

        }
        
        /*
         * Make sure that the key znodes are defined and then push the service
         * configurations into zookeeper.
         */
        
        final ZooKeeper zookeeper = fed.getZookeeper();
        
        if (zookeeper != null) {

            final ZooKeeper.States state = zookeeper.getState();
            
            switch (state) {

            default:
            
                log.error("Zookeeper: "+state+" : Will not push configuration.");

                return;

            case CONNECTED:

                break;
                
            }
            
            // root znode for the federation.
            final String zroot = fed.getZooConfig().zroot;

            // znode for configuration metadata.
            final String zconfig = zroot + BigdataZooDefs.ZSLASH
                    + BigdataZooDefs.CONFIG;

            // ACL for the zroot, etc.
            final List<ACL> acl = fed.getClient().zooConfig.acl;

            // create critical nodes used by the federation.
            createKeyZNodes(zookeeper, zroot, acl);

            // push the service configurations into zookeeper (create/update).
            pushConfiguration(zookeeper, zconfig, acl, serviceConfigurations);
            
        }
        
    }

    /**
     * If necessary, start a zookeeper service on this host.
     * 
     * @return <code>true</code> if an instance was started successfully.
     */
    protected boolean startZookeeperService(Configuration config)
            throws ConfigurationException, IOException {

        try {

            return ZookeeperProcessHelper
                    .startZookeeper(config, this/* listener */) > 0;

        } catch (Throwable t) {
            
            log.error("Could not start zookeeper service: " + t, t);
            
            return false;
            
        }

    }

    /**
     * If necessary, start the jini core services on this host.
     * 
     * @return <code>true</code> if an instance was started successfully.
     */
    protected boolean startJiniCoreServices(Configuration config) {

        try {

            return JiniCoreServicesProcessHelper
                    .startCoreServices(config, this/* listener */);
            
        } catch (Throwable t) {
            
            log.error("Could not start jini services: " + t, t);
            
            return false;
            
        }
        
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
     * Pushs the {@link ServiceConfiguration}s for the federation into
     * zookeeper. A new znode is created if none exists. Otherwise this
     * overwrites the existing data for those znodes.
     * 
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void pushConfiguration(final ZooKeeper zookeeper,
            final String zconfig, final List<ACL> acl,
            final ServiceConfiguration[] serviceConfigurations)
            throws KeeperException, InterruptedException,
            ConfigurationException {

        for (ServiceConfiguration x : serviceConfigurations) {

            if(!(x instanceof ManagedServiceConfiguration)) {
                
                // Only the managed services are put into zookeeper.
                continue;
                
            }
            
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
