package com.bigdata.jini.start;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
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
import com.bigdata.jini.start.config.ZookeeperServerConfiguration;
import com.bigdata.jini.start.process.JiniCoreServicesProcessHelper;
import com.bigdata.jini.start.process.ZookeeperProcessHelper;
import com.bigdata.service.jini.JiniFederation;

/**
 * Used to start up the server and to handle SIGHUP. This DOES NOT start the
 * monitor tasks since those are not cancelled unless the server is
 * shutdown.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServicesManagerStartupTask implements Callable<Void> {

    protected static final Logger log = Logger.getLogger(ServicesManagerStartupTask.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    protected final JiniFederation fed;

    protected final Configuration config;

    protected final AbstractServicesManagerService service;
    
    protected final MonitorCreatePhysicalServiceLocksTask monitorCreatePhysicalServiceLocksTask; 

    /**
     * 
     * @param fed
     * @param config
     *            The configuration that will be pushed to zookeeper.
     * @param service
     */
    public ServicesManagerStartupTask(
            final JiniFederation fed,
            final Configuration config,
            final AbstractServicesManagerService service) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (config == null)
            throw new IllegalArgumentException();

        if (service == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.config = config;

        this.service = service;

        this.monitorCreatePhysicalServiceLocksTask = service.monitorCreatePhysicalServiceLocksTask;

    }

    public Void call() throws Exception {

        try {

            synchronized (service) {

                if (service.startupRunning) {

                    log.warn("Startup already running.");

                    return null;

                }

                service.startupRunning = true;

            }

            doStartup();

            return null;

        } finally {

            synchronized (service) {

                service.startupRunning = false;

            }

        }

    }

    protected void doStartup() throws Exception {
        
        if (INFO)
            log.info("Running.");

        // the service manager's own configuration.
        final ServicesManagerConfiguration selfConfig = new ServicesManagerConfiguration(
                config);

        /*
         * These are the services that we will start and/or manage.
         */
        final ServiceConfiguration[] serviceConfigurations = selfConfig
                .getServiceConfigurations(config);

        /*
         * Start zookeeper if configured to run on host and not running.
         */
        for (ServiceConfiguration serviceConfig : serviceConfigurations) {

            if (serviceConfig instanceof ZookeeperServerConfiguration) {

                startZookeeperService(config);

            }

        }

        /*
         * Start jini if configured to run on this host and not running.
         */
        for (ServiceConfiguration serviceConfig : serviceConfigurations) {

            if (serviceConfig instanceof JiniCoreServicesConfiguration) {

                startJiniCoreServices(config);

            }

        }

        /*
         * Wait for zookeeper and jini to become connected.
         * 
         * Note: This is event driven so it will not wait any longer than
         * necessary.
         */
        {

            // @todo configure timeout
            final long timeout = 10000;

            final long begin = System.nanoTime();

            long nanos = TimeUnit.MILLISECONDS.toNanos(timeout);

            // await zookeeper connection.
            if (!fed.awaitZookeeperConnected(nanos, TimeUnit.NANOSECONDS)) {

                throw new Exception(
                        "Zookeeper not connected: startup sequence aborted.");

            }

            nanos -= (System.nanoTime() - begin);

            // await jini registrar(s)
            if (!fed.awaitJiniRegistrars(nanos, TimeUnit.NANOSECONDS)) {

                throw new Exception(
                        "No jini registrars: startup sequence aborted.");

            }

        }

        /*
         * Make sure that the key znodes are defined and then push the
         * service configurations into zookeeper.
         */
        pushConfiguration(serviceConfigurations);

        /*
         * Restart any persistent services that can not be discovered.
         * 
         * Note: This must wait until we have started zookeeper and/or jini
         * in case they are supposed to run on this host and are not running
         * elsewhere at this time.
         */

        fed.submitMonitoredTask(new RestartPersistentServices(fed,
                monitorCreatePhysicalServiceLocksTask));

    }
    
    /**
     * If necessary, start a zookeeper service on this host.
     * 
     * @return <code>true</code> if an instance was started successfully.
     */
    protected boolean startZookeeperService(final Configuration config)
            throws ConfigurationException, IOException {

        try {

            return ZookeeperProcessHelper.startZookeeper(config, service) > 0;

        } catch (Throwable t) {

            log.error(
                    "Could not start zookeeper service: " + t, t);

            return false;

        }

    }

    /**
     * If necessary, start the jini core services on this host.
     * 
     * @return <code>true</code> if an instance was started successfully.
     */
    protected boolean startJiniCoreServices(final Configuration config) {

        try {

            return JiniCoreServicesProcessHelper.startCoreServices(config,
                    service);

        } catch (Throwable t) {

            log.error(
                    "Could not start jini services: " + t, t);

            return false;

        }

    }

    /**
     * Make sure that the key znodes are defined and then push the service
     * configurations into zookeeper.
     * 
     * @param serviceConfigurations
     *            The {@link ServiceConfiguration}s to be pushed.
     * 
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws KeeperException
     */
    protected boolean pushConfiguration(
            final ServiceConfiguration[] serviceConfigurations)
            throws KeeperException, InterruptedException,
            ConfigurationException {

        final ZooKeeper zookeeper = fed.getZookeeper();

        if (zookeeper == null)
            return false;

        final ZooKeeper.States state = zookeeper.getState();

        switch (state) {
        default:
            log.error("Zookeeper: " + state
                    + " : Will not push configuration.");
            return false;
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

        return true;

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

                // znode dominating lock nodes for creating new physical services.
                zroot + "/" + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE,

                // znode whose children are the per-service type service configurations.
                zroot + "/" + BigdataZooDefs.LOCKS_SERVICE_CONFIG_MONITOR,

                // znode for the resource locks (IResourceLockManager)
                zroot + "/" + BigdataZooDefs.LOCKS_RESOURCES,

        };

        for (String zpath : a) {

            try {

                zookeeper.create(zpath, new byte[] {}/* data */, acl,
                        CreateMode.PERSISTENT);

            } catch (NodeExistsException ex) {

                // that's fine - the configuration already exists.
                if (DEBUG)
                    log.debug("exists: " + zpath);

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

            if (!(x instanceof ManagedServiceConfiguration)) {

                // Only the managed services are put into zookeeper.
                continue;

            }

            final String zpath = zconfig + BigdataZooDefs.ZSLASH + x.className;

            final byte[] data = SerializerUtil.serialize(x);

            try {

                zookeeper.create(zpath, data, acl, CreateMode.PERSISTENT);

                if (DEBUG)
                    log.debug("Created: " + zpath + " : " + x);
                else if (INFO)
                    log.info("Created: " + zpath);

            } catch (NodeExistsException ex) {

                try {
                    zookeeper.setData(zpath, data, -1/* version */);

                    if (DEBUG)
                        log.debug("Updated: " + zpath + " : " + x);
                    else if (INFO)
                        log.info("Updated: " + zpath);

                } catch (KeeperException ex2) {

                    log.error("Could not update: zpath=" + zpath);

                }

            }

        }

    }

}
