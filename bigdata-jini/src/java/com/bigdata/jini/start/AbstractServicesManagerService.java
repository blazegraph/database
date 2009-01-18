package com.bigdata.jini.start;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.jini.config.Configuration;

import com.bigdata.jini.start.config.ServicesManagerConfiguration;
import com.bigdata.jini.start.process.JiniCoreServicesProcessHelper;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.start.process.ZookeeperProcessHelper;
import com.bigdata.service.AbstractService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.RemoteDestroyAdmin;

/**
 * Core impl.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractServicesManagerService extends AbstractService
        implements IServicesManagerService, IServiceListener, IServiceShutdown {

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

    /*
     * Note: The means by which we lock out child processes from starting is not
     * very clean.
     */
    public void add(final ProcessHelper service) {

        // add it first.
        runningProcesses.add(service);

        if (!open || !childStartsAllowed) {

            log.warn("New processes may not start: " + service);

            try {

                log.warn("Killing: " + service);
                
                /*
                 * Kill it - this will remove it from the set of running
                 * processes.
                 */
                
                service.kill();
                
            } catch (InterruptedException e) {
                
                log.warn(e);
                
            }

        }

    }

    public void remove(final ProcessHelper service) {

        runningProcesses.remove(service);

    }

    protected AbstractServicesManagerService(final Properties properties) {

        super();
        
        this.properties = (Properties) properties.clone();

    }

    /**
     * Kill the child processes, using {@link RemoteDestroyAdmin#shutdown()}
     * where supported.
     */
    public void shutdown() {

        synchronized (this) {

            if (!open)
                return;

            open = false;

            childStartsAllowed = false;

        }

        killChildProcesses(false/* immediateShutdown */);

    }

    /**
     * Kill the child processes, using {@link RemoteDestroyAdmin#shutdownNow()}
     * where supported.
     */
    public void shutdownNow() {

        synchronized (this) {

            if (!open)
                return;

            open = false;

            childStartsAllowed = false;

        }
        
        killChildProcesses(true/* immediateShutdown */);
        
    }

    public boolean isOpen() {

        return open;
        
    }
    private volatile boolean open = true;
    private volatile boolean childStartsAllowed = true;

    /**
     * Sets a flag to disallow new process starts and kills any running child
     * processes.
     * 
     * @param immediateShutdown
     *            When <code>true</code>
     *            {@link RemoteDestroyAdmin#shutdownNow()} will be used by
     *            preference to terminate child processes which support
     *            {@link RemoteDestroyAdmin}. Otherwise
     *            {@link RemoteDestroyAdmin#shutdown()} will be used to terminat
     *            child processes supporting that interface.
     */
    protected void killChildProcesses(final boolean immediateShutdown) {

       childStartsAllowed = false; 
        
//        final List<ProcessHelper> problems = new LinkedList<ProcessHelper>();

        // destroy any running processes
        for (ProcessHelper helper : runningProcesses) {

            if (helper instanceof JiniCoreServicesProcessHelper)
                continue;

            if (helper instanceof ZookeeperProcessHelper)
                continue;

            try {
                helper.kill();
            } catch (Throwable t) {
                log.error("Could not kill process: " + helper);
//                // add to list of problem processes.
//                problems.add(helper);
                // remove from list of running processes.
                runningProcesses.remove(helper);
            }

        }

//        // try again for the problem processes, raising the logging level.
//        for (ProcessHelper helper : problems) {
//
//            try {
//                helper.kill();
//            } catch (Throwable t) {
//                log.error("Could not kill process: " + helper);
//            }
//
//        }

        /*
         * This time we take down zookeeper and jini.
         */
        for (ProcessHelper helper : runningProcesses) {

            try {
                helper.kill();
            } catch (Throwable t) {
                log.warn("Could not kill process: " + helper);
            }

        }

    }
    
    @Override
    public Class getServiceIface() {

        return IServicesManagerService.class;

    }

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

        final Configuration config = fed.getClient().getConfiguration();
        
        // the service manager's own configuration.
        final ServicesManagerConfiguration selfConfig = new ServicesManagerConfiguration(config);

        if (!selfConfig.canStartService(fed)) {

            // refuse to start.
            throw new RuntimeException("Constraints do not permit start: "
                    + selfConfig);
            
        }

        /*
         * Create and start task that will monitor the config znode. If any
         * children are added then this task will set up a watcher on the
         * service configuration node. From there everything will happen
         * automatically.
         * 
         * We monitor future on this task and make sure that it is still
         * running, but it is really only used when the config znode children
         * are created.
         * 
         * Note: This task will run until cancelled. If necessary it will wait
         * for the zookeeper client to become connected and the znode to be
         * created.
         */
        fed.submitMonitoredTask(new MonitorConfigZNodeTask(fed, this/* listener */));

        /*
         * Create and start task that will compete for locks to start physical
         * service instances.
         * 
         * Note: This task will run until cancelled. If necessary it will wait
         * for the zookeeper client to become connected and the znode to be
         * created.
         */
        monitorCreatePhysicalServiceLocksTask = new MonitorCreatePhysicalServiceLocksTask(
                fed, this/* listener */);

        fed.submitMonitoredTask(monitorCreatePhysicalServiceLocksTask);

        /*
         * Run startup.
         */
        new ServicesManagerStartupTask(fed, config, this/* listener */,
                monitorCreatePhysicalServiceLocksTask).call();
        
    }
    
    /**
     * The task used to start and restart services.
     */
    protected MonitorCreatePhysicalServiceLocksTask monitorCreatePhysicalServiceLocksTask;
    
}
