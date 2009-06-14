package com.bigdata.jini.start;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.ManagedServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.InnerCause;
import com.bigdata.zookeeper.HierarchicalZNodeWatcher;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZLockImpl;

/**
 * This is a standing task whose {@link Future} is monitored by the
 * {@link ServicesManagerServer}. For each {@link ServiceConfiguration} znode,
 * there is one such task per {@link ServicesManagerServer}. The task holding
 * the {@link ZLock} is the one that handles state changes for
 * {@link ServiceConfiguration} znode, including its children, which are the
 * logical service instance znodes.
 * <p>
 * If the {@link ServicesManagerServer} dies, then its znodes will be deleted
 * from the lock queues and someone else (if anyone is left alive) will get the
 * lock and manage the logical service.
 * <p>
 * If the task itself dies, then the {@link ServicesManagerServer} needs to log
 * an error and create a new instance of the task.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServiceConfigurationZNodeMonitorTask implements Callable<Void> {

    final static protected Logger log = Logger
            .getLogger(ServiceConfigurationZNodeMonitorTask.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();
    
    protected final JiniFederation fed;

    protected final IServiceListener listener;
    
    protected final String className;

    final ZooKeeper zookeeper;
    
    final String zroot;
    
    /** zpath for the lock node. */
    final String lockZPath;
    
    /** zpath for the service configuration node. */
    final String serviceConfigZPath;
    
    /**
     * 
     * @param fed
     * @param className
     *            The class name (aka service type).
     */
    public ServiceConfigurationZNodeMonitorTask(final JiniFederation fed,
            final IServiceListener listener,
            final String className) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (listener == null)
            throw new IllegalArgumentException();

        if (className == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.listener = listener;
        
        this.className = className;

        zookeeper = fed.getZookeeper();
        
        zroot = fed.getZooConfig().zroot;
        
        // zpath for the lock node.
        lockZPath = zroot + "/" + BigdataZooDefs.LOCKS_SERVICE_CONFIG_MONITOR
                + "/" + className;

        // zpath for the service configuration node.
        serviceConfigZPath = zroot + "/" + BigdataZooDefs.CONFIG + "/" + className;
        
    }

    /**
     * Contends for a {@link ZLock} on {@link #zpath} and then maintains a
     * balance between: (a) the {@link ServiceConfiguration} state for a service
     * type; (b) the #of logical service instances for that service type; and
     * (c) the #of physical service instances for any logical service instance
     * for that service type.
     * <p>
     * The set of znodes to be watched is dynamic. Its only fixed fixed member
     * is the {@link ServiceConfiguration} znode. We need to change the watch
     * set as new logical services are created or destroyed and as new physical
     * services are created or destroyed.
     * <p>
     * This only watch znodes and takes action while the {@link ZLock} is held.
     * If it looses the {@link ZLock}, it will seek to re-acquire the lock. No
     * actions are taken in the zookeeper event thread. They are all pushed into
     * a queue and then processed in the thread that executes {@link #call()}.
     * The queue also prevents us from attempting to make more than one
     * adjustment to the managed services at a time.
     * <p>
     * Note: This task is designed to run until cancelled. It will trap anything
     * but an {@link InterruptedException} and restart from
     * {@link #acquireLockAndRun()}.
     */
    public Void call() throws Exception {

        while (true) {

            try {

                acquireLockAndRun();

            } catch(Throwable t) {

                if(InnerCause.isInnerCause(t, InterruptedException.class)) {
                
                    if(INFO)
                        log.info("Interrupted");
                    
                    throw new RuntimeException(t);
                    
                }
                
                log.error(this, t);

                /*
                 * @todo A tight loop can appear here if the znodes for the
                 * federation are destroyed. This shows up as a NoNodeException
                 * thrown out of getZLock(), which indicates that zroot/locks
                 * does not exist so the lock node can not be created.
                 * 
                 * A timeout introduces a delay which reduces the problem. This
                 * can arise if the federation is being destroyed -or- if this
                 * is a new federation but we are not yet connected to a
                 * zookeeper ensemble so the znodes for the federation do not
                 * yet exist.
                 */

                Thread.sleep(2000/* ms */);
                
            }
            
        }

    }

    /**
     * Waits until it acquires the lock and then {@link #runWithLock(ZLock)}
     * 
     * @throws Exception 
     */
    protected void acquireLockAndRun() throws Exception {

        final ZLock zlock = ZLockImpl.getLock(zookeeper,
                lockZPath, fed.getZooConfig().acl);

        zlock.lock();

        try {

            runWithLock(zlock);
        
        } finally {

            zlock.unlock();

        }

    }
    
    /**
     * Runs with the zlock held.
     * 
     * @param zlock
     * 
     * @throws Exception 
     */
    protected void runWithLock(final ZLock zlock) throws Exception {

        if (INFO)
            log.info("Setting watcher: zlock=" + zlock
                    + ", serviceConfigZPath=" + serviceConfigZPath);

        /*
         * Setup a watcher that will watch all of the znodes of interest.
         * 
         * Note: This is done while we are holding the zlock. If we loose the
         * zlock, the we cancel the watcher until we can reacquire the zlock.
         */
        final ServiceConfigurationHierarchyWatcher watcher = new ServiceConfigurationHierarchyWatcher(
                zookeeper, serviceConfigZPath);

        /*
         * This is the initial set of znodes to be watched. We don't get events
         * for these from zookeeper until someone touches them, so we process
         * them now and make sure that everything is in balance.
         */
        final String[] watchedSet = watcher.getWatchedNodes();

        // Get the service configuration.
        final ManagedServiceConfiguration config = (ManagedServiceConfiguration) SerializerUtil
                .deserialize(zookeeper.getData(serviceConfigZPath, false,
                        new Stat()));

        balanceAll(config, watchedSet);

        try {

            while (true) {

                final WatchedEvent e = watcher.queue.take();

                if (!zlock.isLockHeld()) {

                    // Lost the lock.
                    break;

                }

                handleEvent(watcher, e);

            }

        } finally {

            // cancel so that we stop seeing events.
            watcher.cancel();

        }

    }
    
    /**
     * 
     * @param config
     * @param watchedSet
     * @throws Exception
     */
    protected void balanceAll(final ManagedServiceConfiguration config,
            final String[] watchedSet) throws Exception {

        if (INFO)
            log.info("serviceConfigZPath=" + serviceConfigZPath
                    + ", watchedSet=" + Arrays.toString(watchedSet));
        
        // rebalance the logical service instances w/ the config.
        balanceLogicalServices(config);
        
        // rebalance the physical service instances w/ the config.
        balancePhysicalServices(config, watchedSet);
   
    }
    
    /**
     * Consider the logical service instances for the
     * {@link ServiceConfiguration} and if necessary makes an adjustment to the
     * #of logical services that are running.
     * 
     * @param config
     *            A recent snapshot of the {@link ServiceConfiguration} state.
     * 
     * @throws Exception
     */
    protected void balanceLogicalServices(
            final ManagedServiceConfiguration config) throws Exception {

        // get children (the list of logical services).
        final List<String> children = zookeeper.getChildren(serviceConfigZPath,
                false);

        if (INFO)
            log.info("serviceConfigZPath=" + serviceConfigZPath
                    + ", targetServiceCount=" + config.serviceCount
                    + ", #children=" + children.size() + ", children="
                    + children);

        if (config.serviceCount != children.size()) {

            // adjust the #of logical service instances (blocks).
            config.newLogicalServiceTask(fed, listener, serviceConfigZPath,
                    children).call();

        }

    }
    
    /**
     * Consider all the physical service containers and initiates a competition
     * to create or destroy a physical service instance for each logical service
     * that is out of balance. The competition itself is handled by the
     * {@link MonitorCreatePhysicalServiceLocksTask}, which watches a well
     * known znode for the appearence of new lock nodes and then enters into a
     * competition to create a new physical service instance. Therefore while
     * this initiates the competition for processing to handle the imbalance,
     * the imbalance is NOT correct while we are in this method.
     * 
     * @param config
     *            A recent snapshot of the {@link ServiceConfiguration} state.
     * @param watchedSet
     *            The set of watched znodes (this includes the physical service
     *            containers, which is what we need here).
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    protected void balancePhysicalServices(
            final ManagedServiceConfiguration config, final String[] watchedSet)
            throws KeeperException, InterruptedException {

        for (String s : watchedSet) {

            switch (ServiceConfigurationZNodeEnum
                    .getType(serviceConfigZPath, s)) {
            
            case PhysicalServicesContainer:

                // the parent is the logicalService.
                final String logicalServiceZPath = s.substring(0, s
                        .lastIndexOf('/'));
                
                // the znode of the logical service (last path component).
                final String logicalServiceZNode = logicalServiceZPath
                        .substring(logicalServiceZPath.lastIndexOf('/') + 1);
                
                // get children (the list of physical services).
                final List<String> children = zookeeper.getChildren(
                        logicalServiceZPath + "/"
                                + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER,
                        false);

                if (INFO)
                    log.info("serviceConfigZPath=" + serviceConfigZPath
                            + ", logicalServiceZPath=" + logicalServiceZPath
                            + ", targetReplicationCount="
                            + config.replicationCount + ", #children="
                            + children.size() + ", children=" + children);

                // too few instances? : @todo handle too many instances also.
                if (config.replicationCount > children.size()) {

                    try {

                        /*
                         * Note: MonitorCreatePhysicalServiceLocksTasks will
                         * notice the create of this lock node, contend for the
                         * zlock, and create a new service instance if it gains
                         * the lock and we are still short at least one physical
                         * service for this logical service.
                         */

                        final String lockNodeZPath = zroot + "/"
                                + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE
                                + "/" + config.className + "_"
                                + logicalServiceZNode;

                        zookeeper.create(lockNodeZPath, SerializerUtil
                                .serialize(logicalServiceZPath), fed
                                .getZooConfig().acl, CreateMode.PERSISTENT);

                        if (INFO)
                            log.info("Created lock node: " + lockNodeZPath);

                    } catch (NodeExistsException ex) {

                        // ignore.

                    }

                }
            
            default:
                continue;
            
            }

        }

    }
    
    /**
     * Handle a {@link WatchedEvent} that was read from a queue.
     * 
     * <ul>
     * 
     * <li>If the path is the {@link ServiceConfiguration} znode itself, then
     * we need to test ALL watched logicalService znodes and ALL watched
     * physicalService znodes. This is identical to the setup procedure when we
     * gain the {@link ZLock} and setup the
     * {@link ServiceConfigurationHierarchyWatcher}.</li>
     * 
     * <li>If the path is a logical service, then we want to compare its
     * replication count (on the service configuration znode) to the #of
     * physical service instances AND we also want to compare the #of logical
     * services to the service count (on the service configuration znode). This
     * is a top-down trigger.</li>
     * 
     * <li>If the path is a physical service, then we want to compare
     * replication count (on the service configuration znode) to the #of
     * physical service instances. This provides a bottom up trigger for
     * rebalancing.</li>
     * 
     * </ul>
     * 
     * Note: Since the events were read from a queue, this method runs in the
     * application thread NOT the zookeeper event thread.
     */
    protected void handleEvent(
            final ServiceConfigurationHierarchyWatcher watcher,
            final WatchedEvent e) throws Exception {

        switch (e.getState()) {
        case SyncConnected:
            return;
        case Disconnected:
            return;
        }

        final String zpath = e.getPath();

        if (zpath == null) {
            
            throw new AssertionError("No zpath: event=" + e);
            
        }
        
        final String[] watchedSet = watcher.getWatchedNodes();

        switch (ServiceConfigurationZNodeEnum
                .getType(serviceConfigZPath, zpath)) {

        case ServiceConfiguration: {

            // Get the service configuration.
            final ManagedServiceConfiguration config = (ManagedServiceConfiguration) SerializerUtil
                    .deserialize(zookeeper.getData(serviceConfigZPath, false,
                            new Stat()));

            balanceAll(config, watchedSet);

            break;

        }

        case LogicalService: {

            // Get the service configuration.
            final ManagedServiceConfiguration config = (ManagedServiceConfiguration) SerializerUtil
                    .deserialize(zookeeper.getData(serviceConfigZPath, false,
                            new Stat()));

            balanceLogicalServices(config);

            break;

        }

        case PhysicalServicesContainer: {

            // Get the service configuration.
            final ManagedServiceConfiguration config = (ManagedServiceConfiguration) SerializerUtil
                    .deserialize(zookeeper.getData(serviceConfigZPath, false,
                            new Stat()));

            balancePhysicalServices(config, watchedSet);

            break;

        }

        }

    }
    
    /**
     * Extended to accept the various znodes of interest under a
     * {@link ServiceConfiguration} znode. The znodes of interest are the
     * logical service instances and the physical service instances. Thisa also
     * keeps a watch on the data for the {@link ServiceConfiguration} znode
     * itself.
     * 
     * FIXME Zookeeper DOES NOT guarentee that you will see all events. To have
     * a more robust guarentee you need to impose additional constraints. Make
     * sure that we are imposing sufficient constraints to see all events when
     * we hold the {@link ZLock} for a {@link ServiceConfiguration}. This might
     * require that we explicitly message this class with paths to be watched as
     * znodes for logical services are created.
     * 
     * Only the service holding the {@link ZLock} for a
     * {@link ServiceConfiguration} znode watches its znode hierarchy. The set
     * of znodes to be watched is dynamic. We need to change the watch set as
     * new logical services are created or destroyed (by the service holding the
     * {@link ZLock}) and as new physical services are created (this event is
     * seen by watching the logical service's children) or destroyed (this event
     * is seen by watching the physical service znode).
     * <p>
     * The events that we MUST NOT miss are changes to the ServiceConfiguration
     * znode (target #of logical services and target #of replicated instances)
     * and the create and destroy of physical services.
     * <p>
     * We require that this task holds a {@link ZLock} in order to watch the
     * hierarchy. While it should be the only one making structure changes to
     * the logicalServices, physical services which may come and go at any time
     * since they create their own ephemeral znodes.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ServiceConfigurationHierarchyWatcher extends
            HierarchicalZNodeWatcher {
        
        /**
         * @param zookeeper
         * @param zroot
         * @param flags
         * @throws InterruptedException
         * @throws KeeperException
         */
        public ServiceConfigurationHierarchyWatcher(ZooKeeper zookeeper,
                String zroot) throws InterruptedException,
                KeeperException {

            super(zookeeper, zroot, EXISTS | DATA | CHILDREN);
            
        }

        /**
         * We need to watch all of the znodes that have a "W" at the start of
         * the line.
         * 
         * <pre>
         * test-fed 
         *   locks 
         *     serviceConfigMonitor 
         *       com.bigdata.service.jini.TransactionServer 
         *         lock0000000000 (Ephemeral) 
         *     createPhysicalService 
         *   config 
         * W   com.bigdata.service.jini.TransactionServer {TransactionServiceConfiguration}
         * W     logicalService0000000000 
         * W       physicalServices 
         * W         abde9b91-24d5-4dc5-9bbf-41d7e7cac272 (Ephemeral) 
         *         masterElection 
         *           lock0000000000 (Ephemeral) 
         * </pre>
         */
        @Override
        protected int watch(final String parent, final String child) {

            // the full path.
            final String zpath = parent + "/" + child;

            switch (ServiceConfigurationZNodeEnum.getType(serviceConfigZPath,
                    zpath)) {
            
            case ServiceConfiguration:
            
                return ALL;
            
            case LogicalService:
                
                return EXISTS | CHILDREN;
                
            case PhysicalServicesContainer:
                
                return EXISTS | CHILDREN;
                
            case PhysicalService:
                
                return EXISTS;
                
            case MasterElection:
 
            case MasterElectionLock:
            
                return NONE;
                
            default:
            
                throw new AssertionError(zpath);
            
            }
            
        }

    };
    
}
