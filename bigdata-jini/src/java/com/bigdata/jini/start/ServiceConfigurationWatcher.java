package com.bigdata.jini.start;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.ManagedServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.AbstractZNodeConditionWatcher;
import com.bigdata.zookeeper.ZLock;

/**
 * Watcher ONLY runs while it hold a {@link ZLock} granting it permission to
 * manage some {@link ServiceConfiguration}. If an event is received which
 * indicates that something is out of balance then we verify that we still hold
 * the {@link ZLock} and then make any necessary adjustments. If we do not hold
 * the {@link ZLock} then we cancel the watcher. 
 * 
 * @see ServiceConfigurationZNodeMonitorTask
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated
 */
public class ServiceConfigurationWatcher extends
        AbstractZNodeConditionWatcher {

    protected static final Logger log = Logger
            .getLogger(ServiceConfigurationWatcher.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    protected final JiniFederation fed;

    protected final IServiceListener listener;

    protected final ZLock zlock;

//    /**
//     * Map of watches set of the physical service children of each logical
//     * service znode for this service type.
//     * 
//     * @todo This could be a {@link ConcurrentWeakValueCache}. the futures
//     *       would be cleared once the task was finished since the hard
//     *       reference would be maintained by
//     *       {@link JiniFederation#submitMonitoredTask(Callable)} until then.
//     */
//    private ConcurrentHashMap<String/* zpath */, Future> physicalServiceMonitors = new ConcurrentHashMap<String, Future>(); 
    
    /**
     * 
     * @param fed
     * @param listener
     * @param zpath
     *            For the {@link ServiceConfiguration} node.
     * @param zlock
     *            The lock that allows us to act.
     */
    public ServiceConfigurationWatcher(final JiniFederation fed,
            final IServiceListener listener, final String zpath,
            final ZLock zlock) {

        super(fed.getZookeeper(), zpath);

        if (fed == null)
            throw new IllegalArgumentException();

        if (listener == null)
            throw new IllegalArgumentException();

        if (zlock == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;
        
        this.listener = listener;
                
        this.zlock = zlock;
        
    }
    
    /**
     * delegated to {@link #isConditionSatisified()} 
     */
    @Override
    protected boolean isConditionSatisified(WatchedEvent event)
            throws KeeperException, InterruptedException {

        if(INFO)
            log.info(event.toString());
        
        return isConditionSatisified();
        
    }

    /**
     * Compares the current state of the {@link ServiceConfiguration} with the
     * #of logical services (its child znodes). If necessary, joins an election
     * to decide who gets to start/stop a logical service.
     * 
     * @return returns <code>true</code> iff we loose the {@link ZLock}.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    protected boolean isConditionSatisified() throws KeeperException,
            InterruptedException {

        if(!zlock.isLockHeld()) {

            log.warn("Lost lock: "+zpath);
            
            // exit.
            return true;
            
        }

        if (zookeeper.exists(zpath, this) == null) {
            
            if (INFO)
                log.info("No node: "+zpath);
            
            // continue watching.
            return false;
            
        }
        
        // get the service configuration (and reset our watch).
        final ManagedServiceConfiguration config = (ManagedServiceConfiguration) SerializerUtil
                .deserialize(zookeeper.getData(zpath, this, new Stat()));

        // get children (the list of logical services) (and reset our watch).
        final List<String> children = zookeeper.getChildren(zpath, this);

        // handle the event.
        return handleEvent(config, children);

    }

    /**
     * Handle the event.
     * <p>
     * Note: we MUST NOT attempt to manage this service again until the event
     * has been handled. However, at the same time we MUST NOT block while in
     * the zookeeper event thread.
     * 
     * FIXME therefore this should have a queue of logical service instances
     * that it will test. alternatively, for each even it must test all logical
     * service instances.
     */
    protected boolean handleEvent(final ManagedServiceConfiguration config,
            final List<String> children) {
        
        if (future != null) {

            if (!future.isDone()) {

                log.warn("still running the last task...");

                /*
                 * ignore event.
                 * 
                 * Note: we need to continue watching, which is why we get the
                 * data and children above before testing the future.
                 */
                return false;

            }

            /*
             * The last task is done. Get its future to check it for errors.
             */
            try {

                future.get();

                // clear reference.
                future = null;

            } catch (Throwable t) {

                log.error("zpath=" + zpath, t);

                // exit on error.
                return true;

            }

        }

//        /*
//         * Establish monitors for each logical service that ensure that the #of
//         * physical service instances is maintained.
//         */
//        {
//
//            for (String logicalService : children) {
//
//                final String s = zpath + "/" + logicalService + "/"
//                        + BigdataZooDefs.PHYSICAL_SERVICES;
//
//                final Future f = physicalServiceMonitors.get(s);
//
//                if (f == null || f.isDone()) {
//
//                    final PhysicalServiceInstancesMonitor childTask = new PhysicalServiceInstancesMonitor(
//                            s);
//
//                    /*
//                     * Note: These monitor tasks need to get cancelled if the
//                     * outer monitor is cancelled, so we put them into a hash
//                     * map which we process if the outer task is cancelled (when
//                     * it looses its lock).
//                     * 
//                     * Note: These are submitted as monitored tasks so that any
//                     * exceptions will be logged. However, the tasks are not
//                     * retried so if there is a problem we will wind up not
//                     * monitoring the physical services for that logical service -
//                     * at least, not until the service manager is restarted, the
//                     * ServiceConfiguration znode is touched, or the zlock is
//                     * lost and responsibility passes to another services
//                     * manager.
//                     */
//
//                    physicalServiceMonitors.put(s, fed
//                            .submitMonitoredTask(childTask));
//
//                }
//                
//            }
//            
//        }
        
        if (config.serviceCount != children.size()) {
            
            // the task.
            final ManageLogicalServiceTask<ManagedServiceConfiguration> task = config
                    .newLogicalServiceTask(fed, listener, zpath, children);
            
            // wrap it up to notify us when it is done.
            final Callable wrappedTask = new Callable() {
              
                public Object call() throws Exception {
                 
                    try {

                        return task.call();
                                                
                    } finally {

                        synchronized (ServiceConfigurationWatcher.this) {

                            /*
                             * Wake up awaitCondition(). It will cause
                             * isConditionSatisified() to be invoked and that
                             * will consume the Future and handle any error.
                             */

                            if(INFO)
                                log.info("Notifying watcher");
                            
                            ServiceConfigurationWatcher.this.notify();

                        }
                        
                    }
                    
                }
                
            };
            
            // submit as a monitored task so that errors are logged.
            future = fed.submitMonitoredTask(wrappedTask);

        }
        
        // continue to watch the service configuration znode.
        return false;

    }

    private volatile Future future = null;
    
    /**
     * clears all watches used by this class.
     * 
     * @todo cancel the {@link Future} (if not null and not done)?
     */
    protected void clearWatch() throws KeeperException, InterruptedException {

        if(INFO)
            log.info("Lost lock: zpath="+zpath);
        
        zookeeper.exists(zpath, false);

        // @todo don't bother if no node?
        zookeeper.getData(zpath, false, new Stat());

        // @todo don't bother if no node?
        zookeeper.getChildren(zpath, false);

//        for (Future f : physicalServiceMonitors.values()) {
//
//            if (!f.isDone()) {
//
//                f.cancel(true/* mayInterruptedIfRunning */);
//                
//            }
//            
//        }
        
    }

//    /**
//     * Watches the physical service children of a logical service instance. If
//     * physical services are added or removed then this task notifies the
//     * {@link ServiceConfigurationWatcher}. That task will wake up and
//     * rebalance things.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    protected class PhysicalServiceInstancesMonitor implements Callable<Void> {
//        
//        /**
//         * The zpath to the {@link BigdataZooDefs#PHYSICAL_SERVICES} child of
//         * the logical service whose physical services are to be watched.
//         */
//        private final String zpath;
//        
//        /**
//         * 
//         * @param zpath
//         *            The zpath to the {@link BigdataZooDefs#PHYSICAL_SERVICES}
//         *            child of the logical service whose physical services are
//         *            to be watched.
//         */
//        public PhysicalServiceInstancesMonitor(final String zpath) {
//            
//            if (zpath == null)
//                throw new IllegalArgumentException();
//
//            this.zpath = zpath;
//            
//        }
//
//        /**
//         * Notifies the {@link ServiceConfigurationWatcher} if there is a change
//         * to the set of physical services. The task will then exit, but it will
//         * be re-established if necessary.
//         */
//        public Void call() throws Exception {
//
//            final AbstractZNodeConditionWatcher watcher = new AbstractZNodeConditionWatcher(
//                    zookeeper, zpath) {
//
//                @Override
//                protected void clearWatch() throws KeeperException,
//                        InterruptedException {
//
//                    zookeeper.getChildren(zpath, false/* watch */);
//
//                }
//
//                @Override
//                protected boolean isConditionSatisified(WatchedEvent event)
//                        throws KeeperException, InterruptedException {
//
//                    return isConditionSatisified();
//
//                }
//
//                @Override
//                protected boolean isConditionSatisified()
//                        throws KeeperException, InterruptedException {
//
//                    zookeeper.getChildren(zpath, this);
//
//                    return true;
//
//                }
//
//            };
//
////            while(true) 
//            {
//
//                /*
//                 * FIXME Is this triggered on entry? If so we will be in what is
//                 * essentially an endless event loop.
//                 */
//
//                if (INFO)
//                    log.info("Watching: " + zpath);
//
//                watcher.awaitCondition(//false/* testConditionOnEntry */,
//                        Long.MAX_VALUE, TimeUnit.SECONDS);
//
//                synchronized (ServiceConfigurationWatcher.this) {
//
//                    if (INFO)
//                        log.info("Notifying (children changed): " + zpath);
//                    
//                    ServiceConfigurationWatcher.this.notify();
//                    
//                }
//                
//            }
//            
//            return null;
//
//        }
//
//    }

}
