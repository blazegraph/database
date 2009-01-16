package com.bigdata.jini.start;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.IServiceConstraint;
import com.bigdata.jini.start.config.ManagedServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.UnknownChildrenWatcher;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZNodeLockWatcher;

/**
 * This task notices when a new lock node is created and creates and runs a
 * {@link CreatePhysicalServiceTask} to handle that lock node. The lock node
 * represents a specific logical service instance which is short at least one
 * physical service instance. The data of the lock node contains the zpath of
 * the logical service instance.
 * <p>
 * Note: A single instance of this task is started by
 * {@link AbstractServicesManagerService#start()}. Further, this task
 * serializes service creations events using a {@link #lock}. If only a single
 * {@link ServicesManagerServer} is run per host, then this allows
 * {@link IServiceConstraint}s to be specified that restrict the mixture of
 * services running a host.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MonitorCreatePhysicalServiceLocksTask implements
        Callable<Void> {

    final static protected Logger log = Logger
            .getLogger(MonitorCreatePhysicalServiceLocksTask.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    private final JiniFederation fed;
    
    private final ZooKeeper zookeeper;
    
    private final IServiceListener listener;
    
    /**
     * Used to serialize physical service creates on a given host.
     * <p>
     * Note: this assumes one {@link MonitorCreatePhysicalServiceLocksTask} per
     * {@link ServicesManagerServer}, which should be true. See the startup
     * logic in {@link AbstractServicesManagerService}.
     * <p>
     * Note: This assumes that there is one {@link ServicesManagerServer} per
     * host. We are not enforcing that constraint.
     */
    final protected Lock lock = new ReentrantLock();
    
    public MonitorCreatePhysicalServiceLocksTask(final JiniFederation fed,
            final IServiceListener listener) {

        if (fed == null)
            throw new IllegalArgumentException();
        
        if (listener == null)
            throw new IllegalArgumentException();

        this.fed = fed;
        
        this.listener = listener;
        
        this.zookeeper = fed.getZookeeper();

    }

    /**
     * Task runs until cancelled.
     */
    public Void call() throws Exception {

        /*
         * All the locks of interest are direct children of this znode.
         */
        final String locksZPath = fed.getZooConfig().zroot + "/"
                + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE;

        final UnknownChildrenWatcher watcher = new UnknownChildrenWatcher(
                zookeeper, locksZPath);
     
        try {

            // consume new lock nodes from the watcher's queue.
            while (true) {

                try {

                    // child znode.
                    final String znode = watcher.queue.take();

                    if (znode.endsWith(ZNodeLockWatcher.INVALID)) {

                        /*
                         * This is not a lock node. It is a marker indicating
                         * that the corresponding lock node is about to be
                         * destroyed. We just ignore it here.
                         */

                        continue;

                    }

                    // path to the new lock node.
                    final String lockNodeZPath = locksZPath + "/" + znode;

                    if (INFO)
                        log.info("new lock: zpath=" + lockNodeZPath);

                    /*
                     * Note: This task should run until either it or another
                     * services manager causes the service to be created, until
                     * the lock node is destroyed, or until the constraints no
                     * longer indicate that we need to create a new service
                     * instance.
                     */

                    fed.submitMonitoredTask(new CreatePhysicalServiceTask(
                            lockNodeZPath));

                } catch (InterruptedException ex) {

                    // exit on interrupt (task cancelled)

                    log.warn("Interrupted.");

                    throw ex;

                } catch (Throwable t) {

                    /*
                     * Continue processing if there are errors since we still
                     * want to monitor for new service start requests.
                     */

                    log.error(this, t);

                }

            }
            
        } finally {

            watcher.cancel();

        }

    }

    /**
     * Task contends for the {@link ZLock}. If the lock is obtained, the
     * {@link ServiceConfiguration} is fetched using the zpath written into the
     * data of the lock node and the service constraints are checked. If the
     * constraints are satisified by this host, then the task attempts to start
     * the service. If the service can be started successfully, then the lock
     * node is destroyed and the task exits. Otherwise the task releases the
     * lock and sleeps a bit. Either it or another task running on another
     * {@link ServicesManagerServer} will gain the lock and try again. (If the
     * lock node is invalidated or destroyed, then the task will quit.)
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class CreatePhysicalServiceTask implements Callable<Boolean> {

        /**
         * The zpath of the lock node. The data in this node is the xpath of the
         * logical service whose instance needs to be created.
         */
        protected final String lockNodeZPath;

        public CreatePhysicalServiceTask(final String lockNodeZPath) {

            if (lockNodeZPath == null)
                throw new IllegalArgumentException();

            this.lockNodeZPath = lockNodeZPath;

        }

        /**
         * Contends for the {@link ZLock} and then invokes
         * {@link #runWithLock()}, which will verify the constraints and
         * attempt to start the service.
         * <p>
         * Note: If we are unable to create the service while we are holding the
         * lock then we wait a little bit and try again. This covers the case
         * where there are preconditions for the service start which have not
         * been met but which might become satisified at any time.
         * <p>
         * Note: If the lock node is deleted, then we will exit and return
         * <code>false</code>.
         * 
         * @param lockNodeZPath
         *            The path to the lock node.
         * 
         * @return <code>true</code> if we started the service.
         * 
         * @throws Exception
         *             if we could not start the service.
         */
        public Boolean call() throws Exception {

            // until we create the service or the lock node is destroyed.
            while (true) {

                /*
                 * This enforces a single lock acquisition attempt at a time.
                 * This is important in order for constaints on the maximum #of
                 * service instances on a single host to be respected (and
                 * assumes only a single services manager per host).
                 * 
                 * Consider: A new {@link ServicesManagerServer} starts on a
                 * host. It sees a demand for 10 data services. For each one, it
                 * tests a constraint on the maximum #of data service instances
                 * (LTE 2) and finds that it has NO data services running, so
                 * for each one it creates a data service and then it has 10. By
                 * synchronizing here we force this to single thread the service
                 * creates and in order to avoid this problem.
                 */
                lock.lock();
                
                try {

                    if (zookeeper.exists(lockNodeZPath, false) == null) {

                        /*
                         * End of the competition. Either someone created the
                         * service or someone destroyed the lock node.
                         */

                        return false;

                    }

                    if (runOnce()) {

                        /*
                         * Service was created.
                         */

                        return true;

                    }

                } catch (InterruptedException ex) {

                    // interrupted - stop competing.
                    throw ex;

                } catch (Throwable t) {

                    log.warn("lockNode=" + lockNodeZPath, t);

                    // fall through.

                } finally {

                    lock.unlock();

                }

                /*
                 * We were not able to create the service while we held the
                 * lock. This can occur if there is a precondition for the
                 * service which is not satisified. Wait a bit a try again.
                 * 
                 * Note: We have released the [lock] so other service creates
                 * can proceed while we sleep on this one.
                 * 
                 * @todo configure retry interval.
                 */

                Thread.sleep(5000/* ms */);

                if (INFO)
                    log.info("Retrying: " + lockNodeZPath);

            } // while true

        }

        /**
         * Run once, trying to acquire the {@link ZLock}.
         * 
         * @return <code>true</code> iff the service was created.
         * 
         * @throws Exception
         */
        private boolean runOnce() throws Exception {

            // enter the competition.
            final ZLock zlock = ZNodeLockWatcher.getLock(zookeeper,
                    lockNodeZPath, fed.getZooConfig().acl);

            zlock.lock();
            try {

                if (INFO)
                    log.info("have lock: zpath=" + lockNodeZPath);

                if (runWithLock()) {

                    // iff successful, then destroy the lock.
                    zlock.destroyLock();

                    // end of the competition.
                    return true;

                }

                return false;
                
            } finally {

                zlock.unlock();

            }

        }
        
        /**
         * Starts the service if the the {@link IServiceConstraint}s are
         * satisified.
         * <p>
         * Note: This fetches the {@link ServiceConfiguration} and tests the
         * {@link IServiceConstraint}s after we hold the {@link ZLock} and then
         * makes the decision whether or not to start the service. That way it
         * judges matters as they stand at the time of the decision, rather than
         * when we joined the queue to contend for the {@link ZLock}.
         * 
         * @return <code>true</code> iff the service was started.
         * 
         * @throws Exception
         */
        private boolean runWithLock()
                throws Exception {

            /*
             * Note: The data is the logicalService zpath.
             */
            final String logicalServiceZPath = (String) SerializerUtil
                    .deserialize(zookeeper.getData(lockNodeZPath, false,
                            new Stat()));

            /*
             * If we hack off the last path component, we now have the zpath for the
             * ServiceConfiguration znode.
             */
            final String serviceConfigZPath = logicalServiceZPath.substring(0,
                    logicalServiceZPath.lastIndexOf('/'));

            if (INFO)
                log.info("logicalServiceZPath=" + logicalServiceZPath);

            final ManagedServiceConfiguration config = (ManagedServiceConfiguration) SerializerUtil
                    .deserialize(zookeeper.getData(serviceConfigZPath, false,
                            new Stat()));

            if (INFO)
                log.info("Considering: " + config);

            if (!config.canStartService(fed)) {

                // will not start this service.

                if (INFO)
                    log.info("Constraint(s) do not allow service start: "
                            + config);

                return false;

            }

            // get children (the list of physical services).
            final List<String> children = zookeeper
                    .getChildren(logicalServiceZPath + "/"
                            + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER, false);

            if (INFO)
                log.info("serviceConfigZPath=" + serviceConfigZPath
                        + ", logicalServiceZPath=" + logicalServiceZPath
                        + ", targetReplicationCount=" + config.replicationCount
                        + ", #children=" + children.size() + ", children="
                        + children);

            final int ninstances = children.size();
            
            if (ninstances >= config.replicationCount) {

                /*
                 * Stops the task from running if the #of physical service
                 * instances is GTE the target replicationCount.
                 */

                throw new InterruptedException("No new instances required.");

            } else {

                /*
                 * There are not enough physical service instances so try to
                 * start one now.
                 */

                // start the service.
                startService(config, logicalServiceZPath);

                return true;

            }

        }

        /**
         * Start the service.
         * 
         * @param config
         * @param logicalServiceZPath
         * @throws Exception
         */
        @SuppressWarnings("unchecked")
        protected void startService(final ManagedServiceConfiguration config,
                final String logicalServiceZPath) throws Exception {

            if (INFO)
                log.info("config=" + config + ", zpath=" + logicalServiceZPath);

            // get task to start the service.
            final Callable task = config.newServiceStarter(fed, listener,
                    logicalServiceZPath);

            /*
             * Submit the task and waits for its Future (up to the timeout).
             */
            fed.getExecutorService().submit(task).get(config.timeout,
                    TimeUnit.MILLISECONDS);

        }

    }

}
