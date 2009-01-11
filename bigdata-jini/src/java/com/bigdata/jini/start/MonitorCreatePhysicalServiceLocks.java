package com.bigdata.jini.start;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

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
 * Notices when new a new lock node is created and contends for the lock if the
 * localhost can satisify the {@link IServiceConstraint}s for the new physical
 * service. The {@link ServiceConfiguration} is fetched using the zpath written
 * into the data of the lock node. The {@link IServiceConstraint}s found are in
 * that {@link ServiceConfiguration}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MonitorCreatePhysicalServiceLocks implements
        Callable<Void> {

    final static protected Logger log = Logger
            .getLogger(MonitorCreatePhysicalServiceLocks.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    private final JiniFederation fed;
    
    private final ZooKeeper zookeeper;
    
    private final IServiceListener listener;
    
    public MonitorCreatePhysicalServiceLocks(final JiniFederation fed,
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

        // all the locks of interest are children of this znode.
        final String locksZPath = fed.getZooConfig().zroot + "/"
                + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE;

        final UnknownChildrenWatcher watcher = new UnknownChildrenWatcher(
                zookeeper, locksZPath);
     
        try {

            while (true) {

                final String znode = watcher.queue.take();

                // path to the new lock node.
                final String zpath = locksZPath + "/" + znode;

                if (INFO)
                    log.info("new lock: zpath=" + zpath);
                
                try {
                    
                    handleNewLock(zpath);
                    
                } catch(InterruptedException ex) {
                    
                    // exit on interrupt (task cancelled)
                    
                    log.warn("Interrupted.");
                    
                    return null;
                    
                } catch (Throwable t) {
                    
                    /*
                     * Continue processing if there are errors since we still
                     * want to monitor for new service start requests and see if
                     * we can handle them.
                     */
                    
                    log.error(this, t);
                    
                }
                
            }

        } finally {

            watcher.cancel();

        }

    }
    
    /**
     * Contends for the {@link ZLock} and starts the service if the the
     * {@link IServiceConstraint}s are satisified.
     * <p>
     * Note: This fetches the {@link ServiceConfiguration} and tests the
     * {@link IServiceConstraint}s after we hold the {@link ZLock} and then
     * makes the decision whether or not to start the service. That way it
     * judges matters as they stand at the time of the decision, rather than
     * when we joined the queue to contend for the {@link ZLock}.
     * 
     * @param lockNodeZPath
     *            The path to the lock node.
     * 
     * @return <code>true</code> if we started the service.
     * 
     * @throws Exception
     *             if we could not start the service.
     */
    protected boolean handleNewLock(final String lockNodeZPath) throws Exception {

        /*
         * Note: The data is the logicalService zpath.
         */
        final String logicalServiceZPath = (String) SerializerUtil
                .deserialize(zookeeper.getData(lockNodeZPath, false, new Stat()));

        /*
         * If we hack off the last path component, we now have the zpath for
         * the ServiceConfiguration znode.
         */
        final String serviceConfigZPath = logicalServiceZPath.substring(0,
                logicalServiceZPath.lastIndexOf('/'));
        
        if (INFO)
            log.info("logicalServiceZPath=" + logicalServiceZPath);

        // enter the competition.
        final ZLock zlock = ZNodeLockWatcher.getLock(zookeeper, lockNodeZPath);

        zlock.lock();
        try {

            if (INFO)
                log.info("have lock: zpath=" + lockNodeZPath);

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

            // start the service.
            startService(config, logicalServiceZPath);

            // iff successful, then destroy the lock.
            zlock.destroyLock();
            
            return true;
            
        } finally {
            
            zlock.unlock();
            
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
