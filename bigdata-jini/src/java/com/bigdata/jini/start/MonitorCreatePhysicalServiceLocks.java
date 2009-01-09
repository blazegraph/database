package com.bigdata.jini.start;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZNodeLockWatcher;

/**
 * Notices when new a new lock node is created and contends for the lock if
 * the localhost can satisify the {@link IServiceConstraint}s for the new
 * physical service. The {@link ServiceConfiguration} is fetched from the
 * zpath written into the data of the lock node. The
 * {@link IServiceConstraint}s found are in that
 * {@link ServiceConfiguration}.
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

    public Void call() throws Exception {

        // all the locks of interest are created here.
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
                    
                } catch (KeeperException ex) {
                    
                    // continue processing if there are errors.
                    
                    log.error(this, ex);
                    
                }
                
            }

        } finally {

            watcher.cancel();

        }

    }
    
    /**
     * Verify that we could start this service.
     * 
     * @todo add load-based constraints, e.g., can not start a new service if
     *       heavily swapping, near limits on RAM or on disk.
     *       <p>
     *       Might be interesting to evaluate all constraints and see how many
     *       cause us not to start the service.
     */
    protected boolean canStartService(final ServiceConfiguration config) {
        
        for (IServiceConstraint constraint : config.constraints) {

            if (!constraint.allow(fed)) {

                if (INFO) 
                    log.info("Constraint does not allow service start: "
                            + constraint);
                
                return false;
                
            }
            
        }
        
        return true;

    }

    /**
     * 
     * @param zpath
     *            The path to the logical service znode.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     * 
     * @todo it could be safer to move the fetch of the
     *       {@link ServiceConfiguration} and the
     *       {@link #canStartService(ServiceConfiguration)} call to after we
     *       hold the lock. that way we judge matters as they stand at the time
     *       of the decision.
     */
    protected void handleNewLock(final String zpath) throws KeeperException,
            InterruptedException {

        final String configZPath = (String) SerializerUtil
                .deserialize(zookeeper.getData(zpath, false, new Stat()));

        if (INFO)
            log.info("configZPath=" + configZPath);

        // enter the competition.
        final ZLock zlock = ZNodeLockWatcher.getLock(zookeeper, zpath);

        zlock.lock();
        try {

            if (INFO)
                log.info("have lock: zpath=" + zpath);

            final ServiceConfiguration config = (ServiceConfiguration) SerializerUtil
                    .deserialize(zookeeper.getData(configZPath, false,
                            new Stat()));

            if (INFO)
                log.info("Considering: " + config);

            if (!canStartService(config)) {

                // will not start this service.

                if (INFO)
                    log.info("Constraint(s) do not allow service start: "
                            + config);

                return;

            }

            try {
                
                // start the service.
                startService(config, zpath);

                // iff successful, then destroy the lock.
                zlock.destroyLock();
                
            } catch (Exception e) {

                // could not start.
                log.error(this, e); // FIXME comment out - MUST watch its Future.
                
                throw new RuntimeException(e);
                
            }

        } finally {
            
            zlock.unlock();
            
        }

    }

    /**
     * Start the service.
     * 
     * @param config
     * @param zpath
     * @throws Exception
     */
    protected void startService(ServiceConfiguration config, String zpath)
            throws Exception {

        if (INFO)
            log.info("config=" + config + ", zpath=" + zpath);
        
        // get task to start the service.
        final Callable task = config.newServiceStarter(fed, listener, zpath);

        // Submit the task and wait for its Future.
        fed.getExecutorService().submit(task).get();

    }

}
