package com.bigdata.jini.start;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.ManagedServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.InnerCause;
import com.bigdata.zookeeper.UnknownChildrenWatcher;

/**
 * Task monitors the {@link BigdataZooDefs#CONFIG} znode and creates a new
 * {@link ServiceConfigurationZNodeMonitorTask} each time a new child is
 * created. The data for those children are {@link ServiceConfiguration}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MonitorConfigZNodeTask implements Callable<Void> {

    final static protected Logger log = Logger
            .getLogger(MonitorConfigZNodeTask.class);

    private final JiniFederation fed;
    
    private final IServiceListener listener;
    
    public MonitorConfigZNodeTask(final JiniFederation fed,
            final IServiceListener listener) {

        if (fed == null)
            throw new IllegalArgumentException();
        
        if (listener == null)
            throw new IllegalArgumentException();

        this.fed = fed;
        
        this.listener = listener;

    }

    /**
     * Start monitoring the {@link BigdataZooDefs#CONFIG} znode.
     * <p>
     * Note: If the znode does not exist or {@link ZooKeeper} is not connected,
     * then the task will keep trying to establish it watch until the znode is
     * created.
     * <p>
     * Note: This task runs until cancelled.
     */
    public Void call() throws Exception {
    
        /*
         * This is what we want to keep our eye on. Any new children are new
         * service configurations and we need to start tasks which will monitor
         * those service configurations.
         * 
         * Note: The UnknownChildWatcher will keep trying until it is able to
         * establish the watch.
         */

        final String configZPath = fed.getZooConfig().zroot + "/"
                + BigdataZooDefs.CONFIG;

        while (true) {

            try {

                acquireWatcherAndRun(configZPath);

            } catch(Throwable t) {

                if(InnerCause.isInnerCause(t, InterruptedException.class)) {
                
                    if(log.isInfoEnabled())
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

    protected void acquireWatcherAndRun(final String configZPath)
            throws KeeperException, InterruptedException {

        final ZooKeeper zookeeper = fed.getZookeeper();
        
        final UnknownChildrenWatcher watcher = new UnknownChildrenWatcher(
                zookeeper, configZPath);

        try {

            while (true) {

                final String znode = watcher.queue.take();

                // path to the new config node.
                final String serviceConfigZPath = configZPath + "/" + znode;

                handleNewConfigZNode(zookeeper, serviceConfigZPath);

            }

        } finally {

            watcher.cancel();

        }

    }

    /**
     * Create a {@link ServiceConfigurationZNodeMonitorTask} for a specific
     * service config node and submits it for execution.
     * 
     * @param serviceConfigZPath
     *            The zpath to the new {@link ServiceConfiguration} znode.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    protected void handleNewConfigZNode(final ZooKeeper zookeeper,
            final String serviceConfigZPath) throws KeeperException,
            InterruptedException {

        if (log.isInfoEnabled())
            log.info("new config: zpath=" + serviceConfigZPath);
        
        final ManagedServiceConfiguration config = (ManagedServiceConfiguration) SerializerUtil
                .deserialize(zookeeper.getData(serviceConfigZPath, false,
                        new Stat()));
        
        if (log.isInfoEnabled())
            log.info("config state: " + config);

        // create task.
        final ServiceConfigurationZNodeMonitorTask task = new ServiceConfigurationZNodeMonitorTask(
                fed, listener, config.className);

        /*
         * Start monitored task.
         * 
         * Note: the task should not terminate unless cancelled.
         */
        fed.submitMonitoredTask(task);

    }

}
