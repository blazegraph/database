package com.bigdata.jini.start;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
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
public class MonitorConfigZNodeTask implements Callable {

    final static protected Logger log = Logger
            .getLogger(MonitorConfigZNodeTask.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    private final JiniFederation fed;
    
    private final ZooKeeper zookeeper;
    
    private final IServiceListener listener;
    
    public MonitorConfigZNodeTask(final JiniFederation fed,
            final IServiceListener listener) {

        if (fed == null)
            throw new IllegalArgumentException();
        
        if (listener == null)
            throw new IllegalArgumentException();

        this.fed = fed;
        
        this.listener = listener;
        
        this.zookeeper = fed.getZookeeper();

    }

    public Object call() throws Exception {
    
        /*
         * This is what we want to keep our eye on. Any new children are new
         * service configurations and we need to start tasks which will
         * monitor those service configurations.
         */

        final String configZPath = fed.getZooConfig().zroot + "/"
                + BigdataZooDefs.CONFIG;

        final UnknownChildrenWatcher watcher = new UnknownChildrenWatcher(
                zookeeper, configZPath);

        try {

            while (true) {

                final String znode = watcher.queue.take();

                // path to the new config node.
                final String serviceConfigZPath = configZPath + "/" + znode;

                try {
                    
                    handleNewConfigZNode(serviceConfigZPath);
                    
                } catch (Throwable t) {

                    /*
                     * Continue processing if there are errors, but not if
                     * the cause was an interrupt since we need to be
                     * responsive if the task is cancelled.
                     */
                    
                    if (InnerCause.isInnerCause(t,
                            InterruptedException.class)) {

                        log.info("interrupted", t);

                        // exit
                        return null;
                        
                    }
                    
                    log.error(this, t);
                    
                }
                
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
    public void handleNewConfigZNode(final String serviceConfigZPath)
            throws KeeperException, InterruptedException {

        if (INFO)
            log.info("new config: zpath=" + serviceConfigZPath);
        
        final ServiceConfiguration config = (ServiceConfiguration) SerializerUtil
                .deserialize(zookeeper.getData(serviceConfigZPath, false, new Stat()));
        
        if (INFO)
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
