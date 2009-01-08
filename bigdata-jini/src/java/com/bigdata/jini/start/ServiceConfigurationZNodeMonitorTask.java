package com.bigdata.jini.start;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZNodeLockWatcher;
import com.sun.corba.se.impl.orbutil.closure.Future;

/**
 * This is a standing task whose {@link Future} is monitored by the
 * {@link ServicesManagerServer}. For each {@link ServiceConfiguration} znode, there
 * is one such task per {@link ServicesManagerServer}. The task with holding the
 * {@link ZLock} is the one that handles state changes for
 * {@link ServiceConfiguration} znode, including its children, which are the
 * logical service instance znodes.
 * <p>
 * If the {@link ServicesManagerServer} dies, then its znodes will be deleted from the
 * lock queues and someone else (if anyone is left alive) will get the lock and
 * manage the logical service.
 * <p>
 * If the task itself dies, then the {@link ServicesManagerServer} needs to log an
 * error and create a new instance of the task.
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
        lockZPath = zroot + "/" + BigdataZooDefs.LOCKS + "/" + className;

        // zpath for the service configuration node.
        serviceConfigZPath = zroot + "/" + BigdataZooDefs.CONFIG + "/" + className;
        
    }

    /**
     * Contends for a {@link ZLock} on {@link #zpath} and then establishes a
     * {@link ServiceConfigurationWatcher} to maintain the logical service in
     * balance with its {@link ServiceConfiguration}.
     * <p>
     * Note: The watcher only runs while the {@link ZLock} is held. When it
     * looses the {@link ZLock} it will exit (that is the condition that it
     * watches) and the seek to re-acquire the lock.
     */
    public Void call() throws Exception {

        while (true) {

            final ZLock zlock = ZNodeLockWatcher.getLock(zookeeper,
                    lockZPath);

            zlock.lock();

            try {

                if(INFO) {
                    
                    log.info("Have lock: setting watcher: zlock=" + zlock
                            + ", serviceConfigZPath=" + serviceConfigZPath);
                    
                }
                
                final ServiceConfigurationWatcher watcher = new ServiceConfigurationWatcher(
                        fed, listener, serviceConfigZPath, zlock);

                watcher.awaitCondition(Long.MAX_VALUE, TimeUnit.SECONDS);

                return null;

            } finally {

                zlock.unlock();

            }
            
        }

    }

}