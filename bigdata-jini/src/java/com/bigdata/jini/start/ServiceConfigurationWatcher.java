package com.bigdata.jini.start;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.AbstractZNodeConditionWatcher;
import com.bigdata.zookeeper.ZLock;

/**
 * Watcher ONLY runs while it hold a {@link ZLock} granting it permission to
 * manage some {@link ServiceConfiguration}. If an event is received which
 * indicates that something is out of balance then we verify that we still hold
 * the {@link ZLock} and then make any necessary adjustments. If we do not hold
 * the {@link ZLock} then we cancel the watcher. In order to prevent blocking
 * the zookeeper event thread, we ignore any events received while we are
 * already processing a previous event.
 * 
 * @see ServiceConfigurationZNodeMonitorTask
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
        final ServiceConfiguration config = (ServiceConfiguration) SerializerUtil
                .deserialize(zookeeper.getData(zpath, this, new Stat()));

        // get children (the logical services) (and reset our watch).
        final List<String> children = zookeeper.getChildren(zpath, this);

        /*
         * Handle the event.
         * 
         * Note: we MUST NOT attempt to manage this service again until the
         * event has been handled. However, at the same time we MUST NOT block
         * while in the zookeeper event thread.
         */
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

        if (config.serviceCount != children.size()) {

            // the task.
            final Callable task = config.newLogicalServiceTask(fed, listener, zpath,
                    children);
            
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

        zookeeper.exists(zpath, false);

        // @todo don't bother if no node?
        zookeeper.getData(zpath, false, new Stat());

        // @todo don't bother if no node?
        zookeeper.getChildren(zpath, false);

    }

}
