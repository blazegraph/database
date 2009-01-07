package com.bigdata.jini.start;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * An instance of this class may be used to watch for the delete of a
 * single znode.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZNodeDeletedWatcher implements Watcher {

    final static protected Logger log = Logger.getLogger(ZNodeDeletedWatcher.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    protected final ZooKeeper zookeeper;
    
    public ZNodeDeletedWatcher(final ZooKeeper zookeeper) {

        if (zookeeper == null)
            throw new IllegalArgumentException();

        this.zookeeper = zookeeper;
        
    }
    
    volatile boolean deleted = false;
    
    /**
     * Clear the watch. This is necessary for the {@link Watcher} to stop
     * getting notices of changes after it has noticed the change that it was
     * looking for.
     */
    private void clearWatch(final String zpath) {
        try {
            zookeeper.exists(zpath, false);
        } catch (KeeperException ex) {
            // ignore
            log.warn(ex);
        } catch (InterruptedException ex) {
            // ignore.
            log.warn(ex);
        }

    }
    
    /**
     * The watcher notifies a Thread synchronized on itself when the znode that
     * it is watching is deleted. The caller should set an instance of this
     * watcher on a <strong>single</strong> znode if it wishes to be notified
     * when that znode is deleted and then {@link Object#wait()} on the watcher.
     * The {@link Thread} MUST test {@link #deleted} while holding the lock and
     * before waiting (in case the event has already occurred), and again each
     * time {@link Object#wait()} returns (since wait and friends MAY return
     * spuriously). The watcher will re-establish the watch if the
     * {@link Watcher.Event.EventType} was not
     * {@link Watcher.Event.EventType#NodeDeleted}, but it WILL NOT continue to
     * watch the znode once it has been deleted.
     */
    public void process(final WatchedEvent event) {

        if(INFO)
            log.info(event.toString());
        
        synchronized (this) {

            if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {

                // node was deleted.
                deleted = true;

                this.notify();
                
                // clear watch or we will keep getting notices.
                clearWatch(event.getPath());

            } else {
                
                try {
                    
                    if(INFO)
                        log.info("will reset watch");

                    // reset the watch.
                    if (zookeeper.exists(event.getPath(), this) == null) {

                        // node already deleted.

                        if(INFO)
                            log.info("node already deleted");
                        
                        deleted = true;
                        
                        this.notify();
                        
                        // clear watch or we will keep getting notices.
                        clearWatch(event.getPath());

                    }

                    if(INFO)
                        log.info("did reset watch");

                } catch (Exception e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }

        }

    }

    /**
     * Wait up to timeout units for the watched znode to be deleted.
     * <p>
     * Note: the resolution is millseconds at most.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The units.
     * 
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void awaitRemove(final long timeout, final TimeUnit unit)
            throws TimeoutException, InterruptedException {

        synchronized (this) {

            final long begin = System.currentTimeMillis();

            long millis = unit.toMillis(timeout);

            while (millis > 0 && !deleted) {

                this.wait(millis);

                millis -= (System.currentTimeMillis() - begin);

            }

            if (!deleted)

                throw new TimeoutException();

        }

    }

}
