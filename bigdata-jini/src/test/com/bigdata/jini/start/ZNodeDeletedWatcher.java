package com.bigdata.jini.start;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.swing.event.DocumentEvent.EventType;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Watcher notifies a Thread synchronized on itself when the znode that it
 * is watching is deleted. The caller should set an instance of this watcher
 * on a <strong>single</strong> znode if it wishes to be notified when that
 * znode is deleted and then {@link Object#wait()} on the watcher. The
 * {@link Thread} MUST test {@link #deleted} while holding the lock and
 * before waiting (in case the event has already occurred), and again each
 * time {@link Object#wait()} returns (since wait and friends MAY return
 * spuriously). The watcher will re-establish the watch if the
 * {@link EventType} was not {@link EventType#REMOVE}, but it WILL NOT
 * continue to watch the znode once it has been deleted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZNodeDeletedWatcher implements Watcher {

    protected final ZooKeeper zookeeper;
    
    public ZNodeDeletedWatcher(final ZooKeeper zookeeper) {

        if (zookeeper == null)
            throw new IllegalArgumentException();

        this.zookeeper = zookeeper;
        
    }
    
    volatile boolean deleted = false;
    
    public void process(final WatchedEvent event) {

        synchronized (this) {

            if (event.getType().equals(EventType.REMOVE)) {

                deleted = true;

                this.notify();

            } else {
                
                try {
                    
                    // reset the watch.
                    zookeeper.exists(event.getPath(), this);
                    
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

            throw new TimeoutException();
            
        }
        
    }
    
}
