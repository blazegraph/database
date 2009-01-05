package com.bigdata.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Base class for some distributed concurrency patterns using {@link ZooKeeper}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractZooPrimitive implements Watcher {

    protected final ZooKeeper zookeeper;

    /**
     * Object is notified each time there is a {@link WatchedEvent}.
     */
    protected final Object lock = new Object();

    protected AbstractZooPrimitive(final ZooKeeper zookeeper) {

        if (zookeeper == null)
            throw new IllegalArgumentException();

        this.zookeeper = zookeeper;

    }

    public void process(final WatchedEvent event) {

        synchronized (lock) {

            lock.notify();

        }

    }
    
}
