package com.bigdata.zookeeper;

import java.util.concurrent.TimeUnit;

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
public class ZNodeDeletedWatcher extends AbstractZNodeConditionWatcher {

    /**
     * If the znode identified by the path exists, then wait up to the
     * timeout for the znode to be deleted.
     * 
     * @param zookeeper
     * @param zpath
     * @param timeout
     * @param unit
     * @return <code>false</code> if the waiting time detectably elapsed
     *         before return from the method, else <code>true</code>.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    static public boolean awaitDelete(final ZooKeeper zookeeper,
            final String zpath, final long timeout, final TimeUnit unit)
            throws InterruptedException {

        return new ZNodeDeletedWatcher(zookeeper, zpath).awaitCondition(
                timeout, unit);

    };

    protected ZNodeDeletedWatcher(final ZooKeeper zookeeper, final String zpath) {

        super(zookeeper, zpath);

    }

    protected boolean isConditionSatisfied(WatchedEvent event)
            throws KeeperException, InterruptedException {

        return event.getType().equals(Watcher.Event.EventType.NodeDeleted);

    }

    protected boolean isConditionSatisfied() throws KeeperException,
            InterruptedException {

        return zookeeper.exists(zpath, this) == null;

    }

    protected void clearWatch() throws KeeperException, InterruptedException {

        // clears the watch.
        zookeeper.exists(zpath, false);

    }

}
