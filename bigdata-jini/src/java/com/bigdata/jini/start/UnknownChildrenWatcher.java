package com.bigdata.jini.start;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Notices when unknown children appear for the watched node. The children
 * are added to a {@link #queue} which should be drained by the application.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnknownChildrenWatcher implements Watcher {

    final static protected Logger log = Logger.getLogger(UnknownChildrenWatcher.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    final ZooKeeper zookeeper;
    final String zpath;

    /**
     * Set of children that we have already seen.
     * 
     * FIXME There should be a way to clear {@link #knownLocks} over time as it
     * will grow without bound. An LRU would be one approximation. Or a weak
     * value hash map and clearing the entries references after a timeout.
     */
    private final LinkedHashSet<String> knownLocks = new LinkedHashSet<String>();
    
    /**
     * Queue to be drained. It is populated by the {@link Watcher} in the
     * event thread and drained in an application thread.
     */
    public final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
    
    /**
     * @param zookeeper
     * @param zpath
     */
    protected UnknownChildrenWatcher(final ZooKeeper zookeeper,
            final String zpath) {

        if (zookeeper == null)
            throw new IllegalArgumentException();
        if (zpath == null)
            throw new IllegalArgumentException();

        this.zookeeper = zookeeper;

        this.zpath = zpath;
        
    }

    synchronized public void process(WatchedEvent event) {
        
        switch(event.getState()) {
        case Disconnected:
            return;
        default:
            break;
        }
        
        switch (event.getType()) {
        case NodeChildrenChanged:
            break;
        default:
            return;
        }

        final List<String> children;
        try {
            children = zookeeper.getChildren(zpath, this);
        } catch (Exception e) {
            log.error(this, e);
            return;
        }
        
        int nadded = 0;
        
        for( String child : children) {
            
            if(!knownLocks.contains(child)) {

                /*
                 * Add locks not previously known to the set of known locks
                 * and to the queue.
                 */
                
                knownLocks.add(child);
                
                queue.add(child);
                
                nadded++;
                
            }
            
        }
        
        if (INFO)
            log.info("added " + nadded + " : known=" + knownLocks.size());

    }

    /**
     * Cancel the watch.
     * <p>
     * Note: synchronized to avoid contention with
     * {@link #process(WatchedEvent)}
     */
    synchronized public void cancel() {

        try {
            zookeeper.getChildren(zpath, false);
        } catch (Exception e) {
            log.error(this, e);
        }
        
    }
    
    public String toString() {
        
        final int knownSize;
        synchronized (this) {

            knownSize = knownLocks.size();

        }

        return getClass().getName() + "{zpath=" + zpath + ", queueSize="
                + queue.size() + ", knownSize=" + knownSize + "}";

    }

}
