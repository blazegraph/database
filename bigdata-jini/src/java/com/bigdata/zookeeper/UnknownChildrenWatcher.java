package com.bigdata.zookeeper;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;

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

    final private ZooKeeper zookeeper;
    final private String zpath;
    private boolean cancelled = false;

    /**
     * Set of children that we have already seen.
     * 
     * FIXME There should be a way to clear {@link #known} over time as it
     * will grow without bound. An LRU would be one approximation. Or a weak
     * value hash map and clearing the entries references after a timeout.
     */
    private final LinkedHashSet<String> known = new LinkedHashSet<String>();
    
    /**
     * Queue to be drained. It is populated by the {@link Watcher} in the
     * event thread and drained in an application thread.
     */
    public final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
    
    /**
     * @param zookeeper
     * @param zpath
     * 
     * @throws InterruptedException 
     * @throws KeeperException 
     */
    public UnknownChildrenWatcher(final ZooKeeper zookeeper,
            final String zpath) throws KeeperException, InterruptedException {

        if (zookeeper == null)
            throw new IllegalArgumentException();
        if (zpath == null)
            throw new IllegalArgumentException();

        this.zookeeper = zookeeper;

        this.zpath = zpath;
        
        if(INFO)
            log.info("watching: "+zpath);
        
        // loop until we are able to set the watch.
        while (true) {
            
            try {
            
                acceptChildren(zookeeper.getChildren(zpath, this));
                
                break;
                
            } catch (InterruptedException t) {

                cancelled = true;
                
                // task was cancelled.
                throw t;
                
            } catch (Throwable t) {
                
                if (INFO)
                    log.info("will retry: " + this + " : " + t);

                // sleep, but if we are interrupted then exit/
                Thread.sleep(500/* ms */);
                
            }
       
        }
        
    }

    synchronized public void process(WatchedEvent event) {

        if(cancelled) {
            
            // ignore events after the watcher was cancelled.
            return;
            
        }
        
        if(INFO)
            log.info(event.toString());

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

        try {
            acceptChildren(zookeeper.getChildren(zpath, this));
        } catch (Exception e) {
            log.error(this, e);
            return;
        }
        
    }

    private void acceptChildren(final List<String> children) {
        
        int nadded = 0;
        
        for( String child : children) {
            
            if(!known.contains(child)) {

                /*
                 * Add children not previously known to the set of known
                 * children and to the queue.
                 */
                
                known.add(child);
                
                queue.add(child);
                
                nadded++;
                
            }
            
        }
        
        if (INFO)
            log.info("added " + nadded + " : known=" + known.size());

    }
    
    /**
     * Cancel the watch.
     * <p>
     * Note: synchronized to avoid contention with
     * {@link #process(WatchedEvent)}
     */
    synchronized public void cancel() {

        /*
         * Set a flag so that we will ignore any future events even if we are
         * temporarily disconnected from zookeeper and therefore unable to clear
         * out watcher.
         * 
         * @todo we can probably forgo actually clearing the watcher since
         * requesting the children now does as much work as being information
         * about them later and ignoring the event.
         */
        
        cancelled = true;

        try {
            
            zookeeper.getChildren(zpath, false);
            
        } catch(ConnectionLossException ex) {
            
            if (INFO)
                log.info(toString() + ":" + ex);
            
        } catch (Exception e) {
            
            log.error(this, e);
            
        }
        
    }
    
    public String toString() {
        
        final int knownSize;
        synchronized (this) {

            knownSize = known.size();

        }

        return getClass().getName() + "{zpath=" + zpath + ", queueSize="
                + queue.size() + ", knownSize=" + knownSize + "}";

    }

}
