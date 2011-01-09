/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jan 11, 2009
 */

package com.bigdata.zookeeper;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import com.bigdata.btree.IRangeQuery;

/**
 * This class accepts a dynamic set of watch criteria and pump events into a
 * queue when any watch was triggered. Hooks are defined that you can use to
 * identify new watch criteria based on {@link WatchedEvent}s. By default, this
 * will extend a watch over the transitive closure of the children of some
 * znode. However, you can limit the set of watch criteria using some hook
 * methods. For example, only following certain children for a given path.
 * <p>
 * {@link WatchedEvent}s are received in the zookeeper event thread. In order to
 * minimize the work performed in that thread, events are triaged. The triage
 * determines the "type" of the znode for which the event was received. Types
 * reflect application considerations. For example, a change in the #of children
 * of a logical service versus a change in the data for a service configuration.
 * If the typed event adds or removes children that are of interest (by default,
 * all children are of interest, but you may place restrictions on that), then
 * new watch criteria are added for the newly identified children. The typed
 * event is then placed on an unbounded {@link BlockingQueue}. The application
 * is responsible for draining the queue. The constructor establishes the
 * initial watch on the root of the hierarchy of interest and then returns
 * control to the application. All other work by this class happens in the
 * zookeeper event thread.
 * <p>
 * Zookeeper DOES NOT provide a guarantee that we will see all events of
 * interest since state changes may occur after a watch has been triggered and
 * before the {@link WatchedEvent} is received and before we have the chance to
 * reset the watch. By queuing events we reduce the time in the zookeeper event
 * thread and thus increase responsiveness and reduce the opportunity for missed
 * events, but we DO NOT guarantee that the application will see all events.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo This re-establishes the watcher when the event is handled in the
 *       zookeeper thread. That is probably not a good idea as it increases the
 *       demand on the zookeeper thread. Instead, when the application sees the
 *       wrapped event it should read the data, re-establishing the watch as a
 *       side-effect, and then issue whatever actions are necessary based on the
 *       delta between the historical state of the znode/children and their
 *       current state. This pattern guarantees that no state changes are
 *       missed, but requires the application to track the old state of each
 *       znode/children of interest.
 */
abstract public class HierarchicalZNodeWatcher implements Watcher,
        HierarchicalZNodeWatcherFlags {

    final static protected Logger log = Logger
            .getLogger(UnknownChildrenWatcher.class);

    private ZooKeeper zookeeper;
    
    /**
     * The root of the hierarchy of watched znodes.
     */
    final protected String zroot;

    /**
     * New events are not accepted once this flag has been set.
     * 
     * @see #cancel()
     */
    private volatile boolean cancelled = false;
    
    /**
     * This is true until the startup scan is complete.
     */
    private volatile boolean pumpMockEvents = true;
    
    /**
     * Queue to be drained. It is populated by the {@link Watcher} in the event
     * thread and must be drained in an application thread. All events for
     * watched znodes are included.
     */
    public final BlockingQueue<WatchedEvent> queue = new LinkedBlockingQueue<WatchedEvent>();

    /**
     * The set of znodes that are being watched.
     * <p>
     * Note: {@link #process(WatchedEvent)} runs in the zookeeper event thread.
     * It is marked as <code>synchronized</code> so that any methods which
     * access this collection outside of that thread may be mutex with the
     * event thread simply by synchronizing on <i>this</i>.
     */
    private final LinkedHashMap<String/* zpath */, Integer/* flags */> watched = new LinkedHashMap<String, Integer>();

    /**
     * Sets a watch on the <i>zroot</i> and any descendants selected by
     * <i>flags</i> and {@link #watch(String, String)}.
     * <p>
     * If {@link HierarchicalZNodeWatcherFlags#CHILDREN} is set, then watches
     * are set using {@link #setWatch(String, int)} for any child for which
     * {@link #watch(String, String)} returns non-zero. This process is
     * recursive if {@link HierarchicalZNodeWatcherFlags#CHILDREN} is set for
     * any of those children.
     * 
     * @param zookeeper
     * @param zroot
     *            The root of the watched hierarchy.
     * @param flags
     *            The flags for the watches to be set on the zroot. The
     *            {@link HierarchicalZNodeWatcherFlags#EXITS} and
     *            {@link HierarchicalZNodeWatcherFlags#CHILDREN} are
     *            automatically included, but you must specify
     *            {@link HierarchicalZNodeWatcherFlags#DATA} if you want to
     *            watch the data on the zroot as well.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    public HierarchicalZNodeWatcher(final ZooKeeper zookeeper,
            final String zroot, int flags) throws InterruptedException,
            KeeperException {
    
        this(zookeeper, zroot, flags, false/* pumpMockEventsDuringStartup */);
        
    }

    /**
     * 
     * @param zookeeper
     * @param zroot
     * @param flags
     * @param pumpMockEventsDuringStartup
     *            When true, mock events are placed in the {@link #queue} during
     *            the startup scan. This gives the application pre-order
     *            traversal events for the hierarchy. The children of a given
     *            not will appear in whatever order zookeeper reports (they are
     *            not sorted).
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    public HierarchicalZNodeWatcher(final ZooKeeper zookeeper,
            final String zroot, int flags, final boolean pumpMockEventsDuringStartup)
            throws InterruptedException, KeeperException {

        if (zookeeper == null)
            throw new IllegalArgumentException();

        if (zroot == null)
            throw new IllegalArgumentException();

        flags |= EXISTS | CHILDREN;
        
//        if ((flags & ALL) == 0)
//            throw new IllegalArgumentException();
        
        this.zookeeper = zookeeper;

        this.zroot = zroot;
        
        if (log.isInfoEnabled())
            log.info("zroot=" + zroot + ", flags=" + flagString(flags));

        /*
         * Set the initial watches.
         * 
         * @todo If we are going to pump events into the queue during startup
         * then we might consider making start mutex with the event handler.
         * 
         * synchronized(this) does the trick.
         */
        pumpMockEvents = pumpMockEventsDuringStartup;

        synchronized (this) {

            if (pumpMockEvents)
                placeMockEventInQueue(zroot, flags);

            setWatch(zroot, flags);

            addedWatch(zroot, flags);

        }
        
        pumpMockEvents = false;
        
    }

    /**
     * Note: Synchronized for mutex with {@link #cancel()}.
     */
    synchronized 
    public void process(final WatchedEvent event) {

        if(cancelled) {
            
            // ignore events once cancelled.
            
            return;
            
        }
        
        if(log.isInfoEnabled())
            log.info(event.toString());

        /*
         * Put ALL events in the queue.
         * 
         * Note: This does NOT mean that the application will see all state
         * changes for watched znodes. Zookeeper DOES NOT provide that
         * guarantee. The application CAN miss events between the time that a
         * state change triggers a WatchedEvent and the time that the
         * application handles the event and resets the watch.
         */

        queue.add(event);

        switch(event.getState()) {
        case Disconnected:
            return;
        default:
            break;
        }
        
        final String path = event.getPath();
        
        switch (event.getType()) {
        
        case NodeCreated:
            
            /*
             * We always reset the watch when we see a NodeCreated event for a
             * znode that we are already watching.
             * 
             * This event type can occur for the root of the watched hierarchy
             * since we set the watch regardless of the existence of the zroot.
             * 
             * If the event occurs for a different znode then it may have been
             * deleted and re-created and we may have missed the delete event.
             */
        
            try {
                zookeeper.exists(path, this);
            } catch (KeeperException e1) {
                log.error("path=" + path, e1);
            } catch (InterruptedException e1) {
                if (log.isInfoEnabled())
                    log.info("path=" + path);
            }

            return;
            
        case NodeDeleted:
            
            /*
             * A watched znode was deleted. Unless this is the zroot, we will
             * remove our watch on the znode (the expectation is that the watch
             * will be re-established if the child is re-created since we will
             * notice the NodeChildrenChanged event).
             */
            
            if (zroot.equals(path)) {

                try {
                    zookeeper.exists(path, this);
                } catch (KeeperException e1) {
                    log.error("path=" + path, e1);
                } catch (InterruptedException e1) {
                    if (log.isInfoEnabled())
                        log.info("path=" + path);
                }

            } else {

                watched.remove(path);
                
                removedWatch(path);

            }

            return;

        case NodeDataChanged:

            try {
                zookeeper.getData(path, this, new Stat());
            } catch (KeeperException e1) {
                log.error("path=" + path, e1);
            } catch (InterruptedException e1) {
                if (log.isInfoEnabled())
                    log.info("path=" + path);
            }

            return;
            
        case NodeChildrenChanged:
            /*
             * Get the children (and reset our watch on the path).
             */
            try {
                acceptChildren(path, zookeeper.getChildren(event.getPath(),
                        this));
            } catch (KeeperException e) {
                log.error(this, e);
            } catch (InterruptedException e1) {
                if (log.isInfoEnabled())
                    log.info("path=" + path);
            }
            return;
        default:
            return;
        }

    }

    /**
     * Sets watches for the path identified by the flags. If the
     * {@link #CHILDREN} flag is set, then watches are set recursively for any
     * child for which {@link #watch(String, String)} returns non-zero.
     * 
     * @param path
     * @param flags
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected void setWatch(final String path, final int flags)
            throws KeeperException, InterruptedException {

        if (log.isInfoEnabled())
            log.info("zpath=" + path + ", flags=" + flagString(flags));

        watched.put(path, flags);

        if ((flags & EXISTS) != 0) {

            zookeeper.exists(path, this);

        }

        if ((flags & DATA) != 0) {

            zookeeper.getData(path, this, new Stat());

        }

        if ((flags & CHILDREN) != 0) {

            try {
                
                acceptChildren(path, zookeeper.getChildren(path, this));
                
            } catch(NoNodeException ex) {
                
                /*
                 * If there are no children now, then that is Ok. If they are
                 * created later then we will notice it if EXISTS was specified
                 * and we will handle them then.
                 */
                
                if(log.isInfoEnabled())
                    log.info("No children: "+path);
                
            }

        }

    }

    /**
     * Return <code>true</code> if the path is being watched.
     * 
     * @param path
     * 
     * @return
     */
    synchronized public boolean isWatched(final String path) {

        if (path == null)
            throw new IllegalArgumentException();

        return watched.containsKey(path);

    }

    /**
     * Return the flags used to watch the path.
     * 
     * @param path The path.
     * 
     * @return The flags -or- {@link HierarchicalZNodeWatcherFlags#NONE} if the
     *         path is not watched.
     */
    synchronized public int getFlags(final String path) {

        if (path == null)
            throw new IllegalArgumentException();

        final Integer flags = watched.get(path);

        if (flags == null){ 
         
            if (log.isDebugEnabled())
                log.debug("path=" + path + " : Not watched");

            return NONE;

        }

        if (log.isDebugEnabled())
            log.debug("path=" + path + " : " + flagString(flags));

        return flags;

    }

    /**
     * Returns a snapshot of the set of watched nodes.
     * 
     * @return The zpath to all nodes that are being watched.
     */
    synchronized public String[] getWatchedNodes() {
        
        return watched.keySet().toArray(new String[0]);
        
    }
    
    /**
     * Clears watches for the path identified by the flags, but does NOT
     * recursively clear watches for the children even if the {@link #CHILDREN}
     * flag is set.
     * 
     * @param path
     * @param flags
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void clearWatch(final String path, final int flags)
            throws KeeperException, InterruptedException {

        if (log.isInfoEnabled())
            log.info("zpath=" + path + ", flags=" + flagString(flags));

        if ((flags & EXISTS) != 0) {

            zookeeper.exists(path, false);

        }

        if ((flags & DATA) != 0) {

            zookeeper.getData(path, false, new Stat());

        }

        if ((flags & CHILDREN) != 0) {

            zookeeper.getChildren(path, false);

        }
        
        removedWatch(path);

    }

    /**
     * Examines the list of children. If the child is not being watched and
     * {@link #watch(String, String)} returns non-zero, then sets the
     * appropriate watches.
     * <p>
     * Note: This can cause recursion through the znode hierarchy. That
     * recursion will occur in the zookeeper event thread. Therefor it is
     * important to focus recursion only on the znodes that are truely of
     * interest.
     * 
     * @param path
     * @param children
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void acceptChildren(final String path, final List<String> children)
            throws KeeperException, InterruptedException {

        for (String child : children) {

            final String zpath = path + "/" + child;

            if (isWatched(zpath))
                continue;

            final int flags = watch(path, child);

            if (log.isInfoEnabled())
                log.info("watch? " + zpath + " : flags=" + flagString(flags));

            if (flags != 0) {

                if(pumpMockEvents)
                    placeMockEventInQueue(zpath, flags);
                
                setWatch(zpath, flags);
                
                addedWatch(zpath, flags);
                
            }

        }

    }

    /**
     * Notification method is invoked each time we add a znode to the set of
     * znodes that we are watching. <strong>This method is invoked in the
     * zookeeper event thread.</strong>
     * 
     * @param zpath
     *            The path to the znode.
     * @param flags
     *            The flags that will be used to watch the znode.
     */
    protected void addedWatch(String zpath, int flags) {

    }

    /**
     * This is used pump mock events into the {@link #queue} during startup.
     * 
     * @param path
     * @param flags
     */
    protected void placeMockEventInQueue(final String path, final int flags) {

        if (!pumpMockEvents) {

            /*
             * Note: If we are receiving real events then we don't want to do
             * this. It is just for producing fake events during startup.
             */

            throw new IllegalStateException();
            
        }
        
        if(log.isInfoEnabled())
            log.info("path=" + path + ", flags=" + flagString(flags));

        if ((flags & EXISTS) != 0) {

            queue.add(new WatchedEvent(Event.EventType.NodeCreated,
                    Event.KeeperState.Unknown, path));

        }

        if ((flags & DATA) != 0) {

            queue.add(new WatchedEvent(Event.EventType.NodeDataChanged,
                    Event.KeeperState.Unknown, path));

        }

        if ((flags & CHILDREN) != 0) {

            queue.add(new MockWatchedEvent(Event.EventType.NodeChildrenChanged,
                    Event.KeeperState.Unknown, path));

        }

    }

    /**
     * A mock {@link WatchedEvent} used to pump mock events into the queue
     * during a startup scan.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class MockWatchedEvent extends WatchedEvent {

        /**
         * @param eventType
         * @param keeperState
         * @param path
         */
        public MockWatchedEvent(EventType eventType, KeeperState keeperState,
                String path) {

            super(eventType, keeperState, path);
            
        }
        
    }
    
    /**
     * Notification method is invoked each time we remove a znode from the set
     * of znodes that we are watching. <strong>This method is invoked in the
     * zookeeper event thread.</strong>
     * 
     * @param zpath
     *            The path to the znode.
     */
    protected void removedWatch(String zpath) {
        
    }
    
    /**
     * Return flags indicating whether the child should be watched.
     * <p>
     * <strong>Applications SHOULD use this method to restrict the growth of the
     * watched znode hierarchy to ONLY those znodes of interest</strong>.
     * Failure to do this will cause processing in the zookeeper event thread
     * which grows with the #of znodes spanned by the watched hierarchy and can
     * seriously impact the responsiveness of the zookeeper event thread.
     * 
     * @param parent
     *            The zpath to the parent.
     * @param child
     *            The child znode.
     * 
     * @return The watches to establish for the child -or- {@link #NONE} to not
     *         watch the child.
     */
    abstract protected int watch(final String parent, final String child);
    
    /**
     * Cancel all watches. This clears all watches, including on the
     * {@link #zroot}, so that zookeeper will no longer deliver
     * {@link WatchedEvent}s to this class. The {@link #queue} is also cleared
     * and a flag is set such that such that subsequent events are ignored.
     * <p>
     * Note: This is <code>synchronized</code> so that it is mutex with
     * {@link #process(WatchedEvent)}.
     * 
     * @throws InterruptedException
     */
    synchronized 
    public void cancel() throws InterruptedException {

        /*
         * Note: Once we set this flag we will not propagate any more events to
         * the application. If we are unable to clear some watches then those
         * events will just be discarded if they arrive.
         * 
         * Note: This allows us to safely ignore various errors from zookeeper
         * below.
         */
        cancelled = true;
        
        if(log.isInfoEnabled())
            log.info("Cancelling watches: #watched="+watched.size());
        
        final Iterator<Map.Entry<String, Integer>> itr = watched.entrySet()
                .iterator();
        
        while(itr.hasNext()) {

            final Map.Entry<String, Integer> entry = itr.next();
            
            final String path = entry.getKey();
            
            final int flags = entry.getValue();

            try {

                clearWatch(path, flags);

            } catch (ConnectionLossException e) {
            
                /*
                 * We can ignore these errors due to the cancelled flag above.
                 * The errors are logged at a low level because a modestly large
                 * number of such exceptions will occur if the application
                 * cancels this watcher it is NOT connected to zookeeper and
                 */
                
                if (log.isInfoEnabled())
                    log.info("path=" + path + " : " + e);

            } catch (SessionExpiredException e) {

                /*
                 * We can ignore these errors due to the cancelled flag above.
                 * The errors are logged at a low level because a modestly large
                 * number of such exceptions will occur if the application
                 * cancels this watcher it is NOT connected to zookeeper and
                 */
                
                if (log.isInfoEnabled())
                    log.info("path=" + path + " : " + e);

            } catch (KeeperException e) {

                log.error("path=" + path + " : " + e);

            }

            itr.remove();

        }

    }
    
    public String toString() {

        return getClass().getName() + "{zroot=" + zroot + ", watchedSize="
                + getWatchedSize() + "}";

    }

    public int getWatchedSize() {

        /*
         * Note: Not synchronized since impl returns a member field.
         */

        // synchronized (this) {
        return watched.size();

        // }

    }

    /**
     * Externalizes the flags as a list of symbolic constants.
     * 
     * @param flags
     *            The {@link IRangeQuery} flags.
     * 
     * @return The list of symbolic constants.
     */
    public static String flagString(final int flags) {
        
        final StringBuilder sb = new StringBuilder();
        
        // #of flags that are turned on.
        int onCount = 0;
        
        sb.append("[");

        if ((flags & EXISTS) != 0) {
            
            if (onCount++ > 0)
                sb.append(",");
            
            sb.append("EXISTS");
            
        }
        
        if ((flags & DATA) != 0) {
            
            if (onCount++ > 0)
                sb.append(",");
            
            sb.append("DATA");
            
        }
        
        if ((flags & CHILDREN) != 0) {
            
            if (onCount++ > 0)
                sb.append(",");
            
            sb.append("CHILDREN");
            
        }
        
        sb.append("]");
        
        return sb.toString();
        
    }
    
}
