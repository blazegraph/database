package com.bigdata.zookeeper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.ACL;

/**
 * Watcher supporting distributed synchronous lock pattern.
 * <p>
 * A lock is just a node with {@link CreateMode#EPHEMERAL_SEQUENTIAL} children
 * modeling a queue of processes obeying a contract on the conditions under
 * which a process may proceed. The process whose child is ordered first in the
 * queue holds the lock. To release the lock the process simply deletes its node
 * in the queue. Each process seeking a lock creates a new child that watches
 * the previous child in the ordered queue. When it receives a
 * {@link WatchedEvent} (NodeDeleted) for that child, it has reached the head of
 * the queue.
 * <p>
 * Any processes wishing to contend for the lock must ensure the creation of the
 * lock node. This is a {@link CreateMode#PERSISTENT} znode. The SEQUENTIAL flag
 * COULD be used if processes have another means to communicate the identity of
 * the lock node. Lacking such a mechanism, the lock node must have a zpath
 * which can be computed by any process seeking to contend for the lock. That is
 * normally accomplished by naming the lock node for a resource with a well
 * known identity.
 * <p>
 * This class only watches the previous child in the queue (if any). Zookeeper
 * does not permit you to delete the parent without deleting the children, so
 * you will always the queue purged of children before the lock node itself is
 * deleted. If the child deleted from the queue, the next child in the queue
 * will be granted the lock.
 * <p>
 * Destroying the lock node will not cause the lock to be persistently destroyed
 * if the conditions still exist under which processes will contend for that
 * lock as some process will simply re-create the lock node and the queue will
 * become re-populated over time.
 * <p>
 * While a queue may seem excessive for one time operations, it provides
 * failover if the process holding the lock dies.
 * 
 * @see #getLock(ZooKeeper, String, List)
 * @see ZLockImpl
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @deprecated Replaced by {@link ZLockImpl}
 */
public class ZNodeLockWatcher extends AbstractZNodeConditionWatcher {

    final static protected Logger log = Logger.getLogger(ZNodeLockWatcher.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    private volatile boolean cancelled = false;
    
    protected boolean isCancelled() {
        
        return cancelled;
        
    }
    
    /**
     * The znode (not zpath) of the ephemeral child representing the process in
     * the queue contending for the lock.
     */
    private final String zchild;

    /**
     * This is the znode of the child that is lexically ordered before us in the
     * queue. This is the only znode that we actually watch. When we are in the
     * first position in the queue we don't watch anything.
     * 
     * @todo Could watch self when in the first position in the queue to allow
     *       the process to notice if the its lock was cancelled by deleting its
     *       znode from the head of the queue. However, we would need a means to
     *       interrupt that process. E.g., by noting the Thread that invoked
     *       lock() and interrupting it.
     */
    private volatile String priorChildZNode = null;
    
    protected void toString(StringBuilder sb) {

        super.toString(sb);
        
        sb.append(", zchild=" + zchild);
        
        sb.append(", priorChild=" + priorChildZNode);
        
        sb.append(", cancelled=" + cancelled);
        
    }
    
    /**
     * The suffix for a marker that is a <strong>sibling</strong> of the lock
     * node. The presence of this marker indicates that the queue is being
     * destroyed. The {@link #isConditionSatisified()} logic will not allow a
     * lock to be granted if this marker is found. Likewise, new children are
     * not permitted into the queue when this marker is present.
     * 
     * Note: If you are monitoring a znode whose children a lock nodes, then a
     * child ending with this string IS NOT a lock node!
     */
    public static final transient String INVALID = "_INVALID";
    
    protected ZNodeLockWatcher(final ZooKeeper zookeeper, final String zpath,
            final String zchild) {

        super(zookeeper, zpath);

        if (zchild == null)
            throw new IllegalArgumentException();

        this.zchild = zchild;

    }

    @Override
    protected void clearWatch() throws KeeperException, InterruptedException {

        // this is the only thing that we are watching.
        String s = priorChildZNode;
        if (s != null) {

            if(INFO)
                log.info("Cancelling watch on " + s + " : " + this);
            
            zookeeper.getChildren(zpath + "/" + s, false/* watch */);
            
        }

    }

    @Override
    protected boolean isConditionSatisfied(final WatchedEvent event)
            throws KeeperException, InterruptedException {

        if(cancelled) {
            
            /*
             * If an event arrives after the lock was cancelled then we make
             * sure that the znode representing this process is deleted. This
             * handles the case of a transient disconnect where the process is
             * reconnected to zookeeper before it times out. This is necessary
             * in order to release the lock which otherwise would be held
             * forever by this process.
             */
            
            switch(event.getState()) {
            case NoSyncConnected: 
            case SyncConnected:
                
                try {

                    zookeeper.delete(zpath + "/" + zchild, -1);
                    
                    if (INFO)
                        log.info("released lock: " + this);

                } catch (NoNodeException ex) {
                    
                    // ignore.
                    
                }
                
            }
            
        }
        
        return isConditionSatisfied();

    }

    /**
     * Obtains the children of the lock node. If the process is the first child,
     * then it holds the lock and we return <code>true</code>. Otherwise, set
     * a watch on the prior child in the queue.
     * <p>
     * If the lock node has been deleted then set the {@link #cancelled} flag
     * (the entire queue was destroyed, so we cancel any process contending for
     * a lock).
     * <p>
     * If the zchild has been deleted then set the {@link #cancelled} flag (this
     * let's you break the lock by deleting the child).
     * <p>
     * If the client is not connected, then an exception will be thrown and the
     * watch will resume with the next {@link WatchedEvent}.
     * 
     * @return Return <code>true</code> iff the process holds the lock
     */
    @Override
    protected boolean isConditionSatisfied() throws KeeperException,
            InterruptedException {

        if(cancelled) {
            
            // ignore any events once the lock was cancelled.
            
            return true;
            
        }
        
        if (zookeeper.exists(zpath + INVALID, false) != null) {

            /*
             * The lock node has been invalidation as part of the protocol to
             * destroy the lock node together with its queue. Once this is done,
             * NO ONE will be granted the lock.
             */

            cancelled = true;

            priorChildZNode = null;

            log.warn("Watch cancelled (lock node was invalidated): " + this);

            return false;

        }
        
        final List<String> children;
        try {

            /*
             * Get children, but DO NOT set the watch (we don't watch the lock
             * node itself, just the prior child).
             */
            final String[] a = zookeeper.getChildren(zpath, false/* watch */)
                    .toArray(new String[0]);

            // sort
            Arrays.sort(a);

            // wrap as list again.
            children = Arrays.asList(a);

//            if(INFO)
//                log.info("queue: "+children);
            
        } catch (NoNodeException ex) {
            
            /*
             * Someone deleted the lock node, so all outstanding requests will
             * be cancelled.
             */
            
            cancelled = true;

            priorChildZNode = null;

            log.warn("Lock cancelled (lock node destroyed): " + this);
            
            return true;
            
        }

        final int pos = children.indexOf(zchild);

        if (pos == -1) {

            // since [zchild] is not in the list.
            cancelled = true;

            priorChildZNode = null;

            log.warn("Watch cancelled (child not in queue): " + this);
            
            return true;
            
        }

        if (pos == 0) {
            
            if (INFO)
                log.info("Process holds the lock: " + this);
            
            priorChildZNode = null;
            
            return true;
            
        }

        // remember since we need to it cancel our watch.
        priorChildZNode = children.get(pos - 1);
        
        // set watch on the prior child in the list.
        zookeeper.exists(zpath + "/" + priorChildZNode, this);

        if (INFO)
            log.info("Process in queue: pos=" + pos + " out of "
                    + children.size() + " children, " + this
                    + (DEBUG ? " : children=" + children.toString() :""));

        return false;

    }

    /**
     * Return an object that may be used to acquire a distributed synchronous
     * lock.
     * <p>
     * This method will create a new lock node if the znode identified by the
     * zpath does not exist. However, it will reject the request if the lock
     * node has been invalidated pending the destruction of the lock node and
     * its queue. In design patterns where the lock node may be destroyed, this
     * places the responsibility on the caller to verify that the lock node is
     * pre-existing if they wish to avoid re-creating a lock node which has been
     * destroyed.
     * 
     * @param zookeeper
     * @param zpath
     *            The path identifying the lock node.
     * @param acl
     * 
     * @return
     * 
     * @throws KeeperException
     * @throws InterruptedException
     * @throws ZLockNodeInvalidatedException
     *             if the lock node has been invalidated but not yet destroyed.
     * 
     * @deprecated Replaced by {@link com.bigdata.zookeeper.ZLockImpl}
     */
    public static ZLockImpl getLock(final ZooKeeper zookeeper,
            final String zpath, final List<ACL> acl) throws KeeperException,
            InterruptedException {

        if (zookeeper.exists(zpath + INVALID, false) != null) {

            /*
             * End of the competition. Either someone created the service or
             * someone destroyed the lock node.
             */

            throw new ZLockNodeInvalidatedException(zpath);

        }

        try {

            /*
             * Ensure that the lock node exists.
             */
            
            zookeeper.create(zpath, new byte[0], acl, CreateMode.PERSISTENT);

        } catch (NodeExistsException ex) {

            // ignore - the lock node exists.

        }

        return new ZLockImpl(zookeeper, zpath, acl);

    }

    /**
     * {@link ZLock} implementation class. The lock is realized as an EPHEMERAL
     * SEQUENTIAL child of the lock node. If the child is in the first position
     * of the lexically sorted children then it holds the lock.
     * <p>
     * Note: Assuming it has the correct ACL, any thread in any process MAY
     * release the lock, NOT just the one that acquired it. This is done by
     * deleting the ephemeral znode corresponding to the process holding the
     * lock.
     * 
     * @todo The lock is not reentrant (you can not re-acquire it by requesting
     *       it while already held in the same or a different thread).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class ZLockImpl implements ZLock {

        final protected ZooKeeper zookeeper;
        
        /**
         * The zpath of the lock node (the parent node whose ephemeral
         * sequential children represent the queue of processes contending for
         * the lock).
         */
        final protected String zpath;

        final protected List<ACL> acl;
        
        /**
         * Used to control access to our internal state (this is NOT the
         * distributed synchronous lock!).
         */
        private final ReentrantLock lock = new ReentrantLock();

        /**
         * The watcher that manages the participating of the process in the
         * contention for the lock. This is set by {@link #lock(long, TimeUnit)}.
         * It is used by {@link #unlock()} to release the lock.
         */
        private volatile ZNodeLockWatcher watcher = null;
        
        /**
         * Non-blocking representation of lock state.
         */
        public String toString() {
            
            return "ZLock{zpath=" + zpath
                    + (watcher == null ? "" : watcher.toString()) + "}";
            
        }
        
        /**
         * The zpath of the lock node.
         */
        public String getZPath() {
            
            return zpath;
            
        }

        /**
         * The znode (not zpath) of the child and <code>null</code> iff the
         * child does not hold the lock.
         * 
         * @throws InterruptedException
         */
        public String getChild() throws InterruptedException {

            lock.lockInterruptibly();
            try {
                
                if (watcher == null)
                    return null;

                return watcher.zchild;
                
            } finally {

                lock.unlock();

            }

        }

        /**
         * 
         * @param zookeeper
         * @param zpath
         *            The zpath of the lock node.
         * @param acl
         */
        protected ZLockImpl(final ZooKeeper zookeeper, final String zpath,
                final List<ACL> acl) {

            if (zookeeper == null)
                throw new IllegalArgumentException();

            if (zpath == null)
                throw new IllegalArgumentException();

            if (acl == null)
                throw new IllegalArgumentException();

            this.zookeeper = zookeeper;

            this.zpath = zpath;

            this.acl = acl;
            
        }
        
        public void lock() throws KeeperException, InterruptedException {

            try {

                lock(Long.MAX_VALUE, TimeUnit.SECONDS);

            } catch (TimeoutException e) {

                /*
                 * Note: should not be thrown since timeout is
                 * (near)infinite.
                 */

                throw new AssertionError(e);

            }

        }

        public boolean isLockHeld() throws KeeperException,
                InterruptedException {

            if(DEBUG)
                log.debug(this.toString());
            
            lock.lockInterruptibly();
            try {

                if (watcher == null) {

                    if(INFO) 
                        log.info("Not contending for a lock (no watcher): "+zpath);
                    
                    return false;
                    
                }

                /*
                 * Note: isCancelled() must be tested after
                 * isCondititionSatisified() since it is set as a side effect.
                 */
                final boolean ret = watcher.isConditionSatisfied()
                        && !watcher.isCancelled();
                
                if (INFO)
                    log.info("isLockHeld=" + ret + ", zpath=" + zpath
                            + ", zchild=" + watcher.zchild);
                
                return ret;

            } finally {

                lock.unlock();
                
            }

        }

        public void lock(final long timeout, final TimeUnit unit)
                throws KeeperException, InterruptedException, TimeoutException {

            if(DEBUG)
                log.debug(this.toString());
            
            lock.lockInterruptibly();
            try {
                
                final long begin = System.nanoTime();

                long nanos = unit.toNanos(timeout);

                /*
                 * If there is a marker node indicating that the lock has been
                 * invalidated then we DO NOT enter a child into the queue.
                 */
                if (zookeeper.exists(zpath + INVALID, false) != null) {

                    throw new ZLockNodeInvalidatedException(zpath);
                    
                }
                
                /*
                 * There is no data in the ephemeral znode representing the
                 * process contending for the lock. Therefore no one needs to
                 * "read" this child znode. Since you can not delete the parent
                 * until the children have been deleted, the ACL here must not
                 * prevent the deletion of the node by another process.
                 */ 
                
                // create a child contending for that lock.
                final String s = zookeeper.create(zpath + "/lock",
                        new byte[0], acl, CreateMode.EPHEMERAL_SEQUENTIAL);

                // last path component is the znode of the child.
                final String zchild = s.substring(s.lastIndexOf('/') + 1);

                if (INFO)
                    log.info("Seeking lock: zpath=" + zpath + ", zchild="
                            + zchild);
                
                final ZNodeLockWatcher watcher = new ZNodeLockWatcher(
                        zookeeper, zpath, zchild);

                nanos -= (System.nanoTime() - begin);

                try {

                    watcher.awaitCondition(nanos, TimeUnit.NANOSECONDS);

                    // set now that the process holds the lock.
                    this.watcher = watcher;
                    
                    if(INFO)
                        log.info("lock acquired: zpath=" + zpath + ", zchild="
                            + zchild);
                        
                    return;

                } catch (Throwable t) {

                    /*
                     * Destroy the child (release lock if we have it and drop
                     * out of queue contending for that lock).
                     */

                    try {

                        zookeeper
                                .delete(zpath + "/" + zchild, -1/* version */);

                    } catch (NoNodeException ex) {

                        // ignore.

                    } catch (Throwable t2) {

                        // log warning and ignore.
                        log.warn("Problem deleting our child: zpath=" + zpath
                                + ", zchild=" + zchild, t2);

                    }

                    if (t instanceof InterruptedException)
                        throw (InterruptedException) t;

                    if (t instanceof TimeoutException)
                        throw (TimeoutException) t;

                    if (t instanceof KeeperException)
                        throw (KeeperException) t;

                    throw new RuntimeException(t);

                }

            } finally {

                lock.unlock();
                
            }

        }

        /**
         * Note: In the case where the lock was stolen by deleting the zchild
         * node this method logs a warning but does not throw an exception.
         */
        public void unlock() throws KeeperException, InterruptedException {

            if(DEBUG)
                log.debug(this.toString());
            
            lock.lockInterruptibly();
            try {

                if(DEBUG)
                    log.debug(this.toString());

                if (watcher == null) {

                    throw new IllegalStateException("Lock not held.");

                }
                
                synchronized(watcher) {
                    
                    /*
                     * We set this flag first so that we will be able to discard
                     * the lock if there is a transient disconnect.
                     */
                    
                    watcher.cancelled = true;
                    
                    if(DEBUG)
                        log.debug(this.toString());
                    
//                    // clear watch.
//                    watcher.clearWatch();

                    /*
                     * Note: The watcher should no have any watches while it
                     * holds the lock so we don't have to clear the watch.
                     */
                    
                    final String zchild = watcher.zchild;

                    // delete the child (releases the lock).
                    try {

                        zookeeper
                                .delete(zpath + "/" + zchild, -1/* version */);
                        
                        if (INFO)
                            log.info("released lock: " + watcher);

                    } catch (SessionExpiredException ex) {

                        /*
                         * See notes on ConnectionLossException below.
                         */

                        log.warn("Not connected: zpath=" + zpath + ", child="
                                + zchild + " : " + ex);

                    } catch (ConnectionLossException ex) {

                        /*
                         * Note: We MUST NOT ignore a ConnectionLossException if
                         * unless this process is being killed. If the process
                         * remains alive but it was unable to delete its child
                         * because of a temporary disconnect AND it reconnects
                         * before zookeeper times out the client then THE LOCK
                         * WILL NOT BE RELEASED.
                         * 
                         * This is handled by setting [cancelled := true] above
                         * and by explicitly deleting the child znode if the
                         * lock has been cancelled in the Watcher.
                         */

                        log.warn("Not connected: zpath=" + zpath + ", child="
                                + zchild + " : " + ex);
                        
                    } catch (NoNodeException ex) {
                        
                        /*
                         * Someone has stomped on the child, so the process does
                         * not hold the lock anymore.
                         */
                        
                        log.warn("Child already deleted: zpath=" + zpath
                                + ", child=" + zchild);
                        
                    }

                }
                
            } finally {

                watcher = null;
                
                lock.unlock();
                
            }

            if(DEBUG)
                log.debug(this.toString());
            
        }

        /**
         * Creates a marker node (a sibling of the lock node) to prevent new
         * children from being added to the queue and then deletes all children
         * in the queue in reverse lexical order so as to not trigger cascades
         * of watchers and finally deletes the lock node itself and then the
         * marker node.
         * 
         * @todo write unit tests for this.
         * 
         * @todo verify {@link #unlock()} succeeds after {@link #destroyLock()}
         * 
         * @todo consider allowing even if you do not hold the lock.
         * 
         * @todo consider what happens if there is a transient disconnect.
         */
        public void destroyLock() throws KeeperException, InterruptedException {

            lock.lockInterruptibly();
            try {

                if (!isLockHeld())
                    throw new IllegalStateException("Lock not held.");

                /*
                 * Create a marker prevent any new child from entering the queue
                 * for the lock node. This agreement is necessary in order for
                 * us to have a guarentee that the queue will be empty when we
                 * try to delete the lock node. Without this protocol any
                 * concurrent request for the lock could cause the queue to be
                 * non-empty when our delete request for the lock node is
                 * processed by zookeeper.
                 * 
                 * Note: The marker CAN NOT be a child since we can't make that
                 * atomic. We could locate it somewhere completely different in
                 * the tree, but it is easy enough to make it a sibling
                 * 
                 * locknode_INVALID
                 * 
                 * The marker is ephemeral in case this process dies while
                 * trying to destroy the queue.
                 */
                zookeeper.create(zpath + INVALID, new byte[0], acl,
                        CreateMode.EPHEMERAL);
                
                List<String> children;
                
                // until empty.
                while (!(children = zookeeper.getChildren(zpath, false)).isEmpty()) {

                    final String[] a = children.toArray(new String[] {});
                    
                    // sort
                    Arrays.sort(a);

                    // process in reverse order to avoid cascades of watchers.
                    for (int i = a.length - 1; i >= 0; i--) {

                        final String child = a[i];

                        zookeeper.delete(zpath + "/" + child, -1/* version */);
                        
                    }
                    
                }

                // delete the lock node.
                zookeeper.delete(zpath, -1/* version */);

                // delete the marker node that was used to invalidate the lock node.
                zookeeper.delete(zpath + INVALID, -1/* version */);
                
            } finally {
             
                lock.unlock();
                
            }
            
        }

    }
    
}
