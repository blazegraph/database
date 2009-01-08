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
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
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
 * This class only watches the previous child in the queue (if any). If the lock
 * node is destroyed, then the watcher will throw an exception back to the
 * caller rather than returning normally. However, destroying the lock node will
 * not cause the lock to be persistently destroyed if the conditions still exist
 * under which processes will contend for that lock as processes will simply
 * re-create the lock node and re-populate the queue.
 * <p>
 * While a queue may seem excessive for one time operations, it provides
 * failover if the process holding the lock dies.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
     * @todo could watch self when in the first position in the queue to allow
     *       the process to be responsive if the its lock was cancelled by
     *       deleting its znode from the head of the queue.
     */
    private volatile String priorChildZNode = null;
    
    protected void toString(StringBuilder sb) {

        super.toString(sb);
        
        sb.append(", zchild=" + zchild);
        
        sb.append(", priorChild=" + priorChildZNode);
        
        sb.append(", cancelled=" + cancelled);
        
    }
    
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
    protected boolean isConditionSatisified(WatchedEvent event)
            throws KeeperException, InterruptedException {

        // @todo probably could be optimized.
        return isConditionSatisified();

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
     * 
     * @todo Consider this: A new zchild will be generated if this is
     *       <code>null</code> or if the queue is inspected and is found to
     *       not contain this znode. This could handle both the initial create
     *       of the zchild and the re-create if someone stomps on the lock node
     *       or the zchild). However, it would make it impossible to terminate
     *       the contention of the process for the lock simply be terminating
     *       deleting either the zchild or the lock node itself.
     */
    @Override
    protected boolean isConditionSatisified() throws KeeperException,
            InterruptedException {

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

            log.info("queue: "+children); // @todo remove
            
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

            log.warn("Lock cancelled (child not in queue): " + this);
            
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
            log.info("Process in queue: pos=" + pos + ", " + this);

        return false;

    }

    /**
     * Return an object that may be used to acquire a distributed synchronous
     * lock.
     * 
     * @param zookeeper
     * @param zpath
     * @param acl
     * 
     * @return
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static ZLock getLock(final ZooKeeper zookeeper, final String zpath,
            final List<ACL> acl) throws KeeperException, InterruptedException {

        try {

            /*
             * Ensure that the lock node exists.
             */

            zookeeper.create(zpath, new byte[0], acl, CreateMode.PERSISTENT);

        } catch (NodeExistsException ex) {

            // ignore - the lock node exists.

        }

        return new ZLockImpl(zookeeper, zpath);

    }

    /**
     * {@link ZLock} implementation class. The lock is not reentract (you can
     * not re-acquire it by requesting it while already held in the same or a
     * different thread). Any thread may release the lock, not just the one that
     * acquired it.
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
         * The zpath of the lock node.
         */
        public String getZPath() {
            
            return zpath;
            
        }

        /**
         * The znode (not zpath) of the child and <code>null</code> iff the
         * child does not hold the lock.
         */
        public String getChild() {

            lock.lock();
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
         */
        protected ZLockImpl(final ZooKeeper zookeeper, final String zpath) {
            
            if (zookeeper == null)
                throw new IllegalArgumentException();
            
            if (zpath == null)
                throw new IllegalArgumentException();
            
            this.zookeeper = zookeeper;
            
            this.zpath = zpath;
            
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

            lock.lock();
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
                final boolean ret = watcher.isConditionSatisified()
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

            lock.lock();
            try {
                
                final long begin = System.nanoTime();

                long nanos = unit.toNanos(timeout);

                /*
                 * There is no data in the ephemeral znode representing the
                 * process contending for the lock. Therefore no one needs to
                 * "read" this child znode. Since you can not delete the parent
                 * until the children have been deleted, the ACL here must not
                 * prevent the deletion of the node by another process.
                 * 
                 * @todo what is a suitable ACL?
                 */ 
                final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
                
                // create a child contending for that lock.
                final String s = zookeeper.create(zpath + "/child",
                        new byte[0], acl, CreateMode.EPHEMERAL_SEQUENTIAL);

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

                    try {

                        /*
                         * destroy the child (release lock if leader and
                         * drop out of queue contending for that lock).
                         */

                        zookeeper
                                .delete(zpath + "/child", -1/* version */);

                    } catch (Throwable t2) {

                        // log warning and ignore.
                        log.warn(t2, t2);

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

        public void unlock() throws KeeperException, InterruptedException {

            lock.lock();
            try {

                if (watcher == null) {

                    throw new IllegalStateException("Lock not held.");

                }
                
                synchronized(watcher) {
                    
//                    // clear watch.
//                    watcher.clearWatch();

                    /*
                     * Note: The watcher should no have any watches while it
                     * holds the lock so we don't have to clear the watch.
                     */
                    
                    final String zchild = watcher.zchild;

                    // delete the child (releases the lock).
                    zookeeper.delete(zpath + "/" + zchild, -1/* version */);

                    if (INFO)
                        log.info("released lock: "+watcher);

                }
                
            } finally {

                watcher = null;
                
                lock.unlock();
                
            }

        }

    }

}
