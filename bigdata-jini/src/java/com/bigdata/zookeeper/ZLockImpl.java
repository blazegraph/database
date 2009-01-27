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
 * Created on Jan 22, 2009
 */

package com.bigdata.zookeeper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.ACL;

/**
 * {@link ZLock} implementation class. The lock request is realized as an
 * EPHEMERAL SEQUENTIAL child of the lock node. If the lock request znode is in
 * the first position of the lexically sorted children of the lock node then it
 * holds the {@link ZLock}. Some points to note:
 * <ul>
 * <li>{@link ZLock}s are NOT reentrant.</li>
 * <li>{@link ZLock}s DO NOT detect deadlocks.</li>
 * <li>{@link ZLock}s may be broken asynchronously.</li>
 * </ul>
 * 
 * Assuming it has the correct ACL, any thread in any process MAY release the
 * {@link ZLock}, NOT just the one that acquired it. This is done by deleting
 * the EPHERMERAL znode corresponding to the lock request. The {@link ZLock}
 * will also be lost if the {@link ZooKeeper} client is timed out by the
 * zookeeper server ensemble. You can use {@link #isLockHeld()} to determine if
 * the {@link ZLock} has been broken asynchronously.
 * 
 * @see #getLock(ZooKeeper, String, List)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZLockImpl implements ZLock {

    final static protected Logger log = Logger.getLogger(ZLockImpl.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

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

    /**
     * Text of an message generated when an interrupt occurs in the zookeeper
     * event thread.
     * 
     * @see ZLockWatcher#process(WatchedEvent)
     */
    private static final transient String ERR_INTERRUPT_IN_EVENT_THREAD = "Interrupt in event thread";

    /**
     * Factory for a {@link ZLock} which may be used to contend for the a global
     * synchronous lock.
     * 
     * @param zookeeper
     *            The zookeeper client.
     * @param zpath
     *            The path identifying the lock node.
     * @param acl
     *            The ACL to be used.
     * 
     * @return
     * 
     * @throws KeeperException
     * @throws InterruptedException
     * @throws ZLockNodeInvalidatedException
     *             if the lock node has been invalidated but not yet destroyed.
     */
    public static ZLockImpl getLock(final ZooKeeper zookeeper,
            final String zpath, final List<ACL> acl) {

        return new ZLockImpl(zookeeper, zpath, acl);

    }

    final protected ZooKeeper zookeeper;

    /**
     * The zpath of the lock node (the parent node whose ephemeral sequential
     * children represent the queue of processes contending for the lock).
     */
    final protected String zpath;

    final protected List<ACL> acl;

    /**
     * Used to control access to our internal state (this is NOT the distributed
     * synchronous lock!).
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * The watcher that manages the participating of the process in the
     * contention for the lock. This is set by {@link #lock(long, TimeUnit)}.
     * It is used by {@link #unlock()} to release the lock.
     */
    private volatile ZLockWatcher watcher = null;

    /**
     * Non-blocking representation of lock state (does not tell you if the lock
     * is held).
     */
    public String toString() {

        // hold ref since [watcher] is volatile.
        final ZLockWatcher w = watcher;

        return "ZLock{ zpath=" + zpath + (w == null ? "" : w.toString()) + "}";

    }

    /**
     * The zpath of the lock node.
     */
    final public String getZPath() {

        return zpath;

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

    /**
     * Validate the lock node. If it has not been invalidated and does not exist
     * then create it.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void validateLockNode() throws InterruptedException,
            KeeperException {

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

    }
    
    /**
     * Inner class provides watcher for the lock. The watcher will continue to
     * receive {@link WatchedEvent}s until the lock node is either invalidated
     * or destroyed or until the watcher has been cancelled and has further
     * satisified itself that the EPHEMERAL znode corresponding to the lock
     * request no longer exists.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class ZLockWatcher implements Watcher {

        /**
         * The znode (not zpath) of the ephemeral child representing the lock
         * request.
         */
        private final String zchild;

        /**
         * This is the znode of the child that is lexically ordered before us in
         * the queue. This is the only znode that we actually watch until the
         * {@link ZLock} is granted. When we are in the first position (holding
         * the {@link ZLock}) we watch ourselves so that we can notice if we
         * loose the {@link ZLock}.
         */
        private volatile String priorChildZNode = null;

        /**
         * {@link Condition} signalled if the zlock becomes held by the caller.
         */
        private final Condition zlock = lock.newCondition();

        /**
         * Flag is set <code>true</code> by {@link #isConditionSatisified()}
         * if the watcher notices that the zlock has been granted (it is never
         * cleared). The watcher will continue to notice events and evaluate
         * {@link #isConditionSatisified()}. If the zlock is lost, then
         * {@link #cancelled} will be set. {@link ZLockImpl#isLockHeld()} tests
         * {@link #cancelled} and will notice that the zlock has been lost.
         */
        private volatile boolean zlockGranted = false;
         
        /**
         * flag is set <code>true</code> if the watch is cancelled or the
         * {@link ZLock} is lost. This is used by {@link #process(WatchedEvent)}
         * to ignore subsequent events.
         */
        private volatile boolean cancelled = false;

        /**
         * Set to <code>true</code> iff we successfully delete the EPHEMERAL
         * zchild representing the lock request.
         */
        private volatile boolean knownDeleted = false;

        /**
         * Set to true anytime we are disconnected from the zookeeper ensemble.
         */
        private volatile boolean disconnected = false;

//        /**
//         * The lock count is incremented when the lock is acquired and
//         * decremented when it is released. It is local to the watcher since it
//         * is invalid if the watcher is cancelled.
//         */
//        private volatile int lockCount = 0;
        
        /**
         * Return a representation of the watcher state.
         * <p>
         * Note: The implementation MUST be safe and non-blocking.
         */
        public String toString() {

            final StringBuilder sb = new StringBuilder();

            sb.append(getClass().getSimpleName());
            sb.append("{ zpath=" + zpath);
            sb.append(", conditionSatisified=" + zlockGranted);
            sb.append(", disconnected=" + disconnected);
            sb.append(", knownDeleted=" + knownDeleted);
            sb.append(", cancelled=" + cancelled);
//            sb.append(", lockCount=" + lockCount);
            sb.append(", zchild=" + zchild);
            sb.append(", priorChild=" + priorChildZNode);
            sb.append("}");

            return sb.toString();

        }

        protected ZLockWatcher(final String zchild) {

            if (zchild == null)
                throw new IllegalArgumentException();

            this.zchild = zchild;

        }

        public void process(final WatchedEvent event) {

            if (INFO)
                log.info(event.toString());

            switch (event.getState()) {
            
            case Disconnected: {
                /*
                 * There is nothing to do until we are reconnected.
                 */
                disconnected = true;
                return;
            }

            case NoSyncConnected:
            case SyncConnected: {
                if (disconnected) {
                    // Reconnect
                    disconnected = false;
                    if(INFO)
                        log.info("Reconnect: "+this);
                }
            } 

            default: {

                /*
                 * Fall through. If we were disconnected and are now reconnected
                 * then we will again try to acquire the zlock. However, if the
                 * EPHEMERAL znode has been deleted (either by a nefarious party
                 * or by zookeeper after our client timed out) the watcher will
                 * be cancelled and the caller will see an InterruptedException.
                 */

                break;

            }

            }// case(event.getState())

            if(disconnected) {
                // not connected.
                return;
            }

            if (cancelled) {
                /*
                 * The watcher has been cancelled so we are no longer seeking
                 * the zlock and we will ignore most events. However, if an
                 * event arrives after the lock was cancelled then we make sure
                 * that the EPHEMERAL zchild representing the lock request is
                 * deleted. This handles the case of a transient disconnect
                 * where the process is reconnected to zookeeper before it times
                 * out. This is necessary in order to release the lock which
                 * otherwise would be held forever by this process.
                 */
                if (!knownDeleted) {
                    try {
                        zookeeper.delete(zpath + "/" + zchild, -1);
                        knownDeleted = true;
                        if (INFO)
                            log.info("released lock: " + this);
                    } catch (NoNodeException ex) {
                        /*
                         * ignore - either deleted by zookeeper on timeout or by
                         * another process.
                         */
                        knownDeleted = true;
                    } catch (KeeperException ex) {
                        log.warn(this, ex);
                    } catch (InterruptedException ex) {
                        /*
                         * The watcher is already cancelled so there is nothing
                         * more that we can do.
                         */
                        log.warn(ERR_INTERRUPT_IN_EVENT_THREAD + " : " + this);
                    }
                } // if(!knownDeleted)
                // return since watcher is cancelled.
                return;
            } // if(cancelled)
            
            /*
             * At this point we have an event in some kind of connected state
             * and the watcher has not been [cancelled]. We will figure out
             * whether or not the zlock has been granted or if the EPHEMERAL
             * znode has been deleted (either by zookeeper on timeout or by
             * someone else).
             */
            try {
                // gain the local lock.
                lock.lockInterruptibly();
            } catch (InterruptedException ex) {
                // interrupt in the event thread.
                cancelled = true; // stop watching; release zlock if held
                log.warn(ERR_INTERRUPT_IN_EVENT_THREAD + " : " + this);
                return;
            }
            try { // with [lock]
                try {
                    if(isConditionSatisified()) {
                        zlockGranted = true;
                        zlock.signal();
                        if(INFO)
                            log.info("ZLock granted.");
                        return;
                    } else if (cancelled) {
                        /*
                         * Either the zlock was lost, lock node was invalidated
                         * or destroyed, or EPHEMERAL lock request znode was
                         * destroyed.
                         * 
                         * Regardless, we signal [zlock]. This will wake up the
                         * caller if they are awaiting the zlock. If they are
                         * not awaiting the zlock and they previously held the
                         * zlock, then they will notice that they have lost the
                         * zlock the next time they test ZLockImpl#isLockHeld().
                         */
                        zlock.signal();
                        if(INFO)
                            log.info("ZLock request cancelled.");
                        return;
                    }
                } catch (KeeperException e) {
                    log.warn(this, e);
                    return;
                } catch (InterruptedException e) {
                    // interrupt in the event thread.
                    cancelled = true; // stop watching; release zlock if held
                    log.warn(ERR_INTERRUPT_IN_EVENT_THREAD + " : " + this);
                    return;
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Wait up to timeout units for the watched znode to be created.
         * <p>
         * An instance of this watcher is set on a <strong>single</strong>
         * znode. The caller then {@link Object#wait()}s on the watcher until
         * the watcher notifies itself. When the caller wakes up it checks the
         * time remaining and whether or not the condition has been satisified.
         * If the timeout has noticeably expired then it returns false. If the
         * condition has been satisified and the timeout has not expired it
         * returns true. Otherwise we continue to wait.
         * <p>
         * The {@link Thread} MUST test {@link #zlockGranted} while holding the
         * lock and before waiting (in case the event has already occurred), and
         * again each time {@link Object#wait()} returns (since wait and friends
         * MAY return spuriously). The watch will be re-established until the
         * timeout has elapsed or the condition has been satisified, at which
         * point the watch is explicitly cleared before returning to the caller.
         * <p>
         * This pattern should be robust in the face of a service disconnect.
         * When a reconnect {@link WatchedEvent} is received, it will test the
         * condition and then reset or clear its watch as necessary.
         * <p>
         * Note: the resolution is millseconds at most.
         * 
         * @param nanos
         *            The timeout in nanoseconds.
         * 
         * @return <code>false</code> if the waiting time detectably elapsed
         *         before return from the method, else <code>true</code>.
         * 
         * @throws TimeoutException
         * @throws InterruptedException
         */
        protected boolean awaitZLockNanos(long nanos)
                throws InterruptedException {

            final long begin = System.nanoTime();

            lock.lockInterruptibly();
            try {

                while ((nanos -= (System.nanoTime() - begin)) > 0
                        && !zlockGranted && !cancelled) {

                    if (DEBUG)
                        log.debug("remaining="
                                + TimeUnit.NANOSECONDS.toMillis(nanos) + "ms");

                    try {

                        if(isConditionSatisified()) {
                        
                            /*
                             * The zlock was granted, but it will be discarded
                             * by the caller if the timeout has also elapsed.
                             */ 

                            zlockGranted = true;

                            if(DEBUG)
                                log.debug("Condition satisified.");
                            
                            break;
                        
                        }
                        
                    } catch (KeeperException ex) {

                        log.warn(this, ex);

                        /*
                         * Fall through.
                         * 
                         * Note: by falling through we handle the case where the
                         * client was not connected to a server when the caller
                         * made their request or where a node does not yet
                         * exist, etc.
                         */

                    }

                    /*
                     * Note: awaitNanos() can return spuriously so we have a
                     * separate [zlockGranted] field that is set IFF the zlock
                     * is granted to the caller.
                     */

                    zlock.awaitNanos((nanos -= (System.nanoTime() - begin)));
                    
                } // while 

                if (cancelled) {

                    throw new InterruptedException();

                }

                if (DEBUG)
                    log.debug("nanos remaining=" + nanos);
                
                // lock granted iff nanos remaining is GT zero.
                return nanos > 0;

            } finally {
                
                lock.unlock();
                
            }

        }

        /**
         * Return <code>true</code> iff the {@link ZLock} is granted (the
         * first child in the ordered set of children).
         * <p>
         * If the lock node has been deleted, sets the {@link #cancelled} flag
         * and returns <code>false</code> (the entire queue was destroyed, so
         * we cancel any process contending for a lock).
         * <p>
         * If the lock node has been invalidated, sets the {@link #cancelled}
         * flag and returns <code>false</code> (the lock node is about to be
         * destroyed so noone is granted the lock).
         * <p>
         * If the zchild has been deleted, sets the {@link #cancelled} flag and
         * returns <code>false</code> (this let's you break the lock by
         * deleting the child).
         * <p>
         * If the client is not connected, then an exception will be thrown and
         * the watch will resume with the next {@link WatchedEvent}.
         * <p>
         * Note: {@link #cancelled} MUST be tested after
         * {@link #isConditionSatisified()} since it MAY be set as a side
         * effect.
         * 
         * @return <code>true</code> iff the process holds the lock
         */
        protected boolean isConditionSatisified() throws KeeperException,
                InterruptedException {

            if (cancelled) {
                /*
                 * Ignore any events once the lock was cancelled.
                 */
                return true;

            }

            if (zookeeper.exists(zpath + INVALID, this) != null) {

                /*
                 * The lock node has been invalidation as part of the protocol
                 * to destroy the lock node together with its queue. Once this
                 * is done, NO ONE will be granted the lock.
                 */

                cancelled = true;

                priorChildZNode = null;

                log.warn("Lock node invalidated: " + this);

                return false;

            }

            if(zlockGranted) {

                // set watch on ourself
                if (zookeeper.exists(zpath + "/" + zchild, this) == null) {

                    // lost the zlock.
                    cancelled = true;

                    log.warn("Lost zlock: " + this);
                    
                    return false;
                    
                }

                // still hold the zlock.
                return true;
                
            }

            final List<String> children;
            try {

                /*
                 * Get children, but DO NOT set the watch (we don't watch the
                 * lock node itself, just the prior child).
                 */
                final String[] a = zookeeper
                        .getChildren(zpath, false/* watch */).toArray(
                                new String[0]);

                // sort
                Arrays.sort(a);

                // wrap as list again.
                children = Arrays.asList(a);

                // if(INFO)
                // log.info("queue: "+children);

            } catch (NoNodeException ex) {

                /*
                 * Someone deleted the lock node, so all outstanding requests
                 * will be cancelled.
                 */

                cancelled = true;

                priorChildZNode = null;

                log.warn("Lock node destroyed: " + this);

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

                priorChildZNode = null;

                if (INFO)
                    log.info("ZLock granted: " + this);

                return true;

            }

            // remember since we need to it cancel our watch.
            priorChildZNode = children.get(pos - 1);

            // set watch on the prior child in the list.
            if (zookeeper.exists(zpath + "/" + priorChildZNode, this) == null) {
                
                /*
                 * Recursive evaluation if the prior child in the queue has been
                 * asynchronously deleted.
                 */
                
                if (INFO)
                    log.info("Prior child was asynchronously deleted: " + this);

                return isConditionSatisified();
                
            }

            if (INFO)
                log.info("Process in queue: pos=" + pos + " out of "
                        + children.size() + " children, " + this
                        + (DEBUG ? " : children=" + children.toString() : ""));

            return false;

        }

    } // inner class ZLockWatcher

    /**
     * The EPHEMERAL znode (not zpath) representing the lock request and
     * <code>null</code> iff the {@link ZLockImpl} is not contending for the
     * lock.
     * 
     * @throws InterruptedException
     */
    public String getLockRequestZNode() throws InterruptedException {

        lock.lockInterruptibly();
        try {

            return (watcher == null ? null : watcher.zchild);

        } finally {

            lock.unlock();

        }

    }

    /**
     * Synchronously verifies that the lock is still held by testing the
     * children of the lock node. This is necessary in order to avoid snafus
     * such as
     * 
     * <pre>
     * lock.unlock();
     * lock.isLockHeld()==true
     * </pre>
     * 
     * where the process still things it holds the lock.
     * <p>
     * There are distributed variants of this snafu as well. For example,
     * someone deletes the znode corresponding to the lock request. If we do not
     * synchronously verify that the lock request znode still exists then we
     * will fail to notice that someone else now holds the lock!
     * <p>
     * As an optimization, we do not test zookeeper if we have already been
     * informed that the {@link ZLock} has been lost.
     * 
     * @throws KeeperException 
     */
    public boolean isLockHeld() throws InterruptedException, KeeperException {

//        if (DEBUG)
//            log.debug(this.toString());

        lock.lockInterruptibly(); 
        try {

            if (watcher == null || !watcher.zlockGranted || watcher.cancelled) {

                if(INFO)
                    log.info("Caller does not hold lock.");
                
                // It is already known that we do not hold the lock.
                return false;
                
            }

            // verify that we hold the lock (synchronous).
            if(!watcher.isConditionSatisified()) {

                if(INFO)
                    log.info("Caller does not hold lock.");

                return false;
                
            }
            
            if(INFO)
                log.info("Caller holds lock.");

            return true;

        } finally {

            lock.unlock();

        }

    }

    /**
     * Creates a new lock request (an EPHEMERAL SEQUENTIAL znode that is a child
     * of the lock node) and waits for the {@link ZLock} to be granted.
     */
    public void lock() throws KeeperException, InterruptedException {

        try {

            lock(Long.MAX_VALUE, TimeUnit.SECONDS);

        } catch (TimeoutException e) {

            /*
             * Note: should not be thrown since timeout is (near)infinite.
             */

            throw new AssertionError(e);

        }

    }

    /**
     * Creates a new lock request (an EPHEMERAL SEQUENTIAL znode that is a child
     * of the lock node) and awaits up to the timeout for the {@link ZLock} to
     * be granted.
     * 
     * @param timeout
     * @parma unit
     */
    public void lock(final long timeout, final TimeUnit unit)
            throws KeeperException, InterruptedException, TimeoutException {

        if (DEBUG)
            log.debug(this.toString());

        lock.lockInterruptibly();
        try {
            
//            if (watcher != null && watcher.lockCount > 0) {
//
//                if (!isLockHeld()) {
//
//                    throw new InterruptedException("Lost lock.");
//                    
//                }
//
//                watcher.lockCount++;
//
//                return;
//
//            }

            final long begin = System.nanoTime();

            long nanos = unit.toNanos(timeout);

            /*
             * Ensure that the lock node exists.
             * 
             * Note: Throws an InterruptedException if the lock node has been
             * invalidated.
             */
            validateLockNode();

            /*
             * There is no data in the ephemeral znode representing the process
             * contending for the lock. Therefore no one needs to "read" this
             * child znode. Since you can not delete the parent until the
             * children have been deleted, the ACL here must not prevent the
             * deletion of the node by another process.
             */

            // create a child contending for that lock.
            final String s = zookeeper.create(zpath + "/lock", new byte[0],
                    acl, CreateMode.EPHEMERAL_SEQUENTIAL);

            // last path component is the znode of the child.
            final String zchild = s.substring(s.lastIndexOf('/') + 1);

            if (INFO)
                log.info("Seeking lock: zpath=" + zpath + ", zchild=" + zchild);

            this.watcher = new ZLockWatcher(zchild);

            nanos -= (System.nanoTime() - begin);

            try {
                /* Note: The state reported here is incomplete since [priorZChild] is not set until we test things in awaitZLockNanos(). */
                if(INFO)
                    log.info("Will await zlock: "+this);
                
                if(!watcher.awaitZLockNanos(nanos)) {

                    // timeout (lock not granted).
                    throw new TimeoutException();
                    
                }

//                watcher.lockCount = 1;
                
                if (INFO)
                    log.info("ZLock granted: zpath=" + zpath + ", zchild="
                            + zchild);

                return;

            } catch (Throwable t) {

                /*
                 * Destroy the child (release lock if we have it and drop out of
                 * queue contending for that lock).
                 */

                try {

                    if (!watcher.knownDeleted) {

                        zookeeper
                                .delete(zpath + "/" + zchild, -1/* version */);

                        watcher.knownDeleted = true;

                    }

                } catch (NoNodeException ex) {

                    // ignore.
                    watcher.knownDeleted = true;

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
     * Releases the {@link ZLock}. A warning is logged if the {@link ZLock} was
     * asynchronously broken. Use {@link #isLockHeld()} to verify that you still
     * hold the {@link ZLock}.
     * 
     * @throws IllegalStateException
     *             if the caller had never acquired the {@link ZLock}.
     */
    public void unlock() throws KeeperException, InterruptedException {

        lock.lockInterruptibly();
        try {

            if (watcher == null) {

                // never held the lock.
                throw new IllegalStateException("Lock not held.");

            }
            
//            if (watcher.lockCount == 0) {
//
//                throw new AssertionError("Lock counter is zero:" + toString());
//
//            }

            if (watcher.cancelled) {

                log.warn("Lock has been asynchronously cancelled.");
                
            }
            
//            // decrement the lock counter.
//            watcher.lockCount--;
//
//            if (watcher.lockCount > 0) {
//
//                // don't release the lock yet.
//                return;
//                
//            }
            
            /*
             * We set this flag first so that we will be able to discard the
             * lock if there is a transient disconnect.
             */

            // hold reference to the inner class.
            final ZLockWatcher watcher = this.watcher;
            
            // clear reference on the outer class.
            this.watcher = null;
            
            watcher.cancelled = true;

            if (DEBUG)
                log.debug(this.toString());

            // // clear watch.
            // watcher.clearWatch();

            /*
             * Note: The watcher should not have any watches while it holds the
             * lock so we don't have to clear the watch.
             */

            final String zchild = watcher.zchild;

            // delete the child (releases the lock).
            try {

                zookeeper.delete(zpath + "/" + zchild, -1/* version */);

                watcher.knownDeleted = true;

                if (INFO)
                    log.info("released lock: " + watcher);

            } catch (NoNodeException ex) {

                /*
                 * Someone has stomped on the child, so the process does not
                 * hold the lock anymore.
                 */

                log.warn("Child already deleted: zpath=" + zpath + ", child="
                        + zchild);

                watcher.knownDeleted = true;

            } catch (SessionExpiredException ex) {

                /*
                 * See notes on ConnectionLossException below.
                 */

                log.warn("Not connected: zpath=" + zpath + ", child=" + zchild
                        + " : " + ex);

            } catch (ConnectionLossException ex) {

                /*
                 * Note: We MUST NOT ignore a ConnectionLossException if unless
                 * this process is being killed. If the process remains alive
                 * but it was unable to delete its child because of a temporary
                 * disconnect AND it reconnects before zookeeper times out the
                 * client then THE LOCK WILL NOT BE RELEASED.
                 * 
                 * This is handled by setting [cancelled := true] above and by
                 * explicitly deleting the child znode if the lock has been
                 * cancelled in the Watcher.
                 */

                log.warn("Not connected: zpath=" + zpath + ", child=" + zchild
                        + " : " + ex);
            }

        } finally {

            lock.unlock();

        }

    }

    /**
     * Creates a marker node (a sibling of the lock node) to prevent new
     * children from being added to the queue and then deletes all children in
     * the queue in reverse lexical order so as to not trigger cascades of
     * watchers and finally deletes the lock node itself and then the marker
     * node.
     */
    public void destroyLock() throws KeeperException, InterruptedException {

        lock.lockInterruptibly();
        try {

            if (!isLockHeld())
                throw new IllegalStateException("Lock not held.");

            /*
             * Create a marker prevent any new child from entering the queue for
             * the lock node. This agreement is necessary in order for us to
             * have a guarentee that the queue will be empty when we try to
             * delete the lock node. Without this protocol any concurrent
             * request for the lock could cause the queue to be non-empty when
             * our delete request for the lock node is processed by zookeeper.
             * 
             * Note: The marker CAN NOT be a child since we can't make that
             * atomic. We could locate it somewhere completely different in the
             * tree, but it is easy enough to make it a sibling
             * 
             * locknode_INVALID
             * 
             * The marker is ephemeral in case this process dies while trying to
             * destroy the queue.
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
