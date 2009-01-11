package com.bigdata.zookeeper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.KeeperException;

/**
 * Distributed synchronous lock.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see ZNodeLockWatcher#getLock(org.apache.zookeeper.ZooKeeper, String)
 */
public interface ZLock {

    /**
     * Acquire the lock (blocking).
     * 
     * @throws InterruptedException
     *             ? thrown if the lock node is destroyed?
     * @throws KeeperException
     *             if the lock node or the ephemeral child could not be
     *             created.
     * @throws IllegalStateException
     *             if the lock is already held
     * @throws IllegalStateException
     *             if the lock was already requested.
     */
    public void lock() throws KeeperException, InterruptedException;

    /**
     * 
     * @param timeout
     * @param unit
     * 
     * @throws KeeperException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void lock(long timeout, TimeUnit unit) throws KeeperException,
            InterruptedException, TimeoutException;

    /**
     * Release the lock.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IllegalStateException
     *             if the lock is not held.
     */
    public void unlock() throws KeeperException, InterruptedException;

    /**
     * Destroys the lock (deletes everyone in the queue and deletes the lock
     * node).  The caller MUST own the lock.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void destroyLock() throws KeeperException, InterruptedException;
    
    /**
     * Return <code>true</code> if the lock is currently held (this verifies
     * that the zchild for the lock is in fact the leader in the queue of
     * ephemeral znodes contending for the lock).
     * 
     * @return <code>true</code> iff the lock is held.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    public boolean isLockHeld() throws KeeperException, InterruptedException;

}
