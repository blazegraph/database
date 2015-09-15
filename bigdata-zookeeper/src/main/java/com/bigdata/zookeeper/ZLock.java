package com.bigdata.zookeeper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

/**
 * Distributed synchronous lock.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see ZLockImpl#getLock(org.apache.zookeeper.ZooKeeper, String)
 */
public interface ZLock {

    /**
     * Acquire the lock (blocking).
     * 
     * @throws InterruptedException
     *             if the thread was interrupted while awaiting the lock.
     * @throws InterruptedException
     *             ? thrown if the lock node is destroyed?
     * @throws KeeperException
     *             if the lock node or the ephemeral child could not be
     *             created.
     * @throws SessionExpiredException
     *             If the {@link ZooKeeper} client's session was expired. note
     *             that this exception is non-recoverable for a given
     *             {@link ZooKeeper} instance. However, if the application
     *             handles the exception according to its own semantics then it
     *             MAY obtain a new {@link ZooKeeper} instance associated with a
     *             new session.
     * @throws IllegalStateException
     *             if the lock is already held
     * @throws IllegalStateException
     *             if the lock was already requested.
     */
    public void lock() throws KeeperException, InterruptedException;

    /**
     * Creates a new lock request (an EPHEMERAL SEQUENTIAL znode that is a child
     * of the lock node) and awaits up to the timeout for the {@link ZLock} to
     * be granted.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The unit in which that timeout is expressed.
     * 
     * @throws InterruptedException
     *             if the thread was interrupted while awaiting the lock.
     * @throws TimeoutException
     *             if the timeout noticeably expired before the lock was
     *             granted.
     * @throws KeeperException
     * @throws SessionExpiredException
     *             If the {@link ZooKeeper} client's session was expired. note
     *             that this exception is non-recoverable for a given
     *             {@link ZooKeeper} instance. However, if the application
     *             handles the exception according to its own semantics then it
     *             MAY obtain a new {@link ZooKeeper} instance associated with a
     *             new session.
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
     * @throws SessionExpiredException
     *             If the {@link ZooKeeper} client's session was expired. note
     *             that this exception is non-recoverable for a given
     *             {@link ZooKeeper} instance. However, if the application
     *             handles the exception according to its own semantics then it
     *             MAY obtain a new {@link ZooKeeper} instance associated with a
     *             new session.
     */
    public boolean isLockHeld() throws KeeperException, InterruptedException;

}
