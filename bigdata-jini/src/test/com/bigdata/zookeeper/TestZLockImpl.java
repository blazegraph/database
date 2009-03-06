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
 * Created on Jan 7, 2009
 */

package com.bigdata.zookeeper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Test suite for {@link ZLockImpl}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo do test w/ ensemble where we kill the server to which the client is
 *       connected and verify that the client transparently reconnects to
 *       another server and continues to await the lock.
 */
public class TestZLockImpl extends AbstractZooTestCase {

    /**
     * 
     */
    public TestZLockImpl() {
    }

    /**
     * @param name
     */
    public TestZLockImpl(String name) {
        super(name);
    }

    /**
     * Simple lock protocol test.
     * 
     * @todo test w/ timeout.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void test_lock() throws KeeperException, InterruptedException {

        final Thread mainThread = Thread.currentThread();

        // a node that is guarenteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        try {
            /*
             * verify no such node (should be unique and therefore not
             * preexist).
             */
            zookeeper.getChildren(zpath, false);
            fail("zpath exists: " + zpath);
        } catch (NoNodeException ex) {
            // ignore.
        }

        // instances that can contend for the lock.
        final ZLockImpl lock1 = ZLockImpl.getLock(zookeeper, zpath, acl);

        // znode not created during ctor (more robust this way).
        assertNull(zookeeper.exists(zpath, false));

        final ZLockImpl lock2 = ZLockImpl.getLock(zookeeper, zpath, acl);

        // znode not created during ctor (more robust this way).
        assertNull(zookeeper.exists(zpath, false));

        // obtain the lock.
        lock1.lock();

        log.info("lock1 was granted in main thread");

        // one child in the queue - the one that holds the lock.
        assertEquals(1, zookeeper.getChildren(zpath, false).size());

        assertTrue(lock1.isLockHeld());

        // run a thread that will contend for the lock.
        final Thread t2 = new Thread() {

            public void run() {

                try {

                    log.info("Starting 2nd thread.");

                    assertTrue(lock1.isLockHeld());

                    log.info("Should block seeking lock2 in 2nd thread.");

                    lock2.lock();

                    log.info("lock2 was granted");

                    // one child in the queue - the one that holds the lock.
                    assertEquals(1, zookeeper.getChildren(zpath, false).size());

                } catch (Throwable t) {

                    // log error
                    log.error(t, t);

                    // interrupt the main thread.
                    mainThread.interrupt();

                }
            }

        };

        t2.setDaemon(true);

        t2.start();

        // wait until the other child is also contending for the lock
        for (int i = 0; i < 10; i++) {

            final int n = zookeeper.getChildren(zpath, false).size();

            log.info("nchildren=" + n);

            if (n == 2)
                break;

            Thread.sleep(10/* ms */);

        }

        // should be exactly two children in the queue.
        assertEquals(2, zookeeper.getChildren(zpath, false).size());

        log.info("Will release lock1.");

        // release the lock.
        lock1.unlock();

        log.info("Released lock1.");

        // wait until the other thread gains the lock.
        for (int i = 0; i < 10 && !lock2.isLockHeld(); i++) {

            Thread.sleep(10/* ms */);

        }

        log.info("Verifying lock2 is held.");

        // verify lock is held.
        assertTrue(lock2.isLockHeld());

        log.info("Verifying queue contains only lock2.");

        // verify one child in the queue.
        assertEquals(1, zookeeper.getChildren(zpath, false).size());

        log.info("Releasing lock2 from main thread.");

        // release the lock.
        lock2.unlock();

        log.info("Verifying queue is empty.");

        // queue is empty.
        assertEquals(0, zookeeper.getChildren(zpath, false).size());

        log.info("Test done.");

    }

    /**
     * Unit test explores behavior when someone stomps on the zchild while a
     * lock is held and another lock is in the queue (note that you can not
     * delete the parent without deleting the children in zookeeper, so you will
     * always see a queue purged of children before the queue node itself is
     * deleted).
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void test_breakLock() throws KeeperException, InterruptedException {

        final Thread mainThread = Thread.currentThread();

        // a node that is guarenteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        try {
            /*
             * verify no such node (should be unique and therefore not
             * preexist).
             */
            zookeeper.getChildren(zpath, false);
            fail("zpath exists: " + zpath);
        } catch (NoNodeException ex) {
            // ignore.
        }

        // instances that can contend for the lock.
        final ZLockImpl lock1 = ZLockImpl.getLock(zookeeper, zpath, acl);

        // znode not created during ctor (more robust this way).
        assertNull(zookeeper.exists(zpath, false));

        final ZLockImpl lock2 = ZLockImpl.getLock(zookeeper, zpath, acl);

        // znode not created during ctor (more robust this way).
        assertNull(zookeeper.exists(zpath, false));

        // obtain the lock.
        lock1.lock();

        log.info("lock1 was granted");

        // one child in the queue - the one that holds the lock.
        assertEquals(1, zookeeper.getChildren(zpath, false).size());

        assertTrue(lock1.isLockHeld());

        // run a thread that will contend for the lock.
        final Thread t2 = new Thread() {

            public void run() {

                try {

                    assertTrue(lock1.isLockHeld());

                    lock2.lock();

                    log.info("lock2 granted.");

                } catch (Throwable t) {

                    // log error
                    log.error(t, t);

                    // interrupt the main thread.
                    mainThread.interrupt();

                }

            }

        };

        t2.setDaemon(true);

        t2.start();

        // wait until the other child is also contending for the lock
        for (int i = 0; i < 10
                && zookeeper.getChildren(zpath, false).size() != 2; i++) {

            Thread.sleep(10/* ms */);

        }

        // should be exactly two children in the queue.
        assertEquals(2, zookeeper.getChildren(zpath, false).size());

        // break the lock.
        {
            final String z = zpath + "/"
                    + ((ZLockImpl) lock1).getLockRequestZNode();
            log.info("breaking lock: deleting " + z);
            zookeeper.delete(z, -1/* version */);
            log.info("broke lock: deleted " + z);
        }

        assertTrue(!lock1.isLockHeld());

        assertTrue(lock2.isLockHeld());

        log.info("lock1.unlock() - begin");

        lock1.unlock();

        log.info("lock1.unlock() - done");

        assertFalse(lock1.isLockHeld());

    }

    /**
     * Unit test verifies that a {@link Thread} holding a {@link ZLock} may NOT
     * acquire it again.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void test_notReentrant() throws KeeperException,
            InterruptedException {

        // a node that is guarenteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        final ZLockImpl zlock = ZLockImpl.getLock(zookeeper, zpath, acl);

        zlock.lock();

        assertTrue(zlock.isLockHeld());

        try {
            zlock.lock(500, TimeUnit.MILLISECONDS);
            fail("Expecting: " + TimeoutException.class);
        } catch (TimeoutException ex) {
            log.info("Expected exception: " + ex);
        }

        // assertTrue(zlock.isLockHeld());
        //
        // zlock.unlock();
        //        
        // assertTrue(zlock.isLockHeld());
        //
        // zlock.unlock();
        //
        // assertFalse(zlock.isLockHeld());

    }

    /**
     * Unit test where the session is expired before the lock is requested.
     * lock() should throw out the {@link SessionExpiredException}. We then
     * verify that we can obtain a new {@link ZooKeeper} instance associated
     * with a new session and request and obtain the zlock.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void test_sessionExpiredBeforeLockRequest() throws IOException,
            KeeperException, InterruptedException {

        // a node that is guarenteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        expireSession(zookeeper);

        {

            // obtain a lock object.
            final ZLockImpl zlock = ZLockImpl.getLock(zookeeper, zpath, acl);

            try {

                zlock.lock();

                fail("Expecting: " + SessionExpiredException.class);

            } catch (SessionExpiredException ex) {

                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);

            }

        }

        // get a new instance associated with a new session.
        zookeeper = zookeeperAccessor.getZookeeper();

        // obtain a lock object.
        final ZLockImpl zlock = ZLockImpl.getLock(zookeeper, zpath, acl);
        zlock.lock();
        try {

        } finally {
            zlock.unlock();
        }

    }

    /**
     * Unit test where the session is expired while the caller is holding the
     * lock. The test verifies that isLockHeld() throws a
     * {@link SessionExpiredException}.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void test_sessionExpiredWhileHoldingLock() throws IOException,
            KeeperException, InterruptedException {

        // a node that is guarenteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        // obtain a lock object.
        final ZLockImpl zlock = ZLockImpl.getLock(zookeeper, zpath, acl);
        zlock.lock();
        try {

            assertTrue(zlock.isLockHeld());

            expireSession(zookeeper);

            try {

                zlock.isLockHeld();

                fail("Expecting: " + SessionExpiredException.class);

            } catch (SessionExpiredException ex) {

                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);

            }

        } finally {
            /*
             * Note: This also verifies that NO exception is thrown here even
             * though the session has been expired. This is done to avoid have
             * the expired session problem appear to arise from unlock() when it
             * matters more that people see if when testing to verify that they
             * hold the lock.
             */
            zlock.unlock();
        }

    }

    /**
     * Unit test for destroying a lock which is actively contended by other
     * processes.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     * @throws ExecutionException
     */
    public void test_destroyLock() throws KeeperException,
            InterruptedException, ExecutionException {

        // a node that is guarenteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        final int ntasks = 4;

        final ExecutorService service = Executors.newFixedThreadPool(ntasks,
                DaemonThreadFactory.defaultThreadFactory());

        final LinkedList<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

        for (int i = 0; i < ntasks; i++) {

            tasks.add(new Callable<Void>() {

                /**
                 * Contends for the zlock.
                 * <p>
                 * Note: Task uses a distinct ZooKeeper having a distinct
                 * session.
                 */
                public Void call() throws Exception {

                    final ZooKeeper zookeeper2 = getDistinctZooKeeperWithDistinctSession();

                    // obtain a lock object.
                    final ZLockImpl zlock = ZLockImpl.getLock(zookeeper2,
                            zpath, acl);

                    zlock.lock();
                    try {

                        fail("Should not have obtained the lock.");

                    } finally {

                        zlock.unlock();

                    }

                    return null;

                }

            });

        }

        final List<Future<Void>> futures = new LinkedList<Future<Void>>();
        try {

            // obtain a lock object.
            final ZLockImpl zlock = ZLockImpl.getLock(zookeeper, zpath, acl);

            zlock.lock();
            try {

                // verify that the main thread holds the zlock.
                assertTrue(zlock.isLockHeld());

                // start the other tasks. they will contend for the same zlock.
                for (Callable<Void> task : tasks) {

                    futures.add(service.submit(task));
                    
                }

                // wait until everyone is contending for the lock.
                int queueSize;
                while ((queueSize = zlock.getQueue().length) < ntasks + 1) {

                    if (log.isInfoEnabled())
                        log.info("Waiting for other processes: queueSize="
                                + queueSize);

                    Thread.sleep(100/* ms */);

                }

                if (log.isInfoEnabled())
                    log.info("Main thread will now destroy the lock.");

                zlock.destroyLock();

                // verify lock no longer held.
                assertFalse(zlock.isLockHeld());

            } finally {

                // note: should quitely succeed if the lock was destroyed.
                zlock.unlock();

            }

        } finally {

            service.shutdownNow();

        }

        // verify all tasks started.
        assertEquals(ntasks, futures.size());

        // check their futures.
        for (Future<Void> f : futures) {

            try {

                f.get();

            } catch (ExecutionException ex) {
                
                final Throwable cause = ex.getCause();

                if (cause != null && cause instanceof InterruptedException) {

                    /*
                     * When the lock znode is destroyed, the other processes
                     * contending for the zlock will notice in their
                     * ZLockWatcher. The ZLockWatcher will be set its
                     * [cancelled] flag and an InterruptedException will be
                     * thrown out of lock().
                     */

                    if (log.isInfoEnabled()) {

                        log.info("Ignoring expected exception: " + cause);

                    }
                    
                    continue;

                }

                /*
                 * Rethrow the execption.
                 * 
                 * Note: If any of the tasks gains the lock, then it will throw
                 * an AssertionFailedError.
                 */

                throw ex;
            
            }

        }

    }

}
