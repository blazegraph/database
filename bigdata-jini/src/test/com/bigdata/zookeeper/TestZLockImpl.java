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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

/**
 * Test suite for {@link ZLockImpl}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test of reentrant semantics for {@link ZLockImpl}.
 * 
 * @todo do test where we kill and then restart the server while awaiting the
 *       event and verify that we reconnect to the server and continue to await
 *       the event.
 * 
 * @todo do test w/ ensemble where we kill the server to which the client is
 *       connected and verify that reconnect to another server and continue to
 *       await the event.
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
    public void test_breakLock() throws KeeperException,
            InterruptedException {
        
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
            final String z = zpath + "/" + ((ZLockImpl) lock1).getLockRequestZNode();
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
    public void test_notReentrant() throws KeeperException, InterruptedException {

        // a node that is guarenteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        final ZLockImpl zlock = ZLockImpl.getLock(zookeeper, zpath, acl);
        
        zlock.lock();

        assertTrue(zlock.isLockHeld());
        
        try {
            zlock.lock(500,TimeUnit.MILLISECONDS);
            fail("Expecting: "+TimeoutException.class);
        } catch(TimeoutException ex) {
            log.info("Expected exception: "+ex);
        }

//        assertTrue(zlock.isLockHeld());
//
//        zlock.unlock();
//        
//        assertTrue(zlock.isLockHeld());
//
//        zlock.unlock();
//
//        assertFalse(zlock.isLockHeld());

    }
    
}
