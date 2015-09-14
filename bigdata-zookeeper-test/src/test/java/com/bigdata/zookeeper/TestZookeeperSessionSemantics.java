/*

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Jan 30, 2009
 */

package com.bigdata.zookeeper;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.bigdata.io.TestCase3;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.config.NicUtil;

/**
 * Test suite for {@link ZooKeeper} session expire and disconnect semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestZookeeperSessionSemantics extends TestCase3 {

    /**
     * 
     */
    public TestZookeeperSessionSemantics() {
    }

    /**
     * @param name
     */
    public TestZookeeperSessionSemantics(String name) {
        super(name);
    }

    /**
     * The requested session timeout. The negotiated session timeout is obtained
     * from the {@link ZooKeeper} client connection.
     */
    private int requestedSessionTimeout;

    private static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

    // the chosen client port.
    protected int clientPort = -1;

    @Override
    protected void setUp() throws Exception {

        try {

            if (log.isInfoEnabled())
                log.info(getName());

            final int tickTime = Integer.valueOf(System.getProperty(
                    "test.zookeeper.tickTime", "2000"));

            clientPort = Integer.valueOf(System.getProperty(
                    "test.zookeeper.clientPort", "2081"));

            /*
             * Note: This MUST be the actual session timeout that the zookeeper
             * service will impose on the client. Some unit tests depend on
             * this.
             */
            this.requestedSessionTimeout = tickTime * 2;

            log.warn("clientPort=" + clientPort + ", tickTime=" + tickTime
                    + ", requestedSessionTimeout=" + requestedSessionTimeout);

            // Verify zookeeper is running on the local host at the client port.
            final InetAddress localIpAddr = NicUtil.getInetAddress(null, 0,
                    null, true);
            {
                try {
                    ZooHelper.ruok(localIpAddr, clientPort);
                } catch (Throwable t) {
                    fail("Zookeeper not running:: " + localIpAddr + ":"
                            + clientPort, t);
                }
            }

        } catch (Throwable t) {

            // // don't leave around the dataDir if the setup fails.
            // recursiveDelete(dataDir);
            log.error(getName() + " : " + t, t);
            throw new Exception(t);

        }

    }

//    private void killZKServer() throws InterruptedException {
//        
//        try {
//            ZooHelper.kill(clientPort);
//        } catch (Throwable t) {
//            fail("Zookeeper not running::" + clientPort, t);
//        }
//        
//        Thread.sleep(1000);// wait.
//        
//        final InetAddress localIpAddr = NicUtil.getInetAddress(null, 0,
//                null, true);
//        try {
//            ZooHelper.ruok(localIpAddr, clientPort);
//        } catch (Throwable t) {
//            log.info("Expected exception: Zookeeper not running:: "
//                    + localIpAddr + ":" + clientPort, t);
//            return;
//        }
//        fail("Zookeeper still running.");
//    }

    @Override
    protected void tearDown() throws Exception {

        super.tearDown();

    }

    public void test_handleExpiredSession() throws InterruptedException,
            KeeperException, IOException {

        final String hosts = "localhost:" + clientPort;

        final Lock lock = new ReentrantLock();
        final Condition expireCond = lock.newCondition();
        final Condition connectCond = lock.newCondition();
        final Condition disconnectCond = lock.newCondition();
        final AtomicBoolean didExpire = new AtomicBoolean(false);
        final AtomicBoolean didDisconnect = new AtomicBoolean(false);

        /*
         * Start an instance and run until it gets an assigned sessionId.
         */
        {
            final ZooKeeper zk1a = new ZooKeeper(hosts,
                    requestedSessionTimeout, new Watcher(){
                        @Override
                        public void process(WatchedEvent event) {
                            log.warn(event);
                        }});
            int i = 0;
            while (i < 10) {
                boolean done = false;
                if (zk1a.getState() == ZooKeeper.States.CONNECTED) {
                    done = true;
                }
                log.info("zk.getState()=" + zk1a.getState()
                        + ", zk.getSessionId()=" + zk1a.getSessionId());
                if (done)
                    break;
                Thread.sleep(500);
                i++;
            }
            if(zk1a.getState() != ZooKeeper.States.CONNECTED) {
                fail("Did not connect.");
            }
            zk1a.close();
        }
        
        final ZooKeeper zk1 = new ZooKeeper(hosts, requestedSessionTimeout,
                new Watcher() {
                    /**
                     * Note: The default watcher will not receive any events
                     * after a session expire. A {@link Zookeeper#close()}
                     * causes an immediate session expire. Thus, no events
                     * (include the session expire) will be received after a
                     * close().
                     */
                    @Override
                    public void process(final WatchedEvent event) {
                        log.warn(event);
                        switch (event.getState()) {
                        case AuthFailed:
                            break;
                        case Disconnected:
                            lock.lock();
                            try {
                                didDisconnect.set(true);
                                disconnectCond.signalAll();
                            } finally {
                                lock.unlock();
                            }
                            break;
                        case Expired:
                            lock.lock();
                            try {
                                didExpire.set(true);
                                expireCond.signalAll();
                            } finally {
                                lock.unlock();
                            }
                            break;
//                        case ConnectedReadOnly: // not in 3.3.3
//                            break;
//                        case NoSyncConnected: // not in 3.3.3
//                            break;
//                        case SaslAuthenticated: // not in 3.3.3
//                            break;
                        case SyncConnected:
                            lock.lock();
                            try {
                                connectCond.signalAll();
                            } finally {
                                lock.unlock();
                            }
                            break;
                        case Unknown:
                            break;
                        }

                    }
                });

        /*
         * Note: You can not obtain the negotiated session timeout until the
         * zookeeper client has connected to a zookeeper service (or rather,
         * it will return ZERO until it is connected).
         */
        final int negotiatedSessionTimeout;
        lock.lock();
        try {
            log.info("Waiting zk connected.");
            connectCond.await(10, TimeUnit.SECONDS);
            negotiatedSessionTimeout = zk1.getSessionTimeout();
            if (log.isInfoEnabled())
                log.info("Negotiated sessionTimeout="
                        + negotiatedSessionTimeout);
            assertNotSame(0, negotiatedSessionTimeout);
            assertTrue(negotiatedSessionTimeout > 0);
        } finally {
            lock.unlock();
        }

        assertTrue(zk1.getState().isAlive());

        assertFalse(didDisconnect.get());

        assertFalse(didExpire.get());

        // clear out test znodes.
        destroyZNodes(zk1, "/test");

        // ensure root /test znode exists.
        try {
            zk1.create("/test", new byte[] {}, acl, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ex) {
            log.warn("Ignoring: " + ex);
        }

        // look at that znode, establishing a watcher.
        zk1.getData("/test", true/* watch */, null/* stat */);

        // update the znode's data.
        zk1.setData("/test", new byte[] { 1 }, -1/* version */);

        // create an ephemeral sequential znode that is a child of /test.
        final String foozpath = zk1.create("/test/foo", new byte[] {}, acl,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        // create a 2nd ephemeral sequential znode that is a child of /test.
        final String foozpath2 = zk1.create("/test/foo", new byte[] {}, acl,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        /*
         * Look at that znode, establishing a watcher.
         * 
         * Note: We appear to see node deleted events for the ephemeral znodes
         * if the client connection is closed, but the state is still reported
         * as SyncConnected rather than SessionExpired.
         * 
         * Note: If we do not establish a watcher for an ephemeral znode, then
         * we DO NOT see an node deleted event when the client is closed!
         */
        zk1.getData(foozpath, true/* watch */, null/* stat */);
//        zk1.getData(foozpath2, true/* watch */, null/* stat */);

////      close the existing instance.
//        log.info("Closing ZK client");
//        zk1.close();
        
//        log.fatal("killing local zookeeper service.");
//        killZKServer();
//        Thread.sleep(5000);
//        fail("done");

        if (false) {
            log.info("Spin loop awaiting !isAlive() for client.");
            final long begin = System.currentTimeMillis();
            while (zk1.getState().isAlive()) {
                log.info("zk.getState()=" + zk1.getState()
                        + ", zk.getSessionId()=" + zk1.getSessionId());
                final long elapsed = System.currentTimeMillis() - begin;
                if (elapsed > 60000 * 2)
                    fail("Client still isAlive().");
                Thread.sleep(1000);
            }
            log.info("Continuing");
        }

        if (true) {
            log.error("KILL ZOOKEEPER.");
            Thread.sleep(5000);
            log.info("Spin loop on ephemeral znode getData() for client.");
            while (true) {
                try {
                    zk1.getData(foozpath, true/* watch */, null/* stat */);
                } catch (KeeperException ex) {
                    log.error(ex, ex);
                    Thread.sleep(1000);
                    continue;
                }
                log.info("zk.getState()=" + zk1.getState()
                        + ", zk.getSessionId()=" + zk1.getSessionId());
                break;
//                final long elapsed = System.currentTimeMillis() - begin;
//                if (elapsed > 60000 * 2)
//                    fail("Client still isAlive().");
//                Thread.sleep(1000);
            }
            log.info("Continuing");
            final byte[] a = zk1.getData(foozpath, true/* watch */, null/* stat */);
            assertTrue("Expected " + Arrays.toString(new byte[] { 1 })
                    + ", not " + Arrays.toString(a),
                    BytesUtil.bytesEqual(new byte[] { 1 }, a));
        }

        // // The disconnect event should be immediate.
        // lock.lock();
        // try {
        // disconnectCond.await(100, TimeUnit.MILLISECONDS);
        // } finally {
        // lock.unlock();
        // }
        //
        // assertTrue(didDisconnect.get());

        assertFalse(didDisconnect.get());
        assertFalse(didExpire.get());

        assertFalse(zk1.getState().isAlive());

        /*
         * Wait up to a little more than the negotiated session timeout for the
         * session to be expired.
         */
        lock.lock();
        try {
            // Attempt to get the znode again.
            new Thread(new Runnable() {
                public void run() {
                    try {
                        final byte[] tmp = zk1.getData("/test",
                                true/* watch */, null/* stat */);
                    } catch (KeeperException e) {
                        log.error(e, e);
                    } catch (InterruptedException e) {
                        log.error(e, e);
                    }
                }
            }).start();
            expireCond.await(negotiatedSessionTimeout + 10000,
                    TimeUnit.MILLISECONDS);
            /*
             * Note: No events are ever delivered to the watcher with
             * KeeperStates:=SessionExpired. This appears to be a design
             * decision.
             */
            assertFalse(didExpire.get());
        } finally {
            lock.unlock();
        }

        /*
         * Now obtain a new session.
         */
        {
            log.warn("Starting new ZK connection");
            final ZooKeeper zk2 = new ZooKeeper(hosts, requestedSessionTimeout,
                    new Watcher() {

                        @Override
                        public void process(WatchedEvent event) {
                            log.warn(event);
                        }
                    });

            assertTrue(zk2.getState().isAlive());

        }

    }

    /**
     * Recursive delete of znodes.
     * 
     * @param zpath
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected void destroyZNodes(final ZooKeeper zookeeper, final String zpath)
            throws KeeperException, InterruptedException {

        // System.err.println("enter : " + zpath);

        final List<String> children = zookeeper.getChildren(zpath, false);

        for (String child : children) {

            destroyZNodes(zookeeper, zpath + "/" + child);

        }

        if(log.isInfoEnabled())
            log.info("delete: " + zpath);

        zookeeper.delete(zpath, -1/* version */);

    }

}

