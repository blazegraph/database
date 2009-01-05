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
 * Created on Jan 4, 2009
 */

package com.bigdata.zookeeper;

import java.net.InetAddress;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import junit.framework.TestCase2;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractZooTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractZooTestCase() {
        super();
    }

    public AbstractZooTestCase(String name) {
        super(name);
    }
    
    final int sessionTimeout = 2000;// ms

    ZooKeeper zookeeper;

    public void setUp() throws Exception {

        // FIXME remove default for this property and report error if not set.
        final int clientPort = Integer.parseInt(System.getProperty(
                "clientPort", "2181"));
        
        // verify server is alive on that port.
        ZooHelper
                .ruok(InetAddress.getLocalHost(), clientPort, 100/*timeout(ms)*/);

        zookeeper = new ZooKeeper("localhost:" + clientPort, sessionTimeout,

                new Watcher() {

                    public void process(WatchedEvent event) {

                        log.info(event.toString());

                    }
                });
     
        // all unit tests use children of this node.
        try {
            zookeeper.create("/test", new byte[] {}, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ex) {
            log.info("/test already exits.");
        }
        
    }

    public void tearDown() throws Exception {
        
        if (zookeeper != null) {

            // @todo clean up /test?
            
            zookeeper.close();

        }
        
    }
    
    /**
     * Class used to test concurrency primitives.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract protected class ClientThread extends Thread {

        private final Thread main;
        
        protected final ReentrantLock lock;

        /**
         * 
         * @param main
         *            The thread in which the test is running.
         * @param lock
         *            A lock.
         */
        public ClientThread(final Thread main, final ReentrantLock lock) {

            if (main == null)
                throw new IllegalArgumentException();

            if (lock == null)
                throw new IllegalArgumentException();

            this.main = main;

            this.lock = lock;

            setDaemon(true);

        }

        public void run() {

            try {

                run2();

            } catch (Throwable t) {

                // log error since won't be seen otherwise.
                log.error(t.getLocalizedMessage(), t);

                // interrupt the main thread.
                main.interrupt();

            }

        }

        abstract void run2() throws Exception;

    }

}
