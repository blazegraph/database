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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * Test suite for {@link ZNodeCreatedWatcher}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo do test where we kill and then restart the server while awaiting the
 *       event and verify that we reconnect to the server and continue to await
 *       the event.
 * 
 * @todo do test w/ ensemble where we kill the server to which the client is
 *       connected and verify that reconnect to another server and continue to
 *       await the event.
 */
public class TestZNodeCreatedWatcher extends AbstractZooTestCase {

    /**
     * 
     */
    public TestZNodeCreatedWatcher() {
    }

    /**
     * @param name
     */
    public TestZNodeCreatedWatcher(String name) {
        super(name);
    }

    /**
     * Verify that we can detect the create of a znode.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void test_awaitCreate() throws KeeperException, InterruptedException {

        // a node that is guarenteed to be unique w/in the test namespace.
        final String zpath = "/test/" + getName() + UUID.randomUUID();

        final Thread mainThread = Thread.currentThread();
        
        final Thread t = new Thread() {
            
            public void run() {
                
                try {
                    
                    Thread.sleep(100/*ms*/);

                    zookeeper.create(zpath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                    
                } catch (Throwable t) {
                    
                    // log error 
                    log.error(t, t);
                    
                    // interrupt the main thread.
                    mainThread.interrupt();
                    
                }
            }

        };

        t.setDaemon(true);

        t.start();

        ZNodeCreatedWatcher.awaitCreate(zookeeper, zpath, 250,
                TimeUnit.MILLISECONDS);

//        ZNodeCreatedWatcher.awaitCreate(zookeeper, zpath, 250,
//                TimeUnit.MILLISECONDS);

        // verify znode was created.
        assertNotNull(zookeeper.exists(zpath, false));
        
    }

}
