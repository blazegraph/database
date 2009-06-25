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
 * Created on Jan 30, 2009
 */

package com.bigdata.zookeeper;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;

/**
 * Test suite for {@link ZooKeeperAccessor}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestZookeeperAccessor extends AbstractZooTestCase {

    /**
     * 
     */
    public TestZookeeperAccessor() {
    }

    /**
     * @param name
     */
    public TestZookeeperAccessor(String name) {
        super(name);
    }

    public void test_handleExpiredSession() throws InterruptedException {

        final ZooKeeperAccessor accessor = new ZooKeeperAccessor("localhost:"
                + clientPort, sessionTimeout);

        assertTrue(accessor
                .awaitZookeeperConnected(4000, TimeUnit.MILLISECONDS));

        ZooKeeper zookeeper = accessor.getZookeeper();
        
        assertTrue(zookeeper.getState().isAlive());
        
        // close the existing instance.
        zookeeper.close();

        assertFalse(zookeeper.getState().isAlive());

        /*
         * Now obtain a new session.
         */
        
        assertTrue(accessor
                .awaitZookeeperConnected(4000, TimeUnit.MILLISECONDS));

        zookeeper = accessor.getZookeeper();
        
        assertTrue(zookeeper.getState().isAlive());

    }
    
}
