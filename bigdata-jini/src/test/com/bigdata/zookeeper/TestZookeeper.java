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
 * Created on Dec 7, 2008
 */

package com.bigdata.zookeeper;

import com.bigdata.journal.IResourceLockService;

import junit.framework.TestCase2;

/**
 * Basic integration tests for zookeeper.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestZookeeper extends TestCase2 {

    /**
     * 
     */
    public TestZookeeper() {
    }

    /**
     * @param arg0
     */
    public TestZookeeper(String arg0) {
        super(arg0);
    }

    /**
     * Test the ability to start and stop an embedded zoo keeper instance. An
     * embedded instance is used when we run the {@link JiniFederation} as an
     * embedded federation for testing purposes using the
     * {@link JiniServicesHelper}.
     * 
     * @todo config
     * 
     * <pre>
     * tickTime=2000
     * dataDir=/var/zookeeper
     * clientPort=2181
     * </pre>
     * 
     * plus a dedicated transaction log directory for low latency updates.
     * 
     * server: java -cp zookeeper-dev.jar:src/java/lib/log4j-1.2.15.jar:conf
     * org.apache.zookeeper.server.quorum.QuorumPeerMain zoo.cfg
     * <p>
     * client: Use java -cp zookeeper-dev.jar:src/java/lib/log4j-1.2.15.jar:conf
     * org.apache.zookeeper.ZooKeeperMain 127.0.0.1:2181
     */
    public void test_startStopEmbedded() {

        fail("write test");
        
    }

    /**
     * Unit test for basic zookeeper operations that are used to implement
     * the {@link IResourceLockService}.
     */
    public void test_basicOperations() {

        fail("write test");
        
    }
    
}
