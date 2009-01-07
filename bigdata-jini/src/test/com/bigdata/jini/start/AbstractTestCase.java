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

package com.bigdata.jini.start;

import java.util.List;
import java.util.UUID;

import junit.framework.TestCase2;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.ZookeeperClientConfig;

/**
 * Abstract base class for unit tests requiring a running zookeeper and a
 * running federation as configured from a test resource.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractTestCase() {
    }

    /**
     * @param arg0
     */
    public AbstractTestCase(String arg0) {
        super(arg0);
    }

    /**
     * A configuration file used by some of the unit tests in this package.
     */
    protected final String configFile = "file:src/test/com/bigdata/jini/start/testfed.config";

    // ACL used for the unit tests.
    protected final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

//    JiniServicesHelper helper;

    Configuration config;

    MockListener listener = new MockListener();

    JiniFederation fed;

    public void setUp() throws Exception {

//        helper = new JiniServicesHelper("src/resources/config/standalone/");
//
//        helper.start();

        // a unique zroot in the /test namespace.
        final String zroot = "/test/" + getName() + UUID.randomUUID();

        System.err.println("zroot: "+zroot);

        final String[] args = new String[] { configFile,
                // Note: overrides the zroot to be unique.
                ZookeeperClientConfig.Options.NAMESPACE + "."
                        + ZookeeperClientConfig.Options.ZROOT + "=" + "\""
                        + zroot + "\"" };
        
        config = ConfigurationProvider.getInstance(args);

        // if necessary, start zookeeper (a server instance).
        ZookeeperProcessHelper.startZookeeper(config, listener);

// fed = helper.client.connect();
        
        fed = JiniClient.newInstance(args).connect();

        /*
         * Create the federation zroot and config znodes.
         */
        final ZooKeeper zookeeper = fed.getZookeeper();

        // make sure that we have the zroot that we overrode above.
        assertEquals(zroot, fed.getZooConfig().zroot);

        final String zconfig = zroot + BigdataZooDefs.ZSLASH
                + BigdataZooDefs.CONFIG;
        
        /*
         * Create the zroot and config nodes
         */

        try {

            // make sure /test exists so that we can create its child.
            zookeeper.create("/test", new byte[] {}/* data */, acl,
                    CreateMode.PERSISTENT);
            
        } catch (NodeExistsException ex) {
            
            // ignore.
            
        }

        zookeeper.create(zroot, new byte[] {}/* data */, acl,
                CreateMode.PERSISTENT);

        zookeeper.create(zconfig, new byte[] {}/* data */, acl,
                CreateMode.PERSISTENT);


    }
    
    public void tearDown() throws Exception {
        
        // destroy any processes started by this test suite.
        for(ProcessHelper t : listener.running) {
            
            t.destroy();
            
        }
        
        if (fed != null) {

            fed.shutdownNow();
            
        }

//        helper.destroy();
        
    }

//    /**
//     * Create a new zroot and a {@link BigdataZooDefs#CONFIG} znode under that
//     * root that is unique in the /test/fed namespace. This keeps the unit test
//     * behavior isolated from the behavior of the federation described in the
//     * config file.
//     * 
//     * @return The zpath of the mock federation zroot.
//     * 
//     * @throws InterruptedException
//     * @throws KeeperException
//     */
//    protected String createTestZRoot() throws KeeperException,
//            InterruptedException {
//
//        final ZooKeeper zookeeper = fed.getZookeeper();
//
//        // Note: uses SEQUENTIAL to make this unique.
//        final String zroot = zookeeper.create("/test/fed", new byte[0], acl,
//                CreateMode.PERSISTENT_SEQUENTIAL);
//
//        zookeeper.create(zroot + "/config", new byte[0], acl,
//                CreateMode.PERSISTENT);
//
//        return zroot;
//        
//    }

}
