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

import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.start.process.ZookeeperProcessHelper;
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
public class AbstractFedZooTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractFedZooTestCase() {
    }

    /**
     * @param arg0
     */
    public AbstractFedZooTestCase(String arg0) {
        super(arg0);
    }

    /**
     * A configuration file used by some of the unit tests in this package.
     */
    protected final String configFile = "file:src/test/com/bigdata/jini/start/testfed.config";

    // ACL used for the unit tests.
    protected final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

    Configuration config;

    final protected MockListener listener = new MockListener();

    JiniFederation fed;

    public void setUp() throws Exception {

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
        
        fed = JiniClient.newInstance(args).connect();

        /*
         * Create the federation zroot and config znodes.
         */
        final ZooKeeper zookeeper = fed.getZookeeper();

        // make sure that we have the zroot that we overrode above.
        assertEquals(zroot, fed.getZooConfig().zroot);

        final String zconfig = zroot + BigdataZooDefs.ZSLASH
                + BigdataZooDefs.CONFIG;

        final String zlocks = zroot + BigdataZooDefs.ZSLASH
                + BigdataZooDefs.LOCKS;

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

        zookeeper.create(zlocks, new byte[0], acl, CreateMode.PERSISTENT);

        zookeeper.create(zroot + "/"
                + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE, new byte[0],
                acl, CreateMode.PERSISTENT);

    }

    public void tearDown() throws Exception {

        // destroy any processes started by this test suite.
        for (ProcessHelper t : listener.running) {
            
            t.kill();
            
        }
        
        if (fed != null) {

            fed.shutdownNow();
            
        }
        
    }


}
