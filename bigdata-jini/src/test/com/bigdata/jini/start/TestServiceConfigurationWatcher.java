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

import net.jini.config.ConfigurationException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.io.SerializerUtil;
import com.bigdata.service.jini.TransactionServer;


/**
 * Test suite for managing state changes for a {@link ServiceConfiguration}
 * using a {@link ServiceConfigurationWatcher}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestServiceConfigurationWatcher extends AbstractTestCase {

    /**
     * 
     */
    public TestServiceConfigurationWatcher() {
    }

    /**
     * @param arg0
     */
    public TestServiceConfigurationWatcher(String arg0) {
        super(arg0);
    }

    /**
     * @throws InterruptedException
     * @throws KeeperException
     * @throws ConfigurationException
     * @todo Unit test where the appropriate watchers are established and we
     *       then create the service configuration znode and let the watchers
     *       handle the creation of the logical and physical services and their
     *       znodes.
     * 
     * @todo could do shutdown or destory using the proxy.
     * 
     * @todo verify that normal service shutdown does remove the ephemeral znode
     *       and that service restart re-creates the SAME ephemeral znode (both
     *       should be true as the znode is created using the assigned service
     *       UUID rather than SEQUENTIAL so that it can be a restart safe
     *       zpath).
     */
    public void test_logicalServiceWatcher() throws KeeperException,
            InterruptedException, ConfigurationException {

//        // create a unique fake zroot
//        final String zroot = createTestZRoot();

        // the config for that fake zroot.
        final String zconfig = fed.getZooConfig().zroot + BigdataZooDefs.ZSLASH
                + BigdataZooDefs.CONFIG;

        final ZooKeeper zookeeper = fed.getZookeeper();

        final TransactionServiceConfiguration serviceConfig = new TransactionServiceConfiguration(
                config);
        
        // zpath for the service configuration znode.
        final String serviceConfigurationZPath = zconfig
                + BigdataZooDefs.ZSLASH
                + TransactionServer.class.getSimpleName();
        
        // create the watcher.
        final ServiceConfigurationWatcher watcher = new ServiceConfigurationWatcher(
                fed, listener);

        // set watcher on that znode : @todo clear watcher when node is destroyed?
        zookeeper.exists(serviceConfigurationZPath, watcher);
        
        /*
         * Create znode for the ServiceConfiguration.
         * 
         * Note: This should trigger the watcher. In turn, then watcher should
         * create an instance of the service on our behalf.
         */
        zookeeper.create(serviceConfigurationZPath, SerializerUtil
                .serialize(serviceConfig), acl, CreateMode.PERSISTENT);

        /*
         * FIXME verify service is created, query service, shutdown service and
         * then verify service restart re-creates the same ephemeral node.
         */
        
//        /*
//         * znode for a logical service (the logical service is either a
//         * collection of peers or a service failover chain, depending on the
//         * type of the service). Logical services are persistent. Each one is
//         * assigned a unique (sequential) identifier by zookeeper. It is also
//         * assigned a random UUID.
//         */
//        final String logicalServiceZPath = zookeeper.create(serviceConfigurationZPath
//                + BigdataZooDefs.LOGICAL_SERVICE, SerializerUtil.serialize(UUID
//                .randomUUID()), acl, CreateMode.PERSISTENT_SEQUENTIAL);
//
//        // will be zero unless we started a zookeeper server above.
//        final int processCountBefore = listener.running.size();
//        
//        final JavaServiceStarter serviceStarter = (JavaServiceStarter) serviceConfig
//                .newServiceStarter(fed, listener, logicalServiceZPath);
        
    }

}
