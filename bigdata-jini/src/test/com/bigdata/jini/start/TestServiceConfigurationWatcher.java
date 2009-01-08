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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
public class TestServiceConfigurationWatcher extends AbstractFedZooTestCase {

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
     * @throws ExecutionException 
     * @throws TimeoutException 
     * 
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
            InterruptedException, ConfigurationException, ExecutionException,
            TimeoutException {

        // the config for that fake zroot.
        final String zconfig = fed.getZooConfig().zroot + BigdataZooDefs.ZSLASH
                + BigdataZooDefs.CONFIG;

        final ZooKeeper zookeeper = fed.getZookeeper();

        final int numBefore = listener.running.size();
        
        // zpath for the service configuration znode.
        final String serviceConfigurationZPath = zconfig
                + BigdataZooDefs.ZSLASH
                + TransactionServer.class.getName();
        
        // create monitor task that will compete for locks and start procsses.
        MonitorCreatePhysicalServiceLocks task1 = new MonitorCreatePhysicalServiceLocks(
                fed, listener);

        final Future f1 = fed.getExecutorService().submit(task1);

        assertFalse(f1.isDone());
        
        // create monitor task for new service config nodes.
        ServiceConfigurationZNodeMonitorTask task = new ServiceConfigurationZNodeMonitorTask(
                fed, listener, TransactionServer.class.getName());

        final Future f = fed.getExecutorService().submit(task);
        
        assertFalse(f.isDone());
        
        /*
         * Create znode for the ServiceConfiguration.
         * 
         * Note: This should trigger the watcher. In turn, then watcher should
         * create an instance of the service on our behalf.
         */
        log.info("Creating zpath: " + serviceConfigurationZPath);
        zookeeper.create(serviceConfigurationZPath, SerializerUtil
                .serialize(new TransactionServiceConfiguration(config)), acl,
                CreateMode.PERSISTENT);
        log.info("Created zpath: " + serviceConfigurationZPath);

        /*
         * Run the task for a few seconds and then cancel it (it should still be
         * running).
         * 
         * This give the task the chance to notice the ServiceConfiguration
         * znode (we just created it) and to execute the task that creates the
         * new logical service.
         */
        try {
            f.get(2L, TimeUnit.SECONDS);
            fail("Not expecting task to quit by itself.");
        } catch(TimeoutException ex) {
            log.info("Ignoring expected exception: "+ex);
        }
        
        /*
         * Verify that a logicalService znode was created for that configuration
         * znode.
         */
        
        log.info("logicalServices: "
                + zookeeper.getChildren(serviceConfigurationZPath, false));
        
        assertEquals(1, zookeeper.getChildren(serviceConfigurationZPath, false)
                .size());
        
        /*
         * FIXME verify service is created, query service, shutdown service and
         * then verify service restart re-creates the same ephemeral node.
         */
        Thread.sleep(5000/*ms*/);
        assertEquals(numBefore + 1, listener.running.size());
        
    }

}
