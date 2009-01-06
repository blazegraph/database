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
 * Created on Jan 5, 2009
 */

package com.bigdata.jini.start;

import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase2;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationFile;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.JavaServiceConfiguration.JavaServiceStarter;
import com.bigdata.service.IService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.JiniServicesHelper;
import com.bigdata.service.jini.JiniUtil;
import com.bigdata.service.jini.TransactionServer;

/**
 * Test suite for starting bigdata services based on a
 * {@link ServiceConfiguration} stored in {@link ZooKeeper}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestServiceStarter extends TestCase2 {

    /**
     * 
     */
    public TestServiceStarter() {
        
    }

    /**
     * @param arg0
     */
    public TestServiceStarter(String arg0) {

        super(arg0);
        
    }

    JiniServicesHelper helper;
    
    public void setUp() throws Exception {
        
        helper = new JiniServicesHelper("src/resources/config/standalone/");
        
        helper.start();
        
    }
    
    public void tearDown() throws Exception {
        
        helper.destroy();
        
    }
    
    protected static class MockListener implements IServiceListener {

        public List<ProcessHelper> running = Collections
                .synchronizedList(new LinkedList<ProcessHelper>());
        
        public void add(ProcessHelper service) {
        
            System.err.println("adding: " + service);
            
            running.add(service);
            
        }

        public void remove(ProcessHelper service) {

            System.err.println("removing: " + service);

            running.remove(service);
            
        }
        
    }
    
    // @todo use a test resource (as a URI)
    final String configFile = "src/resources/config/bigdata.config";
    final String[] configOptions = null;
    
    /**
     * Unit test verifies that we can start and destroy a service instance using
     * a {@link BigdataServiceConfiguration}. The test waits until the service
     * has been assigned its serviceId by jini and verify that the serviceId is
     * recorded in the physicalService znode.
     * 
     * @throws ConfigurationException
     * @throws Exception
     */
    public void test_startServer() throws ConfigurationException, Exception {
        
        final Configuration config = new ConfigurationFile(new FileReader(
                configFile), configOptions);

        final JiniFederation fed = helper.client.connect();

        final ZooKeeper zookeeper = fed.getZookeeper();

        final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

        if (zookeeper.exists("/test", false/* watch */) == null) {

            zookeeper.create("/test", new byte[0], acl, CreateMode.PERSISTENT);

        }

        final String zroot = zookeeper.create("/test/fed", new byte[0], acl,
                CreateMode.PERSISTENT_SEQUENTIAL);

        final String zconfig = zookeeper.create(zroot + "/config", new byte[0],
                acl, CreateMode.PERSISTENT);

        final ServiceConfiguration serviceConfig = new TransactionServiceConfiguration(
                config);

        // znode for serviceConfiguration
        final String zserviceConfig = zookeeper.create(zconfig + "/"
                + TransactionServer.class.getSimpleName(), SerializerUtil
                .serialize(serviceConfig), acl, CreateMode.PERSISTENT);

        /*
         * znode for a logical service (the logical service is either a
         * collection of peers or a service failover chain, depending on the
         * type of the service). Logical services are persistent. Each one is
         * assigned a unique (sequential) identifier by zookeeper. It is also
         * assigned a random UUID.
         * 
         * @todo The logical UUID is for compatibility with the bigdata APIs,
         * which expect to refer to a service by a UUID. Lookup by UUID against
         * jini will require hashing the physical services by their logical
         * service UUID in the client, which is not hard.
         * 
         * @todo make sure that the physical service looks up the logical
         * service UUID in zookeeper.
         */
        final String logicalServiceZPath = zookeeper.create(zserviceConfig
                + "/logicalService", SerializerUtil
                .serialize(UUID.randomUUID()), acl,
                CreateMode.PERSISTENT_SEQUENTIAL);

        final MockListener listener = new MockListener();

//        final MockFederationDelegate delegate = new MockFederationDelegate(fed);
//        
//        helper.client.setDelegate(delegate);

        final JavaServiceStarter task = (JavaServiceStarter) serviceConfig
                .newServiceStarter(fed, listener, logicalServiceZPath);

        // start the service.
        task.call();

        // verify listener was notified of service start.
        assertEquals(1, listener.running.size());

        // verify that the physicalService was registered with zookeeper. 
        final IService proxy;
        final String physicalServiceZPath;
        final ZNodeDeletedWatcher watcher;
        {
            
            final List<String> children = zookeeper.getChildren(
                    logicalServiceZPath, false/* watch */);

            System.err.println("physicalServices=" + children);
            
            // will fail if the znode was not registered.
            assertEquals(1, children.size());

            physicalServiceZPath = logicalServiceZPath + "/"
                    + children.get(0);

            // get the serviceUUID from the physicalServiceZNode's data.
            final UUID serviceUUID = (UUID) SerializerUtil
                    .deserialize(zookeeper.getData(physicalServiceZPath,
                            false/* watch */, new Stat()));
            
            final ServiceItem serviceItem = discoverService(serviceUUID);

            // verify that the service item is registered with jini. 
            assertNotNull(serviceItem);
            
            // save reference to the service proxy.
            proxy = (IService)serviceItem.service;
            
            /*
             * Set watcher that is notified when the physicalService znode is
             * deleted.
             */ 
            zookeeper.exists(physicalServiceZPath,
                    watcher = new ZNodeDeletedWatcher(zookeeper));
            
        }
        
        /*
         * @todo verify RMI to the service using its proxy (could do shutdown or
         * destory that way).
         */
        
        listener.running.get(0).destroy();

        assertEquals(0, listener.running.size());

        /*
         * Wait until the znode for the physical service has been removed.
         * 
         * Note: An ephemeral znode will be removed once the zookeeper client
         * either times out or is explicitly closed. Since we are killing the
         * process rather than terminating the service normally we may have to
         * raise the timeout before zookeeper will delete the service's znode on
         * its behalf.
         * 
         * @todo verify that normal service shutdown does remove the ephemeral
         * znode and that service restart re-creates the SAME ephemeral znode
         * (both should be true as the znode is created using the assigned
         * service UUID rather than SEQUENTIAL so that it can be a restart safe
         * zpath).
         */
        try {

            watcher.awaitRemove(5000, TimeUnit.MILLISECONDS);

        } catch (TimeoutException ex) {

            fail("znode not removed: zpath=" + physicalServiceZPath);

        }

    }

    /**
     * Looks up the service item in any joined jini registrars but does not wait
     * for the service item to become registered.
     * 
     * @param serviceUUID
     * 
     * @return
     * 
     * @throws IOException
     */
    protected ServiceItem discoverService(final UUID serviceUUID) throws IOException {
        
        final ServiceID serviceId = JiniUtil.uuid2ServiceID(serviceUUID);
        
        ServiceDiscoveryManager serviceDiscoveryManager = null;
        try {

            serviceDiscoveryManager = new ServiceDiscoveryManager(helper.client
                    .getFederation().getDiscoveryManagement(),
                    new LeaseRenewalManager());

            final ServiceItem item = serviceDiscoveryManager
                    .lookup(new ServiceTemplate(serviceId, null/* iface[] */,
                            null/* entry[] */),//
                            null // filter
                    );

            return item;
            
        } finally {

            if (serviceDiscoveryManager != null) {

                serviceDiscoveryManager.terminate();
                
            }

        }

    }
    
    /**
     * @todo Unit test where the appropriate watchers are established and we
     *       then create the service configuration znode and let the watchers
     *       handle the creation of the logical and physical services and their
     *       znodes.
     * 
     */
//    public void test_logicalServiceWatcher() {
//        
//    }
    
}
