/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import net.jini.core.lookup.ServiceID;

import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.UnisolatedBTreePartitionConstructor;

/**
 * Test ability to launch, register, discover and use a {@link MetadataService}
 * 
 * @todo add tests of all methods on the {@link IMetadataService} api, e.g.,
 *       {@link IMetadataService#getPartition(String, byte[]) and friends.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMetadataServer0 extends AbstractServerTestCase {

    /**
     * 
     */
    public TestMetadataServer0() {
    }

    /**
     * @param arg0
     */
    public TestMetadataServer0(String arg0) {
        super(arg0);
    }

    /**
     * Starts in {@link #setUp()}.
     */
    MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer1;
    /**
     * Must be started by the test.
     */
    DataServer dataServer0;

    /**
     * Starts a {@link DataServer} ({@link #dataServer1}) and then a
     * {@link MetadataServer} ({@link #metadataServer0}). Each runs in its own
     * thread.
     */
    public void setUp() throws Exception {
        
        super.setUp();

        /*
         * Start up a data server before the metadata server so that we can make
         * sure that it is detected by the metadata server once it starts up.
         */
        dataServer1 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer1.config" });

        new Thread() {

            public void run() {
                
                dataServer1.run();
                
            }
            
        }.start();

        /*
         * Start the metadata server.
         */
        metadataServer0 = new MetadataServer(
                new String[] { "src/resources/config/standalone/MetadataServer0.config" });
        
        new Thread() {

            public void run() {
                
                metadataServer0.run();
                
            }
            
        }.start();

    }
    
    /**
     * destroy the test services.
     */
    public void tearDown() throws Exception {
        
        metadataServer0.destroy();

        dataServer1.destroy();
        
        if(dataServer0!=null) {
            
            destroyDataServer0();
            
        }

        super.tearDown();
        
    }

    /**
     * Start data service 0.
     */
    protected void startDataServer0() {

        assert dataServer0 == null;
        
        dataServer0 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer0.config" });

        new Thread() {

            public void run() {

                dataServer0.run();

            }

        }.start();

    }
    
    /**
     * Destroy data server 0.
     */
    protected void destroyDataServer0() {

        assert dataServer0 != null;
        
        System.err.println("Destroying DataServer0");

        dataServer0.destroy();

        dataServer0 = null;
        
    }
    
    /**
     * Test the ability to discover the {@link MetadataService} and the ability
     * of the {@link MetadataServer} to track {@link DataService}s.
     * <p>
     * Note: We start a data service both before and after the metadata server
     * and verify that both wind up in the service cache and that the metadata
     * server itself does not wind up in the cache since it should be excluded
     * by the service item filter.
     */
    public void test_serverRunning() throws Exception {

        // wait for the service to be ready.
        ServiceID dataService1ID = getServiceID(dataServer1);

        // wait for the service to be ready.
        ServiceID metadataServiceID = getServiceID(metadataServer0);

        // get proxy for this metadata service.
        final IMetadataService metadataServiceProxy = (IMetadataService) lookupDataService(metadataServiceID);

        assertNotNull("service not discovered", metadataServiceProxy);

        /*
         * Start a data service and verify that the metadata service will
         * discover it.
         */

        ServiceID dataService0ID = null;

        try {

            startDataServer0();

            /*
             * wait until we get the serviceID as an indication that the data
             * service is running.
             */

            dataService0ID = getServiceID(dataServer0);

            /*
             * verify that both data services were discovered by the metadata
             * server.
             */

            System.err.println("Sleeping");

            Thread.sleep(500);

            assertNotNull(metadataServer0.dataServiceMap.getServiceItemByID(dataService0ID));

            assertNotNull(metadataServer0.dataServiceMap.getServiceItemByID(dataService1ID));

            assertEquals("#dataServices", 2, metadataServer0.
                    dataServiceMap.getServiceCount());

        } finally {

            /*
             * Destroy one of the data services and verify that the metadata
             * server notices this event.
             */
            destroyDataServer0();

            if (dataService0ID != null) {

                System.err.println("Sleeping");

                Thread.sleep(500);

                assertEquals("#dataServices", 1, metadataServer0
                        .dataServiceMap.getServiceCount());

                assertNull(metadataServer0.dataServiceMap.getServiceItemByID(dataService0ID));

                assertNotNull(metadataServer0
                        .dataServiceMap.getServiceItemByID(dataService1ID));

            }

        }

    }

    /**
     * Registers a scale-out index and pre-partitions it to have data on each
     * of two {@link DataService} instances.
     */
    public void test_registerScaleOutIndex() throws Exception {

        // wait for the service to be ready.
        getServiceID(dataServer1);

        // wait for the service to be ready.
        ServiceID metadataServiceID = getServiceID(metadataServer0);

        // get proxy for this metadata service.
        final IMetadataService metadataServiceProxy = (IMetadataService) lookupDataService(metadataServiceID);

        assertNotNull("service not discovered", metadataServiceProxy);

        /*
         * wait until we get the serviceID as an indication that the data
         * service is running.
         */

        startDataServer0();

        // wait for the service to be ready.
        ServiceID dataService0ID = getServiceID(dataServer0);

        try {

            // lookup proxy for dataService0
            final IDataService dataService0Proxy = lookupDataService(dataService0ID); 

            // should be null since the index was not registered.
            assertNull("indexUUID", dataService0Proxy.getIndexUUID("xyz"));
            
            /*
             * This should fail since the index was never registered.
             */
            dataService0Proxy.rangeCount(ITx.UNISOLATED, DataService
                    .getIndexPartitionName("xyz", 0/* partitionId */), null, null);
            
        } catch (ExecutionException ex) {
            
            assertNotNull("cause",ex.getCause());
            
            ex.getCause().printStackTrace();
            
            assertTrue(ex.getCause() instanceof NoSuchIndexException);
            
            log.info("Ignoring expected exception: " + ex);
            
        }

        //
        assertNotNull(metadataServiceProxy.getUnderUtilizedDataService());

        /*
         * register a scale-out index.
         */
        final String indexName = "testIndex";
        
        final UnisolatedBTreePartitionConstructor ctor = new UnisolatedBTreePartitionConstructor();

        UUID indexUUID = metadataServiceProxy.registerManagedIndex(indexName,
                ctor, null);
        
        log.info("Registered scale-out index: indexUUID="+indexUUID);
        
        /*
         * Request the index partition metadata for the initial partition of the
         * scale-out index.
         */

        IPartitionMetadata pmd;
        
        {

            byte[][] data = metadataServiceProxy.getPartition(indexName,
                    new byte[] {});

            assertNotNull(data);

            // left separator key.
            assertEquals(new byte[]{},data[0]);
            
            // partition metadata.
            pmd = (IPartitionMetadata) SerializerUtil.deserialize(data[1]);
            
            // right separator key.
            assertEquals(null,data[2]);
            
        }

        /*
         * Resolve the data service to which the initial index partition was
         * mapped and verify that we can invoke an operation on that index on
         * that data service (i.e., that the data service recognizes that it
         * has an index registered by that name).
         */
        {

            ServiceID serviceID = JiniUtil.uuid2ServiceID(pmd.getDataServices()[0]);

            // lookup the data service (does not use a cache).
            IDataService proxy = lookupDataService(serviceID);

            /*
             * Note: this will be null iff the index is not registered with this
             * data service.
             */
            
            assertNotNull("indexUUID", proxy.getIndexUUID(DataService
                    .getIndexPartitionName(indexName, pmd.getPartitionId())));

        }

    }

}
