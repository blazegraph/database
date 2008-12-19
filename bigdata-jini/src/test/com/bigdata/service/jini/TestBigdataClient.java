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
 * Created on Apr 23, 2007
 */

package com.bigdata.service.jini;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.ITx;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IMetadataService;

/**
 * Test suite for the {@link JiniClient}.
 * <p>
 * Note: The core test suite has already verified the basic semantics of the
 * {@link IDataService} interface and partitioned indices so all we have to
 * focus on here is the jini integration and verifying that the serialization
 * imposed by RMI goes off without a hitch (e.g., that everything implements
 * {@link Serializable} and that those {@link Serializable} implementations can
 * correctly round trip the data).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo rewrite {@link #setUp()} to use the {@link JiniServicesHelper}
 */
public class TestBigdataClient extends AbstractServerTestCase {

    public TestBigdataClient() {
    }

    public TestBigdataClient(String name) {
        super(name);
    }

    /**
     * Starts in {@link #setUp()}.
     */
    TransactionServer timestampServer0;
    ITimestampService timestampService0;
    
    /**
     * Starts in {@link #setUp()}.
     */
    MetadataServer metadataServer0;
    IMetadataService metadataService0;

    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer0;
    IDataService dataService0;
    
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer1;
    IDataService dataService1;
    
    /**
     * Starts in {@link #setUp()}.
     */
    LoadBalancerServer loadBalancerServer0;
    ILoadBalancerService loadBalancerService0;
    
    /**
     * Starts in {@link #setUp()}.
     */
    JiniClient client;
    
    /**
     * Starts a {@link DataServer} ({@link #dataServer1}) and then a
     * {@link MetadataServer} ({@link #metadataServer0}). Each runs in its own
     * thread.
     */
    public void setUp() throws Exception {

        super.setUp();
        
//        final String groups = ".groups = new String[]{\"" + getName() + "\"}";

        /*
         * Start up a timestamp server.
         */
        timestampServer0 = new TransactionServer(new String[] {
                "src/resources/config/standalone/TimestampServer0.config"
//                , AbstractServer.ADVERT_LABEL+groups 
                });

        new Thread() {

            public void run() {
                
                timestampServer0.run();
                
            }
            
        }.start();

        /*
         * Start up a data server before the metadata server so that we can make
         * sure that it is detected by the metadata server once it starts up.
         */
        dataServer1 = new DataServer(new String[] {
                "src/resources/config/standalone/DataServer1.config"
//                , AbstractServer.ADVERT_LABEL+groups 
                });

        new Thread() {

            public void run() {
                
                dataServer1.run();
                
            }
            
        }.start();

        /*
         * Start the metadata server.
         */
        metadataServer0 = new MetadataServer(
                new String[] { "src/resources/config/standalone/MetadataServer0.config"
//                        , AbstractServer.ADVERT_LABEL+groups
                        });
        
        new Thread() {

            public void run() {
                
                metadataServer0.run();
                
            }
            
        }.start();

        /*
         * Start up a data server after the metadata server so that we can make
         * sure that it is detected by the metadata server once it starts up.
         */
        dataServer0 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer0.config"
//                        , AbstractServer.ADVERT_LABEL+groups
                        });

        new Thread() {

            public void run() {
                
                dataServer0.run();
                
            }
            
        }.start();

        /*
         * Start up a load balancer server.
         */
        loadBalancerServer0 = new LoadBalancerServer(
                new String[] { "src/resources/config/standalone/LoadBalancerServer0.config"
//                        , AbstractServer.ADVERT_LABEL+groups
                        });

        new Thread() {

            public void run() {
                
                loadBalancerServer0.run();
                
            }
            
        }.start();

        client = JiniClient.newInstance(
                new String[] { "src/resources/config/standalone/Client.config"
//                        , BigdataClient.CLIENT_LABEL+groups
                        });

        // Wait until all the services are up.
        getServiceID(timestampServer0);
        getServiceID(metadataServer0);
        getServiceID(dataServer0);
        getServiceID(dataServer1);
        getServiceID(loadBalancerServer0);
        
        final JiniFederation fed = client.connect();

        // resolve proxy.
        timestampService0 = fed.getTransactionService();
        assertNotNull("timestampService",timestampService0);
        
        // resolve proxy.
        loadBalancerService0 = fed.getLoadBalancerService();
        assertNotNull("loadBalancerService",loadBalancerService0);

        // verify that the client has/can get the metadata service.
        metadataService0 = fed.getMetadataService();
        assertNotNull("metadataService", metadataService0);

        assertEquals("#dataServices", 2, fed.awaitServices(
                2/* minDataServices */, 2000/* timeout(ms) */).length);
        
        assertTrue(metadataServer0.getProxy() instanceof IMetadataService);
        assertTrue(fed.getMetadataService() instanceof IMetadataService);

        // resolve proxy.
        dataService0 = fed.getDataService(JiniUtil.serviceID2UUID(dataServer0.getServiceID()));
        assertNotNull("dataService0",dataService0);

        // resolve proxy.
        dataService1 = fed.getDataService(JiniUtil.serviceID2UUID(dataServer0.getServiceID()));
        assertNotNull("dataService1",dataService1);
        
//        /*
//         * Verify that we have discovered the _correct_ metadata service. This
//         * is a potential problem when starting a stopping services for the test
//         * suite.
//         */
//        assertEquals("serviceID(metadataServer)", metadataServer0
//                .getServiceID(), JiniUtil
//                .uuid2ServiceID(((IMetadataService) client.getMetadataService())
//                        .getServiceUUID()));

    }
    
    /**
     * Destroy the test services.
     */
    public void tearDown() throws Exception {
        
        if(metadataServer0!=null) {

            metadataServer0.destroy();
        
            metadataServer0 = null;

        }

        if(dataServer0!=null) {

            dataServer0.destroy();
        
            dataServer0 = null;

        }
        
        if (dataServer1 != null) {
            
            dataServer1.destroy();

            dataServer1 = null;
            
        }

        if (loadBalancerServer0 != null) {
            
            loadBalancerServer0.destroy();

            loadBalancerServer0 = null;
            
        }
        
        if (timestampServer0 != null) {
            
            timestampServer0.destroy();

            timestampServer0 = null;
            
        }

        if (client != null && client.isConnected()) {

            client.disconnect(true/*immediateShutdown*/);

            client = null;

        }

        super.tearDown();

    }

    /**
     * Test ability to registers a scale-out index on one of the
     * {@link DataService}s.
     * 
     * @throws Exception
     */
    public void test_registerIndex1() throws Exception {

        IBigdataFederation fed = client.connect();

        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name, UUID
                .randomUUID());

        metadata.setDeleteMarkers(true);

        fed.registerIndex(metadata);

        IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

        assertEquals("indexUUID", metadata.getIndexUUID(), ndx.getIndexMetadata()
                .getIndexUUID());

        doBasicIndexTests(ndx);
        
    }

    /**
     * Test ability to registers a scale-out index on both of the
     * {@link DataService}s.
     * 
     * @throws Exception
     */
    public void test_registerIndex2() throws Exception {

        IBigdataFederation fed = client.connect();
        
        final String name = "testIndex";
        
        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());
        
        metadata.setDeleteMarkers(true);

        UUID indexUUID = fed.registerIndex( metadata, //
                // separator keys.
                new byte[][] {
                    new byte[]{},
                    KeyBuilder.asSortKey(500)
                },//
                // data service assignments.
                new UUID[] { //
                    dataService0.getServiceUUID(),//
                    dataService1.getServiceUUID() //
                });
        
        IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);
        
        assertEquals("indexUUID", indexUUID, ndx.getIndexMetadata()
                .getIndexUUID());

        // verify partition 0 on dataService0
        assertNotNull(dataService0.getIndexMetadata(DataService.getIndexPartitionName(name, 0),ITx.UNISOLATED));

        // verify partition 1 on dataService1
        assertNotNull(dataService1.getIndexMetadata(DataService.getIndexPartitionName(name, 1),ITx.UNISOLATED));

        doBasicIndexTests(ndx);

    }

    /**
     * Test helper reads and writes some data on the index in order to verify
     * that these operations can be performed without serialization errors
     * arising from the RPC calls.
     * 
     * @param ndx
     */
    protected void doBasicIndexTests(IIndex ndx) {
        
        int limit = 1000;
        
        byte[][] keys = new byte[limit][];
        byte[][] vals = new byte[limit][];
        
        Random r = new Random();
        
        for(int i=0; i<limit; i++) {
         
            keys[i] = KeyBuilder.asSortKey(i);
            
            byte[] val = new byte[10];
            
            r.nextBytes(val);
            
            vals[i] = val;
            
        }

        // batch insert.
        ndx.submit(0/*fromIndex*/, limit/*toIndex*/, keys, vals, BatchInsertConstructor.RETURN_NO_VALUES,
                null);

        // verify #of index entries.
        assertEquals(limit,ndx.rangeCount(null, null));
        
        // verify data.
        {
        
            ITupleIterator itr = ndx.rangeIterator(null,null);
            
            int i = 0;
            
            while(itr.hasNext()) {
                
                ITuple tuple = itr.next();
                
                assertEquals(keys[i],tuple.getKey());

                assertEquals(vals[i],tuple.getValue());

                i++;
                
            }

            assertEquals(limit,i);
            
        }
        
    }
    
}
