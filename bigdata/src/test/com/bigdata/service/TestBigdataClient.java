/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 23, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.service.BigdataClient.BigdataFederation;
import com.bigdata.service.BigdataClient.IBigdataFederation;

/**
 * Test suite for the {@link BigdataClient}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
    MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer1;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    BigdataClient client;
    
    /**
     * Starts a {@link DataServer} ({@link #dataServer1}) and then a
     * {@link MetadataServer} ({@link #metadataServer0}). Each runs in its own
     * thread.
     */
    public void setUp() throws Exception {

        log.info(getName());

//        final String groups = ".groups = new String[]{\"" + getName() + "\"}";
        
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

        client = new BigdataClient(
                new String[] { "src/resources/config/standalone/Client.config"
//                        , BigdataClient.CLIENT_LABEL+groups
                        });

        // Wait until all the services are up.
        getServiceID(metadataServer0);
        getServiceID(dataServer0);
        getServiceID(dataServer1);
        
        // verify that the client has/can get the metadata service.
        assertNotNull("metadataService", client.getMetadataService());

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
        
        if(client!=null) {

            client.terminate();

            client = null;
            
        }
        
        log.info(getName());
        
    }
    
    /**
     * Tests basics with a single scale-out index having a single partition.
     * 
     * @throws Exception
     */
    public void test_federationRunning() throws Exception {

        IBigdataFederation fed = client.connect();
        
        final String name = "testIndex";
        
        UUID indexUUID = fed.registerIndex(name);
        
        IIndex ndx = fed.getIndex(IBigdataFederation.UNISOLATED,name);
        
        // fetches the index UUID from the metadata service.
        assertEquals("indexUUID",indexUUID,ndx.getIndexUUID());

        // uses the cached copy of the index UUID.
        assertEquals("indexUUID",indexUUID,ndx.getIndexUUID());

        // the index is empty.
        assertFalse(ndx.contains(new byte[]{1}));
     
        // the key is not in the index.
        assertEquals(null,(byte[])ndx.lookup(new byte[]{1}));
        
        // insert a key-value pair.
        assertNull(ndx.insert(new byte[]{1}, new byte[]{1}));

        // verify index reports value exists for the key.
        assertTrue(ndx.contains(new byte[]{1}));

        // verify correct value in the index.
        assertEquals(new byte[]{1},(byte[])ndx.lookup(new byte[]{1}));

        // verify some range counts.
        assertEquals(0,ndx.rangeCount(new byte[]{}, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{}, new byte[]{2}));
        assertEquals(1,ndx.rangeCount(new byte[]{1}, new byte[]{2}));
        assertEquals(0,ndx.rangeCount(null, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{1},null));
        assertEquals(1,ndx.rangeCount(null,new byte[]{2}));
        assertEquals(1,ndx.rangeCount(null,null));
        
        // remove the index entry.
        assertEquals(new byte[]{1},(byte[])ndx.remove(new byte[]{1}));

        // the index is empty.
        assertFalse(ndx.contains(new byte[]{1}));

        // the key is not in the index.
        assertEquals(null,(byte[])ndx.lookup(new byte[]{1}));
        
        /*
         * verify some range counts.
         * 
         * Note: the range counts do NOT immediately adjust when keys are
         * removed since deletion markers are written into those entries. The
         * relevant index partition(s) need to be compacted for those deletion
         * markers to be removed and the range counts adjusted to match.
         */
        assertEquals(0,ndx.rangeCount(new byte[]{}, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{}, new byte[]{2}));
        assertEquals(1,ndx.rangeCount(new byte[]{1}, new byte[]{2}));
        assertEquals(0,ndx.rangeCount(null, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{1},null));
        assertEquals(1,ndx.rangeCount(null,new byte[]{2}));
        assertEquals(1,ndx.rangeCount(null,null));

    }

    /**
     * Test for {@link IBigdataFederation#getIndex(long, String)} and
     * {@link IBigdataFederation#registerIndex(String)}.
     */
    public void test_getIndex() {

        IBigdataFederation fed = client.connect();
        
        final String name = "testIndex";
        
        final long tx = IBigdataFederation.UNISOLATED;
        
        // verify index does not exist.
        assertNull(fed.getIndex(tx, name));
        
        // register.
        UUID indexUUID = fed.registerIndex(name);
        
        // obtain view.
        IIndex ndx = fed.getIndex(IBigdataFederation.UNISOLATED,name);

        // verify view is non-null
        assertNotNull(ndx);
        
        // verify same index UUID.
        assertEquals(indexUUID,ndx.getIndexUUID());
        
    }

    /**
     * @todo write a test the verifies the client code which caches the metadata
     *       index for a scale-out index.
     */
    public void test_indexPartitionCache() throws Exception {
        fail("write test");
    }
    
    /**
     * Tests the ability to statically partition an index.
     */
    public void test_staticPartitioning() throws Exception {
        
        // Store reference to each data service.
        final IDataService dataService0 = client.getDataService(dataServer0.getServiceID());
        
        final IDataService dataService1 = client.getDataService(dataServer1.getServiceID());
        
        // Connect to the federation.
        BigdataFederation fed = (BigdataFederation)client.connect();
        
        final String name = "testIndex";

        // register the initial partition on dataService0.
//        UUID indexUUID = fed.registerIndex(name, JiniUtil
//                .serviceID2UUID(dataServer0.getServiceID()));
        UUID indexUUID = fed.registerIndex(name, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
                JiniUtil.serviceID2UUID(dataServer0.getServiceID()),
                JiniUtil.serviceID2UUID(dataServer1.getServiceID())
        });
        
        IIndex ndx = fed.getIndex(IBigdataFederation.UNISOLATED,name);

        final int partition0 = 0;
        final int partition1 = 1;
        
//        // Verify the original partition for the index.
//        assertEquals(new PartitionMetadataWithSeparatorKeys(partition0,
//                new UUID[] { JiniUtil
//                        .serviceID2UUID(dataServer0.getServiceID()) },
//                new IResourceMetadata[] { new JournalMetadata(dataServer0
//                        .getJournal(), ResourceState.Live) }, new byte[] {},
//                null /* no rightSeparatorKey */
//        ), fed.getPartition(IBigdataFederation.UNISOLATED, name, new byte[] {}));
//
//        // create a 2nd partition.
//        assertEquals(new PartitionMetadata(partition1, new UUID[] { JiniUtil
//                .serviceID2UUID(dataServer1.getServiceID()) },
//                new IResourceMetadata[] {new JournalMetadata(dataServer1
//                        .getJournal(), ResourceState.Live)}), fed.getMetadataService()
//                .createPartition(name, new byte[] { 5 },
//                        JiniUtil.serviceID2UUID(dataServer1.getServiceID())));
//        
//        // verify the 2nd partition (via getPartition()).
//        assertEquals(
//                new PartitionMetadataWithSeparatorKeys(partition1,
//                        new UUID[] { JiniUtil
//                        .serviceID2UUID(dataServer1.getServiceID()) },
//                new IResourceMetadata[] { new JournalMetadata(dataServer1
//                        .getJournal(), ResourceState.Live) },
//                        new byte[] { 5 }, null /* no rightSeparatorKey */), fed
//                        .getPartition(UNISOLATED, name, new byte[] { 5 }));
//
//        /*
//         * Re-validate the first partition - we should see the new right
//         * separator key now that the 2nd partition has been created.
//         */
//        assertEquals(new PartitionMetadataWithSeparatorKeys(partition0,
//                new UUID[] { JiniUtil
//                        .serviceID2UUID(dataServer0.getServiceID()) },
//                new IResourceMetadata[] {new JournalMetadata(dataServer0
//                        .getJournal(), ResourceState.Live)}, new byte[] {}, new byte[] { 5 }),
//                fed.getPartition(IBigdataFederation.UNISOLATED, name,
//                        new byte[] {}));

        /*
         * Verify index is registered on each data service. The index on each
         * data service must have the same indexUUID since they are just
         * components of the same scale-out index.
         */
        assertIndexRegistered(dataService0, name, indexUUID);
        assertIndexRegistered(dataService1, name, indexUUID);

        /*
         * @todo verify writes and reads, including operations that cross over
         * the separatorKey.
         * 
         * @todo verify that methods read and write in the correct partition by
         * going to the data service for that partition to verify the operation.
         */

        // the index is empty.
        assertFalse(ndx.contains(new byte[]{1}));
        assertFalse(ndx.contains(new byte[]{5}));
        
        // the key is not in the index.
        assertEquals(null,(byte[])ndx.lookup(new byte[]{1}));
        assertEquals(null,(byte[])ndx.lookup(new byte[]{5}));
        
        // insert a key-value pair into each partition.
        assertNull(ndx.insert(new byte[]{1}, new byte[]{1}));
        assertNull(ndx.insert(new byte[]{5}, new byte[]{5}));

        // verify index reports value exists for the key.
        assertTrue(ndx.contains(new byte[]{1}));
        assertTrue(ndx.contains(new byte[]{5}));

        // verify correct value in the index.
        assertEquals(new byte[]{1},(byte[])ndx.lookup(new byte[]{1}));
        assertEquals(new byte[]{5},(byte[])ndx.lookup(new byte[]{5}));

        // verify correct value in the index on the correct data service.
        assertEquals(new byte[] { 1 }, (byte[]) dataService0.batchLookup(
                UNISOLATED, name, partition0, 1, new byte[][] { new byte[] { 1 } })[0]);
        //
        assertEquals(new byte[] { 5 }, (byte[]) dataService1.batchLookup(
                UNISOLATED, name, partition1, 1, new byte[][] { new byte[] { 5 } })[0]);

        // verify some range counts.
        assertEquals(0,ndx.rangeCount(new byte[]{}, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{}, new byte[]{2}));
        assertEquals(1,ndx.rangeCount(new byte[]{1}, new byte[]{2}));
        assertEquals(0,ndx.rangeCount(null, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{1},null));
        assertEquals(1,ndx.rangeCount(null,new byte[]{2}));
        assertEquals(1,ndx.rangeCount(null,null));
        
        // remove the index entry.
        assertEquals(new byte[]{1},(byte[])ndx.remove(new byte[]{1}));

        // the index is empty.
        assertFalse(ndx.contains(new byte[]{1}));

        // the key is not in the index.
        assertEquals(null,(byte[])ndx.lookup(new byte[]{1}));
        
        /*
         * verify some range counts.
         * 
         * Note: the range counts do NOT immediately adjust when keys are
         * removed since deletion markers are written into those entries. The
         * relevant index partition(s) need to be compacted for those deletion
         * markers to be removed and the range counts adjusted to match.
         */
        assertEquals(0,ndx.rangeCount(new byte[]{}, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{}, new byte[]{2}));
        assertEquals(1,ndx.rangeCount(new byte[]{1}, new byte[]{2}));
        assertEquals(0,ndx.rangeCount(null, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{1},null));
        assertEquals(1,ndx.rangeCount(null,new byte[]{2}));
        assertEquals(1,ndx.rangeCount(null,null));

    }
    
    /**
     * Test of batch operations (contains, lookup, insert, remove) that span
     * more than one partition. The client is responsible for examining the keys
     * provided by the application in a batch operation and issuing multiple
     * requests (one per partition) if necessary. Those requests can be issued
     * in parallel, but that is not required.
     */
    public void test_batchOps_staticPartitions() {
       
        // Store reference to each data service.
        final IDataService dataService0 = client.getDataService(dataServer0.getServiceID());
        
        final IDataService dataService1 = client.getDataService(dataServer1.getServiceID());
        
        // Connect to the federation.
        BigdataFederation fed = (BigdataFederation)client.connect();
        
        final String name = "testIndex";

        /*
         * Register and statically partition an index.
         */
        fed.registerIndex(name, new byte[][]{//
                new byte[]{}, // keys less than 5...
                new byte[]{5} // keys GTE 5....
        }, new UUID[]{//
                JiniUtil.serviceID2UUID(dataServer0.getServiceID()),
                JiniUtil.serviceID2UUID(dataServer1.getServiceID())
        });
        
        /*
         * Request a view of that index.
         */
        IIndex ndx = fed.getIndex(IBigdataFederation.UNISOLATED,name);
        
        /*
         * Range count the index to verify that it is empty.
         */
        assertEquals("rangeCount",0,ndx.rangeCount(null, null));
       
        
        
        
        fail("write test");
        
    }
    
    /**
     * Test of range count with a statically partitioned index.
     * 
     * @todo test with partitions on different data services.
     * 
     * @todo write tests to verify that we do not double count (multiple
     *       partitions on the same data service).
     * 
     * @todo write tests for key scans as well.
     */
    public void test_rangeCount_staticPartitions() {

        fail("write test");

    }
    
    /**
     * Verify that a named index is registered on a specific {@link DataService}
     * with the specified indexUUID.
     * 
     * @param dataService
     *            The data service.
     * @param name
     *            The index name.
     * @param indexUUID
     *            The unique identifier assigned to all instances of that index.
     */
    protected void assertIndexRegistered(IDataService dataService, String name,
            UUID indexUUID) throws IOException {

        assertEquals("indexUUID", indexUUID, dataService.getIndexUUID(name));
        
    }
    
}
