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
 * Created on Apr 20, 2007
 */

package com.bigdata.service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import net.jini.core.lookup.ServiceID;

import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.scaleup.IResourceMetadata;

/**
 * Test of client-server communications. The test starts a {@link DataServer}
 * and then verifies that basic operations can be carried out against that
 * server. The server is stopped when the test is torn down.
 * <p>
 * Note: This test uses the <code>DataServer0.config</code> file from the
 * src/resources/config/standalone package.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDataServer0 extends AbstractServerTestCase {

    /**
     * 
     */
    public TestDataServer0() {
    }

    /**
     * @param arg0
     */
    public TestDataServer0(String arg0) {
        super(arg0);
    }
    
    DataServer dataServer0;

    // start server in its own thread.
    public void setUp() throws Exception {
        
        super.setUp();
        
        dataServer0 = new DataServer(new String[]{
                "src/resources/config/standalone/DataServer0.config"
        });
        
        new Thread() {

            public void run() {
                
                dataServer0.run();
                
            }
            
        }.start();
        
    }
    
    /**
     * destroy the test service.
     */
    public void tearDown() throws Exception {
        
        dataServer0.destroy();

        super.tearDown();
        
    }
   
    /**
     * Exercises the basic features of the {@link IDataService} interface using
     * unisolated operations, including creating a B+Tree, insert, contains,
     * lookup, and remove operations on key-value pairs in that B+Tree, and
     * dropping the B+Tree.
     * 
     * @todo this test needs to be (re-)developed - it is some code from before
     *       I decided on static partitioning for now and could lead the way for
     *       some tests of dynamic partitioning. Dynamic partitioning can
     *       probably be its own tests suite.
     *       
     * @throws Exception
     */
    public void test_serverRunning() throws Exception {

        ServiceID serviceID = getServiceID(dataServer0);
        
        final IDataService proxy = lookupDataService(serviceID); 
        
        assertNotNull("service not discovered",proxy);
        
        final String name = "testIndex";
        
        final int partitionId = 0;
        
        /* 
         * Verify rejection of operations on unmapped partition.
         */
        {

            try {
                proxy.batchContains(IDataService.UNISOLATED, name, partitionId,
                        1, new byte[][] { new byte[] { 1 } });
                fail("Expecting exception.");
            } catch (ExecutionException ex) {
                if(!(ex.getCause() instanceof NoSuchIndexException)) {
                    fail("Unexpected exception: "+ex, ex);
                } else {
                    log.info("Ignoring expected exception: "+ex);
                }
            }

            try {
                proxy.batchLookup(IDataService.UNISOLATED, name, partitionId,
                        1, new byte[][] { new byte[] { 1 } });
                fail("Expecting exception.");
            } catch (ExecutionException ex) {
                if(!(ex.getCause() instanceof NoSuchIndexException)) {
                    fail("Unexpected exception: "+ex,ex);
                } else {
                    log.info("Ignoring expected exception: "+ex);
                }
            }

            try {
                proxy.batchInsert(IDataService.UNISOLATED, name, partitionId,
                        1, new byte[][] { new byte[] { 1 } },
                        new byte[][] { new byte[] { 1 } }, false /* returnOldValues */
                );
                fail("Expecting exception.");
            } catch (ExecutionException ex) {
                if(!(ex.getCause() instanceof NoSuchIndexException)) {
                    fail("Unexpected exception: "+ex,ex);
                } else {
                    log.info("Ignoring expected exception: "+ex);
                }
            }

            try {
                proxy.batchRemove(IDataService.UNISOLATED, name, partitionId,
                        1, new byte[][] { new byte[] { 1 } }, false /* returnOldValues */
                );
                fail("Expecting exception.");
            } catch (ExecutionException ex) {
                if(!(ex.getCause() instanceof NoSuchIndexException)) {
                    fail("Unexpected exception: "+ex,ex);
                } else {
                    log.info("Ignoring expected exception: "+ex);
                }
            }

        }
        
        /*
         * Register an index partition on the data service.
         */
        proxy
                .registerIndex(
                        DataService.getIndexPartitionName(name, partitionId),//
                        UUID.randomUUID(),
                        UnisolatedBTreePartition.class.getName(),
                        new UnisolatedBTreePartition.Config(
                                new PartitionMetadataWithSeparatorKeys(
                                        partitionId,
                                        new UUID[] { JiniUtil
                                                .serviceID2UUID(serviceID) },
                                        new IResourceMetadata[] {/* @todo resource metadata */},
                                        new byte[] {}, null/* no right sibling */)));
        
//        proxy.mapPartition(name, new PartitionMetadataWithSeparatorKeys(
//                partitionId, new UUID[] { JiniUtil.serviceID2UUID(serviceID) },
//                new IResourceMetadata[] {/* @todo resource metadata */},
//                new byte[] {}, null/* no right sibling */));
        
        // batch insert into that index partition.
        proxy.batchInsert(IDataService.UNISOLATED, name, partitionId, 1,
                new byte[][] { new byte[] { 1 } },
                new byte[][] { new byte[] { 1 } },
                true /*returnOldValues*/ );

        // verify keys that exist/do not exist.
        boolean contains[] = proxy.batchContains(IDataService.UNISOLATED, name, partitionId, 2,
                new byte[][] { new byte[] { 1 }, new byte[] { 2 } });

        assertNotNull(contains);
        assertEquals(2,contains.length);
        assertTrue(contains[0]);
        assertFalse(contains[1]);

        // lookup keys that do and do not exist. 
        byte[][] values = proxy.batchLookup(IDataService.UNISOLATED, name, partitionId, 2,
                new byte[][] { new byte[] { 1 }, new byte[] { 2 } });

        assertNotNull(values);
        assertEquals(2,values.length);
        assertEquals(new byte[]{1},values[0]);
        assertEquals(null,values[1]);
        
        // rangeCount the partition of the index on the data service.
        assertEquals(1, proxy.rangeCount(IDataService.UNISOLATED, name,
                partitionId, null, null));
        
        /*
         * visit all keys and values for the partition of the index on the data
         * service.
         */
        
        final int flags = IDataService.KEYS | IDataService.VALS;
        
        ResultSet rset = proxy.rangeQuery(IDataService.UNISOLATED, name,
                partitionId, null, null, 100, flags);
        
        assertEquals("rangeCount",1,rset.getRangeCount());
        assertEquals("ntuples",1,rset.getNumTuples());
        assertTrue("exhausted",rset.isExhausted());
        assertEquals("lastKey",new byte[]{1},rset.getLastKey());
        assertNotNull("keys",rset.getKeys());
        assertEquals("keys.length",1,rset.getKeys().length);
        assertEquals(new byte[]{1},rset.getKeys()[0]);
        assertNotNull("vals",rset.getValues());
        assertEquals("vals.length",1,rset.getValues().length);
        assertEquals(new byte[]{1},rset.getValues()[0]);
        
        // remove key that exists, verifying the returned values.
        values = proxy.batchRemove(IDataService.UNISOLATED, name, partitionId, 2,
                new byte[][] { new byte[] { 1 }, new byte[] { 2 } }, true);

        assertNotNull(values);
        assertEquals(2,values.length);
        assertEquals(new byte[]{1},values[0]);
        assertEquals(null,values[1]);

        // remove again, verifying that the returned values are now null.
        values = proxy.batchRemove(IDataService.UNISOLATED, name, partitionId, 2,
                new byte[][] { new byte[] { 1 }, new byte[] { 2 } }, true);

        assertNotNull(values);
        assertEquals(2,values.length);
        assertEquals(null,values[0]);
        assertEquals(null,values[1]);

        /*
         * run a server local procedure.
         */
        {
            
            IProcedure proc = new RangeCountProcedure();
            
            /*
             * Note: The result is ONE (1) since there is one deleted entry in
             * the UnisolatedBTree and rangeCount does not correct for deletion
             * markers!
             */
            assertEquals("result", 1, proxy.submit(IDataService.UNISOLATED,
                    name, partitionId, proc));
            
        }
        
        proxy.dropIndex(DataService.getIndexPartitionName(name, partitionId));

    }
    
    /**
     * This procedure just computes a range count on the index.
     */
    private static class RangeCountProcedure implements IProcedure {

        private static final long serialVersionUID = 5856712176446915328L;

        public RangeCountProcedure() {

        }

        public Object apply(IIndexWithCounter ndx) {

            return new Integer(ndx.rangeCount(null, null));

        }

    }
    
}
