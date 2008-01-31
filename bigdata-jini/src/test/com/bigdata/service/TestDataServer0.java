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
 * Created on Apr 20, 2007
 */

package com.bigdata.service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import net.jini.core.lookup.ServiceID;

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.RangeCountProcedure;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;
import com.bigdata.mdi.UnisolatedBTreePartitionConstructor;

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
                proxy.submit(ITx.UNISOLATED, DataService.getIndexPartitionName(
                        name, partitionId), new RangeCountProcedure(
                        null/* fromKey */, null/* toKey */));
                fail("Expecting exception.");
            } catch (ExecutionException ex) {
                if(!(ex.getCause() instanceof NoSuchIndexException)) {
                    fail("Unexpected exception: "+ex, ex);
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
                        new UnisolatedBTreePartitionConstructor(),
                        new PartitionMetadataWithSeparatorKeys(
                                partitionId,
                                new UUID[] { JiniUtil.serviceID2UUID(serviceID) },
                                new IResourceMetadata[] {/*
                                                             * @todo resource
                                                             * metadata
                                                             */},
                                new byte[] {}, null/* no right sibling */));
        
// proxy.mapPartition(name, new PartitionMetadataWithSeparatorKeys(
// partitionId, new UUID[] { JiniUtil.serviceID2UUID(serviceID) },
//                new IResourceMetadata[] {/* @todo resource metadata */},
//                new byte[] {}, null/* no right sibling */));
        
        // batch insert into that index partition.
        proxy.submit(ITx.UNISOLATED, //
                DataService.getIndexPartitionName(name, partitionId),// 
                new BatchInsert(//
                        1, // n
                        0, // offset
                        new byte[][] { new byte[] { 1 } },// keys
                        new byte[][] { new byte[] { 1 } },// vals
                        false // returnOldValues
                ));

        // verify keys that exist/do not exist.
        boolean contains[] = ((ResultBitBuffer) proxy.submit(
                ITx.UNISOLATED, //
                DataService.getIndexPartitionName(name, partitionId),
                new BatchContains(//
                        2,// n
                        0,// offset
                        new byte[][] { new byte[] { 1 }, new byte[] { 2 } } // keys
                ))).getResult();

        assertNotNull(contains);
        assertEquals(2,contains.length);
        assertTrue(contains[0]);
        assertFalse(contains[1]);

        // lookup keys that do and do not exist. 
        byte[][] values = ((ResultBuffer) proxy.submit(ITx.UNISOLATED, //
                DataService.getIndexPartitionName(name, partitionId),//
                new BatchLookup(//
                        2, // n
                        0, // offset
                        new byte[][] { new byte[] { 1 }, new byte[] { 2 } }// keys
                ))).getResult();

        assertNotNull(values);
        assertEquals(2,values.length);
        assertEquals(new byte[]{1},values[0]);
        assertEquals(null,values[1]);
        
        // rangeCount the partition of the index on the data service.
        assertEquals(1L, 
                proxy.submit(ITx.UNISOLATED, DataService.getIndexPartitionName(
                        name, partitionId), new RangeCountProcedure(
                        null/* fromKey */, null/* toKey */)));
        
        /*
         * visit all keys and values for the partition of the index on the data
         * service.
         */
        
        final int flags = IRangeQuery.KEYS | IRangeQuery.VALS;
        
        ResultSet rset = proxy.rangeIterator(ITx.UNISOLATED, DataService
                .getIndexPartitionName(name, partitionId), null, null, 100, flags, null/*filter*/);
        
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
        values = ((ResultBuffer) proxy.submit(ITx.UNISOLATED, //
                DataService.getIndexPartitionName(name, partitionId), //
                new BatchRemove(//
                        2,// n
                        0,// offset
                        new byte[][] { new byte[] { 1 }, new byte[] { 2 } },// keys
                        true// returnOldValues
                ))).getResult();

        assertNotNull(values);
        assertEquals(2,values.length);
        assertEquals(new byte[]{1},values[0]);
        assertEquals(null,values[1]);

        // remove again, verifying that the returned values are now null.
        values = ((ResultBuffer) proxy.submit(ITx.UNISOLATED, //
                DataService.getIndexPartitionName(name, partitionId), //
                new BatchRemove(//
                        2,// n
                        0,// offset
                        new byte[][] { new byte[] { 1 }, new byte[] { 2 } },// keys
                        true// returnOldValues
                ))).getResult();

        assertNotNull(values);
        assertEquals(2,values.length);
        assertEquals(null,values[0]);
        assertEquals(null,values[1]);

        /*
         * run a server local procedure.
         */
        {
            
            IIndexProcedure proc = new RangeCountProcedure(null/* fromKey */,
                    null/*toKey*/);
            
            /*
             * Note: The result is ONE (1) since there is one deleted entry in
             * the UnisolatedBTree and rangeCount does not correct for deletion
             * markers!
             */
            assertEquals("result", 1L, proxy.submit(ITx.UNISOLATED,
                    DataService.getIndexPartitionName(name, partitionId), proc));
            
        }
        
        proxy.dropIndex(DataService.getIndexPartitionName(name, partitionId));

    }

}
