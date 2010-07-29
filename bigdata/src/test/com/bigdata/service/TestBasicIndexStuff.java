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
 * Created on Feb 13, 2008
 */

package com.bigdata.service;

import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.proc.RangeCountProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StaleLocatorException;

/**
 * Test of basic index operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBasicIndexStuff extends
        AbstractEmbeddedFederationTestCase {

    /**
     * 
     */
    public TestBasicIndexStuff() {
    }

    /**
     * @param arg0
     */
    public TestBasicIndexStuff(String arg0) {
        super(arg0);
    }

//    /**
//     * Test verifies the behavior of the {@link IDataService} when requesting an
//     * operation for an index that is not registered on that data service.
//     * <p>
//     * Note: This test is very important. Clients depends on
//     * {@link StaleLocatorException} being thrown when an index partition has
//     * been split, joined or moved in order to automatically refresh their cache
//     * information and reissue their request.
//     * 
//     * @throws Exception
//     * 
//     * FIXME Revisit this test. The {@link StaleLocatorException} should be
//     * thrown only if a registered index has been split, joined or moved. If an
//     * index simply does not exist or was dropped then
//     * {@link NoSuchIndexException} should be thrown. This means that this test
//     * will have to be written either directly in terms of states where a split,
//     * join or move has occurred or using the {@link ResourceManager} to fake
//     * the condition.
//     */
//    public void test_noSuchIndex() throws Exception {
//       
//        final String name = "testIndex";
//
//        assertNull(fed.getIndex(name,ITx.UNISOLATED));
//        
//        /*
//         * Try various operations and make sure that they all throw the expected
//         * exception.
//         */
//
//        // obtaining index metadata
//        try {
//
//            dataService0.getIndexMetadata(name, ITx.UNISOLATED);
//            
//        } catch (Exception ex) {
//
//            if (!isInnerCause(ex, StaleLocatorException.class)) {
//
//                fail("Expecting: " + StaleLocatorException.class + ", not "
//                        + ex, ex);
//
//            }
//
//            System.err.print("Ignoring expected exception: ");
//            getInnerCause(ex, StaleLocatorException.class).printStackTrace(System.err);
//            
//        }
//        
////        // obtaining index statistics
////        try {
////
////            dataService0.getStatistics(name);
////            
////        } catch (Exception ex) {
////
////            assertTrue( isInnerCause(ex, StaleLocatorException.class));
////
////            System.err.print("Ignoring expected exception: ");
////            getInnerCause(ex, StaleLocatorException.class).printStackTrace(System.err);
////            
////        }
//        
//        // running a procedure
//        try {
//
//            dataService0.submit(
//                    ITx.UNISOLATED,
//                    name,
//                    new RangeCountProcedure(false/* exact */,
//                            false/*deleted*/, null, null)).get();
//            
//        } catch (Exception ex) {
//
//            assertTrue( isInnerCause(ex, StaleLocatorException.class));
//
//            System.err.print("Ignoring expected exception: ");
//            getInnerCause(ex, StaleLocatorException.class).printStackTrace(System.err);
//            
//        }
//        
//        // range iterator
//        try {
//
//            dataService0
//                    .rangeIterator(ITx.UNISOLATED, name, null/* fromKey */,
//                            null/* toKey */, 0/* capacity */,
//                            IRangeQuery.DEFAULT, null/*filter*/);
//            
//        } catch (Exception ex) {
//
//            assertTrue( isInnerCause(ex, StaleLocatorException.class) );
//
//            System.err.print("Ignoring expected exception: ");
//            getInnerCause(ex, StaleLocatorException.class).printStackTrace(System.err);
//            
//        }
//        
//    }
    
    /**
     * Tests basics with a single scale-out index having a single partition.
     * 
     * @throws Exception
     */
    public void test_onePartition() throws Exception {

        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name, UUID
                .randomUUID());

        metadata.setDeleteMarkers(true);

        fed.registerIndex(metadata);

        IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

        assertEquals("indexUUID", metadata.getIndexUUID(), ndx.getIndexMetadata()
                .getIndexUUID());

        // the index is empty.
        assertFalse(ndx.contains(new byte[]{1}));
     
        // the key is not in the index.
        assertEquals(null, ndx.lookup(new byte[]{1}));
        
        // insert a key-value pair.
        assertNull(ndx.insert(new byte[]{1}, new byte[]{1}));

        // verify index reports value exists for the key.
        assertTrue(ndx.contains(new byte[]{1}));

        // verify correct value in the index.
        assertEquals(new byte[]{1}, ndx.lookup(new byte[]{1}));

        // verify some range counts.
        assertEquals(0, ndx.rangeCount(new byte[] {}, new byte[] { 1 }));
        assertEquals(1, ndx.rangeCount(new byte[] {}, new byte[] { 2 }));
        assertEquals(1, ndx.rangeCount(new byte[] { 1 }, new byte[] { 2 }));
        assertEquals(0, ndx.rangeCount(null, new byte[] { 1 }));
        assertEquals(1, ndx.rangeCount(new byte[] { 1 }, null));
        assertEquals(1, ndx.rangeCount(null, new byte[] { 2 }));
        assertEquals(1, ndx.rangeCount(null, null));
        
        // verify iterator for the same queries.
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] {}, new byte[] { 1 }));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(new byte[] {}, new byte[] { 2 }));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(new byte[] { 1 }, new byte[] { 2 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(null, new byte[] { 1 }));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(new byte[] { 1 }, null));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(null, new byte[] { 2 }));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(null, null));

        // remove the index entry.
        assertEquals(new byte[] { 1 }, ndx.remove(new byte[] { 1 }));

        // the index is empty.
        assertFalse(ndx.contains(new byte[] { 1 }));

        // the key is not in the index.
        assertEquals(null, ndx.lookup(new byte[] { 1 }));
        
        /*
         * verify some range counts.
         * 
         * Note: the range counts do NOT immediately adjust when keys are
         * removed since deletion markers are written into those entries. The
         * relevant index partition(s) need to be compacted for those deletion
         * markers to be removed and the range counts adjusted to match.
         */
        assertEquals(0, ndx.rangeCount(new byte[] {}, new byte[] { 1 }));
        assertEquals(1, ndx.rangeCount(new byte[] {}, new byte[] { 2 }));
        assertEquals(1, ndx.rangeCount(new byte[] { 1 }, new byte[] { 2 }));
        assertEquals(0, ndx.rangeCount(null, new byte[] { 1 }));
        assertEquals(1, ndx.rangeCount(new byte[] { 1 }, null));
        assertEquals(1, ndx.rangeCount(null, new byte[] { 2 }));
        assertEquals(1, ndx.rangeCount(null, null));

        // verify iterator for the same queries.
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] {}, new byte[] { 1 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] {}, new byte[] { 2 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] { 1 }, new byte[] { 2 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(null, new byte[] { 1 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] { 1 }, null));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(null, new byte[] { 2 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(null, null));

    }

    /**
     * Tests a variety of operations on a scale-out index with two index
     * partitions.
     */
    public void test_twoPartitions() throws Exception {

        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        metadata.setDeleteMarkers(true);

        final int partitionId0 = 0;
        final int partitionId1 = 1;

        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

        // the index is empty.
        assertFalse(ndx.contains(new byte[]{1}));
        assertFalse(ndx.contains(new byte[]{5}));
        
        // the key is not in the index.
        assertEquals(null, ndx.lookup(new byte[]{1}));
        assertEquals(null, ndx.lookup(new byte[]{5}));
        
        // insert a key-value pair into each partition.
        assertNull(ndx.insert(new byte[]{1}, new byte[]{1}));
        assertNull(ndx.insert(new byte[]{5}, new byte[]{5}));

        // verify index reports value exists for the key.
        assertTrue(ndx.contains(new byte[]{1}));
        assertTrue(ndx.contains(new byte[]{5}));

        // verify correct value in the index.
        assertEquals(new byte[]{1}, ndx.lookup(new byte[]{1}));
        assertEquals(new byte[]{5}, ndx.lookup(new byte[]{5}));

        // verify correct value in the index on the correct data service.
        assertEquals(new byte[] { 1 }, ((ResultBuffer) dataService0.submit(
                ITx.UNISOLATED,//
                DataService.getIndexPartitionName(name, partitionId0),//
                BatchLookupConstructor.INSTANCE.newInstance(//
                        metadata, //
                        0,// fromIndex
                        1,// toIndex
                        new byte[][] { new byte[] { 1 } },// keys
                        null //vals
                )).get()).getResult(0));
        //
        assertEquals(new byte[] { 5 }, ((ResultBuffer) dataService1.submit(
                ITx.UNISOLATED,//
                DataService.getIndexPartitionName(name, partitionId1),//
                BatchLookupConstructor.INSTANCE.newInstance(//
                        metadata,//
                        0,// fromIndex
                        1,// toIndex
                        new byte[][] { new byte[] { 5 } },// keys
                        null//vals
                )).get()).getResult(0));

        // verify some range counts.
        assertEquals(0,ndx.rangeCount(new byte[]{}, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{}, new byte[]{2}));
        assertEquals(1,ndx.rangeCount(new byte[]{1}, new byte[]{2}));
        assertEquals(0,ndx.rangeCount(null, new byte[]{1}));
        assertEquals(2,ndx.rangeCount(new byte[]{1},null));
        assertEquals(1,ndx.rangeCount(null,new byte[]{2}));
        assertEquals(2,ndx.rangeCount(null,null));
        
        // verify range iterator for the same cases as range count.
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(new byte[] {}, new byte[] { 1 }));
        assertSameIterator(new byte[][] {//
                new byte[] { 1 } //
                }, ndx.rangeIterator(new byte[] {}, new byte[] { 2 }));
        assertSameIterator(new byte[][] {//
                new byte[] { 1 }//
                }, ndx.rangeIterator(new byte[] {1}, new byte[] { 2 }));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(null, new byte[] { 1 }));
        assertSameIterator(new byte[][] {//
                new byte[] { 1 },//
                new byte[] { 5 } //
                }, ndx.rangeIterator(new byte[] {1}, null));
        assertSameIterator(new byte[][] {//
                new byte[] { 1 }//
                }, ndx.rangeIterator(null, new byte[] {2}));
        assertSameIterator(new byte[][] {//
                new byte[] { 1 },//
                new byte[] { 5 } //
                }, ndx.rangeIterator(null, null));
        
        // remove the index entry.
        assertEquals(new byte[] { 1 }, ndx.remove(new byte[] { 1 }));

        // verify that this entry is gone (actually it is marked as deleted).
        assertFalse(ndx.contains(new byte[] { 1 }));

        // the key is not in the index.
        assertEquals(null, ndx.lookup(new byte[] { 1 }));
        
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
        assertEquals(2,ndx.rangeCount(new byte[]{1},null));
        assertEquals(1,ndx.rangeCount(null,new byte[]{2}));
        assertEquals(2,ndx.rangeCount(null,null));

        /*
         * Verify the range iterator for the same cases as the range count.
         * 
         * Note: Unlike rangeCount, the range iterator filters out deleted
         * entries so the deleted entry {1} MUST NOT be visited by any of these
         * iterators.
         */
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(new byte[] {}, new byte[] { 1 }));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(new byte[] {}, new byte[] { 2 }));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(new byte[] {1}, new byte[] { 2 }));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(null, new byte[] { 1 }));
        assertSameIterator(new byte[][] {//
                new byte[] { 5 } //
                }, ndx.rangeIterator(new byte[] {1}, null));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(null, new byte[] {2}));
        assertSameIterator(new byte[][] {//
                new byte[] { 5 } //
                }, ndx.rangeIterator(null, null));

        // remove the other index entry.
        assertEquals(new byte[]{5},(byte[])ndx.remove(new byte[]{5}));

        // verify that this entry is gone (actually it is marked as deleted).
        assertFalse(ndx.contains(new byte[]{5}));

        // the key is not in the index.
        assertEquals(null,(byte[])ndx.lookup(new byte[]{5}));

        /*
         * verify some range counts -- they are unchanged since the deleted keys
         * are still counted until the index parition(s) are compacted.
         */
        assertEquals(0,ndx.rangeCount(new byte[]{}, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{}, new byte[]{2}));
        assertEquals(1,ndx.rangeCount(new byte[]{1}, new byte[]{2}));
        assertEquals(0,ndx.rangeCount(null, new byte[]{1}));
        assertEquals(2,ndx.rangeCount(new byte[]{1},null));
        assertEquals(1,ndx.rangeCount(null,new byte[]{2}));
        assertEquals(2,ndx.rangeCount(null,null));

        /*
         * verify range iterator for the same cases as range count.
         * 
         * Note: Unlike rangeCount, the range iterator filters out deleted
         * entries so all of these cases will be an empty iterator.
         */
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(new byte[] {}, new byte[] { 1 }));
        assertSameIterator(new byte[][]{},//
                ndx.rangeIterator(new byte[] {}, new byte[] { 2 }));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(new byte[] {1}, new byte[] { 2 }));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(null, new byte[] { 1 }));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(new byte[] {1}, null));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(null, new byte[] {2}));
        assertSameIterator(new byte[][] {},//
                ndx.rangeIterator(null, null));

    }

}
