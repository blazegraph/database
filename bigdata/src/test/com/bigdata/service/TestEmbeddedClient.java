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

package com.bigdata.service;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;

/**
 * Test suite for the {@link EmbeddedClient}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEmbeddedClient extends AbstractEmbeddedFederationTestCase {

    public TestEmbeddedClient() {
    }

    public TestEmbeddedClient(String name) {
        super(name);
    }

    /**
     * Test ability to register a scale-out index, access it, and then drop the
     * index.
     */
    public void test_registerIndex() {

        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());
        
        metadata.setDeleteMarkers(true);

        // verify index does not exist.
        assertNull(fed.getIndex(name,ITx.UNISOLATED));
        assertNull(fed.getIndex(name,ITx.READ_COMMITTED));
        
        // register.
        fed.registerIndex(metadata);
        
        // obtain unisolated view.
        {

            final long tx = ITx.UNISOLATED;

            final IIndex ndx = fed.getIndex(name, tx);

            // verify view is non-null
            assertNotNull(ndx);

            // verify same index UUID.
            assertEquals(metadata.getIndexUUID(), ndx.getIndexMetadata()
                    .getIndexUUID());
            
        }
        
        // obtain read-committed view.
        {

            final long tx = ITx.READ_COMMITTED;

            final IIndex ndx = fed.getIndex(name, tx);

            // verify view is non-null
            assertNotNull(ndx);

            // verify same index UUID.
            assertEquals(metadata.getIndexUUID(), ndx.getIndexMetadata()
                    .getIndexUUID());

        }
        
        // drop the index.
        fed.dropIndex(name);
        
        // no longer available to read committed requests.
        assertNull(fed.getIndex(name, ITx.READ_COMMITTED));
        
        // no longer available to unisolated requests.
        assertNull(fed.getIndex(name, ITx.UNISOLATED));
        
        /*
         * @todo obtain a valid commit timestamp for the index during the period
         * in which it existed and verify that a historical view may still be
         * obtained for that timestamp.
         */

    }

    /**
     * Tests the ability to statically partition a scale-out index.
     */
    public void test_staticPartitioning() throws Exception {
        
        final String name = "testIndex";
        
        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        metadata.setDeleteMarkers(true);

        UUID indexUUID = fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });

        final int partitionId0 = 0;
        final int partitionId1 = 1;
        
        /*
         * Verify index is registered on each data service under the correct
         * name for that index partition. The index on each data service must
         * have the same indexUUID since they are just components of the same
         * scale-out index.
         */
        assertIndexRegistered(dataService0, DataService.getIndexPartitionName(
                name, partitionId0), indexUUID);

        assertIndexRegistered(dataService1, DataService.getIndexPartitionName(
                name, partitionId1), indexUUID);

        /*
         * Verify metadata for index partition#0 on dataService0
         * 
         * @todo test more of the metadata for correctness.
         */
        {

            IndexMetadata actual = dataService0.getIndexMetadata(DataService
                    .getIndexPartitionName(name, partitionId0), ITx.UNISOLATED);

            // verify index partition exists on that data service.
            assertNotNull(actual);

            // partition metadata.
            assertEquals("partitionId", partitionId0, actual
                    .getPartitionMetadata().getPartitionId());

            assertEquals("leftSeparator", new byte[] {}, actual
                    .getPartitionMetadata().getLeftSeparatorKey());
            
            assertEquals("rightSeparator", new byte[] { 5 }, actual
                    .getPartitionMetadata().getRightSeparatorKey());
            
            // other metadata.
            assertEquals(metadata.getDeleteMarkers(),actual.getDeleteMarkers());
            
        }

        /*
         * Verify metadata for index partition#1 on dataService1
         */
        {

            IndexMetadata actual = dataService1.getIndexMetadata(DataService
                    .getIndexPartitionName(name, partitionId1), ITx.UNISOLATED);

            // verify index partition exists on that data service.
            assertNotNull(actual);
            
            // partition metadata
            assertEquals("partitionId", partitionId1, actual
                    .getPartitionMetadata().getPartitionId());

            assertEquals("leftSeparator", new byte[] {5}, actual
                    .getPartitionMetadata().getLeftSeparatorKey());
            
            assertEquals("rightSeparator", null, actual.getPartitionMetadata()
                    .getRightSeparatorKey());

            // other metadata
            assertEquals(metadata.getDeleteMarkers(),actual.getDeleteMarkers());
            
        }
        
    }
    
    /**
     * Test of the routine responsible for identifying the split points in an
     * ordered set of keys for a batch index operation. Note that the routine
     * requires access to the partition definitions in the form of a
     * {@link MetadataIndex} in order to identify the split points in the
     * keys[].
     */
    public void test_splitKeys_staticPartitions01() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        metadata.setDeleteMarkers(true);
        
        /*
         * Register and statically partition an index.
         */
        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{}, // keys less than 5...
                new byte[]{5} // keys GTE 5....
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });
        
        /*
         * Request a view of that index.
         */
        ClientIndexView ndx = (ClientIndexView) fed.getIndex(name,ITx.UNISOLATED);

        /*
         * Range count the index to verify that it is empty.
         */
        assertEquals("rangeCount",0,ndx.rangeCount(null, null));

        /*
         * Get metadata for the index partitions that we will need to verify
         * the splits.
         */
        final PartitionLocator pmd0 = ndx.getMetadataIndex().get(new byte[]{});
        final PartitionLocator pmd1 = ndx.getMetadataIndex().get(new byte[]{5});
        assertNotNull("partition#0",pmd0);
        assertNotNull("partition#1",pmd1);
        
        /*
         * Setup data and test splitKeys().
         * 
         * Note: In this test there is a key that is an exact match on the 
         * separator key between the index partitions.
         */
        {
            
            final byte[][] keys = new byte[][] {//
            new byte[]{1}, // [0]
            new byte[]{2}, // [1]
            new byte[]{5}, // [2]
            new byte[]{6}, // [3]
            new byte[]{9}  // [4]
            };
            
            List<Split> splits = ndx.splitKeys( 0, keys.length, keys);
        
            assertNotNull(splits);
            
            assertEquals("#splits", 2, splits.size());

            assertEquals(new Split(pmd0, 0, 2), splits.get(0));
            assertEquals(new Split(pmd1, 2, 5), splits.get(1));
            
        }

        /*
         * Variant in which there are duplicates of the key that corresponds to
         * the rightSeparator for the 1st index partition. This causes a problem
         * where the binarySearch returns the index of ONE of the keys that is
         * equal to the rightSeparator key and we need to back up until we have
         * found the FIRST ONE. While not every example with duplicate keys
         * equal to the rightSeparator will trigger the problem, this example
         * will.
         */
        {
            
            final byte[][] keys = new byte[][] {//
            new byte[]{1}, // [0]
            new byte[]{5}, // [1]
            new byte[]{5}, // [2]
            new byte[]{5}, // [3]
            new byte[]{9}  // [4]
            };
            
            List<Split> splits = ndx.splitKeys( 0, keys.length, keys);
        
            assertNotNull(splits);
            
            assertEquals("#splits", 2, splits.size());

            assertEquals(new Split(pmd0, 0, 1), splits.get(0));
            assertEquals(new Split(pmd1, 1, 5), splits.get(1));
            
        }

        /*
         * Setup data and test splitKeys().
         * 
         * Note: In this test there is NOT an exact match on the separator key
         * between the index partitions. This will result in a negative encoding
         * of the insertion point by the binary search routine. This test
         * verifies that the correct index is selected for the last key to enter
         * the first partition.
         */
        {
            
            final byte[][] keys = new byte[][] {//
            new byte[]{1}, // [0]
            new byte[]{2}, // [1]
            new byte[]{4}, // [2]
            new byte[]{6}, // [3]
            new byte[]{9}  // [4]
            };
            
            List<Split> splits = ndx.splitKeys( 0, keys.length, keys);
        
            assertNotNull(splits);

            assertEquals("#splits", 2, splits.size());

            assertEquals(new Split(pmd0, 0, 3), splits.get(0));
            assertEquals(new Split(pmd1, 3, 5), splits.get(1));
            
        }
                
    }

    public void test_addDropIndex_twoPartitions() throws IOException {
        
        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        metadata.setDeleteMarkers(true);
        
        /*
         * Register and statically partition an index.
         */
        fed.registerIndex(metadata, new byte[][]{//
                new byte[]{}, // keys less than 5...
                new byte[]{5} // keys GTE 5....
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID()
        });

        // view of that index.
        ClientIndexView ndx = (ClientIndexView) fed.getIndex(name,ITx.UNISOLATED);
        
        assertNotNull("Expecting index to be registered", ndx);

        
        /*
         * Range count the index to verify that it is empty.
         */
        assertEquals("rangeCount",0,ndx.rangeCount(null, null));
        
        // drop the index.
        fed.dropIndex(name);

        // request view of index.
        assertNull("Not expecting index to exist", fed.getIndex(name,ITx.UNISOLATED));
        
    }
    
}
