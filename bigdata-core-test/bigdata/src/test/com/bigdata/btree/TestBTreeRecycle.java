/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Feb 7, 2012
 */

package com.bigdata.btree;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for recycling of B+Tree records.
 * <p>
 * Note: Due to the pattern by which a {@link BTree} is created, it is always
 * loaded from an existing checkpoint.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/473">
 *      PhysicalAddressResolutionException after reopen using RWStore and
 *      recycler</a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBTreeRecycle extends TestCase2 {

    /**
     * 
     */
    public TestBTreeRecycle() {
    }

    /**
     * @param name
     */
    public TestBTreeRecycle(String name) {
        super(name);
    }

//    /**
//     * Helper class overrides {@link #recycle(long)} in order to observe
//     * recycling events.
//     */
//    private static class RecycleListenerBTree extends BTree {
//
//        public RecycleListenerBTree(IRawStore store, Checkpoint checkpoint,
//                IndexMetadata metadata, boolean readOnly) {
//            super(store, checkpoint, metadata, readOnly);
//        }
//
//        @Override
//        protected int recycle(final long addr) {
//            return super.recycle(addr);
//        }
//
//    }

    /**
     * Helper class overrides {@link IRawStore#delete(long)} to notice delete
     * events.
     */
    private static class RawStoreDeleteListener extends SimpleMemoryRawStore
    {

        /**
         * A set of addresses which SHOULD be deleted. The basic pattern is that
         * you add addresses to this collection before the operation which will
         * cause those addresses to be recycled. You then do that operation. You
         * then verify that the collection is empty. Addresses which are NOT
         * found in this collection when a {@link #delete(long)} is observed
         * will result in a thrown exception.
         */
        private final Set<Long> addrs = new HashSet<Long>();

        /**
         * Add an address which should be deleted to the set of such addresses.
         * 
         * @param addr
         *            An address which should be deleted.
         */
        public void expectDelete(final long addr) {

            if (addr == IRawStore.NULL)
                fail("Not allowed to expect a NULL address");

            if (!addrs.add(addr)) {

                fail("Address already in expectedDelete set: " + addr);

            }

        }

        /**
         * Assert that all addresses which should have been deleted were in fact
         * deleted.
         */
        public void assertDeleteSetEmpty() {

            if (!addrs.isEmpty())
                fail("expectedDeleteAddrs is not empty: " + addrs);

        }

        @Override
        public void delete(final long addr) {

            if(addr == IRawStore.NULL) {
                
                fail("Not allowed to delete a NULL address");
                
            }
            
            if (!addrs.remove(addr)) {

                fail("Not expecting delete: addr=" + addr);

            }

            super.delete(addr);

        }

    }

    /**
     * Unit test examines case of a {@link BTree} without a bloom filter.
     */
    public void test_writeCheckpoint_btree() {

        final byte[] key0 = new byte[] { 1, 2, 3 };
        final byte[] val0 = new byte[] { 1, 2, 3 };
        
        Checkpoint lastCheckpoint = null;
        
        final RawStoreDeleteListener store = new RawStoreDeleteListener();

        try {

            final BTree btree;
            {
                
                final IndexMetadata md = new IndexMetadata(getName(),
                        UUID.randomUUID());

                md.setBranchingFactor(3);

                btree = BTree.create(store, md);

            }

            // Get the current checkpoint record.
            lastCheckpoint = btree.getCheckpoint();
            
            /*
             * Initial checkpoint required because the btree root did not exist
             * when we loaded the B+Tree from the disk. Therefore it is dirty
             * and will be written out now.
             */
            {

                // BTree is dirty.
                assertTrue(btree.needsCheckpoint());

                // Checkpoint record should be recycled.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = btree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(btree.needsCheckpoint());
                
            }

            /*
             * Attempting to write a checkpoint on a clean BTree should return
             * the old checkpoint reference.
             */
            {

                final Checkpoint checkpoint3 = btree.writeCheckpoint2();

                assertTrue(checkpoint3 == lastCheckpoint);

            }
            
            /*
             * Make the counter dirty. Verify that the BTree needs a checkpoint
             * and verify that the checkpoint recycles only the correct records.
             */
            {

                // BTree is clean.
                assertFalse(btree.needsCheckpoint());

                // Make the counter dirty.
                btree.getCounter().incrementAndGet();

                // BTree needs checkpoint.
                assertTrue(btree.needsCheckpoint());

                // Checkpoint record should be recycled.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = btree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(btree.needsCheckpoint());
                                
            }
            
            /*
             * Test where just the root has changed (insert or delete tuple).
             */
            {

                // Checkpoint is not required.
                assertFalse(btree.needsCheckpoint());
                
                btree.insert(key0, val0);

                // Checkpoint is required.
                assertTrue(btree.needsCheckpoint());

                // Should recycle the old checkpoint record.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // Should recycle the old root node/leaf record.
                store.expectDelete(lastCheckpoint.getRootAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = btree.writeCheckpoint2();

                // Root has new address.
                assertNotSame(newCheckpoint.getRootAddr(),
                        lastCheckpoint.getRootAddr());

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(btree.needsCheckpoint());
                
            }

            /*
             * Unit test where the {@link IndexMetadata} has changed.
             * 
             * Note: There are not a lot of ways in which you are allowed to
             * change the IndexMetadata once the index has been created. This
             * picks one of them.
             */
            if(true) {

                // BTree is clean.
                assertFalse(btree.needsCheckpoint());

                final IndexMetadata md = btree.getIndexMetadata().clone();

                md.setIndexSegmentBranchingFactor(40);

                btree.setIndexMetadata(md);

                // BTree needs checkpoint.
                assertTrue(btree.needsCheckpoint());

                // Checkpoint record should be deleted.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // IndexMetadata record should be deleted.
                store.expectDelete(lastCheckpoint.getMetadataAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = btree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Root has new address.
                assertNotSame(newCheckpoint.getRootAddr(),
                        lastCheckpoint.getRootAddr());

                // IndexMetadata has new address.
                assertNotSame(newCheckpoint.getMetadataAddr(),
                        lastCheckpoint.getMetadataAddr());

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(btree.needsCheckpoint());
                
            }
            
        } finally {

            store.destroy();

        }

    }

    /**
     * Unit test examines case of a {@link BTree} with a bloom filter.
     */
    public void test_writeCheckpoint_btree_bloomFilter() {

        final byte[] key0 = new byte[] { 1, 2, 3 };
        final byte[] key1 = new byte[] { 1, 2, 3, 4 };
        final byte[] val0 = new byte[] { 1, 2, 3 };
        final byte[] val1 = new byte[] { 1, 2, 3, 4 };
        
        Checkpoint lastCheckpoint = null;
        
        final RawStoreDeleteListener store = new RawStoreDeleteListener();

        try {

            final BTree btree;
            {
                
                final IndexMetadata md = new IndexMetadata(getName(),
                        UUID.randomUUID());

                md.setBranchingFactor(3);

                // enable bloom filter for this version of the test.
                md.setBloomFilterFactory(BloomFilterFactory.DEFAULT);

                btree = BTree.create(store, md);

            }

            // Get the current checkpoint record.
            lastCheckpoint = btree.getCheckpoint();
            
            /*
             * Initial checkpoint required because the btree root did not exist
             * when we loaded the B+Tree from the disk. Therefore it is dirty
             * and will be written out now.
             */
            {

                // BTree is dirty.
                assertTrue(btree.needsCheckpoint());

                // Checkpoint record should be recycled.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = btree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(btree.needsCheckpoint());
                
            }

            /*
             * Attempting to write a checkpoint on a clean BTree should return
             * the old checkpoint reference.
             */
            {

                final Checkpoint checkpoint3 = btree.writeCheckpoint2();

                assertTrue(checkpoint3 == lastCheckpoint);

            }
            
            /*
             * Make the counter dirty. Verify that the BTree needs a checkpoint
             * and verify that the checkpoint recycles only the correct records.
             */
            {

                // BTree is clean.
                assertFalse(btree.needsCheckpoint());

                // Make the counter dirty.
                btree.getCounter().incrementAndGet();

                // BTree needs checkpoint.
                assertTrue(btree.needsCheckpoint());

                // Checkpoint record should be recycled.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = btree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(btree.needsCheckpoint());
                                
            }
            
            /*
             * Test where just the root has changed (insert or delete tuple).
             */
            {

                // Checkpoint is not required.
                assertFalse(btree.needsCheckpoint());
                
                btree.insert(key0, val0);

                // Checkpoint is required.
                assertTrue(btree.needsCheckpoint());

                // Should recycle the old checkpoint record.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // Should recycle the old root node/leaf record.
                store.expectDelete(lastCheckpoint.getRootAddr());

                // The bloom filter is NULL until written on.
                assertEquals(IRawStore.NULL,
                        lastCheckpoint.getBloomFilterAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = btree.writeCheckpoint2();

                // Root has new address.
                assertNotSame(newCheckpoint.getRootAddr(),
                        lastCheckpoint.getRootAddr());

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(btree.needsCheckpoint());

                // Bloom filter is non-null
                assertNotSame(IRawStore.NULL,
                        lastCheckpoint.getBloomFilterAddr());

            }

            /*
             * Test where just the root has changed again (insert or delete tuple).
             * 
             * Note: This time the bloom filter address in the checkpoint is non-null.
             */
            {

                // Checkpoint is not required.
                assertFalse(btree.needsCheckpoint());

                // Bloom filter is non-null
                assertNotSame(IRawStore.NULL,
                        lastCheckpoint.getBloomFilterAddr());

                btree.insert(key1, val1);

                // Checkpoint is required.
                assertTrue(btree.needsCheckpoint());

                // Should recycle the old checkpoint record.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // Should recycle the old root node/leaf record.
                store.expectDelete(lastCheckpoint.getRootAddr());

                // Should recycle the old bloom filter.
                store.expectDelete(lastCheckpoint.getBloomFilterAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = btree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Root has new address.
                assertNotSame(newCheckpoint.getRootAddr(),
                        lastCheckpoint.getRootAddr());

                // Bloom filter has new address.
                assertNotSame(newCheckpoint.getBloomFilterAddr(),
                        lastCheckpoint.getBloomFilterAddr());
                
                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(btree.needsCheckpoint());
                
            }

            /*
             * Unit test where the {@link IndexMetadata} has changed.
             * 
             * Note: There are not a lot of ways in which you are allowed to
             * change the IndexMetadata once the index has been created. This
             * picks one of them.
             */
            if(true) {

                // BTree is clean.
                assertFalse(btree.needsCheckpoint());

                final IndexMetadata md = btree.getIndexMetadata().clone();

                md.setIndexSegmentBranchingFactor(40);

                btree.setIndexMetadata(md);

                // BTree needs checkpoint.
                assertTrue(btree.needsCheckpoint());

                // Checkpoint record should be deleted.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // IndexMetadata record should be deleted.
                store.expectDelete(lastCheckpoint.getMetadataAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = btree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Root has new address.
                assertNotSame(newCheckpoint.getRootAddr(),
                        lastCheckpoint.getRootAddr());

                // IndexMetadata has new address.
                assertNotSame(newCheckpoint.getMetadataAddr(),
                        lastCheckpoint.getMetadataAddr());

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(btree.needsCheckpoint());
                
            }
            
        } finally {

            store.destroy();

        }

    }

}
