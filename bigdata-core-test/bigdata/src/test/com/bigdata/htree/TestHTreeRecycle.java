package com.bigdata.htree;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.sector.MemStore;

/**
 * Tests recycling of HTree storage.
 */
public class TestHTreeRecycle extends AbstractHTreeTestCase {

    /**
     * 
     */
    public TestHTreeRecycle() {
    }

    /**
     * @param name
     */
    public TestHTreeRecycle(String name) {
        super(name);
    }
    
    class MemStoreListener extends MemStore {
    	MemStoreListener() {
    		super(DirectBufferPool.INSTANCE);
    	}
    	
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
    
    public void testRecycle() {
        final MemStoreListener store = new MemStoreListener();

        final byte[] key0 = new byte[] { 1, 2, 3 };
        final byte[] val0 = new byte[] { 1, 2, 3 };
        
        Checkpoint lastCheckpoint = null;
        
        try {

            final HTree htree;
            {
                
				final HTreeIndexMetadata md = new HTreeIndexMetadata(getName(),
						UUID.randomUUID());

                md.setAddressBits(6);

                htree = HTree.create(store, md);

            }

            // Get the current checkpoint record.
            lastCheckpoint = htree.getCheckpoint();
            
            /*
             * Initial checkpoint required because the htree root did not exist
             * when we initialized the store. Therefore it is dirty
             * and will be written out now.
             */
            {

                // HTree is dirty.
                assertTrue(htree.needsCheckpoint());

                // Checkpoint record should be recycled.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = htree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(htree.needsCheckpoint());
                
            }

            /*
             * Attempting to write a checkpoint on a clean HTree should return
             * the old checkpoint reference.
             */
            {

                final Checkpoint checkpoint3 = htree.writeCheckpoint2();

                assertTrue(checkpoint3 == lastCheckpoint);

            }
            
            /*
             * Make the counter dirty. Verify that the HTree needs a checkpoint
             * and verify that the checkpoint recycles only the correct records.
             */
            {

                // HTree is clean.
                assertFalse(htree.needsCheckpoint());

                // Make the counter dirty.
                htree.getCounter().incrementAndGet();

                // HTree needs checkpoint.
                assertTrue(htree.needsCheckpoint());

                // Checkpoint record should be recycled.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = htree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the HTree is dirty.
                assertFalse(htree.needsCheckpoint());
                                
            }
            
            /*
             * Test where just the root has changed (insert or delete tuple).
             */
            {

                // Checkpoint is not required.
                assertFalse(htree.needsCheckpoint());
                
                // Insert will delete root node and first child BucketPage.
                store.expectDelete(htree.getRoot().getChildAddr(0));
                
                htree.insert(key0, val0);
                
                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Checkpoint is required.
                assertTrue(htree.needsCheckpoint());

                // Should recycle the old checkpoint record.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());
                store.expectDelete(lastCheckpoint.getRootAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = htree.writeCheckpoint2();

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Root has new address.
                assertNotSame(newCheckpoint.getRootAddr(),
                        lastCheckpoint.getRootAddr());

                // Everything which should have been deleted was deleted.
                store.assertDeleteSetEmpty();

                // Verify that a new checkpoint was written.
                assertTrue(lastCheckpoint != newCheckpoint);

                lastCheckpoint = newCheckpoint;
                
                // No longer reports that the B+Tree is dirty.
                assertFalse(htree.needsCheckpoint());
                
            }

            /*
             * Unit test where the {@link IndexMetadata} has changed.
             * 
             * Note: There are not a lot of ways in which you are allowed to
             * change the IndexMetadata once the index has been created. This
             * picks one of them.
             */
            if (true) {

                // HTree is clean.
                assertFalse(htree.needsCheckpoint());

                final HTreeIndexMetadata md = htree.getIndexMetadata().clone();

                md.setIndexSegmentBranchingFactor(40);

                htree.setIndexMetadata(md);

                // HTree needs checkpoint.
                assertTrue(htree.needsCheckpoint());

                // Checkpoint record should be deleted.
                store.expectDelete(lastCheckpoint.getCheckpointAddr());

                // IndexMetadata record should be deleted.
                store.expectDelete(lastCheckpoint.getMetadataAddr());

                // Checkpoint the index.
                final Checkpoint newCheckpoint = htree.writeCheckpoint2();

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
                
                // No longer reports that the HTree is dirty.
                assertFalse(htree.needsCheckpoint());
                
            }
            
        } finally {

            store.destroy();

        }

    }

}
