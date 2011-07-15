package com.bigdata.htree;

import java.util.UUID;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for bootstrap of an {@link HTree} instance.
 */
public class TestHTree_init extends AbstractHTreeTestCase {

	public TestHTree_init() {
	}

	public TestHTree_init(String name) {
		super(name);
	}

	/**
	 * Test initialization of an {@link HTree}.
	 * 
	 * TODO Add test with store := null (transient mode).
	 * 
	 * TODO Test high level create/load methods.
	 */
	public void test_init_min_addressBits() {

		final IRawStore store = new SimpleMemoryRawStore();
		try {
			final int addressBits = 1;
			final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
			metadata.setAddressBits(addressBits);
			final HTree htree = HTree.create(store, metadata);
			assertEquals(addressBits, htree.addressBits);
		} finally {
			store.destroy();
		}

	}

	public void test_init_max_addressBits_2() {

		final IRawStore store = new SimpleMemoryRawStore();
		try {
			final int addressBits = 2;
			final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
			metadata.setAddressBits(addressBits);
			final HTree htree = HTree.create(store, metadata);
			assertEquals(addressBits, htree.addressBits);
		} finally {
			store.destroy();
		}

	}	

	public void test_init_max_addressBits() {

		final IRawStore store = new SimpleMemoryRawStore();
		try {
			final int addressBits = 16;
			final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
			metadata.setAddressBits(addressBits);
			final HTree htree = HTree.create(store, metadata);
			assertEquals(addressBits, htree.addressBits);
		} finally {
			store.destroy();
		}

	}	

	/**
     * Basic test for correct construction of the initial state of an
     * {@link HTree}.
     */
    public void test_ctor() {

        final int addressBits = 10; // implies ~ 4k page size.
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final HTree htree = getHTree(store, addressBits);
            
			assertTrue("store", store == htree.getStore());
			assertEquals("addressBits", addressBits, htree.getAddressBits());
			assertEquals("nnodes", 1, htree.getNodeCount());
			assertEquals("nleaves", 1, htree.getLeafCount());
			assertEquals("nentries", 0, htree.getEntryCount());
            
        } finally {

            store.destroy();
            
        }

    }

    // TODO The thrown exceptions are wrapped, so we have to test w/ inner cause.
    public void test_ctor_correctRejection() {

		{
			final IRawStore store = new SimpleMemoryRawStore();
			try {
				getHTree(store, 0/* addressBits */);
				fail("Expecting: " + IllegalArgumentException.class);
			} catch (IllegalArgumentException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			} finally {
				store.destroy();
			}
		}

		{
			final IRawStore store = new SimpleMemoryRawStore();
			try {
				getHTree(store, -1/* addressBits */);
				fail("Expecting: " + IllegalArgumentException.class);
			} catch (IllegalArgumentException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			} finally {
				store.destroy();
			}
		}

		// address bits is too large.
		{
			final IRawStore store = new SimpleMemoryRawStore();
			try {
				getHTree(store, 17/* addressBits */);
				fail("Expecting: " + IllegalArgumentException.class);
			} catch (IllegalArgumentException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			} finally {
				store.destroy();
			}
		}

    }

}
