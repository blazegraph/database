package com.bigdata.htree;

import java.util.UUID;

import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.util.InnerCause;

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
	 */
	public void test_init_min_addressBits() {

		final IRawStore store = new SimpleMemoryRawStore();
		try {
			final int addressBits = 1;
			final HTreeIndexMetadata metadata = new HTreeIndexMetadata(UUID.randomUUID());
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
			final HTreeIndexMetadata metadata = new HTreeIndexMetadata(UUID.randomUUID());
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
			final HTreeIndexMetadata metadata = new HTreeIndexMetadata(UUID.randomUUID());
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

	/**
     * Basic test for correct construction of the initial state of an
     * {@link HTree} when used without a backing {@link IRawStore}.
     */
    public void test_ctor_transient() {

        final int addressBits = 10; // implies ~ 4k page size.
        
		final IRawStore store = null;

		final HTree htree = getHTree(store, addressBits);

		assertTrue("store", store == htree.getStore());
		assertEquals("addressBits", addressBits, htree.getAddressBits());
		assertEquals("nnodes", 1, htree.getNodeCount());
		assertEquals("nleaves", 1, htree.getLeafCount());
		assertEquals("nentries", 0, htree.getEntryCount());

    }

    public void test_ctor_correctRejection_addressBits_0() {

		final IRawStore store = new SimpleMemoryRawStore();
		try {
			doCorrectRejectionTest(store, 0/*addressBits*/);
		} finally {
			store.destroy();
		}
    }

    public void test_ctor_correctRejection_addressBits_negative() {

		final IRawStore store = new SimpleMemoryRawStore();
		try {
			doCorrectRejectionTest(store, -1/*addressBits*/);
		} finally {
			store.destroy();
		}
    }

    public void test_ctor_correctRejection_addressBits_tooLarge() {

		final IRawStore store = new SimpleMemoryRawStore();
		try {
			doCorrectRejectionTest(store, 17/*addressBits*/);
		} finally {
			store.destroy();
		}
    }
    
	/*
	 * Note: The thrown exceptions are wrapped, so we have to test w/ inner
	 * cause.
	 */
    private void doCorrectRejectionTest(final IRawStore store, final int addressBits) {
    	
			try {
				getHTree(store, addressBits);
				fail("Expecting: " + IllegalArgumentException.class);
			} catch (Throwable t) {
				if (InnerCause.isInnerCause(t, IllegalArgumentException.class)) {
					if (log.isInfoEnabled())
						log.info("Ignoring expected exception: " + t);
				} else {
					fail("Expecting: " + IllegalArgumentException.class);
				}
			}

    }
    
}
