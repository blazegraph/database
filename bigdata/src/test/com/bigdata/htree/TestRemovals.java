package com.bigdata.htree;

import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.TestCase3;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rwstore.sector.MemStore;

public class TestRemovals extends AbstractHTreeTestCase {


	public TestRemovals() {
	}

	public TestRemovals(String name) {
		super(name);
	}
	
	/**
	 * Just insert and remove a single key
	 */
	public void test_simpleRemoval() {
        final IRawStore store = new SimpleMemoryRawStore();

        try {
        
			final Random r = new Random();
			
	        final HTreeIndexMetadata metadata = new HTreeIndexMetadata(UUID.randomUUID());
	
	        metadata.setAddressBits(2);
	        
			final HTree htree = HTree.create(store, metadata);
	
			final byte[] key = new byte[] { 1, 2, 3 };
			final byte[] val = new byte[24]; // some random 24 character value
			r.nextBytes(val);
			
			// insert an entry under that key.
			assertNull(htree.insert(key, val));
	
			// this test assumes that everything is in a single bucket page.
			assertEquals(1, htree.getEntryCount());
	
			// examine the root.
			final DirectoryPage root = (DirectoryPage) htree.getRoot();
			
			// verify that the expected data were read.
			TestCase3.assertEquals(val, htree.lookupFirst(key));
			
			htree.remove(key);
		
			assertEquals(0, htree.getEntryCount());
			
			TestCase3.assertEquals(null, htree.lookupFirst(key));
        } finally {
        	
        	store.destroy();
        	
        }		
	}
	
	/**
	 * Insert and remove several copies of the same key.  Test includes
	 * insertions of other key values, unaffected by the removal.
	 */
	public void test_simpleRemoveAll() {
        final IRawStore store = new SimpleMemoryRawStore();

        try {
        
			final Random r = new Random();
			
	        final HTreeIndexMetadata metadata = new HTreeIndexMetadata(UUID.randomUUID());
	
	        metadata.setAddressBits(2);
	        
			final HTree htree = HTree.create(store, metadata);
	
			final byte[] key = new byte[] { 1, 2, 3 };
			final byte[] val = new byte[24]; // some random 24 character value
			r.nextBytes(val);
			
			// insert n entries under that key.
			final int entries = 23;
			for (int i = 0; i < entries; i++) {
				htree.insert(key, val);
			}
	
			final byte[] altkey = new byte[] { 2, 3, 4 };
			final int altentries = 11;
			for (int i = 0; i < altentries; i++) {
				htree.insert(altkey, val);
			}
			
			// this test assumes that everything is in a single bucket page.
			assertEquals(entries+altentries, htree.getEntryCount());
	
			// examine the root.
			final DirectoryPage root = (DirectoryPage) htree.getRoot();
				
			// verify that the expected data were read.
			TestCase3.assertEquals(val, htree.lookupFirst(key));
			
			assertEquals(entries, htree.removeAll(key));
		
			assertEquals(altentries, htree.getEntryCount());
			
			TestCase3.assertEquals(null, htree.lookupFirst(key));
        } finally {
        	
        	store.destroy();
        	
        }		
	}
	
	/**
	 * Test removal with raw records.
	 */
	public void test_simpleRemovalWithRawRecords() {
        
	    final MemStore store = new MemStore(DirectBufferPool.INSTANCE, 5);

        try {
        
		final Random r = new Random();
		
        final HTreeIndexMetadata metadata = new HTreeIndexMetadata(UUID.randomUUID());

        metadata.setAddressBits(2);
        
        metadata.setRawRecords(true);
        metadata.setMaxRecLen(64);

		final HTree htree = HTree.create(store, metadata);

		assertEquals(64, htree.getMaxRecLen());

		assertTrue(htree.rawRecords);

		final byte[] key = new byte[] { 1, 2, 3 };
		final byte[] val = new byte[htree.getMaxRecLen() + 1];
		r.nextBytes(val);
		
		// insert several entries under that key.
		final int ninserts = 15;
		for (int i = 0; i < ninserts; i++) {
			htree.insert(key, val);
		}

		// this test assumes that everything is in a single bucket page.
		assertEquals(ninserts, htree.getEntryCount());
		
		// verify that the expected data were read.
		TestCase3.assertEquals(val, htree.lookupFirst(key));
		
		assertEquals(ninserts, htree.removeAll(key));
	
		assertEquals(0, htree.getEntryCount());
		
		TestCase3.assertEquals(null, htree.lookupFirst(key));

       } finally {
        	
        	store.destroy();
        	
        }
		
	}
}
