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
package com.bigdata.htree;

import java.util.Random;
import java.util.UUID;

import junit.framework.AssertionFailedError;

import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.util.PseudoRandom;

/**
 * Unit tests for the close/checkpoint/reopen protocol designed to manage the
 * resource burden of indices without invalidating the index objects (indices
 * opens can be reopened as long as their backing store remains available).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReopen extends AbstractHTreeTestCase {

    /**
     * 
     */
    public TestReopen() {
    }

    /**
     * @param name
     */
    public TestReopen(String name) {
        super(name);
    }

    /**
     * Test close on a new tree - should force the root to the store since a new
     * root is dirty (if empty). reopen should then reload the empty root and on
     * life goes.
     */
    public void test_reopen01() {

        final IRawStore store = new SimpleMemoryRawStore();

		try {

			/*
			 * The htree under test.
			 */
			final HTree htree = getHTree(store, 2/*addressBits*/);

			assertTrue(htree.isOpen());

			htree.close();

			assertFalse(htree.isOpen());

			try {
				htree.close();
				fail("Expecting: " + IllegalStateException.class);
			} catch (IllegalStateException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			}

			assertNotNull(htree.getRoot());

			assertTrue(htree.isOpen());

		} finally {

			store.destroy();

		}

    }

    /**
     * Test with a btree containing both branch nodes and leaves.
     */
    public void test_reopen02() {
     
        final IRawStore store = new SimpleMemoryRawStore();

        try {

			/*
			 * The htree under test.
			 */
			final HTree htree = getHTree(store, 2/* addressBits */);

			final byte[] k1 = new byte[] { 0x10 };
			final byte[] k2 = new byte[] { 0x11 };
			final byte[] k3 = new byte[] { 0x20 };
			final byte[] k4 = new byte[] { 0x21 };

			final byte[] v1 = new byte[] { 0x10 };
			final byte[] v2 = new byte[] { 0x11 };
			final byte[] v3 = new byte[] { 0x20 };
			final byte[] v4 = new byte[] { 0x21 };

			htree.insert(k1, v1);
			htree.insert(k2, v2);
			htree.insert(k3, v3);
			htree.insert(k4, v4);

			// dump after inserts.
			if (log.isInfoEnabled())
				log.info("Dump after inserts: \n" + htree.PP());

			// checkpoint the index.
			htree.writeCheckpoint();

			// force close.
			htree.close();

			// force reopen.
			assertNotNull(htree.getRoot());
			assertTrue(htree.isOpen());

			// verify data still there.
			assertEquals(v1, htree.lookupFirst(k1));
			assertEquals(v2, htree.lookupFirst(k2));
			assertEquals(v3, htree.lookupFirst(k3));
			assertEquals(v4, htree.lookupFirst(k4));
			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4 },
					htree.values());

			// dump after reopen.
			if (log.isInfoEnabled())
				log.info("Dump after reopen: \n" + htree.PP());

			// reload the tree from the store.
			final HTree htree2 = HTree.load(store, htree.getCheckpoint()
					.getCheckpointAddr(), true/* readOnly */);

			// verify data still there.
			assertEquals(v1, htree2.lookupFirst(k1));
			assertEquals(v2, htree2.lookupFirst(k2));
			assertEquals(v3, htree2.lookupFirst(k3));
			assertEquals(v4, htree2.lookupFirst(k4));
			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4 },
					htree2.values());

		} finally {

			store.destroy();

		}

	}

	/**
	 * Stress test comparison with ground truth htree when {@link HTree#close()}
	 * is randomly invoked during mutation operations.
	 */
	public void test_reopen03() {

		final Random r = new Random();

        final IRawStore store = new MemStore(DirectBufferPool.INSTANCE);

		try {

			final UUID indexUUID = UUID.randomUUID();

			/*
			 * The btree used to maintain ground truth.
			 * 
			 * Note: the fixture factory is NOT used here since the stress test
			 * will eventually overflow the hard reference queue and begin
			 * evicting nodes and leaves onto the store.
			 */
			final HTree htree ;
			final HTree groundTruth;
			{

				final HTreeIndexMetadata gmd = new HTreeIndexMetadata(indexUUID);

				gmd.setWriteRetentionQueueCapacity(100);
				gmd.setWriteRetentionQueueScan(5); // Must be LTE capacity.

		        gmd.setAddressBits(10);

				groundTruth = HTree.create(store, gmd);
				
				final HTreeIndexMetadata hmd = new HTreeIndexMetadata(indexUUID);

				hmd.setWriteRetentionQueueCapacity(30);
				hmd.setWriteRetentionQueueScan(1); // Must be LTE capacity.

				hmd.setAddressBits(10);
				htree = HTree.create(store, hmd);

			}

			final int limit = 20000; // 20000
			final int keylen = 1 + r.nextInt(12);
			
			for (int i = 0; i < limit; i++) {

				final int n = r.nextInt(100);

				if (n < 5) {
					/* periodically force a checkpoint + close of the btree. */
					if (htree.isOpen()) {
						htree.writeCheckpoint();
						htree.close();
					}
				} else if (n < 20) {
					// remove an entry.
					final byte[] key = new byte[keylen];
					r.nextBytes(key);
					htree.remove(key);
					groundTruth.remove(key);
				} else {
					// add an entry.
					final byte[] key = new byte[keylen];
					r.nextBytes(key);
					htree.insert(key, key);
					groundTruth.insert(key, key);
				}

			}

			try {
				assertSameHTree(groundTruth, htree);
			} catch (AssertionFailedError ae) {
				throw ae;
			}

		} finally {

			store.destroy();

		}
        
    }
	
	public void test_reopenPseudoRandom() {

		final Random r = new Random();
		final PseudoRandom psr = new PseudoRandom(2000, 13);
		final PseudoRandom psr2 = new PseudoRandom(4000, 13);

        final IRawStore store = new MemStore(DirectBufferPool.INSTANCE);

		try {

			final UUID indexUUID = UUID.randomUUID();

			/*
			 * The btree used to maintain ground truth.
			 * 
			 * Note: the fixture factory is NOT used here since the stress test
			 * will eventually overflow the hard reference queue and begin
			 * evicting nodes and leaves onto the store.
			 */
			final HTree htree ;
			final HTree groundTruth;
			{

				final HTreeIndexMetadata gmd = new HTreeIndexMetadata(indexUUID);

				gmd.setWriteRetentionQueueCapacity(100);
				gmd.setWriteRetentionQueueScan(5); // Must be LTE capacity.

		        gmd.setAddressBits(10);

				groundTruth = HTree.create(store, gmd);
				
				final HTreeIndexMetadata hmd = new HTreeIndexMetadata(indexUUID);

				hmd.setWriteRetentionQueueCapacity(30);
				hmd.setWriteRetentionQueueScan(1); // Must be LTE capacity.

				hmd.setAddressBits(10);
				htree = HTree.create(store, hmd);

			}
			
			final int limit = 20000; // 20000
            final int keylen = r.nextInt(12) + 1;
//			final int keylen = 8;
            
			for (int i = 0; i < limit; i++) {

				final int n = psr.next(); // r.nextInt(100);

				if (n < 5) {
					/* periodically force a checkpoint + close of the btree. */
					if (htree.isOpen()) {
						htree.writeCheckpoint();
						htree.close();
					} 
					
				} else if (n < 20) {
					// remove an entry.
					final byte[] key = new byte[keylen];
					psr2.nextBytes(key, i);
					//r.nextBytes(key);
					htree.remove(key);
					groundTruth.remove(key);
				} else {
					// add an entry.
					final byte[] key = new byte[keylen];
					psr2.nextBytes(key, i);
					// r.nextBytes(key);
					htree.insert(key, key);
					groundTruth.insert(key, key);
				}

			}

			try {
				assertSameHTree(groundTruth, htree);
			} catch (AssertionFailedError ae) {
				throw ae;
			}

		} finally {

			store.destroy();

		}
        
    }
	
	public void test_multipleTreesPseudoRandom() {

		final PseudoRandom psr = new PseudoRandom(100, 13);
		final PseudoRandom psr2 = new PseudoRandom(255, 13);

        final IRawStore store = new MemStore(DirectBufferPool.INSTANCE);

		try {

			final UUID indexUUID = UUID.randomUUID();

			/*
			 * The btree used to maintain ground truth.
			 * 
			 * Note: the fixture factory is NOT used here since the stress test
			 * will eventually overflow the hard reference queue and begin
			 * evicting nodes and leaves onto the store.
			 */
			final HTree htree ;
			final HTree groundTruth;
			{

				final HTreeIndexMetadata gmd = new HTreeIndexMetadata(indexUUID);

				gmd.setWriteRetentionQueueCapacity(100);
				gmd.setWriteRetentionQueueScan(5); // Must be LTE capacity.

		        gmd.setAddressBits(3); // 3

				groundTruth = HTree.create(store, gmd);
				
				final HTreeIndexMetadata hmd = new HTreeIndexMetadata(indexUUID);

				hmd.setWriteRetentionQueueCapacity(30);
				hmd.setWriteRetentionQueueScan(1); // Must be LTE capacity.

				hmd.setAddressBits(8); // 4,6
				htree = HTree.create(store, hmd);

			}

			final int limit = 20000; // 20000
			final int keylen = 8; // r.nextInt(1 + 12);
			int entries = 0;
			for (int i = 0; i < limit; i++) {

				final int n = psr.next(); // r.nextInt(100);

				if (n < 20) {
					// remove an entry.
					final byte[] key = new byte[keylen];
					psr2.nextBytes(key, i);
					byte[] r1 = htree.remove(key);
					byte[] r2 = groundTruth.remove(key);
					if (r1 == null && r2 != null || r1 != null && r2 == null)
						fail("Inconsistency on remove!");
					if (r1 != null) entries--;
				} else {
					// add an entry.
					final byte[] key = new byte[keylen];
					psr2.nextBytes(key, i);
					htree.insert(key, key);
					groundTruth.insert(key, key);
					entries++;
				}

			}
			int gcnt = count(groundTruth.getRoot().getTuples());
			int tcnt = count(htree.getRoot().getTuples());
			assertTrue(gcnt == entries && gcnt == tcnt);
			try {
				assertSameIterator(groundTruth.getRoot().getTuples(), htree.getRoot().getTuples());
			} catch (AssertionFailedError afe) {
//				FileWriter writer;
//				try {
//					writer = new FileWriter("/tmp/PP.txt");
//					writer.write(htree.PP(false));
//					writer.write(groundTruth.PP(false));
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
				
				throw afe;
			}

		} finally {

			store.destroy();

		}
        
    }

	private int count(ITupleIterator tuples) {
		int ret = 0;
		while (tuples.hasNext()) {
			tuples.next();
			ret++;
		}
		return ret;
	}

}
