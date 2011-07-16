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
package com.bigdata.htree;

import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

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
			final HTree btree;
			{
				final IndexMetadata md = new IndexMetadata(UUID.randomUUID());

				md.setAddressBits(2);

				btree = HTree.create(store, md);

			}

			assertTrue(btree.isOpen());

			btree.close();

			assertFalse(btree.isOpen());

			try {
				btree.close();
				fail("Expecting: " + IllegalStateException.class);
			} catch (IllegalStateException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			}

			assertNotNull(btree.getRoot());

			assertTrue(btree.isOpen());

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
			final HTree btree;
			{
				final IndexMetadata md = new IndexMetadata(UUID.randomUUID());

				md.setBranchingFactor(3);

				btree = HTree.create(store, md);

			}

			final byte[] k1 = new byte[] { 0x10 };
			final byte[] k2 = new byte[] { 0x11 };
			final byte[] k3 = new byte[] { 0x20 };
			final byte[] k4 = new byte[] { 0x21 };

			final byte[] v1 = new byte[] { 0x10 };
			final byte[] v2 = new byte[] { 0x11 };
			final byte[] v3 = new byte[] { 0x20 };
			final byte[] v4 = new byte[] { 0x21 };

			btree.insert(k1, v1);
			btree.insert(k2, v2);
			btree.insert(k3, v3);
			btree.insert(k4, v4);

			// dump after inserts.
			if (log.isInfoEnabled())
				log.info("Dump after inserts: \n" + btree.PP());

			// checkpoint the index.
			btree.writeCheckpoint();

			// force close.
			btree.close();

			// force reopen.
			assertNotNull(btree.getRoot());
			assertTrue(btree.isOpen());

			// force materialization of the leaves.
			btree.lookupFirst(k1);
			btree.lookupFirst(k2);
			btree.lookupFirst(k3);
			btree.lookupFirst(k4);

			// dump after reopen.
			if (log.isInfoEnabled())
				log.info("Dump after reopen: \n" + btree.PP());

			// reload the tree from the store.
			final HTree btree2 = HTree.load(store, btree.getCheckpoint()
					.getCheckpointAddr(), true/* readOnly */);

			// verify same data.
			assertSameHTree(btree, btree2);

		} finally {

			store.destroy();

		}

	}

	/**
	 * Stress test comparison with ground truth btree when {@link HTree#close()}
	 * is randomly invoked during mutation operations.
	 */
	public void test_reopen03() {

		final Random r = new Random();

		final IRawStore store = new SimpleMemoryRawStore();

		try {

			final UUID indexUUID = UUID.randomUUID();

			/*
			 * The btree under test.
			 * 
			 * Note: the fixture factory is NOT used since this node evictions
			 * will be forced when this tree is closed (node evictions are not
			 * permitted by the default fixture factory).
			 */
			final HTree btree;
			{

				final IndexMetadata md = new IndexMetadata(indexUUID);

				md.setAddressBits(2);

				btree = HTree.create(store, md);

			}

			/*
			 * The btree used to maintain ground truth.
			 * 
			 * Note: the fixture factory is NOT used here since the stress test
			 * will eventually overflow the hard reference queue and begin
			 * evicting nodes and leaves onto the store.
			 */
			final HTree groundTruth;
			{

				final IndexMetadata md = new IndexMetadata(indexUUID);

				md.setAddressBits(2);

				groundTruth = HTree.create(store, md);

			}

			final int limit = 10000;
			final int keylen = r.nextInt(1 + 12);

			for (int i = 0; i < limit; i++) {

				final int n = r.nextInt(100);

				if (n < 5) {
					/* periodically force a checkpoint + close of the btree. */
					if (btree.isOpen()) {
						// System.err.println("checkpoint+close");
						btree.writeCheckpoint();
						btree.close();
						// assertSameBTree(groundTruth, btree);
					}
				} else if (n < 20) {
					// remove an entry.
					final byte[] key = new byte[keylen];
					r.nextBytes(key);
					btree.remove(key);
					groundTruth.remove(key);
					// assertSameBTree(groundTruth, btree);
				} else {
					// add an entry.
					final byte[] key = new byte[keylen];
					r.nextBytes(key);
					btree.insert(key, key);
					groundTruth.insert(key, key);
					// assertSameBTree(groundTruth, btree);
				}

			}

			assertSameHTree(groundTruth, btree);

		} finally {

			store.destroy();

		}
        
    }

}
