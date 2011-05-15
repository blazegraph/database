/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Apr 19, 2011
 */

package com.bigdata.htree;

import junit.framework.TestCase2;

import org.apache.log4j.Level;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.htree.HTree.BucketPage;
import com.bigdata.htree.HTree.DirectoryPage;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for {@link HTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Unit test for the code path where the sole buddy bucket on a
 *          page consists solely of duplicate keys thus forcing the size of the
 *          bucket page to double rather than splitting the page.
 */
public class TestHTree extends TestCase2 {

    /**
     * 
     */
    public TestHTree() {
    }

    /**
     * @param name
     */
    public TestHTree(String name) {
        super(name);
    }
    
    public void test_ctor_correctRejection() {
        
		try {
			new HTree(null/* store */, 2/* addressBits */);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		{
			final IRawStore store = new SimpleMemoryRawStore();
			try {
				new HTree(store, 0/* addressBits */);
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
				new HTree(store, -1/* addressBits */);
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
				new HTree(store, 33/* addressBits */);
				fail("Expecting: " + IllegalArgumentException.class);
			} catch (IllegalArgumentException ex) {
				if (log.isInfoEnabled())
					log.info("Ignoring expected exception: " + ex);
			} finally {
				store.destroy();
			}
		}

		/*
		 * and spot check some valid ctor forms to verify that no exceptions are
		 * thrown.
		 */
		{
			final IRawStore store = new SimpleMemoryRawStore();
			new HTree(store, 1/* addressBits */); // min addressBits
			new HTree(store, 32/* addressBits */); // max addressBits
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

            final HTree htree = new HTree(store, addressBits);
            
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
	 * Test of basic insert, lookup, and split page operations (including when a
	 * new directory page must be introduced) using an address space with only
	 * TWO (2) bits.
	 * 
	 * @see bigdata/src/architecture/htree.xls
	 * 
	 *      TODO Verify that we can store keys having more than 2 bits (or 4
	 *      bits in a 4-bit address space) through a deeper hash tree.
	 */
	public void test_example_addressBits2_01() {

		final int addressBits = 2;

		final IRawStore store = new SimpleMemoryRawStore();

		try {

			final HTree htree = new HTree(store, addressBits);

			// Verify initial conditions.
			assertTrue("store", store == htree.getStore());
			assertEquals("addressBits", addressBits, htree.getAddressBits());

			final DirectoryPage root = htree.getRoot();
			assertEquals(4, root.childRefs.length);
			final BucketPage a = (BucketPage) root.childRefs[0].get();
			assertTrue(a == (BucketPage) root.childRefs[1].get());
			assertTrue(a == (BucketPage) root.childRefs[2].get());
			assertTrue(a == (BucketPage) root.childRefs[3].get());
			assertEquals(2, root.getGlobalDepth());// starts at max.
			assertEquals(0, a.getGlobalDepth());// starts at min.
			
			// verify preconditions.
			assertEquals("nnodes", 1, htree.getNodeCount());
			assertEquals("nleaves", 1, htree.getLeafCount());
			assertEquals("nentries", 0, htree.getEntryCount());
			htree.dump(Level.ALL, System.err, true/* materialize */);
			assertEquals(2, root.getGlobalDepth());
			assertEquals(0, a.getGlobalDepth());
			assertFalse(htree.contains(new byte[] { 0x01 }));
			assertFalse(htree.contains(new byte[] { 0x02 }));
			assertEquals(null,htree.lookupFirst(new byte[] { 0x01 }));
			assertEquals(null,htree.lookupFirst(new byte[] { 0x02 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x01 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));

			/*
			 * 1. Insert a tuple and verify post-conditions. The tuple goes into
			 * an empty buddy bucket with a capacity of one.
			 */
			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
			assertEquals("nnodes", 1, htree.getNodeCount());
			assertEquals("nleaves", 1, htree.getLeafCount());
			assertEquals("nentries", 1, htree.getEntryCount());
			htree.dump(Level.ALL, System.err, true/* materialize */);
			assertEquals(2, root.getGlobalDepth());
			assertEquals(0, a.getGlobalDepth());
			assertTrue(htree.contains(new byte[] { 0x01 }));
			assertFalse(htree.contains(new byte[] { 0x02 }));
			assertEquals(new byte[] { 0x01 }, htree
					.lookupFirst(new byte[] { 0x01 }));
			assertEquals(null,htree.lookupFirst(new byte[] { 0x02 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] { new byte[] { 0x01 } }, htree
							.lookupAll(new byte[] { 0x01 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));

			/*
			 * 2. Insert a duplicate key. Since the localDepth of the bucket
			 * page is zero, each buddy bucket on the page can only accept one
			 * entry and this will force a split of the buddy bucket. That means
			 * that a new bucket page will be allocated, the pointers in the
			 * parent will be updated to link in the new buck page, and the
			 * buddy buckets will be redistributed among the old and new bucket
			 * page.
			 * 
			 * Note: We do not know the order of the tuples in the bucket so
			 * lookupFirst() is difficult to test when there are tuples for the
			 * same key with different values.
			 */
			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
			assertEquals("nnodes", 1, htree.getNodeCount());
			assertEquals("nleaves", 2, htree.getLeafCount());
			assertEquals("nentries", 2, htree.getEntryCount());
			htree.dump(Level.ALL, System.err, true/* materialize */);
			assertTrue(root == htree.getRoot());
			assertEquals(4, root.childRefs.length);
			final BucketPage b = (BucketPage) root.childRefs[2].get();
			assertTrue(a == (BucketPage) root.childRefs[0].get());
			assertTrue(a == (BucketPage) root.childRefs[1].get());
			assertTrue(b == (BucketPage) root.childRefs[2].get());
			assertTrue(b == (BucketPage) root.childRefs[3].get());
			assertEquals(2, root.getGlobalDepth());
			assertEquals(1, a.getGlobalDepth());// localDepth has increased.
			assertEquals(1, b.getGlobalDepth());// localDepth is same as [a].
			assertTrue(htree.contains(new byte[] { 0x01 }));
			assertFalse(htree.contains(new byte[] { 0x02 }));
			assertEquals(new byte[] { 0x01 }, htree
					.lookupFirst(new byte[] { 0x01 }));
			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
			AbstractBTreeTestCase.assertSameIterator(
					//
					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 } },
					htree.lookupAll(new byte[] { 0x01 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));

			/*
			 * 3. Insert another duplicate key. This forces another split.
			 * 
			 * TODO Could check the #of entries on a bucket page and even in
			 * each bucket of the bucket pages.
			 */
			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
			assertEquals("nnodes", 1, htree.getNodeCount());
			assertEquals("nleaves", 3, htree.getLeafCount());
			assertEquals("nentries", 3, htree.getEntryCount());
			htree.dump(Level.ALL, System.err, true/* materialize */);
			assertTrue(root == htree.getRoot());
			assertEquals(4, root.childRefs.length);
			final BucketPage c = (BucketPage) root.childRefs[1].get();
			assertTrue(a == (BucketPage) root.childRefs[0].get());
			assertTrue(c == (BucketPage) root.childRefs[1].get());
			assertTrue(b == (BucketPage) root.childRefs[2].get());
			assertTrue(b == (BucketPage) root.childRefs[3].get());
			assertEquals(2, root.getGlobalDepth());
			assertEquals(2, a.getGlobalDepth());// localDepth has increased.
			assertEquals(1, b.getGlobalDepth());// 
			assertEquals(2, c.getGlobalDepth());// localDepth is same as [a].
			assertTrue(htree.contains(new byte[] { 0x01 }));
			assertFalse(htree.contains(new byte[] { 0x02 }));
			assertEquals(new byte[] { 0x01 }, htree
					.lookupFirst(new byte[] { 0x01 }));
			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
			AbstractBTreeTestCase.assertSameIterator(
			//
					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 },
							new byte[] { 0x01 } }, htree
							.lookupAll(new byte[] { 0x01 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));

			/*
			 * 4. Insert another duplicate key. The buddy bucket is now full
			 * again. It is only the only buddy bucket on the page.
			 * 
			 * Note: At this point if we insert another duplicate key the page
			 * size will be doubled because all keys on the page are duplicates
			 * and there is only one buddy bucket on the page.
			 * 
			 * However, rather than test the doubling of the page size, this
			 * example is written to test a split where global depth == local
			 * depth, which will cause the introduction of a new directory page
			 * in the htree.
			 */
			htree.insert(new byte[] { 0x01 }, new byte[] { 0x01 });
			assertEquals("nnodes", 1, htree.getNodeCount());
			assertEquals("nleaves", 3, htree.getLeafCount());
			assertEquals("nentries", 4, htree.getEntryCount());
			htree.dump(Level.ALL, System.err, true/* materialize */);
			assertTrue(root == htree.getRoot());
			assertEquals(4, root.childRefs.length);
			assertTrue(a == (BucketPage) root.childRefs[0].get());
			assertTrue(c == (BucketPage) root.childRefs[1].get());
			assertTrue(b == (BucketPage) root.childRefs[2].get());
			assertTrue(b == (BucketPage) root.childRefs[3].get());
			assertEquals(2, root.getGlobalDepth());
			assertEquals(2, a.getGlobalDepth());// localDepth has increased.
			assertEquals(1, b.getGlobalDepth());// 
			assertEquals(2, c.getGlobalDepth());// localDepth is same as [a].
			assertTrue(htree.contains(new byte[] { 0x01 }));
			assertFalse(htree.contains(new byte[] { 0x02 }));
			assertEquals(new byte[] { 0x01 }, htree
					.lookupFirst(new byte[] { 0x01 }));
			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
			AbstractBTreeTestCase.assertSameIterator(
			//
					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 },
							new byte[] { 0x01 }, new byte[] { 0x01 } }, htree
							.lookupAll(new byte[] { 0x01 }));
			AbstractBTreeTestCase.assertSameIterator(//
					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));

			/*
			 * FIXME We MUST NOT decrease the localDepth of (a) since that would
			 * place duplicate keys into different buckets (lost retrival).
			 * Since (a) can not have its localDepth decreased, we need to have
			 * localDepth(d=2) with one pointer to (a) to get localDepth(a:=2).
			 * That makes this a very complicated example. It would be a lot
			 * easier if we started with distinct keys such that the keys in (a)
			 * could be redistributed. This case should be reserved for later
			 * once the structural mutations are better understood.
			 * 
			 * 5. Insert 0x20. This key is directed into the same buddy bucket
			 * as the 0x01 keys that we have been inserting. That buddy bucket
			 * is already full and, further, it is the only buddy bucket on the
			 * page. Since we are not inserting a duplicate key we can split the
			 * buddy bucket (rather than doubling the size of the bucket).
			 * However, since global depth == local depth (i.e., only one buddy
			 * bucket on the page), this split will introduce a new directory
			 * page. The new directory page basically codes for the additional
			 * prefix bits required to differentiate the two distinct keys such
			 * that they are directed into the appropriate buckets.
			 * 
			 * The new directory page (d) is inserted one level below the root
			 * on the path leading to (a). The new directory must have a local
			 * depth of ONE (1), since it will add a one bit distinction. Since
			 * we know the global depth of the root and the local depth of the
			 * new directory page, we solve for npointers := 1 << (globalDepth -
			 * localDepth). This tells us that we will have 2 pointers in the
			 * root to the new directory page.
			 * 
			 * The precondition state of root is {a,c,b,b}. Since we know that
			 * we need npointers:=2 pointers to (d), this means that we will
			 * copy the {a,c} references into (d) and replace those references
			 * in the root with references to (d). The root is now {d,d,b,b}. d
			 * is now {a,a;c,c}. Since d has a local depth of 1 and address bits
			 * of 2, it is comprised of two buddy hash tables {a,a} and {c,c}.
			 * 
			 * Linking in (d) has also changes the local depths of (a) and (c).
			 * Since they each now have npointers:=2, their localDepth is has
			 * been reduced from TWO (2) to ONE (1) (and their transient cached
			 * depth values must be either invalidated or recomputed). Note that
			 * the local depth of (d) and its children (a,c)) are ONE after this
			 * operation so if we force another split in (a) that will force a
			 * split in (d).
			 * 
			 * Having introduced a new directory page into the hash tree, we now
			 * retry the insert. Once again, the insert is directed into (a).
			 * Since (a) is still full it is split. (d) is now the parent of
			 * (a). Once again, we have globalDepth(d=1)==localDepth(a=1) so we
			 * need to split (d). However, since localDepth(d=1) is less than
			 * globalDepth(root=2) we can split the buddy hash tables in (d).
			 * This will require us to allocate a new page (f) which will be the
			 * right sibling of (d). There are TWO (2) buddy hash tables in (d).
			 * They are now redistributed between (d) and (f). We also have to
			 * update the pointers to (d) in the parent such that 1/2 of them
			 * point to the new right sibling of (d). Since we have changed the
			 * #of pointers to (d) (from 2 to 1) the local depth of (d) (and of
			 * f) is now TWO (2). Since globalDepth(d=2) is greater than
			 * localDepth(a=1) we can now split (a) into (a,e), redistribute the
			 * tuples in the sole buddy page (a) between (a,e) and update the
			 * pointers in (d) to (a,a,e,e).
			 * 
			 * Having split (d) into (d,f), we now retry the insert. This time
			 * the insert is directed into (e). There is room in (e) (it is
			 * empty) and the tuple is inserted without further mutation to the
			 * structure of the hash tree.
			 * 
			 * TODO At this point we should also prune the buckets [b] and [c]
			 * since they are empty and replace them with null references.
			 * 
			 * TODO The #of new directory levels which have to be introduced
			 * here is a function of the #of prefix bits which have to be
			 * consumed before a distinction can be made between the existing
			 * key (0x01) and the new key (0x20). With an address space of 2
			 * bits, each directory level examines the next 2-bits of the key.
			 * The key (x20) was chosen since the distinction can be made by
			 * adding only one directory level (the keys differ in the first 4
			 * bits). [Do an alternative example which requires recursive splits
			 * in order to verify that we reenter the logic correctly each time.
			 * E.g., by inserting 0x02 rather than 0x20.]
			 * 
			 * TODO Do an example in which we explore an insert which introduces
			 * a new directory level in a 3-level tree. This should be a driven
			 * by a single insert so we can examine in depth how the new
			 * directory is introduced and verify whether it is introduced below
			 * the root or above the bucket. [I believe that it is introduced
			 * immediately below below the root. Note that a balanced tree,
			 * e.g., a B-Tree, introduces the new level above the root. However,
			 * the HTree is intended to be unbalanced in order to optimize
			 * storage and access times to the parts of the index which
			 * correspond to unequal distributions in the hash codes.]
			 */
//			htree.insert(new byte[] { 0x20 }, new byte[] { 0x20 });
//			assertEquals("nnodes", 2, htree.getNodeCount());
//			assertEquals("nleaves", 4, htree.getLeafCount());
//			assertEquals("nentries", 5, htree.getEntryCount());
//			htree.dump(Level.ALL, System.err, true/* materialize */);
//			assertTrue(root == htree.getRoot());
//			assertEquals(4, root.childRefs.length);
//			final DirectoryPage d = (DirectoryPage)root.childRefs[0].get();
//			assertTrue(d == (DirectoryPage) root.childRefs[0].get());
//			assertTrue(d == (DirectoryPage) root.childRefs[1].get());
//			assertTrue(b == (BucketPage) root.childRefs[2].get());
//			assertTrue(b == (BucketPage) root.childRefs[3].get());
//			assertEquals(4, d.childRefs.length);
//			final BucketPage e = (BucketPage)d.childRefs[1].get();
//			assertTrue(a == (BucketPage) d.childRefs[0].get());
//			assertTrue(e == (BucketPage) d.childRefs[1].get());
//			assertTrue(c == (BucketPage) d.childRefs[2].get());
//			assertTrue(c == (BucketPage) d.childRefs[3].get());
//			assertEquals(2, root.getGlobalDepth());
//			assertEquals(2, d.getGlobalDepth());
//			assertEquals(2, a.getGlobalDepth());// unchanged
//			assertEquals(2, e.getGlobalDepth());// same as [a].
//			assertEquals(1, b.getGlobalDepth());// unchanged.
//			assertEquals(2, c.getGlobalDepth());// unchanged.
//			assertTrue(htree.contains(new byte[] { 0x01 }));
//			assertFalse(htree.contains(new byte[] { 0x02 }));
//			assertEquals(new byte[] { 0x01 }, htree
//					.lookupFirst(new byte[] { 0x01 }));
//			assertNull(htree.lookupFirst(new byte[] { 0x02 }));
//			AbstractBTreeTestCase.assertSameIterator(
//			//
//					new byte[][] { new byte[] { 0x01 }, new byte[] { 0x01 },
//							new byte[] { 0x01 }, new byte[] { 0x01 } }, htree
//							.lookupAll(new byte[] { 0x01 }));
//			AbstractBTreeTestCase.assertSameIterator(//
//					new byte[][] {}, htree.lookupAll(new byte[] { 0x02 }));

			// TODO REMOVE (or test suite for remove).
			
			// TODO Continue progression here? 

		} finally {

			store.destroy();

		}

	}

	/**
	 * Work through a detailed example.
	 * 
	 * @see bigdata/src/architecture/htree.xls
	 */
	public void test_example_addressBits2_02() {

		fail("write test");

	}

	/**
	 * Work through a detailed example in which we have an elided bucket page
	 * or directory page because nothing has been inserted into that part of
	 * the address space.
	 */
	public void test_example_addressBits2_elidedPages() {

		fail("write test");

	}

}
