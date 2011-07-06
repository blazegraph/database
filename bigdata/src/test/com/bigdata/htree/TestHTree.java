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
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.htree.HTree.BucketPage;
import com.bigdata.htree.HTree.DirectoryPage;
import com.bigdata.htree.data.MockBucketData;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for {@link HTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Work through a detailed example in which we have an elided
 *          bucket page or directory page because nothing has been inserted into
 *          that part of the address space.
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

		// address bits is too large.
		{
			final IRawStore store = new SimpleMemoryRawStore();
			try {
				new HTree(store, 17/* addressBits */);
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
			new HTree(store, 16/* addressBits */); // max addressBits
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
     * Test of basic operations using a TWO (2) bit address space inserting the
     * key sequence {1,2,3,4}.
     * <p>
     * Note: This test stops after the 4th tuple insert. At that point the
     * {@link HTree} is in a state where inserting the next tuple in the
     * sequence (5) will force a new directory page to be introduced. The next
     * two unit tests examine in depth the logic which introduces that directory
     * page. They are followed by another unit test which picks up on this same
     * insert sequences and goes beyond the introduction of the new directory
     * page.
     * 
     * @see bigdata/src/architecture/htree.xls#example1
     * @see #test_example1_splitDir_addressBits2_splitBits1()
     * @see #test_example1_splitDir_addressBits2_splitBits2()
     */
    public void test_example1_addressBits2_insert_1_2_3_4() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            
            // a key which we never insert and which should never be found by lookup.
            final byte[] unused = new byte[]{-127};
            
            final HTree htree = new HTree(store, addressBits, false/* rawRecords */);
            
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

            /**
             * verify preconditions.
             * 
             * <pre>
             * root := [2] (a,a,a,a)   //
             * a    := [0]   (-;-;-;-) //
             * </pre>
             */
            assertEquals("nnodes", 1, htree.getNodeCount());
            assertEquals("nleaves", 1, htree.getLeafCount());
            assertEquals("nentries", 0, htree.getEntryCount());
            htree.dump(Level.ALL, System.err, true/* materialize */);
            assertEquals(2, root.getGlobalDepth());
            assertEquals(0, a.getGlobalDepth());
            assertEquals(0, a.getKeyCount());
            assertEquals(0, a.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    a);
            assertFalse(htree.contains(k1));
            assertFalse(htree.contains(unused));
            assertEquals(null, htree.lookupFirst(k1));
            assertEquals(null, htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(//
                    new byte[][] {}, htree.lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(//
                    new byte[][] {}, htree.lookupAll(unused));

            /**
             * 1. Insert a tuple (0x01) and verify post-conditions. The tuple
             * goes into an empty buddy bucket with a capacity of one.
             * 
             * <pre>
             * root := [2] (a,a,a,a)   //
             * a    := [0]   (1;-;-;-) //
             * </pre>
             */
            htree.insert(k1, v1);
            assertEquals("nnodes", 1, htree.getNodeCount());
            assertEquals("nleaves", 1, htree.getLeafCount());
            assertEquals("nentries", 1, htree.getEntryCount());
            htree.dump(Level.ALL, System.err, true/* materialize */);
            assertEquals(2, root.getGlobalDepth());
            assertEquals(0, a.getGlobalDepth());
            assertEquals(1, a.getKeyCount());
            assertEquals(1, a.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(1, new byte[][] { // keys
                            k1, null, null, null }),//
                    new ReadOnlyValuesRaba(1, new byte[][] { // vals
                            v1, null, null, null})),//
                    a);
            assertTrue(htree.contains(k1));
            assertFalse(htree.contains(unused));
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(null,htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(//
                    new byte[][] { v1 }, htree.lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(//
                    new byte[][] {}, htree.lookupAll(unused));

            /**
             * 2. Insert a tuple (0x02). Since the root directory is only paying
             * attention to the 2 MSB bits, this will be hashed into the same
             * buddy hash bucket as the first key. Since the localDepth of the
             * bucket page is zero, each buddy bucket on the page can only
             * accept one entry and this will force a split of the buddy bucket.
             * That means that a new bucket page will be allocated, the pointers
             * in the parent will be updated to link in the new bucket page, and
             * the buddy buckets will be redistributed among the old and new
             * bucket page.
             * <pre> 
             * root := [2] (a,a,b,b)   //
             * a    := [1]   (1,2;-,-) //
             * b    := [1]   (-,-;-,-) //
             * </pre>
             */
            htree.insert(k2, v2);
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
            assertEquals(2, a.getKeyCount());
            assertEquals(2, a.getValueCount());
            assertEquals(0, b.getKeyCount());
            assertEquals(0, b.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k1, k2, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v1, k2, null, null})),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);
            assertTrue(htree.contains(k1));
            assertTrue(htree.contains(k2));
            assertFalse(htree.contains(unused));
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertNull(htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v1 }, htree
                    .lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v2 }, htree
                    .lookupAll(k2));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] {}, htree
                    .lookupAll(unused));

            /**
             * 3. Insert 0x03. This forces another split.
             * <pre> 
             * root := [2] (a,c,b,b)   //
             * a    := [2]   (1,2,3,-) //
             * c    := [2]   (-,-,-,-) //
             * b    := [1]   (-,-;-,-) //
             * </pre>
             */
            htree.insert(k3, v3);
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
            assertEquals(3, a.getKeyCount());
            assertEquals(3, a.getValueCount());
            assertEquals(0, b.getKeyCount());
            assertEquals(0, b.getValueCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, c.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(3, new byte[][] { // keys
                            k1, k2, k3, null }),//
                    new ReadOnlyValuesRaba(3, new byte[][] { // vals
                            v1, k2, k3, null})),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertTrue(htree.contains(k1));
            assertTrue(htree.contains(k2));
            assertTrue(htree.contains(k3));
            assertFalse(htree.contains(unused));
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertNull(htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v1 }, htree
                    .lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v2 }, htree
                    .lookupAll(k2));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v3 }, htree
                    .lookupAll(k3));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] {}, htree
                    .lookupAll(unused));

            /**
             * 4. Insert 0x04. This goes into the same buddy bucket. The buddy
             * bucket is now full again. It is only the only buddy bucket on the
             * page, e.g., global depth == local depth.
             * 
             * Note: Inserting another tuple into this buddy bucket will not
             * only cause it to split but it will also introduce a new directory
             * page into the hash tree.
             * 
             * <pre> 
             * root := [2] (a,c,b,b)   //
             * a    := [2]   (1,2,3,4) //
             * c    := [2]   (-,-,-,-) //
             * b    := [1]   (-,-;-,-) //
             * </pre>
             */
            htree.insert(k4, v4);
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
            assertEquals(4, a.getKeyCount());
            assertEquals(4, a.getValueCount());
            assertEquals(0, b.getKeyCount());
            assertEquals(0, b.getValueCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, c.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, k2, k3, k4 })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertTrue(htree.contains(k1));
            assertTrue(htree.contains(k2));
            assertTrue(htree.contains(k3));
            assertTrue(htree.contains(k4));
            assertFalse(htree.contains(unused));
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));
            assertNull(htree.lookupFirst(unused));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v1 }, htree
                    .lookupAll(k1));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v2 }, htree
                    .lookupAll(k2));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v3 }, htree
                    .lookupAll(k3));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] { v4 }, htree
                    .lookupAll(k4));
            AbstractBTreeTestCase.assertSameIterator(new byte[][] {}, htree
                    .lookupAll(unused));

        } finally {

            store.destroy();

        }

    }

    /**
     * Unit test for
     * {@link HTree#addLevel(DirectoryPage, int, int, com.bigdata.htree.HTree.AbstractPage)}
     * .
     */
    public void test_example1_splitDir_addressBits2_splitBits1() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k20 = new byte[]{0x20};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v20 = new byte[]{0x20};
            
            final HTree htree = new HTree(store, addressBits);

            final DirectoryPage root = htree.getRoot();
            
            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);
            
            /*
             * Verify preconditions for the unit test.
             */
            assertTrue(root == htree.getRoot());
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            final BucketPage b = (BucketPage) root.childRefs[2].get();
            assertTrue(a == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);

            /*
             * Force a split of the root directory page. Note that (a) has the
             * same depth as the directory page we want to split, which is a
             * precondition for being able to split the directory.
             */

            // verify that [a] will not accept an insert.
            assertFalse(a
                    .insert(k20, v20, root/* parent */, 0/* buddyOffset */));
            
            // Add a new level below the root in the path to the full child.
            htree
                    .addLevel(root/* oldParent */, 0/* buddyOffset */, 1/* splitBits */);

            /*
             * Verify post-conditions.
             */
            
            assertEquals("nnodes", 2, htree.getNodeCount());
            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(d == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(a == d.childRefs[1].get());
            assertTrue(c == d.childRefs[2].get());
            assertTrue(c == d.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(1, d.globalDepth);
            assertEquals(0, a.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);

        } finally {

            store.destroy();
            
        }

    }
    
    /**
     * Unit test for
     * {@link HTree#addLevel(DirectoryPage, int, int, com.bigdata.htree.HTree.AbstractPage)}
     * .
     */
    public void test_example1_splitDir_addressBits2_splitBits2() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k20 = new byte[]{0x20};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v20 = new byte[]{0x20};
            
            final HTree htree = new HTree(store, addressBits);

            final DirectoryPage root = htree.getRoot();
            
            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);
            
            /*
             * Verify preconditions for the unit test.
             */
            assertTrue(root == htree.getRoot());
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            final BucketPage b = (BucketPage) root.childRefs[2].get();
            assertTrue(a == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);

            /*
             * Force a split of the root directory page. Note that (a) has the
             * same depth as the directory page we want to split, which is a
             * precondition for being able to split the directory.
             */

            // verify that [a] will not accept an insert.
            assertFalse(a
                    .insert(k20, v20, root/* parent */, 0/* buddyOffset */));
            
            // Add a new level below the root in the path to the full child.
            htree
                    .addLevel(root/* oldParent */, 0/* buddyOffset */, 2/* splitBits */);
            
            /*
             * Verify post-conditions.
             */
            
            assertEquals("nnodes", 2, htree.getNodeCount());
            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(a == d.childRefs[1].get());
            assertTrue(a == d.childRefs[2].get());
            assertTrue(a == d.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, d.globalDepth);
            assertEquals(0, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);

        } finally {

            store.destroy();
            
        }

    }

    /**
     * A unit test which continues the scenario begun above (insert 1,2,3,4),
     * working through the structural changes required until we can finally
     * insert the key (5) into the {@link HTree}.  This test uses high level
     * insert operations when they do not cause structural changes and low 
     * level operations when structural changes are required. This allows us
     * to view all of the intermediate states of the {@link HTree} by taking
     * the control logic "out of the loop" as it were.
     * <p>
     * In order to insert the key (5) we will need to split (a). However, the
     * prefix length for (a) is only THREE (3) bits and we need a prefix length
     * of 5 bits before we can split (a). This prefix length requirement arises
     * because we can not split (a) until the prefix bits considered are
     * sufficient to redistribute the tuples between (a) and its siblings
     * (children of the same parent directory page). The first such split will
     * occur when we have a prefix length of 5 bits and therefore can make a
     * distinction among the keys in (a), which are:
     * 
     * <pre>
     * key|bits
     * ---+--------
     * 1   00000001
     * 2   00000010
     * 3   00000011
     * 4   00000100
     * ---+--------
     *    |01234567
     * </pre>
     * 
     * Consulting the table immediately above, the first non-zero bit is bit 5
     * of the key whose value is FOUR (4).
     */
    public void test_example1_splitDir_addressBits2_splitBits1_increasePrefixBits() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k5 = new byte[]{0x05};
            final byte[] k6 = new byte[]{0x06};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v5 = new byte[]{0x05};
            final byte[] v6 = new byte[]{0x06};
            
            final HTree htree = new HTree(store, addressBits, false/*rawRecords*/);

            // Note: The test assumes splitBits := 1.
            assertEquals("splitBits", 1, htree.splitBits);

            final DirectoryPage root = htree.getRoot();

            /**
             * Insert a series of keys to bring us to the starting point for
             * this test. The post-condition of this insert sequence is:
             * 
             * <pre>
             * root := [2] ######## (a,c,b,b)   //
             * a    := [2] 00######  (1,2,3,4) // Note: depth(root) == depth(a) !
             * c    := [2] 01######  (-,-,-,-) //
             * b    := [1] 1#######  (-,-;-,-) //
             * </pre>
             */
            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);

            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));

            assertTrue(root == htree.getRoot());
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            final BucketPage b = (BucketPage) root.childRefs[2].get();
            assertTrue(a == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(4, a.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);

            // verify that [a] will not accept an insert.
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));

            /**
             * Add a directory level.
             * 
             * Note: Since depth(root) EQ depth(a) we can not split (a).
             * Instead, we have to add a level which decreases depth(d) and
             * makes it possible to split (a).  There will be two references
             * to (a) in the post-condition, at which point it will be possible
             * to split (a).
             * 
             * <pre>
             * root := [2] ######## (d,d,b,b)     //
             * d    := [1] 0#######   (a,a;c,c)   // added new level below the root.  
             * a    := [0] ########     (1;2;3;4) // local depth now 0. NB: inconsistent intermediate state!!!
             * c    := [0] ########     (-;-;-;-) // local depth now 0.
             * b    := [1] 1#######   (-,-;-,-)   //
             * </pre>
             */

            // Add a new level below the root in the path to the full child.
            htree
                    .addLevel(root/* oldParent */, 0/* buddyOffset */, 1/* splitBits */);
            
            assertEquals("nnodes", 2, htree.getNodeCount());
            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(d == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(a == d.childRefs[1].get());
            assertTrue(c == d.childRefs[2].get());
            assertTrue(c == d.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(1, d.globalDepth);
            assertEquals(0, a.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(4, a.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);

            // verify that [a] will not accept an insert.
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));
            
            /**
             * Split (a), creating a new bucket page (e) and re-index the tuples
             * from (a) into (a,e). In this case, they all wind up back in (a)
             * so (a) remains full (no free slots). The local depth of (a) and
             * (e) are now (1).
             * 
             * <pre>
             * root := [2] ######## (d,d,b,b)     //
             * d    := [1] 0#######   (a,e;c,c)   // 
             * a    := [1] 00######     (1,2;3,4) // local depth now 1. 
             * e    := [1] 01######     (-,-;-,-) // local depth now 1 (new sibling)
             * c    := [0] ########     (-;-;-;-) // 
             * b    := [1] ########   (-,-;-,-)   //
             * </pre>
             */

            // FIXME This now fails since it correctly rejects the reindex!
            // split (a) into (a,e), re-indexing the tuples.
            assertTrue(htree.splitAndReindexFullBucketPage(d/* parent */,
                    0/* buddyOffset */, 4 /* prefixLength */, a/* oldBucket */));

            assertEquals("nnodes", 2, htree.getNodeCount()); // unchanged.
            assertEquals("nleaves", 4, htree.getLeafCount());
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final BucketPage e = (BucketPage) d.childRefs[1].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(d == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(e == d.childRefs[1].get());
            assertTrue(c == d.childRefs[2].get());
            assertTrue(c == d.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(1, d.globalDepth);
            assertEquals(1, a.globalDepth);
            assertEquals(1, e.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(4, a.getKeyCount());
            assertEquals(0, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    e);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);

            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));
            
            // verify that [a] will not accept an insert (still full).
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));

            /**
             * We still can not insert (5) since it would be insert into a buddy
             * bucket on (a) which is full. So, we need to split (a). However,
             * depth(a) == depth(a.parent) so we have to split the parent first.
             * In this case, (d) is not a single buddy hash table so we can
             * split it without adding a new level. This doubles the #of
             * pointers to each child that was linked from (d). The
             * post-condition is:
             * 
             * <pre>
             * root := [2] (d,f,b,b)     //
             * d    := [2]   (a,a,e,e)   // local depth += 1. 
             * f    := [2]   (c,c,c,c)   // local depth same as (d).
             * a    := [1]     (1,2;3,4) // unchanged 
             * e    := [1]     (-,-;-,-) // unchanged
             * c    := [0]     (-,-,-,-) // unchanged 
             * b    := [1]   (-,-;-,-)   //
             * </pre>
             * 
             * Note that the local depth of (a), (e), and (c) DO NOT change.
             * When we double the #of pointers to those nodes this exactly
             * offsets the increased in the depth of (d) resulting in NO change
             * in the depth of (a,e,c).
             */
            
            htree
                    .splitDirectoryPage(root/* parent */, 0/* buddyOffset */, d/* oldChild */);
            
            assertEquals("nnodes", 3, htree.getNodeCount()); 
            assertEquals("nleaves", 4, htree.getLeafCount()); // unchanged.
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final DirectoryPage f = (DirectoryPage) root.childRefs[1].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(f == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(a == d.childRefs[1].get());
            assertTrue(e == d.childRefs[2].get());
            assertTrue(e == d.childRefs[3].get());
            assertTrue(c == f.childRefs[0].get());
            assertTrue(c == f.childRefs[1].get());
            assertTrue(c == f.childRefs[2].get());
            assertTrue(c == f.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, d.globalDepth);
            assertEquals(2, f.globalDepth);
            assertEquals(1, a.globalDepth);
            assertEquals(1, e.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(4, a.getKeyCount());
            assertEquals(0, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    e);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);

            // verify that [a] will not accept an insert (still full).
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));

            /**
             * Split (a).
             * 
             * <pre>
             * root := [2] (d,f,b,b)     //
             * d    := [2]   (a,g,e,e)   //  
             * f    := [2]   (c,c,c,c)   // 
             * a    := [2]     (1,2,_,_) // local depth += 1. 
             * g    := [2]     (3,4,_,_) // local depth same as (a). 
             * e    := [1]     (-,-;-,-) // 
             * c    := [0]     (-,-,-,-) //  
             * b    := [1]   (-,-;-,-)   //
             * </pre>
             */

            htree
                    .splitBucketsOnPage(d/* parent */, 0/* buddyOffset */, a/* oldChild */);
            
            assertEquals("nnodes", 3, htree.getNodeCount());  // unchanged.
            assertEquals("nleaves", 5, htree.getLeafCount());
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            assertTrue(d == root.childRefs[0].get());
            assertTrue(f == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            final BucketPage g = (BucketPage) d.childRefs[1].get();
            assertTrue(a == d.childRefs[0].get());
            assertTrue(g == d.childRefs[1].get());
            assertTrue(e == d.childRefs[2].get());
            assertTrue(e == d.childRefs[3].get());
            assertTrue(c == f.childRefs[0].get());
            assertTrue(c == f.childRefs[1].get());
            assertTrue(c == f.childRefs[2].get());
            assertTrue(c == f.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, d.globalDepth);
            assertEquals(2, f.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, g.globalDepth);
            assertEquals(1, e.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(2, a.getKeyCount());
            assertEquals(2, g.getKeyCount());
            assertEquals(0, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k1, k2, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v1, v2, null, null })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k3, k4, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v3, v4, null, null})),//
                    g);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    e);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);

            /**
             * Insert (5).
             * 
             * <pre>
             * root := [2] (d,f,b,b)     //
             * d    := [2]   (a,g,e,e)   //  
             * f    := [2]   (c,c,c,c)   // 
             * a    := [2]     (1,2,5,_) //  
             * g    := [2]     (3,4,_,_) //  
             * e    := [1]     (-,-;-,-) // 
             * c    := [0]     (-,-,-,-) //  
             * b    := [1]   (-,-;-,-)   //
             * </pre>
             */

            htree.insert(k5, v5);
            
            assertEquals("nnodes", 3, htree.getNodeCount()); // unchanged.
            assertEquals("nleaves", 5, htree.getLeafCount());// unchanged
            assertEquals("nentries", 5, htree.getEntryCount());

            assertTrue(root == htree.getRoot());
            assertTrue(d == root.childRefs[0].get());
            assertTrue(f == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(g == d.childRefs[1].get());
            assertTrue(e == d.childRefs[2].get());
            assertTrue(e == d.childRefs[3].get());
            assertTrue(c == f.childRefs[0].get());
            assertTrue(c == f.childRefs[1].get());
            assertTrue(c == f.childRefs[2].get());
            assertTrue(c == f.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, d.globalDepth);
            assertEquals(2, f.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, g.globalDepth);
            assertEquals(1, e.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(3, a.getKeyCount());
            assertEquals(2, g.getKeyCount());
            assertEquals(0, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(3, new byte[][] { // keys
                            k1, k2, k5, null }),//
                    new ReadOnlyValuesRaba(3, new byte[][] { // vals
                            v1, v2, v5, null })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k3, k4, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v3, v4, null, null})),//
                    g);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    e);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);

            /**
             * Insert (6).
             * 
             * There are no structural changes required for this insert.
             * 
             * <pre>
             * root := [2] (d,f,b,b)     //
             * d    := [2]   (a,g,e,e)   //  
             * f    := [2]   (c,c,c,c)   // 
             * a    := [2]     (1,2,5,6) //  
             * g    := [2]     (3,4,_,_) //  
             * e    := [1]     (-,-;-,-) // 
             * c    := [0]     (-,-,-,-) //  
             * b    := [1]   (-,-;-,-)   //
             * </pre>
             */

            htree.insert(k6, v6);

            assertEquals("nnodes", 3, htree.getNodeCount());
            assertEquals("nleaves", 5, htree.getLeafCount());
            assertEquals("nentries", 6, htree.getEntryCount());

            assertTrue(root == htree.getRoot());
            assertTrue(d == root.childRefs[0].get());
            assertTrue(f == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertTrue(a == d.childRefs[0].get());
            assertTrue(g == d.childRefs[1].get());
            assertTrue(e == d.childRefs[2].get());
            assertTrue(e == d.childRefs[3].get());
            assertTrue(c == f.childRefs[0].get());
            assertTrue(c == f.childRefs[1].get());
            assertTrue(c == f.childRefs[2].get());
            assertTrue(c == f.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, d.globalDepth);
            assertEquals(2, f.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, g.globalDepth);
            assertEquals(1, e.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(4, a.getKeyCount());
            assertEquals(2, g.getKeyCount());
            assertEquals(0, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k5, k6 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v5, v6})),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k3, k4, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v3, v4, null, null})),//
                    g);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    e);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);

            /**
             * Insert (7).
             * 
             * The insert would be directed into (a), but (a) is full and can
             * not be split since depth(a) == depth(d). Also, we can not split
             * (d) since depth(d) == depth(root).
             * 
             * The first step is to add a new level under the root on the path
             * to (a). The post-condition for that step is:
             * 
             * <pre>
             * root := [2] (h,h,b,b)       //
             * h    := [1]   (d,d,f,f)     // new level @ depth := 1. 
             * d    := [0]     (a,g,e,e)   // local depth is changed.  
             * f    := [0]     (c,c,c,c)   // local depth is changed.
             * a    := [2]       (1,2,5,6) //  
             * g    := [2]       (3,4,_,_) //  
             * e    := [1]       (-,-;-,-) // 
             * c    := [0]       (-,-,-,-) //  
             * b    := [1]   (-,-;-,-)     //
             * </pre>
             */
//            htree.addLevel(root, 0/* buddyOffsetInChild */, htree.splitBits);
//
//            // FIXME At least one dir split will be necessary as well.
////            htree.splitDirectoryPage(d/*parent*/, 0/*buddyOffset*/, oldChild);
//            
//            // split (a) into (a,e), re-indexing the tuples.
//            htree.splitAndReindexFullBucketPage(d/* parent */,
//                    0/* buddyOffset */, 4 /* prefixLength */, a/* oldBucket */);
//
//            assertEquals("nnodes", 4, htree.getNodeCount());
//            assertEquals("nleaves", 5, htree.getLeafCount());
//            assertEquals("nentries", 6, htree.getEntryCount());
//
//            assertTrue(root == htree.getRoot());
//            final DirectoryPage h = (DirectoryPage) root.childRefs[0].get();
//            assertTrue(h == root.childRefs[0].get());
//            assertTrue(h == root.childRefs[1].get());
//            assertTrue(b == root.childRefs[2].get());
//            assertTrue(b == root.childRefs[3].get());
//            assertTrue(d == h.childRefs[0].get());
//            assertTrue(d == h.childRefs[1].get());
//            assertTrue(f == h.childRefs[2].get());
//            assertTrue(f == h.childRefs[3].get());
//            assertTrue(a == d.childRefs[0].get());
//            assertTrue(g == d.childRefs[1].get());
//            assertTrue(e == d.childRefs[2].get());
//            assertTrue(e == d.childRefs[3].get());
//            assertTrue(c == f.childRefs[0].get());
//            assertTrue(c == f.childRefs[1].get());
//            assertTrue(c == f.childRefs[2].get());
//            assertTrue(c == f.childRefs[3].get());
//            assertEquals(2, root.globalDepth);
//            assertEquals(1, h.globalDepth);
//            assertEquals(2, d.globalDepth);
//            assertEquals(2, f.globalDepth);
//            assertEquals(2, a.globalDepth);
//            assertEquals(2, g.globalDepth);
//            assertEquals(1, e.globalDepth);
//            assertEquals(0, c.globalDepth);
//            assertEquals(1, b.globalDepth);
//            assertEquals(4, a.getKeyCount());
//            assertEquals(2, g.getKeyCount());
//            assertEquals(0, e.getKeyCount());
//            assertEquals(0, c.getKeyCount());
//            assertEquals(0, b.getKeyCount());
//
//            assertSameBucketData(new MockBucketData(//
//                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
//                            k1, k2, k5, k6 }),//
//                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
//                            v1, v2, v5, v6})),//
//                    a);
//            assertSameBucketData(new MockBucketData(//
//                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
//                            k3, k4, null, null }),//
//                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
//                            v3, v4, null, null})),//
//                    g);
//            assertSameBucketData(new MockBucketData(//
//                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
//                            null, null, null, null }),//
//                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
//                            null, null, null, null})),//
//                    e);
//            assertSameBucketData(new MockBucketData(//
//                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
//                            null, null, null, null }),//
//                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
//                            null, null, null, null})),//
//                    c);
//            assertSameBucketData(new MockBucketData(//
//                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
//                            null, null, null, null }),//
//                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
//                            null, null, null, null})),//
//                    b);

        } finally {

            store.destroy();
            
        }

    }

    /**
     * Unit test inserts a sequences of keys into an {@link HTree} having an
     * address space of TWO (2) bits.
     * <p>
     * Note: This test relies on the tests above to validate the preconditions
     * for the insert of the key sequences {1,2,3,4} and the introduction of a
     * new direction page in the {@link HTree} formed by inserting that key
     * sequence when another insert would be directed into a bucket page where
     * global depth == local depth.
     * 
     * @see bigdata/src/architecture/htree.xls#example1
     */
    public void test_example_addressBits2_insert_1_2_3_4_5() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k5 = new byte[]{0x05};
            final byte[] k6 = new byte[]{0x06};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v5 = new byte[]{0x05};
            final byte[] v6 = new byte[]{0x06};
            
            final HTree htree = new HTree(store, addressBits, false/*rawRecords*/);

            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            // Note: The test is assumes splitBits := 1.
            assertEquals("splitBits", 1, htree.splitBits);

            final DirectoryPage root = htree.getRoot();

            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);

            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));

            assertTrue(root == htree.getRoot());
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            final BucketPage b = (BucketPage) root.childRefs[2].get();
            assertTrue(a == root.childRefs[0].get());
            assertTrue(c == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(4, a.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null })),//
                    b);

            /**
             * Insert (5).
             * 
             * <pre>
             * root := [2] (d,f,b,b)     //
             * d    := [2]   (a,g,e,e)   //  
             * f    := [2]   (c,c,c,c)   // 
             * a    := [2]     (1,2,5,_) //  
             * g    := [2]     (3,4,_,_) //  
             * e    := [1]     (-,-;-,-) // 
             * c    := [0]     (-,-,-,-) //  
             * b    := [1]   (-,-;-,-)   //
             * </pre>
             * 
             * Note: The post-condition verified here is established in the unit
             * test above this one. Here we are merely verifying that the
             * control logic in insert() achieves the same outcome as that unit
             * test.
             */

            htree.insert(k5, v5);

            assertEquals("nnodes", 3, htree.getNodeCount());
            assertEquals("nleaves", 5, htree.getLeafCount());
            assertEquals("nentries", 5, htree.getEntryCount());

            assertTrue(root == htree.getRoot());
            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
            final DirectoryPage f = (DirectoryPage) root.childRefs[1].get();
            assertTrue(d == root.childRefs[0].get());
            assertTrue(f == root.childRefs[1].get());
            assertTrue(b == root.childRefs[2].get());
            assertTrue(b == root.childRefs[3].get());
            final BucketPage g = (BucketPage) d.childRefs[1].get();
            final BucketPage e = (BucketPage) d.childRefs[2].get();
            assertTrue(a == d.childRefs[0].get());
            assertTrue(g == d.childRefs[1].get());
            assertTrue(e == d.childRefs[2].get());
            assertTrue(e == d.childRefs[3].get());
            assertTrue(c == f.childRefs[0].get());
            assertTrue(c == f.childRefs[1].get());
            assertTrue(c == f.childRefs[2].get());
            assertTrue(c == f.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, d.globalDepth);
            assertEquals(2, f.globalDepth);
            assertEquals(2, a.globalDepth);
            assertEquals(2, g.globalDepth);
            assertEquals(1, e.globalDepth);
            assertEquals(0, c.globalDepth);
            assertEquals(1, b.globalDepth);
            assertEquals(3, a.getKeyCount());
            assertEquals(2, g.getKeyCount());
            assertEquals(0, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(3, new byte[][] { // keys
                            k1, k2, k5, null }),//
                    new ReadOnlyValuesRaba(3, new byte[][] { // vals
                            v1, v2, v5, null })),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k3, k4, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v3, v4, null, null})),//
                    g);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    e);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    b);

            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));
            assertEquals(v5, htree.lookupFirst(k5));

        } finally {
            
            store.destroy();
            
        }
        
    }

    /**
     * FIXME Write a unit test in which we insert duplicate tuples forcing the
     * overflow of a bucket page. This condition is currently detected and will
     * cause an {@link UnsupportedOperationException} to be thrown. The test
     * will be used to work through the handling of page overflow. The basic
     * approach to page overflow is to double the address space on the page.
     * This is conceptually simple. However, this means that the address space
     * of the page will not be the same as the address space of the other pages
     * and that could violate various assumptions in the existing control logic.
     */
    public void test_overflowPage() {

        final int addressBits = 2;

        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final HTree htree = new HTree(store, addressBits, false/* rawRecords */);

            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            final byte[] key = new byte[]{1};
            final byte[] val = new byte[]{2};
            
            // insert enough tuples to fill the page twice over.
            for (int i = 0; i <= (addressBits * 2); i++) {

                htree.insert(key, val);
                
            }
            
        } finally {

            store.destroy();

        }

    }

    /**
     * FIXME Write a stress test which inserts a series of integer keys and then
     * verifies the state of the index. Any consistency problems should be
     * worked out in depth with an extension of the unit test for the insert
     * sequence (1,2,3,4,5).
     * 
     * TODO Test with int32 keys (versus single byte keys).
     * 
     * TODO This could be parameterized for the #of address bits, the #of keys
     * to insert, etc. [Test w/ 1 address bit (fan-out := 2). Test w/ 1024
     * address bits.  Use the stress test with the nominal fan out to look at
     * hot spots in the code.  Instrument counters for structural and other
     * modifications.]
     * 
     * TODO Stress test with random integers.
     * 
     * TODO Stress test with sequential negative numbers and a sequence of
     * alternating negative and positive integers.
     */
    public void test_stressInsert() {

        final int limit = 10;
        
        // validate against ground truth even N inserts.
        final int validateInterval = 1;
        
        final int addressBits = 2;

        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final HTree htree = new HTree(store, addressBits, false/* rawRecords */);

            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            final IKeyBuilder keyBuilder = new KeyBuilder();
            
            final byte[][] keys = new byte[limit][];

            for (int i = 1; i < limit; i++) {

                final byte[] key = keyBuilder.reset().append((byte)i).getKey();

                keys[i] = key;
                
                htree.insert(key, key);

                if (i % validateInterval == 0) {

//                    for(int j=0; j<i) // FIXME Verify lookup and iterator scan.
                    
                }
                
            }

            // Verify all tuples are found.
            for (int i = 0; i < limit; i++) {

                final byte[] key = keyBuilder.reset().append(i).getKey();

                assertEquals(key, htree.lookupFirst(key));

            }
            
            assertEquals(limit, htree.getEntryCount());

            /*
             * FIXME Verify the iterator visits all of the tuples in an
             * arbitrary order. E.g., assertSameIteratorAnyOrder(). This needs
             * to use an appropriate resolver pattern and we need to implement
             * the iterator!
             */
//            assertSameIteratorAnyOrder(keys, htree.iterator());
            
        } finally {

            store.destroy();

        }
        
    }
    
    /*
     * TODO This might need to be modified to verify the sets of tuples in each
     * buddy bucket without reference to their ordering within the buddy bucket.
     * If so, then we will need to pass in the global depth of the bucket page
     * and clone and write new logic for the comparison of the leaf data state.
     */
    static void assertSameBucketData(ILeafData expected, ILeafData actual) {
        
        AbstractBTreeTestCase.assertSameLeafData(expected, actual);
        
    }
    
}
