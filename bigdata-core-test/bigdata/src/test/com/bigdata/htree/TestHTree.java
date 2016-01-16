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
 * Created on Apr 19, 2011
 */

package com.bigdata.htree;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.htree.data.MockBucketData;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for {@link HTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *          TODO Work through a detailed example in which we have an elided
 *          bucket page or directory page because nothing has been inserted into
 *          that part of the address space.
 */
public class TestHTree extends AbstractHTreeTestCase {

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
            
            final HTree htree = getHTree(store, addressBits);
            
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
            // htree.dump(Level.ALL, System.err, true/* materialize */);
            assertEquals(2, root.getGlobalDepth());
            assertEquals(0, a.getGlobalDepth());
            assertEquals(0, a.getKeyCount());
            assertEquals(0, a.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyKeysRaba(0, new byte[][] { // keys
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
            // htree.dump(Level.ALL, System.err, true/* materialize */);
            assertEquals(2, root.getGlobalDepth());
            assertEquals(0, a.getGlobalDepth());
            assertEquals(1, a.getKeyCount());
            assertEquals(1, a.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyKeysRaba(1, new byte[][] { // keys
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
            //assertSameIterator(new AbstractPage[]{a}); // TODO Verify the direct and recursive iterators.
            assertSameIterator(new byte[][]{v1}, htree.values());
            
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
			if (log.isInfoEnabled())
				log.info(htree.PP());
            assertEquals("nnodes", 1, htree.getNodeCount());
            assertEquals("nleaves", 1, htree.getLeafCount());
            assertEquals("nentries", 2, htree.getEntryCount());
            // htree.dump(Level.ALL, System.err, true/* materialize */);
            assertTrue(root == htree.getRoot());
            assertEquals(4, root.childRefs.length);
            assertTrue(a == (BucketPage) root.childRefs[0].get());
            assertTrue(a == (BucketPage) root.childRefs[3].get());
            assertEquals(2, root.getGlobalDepth());
            assertEquals(0, a.getGlobalDepth());// localDepth has increased.
            assertEquals(2, a.getKeyCount());
            assertEquals(2, a.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyKeysRaba(2, new byte[][] { // keys
                            k1, k2, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v1, k2, null, null})),//
                    a);
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
            assertSameIterator(new byte[][]{v1,v2}, htree.values());
            
            /**
             * 3. Insert 0x03. This forces another split.
             * <pre> 
             * root := [2] (a,a,a,a)   //
             * a    := [2]   (1,2,3,-) //
             * </pre>
             */
            htree.insert(k3, v3);
            assertEquals("nnodes", 1, htree.getNodeCount());
            assertEquals("nleaves", 1, htree.getLeafCount());
            assertEquals("nentries", 3, htree.getEntryCount());
            // htree.dump(Level.ALL, System.err, true/* materialize */);
            assertTrue(root == htree.getRoot());
            assertEquals(4, root.childRefs.length);
            final BucketPage c = (BucketPage) root.childRefs[1].get();
            assertTrue(a == (BucketPage) root.childRefs[0].get());
            assertTrue(a == (BucketPage) root.childRefs[3].get());
            assertEquals(2, root.getGlobalDepth());
            assertEquals(0, a.getGlobalDepth());// localDepth has increased.
            assertEquals(0, c.getGlobalDepth());// localDepth is same as [a].
            assertEquals(3, a.getKeyCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyKeysRaba(3, new byte[][] { // keys
                            k1, k2, k3, null }),//
                    new ReadOnlyValuesRaba(3, new byte[][] { // vals
                            v1, k2, k3, null})),//
                    a);
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
            assertSameIterator(new byte[][]{v1,v2,v3}, htree.values());
            
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
            assertEquals("nleaves", 1, htree.getLeafCount());
            assertEquals("nentries", 4, htree.getEntryCount());
            // htree.dump(Level.ALL, System.err, true/* materialize */);
            assertTrue(root == htree.getRoot());
            assertEquals(4, root.childRefs.length);
            assertTrue(a == (BucketPage) root.childRefs[0].get());
             assertEquals(2, root.getGlobalDepth());
            assertEquals(0, a.getGlobalDepth());// localDepth has increased.
            assertEquals(4, a.getKeyCount());
            assertEquals(4, a.getValueCount());
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyKeysRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, k2, k3, k4 })),//
                    a);
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
            assertSameIterator(new byte[][]{v1,v2,v3,v4}, htree.values());
            
        } finally {

            store.destroy();

        }

    }

//    /**
//     * Unit test for
//     * {@link HTree#addLevel(DirectoryPage, int, int, com.bigdata.htree.HTree.AbstractPage)}
//     * .
//     */
//    public void test_example1_splitDir_addressBits2_splitBits1() {
//
//        final int addressBits = 2;
//        
//        final IRawStore store = new SimpleMemoryRawStore();
//
//        try {
//
//            final byte[] k1 = new byte[]{0x01};
//            final byte[] k2 = new byte[]{0x02};
//            final byte[] k3 = new byte[]{0x03};
//            final byte[] k4 = new byte[]{0x04};
//            final byte[] k20 = new byte[]{0x20};
//
//            final byte[] v1 = new byte[]{0x01};
//            final byte[] v2 = new byte[]{0x02};
//            final byte[] v3 = new byte[]{0x03};
//            final byte[] v4 = new byte[]{0x04};
//            final byte[] v20 = new byte[]{0x20};
//            
//            final HTree htree = new HTree(store, addressBits);
//
//            final DirectoryPage root = htree.getRoot();
//            
//            htree.insert(k1, v1);
//            htree.insert(k2, v2);
//            htree.insert(k3, v3);
//            htree.insert(k4, v4);
//            
//            /*
//             * Verify preconditions for the unit test.
//             */
//            assertTrue(root == htree.getRoot());
//            final BucketPage a = (BucketPage) root.childRefs[0].get();
//            final BucketPage c = (BucketPage) root.childRefs[1].get();
//            final BucketPage b = (BucketPage) root.childRefs[2].get();
//            assertTrue(a == root.childRefs[0].get());
//            assertTrue(c == root.childRefs[1].get());
//            assertTrue(b == root.childRefs[2].get());
//            assertTrue(b == root.childRefs[3].get());
//            assertEquals(2, root.globalDepth);
//            assertEquals(2, a.globalDepth);
//            assertEquals(2, c.globalDepth);
//            assertEquals(1, b.globalDepth);
//
//            /*
//             * Force a split of the root directory page. Note that (a) has the
//             * same depth as the directory page we want to split, which is a
//             * precondition for being able to split the directory.
//             */
//
//            // verify that [a] will not accept an insert.
//            assertFalse(a
//                    .insert(k20, v20, root/* parent */, 0/* buddyOffset */));
//            
//            // Add a new level below the root in the path to the full child.
//            htree
//                    .addLevel(root/* oldParent */, 0/* buddyOffset */, 1/* splitBits */);
//
//            /*
//             * Verify post-conditions.
//             */
//            
//            assertEquals("nnodes", 2, htree.getNodeCount());
//            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
//            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged
//
//            assertTrue(root == htree.getRoot());
//            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
//            assertTrue(d == root.childRefs[0].get());
//            assertTrue(d == root.childRefs[1].get());
//            assertTrue(b == root.childRefs[2].get());
//            assertTrue(b == root.childRefs[3].get());
//            assertTrue(a == d.childRefs[0].get());
//            assertTrue(a == d.childRefs[1].get());
//            assertTrue(c == d.childRefs[2].get());
//            assertTrue(c == d.childRefs[3].get());
//            assertEquals(2, root.globalDepth);
//            assertEquals(1, d.globalDepth);
//            assertEquals(0, a.globalDepth);
//            assertEquals(0, c.globalDepth);
//            assertEquals(1, b.globalDepth);
//
//        } finally {
//
//            store.destroy();
//            
//        }
//
//    }
//    
//    /**
//     * Unit test for
//     * {@link HTree#addLevel(DirectoryPage, int, int, com.bigdata.htree.HTree.AbstractPage)}
//     * .
//     */
//    public void test_example1_splitDir_addressBits2_splitBits2() {
//
//        final int addressBits = 2;
//        
//        final IRawStore store = new SimpleMemoryRawStore();
//
//        try {
//
//            final byte[] k1 = new byte[]{0x01};
//            final byte[] k2 = new byte[]{0x02};
//            final byte[] k3 = new byte[]{0x03};
//            final byte[] k4 = new byte[]{0x04};
//            final byte[] k20 = new byte[]{0x20};
//
//            final byte[] v1 = new byte[]{0x01};
//            final byte[] v2 = new byte[]{0x02};
//            final byte[] v3 = new byte[]{0x03};
//            final byte[] v4 = new byte[]{0x04};
//            final byte[] v20 = new byte[]{0x20};
//            
//            final HTree htree = new HTree(store, addressBits);
//
//            final DirectoryPage root = htree.getRoot();
//            
//            htree.insert(k1, v1);
//            htree.insert(k2, v2);
//            htree.insert(k3, v3);
//            htree.insert(k4, v4);
//            
//            /*
//             * Verify preconditions for the unit test.
//             */
//            assertTrue(root == htree.getRoot());
//            final BucketPage a = (BucketPage) root.childRefs[0].get();
//            final BucketPage c = (BucketPage) root.childRefs[1].get();
//            final BucketPage b = (BucketPage) root.childRefs[2].get();
//            assertTrue(a == root.childRefs[0].get());
//            assertTrue(c == root.childRefs[1].get());
//            assertTrue(b == root.childRefs[2].get());
//            assertTrue(b == root.childRefs[3].get());
//            assertEquals(2, root.globalDepth);
//            assertEquals(2, a.globalDepth);
//            assertEquals(2, c.globalDepth);
//            assertEquals(1, b.globalDepth);
//
//            /*
//             * Force a split of the root directory page. Note that (a) has the
//             * same depth as the directory page we want to split, which is a
//             * precondition for being able to split the directory.
//             */
//
//            // verify that [a] will not accept an insert.
//            assertFalse(a
//                    .insert(k20, v20, root/* parent */, 0/* buddyOffset */));
//            
//            // Add a new level below the root in the path to the full child.
//            htree
//                    .addLevel(root/* oldParent */, 0/* buddyOffset */, 2/* splitBits */);
//            
//            /*
//             * Verify post-conditions.
//             */
//            
//            assertEquals("nnodes", 2, htree.getNodeCount());
//            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
//            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged
//
//            assertTrue(root == htree.getRoot());
//            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
//            assertTrue(d == root.childRefs[0].get());
//            assertTrue(c == root.childRefs[1].get());
//            assertTrue(b == root.childRefs[2].get());
//            assertTrue(b == root.childRefs[3].get());
//            assertTrue(a == d.childRefs[0].get());
//            assertTrue(a == d.childRefs[1].get());
//            assertTrue(a == d.childRefs[2].get());
//            assertTrue(a == d.childRefs[3].get());
//            assertEquals(2, root.globalDepth);
//            assertEquals(2, d.globalDepth);
//            assertEquals(0, a.globalDepth);
//            assertEquals(2, c.globalDepth);
//            assertEquals(1, b.globalDepth);
//
//        } finally {
//
//            store.destroy();
//            
//        }
//
//    }

	/**
	 * A unit test which continues the scenario begun above (insert 1,2,3,4),
	 * working through the structural changes required until we can finally
	 * insert the key (5) into the {@link HTree}. This test uses high level
	 * insert operations when they do not cause structural changes and low level
	 * operations when structural changes are required. This allows us to view
	 * as many of the intermediate states of the {@link HTree} as possible by
	 * taking the control logic "out of the loop" as it were.
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
    public void test_example1_splitDir_addressBits2_1_2_3_4_5() {

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
            
            final HTree htree = getHTree(store, addressBits);

            // Note: The test assumes splitBits := 1.
            assertEquals("splitBits", 1, htree.splitBits);

            final DirectoryPage root = htree.getRoot();

            /**
             * Insert a series of keys to bring us to the starting point for
             * this test. The post-condition of this insert sequence is:
             * 
             * <pre>
             * root := [2] (a,c,b,b)   //
             * a    := [2]   (1,2,3,4) // Note: depth(root) == depth(a) !
             * c    := [2]   (-,-,-,-) //
             * b    := [1]   (-,-;-,-) //
             * </pre>
             */
            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);

			if (log.isInfoEnabled())
				log.info("after inserting 1,2,3,4: \n" + htree.PP());

            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));

            assertTrue(root == htree.getRoot());
            final BucketPage a = (BucketPage) root.childRefs[0].get();
            assertTrue(a == root.childRefs[0].get());
            assertEquals(2, root.globalDepth);
            assertEquals(0, a.globalDepth);
            assertEquals(4, a.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyKeysRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    a);

            // verify that [a] will not accept an insert.
            assertFalse(a.insert(k5, v5));//, root/* parent */, 0/* buddyOffset */));

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
             * root := [2]  (d,c,b,b)     //
             * d    := [2]    (e,e;f,f)   // new directory page replaces (a) 
             * e    := [1]      (1,2;3,4) // 
             * f    := [1]      (-,-;-,-) // 
             * c    := [0]    (-;-;-;-)   // 
             * b    := [1]    (-,-;-,-)   //
             * </pre>
             */

			// Replace the full bucket page with a new directory over two child
			// bucket pages and redistribute the tuples between the new bucket
			// pages.
			// htree.addLevel2(a/* oldPage */);
            a.addLevel();

			if (log.isInfoEnabled())
				log.info("after addLevel(): \n" + htree.PP());
				
			/**
			 * FIXME Validation here is failing because the tree has actually
			 * split the new bucket page (e) twice after the addLevel() in order
			 * to get all of the tuples back into the buddy bucket. After all
			 * that, the tree once again has a full bucket page for (e). The
			 * test can be updated to verify this structure:
			 * 
			 * <pre>
			 * D#88 [2] (D#48,-,-,-)
			 * D#48 [2]     (B#68,B#68,-,-)
			 * B#68 [2]         ([1](00000001),[2](00000010),[3](00000011),[4](00000100))
			 * </pre>
			 */
			
            assertEquals("nnodes", 2, htree.getNodeCount()); // +1
            assertEquals("nleaves", 1, htree.getLeafCount()); // net +2
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
            assertTrue(d == root.childRefs[0].get());
            final BucketPage e = (BucketPage) d.childRefs[0].get();
            final BucketPage g = (BucketPage) d.childRefs[1].get();
            assertTrue(d.childRefs[2] == null);
            assertTrue(e == d.childRefs[0].get());
            assertTrue(g == d.childRefs[1].get());
			assertTrue(e == g);
            assertEquals(2, root.globalDepth);
            assertEquals(2, d.globalDepth);
            assertEquals(1, e.globalDepth);
            assertEquals(4, e.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyKeysRaba(4, new byte[][] { // keys
                            k1, k2, k3, k4 }),//
                    new ReadOnlyValuesRaba(4, new byte[][] { // vals
                            v1, v2, v3, v4 })),//
                    e);

            // verify that [e] will not accept an insert.
            assertFalse(e.insert(k5, v5));//, root/* parent */, 0/* buddyOffset */));

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

            final HTree htree = getHTree(store, addressBits);

            // Verify initial conditions.
            assertTrue("store", store == htree.getStore());
            assertEquals("addressBits", addressBits, htree.getAddressBits());

            final byte[] key = new byte[]{1};
            final byte[] val = new byte[]{2};
            
            final int inserts = (1 << addressBits) * 20; // that'll be 20 pages then
            
            // insert enough tuples to fill the page twice over.
            for (int i = 0; i < inserts; i++) {

                htree.insert(key, val);
                
            }
            
            // now iterate over all the values
            ITupleIterator tups = htree.lookupAll(key);
            int visits = 0;
            while (tups.hasNext()) {
            	ITuple tup = tups.next();
            	visits++;
            }
            assertTrue(visits == inserts);
            
        } finally {

            store.destroy();

        }

    }

	public void test_distinctBits() {

        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k5 = new byte[]{0x05};
            final byte[] k6 = new byte[]{0x06};
            final byte[] k8 = new byte[]{0x08};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v5 = new byte[]{0x05};
            final byte[] v6 = new byte[]{0x06};
            final byte[] v8 = new byte[]{0x08};
            
            final HTree htree = getHTree(store, addressBits);
            
            try {
	
	            // Verify initial conditions.
	            assertTrue("store", store == htree.getStore());
	            assertEquals("addressBits", addressBits, htree.getAddressBits());
	
	            // Note: The test is assumes splitBits := 1.
	            assertEquals("splitBits", 1, htree.splitBits);
	
	            final DirectoryPage root = htree.getRoot();
	
	            htree.insert(k1, v1);
	            htree.insert(k2, v2);
	
	            assertEquals(v1, htree.lookupFirst(k1));
	            assertEquals(v2, htree.lookupFirst(k2));
	
	            assertTrue(root == htree.getRoot());
	            final BucketPage a = (BucketPage) root.childRefs[0].get();
	            
				if (log.isInfoEnabled())
					log.info("1#: Bit resolution: " + a.getPrefixLength()
							+ ", additional bits required: "
							+ a.distinctBitsRequired());
	
				assertTrue(a.distinctBitsRequired() == 4);
	            
	            htree.insert(k3, v3);
	            htree.insert(k8, v8);
	            
		        final BucketPage b = (BucketPage) root.childRefs[0].get();
	
		        if (log.isInfoEnabled())
					log.info("2#: Bit resolution: " + b.getPrefixLength()
							+ ", additional bits required: "
							+ b.distinctBitsRequired());
	
				assertTrue(b.distinctBitsRequired() == 2);
	
	            // insert extra level
	            htree.insert(k4, v4);
	            
		        final DirectoryPage pd = (DirectoryPage) root.childRefs[0].get();
		        final DirectoryPage ppd = (DirectoryPage) pd.childRefs[0].get();
		        final BucketPage c = (BucketPage) ppd.childRefs[0].get();
	
		        if (log.isInfoEnabled())
					log.info("3#: Bit resolution: " + c.getPrefixLength()
							+ ", additional bits required: "
							+ c.distinctBitsRequired());
	
				assertTrue(c.distinctBitsRequired() == 1);
				
				// System.out.println(htree.PP());
            } catch (Throwable t) {
            	if (log.isInfoEnabled())
            		log.info("Pretty Print Error: " + htree.PP());
            	
            	throw new RuntimeException(t);
            }

        } finally {
            
            store.destroy();
            
        }
        
    }
    
}
