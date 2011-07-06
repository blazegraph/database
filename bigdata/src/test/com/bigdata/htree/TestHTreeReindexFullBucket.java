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
 * Created on Jul 6, 2011
 */

package com.bigdata.htree;

import junit.framework.TestCase2;

import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.htree.HTree.BucketPage;
import com.bigdata.htree.HTree.DirectoryPage;
import com.bigdata.htree.data.MockBucketData;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for re-indexing tuples when splitting a full buddy bucket (a
 * {@link BucketPage} consisting of a single buddy hash bucket which is full).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHTreeReindexFullBucket extends TestCase2 {

    /**
     * 
     */
    public TestHTreeReindexFullBucket() {
    }

    /**
     * @param name
     */
    public TestHTreeReindexFullBucket(String name) {
        super(name);
    }

    /**
     * Unit test verifies that an attempt to split and reindex a full bucket
     * page which should fail. The test verifies that the operation does, in
     * fact, fail and that it fails without side-effects on the {@link HTree}
     * state.
     */
    public void test_reindexFullBucket_failsWithoutSideEffects() {
        
        final int addressBits = 2;
        
        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[]{0x01};
            final byte[] k2 = new byte[]{0x02};
            final byte[] k3 = new byte[]{0x03};
            final byte[] k4 = new byte[]{0x04};
            final byte[] k5 = new byte[]{0x05};

            final byte[] v1 = new byte[]{0x01};
            final byte[] v2 = new byte[]{0x02};
            final byte[] v3 = new byte[]{0x03};
            final byte[] v4 = new byte[]{0x04};
            final byte[] v5 = new byte[]{0x05};
            
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
             * Attempt to split and reindex (a) into (a,e). This operation
             * should fail.
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

            // attempt to split (a) into (a,e).
            assertFalse(htree.splitAndReindexFullBucketPage(d/* parent */,
                    0/* buddyOffset */, 4 /* prefixLength */, a/* oldBucket */));

            // verify that there were no side effects on the HTree.
            assertEquals("nnodes", 2, htree.getNodeCount());
            assertEquals("nleaves", 3, htree.getLeafCount()); // unchanged
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
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
            
        } finally {
            
            store.destroy();
            
        }

    }

    /**
     * TODO Write a unit test in which the pre-conditions for the split and
     * re-index of a full bucket page are met and verify that the operation
     * succeeds.
     * 
     * TODO Write a varient of this test in which the parent is at depth:=1 and
     * another variant where the parent is a depth:=2 (assuming that
     * addressBits:=2 in both cases).
     */
    public void test_reindexFullBucket_succeeds() {
        
        fail("write test");
        
    }
            
    static void assertSameBucketData(ILeafData expected, ILeafData actual) {
        
        TestHTree.assertSameBucketData(expected, actual);
        
    }

}
