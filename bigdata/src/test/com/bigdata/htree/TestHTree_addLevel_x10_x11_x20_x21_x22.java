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
 * Created on Jul 7, 2011
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
 * Unit test which explores a simple add level case.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHTree_addLevel_x10_x11_x20_x21_x22 extends TestCase2 {

    /**
     * 
     */
    public TestHTree_addLevel_x10_x11_x20_x21_x22() {
    }

    /**
     * @param name
     */
    public TestHTree_addLevel_x10_x11_x20_x21_x22(String name) {
        super(name);
    }

    public void test_addLevel() {

        final int addressBits = 2;

        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[] { 0x10 };
            final byte[] k2 = new byte[] { 0x11 };
            final byte[] k3 = new byte[] { 0x20 };
            final byte[] k4 = new byte[] { 0x21 };
            final byte[] k5 = new byte[] { 0x12 };

            final byte[] v1 = new byte[] { 0x10 };
            final byte[] v2 = new byte[] { 0x11 };
            final byte[] v3 = new byte[] { 0x20 };
            final byte[] v4 = new byte[] { 0x21 };
            final byte[] v5 = new byte[] { 0x12 };

            final HTree htree = new HTree(store, addressBits, false/* rawRecords */);

            // Note: The test assumes splitBits := 1.
            assertEquals("splitBits", 1, htree.splitBits);

            final DirectoryPage root = htree.getRoot();

            /**
             * Insert a series of keys to bring us to the starting point for
             * this test. The post-condition of this insert sequence is:
             * 
             * <pre>
             * root := [2] ######## (a,c,b,b)   //
             * a    := [2] 00######  (x10,x11,x20,x21) // Note: depth(root) == depth(a) !
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
                            null, null, null, null })),//
                    c);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null })),//
                    b);

            // verify that [a] will not accept an insert.
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));
        } finally {

            store.destroy();

        }
    }

    static void assertSameBucketData(ILeafData expected, ILeafData actual) {

        TestHTree.assertSameBucketData(expected, actual);

    }

}
