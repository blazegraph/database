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
public class TestHTree_addLevel extends AbstractHTreeTestCase {

    /**
     * 
     */
    public TestHTree_addLevel() {
    }

    /**
     * @param name
     */
    public TestHTree_addLevel(String name) {
        super(name);
    }

    /**
     * Unit test which inserts <code>
     * x10, x11, x20, x21</code> and then works through
     * addLevel() and splitAndReindex(). For that sequence of keys, we can get
     * by with a single addLevel() and then immediately split and reindex the
     * full bucket page.
     */
    public void test_addLevel() {

        final int addressBits = 2;

        final IRawStore store = new SimpleMemoryRawStore();

        try {

            final byte[] k1 = new byte[] { 0x10 };
            final byte[] k2 = new byte[] { 0x11 };
            final byte[] k3 = new byte[] { 0x20 };
            final byte[] k4 = new byte[] { 0x21 };
            final byte[] k5 = new byte[] { 0x12 };
            final byte[] k6 = new byte[] { 0x13 };

            final byte[] v1 = new byte[] { 0x10 };
            final byte[] v2 = new byte[] { 0x11 };
            final byte[] v3 = new byte[] { 0x20 };
            final byte[] v4 = new byte[] { 0x21 };
            final byte[] v5 = new byte[] { 0x12 };
            final byte[] v6 = new byte[] { 0x13 };

            final HTree htree = new HTree(store, addressBits, false/* rawRecords */);

            // Note: The test assumes splitBits := 1.
            assertEquals("splitBits", 1, htree.splitBits);

            final DirectoryPage root = htree.getRoot();

            /**
             * Insert a series of keys to bring us to the starting point for
             * this test. The post-condition of this insert sequence is:
             * 
             * <pre>
             * root := [2] (a,c,b,b)   //
             * a    := [2]   (x10,x11,x20,x21) // Note: depth(root) == depth(a) !
             * c    := [2]   (-,-,-,-) //
             * b    := [1]   (-,-;-,-) //
             * </pre>
             */
            htree.insert(k1, v1);
            htree.insert(k2, v2);
            htree.insert(k3, v3);
            htree.insert(k4, v4);

			if (log.isInfoEnabled())
				log.info("\n" + htree.PP());

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
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));

			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4 },
					htree.values());

            assertTrue(a.distinctBitsRequired() == 1);
            
            // verify that [a] will not accept an insert.
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));
            
            assertTrue(a.distinctBitsRequired() == 1);

			/**
			 * Add a directory level.
			 * 
			 * Note: Since depth(root) EQ depth(a) we can not split (a).
			 * Instead, we have to add levels and split directory pages until we
			 * have enough prefix bits to split the tuples in (a) across (a,e).
			 * 
			 * <pre>
			 * root := [2] (d,d,b,b)     //
			 * d    := [1]   (a,a;c,c)   // added new level below the root.  
			 * a    := [-]     (x10;x11;x20;x21) // local depth now 0. NB: inconsistent intermediate state!!!
			 * c    := [0]     (-;-;-;-) // local depth now 0.
			 * b    := [1]   (-,-;-,-)   //
			 * </pre>
			 */

            // Add a new level below the root in the path to the full child.
            htree
                    .addLevel(root/* oldParent */, 0/* buddyOffset */, 1/* splitBits */);
            
			if (log.isInfoEnabled())
				log.info("\n" + htree.PP());

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

			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4 },
					htree.values());

			// verify that [a] will not accept an insert.
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));

			/**
			 * Split (a), creating a new bucket page (e) and re-index the tuples
			 * from (a) into (a,e).
			 * 
			 * <pre>
			 * root := [2] (d,d,b,b)     //
			 * d    := [1]   (a,e;c,c)   // 
			 * a    := [1]     (x10,x11;_,_) // local depth now 1. 
			 * e    := [1]     (x20,x21;-,-) // local depth now 1 (new sibling)
			 * c    := [0]     (-;-;-;-) // 
			 * b    := [1]   (-,-;-,-)   //
			 * </pre>
			 */

            // split (a) into (a,e), re-indexing the tuples.
            assertTrue(htree.splitAndReindexFullBucketPage(d/* parent */,
                    0/* buddyOffset */, 2 /* prefixLengthOfParent */, a/* oldBucket */));

			if (log.isInfoEnabled())
				log.info("\n" + htree.PP());

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
            assertEquals(2, a.getKeyCount());
            assertEquals(2, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k1, k2, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v1, v2, null, null})),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k3, k4, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v3, v4, null, null})),//
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
            
			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4 },
					htree.values());

            // verify that [a] still will not accept an insert since the split has created
            // two buddies both filling their two available slots.            
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));

            /**
             * Split directory (d) to increase the depth so we can split (a).
             * 
             * <pre>
             * root := [2] (d,f,b,b)         //
             * d    := [2]   (a,a,e,e)       // depth(d) += 1
             * f    := [2]   (c,c,c,c)       // new sibling
             * a    := [1]     (x10,x11;_,_) // 
             * e    := [1]     (x20,x21;-,-) // 
             * c    := [0]     (-;-;-;-)     // 
             * b    := [1]   (-,-;-,-)       //
             * </pre>
             */

            htree.splitDirectoryPage(root/*parent*/, 0/*buddyOffset*/, d/*oldChild*/);

//            htree.reindexTuples(d/*parent*/, 0/*buddyOffset*/, 2/*prefixLengthOfParent*/, a, e);
            
			if (log.isInfoEnabled())
				log.info("\n" + htree.PP());

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
            assertEquals(2, a.getKeyCount());
            assertEquals(2, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k1, k2, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v1, v2, null, null})),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k3, k4, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v3, v4, null, null})),//
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

			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4 },
					htree.values());

            // verify that [a] still will not accept an insert since the split has created
            // two buddies both filling their two available slots.            
            assertFalse(a.insert(k5, v5, root/* parent */, 0/* buddyOffset */));

            /**
             * Split (a).
             * 
             * <pre>
             * root := [2] (d,f,b,b)         //
             * d    := [2]   (a,g,e,e)       // 
             * f    := [2]   (c,c,c,c)       // 
             * a    := [2]     (x10,x11,_,_) // double buddy size and increase depth by 1.
             * g    := [2]     (_,_,_,_)     // new sibling of (a)
             * e    := [1]     (x20,x21;-,-) // 
             * c    := [0]     (-;-;-;-)     // 
             * b    := [1]   (-,-;-,-)       //
             * </pre>
             */

            htree.splitBucketsOnPage(d/*parent*/, 0/*buddyOffset*/, a/*oldChild*/);
            
			if (log.isInfoEnabled())
				log.info("\n" + htree.PP());

			assertEquals("nnodes", 3, htree.getNodeCount());  // unchanged.
            assertEquals("nleaves", 5, htree.getLeafCount());
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final BucketPage g = (BucketPage) d.childRefs[1].get();
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
            assertEquals(2, a.getKeyCount());
            assertEquals(2, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k1, k2, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v1, v2, null, null})),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            null, null, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            null, null, null, null})),//
                    g);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k3, k4, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v3, v4, null, null})),//
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

//            System.err.println(htree.PP());
            
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));

			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4 },
					htree.values());

			/**
             * Insert the key (0x12).
             * 
             * <pre>
             * root := [2] (d,f,b,b)         //
             * d    := [2]   (a,g,e,e)       // 
             * f    := [2]   (c,c,c,c)       // 
             * a    := [2]     (x10,x11,x12,_) // 
             * g    := [2]     (_,_,_,_)     // 
             * e    := [1]     (x20,x21;-,-) // 
             * c    := [0]     (-;-;-;-)     // 
             * b    := [1]   (-,-;-,-)       //
             * </pre>
             */
            
            htree.insert(k5, v5);
            
			if (log.isInfoEnabled())
				log.info("\n" + htree.PP());

            assertEquals("nnodes", 3, htree.getNodeCount());  // unchanged.
            assertEquals("nleaves", 5, htree.getLeafCount()); // unchanged
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
            assertEquals(2, e.getKeyCount());
            assertEquals(0, c.getKeyCount());
            assertEquals(0, b.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(3, new byte[][] { // keys
                            k1, k2, k3, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v1, v2, v3, null})),//
                    a);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(0, new byte[][] { // keys
                            k1, k2, null, null }),//
                    new ReadOnlyValuesRaba(0, new byte[][] { // vals
                            v1, v2, null, null})),//
                    g);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyValuesRaba(2, new byte[][] { // keys
                            k3, k4, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v3, v4, null, null})),//
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
            
			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4, v5 },
					htree.values());

//            htree.insert(k6, v6);
//            
//            assertEquals(v5, htree.lookupFirst(k5));
//            assertEquals(v6, htree.lookupFirst(k6));
            
        } finally {

            store.destroy();

        }
    }
    
}
