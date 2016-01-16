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
 * Created on Jul 7, 2011
 */

package com.bigdata.htree;

import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
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
     * x10, x11, x20, x21</code> and then works through addLevel() and
     * splitAndReindex(). For that sequence of keys, we can get by with a single
     * addLevel() and then immediately split and reindex the full bucket page.
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

			final HTree htree = getHTree(store, addressBits,
					false/* rawRecords */, false/* persistent */);

            // Note: The test assumes splitBits := 1.
            assertEquals("splitBits", 1, htree.splitBits);

            final DirectoryPage root = htree.getRoot();

            /**
             * Insert a series of keys to bring us to the starting point for
             * this test. The post-condition of this insert sequence is:
             * 
             * <pre>
             * root := [2] (a,a,a,a)   //
             * a    := [2]   (x10,x11,x20,x21) // Note: depth(root) == depth(a) !
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
            assertEquals(v1, htree.lookupFirst(k1));
            assertEquals(v2, htree.lookupFirst(k2));
            assertEquals(v3, htree.lookupFirst(k3));
            assertEquals(v4, htree.lookupFirst(k4));

			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4 },
					htree.values());

            assertTrue(a.distinctBitsRequired() == 1);
            
            // verify that [a] will not accept an insert.
            assertFalse(a.insert(k5, v5));
            
            assertTrue(a.distinctBitsRequired() == 1);

			/**
			 * Add a directory level, replacing the full bucket page with a new
			 * directory (d) and two new child bucket pages (e,f).  The tuples
			 * in (a) are redistributed between (e,f) based on their hash codes. 
			 * 
			 * <pre>
			 * root := [2] (d,c,b,b)     //
			 * d    := [2]   (e,e;f,f)   // replace (a) with (d).  
			 * e    := [1]     (-,-;x10,x11) // new bucket page child of (d)
			 * f    := [1]     (x20,x21;-,-) // new bucket page child of (d)
			 * c    := [2]   (-,-,-,-)   // unchanged
			 * b    := [1]   (-,-;-,-)   // unchanged
			 * </pre>
			 */

			// htree.addLevel2(a);
            a.addLevel();
            
			if (log.isInfoEnabled())
				log.info("\n" + htree.PP());

            assertEquals("nnodes", 2, htree.getNodeCount()); // +1
            assertEquals("nleaves", 2, htree.getLeafCount()); // net +1
            assertEquals("nentries", 4, htree.getEntryCount()); // unchanged

            assertTrue(root == htree.getRoot());
            final DirectoryPage d = (DirectoryPage) root.childRefs[0].get();
            assertTrue(d == root.childRefs[0].get());
            final BucketPage e = (BucketPage)d.childRefs[0].get();
            final BucketPage f = (BucketPage)d.childRefs[2].get();
            assertTrue(e == d.childRefs[0].get());
            assertTrue(e == d.childRefs[1].get());
            assertTrue(f == d.childRefs[2].get());
            assertTrue(f == d.childRefs[3].get());
            assertEquals(2, root.globalDepth);
            assertEquals(2, d.globalDepth);
            assertEquals(1, e.globalDepth);
            assertEquals(1, f.globalDepth);
            assertEquals(2, e.getKeyCount());
            assertEquals(2, f.getKeyCount());

            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyKeysRaba(2, new byte[][] { // keys
                            k1, k2, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v1, v2, null, null })),//
                    e);
            assertSameBucketData(new MockBucketData(//
                    new ReadOnlyKeysRaba(2, new byte[][] { // keys
                            k3, k4, null, null }),//
                    new ReadOnlyValuesRaba(2, new byte[][] { // vals
                            v3, v4, null, null })),//
                    f);
			assertSameIteratorAnyOrder(new byte[][] { v1, v2, v3, v4 },
					htree.values());
            
        } finally {

            store.destroy();

        }
    }
    
}
