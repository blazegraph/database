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
 * Created on Dec 11, 2006
 */

package com.bigdata.htree;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for iterators that visit only dirty nodes or leaves. This suite
 * relies on (and to some extent validates) both node and leaf IO and
 * copy-on-write mechanisms.
 * 
 * @see DirectoryPage#childIterator(boolean)
 * @see AbstractPage#postOrderNodeIterator(boolean, boolean)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Extend this test suite to verify that remove() on the htree
 *          uses the copy-on-write pattern correctly.
 */
public class TestDirtyIterators extends AbstractHTreeTestCase {

    /**
     * 
     */
	public TestDirtyIterators() {
	}

	/**
	 * @param name
	 */
	public TestDirtyIterators(String name) {
		super(name);
	}

	/**
	 * Test ability to visit the direct dirty children of a node. For this test
	 * we only verify that the dirty child iterator will visit the same children
	 * as the normal child iterator. This is true since we never evict a node
	 * onto the store during this test (the index is setup to throw an exception
	 * if the tree attempts a node eviction).
	 */
	public void test_dirtyChildIterator01() {

		final IRawStore store = new SimpleMemoryRawStore();

		try {

			final HTree htree = getHTree(store, 2/* addressBits */);

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

            {

            	// verify initial structure.
				final DirectoryPage root = htree.root;
				final BucketPage a = (BucketPage) root.getChild(0);
				assertTrue(a == root.getChild(1));
				assertTrue(a == root.getChild(2));
				assertTrue(a == root.getChild(3));

				// direct child iterator.
				assertSameIterator(new AbstractPage[] { a },
						root.childIterator(false/* dirtyOnly */));
				assertSameIterator(new AbstractPage[] { a },
						root.childIterator(true/* dirtyOnly */));

		        // empty tree visits the sole bucket page followed by the root.
				assertSameIterator(new AbstractPage[] { a, root },
						htree.root.postOrderNodeIterator(false));
				assertSameIterator(new AbstractPage[] { a, root },
						htree.root.postOrderNodeIterator(true));

            }
			{

				htree.insert(k1, v1);
				htree.insert(k2, v2);
				htree.insert(k3, v3);
				htree.insert(k4, v4);

				if (log.isInfoEnabled())
					log.info("After insert: " + htree.PP());

				// There should be only one directory page and one bucket page.
				assertEquals("nnodes", 1, htree.getNodeCount());
				assertEquals("nleaves", 1, htree.getLeafCount());
				assertEquals("nentries", 4, htree.getEntryCount());
				final BucketPage a = (BucketPage) htree.root.getChild(0);

				assertSameIterator(new AbstractPage[] { a },
						htree.root.childIterator(false/* onlyDirty */));

				assertSameIterator(new AbstractPage[] { a },
						htree.root.childIterator(true/* onlyDirty */));

			}
			
			/**
			 * Insert another tuple to force some splits.
			 * 
			 * The expected post-condition is:
			 * 
			 * <pre>
			 * #nodes=3, #leaves=3, #entries=5
			 * D#88 [2] (D#62,-,-,-)
			 * D#62 [2]     (D#65,-,-,-)
			 * D#65 [2]         (B#99,B#35,-,-)
			 * B#99 [2]             ([1](00000001),[2](00000010),[3](00000011),-)
			 * B#35 [2]             ([4](00000100),[5](00000101),-,-)
			 * </pre>
			 */
			{

				htree.insert(k5, v5);
				
				if (log.isInfoEnabled())
					log.info("After another insert: " + htree.PP());
				
				assertEquals("nnodes", 3, htree.getNodeCount());
				assertEquals("nleaves", 2, htree.getLeafCount());
				assertEquals("nentries", 5, htree.getEntryCount());
				
				final DirectoryPage a = (DirectoryPage) htree.root.getChild(0);
				final BucketPage b = (BucketPage) htree.root.getChild(1);
				final BucketPage c = (BucketPage) htree.root.getChild(2);
				assertTrue(c == (BucketPage) htree.root.getChild(3));

				assertSameIterator(new AbstractPage[] { a, b, c },
						htree.root.childIterator(false/* onlyDirty */));

				assertSameIterator(new AbstractPage[] { a, b, c},
						htree.root.childIterator(true/* onlyDirty */));

			}

		} finally {

			store.destroy();

		}

    }

    /**
     * Test ability to visit the direct dirty children of a node. This test
     * works by explicitly writing out either the root node or a leaf and
     * verifying that the dirty children iterator correctly visits only those
     * children that should be marked as dirty after the others have been
     * written onto the store. Note that this does not force the eviction of
     * nodes or leaves but rather requests that the are written out directly.
     * Whenever we make an immutable node or leaf mutable using copy-on-write,
     * we wind up with a new reference for that node or leaf and update the
     * variables in the test appropriately.
     */
    public void test_dirtyChildIterator02() {

        final byte[] k1 = new byte[]{0x01};
        final byte[] k2 = new byte[]{0x02};
        final byte[] k3 = new byte[]{0x03};
        final byte[] k4 = new byte[]{0x04};
        final byte[] k5 = new byte[]{0x05};

        final byte[] v1 = new byte[]{0x01};
		final byte[] v2 = new byte[] { 0x02 };
		final byte[] v3 = new byte[] { 0x03 };
		final byte[] v4 = new byte[] { 0x04 };
		final byte[] v5 = new byte[] { 0x05 };

		final IRawStore store = new SimpleMemoryRawStore();

		try {

			final HTree htree = getHTree(store, 2/* addressBits */);

			/**
			 * Insert another tuple to force some splits. The expected
			 * post-condition is:
			 * 
			 * <pre>
			 * #nodes=3, #leaves=3, #entries=5
			 * D#88 [2] (D#62,B#80,B#20,B#20) // root
			 * D#62 [2]     (D#65,B#94,B#92,B#92) // a
			 * D#65 [2]         (B#99,B#35,B#65,B#65) // d
			 * B#99 [2]             ([1](00000001),[2](00000010),[3](00000011),-) // g
			 * B#35 [2]             ([4](00000100),[5](00000101),-,-) // h
			 * B#65 [1]             (-,-,-,-) // i
			 * B#94 [2]         (-,-,-,-) // e
			 * B#92 [1]         (-,-,-,-) // f
			 * B#80 [2]     (-,-,-,-) // b
			 * B#20 [1]     (-,-,-,-) // c
			 * </pre>
			 */
			htree.insert(k1, v1);
			htree.insert(k2, v2);
			htree.insert(k3, v3);
			htree.insert(k4, v4);
			htree.insert(k5, v5);

			{

				if (log.isInfoEnabled())
					log.info("After initial inserts: " + htree.PP());

				assertEquals("nnodes", 3, htree.getNodeCount());
				assertEquals("nleaves", 2, htree.getLeafCount());
				assertEquals("nentries", 5, htree.getEntryCount());

				final DirectoryPage root = (DirectoryPage) htree.root;
				final DirectoryPage a = (DirectoryPage) htree.root.getChild(0);
				final BucketPage b = (BucketPage) htree.root.getChild(1);
				final BucketPage c = (BucketPage) htree.root.getChild(2);
				assertTrue(c == (BucketPage) htree.root.getChild(3));
				final DirectoryPage d = (DirectoryPage) a.getChild(0);
				final BucketPage e = (BucketPage) a.getChild(1);
				final BucketPage f = (BucketPage) a.getChild(2);
				assertTrue(f == (BucketPage) a.getChild(3));
				final BucketPage g = (BucketPage) d.getChild(0);
				final BucketPage h = (BucketPage) d.getChild(1);
				final BucketPage i = (BucketPage) d.getChild(2);
				assertTrue(i == (BucketPage) d.getChild(3));

				/*
				 * Verify that iterator visits all direct children (all direct
				 * children are dirty).
				 */

				// root's children
				assertSameIterator(new AbstractPage[] { a, b, c },
						htree.root.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { a, b, c },
						htree.root.childIterator(true/* onlyDirty */));
				// a's children
				assertSameIterator(new AbstractPage[] { d, e, f },
						a.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { d, e, f },
						a.childIterator(true/* onlyDirty */));
				// d's children
				assertSameIterator(new AbstractPage[] { g, h, i },
						d.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { g, h, i },
						d.childIterator(true/* onlyDirty */));

				// verify the post-order iterator (all pages are dirty).
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, b,
						c, root },
						htree.root.postOrderNodeIterator(false/* dirtyOnly */));
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, b,
						c, root },
						htree.root.postOrderNodeIterator(true/* dirtyOnly */));

				/*
				 * write (b) onto the store and verify that it is no longer
				 * visited by the dirty child iterator for its parent.
				 */
				htree.writeNodeRecursive(b);
				assertFalse(b.isDirty());
				assertTrue(b.isPersistent());
				// direct children
				assertSameIterator(new AbstractPage[] { a, b, c },
						htree.root.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { a, c },
						htree.root.childIterator(true/* onlyDirty */));
				// verify the post-order iterator.
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, b,
						c, root },
						htree.root.postOrderNodeIterator(false/* dirtyOnly */));
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, c,
						root },
						htree.root.postOrderNodeIterator(true/* dirtyOnly */));

				/*
				 * write (c) onto the store and verify that it is no longer
				 * visited by the dirty child iterator for its parent.
				 */
				htree.writeNodeRecursive(c);
				assertFalse(c.isDirty());
				assertTrue(c.isPersistent());
				// direct children.
				assertSameIterator(new AbstractPage[] { a, b, c },
						htree.root.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { a },
						htree.root.childIterator(true/* onlyDirty */));
				// verify the post-order iterator.
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, b,
						c, root },
						htree.root.postOrderNodeIterator(false/* dirtyOnly */));
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, 
						root },
						htree.root.postOrderNodeIterator(true/* dirtyOnly */));

				/*
				 * write (d) onto the store and verify that it is no longer
				 * visited by the dirty child iterator for its parent.
				 */
				// before we write out (d).
				assertTrue(d.isDirty());
				assertFalse(d.isPersistent());
				// verify direct children.
				assertSameIterator(new AbstractPage[] { g, h, i },
						d.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { g, h, i },
						d.childIterator(true/* onlyDirty */));
				htree.writeNodeRecursive(d); // write page (recursive)
				assertFalse(d.isDirty());
				assertTrue(d.isPersistent());
				// verify direct children.
				assertSameIterator(new AbstractPage[] { d, e, f },
						a.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { e, f },
						a.childIterator(true/* onlyDirty */));
				// also verify that none of the children of (d) are visited by
				// the dirty iterator for (d).
				assertSameIterator(new AbstractPage[] { g, h, i },
						d.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] {},
						d.childIterator(true/* onlyDirty */));
				// verify the post-order iterator.
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, b,
						c, root },
						htree.root.postOrderNodeIterator(false/* dirtyOnly */));
				assertSameIterator(new AbstractPage[] { e, f, a, root },
						htree.root.postOrderNodeIterator(true/* dirtyOnly */));

				/*
				 * write (f) onto the store and verify that it is no longer
				 * visited by the dirty child iterator for its parent (a).
				 */
				htree.writeNodeRecursive(f);
				assertFalse(f.isDirty());
				assertTrue(f.isPersistent());
				// direct children of f's parent (a).
				assertSameIterator(new AbstractPage[] { d, e, f },
						a.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { e },
						a.childIterator(true/* onlyDirty */));
				// verify the post-order iterator.
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, b,
						c, root },
						htree.root.postOrderNodeIterator(false/* dirtyOnly */));
				assertSameIterator(new AbstractPage[] { e, a, root },
						htree.root.postOrderNodeIterator(true/* dirtyOnly */));

				/*
				 * write (a) onto the store and verify that it is no longer
				 * visited by the dirty child iterator for its parent.
				 */
				htree.writeNodeRecursive(a);
				assertFalse(a.isDirty());
				assertTrue(a.isPersistent());
				// direct children of a's parent (root).
				assertSameIterator(new AbstractPage[] { a, b, c },
						htree.root.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] {},
						htree.root.childIterator(true/* onlyDirty */));
				// verify the post-order iterator.
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, b,
						c, root },
						htree.root.postOrderNodeIterator(false/* dirtyOnly */));
				assertSameIterator(new AbstractPage[] { root },
						htree.root.postOrderNodeIterator(true/* dirtyOnly */));

				// Verify dirty/clear state before insert of duplicate key.
				assertFalse(d.isDirty());
				assertTrue(d.isPersistent());
				assertFalse(g.isDirty());
				assertTrue(g.isPersistent());

			}

			/**
			 * insert a duplicate key (3). this goes into (g) which becomes full
			 * but is not split. verify that the children of the parent (d) are
			 * unchanged but that (g) is no longer persistent and is now dirty.
			 * Since the parent (d) of (g) was persistent, copy-on-write was
			 * also invoked for (d) and it is now mutable and dirty.
			 * 
			 * Note: Some of the page references will have been changed by
			 * copy-on-write, so we re-validate the index structure at this
			 * time. The expected post-condition is:
			 * 
			 * <pre>
			 * D#95* [2] (D#05*,B#58 ,B#62 ,B#62 )
			 * D#05* [2]     (D#99*,B#65 ,B#94 ,B#94 )
			 * D#99* [2]         (B#48*,B#99 ,B#35 ,B#35 )
			 * B#48* [2]             ([1](00000001),[2](00000010),[3](00000011),[3](00000011))
			 * B#99  [2]             ([4](00000100),[5](00000101),-,-)
			 * B#35  [1]             (-,-,-,-)
			 * B#65  [2]         (-,-,-,-)
			 * B#94  [1]         (-,-,-,-)
			 * B#58  [2]     (-,-,-,-)
			 * B#62  [1]     (-,-,-,-)
			 * </pre>
			 */
			{

				if (log.isInfoEnabled())
					log.info("Before insert of duplicate key (3) into (g): "
							+ htree.PP());

				htree.insert(k3, v3);

				if (log.isInfoEnabled())
					log.info("After insert of duplicate key (3) into (g): "
							+ htree.PP());

				// re-validate the index structure.
				final DirectoryPage root = htree.root;
				final DirectoryPage a = (DirectoryPage) htree.root.getChild(0);
				final BucketPage b = (BucketPage) htree.root.getChild(1);
				final BucketPage c = (BucketPage) htree.root.getChild(2);
				assertTrue(c == (BucketPage) htree.root.getChild(3));
				final DirectoryPage d = (DirectoryPage) a.getChild(0);
				final BucketPage e = (BucketPage) a.getChild(1);
				final BucketPage f = (BucketPage) a.getChild(2);
				assertTrue(f == (BucketPage) a.getChild(3));
				final BucketPage g = (BucketPage) d.getChild(0);
				final BucketPage h = (BucketPage) d.getChild(1);
				final BucketPage i = (BucketPage) d.getChild(2);
				assertTrue(i == (BucketPage) d.getChild(3));

				assertTrue(d.isDirty());
				assertFalse(d.isPersistent());
				assertTrue(g.isDirty());
				assertFalse(g.isPersistent());

				// check dirty iterator for (g)'s parent (d).
				assertSameIterator(new AbstractPage[] { g, h, i },
						d.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { g },
						d.childIterator(true/* onlyDirty */));

				// check dirty iterator for (d)'s parent (a).
				assertSameIterator(new AbstractPage[] { d, e, f },
						a.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { d },
						a.childIterator(true/* onlyDirty */));

				// check dirty iterator for (a)'s parent (root)
				assertSameIterator(new AbstractPage[] { a, b, c },
						htree.root.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] { a },
						htree.root.childIterator(true/* onlyDirty */));

				// verify the post-order iterator.
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, b,
						c, root },
						htree.root.postOrderNodeIterator(false/* dirtyOnly */));
				assertSameIterator(new AbstractPage[] { g, d, a, root },
						htree.root.postOrderNodeIterator(true/* dirtyOnly */));

				/*
				 * Write the root node of the tree onto the store.
				 */
				htree.writeNodeRecursive(htree.root);

				assertFalse(htree.root.isDirty());
				assertFalse(a.isDirty());
				assertFalse(d.isDirty());

				// check dirty iterator for (root)
				assertSameIterator(new AbstractPage[] { a, b, c },
						htree.root.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] {},
						htree.root.childIterator(true/* onlyDirty */));

				// check dirty iterator for (a).
				assertSameIterator(new AbstractPage[] { d, e, f },
						a.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] {},
						a.childIterator(true/* onlyDirty */));

				// check dirty iterator for (d)
				assertSameIterator(new AbstractPage[] { g, h, i },
						d.childIterator(false/* onlyDirty */));
				assertSameIterator(new AbstractPage[] {},
						d.childIterator(true/* onlyDirty */));
				
				// verify the post-order iterator.
				assertSameIterator(new AbstractPage[] { g, h, i, d, e, f, a, b,
						c, root },
						htree.root.postOrderNodeIterator(false/* dirtyOnly */));
				assertSameIterator(new AbstractPage[] { },
						htree.root.postOrderNodeIterator(true/* dirtyOnly */));


			}

		} finally {

			store.destroy();

		}

	}

//    /**
//     * Test ability to visit the dirty nodes of the tree in a post-order
//     * traversal. This version of the test verifies that the dirty post-order
//     * iterator will visit the same nodes as the normal post-order iterator
//     * since all nodes are dirty.
//     */
//    public void test_dirtyPostOrderIterator01() {
//
//        BTree btree = getBTree(3);
//
//        final Leaf a = (Leaf) btree.root;
//        
//        SimpleEntry v1 = new SimpleEntry(1);
//        SimpleEntry v2 = new SimpleEntry(2);
//        SimpleEntry v3 = new SimpleEntry(3);
//        SimpleEntry v4 = new SimpleEntry(4);
//        SimpleEntry v6 = new SimpleEntry(6);
//        SimpleEntry v5 = new SimpleEntry(5);
//        SimpleEntry v7 = new SimpleEntry(7);
//        SimpleEntry v9 = new SimpleEntry(9);
//
//        // empty tree visits the root leaf.
//        assertSameIterator(new IAbstractNode[] { btree.root }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { btree.root }, btree.root
//                .postOrderNodeIterator(true));
//        
//        // fill up the root leaf.
//        btree.insert(TestKeyBuilder.asSortKey(3), v3);
//        btree.insert(TestKeyBuilder.asSortKey(5), v5);
//        btree.insert(TestKeyBuilder.asSortKey(7), v7);
//
//        // split the root leaf.
//        btree.insert(TestKeyBuilder.asSortKey(9), v9);
//        final Node c = (Node) btree.root;
//        assertKeys(new int[]{7},c);
//        assertEquals(a,c.getChild(0));
//        final Leaf b = (Leaf)c.getChild(1);
//        assertKeys(new int[]{3,5},a);
//        assertValues(new Object[]{v3,v5}, a);
//        assertKeys(new int[]{7,9},b);
//        assertValues(new Object[]{v7,v9}, b);
//        
//        // verify iterator.
//        assertSameIterator(new IAbstractNode[] { a, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { a, b, c }, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * split another leaf so that there are now three children to visit. at
//         * this point the root is full.
//         */
//        btree.insert(TestKeyBuilder.asSortKey(1), v1);
//        btree.insert(TestKeyBuilder.asSortKey(2), v2);
//        assertKeys(new int[]{3,7},c);
//        assertEquals(a,c.getChild(0));
//        Leaf d = (Leaf)c.getChild(1);
//        assertEquals(b,c.getChild(2));
//        assertKeys(new int[]{1,2},a);
//        assertValues(new Object[]{v1,v2}, a);
//        assertKeys(new int[]{3,5},d);
//        assertValues(new Object[]{v3,v5}, d);
//        assertKeys(new int[]{7,9},b);
//        assertValues(new Object[]{v7,v9}, b);
//
//        // verify iterator
//        assertSameIterator(new IAbstractNode[] { a, d, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { a, d, b, c }, btree.root
//                .postOrderNodeIterator(true));
//        
//        /*
//         * cause another leaf (d) to split, forcing the split to propagate to and
//         * split the root and the tree to increase in height.
//         */
//        btree.insert(TestKeyBuilder.asSortKey(4), v4);
//        btree.insert(TestKeyBuilder.asSortKey(6), v6);
////        btree.dump(Level.DEBUG,System.err);
//        assertNotSame(c,btree.root);
//        final Node g = (Node)btree.root;
//        assertKeys(new int[]{5},g);
//        assertEquals(c,g.getChild(0));
//        final Node f = (Node)g.getChild(1);
//        assertKeys(new int[]{3},c);
//        assertEquals(a,c.getChild(0));
//        assertEquals(d,c.getChild(1));
//        assertKeys(new int[]{1,2},a);
//        assertValues(new Object[]{v1,v2}, a);
//        assertKeys(new int[]{3,4},d);
//        assertValues(new Object[]{v3,v4}, d);
//        assertKeys(new int[]{7},f);
//        Leaf e = (Leaf)f.getChild(0);
//        assertEquals(b,f.getChild(1));
//        assertKeys(new int[]{5,6},e);
//        assertValues(new Object[]{v5,v6}, e);
//        assertKeys(new int[]{7,9},b);
//        assertValues(new Object[]{v7,v9}, b);
//
//        // verify iterator
//        assertSameIterator(new IAbstractNode[] { a, d, c, e, b, f, g }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { a, d, c, e, b, f, g }, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * remove a key (4) from (d) forcing (d,a) to merge into (d) and (a) to
//         * be deleted. this causes (c,f) to merge as well, which in turn forces
//         * the root to be replaced by (c).
//         */
//        assertEquals(v4,btree.remove(TestKeyBuilder.asSortKey(4)));
////        btree.dump(Level.DEBUG,System.err);
//        assertKeys(new int[]{5,7},c);
//        assertEquals(d,c.getChild(0));
//        assertEquals(e,c.getChild(1));
//        assertEquals(b,c.getChild(2));
//        assertKeys(new int[]{1,2,3},d);
//        assertValues(new Object[]{v1,v2,v3}, d);
//        assertKeys(new int[]{5,6},e);
//        assertValues(new Object[]{v5,v6}, e);
//        assertKeys(new int[]{7,9},b);
//        assertValues(new Object[]{v7,v9}, b);
//        assertTrue(a.isDeleted());
//
//        // verify iterator
//        assertSameIterator(new IAbstractNode[] { d, e, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { d, e, b, c }, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * remove a key (7) from a leaf (b) forcing two leaves to join and
//         * verify the visitation order.
//         */
//        assertEquals(v7,btree.remove(TestKeyBuilder.asSortKey(7)));
//        btree.dump(Level.DEBUG,System.err);
//        assertKeys(new int[]{5},c);
//        assertEquals(d,c.getChild(0));
//        assertEquals(b,c.getChild(1));
//        assertKeys(new int[]{1,2,3},d);
//        assertValues(new Object[]{v1,v2,v3}, d);
//        assertKeys(new int[]{5,6,9},b);
//        assertValues(new Object[]{v5,v6,v9}, b);
//        assertTrue(e.isDeleted());
//
//        // verify iterator
//        assertSameIterator(new IAbstractNode[] { d, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { d, b, c }, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * remove keys from a leaf forcing the remaining two leaves to join and
//         * verify the visitation order.
//         */
//        assertEquals(v3,btree.remove(TestKeyBuilder.asSortKey(3)));
//        assertEquals(v5,btree.remove(TestKeyBuilder.asSortKey(5)));
//        assertEquals(v6,btree.remove(TestKeyBuilder.asSortKey(6)));
//        assertKeys(new int[]{1,2,9},b);
//        assertValues(new Object[]{v1,v2,v9}, b);
//        assertTrue(d.isDeleted());
//        assertTrue(c.isDeleted());
//
//        // verify iterator
//        assertSameIterator(new IAbstractNode[] { b }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { b }, btree.root
//                .postOrderNodeIterator(true));
//
//    }
//
//    /**
//     * Test ability to visit the dirty nodes of the tree in a post-order
//     * traversal. This version of the test writes out some nodes and/or leaves
//     * in order to verify that the post-order iterator will visit only those
//     * nodes and leaves that are currently dirty. Note that writing out a node
//     * or leaf makes it immutable. In order to make the node or leaf dirty again
//     * we have to modify it, which triggers copy-on-write. Copy on write
//     * propagates up from the leaf where we make the mutation and causes any
//     * immutable parents to be cloned as well. Nodes and leaves that have been
//     * cloned by copy-on-write are distinct objects from their immutable
//     * predecessors.
//     */
//    public void test_dirtyPostOrderIterator02() {
//
//        BTree btree = getBTree(3);
//
//        Leaf a = (Leaf) btree.root;
//        
//        SimpleEntry v1 = new SimpleEntry(1);
//        SimpleEntry v2 = new SimpleEntry(2);
//        SimpleEntry v3 = new SimpleEntry(3);
//        SimpleEntry v4 = new SimpleEntry(4);
//        SimpleEntry v6 = new SimpleEntry(6);
//        SimpleEntry v5 = new SimpleEntry(5);
//        SimpleEntry v7 = new SimpleEntry(7);
//        SimpleEntry v9 = new SimpleEntry(9);
//
//        // empty tree visits the root leaf.
//        assertSameIterator(new IAbstractNode[] { btree.root }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { btree.root }, btree.root
//                .postOrderNodeIterator(true));
//        /*
//         * write out the root leaf on the store and verify that the dirty
//         * iterator does not visit anything while the normal iterator visits the
//         * root.
//         */
//        btree.writeNodeRecursive(btree.root);
//        assertSameIterator(new IAbstractNode[] { btree.root }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] {}, btree.root
//                .postOrderNodeIterator(true));
//        
//        /*
//         * Fill up the root leaf. Since it was immutable, this will trigger
//         * copy-on-write.  We verify that the root leaf reference is changed
//         * and verify that both iterators now visit the root.
//         */
//        assertEquals(a,btree.root);
//        btree.insert(TestKeyBuilder.asSortKey(3), v3);
//        assertNotSame(a,btree.root);
//        a = (Leaf)btree.root; // new reference for the root leaf.
//        assertSameIterator(new IAbstractNode[] { btree.root }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { btree.root }, btree.root
//                .postOrderNodeIterator(true));
//        btree.insert(TestKeyBuilder.asSortKey(5), v5);
//        btree.insert(TestKeyBuilder.asSortKey(7), v7);
//
//        // split the root leaf.
//        btree.insert(TestKeyBuilder.asSortKey(9), v9);
//        Node c = (Node) btree.root;
//        assertKeys(new int[]{7},c);
//        assertEquals(a,c.getChild(0));
//        Leaf b = (Leaf)c.getChild(1);
//        assertKeys(new int[]{3,5},a);
//        assertValues(new Object[]{v3,v5}, a);
//        assertKeys(new int[]{7,9},b);
//        assertValues(new Object[]{v7,v9}, b);
//        
//        // verify iterator.
//        assertSameIterator(new IAbstractNode[] { a, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { a, b, c }, btree.root
//                .postOrderNodeIterator(true));
//        
//        /*
//         * write out (a) and verify the iterator behaviors.
//         */
//        btree.writeNodeOrLeaf(a);
//        assertTrue(a.isPersistent());
//        assertFalse(b.isPersistent());
//        assertFalse(c.isPersistent());
//        assertSameIterator(new IAbstractNode[] { a, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { b, c }, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * write out (c) and verify the iterator behaviors.
//         */
//        btree.writeNodeRecursive(c);
//        assertTrue(a.isPersistent());
//        assertTrue(b.isPersistent());
//        assertTrue(c.isPersistent());
//        assertSameIterator(new IAbstractNode[] { a, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { }, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * split another leaf (a) so that there are now three children to visit.
//         * at this point the root is full.
//         */
//        assertTrue(a.isPersistent());
//        assertTrue(b.isPersistent());
//        assertTrue(c.isPersistent());
//        btree.insert(TestKeyBuilder.asSortKey(1), v1); // triggers copy on write for (a) and (c).
//        assertNotSame(c,btree.root);
//        c = (Node)btree.root;
//        assertNotSame(a,c.getChild(0));
//        a = (Leaf)c.getChild(0);
//        assertEquals(b,c.getChild(1)); // b was not copied.
//        assertFalse(a.isPersistent());
//        assertTrue(b.isPersistent());
//        assertFalse(c.isPersistent());
//        btree.insert(TestKeyBuilder.asSortKey(2), v2);
//        assertKeys(new int[]{3,7},c);
//        assertEquals(a,c.getChild(0));
//        Leaf d = (Leaf)c.getChild(1);
//        assertEquals(b,c.getChild(2));
//        assertKeys(new int[]{1,2},a);
//        assertValues(new Object[]{v1,v2}, a);
//        assertKeys(new int[]{3,5},d);
//        assertValues(new Object[]{v3,v5}, d);
//        assertKeys(new int[]{7,9},b);
//        assertValues(new Object[]{v7,v9}, b);
//
//        // verify iterator
//        assertFalse(a.isPersistent());
//        assertTrue(b.isPersistent());
//        assertFalse(c.isPersistent());
//        assertSameIterator(new IAbstractNode[] { a, d, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { a, d, c }, btree.root
//                .postOrderNodeIterator(true));
//        
//        /*
//         * cause another leaf (d) to split, forcing the split to propagate to and
//         * split the root and the tree to increase in height.
//         */
//        btree.insert(TestKeyBuilder.asSortKey(4), v4);
//        btree.insert(TestKeyBuilder.asSortKey(6), v6);
////        btree.dump(Level.DEBUG,System.err);
//        assertNotSame(c,btree.root);
//        final Node g = (Node)btree.root;
//        assertKeys(new int[]{5},g);
//        assertEquals(c,g.getChild(0));
//        final Node f = (Node)g.getChild(1);
//        assertKeys(new int[]{3},c);
//        assertEquals(a,c.getChild(0));
//        assertEquals(d,c.getChild(1));
//        assertKeys(new int[]{1,2},a);
//        assertValues(new Object[]{v1,v2}, a);
//        assertKeys(new int[]{3,4},d);
//        assertValues(new Object[]{v3,v4}, d);
//        assertKeys(new int[]{7},f);
//        Leaf e = (Leaf)f.getChild(0);
//        assertEquals(b,f.getChild(1));
//        assertKeys(new int[]{5,6},e);
//        assertValues(new Object[]{v5,v6}, e);
//        assertKeys(new int[]{7,9},b);
//        assertValues(new Object[]{v7,v9}, b);
//
//        // verify iterator
//        assertSameIterator(new IAbstractNode[] { a, d, c, e, b, f, g }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { a, d, c, e, f, g }, btree.root
//                .postOrderNodeIterator(true));
//        
//        /*
//         * write out a subtree and revalidate the iterators.
//         */
//        btree.writeNodeRecursive(c);
//        assertSameIterator(new IAbstractNode[] { a, d, c, e, b, f, g }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { e, f, g }, btree.root
//                .postOrderNodeIterator(true));
//        
//        /*
//         * write out a leaf and revalidate the iterators.
//         */
//        btree.writeNodeRecursive(e);
//        assertSameIterator(new IAbstractNode[] { a, d, c, e, b, f, g }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { f, g }, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * write out the entire tree and revalidate the iterators.
//         */
//        btree.writeNodeRecursive(g);
//        assertSameIterator(new IAbstractNode[] { a, d, c, e, b, f, g }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] {}, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * remove a key (4) from (d) forcing (d,a) to merge into (d) and (a) to
//         * be deleted. this causes (c,f) to merge as well, which in turn forces
//         * the root to be replaced by (c).
//         * 
//         * the following are cloned: d, c, g.
//         */
//        assertEquals(v4,btree.remove(TestKeyBuilder.asSortKey(4)));
//        assertNotSame(g,btree.root);
//        assertNotSame(c,btree.root);
//        c = (Node) btree.root;
//        assertNotSame(d,c.getChild(0));
//        d = (Leaf) c.getChild(0);
////        btree.dump(Level.DEBUG,System.err);
//        assertKeys(new int[]{5,7},c);
//        assertEquals(d,c.getChild(0));
//        assertEquals(e,c.getChild(1));
//        assertEquals(b,c.getChild(2));
//        assertKeys(new int[]{1,2,3},d);
//        assertValues(new Object[]{v1,v2,v3}, d);
//        assertKeys(new int[]{5,6},e);
//        assertValues(new Object[]{v5,v6}, e);
//        assertKeys(new int[]{7,9},b);
//        assertValues(new Object[]{v7,v9}, b);
//        assertTrue(a.isDeleted());
//        assertTrue(f.isDeleted());
//
//        // verify iterator
//        assertSameIterator(new IAbstractNode[] { d, e, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { d, c }, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * remove a key (7) from a leaf (b) forcing two leaves (b,e) into (b) to
//         * join and verify the visitation order.
//         */
//        assertEquals(v7,btree.remove(TestKeyBuilder.asSortKey(7)));
//        btree.dump(Level.DEBUG,System.err);
//        assertKeys(new int[]{5},c);
//        assertEquals(d,c.getChild(0));
//        assertNotSame(b,c.getChild(1));
//        b = (Leaf) c.getChild(1);
//        assertKeys(new int[]{1,2,3},d);
//        assertValues(new Object[]{v1,v2,v3}, d);
//        assertKeys(new int[]{5,6,9},b);
//        assertValues(new Object[]{v5,v6,v9}, b);
//        assertTrue(e.isDeleted());
//
//        // verify iterator
//        assertSameIterator(new IAbstractNode[] { d, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { d, b, c }, btree.root
//                .postOrderNodeIterator(true));
//        /*
//         * write out the root and verify the visitation orders.
//         */
//        btree.writeNodeRecursive(c);
//        assertSameIterator(new IAbstractNode[] { d, b, c }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] {}, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * remove keys from a leaf (b) forcing the remaining two leaves (b,d) to
//         * join into (b). Since there is only one leaf, that leaf now becomes
//         * the new root leaf of the tree.
//         */
//        assertEquals(c,btree.root);
//        assertEquals(d,c.getChild(0));
//        assertEquals(b,c.getChild(1));
//        assertEquals(v3, btree.remove(TestKeyBuilder.asSortKey(3))); // remove from (d)
//        assertNotSame(c,btree.root); // c was cloned.
//        c = (Node) btree.root;
//        assertNotSame(d,c.getChild(0));
//        d = (Leaf)c.getChild(0); // d was cloned.
//        assertEquals(b,c.getChild(1));
//        assertEquals(v5,btree.remove(TestKeyBuilder.asSortKey(5))); // remove from (b)
//        assertNotSame(b,c.getChild(1));
//        b = (Leaf)c.getChild(1); // b was cloned.
//        assertEquals(v6,btree.remove(TestKeyBuilder.asSortKey(6))); // remove from (b)
//        assertKeys(new int[]{1,2,9},b);
//        assertValues(new Object[]{v1,v2,v9}, b);
//        assertTrue(d.isDeleted());
//        assertTrue(c.isDeleted());
//
//        // verify iterator
//        assertSameIterator(new IAbstractNode[] { b }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] { b }, btree.root
//                .postOrderNodeIterator(true));
//
//        /*
//         * write out the root and reverify the iterators.
//         */
//        btree.writeNodeRecursive(b);
//        assertSameIterator(new IAbstractNode[] { b }, btree.root
//                .postOrderNodeIterator(false));
//        assertSameIterator(new IAbstractNode[] {}, btree.root
//                .postOrderNodeIterator(true));
//        
//    }

}
