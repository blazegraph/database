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
/*
 * Created on Nov 27, 2006
 */

package com.bigdata.btree;

import java.util.Arrays;

import org.apache.log4j.Level;

import com.bigdata.btree.keys.KeyBuilder;

/**
 * Test suite using {@link BTree#insert(Object, Object)} to split a tree to
 * height two (2) (three levels) and then using {@link BTree#remove(Object)} to
 * reduce the tree back to a single, empty root leaf. This test suite is focused
 * on m := 3 since we are capable of exercising all split() and join() code 
 * paths with that branching factor.
 * <p>
 * Note: This also tests the {@link AbstractNode#isLeftMostNode()} and
 * {@link AbstractNode#isRightMostNode()} methods. In order to test those
 * methods as applied to track the #of head splits and tail splits we need a
 * btree with at least 2 levels of nodes above a layer of leaves. This is
 * because the methods are tested on a leaf, which checks its parent, which,
 * really, needs to test a non-root parent before we can say that this is
 * working as it should.
 * 
 * @see src/architecture/btree.xls for the examples used in this test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSplitJoinThreeLevels extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestSplitJoinThreeLevels() {
    }

    /**
     * @param name
     */
    public TestSplitJoinThreeLevels(String name) {
        super(name);
    }

    /**
     * Test ability to split and join a tree of order m == 3 driven by the
     * insertion and then the removal of a known sequence of keys. This test
     * checks the state of the tree after each operation against the expected
     * postconditions for that operation. In particular, testing at m == 3 helps
     * to check for fenceposts in the split/join logic.
     * 
     * Note: a branching factor of three (3) is equivalent to a 2-3 tree, where
     * the minimum #of children (for a node) or values (for a leaf) is two (2)
     * and the maximum #of children (for a node) or values (for a leaf) is three
     * (3). This makes it very easy to provoke splits and joins.
     * 
     * There is another version of this test that builds the same tree but uses
     * a different sequence of keys during removal. This provokes some code
     * paths in {@link Node#merge(AbstractNode, boolean)} and
     * {@link Node#redistributeKeys(AbstractNode, boolean)} that are not
     * exercised by this test.
     * 
     * @see #test_removeOrder3a()
     */
    public void test_removeOrder3a() {

        /*
         * Generate keys, values, and visitation order.
         */
        // keys
        final int[] keys = new int[]{5,6,7,8,3,4,2,1};
        // values
        final SimpleEntry v1 = new SimpleEntry(1);
        final SimpleEntry v2 = new SimpleEntry(2);
        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v4 = new SimpleEntry(4);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v6 = new SimpleEntry(6);
        final SimpleEntry v7 = new SimpleEntry(7);
        final SimpleEntry v8 = new SimpleEntry(8);
        final SimpleEntry[] vals = new SimpleEntry[]{v5,v6,v7,v8,v3,v4,v2,v1};
        // permutation vector for visiting values in key order.
        final int[] order = new int[keys.length];
        // generate visitation order.
        {
            System.arraycopy(keys, 0, order, 0, keys.length);
            Arrays.sort(order);
            System.err.println("keys="+Arrays.toString(keys));
            System.err.println("vals="+Arrays.toString(vals));
            System.err.println("order="+Arrays.toString(order));
        }
        
        final int m = 3;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        assertTrue(btree.dump(System.err));

        Leaf a = (Leaf) btree.getRoot();
        assertKeys(new int[]{},a);
        assertValues(new Object[]{},a);
        
        int n = 0;
        
        { // insert(5,5)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 5 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5},a);
            assertValues(new Object[]{v5},a);
            assertTrue(btree.dump(System.err));
        }

        { // insert(6,6)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 6 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5,6},a);
            assertValues(new Object[]{v5,v6},a);
            assertTrue(btree.dump(System.err));
        }
        
        /*
         * fills the root leaf to capacity.
         * 
         * postcondition:
         * 
         * keys: [ 5 6 7 ]
         */
        { // insert(7,7)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 7 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5,6,7},a);
            assertValues(new Object[]{v5,v6,v7},a);
            assertTrue(btree.dump(System.err));
        }

        /*
         * splits the root leaf
         * 
         * split(a)->(a,b), c is the new root.
         * 
         * postcondition:
         * 
         * c.keys[ 7 - x ]
         * c.clds[ a b - ]
         * 
         * a.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Node c;
        final Leaf b;
        { // insert(8,8)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 8 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate new root (c).
            c = (Node)btree.getRoot();
            assertKeys(new int[]{7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChild(1));
            assertNull(c.getChildRef(2));
            b = (Leaf)c.getChild(1);
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate original leaf (a).
            assertKeys(new int[]{5,6},a);
            assertValues(new Object[]{v5,v6},a);
            
            // validate new leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);
            
            assertTrue(btree.dump(System.err));
        }
        
        /*
         * insert(3,3)
         * 
         * postcondition:
         * 
         * c.keys[ 7 - x ]
         * c.clds[ a b - ]
         * 
         * a.keys[ 3 5 6 ]
         * b.keys[ 7 8 - ]
         */
        {
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 3 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            // validate original leaf (a).
            assertKeys(new int[]{3,5,6},a);
            assertValues(new Object[]{v3,v5,v6},a);
            assertEntryCounts(new int[]{3,2}, c);
            
        }

        /*
         * insert(4,4), causing split(a)->(a,d) and bringing (c) to capacity.
         * 
         * postcondition:
         * 
         * c.keys[ 5 7 x ]
         * c.clds[ a d b ]
         * 
         * a.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Leaf d;
        {
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 4 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate root (c).
            assertKeys(new int[]{5,7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChildRef(1));
            d = (Leaf) c.getChild(1);
            assertEquals(b,c.getChild(2));
            assertEntryCounts(new int[]{2,2,2}, c);
            
            // validate original leaf (a).
            assertKeys(new int[]{3,4},a);
            assertValues(new Object[]{v3,v4},a);
            
            // validate new leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);
            
        }
        
        /*
         * insert(2,2), bringing (a) to capacity again.
         */
        {
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 2 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            assertEntryCounts(new int[]{3,2,2}, c);
            
            // validate original leaf (a).
            assertKeys(new int[]{2,3,4},a);
            assertValues(new Object[]{v2,v3,v4},a);
            
        }
        
        /*
         * insert(1,1) causing (a) to split(a)->(a,e). Since the root (c) is
         * already at capacity this also causes the root to split(c)->(c,f) and
         * creating a new root(g).
         * 
         * postcondition:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a e - ]
         * 
         * f.keys[ 7 - x ]
         * f.clds[ d b - ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Leaf e;
        final Node f, g;
        {
            
            final int ikey = keys[n];
            final SimpleEntry val = vals[n++];
            assert ikey == 1 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));

            // validate the new root(g).
            assertNotSame(c,btree.getRoot());
            g = (Node)btree.getRoot();
            assertKeys(new int[]{5},g);
            assertEquals(c,g.getChild(0));
            assertNotNull(g.getChildRef(1));
            f = (Node) g.getChild(1);
            assertNull(g.getChildRef(2));
            assertEntryCounts(new int[]{4,4}, g);
            
            // validate old root (c).
            assertKeys(new int[]{3},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChildRef(1));
            e = (Leaf) c.getChild(1);
            assertNull(c.getChildRef(2));
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate node(f) split from the old root split(c)->(c,f).
            assertKeys(new int[]{7},f);
            assertEquals(d,f.getChild(0));
            assertEquals(b,f.getChild(1));
            assertNull(f.getChildRef(2));
            assertEntryCounts(new int[]{2,2}, f);
            
            // validate original leaf (a), which was re-split into (a,e).
            assertKeys(new int[]{1,2},a);
            assertValues(new Object[]{v1,v2},a);
            
            // validate new leaf (e).
            assertKeys(new int[]{3,4},e);
            assertValues(new Object[]{v3,v4},e);
            
            // validate new leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);

        }

        /*
         * Do some tests of isLeftMostNode() and isRightMostNode().
         */
        {
            
            // test the root.
            assertTrue(g.isLeftMostNode());
            assertTrue(g.isRightMostNode());
            
            // test the next layer of nodes.
            assertTrue(c.isLeftMostNode());
            assertFalse(c.isRightMostNode());
            
            assertFalse(f.isLeftMostNode());
            assertTrue(f.isRightMostNode());
            
            // test the leaves.
            assertTrue(a.isLeftMostNode());
            assertFalse(a.isRightMostNode());
            
            assertFalse(e.isLeftMostNode());
            assertFalse(e.isRightMostNode());
            
            assertFalse(d.isLeftMostNode());
            assertFalse(d.isRightMostNode());
            
            assertFalse(b.isLeftMostNode());
            assertTrue(b.isRightMostNode());
            
        }
        
        /*
         * Do some tests of getRightMostChild()
         */
        {
         
            assertTrue(f == btree.getRightMostNode(true/* nodesOnly */));

            assertTrue(b == btree.getRightMostNode(false/* nodesOnly */));
            
        }
        
        /*
         * At this point the tree is setup and we start deleting keys. We delete
         * the keys in (nearly) the reverse order and verify that joins correctly
         * reduce the tree as each node or leaf is reduced below its minimum.
         *  
         * before:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a e - ]
         * 
         * f.keys[ 7 - x ]
         * f.clds[ d b - ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        assertTrue("before removing keys", btree.dump(Level.DEBUG,System.err));
        
        /*
         * step#1 : remove(1) triggers a cascade of operations: a.join(e) calls
         * a.merge(e) and c.removeChild(3,e). This forces c.join(f), which calls
         * c.merge(f) and g.removeChild(5,f). Since (g) now has a single child
         * we replace the root with (c). e, f, and g are deleted as we go.
         *
         * postcondition:
         * 
         * c.keys[ 5 7 x ]
         * c.clds[ a d b ]
         * 
         * a.keys[ 2 3 4 ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         * 
         * e, f, g are deleted.
         */
        assertEquals(v1,btree.remove(KeyBuilder.asSortKey(1)));
        assertTrue("after remove(1)", btree.dump(Level.DEBUG,System.err));
        // verify leaves.
        assertKeys(new int[]{2,3,4},a);
        assertValues(new Object[]{v2,v3,v4},a);
        assertKeys(new int[]{5,6},d);
        assertValues(new Object[]{v5,v6},d);
        assertKeys(new int[]{7,8},b);
        assertValues(new Object[]{v7,v8},b);
        // verify the new root.
        assertKeys(new int[]{5,7},c);
        assertEquals(c,btree.root);
        assertEquals(a,c.getChild(0));
        assertEquals(d,c.getChild(1));
        assertEquals(b,c.getChild(2));
        assertEntryCounts(new int[]{3,2,2}, c);
        // verify deleted nodes and leaves.
        assertTrue(e.isDeleted());
        assertTrue(f.isDeleted());
        assertTrue(g.isDeleted());

        /*
         * step#2 : remove(2) - simple operation just removes(2) from (a).
         */
        assertEquals(v2,btree.remove(KeyBuilder.asSortKey(2)));
        assertTrue("after remove(2)", btree.dump(Level.DEBUG,System.err));
        assertKeys(new int[]{3,4},a);
        assertValues(new Object[]{v3,v4},a);
        assertEntryCounts(new int[]{2,2,2}, c);
        
        /*
         * step#3 : remove(4) triggers a.join(d), which in turn calls a.merge(d)
         * and causes c.removeChild(5,d).
         */
        assertEquals(v4,btree.remove(KeyBuilder.asSortKey(4)));
        assertTrue("after remove(4)", btree.dump(Level.DEBUG,System.err));
        // verify leaves.
        assertKeys(new int[]{3,5,6},a);
        assertValues(new Object[]{v3,v5,v6},a);
        assertKeys(new int[]{7,8},b);
        assertValues(new Object[]{v7,v8},b);
        // verify the root.
        assertKeys(new int[]{7},c);
        assertEquals(c,btree.root);
        assertEquals(a,c.getChild(0));
        assertEquals(b,c.getChild(1));
        assertNull(c.getChildRef(2));
        assertEntryCounts(new int[]{3,2}, c);
        // verify deleted nodes and leaves.
        assertTrue(d.isDeleted());

        /*
         * step#4 : remove(8) triggers b.join(a), which in turn calls
         * b.redistributeKeys(a) which sends (6,v6) to (b) and updates the
         * separatorKey in (c) to (6).
         */
        assertEquals(v8,btree.remove(KeyBuilder.asSortKey(8)));
        assertTrue("after remove(8)", btree.dump(Level.DEBUG,System.err));
        // verify leaves.
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5},a);
        assertKeys(new int[]{6,7},b);
        assertValues(new Object[]{v6,v7},b);
        // verify the root.
        assertKeys(new int[]{6},c);
        assertEquals(c,btree.root);
        assertEquals(a,c.getChild(0));
        assertEquals(b,c.getChild(1));
        assertNull(c.getChildRef(2));
        assertEntryCounts(new int[]{2,2}, c);

        /*
         * step#5 : remove(6) triggers b.join(a), which calls b.merge(a) and
         * c.removeChild(-,a). Since (c) now has a single child we replace the
         * root of the tree with (b).
         */
        assertEquals(v6,btree.remove(KeyBuilder.asSortKey(6)));
        assertTrue("after remove(6)", btree.dump(Level.DEBUG,System.err));
        // verify the new root leaf.
        assertKeys(new int[]{3,5,7},b);
        assertValues(new Object[]{v3,v5,v7},b);
        assertEquals(b,btree.root);
        assertTrue(a.isDeleted());
        assertTrue(c.isDeleted());
        
        assertEquals(v7,btree.remove(KeyBuilder.asSortKey(7)));
        assertTrue("after remove(7)", btree.dump(Level.DEBUG,System.err));
        assertKeys(new int[]{3,5},b);
        assertValues(new Object[]{v3,v5},b);
        
        assertEquals(v3,btree.remove(KeyBuilder.asSortKey(3)));
        assertTrue("after remove(3)", btree.dump(Level.DEBUG,System.err));
        assertKeys(new int[]{5},b);
        assertValues(new Object[]{v5},b);
        
        assertEquals(v5,btree.remove(KeyBuilder.asSortKey(5)));
        assertTrue("after remove(5)", btree.dump(Level.DEBUG,System.err));
        assertKeys(new int[]{},b);
        assertValues(new Object[]{},b);
        
        assertEquals("height",0,btree.height);
        assertEquals("nodes",0,btree.nnodes);
        assertEquals("leaves",1,btree.nleaves);
        assertEquals("entries",0,btree.nentries);
        
    }
    
    /**
     * Variant of {@link #test_removeOrder3a()} that excercises some different
     * code paths while removing keys by choosing a different order in which to
     * remove some keys. Both tests build the same initial tree. However, this
     * tests begins by removing a key (7) from the right edge of the tree while
     * the other test beings by removing a key (1) from the left edge of the
     * tree.
     */
    public void test_removeOrder3b() {

        /*
         * Generate keys, values, and visitation order.
         */
        // keys
        final int[] keys = new int[]{5,6,7,8,3,4,2,1};
        // values
        final SimpleEntry v1 = new SimpleEntry(1);
        final SimpleEntry v2 = new SimpleEntry(2);
        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v4 = new SimpleEntry(4);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v6 = new SimpleEntry(6);
        final SimpleEntry v7 = new SimpleEntry(7);
        final SimpleEntry v8 = new SimpleEntry(8);
        final SimpleEntry[] vals = new SimpleEntry[]{v5,v6,v7,v8,v3,v4,v2,v1};
        // permutation vector for visiting values in key order.
        final int[] order = new int[keys.length];
        // generate visitation order.
        {
            System.arraycopy(keys, 0, order, 0, keys.length);
            Arrays.sort(order);
            System.err.println("keys="+Arrays.toString(keys));
            System.err.println("vals="+Arrays.toString(vals));
            System.err.println("order="+Arrays.toString(order));
        }
        
        final int m = 3;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        assertTrue(btree.dump(System.err));

        Leaf a = (Leaf) btree.getRoot();
        assertKeys(new int[]{},a);
        assertValues(new Object[]{},a);
        
        int n = 0;
        
        { // insert(5,5)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 5 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5},a);
            assertValues(new Object[]{v5},a);
            assertTrue(btree.dump(System.err));
        }

        { // insert(6,6)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 6 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5,6},a);
            assertValues(new Object[]{v5,v6},a);
            assertTrue(btree.dump(System.err));
        }
        
        /*
         * fills the root leaf to capacity.
         * 
         * postcondition:
         * 
         * keys: [ 5 6 7 ]
         */
        { // insert(7,7)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 7 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5,6,7},a);
            assertValues(new Object[]{v5,v6,v7},a);
            assertTrue(btree.dump(System.err));
        }

        /*
         * splits the root leaf
         * 
         * split(a)->(a,b), c is the new root.
         * 
         * postcondition:
         * 
         * c.keys[ 7 - x ]
         * c.clds[ a b - ]
         * 
         * a.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Node c;
        final Leaf b;
        { // insert(8,8)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 8 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate new root (c).
            c = (Node)btree.getRoot();
            assertKeys(new int[]{7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChild(1));
            assertNull(c.getChildRef(2));
            b = (Leaf)c.getChild(1);
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate original leaf (a).
            assertKeys(new int[]{5,6},a);
            assertValues(new Object[]{v5,v6},a);
            
            // validate new leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);
            
            assertTrue(btree.dump(System.err));
        }
        
        /*
         * insert(3,3)
         * 
         * postcondition:
         * 
         * c.keys[ 7 - x ]
         * c.clds[ a b - ]
         * 
         * a.keys[ 3 5 6 ]
         * b.keys[ 7 8 - ]
         */
        {
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 3 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            // validate original leaf (a).
            assertKeys(new int[]{3,5,6},a);
            assertValues(new Object[]{v3,v5,v6},a);
            // validate root.
            assertEntryCounts(new int[]{3,2}, c);
            
        }

        /*
         * insert(4,4), causing split(a)->(a,d) and bringing (c) to capacity.
         * 
         * postcondition:
         * 
         * c.keys[ 5 7 x ]
         * c.clds[ a d b ]
         * 
         * a.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Leaf d;
        {
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 4 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate root (c).
            assertKeys(new int[]{5,7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChildRef(1));
            d = (Leaf) c.getChild(1);
            assertEquals(b,c.getChild(2));
            assertEntryCounts(new int[]{2,2,2}, c);
            
            // validate original leaf (a).
            assertKeys(new int[]{3,4},a);
            assertValues(new Object[]{v3,v4},a);
            
            // validate new leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);
            
        }
        
        /*
         * insert(2,2), bringing (a) to capacity again.
         */
        {
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 2 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate original leaf (a).
            assertKeys(new int[]{2,3,4},a);
            assertValues(new Object[]{v2,v3,v4},a);

            // validate root.
            assertEntryCounts(new int[]{3,2,2}, c);

        }
        
        /*
         * insert(1,1) causing (a) to split(a)->(a,e). Since the root (c) is
         * already at capacity this also causes the root to split(c)->(c,f) and
         * creating a new root(g).
         * 
         * postcondition:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a e - ]
         * 
         * f.keys[ 7 - x ]
         * f.clds[ d b - ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Leaf e;
        final Node f, g;
        {
            
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 1 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));

            // validate the new root(g).
            assertNotSame(c,btree.getRoot());
            g = (Node)btree.getRoot();
            assertKeys(new int[]{5},g);
            assertEquals(c,g.getChild(0));
            assertNotNull(g.getChildRef(1));
            f = (Node) g.getChild(1);
            assertNull(g.getChildRef(2));
            assertEntryCounts(new int[]{4,4}, g);
            
            // validate old root (c).
            assertKeys(new int[]{3},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChildRef(1));
            e = (Leaf) c.getChild(1);
            assertNull(c.getChildRef(2));
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate node(f) split from the old root split(c)->(c,f).
            assertKeys(new int[]{7},f);
            assertEquals(d,f.getChild(0));
            assertEquals(b,f.getChild(1));
            assertNull(f.getChildRef(2));
            assertEntryCounts(new int[]{2,2}, f);
            
            // validate original leaf (a), which was re-split into (a,e).
            assertKeys(new int[]{1,2},a);
            assertValues(new Object[]{v1,v2},a);
            
            // validate new leaf (e).
            assertKeys(new int[]{3,4},e);
            assertValues(new Object[]{v3,v4},e);
            
            // validate new leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);

        }
        
        /*
         * At this point the tree is setup and we start deleting keys. We delete
         * the keys in (nearly) the reverse order and verify that joins correctly
         * reduce the tree as each node or leaf is reduced below its minimum.
         *  
         * before:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a e - ]
         * 
         * f.keys[ 7 - x ]
         * f.clds[ d b - ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        assertTrue("before removing keys", btree.dump(Level.DEBUG,System.err));
        
        /*
         * step#1 : remove(7) triggers a cascade of operations: b.join(d) calls
         * b.merge(d) and f.removeChild(-,d). This forces f.join(c), which calls
         * f.merge(c) and g.removeChild(-,c). Since (g) now has a single child
         * we replace the root with (f). d, c, and g are deleted as we go.
         *
         * postcondition:
         * 
         * f.keys[ 3 5 x ]
         * f.clds[ a e b ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * b.keys[ 5 6 8 ]
         * 
         * c, d, g are deleted.
         */
        assertEquals(v7,btree.remove(KeyBuilder.asSortKey(7)));
        assertTrue("after remove(7)", btree.dump(Level.DEBUG,System.err));
        // verify leaves.
        assertKeys(new int[]{1,2},a);
        assertValues(new Object[]{v1,v2},a);
        assertKeys(new int[]{3,4},e);
        assertValues(new Object[]{v3,v4},e);
        assertKeys(new int[]{5,6,8},b);
        assertValues(new Object[]{v5,v6,v8},b);
        // verify the new root.
        assertKeys(new int[]{3,5},f);
        assertEquals(f,btree.root);
        assertEquals(a,f.getChild(0));
        assertEquals(e,f.getChild(1));
        assertEquals(b,f.getChild(2));
        assertEntryCounts(new int[]{2,2,3}, f);
        // verify deleted nodes and leaves.
        assertTrue(c.isDeleted());
        assertTrue(d.isDeleted());
        assertTrue(g.isDeleted());

        /*
         * step#2 : remove(3) triggers e.join(b) which calls
         * e.redistributeKeys(b) and sends (5,v5) to e. (This tests the code
         * path for redistribution of keys with a rightSibling of a leaf).
         *
         * postcondition:
         * 
         * f.keys[ 3 6 x ]
         * f.clds[ a e b ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 4 5 - ]
         * b.keys[ 6 8 - ]
         */
        assertEquals(v3,btree.remove(KeyBuilder.asSortKey(3)));
        assertTrue("after remove(3)", btree.dump(Level.DEBUG,System.err));
        // verify leaves.
        assertKeys(new int[]{1,2},a);
        assertValues(new Object[]{v1,v2},a);
        assertKeys(new int[]{4,5},e);
        assertValues(new Object[]{v4,v5},e);
        assertKeys(new int[]{6,8},b);
        assertValues(new Object[]{v6,v8},b);
        // verify the new root.
        assertKeys(new int[]{3,6},f);
        assertEquals(f,btree.root);
        assertEquals(a,f.getChild(0));
        assertEquals(e,f.getChild(1));
        assertEquals(b,f.getChild(2));
        assertEntryCounts(new int[]{2,2,2}, f);

        /*
         * step#3 : remove(8) triggers b.join(e) which calls b.merge(e), which
         * updates the separator in (f) to the separator for the leftSibling
         * which is (3) and then invokes f.removeChild(e).
         * 
         * postcondition:
         * 
         * f.keys[ 3 - x ]
         * f.clds[ a b - ]
         * 
         * a.keys[ 1 2 - ]
         * b.keys[ 4 5 6 ]
         * 
         * e is deleted.
         */
        assertEquals(v8,btree.remove(KeyBuilder.asSortKey(8)));
        assertTrue("after remove(8)", btree.dump(Level.DEBUG,System.err));
        // verify leaves.
        assertKeys(new int[]{1,2},a);
        assertValues(new Object[]{v1,v2},a);
        assertKeys(new int[]{4,5,6},b);
        assertValues(new Object[]{v4,v5,v6},b);
        // verify the root.
        assertKeys(new int[]{3},f);
        assertEquals(f,btree.root);
        assertEquals(a,f.getChild(0));
        assertEquals(b,f.getChild(1));
        assertNull(f.getChildRef(2));
        assertTrue(e.isDeleted());
        assertEntryCounts(new int[]{2,3}, f);

        /*
         * step#4 : remove(2) triggers a.join(b) which triggers
         * a.redistributeKeys(b) which sends (4,v4) to (a) and updates the
         * separatorKey on (f) to (5).
         *
         * postcondition:
         * 
         * f.keys[ 3 - x ]
         * f.clds[ a b - ]
         * 
         * a.keys[ 1 4 - ]
         * b.keys[ 5 6 - ]
         */
        assertEquals(v2,btree.remove(KeyBuilder.asSortKey(2)));
        assertTrue("after remove(2)", btree.dump(Level.DEBUG,System.err));
        // verify leaves.
        assertKeys(new int[]{1,4},a);
        assertValues(new Object[]{v1,v4},a);
        assertKeys(new int[]{5,6},b);
        assertValues(new Object[]{v5,v6},b);
        // verify the root.
        assertKeys(new int[]{5},f);
        assertEquals(f,btree.root);
        assertEquals(a,f.getChild(0));
        assertEquals(b,f.getChild(1));
        assertNull(f.getChildRef(2));
        assertEntryCounts(new int[]{2,2}, f);
        
        /*
         * step#5 : remove(1) triggers a.join(b) which calls a.merge(b) and
         * f.removeChild(b). Since this leaves (f) with only one child, we make
         * (a) the new root of the tree.
         *
         * postcondition:
         * 
         * a.keys[ 4 5 6 ]
         * 
         * b, f is deleted.
         */
        assertEquals(v1,btree.remove(KeyBuilder.asSortKey(1)));
        assertTrue("after remove(1)", btree.dump(Level.DEBUG,System.err));
        // verify the remaining leaf, which is now the root of the tree.
        assertKeys(new int[]{4,5,6},a);
        assertValues(new Object[]{v4,v5,v6},a);
        assertEquals(a,btree.root);
        assertTrue(b.isDeleted());
        assertTrue(f.isDeleted());
        
        /*
         * At this point we have only the root leaf and we just delete the final
         * keys.
         */
        assertEquals(v4,btree.remove(KeyBuilder.asSortKey(4)));
        assertTrue("after remove(4)", btree.dump(Level.DEBUG,System.err));
        assertKeys(new int[]{5,6},a);
        assertValues(new Object[]{v5,v6},a);
        assertEquals(a,btree.root);

        assertEquals(v6,btree.remove(KeyBuilder.asSortKey(6)));
        assertTrue("after remove(6)", btree.dump(Level.DEBUG,System.err));
        assertKeys(new int[]{5},a);
        assertValues(new Object[]{v5},a);
        assertEquals(a,btree.root);

        assertEquals(v5,btree.remove(KeyBuilder.asSortKey(5)));
        assertTrue("after remove(5)", btree.dump(Level.DEBUG,System.err));
        assertKeys(new int[]{},a);
        assertValues(new Object[]{},a);
        assertEquals(a,btree.root);

        assertEquals("height",0,btree.height);
        assertEquals("nodes",0,btree.nnodes);
        assertEquals("leaves",1,btree.nleaves);
        assertEquals("entries",0,btree.nentries);

    }
    
    /**
     * Variant of {@link #test_removeOrder3a()} that is focused on testing the
     * redistribution of keys among the left and right siblings of a node when
     * that node underflows during a deletion operation.
     */
    public void test_removeOrder3c() {

        /*
         * Generate keys, values, and visitation order.
         */
        // keys
        final int[] keys = new int[]{5,6,7,8,3,4,2,1};
        // values
        final SimpleEntry v1 = new SimpleEntry(1);
        final SimpleEntry v2 = new SimpleEntry(2);
        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v4 = new SimpleEntry(4);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v6 = new SimpleEntry(6);
        final SimpleEntry v7 = new SimpleEntry(7);
        final SimpleEntry v8 = new SimpleEntry(8);
        final SimpleEntry[] vals = new SimpleEntry[]{v5,v6,v7,v8,v3,v4,v2,v1};
        // permutation vector for visiting values in key order.
        final int[] order = new int[keys.length];
        // generate visitation order.
        {
            System.arraycopy(keys, 0, order, 0, keys.length);
            Arrays.sort(order);
            System.err.println("keys="+Arrays.toString(keys));
            System.err.println("vals="+Arrays.toString(vals));
            System.err.println("order="+Arrays.toString(order));
        }
        
        final int m = 3;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        assertTrue(btree.dump(System.err));

        Leaf a = (Leaf) btree.getRoot();
        assertKeys(new int[]{},a);
        assertValues(new Object[]{},a);
        
        int n = 0;
        
        { // insert(5,5)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 5 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5},a);
            assertValues(new Object[]{v5},a);
            assertTrue(btree.dump(System.err));
        }

        { // insert(6,6)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 6 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5,6},a);
            assertValues(new Object[]{v5,v6},a);
            assertTrue(btree.dump(System.err));
        }
        
        /*
         * fills the root leaf to capacity.
         * 
         * postcondition:
         * 
         * keys: [ 5 6 7 ]
         */
        { // insert(7,7)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 7 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5,6,7},a);
            assertValues(new Object[]{v5,v6,v7},a);
            assertTrue(btree.dump(System.err));
        }

        /*
         * splits the root leaf
         * 
         * split(a)->(a,b), c is the new root.
         * 
         * postcondition:
         * 
         * c.keys[ 7 - x ]
         * c.clds[ a b - ]
         * 
         * a.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Node c;
        final Leaf b;
        { // insert(8,8)
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 8 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate new root (c).
            c = (Node)btree.getRoot();
            assertKeys(new int[]{7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChild(1));
            assertNull(c.getChildRef(2));
            b = (Leaf)c.getChild(1);
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate original leaf (a).
            assertKeys(new int[]{5,6},a);
            assertValues(new Object[]{v5,v6},a);
            
            // validate new leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);
            
            assertTrue(btree.dump(System.err));
        }
        
        /*
         * insert(3,3)
         * 
         * postcondition:
         * 
         * c.keys[ 7 - x ]
         * c.clds[ a b - ]
         * 
         * a.keys[ 3 5 6 ]
         * b.keys[ 7 8 - ]
         */
        {
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 3 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            // validate original leaf (a).
            assertKeys(new int[]{3,5,6},a);
            assertValues(new Object[]{v3,v5,v6},a);
            // validate root.
            assertEntryCounts(new int[]{3,2}, c);
            
        }

        /*
         * insert(4,4), causing split(a)->(a,d) and bringing (c) to capacity.
         * 
         * postcondition:
         * 
         * c.keys[ 5 7 x ]
         * c.clds[ a d b ]
         * 
         * a.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Leaf d;
        {
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 4 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate root (c).
            assertKeys(new int[]{5,7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChildRef(1));
            d = (Leaf) c.getChild(1);
            assertEquals(b,c.getChild(2));
            assertEntryCounts(new int[]{2,2,2}, c);
            
            // validate original leaf (a).
            assertKeys(new int[]{3,4},a);
            assertValues(new Object[]{v3,v4},a);
            
            // validate new leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);
            
        }
        
        /*
         * insert(2,2), bringing (a) to capacity again.
         */
        {
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 2 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate original leaf (a).
            assertKeys(new int[]{2,3,4},a);
            assertValues(new Object[]{v2,v3,v4},a);
            
            // validate root.
            assertEntryCounts(new int[]{3,2,2}, c);
            
        }
        
        /*
         * insert(1,1) causing (a) to split(a)->(a,e). Since the root (c) is
         * already at capacity this also causes the root to split(c)->(c,f) and
         * creating a new root(g).
         * 
         * postcondition:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a e - ]
         * 
         * f.keys[ 7 - x ]
         * f.clds[ d b - ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Leaf e;
        final Node f, g;
        {
            
            final int ikey = keys[n];
            SimpleEntry val = vals[n++];
            assert ikey == 1 && val.id() == ikey;
            final byte[] key = KeyBuilder.asSortKey(ikey);
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));

            // validate the new root(g).
            assertNotSame(c,btree.getRoot());
            g = (Node)btree.getRoot();
            assertKeys(new int[]{5},g);
            assertEquals(c,g.getChild(0));
            assertNotNull(g.getChildRef(1));
            f = (Node) g.getChild(1);
            assertNull(g.getChildRef(2));
            assertEntryCounts(new int[]{4,4}, g);
            
            // validate old root (c).
            assertKeys(new int[]{3},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChildRef(1));
            e = (Leaf) c.getChild(1);
            assertNull(c.getChildRef(2));
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate node(f) split from the old root split(c)->(c,f).
            assertKeys(new int[]{7},f);
            assertEquals(d,f.getChild(0));
            assertEquals(b,f.getChild(1));
            assertNull(f.getChildRef(2));
            assertEntryCounts(new int[]{2,2}, f);
            
            // validate original leaf (a), which was re-split into (a,e).
            assertKeys(new int[]{1,2},a);
            assertValues(new Object[]{v1,v2},a);
            
            // validate new leaf (e).
            assertKeys(new int[]{3,4},e);
            assertValues(new Object[]{v3,v4},e);
            
            // validate new leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);

        }
        
        /*
         * At this point this test forks from the other two tests for trees of
         * order three (3) in this suite. Rather than begin deleting keys we
         * first bring some of the internal nodes to capacity so that deletes
         * will cause the redistribution of keys among the internal nodes. We
         * will do this a few times to test redistribution of keys with the left
         * and the right sibling, each time inserting more keys so that a
         * redistribution will be forced rather than forcing the merger of a
         * node with its left or right sibling.
         *  
         * before:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a e - ]
         * 
         * f.keys[ 7 - x ]
         * f.clds[ d b - ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        assertTrue("common baseline established", btree.dump(Level.DEBUG,System.err));
        
        /*
         * insert(9,9) and insert(10,10), bringing the tree to the point where a
         * delete will trigger the redistribution of keys among the internal
         * nodes.
         * 
         * postcondition:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a e - ]
         * 
         * f.keys[ 7 9 x ]
         * f.clds[ d b h ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         * h.keys[ 9 10 - ]
         */
        final SimpleEntry v9 = new SimpleEntry(9);
        final SimpleEntry v10 = new SimpleEntry(10);
        final Leaf h;
        {
            
            { // insert(9,v9)
                final byte[] key = KeyBuilder.asSortKey(9);
                SimpleEntry val = v9;
                assertNull(btree.remove(key)); // not found / no change.
                assertNull(btree.lookup(key)); // not found.
                assertNull(btree.insert(key, val)); // insert.
                assertEquals(val, btree.lookup(key)); // found.
                assertTrue(btree.dump(Level.DEBUG, System.err));
                assertEntryCounts(new int[]{2,3}, f);
            }
            
            { /*
               * insert(10,v10) - splits (b) into (b,h) and adds (h) as a
               * child of (f).
               */
                final byte[] key = KeyBuilder.asSortKey(10);
                SimpleEntry val = v10;
                assertNull(btree.remove(key)); // not found / no change.
                assertNull(btree.lookup(key)); // not found.
                assertNull(btree.insert(key,val)); // insert.
                assertEquals(val,btree.lookup(key)); // found.
                assertTrue(btree.dump(Level.DEBUG,System.err));
                assertEntryCounts(new int[]{2,2,2}, f);
            }

            // validate the root(g).
            assertEquals(g,btree.getRoot());
            assertKeys(new int[]{5},g);
            assertEquals(c,g.getChild(0));
            assertEquals(f,g.getChild(1));
            assertEntryCounts(new int[]{4,6}, g);

            // validate node(c).
            assertKeys(new int[]{3},c);
            assertEquals(a,c.getChild(0));
            assertEquals(e,c.getChild(1));
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate node(f).
            assertKeys(new int[]{7,9},f);
            assertEquals(d,f.getChild(0));
            assertEquals(b,f.getChild(1));
            assertNotNull(f.getChildRef(2));
            h = (Leaf) f.getChild(2);
            assertEntryCounts(new int[]{2,2,2}, f);
            
            // validate original leaf (a).
            assertKeys(new int[]{1,2},a);
            assertValues(new Object[]{v1,v2},a);
            
            // validate leaf (e).
            assertKeys(new int[]{3,4},e);
            assertValues(new Object[]{v3,v4},e);
            
            // validate leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);

            // validate new leaf (h).
            assertKeys(new int[]{9,10},h);
            assertValues(new Object[]{v9,v10},h);

        }
        
        /*
         * Test redistribution of keys among nodes in the tree.
         */
        assertTrue("before removing keys", btree.dump(Level.DEBUG,System.err));
        
        /*
         * step#1 : remove(1). This triggers a.join(e) which forces
         * a.merge(e,rightSibling:=true). Since (e) is merged into (a) we then
         * c.removeChild(3,e). This causes (c) to underflow, triggering
         * c.join(f). Since (f) is over its minimum capacity this causes
         * c.redistributeKeys(f,rightSibling:=true) - this is the first time
         * that we have triggered the redistribution of keys among the internal
         * nodes of the tree. This causes the first key (7) in (f) to be rotated
         * through the common parent (g) (which also happens to be the root) and
         * sends the ke (5) down from the parent (g) to (c). The child
         * associated with (7) in (f) is appended as the last child in (c).
         * 
         * postcondition:
         * 
         * g.keys[ 7 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 5 - x ]
         * c.clds[ a d - ]
         * 
         * f.keys[ 9 - x ]
         * f.clds[ b h - ]
         * 
         * a.keys[ 1 2 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         * h.keys[ 9 10 - ]
         * 
         * e is deleted.
         */
        {

            assertEquals(v1,btree.remove(KeyBuilder.asSortKey(1)));
            assertTrue("after remove(1)", btree.dump(Level.DEBUG,System.err));
            
            // validate the root(g).
            assertEquals(g,btree.getRoot());
            assertKeys(new int[]{7},g);
            assertEquals(c,g.getChild(0));
            assertEquals(f,g.getChild(1));
            assertEntryCounts(new int[]{5,4}, g);
            
            // validate node(c).
            assertKeys(new int[]{5},c);
            assertEquals(a,c.getChild(0));
            assertEquals(d,c.getChild(1));
            assertEntryCounts(new int[]{3,2}, c);
            
            // validate node(f).
            assertKeys(new int[]{9},f);
            assertEquals(b,f.getChild(0));
            assertEquals(h,f.getChild(1));
            assertEntryCounts(new int[]{2,2}, f);
            
            // validate original leaf (a).
            assertKeys(new int[]{2,3,4},a);
            assertValues(new Object[]{v2,v3,v4},a);
            
            // validate leaf (e).
            assertTrue(e.isDeleted());
            
            // validate leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);

            // validate leaf (h).
            assertKeys(new int[]{9,10},h);
            assertValues(new Object[]{v9,v10},h);

        }
     
        /*
         * step#2 : insert(1). This causes leaf(a) to overflow, splitting into
         * leaf(a) and leaf(i) and inserts (i) as the rightSibling of (a) into
         * (c). This brings (c) over its minimal capacity and paves the way for
         * us to force the redistribution of a key from (c) to (f) in the next
         * step.
         * 
         * postcondition:
         * 
         * g.keys[ 7 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 5 x ]
         * c.clds[ a i d ]
         * 
         * f.keys[ 9 - x ]
         * f.clds[ b h - ]
         * 
         * a.keys[ 1 2 - ]
         * i.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         * h.keys[ 9 10 - ]
         */
        final Leaf i;
        {

            assertNull(btree.insert(KeyBuilder.asSortKey(1),v1));
            assertTrue("after insert(1)", btree.dump(Level.DEBUG,System.err));
            
            // validate the root(g).
            assertEquals(g,btree.getRoot());
            assertKeys(new int[]{7},g);
            assertEquals(c,g.getChild(0));
            assertEquals(f,g.getChild(1));
            assertEntryCounts(new int[]{6,4}, g);
            
            // validate node(c).
            assertKeys(new int[]{3,5},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChildRef(1));
            i = (Leaf) c.getChild(1);
            assertEquals(d,c.getChild(2));
            assertEntryCounts(new int[]{2,2,2}, c);
            
            // validate node(f).
            assertKeys(new int[]{9},f);
            assertEquals(b,f.getChild(0));
            assertEquals(h,f.getChild(1));
            assertEntryCounts(new int[]{2,2}, f);
            
            // validate original leaf (a), which we just split into (a,i)
            assertKeys(new int[]{1,2},a);
            assertValues(new Object[]{v1,v2},a);
            
            // validate the new leaf (i)
            assertKeys(new int[]{3,4},i);
            assertValues(new Object[]{v3,v4},i);
            
            // validate leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{v7,v8},b);

            // validate leaf (h).
            assertKeys(new int[]{9,10},h);
            assertValues(new Object[]{v9,v10},h);

        }

        /*
         * step#3 : remove(10). This causes (h) to underflow, triggering
         * h.join(b) and h.merge(b). This causes f.removeChild(-,b). Since (f)
         * now underflows this triggers f.join(c,rightSibling:=false) and
         * f.redistributeKeys(c,rightSibiling:=false).  The right most key in
         * (c) is 5.  This key becomes the new separatorKey in the parent (g),
         * which happens to be the root node as well.  The old separatorKey is
         * 7 and that key is moved down to become the first key in (f).  The
         * data for last child in (c) is moved from (c) to (f).
         * 
         * postcondition:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a i - ]
         * 
         * f.keys[ 7 - x ]
         * f.clds[ d h - ]
         * 
         * a.keys[ 1 2 - ]
         * i.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * h.keys[ 7 8 9 ]
         * 
         * b is deleted.
         */

        {
            
            assertEquals(v10,btree.remove(KeyBuilder.asSortKey(10)));
            assertTrue("after remove(10)", btree.dump(Level.DEBUG,System.err));
            
            // validate the root(g).
            assertEquals(g,btree.getRoot());
            assertKeys(new int[]{5},g);
            assertEquals(c,g.getChild(0));
            assertEquals(f,g.getChild(1));
            assertEntryCounts(new int[]{4,5}, g);
            
            // validate node(c).
            assertKeys(new int[]{3},c);
            assertEquals(a,c.getChild(0));
            assertEquals(i,c.getChild(1));
            assertNull(c.getChildRef(2));
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate node(f).
            assertKeys(new int[]{7},f);
            assertEquals(d,f.getChild(0));
            assertEquals(h,f.getChild(1));
            assertEntryCounts(new int[]{2,3}, f);
            
            // validate original leaf (a)
            assertKeys(new int[]{1,2},a);
            assertValues(new Object[]{v1,v2},a);
            
            // validate leaf (i)
            assertKeys(new int[]{3,4},i);
            assertValues(new Object[]{v3,v4},i);
            
            // validate leaf (d), which was just moved from (c) to (f).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b), which was just deleted.
            assertTrue(b.isDeleted());

            // validate leaf (h).
            assertKeys(new int[]{7,8,9},h);
            assertValues(new Object[]{v7,v8,v9},h);

        }

    }
    
}
