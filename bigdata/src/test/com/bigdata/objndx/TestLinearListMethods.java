/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jan 2, 2007
 */

package com.bigdata.objndx;

import java.util.Arrays;

import org.apache.log4j.Level;

/**
 * Test suite for the "linear list" access methods.
 * 
 * @see AbstractBTree#indexOf(Object)
 * @see AbstractBTree#keyAt(int)
 * @see AbstractBTree#valueAt(int)
 * @see AbstractBTree#rangeCount(Object, Object)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLinearListMethods extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestLinearListMethods() {
    }

    /**
     * @param name
     */
    public TestLinearListMethods(String name) {
        super(name);
    }
    
    /**
     * Tests on the root leaf.
     */
    public void test_linearListHeight0() {
        
        BTree btree = getBTree(3);

        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);

        btree.insert(i2k(3), v3);
        btree.insert(i2k(5), v5);
        btree.insert(i2k(7), v7);
        
        assertKeys(new int[]{3,5,7},a);
        assertValues(new Object[]{v3,v5,v7},a);

        /*
         * test indexOf on the root leaf, including all cases where the key is
         * not found and the encoded insertion point is returned.  The "not
         * found" indices are a computed as (-(insert position)-1).
         */
        assertEquals(-1,btree.indexOf(i2k(1)));
        assertEquals(-1,btree.indexOf(i2k(2)));
        assertEquals(0,btree.indexOf(i2k(3)));
        assertEquals(-2,btree.indexOf(i2k(4)));
        assertEquals(1,btree.indexOf(i2k(5)));
        assertEquals(-3,btree.indexOf(i2k(6)));
        assertEquals(2,btree.indexOf(i2k(7)));
        assertEquals(-4,btree.indexOf(i2k(8)));
        assertEquals(-4,btree.indexOf(i2k(9)));

        /*
         * test of rangeCount [3,5,7]
         */
        assertEquals(0,btree.rangeCount(i2k(2), i2k(1)));
        assertEquals(0,btree.rangeCount(i2k(3), i2k(2)));
        assertEquals(0,btree.rangeCount(i2k(5), i2k(2)));
        assertEquals(0,btree.rangeCount(i2k(9), i2k(3)));

        assertEquals(0,btree.rangeCount(i2k(2), i2k(2)));
        assertEquals(0,btree.rangeCount(i2k(2), i2k(3)));
        assertEquals(1,btree.rangeCount(i2k(2), i2k(4)));

        assertEquals(0,btree.rangeCount(i2k(3), i2k(3)));
        assertEquals(1,btree.rangeCount(i2k(3), i2k(4)));
        assertEquals(1,btree.rangeCount(i2k(3), i2k(5)));
        assertEquals(2,btree.rangeCount(i2k(3), i2k(6)));
        assertEquals(2,btree.rangeCount(i2k(3), i2k(7)));

        assertEquals(3,btree.rangeCount(i2k(2), i2k(8)));
        assertEquals(3,btree.rangeCount(i2k(3), i2k(8)));
        assertEquals(2,btree.rangeCount(i2k(4), i2k(8)));
        assertEquals(2,btree.rangeCount(i2k(5), i2k(8)));
        assertEquals(1,btree.rangeCount(i2k(6), i2k(8)));
        assertEquals(1,btree.rangeCount(i2k(7), i2k(8)));
        assertEquals(0,btree.rangeCount(i2k(8), i2k(8)));
        assertEquals(0,btree.rangeCount(i2k(9), i2k(8)));

        /*
         * test of keyAt [3,5,7]
         */
        try {
            btree.keyAt(-1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        assertEquals(i2k(3),btree.keyAt(0));
        assertEquals(i2k(5),btree.keyAt(1));
        assertEquals(i2k(7),btree.keyAt(2));
        try {
            btree.keyAt(3);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        
        /*
         * test of valueAt [v3,v5,v7]
         */
        try {
            btree.valueAt(-1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        assertEquals(v3,btree.valueAt(0));
        assertEquals(v5,btree.valueAt(1));
        assertEquals(v7,btree.valueAt(2));
        try {
            btree.valueAt(3);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        
    }
    
    /**
     * Tests on a tree with one root node and two leaves.
     */
    public void test_linearListHeight1() throws NoSuccessorException {
        
        BTree btree = getBTree(3);
        
        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);

        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        
        assertKeys(new int[]{3,5,7},a);
        assertValues(new Object[]{v3,v5,v7},a);
        
        /*
         * split the leaf.
         */
        btree.insert(2,v2);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{5},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[]{2,2},root);
        
        assertKeys(new int[]{2,3},a);
        assertValues(new Object[]{v2,v3},a);
        
        assertKeys(new int[]{5,7},b);
        assertValues(new Object[]{v5,v7},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        /*
         * test indexOf at height=1 [2,3,5,7]
         */
        assertEquals(-1,btree.indexOf(i2k(1)));
        assertEquals(0,btree.indexOf(i2k(2)));
        assertEquals(1,btree.indexOf(i2k(3)));
        assertEquals(-3,btree.indexOf(i2k(4)));
        assertEquals(2,btree.indexOf(i2k(5)));
        assertEquals(-4,btree.indexOf(i2k(6)));
        assertEquals(3,btree.indexOf(i2k(7)));
        assertEquals(-5,btree.indexOf(i2k(8)));
        
        /*
         * test rangeCount at height=1 [2,3,5,7]
         */

        assertEquals(0,btree.rangeCount(i2k(2), i2k(1)));
        assertEquals(0,btree.rangeCount(i2k(3), i2k(2)));
        assertEquals(0,btree.rangeCount(i2k(5), i2k(2)));
        assertEquals(0,btree.rangeCount(i2k(9), i2k(3)));
        
        assertEquals(0,btree.rangeCount(i2k(1), i2k(1)));
        assertEquals(0,btree.rangeCount(i2k(1), i2k(2)));
        assertEquals(0,btree.rangeCount(i2k(2), i2k(2)));
        assertEquals(1,btree.rangeCount(i2k(2), i2k(3)));
        assertEquals(2,btree.rangeCount(i2k(2), i2k(4)));

        assertEquals(0,btree.rangeCount(i2k(3), i2k(3)));
        assertEquals(1,btree.rangeCount(i2k(3), i2k(4)));
        assertEquals(1,btree.rangeCount(i2k(3), i2k(5)));
        assertEquals(2,btree.rangeCount(i2k(3), i2k(6)));
        assertEquals(2,btree.rangeCount(i2k(3), i2k(7)));

        assertEquals(4,btree.rangeCount(i2k(1), i2k(8)));
        assertEquals(4,btree.rangeCount(i2k(2), i2k(8)));
        assertEquals(3,btree.rangeCount(i2k(3), i2k(8)));
        assertEquals(2,btree.rangeCount(i2k(4), i2k(8)));
        assertEquals(2,btree.rangeCount(i2k(5), i2k(8)));
        assertEquals(1,btree.rangeCount(i2k(6), i2k(8)));
        assertEquals(1,btree.rangeCount(i2k(7), i2k(8)));
        assertEquals(0,btree.rangeCount(i2k(8), i2k(8)));
        assertEquals(0,btree.rangeCount(i2k(9), i2k(8)));

        /*
         * test of keyAt [2,3,5,7]
         */
        try {
            btree.keyAt(-1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        assertEquals(i2k(2),btree.keyAt(0));
        assertEquals(i2k(3),btree.keyAt(1));
        assertEquals(i2k(5),btree.keyAt(2));
        assertEquals(i2k(7),btree.keyAt(3));
        try {
            btree.keyAt(4);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        
        /*
         * test of valueAt [v2,v3,v5,v7]
         */
        try {
            btree.valueAt(-1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        assertEquals(v2,btree.valueAt(0));
        assertEquals(v3,btree.valueAt(1));
        assertEquals(v5,btree.valueAt(2));
        assertEquals(v7,btree.valueAt(3));
        try {
            btree.valueAt(4);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * Tests on a tree of height (2).
     * 
     * @see src/architecture/btree.xls for the example used in this test.
     */
    public void test_linearListHeight2() throws NoSuccessorException {

        /*
         * Generate keys, values, and visitation order.
         */
        // keys
        final byte[] k1 = i2k(1);
        final byte[] k2 = i2k(2);
        final byte[] k3 = i2k(3);
        final byte[] k4 = i2k(4);
        final byte[] k5 = i2k(5);
        final byte[] k6 = i2k(6);
        final byte[] k7 = i2k(7);
        final byte[] k8 = i2k(8);
        final byte[][] keys = new byte[][] { k5, k6, k7, k8, k3, k4, k2, k1 };
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
        final int[] order = new int[] {1,2,3,4,5,6,7,8};
        // generate visitation order.
        {
//            System.arraycopy(keys, 0, order, 0, keys.length);
//            Arrays.sort(order);
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
            byte[] key = keys[n];
            SimpleEntry val = vals[n++];
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new byte[][]{k5},a);
            assertValues(new Object[]{v5},a);
            assertTrue(btree.dump(System.err));
        }

        { // insert(6,6)
            byte[] key = keys[n];
            SimpleEntry val = vals[n++];
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new byte[][]{k5,k6},a);
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
            byte[] key = keys[n];
            SimpleEntry val = vals[n++];
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new byte[][]{k5,k6,k7},a);
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
            byte[] key = keys[n];
            SimpleEntry val = vals[n++];
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate new root (c).
            c = (Node)btree.getRoot();
            assertKeys(new byte[][]{k7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChild(1));
            assertNull(c.childRefs[2]);
            b = (Leaf)c.getChild(1);
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate original leaf (a).
            assertKeys(new byte[][]{k5,k6},a);
            assertValues(new Object[]{v5,v6},a);
            
            // validate new leaf (b).
            assertKeys(new byte[][]{k7,k8},b);
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
            byte[] key = keys[n];
            SimpleEntry val = vals[n++];
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            // validate original leaf (a).
            assertKeys(new byte[][]{k3,k5,k6},a);
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
            byte[] key = keys[n];
            SimpleEntry val = vals[n++];
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate root (c).
            assertKeys(new byte[][]{k5,k7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.childRefs[1]);
            d = (Leaf) c.getChild(1);
            assertEquals(b,c.getChild(2));
            assertEntryCounts(new int[]{2,2,2}, c);
            
            // validate original leaf (a).
            assertKeys(new byte[][]{k3,k4},a);
            assertValues(new Object[]{v3,v4},a);
            
            // validate new leaf (d).
            assertKeys(new byte[][]{k5,k6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new byte[][]{k7,k8},b);
            assertValues(new Object[]{v7,v8},b);
            
        }
        
        /*
         * insert(2,2), bringing (a) to capacity again.
         */
        {
            byte[] key = keys[n];
            SimpleEntry val = vals[n++];
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate original leaf (a).
            assertKeys(new byte[][]{k2,k3,k4},a);
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
            
            byte[] key = keys[n];
            SimpleEntry val = vals[n++];
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));

            // validate the new root(g).
            assertNotSame(c,btree.getRoot());
            g = (Node)btree.getRoot();
            assertKeys(new byte[][]{k5},g);
            assertEquals(c,g.getChild(0));
            assertNotNull(g.childRefs[1]);
            f = (Node) g.getChild(1);
            assertNull(g.childRefs[2]);
            assertEntryCounts(new int[]{4,4}, g);
            
            // validate old root (c).
            assertKeys(new byte[][]{k3},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.childRefs[1]);
            e = (Leaf) c.getChild(1);
            assertNull(c.childRefs[2]);
            assertEntryCounts(new int[]{2,2}, c);
            
            // validate node(f) split from the old root split(c)->(c,f).
            assertKeys(new byte[][]{k7},f);
            assertEquals(d,f.getChild(0));
            assertEquals(b,f.getChild(1));
            assertNull(f.childRefs[2]);
            assertEntryCounts(new int[]{2,2}, f);
            
            // validate original leaf (a), which was re-split into (a,e).
            assertKeys(new byte[][]{k1,k2},a);
            assertValues(new Object[]{v1,v2},a);
            
            // validate new leaf (e).
            assertKeys(new byte[][]{k3,k4},e);
            assertValues(new Object[]{v3,v4},e);
            
            // validate new leaf (d).
            assertKeys(new byte[][]{k5,k6},d);
            assertValues(new Object[]{v5,v6},d);
            
            // validate leaf (b).
            assertKeys(new byte[][]{k7,k8},b);
            assertValues(new Object[]{v7,v8},b);

        }

        /*
         * test indexOf: [1,2,3,4,5,6,7,8]
         */
        final byte[] k0 = i2k(0);
        final byte[] k9 = i2k(9);
        assertEquals(-1,btree.indexOf(k0));
        assertEquals(0,btree.indexOf(k1));
        assertEquals(1,btree.indexOf(k2));
        assertEquals(2,btree.indexOf(k3));
        assertEquals(3,btree.indexOf(k4));
        assertEquals(4,btree.indexOf(k5));
        assertEquals(5,btree.indexOf(k6));
        assertEquals(6,btree.indexOf(k7));
        assertEquals(7,btree.indexOf(k8));
        assertEquals(((-8)-1),btree.indexOf(k9));
        
        /*
         * test rangeCount: [1,2,3,4,5,6,7,8]
         */
        assertEquals(0,btree.rangeCount(k0, k1));
        assertEquals(1,btree.rangeCount(k0, k2));
        assertEquals(2,btree.rangeCount(k0, k3));
        assertEquals(3,btree.rangeCount(k0, k4));
        assertEquals(4,btree.rangeCount(k0, k5));
        assertEquals(5,btree.rangeCount(k0, k6));
        assertEquals(6,btree.rangeCount(k0, k7));
        assertEquals(7,btree.rangeCount(k0, k8));
        assertEquals(8,btree.rangeCount(k0, k9));

        assertEquals(5,btree.rangeCount(k4, k9));
        assertEquals(4,btree.rangeCount(k4, k8));
        assertEquals(3,btree.rangeCount(k4, k7));
        assertEquals(2,btree.rangeCount(k4, k6));
        assertEquals(1,btree.rangeCount(k4, k5));
        assertEquals(0,btree.rangeCount(k4, k4));
        assertEquals(0,btree.rangeCount(k4, k3));

        assertEquals(1,btree.rangeCount(k3, k4));
        assertEquals(2,btree.rangeCount(k3, k5));
        assertEquals(3,btree.rangeCount(k3, k6));
        assertEquals(4,btree.rangeCount(k3, k7));

        /*
         * test keyAt: [1,2,3,4,5,6,7,8]
         */
        try {
            btree.keyAt(-1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        assertEquals(k1,btree.keyAt(0));
        assertEquals(k2,btree.keyAt(1));
        assertEquals(k3,btree.keyAt(2));
        assertEquals(k4,btree.keyAt(3));
        assertEquals(k5,btree.keyAt(4));
        assertEquals(k6,btree.keyAt(5));
        assertEquals(k7,btree.keyAt(6));
        assertEquals(k8,btree.keyAt(7));
        try {
            btree.keyAt(8);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        
        /*
         * test valueAt: [v1,v2,v3,v4,v5,v6,v7,v8]
         */
        try {
            btree.valueAt(-1);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        assertEquals(v1,btree.valueAt(0));
        assertEquals(v2,btree.valueAt(1));
        assertEquals(v3,btree.valueAt(2));
        assertEquals(v4,btree.valueAt(3));
        assertEquals(v5,btree.valueAt(4));
        assertEquals(v6,btree.valueAt(5));
        assertEquals(v7,btree.valueAt(6));
        assertEquals(v8,btree.valueAt(7));
        try {
            btree.valueAt(8);
            fail("Expecting: " + IndexOutOfBoundsException.class);
        } catch (IndexOutOfBoundsException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
    
    }
    
}
