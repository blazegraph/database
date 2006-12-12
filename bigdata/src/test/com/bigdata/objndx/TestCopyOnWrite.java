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
 * Created on Nov 18, 2006
 */

package com.bigdata.objndx;

import org.apache.log4j.Level;

/**
 * Test suite for copy-on-write semantics.
 * 
 * @todo the tests in this suite should verify {@link Node#childKeys}[].
 * 
 * @todo the tests in this suite should verify that the parent reference on a
 *       clean child was updated to point to the cloned parent.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCopyOnWrite extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestCopyOnWrite() {
    }

    /**
     * @param name
     */
    public TestCopyOnWrite(String name) {
        super(name);
    }
   
    /**
     * Test copy-on-write for a tree of height 1 (two levels). This test works
     * by explicitly writing out either the root node or a leaf and verifying
     * that the persistent and dirty state of each node or leaf. Note that this
     * does not force the eviction of nodes or leaves but rather requests that
     * they are written out directly. Whenever we make an immutable node or leaf
     * mutable using copy-on-write, we wind up with a new reference for that
     * node or leaf and update the variables in the test appropriately.
     */
    public void test_dirtyChildIterator02() {

        BTree btree = getBTree(3);

        final Leaf a = (Leaf) btree.root;
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        SimpleEntry v9 = new SimpleEntry(9);

        // fill up the root leaf.
        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);

        // split the root leaf.
        btree.insert(9, v9);
        final Node c = (Node) btree.root;
        assertKeys(new int[]{7},c);
        assertEquals(a,c.getChild(0));
        final Leaf b = (Leaf)c.getChild(1);
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5}, a);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);
        assertTrue(a.isDirty());
        assertTrue(b.isDirty());
        
        /*
         * split another leaf so that there are now three children to visit. at
         * this point the root is full.
         */
        btree.insert(1, v1);
        btree.insert(2, v2);
        assertKeys(new int[]{3,7},c);
        assertEquals(a,c.getChild(0));
        Leaf d = (Leaf)c.getChild(1);
        assertEquals(b,c.getChild(2));
        assertKeys(new int[]{1,2},a);
        assertValues(new Object[]{v1,v2}, a);
        assertKeys(new int[]{3,5},d);
        assertValues(new Object[]{v3,v5}, d);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);
        assertTrue(a.isDirty());
        assertTrue(d.isDirty());
        assertTrue(b.isDirty());

        // write (a) onto the store and verify.
        btree.writeNodeOrLeaf(a);
        assertFalse(a.isDirty());
        assertTrue(a.isPersistent());
        
        // write (b) onto the store and verify.
        btree.writeNodeOrLeaf(b);
        assertFalse(b.isDirty());
        assertTrue(b.isPersistent());
        
        // write (d) onto the store and verify.
        btree.writeNodeOrLeaf(d);
        assertFalse(d.isDirty());
        assertTrue(d.isPersistent());
        
        /*
         * remove a key from a leaf forcing two leaves to join and verify the
         * visitation order.  this triggers copy-on-write for (a) and (a) is
         * dirty as a post-condition.
         */
        assertEquals(v1,btree.remove(1));
        assertKeys(new int[]{7},c);
        assertNotSame(a,c.getChild(0));
        Leaf a1 = (Leaf)c.getChild(0);
        assertEquals(b,c.getChild(1));
        assertKeys(new int[]{2,3,5},a1);
        assertValues(new Object[]{v2,v3,v5}, a1);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);
        assertTrue(d.isDeleted());
        assertTrue(a1.isDirty());
        assertFalse(b.isDirty());

        /*
         * insert a key that will go into (b).  since (b) is immutable this
         * triggers copy-on-write.
         */
        btree.insert(8,v8);
        assertKeys(new int[]{7},c);
        assertEquals(a1,c.getChild(0));
        assertNotSame(b,c.getChild(1));
        Leaf b1 = (Leaf)c.getChild(1);
        assertKeys(new int[]{2,3,5},a1);
        assertValues(new Object[]{v2,v3,v5}, a1);
        assertKeys(new int[]{7,8,9},b1);
        assertValues(new Object[]{v7,v8,v9}, b1);
        assertTrue(c.isDirty());
        assertTrue(d.isDeleted());
        assertTrue(a1.isDirty());
        assertTrue(b1.isDirty());

        /*
         * write the root node of the tree onto the store.
         */
        btree.writeNodeRecursive(c);
        assertFalse(c.isDirty());
        assertFalse(a1.isDirty());
        assertFalse(b1.isDirty());

        /*
         * remove a key from (a1). since (a1) is immutable this triggers
         * copy-on-write. since the root is immtuable, it is also copied.
         */
        assertEquals(v2,btree.remove(2));
        assertNotSame(c,btree.root);
        Node c1 = (Node)btree.root;
        assertKeys(new int[]{7},c1);
        assertNotSame(a1,c1.getChild(0));
        Leaf a2 = (Leaf) c1.getChild(0);
        assertEquals( b1, c.getChild(1));
        assertKeys(new int[]{3,5},a2);
        assertValues(new Object[]{v3,v5}, a2);
        assertKeys(new int[]{7,8,9},b1);
        assertValues(new Object[]{v7,v8,v9}, b1);
        assertTrue(c1.isDirty());
        assertTrue(a2.isDirty());
        assertFalse(b1.isDirty());

    }

    /**
     * Test copy on write for a tree of height 2 (three levels). Copy on write
     * propagates up from the leaf where we make the mutation and causes any
     * immutable parents to be cloned as well. Nodes and leaves that have been
     * cloned by copy-on-write are distinct objects from their immutable
     * predecessors.
     */
    public void test_dirtyPostOrderIterator02() {

        BTree btree = getBTree(3);

        Leaf a = (Leaf) btree.root;
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v9 = new SimpleEntry(9);

        /*
         * write out the root leaf on the store.
         */
        btree.writeNodeRecursive(btree.root);
        
        /*
         * Fill up the root leaf. Since it was immutable, this will trigger
         * copy-on-write.  We verify that the root leaf reference is changed
         * and verify.
         */
        assertEquals(a,btree.root);
        btree.insert(3, v3);
        assertNotSame(a,btree.root);
        a = (Leaf)btree.root; // new reference for the root leaf.
        btree.insert(5, v5);
        btree.insert(7, v7);

        // split the root leaf.
        btree.insert(9, v9);
        Node c = (Node) btree.root;
        assertKeys(new int[]{7},c);
        assertEquals(a,c.getChild(0));
        Leaf b = (Leaf)c.getChild(1);
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5}, a);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);
        
        /*
         * write out (a) and verify.
         */
        btree.writeNodeOrLeaf(a);
        assertTrue(a.isPersistent());
        assertFalse(b.isPersistent());
        assertFalse(c.isPersistent());

        /*
         * write out (c) and verify.
         */
        btree.writeNodeRecursive(c);
        assertTrue(a.isPersistent());
        assertTrue(b.isPersistent());
        assertTrue(c.isPersistent());

        /*
         * split another leaf (a) so that there are now three children to visit.
         * at this point the root is full.
         */
        assertTrue(a.isPersistent());
        assertTrue(b.isPersistent());
        assertTrue(c.isPersistent());
        btree.insert(1, v1); // triggers copy on write for (a) and (c).
        assertNotSame(c,btree.root);
        c = (Node)btree.root;
        assertNotSame(a,c.getChild(0));
        a = (Leaf)c.getChild(0);
        assertEquals(b,c.getChild(1)); // b was not copied.
        assertFalse(a.isPersistent());
        assertTrue(b.isPersistent());
        assertFalse(c.isPersistent());
        btree.insert(2, v2);
        assertKeys(new int[]{3,7},c);
        assertEquals(a,c.getChild(0));
        Leaf d = (Leaf)c.getChild(1);
        assertEquals(b,c.getChild(2));
        assertKeys(new int[]{1,2},a);
        assertValues(new Object[]{v1,v2}, a);
        assertKeys(new int[]{3,5},d);
        assertValues(new Object[]{v3,v5}, d);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);

        // verify
        assertFalse(a.isPersistent());
        assertTrue(b.isPersistent());
        assertFalse(c.isPersistent());
        
        /*
         * cause another leaf (d) to split, forcing the split to propagate to and
         * split the root and the tree to increase in height.
         */
        btree.insert(4, v4);
        btree.insert(6, v6);
//        btree.dump(Level.DEBUG,System.err);
        assertNotSame(c,btree.root);
        final Node g = (Node)btree.root;
        assertKeys(new int[]{5},g);
        assertEquals(c,g.getChild(0));
        final Node f = (Node)g.getChild(1);
        assertKeys(new int[]{3},c);
        assertEquals(a,c.getChild(0));
        assertEquals(d,c.getChild(1));
        assertKeys(new int[]{1,2},a);
        assertValues(new Object[]{v1,v2}, a);
        assertKeys(new int[]{3,4},d);
        assertValues(new Object[]{v3,v4}, d);
        assertKeys(new int[]{7},f);
        Leaf e = (Leaf)f.getChild(0);
        assertEquals(b,f.getChild(1));
        assertKeys(new int[]{5,6},e);
        assertValues(new Object[]{v5,v6}, e);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);
        
        /*
         * write out the entire tree.
         */
        btree.writeNodeRecursive(g);

        /*
         * remove a key (4) from (d) forcing (d,a) to merge into (d) and (a) to
         * be deleted. this causes (c,f) to merge as well, which in turn forces
         * the root to be replaced by (c).
         * 
         * the following are cloned: d, c, g.
         */
        assertEquals(v4,btree.remove(4));
        assertNotSame(g,btree.root);
        assertNotSame(c,btree.root);
        c = (Node) btree.root;
        assertNotSame(d,c.getChild(0));
        d = (Leaf) c.getChild(0);
//        btree.dump(Level.DEBUG,System.err);
        assertKeys(new int[]{5,7},c);
        assertEquals(d,c.getChild(0));
        assertEquals(e,c.getChild(1));
        assertEquals(b,c.getChild(2));
        assertKeys(new int[]{1,2,3},d);
        assertValues(new Object[]{v1,v2,v3}, d);
        assertKeys(new int[]{5,6},e);
        assertValues(new Object[]{v5,v6}, e);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);
        assertTrue(a.isDeleted());
        assertTrue(f.isDeleted());

        /*
         * remove a key (7) from a leaf (b) forcing two leaves (b,e) into (b) to
         * join and verify the visitation order.
         */
        assertEquals(v7,btree.remove(7));
        btree.dump(Level.DEBUG,System.err);
        assertKeys(new int[]{5},c);
        assertEquals(d,c.getChild(0));
        assertNotSame(b,c.getChild(1));
        b = (Leaf) c.getChild(1);
        assertKeys(new int[]{1,2,3},d);
        assertValues(new Object[]{v1,v2,v3}, d);
        assertKeys(new int[]{5,6,9},b);
        assertValues(new Object[]{v5,v6,v9}, b);
        assertTrue(e.isDeleted());

        /*
         * write out the root.
         */
        btree.writeNodeRecursive(c);

        /*
         * remove keys from a leaf (b) forcing the remaining two leaves (b,d) to
         * join into (b). Since there is only one leaf, that leaf now becomes
         * the new root leaf of the tree.
         */
        assertEquals(c,btree.root);
        assertEquals(d,c.getChild(0));
        assertEquals(b,c.getChild(1));
        assertEquals(v3, btree.remove(3)); // remove from (d)
        assertNotSame(c,btree.root); // c was cloned.
        c = (Node) btree.root;
        assertNotSame(d,c.getChild(0));
        d = (Leaf)c.getChild(0); // d was cloned.
        assertEquals(b,c.getChild(1));
        assertEquals(v5,btree.remove(5)); // remove from (b)
        assertNotSame(b,c.getChild(1));
        b = (Leaf)c.getChild(1); // b was cloned.
        assertEquals(v6,btree.remove(6)); // remove from (b)
        assertKeys(new int[]{1,2,9},b);
        assertValues(new Object[]{v1,v2,v9}, b);
        assertTrue(d.isDeleted());
        assertTrue(c.isDeleted());

        /*
         * write out the root.
         */
        btree.writeNodeRecursive(b);
        
    }

}
