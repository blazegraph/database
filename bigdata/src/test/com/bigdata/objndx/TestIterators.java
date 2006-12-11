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
 * Created on Dec 11, 2006
 */

package com.bigdata.objndx;

import org.apache.log4j.Level;

/**
 * Test suite for iterators. The tests are presented from the least dependencies
 * to the most dependencies ((traversal of the entries for a single leaf, then
 * children of a node, then dirty child of a node, then post-order traversal,
 * then post-order traversal of dirty nodes).
 * 
 * @see Leaf#entryIterator()
 * @see Node#childIterator(boolean)
 * @see AbstractNode#postOrderIterator(boolean)
 * 
 * @see TestDirtyIterators, which handles tests when some nodes or leaves are
 *      NOT dirty and verifies that the iterators do NOT visit such nodes or
 *      leaves.
 * 
 * @todo test {@link IKeyVisitor} for each of the iterators.
 * 
 * @todo Define key range iterator visiting values with keys available for
 *       inspection. This will be used for key range scans. Ideally this will
 *       eventually allow concurrent modification of the btree during traversal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIterators extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestIterators() {
    }

    /**
     * @param name
     */
    public TestIterators(String name) {
        super(name);
    }

    /**
     * Test ability to visit the entries on a leaf in key order.
     */
    public void test_leaf_entryIterator01() {
        
        BTree btree = getBTree(3);
        
        final Leaf root = (Leaf) btree.root;
        
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);

        // insert keys until the root leaf is full.
        assertSameIterator(new Object[]{},root.entryIterator());
        btree.insert(7, v7);
        assertSameIterator(new Object[]{v7},root.entryIterator());
        btree.insert(5, v5);
        assertSameIterator(new Object[]{v5,v7},root.entryIterator());
        btree.insert(3, v3);
        assertSameIterator(new Object[]{v3,v5,v7},root.entryIterator());
        
        // remove keys until the root leaf is empty.
        assertEquals(v5,btree.remove(5));
        assertSameIterator(new Object[]{v3,v7},root.entryIterator());
        assertEquals(v7,btree.remove(7));
        assertSameIterator(new Object[]{v3},root.entryIterator());
        assertEquals(v3,btree.remove(3));
        assertSameIterator(new Object[]{},root.entryIterator());
        
    }

    /**
     * Test ability to visit the direct children of a node.
     */
    public void test_childIterator01() {

        BTree btree = getBTree(3);

        final Leaf a = (Leaf) btree.root;
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
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
        
        // verify visiting all children.
        assertSameIterator(new AbstractNode[] { a, b }, ((Node) btree.root)
                .childIterator(false));

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

        // verify visiting all children.
        assertSameIterator(new AbstractNode[] { a, d, b }, ((Node) btree.root)
                .childIterator(false));

        /*
         * remove a key from a leaf forcing two leaves to join and verify the
         * visitation order.
         */
        assertEquals(v1,btree.remove(1));
        assertKeys(new int[]{7},c);
        assertEquals(a,c.getChild(0));
        assertEquals(b,c.getChild(1));
        assertKeys(new int[]{2,3,5},a);
        assertValues(new Object[]{v2,v3,v5}, a);
        assertKeys(new int[]{7,9},b);
        assertValues(new Object[]{v7,v9}, b);
        assertTrue(d.isDeleted());

        // verify visiting all children.
        assertSameIterator(new AbstractNode[] { a, b }, ((Node) btree.root)
                .childIterator(false));

        /*
         * Note: the test ends here since there must be either 2 or 3 children
         * for the root node.  If we force the remaining leaves to join, then
         * the root node will be replaced by a root leaf.
         */

    }

    /**
     * Test ability to visit the nodes of the tree in a post-order traversal.
     */
    public void test_postOrderIterator01() {

        BTree btree = getBTree(3);

        final Leaf a = (Leaf) btree.root;
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v9 = new SimpleEntry(9);

        // empty tree visits the root leaf.
        assertSameIterator(new AbstractNode[] { btree.root }, btree.root
                .postOrderIterator());
        
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
        
        // verify iterator.
        assertSameIterator(new AbstractNode[] { a, b, c }, btree.root
                .postOrderIterator());

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

        // verify iterator
        assertSameIterator(new AbstractNode[] { a, d, b, c }, btree.root
                .postOrderIterator());
        
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

        // verify iterator
        assertSameIterator(new AbstractNode[] { a, d, c, e, b, f, g }, btree.root
                .postOrderIterator());

        /*
         * remove a key (4) from (d) forcing (d,a) to merge into (d) and (a) to
         * be deleted. this causes (c,f) to merge as well, which in turn forces
         * the root to be replaced by (c).
         */
        assertEquals(v4,btree.remove(4));
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

        // verify iterator
        assertSameIterator(new AbstractNode[] { d, e, b, c }, btree.root
                .postOrderIterator());

        /*
         * remove a key (7) from a leaf (b) forcing two leaves to join and
         * verify the visitation order.
         */
        assertEquals(v7,btree.remove(7));
        btree.dump(Level.DEBUG,System.err);
        assertKeys(new int[]{5},c);
        assertEquals(d,c.getChild(0));
        assertEquals(b,c.getChild(1));
        assertKeys(new int[]{1,2,3},d);
        assertValues(new Object[]{v1,v2,v3}, d);
        assertKeys(new int[]{5,6,9},b);
        assertValues(new Object[]{v5,v6,v9}, b);
        assertTrue(e.isDeleted());

        // verify iterator
        assertSameIterator(new AbstractNode[] { d, b, c }, btree.root
                .postOrderIterator());

        /*
         * remove keys from a leaf forcing the remaining two leaves to join and
         * verify the visitation order.
         */
        assertEquals(v3,btree.remove(3));
        assertEquals(v5,btree.remove(5));
        assertEquals(v6,btree.remove(6));
        assertKeys(new int[]{1,2,9},b);
        assertValues(new Object[]{v1,v2,v9}, b);
        assertTrue(d.isDeleted());
        assertTrue(c.isDeleted());

        // verify iterator
        assertSameIterator(new AbstractNode[] { b }, btree.root
                .postOrderIterator());

    }

}
