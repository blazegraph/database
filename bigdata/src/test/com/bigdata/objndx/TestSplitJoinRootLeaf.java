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
 * Created on Nov 27, 2006
 */

package com.bigdata.objndx;

import org.apache.log4j.Level;

/**
 * Test split and join of the root leaf (the tree never has more than two
 * levels).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSplitJoinRootLeaf extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestSplitJoinRootLeaf() {
    }

    /**
     * @param name
     */
    public TestSplitJoinRootLeaf(String name) {
        super(name);
    }

    /**
     * A series of tests of the mechanisms for splitting and joining a leaf with
     * a branching factor of three (3). Each tests uses the same initial
     * conditions but forces removes a different key to trigger the join to test
     * the various edge conditions.
     * 
     * Note: this does not test all edge conditions since we only have two
     * leaves and therefore only a single sibling is defined, but the general
     * case allows two siblings, even with a branching factor of (3).
     * 
     * Note: this does vary the insert key that drives the split, but we verify
     * in other tests that the split key is correctly computed.
     * 
     * Note: when m := 3, a node is either at its minimum or at its maximum so
     * {@link Leaf#join()} always triggers {@link Leaf#merge(Leaf)}.  This is
     * not true when the branching factor is higher than three (3).
     * 
     * @see Leaf#getSplitIndex(int)
     * @see Leaf#split(int)
     * @see Leaf#join()
     * @see Leaf#merge(Leaf)
     * @see Leaf#redistributeKeys(Leaf)
     * @see Node#getIndexOf(AbstractNode)
     * 
     * @todo Do detailed tests for a Node with a branching factor of 3.
     * @todo Do detailed tests for a Node with a branching factor of 4.
     */
    public void test_splitJoinLeafBranchingFactor3_01() {
        
        BTree btree = getBTree(3);

        Leaf a = (Leaf)btree.getRoot();
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        
        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{3,5,7},a.keys);
        assertEquals(new Object[]{v3,v5,v7},a.values);

        // split the root leaf.
        btree.insert(1, v1);
        
        Node root = (Node)btree.getRoot();
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{5,0},root.keys);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{1,3,0},a.keys);
        assertEquals(new Object[]{v1,v3,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{5,7,0},b.keys);
        assertEquals(new Object[]{v5,v7,null},b.values);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        /*
         * make (a) deficient by removing the key/value for (1). This will cause
         * a.join() to be invoked and trigger a.merge(b). The post-condition is
         * a with the remaining keys and values. Both the root node and the
         * merged sibling should be marked as deleted.
         */
        btree.remove(1);
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{3,5,7},a.keys);
        assertEquals(new Object[]{v3,v5,v7},a.values);

        assertTrue(!a.isDeleted());
        assertTrue(b.isDeleted());
        assertTrue(root.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 3, btree.nentries);

    }
    
    public void test_splitJoinLeafBranchingFactor3_02() {
        
        BTree btree = getBTree(3);

        Leaf a = (Leaf)btree.getRoot();
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        
        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{3,5,7},a.keys);
        assertEquals(new Object[]{v3,v5,v7},a.values);

        // split the root leaf.
        btree.insert(1, v1);
        
        Node root = (Node)btree.getRoot();
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{5,0},root.keys);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{1,3,0},a.keys);
        assertEquals(new Object[]{v1,v3,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{5,7,0},b.keys);
        assertEquals(new Object[]{v5,v7,null},b.values);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        /*
         * make (a) deficient by removing the key/value for (3). This will cause
         * a.join() to be invoked and trigger a.merge(b). The post-condition is
         * a with the remaining keys and values. Both the root node and the
         * merged sibling should be marked as deleted.
         */
        btree.remove(3);
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{1,5,7},a.keys);
        assertEquals(new Object[]{v1,v5,v7},a.values);

        assertTrue(!a.isDeleted());
        assertTrue(b.isDeleted());
        assertTrue(root.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 3, btree.nentries);

    }
    
    public void test_splitJoinLeafBranchingFactor3_03() {
        
        BTree btree = getBTree(3);

        Leaf a = (Leaf)btree.getRoot();
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        
        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{3,5,7},a.keys);
        assertEquals(new Object[]{v3,v5,v7},a.values);

        // split the root leaf.
        btree.insert(1, v1);
        
        Node root = (Node)btree.getRoot();
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{5,0},root.keys);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{1,3,0},a.keys);
        assertEquals(new Object[]{v1,v3,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{5,7,0},b.keys);
        assertEquals(new Object[]{v5,v7,null},b.values);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        /*
         * make (b) deficient by removing the key/value for (5). This will cause
         * b.join() to be invoked and trigger b.merge(a). The post-condition is
         * b with the remaining keys and values. Both the root node and the
         * merged sibling should be marked as deleted.
         */
        btree.remove(5);
        
        assertEquals(3,b.nkeys);
        assertEquals(new int[]{1,3,7},b.keys);
        assertEquals(new Object[]{v1,v3,v7},b.values);

        assertTrue(a.isDeleted());
        assertTrue(!b.isDeleted());
        assertTrue(root.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 3, btree.nentries);

    }
    
    public void test_splitJoinLeafBranchingFactor3_04() {
        
        BTree btree = getBTree(3);

        Leaf a = (Leaf)btree.getRoot();
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        
        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{3,5,7},a.keys);
        assertEquals(new Object[]{v3,v5,v7},a.values);

        // split the root leaf.
        btree.insert(1, v1);
        
        Node root = (Node)btree.getRoot();
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{5,0},root.keys);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{1,3,0},a.keys);
        assertEquals(new Object[]{v1,v3,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{5,7,0},b.keys);
        assertEquals(new Object[]{v5,v7,null},b.values);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        /*
         * make (b) deficient by removing the key/value for (7). This will cause
         * b.join() to be invoked and trigger b.merge(a). The post-condition is
         * b with the remaining keys and values. Both the root node and the
         * merged sibling should be marked as deleted.
         */
        btree.remove(7);
        
        assertEquals(3,b.nkeys);
        assertEquals(new int[]{1,3,5},b.keys);
        assertEquals(new Object[]{v1,v3,v5},b.values);

        assertTrue(a.isDeleted());
        assertTrue(!b.isDeleted());
        assertTrue(root.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 3, btree.nentries);

    }
    
    /**
     * Note: with a branching factor greater than (3) it is possible for
     * {@link Leaf#join()} to trigger {@link Leaf#redistributeKeys(Leaf)} rather
     * than {@link Leaf#merge(Leaf)}. Since we tested {@link Leaf#merge(Leaf)}
     * for m := 3, we focus on testing {@link Leaf#redistributeKeys(Leaf)} with
     * m := 4.
     * 
     * This test triggers the redistribution of a key from (a) to (b).
     */
    public void test_splitJoinLeafBranchingFactor4_01() {
        
        BTree btree = getBTree(4);

        Leaf a = (Leaf)btree.getRoot();
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v9 = new SimpleEntry(9);
        
        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        btree.insert(9, v9);
        
        assertEquals(4,a.nkeys);
        assertEquals(new int[]{3,5,7,9},a.keys);
        assertEquals(new Object[]{v3,v5,v7,v9},a.values);

        // split the root leaf.
        btree.insert(1, v1);
        btree.dump(Level.DEBUG,System.err);
        
        Node root = (Node)btree.getRoot();
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{7,0,0},root.keys);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{1,3,5,0},a.keys);
        assertEquals(new Object[]{v1,v3,v5,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{7,9,0,0},b.keys);
        assertEquals(new Object[]{v7,v9,null,null},b.values);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);
        
        /*
         * make (b) deficient by removing the key/value for (9). This will cause
         * b.join() to be invoked and trigger b.redistributeKeys(a).  The latter
         * will move one key from (a) -> (b) and update the separator key on the
         * parent.
         */
        btree.remove(9);
        btree.dump(Level.DEBUG,System.err);
        
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{5,0,0},root.keys);
        assertEquals(a,root.getChild(0));
        assertEquals(b,root.getChild(1));
        
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{1,3,0,0},a.keys);
        assertEquals(new Object[]{v1,v3,null,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{5,7,0,0},b.keys);
        assertEquals(new Object[]{v5,v7,null,null},b.values);

        assertTrue(!a.isDeleted());
        assertTrue(!b.isDeleted());
        assertTrue(!root.isDeleted());
        
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);

        /*
         * Continue by removing another key and forcing merge. This removes (7)
         * from (b), making (b) deficient and forcing b.merge(a). The
         * postcondition is that (b) has all the remaining keys and has become
         * the root leaf and the (a) and the root node are deleted.
         */

        btree.remove(7);
        
        assertEquals(3,b.nkeys);
        assertEquals(new int[]{1,3,5,0},b.keys);
        assertEquals(new Object[]{v1,v3,v5,null},b.values);
        
        assertTrue(a.isDeleted());
        assertTrue(!b.isDeleted());
        assertTrue(root.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 3, btree.nentries);

    }

    /**
     * This test triggers the redistribution of a key from (b) to (a).
     */
    public void test_splitJoinLeafBranchingFactor4_02() {
        
        BTree btree = getBTree(4);

        Leaf a = (Leaf)btree.getRoot();
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v9 = new SimpleEntry(9);
        SimpleEntry v11 = new SimpleEntry(11);
        
        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        btree.insert(9, v9);
        
        assertEquals(4,a.nkeys);
        assertEquals(new int[]{3,5,7,9},a.keys);
        assertEquals(new Object[]{v3,v5,v7,v9},a.values);

        // split the root leaf.
        btree.insert(1, v1);
        
        Node root = (Node)btree.getRoot();
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{7,0,0},root.keys);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{1,3,5,0},a.keys);
        assertEquals(new Object[]{v1,v3,v5,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{7,9,0,0},b.keys);
        assertEquals(new Object[]{v7,v9,null,null},b.values);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);
        
        /*
         * insert another key that will go into (b) bringing it above its
         * minimum capacity.
         */
        btree.insert(11,v11);
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{7,0,0},root.keys);
        assertEquals(a,root.getChild(0));
        assertEquals(b,root.getChild(1));
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{1,3,5,0},a.keys);
        assertEquals(new Object[]{v1,v3,v5,null},a.values);
        
        assertEquals(3,b.nkeys);
        assertEquals(new int[]{7,9,11,0},b.keys);
        assertEquals(new Object[]{v7,v9,v11,null},b.values);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 6, btree.nentries);
        
        /*
         * make (b) deficient by removing the key/value for (1) and then (3).
         * This will cause a.join() to be invoked and trigger
         * a.redistributeKeys(b). The latter will move one key from (b) -> (a)
         * and update the separator key on the parent.
         */
        btree.remove(1);
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{3,5,0,0},a.keys);
        assertEquals(new Object[]{v3,v5,null,null},a.values);

        btree.remove(3);
        btree.dump(Level.DEBUG,System.err);
        
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{9,0,0},root.keys);
        assertEquals(a,root.getChild(0));
        assertEquals(b,root.getChild(1));
        
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{5,7,0,0},a.keys);
        assertEquals(new Object[]{v5,v7,null,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{9,11,0,0},b.keys);
        assertEquals(new Object[]{v9,v11,null,null},b.values);

        assertTrue(!a.isDeleted());
        assertTrue(!b.isDeleted());
        assertTrue(!root.isDeleted());
        
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);

        /*
         * Continue by removing another key and forcing merge. This removes (11)
         * from (b), making (b) deficient and forcing b.merge(a). The
         * postcondition is that (b) has all the remaining keys and has become
         * the root leaf and the (a) and the root node are deleted.
         */

        btree.remove(11);
        
        assertEquals(3,b.nkeys);
        assertEquals(new int[]{5,7,9,0},b.keys);
        assertEquals(new Object[]{v5,v7,v9,null},b.values);
        
        assertTrue(a.isDeleted());
        assertTrue(!b.isDeleted());
        assertTrue(root.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 3, btree.nentries);

    }

}
