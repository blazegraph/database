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
 * Test split of the root leaf.
 * 
 * @see src/architecture/btree.xls for the examples used in this suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSplitRootLeaf extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestSplitRootLeaf() {
    }

    /**
     * @param name
     */
    public TestSplitRootLeaf(String name) {
        super(name);
    }

    /**
     * A series of tests split with the key below, at, and above the low key in
     * the right sibling. This verifies that the insert logic looks at the
     * separator key in the parent when determining which leaf receives the
     * insert key after a split and NOT at the low key in the rightSibling.
     */
    public void test_leafSplitBranchingFactor3_01() {
        
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
        
        assertKeys(new int[]{2,3},a);
        assertValues(new Object[]{v2,v3},a);
        
        assertKeys(new int[]{5,7},b);
        assertValues(new Object[]{v5,v7},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);

    }
    
    public void test_leafSplitBranchingFactor3_02() {
        
        BTree btree = getBTree(3);
        
        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
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
        btree.insert(4,v4);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{5},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertKeys(new int[]{3,4},a);
        assertValues(new Object[]{v3,v4},a);
        
        assertKeys(new int[]{5,7},b);
        assertValues(new Object[]{v5,v7},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);

    }
    
    public void test_leafSplitBranchingFactor3_03() {
        
        BTree btree = getBTree(3);
        
        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);

        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        
        assertKeys(new int[]{3,5,7},a);
        assertValues(new Object[]{v3,v5,v7},a);

        /*
         * split the leaf.
         */
        btree.insert(6,v6);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{6},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5},a);
        
        assertKeys(new int[]{6,7},b);
        assertValues(new Object[]{v6,v7},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);

    }
    
    public void test_leafSplitBranchingFactor3_04() {
        
        BTree btree = getBTree(3);
        
        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);

        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        
        assertKeys(new int[]{3,5,7},a);
        assertValues(new Object[]{v3,v5,v7},a);

        /*
         * insert key that will go into the high leaf.
         */
        btree.insert(8,v8);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{7},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5},a);
        
        assertKeys(new int[]{7,8},b);
        assertValues(new Object[]{v7,v8},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);

    }
    
    public void test_leafSplitBranchingFactor4_01() {
        
        BTree btree = getBTree(4);
        
        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v9 = new SimpleEntry(9);

        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        btree.insert(9, v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(2,v2);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{5},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertKeys(new int[]{2,3},a);
        assertValues(new Object[]{v2,v3},a);
        
        assertKeys(new int[]{5,7,9},b);
        assertValues(new Object[]{v5,v7,v9},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

    }

    public void test_leafSplitBranchingFactor4_02() {
        
        BTree btree = getBTree(4);
        
        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v9 = new SimpleEntry(9);

        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        btree.insert(9, v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(4,v4);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{5},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertKeys(new int[]{3,4},a);
        assertValues(new Object[]{v3,v4},a);
        
        assertKeys(new int[]{5,7,9},b);
        assertValues(new Object[]{v5,v7,v9},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

    }

    public void test_leafSplitBranchingFactor4_03() {
        
        BTree btree = getBTree(4);
        
        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v9 = new SimpleEntry(9);

        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        btree.insert(9, v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(6,v6);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{6},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5},a);
        
        assertKeys(new int[]{6,7,9},b);
        assertValues(new Object[]{v6,v7,v9},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

    }

    public void test_leafSplitBranchingFactor4_04() {
        
        BTree btree = getBTree(4);
        
        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        SimpleEntry v9 = new SimpleEntry(9);

        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        btree.insert(9, v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(8,v8);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{7},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5},a);
        
        assertKeys(new int[]{7,8,9},b);
        assertValues(new Object[]{v7,v8,v9},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

    }

    public void test_leafSplitBranchingFactor4_05() {
        
        BTree btree = getBTree(4);
        
        Leaf a = (Leaf)btree.getRoot();

        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v9 = new SimpleEntry(9);
        SimpleEntry v10 = new SimpleEntry(10);

        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
        btree.insert(9, v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(10,v10);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{7},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertKeys(new int[]{3,5},a);
        assertValues(new Object[]{v3,v5},a);
        
        assertKeys(new int[]{7,9,10},b);
        assertValues(new Object[]{v7,v9,v10},b);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

    }

}
