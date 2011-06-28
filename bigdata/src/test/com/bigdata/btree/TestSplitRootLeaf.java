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

import org.apache.log4j.Level;

import com.bigdata.btree.keys.TestKeyBuilder;

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

        final BTree btree = getBTree(3);

        final Leaf a = (Leaf) btree.getRoot();

        final SimpleEntry v2 = new SimpleEntry(2);
        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v7 = new SimpleEntry(7);

        btree.insert(TestKeyBuilder.asSortKey(3), v3);
        btree.insert(TestKeyBuilder.asSortKey(5), v5);
        btree.insert(TestKeyBuilder.asSortKey(7), v7);
        
        assertKeys(new int[] { 3, 5, 7 }, a);
        assertValues(new Object[] { v3, v5, v7 }, a);

        /*
         * split the leaf.
         */
        assertTrue(btree.dump(Level.DEBUG, System.err));
        btree.insert(TestKeyBuilder.asSortKey(2), v2);
        assertTrue(btree.dump(Level.DEBUG, System.err));

        final Node root = (Node) btree.getRoot();
        assertKeys(new int[] { 5 }, root);
        assertEquals(a, root.getChild(0));
        final Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[] { 2, 2 }, root);

        assertKeys(new int[] { 2, 3 }, a);
        assertValues(new Object[] { v2, v3 }, a);

        assertKeys(new int[] { 5, 7 }, b);
        assertValues(new Object[] { v5, v7 }, b);

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

        btree.insert(TestKeyBuilder.asSortKey(3), v3);
        btree.insert(TestKeyBuilder.asSortKey(5), v5);
        btree.insert(TestKeyBuilder.asSortKey(7), v7);
        
        assertKeys(new int[]{3,5,7},a);
        assertValues(new Object[]{v3,v5,v7},a);

        /*
         * split the leaf.
         */
        btree.insert(TestKeyBuilder.asSortKey(4),v4);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{5},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[]{2,2},root);
        
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

        btree.insert(TestKeyBuilder.asSortKey(3), v3);
        btree.insert(TestKeyBuilder.asSortKey(5), v5);
        btree.insert(TestKeyBuilder.asSortKey(7), v7);
        
        assertKeys(new int[]{3,5,7},a);
        assertValues(new Object[]{v3,v5,v7},a);

        /*
         * split the leaf.
         */
        btree.insert(TestKeyBuilder.asSortKey(6),v6);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{6},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[]{2,2},root);
        
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

        btree.insert(TestKeyBuilder.asSortKey(3), v3);
        btree.insert(TestKeyBuilder.asSortKey(5), v5);
        btree.insert(TestKeyBuilder.asSortKey(7), v7);
        
        assertKeys(new int[]{3,5,7},a);
        assertValues(new Object[]{v3,v5,v7},a);

        /*
         * insert key that will go into the high leaf.
         */
        btree.insert(TestKeyBuilder.asSortKey(8),v8);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{7},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[]{2,2},root);
        
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

        btree.insert(TestKeyBuilder.asSortKey(3), v3);
        btree.insert(TestKeyBuilder.asSortKey(5), v5);
        btree.insert(TestKeyBuilder.asSortKey(7), v7);
        btree.insert(TestKeyBuilder.asSortKey(9), v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(TestKeyBuilder.asSortKey(2),v2);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{5},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[]{2,3},root);
        
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

        btree.insert(TestKeyBuilder.asSortKey(3), v3);
        btree.insert(TestKeyBuilder.asSortKey(5), v5);
        btree.insert(TestKeyBuilder.asSortKey(7), v7);
        btree.insert(TestKeyBuilder.asSortKey(9), v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(TestKeyBuilder.asSortKey(4),v4);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{5},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[]{2,3},root);
        
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

        btree.insert(TestKeyBuilder.asSortKey(3), v3);
        btree.insert(TestKeyBuilder.asSortKey(5), v5);
        btree.insert(TestKeyBuilder.asSortKey(7), v7);
        btree.insert(TestKeyBuilder.asSortKey(9), v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(TestKeyBuilder.asSortKey(6),v6);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{6},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[]{2,3},root);
        
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

        btree.insert(TestKeyBuilder.asSortKey(3), v3);
        btree.insert(TestKeyBuilder.asSortKey(5), v5);
        btree.insert(TestKeyBuilder.asSortKey(7), v7);
        btree.insert(TestKeyBuilder.asSortKey(9), v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(TestKeyBuilder.asSortKey(8),v8);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{7},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[]{2,3},root);
        
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

        btree.insert(TestKeyBuilder.asSortKey(3), v3);
        btree.insert(TestKeyBuilder.asSortKey(5), v5);
        btree.insert(TestKeyBuilder.asSortKey(7), v7);
        btree.insert(TestKeyBuilder.asSortKey(9), v9);
        
        assertKeys(new int[]{3,5,7,9},a);
        assertValues(new Object[]{v3,v5,v7,v9},a);

        /*
         * split the leaf.
         */
        btree.insert(TestKeyBuilder.asSortKey(10),v10);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        
        Node root = (Node)btree.getRoot();
        assertKeys(new int[]{7},root);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        assertEntryCounts(new int[]{2,3},root);
        
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
