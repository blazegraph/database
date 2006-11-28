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
     * insert(1) into [2,11,21,31] yeilds [1,2,11,-] and [21,31,-,-].
     */
    public void test_splitRootLeaf01() {

        final int m = 4;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{2,11,21,31};
        
        SimpleEntry[] entries = new SimpleEntry[] { new SimpleEntry(),
                new SimpleEntry(), new SimpleEntry(), new SimpleEntry() };
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            SimpleEntry entry = entries[i];
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);
            
            assertEquals(entry,btree.lookup(key));
            
        }
        
        System.err.print("Root leaf before split: ");
        btree.root.dump(System.err);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        assertSameIterator(entries,btree.root.entryIterator());
        
        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());

        /*
         * Split low. The key will be inserted into the first position of the
         * low leaf.
         */

        int splitKey = 1;

        SimpleEntry splitEntry = new SimpleEntry();

        assertNull(btree.lookup(splitKey));

        btree.insert(splitKey, splitEntry);

        assertEquals(splitEntry, btree.lookup(splitKey));
    
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

        assertNotSame(leaf1,btree.root);
        assertFalse(btree.root.isLeaf());
        Node root = (Node)btree.root; // the new root.
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{21,0,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        Leaf leaf2 = (Leaf)root.getChild(1); // high leaf from the split.
        assertNotSame(leaf1,leaf2);

        System.err.print("Root node after split: ");
        root.dump(System.err);

        System.err.print("Low leaf after split: ");
        leaf1.dump(System.err);

        System.err.print("High leaf after split: ");
        leaf2.dump(System.err);

        assertEquals(3,leaf1.nkeys);
        assertEquals(new int[]{1,2,11,0},leaf1.keys);

        assertEquals(2,leaf2.nkeys);
        assertEquals(new int[]{21,31,0,0},leaf2.keys);

        assertSameIterator(new SimpleEntry[] { splitEntry, entries[0], entries[1] },
                leaf1.entryIterator());

        assertSameIterator(new SimpleEntry[] { entries[2], entries[3] }, leaf2
                .entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new SimpleEntry[] { splitEntry, entries[0], entries[1],
                entries[2], entries[3] }, root.entryIterator());
        
    }
    
    /**
     * insert(4) into [1,11,21,31] yeilds [1,4,11,-] and [21,31,-,-].
     */
    public void test_splitRootLeaf02() {

        final int m = 4;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{1,11,21,31};
        
        SimpleEntry[] entries = new SimpleEntry[]{new SimpleEntry(),new SimpleEntry(),new SimpleEntry(),new SimpleEntry()};
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            SimpleEntry entry = entries[i];
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);
            
            assertEquals(entry,btree.lookup(key));
            
        }
        
        System.err.print("Root leaf before split: ");
        btree.root.dump(System.err);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        assertSameIterator(entries,btree.root.entryIterator());
        
        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());

        /*
         * Split low.  The key will be inserted into the middle of the low leaf.
         */

        int splitKey = 4;

        SimpleEntry splitEntry = new SimpleEntry();

        assertNull(btree.lookup(splitKey));

        btree.insert(splitKey, splitEntry);

        assertEquals(splitEntry, btree.lookup(splitKey));
    
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

        assertNotSame(leaf1,btree.root);
        assertFalse(btree.root.isLeaf());
        Node root = (Node)btree.root; // the new root.
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{21,0,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        Leaf leaf2 = (Leaf)root.getChild(1); // high leaf from the split.
        assertNotSame(leaf1,leaf2);

        System.err.print("Root node after split: ");
        root.dump(System.err);

        System.err.print("Low leaf after split: ");
        leaf1.dump(System.err);

        System.err.print("High leaf after split: ");
        leaf2.dump(System.err);

        assertEquals(3,leaf1.nkeys);
        assertEquals(new int[]{1,4,11,0},leaf1.keys);

        assertEquals(2,leaf2.nkeys);
        assertEquals(new int[]{21,31,0,0},leaf2.keys);

        assertSameIterator(new SimpleEntry[] { entries[0], splitEntry, entries[1] },
                leaf1.entryIterator());

        assertSameIterator(new SimpleEntry[] { entries[2], entries[3] }, leaf2
                .entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new SimpleEntry[] { entries[0], splitEntry, entries[1],
                entries[2], entries[3] }, root.entryIterator());

    }

    /**
     * insert(20) into [1,11,21,31] yeilds [1,11,20,-] and [21,31,-,-].
     */
    public void test_splitRootLeaf03() {

        final int m = 4;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{1,11,21,31};
        
        SimpleEntry[] entries = new SimpleEntry[]{new SimpleEntry(),new SimpleEntry(),new SimpleEntry(),new SimpleEntry()};
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            SimpleEntry entry = entries[i];
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);
            
            assertEquals(entry,btree.lookup(key));
            
        }
        
        System.err.print("Root leaf before split: ");
        btree.root.dump(System.err);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        assertSameIterator(entries,btree.root.entryIterator());
        
        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());

        /*
         * Split low.
         */

        int splitKey = 20;

        SimpleEntry splitEntry = new SimpleEntry();

        assertNull(btree.lookup(splitKey));

        btree.insert(splitKey, splitEntry);

        assertEquals(splitEntry, btree.lookup(splitKey));
    
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

        assertNotSame(leaf1,btree.root);
        assertFalse(btree.root.isLeaf());
        Node root = (Node)btree.root; // the new root.
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{21,0,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        Leaf leaf2 = (Leaf)root.getChild(1); // high leaf from the split.
        assertNotSame(leaf1,leaf2);

        System.err.print("Root node after split: ");
        root.dump(System.err);

        System.err.print("Low leaf after split: ");
        leaf1.dump(System.err);

        System.err.print("High leaf after split: ");
        leaf2.dump(System.err);

        assertEquals(3,leaf1.nkeys);
        assertEquals(new int[]{1,11,20,0},leaf1.keys);

        assertEquals(2,leaf2.nkeys);
        assertEquals(new int[]{21,31,0,0},leaf2.keys);

        assertSameIterator(new SimpleEntry[] { entries[0], entries[1], splitEntry },
                leaf1.entryIterator());

        assertSameIterator(new SimpleEntry[] { entries[2], entries[3] }, leaf2
                .entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new SimpleEntry[] { entries[0], entries[1], splitEntry, 
                entries[2], entries[3] }, root.entryIterator());

    }

    /**
     * insert(22) into [1,11,21,31] yeilds [1,11,21,-] and [22,31,-,-].
     */
    public void test_splitRootLeaf04() {

        final int m = 4;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{1,11,21,31};
        
        SimpleEntry[] entries = new SimpleEntry[]{new SimpleEntry(),new SimpleEntry(),new SimpleEntry(),new SimpleEntry()};
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            SimpleEntry entry = entries[i];
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);
            
            assertEquals(entry,btree.lookup(key));
            
        }
        
        System.err.print("Root leaf before split: ");
        btree.root.dump(System.err);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        assertSameIterator(entries,btree.root.entryIterator());
        
        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());

        /*
         * Split high - the key will be insert into the middle of the high leaf.
         */

        int splitKey = 22;

        SimpleEntry splitEntry = new SimpleEntry();

        assertNull(btree.lookup(splitKey));

        btree.insert(splitKey, splitEntry);
        assertTrue(btree.dump(Level.DEBUG,System.err));

        assertEquals(splitEntry, btree.lookup(splitKey));
    
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

        assertNotSame(leaf1,btree.root);
        assertFalse(btree.root.isLeaf());
        Node root = (Node)btree.root; // the new root.
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{22,0,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        Leaf leaf2 = (Leaf)root.getChild(1); // high leaf from the split.
        assertNotSame(leaf1,leaf2);

        System.err.print("Root node after split: ");
        root.dump(System.err);

        System.err.print("Low leaf after split: ");
        leaf1.dump(System.err);

        System.err.print("High leaf after split: ");
        leaf2.dump(System.err);

        assertEquals(3,leaf1.nkeys);
        assertEquals(new int[]{1,11,21,0},leaf1.keys);

        assertEquals(2,leaf2.nkeys);
        assertEquals(new int[]{22,31,0,0},leaf2.keys);

        assertSameIterator(new SimpleEntry[] { entries[0], entries[1], entries[2] }, leaf1
                .entryIterator());

        assertSameIterator(new SimpleEntry[] { splitEntry, entries[3] },
                leaf2.entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new SimpleEntry[] { entries[0], entries[1], entries[2],
                splitEntry, entries[3] }, root.entryIterator());

    }
    
    /**
     * insert(40) into [1,11,21,31] yeilds [1,11,21,-] and [31,40,-,-].
     */
    public void test_splitRootLeaf05() {

        final int m = 4;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{1,11,21,31};
        
        SimpleEntry[] entries = new SimpleEntry[]{new SimpleEntry(),new SimpleEntry(),new SimpleEntry(),new SimpleEntry()};
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            SimpleEntry entry = entries[i];
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);
            
            assertEquals(entry,btree.lookup(key));
            
        }
        
        System.err.print("Root leaf before split: ");
        btree.root.dump(System.err);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);
        
        assertSameIterator(entries,btree.root.entryIterator());
        
        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());

        /*
         * Split high - the key will be added at the end of the high leaf.
         */

        int splitKey = 40;

        SimpleEntry splitEntry = new SimpleEntry();

        assertNull(btree.lookup(splitKey));

        btree.insert(splitKey, splitEntry);
        assertTrue(btree.dump(Level.DEBUG,System.err));
        assertEquals(splitEntry, btree.lookup(splitKey));
    
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

        assertNotSame(leaf1,btree.root);
        assertFalse(btree.root.isLeaf());
        Node root = (Node)btree.root; // the new root.
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{31,0,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        Leaf leaf2 = (Leaf)root.getChild(1); // high leaf from the split.
        assertNotSame(leaf1,leaf2);

        System.err.print("Root node after split: ");
        root.dump(System.err);

        System.err.print("Low leaf after split: ");
        leaf1.dump(System.err);

        System.err.print("High leaf after split: ");
        leaf2.dump(System.err);

        assertEquals(3,leaf1.nkeys);
        assertEquals(new int[]{1,11,21,0},leaf1.keys);

        assertEquals(2,leaf2.nkeys);
        assertEquals(new int[]{31,40,0,0},leaf2.keys);

        assertSameIterator(new SimpleEntry[] { entries[0], entries[1], entries[2] }, leaf1
                .entryIterator());

        assertSameIterator(new SimpleEntry[] { entries[3], splitEntry }, leaf2
                .entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new SimpleEntry[] { entries[0], entries[1], entries[2],
                entries[3], splitEntry }, root.entryIterator());

    }
    
    /**
     * A series of tests split with the key below, at, and above the low key in
     * the right sibling. This verifies that the insert logic looks at the
     * separator key in the parent when determining which leaf receives the
     * insert key after a split and NOT at the low key in the rightSibling.
     * 
     * FIXME Write similar tests with m := 4.
     */
    public void test_leafSplitBranchingFactor3_01() {
        
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

        /*
         * insert key that will go into the low leaf.
         */
        btree.insert(1,v1);
        
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
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{3,5,7},a.keys);

        /*
         * insert key that will go into the low leaf.
         */
        btree.insert(4,v4);
        
        Node root = (Node)btree.getRoot();
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{5,0},root.keys);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{3,4,0},a.keys);
        assertEquals(new Object[]{v3,v4,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{5,7,0},b.keys);
        assertEquals(new Object[]{v5,v7,null},b.values);

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
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{3,5,7},a.keys);

        /*
         * insert key that will go into the high leaf.
         */
        btree.insert(6,v6);
        
        Node root = (Node)btree.getRoot();
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{6,0},root.keys);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{3,5,0},a.keys);
        assertEquals(new Object[]{v3,v5,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{6,7,0},b.keys);
        assertEquals(new Object[]{v6,v7,null},b.values);

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
        
        assertEquals(3,a.nkeys);
        assertEquals(new int[]{3,5,7},a.keys);

        /*
         * insert key that will go into the high leaf.
         */
        btree.insert(8,v8);
        
        Node root = (Node)btree.getRoot();
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{7,0},root.keys);
        assertEquals(a,root.getChild(0));
        Leaf b = (Leaf) root.getChild(1);
        
        assertEquals(2,a.nkeys);
        assertEquals(new int[]{3,5,0},a.keys);
        assertEquals(new Object[]{v3,v5,null},a.values);
        
        assertEquals(2,b.nkeys);
        assertEquals(new int[]{7,8,0},b.keys);
        assertEquals(new Object[]{v7,v8,null},b.values);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 4, btree.nentries);

    }

}
