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
 * Created on Nov 8, 2006
 */

package com.bigdata.objndx;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Level;

/**
 * Test basic btree operations, starting with insert into the root leaf, then
 * splitting the root leaf, then inserts that cause the root node to be split.
 * 
 * @todo test tree under sequential insertion, nearly sequential insertion
 *       (based on a model identifier generation for the read-optimized
 *       database, including filling up pages, eventually releasing space on
 *       pages, and then reclaiming space where it becomes available on pages),
 *       and under random key generation. Does the tree stay nicely balanced for
 *       all scenarios? What about the average search time? Periodically verify
 *       that the tree remains fully ordered. Choose a split rule that works
 *       well for mostly sequential and mostly monotonic keys with some removal
 *       of entries (sparsity).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBTree extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestBTree() {
    }

    /**
     * @param arg0
     */
    public TestBTree(String arg0) {

        super(arg0);

    }

    /*
     * test helpers.
     */
    
    /*
     * insert, lookup, and value scan for leaves.
     */

    /**
     * Test ability to insert entries into a leaf. Random (legal) external keys
     * are inserted into a leaf until the leaf would overflow. The random keys
     * are then sorted and compared with the actual keys in the leaf. If the
     * keys were inserted correctly into the leaf then the two arrays of keys
     * will have the same values in the same order.
     */
    public void test_insertKeyIntoLeaf01() {

        final int branchingFactor = 20;
        
        IBTree btree = getBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();
        
        // array of inserted keys.
        int[] expectedKeys = new int[branchingFactor];
        
        int nkeys = 0;
        
        while( nkeys < branchingFactor ) {
            
            // Valid random key.
            int key = r.nextInt(IBTree.POSINF-1)+1;
            
            int index = Search.search(key, root.keys, root.nkeys);
            
            if( index >= 0 ) {
                
                /*
                 * The key is already present in the leaf.
                 */
                
                continue;
                
            }
        
            // Convert the position to obtain the insertion point.
            index = -index - 1;

            // save the key.
            expectedKeys[ nkeys ] = key;
            
            System.err.println("Will insert: key=" + key + " at index=" + index
                    + " : nkeys=" + nkeys);

            // insert an entry under that key.
            root.insert(key, index, new SimpleEntry() );
            
            nkeys++;
            
            assertEquals( nkeys, root.nkeys );
            
        }

        // sort the keys that we inserted.
        Arrays.sort(expectedKeys);
        
        // verify that the leaf has the same keys in the same order.
        assertEquals( expectedKeys, root.keys );
        
    }
    
    /**
     * Test ability to insert entries into a leaf. Random (legal) external keys
     * are inserted into a leaf until the leaf would overflow. The random keys
     * are then sorted and compared with the actual keys in the leaf. If the
     * keys were inserted correctly into the leaf then the two arrays of keys
     * will have the same values in the same order. (This is a variation on the
     * previous test that inserts keys into the leaf using a slightly higher
     * level API).
     */
    public void test_insertKeyIntoLeaf02() {

        final int branchingFactor = 20;
        
        IBTree btree = getBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();

        // array of inserted keys.
        int[] expectedKeys = new int[branchingFactor];
        
        int nkeys = 0;
        
        while( nkeys < branchingFactor ) {
            
            // Valid random key.
            int key = r.nextInt(IBTree.POSINF-1)+1;

            int nkeysBefore = root.nkeys;
            
            boolean exists = root.lookup(key) != null;
            
            root.insert(key, new SimpleEntry() );

            if( nkeysBefore == root.nkeys ) {
                
                // key already exists.
                assertTrue(exists);
                
                continue;
                
            }
            
            assertFalse(exists);
            
            // save the key.
            expectedKeys[ nkeys ] = key;
            
            nkeys++;
            
            assertEquals( nkeys, root.nkeys );
            
        }

        // sort the keys that we inserted.
        Arrays.sort(expectedKeys);
        
        // verify that the leaf has the same keys in the same order.
        assertEquals( expectedKeys, root.keys );
        
    }

    /**
     * Test ability to insert entries into a leaf. Known keys and values are
     * generated and inserted into the leaf in a random order. The sequence of
     * keys and values in the leaf is then compared with the pre-generated
     * sequences known to the unit test. The correct behavior of the
     * {@link Leaf#entryIterator()} is also tested.
     * 
     * @see Leaf#insert(int, com.bigdata.objndx.TestBTree.SimpleEntry)
     * @see Leaf#lookup(int)
     * @see Leaf#entryIterator()
     */
    public void test_insertKeyIntoLeaf03() {

        final int branchingFactor = 20;
        
        IBTree btree = getBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();

        // array of keys to insert.
        int[] expectedKeys = new int[branchingFactor];

        // the value to insert for each key.
        SimpleEntry[] expectedValues = new SimpleEntry[branchingFactor];
        
        /*
         * Generate keys and values. The keys are a monotonic progression with
         * random non-zero intervals.
         */
        
        int lastKey = IBTree.NEGINF;
        
        for( int i=0; i<branchingFactor; i++ ) {
            
            int key = lastKey + r.nextInt(100) + 1;
            
            expectedKeys[ i ] = key;
            
            expectedValues[ i ] = new SimpleEntry();
            
            lastKey = key; 
            
        }
        
        for( int i=0; i<branchingFactor; i++ ) {

            int key = expectedKeys[i];
            
            SimpleEntry value = expectedValues[i];
            
            assertEquals(i, root.nkeys );
            
            assertNull("Not expecting to find key=" + key, root.lookup(key));
            
//            root.dump(System.err);
            
            root.insert(key, value );
            
//            root.dump(System.err);

            assertEquals("nkeys(i=" + i + " of " + branchingFactor + ")", i + 1,
                    root.nkeys);
            
            assertEquals("value(i=" + i + " of " + branchingFactor + ")",
                    value, root.lookup(key));
            
            // verify the values iterator
            Object[] tmp = new Object[root.nkeys];
            for( int j=0; j<root.nkeys; j++ ) {
                tmp[j] = root.values[j];
            }
            assertSameIterator( "values", tmp, root.entryIterator() ); 
            
        }

        // verify that the leaf has the same keys in the same order.
        assertEquals( "keys", expectedKeys, root.keys );

        // verify that the leaf has the same values in the same order.
        assertEquals( "values", expectedValues, root.values );
        
        // verify the expected behavior of the iterator.
        assertSameIterator( "values", expectedValues, root.entryIterator() );
        
    }

    /**
     * Test insert, lookup and remove of keys in the root leaf.
     */
    public void test_insertLookupRemoveFromLeaf01() {

        final int m = 4;
        
        final IBTree btree = getBTree(m);

        final Leaf root = (Leaf) btree.getRoot();
        
        Object e1 = new SimpleEntry();
        Object e2 = new SimpleEntry();
        Object e3 = new SimpleEntry();
        Object e4 = new SimpleEntry();

        /*
         * fill the root leaf.
         */
        assertNull(root.insert(4, e4));
        assertNull(root.insert(2, e2));
        assertNull(root.insert(1, e1));
        assertNull(root.insert(3, e3));

        /*
         * verify that re-inserting does not split the leaf and returns the old
         * value.
         */
        assertEquals(e4,root.insert(4,e4));
        assertEquals(e2,root.insert(2,e2));
        assertEquals(e1,root.insert(1,e1));
        assertEquals(e3,root.insert(3,e3));
        assertEquals(root,btree.getRoot());
        
        // validate
        assertEquals(4,root.nkeys);
        assertEquals(new int[]{1,2,3,4},root.keys);
        assertSameIterator(new Object[]{e1,e2,e3,e4}, root.entryIterator());

        // remove (2).
        assertEquals(e2,root.remove(2));
        assertEquals(3,root.nkeys);
        assertEquals(new int[]{1,3,4,0},root.keys);
        assertSameIterator(new Object[]{e1,e3,e4}, root.entryIterator());

        // remove (1).
        assertEquals(e1,root.remove(1));
        assertEquals(2,root.nkeys);
        assertEquals(new int[]{3,4,0,0},root.keys);
        assertSameIterator(new Object[]{e3,e4}, root.entryIterator());

        // remove (4).
        assertEquals(e4,root.remove(4));
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{3,0,0,0},root.keys);
        assertSameIterator(new Object[]{e3}, root.entryIterator());

        // remove (3).
        assertEquals(e3,root.remove(3));
        assertEquals(0,root.nkeys);
        assertEquals(new int[]{0,0,0,0},root.keys);
        assertSameIterator(new Object[]{}, root.entryIterator());

        assertNull(root.remove(1));
        assertNull(root.remove(2));
        assertNull(root.remove(3));
        assertNull(root.remove(4));
        
    }
    
    /**
     * Test insert and removal of keys in the root leaf.
     */
    public void test_insertLookupRemoveFromLeaf02() {

        final int m = 4;
        
        final BTree btree = getBTree(m);

        Map<Integer,SimpleEntry> expected = new TreeMap<Integer,SimpleEntry>();
        
        Integer k1 = 1;
        Integer k2 = 2;
        Integer k3 = 3;
        Integer k4 = 4;
        
        SimpleEntry e1 = new SimpleEntry();
        SimpleEntry e2 = new SimpleEntry();
        SimpleEntry e3 = new SimpleEntry();
        SimpleEntry e4 = new SimpleEntry();

        Integer[] keys = new Integer[] {k1,k2,k3,k4};
        SimpleEntry[] vals = new SimpleEntry[]{e1,e2,e3,e4};
        
        for( int i=0; i<1000; i++ ) {
            
            boolean insert = r.nextBoolean();
            
            int index = r.nextInt(m);
            
            Integer key = keys[index];
            
            SimpleEntry val = vals[index];
            
            if( insert ) {
                
//                System.err.println("insert("+key+", "+val+")");
                SimpleEntry old = expected.put(key, val);
                
                SimpleEntry old2 = (SimpleEntry) btree.insert(key.intValue(), val);
                
//                btree.dump(Level.DEBUG,System.err);
                
                assertEquals(old, old2);

                // verify that the root leaf was not split.
                assertEquals("height",0,btree.height);
                assertTrue(btree.root instanceof Leaf);

            } else {
                
//                System.err.println("remove("+key+")");
                SimpleEntry old = expected.remove(key);
                
                SimpleEntry old2 = (SimpleEntry) btree.remove(key.intValue());
                
//                btree.dump(Level.DEBUG,System.err);
                
                assertEquals(old, old2);
                
            }

            if( i % 100 == 0 ) {

                /*
                 * Validate the keys and entries.
                 */
                
                assertEquals("#entries", expected.size(), btree.size());
                
                Iterator<Map.Entry<Integer,SimpleEntry>> itr = expected.entrySet().iterator();
                
                while( itr.hasNext()) { 
                    
                    Map.Entry<Integer,SimpleEntry> entry = itr.next();
                    
                    assertEquals("lookup(" + entry.getKey() + ")", entry
                            .getValue(), btree
                            .lookup(entry.getKey().intValue()));
                    
                }
                
            }
            
        }

    }

    /*
     * Test structure modification.
     */

    /**
     * Test causes the root leaf to be split such that the insert key goes into
     * the first position of the low leaf (the pre-existing roof leaf).
     */
    public void test_splitRootLeaf_low01() {

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
     * Test causes the root leaf to be split such that the insert key goes into
     * the middle of the low leaf (the pre-existing roof leaf).
     */
    public void test_splitRootLeaf_low02() {

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
     * Test causes the root leaf to be split such that the insert key goes into
     * the end of the low leaf (the pre-existing roof leaf).
     */
    public void test_splitRootLeaf_low03() {

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
         * Split low.  The key will be inserted at the end of the low leaf.
         */

        int splitKey = 15;

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
        assertEquals(new int[]{1,11,15,0},leaf1.keys);

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
     * Test causes the root leaf to be split such that the insert key lies
     * within the key range of the high leaf and therefore the insert key goes
     * into the middle of the high leaf (the leaf created by the split leaf).
     */
    public void test_splitRootLeaf_high01() {

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

        assertEquals(2,leaf1.nkeys);
        assertEquals(new int[]{1,11,0,0},leaf1.keys);

        assertEquals(3,leaf2.nkeys);
        assertEquals(new int[]{21,22,31,0},leaf2.keys);

        assertSameIterator(new SimpleEntry[] { entries[0], entries[1] }, leaf1
                .entryIterator());

        assertSameIterator(new SimpleEntry[] { entries[2], splitEntry, entries[3] },
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
     * Test causes the root leaf to be split such that the insert key is larger
     * than any existing key and is therefore appended to the keys in the high
     * leaf (the leaf created by the split leaf)
     */
    public void test_splitRootLeaf_high02() {

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

        assertEquals(2,leaf1.nkeys);
        assertEquals(new int[]{1,11,0,0},leaf1.keys);

        assertEquals(3,leaf2.nkeys);
        assertEquals(new int[]{21,31,40,0},leaf2.keys);

        assertSameIterator(new SimpleEntry[] { entries[0], entries[1] }, leaf1
                .entryIterator());

        assertSameIterator(new SimpleEntry[] { entries[2], entries[3], splitEntry },
                leaf2.entryIterator());

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
     * <p>
     * Test ability to split the root leaf. A Btree is created with a known
     * capacity. The root leaf is filled to capacity and then split. The keys
     * are choosen so as to create room for an insert into the left and right
     * leaves after the split. The state of the root leaf before the split is:
     * </p>
     * 
     * <pre>
     *   root keys : [ 1 11 21 31 ]
     * </pre>
     * 
     * <p>
     * The root leaf is split by inserting the external key <code>15</code>.
     * The state of the tree after the split is:
     * </p>
     * 
     * <pre>
     *   m     = 4 (branching factor)
     *   m/2   = 2 (index of first key moved to the new leaf)
     *   m/2-1 = 1 (index of last key retained in the old leaf).
     *              
     *   root  keys : [ 21 ]
     *   leaf1 keys : [  1 11 15  - ]
     *   leaf2 keys : [ 21 31  -  - ]
     * </pre>
     * 
     * <p>
     * The test then inserts <code>2</code> (goes into leaf1, filling it to
     * capacity), <code>22</code> (goes into leaf2, testing the edge condition
     * for inserting the key greater than the split key), and <code>24</code>
     * (goes into leaf2, filling it to capacity). At this point the tree looks
     * like this:
     * </p>
     * 
     * <pre>
     *   root  keys : [ 21 ]
     *   leaf1 keys : [  1  2 11 15 ]
     *   leaf2 keys : [ 21 22 24 31 ]
     * </pre>
     * 
     * <p>
     * The test now inserts <code>7</code>, causing leaf1 into split (note
     * that the leaves are named by their creation order, not their traveral
     * order):
     * </p>
     * 
     * <pre>
     *   root  keys : [ 11 21 ]
     *   leaf1 keys : [  1  2  7  - ]
     *   leaf3 keys : [ 11 15  -  - ]
     *   leaf2 keys : [ 21 22 24 31 ]
     * </pre>
     * 
     * <p>
     * The test now inserts <code>23</code>, causing leaf2 into split:
     * </p>
     * 
     * <pre>
     *   root  keys : [ 11 21 24 ]
     *   leaf1 keys : [  1  2  7  - ]
     *   leaf3 keys : [ 11 15  -  - ]
     *   leaf2 keys : [ 21 22 23  - ]
     *   leaf4 keys : [ 24 31  -  - ]
     * </pre>
     * 
     * <p>
     * At this point the root node is at capacity and another split of a leaf
     * will cause the root node to split and increase the height of the btree.
     * To prepare for this, we insert <4> (into leaf1), <code>17</code> and
     * <code>18</code> (into leaf3), and <code>35</code> and <code>40</code>
     * (into leaf4). This gives us the following scenario.
     * </p>
     * 
     * <pre>
     *   root  keys : [ 11 21 24 ]
     *   leaf1 keys : [  1  2  4  7 ]
     *   leaf3 keys : [ 11 15 17 18 ]
     *   leaf2 keys : [ 21 22 23  - ]
     *   leaf4 keys : [ 24 31 35 40 ]
     * </pre>
     * 
     * <p>
     * Note that leaf2 has a hole that can not be filled by an insert since
     * the key <code>24</code> is already in leaf4.
     * </p>
     * <p>
     * Now we insert <code>50</code> into leaf4, forcing leaf4 to split, which
     * in turn requires the root node to split. The result is as follows (note
     * that the old root is now named 'node1' and 'node2' is the new non-root
     * node created by the split of the old root node).  The split is not made
     * until the insert reaches the leaf, discovers that the key is not already
     * present, and then discovers that the leaf is full.  The key does into
     * the new leaf, leaf5.
     * </p> 
     * <pre>
     *   root  keys : [ 21  -  - ]
     *   node1 keys : [ 11  -  - ]
     *   node2 keys : [ 24 35  - ]
     *   leaf1 keys : [  1  2  4  7 ]
     *   leaf3 keys : [ 11 15 17 18 ]
     *   leaf2 keys : [ 21 22 23  - ]
     *   leaf4 keys : [ 24 31  -  - ]
     *   leaf5 keys : [ 35 40 50  - ]
     * </pre>
     * 
     * @todo Force at least one other leaf to split and verify the outcome.
     *       Ideally, carry through the example until we can force the root
     *       to split one more time.  Note that this detailed test requires
     *       a specific split rule.
     */
    public void test_splitRootLeaf01_even() {

        final int m = 4;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        int[] keys = new int[]{1,11,21,31};
        
        for( int i=0; i<m; i++ ) {
         
            int key = keys[i];
            
            SimpleEntry entry = new SimpleEntry();
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);
            
            assertEquals(entry,btree.lookup(key));
            
        }

        // Verify leaf is full.
        assertEquals( m, leaf1.nkeys );
        
        // Verify keys.
        assertEquals( keys, leaf1.keys );
        
        // Verify root node has not been changed.
        assertEquals( leaf1, btree.getRoot() );

        /*
         * split the root leaf.
         */

        // Insert [key := 15] goes into leaf1 (forces split).
        System.err.print("leaf1 before split : "); leaf1.dump(System.err);
        SimpleEntry splitEntry = new SimpleEntry();
        assertNull(btree.lookup(15));
        btree.insert(15,splitEntry);
        System.err.print("leaf1 after split : "); leaf1.dump(System.err);
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,11,15,0},leaf1.keys);
        assertEquals(splitEntry,btree.lookup(15));

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

        /*
         * Verify things about the new root node.
         */
        assertTrue( btree.getRoot() instanceof Node );
        Node root = (Node) btree.getRoot();
        System.err.print("root after split : "); root.dump(System.err);
        assertEquals("root.nkeys",1,root.nkeys);
        assertEquals("root.keys",new int[]{21,0,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertNotNull(root.getChild(1));
        assertNotSame(leaf1,root.getChild(1));

        /*
         * Verify things about the new leaf node, which we need to access
         * from the new root node.
         */
        Leaf leaf2 = (Leaf)root.getChild(1);
        System.err.print("leaf2 after split : "); leaf2.dump(System.err);
        assertEquals("leaf2.nkeys",m/2,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,31,0,0},leaf2.keys);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());

        // Insert [key := 2] goes into leaf1, filling it to capacity.
        btree.insert(2,new SimpleEntry());
        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,11,15},leaf1.keys);

        // Insert [key := 22] goes into leaf2 (tests edge condition)
        btree.insert(22,new SimpleEntry());
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,31,0},leaf2.keys);

        // Insert [key := 24] goes into leaf2, filling it to capacity.
        btree.insert(24,new SimpleEntry());
        assertEquals("leaf2.nkeys",4,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,24,31},leaf2.keys);

//        System.err.print("root  final : "); root.dump(System.err);
//        System.err.print("leaf1 final : "); leaf1.dump(System.err);
//        System.err.print("leaf2 final : "); leaf2.dump(System.err);
        
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 8, btree.nentries);

        /*
         * Insert into leaf1, causing it to split. The split will cause a new
         * child to be added to the root. Verify the post-conditions.
         */
        
        // Insert [key := 7] goes into leaf1, forcing split.
        assertEquals("leaf1.nkeys",m,leaf1.nkeys);
        System.err.print("root  before split: ");root.dump(System.err);
        System.err.print("leaf1 before split: ");leaf1.dump(System.err);
        btree.insert(7,new SimpleEntry());
        System.err.print("root  after split: ");root.dump(System.err);
        System.err.print("leaf1 after split: ");leaf1.dump(System.err);
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,7,0},leaf1.keys);

        assertEquals("root.nkeys",2,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf2,root.getChild(2));

        Leaf leaf3 = (Leaf)root.getChild(1);
        assertNotNull( leaf3 );
        assertEquals("leaf3.nkeys",2,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,0,0},leaf3.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 3, btree.nleaves);
        assertEquals("#entries", 9, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, root },
                root.postOrderIterator());

        /*
         * Insert into leaf2, causing it to split. The split will cause a new
         * child to be added to the root. At this point the root node is at
         * capacity.  Verify the post-conditions.
         */
        
        // Insert [key := 23] goes into leaf2, forcing split.
        assertEquals("leaf2.nkeys",m,leaf2.nkeys);
        System.err.print("root  before split: ");root.dump(System.err);
        System.err.print("leaf2 before split: ");leaf2.dump(System.err);
        btree.insert(23,new SimpleEntry());
        System.err.print("root  after split: ");root.dump(System.err);
        System.err.print("leaf2 after split: ");leaf2.dump(System.err);
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,23,0},leaf2.keys);

        assertEquals("root.nkeys",3,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,24},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf3,root.getChild(1));
        assertEquals(leaf2,root.getChild(2));

        Leaf leaf4 = (Leaf)root.getChild(3);
        assertNotNull( leaf4 );
        assertEquals("leaf4.nkeys",2,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,0,0},leaf4.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 4, btree.nleaves);
        assertEquals("#entries", 10, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, leaf4,
                root }, root.postOrderIterator());

        /*
         * At this point the root node is at capacity and another split of a
         * leaf will cause the root node to split and increase the height of the
         * btree. We prepare for that scenario now by filling up a few of the
         * leaves to capacity.
         */
        
        // Save a reference to the root node before the split.
        Node node1 = (Node) btree.root;
        assertEquals(root,node1);

        // Insert [key := 4] into leaf1, filling it to capacity.
        btree.insert(4,new SimpleEntry());
        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,4,7},leaf1.keys);

        // Insert [key := 17] into leaf3.
        btree.insert(17,new SimpleEntry());
        assertEquals("leaf3.nkeys",3,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,17,0},leaf3.keys);

        // Insert [key := 18] into leaf3, filling it to capacity.
        btree.insert(18,new SimpleEntry());
        assertEquals("leaf3.nkeys",4,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,17,18},leaf3.keys);

        // Insert [key := 35] into leaf4.
        btree.insert(35,new SimpleEntry());
        assertEquals("leaf4.nkeys",3,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,35,0},leaf4.keys);

        // Insert [key := 40] into leaf4, filling it to capacity.
        btree.insert(40,new SimpleEntry());
        assertEquals("leaf4.nkeys",4,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,35,40},leaf4.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 4, btree.nleaves);
        assertEquals("#entries", 15, btree.nentries);
        
        /*
         * verify iterator (no change).
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, leaf4,
                root }, root.postOrderIterator());

        /*
         * Force leaf4 to split.
         */
        
        System.err.print("Tree pre-split");
        assertTrue(btree.dump(System.err));
        
        // Insert [key := 50] into leaf4, forcing it to split.  The insert
        // goes into the _new_ leaf.
        leaf4.dump(System.err);
        leaf4.getParent().dump(System.err);
        btree.insert(50,new SimpleEntry());
        leaf4.dump(System.err);
        leaf4.getParent().dump(System.err);

        /*
         * verify keys the entire tree, starting at the new root.
         */

        System.err.print("Tree post-split");
        assertTrue(btree.dump(System.err));
        
        assertNotSame(root,btree.root); // verify new root.
        root = (Node)btree.root;
        assertEquals("root.nkeys",1,root.nkeys);
        assertEquals("root.keys",new int[]{21,0,0},root.keys);
        assertEquals(node1,root.getChild(0));
        assertNotNull(root.getChild(1));
        Node node2 = (Node)root.getChild(1);
        
        assertEquals("node1.nkeys",1,node1.nkeys);
        assertEquals("node1.keys",new int[]{11,0,0},node1.keys);
        assertEquals(root,node1.getParent());
        assertEquals(leaf1,node1.getChild(0));
        assertEquals(leaf3,node1.getChild(1));

        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,4,7},leaf1.keys);
        assertEquals(node1,leaf1.getParent());

        assertEquals("leaf3.nkeys",4,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,17,18},leaf3.keys);
        assertEquals(node1,leaf3.getParent());
        
        assertEquals("node2.nkeys",2,node2.nkeys);
        assertEquals("node2.keys",new int[]{24,35,0},node2.keys);
        assertEquals(root,node2.getParent());
        assertEquals(leaf2,node2.getChild(0));
        assertEquals(leaf4,node2.getChild(1));
        assertNotNull(node2.getChild(2));
        Leaf leaf5 = (Leaf)node2.getChild(2);

        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,23,0},leaf2.keys);
        assertEquals(node2,leaf2.getParent());

        assertEquals("leaf4.nkeys",2,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,0,0},leaf4.keys);
        assertEquals(node2,leaf4.getParent());
        
        assertEquals("leaf5.nkeys",3,leaf5.nkeys);
        assertEquals("leaf5.keys",new int[]{35,40,50,0},leaf5.keys);
        assertEquals(node2,leaf5.getParent());

        assertEquals("height", 2, btree.height);
        assertEquals("#nodes", 3, btree.nnodes);
        assertEquals("#leaves", 5, btree.nleaves);
        assertEquals("#entries", 16, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, node1, leaf2, 
                leaf4, leaf5, node2, root }, root.postOrderIterator());
        
    }
    
    /**
     * A test of {@link Node#findChild(int key)}.
     */
    public void test_node_findChild() {
     
        int m = 4;
        
        BTree btree = getBTree(m);

        /*
         * Create a test node.  We do not both to build this up from scratch
         * by inserting keys into the tree.
         */
        //  keys[]  : [ 5 9 12 ]
        //  child[] : [ a b  c  d ]

        Node node = new Node(btree, 1, m, 3, new int[] { 5, 9, 12 },
                new long[] { 1, 2, 3, 4 });
        
        assertEquals(0,node.findChild(1));
        assertEquals(0,node.findChild(2));
        assertEquals(0,node.findChild(3));
        assertEquals(0,node.findChild(4));
        assertEquals(1,node.findChild(5));
        assertEquals(1,node.findChild(6));
        assertEquals(1,node.findChild(7));
        assertEquals(1,node.findChild(8));
        assertEquals(2,node.findChild(9));
        assertEquals(2,node.findChild(10));
        assertEquals(2,node.findChild(11));
        assertEquals(3,node.findChild(12));
        assertEquals(3,node.findChild(13));
        
    }
    
    /**
     * <p>
     * Test ability to remove keys and to remove child leafs as they become
     * empty. A Btree is created with a known capacity. The root leaf is filled
     * to capacity and then split. The keys are choosen so as to create room for
     * an insert into the left and right leaves after the split. The state of
     * the root leaf before the split is:
     * </p>
     * 
     * <pre>
     *    root keys : [ 1 11 21 31 ]
     * </pre>
     * 
     * <p>
     * The root leaf is split by inserting the external key <code>15</code>.
     * The state of the tree after the split is:
     * </p>
     * 
     * <pre>
     *    m     = 4 (branching factor)
     *    m/2   = 2 (index of first key moved to the new leaf)
     *    m/2-1 = 1 (index of last key retained in the old leaf).
     *               
     *    root  keys : [ 21 ]
     *    leaf1 keys : [  1 11 15  - ]
     *    leaf2 keys : [ 21 31  -  - ]
     * </pre>
     * 
     * <p>
     * The test then inserts <code>2</code> (goes into leaf1, filling it to
     * capacity), <code>22</code> (goes into leaf2, testing the edge condition
     * for inserting the key greater than the split key), and <code>24</code>
     * (goes into leaf2, filling it to capacity). At this point the tree looks
     * like this:
     * </p>
     * 
     * <pre>
     *    root  keys : [ 21 ]
     *    leaf1 keys : [  1  2 11 15 ]
     *    leaf2 keys : [ 21 22 24 31 ]
     * </pre>
     * 
     * <p>
     * The test now inserts <code>7</code>, causing leaf1 into split (note
     * that the leaves are named by their creation order, not their traveral
     * order):
     * </p>
     * 
     * <pre>
     *    root  keys : [ 11 21 ]
     *    leaf1 keys : [  1  2  7  - ]
     *    leaf3 keys : [ 11 15  -  - ]
     *    leaf2 keys : [ 21 22 24 31 ]
     * </pre>
     * 
     * <p>
     * The test now inserts <code>23</code>, causing leaf2 into split:
     * </p>
     * 
     * <pre>
     *    root  keys : [ 11 21 24 ]
     *    leaf1 keys : [  1  2  7  - ]
     *    leaf3 keys : [ 11 15  -  - ]
     *    leaf2 keys : [ 21 22 23  - ]
     *    leaf4 keys : [ 24 31  -  - ]
     * </pre>
     * 
     * <p>
     * At this point the root node is at capacity and we are ready to begin
     * deleting keys. We begin by deleting <code>11</code> and then
     * <code>15</code>. This reduces leaf3 to zero keys, at which point the
     * leaf should be removed from the root node and deallocated on the store:
     * </p>
     * 
     * <pre>
     *    root  keys : [ 21 24 0 ]
     *    leaf1 keys : [  1  2  7  - ]
     *    leaf2 keys : [ 21 22 23  - ]
     *    leaf4 keys : [ 24 31  -  - ]
     * </pre>
     */
    public void test_removeStructure01() {

        final int m = 4;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        /*
         * generate keys and values.
         */
        
        // keys
        int[] keys = new int[]{1,11,21,31,15,2,22,24,7,23};

        // values
        SimpleEntry[] vals = new SimpleEntry[keys.length];

        // key value mapping used once we begin to delete keys.
        Map<Integer,SimpleEntry> map = new TreeMap<Integer,SimpleEntry>();

        // generate values and populate the key-value mapping.
        for( int i=0; i<vals.length; i++ ) {
         
            vals[i] = new SimpleEntry();
            
            map.put(keys[i], vals[i]);
            
        }

        /*
         * Populate the root leaf.
         */
        int n = 0;
        btree.insert(keys[n], vals[n]); n++;
        btree.insert(keys[n], vals[n]); n++;
        btree.insert(keys[n], vals[n]); n++;
        btree.insert(keys[n], vals[n]); n++;

        // Verify leaf is full.
        assertEquals( m, leaf1.nkeys );
        
        // Verify keys.
        assertEquals( new int[]{1,11,21,31}, leaf1.keys );
        
        // Verify root node has not been changed.
        assertEquals( leaf1, btree.getRoot() );

        /*
         * split the root leaf.
         */

        // Insert [key := 15] goes into leaf1 (forces split).
        System.err.print("leaf1 before split : "); leaf1.dump(System.err);
        SimpleEntry splitEntry = vals[n];
        assertNull(btree.lookup(15));
        assert keys[n] == 15;
        btree.insert(keys[n],splitEntry); n++;
        System.err.print("leaf1 after split : "); leaf1.dump(System.err);
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,11,15,0},leaf1.keys);
        assertEquals(splitEntry,btree.lookup(15));

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

        /*
         * Verify things about the new root node.
         */
        assertTrue( btree.getRoot() instanceof Node );
        Node root = (Node) btree.getRoot();
        System.err.print("root after split : "); root.dump(System.err);
        assertEquals("root.nkeys",1,root.nkeys);
        assertEquals("root.keys",new int[]{21,0,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertNotNull(root.getChild(1));
        assertNotSame(leaf1,root.getChild(1));

        /*
         * Verify things about the new leaf node, which we need to access
         * from the new root node.
         */
        Leaf leaf2 = (Leaf)root.getChild(1);
        System.err.print("leaf2 after split : "); leaf2.dump(System.err);
        assertEquals("leaf2.nkeys",m/2,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,31,0,0},leaf2.keys);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
    
        // Insert [key := 2] goes into leaf1, filling it to capacity.
        assert keys[n] == 2;
        btree.insert(keys[n],vals[n]); n++;
        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,11,15},leaf1.keys);

        // Insert [key := 22] goes into leaf2 (tests edge condition)
        assert keys[n] == 22;
        btree.insert(keys[n],vals[n]); n++;
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,31,0},leaf2.keys);

        // Insert [key := 24] goes into leaf2, filling it to capacity.
        assert keys[n] == 24;
        btree.insert(keys[n],vals[n]); n++;
        assertEquals("leaf2.nkeys",4,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,24,31},leaf2.keys);

//        System.err.print("root  final : "); root.dump(System.err);
//        System.err.print("leaf1 final : "); leaf1.dump(System.err);
//        System.err.print("leaf2 final : "); leaf2.dump(System.err);
        
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 8, btree.nentries);

        /*
         * Insert into leaf1, causing it to split. The split will cause a new
         * child to be added to the root. Verify the post-conditions.
         */
        
        // Insert [key := 7] goes into leaf1, forcing split.
        assertEquals("leaf1.nkeys",m,leaf1.nkeys);
        System.err.print("root  before split: ");root.dump(System.err);
        System.err.print("leaf1 before split: ");leaf1.dump(System.err);
        assert keys[n] == 7;
        btree.insert(keys[n],vals[n]); n++;
        System.err.print("root  after split: ");root.dump(System.err);
        System.err.print("leaf1 after split: ");leaf1.dump(System.err);
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,7,0},leaf1.keys);

        assertEquals("root.nkeys",2,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf2,root.getChild(2));

        Leaf leaf3 = (Leaf)root.getChild(1);
        assertNotNull( leaf3 );
        assertEquals("leaf3.nkeys",2,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,0,0},leaf3.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 3, btree.nleaves);
        assertEquals("#entries", 9, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, root },
                root.postOrderIterator());

        /*
         * Insert into leaf2, causing it to split. The split will cause a new
         * child to be added to the root. At this point the root node is at
         * capacity.  Verify the post-conditions.
         */
        
        // Insert [key := 23] goes into leaf2, forcing split.
        assertEquals("leaf2.nkeys",m,leaf2.nkeys);
        System.err.print("root  before split: ");root.dump(System.err);
        System.err.print("leaf2 before split: ");leaf2.dump(System.err);
        assert keys[n] == 23;
        btree.insert(keys[n],vals[n]); n++;
        System.err.print("root  after split: ");root.dump(System.err);
        System.err.print("leaf2 after split: ");leaf2.dump(System.err);
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,23,0},leaf2.keys);

        assertEquals("root.nkeys",3,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,24},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf3,root.getChild(1));
        assertEquals(leaf2,root.getChild(2));

        Leaf leaf4 = (Leaf)root.getChild(3);
        assertNotNull( leaf4 );
        assertEquals("leaf4.nkeys",2,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,0,0},leaf4.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 4, btree.nleaves);
        assertEquals("#entries", 10, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, leaf4,
                root }, root.postOrderIterator());

        assertTrue(btree.dump(Level.DEBUG,System.out));

        /*
         * Validate the keys and entries.
         */
        {

            assertEquals("#entries", keys.length, btree.size());

            for (int i = 0; i < keys.length; i++) {

                int key = keys[i];

                SimpleEntry val = vals[i];

                assertEquals("lookup(" + key + ")", val,
                        btree.lookup(key));

            }

        }

        /*
         * Begin deleting keys:
         * 
         * before:         v-- remove @ index = 1
         *       index:    0  1  2
         * root  keys : [ 11 21 24 ]
         *                              index
         * leaf1 keys : [  1  2  7  - ]   0
         * leaf3 keys : [ 11 15  -  - ]   1 <--remove @ index = 1
         * leaf2 keys : [ 21 22 23  - ]   2
         * leaf4 keys : [ 24 31  -  - ]   3
         * 
         * This can also be represented as
         * 
         * ( leaf1, 11, leaf3, 21, leaf2, 24, leaf4 )
         * 
         * and we remove the sequence ( leaf3, 21 ) leaving a well-formed node.
         * 
         * removeChild(leaf3) - occurs when we delete key 15 and then key 11.
         * nkeys := 3
         * nchildren := nkeys(3) + 1 = 4
         * index := 1
         * lengthChildCopy = nchildren(4) - index(1) - 1 = 2;
         * lengthKeyCopy = lengthChildCopy - 1 = 1;
         * copyChildren from index+1(2) to index(1) lengthChildCopy(2)
         * copyKeys from index+1(2) to index(1) lengthKeyCopy(1)
         * erase keys[ nkeys - 1 = 2 ]
         * erase children[ nkeys = 3 ]
         * 
         * after:
         *       index:    0  1  2
         * root  keys : [ 11 24  0 ]
         *                              index 
         * leaf1 keys : [  1  2  7  - ]   0
         * leaf2 keys : [ 21 22 23  - ]   1
         * leaf4 keys : [ 24 31  -  - ]   2
         */

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 4, btree.nleaves);
        assertEquals("#entries", 10, btree.nentries);
        assertTrue(btree.dump(System.out));

        // validate pre-conditions for the root.
        assertEquals("root.nkeys",3,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,24},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf3,root.getChild(1));
        assertEquals(leaf2,root.getChild(2));
        assertEquals(leaf4,root.getChild(3));

        // verify the pre-condition for leaf3.
        assertEquals("leaf3.nkeys",2,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,0,0},leaf3.keys);
        
        // delete 15
        assertEquals(map.get(15),btree.remove(15));
        assertEquals("leaf3.nkeys",1,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,0,0,0},leaf3.keys);
        
        // delete 11, causes leaf3 to be removed from the root node and deleted.
        assertEquals(map.get(11),btree.remove(11));
        assertEquals("leaf3.nkeys",0,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{0,0,0,0},leaf3.keys);
        assertTrue("leaf3.isDeleted", leaf3.isDeleted());
        // verify gone from the root node. 
        assertEquals("root.nkeys",2,root.nkeys);
        assertEquals("root.keys",new int[]{11,24,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf2,root.getChild(1));
        assertEquals(leaf4,root.getChild(2));
        assertNull(root.childRefs[3]);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 3, btree.nleaves);
        assertEquals("#entries", 8, btree.nentries);
        assertTrue(btree.dump(System.out));

        /*
         * delete keys 1, 2, and 7 causing leaf1 to be deleted and the key 21
         * to be removed from the root node.
         * 
         * before: 
         * root  keys : [ 11 24  0 ]
         * leaf1 keys : [  1  2  7  - ]
         * leaf2 keys : [ 21 22 23  - ]
         * leaf4 keys : [ 24 31  -  - ]
         *    
         * after:
         * root  keys : [ 24  0  0]
         * leaf2 keys : [ 21 22 23  - ]
         * leaf4 keys : [ 24 31  -  - ]
         */
        // verify leaf1 preconditions.
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,7,0},leaf1.keys);
        // delete 1 and verify postconditions.
        assertEquals(map.get(1),btree.remove(1));
        assertEquals("leaf1.nkeys",2,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{2,7,0,0},leaf1.keys);
        // delete 2 and verify postconditions.
        assertEquals(map.get(2),btree.remove(2));
        assertEquals("leaf1.nkeys",1,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{7,0,0,0},leaf1.keys);
        // delete 7 and verify postconditions (this causes leaf1 to be removed)
        assertEquals(map.get(7),btree.remove(7));
        assertEquals("leaf1.nkeys",0,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{0,0,0,0},leaf1.keys);
        assertTrue("leaf1.isDeleted", leaf1.isDeleted());
        // verify gone from the root node. 
        assertEquals("root.nkeys",1,root.nkeys);
        assertEquals("root.keys",new int[]{24,0,0},root.keys);
        assertEquals(leaf2,root.getChild(0));
        assertEquals(leaf4,root.getChild(1));
        assertNull(root.childRefs[2]);
        assertNull(root.childRefs[3]);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);
        assertTrue(btree.dump(System.out));

        /*
         * Delete keys 31, and 24 causing leaf4 to be deleted and the key 24 to
         * be removed from the root node. At this point we have only a single
         * child leaf in the root node. During a split a new node will
         * temporarily have no keys and only a single child, but we do not
         * permit that state to persist in the tree. Therefore we replace the
         * root node with leaf2 and deallocate the root node. leaf2 now becomes
         * the new root leaf of the tree.
         * 
         * before: 
         * root  keys : [ 24  0  0]
         * leaf2 keys : [ 21 22 23  - ]
         * leaf4 keys : [ 24 31  -  - ]
         * 
         * intermediate state before the root node is replaced by leaf2:
         * root  keys : [  0  0  0]
         * leaf2 keys : [ 21 22 23  - ]
         * 
         * after: leaf2 is now the root leaf.
         * leaf2 keys : [ 21 22 23  - ]
         */
        // verify leaf4 preconditions.
        assertEquals("leaf4.nkeys",2,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,0,0},leaf4.keys);
        // delete 24 and verify postconditions.
        assertEquals(map.get(24),btree.remove(24));
        assertEquals("leaf4.nkeys",1,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{31,0,0,0},leaf4.keys);
        // delete 31 and verify postconditions.
        assertEquals(map.get(31),btree.remove(31));
        assertEquals("leaf4.nkeys",0,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{0,0,0,0},leaf4.keys);
        assertTrue("leaf4.isDeleted", leaf4.isDeleted());
        // verify gone from the root node. 
        assertEquals("root.nkeys",0,root.nkeys);
        assertEquals("root.keys",new int[]{0,0,0},root.keys);
        assertEquals(leaf2,root.getChild(0)); // @todo is this reference cleared?
        assertNull(root.childRefs[1]);
        assertNull(root.childRefs[2]);
        assertNull(root.childRefs[3]);
        assertTrue(root.isDeleted());
        assertEquals(leaf2,btree.root);
        assertFalse(leaf2.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 3, btree.nentries);
        assertTrue(btree.dump(System.out));
        
        /*
         * Delete all keys remaining in the root leaf and verify that the root
         * leaf is allowed to become empty.
         * 
         * before:
         * leaf2 keys : [ 21 22 23  - ]
         * 
         * after:
         * leaf2 keys : [  -  -  -  - ]
         */

        // preconditions.
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,23,0},leaf2.keys);
        // delete 21 and verify postconditions.
        assertEquals(map.get(21),btree.remove(21));
        assertEquals("leaf2.nkeys",2,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{22,23,0,0},leaf2.keys);
        // delete 23 and verify postconditions.
        assertEquals(map.get(23),btree.remove(23));
        assertEquals("leaf2.nkeys",1,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{22,0,0,0},leaf2.keys);
        // delete 22 and verify postconditions.
        assertEquals(map.get(22),btree.remove(22));
        assertEquals("leaf2.nkeys",0,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{0,0,0,0},leaf2.keys);
        assertFalse(leaf2.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        assertTrue(btree.dump(System.out));
        
    }
    
    /**
     * Stress test for building up a tree and then removing all keys in a random
     * order.
     */
    public void test_stress_removeStructure() {
       
        // FIXME Try with nkeys=100 when debugged.
        int nkeys = 20;
        
        // FIXME try with m == 3 and lookout for fence posts!
//        doRemoveStructureStressTest(3,nkeys);

        doRemoveStructureStressTest(4,nkeys);

        doRemoveStructureStressTest(5,nkeys);

    }
    
    /**
     * Stress test of insert, removal and lookup of keys in the tree (allows
     * splitting of the root leaf).
     * 
     * Note: The #of inserts is limited by the size of the leaf hard reference
     * queue since evictions are disabled for the tests in this file. We can not
     * know in advance how many touches will result and when leaf evictions will
     * begin, so ntrials is set heuristically.
     */
    public void test_insertLookupRemoveKeyTreeStressTest() {

        int ntrials = 1000;
        
        // FIXME This is hitting fenceposts at m == 3.
//        doInsertLookupRemoveStressTest(3, 1000, ntrials);
        
        doInsertLookupRemoveStressTest(4, 1000, ntrials);

        doInsertLookupRemoveStressTest(5, 1000, ntrials);

        doInsertLookupRemoveStressTest(16, 10000, ntrials);

    }
    
    /**
     * Note: This error was actually a fence post in
     * {@link Node#dump(java.io.PrintStream, int, boolean))}. That method was
     * incorrectly reporting an error when nkeys was zero after a split of a
     * node.
     */
    public void test_errorSequence001() {

        int m = 3;
        
        int[] order = new int[] { 0, 1, 6, 3, 7, 2, 4, 5, 8 };

        doKnownKeySequenceTest( m, order, 3 );
        
    }
    
    /**
     * A stress test for sequential key insertion that runs with a variety of
     * branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_increasingKeySequence() {

        int[] branchingFactors = new int[]{3,4,5};// 6,7,8,20,55,79,256,512,1024,4097};
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doSplitWithIncreasingKeySequence( getBTree(m), m, m );
            
            doSplitWithIncreasingKeySequence( getBTree(m), m, m*m );

            doSplitWithIncreasingKeySequence( getBTree(m), m, m*m*m );

            doSplitWithIncreasingKeySequence( getBTree(m), m, m*m*m*m );

        }
        
    }

    /**
     * A stress test for sequential decreasing key insertions that runs with a
     * variety of branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_decreasingKeySequence() {

        int[] branchingFactors = new int[]{3,4,5};// 6,7,8,20,55,79,256,512,1024,4097};
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doSplitWithDecreasingKeySequence( getBTree(m), m, m );
            
            doSplitWithDecreasingKeySequence( getBTree(m), m, m*m );

            doSplitWithDecreasingKeySequence( getBTree(m), m, m*m*m );

            doSplitWithDecreasingKeySequence( getBTree(m), m, m*m*m*m );

        }
        
    }

    /**
     * Stress test inserts random permutations of keys into btrees of order m
     * for several different btrees, #of keys to be inserted, and permutations
     * of keys.
     */
    public void test_stress_split() {

        doSplitTest( 3, 0 );
        
        doSplitTest( 4, 0 );
        
        doSplitTest( 5, 0 );
        
    }
    
    /**
     * A stress test for random key insertion using a that runs with a variety
     * of branching factors and #of keys to insert.
     */
    public void test_splitRootLeaf_randomKeySequence() {

        int[] branchingFactors = new int[]{3,4,5};// 6,7,8,20,55,79,256,512,1024,4097};
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doSplitWithRandomKeySequence( getBTree(m), m, m );
            
            doSplitWithRandomKeySequence( getBTree(m), m, m*m );

            doSplitWithRandomKeySequence( getBTree(m), m, m*m*m );

            doSplitWithRandomKeySequence( getBTree(m), m, m*m*m*m );

        }
        
    }

}
