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

package com.bigdata.objectIndex;

import java.util.Arrays;

import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * <p>
 * Test suite that seeks to develop a persistence capable B-Tree supporting
 * copy-on-write semantics. The nodes of the tree should be wired into memory
 * while the leaves of the tree should be written incrementally as they are
 * evicted from a hard reference queue. During a commit, a pre-order traversal
 * should write any dirty leaves to the store followed by their parents up to
 * the root of the tree.
 * </p>
 * <p>
 * Note: The index does not support traversal with concurrent modification of
 * its structure (adding or removing keys, nodes, or leaves).
 * </p>
 * <p>
 * A btree grows through splitting. We do not need to insert keys in order to
 * drive splits since we can simply demand a split at any time. Growth by
 * splitting is naturally produces tree structures in which the nodes have one
 * fewer keys than they have children. The test plan reflects this.
 * </p>
 * The basic test plan tests features of the btree semantics and isolates the
 * correct tests of those features as far as is possible from the specifics of
 * the persistence mechanism (copy-on-write, incremental write of leaves, and
 * commit protocol for the object index).
 * <ul>
 * 
 * <li>tests operations on the root leaf, including leaf.insert(key,value),
 * leaf.lookup(key), and leaf.values() </li>
 * 
 * <li> tests the ability to split the root leaf. leaf.split(key) with root
 * produces node with two leaves. more splits produces more leaves. continue to
 * split until the root node would be split again. As part of this, tests the
 * ability find leaves that are direct children of the root node using
 * node.findChild(int key)</li>
 * 
 * <li>tests node.insert(key,value), node.lookup(key), node.values() with
 * simple structure (root and two leaves).</li>
 * 
 * <li>tests node.insert(key,value) driving additional leaf splits producing a
 * single root with branchingFactor leaves (perhaps this can be tested with just
 * split(key) rather than insert(key,value). The next split would cause the root
 * to split, producing deeper structure.</li>
 * 
 * <li>tests node.split() for a root {@link Node}, which produces deeper
 * structure, and tests node.findChild(key) again with deeper structure. These
 * tests need to generate a structure having at least a root {@link Node},
 * child {@link Node}s, and {@link Leaf} nodes under those children in order to
 * test the various code paths. The leaf hard reference queue should have enough
 * capacity for these tests to ensure that incremental writes on leaves are NOT
 * perform, thereby isolating the btree features under test from the incremental
 * write and commit protocols.</li>
 * 
 * <li>test post-order visitation with the root leaf (visits self), with a tree
 * having a root {@link Node} and child leaves (visits leaf1 ... leafn and then
 * the root), and with deeper trees.</li>
 * 
 * <li>test value visitation with the root leaf (visits values on the root
 * leaf), with a tree having a root {@link Node} and child leaves (visits values
 * on leaf1 ... leafn), and with deeper trees.</li>
 * 
 * </ul>
 * 
 * A second test plan focuses the persistence mechanism, including the
 * copy-on-write, incremental write of leaves, and commit protocol.
 * <ul>
 * 
 * <li> Test commit logic with progression of btrees having increasing
 * structure. </li>
 * <li> Test correct triggering of the copy-on-write mechanism</li>
 * <li> Test correctness of the post-order traversal for dirty nodes (clean
 * nodes are not visited).</li>
 * <li> Test stealing of clean children when cloning {@link Node}s and
 * invalidation of cloned nodes and their ancestors</li>
 * 
 * </ul>
 * 
 * A third test plan focuses on stress tests for the tree that rely on both
 * correct functioning of the btree semantics and correct functionion of the
 * persistence mechanisms.
 * 
 * @todo Implement logic to steal immutable children when cloning their parent
 *       and discard unreachable nodes from the hard reference node cache (the
 *       cloned node and all ancestors of the cloned node). We can also mark
 *       those discarded nodes as "invalid" to eagerly detect inadvertant access
 *       (or develop the idea to wrap the node state with a flyweight object
 *       holding its parent, which makes discarding nodes somewhat more tricky -
 *       perhaps a hard reference queue rather than a Set?).
 * 
 * @todo test inserts leading to repeated splits of the root out to at least a
 *       depth of 5 (this is easier to do with a smaller branching factor).
 *       Periodically verify that the tree remains fully ordered. SimpleStore all of
 *       the keys and values in a hard reference Map and re-verify that the tree
 *       holds the correct mapping from time to time. Do periodic commits. Do
 *       tests where the leaf cache is small enough that we are doing
 *       incremental leaf evictions. Test with random deletes.
 * 
 * @todo Test delete, proving out a solution for recombining nodes as required.
 * 
 * @todo test tree under sequential insertion, nearly sequential insertion
 *       (based on a model identifier generation for the read-optimized
 *       database, including filling up pages, eventually releasing space on
 *       pages, and then reclaiming space where it becomes available on pages),
 *       and under random key generation. Does the tree stay nicely balanced for
 *       all scenarios? What about the average search time? Periodically verify
 *       that the tree remains fully ordered.  Choose a split rule that works
 *       well for mostly sequential and mostly monotonic keys with some removal
 *       of entries (sparsity).
 * 
 * @todo test delete of keys, including the rotations required to keep the tree
 *       balanced.
 * 
 * @todo support object index semantics for values. is there some natural way to
 *       share the code to support both standard btrees and an object index? If
 *       not, then bifurcate the code for that purpose.
 * 
 * @todo value iterators can scan key ranges, but we don't need that for an
 *       object index.
 * 
 * @todo implement B*Tree overflow operations or defer for a non-object index?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSimpleBTree extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestSimpleBTree() {
    }

    /**
     * @param arg0
     */
    public TestSimpleBTree(String arg0) {

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
    public void test_insertIntoLeaf01() {

        final int branchingFactor = 20;
        
        BTree btree = getNoEvictionBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();
        
        // array of inserted keys.
        int[] expectedKeys = new int[branchingFactor];
        
        int nkeys = 0;
        
        while( nkeys < branchingFactor ) {
            
            // Valid random key.
            int key = r.nextInt(Node.POSINF-1)+1;
            
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
            root.insert(key, index, new Entry() );
            
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
    public void test_insertIntoLeaf02() {

        final int branchingFactor = 20;
        
        BTree btree = getNoEvictionBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();

        // array of inserted keys.
        int[] expectedKeys = new int[branchingFactor];
        
        int nkeys = 0;
        
        while( nkeys < branchingFactor ) {
            
            // Valid random key.
            int key = r.nextInt(Node.POSINF-1)+1;

            int nkeysBefore = root.nkeys;
            
            boolean exists = root.lookup(key) != null;
            
            root.insert(key, new Entry() );

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
     * @see Leaf#insert(int, com.bigdata.objndx.TestSimpleBTree.Entry)
     * @see Leaf#lookup(int)
     * @see Leaf#entryIterator()
     */
    public void test_insertIntoLeaf03() {

        final int branchingFactor = 20;
        
        BTree btree = getNoEvictionBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();

        // array of keys to insert.
        int[] expectedKeys = new int[branchingFactor];

        // the value to insert for each key.
        Entry[] expectedValues = new Entry[branchingFactor];
        
        /*
         * Generate keys and values. The keys are a monotonic progression with
         * random non-zero intervals.
         */
        
        int lastKey = Node.NEGINF;
        
        for( int i=0; i<branchingFactor; i++ ) {
            
            int key = lastKey + r.nextInt(100) + 1;
            
            expectedKeys[ i ] = key;
            
            expectedValues[ i ] = new Entry();
            
            lastKey = key; 
            
        }
        
        for( int i=0; i<branchingFactor; i++ ) {

            int key = expectedKeys[i];
            
            Entry value = expectedValues[i];
            
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
            IObjectIndexEntry[] tmp = new IObjectIndexEntry[root.nkeys];
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

    /*
     * Test structure modification.
     */

    /**
     * Test causes the root leaf to be split such that the insert key goes into
     * the first position of the low leaf (the pre-existing roof leaf).
     */
    public void test_splitRootLeaf_low01() {

        final int m = 4;

        BTree btree = getNoEvictionBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{2,11,21,31};
        
        Entry[] entries = new Entry[]{new Entry(),new Entry(),new Entry(),new Entry()};
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            Entry entry = entries[i];
            
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

        Entry splitEntry = new Entry();

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

        assertSameIterator(new Entry[] { splitEntry, entries[0], entries[1] },
                leaf1.entryIterator());

        assertSameIterator(new Entry[] { entries[2], entries[3] }, leaf2
                .entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new Entry[] { splitEntry, entries[0], entries[1],
                entries[2], entries[3] }, root.entryIterator());

    }
    
    /**
     * Test causes the root leaf to be split such that the insert key goes into
     * the middle of the low leaf (the pre-existing roof leaf).
     */
    public void test_splitRootLeaf_low02() {

        final int m = 4;

        BTree btree = getNoEvictionBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{1,11,21,31};
        
        Entry[] entries = new Entry[]{new Entry(),new Entry(),new Entry(),new Entry()};
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            Entry entry = entries[i];
            
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

        Entry splitEntry = new Entry();

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

        assertSameIterator(new Entry[] { entries[0], splitEntry, entries[1] },
                leaf1.entryIterator());

        assertSameIterator(new Entry[] { entries[2], entries[3] }, leaf2
                .entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new Entry[] { entries[0], splitEntry, entries[1],
                entries[2], entries[3] }, root.entryIterator());

    }

    /**
     * Test causes the root leaf to be split such that the insert key goes into
     * the end of the low leaf (the pre-existing roof leaf).
     */
    public void test_splitRootLeaf_low03() {

        final int m = 4;

        BTree btree = getNoEvictionBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{1,11,21,31};
        
        Entry[] entries = new Entry[]{new Entry(),new Entry(),new Entry(),new Entry()};
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            Entry entry = entries[i];
            
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

        Entry splitEntry = new Entry();

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

        assertSameIterator(new Entry[] { entries[0], entries[1], splitEntry },
                leaf1.entryIterator());

        assertSameIterator(new Entry[] { entries[2], entries[3] }, leaf2
                .entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new Entry[] { entries[0], entries[1], splitEntry, 
                entries[2], entries[3] }, root.entryIterator());

    }
    
    /**
     * Test causes the root leaf to be split such that the insert key lies
     * within the key range of the high leaf and therefore the insert key goes
     * into the middle of the high leaf (the leaf created by the split leaf).
     */
    public void test_splitRootLeaf_high01() {

        final int m = 4;

        BTree btree = getNoEvictionBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{1,11,21,31};
        
        Entry[] entries = new Entry[]{new Entry(),new Entry(),new Entry(),new Entry()};
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            Entry entry = entries[i];
            
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

        Entry splitEntry = new Entry();

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

        assertSameIterator(new Entry[] { entries[0], entries[1] }, leaf1
                .entryIterator());

        assertSameIterator(new Entry[] { entries[2], splitEntry, entries[3] },
                leaf2.entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new Entry[] { entries[0], entries[1], entries[2],
                splitEntry, entries[3] }, root.entryIterator());

    }
    
    /**
     * Test causes the root leaf to be split such that the insert key is larger
     * than any existing key and is therefore appended to the keys in the high
     * leaf (the leaf created by the split leaf)
     */
    public void test_splitRootLeaf_high02() {

        final int m = 4;

        BTree btree = getNoEvictionBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        // The root before the split.
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        // The keys to insert before the split.
        int[] keys = new int[]{1,11,21,31};
        
        Entry[] entries = new Entry[]{new Entry(),new Entry(),new Entry(),new Entry()};
        
        for( int i=0; i<m; i++ ) {
        
            int key = keys[i];
            
            Entry entry = entries[i];
            
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

        Entry splitEntry = new Entry();

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

        assertSameIterator(new Entry[] { entries[0], entries[1] }, leaf1
                .entryIterator());

        assertSameIterator(new Entry[] { entries[2], entries[3], splitEntry },
                leaf2.entryIterator());

        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());
        
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
        
        assertSameIterator(new Entry[] { entries[0], entries[1], entries[2],
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

        BTree btree = getNoEvictionBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        int[] keys = new int[]{1,11,21,31};
        
        for( int i=0; i<m; i++ ) {
         
            int key = keys[i];
            
            Entry entry = new Entry();
            
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
        Entry splitEntry = new Entry();
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
        root.insert(2,new Entry());
        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,11,15},leaf1.keys);

        // Insert [key := 22] goes into leaf2 (tests edge condition)
        root.insert(22,new Entry());
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,31,0},leaf2.keys);

        // Insert [key := 24] goes into leaf2, filling it to capacity.
        root.insert(24,new Entry());
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
        root.insert(7,new Entry());
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
        root.insert(23,new Entry());
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
        btree.insert(4,new Entry());
        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,4,7},leaf1.keys);

        // Insert [key := 17] into leaf3.
        btree.insert(17,new Entry());
        assertEquals("leaf3.nkeys",3,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,17,0},leaf3.keys);

        // Insert [key := 18] into leaf3, filling it to capacity.
        btree.insert(18,new Entry());
        assertEquals("leaf3.nkeys",4,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,17,18},leaf3.keys);

        // Insert [key := 35] into leaf4.
        btree.insert(35,new Entry());
        assertEquals("leaf4.nkeys",3,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,35,0},leaf4.keys);

        // Insert [key := 40] into leaf4, filling it to capacity.
        btree.insert(40,new Entry());
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
        btree.insert(50,new Entry());
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
            
            doSplitWithIncreasingKeySequence( getNoEvictionBTree(m), m, m );
            
            doSplitWithIncreasingKeySequence( getNoEvictionBTree(m), m, m*m );

            doSplitWithIncreasingKeySequence( getNoEvictionBTree(m), m, m*m*m );

            doSplitWithIncreasingKeySequence( getNoEvictionBTree(m), m, m*m*m*m );

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
            
            doSplitWithDecreasingKeySequence( getNoEvictionBTree(m), m, m );
            
            doSplitWithDecreasingKeySequence( getNoEvictionBTree(m), m, m*m );

            doSplitWithDecreasingKeySequence( getNoEvictionBTree(m), m, m*m*m );

            doSplitWithDecreasingKeySequence( getNoEvictionBTree(m), m, m*m*m*m );

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
            
            doSplitWithRandomKeySequence( getNoEvictionBTree(m), m, m );
            
            doSplitWithRandomKeySequence( getNoEvictionBTree(m), m, m*m );

            doSplitWithRandomKeySequence( getNoEvictionBTree(m), m, m*m*m );

            doSplitWithRandomKeySequence( getNoEvictionBTree(m), m, m*m*m*m );

        }
        
    }

}
