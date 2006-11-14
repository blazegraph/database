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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import com.bigdata.cache.HardReferenceCache;
import com.bigdata.cache.HardReferenceCache.HardReferenceCacheEvictionListener;
import com.bigdata.journal.Bytes;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

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
 *       Periodically verify that the tree remains fully ordered. Store all of
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
 *       that the tree remains fully ordered.
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
public class TestSimpleBTree extends TestCase2 {

    final Random r = new Random();
    
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

    /**
     * <p>
     * Unit test for the {@link #getRandomKeys(int, int, int)} test helper. The
     * test verifies fence posts by requiring the randomly generated keys to be
     * dense in the target array. The test checks for several different kinds of
     * fence post errors.
     * </p>
     * 
     * <pre>
     *   nkeys = 6;
     *   min   = 1; (inclusive)
     *   max   = min + nkeys = 7; (exclusive)
     *   indices : [ 0 1 2 3 4 5 ]
     *   keys    : [ 1 2 3 4 5 6 ]
     * </pre>
     */
    public void test_randomKeys() {
        
        /*
         * Note: You can raise or lower this value to increase the probability
         * of triggering a fence post error. In practice, I have found that a
         * fence post was reliably triggered at keys = 6.  After debugging, I
         * then raised the #of keys to be generated to increase the likelyhood
         * of triggering a fence post error.
         */
        final int nkeys = 20;
        
        final int min = 1;
        
        final int max = min + nkeys;
        
        final int[] keys = getRandomKeys(nkeys,min,max);
        
        assertNotNull( keys );
        
        assertEquals(nkeys,keys.length);
        
        System.err.println("keys  : "+Arrays.toString(keys));

        Arrays.sort(keys);
        
        System.err.println("sorted: "+Arrays.toString(keys));
        
        // first key is the minimum value (the min is inclusive).
        assertEquals(min,keys[0]);

        // last key is the maximum minus one (since the max is exclusive).
        assertEquals(max-1,keys[nkeys-1]);
        
        for( int i=0; i<nkeys; i++ ) {

            // verify keys in range.
            assertTrue( keys[i] >= min );
            assertTrue( keys[i] < max );
            
            if( i > 0 ) {
                
                // verify monotonically increasing.
                assertTrue( keys[i] > keys[i-1]);

                // verify dense.
                assertEquals( keys[i], keys[i-1]+1);
                
            }
            
        }
        
    }
    
    /**
     * Test helper produces a set of distinct randomly selected external keys.
     */
    public int[] getRandomKeys(int nkeys) {
        
        return getRandomKeys(nkeys,Node.NEGINF+1,Node.POSINF);
        
    }
    
    /**
     * <p>
     * Test helper produces a set of distinct randomly selected external keys in
     * the half-open range [fromKey:toKey).
     * </p>
     * <p>
     * Note: An alternative to generating random keys is to generate known keys
     * and then generate a random permutation of the key order.  This technique
     * works well when you need to permutate the presentation of keys and values.
     * See {@link TestCase2#getRandomOrder(int)}.
     * </p>
     * 
     * @param nkeys
     *            The #of keys to generate.
     * @param fromKey
     *            The smallest key value that may be generated (inclusive).
     * @param toKey
     *            The largest key value that may be generated (exclusive).
     */
    public int[] getRandomKeys(int nkeys,int fromKey,int toKey) {
    
        assert nkeys >= 1;
        
        assert fromKey > Node.NEGINF;
        
        assert toKey <= Node.POSINF;
        
        // Must be enough distinct values to populate the key range.
        if( toKey - fromKey < nkeys ) {
            
            throw new IllegalArgumentException(
                    "Key range too small to populate array" + ": nkeys="
                            + nkeys + ", fromKey(inclusive)=" + fromKey
                            + ", toKey(exclusive)=" + toKey);
            
        }
        
        final int[] keys = new int[nkeys];
        
        int n = 0;
        
        int tries = 0;
        
        while( n<nkeys ) {
            
            if( ++tries >= 100000 ) {
                
                throw new AssertionError(
                        "Possible fence post : fromKey(inclusive)=" + fromKey
                                + ", toKey(exclusive)=" + toKey + ", tries="
                                + tries + ", n=" + n + ", "
                                + Arrays.toString(keys));
                
            }
            
            final int key = r.nextInt(toKey - 1) + fromKey;

            assert key >= fromKey;

            assert key < toKey;

            /*
             * Note: This does a linear scan of the existing keys. We do NOT use
             * a binary search since the keys are NOT sorted.
             */
            boolean exists = false;
            for (int i = 0; i < n; i++) {
                if (keys[i] == key) {
                    exists = true;
                    break;
                }
            }
            if (exists) continue;

            // add the key.
            keys[n++] = key;
            
        }
        
        return keys;
        
    }
    
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

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 20;
        
        BTree btree = new BTree(store, branchingFactor);

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
     * 
     * @todo verify correct tracking on the tree of the #of entries.
     */
    public void test_insertIntoLeaf02() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 20;
        
        BTree btree = new BTree(store, branchingFactor);

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
     * 
     * @todo verify correct tracking on the tree of the #of entries.
     */
    public void test_insertIntoLeaf03() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 20;
        
        BTree btree = new BTree(store, branchingFactor);

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
            Entry[] tmp = new Entry[root.nkeys];
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
     * 
     * @todo the lowest key needs to be 2 not 1 so there is room for this insert.
     */
    public void test_splitRootLeaf_low01() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int m = 4;

        BTree btree = new BTree(store, m);

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

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int m = 4;

        BTree btree = new BTree(store, m);

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

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int m = 4;

        BTree btree = new BTree(store, m);

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

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int m = 4;

        BTree btree = new BTree(store, m);

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

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int m = 4;

        BTree btree = new BTree(store, m);

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
     *       to split one more time.
     * 
     * @todo Carry through this example in gory detail both here and for the
     *       case where m == 5 (or three, which might be easier) below.
     */
    public void test_splitRootLeaf01_even() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int m = 4;

        BTree btree = new BTree(store, m);

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
     * An error run : Different objects at index=8: expected=13, actual=16
     * <pre>
     * m=4
     * keys=[14, 15, 7, 12, 5, 16, 9, 6, 13, 1, 11, 8, 10, 3, 4, 2]
     * </pre>
     */
    public void test_insertSequence01() {

        int m=4;

        int[] keys=new int[]{14, 15, 7, 12, 5, 16, 9, 6, 13, 1, 11, 8, 10, 3, 4, 2};

        doInsertKeySequenceTest(m,keys,1);

    }

    /**
     * An error run : Different objects at index=0: expected=1, actual=2
     * <pre>
     * m=4
     * keys=[13, 1, 4, 3, 8, 6, 2, 10, 15, 5, 12, 11, 16, 7, 9, 14]
     * </pre>
     */
    public void test_insertSequence02() {

        int m=4;

        int[] keys=new int[]{13, 1, 4, 3, 8, 6, 2, 10, 15, 5, 12, 11, 16, 7, 9, 14};

        doInsertKeySequenceTest(m,keys,0);

    }

    /**
     * An error run - lookup after insert does not find a value for that key
     * <pre>
     * m=4
     * keys=[14, 13, 5, 7, 10, 15, 3, 2, 12, 6, 1, 16, 8, 11, 9, 4]
     * </pre>
     */
    public void test_insertSequence03() {

        int m=4;

        int[] keys=new int[]{14, 13, 5, 7, 10, 15, 3, 2, 12, 6, 1, 16, 8, 11, 9, 4};

        doInsertKeySequenceTest(m,keys,0);

    }
    
    /**
     * A stress test for sequential key insertion that runs with a variety of
     * branching factors and #of keys to insert.
     * 
     * @todo The code fails at m := 3 due to fence posts.
     * 
     * @todo as the tree grows larger we get into evictions. This test should
     * run with evictions disabled since evictions interact with the tree in
     * interesting ways.... But we should also do a stress test with evictions
     * enabled, just later in the test suite once we have proven out the basic
     * btree mechanisms and the basic persistence mechanisms.
     */
    public void test_splitRootLeaf_increasingKeySequence() {

        int[] branchingFactors = new int[]{/*3,*/4,5,6};// 20,55,79,256,512,1024,4097};
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doSplitWithIncreasingKeySequence( m, m );
            
            doSplitWithIncreasingKeySequence( m, m*m );

            doSplitWithIncreasingKeySequence( m, m*m*m );

            doSplitWithIncreasingKeySequence( m, m*m*m*m );

        }
        
    }

    /**
     * Test helper for {@link #test_splitRootLeaf_increasingKeySequence()}.
     * creates a sequence of keys in increasing order and inserts them into the
     * tree. Note that you do not know, in general, how many inserts it will
     * take to split the root node since the split decisions are path dependent.
     * They depend on the manner in which the leaves get filled, whether or not
     * holes are created in the leaves, etc.
     * 
     * @param m
     *            The branching factor.
     * @param ninserts
     *            The #of keys to insert.
     */
    public void doSplitWithIncreasingKeySequence(int m,int ninserts) {
        
        Store<Integer, PO> store = new Store<Integer, PO>();

        BTree btree = new BTree(store, m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

        /*
         * Generate a series of external keys in increasing order. When we
         * insert these keys in sequence, the result is that all inserts go into
         * the right-most leaf (this is the original leaf until the first split,
         * and is thereafter always a new leaf).
         */
        
        int[] keys = new int[ninserts];

        Entry[] entries = new Entry[ninserts];
        
        int lastKey = Node.NEGINF + 1;
        
        for (int i = 0; i < ninserts; i++) {
        
            keys[i] = lastKey;
            
            entries[i] = new Entry();
            
            lastKey += 1;
        
        }

        /*
         * Do inserts.
         */
        
        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            int key = keys[i];
            
            Entry entry = entries[i];
            
            System.err.println("i="+i+", key="+key);

            assertEquals("#entries",i,btree.nentries);
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

                System.err.println("Split: i=" + i + ", key=" + key
                        + ", nleaves=" + btree.nleaves);

                lastLeafCount = btree.nleaves;

            }

        }

        // Note: The height, #of nodes, and #of leaves is path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(System.err));

        /*
         * Verify entries in the expected order.
         */
        assertSameIterator(entries, btree.getRoot().entryIterator());

    }
    
    /**
     * A stress test for sequential decreasing key insertions that runs with a
     * variety of branching factors and #of keys to insert.
     * 
     * @todo The code fails at m := 3 due to fence posts.
     * 
     * @todo as the tree grows larger we get into evictions. This test should
     *       run with evictions disabled since evictions interact with the tree
     *       in interesting ways.... But we should also do a stress test with
     *       evictions enabled, just later in the test suite once we have proven
     *       out the basic btree mechanisms and the basic persistence
     *       mechanisms.
     */
    public void test_splitRootLeaf_decreasingKeySequence() {

        int[] branchingFactors = new int[]{/*3,*/4,5,6};// 20,55,79,256,512,1024,4097};
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doSplitWithDecreasingKeySequence( m, m );
            
            doSplitWithDecreasingKeySequence( m, m*m );

            doSplitWithDecreasingKeySequence( m, m*m*m );

            doSplitWithDecreasingKeySequence( m, m*m*m*m );

        }
        
    }

    /**
     * Creates a sequence of keys in decreasing order and inserts them into the
     * tree. Note that you do not know, in general, how many inserts it will
     * take to split the root node since the split decisions are path dependent.
     * They depend on the manner in which the leaves get filled, whether or not
     * holes are created in the leaves, etc.
     * 
     * @param m
     *            The branching factor.
     * @param ninserts
     *            The #of keys to insert.
     */
    public void doSplitWithDecreasingKeySequence(int m, int ninserts) {

        Store<Integer, PO> store = new Store<Integer, PO>();

        BTree btree = new BTree(store, m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

        /*
         * Generate a series of external keys in decreasing order. When we
         * insert these keys in sequence, the result is that all inserts go into
         * the left-most leaf (the original leaf).
         */
        
        int[] keys = new int[ninserts];
        Entry[] entries = new Entry[ninserts];
        Entry[] reverseEntries = new Entry[ninserts];
        {
            int lastKey = ninserts;
            int reverseIndex = ninserts - 1;
            for (int i = 0; i < ninserts; i++) {
                keys[i] = lastKey;
                Entry entry = new Entry();
                entries[i] = entry;
                reverseEntries[reverseIndex--] = entry;
                lastKey -= 1;
            }
        }

        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            int key = keys[i];
            
            Entry entry = entries[i];
            
            System.err.println("i="+i+", key="+key);

            assertEquals("#entries",i,btree.nentries);
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

                System.err.println("Split: i=" + i + ", key=" + key
                        + ", nleaves=" + btree.nleaves);

                lastLeafCount = btree.nleaves;

            }

        }

        /*
         * Verify entries in the expected order.
         */
        assertSameIterator(reverseEntries, btree.getRoot().entryIterator());

        // Note: The height, #of nodes, and #of leaves is path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(System.err));
        
    }
    
    /**
     * Insert key value pairs into the tree - used to debug sequences that
     * cause problems.
     * 
     * @param m The branching factor.
     *            The tree.
     * @param keys
     *            The keys.
     * @param trace
     *            The trace level (zero disables most tracing).
     */
    private void doInsertKeySequenceTest(int m,int[] keys,int trace){
        
        Store<Integer, PO> store = new Store<Integer, PO>();

        BTree btree = new BTree(store, m);

        /*
         * Clone the keys and generate a set of entries that will be in the
         * correct order after the keys have be inserted into the tree.
         */
        
        int[] order = keys.clone();
        
        Arrays.sort(order);
        
        Entry[] entries = new Entry[keys.length];
        
        for(int i=0; i<keys.length; i++ ) {
        
            // Note: order[i] - 1 since keys in sequence are origin ONE.

            entries[ order[ i ] - 1 ] = new Entry();
            
        }

        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            int key = keys[i];
            
            Entry entry = entries[i];
            
            System.err.println("index="+i+", key="+key+", entry="+entry);

            assertEquals("#entries",i,btree.nentries);
            
            assertNull(btree.lookup(key));

            if( trace >=2 ) {
                
                System.err.print("Before insert: index="+i+", key="+key);
                btree.dump(System.err);
                
            }
            
            btree.insert(key, entry);

            if( trace >=2 ) {
                
                System.err.print("After insert: index="+i+", key="+key);
                btree.dump(System.err);
                
            }

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

                System.err.println("Split: i=" + i + ", key=" + key
                        + ", nleaves=" + btree.nleaves);
                
                if(trace>=1) {

                    System.err.print("After split: ");
                    
                    btree.dump(System.err);
                    
                }

                lastLeafCount = btree.nleaves;

            }

        }

        // Note: The height, #of nodes, and #of leaves is path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(System.err));

        /*
         * Verify entries in the expected order.
         */
        assertSameIterator(entries, btree.getRoot().entryIterator());

    }
    
    /**
     * A stress test for random key insertion using a that runs with a variety
     * of branching factors and #of keys to insert.
     * 
     * @todo The code fails at m := 3 due to fence posts.
     * 
     * @todo as the tree grows larger we get into evictions. This test should
     *       run with evictions disabled since evictions interact with the tree
     *       in interesting ways.... But we should also do a stress test with
     *       evictions enabled, just later in the test suite once we have proven
     *       out the basic btree mechanisms and the basic persistence
     *       mechanisms.
     */
    public void test_splitRootLeaf_randomKeySequence() {

        int[] branchingFactors = new int[]{/*3,*/4,5,6};// 20,55,79,256,512,1024,4097};
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doSplitWithRandomKeySequence( m, m );
            
            doSplitWithRandomKeySequence( m, m*m );

            doSplitWithRandomKeySequence( m, m*m*m );

            doSplitWithRandomKeySequence( m, m*m*m*m );

        }
        
    }

    /**
     * Creates a sequence of distinct keys in random order and inserts them into
     * the tree. Note that the split decision points are path dependent and can
     * not be predicated given random inserts.
     * 
     * @param m
     *            The branching factor.
     * 
     * @param ninserts
     *            The #of keys to insert.
     * 
     * FIXME there are errors on some runs, indicating that there is a fence
     * post out there somewhere. Capture the keys for which it fails and create
     * a deterministic test case for each failure.  Consider running all possible
     * insert sequences for m=3, 4, 5, 6.
     * 
     * An error run : Different objects at index=8: expected=13, actual=16
     * <pre>
     * m=4
     * keys=[14, 15, 7, 12, 5, 16, 9, 6, 13, 1, 11, 8, 10, 3, 4, 2]
     * 
     * and also
     * 
     * keys=[13, 1, 4, 3, 8, 6, 2, 10, 15, 5, 12, 11, 16, 7, 9, 14]
     * </pre>
     * 
     * An error run - lookup after insert does not find a value for that key
     * <pre>
     * m=4
     * keys=[14, 13, 5, 7, 10, 15, 3, 2, 12, 6, 1, 16, 8, 11, 9, 4]
     * </pre>
     */
    public void doSplitWithRandomKeySequence(int m, int ninserts) {

        Store<Integer, PO> store = new Store<Integer, PO>();

        BTree btree = new BTree(store, m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

        /*
         * Generate a sequence of keys in increasing order and a sequence of
         * random indices into the keys (and values) that is used to present
         * the key-value pairs in random order to insert(key,value).
         */
        
        int[] keys = new int[ninserts];
        Entry[] entries = new Entry[ninserts];
        
        int lastKey = Node.NEGINF+1;
        for( int i=0; i<ninserts; i++) {
            keys[i] = lastKey;
            entries[i] = new Entry();
            lastKey+=1;
        }
        
        // Random indexing into the generated keys and values.
        int[] order = getRandomOrder(ninserts);

        try {
            doRandomKeyInsertTest(btree,keys,entries, order);
        }
        catch( AssertionError ex ) {
            System.err.println("m="+m);
            System.err.print("keys=[");
            for(int i=0; i<keys.length; i++ ) {
                if( i>0 ) System.err.print(", ");
                System.err.print(keys[order[i]]);
            }
            System.err.println("]");
            throw ex;
        }
        catch( AssertionFailedError ex ) {
            System.err.println("m="+m);
            System.err.print("keys=[");
            for(int i=0; i<keys.length; i++ ) {
                if( i>0 ) System.err.print(", ");
                System.err.print(keys[order[i]]);
            }
            System.err.println("]");
            throw ex;
        }
        
    }

    private void doRandomKeyInsertTest(BTree btree, int[] keys, Entry[] entries, int[] order ) {
        
        /*
         * Insert keys into the tree.
         */

        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            int key = keys[order[i]];
            
            Entry entry = entries[order[i]];
            
            System.err.println("i="+i+", key="+key);

            assertEquals("#entries",i,btree.nentries);
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

                System.err.println("Split: i=" + i + ", key=" + key
                        + ", nleaves=" + btree.nleaves);

                lastLeafCount = btree.nleaves;

            }

        }

        /*
         * Verify entries in the expected order. While we used a random
         * presentation order, the entries MUST now be in the original generated
         * order.
         */
        assertSameIterator(entries,btree.getRoot().entryIterator());

        // Note: The height, #of nodes, and #of leaves are path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(System.err));
        
    }
    
    /*
     * Tests of commit processing (without triggering copy-on-write).
     */
    
    /**
     * Test commit of a new tree (the root is a leaf node).
     */
    public void test_commit01() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 4;
        
        final int metadataId;
        final int rootId;
        {

            BTree btree = new BTree(store, branchingFactor);

            assertTrue(btree.root.isDirty());

            // Commit of tree with dirty root.
            metadataId = btree.commit();

            assertFalse(btree.root.isDirty());

            rootId = btree.root.getIdentity();
            
            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

        }
        
        final int metadata2;
        {

            // Load the tree.
            BTree btree = new BTree(store, branchingFactor, metadataId);

            // verify rootId.
            assertEquals(rootId,btree.root.getIdentity());

            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

            /*
             * Commit of tree with clean root writes a new metadata record but
             * does not change the rootId.
             */
            metadata2 = btree.commit();
            assertNotSame( metadataId, metadata2 );

        }

        {   // re-verify.

            // Load the tree.
            BTree btree = new BTree(store, branchingFactor, metadataId);

            // verify rootId.
            assertEquals(rootId,btree.root.getIdentity());

            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

        }
        
    }

//    /**
//     * Test commit of a tree with some structure.
//     * 
//     * FIXME Re-write commit test to use insert to store keys and drive splits and to verify
//     * the post-condition structure with both node and entry iterators.
//     */
//    public void test_commit02() {
//
//        Store<Integer, PO> store = new Store<Integer, PO>();
//
//        final int branchingFactor = 4;
//        
//        int metadataId;
//        int node1Id;
//        int leaf1Id, leaf2Id, leaf3Id;
//        {
//
//            BTree btree = new BTree(store, branchingFactor);
//
//            /*
//             * Replace the root leaf with a Node. This allows us to write the commit
//             * test without having to rely on logic to split the root leaf on
//             * overflow.
//             */
//            btree.root = new Node(btree);
//
//            // Create children and populate the tree structure.
//            Node root = (Node) btree.getRoot();
//            Leaf leaf1 = new Leaf(btree);
//            Node node1 = new Node(btree);
//            Leaf leaf2 = new Leaf(btree);
//            Leaf leaf3 = new Leaf(btree);
//
//            root.addChild(1, node1);
//            root.addChild(2, leaf1);
//
//            node1.addChild(1, leaf2);
//            node1.addChild(2, leaf3);
//
//            assertSameIterator(new AbstractNode[] { leaf2, leaf3, node1, leaf1,
//                    root }, root.postOrderIterator());
//
//            metadataId = btree.commit();
//
//            node1Id = node1.getIdentity();
//
//            leaf1Id = leaf1.getIdentity();
//            leaf2Id = leaf2.getIdentity();
//            leaf3Id = leaf3.getIdentity();
//
//        }
//
//        {
//
//            // Load the btree.
//            BTree btree = new BTree(store, branchingFactor,metadataId);
//
//            Node root = (Node) btree.getRoot();
//            assertEquals(metadataId, root.getIdentity());
//            assertEquals(null,root.getParent());
//            
//            Node node1 = (Node) root.getChild(0);
//            assertEquals(node1Id, node1.getIdentity());
//            assertEquals(root,node1.getParent());
//            
//            Leaf leaf2 = (Leaf) node1.getChild(0);
//            assertEquals(leaf2Id, leaf2.getIdentity());
//            assertEquals(node1,leaf2.getParent());
//            
//            Leaf leaf3 = (Leaf) node1.getChild(1);
//            assertEquals(leaf3Id, leaf3.getIdentity());
//            assertEquals(node1,leaf3.getParent());
//            
//            Leaf leaf1 = (Leaf) root.getChild(1);
//            assertEquals(leaf1Id, leaf1.getIdentity());
//            assertEquals(root,leaf1.getParent());
//            
//            // verify post-order iterator for root.
//            assertSameIterator(new AbstractNode[] { leaf2, leaf3, node1, leaf1,
//                    root }, root.postOrderIterator());
//
//        }
//
//    }
//
//    /*
//     * Tests of copy-on-write semantics.
//     */
//    
//    /**
//     * This simple test of the copy-on-write mechanism sets up a btree with an
//     * immutable (aka persistent) root {@link Node} and then adds a child node
//     * (a leaf). Adding the child to the immutable root triggers copy-on-write
//     * which forces cloning of the original root node. The test verifies that
//     * the new tree has the expected structure and that the old tree was not
//     * changed by this operation.
//     */
//    public void test_copyOnWrite01() {
//
//        Store<Integer, PO> store = new Store<Integer, PO>();
//
//        final int branchingFactor = 4;
//        
//        final int metadataId;
//        final int rootId;
//        final BTree oldBTree;
//        {
//            /*
//             * Replace the root leaf with a node and write it on the store. This
//             * gives us an immutable root node as a precondition for what
//             * follows.
//             */
//            BTree btree = new BTree(store, branchingFactor);
//
//            Node root = new Node(btree);
//            btree.root = root; // Note: replace root on the btree.
//
//            rootId = root.write();
//
//            metadataId = btree.commit();
//            
//            assertTrue("persistent", root.isPersistent());
//            assertEquals("identity", rootId, root.getIdentity());
//            
//            oldBTree = btree;
//
//        }
//
//        int newMetadataId;
//        int newRootId;
//        int leaf1Id;
//        { // Add a leaf node - this should trigger copy-on-write.
//
//            // load the btree.
//            BTree btree = new BTree(store,branchingFactor,metadataId);
//
//            Node root = (Node) btree.getRoot();
//            assertTrue("persistent", root.isPersistent());
//            assertEquals("identity", rootId, root.getIdentity());
//
//            Leaf leaf1 = new Leaf(btree);
//
//            Node newRoot = root.addChild(/* external id */2, leaf1);
//            assertNotSame(newRoot, root);
//            assertEquals(newRoot,btree.getRoot()); // check : btree root was set?
//            assertTrue("persistent", root.isPersistent());
//            assertFalse("persistent", newRoot.isPersistent());
//
//            newMetadataId = btree.commit();
//            
//            newRootId = newRoot.getIdentity();
//            
//            leaf1Id = leaf1.getIdentity();
//            
//        }
//
//        { // Verify read back from the store.
//
//            BTree btree = new BTree(store,branchingFactor,newMetadataId);
//
//            // Verify we got the new root.
//            Node root = (Node) btree.getRoot();
//            assertTrue("persistent", root.isPersistent());
//            assertEquals("identity", newRootId, root.getIdentity());
//
//            // Read back the child at index position 0.
//            Leaf leaf1 = (Leaf) root.getChild(0);
//            assertTrue("persistent", leaf1.isPersistent());
//            assertEquals("identity", leaf1Id, leaf1.getIdentity());
//
//            assertSameIterator(new AbstractNode[] { leaf1, root }, root
//                    .postOrderIterator());
//            
//        }
//        
//        { // Verify the old tree is unchanged.
//            
//            /*
//             * Note: We do NOT reload the old tree for this test since we want
//             * to make sure that its state was not modified and we have not done
//             * a commit so a reload would just cause changes to be discarded if
//             * there were any.
//             */
//            
//            Node root = (Node) oldBTree.getRoot();
//            
//            assertSameIterator(new AbstractNode[] { root }, root
//                    .postOrderIterator());
//
//        }
//
//    }
//
//    /**
//     * <p>
//     * Test of copy on write when the pre-condition is a committed tree with the
//     * following committed structure:
//     * </p>
//     * 
//     * <pre>
//     *  root
//     *    leaf1
//     *    node1
//     *      leaf2
//     *      leaf3
//     * </pre>
//     * 
//     * The test adds a leaf to node1 in the 3rd position. This should cause
//     * node1 and the root node to both be cloned and a new root node set on the
//     * tree. The post-modification structure is:
//     * 
//     * <pre>
//     *  root
//     *    leaf1
//     *    node1
//     *      leaf2
//     *      leaf3
//     *      leaf4
//     * </pre>
//     * 
//     * @todo Do tests of copy-on-write that go down to the key-value level.
//     */
//    public void test_copyOnWrite02() {
//
//        Store<Integer, PO> store = new Store<Integer, PO>();
//
//        final int branchingFactor = 4;
//        
//        final int metadataId;
//        final int rootId_v0;
//        final int leaf1Id_v0;
//        final int leaf3Id_v0;
//        final int leaf2Id_v0;
//        final int node1Id_v0;
//        {
//
//            BTree btree = new BTree(store, branchingFactor);
//
//            // Create a root node and its children.
//            Node root = new Node(btree);
//            btree.root = root; // replace the root with a Node.
//            Leaf leaf1 = new Leaf(btree);
//            Node node1 = new Node(btree);
//            Leaf leaf2 = new Leaf(btree);
//            Leaf leaf3 = new Leaf(btree);
//
//            root.addChild(1, leaf1);
//            root.addChild(2, node1);
//
//            node1.addChild(1, leaf2);
//            node1.addChild(2, leaf3);
//
//            // verify post-order iterator for root.
//            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, node1,
//                    root }, root.postOrderIterator());
//
//            metadataId = btree.commit();
//            leaf1Id_v0 = leaf1.getIdentity();
//            leaf2Id_v0 = leaf2.getIdentity();
//            leaf3Id_v0 = leaf3.getIdentity();
//            node1Id_v0 = node1.getIdentity();
//            rootId_v0  = btree.root.getIdentity();
//            
//        }
//
//        final int rootId_v1, leaf4Id_v0, node1Id_v1;
//        {
//
//            /*
//             * Read the tree back from the store and re-verify the tree
//             * structure.
//             */
//            
//            BTree btree = new BTree(store,branchingFactor,metadataId);
//
//            Node root = (Node) btree.getRoot();
//            assertEquals(rootId_v0,root.getIdentity());
//
//            Leaf leaf1 = (Leaf) root.getChild(0);
//            assertEquals(leaf1Id_v0, leaf1.getIdentity());
//            assertEquals(root,leaf1.getParent());
//
//            Node node1 = (Node) root.getChild(1);
//            assertEquals(node1Id_v0, node1.getIdentity());
//            assertEquals(root,node1.getParent());
//            
//            Leaf leaf2 = (Leaf) node1.getChild(0);
//            assertEquals(leaf2Id_v0, leaf2.getIdentity());
//            assertEquals(node1,leaf2.getParent());
//            
//            Leaf leaf3 = (Leaf) node1.getChild(1);
//            assertEquals(leaf3Id_v0, leaf3.getIdentity());
//            assertEquals(node1,leaf3.getParent());
//
//            // re-verify post-order iterator for root.
//            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, node1,
//                    root }, root.postOrderIterator());
//
//            /*
//             * Add a new leaf to node1.  This triggers copy-on-write.
//             */
//            
//            Leaf leaf4 = new Leaf(btree);
//            Node node1_v1 = node1.addChild(/*external key*/3, leaf4);
//
//            // Verify that node1 was changed.
//            assertNotSame( node1_v1, node1 );
//            
//            // Verify that the root node was changed.
//            Node root_v1 = (Node) btree.getRoot();
//            assertNotSame( root, root_v1 );
//
//            // verify post-order iterator for the original root.
//            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, 
//                    node1, root }, root.postOrderIterator());
//
//            // verify post-order iterator for the new root.
//            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, leaf4,
//                    node1_v1, root_v1 }, root_v1.postOrderIterator());
//
//            /*
//             * Commit the tree, get the current identity for all nodes, and
//             * verify that the nodes that should have been cloned by
//             * copy-on-write do in fact have different persistent identity while
//             * those that should not do not.
//             */
//            
//            rootId_v1 = btree.commit();
//            assertEquals( rootId_v1, root_v1.getIdentity() );
//            assertNotSame( rootId_v0, rootId_v1 ); // changed.
//
//            assertEquals( leaf1Id_v0, leaf1.getIdentity() ); // unchanged.
//            assertEquals( leaf2Id_v0, leaf2.getIdentity() ); // unchanged.
//            assertEquals( leaf3Id_v0, leaf3.getIdentity() ); // unchanged.
//            
//            leaf4Id_v0 = leaf4.getIdentity(); // new.
//            
//            node1Id_v1 = node1.getIdentity();
//            assertNotSame( node1Id_v0, node1Id_v1 ); // changed.
//            
//        }
//        
//        { // @todo reload and re-verify the new structure.
//            
//            
//        }
//        
//        {
//            // @todo verify the old tree was unchanged.
//        }
//
//    }
    
    /**
     * Persistence store.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     * @param <T>
     */
    public static class Store<K, T extends PO> {

        /**
         * Key factory.
         */
        private int nextKey = 1;

        /**
         * "Persistence store" - access to objects by the key.
         */
        private final Map<K, byte[]> store = new HashMap<K, byte[]>();

        private byte[] serialize(T po) {

            try {

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(po);
                oos.flush();
                oos.close();
                byte[] bytes = baos.toByteArray();
                return bytes;

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

        }

        private T deserialize(byte[] bytes) {

            try {

                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais);
                T po = (T) ois.readObject();
                return po;

            } catch (Exception ex) {

                throw new RuntimeException(ex);
            }

        }

        public K nextId() {

            /*
             * Note: The use of generics breaks down here since we need a
             * primitive data type to serve as the value space for the key
             * factory and a means to assign new keys within that value space.
             * This can not be defined "generically", but it rather data type
             * and store semantics specific.
             */
            K key = (K) new Integer(nextKey++);

            return key;

        }

        public byte[] _read(K key) {

            byte[] bytes = store.get(key);

            if (bytes == null)
                throw new IllegalArgumentException("Not found: key=" + key);

            return bytes;
            
        }
        
        public T read(K key) {

            byte[] bytes = store.get(key);

            if (bytes == null)
                throw new IllegalArgumentException("Not found: key=" + key);

            T value = deserialize(bytes);

            // Note: breaks generic isolation.
            value.setIdentity(((Integer) key).intValue()); // set the key - no
                                                            // back references
                                                            // exist yet.

            value.setDirty(false); // just read from the store.

            return value;

        }

        public K _insert(byte[] bytes) {

            assert bytes != null;

            K key = nextId();

            store.put(key, bytes);

            return key;

        }

        public K insert(T value) {

            assert value != null;
            assert value.isDirty();
            assert !value.isPersistent();

            K key = nextId();

            write(key, value);

            return key;

        }

        protected void write(K key, T value) {

            assert key != null;
            assert value != null;
            assert value.isDirty();

            byte[] bytes = serialize(value);

            store.put(key, bytes);

            /*
             * Set identity on persistent object.
             * 
             * Note: breaks generic isolation.
             */
            value.setIdentity(((Integer) key).intValue());

            // just wrote on the store.
            value.setDirty(false);

        }

        // Note: Must also mark the object as invalid.
        public void delete(K key) {

            store.remove(key);

        }

    }

    /**
     * Hard reference cache eviction listener for leaves.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     *            The key type.
     * @param <T>
     *            The value type.
     */
    public static class LeafEvictionListener<K, T extends PO> implements
            HardReferenceCacheEvictionListener<T> {

        /**
         * Persistence store.
         */
        private final Store<K, T> store;

        public LeafEvictionListener(Store<K, T> store) {

            assert store != null;

            this.store = store;

        }

        public void evicted(HardReferenceCache<T> cache, T ref) {

            assert ref instanceof Leaf;
            
            if( ref.isDirty() ) {

                ((Leaf)ref).write();
                
            }

        }

    }

    /**
     * An interface that declares how we access the persistent identity of an
     * object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IIdentityAccess {

        /**
         * Null reference for the store (zero).
         */
        public final int NULL = 0;

        /**
         * The persistent identity.
         * 
         * @exception IllegalStateException
         *                if the object is not persistent.
         */
        public int getIdentity() throws IllegalStateException;

        /**
         * True iff the object is persistent.
         */
        public boolean isPersistent();

    }

    /**
     * An interface that declares how we access the dirty state of an object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IDirty {

        public void setDirty(boolean dirty);

        public boolean isDirty();

    }

    /**
     * A persistent object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     */
    abstract public static class PO implements IIdentityAccess, IDirty,
            Externalizable {

        /**
         * The persistent identity (defined when the object is actually
         * persisted).
         */
        transient private int key = NULL;

        public boolean isPersistent() {

            return key != NULL;

        }

        public int getIdentity() throws IllegalStateException {

            if (key == NULL)
                throw new IllegalStateException();

            return key;

        }

        /**
         * Used by the store to set the persistent identity.
         * 
         * Note: This method should not be public.
         * 
         * @param key
         *            The key.
         * 
         * @throws IllegalStateException
         *             If the key is already defined.
         */
        void setIdentity(int key) throws IllegalStateException {

            if (key == NULL)
                throw new IllegalArgumentException();

            if (this.key != NULL)
                throw new IllegalStateException();

            this.key = key;

        }

        /**
         * New objects are considered to be dirty. When an object is
         * deserialized from the store the dirty flag MUST be explicitly
         * cleared.
         */
        transient private boolean dirty = true;

        public boolean isDirty() {

            return dirty;

        }

        public void setDirty(boolean dirty) {

            this.dirty = dirty;

        }

        /**
         * Extends the basic behavior to display the persistent identity of the
         * object iff the object is persistent.
         */
        public String toString() {

            if (key != NULL) {

                return super.toString() + "#" + key;

            } else {

                return super.toString();

            }

        }

    }

    /**
     * <p>
     * BTree encapsulates metadata about the persistence capable index, but is
     * not itself a persistent object.
     * </p>
     * <p>
     * Note: No mechanism is exposed for recovering a node or leaf of the tree
     * other than the root by its key. This is because the parent reference on
     * the node (or leaf) can only be set when it is read from the store in
     * context by its parent node.
     * </p>
     * <p>
     * Note: This implementation is NOT thread-safe. The object index is
     * intended for use within a single-threaded context.
     * </p>
     * <p>
     * Note: This iterators exposed by this implementation do NOT support
     * concurrent structural modification. Concurrent inserts or removals of
     * keys MAY produce incoherent traversal whether or not they result in
     * addition or removal of nodes in the tree.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class BTree {

        /**
         * The minimum allowed branching factor (3).
         */
        static public final int MIN_BRANCHING_FACTOR = 3;
        
        /**
         * The persistence store.
         */
        final protected Store<Integer, PO> store;

        /**
         * The branching factor for the btree.
         */
        final protected int branchingFactor;

        /**
         * A hard reference hash map for nodes in the btree is used to ensure
         * that nodes remain wired into memory. Dirty nodes are written to disk
         * during commit using a pre-order traversal that first writes any dirty
         * leaves and then (recursively) their parent nodes.
         * 
         * @todo Make sure that nodes are eventually removed from this set.
         *       There are two ways to make that happen. One is to just use a
         *       ring buffer with a large capacity.  This will serve a bit like
         *       an MRU.  The other is to remove nodes from this set explicitly
         *       on certain conditions.  For example, when a copy is made of an
         *       immutable node the immutable node might be removed from this
         *       set.
         */
        final Set<Node> nodes = new HashSet<Node>();

        /**
         * Leaves are added to a hard reference queue when they are created or
         * read from the store. On eviction from the queue the leaf is
         * serialized by {@link #listener} against the {@link #store}. Once the
         * leaf is no longer strongly reachable its weak references may be
         * cleared by the VM.
         * 
         * @todo Write tests to verify incremental write of leaves driven by
         *       eviction from this hard reference queue. This will require
         *       controlling the cache size and #of references scanned in order
         *       to force triggering of leaf eviction under controller
         *       circumstances.
         */
        final HardReferenceCache<PO> leaves;
        
        /**
         * Writes dirty leaves onto the {@link #store} as they are evicted.
         */
        final LeafEvictionListener<Integer, PO> listener;
        
        /**
         * The root of the btree. This is initially a leaf until the leaf is
         * split, at which point it is replaced by a node. The root is also
         * replaced each time copy-on-write triggers a cascade of updates.
         */
        AbstractNode root;

        /**
         * The height of the btree. The height is the #of leaves minus one. A
         * btree with only a root leaf is said to have <code>height := 0</code>.
         * Note that the height only changes when we split the root node.
         */
        int height;

        /**
         * The #of non-leaf nodes in the btree. The is zero (0) for a new btree.
         */
        int nnodes;
        
        /**
         * The #of leaf nodes in the btree.  This is one (1) for a new btree.
         */
        int nleaves;
        
        /**
         * The #of entries in the btree.  This is zero (0) for a new btree.
         */
        int nentries;
        
        /**
         * The root of the btree. This is initially a leaf until the leaf is
         * split, at which point it is replaced by a node. The root is also
         * replaced each time copy-on-write triggers a cascade of updates.
         */
        public AbstractNode getRoot() {

            return root;

        }

        /**
         * Constructor for a new btree.
         * 
         * @param store
         *            The persistence store.
         * @param branchingFactor
         *            The branching factor.
         */
        public BTree(Store<Integer, PO> store, int branchingFactor) {

            assert store != null;
        
            assert branchingFactor >= MIN_BRANCHING_FACTOR;

            this.store = store;
            
            this.branchingFactor = branchingFactor;
            
            listener = new LeafEvictionListener<Integer, PO>(store);
            
            leaves = new HardReferenceCache<PO>(listener,1000);
            
            this.root = new Leaf(this);
            
            this.height = 0;
            
            this.nnodes = 0;
            
            this.nleaves = 1;
            
            this.nentries = 0;

        }

        /**
         * Constructor for an existing btree.
         * 
         * @param store
         *            The persistence store.
         * @param branchingFactor
         *            The branching factor.
         * @param metadataId
         *            The persistent identifier of btree metadata.
         * 
         * @todo move the branching factor into the metadata record, but this
         *       will remove the distinction between the btree constructors.
         */
        public BTree(Store<Integer, PO> store, int branchingFactor,
                int metadataId) {

            assert store != null;

            assert branchingFactor > MIN_BRANCHING_FACTOR;

            assert height >= 0;
            
            assert nnodes >= 0;
            
            assert nleaves >= 0;
            
            assert nentries >= 0;

            this.store = store;
            
            this.branchingFactor = branchingFactor;
            
            listener = new LeafEvictionListener<Integer, PO>(store);
            
            leaves = new HardReferenceCache<PO>(listener,1000);

            // read the btree metadata record.
            final int rootId = read(metadataId);

            /*
             * Read the root node of the btree.
             * 
             * Note: We could optionally run a variant of the post-order
             * iterator to suck in the entire node structure of the btree. If we
             * do nothing, then the nodes will be read in incrementally on
             * demand. Since we always place non-leaf nodes into a hard
             * reference cache, tree operations will speed up over time until
             * the entire non-leaf node structure is loaded.
             */
            this.root = (AbstractNode)store.read(rootId);
            
            this.root.setBTree( this );

        }

        /**
         * Insert an entry under the external key.
         */
        public void insert(int key, Entry entry) {
            
            root.insert(key, entry);
            
        }
        
        /**
         * Lookup an entry for an external key.
         * 
         * @return The entry or null if there is no entry for that key.
         */
        public Entry lookup(int key) {
            
            return root.lookup(key);
            
        }
        
        /**
         * Remove the entry for the external key.
         * 
         * @param key
         *            The external key.
         * 
         * @return The entry stored under that key and null if there was no
         *         entry for that key.
         */
        public Entry remove(int key) {
            
            return root.remove(key);
            
        }

        /**
         * Recursive dump of the tree.
         * 
         * @param out
         *            The dump is written on this stream.
         * 
         * @return true unless an inconsistency is detected.
         * 
         * @todo Compute the utilization of the tree.
         */
        boolean dump(PrintStream out) {
            
            out.println("height=" + height + ", #nodes=" + nnodes
                    + ", #leaves=" + nleaves + ", #entries=" + nentries);
            
            boolean ok = root.dump(out,0,true);
            
            return ok;
            
        }
        
        /**
         * Write out the persistent metadata for the btree on the store and
         * return the persistent identifier for that metadata. The metadata
         * include the persistent identifier of the root of the btree and the
         * height, #of nodes, #of leaves, and #of entries in the btree.
         * 
         * @param rootId
         *            The persistent identifier of the root of the btree.
         * 
         * @return The persistent identifier for the metadata.
         */
        int write() {
            
            int rootId = root.getIdentity();
            
            ByteBuffer buf = ByteBuffer.allocate(SIZEOF_METADATA);
            
            buf.putInt(rootId);
            buf.putInt(height);
            buf.putInt(nnodes);
            buf.putInt(nleaves);
            buf.putInt(nentries);

            return store._insert(buf.array());
            
        }

        /**
         * Read the persistent metadata record for the btree.  Sets the height,
         * #of nodes, #of leavs, and #of entries from the metadata record as a
         * side effect.
         * 
         * @param metadataId
         *            The persistent identifier of the btree metadata record.
         *            
         * @return The persistent identifier of the root of the btree.
         */
        int read(int metadataId) {

            ByteBuffer buf = ByteBuffer.wrap(store._read(metadataId));
            
            final int rootId = buf.getInt();
            System.err.println("rootId="+rootId);
            height = buf.getInt(); assert height>=0;
            nnodes = buf.getInt(); assert nnodes>=0;
            nleaves = buf.getInt(); assert nleaves>=0;
            nentries = buf.getInt(); assert nentries>=0;
            
            return rootId;
            
        }

        /**
         * The #of bytes in the metadata record written by {@link #write(int)}.
         * 
         * @see #write(int)
         */
        public static final int SIZEOF_METADATA = Bytes.SIZEOF_INT * 5;  

        /**
         * Commit dirty nodes using a post-order traversal that first writes any
         * dirty leaves and then (recursively) their parent nodes. The parent
         * nodes are guarenteed to be dirty if there is a dirty child so the
         * commit never triggers copy-on-write.
         * 
         * @return The persistent identity of the metadata record for the btree.
         */
        public int commit() {

            if (!root.isDirty()) {

                /*
                 * Optimization : if the root node is not dirty then the
                 * children can not be dirty either.
                 */

                return root.getIdentity();

            }

            int ndirty = 0; // #of dirty nodes (node or leave) written by
                            // commit.
            int nleaves = 0; // #of dirty leaves written by commit.

            /*
             * Traverse tree, writing dirty nodes onto the store.
             * 
             * Note: This iterator only visits dirty nodes.
             */
            Iterator itr = root.postOrderIterator(true);

            while (itr.hasNext()) {

                AbstractNode node = (AbstractNode) itr.next();

                assert node.isDirty();
                
//                if (node.isDirty()) {

                    if (node != root) {

                        /*
                         * The parent MUST be defined unless this is the root
                         * node.
                         */

                        assertNotNull( node.getParent() );
                    
                    }

                    // write the dirty node on the store.
                    node.write();

                    ndirty++;

                    if (node instanceof Leaf)
                        nleaves++;

//                }

            }

            System.err.println("commit: " + ndirty + " dirty nodes (" + nleaves
                    + " leaves), rootId=" + root.getIdentity() );

            return write();

        }

        /*
         * IObjectIndex.
         *
         * FIXME Implement IObjectIndex API.
         */
        
        /**
         * Add / update an entry in the object index.
         * 
         * @param id
         *            The persistent id.
         * @param slots
         *            The slots on which the current version is written.
         */
        public void put(int id,ISlotAllocation slots) {
           
            assert id > AbstractNode.NEGINF && id < AbstractNode.POSINF;
            
        }
        
        /**
         * Return the slots on which the current version of the object is
         * written.
         * 
         * @param id
         *            The persistent id.
         * @return The slots on which the current version is written.
         */
        public ISlotAllocation get(int id) {
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * Mark the object as deleted.
         * 
         * @param id
         *            The persistent id.
         */
        public void delete(int id) {
            
            throw new UnsupportedOperationException();
            
        }

    }

    /**
     * <p>
     * Abstract node.
     * </p>
     * <p>
     * Note: For nodes in the index, the attributes dirty and persistent are
     * 100% correlated. Since only transient nodes may be dirty and only
     * persistent nodes are clean any time one is true the other is false.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public abstract static class AbstractNode extends PO {

        /**
         * Negative infinity for the external keys.
         */
        static final int NEGINF = 0;

        /**
         * Positive infinity for the external keys.
         */
        static final int POSINF = Integer.MAX_VALUE;

        /**
         * The BTree.
         * 
         * Note: This field MUST be patched when the node is read from the
         * store. This requires a custom method to read the node with the btree
         * reference on hand so that we can set this field.
         * 
         * Note: We also need the branching factor on hand when we deserialize a
         * node. That is why {@link #branchingFactor} is currently defined as a
         * constant.
         */
        transient protected BTree btree;

        /**
         * The branching factor (#of slots for keys or values).
         */
        transient protected int branchingFactor;

        /**
         * The #of valid keys for this node.
         */
        protected int nkeys = 0;

        /**
         * The external keys for the B+Tree. The #of keys depends on whether
         * this is a {@link Node} or a {@link Leaf}. A leaf has one key per
         * value - that is, the maximum #of keys for a leaf is specified by the
         * branching factor. In contrast a node has n-1 keys where n is the
         * maximum #of children (aka the branching factor).  Therefore this
         * field is initialized by the {@link Leaf} or {@link Node} - NOT by
         * the {@link AbstractNode}.
         * 
         * The interpretation of the key index for a leaf is one to one - key[0]
         * corresponds to value[0].
         * 
         * @see #findChild(int key)
         * @see Search#search(int, int[], int)
         */
        protected int[] keys;

        /**
         * The parent of this node. This is null for the root node. The parent
         * is required in order to set the persistent identity of a newly
         * persisted child node on its parent. The reference to the parent will
         * remain strongly reachable as long as the parent is either a root
         * (held by the {@link BTree}) or a dirty child (held by the
         * {@link Node}). The parent reference is set when a node is attached
         * as the child of another node.
         */
        protected WeakReference<Node> parent = null;

        /**
         * The parent iff the node has been added as the child of another node
         * and the parent reference has not been cleared.
         * 
         * @return The parent.
         */
        public Node getParent() {

            Node p = null;
            
            if (parent != null) {

                /*
                 * Note: Will be null if the parent reference has been cleared.
                 */
                p = parent.get();

            }

            /*
             * The parent is allowed to be null iff this is the root of the
             * btree.
             */
            assert (this == btree.root && p == null) || p != null;
            
            return p;

        }

        /**
         * De-serialization constructor used by subclasses.
         */
        protected AbstractNode() {
            
        }

        public AbstractNode(BTree btree) {

            assert btree != null;

            this.branchingFactor = btree.branchingFactor;

            setBTree( btree );

        }

        /**
         * Copy constructor.
         * 
         * @param src
         *            The source node.
         */
        protected AbstractNode(AbstractNode src) {

            /*
             * Note: We do NOT clone the base class since this is a new
             * persistence capable object, but it is not yet persistent and we
             * do not want to copy the persistent identity of the source object.
             */
            super();

            assert ! isPersistent();
            
            assert src != null;

            this.branchingFactor = btree.branchingFactor;
            
            setBTree( src.btree );

        }

        /**
         * This method MUST be used to set the btree reference. This method
         * verifies that the btree reference is not already set. It is extended
         * by {@link Node#setBTree(BTree)} to ensure that the btree holds a hard
         * reference to the node.
         * 
         * @param btree
         *            The btree.
         */
        void setBTree(BTree btree) {

            assert btree != null;

            if (this.btree != null) {
                
                throw new IllegalStateException();

            }

            this.btree = btree;

        }

        /**
         * <p>
         * Return this node iff it is dirty and otherwise return a copy of this
         * node. If a copy is made of the node, then a copy will also be made of
         * each parent of the node up to the root of the tree and the new root
         * node will be set on the {@link BTree}. This method must MUST be
         * invoked any time an mutative operation is requested for the node. For
         * simplicity, it is invoked by the two primary mutative operations
         * {@link #insert(int, com.bigdata.objndx.TestSimpleBTree.Entry)} and
         * {@link #remove(int)}.
         * </p>
         * <p>
         * Note: You can not modify a node that has been written onto the store.
         * Instead, you have to clone the node causing it and all nodes up to
         * the root to be dirty and transient. This method handles that cloning
         * process, but the caller MUST test whether or not the node was copied
         * by this method, MUST delegate the mutation operation to the copy iff
         * a copy was made, and MUST result in an awareness in the caller that
         * the copy exists and needs to be used in place of the immutable
         * version of the node.
         * </p>
         * 
         * @return Either this node or a copy of this node.
         * 
         * FIXME The comments above are not quite correct since you also need to
         * invoke copyOnWrite when splits drive up the tree from a node or leaf
         * towards the root and during rotations.
         */
        protected AbstractNode copyOnWrite() {

            if (isPersistent()) {

                Node parent = this.getParent();
                
                AbstractNode newNode;
                
                if (this instanceof Node) {

                    newNode = new Node((Node) this);

                } else {

                    newNode = new Leaf((Leaf) this);

                }

                if( btree.root == this ) {
                    
                    assert parent == null;
                    
                    // Update the root node on the btree.

                    System.err.println("Copy-on-write : replaced root node on btree.");
                    
                    btree.root = newNode;
                    
                } else {
                    
                    /*
                     * Recursive copy-on-write up the tree. This operations
                     * stops as soon as we reach a parent node that is already
                     * dirty and grounds out at the root in any case.
                     */
                    assert parent != null;
                    
                    if( ! parent.isDirty() ) {

                        Node newParent = (Node) parent.copyOnWrite();
                        
                        newParent.replaceChildRef(this.getIdentity(),newNode);
                    
                    }
                    
                }
                
                return newNode;

            } else {

                return this;

            }

        }

        /**
         * Post-order traveral of nodes and leaves in the tree. For any given
         * node, its children are always visited before the node itself (hence
         * the node occurs in the post-order position in the traveral). The
         * iterator is NOT safe for concurrent modification.
         */
        public Iterator postOrderIterator() {
            
            return postOrderIterator( false );
            
        }

        /**
         * Post-order traveral of nodes and leaves in the tree. For any given
         * node, its children are always visited before the node itself (hence
         * the node occurs in the post-order position in the traveral). The
         * iterator is NOT safe for concurrent modification.
         * 
         * @param dirtyNodesOnly
         *            When true, only dirty nodes and leaves will be visited
         */
        abstract public Iterator postOrderIterator(boolean dirtyNodesOnly);
        
        /**
         * Traversal of index values in key order.
         */
        public Iterator entryIterator() {

            /*
             * Begin with a post-order iterator.
             */
            return new Striterator(postOrderIterator()).addFilter(new Expander() {
                /*
                 * Expand the {@link Entry} objects for each leaf visited in
                 * the post-order traversal.
                 */
                protected Iterator expand(Object childObj) {
                    /*
                     * A child of this node.
                     */
                    AbstractNode child = (AbstractNode) childObj;

                    if( child instanceof Leaf ) {

                        return ((Leaf)child).entryIterator();

                    } else {
                        
                        return EmptyIterator.DEFAULT;
                        
                    }
                }
            });
            
        }

        /**
         * True iff this is a leaf node.
         */
        abstract public boolean isLeaf();
        
        /**
         * <p>
         * Split a node or leaf. This node (or leaf) is the "low" node (or leaf)
         * and will contain the lower half of the keys. The split creates a
         * "high" node (or leaf) to contain the high half of the keys. When this
         * is the root, the split also creates a new root {@link Node}.
         * </p>
         * <p>
         * Note: Splits are triggered on insert into a full node or leaf. The
         * first key of the high node is inserted into the parent of the split
         * node. The caller must test whether the key that is being inserted is
         * greater than or equal to this key. If it is, then the insert goes
         * into the high node. Otherwise it goes into the low node.
         * </p>
         * 
         * @param key
         *            The external key.
         * 
         * @return The high node (or leaf) created by the split.
         */
        abstract protected AbstractNode split();
        
        /**
         * Recursive search locates the approprate leaf and inserts the entry
         * under the key. The leaf is split iff necessary. Splitting the leaf
         * can cause splits to cascade up towards the root. If the root is split
         * then the total depth of the tree is inceased by one.
         * 
         * @param key
         *            The external key.
         * @param entry
         *            The value.
         */
        abstract public void insert(int key, Entry entry);
        
        /**
         * Recursive search locates the appropriate leaf and removes and returns
         * the pre-existing value stored under the key (if any).
         * 
         * @param key
         *            The external key.
         * @return The value or null if there was no entry for that key.
         */
        abstract public Entry remove(int key);
        
        /**
         * Recursive search locates the entry for the probe key.
         * 
         * @param key
         *            The external key.
         * 
         * @return The entry or <code>null</code> iff there is no entry for
         *         that key.
         */
        abstract public Entry lookup(int key);
        
//        /**
//         * <p>
//         * Copy down the elements of the per-key arrays from the index to the
//         * #of keys currently defined thereby creating an unused position at
//         * <i>index</i>. The #of keys is NOT modified by this method. The #of
//         * elements to be copied is computed as:
//         * </p>
//         * 
//         * <pre>
//         *   count := #nkeys - index
//         * </pre>
//         * 
//         * <p>
//         * This method MUST be extended by subclasses that declare additional
//         * per-key or value data. Note that the child arrays defined by
//         * {@link Node} are dimensioned to have one more element than the
//         * external {@link #keys}[] array.
//         * </p>
//         * 
//         * @param index
//         *            The index.
//         * 
//         * @param count
//         *            The #of elements to be copied
//         */
//        abstract void copyDown(int index, int count);
        
        /**
         * Writes the node on the store. The node MUST be dirty. If the node has
         * a parent, then the parent is notified of the persistent identity
         * assigned to the node by the store.
         * 
         * @return The persistent identity assigned by the store.
         */
        int write() {

            assert isDirty();
            assert !isPersistent();
            
            // write the dirty node on the store.
            btree.store.insert(this);

            // The parent should be defined unless this is the root node.
            Node parent = getParent();
            
            if (parent != null) {

                // parent must be dirty if child is dirty.
                assert parent.isDirty();

                // parent must not be persistent if it is dirty.
                assert !parent.isPersistent();

                /*
                 * Set the persistent identity of the child on the
                 * parent.
                 * 
                 * Note: A parent CAN NOT be serialized before all of
                 * its children have persistent identity since it needs
                 * to write the identity of each child in its
                 * serialization record.
                 */
                parent.setChildRef(this);

            }
         
            return getIdentity();
            
        }

        /**
         * Dump the data onto the {@link PrintStream} (non-recursive).
         * 
         * @param out
         *            Where to write the dump.
         * 
         * @return True unless an inconsistency was detected.
         */
        public boolean dump(PrintStream out) {
            
            return dump(out,0,false);
            
        }
        
        /**
         * Dump the data onto the {@link PrintStream}.
         * 
         * @param out
         *            Where to write the dump.
         * @param height
         *            The height of this node in the tree.
         * @param recursive
         *            When true, the node will be dumped recursively using a
         *            pre-order traversal.
         *            
         * @return True unless an inconsistency was detected.
         */
        abstract public boolean dump(PrintStream out,int height,boolean recursive);

        /**
         * Returns a string that may be used to indent a dump of the nodes in
         * the tree.
         * 
         * @param height
         *            The height.
         *            
         * @return A string suitable for indent at that height.
         */
        protected static String indent(int height) {
            
            return ws.substring(0,height*4);
            
        }
        private static final String ws = "                                                                             ";
        
    }

    /**
     * Visits the direct children of a node in the external key ordering.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ChildIterator implements Iterator<AbstractNode> {

        private final Node node;

        private int index = 0;

        public ChildIterator(Node node) {

            assert node != null;

            this.node = node;

        }

        public boolean hasNext() {

            // Note: nchildren == nkeys+1 for a Node.
            return index <= node.nkeys;

        }

        public AbstractNode next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            return node.getChild(index++);
        }

        public void remove() {

            throw new UnsupportedOperationException();

        }

    }

    /**
     * Visits the {@link Entry}s of a {@link Leaf} in the external key ordering.
     * There is exactly one entry per key for a leaf node.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo write test suite for {@link AbstractNode#entryIterator()}.
     */
    static class EntryIterator implements Iterator<Entry> {

        private final Leaf leaf;

        private int index = 0;

        public EntryIterator(Leaf leaf) {

            assert leaf != null;

            this.leaf = leaf;

        }

        public boolean hasNext() {

            return index < leaf.nkeys;

        }

        public Entry next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            return leaf.values[index++];
            
        }

        public void remove() {

            throw new UnsupportedOperationException();

        }

    }

    /**
     * <p>
     * A non-leaf.
     * </p>
     * <p>
     * A non-leaf node with <code>m</code> children has <code>m-1</code>
     * keys. The minimum #of keys that {@link Node} may have is one (1) while
     * the minimum #of children that a {@link Node} may have is two (2).
     * However, only the root {@link Node} is permitted to reach that minimum
     * (unless it is a leaf, in which case it may even be empty). All other
     * nodes or leaves have between <code>m/2</code> and <code>m</code>
     * keys, where <code>m</code> is the {@link AbstractNode#branchingFactor}.
     * </p>
     * <p>
     * Note: We MUST NOT hold hard references to leafs. Leaves account for most
     * of the storage costs of the BTree. We bound those costs by only holding
     * hard references to nodes and not leaves.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Node extends AbstractNode {

        private static final long serialVersionUID = 1L;

        /**
         * Hard reference cache containing dirty child nodes (nodes or leaves).
         */
        transient protected Set<AbstractNode> dirtyChildren;

        /**
         * Weak references to child nodes (may be nodes or leaves). The capacity
         * of this array is m+1, where m is the {@link #branchingFactor}.
         */
        transient protected WeakReference<AbstractNode>[] childRefs;

        /**
         * <p>
         * The persistent keys of the childKeys nodes (may be nodes or leaves).
         * The capacity of this array is m+1, where m is the
         * {@link #branchingFactor}. The key is {@link #NULL} until the child
         * has been persisted. The protocol for persisting child nodes requires
         * that we use a pre-order traversal (the general case is a directed
         * graph) so that we can update the keys on the parent before the parent
         * itself is persisted.
         * </p>
         * <p>
         * Note: It is an error if there is an attempt to serialize a node
         * having a null entry in this array and a non-null entry in the
         * external {@link #keys} array.
         * </p>
         */
        protected int[] childKeys;

        /**
         * Extends the super class implementation to add the node to a hard
         * reference cache so that weak reference to this node will not be
         * cleared while the {@link BTree} is in use.
         */
        void setBTree(BTree btree) {

            super.setBTree(btree);

            btree.nodes.add(this);

        }

        /**
         * De-serialization constructor.
         */
        public Node() {

        }

        /**
         * Used to create a new node when a node is split.
         */
        Node(BTree btree) {

            super(btree);

            keys = new int[branchingFactor-1];
            
            dirtyChildren = new HashSet<AbstractNode>(branchingFactor);
            
            childRefs = new WeakReference[branchingFactor];
            
            childKeys = new int[branchingFactor];

        }

        /**
         * This constructor is used when splitting the either the root
         * {@link Leaf} or a root {@link Node}.
         * 
         * @param btree
         *            Required solely to differentiate the method signature from
         *            the copy constructor.
         * @param oldRoot
         *            The node that was previously the root of the tree (either
         *            a node or a leaf).
         * 
         * @todo The key is ignored and could be removed from the constructor
         *       signature. The key gets added by
         *       {@link #addChild(int, com.bigdata.objndx.TestSimpleBTree.AbstractNode)}
         */
        Node(BTree btree,AbstractNode oldRoot) {

            super(oldRoot.btree);
            
            // Verify that this is the root.
            assert oldRoot == btree.root;
            
            // The old root must be dirty when it is being split.
            assert oldRoot.isDirty();
            
            keys = new int[branchingFactor-1];
                        
            dirtyChildren = new HashSet<AbstractNode>(branchingFactor);
            
            childRefs = new WeakReference[branchingFactor];
            
            childKeys = new int[branchingFactor];

            // Replace the root node on the tree.
            btree.root = this;
            
            /*
             * Attach the old root to this node.
             */
            
            childRefs[0] = new WeakReference<AbstractNode>(oldRoot); 
            
            dirtyChildren.add(oldRoot);

            oldRoot.parent = new WeakReference<Node>(this);
            
            /*
             * The tree is deeper since we just split the root node.
             */
            btree.height++;
            
            btree.nnodes++;
            
        }

        /**
         * Copy constructor.
         * 
         * @param src
         *            The source node.
         * 
         * FIXME Modify to steal the unmodified children by setting their parent
         * fields to the new node. Stealing the parent means that the node can
         * no longer be used by its previous ancestor. However, since the node
         * state is unchanged (it is immutable) the slick trick would be to wrap
         * the state node with a flyweight node having a different parent so
         * that the node remained valid for its old parent. This probably means
         * making getParent() abstract and moving the parent field into a
         * flyweight class.
         */
        protected Node(Node src) {

            super(src);

            assert isDirty();
            assert ! isPersistent();

            keys = new int[branchingFactor-1];
            
            nkeys = src.nkeys;

            dirtyChildren = new HashSet<AbstractNode>(branchingFactor);

            childRefs = new WeakReference[branchingFactor];
            
            childKeys = new int[branchingFactor];

            // Copy keys.
            for (int i = 0; i < nkeys; i++) {

                keys[i] = src.keys[i];

            }

            // Note: There is always one more child than keys for a Node.
            for (int i = 0; i <= nkeys; i++) {

                this.childKeys[i] = src.childKeys[i];

            }
            
        }

        /**
         * Always returns <code>false</code>.
         */
        final public boolean isLeaf() {
        
            return false;
            
        }
        
        /**
         * This method must be invoked on a parent to notify the parent that the
         * child has become persistent. The method scans the weak references for
         * the children, finds the index for the specified child, and then sets
         * the corresponding index in the array of child keys. The child is then
         * removed from the dirty list for this node.
         * 
         * @param child
         *            The child.
         * 
         * @exception IllegalStateException
         *                if the child is not persistent.
         * @exception IllegalArgumentException
         *                if the child is not a child of this node.
         */
        void setChildRef(AbstractNode child) {

            if (!child.isPersistent()) {

                // The child does not have persistent identity.
                throw new IllegalStateException();

            }

            // Scan for location in weak references.
            for (int i = 0; i < nkeys; i++) {

                if (childRefs[i].get() == child) {

                    childKeys[i] = child.getIdentity();

                    if (!dirtyChildren.remove(child)) {

                        throw new AssertionError("Child was not on dirty list.");

                    }

                    return;

                }

            }

            /*
             * @todo I have seen this triggered by increment writes of dirty
             * leaves using the doSplitWithIncreasingKeySequence at m := 20.
             */
            System.err.print("parent: ");
            this.dump(System.err);
            System.err.print("child : ");
            child.dump(System.err);
            throw new IllegalArgumentException("Not our child : child=" + child);

        }

        /**
         * Invoked by {@link #copyOnWrite()} to change the key for a child on a
         * cloned parent to a reference to a cloned child.
         * 
         * @param oldChildKey
         *            The key for the old child.
         * @param newChild
         *            The reference to the new child.
         */
        void replaceChildRef(int oldChildKey, AbstractNode newChild ) {

            assert oldChildKey != NULL;
            assert newChild != null;
            
            // This node MUST have been cloned as a pre-condition, so it can not
            // be persistent.
            assert ! isPersistent();

            // The newChild MUST have been cloned and therefore MUST NOT be
            // persistent.
            assert ! newChild.isPersistent();
            
            // Scan for location in weak references.
            for (int i = 0; i < nkeys; i++) {

                if (childKeys[i] == oldChildKey ) {

                    if (true) {
                        
                        /*
                         * Do some paranoia checks.
                         */
                        
                        AbstractNode oldChild = childRefs[i] != null ? childRefs[i]
                                .get()
                                : null;

                        if( oldChild != null ) {

                            assert oldChild.isPersistent();
                            
                            assert ! dirtyChildren.contains(oldChild);

                        }
                        
                    }

                    // Clear the old key.
                    childKeys[i] = NULL;

                    // Stash reference to the new child.
                    childRefs[i] = new WeakReference<AbstractNode>(newChild);
                    
                    // Add the new child to the dirty list.
                    dirtyChildren.add(newChild);

                    // Set the parent on the new child.
                    newChild.parent = new WeakReference<Node>(this);
                    
                    return;

                }

            }

            throw new IllegalArgumentException("Not our child : oldChildKey="
                    + oldChildKey);

        }

        /**
         * Insert the entry under the key. This finds the index of the first
         * external key in the node whose value is greater than or equal to the
         * supplied key. The insert is then delegated to the child at position
         * <code>index - 1</code>.
         * 
         * @param key
         *            The external key.
         * @param entry
         *            The value.
         */
        public void insert(int key, Entry entry) {

            int index = findChild(key);
            
            AbstractNode child = getChild( index );
            
            child.insert( key, entry );

        }
        
        public Entry lookup(int key) {
            
            int index = findChild(key);

            AbstractNode child = getChild(index);

            return child.lookup(key);
            
        }
        
        /**
         * Recursive descent until we reach the leaf that would hold the key.
         * 
         * @param key
         *            The external key.
         * 
         * @return The value stored under that key or null if the key was not
         *         found.
         */
        public Entry remove(int key) {
            
            int index = findChild(key);

            AbstractNode child = getChild(index);
            
            return child.remove(key);
            
        }
        
        /**
         * Return the index of the child to be searched.
         * 
         * The interpretation of the key index for a node is as follows. When
         * searching nodes of the tree, we search for the index in keys[] of the
         * first key value greater than or equal (GTE) to the probe key and then
         * choose the child having the same index as the GTE key match. For
         * example,
         * 
         * <pre>
         * keys[]  : [ 5 9 12 ]
         * child[] : [ a b  c  d ]
         * </pre>
         * 
         * A probe with keys up to <code>5</code> matches at index zero (0)
         * and we choose the 1st child, a, which is at index zero (0).
         * 
         * A probe with keys in [6:9] matches at index one (1) and we choose the
         * 2nd child, b, which is at index one (1).
         * 
         * A probe with keys in [10:12] matches at index two (2) and we choose
         * the 3rd child, c, which is at index two (2).
         * 
         * A probe with keys greater than 12 exceeds all keys in the node and
         * always matches the last child in that node. In this case, d, which is
         * at index three (3).
         * 
         * Note that we never stop a search on a node, even when there is an
         * exact match on a key. All values are stored in the leaves and we
         * always descend until we reach the leaf in which a value for the key
         * would be stored. A test on the keys of that leaf is then conclusive -
         * either a value is stored in the leaf for that key or it is not stored
         * in the tree.
         * 
         * @param key
         *            The probe (an external key).
         * 
         * @return The child to be searched next for that key.
         */
        public int findChild(int key) {
            
            int index = Search.search(key,keys,nkeys);
            
            if( index >= 0 ) {
                
                /*
                 * exact match.
                 */
                
                return index;
                
            } else {
                
                /*
                 * There is no exact match on the key, so we convert the search
                 * result to find the insertion point. The insertion point is
                 * always an index whose current key (iff it is defined) is
                 * greater than the probe key.
                 * 
                 * keys[] : [ 5 9 12 ]
                 * 
                 * The insertion point for key == 4 is zero.
                 * 
                 * The insertion point for key == 6 is one.
                 * 
                 * etc.
                 * 
                 * When the probe key is greater than any existing key, then
                 * the insertion point is nkeys.  E.g., the insertion point
                 * for key == 20 is 3.
                 */
                
                // Convert to obtain the insertion point.
                index = -index - 1;
                
                return index;
                
            }
            
        }

        /**
         * Split the node. Given m := {@link #branchingFactor}, keys in
         * <code>[0:m/2-1]</code> are retained in this node and keys in
         * <code>[m/2,m)</code> are copied into a new node and the key at
         * <code>m/2</code> is inserted into the parent. If this node is the
         * not of the tree (no parent), then a new {@link Node} is created and
         * made the parent of this node. If adding the key would overflow the
         * parent node, then the parent is split. Finally, the new node is added
         * as a child of the parent node.
         * 
         * @todo Review copyOnWrite triggers when splitting a node. Basically,
         *       everything up to the root will need to be mutable even if the
         *       split does not reach that high.
         */
        protected AbstractNode split() {

            assert isDirty(); // MUST be mutable.

            final int m = branchingFactor;
            final int m2 = m >> 1; // aka m/2
            final int m21 = m2-1; // aka (m/2) - 1
            final int key = keys[m21];

            Node node2 = new Node(btree);

            System.err.print("SPLIT NODE: key=" + key + ": ");
            dump(System.err);

            int j = 0;
            for (int i = m2; i < m; i++, j++) {

                // copy key and value to the new leaf.
                if(i+1<m) node2.keys[j] = keys[i];
                node2.childRefs[j] = childRefs[i];
                node2.childKeys[j] = childKeys[i];
                AbstractNode tmp = (childRefs[i] == null ? null : childRefs[i].get());
                if (tmp != null) {
                    // move hard reference for dirty child to the new node.
                    dirtyChildren.remove(tmp);
                    node2.dirtyChildren.add(tmp);
                    /*
                     * Update the parent on the child (from this node to the new
                     * node).
                     */
                    tmp.parent = new WeakReference<Node>(node2);
                }

                // clear out the old keys and values.
                if(i+1<m) {
                    keys[i] = NEGINF;
                    this.nkeys--; // one less key here.
                    node2.nkeys++; // more more key there.
                }
                childRefs[i] = null;
                childKeys[i] = NULL;

            }
            
            /* 
             * Clear the key that is being move into the parent.
             */
            keys[m21] = NULL;
            nkeys--;
            assert nkeys >= 0;

            Node p = getParent();

            if (p == null) {

                /*
                 * Use a special constructor to split the root.
                 */

                p = new Node(btree, this);

            }

            /*
             * Add the new node to the parent.
             */
            p.addChild(key, node2);

            btree.nnodes++;
            
            // Return the high node.
            return node2;

        }
        
        /**
         * Invoked to insert a key and reference for a child created when
         * another child of this node is split.
         * 
         * @param key
         *            The key on which the old node was split.
         * @param child
         *            The new node.
         */
        public AbstractNode addChild(int key, AbstractNode child) {

            assert key > NEGINF && key < POSINF;
            assert child != null;
            assert child.isDirty(); // always dirty since it was just created.
            assert isDirty(); // must be dirty to permit mutation.

            if (nkeys == keys.length) {

                /*
                 * The node is full. First split the node, then add the
                 * children. If this is the root node, then we handle it
                 * specially. Otherwise it is a split of a node in an
                 * intermediate level.
                 */
                
                Node node2 = (Node) split();

                if( key >= node2.keys[0] ) {
                 
                    /*
                     * The key belongs to the high node after the split.
                     */

                    return node2.addChild(key,child);
                    
                }
                
            }

            /*
             * Find the location where this key belongs. When a new node is
             * created, the constructor sticks the child that demanded the split
             * into childRef[0]. So, when nkeys == 1, we have nchildren==1 and
             * the key goes into keys[0] but we have to copyDown by one anyway
             * to avoid stepping on the existing child.
             */
            int index = Search.search(key, keys, nkeys);

            if (index >= 0) {

                /*
                 * The key is already present. This is an error.
                 */

                throw new AssertionError("Split on existing key: key=" + key);

            }

            // Convert the position to obtain the insertion point.
            index = -index - 1;

            assert index >= 0 && index <= nkeys;

            /*
             * copy down per-key data.
             */
            final int keyCount = nkeys - index;

            if( keyCount > 0 ) System.arraycopy(keys, index, keys, index + 1, keyCount);

            /*
             * Note: This is equivilent to (nkeys + 1) - (index+1). (nkeys+1) is
             * equivilent to nchildren while (index+1) is the position at which
             * the new child reference is written. So, childCount == keyCount.
             * 
             * @todo Refactor to use only one [count/length] parameter and
             * update comments.  Avoid invoking the copy operation when the count
             * is zero.
             */
            final int childCount = nkeys - index;

            /*
             * copy down per-child data. #children == nkeys+1. child[0] is
             * always defined.
             */
            System.arraycopy(childKeys, index + 1, childKeys, index + 2,
                    childCount);
            System.arraycopy(childRefs, index + 1, childRefs, index + 2,
                    childCount);

            /*
             * Insert key at index.
             */
            keys[index] = key;

            /*
             * Insert child at index+1.
             */
            childRefs[index + 1] = new WeakReference<AbstractNode>(child);

            childKeys[index + 1] = NULL;

            dirtyChildren.add(child);

            child.parent = new WeakReference<Node>(this);

            nkeys++;

            return this;

        }
        
// /**
// * Invoked to insert a key and reference for a child created when
// * another child of this node is split.
// *
// * @param key
// * The external key value.
// * @param child
// * The node or leaf.
// *
// * @return Either this node or a copy of this node if this node was
// * persistent and copy-on-write was triggered.
//         */
//        Node addChild(int key, AbstractNode child) {
//
//            Node copy = (Node) copyOnWrite();
//
//            if (copy != this) {
//
//                return copy.addChild(key, child);
//
//            } else {
//
//                assert key > NEGINF && key < POSINF;
//                assert child != null;
//
//                /*
//                 * FIXME This presumes that the child reference and the external
//                 * key for the child are stored at the same index. However, the
//                 * correct interpretation is that child[i] points to the child
//                 * for keys between key[i] and key[i+1]. Therefore child[0] is
//                 * between key[0] and key[1]. Note further that key[0] may be
//                 * elided. On the other end, child[m-1] is for keys between
//                 * key[m-2] and key[m-1], where m is the #of children. Therefore
//                 * the #of keys can be said to be one less than the #of children
//                 * since we can elide key[0] and store child[0:m-1] but only
//                 * keys[1:m-1].
//                 */
//                keys[nkeys] = key;
//
//                if (child.isPersistent()) {
//
//                    childRefs[nkeys] = new WeakReference<AbstractNode>(child);
//                    childKeys[nkeys] = child.getIdentity();
//
//                } else {
//
//                    dirtyChildren.add(child);
//                    child.parent = new WeakReference<Node>(this);
//                    childRefs[nkeys] = new WeakReference<AbstractNode>(child);
//                    childKeys[nkeys] = 0;
//
//                }
//
//                nkeys++;
//
//                setDirty(true);
//
//                return this;
//
//            }
//
//        }

        /**
         * Return the child node or leaf at the specified index in this node. If
         * the node is not in memory then it is read from the store.
         * 
         * @param index
         *            The index in [0:nkeys].
         * 
         * @return The child node or leaf.
         */
        public AbstractNode getChild(int index) {

            assert index >= 0 && index <= nkeys;

            WeakReference<AbstractNode> childRef = childRefs[index];

            AbstractNode child = null;

            if (childRef != null) {

                child = childRef.get();

            }

            if (child == null) {

                int key = childKeys[index];

                assert key != NULL;

                assert btree != null;

                child = (AbstractNode) btree.store.read(key);

                /*
                 * Patch btree reference since loaded from store. If the child
                 * is a {@link Node}, then it is also inserted into the hard
                 * reference cache of nodes maintained by the btree.
                 */
                child.setBTree( btree );
                
                // patch parent reference since loaded from store.
                child.parent = new WeakReference<Node>(this);

                // patch the child reference.
                childRefs[index] = new WeakReference<AbstractNode>(child);

                if (child instanceof Leaf) {
                    
                    /*
                     * Leaves are added to a hard reference queue. On eviction
                     * from the queue the leaf is serialized. Once the leaf is
                     * no longer strongly reachable its weak references may be
                     * cleared by the VM.
                     */

                    btree.leaves.append((Leaf)child);
                    
                }

            }

            return child;

        }

//        /**
//         * Copy down for a node is invoked during a split operation. In this
//         * context, there is always at one more child than key. The key is
//         * inserted at index, while the child is inserted at index+1. Therefore
//         * some adjustment of indices is required.
//         */
//        void copyDown(int index, int count) {
//            
//            /*
//             * copy down per-key data.
//             */
//            System.arraycopy( keys, index, keys, index+1, count );
//
//            /*
//             * copy down per-child data.  #children == nkeys+1.
//             */
//            System.arraycopy( childKeys, index, childKeys, index+1, count+1 );
//            System.arraycopy( childRefs, index, childRefs, index+1, count+1 );
//
//            /*
//             * Clear the entry at the index. This is partly a paranoia check and
//             * partly critical. Some per-key elements MUST be cleared and it is
//             * much safer (and quite cheap) to clear them during copyDown()
//             * rather than relying on maintenance elsewhere.
//             */
//
//            keys[ index ] = NEGINF; // an invalid key.
//            childKeys[ index ] = NULL;
//            childRefs[ index ] = null;
//            
//        }

        /**
         * Iterator visits children, recursively expanding each child with a
         * post-order traversal of its children and finally visits this node
         * itself.
         */
        public Iterator postOrderIterator(final boolean dirtyNodesOnly) {

            /*
             * Iterator append this node to the iterator in the post-order
             * position.
             */

            return new Striterator(postOrderIterator1(dirtyNodesOnly))
                    .append(new SingleValueIterator(this));

        }

        /**
         * Visits the children (recursively) using post-order traversal, but
         * does NOT visit this node.
         */
        private Iterator postOrderIterator1(final boolean dirtyNodesOnly) {

            if( dirtyNodesOnly && ! isDirty() ) {
            
                return EmptyIterator.DEFAULT;
                
            }
            
            /*
             * Iterator visits the direct children, expanding them in turn with
             * a recursive application of the post-order iterator.
             */

//            System.err.println("node: " + this);

            return new Striterator(childIterator()).addFilter(new Expander() {
                /*
                 * Expand each child in turn.
                 */
                protected Iterator expand(Object childObj) {

                    /*
                     * A child of this node.
                     */

                    AbstractNode child = (AbstractNode) childObj;

                    if (child instanceof Node) {

                        /*
                         * The child is a Node (has children).
                         */

//                        System.err.println("child is node: " + child);

                        // visit the children (recursive post-order traversal).
                        Striterator itr = new Striterator(((Node) child)
                                .postOrderIterator1(dirtyNodesOnly));

                        // append this node in post-order position.
                        itr.append(new SingleValueIterator(child));

                        return itr;

                    } else {

                        /*
                         * The child is a leaf.
                         */

//                        System.err.println("child is leaf: " + child);

                        // Visit the leaf itself.
                        return new SingleValueIterator(child);

                    }
                }
            });

        }

        /**
         * Iterator visits the direct child nodes in the external key ordering.
         */
        public Iterator childIterator() {

            return new ChildIterator(this);

        }

        public boolean dump(PrintStream out, int height, boolean recursive) {
            
            boolean ok = true;
            out.println(indent(height)+"Node: "+toString());
            out.println(indent(height)+"  parent="+getParent());
            out.println(indent(height) + "  dirty=" + isDirty() + ", nkeys="
                    + nkeys + ", nchildren=" + (nkeys + 1)
                    + ", branchingFactor=" + branchingFactor);
            out.println(indent(height)+"  keys="+Arrays.toString(keys));
            { // verify keys are monotonically increasing.
                int lastKey = NEGINF;
                for (int i = 0; i < nkeys; i++) {
                    if (keys[i] <= lastKey) {
                        out.println(indent(height)
                                + "ERROR keys out of order at index=" + i
                                + ", lastKey=" + lastKey + ", keys[" + i + "]="
                                + keys[i]);
                        ok = false;
                    }
                    lastKey = keys[i];
                }
            }
            out.println(indent(height)+"  childKeys="+Arrays.toString(childKeys));
            out.print(indent(height)+"  childRefs=[");
            for(int i=0; i<branchingFactor; i++ ) {
                if(i>0) out.print(", ");
                if(childRefs[i]==null) out.print("null");
                else out.print(childRefs[i].get());
            }
            out.println("]");
            out.print(indent(height)+"  #dirtyChildren="+dirtyChildren.size()+" : {");
            int n = 0;
            Iterator<AbstractNode> itr = dirtyChildren.iterator();
            while(itr.hasNext()) {
                if(n++>0) out.print(", ");
                out.print(itr.next());
            }
            out.println("}");
            
            /*
             * Look for inconsistencies for children. A dirty child MUST NOT
             * have an entry in childKeys[] since it is not persistent and MUST
             * show up in dirtyChildren. Likewise if a child is NOT dirty, then
             * it MUST have an entry in childKeys and MUST NOT show up in
             * dirtyChildren.
             * 
             * This also verifies that all entries beyond nchildren (nkeys+1)
             * are unused.
             */
            for( int i=0; i<branchingFactor; i++) {
                
                if (i > nkeys) {

                    /*
                     * Scanning past the last valid child index. 
                     */

                    if (childKeys[i] != NULL) {

                        out.println(indent(height) + "  ERROR childKeys[" + i
                                + "] should be " + NULL + ", not "
                                + childKeys[i]);

                        ok = false;

                    }

                    if (childRefs[i] != null) {

                        out.println(indent(height) + "  ERROR childRefs[" + i
                                + "] should be null, not " + childRefs[i]);

                        ok = false;

                    }

                } else {

                    /*
                     * Scanning a valid child index.
                     * 
                     * Note: This is not fetching the child if it is not in
                     * memory -- perhaps it should using its persistent id?
                     */

                    AbstractNode child = (childRefs[i] == null ? null
                            : childRefs[i].get());

                    if (child != null) {

                        if(child.isDirty()) {
                            /*
                             * Dirty child.
                             */
                            if (childRefs[i] == null) {
                                out.println(indent(height)
                                        + "  ERROR childRefs[" + i
                                        + "] is null, but the child is dirty");                                
                            }
                            if( childKeys[i] != NULL ) {
                                out.println(indent(height)
                                        + "  ERROR childKeys[" + i + "]="
                                        + childKeys[i] + ", but MUST be "
                                        + NULL + " since the child is dirty");
                            }
                            if( ! dirtyChildren.contains(child)) {
                                out.println(indent(height + 1)
                                        + "  ERROR child at index="+i+" is dirty, but not on the dirty list: child="+child);
                            }
                        } else {
                            /*
                             * Clean child (ie, persistent).
                             */
                            if( childKeys[i] == NULL ) {
                                out.println(indent(height)
                                        + "  ERROR childKey[" + i + "] is "
                                        + NULL + ", but child is not dirty");
                            }
                            if( dirtyChildren.contains(child)) {
                                out.println(indent(height)
                                        + "  ERROR child at index="+i+" is not dirty, but is on the dirty list: child="+child);
                            }
                        }
                        
                    }

                }
                
            }
        
            if (recursive) {

                /*
                 * Dump children using pre-order traversal.
                 */
                
                Set<AbstractNode> dirty = new HashSet<AbstractNode>();
                
                for (int i = 0; i <= nkeys; i++) {

                    if( childRefs[i] == null && childKeys[i] == 0 ) {
                    
                        /*
                         * This let's us dump a tree with some kinds of
                         * structural problems (missing child reference or key).
                         */
                        
                        out.println(indent(height + 1)
                                + "ERROR can not find child at index=" + i
                                + ", skipping this index.");

                        ok = false;
                        
                        continue;

                    }

                    AbstractNode child = getChild(i);

                    if( child.parent == null ) {
                        
                        out.println(indent(height + 1)+
                                "ERROR child does not have parent reference at index="+i
                                );
                        
                        ok = false;
                        
                    }
                    
                    if( child.parent.get() != this ) {
                        
                        out.println(indent(height + 1)+
                                "ERROR child has incorrect parent reference at index="+i
                                );

                        ok = false;
                        
                    }
                    
                    if (child.isDirty() && !dirtyChildren.contains(child)) {

                        out.println(indent(height + 1)
                                        + "ERROR dirty child not in node's dirty list at index="
                                        + i);
                        
                        ok = false;

                    }

                    if (! child.isDirty() && dirtyChildren.contains(child)) {

                        out.println(indent(height + 1)
                                + "ERROR clear child found in node's dirty list at index="
                                + i);

                        ok = false;

                    }

                    if( child.isDirty() ) {
                        
                        dirty.add(child);
                        
                    }
                    
                    if( i == 0 ) {

                        /*
                         * Note: All keys on the first child MUST be LT the
                         * first key on this node.
                         */

                        if( child.keys[0] >= keys[0] ) {
                            
                            out.println(indent(height + 1)
                                    + "ERROR first key on first child must be LT "
                                    + keys[0] + ", but found " + child.keys[0]);
                            
                            ok = false;

                        }

                        if( child.nkeys>=1 && child.keys[child.nkeys-1] >= keys[0] ) {
                            
                            out.println(indent(height + 1)
                                    + "ERROR last key on first child must be LT "
                                    + keys[0] + ", but found " + child.keys[child.nkeys-1]);
                            
                            ok = false;

                        }

                    } else if( i < nkeys ) {
                    
                        if( child.isLeaf() && keys[i-1] != child.keys[0] ) {
                        
                            /*
                             * While each key in a node always is the first key
                             * of some leaf, we are only testing the direct
                             * children here. Therefore if the children are not
                             * leaves then we can not cross check their first
                             * key with the keys on this node.
                             */
                            out.println(indent(height + 1)
                                    + "ERROR first key on child leaf must be "
                                    + keys[i-1] + ", not " + child.keys[0]
                                    + " at index=" + i);
                        
                            ok = false;

                        }
                        
                    } else {
                        
                        /*
                         * While there is a child for the last index of a node ,
                         * there is no key for that index.
                         */
                        
                    }
                    
                    if( ! child.dump(out, height + 1, true) ) {
                        
                        ok = false;
                        
                    }

                }
                
                if( dirty.size() != dirtyChildren.size() ) {
                    
                    out.println(indent(height + 1) + "ERROR found "
                            + dirty.size() + " dirty children, but "
                            + dirtyChildren.size() + " in node's dirty list");

                    ok = false;

                }
                
            }

            return ok;
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            if (dirtyChildren.size() > 0) {
                throw new IllegalStateException("Dirty children exist.");
            }
            assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
            assert nkeys > 0;
            out.writeInt(branchingFactor);
            out.writeInt(nkeys);
            for (int i = 0; i < nkeys; i++) {
                int key = keys[i];
                assert key > NEGINF && key < POSINF;
                out.writeInt(key);
            }
            // Note: nchildren == nkeys+1.
            for (int i = 0; i <= nkeys; i++) {
                int childKey = childKeys[i];
                assert childKey != NULL;
                out.writeInt(childKey);
            }
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            branchingFactor = in.readInt();
            assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
            nkeys = in.readInt();
            assert nkeys > 0;
            keys = new int[branchingFactor-1];
            dirtyChildren = new HashSet<AbstractNode>(branchingFactor);
            childRefs = new WeakReference[branchingFactor];
            childKeys = new int[branchingFactor];
            for (int i = 0; i < nkeys; i++) {
                int key = in.readInt();
                assert key > NEGINF && key < POSINF;
                keys[i] = key;
            }
            // Note: nchildren == nkeys+1.
            for (int i = 0; i <= nkeys; i++) {
                int childKey = in.readInt();
                assert childKey != NULL;
                childKeys[i] = childKey;
            }
        }

    }

    /**
     * An entry in a {@link Leaf}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo Reconcile with {@link IObjectIndexEntry} and {@link NodeSerializer}.
     */
    public static class Entry implements Serializable {

        private static final long serialVersionUID = 1L;

        private static int nextId = 1;
        private int id;
        
        /**
         * Create a new entry.
         */
        public Entry() {
            id = nextId++;
        }

        /**
         * Copy constructor.
         * 
         * @param src
         *            The source to be copied.
         */
        public Entry(Entry src) {
            id = src.id;
        }

        public String toString() {
            return ""+id;
        }
        
    }

    /**
     * <p>
     * A leaf.
     * </p>
     * <p>
     * Note: Leaves are NOT chained together for the object index since that
     * forms cycles that make it impossible to set the persistent identity for
     * both the prior and next fields of a leaf.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Leaf extends AbstractNode {

        private static final long serialVersionUID = 1L;
        
        /**
         * The values of the tree.
         */
        protected Entry[] values;

        /**
         * De-serialization constructor.
         */
        public Leaf() {

        }

        public Leaf(BTree btree) {

            super(btree);

            // nkeys == nvalues for a Leaf.
            keys = new int[branchingFactor];
            
            values = new Entry[branchingFactor];
            
            // Add to the hard reference queue.
            btree.leaves.append(this);
            
        }

        /**
         * Copy constructor.
         * 
         * @param src
         *            The source node.
         */
        protected Leaf(Leaf src) {

            super(src);

            // nkeys == nvalues for a Leaf.
            keys = new int[branchingFactor];
            
            values = new Entry[branchingFactor];
            
            for (int i = 0; i < nkeys; i++) {

                /*
                 * Clone the value so that changes to the value in the new leaf
                 * do NOT bleed into the immutable src leaf.
                 */
                this.values[i] = new Entry(src.values[i]);

            }

            // Add to the hard reference queue.
            btree.leaves.append(this);

        }

        /**
         * Always returns <code>true</code>.
         */
        final public boolean isLeaf() {
         
            return true;
            
        }

        /**
         * Inserts an entry under an external key. The caller MUST ensure by
         * appropriate navigation of parent nodes that the external key either
         * exists in or belongs in this node.  If the leaf is full, then it is
         * split before the insert.
         * 
         * @param key
         *            The external key.
         * @param entry
         *            The new entry.
         */
        public void insert(int key, Entry entry) {

            /*
             * Note: This is one of the few gateways for mutation of a leaf via
             * the main btree API (insert, lookup, delete). By ensuring that we
             * have a mutable leaf here, we can assert that the leaf must be
             * mutable in other methods.
             */
            Leaf copy = (Leaf) copyOnWrite();

            if (copy != this) {

                copy.insert(key, entry);
                
                return;

            }

            if( nkeys == branchingFactor ) {

                /*
                 * Split this leaf into a low leaf (this leaf) and a high leaf
                 * (returned by split()). If the key is greater than or equal to
                 * the first key in the high leaf then the insert is directed
                 * into the high leaf.
                 */

                Leaf leaf2 = (Leaf) split();
                
                if( key >= leaf2.keys[0] ) {
                    
                    leaf2.insert(key, entry);
                    
                    return;
                    
                }
                
            }
            
            int index = Search.search(key,keys,nkeys);

            if (index >= 0) {

                /*
                 * The key is already present in the leaf.
                 * 
                 * @todo This is where we would handle replacement of the
                 * existing value.
                 * 
                 * Note: We do NOT search before triggering copy-on-write for an
                 * object index since an insert always triggers a mutation
                 * (unlike a standard btree which can report that the key
                 * already exists, we will actually modify the value as part of
                 * the object index semantics).
                 */

                return;

            }

            // Convert the position to obtain the insertion point.
            index = -index - 1;

            // insert an entry under that key.
            insert(key, index, entry);
            
            // one more entry in the btree.
            btree.nentries++;

        }

        public Entry lookup(int key) {
            
            int index = Search.search(key,keys,nkeys);
            
            if( index < 0 ) {
                
                // Not found.
                
                return null;
                
            }
            
            return values[ index ];
            
        }
        
        /**
         * Split the leaf. Given m := {@link #branchingFactor}, keys in
         * <code>[0:m/2-1]</code> are retained in this leaf and keys in
         * <code>[m/2,m)</code> are copied into a new leaf and the key at
         * <code>m/2</code> is inserted into the parent. If this leaf is the
         * root of the tree (no parent), then a new {@link Node} is created and
         * made the parent of this leaf. If adding the key would overflow the
         * parent node, then the parent is split. Finally, the new leaf is added
         * as a child of the parent node.
         */
        protected AbstractNode split() {
            
            assert isDirty(); // MUST be mutable.
            
            final int m = branchingFactor;
            final int m2 = m>>1; // aka m/2
//            final int m21 = m2-1; // aka (m/2) - 1
            final int key = keys[m2];

            Leaf leaf2 = new Leaf(btree);

            System.err.print("SPLIT LEAF: key="+key+": "); dump(System.err);

            int j=0;
            for (int i = m2; i < m; i++, j++) {
            
                // copy key and value to the new leaf.
                leaf2.keys[j] = keys[i];
                leaf2.values[j] = values[i];

                // clear out the old keys and values.
                keys[i] = NEGINF;
                values[i] = null;
                
                this.nkeys--; // one less key here.
                leaf2.nkeys++; // more more key there.
                
            }

            Node p = getParent();
            
            if( p == null ) {

                /*
                 * Use a special constructor to split the root leaf.
                 */

                p = new Node(btree,this);

            }

            /* 
             * Add the new leaf to the parent.
             */
            p.addChild(key,leaf2);
            
            btree.nleaves++;

            // Return the high leaf.
            return leaf2;
            
        }
        
        /**
         * Inserts an entry under an external key. This method is invoked when a
         * {@link Search#search(int, int[], int)} has already revealed that
         * there is no entry for the key in the node.
         * 
         * @param key
         *            The external key.
         * @param index
         *            The index position for the new entry. Data already present
         *            in the leaf beyond this insertion point will be shifted
         *            down by one.
         * @param entry
         *            The new entry.
         */
        void insert( int key, int index, Entry entry ) {

            assert key != NULL;
            assert index >=0 && index <= nkeys;
            assert entry != null;
            
            if( nkeys == keys.length ) {
                
                throw new RuntimeException("Overflow: key=" + key + ", index="
                        + index);
                
            } else {

                if( index < nkeys ) {
                    
                    /* index = 2;
                     * nkeys = 6;
                     * 
                     * [ 0 1 2 3 4 5 ]
                     *       ^ index
                     * 
                     * count = keys - index = 4;
                     */
                    final int count = nkeys - index;
                    
                    assert count >= 1;

                    copyDown( index, count );
                    
                }
                
                /*
                 * Insert at index.
                 */
                keys[ index ] = key; // defined by AbstractNode
                values[ index ] = entry; // defined by Leaf.
                
              }
            
            nkeys++;
            
        }
        
        void copyDown(int index, int count) {

            /*
             * copy down per-key data (#values == nkeys).
             */
            System.arraycopy( keys, index, keys, index+1, count );
            System.arraycopy( values, index, values, index+1, count );
            
            /*
             * Clear the entry at the index. This is partly a paranoia check and
             * partly critical. Some per-key elements MUST be cleared and it is
             * much safer (and quite cheap) to clear them during copyDown()
             * rather than relying on maintenance elsewhere.
             */

            keys[ index ] = NEGINF; // an invalid key.

            values[ index ] = null;
            
        }

        /**
         * If the key is found on this leaf, then ensures that the leaf is
         * mutable and removes the key and value.
         * 
         * @param key
         *            The external key.
         * 
         * @return The value stored under that key or null.
         * 
         * @todo write unit tests for removing keys in terms of the keys and
         *       values remaining in the leaf, lookup reporting false
         *       afterwards, and not visiting the deleted entry.
         * 
         * @todo Write unit tests for maintaining the tree in balance as entries
         *       are removed.
         */
        public Entry remove(int key) {

            assert key > NEGINF && key < POSINF;
            
            final int index = Search.search(key,keys,nkeys);

            if( index < 0 ) {

                // Not found.
                
                return null;
                
            }
            
            /*
             * Note: This is one of the few gateways for mutation of a leaf via
             * the main btree API (insert, lookup, delete). By ensuring that we
             * have a mutable leaf here, we can assert that the leaf must be
             * mutable in other methods.
             */

            Leaf copy = (Leaf) copyOnWrite();

            if (copy != this) {

                return copy.remove(key);
                
            }

            // The value that is being removed.
            Entry entry = values[ index ];
            
            /*
             * Copy over the hole created when the key and value were removed
             * from the leaf. 
             */
            
            final int length = nkeys - index;
            
            if( length > 0 ) {
             
                System.arraycopy(keys, index+1, keys, index, length);
                
                System.arraycopy(values, index+1, values, index, length);
                
            } else {
                
                keys[index] = NEGINF;
            
                values[index] = null;
            
            }
            
            btree.nentries--;
            
            return entry;
            
        }

        public Iterator postOrderIterator(final boolean dirtyNodesOnly) {
            
            if (dirtyNodesOnly) {

                if (isDirty()) {
                    
                    return new SingleValueIterator(this);
                    
                } else {
                    
                    return EmptyIterator.DEFAULT;
                    
                }

            } else {
                
                return new SingleValueIterator(this);
                
            }

        }

        /**
         * Iterator visits the defined {@link Entry}s in key order.
         * 
         * @todo define key range scan iterator. This would be implemented
         *       differently for a B+Tree since we could just scan leaves. For a
         *       B-Tree, we have to traverse the nodes (pre- or post-order?) and
         *       then visit only those keys that satisify the from/to
         *       constraint. We know when to stop in the leaf scan based on the
         *       keys, but we also have to check when to stop in the node scan
         *       or we will scan all nodes and not just those covering the key
         *       range.
         */
        public Iterator entryIterator() {
         
            if (nkeys == 0) {

                return EmptyIterator.DEFAULT;
                
            }

            return new EntryIterator(this);
            
        }
        
        public boolean dump(PrintStream out, int height, boolean recursive) {

            boolean ok = true;
            out.println(indent(height)+"Leaf: "+toString());
            out.println(indent(height)+"  parent="+getParent());
            out.println(indent(height)+"  dirty=" + isDirty() + ", nkeys="
                    + nkeys + ", branchingFactor=" + branchingFactor);
            out.println(indent(height)+"  keys="+Arrays.toString(keys));
            { // verify keys are monotonically increasing.
                int lastKey = NEGINF;
                for (int i = 0; i < nkeys; i++) {
                    if (keys[i] <= lastKey) {
                        out.println(indent(height)
                                + "ERROR keys out of order at index=" + i
                                + ", lastKey=" + lastKey + ", keys[" + i + "]="
                                + keys[i]);
                        ok = false;
                    }
                    lastKey = keys[i];
                }
            }
            out.println(indent(height)+"  vals="+Arrays.toString(values));
            return ok;
            
        }
        
        /*
         * Note: Serialization is fat since values are not strongly typed.
         */
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(branchingFactor);
            out.writeInt(nkeys);
            for (int i = 0; i < nkeys; i++) {
                int key = keys[i];
                assert keys[i] > NEGINF && keys[i] < POSINF;
                out.writeInt(key);
            }
            for (int i = 0; i < nkeys; i++) {
                Object value = values[i];
                assert value != null;
                out.writeObject(value);
            }
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            branchingFactor = in.readInt();
            nkeys = in.readInt();
            keys = new int[branchingFactor];
            values = new Entry[branchingFactor];
            for (int i = 0; i < nkeys; i++) {
                int key = in.readInt();
                assert keys[i] > NEGINF && keys[i] < POSINF;
                keys[i] = key;
            }
            for (int i = 0; i < nkeys; i++) {
                Entry value = (Entry) in.readObject();
                assert value != null;
                values[i] = value;
            }
        }

    }

}
