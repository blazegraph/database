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
 * Created on Nov 17, 2006
 */

package com.bigdata.objectIndex;

import java.util.Random;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import com.bigdata.journal.ContiguousSlotAllocation;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * Abstract test case for {@link BTree} tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBTreeTestCase extends TestCase2 {

    Random r = new Random();
    
    /**
     * 
     */
    public AbstractBTreeTestCase() {
    }

    /**
     * @param name
     */
    public AbstractBTreeTestCase(String name) {
        super(name);
    }
    
    public void assertSameNodeOrLeaf(AbstractNode n1, AbstractNode n2 ) {
        
        if( n1 == n2 ) return;
        
        if( n1.isLeaf() && n2.isLeaf() ) {
            
            assertSameLeaf((Leaf)n1,(Leaf)n2);
            
        } else if( !n1.isLeaf() && !n2.isLeaf() ) {
            
            assertSameNode((Node)n1,(Node)n2);

        } else {
            
            fail("Expecting two nodes or two leaves, but not a node and a leaf");
            
        }
        
    }

    /**
     * Compares two nodes (or leaves) for the same data.
     * 
     * @param n1
     *            The expected node state.
     * @param n2
     *            The actual node state.
     */
    public void assertSameNode(Node n1, Node n2 ) {

        if( n1 == n2 ) return;
        
        assertEquals("index",n1.btree,n2.btree);
        
        // @todo identity available iff persistent.
        assertEquals("id",n1.getIdentity(),n2.getIdentity());
        
        assertEquals("branchingFactor",n1.branchingFactor,n2.branchingFactor);
        
        assertEquals("nkeys",n1.nkeys,n2.nkeys);

        assertEquals("keys",n1.keys,n2.keys);
        
        assertEquals("children",n1.childKeys,n2.childKeys);
        
    }

    /**
     * Compares leaves for the same data.
     * 
     * @param n1
     *            The expected leaf state.
     * @param n2
     *            The actual leaf state.
     */
    public void assertSameLeaf(Leaf n1, Leaf n2 ) {

        if( n1 == n2 ) return;
        
        assertEquals("index",n1.btree,n2.btree);
        
        // @todo identity available iff persistent.
        assertEquals("id",n1.getIdentity(),n2.getIdentity());
        
        assertEquals("pageSize",n1.branchingFactor,n2.branchingFactor);
        
        assertEquals("first",n1.nkeys,n2.nkeys);

        assertEquals("keys",n1.keys,n2.keys);
        
        assertEquals("values", n1.values, n2.values);
        
//        for( int i=0; i<n1.nkeys; i++ ) {
//
//            assertEquals("values[" + i + "]",
//                    (IObjectIndexEntry) n1.values[i],
//                    (IObjectIndexEntry) n2.values[i]);
//            
//        }
        
    }

    /**
     * Compare an array of {@link IObjectIndexEntry}s for consistent data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals( IObjectIndexEntry[] expected, IObjectIndexEntry[] actual )
    {
        assertEquals( null, expected, actual );
    }

    /**
     * Compare an array of {@link IObjectIndexEntry}s for consistent data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals( String msg, IObjectIndexEntry[] expected, IObjectIndexEntry[] actual )
    {

        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
        
        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        assertEquals
            ( msg+"length differs.",
              expected.length,
              actual.length
              );
        
        for( int i=0; i<expected.length; i++ ) {
            
            assertEquals
                ( msg+"values differ: index="+i,
                  expected[ i ],
                  actual[ i ]
                  );
            
        }
        
    }
    
    /**
     * Test two {@link IObjectIndexEntry entries} for consistent data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(IObjectIndexEntry expected,
            IObjectIndexEntry actual) {
        
        assertEquals(null,expected,actual);
        
    }
    
    /**
     * Test two {@link IObjectIndexEntry entries} for consistent data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(String msg, IObjectIndexEntry expected,
            IObjectIndexEntry actual) {
        
        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }

        if( expected == null) {
            
            assertNull(actual);
            
        } else {
        
            assertEquals(msg+"versionCounter", expected.getVersionCounter(), actual
                    .getVersionCounter());

            assertEquals(msg+"isDeleted", expected.isDeleted(), actual.isDeleted());

            assertEquals(msg+"currentVersion", expected.getCurrentVersionSlots(),
                    actual.getCurrentVersionSlots());

            assertEquals(msg+"isPreExistingVersionOverwritten", expected
                    .isPreExistingVersionOverwritten(), actual
                    .isPreExistingVersionOverwritten());

            assertEquals(msg+"preExistingVersion", expected
                    .getPreExistingVersionSlots(), actual
                    .getPreExistingVersionSlots());
            
        }
        
    }
    
    /**
     * <p>
     * Verify that the {@link ISlotAllocation}s are consistent.
     * </p>
     * 
     * @param expected
     *            The expected slot allocation.
     * @param actual
     *            The actual slot allocation.
     */
    public void assertEquals(ISlotAllocation expected, ISlotAllocation actual) {

        assertEquals(null,expected,actual);

    }

    /**
     * <p>
     * Verify that the {@link ISlotAllocation}s are consistent.
     * </p>
     * <p>
     * Note: This test presumes that contiguous allocations are being used.
     * </p>
     * 
     * @param expected
     *            The expected slot allocation.
     * @param actual
     *            The actual slot allocation.
     */
    public void assertEquals(String msg, ISlotAllocation expected, ISlotAllocation actual) {

        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }

        if( expected == null ) {
            
            assertNull(actual);
            
        } else {

            if (!(expected instanceof ContiguousSlotAllocation)) {
                fail("Not expecting: " + expected.getClass());
            }

            if (!(actual instanceof ContiguousSlotAllocation)) {
                fail("Not expecting: " + actual.getClass());
            }

            assertEquals(msg + "firstSlot", expected.firstSlot(), actual
                    .firstSlot());

            assertEquals(msg + "byteCount", expected.getByteCount(), actual
                    .getByteCount());
        }

    }

    /**
     * Return a new btree backed by a simple transient store. The leaf cache
     * will be large and cache evictions will cause exceptions if they occur.
     * This provides an indication if cache evictions are occurring so that the
     * tests of basic tree operations in this test suite are known to be
     * conducted in an environment without incremental writes of leaves onto the
     * store. This avoids copy-on-write scenarios and let's us test with the
     * knowledge that there should always be a hard reference to a child or
     * parent.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * FIXME Refactor as an {@link IBTreeFactory} and update the test helpers
     * and test harness to match.
     */
    public BTree getNoEvictionBTree(int branchingFactor) {
        
        IRawStore store = new SimpleStore();

        BTree btree = new BTree(store, branchingFactor,
                new NoLeafEvictionListener(), 10000);

        return btree;
        
    }
    
//    /**
//     * <p>
//     * Unit test for the {@link #getRandomKeys(int, int, int)} test helper. The
//     * test verifies fence posts by requiring the randomly generated keys to be
//     * dense in the target array. The test checks for several different kinds of
//     * fence post errors.
//     * </p>
//     * 
//     * <pre>
//     *   nkeys = 6;
//     *   min   = 1; (inclusive)
//     *   max   = min + nkeys = 7; (exclusive)
//     *   indices : [ 0 1 2 3 4 5 ]
//     *   keys    : [ 1 2 3 4 5 6 ]
//     * </pre>
//     */
//    public void test_randomKeys() {
//        
//        /*
//         * Note: You can raise or lower this value to increase the probability
//         * of triggering a fence post error. In practice, I have found that a
//         * fence post was reliably triggered at keys = 6.  After debugging, I
//         * then raised the #of keys to be generated to increase the likelyhood
//         * of triggering a fence post error.
//         */
//        final int nkeys = 20;
//        
//        final int min = 1;
//        
//        final int max = min + nkeys;
//        
//        final int[] keys = getRandomKeys(nkeys,min,max);
//        
//        assertNotNull( keys );
//        
//        assertEquals(nkeys,keys.length);
//        
//        System.err.println("keys  : "+Arrays.toString(keys));
//
//        Arrays.sort(keys);
//        
//        System.err.println("sorted: "+Arrays.toString(keys));
//        
//        // first key is the minimum value (the min is inclusive).
//        assertEquals(min,keys[0]);
//
//        // last key is the maximum minus one (since the max is exclusive).
//        assertEquals(max-1,keys[nkeys-1]);
//        
//        for( int i=0; i<nkeys; i++ ) {
//
//            // verify keys in range.
//            assertTrue( keys[i] >= min );
//            assertTrue( keys[i] < max );
//            
//            if( i > 0 ) {
//                
//                // verify monotonically increasing.
//                assertTrue( keys[i] > keys[i-1]);
//
//                // verify dense.
//                assertEquals( keys[i], keys[i-1]+1);
//                
//            }
//            
//        }
//        
//    }
//    
//    /**
//     * Test helper produces a set of distinct randomly selected external keys.
//     */
//    public int[] getRandomKeys(int nkeys) {
//        
//        return getRandomKeys(nkeys,Node.NEGINF+1,Node.POSINF);
//        
//    }
//    
//    /**
//     * <p>
//     * Test helper produces a set of distinct randomly selected external keys in
//     * the half-open range [fromKey:toKey).
//     * </p>
//     * <p>
//     * Note: An alternative to generating random keys is to generate known keys
//     * and then generate a random permutation of the key order.  This technique
//     * works well when you need to permutate the presentation of keys and values.
//     * See {@link TestCase2#getRandomOrder(int)}.
//     * </p>
//     * 
//     * @param nkeys
//     *            The #of keys to generate.
//     * @param fromKey
//     *            The smallest key value that may be generated (inclusive).
//     * @param toKey
//     *            The largest key value that may be generated (exclusive).
//     */
//    public int[] getRandomKeys(int nkeys,int fromKey,int toKey) {
//    
//        assert nkeys >= 1;
//        
//        assert fromKey > Node.NEGINF;
//        
//        assert toKey <= Node.POSINF;
//        
//        // Must be enough distinct values to populate the key range.
//        if( toKey - fromKey < nkeys ) {
//            
//            throw new IllegalArgumentException(
//                    "Key range too small to populate array" + ": nkeys="
//                            + nkeys + ", fromKey(inclusive)=" + fromKey
//                            + ", toKey(exclusive)=" + toKey);
//            
//        }
//        
//        final int[] keys = new int[nkeys];
//        
//        int n = 0;
//        
//        int tries = 0;
//        
//        while( n<nkeys ) {
//            
//            if( ++tries >= 100000 ) {
//                
//                throw new AssertionError(
//                        "Possible fence post : fromKey(inclusive)=" + fromKey
//                                + ", toKey(exclusive)=" + toKey + ", tries="
//                                + tries + ", n=" + n + ", "
//                                + Arrays.toString(keys));
//                
//            }
//            
//            final int key = r.nextInt(toKey - 1) + fromKey;
//
//            assert key >= fromKey;
//
//            assert key < toKey;
//
//            /*
//             * Note: This does a linear scan of the existing keys. We do NOT use
//             * a binary search since the keys are NOT sorted.
//             */
//            boolean exists = false;
//            for (int i = 0; i < n; i++) {
//                if (keys[i] == key) {
//                    exists = true;
//                    break;
//                }
//            }
//            if (exists) continue;
//
//            // add the key.
//            keys[n++] = key;
//            
//        }
//        
//        return keys;
//        
//    }

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
    public void doSplitWithIncreasingKeySequence(BTree btree, int m,int ninserts) {
        
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
    public void doSplitWithDecreasingKeySequence(BTree btree,int m, int ninserts) {

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
     * Stress test helper inserts random permutations of keys into btrees of
     * order m for several different btrees, #of keys to be inserted, and
     * permutations of keys. Several random permutations of dense and sparse
     * keys are inserted. The #of keys to be inserted is also varied.
     */
    public void doSplitTest(int m, int trace) {
        
        /*
         * Try several permutations of the key-value presentation order.
         */
        for( int i=0; i<20; i++ ) {
         
            doInsertRandomKeySequenceTest(m, m, trace);
            
            doInsertRandomSparseKeySequenceTest(m, m, trace);
            
        }
        
        /*
         * Try several permutations of the key-value presentation order.
         */
        for( int i=0; i<20; i++ ) {
         
            doInsertRandomKeySequenceTest(m, m*m, trace);
            
            doInsertRandomSparseKeySequenceTest(m, m*m, trace);
            
        }
        
        /*
         * Try several permutations of the key-value presentation order.
         */
        for( int i=0; i<20; i++ ) {
         
            doInsertRandomKeySequenceTest(m, m*m*m, trace);
            
            doInsertRandomSparseKeySequenceTest(m, m*m*m, trace);
            
        }
        
        /*
         * Try several permutations of the key-value presentation order.
         */
        for( int i=0; i<20; i++ ) {
         
            doInsertRandomKeySequenceTest(m, m*m*m*m, trace);

            doInsertRandomSparseKeySequenceTest(m, m*m*m*m, trace);
            
        }
        
    }

    /**
     * Insert dense key-value pairs into the tree in a random order and verify
     * the expected entry traversal afterwards.
     * 
     * @param m
     *            The branching factor. The tree.
     * @param ninserts
     *            The #of distinct key-value pairs to insert.
     * @param trace
     *            The trace level (zero disables most tracing).
     */
    public void doInsertRandomKeySequenceTest(int m, int ninserts, int trace) {

        /*
         * generate keys.  the keys are a dense monotonic sequence.
         */

        int keys[] = new int[ninserts];

        Entry entries[] = new Entry[ninserts];
        
        for( int i=0; i<ninserts; i++ ) {
        
            keys[i] = i+1; // Note: origin one.
            
            entries[i] = new Entry();
            
        }

        doInsertRandomKeySequenceTest(m, keys, entries, trace);
        
    }

    /**
     * Insert a sequence of monotonically increase keys with random spacing into
     * a tree in a random order and verify the expected entry traversal
     * afterwards.
     * 
     * @param m
     *            The branching factor. The tree.
     * @param ninserts
     *            The #of distinct key-value pairs to insert.
     * @param trace
     *            The trace level (zero disables most tracing).
     */
    public void doInsertRandomSparseKeySequenceTest(int m, int ninserts, int trace) {
        
        /*
         * generate random keys.  the keys are a sparse monotonic sequence.
         */
        int keys[] = new int[ninserts];

        Entry entries[] = new Entry[ninserts];
        
        int lastKey = Node.NEGINF;

        for( int i=0; i<ninserts; i++ ) {
        
            int key = r.nextInt(100)+lastKey+1;
            
            keys[i] = key;
            
            entries[i] = new Entry();
            
            lastKey = key;
            
        }

        doInsertRandomKeySequenceTest(m, keys, entries, trace);
        
    }

    /**
     * Insert key value pairs into the tree in a random order and verify
     * the expected entry traversal afterwards.
     * 
     * @param m
     *            The branching factor. The tree.
     * @param keys
     *            The keys.
     * @param entries
     *            The entries.
     * @param trace
     *            The trace level (zero disables most tracing).
     */
    public void doInsertRandomKeySequenceTest(int m, int[] keys,
            Entry[] entries, int trace) {

        doInsertKeySequenceTest(m, keys, entries, getRandomOrder(keys.length),
                trace);

    }

    /**
     * Present a known sequence.
     * 
     * @param m
     *            The branching factor.
     * @param order
     *            The key presentation sequence.
     * @param trace
     *            The trace level.
     */
    public void doKnownKeySequenceTest(int m, int[] order, int trace) {

        int ninserts = order.length;
        
        int keys[] = new int[ninserts];

        Entry entries[] = new Entry[ninserts];
        
        for( int i=0; i<ninserts; i++ ) {
        
            keys[i] = i+1; // Note: origin one.
            
            entries[i] = new Entry();
            
        }

        doInsertKeySequenceTest(m, keys, entries, order, trace);

    }
    
    /**
     * Insert key value pairs into the tree in the specified order and verify
     * the expected entry traversal afterwards.  If the test fails, then the
     * details necessary to recreate the test (m, ninserts, and the order[])
     * are printed out.
     * 
     * @param m
     *            The branching factor. The tree.
     * @param keys
     *            The keys.
     * @param entries
     *            The entries.
     * @param order
     *            The order in which the key-entry pairs will be inserted.
     * @param trace
     *            The trace level (zero disables most tracing).
     */
    public void doInsertKeySequenceTest(int m, int[] keys, Entry[] entries, int[] order, int trace){

        try {
            
            BTree btree = getNoEvictionBTree(m);

            int lastLeafCount = btree.nleaves;

            for (int i = 0; i < keys.length; i++) {

                int key = keys[order[i]];

                Entry entry = entries[order[i]];

                System.err.println("index=" + i + ", key=" + key + ", entry="
                        + entry);

                assertEquals("#entries", i, btree.nentries);

                assertNull(btree.lookup(key));

                if (trace >= 2) {

                    System.err.println("Before insert: index=" + i + ", key="
                            + key);
                    assertTrue(btree.dump(System.err));

                }

                btree.insert(key, entry);

                if (trace >= 2) {

                    System.err.println("After insert: index=" + i + ", key="
                            + key);
                    
                    assertTrue(btree.dump(System.err));

                }

                assertEquals(entry, btree.lookup(key));

                assertEquals("#entries", i + 1, btree.nentries);

                if (btree.nleaves > lastLeafCount) {

                    System.err.println("Split: i=" + i + ", key=" + key
                            + ", nleaves=" + btree.nleaves);

                    if (trace >= 1) {

                        System.err.println("After split: ");

                        assertTrue(btree.dump(System.err));

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

        } catch (AssertionFailedError ex) {
            System.err.println("int m=" + m+";");
            System.err.println("int ninserts="+keys.length+";");
            System.err.print("int[] keys   = new   int[]{");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0)
                    System.err.print(", ");
                System.err.print(keys[order[i]]);
            }
            System.err.println("};");
            System.err.print("Entry[] vals = new Entry[]{");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0)
                    System.err.print(", ");
                System.err.print(entries[order[i]]);
            }
            System.err.println("};");
            System.err.print("int[] order  = new   int[]{");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0)
                    System.err.print(", ");
                System.err.print(order[i]);
            }
            System.err.println("};");
            throw ex;
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
     */
    public void doSplitWithRandomKeySequence(BTree btree,int m, int ninserts) {

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

}
