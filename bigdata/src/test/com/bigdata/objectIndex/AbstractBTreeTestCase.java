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
     * Return a new btree backed by a simple transient store.  The leaf cache
     * will be large and cache evictions will cause exceptions if they occur.
     * This provides an indication if cache evictions are occurring so that
     * the tests of basic tree operations in this test suite are known to be
     * conducted in an environment without incremental writes of leaves onto
     * the store.  This avoids copy-on-write scenarios and let's us test with
     * the knowledge that there should always be a hard reference to a child
     * or parent.
     * 
     * @param branchingFactor
     *            The branching factor.
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

}
