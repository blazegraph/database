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
 * Created on Nov 17, 2006
 */

package com.bigdata.btree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KV;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.RandomKeysGenerator;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.service.ndx.ClientIndexView;

/**
 * Abstract test case for {@link BTree} tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBTreeTestCase extends TestCase2 {

    protected Random r = new Random();

    protected IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_INT);
    
    /**
     * Encodes an integer as a unsigned byte[] key.
     * 
     * @param v
     *            An integer.
     *            
     * @return The sort key for that integer.
     */
    protected byte[] i2k(int v) {
        
        return keyBuilder.reset().append(v).getKey();
        
    }

    /**
     * Logger for the test suites in this package.
     */
    protected static final Logger log = Logger.getLogger
    ( AbstractBTreeTestCase.class
      );

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

    /**
     * Test helper verifies the #of keys and their ordered values.
     * 
     * @param expected
     *            A ground truth node.
     * @param actual
     *            The actual node.
     */
    static public void assertKeys(final IRaba expected,
            final IRaba actual) {

        assertSameRaba(expected, actual);
        
//        // verify the #of defined keys.
//        assertEquals("nkeys", expected.size(), actual.size());
//        // assertEquals("nkeys", expected.nkeys, actual.nkeys);
//
//        // verify ordered values for the defined keys.
//        for (int i = 0; i < expected.size(); i++) {
//
//            assertEquals(0, BytesUtil.compareBytes(expected.get(i), actual
//                    .get(i)));
//
//        }

        /*
         * Verifies that the keys are in sort order.
         */
        final int nkeys = expected.size();
        for (int i = 1; i < nkeys; i++) {

            if (BytesUtil.compareBytes(expected.get(i), expected.get(i - 1)) <= 0) {

                throw new AssertionError("Keys out of order at index=" + i
                        + ", keys=" + expected.toString());

            }

            if (BytesUtil.compareBytes(actual.get(i), actual.get(i - 1)) <= 0) {

                throw new AssertionError("Keys out of order at index=" + i
                        + ", keys=" + actual.toString());

            }

        }

    }
    
    /**
     * Test helper provides backwards compatibility for a large #of tests that
     * were written with <code>int</code> keys. Each key is encoded by the
     * {@link KeyBuilder} before comparison with the key at the corresponding
     * index in the node.
     * 
     * @param keys
     *            An array of the defined <code>int</code> keys.
     * @param node
     *            The node whose keys will be tested.
     */
    public void assertKeys(final int[] keys, final AbstractNode<?> node) {
        
//        // verify the capacity of the keys[] on the node.
//        assertEquals("keys[] capacity", (node.maxKeys + 1) * stride,
//                actualKeys.length);
        
        final int nkeys = keys.length;
        
        // verify the #of defined keys.
        assertEquals("nkeys", nkeys, node.getKeyCount());
        
        // verify ordered values for the defined keys.
        for (int i = 0; i < nkeys; i++) {

            final byte[] expectedKey = keyBuilder.reset().append(keys[i]).getKey();
            
            final byte[] actualKey = node.getKeys().get(i);
            
            if(BytesUtil.compareBytes(expectedKey, actualKey)!=0) {

                fail("keys[" + i + "]: expected="
                        + BytesUtil.toString(expectedKey) + ", actual="
                        + BytesUtil.toString(actualKey));
                
            }
            
        }
        
//        // verify the undefined keys are all NEGINF.
//        for (int i = nkeys * stride; i < actualKeys.length; i++) {
//
//            assertEquals("keys[" + i + "]", (byte) 0, actualKeys[i]);
//
//        }
        
    }

    /**
     * Test helper verifies the #of values, their ordered values, and that all
     * values beyond the last defined value are <code>null</code>.
     * 
     * @param msg
     *            A label, typically the node name.
     * @param values
     *            An array containing the expected defined values. The #of
     *            values in this array should be exactly the #of defined values
     *            (that is, do not include trailing nulls or attempt to size the
     *            array to the branching factor of the tree).
     */
    public void assertValues(String msg, final Object[] values, final Leaf leaf) {

        assert values != null;

        // the expected #of values (size, not capacity).
        final int nvalues = values.length;

        if (msg == null) {

            msg = "";

        }

        if (!leaf.isReadOnly()) {
            /*
             * Verify the capacity of the values[] on the node.
             * 
             * Note: When read only, nkeys==size==capacity.
             */
            assertEquals(msg + "values[] capacity", leaf.maxKeys() + 1, leaf
                    .getValues().capacity());
        }

        // verify the #of defined values (same as the #of defined keys).
        assertEquals(msg + "nvalues", nvalues, leaf.getKeyCount());

        // verify ordered values for the defined values.
        for (int i = 0; i < nvalues; i++) {

            assertEquals(msg + "values[" + i + "]", values[i], leaf.getValues()
                    .get(i));

        }

        // verify the undefined values are all null.
        for (int i = nvalues; i < leaf.getValues().size(); i++) {

            assertEquals(msg + "values[" + i + "]", null, leaf.getValues().get(
                    i));

        }

    }

    public void assertValues(final Object[] values, final Leaf leaf) {

        assertValues("", values, leaf);

    }

    static public void assertSameNodeOrLeaf(final AbstractNode<?> n1,
            final AbstractNode<?> n2) {

        if (n1 == n2)
            return;

        if (n1.isLeaf() && n2.isLeaf()) {

            assertSameLeaf((Leaf) n1, (Leaf) n2);

        } else if (!n1.isLeaf() && !n2.isLeaf()) {

            assertSameNode((Node) n1, (Node) n2);

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
    static public void assertSameNode(final Node n1, final Node n2 ) {

        if( n1 == n2 ) return;
        
        assertEquals("index", n1.btree, n2.btree);

        assertEquals("dirty", n1.isDirty(), n2.isDirty());

        assertEquals("persistent", n1.isPersistent(), n2.isPersistent());

        if (n1.isPersistent()) {
            
            assertEquals("id", n1.getIdentity(), n2.getIdentity());

        }

        assertEquals("minKeys", n1.minKeys(), n2.minKeys());

        assertEquals("maxKeys", n1.maxKeys(), n2.maxKeys());

        assertEquals("branchingFactor", n1.getBranchingFactor(), n2
                .getBranchingFactor());

//        assertEquals("nnodes",n1.nnodes,n2.nnodes);
//        
//        assertEquals("nleaves",n1.nleaves,n2.nleaves);
        
//        assertEquals("nentries", n1.nentries, n2.nentries);

//        assertEquals("nkeys", n1.nkeys, n2.nkeys);
//
//        // make sure that the #of keys on the RABA agrees.
//        assertEquals("keys.size()", n1.nkeys, n1.getKeys().size());
//        assertEquals("keys.size()", n1.nkeys, n2.getKeys().size());

        assertSameNodeData(n1, n2);
        
    }

    /**
     * Compares leaves for the same data.
     * 
     * @param n1
     *            The expected leaf state.
     * @param n2
     *            The actual leaf state.
     */
    static public void assertSameLeaf(final Leaf n1, final Leaf n2) {

        if( n1 == n2 ) return;
        
        assertEquals("index",n1.btree,n2.btree);
        
        assertEquals("dirty", n1.isDirty(), n2.isDirty());

        assertEquals("persistent", n1.isPersistent(), n2.isPersistent());
        
        if (n1.isPersistent()) {
            
            assertEquals("id", n1.getIdentity(), n2.getIdentity());
            
        }

        assertEquals("minKeys", n1.minKeys(), n2.minKeys());

        assertEquals("maxKeys", n1.maxKeys(), n2.maxKeys());

        assertEquals("branchingFactor", n1.getBranchingFactor(), n2
                .getBranchingFactor());

        assertSameLeafData(n1, n2);

    }

    /**
     * Verify all data accessible from {@link IAbstractNodeData}.
     */
    static protected void assertSameAbstractNodeData(
            final IAbstractNodeData n1, final IAbstractNodeData n2) {

        assertEquals("isLeaf", n1.isLeaf(), n2.isLeaf());

        assertEquals("entryCount", n1.getSpannedTupleCount(), n2
                .getSpannedTupleCount());

        assertEquals("keyCount", n1.getKeyCount(), n2.getKeyCount());

        assertKeys(n1.getKeys(), n2.getKeys());

        assertEquals("hasVersionTimestamps", n1.hasVersionTimestamps(), n2
                .hasVersionTimestamps());
        
        if (n1.hasVersionTimestamps()) {

            assertEquals("minimumVersionTimestamp", n1
                    .getMinimumVersionTimestamp(), n2
                    .getMinimumVersionTimestamp());

            assertEquals("maximumVersionTimestamp", n1
                    .getMaximumVersionTimestamp(), n2
                    .getMaximumVersionTimestamp());

        }

    }
    
    /**
     * Verify all data accessible from {@link INodeData}.
     */
    static public void assertSameNodeData(final INodeData n1, final INodeData n2) {

        assertSameAbstractNodeData(n1, n2);

        assertEquals("childCount", n1.getChildCount(), n2.getChildCount());

        for (int i = 0; i < n1.getChildCount(); i++) {

            final long expectedAddr = n1.getChildAddr(i);

            final long actualAddr = n2.getChildAddr(i);

            if (expectedAddr != actualAddr) {

                assertEquals("childAddr(" + i + ")", expectedAddr, actualAddr);

            }

        }

        for (int i = 0; i < n1.getChildCount(); i++) {

            final int expectedChildEntryCount = n1.getChildEntryCount(i);

            final int actualChildEntryCount = n2.getChildEntryCount(i);

            if (expectedChildEntryCount != actualChildEntryCount) {

                assertEquals("childEntryCount(" + i + ")",
                        expectedChildEntryCount, actualChildEntryCount);

            }

        }

     }

    /**
     * Verify all data accessible from {@link ILeafData}.
     */
    static public void assertSameLeafData(final ILeafData n1, final ILeafData n2) {

        assertSameAbstractNodeData(n1, n2);

        assertEquals("#keys!=#vals", n1.getKeyCount(), n1.getValueCount());

        assertEquals("#keys!=#vals", n2.getKeyCount(), n2.getValueCount());

        assertEquals("hasDeleteMarkers", n1.hasDeleteMarkers(), n2
                .hasDeleteMarkers());

        if (n1.hasDeleteMarkers()) {

            for (int i = 0; i < n1.getKeyCount(); i++) {

                assertEquals("deleteMarkers[" + i + "]", n1.getDeleteMarker(i),
                        n2.getDeleteMarker(i));

            }

        }

        assertEquals("hasVersionTimestamps", n1.hasVersionTimestamps(), n2
                .hasVersionTimestamps());

        if (n1.hasVersionTimestamps()) {

            for (int i = 0; i < n1.getKeyCount(); i++) {

                assertEquals("versionTimestamps[" + i + "]", n1
                        .getVersionTimestamp(i), n2.getVersionTimestamp(i));
                
            }
            
        }

        assertSameRaba(n1.getValues(), n2.getValues());
        
    }

    /**
     * Compares the data in two {@link IRaba}s but not their
     * <code>capacity</code> or things which depend on their capacity, such as
     * {@link IRaba#isFull()} and whether or not they are
     * {@link IRaba#isReadOnly()}. If the expected {@link IRaba#isKeys()}, then
     * both must represent keys and the search API will also be tested.
     * 
     * @param expected
     * @param actual
     */
    static public void assertSameRaba(final IRaba expected, final IRaba actual) {

        assertEquals("isKeys", expected.isKeys(), actual.isKeys());

        assertEquals("isEmpty", expected.isEmpty(), actual.isEmpty());

        assertEquals("size", expected.size(), actual.size());

        // random permutation for random access test.
        final int[] order = getRandomOrder(expected.size());
        
        // test using random access.
        for (int i = 0; i < expected.size(); i++) {

            // process the elements in a random order.
            final int j = order[i];
            
            assertEquals("isNull(" + j + ")", expected.isNull(j), actual
                    .isNull(j));

            if (!expected.isNull(j)) {

                // verify same byte[] contents.
                final byte[] ea = expected.get(j);
                final byte[] aa = actual.get(j);

                // same length
                if (ea.length != aa.length)
                    assertEquals("get(" + j + ").length", ea.length, aa.length);

                // same data.
                if (!BytesUtil.bytesEqual(ea, aa))
                    assertEquals("get(" + j + ")", ea, aa);

                // verify same byte[] length reported.
                assertEquals("length(" + j + ")", expected.length(j), actual
                        .length(j));

                // verify copy() gives expected byte[].
                try {
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    final DataOutputStream dos = new DataOutputStream(baos);
                    actual.copy(j, dos);
                    dos.flush();
                    assertEquals("copy(" + j + ",dos)", expected.get(j), baos
                            .toByteArray());
                } catch (IOException ex) {
                    fail("Not expecting exception", ex);
                }
                
            } else {

                assertEquals("get(" + j + ")", null, actual.get(j));

                // verify actual throws the expected exception.
                try {
                    actual.length(j);
                    fail("Expecting: " + NullPointerException.class);
                } catch (NullPointerException ex) {
                    if (log.isDebugEnabled())
                        log.debug("Ignoring expected exception: " + ex);
                }

                // verify actual throws the expected exception.
                try {
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    final DataOutputStream dos = new DataOutputStream(baos);
                    actual.copy(j, dos);
                    fail("Expecting: " + NullPointerException.class);
                } catch (NullPointerException ex) {
                    if (log.isDebugEnabled())
                        log.debug("Ignoring expected exception: " + ex);
                } catch (RuntimeException ex) {
                    fail("Not expecting exception: "+ex, ex);
                }

            }
            
        }
        
        // test using iterator access.
        {
            
            final Iterator<byte[]> eitr = expected.iterator();
            final Iterator<byte[]> aitr = actual.iterator();
            int i = 0;
            while (eitr.hasNext()) {

                assertTrue("hasNext", aitr.hasNext());
                
                // verify same byte[] (compare data, may both be null).
                assertEquals("byte[" + i + "]", eitr.next(), aitr.next());

                i++;
                
            }
            
            assertFalse("hasNext", aitr.hasNext());
            
        }

        // test search API (only for B+Tree keys).
        if (expected.isKeys()) {

            final Random r = new Random();
            
            for (int i = 0; i < expected.size(); i++) {

                final int expectedIndex = i;
                
                final byte[] key = expected.get(expectedIndex);

                { // search at the key.
                    
                    final int actualIndex = actual.search(key);

                    if (actualIndex != expectedIndex) {

                        fail("search(" + BytesUtil.toString(key) + ")" + //
                                ": expectedIndex=" + expectedIndex + //
                                ", actualIndex=" + actualIndex + //
                                ",\nexpected=" + expected + //
                                ",\nactual=" + actual//
                        );

                    }
                }
                
                { // search at key plus a random byte[] suffix.
                    
                    // random suffix length.
                    final int suffixLength = r.nextInt(1 + (key.length / 2)) + 1;
                    
                    // random fill of entire key.
                    final byte[] key2 = new byte[key.length + suffixLength];
                    r.nextBytes(key2);
                    
                    // copy shared prefix (all of the original key).
                    System.arraycopy(key, 0, key2, 0, key.length);
                    
                    // expected insert position (or index iff found).
                    final int epos = expected.search(key2);
                    
                    // actual result from search on the coded raba.
                    final int apos = actual.search(key2);
                    
                    assertEquals("search with random prefix", epos, apos);
                    
                }
                { // search at random length prefix of the key.
                    
                    // random prefix length (may be zero).
                    final int prefixLength = Math.max(0,
                            r.nextInt(Math.max(1,key.length)) - 1);
                    
                    // copy shared prefix.
                    final byte[] key2 = new byte[prefixLength];
                    System.arraycopy(key, 0, key2, 0, prefixLength);

                    // expected insert position (or index iff found).
                    final int epos = expected.search(key2);
                    
                    // actual result from search on the coded raba.
                    final int apos = actual.search(key2);
                    
                    assertEquals("search with random suffix", epos, apos);
                    
                }

            }
            
        }

    }

    /**
     * Special purpose helper used to vet {@link Node#childAddr}.
     * 
     * @param childAddr
     *            An array all of whose values will be tested against the
     *            corresponding child identities in the node.
     * @param node
     *            The node.
     */
    public void assertChildKeys(final long[] childAddr, final Node node) {

        final int nChildAddr = childAddr.length;
        
//        long[] actualAddr = node.childAddr;
        
//        // verify the capacity of the childAddr[] on the node.
//        assertEquals("childAddr[] capacity", node.getBranchingFactor() + 1,
//                node.getChildCount());

        // verify the #of children.
        assertEquals("childChild", nChildAddr, node.getChildCount());
        
        // verify the #of defined keys
        assertEquals("nkeys", nChildAddr, node.getKeyCount() + 1);
        
        // verify ordered values for the defined keys.
        for (int i = 0; i < nChildAddr; i++) {

            assertEquals("childAddr[" + i + "]", childAddr[i], node
                    .getChildAddr(i));

        }

//        // verify the undefined keys are all NULL.
//        for (int i = nChildAddr; i < actualAddr.length; i++) {
//
//            assertEquals("childAddr[" + i + "]", IIdentityAccess.NULL,
//                    actualAddr[i]);
//
//        }
        
    }

    /**
     * Validate the keys in the node.
     * 
     * @param keys
     *            An array all of whose entries will be tested against the
     *            corresponding keys in the node.
     * @param node
     *            The node.
     */
    public void assertKeys(final byte[][] keys, final AbstractNode<?> node) {

//        // verify the capacity of the keys[] on the node.
//        assertEquals("keys[] capacity", (node.maxKeys + 1) * stride,
//                actualKeys.length);
        
        // verify the #of defined keys.
        assertEquals("nkeys", keys.length, node.getKeyCount());

        // verify ordered values for the defined keys.
        for (int i = 0; i < keys.length; i++) {

            if (BytesUtil.compareBytes(keys[i], node.getKeys().get(i)) != 0) {

                fail("expected=" + BytesUtil.toString(keys[i]) + ", actual="
                        + BytesUtil.toString(node.getKeys().get(i)));

            }

        }
        
//        // verify the undefined keys are all NEGINF.
//        for (int i = node.nkeys * stride; i < actualKeys.length; i++) {
//
//            assertEquals("keys[" + i + "]", (int) 0, actualKeys[i]);
//
//        }
        
    }

    /**
     * Special purpose helper used to vet the per-child entry counts for an
     * {@link INodeData}.
     * 
     * @param expected
     *            An array all of whose values will be tested against the
     *            corresponding elements in the node as returned by
     *            {@link INodeData#getChildEntryCounts()}. The sum of the
     *            expected array is also tested against the value returned by
     *            {@link IAbstractNodeData#getSpannedTupleCount()}.
     * @param node
     *            The node.
     */
    public void assertEntryCounts(final int[] expected, final INodeData node) {

        final int len = expected.length;
        
//        final int[] actual = (int[]) node.getChildEntryCounts();
        
//        // verify the capacity of the keys[] on the node.
//        assertEquals("childEntryCounts[] capacity", node.getBranchingFactor()+1, actual.length );
        
        // verify the #of defined elements.
        assertEquals("nchildren", len, node.getChildCount());
        
        // verify defined elements.
        int nentries = 0;
        for (int i = 0; i < len; i++) {

            assertEquals("childEntryCounts[" + i + "]", expected[i], node
                    .getChildEntryCount(i));

            nentries += expected[i];
            
        }
        
        // verify total #of spanned entries.
        assertEquals("nentries", nentries, node.getSpannedTupleCount());

//        // verify the undefined keys are all ZERO(0).
//        for( int i=len; i<actual.length; i++ ) {
//            
//            assertEquals("keys[" + i + "]", 0, actual[i]);
//            
//        }
        
    }

    /**
     * Return a new btree backed by a simple transient store that will NOT evict
     * leaves or nodes onto the store. The leaf cache will be large and cache
     * evictions will cause exceptions if they occur. This provides an
     * indication if cache evictions are occurring so that the tests of basic
     * tree operations in this test suite are known to be conducted in an
     * environment without incremental writes of leaves onto the store. This
     * avoids copy-on-write scenarios and let's us test with the knowledge that
     * there should always be a hard reference to a child or parent.
     * 
     * @param branchingFactor
     *            The branching factor.
     */
    public BTree getBTree(final int branchingFactor) {
        
        return getBTree(branchingFactor , DefaultTupleSerializer.newInstance());
        
    }

    public BTree getBTree(final int branchingFactor,
            final ITupleSerializer tupleSer) {

        final IRawStore store = new SimpleMemoryRawStore();

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setBranchingFactor(branchingFactor);

        metadata.setTupleSerializer(tupleSer);

        // override the BTree class.
        metadata.setBTreeClassName(NoEvictionBTree.class.getName());

        return (NoEvictionBTree) BTree.create(store, metadata);
        
    }
    
    /**
     * Specifies a {@link NoEvictionListener}.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class NoEvictionBTree extends BTree {

        /**
         * @param store
         * @param checkpoint
         * @param metadata
         */
        public NoEvictionBTree(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata) {
         
            super(store, checkpoint, metadata);
            
        }
        
        protected HardReferenceQueue<PO> newWriteRetentionQueue() {

            return new HardReferenceQueue<PO>(//
                    new NoEvictionListener(),//
                    10000,//
                    10//
            );

        }
        
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
     * Once all keys have been inserted into the tree the keys are removed in
     * forward order (from the smallest to the largest). This stresses a
     * specific pattern of joining leaves and nodes together with their right
     * sibling.
     * 
     * @param m
     *            The branching factor.
     * @param ninserts
     *            The #of keys to insert.
     */
    public void doSplitWithIncreasingKeySequence(final BTree btree,
            final int m, final int ninserts) {

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
        
        final int[] keys = new int[ninserts];

        final SimpleEntry[] entries = new SimpleEntry[ninserts];
        
        int lastKey = 1;
        
        for (int i = 0; i < ninserts; i++) {
        
            keys[i] = lastKey;
            
            entries[i] = new SimpleEntry();
            
            lastKey += 1;
        
        }

        /*
         * Do inserts.
         */
        
        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            final int ikey = keys[i];
            
            final SimpleEntry entry = entries[i];

            if (i > 0 && i % 10000 == 0) {
    
                if (log.isInfoEnabled())
                    log.info("i=" + i + ", key=" + ikey);
                
            }

            assertEquals("#entries",i,btree.nentries);
            
            final byte[] key = KeyBuilder.asSortKey(ikey);
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

//                System.err.println("Split: i=" + i + ", key=" + key
//                        + ", nleaves=" + btree.nleaves);

                lastLeafCount = btree.nleaves;

            }

        }

        // Note: The height, #of nodes, and #of leaves is path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(/*Level.DEBUG,*/System.err));

        /*
         * Verify entries in the expected order.
         */
        assertSameIterator(entries, btree.rangeIterator());

        // remove keys in forward order.
        {
            
            for( int i=0; i<keys.length; i++ ) {
                
                byte[] key = KeyBuilder.asSortKey(keys[i]);
                
                assertEquals(entries[i],btree.lookup(key));
                assertEquals(entries[i],btree.remove(key));
                assertEquals(null,btree.lookup(key));
                
            }
            
        }
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

    }
    
    /**
     * Creates a sequence of keys in decreasing order and inserts them into the
     * tree. Note that you do not know, in general, how many inserts it will
     * take to split the root node since the split decisions are path dependent.
     * They depend on the manner in which the leaves get filled, whether or not
     * holes are created in the leaves, etc.
     * 
     * Once all keys have been inserted into the tree the keys are removed in
     * reverse order (from the largest to the smallest). This stresses a
     * specific pattern of joining leaves and nodes together with their left
     * sibling.
     * 
     * @param m
     *            The branching factor.
     * @param ninserts
     *            The #of keys to insert.
     */
    public void doSplitWithDecreasingKeySequence(final BTree btree,
            final int m, final int ninserts) {

        if(log.isInfoEnabled())
            log.info("m="+m+", ninserts="+ninserts);
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

        /*
         * Generate a series of external keys in decreasing order. When we
         * insert these keys in sequence, the result is that all inserts go into
         * the left-most leaf (the original leaf).
         */
        
        final int[] keys = new int[ninserts];
        final SimpleEntry[] entries = new SimpleEntry[ninserts];
        final SimpleEntry[] reverseEntries = new SimpleEntry[ninserts];
        {
            int lastKey = ninserts;
            int reverseIndex = ninserts - 1;
            for (int i = 0; i < ninserts; i++) {
                keys[i] = lastKey;
                SimpleEntry entry = new SimpleEntry();
                entries[i] = entry;
                reverseEntries[reverseIndex--] = entry;
                lastKey -= 1;
            }
        }

        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            final int ikey = keys[i];
            
            final SimpleEntry entry = entries[i];
            
            if( i>0 && i%10000 == 0 ) {
            
                if(log.isInfoEnabled())
                    log.info("i=" + i + ", key=" + ikey);
                
            }

            assertEquals("#entries",i,btree.nentries);
            
            final byte[] key = KeyBuilder.asSortKey(ikey);
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

//                System.err.println("Split: i=" + i + ", key=" + key
//                        + ", nleaves=" + btree.nleaves);

                lastLeafCount = btree.nleaves;

            }

        }

        /*
         * Verify entries in the expected order.
         */
        assertSameIterator(reverseEntries, btree.rangeIterator());

        // Note: The height, #of nodes, and #of leaves is path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(System.err));
        
        // remove keys in reverse order.
        {
        
            for( int i=0; i<keys.length; i++ ) {
                
                final byte[] key = KeyBuilder.asSortKey(keys[i]);
                
                assertEquals(entries[i],btree.lookup(key));
                assertEquals(entries[i],btree.remove(key));
                assertEquals(null,btree.lookup(key));

            }
            
        }
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

    }
    
    /**
     * Stress test helper inserts random permutations of keys into btrees of
     * order m for several different btrees, #of keys to be inserted, and
     * permutations of keys. Several random permutations of dense and sparse
     * keys are inserted. The #of keys to be inserted is also varied.
     */
    public void doSplitTest(final int m, final int trace) {
        
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
        
//        /*
//         * Try several permutations of the key-value presentation order.
//         */
//        for( int i=0; i<20; i++ ) {
//         
//            doInsertRandomKeySequenceTest(m, m*m*m*m, trace).getStore().close();
//
//            doInsertRandomSparseKeySequenceTest(m, m*m*m*m, trace).getStore().close();
//            
//        }

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
    public BTree doInsertRandomKeySequenceTest(final int m, final int ninserts,
            final int trace) {

        /*
         * generate keys.  the keys are a dense monotonic sequence.
         */

        final int keys[] = new int[ninserts];

        final SimpleEntry entries[] = new SimpleEntry[ninserts];
        
        for( int i=0; i<ninserts; i++ ) {
        
            keys[i] = i+1; // Note: origin one.
            
            entries[i] = new SimpleEntry();
            
        }

        return doInsertRandomKeySequenceTest(m, keys, entries, trace);
        
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
     * 
     * @return The populated {@link BTree}.
     */
    public BTree doInsertRandomSparseKeySequenceTest(final int m,
            final int ninserts, final int trace) {

        /*
         * generate random keys. the keys are a sparse monotonic sequence.
         */
        final int keys[] = new int[ninserts];

        final SimpleEntry entries[] = new SimpleEntry[ninserts];
        
        int lastKey = 0;

        for( int i=0; i<ninserts; i++ ) {
        
            final int key = r.nextInt(100)+lastKey+1;
            
            keys[i] = key;
            
            entries[i] = new SimpleEntry();
            
            lastKey = key;
            
        }

        return doInsertRandomKeySequenceTest(m, keys, entries, trace);
        
    }

    /**
     * Insert key value pairs into the tree in a random order and verify the
     * expected entry traversal afterwards.
     * 
     * @param m
     *            The branching factor. The tree.
     * @param keys
     *            The keys.
     * @param entries
     *            The entries.
     * @param trace
     *            The trace level (zero disables most tracing).
     * 
     * @return The populated {@link BTree}.
     */
    public BTree doInsertRandomKeySequenceTest(int m, int[] keys,
            SimpleEntry[] entries, int trace) {

        return doInsertKeySequenceTest(m, keys, entries,
                getRandomOrder(keys.length), trace);

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
    public void doKnownKeySequenceTest(final int m, final int[] order, final int trace) {

        final int ninserts = order.length;
        
        final int keys[] = new int[ninserts];

        final SimpleEntry entries[] = new SimpleEntry[ninserts];
        
        for( int i=0; i<ninserts; i++ ) {
        
            keys[i] = i+1; // Note: origin one.
            
            entries[i] = new SimpleEntry();
            
        }

        doInsertKeySequenceTest(m, keys, entries, order, trace);

    }
    
    /**
     * Insert key value pairs into the tree in the specified order and verify
     * the expected entry traversal afterwards. If the test fails, then the
     * details necessary to recreate the test (m, ninserts, and the order[]) are
     * printed out.
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
     * 
     * @return The populated {@link BTree}.
     */
    protected BTree doInsertKeySequenceTest(final int m, final int[] keys,
            final SimpleEntry[] entries, final int[] order, final int trace) {

        final BTree btree = getBTree(m);

        try {
            
            int lastLeafCount = btree.nleaves;

            for (int i = 0; i < keys.length; i++) {

                final int ikey = keys[order[i]];

                final SimpleEntry entry = entries[order[i]];

                if( i>0 && i%10000 == 0 ) {
                    
                    if(log.isInfoEnabled()) log.info("index=" + i + ", key=" + ikey + ", entry="
                            + entry);
                    
                }

                assertEquals("#entries", i, btree.nentries);

                final byte[] key = KeyBuilder.asSortKey(ikey);
                
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

                    if (trace >= 1) {

                        System.err.println("Split: i=" + i + ", key="
                                + BytesUtil.toString(key) + ", nleaves="
                                + btree.nleaves);
                        
                    }

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
            assertSameIterator(entries, btree.rangeIterator());

            return btree;
            
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
            System.err.print("SimpleEntry[] vals = new SimpleEntry[]{");
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
     * Creates a sequence of dense keys in random order and inserts them into
     * the tree. Note that the split decision points are path dependent and can
     * not be predicated given random inserts.
     * 
     * @param m
     *            The branching factor.
     * 
     * @param ninserts
     *            The #of keys to insert.
     */
    public BTree doSplitWithRandomDenseKeySequence(final BTree btree,
            final int m, final int ninserts) {

        if (log.isInfoEnabled())
            log.info("m=" + m + ", ninserts=" + ninserts);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);

        /*
         * Generate a sequence of keys in increasing order and a sequence of
         * random indices into the keys (and values) that is used to present
         * the key-value pairs in random order to insert(key,value).
         */
        
        final int[] keys = new int[ninserts];
        final SimpleEntry[] entries = new SimpleEntry[ninserts];
        
        int lastKey = 1;
        for( int i=0; i<ninserts; i++) {
            keys[i] = lastKey;
            entries[i] = new SimpleEntry();
            lastKey+=1;
        }
        
        // Random indexing into the generated keys and values.
        final int[] order = getRandomOrder(ninserts);

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

        if(log.isInfoEnabled())
            log.info(btree.getBtreeCounters().toString());

        return btree;
        
    }

    protected void doRandomKeyInsertTest(final BTree btree, final int[] keys,
            final SimpleEntry[] entries, final int[] order) {

        if (log.isInfoEnabled())
            log.info("m=" + btree.getBranchingFactor() + ", nkeys="
                    + keys.length);

        /*
         * Insert keys into the tree.
         */

        int lastLeafCount = btree.nleaves;
        
        for (int i = 0; i < keys.length; i++) {

            final int ikey = keys[order[i]];
            
            final SimpleEntry entry = entries[order[i]];
            
            if( i >0 && i%10000 == 0 ) {
            
                log.info("i="+i+", key="+ikey);
                
            }

            assertEquals("#entries",i,btree.nentries);

            final byte[] key = KeyBuilder.asSortKey(ikey);
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);

            assertEquals(entry,btree.lookup(key));

            assertEquals("#entries",i+1,btree.nentries);

            if (btree.nleaves > lastLeafCount) {

//                System.err.println("Split: i=" + i + ", key=" + key
//                        + ", nleaves=" + btree.nleaves);

                lastLeafCount = btree.nleaves;

            }

        }

        /*
         * Verify entries in the expected order. While we used a random
         * presentation order, the entries MUST now be in the original generated
         * order.
         */
        assertSameIterator(entries,btree.rangeIterator());

        // Note: The height, #of nodes, and #of leaves are path dependent.
        assertEquals("#entries", keys.length, btree.nentries);

        assertTrue(btree.dump(Level.ERROR,System.err));
        
        if(log.isInfoEnabled())
            log.info(btree.getBtreeCounters().toString());

    }


    /**
     * Stress test helper performs random inserts, removal and lookup operations
     * and compares the behavior of the {@link BTree} against ground truth as
     * tracked by a {@link TreeMap}.
     * 
     * Note: This test uses dense keys, but that is not a requirement.
     * 
     * @param m
     *            The branching factor
     * @param nkeys
     *            The #of distinct keys.
     * @param ntrials
     *            The #of trials.
     */
    public void doInsertLookupRemoveStressTest(final int m, final int nkeys,
            final int ntrials) {

        if (log.isInfoEnabled())
            log.info("m=" + m + ", nkeys=" + nkeys + ", ntrials=" + ntrials);

        final Integer[] keys = new Integer[nkeys];

        final SimpleEntry[] vals = new SimpleEntry[nkeys];

        for (int i = 0; i < nkeys; i++) {

            keys[i] = i + 1; // Note: this produces dense keys with origin
                             // ONE(1).

            vals[i] = new SimpleEntry();

        }

        final BTree btree = getBTree(m);

        /*
         * Run test.
         */
        final Map<Integer, SimpleEntry> expected = new TreeMap<Integer, SimpleEntry>();

        for( int i=0; i<ntrials; i++ ) {
            
            final boolean insert = r.nextBoolean();
            
            final int index = r.nextInt(nkeys);
            
            final Integer ikey = keys[index];
            
            final byte[] key = KeyBuilder.asSortKey(ikey);
            
            final SimpleEntry val = vals[index];
            
            if( insert ) {
                
//                System.err.println("insert("+key+", "+val+")");
                final SimpleEntry old = expected.put(ikey, val);
                
                final SimpleEntry old2 = (SimpleEntry) btree.insert(key, val);
                
                assertTrue(btree.dump(Level.ERROR,System.err));
                
                assertEquals(old, old2);

            } else {
                
//                System.err.println("remove("+key+")");
                final SimpleEntry old = expected.remove(ikey);
                
                final SimpleEntry old2 = (SimpleEntry) SerializerUtil.deserialize(btree.remove(key));
                
                assertTrue(btree.dump(Level.ERROR,System.err));
                
                assertEquals(old, old2);
                
            }

            if( i % 100 == 0 ) {

                /*
                 * Validate the keys and entries.
                 */
                
                assertEquals("#entries", expected.size(), btree.getEntryCount());
                
                Iterator<Map.Entry<Integer,SimpleEntry>> itr = expected.entrySet().iterator();
                
                while( itr.hasNext()) { 
                    
                    Map.Entry<Integer,SimpleEntry> entry = itr.next();
                    
                    final byte[] tmp = KeyBuilder.asSortKey(entry.getKey()); 
                    
                    assertEquals("lookup(" + entry.getKey() + ")", entry
                            .getValue(), btree.lookup(tmp));
                    
                }
                
            }
            
        }
        
        assertTrue( btree.dump(System.err) );
        
        if(log.isInfoEnabled()) 
            log.info(btree.getBtreeCounters().toString());
    
    }

    /**
     * Stress test for building up a tree and then removing all keys in a random
     * order. The test populates a btree with enough keys to split the root leaf
     * at least once then verifies that delete correctly removes each keys and
     * any unused leaves and finally replaces the root node with a root leaf.
     * All inserted keys are eventually deleted by this test and the end state
     * is an empty btree of height zero(0) having a single root leaf.
     */
    public void doRemoveStructureStressTest(final int m, final int nkeys) {
        
        if (log.isInfoEnabled())
            log.info("m=" + m + ", nkeys=" + nkeys);

        final BTree btree = getBTree(m);
        
        final byte[][] keys = new byte[nkeys][];
        
        final SimpleEntry[] vals = new SimpleEntry[nkeys];

        for( int i=0; i<nkeys; i++ ) {
            
            keys[i] = KeyBuilder.asSortKey(i+1); // Note: this produces dense keys with origin ONE(1).
            
            vals[i] = new SimpleEntry();
            
        }
        
        /*
         * populate the btree.
         */
        for( int i=0; i<nkeys; i++) {
            
            // lookup does not find key.
            assertNull(btree.insert(keys[i], vals[i]));
            
            // insert key and val.
            assertEquals(vals[i],btree.lookup(keys[i]));

            // reinsert finds key and returns existing value.
            assertEquals(vals[i],btree.insert(keys[i], vals[i]));

            assertEquals("size", i + 1, btree.getEntryCount());
            
        }
        
        /*
         * verify the total order.
         */
        assertSameIterator(vals, btree.rangeIterator());
        
        assertTrue(btree.dump(Level.ERROR,System.out));
        
        /*
         * Remove the keys one by one, verifying that leafs are deallocated
         */
        
        final int[] order = getRandomOrder(nkeys);
        
        for( int i=0; i<nkeys; i++ ) {

            final byte[] key = keys[order[i]];
            
            SimpleEntry val = vals[order[i]];
            
            //System.err.println("i="+i+", key="+key+", val="+val);
            
            // lookup finds the key, return the correct value.
            assertEquals("lookup("+key+")", val,btree.lookup(key));
            
            // remove returns the existing key.
            assertEquals("remove(" + key+")", val, btree.remove(key));
            
            // verify structure.
            assertTrue(btree.dump(Level.ERROR,System.out));

            // lookup no longer finds the key.
            assertNull("lookup("+key+")",BytesUtil.toString(btree.lookup(key)));

        }
        
        /*
         * Verify the post-condition for the tree, which is an empty root leaf.
         * If the height, #of nodes, or #of leaves are reported incorrectly then
         * empty leaves probably were not removed from the tree or the root node
         * was not converted into a root leaf when the tree reached m entries.
         */
        assertTrue(btree.dump(Level.ERROR,System.out));
        assertEquals("#entries", 0, btree.nentries);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("height", 0, btree.height);
        
        if (log.isInfoEnabled())
            log.info(btree.getBtreeCounters().toString());
        
    }
    
    /**
     * A suite of tests designed to verify that one btree correctly represents
     * the information present in a ground truth btree. The test verifies the
     * #of entries, key type, the {@link AbstractBTree#rangeIterator()}, and
     * also both lookup by key and lookup by entry index. The height, branching
     * factor, #of nodes and #of leaves may differ (the test does not presume
     * that the btrees were built with the same branching factor, but merely
     * with the same data and key type).
     * 
     * @param expected
     *            The ground truth btree.
     * @param actual
     *            The btree that is being validated.
     */
    static public void assertSameBTree(final AbstractBTree expected,
            final IIndex actual) {

        assert expected != null;
        
        assert actual != null;
        
        // Must be the same "index".
        assertEquals("indexUUID", expected.getIndexMetadata().getIndexUUID(),
                actual.getIndexMetadata().getIndexUUID());
        
//        // The #of entries must agree.
//        assertEquals("entryCount", expected.getEntryCount(), actual
//                .rangeCount(null, null));

        /*
         * Verify the forward tuple iterator.
         * 
         * Note: This compares the total ordering of the actual btree against
         * the total ordering of a ground truth BTree <p> Note: This uses the
         * {@link AbstractBTree#rangeIterator()} method. Due to the manner in
         * which that iterator is implemented, the iterator does NOT rely on the
         * separator keys. Therefore while this validates the total order it
         * does NOT validate that the index may be searched by key (or by entry
         * index).
         */
        {
        
            final long actualTupleCount = doEntryIteratorTest(expected
                    .rangeIterator(), actual.rangeIterator());

            // verifies based on what amounts to an exact range count.
            assertEquals("entryCount", expected.getEntryCount(),
                    actualTupleCount);
            
        }

        /*
         * Verify the reverse tuple iterator.
         */
        {
            
            final long actualTupleCount = doEntryIteratorTest(//
                    expected.rangeIterator(null/* fromKey */, null/* toKey */,
                            0/* capacity */, IRangeQuery.KEYS
                                    | IRangeQuery.VALS | IRangeQuery.REVERSE,
                            null/* filter */),
                    //
                    actual.rangeIterator(null/* fromKey */, null/* toKey */,
                            0/* capacity */, IRangeQuery.KEYS
                                    | IRangeQuery.VALS | IRangeQuery.REVERSE,
                            null/* filter */));

            // verifies based on what amounts to an exact range count.
            assertEquals("entryCount", expected.getEntryCount(),
                    actualTupleCount);

        }

        /*
         * Extract the ground truth mapping from the input btree.
         */
        final byte[][] keys = new byte[expected.getEntryCount()][];
        
        final byte[][] vals = new byte[expected.getEntryCount()][];
        
        getKeysAndValues(expected, keys, vals);
        
        /*
         * Verify lookup against the segment with random keys choosen from
         * the input btree. This vets the separatorKeys. If the separator
         * keys are incorrect then lookup against the index segment will
         * fail in various interesting ways.
         */
        doRandomLookupTest("actual", actual, keys, vals);
 
        /*
         * Verify lookup by entry index with random keys. This vets the
         * childEntryCounts[] on the nodes of the generated index segment.
         * If the are wrong then this test will fail in various interesting
         * ways.
         */
        if(actual instanceof AbstractBTree) {

            doRandomIndexOfTest("actual", ((AbstractBTree) actual), keys, vals);
            
        }

        /*
         * Examine the btree for inconsistencies (we also examine the ground
         * truth btree for inconsistencies to be paranoid).
         */

        if(log.isInfoEnabled())
            log.info("Examining expected tree for inconsistencies");
        assert expected.dump(System.err);

        /*
         * Note: An IndexSegment can underflow a leaf or node if rangeCount was
         * an overestimate so we can't run this task against an IndexSegment.
         */
        if(actual instanceof /*Abstract*/BTree) {
            if(log.isInfoEnabled())
                log.info("Examining actual tree for inconsistencies");
            assert ((AbstractBTree)actual).dump(System.err);
        }

    }

    /**
     * Compares the total ordering of two B+Trees as revealed by their range
     * iterators
     * 
     * @param expected
     *            The ground truth iterator.
     * 
     * @param actual
     *            The iterator to be tested.
     * 
     * @return The #of tuples that were visited in <i>actual</i>.
     * 
     * @see #doRandomLookupTest(String, AbstractBTree, byte[][], Object[])
     * @see #doRandomIndexOfTest(String, AbstractBTree, byte[][], Object[])
     */
    static private long doEntryIteratorTest(
            final ITupleIterator<?> expectedItr,
            final ITupleIterator<?> actualItr
            ) {

        int index = 0;

        long actualTupleCount = 0L;
        
        while( expectedItr.hasNext() ) {
            
            if( ! actualItr.hasNext() ) {
                
                fail("The iterator is not willing to visit enough entries");
                
            }
            
            final ITuple<?> expectedTuple = expectedItr.next();
            
            final ITuple<?> actualTuple = actualItr.next();
            
            actualTupleCount++;
            
            final byte[] expectedKey = expectedTuple.getKey();
            
            final byte[] actualKey = actualTuple.getKey();

//            System.err.println("index=" + index + ": key expected="
//                    + BytesUtil.toString(expectedKey) + ", actual="
//                    + BytesUtil.toString(actualKey));

            try {
                
                assertEquals(expectedKey, actualKey);
                
            } catch (AssertionFailedError ex) {
                
                /*
                 * Lazily generate message.
                 */
                fail("Keys differ: index=" + index + ", expected="
                        + BytesUtil.toString(expectedKey) + ", actual="
                        + BytesUtil.toString(actualKey), ex);
                
            }

            if (expectedTuple.isDeletedVersion()) {

                assert actualTuple.isDeletedVersion();

            } else {

                final byte[] expectedVal = expectedTuple.getValue();

                final byte[] actualVal = actualTuple.getValue();

                try {

                    assertSameValue(expectedVal, actualVal);

                } catch (AssertionFailedError ex) {
                    /*
                     * Lazily generate message.
                     */
                    fail("Values differ: index="
                            + index
                            + ", key="
                            + BytesUtil.toString(expectedKey)
                            + ", expected="
                            + (expectedVal instanceof byte[] ? BytesUtil
                                    .toString((byte[]) expectedVal)
                                    : expectedVal)
                            + ", actual="
                            + (actualVal instanceof byte[] ? BytesUtil
                                    .toString((byte[]) actualVal) : actualVal),
                            ex);

                }

            }

            if (expectedTuple.getVersionTimestamp() != actualTuple
                    .getVersionTimestamp()) {
                /*
                 * Lazily generate message.
                 */
                assertEquals("timestamps differ: index=" + index + ", key="
                        + BytesUtil.toString(expectedKey), expectedTuple
                        .getVersionTimestamp(), actualTuple
                        .getVersionTimestamp());

            }
            
            index++;
            
        }
        
        if( actualItr.hasNext() ) {
            
            fail("The iterator is willing to visit too many entries");
            
        }
        
        return actualTupleCount;

    }

    /**
     * Extract all keys and values from the btree in key order.  The caller must
     * correctly dimension the arrays before calling this method.
     * 
     * @param btree
     *            The btree.
     * @param keys
     *            The keys in key order (out).
     * @param vals
     *            The values in key order (out).
     */
    static public void getKeysAndValues(final AbstractBTree btree, final byte[][] keys,
            final byte[][] vals) {
        
        final ITupleIterator<?> itr = btree.rangeIterator();

        int i = 0;
        
        while( itr.hasNext() ) {

            final ITuple<?> tuple= itr.next();
            
            final byte[] val = tuple.getValue();
            
            final byte[] key = tuple.getKey();

            assert val != null;
            
            assert key != null;
            
            keys[i] = key;
            
            vals[i] = val;
            
            i++;
            
        }
        
    }
    
    /**
     * Tests the performance of random {@link IIndex#lookup(Object)}s on the
     * btree. This vets the separator keys and the childAddr and/or childRef
     * arrays since those are responsible for lookup.
     * 
     * @param label
     *            A descriptive label for error messages.
     * 
     * @param btree
     *            The btree.
     * 
     * @param keys
     *            the keys in key order.
     * 
     * @param vals
     *            the values in key order.
     */
    static public void doRandomLookupTest(final String label,
            final IIndex btree, final byte[][] keys, final byte[][] vals) {

        final int nentries = keys.length;//btree.rangeCount(null, null);

        if (log.isInfoEnabled())
            log.info("\ncondition: " + label + ", nentries=" + nentries);

        final int[] order = getRandomOrder(nentries);

        final long begin = System.currentTimeMillis();

        final boolean randomOrder = true;

        for (int i = 0; i < nentries; i++) {

            final int entryIndex = randomOrder ? order[i] : i;
            
            final byte[] key = keys[entryIndex];
        
            final byte[] val = btree.lookup(key);

            if (val == null && true) {

                // Note: This exists only as a debug point.

                btree.lookup(key);

            }

            final byte[] expectedVal = vals[entryIndex];

            assertEquals(expectedVal, val);
            
        }
 
        if (log.isInfoEnabled()) {
            
            final long elapsed = System.currentTimeMillis() - begin;

            log.info(label + " : tested " + nentries
                + " keys order in " + elapsed + "ms");
        
//        log.info(label + " : " + btree.getCounters().asXML(null/*filter*/));
            
        }
        
    }

    /**
     * Tests the performance of random lookups of keys and values by entry
     * index. This vets the separator keys and childRef/childAddr arrays, which
     * are used to lookup the entry index for a key, and also vets the
     * childEntryCount[] array, since that is responsible for lookup by entry
     * index.
     * 
     * @param label
     *            A descriptive label for error messages.
     * @param btree
     *            The btree.
     * @param keys
     *            the keys in key order.
     * @param vals
     *            the values in key order.
     */
    static public void doRandomIndexOfTest(final String label,
            final AbstractBTree btree, 
            final byte[][] keys, final byte[][] vals) {

        final int nentries = keys.length;//btree.getEntryCount();

        if (log.isInfoEnabled())
            log.info("\ncondition: " + label + ", nentries=" + nentries);

        final int[] order = getRandomOrder(nentries);

        final long begin = System.currentTimeMillis();

        final boolean randomOrder = true;

        for (int i = 0; i < nentries; i++) {

            final int entryIndex = randomOrder ? order[i] : i;

            final byte[] key = keys[entryIndex];

            assertEquals("indexOf", entryIndex, btree.indexOf(key));

            final byte[] expectedVal = vals[entryIndex];

            assertEquals("keyAt", key, btree.keyAt(entryIndex));

            assertEquals("valueAt", expectedVal, btree.valueAt(entryIndex));

        }

        if (log.isInfoEnabled()) {

            final long elapsed = System.currentTimeMillis() - begin;

            log.info(label + " : tested " + nentries + " keys in " + elapsed
                    + "ms");

// log.info(label + " : " + btree.getBtreeCounters());
        }

    }
    
    /**
     * Method verifies that the <i>actual</i> {@link ITupleIterator} produces the
     * expected values in the expected order. Errors are reported if too few or
     * too many values are produced, etc.
     */
    static public void assertSameIterator(byte[][] expected, ITupleIterator actual) {

        assertSameIterator("", expected, actual);

    }

    /**
     * Method verifies that the <i>actual</i> {@link ITupleIterator} produces
     * the expected values in the expected order. Errors are reported if too few
     * or too many values are produced, etc.
     */
    static public void assertSameIterator(String msg, final byte[][] expected,
            final ITupleIterator actual) {

        int i = 0;

        while (actual.hasNext()) {

            if (i >= expected.length) {

                fail(msg + ": The iterator is willing to visit more than "
                        + expected.length + " values.");

            }

            ITuple tuple = actual.next();

            final byte[] val = tuple.getValue();

            if (expected[i] == null) {

                if (val != null) {

                    /*
                     * Only do message construction if we know that the assert
                     * will fail.
                     */
                    fail(msg + ": Different values at index=" + i
                            + ": expected=null" + ", actual="
                            + Arrays.toString(val));

                }

            } else {

                if (val == null) {

                    /*
                     * Only do message construction if we know that the assert
                     * will fail.
                     */
                    fail(msg + ": Different values at index=" + i
                            + ": expected=" + Arrays.toString(expected[i])
                            + ", actual=null");

                }
                
                if (BytesUtil.compareBytes(expected[i], val) != 0) {
                    
                    /*
                     * Only do message construction if we know that the assert
                     * will fail.
                     */
                    fail(msg + ": Different values at index=" + i
                            + ": expected=" + Arrays.toString(expected[i])
                            + ", actual=" + Arrays.toString(val));
                    
                }

            }
            
            i++;

        }

        if (i < expected.length) {

            fail(msg + ": The iterator SHOULD have visited " + expected.length
                    + " values, but only visited " + i + " values.");

        }

    }

    /**
     * Verifies the data in the two indices using a batch-oriented key range
     * scans (this can be used to verify a key-range partitioned scale-out index
     * against a ground truth index) - only the keys and values of non-deleted
     * index entries in the <i>expected</i> index are inspected.  Deleted index
     * entries in the <i>actual</i> index are ignored.
     * 
     * @param expected
     * @param actual
     */
    public static void assertSameEntryIterator(IIndex expected, IIndex actual) {

        final ITupleIterator expectedItr = expected.rangeIterator(null, null);

        final ITupleIterator actualItr = actual.rangeIterator(null, null);

        assertSameEntryIterator(expectedItr, actualItr);

    }
    
    /**
     * Verifies that the iterators visit tuples having the same data in the same
     * order.
     * 
     * @param expectedItr
     * @param actualItr
     */
    public static void assertSameEntryIterator(
            final ITupleIterator expectedItr, final ITupleIterator actualItr) { 
        
        long nvisited = 0L;
        
        while (expectedItr.hasNext()) {

            assertTrue("Expecting another index entry: nvisited=" + nvisited,
                    actualItr.hasNext());

            final ITuple expectedTuple = expectedItr.next();

            final ITuple actualTuple = actualItr.next();

//            if(true) {
//                System.err.println("expected: " + expectedTuple);
//                System.err.println("  actual: " + actualTuple);
//            }
            
            nvisited++;

            if (!BytesUtil.bytesEqual(expectedTuple.getKey(), actualTuple
                    .getKey())) {

                fail("Wrong key: nvisited=" + nvisited + ", expected="
                        + expectedTuple + ", actual=" + actualTuple);

            }

            if (!BytesUtil.bytesEqual(expectedTuple.getValue(), actualTuple
                    .getValue())) {

                fail("Wrong value: nvisited=" + nvisited + ", expected="
                        + expectedTuple + ", actual=" + actualTuple);
                        
            }
            
        }
        
        assertFalse("Not expecting more tuples", actualItr.hasNext());
        
    }

    /**
     * Generate a set of N random distinct byte[] keys in sorted order using an
     * unsigned byte[] comparison function.
     * 
     * @param maxKeys
     *            The capacity of the array.
     * 
     * @param nkeys
     *            The #of keys to generate.
     * 
     * @return A byte[][] with nkeys non-null byte[] entries and a capacity of
     *         maxKeys.
     */
    public static byte[][] getRandomKeys(final int maxKeys, final int nkeys) {

        final int maxKeyLength = 20;

        final Random r = new Random();
        
        return new RandomKeysGenerator(r, maxKeys, maxKeyLength)
                .generateKeys(nkeys);

//
//        assert nkeys >= 0;
//        assert maxKeys >= nkeys;
//
//        /*
//         * generate maxKeys distinct keys (sort requires that the keys are
//         * non-null).
//         */
//        
//        // used to ensure distinct keys.
//        final Set<byte[]> set = new TreeSet<byte[]>(
//                BytesUtil.UnsignedByteArrayComparator.INSTANCE);
//
//        final byte[][] keys = new byte[maxKeys][];
//
//        int n = 0;
//
//        while (n < maxKeys) {
//
//            // random key length in [1:maxKeyLen].
//            final byte[] key = new byte[r.nextInt(maxKeyLen) + 1];
//
//            // random data in the key.
//            r.nextBytes(key);
//
//            if( set.add(key)) {
//
//                keys[n++] = key;
//
//            }
//            
//        }
//
//        /*
//         * place keys into sorted order.
//         */
//        Arrays.sort(keys, BytesUtil.UnsignedByteArrayComparator.INSTANCE);
//
//        /*
//         * clear out keys from keys[nkeys] through keys[maxKeys-1].
//         */
//        for (int i = nkeys; i < maxKeys; i++) {
//
//            keys[i] = null;
//
//        }
//
//        return keys;

    }

    /**
     * Generate random key-value data in key order.
     * <p>
     * Note: The auto-split feature of the scale-out indices depends on the
     * assumption that the data are presented in key order.
     * <p>
     * Note: This method MAY return entries having duplicate keys.
     * 
     * @param N
     *            The #of key-value pairs to generate.
     * 
     * @return The random key-value data, sorted in ascending order by key.
     * 
     * @see ClientIndexView#submit(int, byte[][], byte[][],
     *      com.bigdata.btree.IIndexProcedure.IIndexProcedureConstructor,
     *      com.bigdata.btree.IResultHandler)
     * 
     * @todo parameter for random deletes, in which case we need to reframe the
     *       batch operation since a batch insert won't work. Perhaps a
     *       BatchWrite would be the thing.
     * 
     * @todo use null values ~5% of the time.
     */
    public static KV[] getRandomKeyValues(final int N) {

        final Random r = new Random();

        final KV[] data = new KV[N];

        for (int i = 0; i < N; i++) {

            // @todo param governs chance of a key collision and maximum #of distinct keys.
            final byte[] key = KeyBuilder.asSortKey(r.nextInt(100000));

            // Note: #of bytes effects very little that we want to test so we keep it small.
            final byte[] val = new byte[4];

            r.nextBytes(val);
            
            data[i] = new KV(key, val);

        }

        Arrays.sort(data);
        
        return data;
        
    }

}
