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
 * Created on Nov 6, 2006
 */

package com.bigdata.objectIndex;

import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.journal.ContiguousSlotAllocation;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SlotMath;
import com.bigdata.journal.TestSimpleObjectIndex;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * Abstract test case defines utility methods for testing the object index and
 * its nodes and leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractObjectIndexTestCase extends TestCase2 {

    Random r = new Random();
    
    /**
     * 
     */
    public AbstractObjectIndexTestCase() {
    }

    /**
     * @param name
     */
    public AbstractObjectIndexTestCase(String name) {
        super(name);
    }

    /**
     * Return a random reference to a child node or leaf. References are an
     * {@link ISlotAllocation} encoded as a long integer.
     * 
     * @param isLeaf
     *            When true, the reference will be to a leaf node. Otherwise the
     *            reference will be to a non-leaf node. (This effects the #of
     *            bytes encoded into the reference - the reference is only
     *            syntactically valid and MUST NOT be dereferenced).
     */
    private long nextNodeRef(boolean isLeaf, NodeSerializer nodeSer) {

        int firstSlot = r.nextInt(Integer.MAX_VALUE - 1) + 1;

        return SlotMath.toLong(isLeaf ? nodeSer.LEAF_SIZE : nodeSer.NODE_SIZE,
                firstSlot);

    }

    /**
     * Return a random version counter.
     * 
     * @todo Shape the distribution to make version0 and other low-numbered
     *       versions much more likely.
     */
    private short nextVersionCounter() {

        return (short)r.nextInt((int)Short.MAX_VALUE);

    }

    /**
     * Reference to a random data object.
     * 
     * @return A reference to a random data object. The reference is only
     *         syntactically valid and MUST NOT be dereferenced
     */
    private long nextVersionRef() {

        int nbytes = r.nextInt(512)+1;
        
        int firstSlot = r.nextInt(Integer.MAX_VALUE - 1) + 1;

        return SlotMath.toLong(nbytes, firstSlot);

    }
    
    /**
     * Return a random key value. Keys are always generated in a progressive
     * (sorted) order. A key is an int32 within segment persistent identifier
     * that is mapped by the object index onto an {@link ISlotAllocation}. The
     * key value of zero(0) is reserved to indicate null. Positive and negative
     * infinity are defined by the object index.
     * 
     * @param lastKey
     *            The last key value generated and {@link Node#NEGINF_KEY} on
     *            the first invocation for a node.
     * 
     * @see Node#POSINF_KEY
     * @see Node#NEGINF_KEY
     */
    private int nextKey(int pageSize,int index,int lastKey) {
        
        /*
         * The smallest key that we can generate on this pass.
         */
        int min = lastKey + 1;
        
        /*
         * The #of legal key values remaining.
         */
        int range = Node.POSINF-lastKey-1;
        
        assert range>0;
        
        /*
         * Divide up the remaining range by the key index position. This ensures
         * that we never run out of legal keys that we can assign until we have
         * first run out of keys that we need to assign.
         */

        int positionsRemaining = pageSize-index;
        
        assert positionsRemaining >= 0;
        
        range = range / positionsRemaining;
        
        assert range < Node.POSINF;
        
        /*
         * Generate a random key within the allowed range of legal keys.
         */
        
        int key = r.nextInt(range)+min;
        
        assert key > lastKey;
        assert key > Node.NEGINF;
        assert key < Node.POSINF;
        
        return key;
        
    }
    
    /**
     * Generates a non-leaf node with random data.
     */
    public Node getRandomNode(ObjectIndex ndx, NodeSerializer nodeSer) {

        // #of keys per node.
        final int pageSize = nodeSer.pageSize;
        
        final long recid = nextNodeRef(false,nodeSer); // ref. for this node.

        final int nkeys = r.nextInt(pageSize);
        
        final int[] keys = new int[pageSize];
        
        final long[] children = new long[pageSize];
        
        // node with some valid keys and corresponding child refs.

        int lastKey = Node.NEGINF;

        for (int i = 0; i < nkeys ; i++) {

            // reference is to either a leaf or a non-leaf node.
            boolean isLeaf = r.nextBoolean();

            lastKey = keys[i] = nextKey(pageSize, i, lastKey);

            children[i] = nextNodeRef(isLeaf, nodeSer);

        }

        // children[nkeys] is always defined.

        boolean isLeaf = r.nextBoolean();
        
        children[nkeys] = nextNodeRef(isLeaf, nodeSer);

        return new Node((BTree) ndx, recid, nkeys, keys, children);

    }

    /**
     * Generates a leaf node with random data.
     */
    public Leaf getRandomLeaf(ObjectIndex ndx, NodeSerializer nodeSer) {

        // #of keys per node.
        final int pageSize = nodeSer.pageSize;

        long recid = nextNodeRef(true, nodeSer); // ref. for this leaf.

        int nkeys = r.nextInt(pageSize);

        final int[] keys = new int[pageSize];

        final IObjectIndexEntry[] values = new IndexEntry[pageSize];

        // node with some valid keys and corresponding child refs.

        int lastKey = Node.NEGINF;

        for (int i = 0; i < nkeys; i++) {

            /*
             * Reference is to a data version.
             */

            // the key.
            lastKey = keys[i] = nextKey(pageSize, i, lastKey);

            // when true, the entry marks a deleted version.
            boolean isDeleted = r.nextInt(100) < 10;

            // when true, a preExisting version is defined on the journal.
            boolean isPreExisting = r.nextInt(100) < 50;

            short versionCounter = nextVersionCounter();

            long currentVersion = isDeleted ? 0L : nextVersionRef();

            long preExistingVersion = isPreExisting ? nextVersionRef() : 0L;

            IndexEntry entry = new IndexEntry(nodeSer.slotMath, versionCounter,
                    currentVersion, preExistingVersion);

            values[i] = entry;

        }

//        /*
//         * Set up prior/next leaf node references.
//         */
//        
//        // true unless this is the first leaf node of the index.
//        boolean hasPrevious = r.nextInt(100)>0;
//        
//        // true unless this is the last leaf node of the index.
//        boolean hasNext = r.nextInt(100)>0;
//        
//        long previous = hasPrevious ?nextNodeRef(true,nodeSer) : 0L;
//        
//        long next = hasNext ?nextNodeRef(true,nodeSer) : 0L;

        return new Leaf((BTree)ndx,recid,nkeys,keys,values); //,previous,next);

    }

    /**
     * Generates a node or leaf (randomly) with random data.
     */
    public AbstractNode getRandomNodeOrLeaf(ObjectIndex ndx,NodeSerializer nodeSer) {

        if( r.nextBoolean() ) {
            
            return getRandomNode(ndx,nodeSer);
            
        } else {
            
            return getRandomLeaf(ndx,nodeSer);
            
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
    public void assertEquals(Node n1, Node n2 ) {

        assertEquals("index",n1.btree,n2.btree);
        
        assertEquals("recid",n1.getIdentity(),n2.getIdentity());
        
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
    public void assertEquals(Leaf n1, Leaf n2 ) {

        assertEquals("index",n1.btree,n2.btree);
        
        assertEquals("recid",n1.getIdentity(),n2.getIdentity());
        
        assertEquals("pageSize",n1.branchingFactor,n2.branchingFactor);
        
        assertEquals("first",n1.nkeys,n2.nkeys);

        assertEquals("keys",n1.keys,n2.keys);
        
        assertEquals("values",n1.values,n2.values);
        
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
     * 
     * @todo Reuse for {@link TestSimpleObjectIndex}
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
     * 
     * @todo Reuse for {@link TestSimpleObjectIndex}
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
     * A non-persistence capable implementation of {@link IObjectIndexEntry}
     * used for unit tests.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MyIndexEntry implements IObjectIndexEntry {

        private short versionCounter;
        private ISlotAllocation currentVersionSlots;
        private ISlotAllocation preExistingVersionSlots;

        private MyIndexEntry() {

            throw new UnsupportedOperationException();
            
        }
        
        MyIndexEntry(short versionCounter,ISlotAllocation currentVersion, ISlotAllocation preExistingVersion ) {
            this.versionCounter = versionCounter;
            this.currentVersionSlots = currentVersion;
            this.preExistingVersionSlots = preExistingVersion;
        }
        
        public short getVersionCounter() {
            
            return versionCounter;
            
        }
        
        public boolean isDeleted() {
            
            return currentVersionSlots == null;
            
        }
        
        public boolean isPreExistingVersionOverwritten() {
            
            return preExistingVersionSlots != null;
            
        }
        
        public ISlotAllocation getCurrentVersionSlots() {

            return currentVersionSlots;
            
        }
        
        public ISlotAllocation getPreExistingVersionSlots() {
            
            return preExistingVersionSlots;
            
        }
        
        /**
         * Dumps the state of the entry.
         */
        public String toString() {
            return "{versionCounter=" + versionCounter + ", currentVersion="
                    + currentVersionSlots + ", preExistingVersion="
                    + preExistingVersionSlots + "}";
        }
        
    }

}
