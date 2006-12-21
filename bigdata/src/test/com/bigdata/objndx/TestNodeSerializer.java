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
 * Created on Oct 30, 2006
 */

package com.bigdata.objndx;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.Level;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.SlotMath;

/**
 * Test case for {@link NodeSerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNodeSerializer extends AbstractObjectIndexTestCase {

    /**
     * 
     */
    public TestNodeSerializer() {
    }

    /**
     * @param arg0
     */
    public TestNodeSerializer(String arg0) {
        super(arg0);
    }
    
    /**
     * Prints out the offsets and various other sizing information for nodes and
     * leaves given the slotSize for the journal and the branching factor for
     * the object index. Note that this information reflects the maximum space
     * requirement. Actual serialization uses a variety of techniques to produce
     * a more compact format.
     * 
     * @param slotSize
     *            The size of a slot on the journal.
     * @param branchingFactor
     *            The #of keys in a node of the object index (aka the branching
     *            factor).
     * 
     * @todo this test helper is rather specific to the concept of an object
     *       index and therefore somewhat dated.  Also, it presumes that much
     *       of the serialized format is fixed, but we actually pack everything
     *       except the header.
     */
    public void showInfo(int slotSize,int branchingFactor) {

        // @todo drop use of the slotMath and the computation of #of slots used.
        final SlotMath slotMath = new SlotMath(slotSize);
        
        final IndexEntrySerializer valueSer = new IndexEntrySerializer(slotMath);
        
        final NodeSerializer nodeSer = new NodeSerializer(
                BTree.NodeFactory.INSTANCE, Int32OIdKeySerializer.INSTANCE,
                valueSer);

        System.err.println("Shared record format:");

        System.err.println("slotSize : " + slotSize);
        
        System.err
                .println("pageSize : " + branchingFactor + " (aka branching factor)");
        
        System.err.println(" checksum: offset="
                + NodeSerializer.OFFSET_CHECKSUM + ", size="
                + NodeSerializer.SIZEOF_CHECKSUM);
        
        System.err.println(" nbytes  : offset=" + NodeSerializer.OFFSET_NBYTES
                + ", size=" + NodeSerializer.SIZEOF_NBYTES);

        System.err.println(" nodeType: offset=" + NodeSerializer.OFFSET_NODE_TYPE
                + ", size=" + NodeSerializer.SIZEOF_NODE_TYPE);
        
        System.err.println(" version : offset=" + NodeSerializer.OFFSET_VERSION
                + ", size=" + NodeSerializer.SIZEOF_VERSION);

        /*
         * iff a linked leaf (not used for node or unlinked leaf).
         */
        System.err.println(" prior   : offset=" + NodeSerializer.OFFSET_PRIOR
                + ", size=" + NodeSerializer.SIZEOF_REF);

        System.err.println(" next    : offset=" + NodeSerializer.OFFSET_NEXT
                + ", size=" + NodeSerializer.SIZEOF_REF);

        /*
         * the different fixed length header sizes.
         */
        System.err.println(" node header    : size=" + NodeSerializer.SIZEOF_NODE_HEADER);
        System.err.println(" leaf header    : size=" + NodeSerializer.SIZEOF_LEAF_HEADER);
        System.err.println(" linked leaf hdr: size=" + NodeSerializer.SIZEOF_LINKED_LEAF_HEADER);


        /*
         * a node
         */
        {

            int nkeys = branchingFactor - 1;
            int nchildren = branchingFactor;
            int keysSize = nodeSer.keySerializer.getSize(nkeys);
            int valuesSize = (nchildren * NodeSerializer.SIZEOF_REF);
//            int offsetValues = NodeSerializer.OFFSET_KEYS + keysSize;
            
            System.err.println("Node specific record format:");
            
            System.err.println(" key[]" +
//                    ": offset="+ NodeSerializer.OFFSET_KEYS +
//                    ", size="+ NodeSerializer.SIZEOF_KEY +
                    ": #keys=" + nkeys
                    + ", #bytes=" + keysSize);
            
            System.err.println(" value   : child node ref         ("
                    + NodeSerializer.SIZEOF_REF + ")");
            
            System.err.println(" value   : total node value       ("
                    + NodeSerializer.SIZEOF_REF + ")");
            
            System.err.println(" value[]"+
//                    + " : offset="+offsetValues +
                    ": size="+NodeSerializer.SIZEOF_REF +
                    ", #values="+nchildren +
                    ", #bytes="+ valuesSize );

            final int nodeSize = nodeSer.getSize(false, branchingFactor);

            final int slotsPerNode = slotMath.getSlotCount(nodeSize);

            System.err.println(" totals  : nodeSize=" + nodeSize
                    + ", slotsPerNode=" + slotsPerNode
                    + ", #bytesInThoseSlots=" + (slotsPerNode * slotSize)
                    + ", wastePerNode=" + (slotsPerNode * slotSize - nodeSize));

        }

        /*
         * a leaf
         */
        {
            // assume #of keys == branching factor.
            int nkeys = branchingFactor;
            int keysSize = nodeSer.keySerializer.getSize(nkeys);
            int valuesSize = valueSer.getSize(nkeys);
//            int offsetValues = NodeSerializer.OFFSET_KEYS + keysSize;

            System.err.println("Leaf specific record format:");
            
            System.err.println(" key[]"+
//                    ": offset="+ NodeSerializer.OFFSET_KEYS +
//                    ", size="+ NodeSerializer.SIZEOF_KEY +
                    ": #keys=" + branchingFactor
                    + ", #bytes=" + keysSize);
            
            System.err.println(" value   : versionCounter         ("
                    + IndexEntrySerializer.SIZEOF_VERSION_COUNTER + ")");
            
            System.err.println(" value   : currentVersion ref     ("
                    + IndexEntrySerializer.SIZEOF_SLOTS + ")");
            
            System.err.println(" value   : preExistingVersion ref ("
                    + IndexEntrySerializer.SIZEOF_SLOTS + ")");
            
            System.err.println(" value   : total leaf value       ("
                    + IndexEntrySerializer.SIZEOF_LEAF_VALUE + ")");

            System.err.println(" value[] : #values=" + nkeys + ", #bytes="
                    + valuesSize);

            final int leafSize = nodeSer.getSize(true, branchingFactor - 1);

            final int slotsPerLeaf = slotMath.getSlotCount(leafSize);

            System.err.println(" totals  : leafSize=" + leafSize
                    + ", slotsPerLeaf=" + slotsPerLeaf
                    + ", #bytesInThoseSlots=" + (slotsPerLeaf * slotSize)
                    + ", wastePerLeaf=" + (slotsPerLeaf * slotSize - leafSize));
        }
        
    }

    /**
     * Show size info.
     */
    public void test_sizeInfo_slotSize64_pageSize512() {
        
        showInfo(64, 512);
        
    }
    
//    /**
//     * Show size info.
//     */
//    public void test_sizeInfo_slotSize256_pageSize1024() {
//        
//        showInfo(256,1024);
//        
//    }
    
    /**
     * Overrides to use the {@link IndexEntrySerializer}.
     */
    public BTree getBTree(int branchingFactor) {
        
        IRawStore store = new SimpleStore();
        
        final int leafQueueCapacity = 10000;
        
        final int nscan = 10;

        BTree btree = new BTree(store,
                ArrayType.INT,
                branchingFactor,
                new HardReferenceQueue<PO>(new NoEvictionListener(),
                        leafQueueCapacity, nscan),
                        Integer.valueOf(0),
                        null, // no comparator for primitive key type.
                        Int32OIdKeySerializer.INSTANCE,
                        SimpleEntry.Serializer.INSTANCE);

        return btree;
        
    }

    /**
     * Test of leaf serialization.
     */
    public void test_leaf_serialization01() {
        
        final int branchingFactor = 8;
        
        BTree ndx = getBTree(branchingFactor);
        
        // Create test node.
        final Leaf expected = getRandomLeaf(ndx);
        
        expected.dump(System.err);
        
        final Leaf actual = (Leaf)doRoundTripTest( true, ndx, expected );

        actual.dump(System.err);
        
    }

    /**
     * Test that an attempt to deserialize a leaf as a node will fail.
     */
    public void test_leaf_as_node_serialization_correct_rejection() {
        
        final int branchingFactor = 8;

        BTree ndx = getBTree(branchingFactor);

        // Create test node.
        final Leaf expected = getRandomLeaf(ndx);

        NodeSerializer nodeSer = ndx.getNodeSerializer();
        
        /*
         * Serialize onto a buffer.
         */
        ByteBuffer buf = ByteBuffer.allocate(getSize(nodeSer,expected));
        
        nodeSer.putLeaf(buf, expected);
        
        /*
         * Attempt to read the buffer back as a node (vs a leaf).
         */
        
        buf.clear(); // prepare for reading.

        try {
            
            nodeSer.getNode(ndx, expected.getIdentity(), buf);
            
            fail("Expecting exception: "+RuntimeException.class);
            
        } catch( RuntimeException ex ) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }

    }
    
    /**
     * Test that an attempt to deserialize a node as a leaf will fail.
     */
    public void test_node_as_leaf_serialization_correct_rejection() {
        
        final int branchingFactor = 8;
        
        BTree ndx = getBTree( branchingFactor);

        // Create test node.
        final Node expected = getRandomNode(ndx);

        NodeSerializer nodeSer = ndx.getNodeSerializer();
        
        /*
         * Serialize onto a buffer.
         */
        ByteBuffer buf = ByteBuffer.allocate(getSize(nodeSer,expected));
        
        nodeSer.putNode(buf, expected);
        
        /*
         * Attempt to read the buffer back as a leaf (vs a node).
         */
        
        buf.clear(); // prepare for reading.

        try {
            
            nodeSer.getLeaf(ndx, expected.getIdentity(), buf);
            
            fail("Expecting exception: "+RuntimeException.class);
            
        } catch( RuntimeException ex ) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }

    }
    
    /**
     * Test of node serialization.
     */
    public void test_node_serialization01() {
        
        final int branchingFactor = 8;

        BTree ndx = getBTree(branchingFactor);

        // Create test node.
        final Node expected = getRandomNode(ndx);

        expected.dump(System.err);

        final Node actual = (Node)doRoundTripTest( true, ndx, expected);

        actual.dump(System.err);

    }
    
    /**
     * Round trip serialization test.
     * 
     * @param ndx
     *            The object index.
     * @param expected
     *            The node or leaf to be serialized.
     *            
     * @return The de-serialized node or leaf.
     */
    public IAbstractNode doRoundTripTest(boolean verbose, BTree ndx,
            AbstractNode expected) {

        final boolean isLeaf = expected.isLeaf();
        
        NodeSerializer nodeSer = ndx.getNodeSerializer();
        
        final int BUF_SIZE = getSize(nodeSer,expected);
        
        ByteBuffer buf = ByteBuffer.allocate(BUF_SIZE);
        
        if (isLeaf) {

            nodeSer.putLeaf(buf, (Leaf)expected);
            
        } else {
            
            nodeSer.putNode(buf, (Node)expected);
            
        }
        
        if (verbose)
            System.err.println("buf: " + Arrays.toString(buf.array()));
        
        buf.flip(); // prepare for reading.
        
        AbstractNode actual = (AbstractNode) nodeSer.getNodeOrLeaf(ndx,
                expected.getIdentity(), buf);

        if (verbose)
            actual.dump(Level.DEBUG,System.err);

        // write on buf2.
        ByteBuffer buf2 = ByteBuffer.allocate(BUF_SIZE);

        if (isLeaf) {

            nodeSer.putLeaf(buf2, (Leaf)expected);
            
        } else {
            
            nodeSer.putNode(buf2, (Node)expected);
            
        }
        
        if (verbose)
            System.err.println("buf2: " + Arrays.toString(buf2.array()));
        
        // compare buffers - they should have images of the same node.
        assertEquals(buf.array(),buf2.array());

        /*
         * Overwrite the checksum to test for correct reporting of checksum
         * failure.
         */

        final int checksum = buf.getInt(NodeSerializer.OFFSET_CHECKSUM);
        
        int randomChecksum;
        
        do {
            
            randomChecksum = r.nextInt();
            
        } while( randomChecksum == checksum );
        
        buf.putInt(NodeSerializer.OFFSET_CHECKSUM, randomChecksum);
        
        buf.flip(); // prepare for re-reading.
        
        try {
        
            nodeSer.getNodeOrLeaf(ndx, expected.getIdentity(),buf);
            
            fail("Expecting: "+ChecksumError.class);
            
        } catch( ChecksumError ex ) {
            
            if( verbose) 
                System.err.println("Ignoring expected exception: "+ex);
            
        }

        /*
         * Override the a byte in the serialized record to test for a correctly
         * reported checksum error.
         */

        final byte b = buf2.get(NodeSerializer.OFFSET_NODE_TYPE);
        
        byte randomByte;
        
        do {
            
            randomByte= (byte) r.nextInt(255);
            
        } while( randomByte == b );
        
        buf2.put(NodeSerializer.OFFSET_NODE_TYPE, randomByte );
        
        buf2.flip(); // prepare for re-reading.
        
        try {
        
            nodeSer.getNodeOrLeaf(ndx,expected.getIdentity(),buf2);
            
            fail("Expecting: "+ChecksumError.class);
            
        } catch( ChecksumError ex ) {
            
            if( verbose )
                System.err.println("Ignoring expected exception: "+ex);
            
        }
        
        assertSameNodeOrLeaf(expected,actual);

        return actual;
        
    }
    
    /**
     * Small (de-)serialization stress test conducted for a variety of slot
     * sizes and branching factors.
     * 
     * @see #main(String[] args) for a large stress test.
     */
    public void testStress() {
     
        int ntrials = 20;
        int nnodes = 1000;
        
        doStressTest( ntrials,nnodes);
        
    }

    /**
     * Run a stress test.
     * 
     * @param ntrials
     *            The #of trials. Each trial has a random slotSize and
     *            branchingFactor.
     * @param nnodes
     *            The #of random nodes per trial.
     */
    public void doStressTest(int ntrials,int nnodes) {

        // Some branching factors to choose from.
        int[] branchingFactors = new int[] { 8, 16, 32, 48, 64, 96, 112, 128,
                256, 512, 1024 };
        
        for (int trial = 0; trial < ntrials; trial++) {

            // Choose the branching factor randomly.
            final int branchingFactor = branchingFactors[r.nextInt(branchingFactors.length)];

            BTree ndx = getBTree(branchingFactor);

            System.err.println("Trial " + trial + " of " + ntrials
                    + " : testing " + nnodes
                    + " random nodes:  branchingFactor=" + branchingFactor);
            
            for( int i=0; i<nnodes; i++ ) {
                
                AbstractNode expected = getRandomNodeOrLeaf(ndx);
                
                doRoundTripTest( false, ndx, expected);
                
            }
            
        }
        
    }

    /**
     * The #of bytes requires to serialize this node or leaf.
     * 
     * @param node
     *            The node or leaf.
     * 
     * @return The #of bytes required to serialize that node or leaf.
     */
    protected int getSize(NodeSerializer nodeSer,AbstractNode node) {

        return nodeSer.getSize( node.isLeaf(), node.nkeys );
        
    }
    
    /**
     * Run a large stress test.
     * 
     * @param args
     *            unused.
     */
    public static void main(String[] args) {

        final int NTRIALS = 1000;
        final int NNODES = 10000;

        new TestNodeSerializer().doStressTest(NTRIALS, NNODES);
        
    }
    
}
