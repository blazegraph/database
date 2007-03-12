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
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Level;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.util.ChecksumError;

/**
 * Test case for {@link NodeSerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNodeSerializer extends AbstractBTreeTestCase {

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
    
//    /**
//     * Prints out the offsets and various other sizing information for nodes and
//     * leaves given the slotSize for the journal and the branching factor for
//     * the object index. Note that this information reflects the maximum space
//     * requirement. Actual serialization uses a variety of techniques to produce
//     * a more compact format.
//     * 
//     * @param slotSize
//     *            The size of a slot on the journal.
//     * @param branchingFactor
//     *            The #of keys in a node of the object index (aka the branching
//     *            factor).
//     * 
//     * @todo this test helper is rather specific to the concept of an object
//     *       index and therefore somewhat dated.  Also, it presumes that much
//     *       of the serialized format is fixed, but we actually pack everything
//     *       except the header.
//     */
//    public void showInfo(int slotSize,int branchingFactor) {
//
//        // @todo drop use of the slotMath and the computation of #of slots used.
//        final SlotMath slotMath = new SlotMath(slotSize);
//        
//        final IndexEntrySerializer valueSer = new IndexEntrySerializer(slotMath);
//        
//        final NodeSerializer nodeSer = new NodeSerializer(
//                BTree.NodeFactory.INSTANCE, PackedAddressSerializer.INSTANCE,
//                Int32OIdKeySerializer.INSTANCE, valueSer);
//
//        System.err.println("Shared record format:");
//
//        System.err.println("slotSize : " + slotSize);
//        
//        System.err
//                .println("pageSize : " + branchingFactor + " (aka branching factor)");
//        
//        System.err.println(" checksum: offset="
//                + NodeSerializer.OFFSET_CHECKSUM + ", size="
//                + NodeSerializer.SIZEOF_CHECKSUM);
//        
//        System.err.println(" nbytes  : offset=" + NodeSerializer.OFFSET_NBYTES
//                + ", size=" + NodeSerializer.SIZEOF_NBYTES);
//
//        System.err.println(" nodeType: offset=" + NodeSerializer.OFFSET_NODE_TYPE
//                + ", size=" + NodeSerializer.SIZEOF_NODE_TYPE);
//        
//        System.err.println(" version : offset=" + NodeSerializer.OFFSET_VERSION
//                + ", size=" + NodeSerializer.SIZEOF_VERSION);
//
//        /*
//         * iff a linked leaf (not used for node or unlinked leaf).
//         */
//        System.err.println(" prior   : offset=" + NodeSerializer.OFFSET_PRIOR
//                + ", size=" + NodeSerializer.SIZEOF_REF);
//
//        System.err.println(" next    : offset=" + NodeSerializer.OFFSET_NEXT
//                + ", size=" + NodeSerializer.SIZEOF_REF);
//
//        /*
//         * the different fixed length header sizes.
//         */
//        System.err.println(" node header    : size=" + NodeSerializer.SIZEOF_NODE_HEADER);
//        System.err.println(" leaf header    : size=" + NodeSerializer.SIZEOF_LEAF_HEADER);
//        System.err.println(" linked leaf hdr: size=" + NodeSerializer.SIZEOF_LINKED_LEAF_HEADER);
//
//
//        /*
//         * a node
//         */
//        {
//
//            int nkeys = branchingFactor - 1;
//            int nchildren = branchingFactor;
//            int keysSize = nodeSer.keySerializer.getSize(nkeys);
//            int valuesSize = (nchildren * NodeSerializer.SIZEOF_REF);
////            int offsetValues = NodeSerializer.OFFSET_KEYS + keysSize;
//            
//            System.err.println("Node specific record format:");
//            
//            System.err.println(" key[]" +
////                    ": offset="+ NodeSerializer.OFFSET_KEYS +
////                    ", size="+ NodeSerializer.SIZEOF_KEY +
//                    ": #keys=" + nkeys
//                    + ", #bytes=" + keysSize);
//            
//            System.err.println(" value   : child node ref         ("
//                    + NodeSerializer.SIZEOF_REF + ")");
//            
//            System.err.println(" value   : total node value       ("
//                    + NodeSerializer.SIZEOF_REF + ")");
//            
//            System.err.println(" value[]"+
////                    + " : offset="+offsetValues +
//                    ": size="+NodeSerializer.SIZEOF_REF +
//                    ", #values="+nchildren +
//                    ", #bytes="+ valuesSize );
//
//            final int nodeSize = nodeSer.getSize(false, branchingFactor);
//
//            final int slotsPerNode = slotMath.getSlotCount(nodeSize);
//
//            System.err.println(" totals  : nodeSize=" + nodeSize
//                    + ", slotsPerNode=" + slotsPerNode
//                    + ", #bytesInThoseSlots=" + (slotsPerNode * slotSize)
//                    + ", wastePerNode=" + (slotsPerNode * slotSize - nodeSize));
//
//        }
//
//        /*
//         * a leaf
//         */
//        {
//            // assume #of keys == branching factor.
//            int nkeys = branchingFactor;
//            int keysSize = nodeSer.keySerializer.getSize(nkeys);
//            int valuesSize = valueSer.getSize(nkeys);
////            int offsetValues = NodeSerializer.OFFSET_KEYS + keysSize;
//
//            System.err.println("Leaf specific record format:");
//            
//            System.err.println(" key[]"+
////                    ": offset="+ NodeSerializer.OFFSET_KEYS +
////                    ", size="+ NodeSerializer.SIZEOF_KEY +
//                    ": #keys=" + branchingFactor
//                    + ", #bytes=" + keysSize);
//            
//            System.err.println(" value   : versionCounter         ("
//                    + IndexEntrySerializer.SIZEOF_VERSION_COUNTER + ")");
//            
//            System.err.println(" value   : currentVersion ref     ("
//                    + IndexEntrySerializer.SIZEOF_SLOTS + ")");
//            
//            System.err.println(" value   : preExistingVersion ref ("
//                    + IndexEntrySerializer.SIZEOF_SLOTS + ")");
//            
//            System.err.println(" value   : total leaf value       ("
//                    + IndexEntrySerializer.SIZEOF_LEAF_VALUE + ")");
//
//            System.err.println(" value[] : #values=" + nkeys + ", #bytes="
//                    + valuesSize);
//
//            final int leafSize = nodeSer.getSize(true, branchingFactor - 1);
//
//            final int slotsPerLeaf = slotMath.getSlotCount(leafSize);
//
//            System.err.println(" totals  : leafSize=" + leafSize
//                    + ", slotsPerLeaf=" + slotsPerLeaf
//                    + ", #bytesInThoseSlots=" + (slotsPerLeaf * slotSize)
//                    + ", wastePerLeaf=" + (slotsPerLeaf * slotSize - leafSize));
//        }
//        
//    }
//
//    /**
//     * Show size info.
//     */
//    public void test_sizeInfo_slotSize64_pageSize512() {
//        
//        showInfo(64, 512);
//        
//    }
    
//    /**
//     * Show size info.
//     */
//    public void test_sizeInfo_slotSize256_pageSize1024() {
//        
//        showInfo(256,1024);
//        
//    }
    
    /**
     * Overrides to use the {@link SimpleEntry.Serializer}.
     */
    public BTree getBTree(int branchingFactor) {
        return getBTree(branchingFactor,false);
    }
    
    /**
     * Overrides to use the {@link SimpleEntry.Serializer}.
     * 
     * @param branchingFactor
     * 
     * @param useCompression
     *            when true, uses a {@link RecordCompressor}.
     * 
     * @return
     */
    public BTree getBTree(int branchingFactor,boolean useCompression) {
        
        IRawStore store = new SimpleMemoryRawStore();

        final int leafQueueCapacity = 10000;
        
        final int nscan = 10;
        
        BTree btree = new BTree(store, branchingFactor,
                new HardReferenceQueue<PO>(new NoEvictionListener(),
                        leafQueueCapacity, nscan),
                SimpleEntry.Serializer.INSTANCE,
                useCompression ? new RecordCompressor() : null);

        return btree;
        
    }

    /**
     * Test of leaf serialization.
     */
    public void test_leaf_serialization01() {
        
        final int branchingFactor = 8;
        
        BTree ndx = getBTree(branchingFactor,false);
        
        // Create test node.
        final Leaf expected = getRandomLeaf(ndx);
        
//        expected.dump(System.err);
        
        final Leaf actual = (Leaf)doRoundTripTest( true, ndx, expected );

//        actual.dump(System.err);
        
    }

    /**
     * Test of leaf serialization.
     */
    public void test_leaf_serialization_compressed01() {
        
        final int branchingFactor = 8;
        
        BTree ndx = getBTree(branchingFactor,true);
        
        // Create test node.
        final Leaf expected = getRandomLeaf(ndx);
        
//        expected.dump(System.err);
        
        final Leaf actual = (Leaf)doRoundTripTest( true, ndx, expected );

//        actual.dump(System.err);
        
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
        ByteBuffer buf = nodeSer.putLeaf(expected);
        
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
        ByteBuffer buf = nodeSer.putNode(expected);
        
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

        BTree ndx = getBTree(branchingFactor,false);

        // Create test node.
        final Node expected = getRandomNode(ndx);

//        expected.dump(System.err);

        final Node actual = (Node)doRoundTripTest( true, ndx, expected);

//        actual.dump(System.err);

    }
    
    /**
     * Test of node serialization.
     */
    public void test_node_serialization_compressed01() {
        
        final int branchingFactor = 8;

        BTree ndx = getBTree(branchingFactor,true);

        // Create test node.
        final Node expected = getRandomNode(ndx);

//        expected.dump(System.err);

        final Node actual = (Node)doRoundTripTest( true, ndx, expected);

//        actual.dump(System.err);

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
        
//        final int BUF_SIZE = getSize(nodeSer,expected);
        
//        ByteBuffer buf = ByteBuffer.allocate(BUF_SIZE);
        
        final ByteBuffer buf;
        
        if (isLeaf) {

            buf = clone(nodeSer.putLeaf((Leaf)expected));
            
        } else {
            
            buf = clone(nodeSer.putNode((Node)expected));
            
        }
        
        if (verbose)
            System.err.println("buf: " + Arrays.toString(buf.array()));
        
//        buf.flip(); // prepare for reading.
        assert buf.position() == 0;
        
        AbstractNode actual = (AbstractNode) nodeSer.getNodeOrLeaf(ndx,
                expected.getIdentity(), buf);
        
        assertEquals("position", 0,buf.position());

        if (verbose)
            actual.dump(Level.DEBUG,System.err);

        assertSameNodeOrLeaf(expected,actual);

        // write on buf2.
//        ByteBuffer buf2 = ByteBuffer.allocate(BUF_SIZE);
        final ByteBuffer buf2;

        if (isLeaf) {

            buf2 = clone(nodeSer.putLeaf((Leaf)expected));
            
        } else {
            
            buf2 = clone(nodeSer.putNode((Node)expected));
            
        }
        
        if (verbose)
            System.err.println("buf2: " + Arrays.toString(buf2.array()));
        
        // compare buffers - they should have images of the same node.
        assertEquals(buf.array(),buf2.array());

        /*
         * Overwrite the checksum to test for correct reporting of checksum
         * failure.
         * 
         * @todo the checksum mechanism works well when we are not using a
         * compression technique. depending on the compression technique, a
         * change in the compressed data may trigger a failure of the
         * decompression algorithm. in such cases we never get the decompressed
         * record and there for the checksum is not even computed.
         */

        if(nodeSer.useChecksum && nodeSer.recordCompressor==null ) {
            
        final int checksum = buf.getInt(NodeSerializer.OFFSET_CHECKSUM);
        
        int randomChecksum;
        
        do {
            
            randomChecksum = r.nextInt();
            
        } while( randomChecksum == checksum );
        
        buf.putInt(NodeSerializer.OFFSET_CHECKSUM, randomChecksum);

        assert buf.position() == 0;
//        buf.flip(); // prepare for re-reading.
        
        try {
        
            nodeSer.getNodeOrLeaf(ndx, expected.getIdentity(), buf);
            
            fail("Expecting: "+ChecksumError.class);
            
        } catch( ChecksumError ex ) {
            
            if( verbose) 
                System.err.println("Ignoring expected exception: "+ex);
            
        }

        /*
         * Override a byte in the serialized record to test for a correctly
         * reported checksum error.
         * 
         * Note: i am not overwritting the node type byte since that causes
         * a different exception (unknown node type) before we get as far as
         * the checksum test.
         */

        final byte b = buf2.get(NodeSerializer.OFFSET_VERSION);
        
        byte randomByte;
        
        do {
            
            randomByte= (byte) r.nextInt(255);
            
        } while( randomByte == b );
        
        buf2.put(NodeSerializer.OFFSET_VERSION, randomByte );
        
//        buf2.flip(); // prepare for re-reading.
        
        try {
        
            nodeSer.getNodeOrLeaf(ndx,expected.getIdentity(),buf2);
            
            fail("Expecting: "+ChecksumError.class);
            
        } catch( ChecksumError ex ) {
            
            if( verbose )
                System.err.println("Ignoring expected exception: "+ex);
            
        }
        }
        
        return actual;
        
    }
    
    /**
     * Creates a new buffer containing a copy of the data in the given buffer.
     * 
     * @param buf The buffer (in).  The data between the position and the limit
     * will be copied.
     * 
     * @return The buffer (out). The position will be zero and the limit will
     * be the capacity.
     */
    private ByteBuffer clone(ByteBuffer buf) {
       
        assert buf.position() == 0;
        
        int capacity = buf.limit();
        
        ByteBuffer ret = ByteBuffer.allocate(capacity);
        
        ret.put(buf.asReadOnlyBuffer());
        
        ret.clear();
        
        return ret;
        
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
     *            branchingFactor.  50% of the trials (on average) will
     *            use record compression.
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

            BTree ndx = getBTree(branchingFactor,r.nextBoolean());

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
    
    /**
     * A random {@link Addr} that is only syntactically valid (do not
     * dereference).
     */
    protected long nextAddr() {

        int offset = r.nextInt(Integer.MAX_VALUE/2);

        int nbytes = r.nextInt(1024);
        
        return Addr.toLong(nbytes,offset);
        
    }

//    /**
//     * Generates a random array of keys.
//     * 
//     * @param maxKeys
//     *            The #of key positions to allocate.  This is based on the
//     *            branching factor and whether the keys are for a node or a
//     *            leaf.
//     *            
//     * @param nkeys
//     *            The #of keys position to populate.
//     */
//    protected byte[][] getRandomKeys(int maxKeys, int nkeys) {
//
//        assert nkeys < maxKeys;
//        
//        byte[][] keys = new byte[maxKeys][];
//
//        // used to detect duplicate keys.
//        Set<byte[]> set = new TreeSet<byte[]>(
//                BytesUtil.UnsignedByteArrayComparator.INSTANCE);
//        
//        int n = 0;
//        
//        while( n < nkeys ) {
//            
//            // length of this key in bytes.
//            int len = r.nextInt(10)+1;
//            
//            byte[] key = new byte[len];
//            
//            r.nextBytes(key);
//            
//            if( set.add(key) ) {
//                
//                keys[n++] = key;
//                     
//            }
//            
//        }
//        
//        assert set.size() == nkeys;
//        
//        /*
//         * sort those random keys.
//         */
//        Arrays.sort(keys,BytesUtil.UnsignedByteArrayComparator.INSTANCE);
//
//        /*
//         * return the sorted keys to the caller.
//         */
//        
//        return keys;
//        
//    }
    
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
    public byte[][] getRandomKeys(int maxKeys, int nkeys) {
        
        assert maxKeys >= nkeys;
        
        final int maxKeyLen = 20;

        /*
         * generate maxKeys distinct keys (sort requires that the keys are
         * non-null).
         */
        
        // used to ensure distinct keys.
        Set<byte[]> set = new TreeSet<byte[]>(BytesUtil.UnsignedByteArrayComparator.INSTANCE);
        
        byte[][] keys = new byte[maxKeys][];

        int n = 0;
        
        while( n < maxKeys) {

            // random key length in [1:maxKeyLen].
            byte[] key = new byte[r.nextInt(maxKeyLen)+1]; 

            // random data in the key.
            r.nextBytes(key);

            if( set.add(key)) {

                keys[n++] = key;

            }
            
        }
    
        /* 
         * place keys into sorted order.
         */
        Arrays.sort(keys,BytesUtil.UnsignedByteArrayComparator.INSTANCE);

        /*
         * clear out keys from keys[nkeys] through keys[maxKeys-1].
         */
        for( int i=nkeys; i<maxKeys; i++ ) {
            
            keys[i] = null;
            
        }
        
        return keys;

    }

    /**
     * Generates a non-leaf node with random data.
     */
    public Node getRandomNode(BTree btree) {

        // #of keys per node.
        final int branchingFactor = btree.branchingFactor;

        final long addr = nextAddr();

        final int nchildren = r.nextInt((branchingFactor + 1) / 2)
                + (branchingFactor + 1) / 2;
        
        assert nchildren>=(branchingFactor+1)/2;
        
        assert nchildren<=branchingFactor;
        
        final int nkeys = nchildren-1;

        final byte[][] keys = getRandomKeys(branchingFactor,nkeys);
        
        final long[] children = new long[branchingFactor+1];

        final int[] childEntryCounts = new int[branchingFactor+1];
        
        // node with some valid keys and corresponding child refs.

        int nentries = 0;
        
        for (int i = 0; i < nchildren; i++) {

            children[i] = nextAddr();

            childEntryCounts[i] = r.nextInt(10)+1; // some non-zero count.  
            
            nentries += childEntryCounts[i];
            
        }
                
        /*
         * create the node and set it as the root to fake out the btree.
         */
        
        Node node = new Node(btree, addr, branchingFactor, nentries,
                new ImmutableKeyBuffer(nkeys, branchingFactor, keys), children,
                childEntryCounts);
        
        btree.root = node;

        return node;
        
    }

    /**
     * Generates a leaf node with random data.
     */
    public Leaf getRandomLeaf(BTree btree) {

        // #of keys per node.
        final int branchingFactor = btree.branchingFactor;

        long addr = nextAddr();

        int nkeys = r.nextInt((branchingFactor+1)/2)+(branchingFactor+1)/2;
        assert nkeys>=(branchingFactor+1)/2;
        assert nkeys<=branchingFactor;

        final byte[][] keys = getRandomKeys(branchingFactor+1,nkeys);
        
        final Object[] values = new Object[branchingFactor+1];

        for( int i=0; i<nkeys; i++ ) {

            values[i] = new SimpleEntry(r.nextInt());
        
        }

        /*
         * create the leaf and set it as the root to fake out the btree.
         */

        Leaf leaf = new Leaf(btree, addr, branchingFactor,
                new ImmutableKeyBuffer(nkeys, branchingFactor + 1, keys),
                values); // ,previous,next);
        
        btree.root = leaf;
        
        return leaf;

    }

    /**
     * Generates a node or leaf (randomly) with random data.
     */
    public AbstractNode getRandomNodeOrLeaf(BTree ndx) {

        if( r.nextBoolean() ) {
            
            return getRandomNode(ndx);
            
        } else {
            
            return getRandomLeaf(ndx);
            
        }
        
    }

}
